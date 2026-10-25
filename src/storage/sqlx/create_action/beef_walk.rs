use super::beef_graph::{
    compact_stored_beef, flag_txid_leaf, merge_beef_linked, merge_proof_for, prune_beef_to_roots,
    validate_stored_beef,
};
use super::beef_storage::{get_stored_beef, get_tx_with_proof, parse_input_txids};
use super::broadcast::{
    fetch_and_store_merkle_path, local_record_age_secs, try_network_fallback,
    MIN_AGE_FOR_PROOF_LOOKUP_SECS,
};
use super::StorageSqlx;
use crate::error::{Error, Result};
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use sqlx::sqlite::{SqliteConnection, SqlitePool};
use std::collections::HashSet;

/// How many merkle paths one BEEF assembly may fetch from services.
///
/// The ancestor walk runs newest-first and stops at the first proven
/// ancestor, so in practice one or two fetches settle a chain. The budget
/// only caps the pathological case where nothing on the chain is mined yet.
const MAX_PROOF_FETCHES_PER_WALK: usize = 8;

/// Core BFS ancestor walk for BEEF construction.
///
/// Given a queue of `(txid, depth)` pairs, walks the ancestor chain using:
/// - Stored BEEFs (`get_stored_beef`) — merged and compacted
/// - Individual tx+proof lookups (`get_tx_with_proof`)
/// - Network fallback (`try_network_fallback`) when local lookup fails
///
/// For unproven transactions, recurses into their inputs until each line of
/// ancestry reaches a proven transaction, at any depth: a valid BEEF is never
/// refused, or cut short, for its counts. The reference stops at 12 and
/// throws (ts-stack@edf6e03 StorageProvider.ts:199,
/// getBeefForTransaction.ts:156-157); this walk does not. It ends because each
/// txid is walked once (`processed_txids`) and an ancestor is older than its
/// descendant, so the walk is linear in the transactions it carries.
/// Direct inputs (depth 0) that cannot be found cause an error; deeper ancestors just warn.
///
/// This is the shared core used by both `build_input_beef` (create_action) and
/// `rebuild_beef_for_broadcast` (send_waiting_transactions). Only a walk given
/// `storage` asks a provider (the rebuild); `build_input_beef` passes none.
/// With no `writer` (create_action under its write lock) the walk's read
/// checks decide and write nothing.
/// Does the chain tracker DEFINITELY refute a stored bump for `txid`?
///
/// Only `Ok(false)` from the tracker refutes; no tracker, a tracker fault
/// (`Err`, since 0.3.66 what the fallback tracker answers when both header
/// sources fail), or a bump whose root cannot be computed keeps the bump
/// attached and lets the final BEEF verification decide, as before the
/// reorg work. An unknown never reads as a stale proof, and the walk never
/// mutates storage on this answer.
pub(in crate::storage::sqlx) async fn stored_bump_refuted(
    chain_tracker: Option<&dyn ChainTracker>,
    merkle_path: &MerklePath,
    txid: &str,
) -> bool {
    let Some(tracker) = chain_tracker else {
        return false;
    };
    let Ok(root) = merkle_path.compute_root(Some(txid)) else {
        return false;
    };
    matches!(
        tracker
            .is_valid_root_for_height(&root, merkle_path.block_height)
            .await,
        Ok(false)
    )
}

pub(in crate::storage::sqlx) async fn beef_bfs_walk(
    conn: &mut SqliteConnection,
    writer: Option<&SqlitePool>,
    beef: &mut Beef,
    pending_txids: &mut Vec<(String, usize)>,
    processed_txids: &mut HashSet<String>,
    storage: Option<&StorageSqlx>,
    chain_tracker: Option<&dyn ChainTracker>,
) -> Result<()> {
    let mut proof_fetch_budget = MAX_PROOF_FETCHES_PER_WALK;
    // The tracker for the one-time check of a stored proof that was never
    // checked (P0-1): the caller's, else the storage's own, so the broadcast
    // rebuild and the BEEF-for-txids path (which pass none) check it too.
    let storage_tracker = match (chain_tracker, storage) {
        (None, Some(s)) => s.get_chain_tracker().await,
        _ => None,
    };
    let read_check_tracker: Option<&dyn ChainTracker> =
        chain_tracker.or(storage_tracker.as_deref());

    while let Some((txid, depth)) = pending_txids.first().cloned() {
        pending_txids.remove(0);

        if processed_txids.contains(&txid) {
            continue;
        }
        processed_txids.insert(txid.clone());

        // A BUMP already in the BEEF proves this transaction regardless of what
        // our own proven_txs table knows: the merkle path is the evidence. Carry
        // its raw bytes if they are still missing, LINK the transaction to that
        // BUMP, and stop: no ancestor of a proven transaction belongs in the BEEF.
        //
        // The link is explicit on purpose. `find_bump` matches any level-0 leaf
        // whose hash is this txid, flagged or not, but a merged BUMP built from a
        // sibling's proof carries this txid as a plain sibling hash (`txid:
        // false`), and the merge's own linker only accepts flagged leaves. Left
        // unlinked, the transaction reads as unproven, the sorter demands its
        // parent, the parent was never walked, and the whole BEEF is refused:
        // LOW seat p25, five roots, two of them siblings in block 964422,
        // "missing inputs [2617c486…]" (2026-09-04).
        if let Some(bump_idx) = beef.bumps.iter().position(|b| b.contains(&txid)) {
            flag_txid_leaf(&mut beef.bumps[bump_idx], &txid);
            // Decide from a copy: no borrow of `beef` may live across the await.
            let existing = beef
                .find_txid(&txid)
                .map(|e| (e.bump_index().is_some(), e.raw_tx().map(<[u8]>::to_vec)));
            let raw = match existing {
                Some((true, _)) => None,
                Some((false, raw)) => raw,
                None => get_tx_with_proof(&mut *conn, &txid)
                    .await?
                    .map(|data| data.raw_tx),
            };
            if let Some(raw) = raw {
                beef.merge_raw_tx(raw, Some(bump_idx));
            }
            continue;
        }

        // Check if already in BEEF (from user inputBEEF or previously merged)
        if beef.find_txid(&txid).is_some() {
            continue;
        }

        // Individual transaction lookup — check for proof FIRST.
        // TS/Go pattern: if a tx has a merkle proof, add tx + BUMP and STOP.
        // Do NOT merge stored input_beef for proven txs — the BUMP terminates
        // the chain and ancestors are irrelevant.
        let mut tx_data_opt = get_tx_with_proof(&mut *conn, &txid).await?;

        // We hold the ancestor but not its proof. Ask the chain for the merkle
        // path once and record it: a BUMP terminates the walk here instead of
        // dragging this ancestor's whole unproven history into the BEEF.
        if let Some(data) = tx_data_opt.as_mut() {
            if data.merkle_path.is_none() && proof_fetch_budget > 0 {
                let old_enough = local_record_age_secs(&mut *conn, &txid)
                    .await
                    .map(|age| age >= MIN_AGE_FOR_PROOF_LOOKUP_SECS)
                    .unwrap_or(true);
                if old_enough {
                    proof_fetch_budget -= 1;
                    data.merkle_path = fetch_and_store_merkle_path(storage, &txid).await;
                }
            }
        }

        // Only merge stored input_beef for UNPROVEN transactions.
        // TS: "if (r.inputBEEF) beef.mergeBeef(r.inputBEEF)" — only when no proof.
        // Go: merges inputBEEF then checks "if subjectTx.MerklePath != nil { return }".
        // Both skip stored BEEF when a proof exists.
        let has_proof = tx_data_opt
            .as_ref()
            .map(|d| d.merkle_path.is_some())
            .unwrap_or(false);

        if !has_proof {
            if let Some(mut stored_beef) = get_stored_beef(&mut *conn, &txid).await? {
                compact_stored_beef(&mut *conn, writer, &mut stored_beef, read_check_tracker)
                    .await?;

                // A stored input_beef is whatever the chain looked like when the
                // transaction was built; ancestors proven since then make most of
                // it dead weight. Keep only the subject's closure, or, when the
                // stored BEEF holds only ancestors, the closure of its inputs.
                let mut stored_roots = vec![txid.clone()];
                if stored_beef.find_txid(&txid).is_none() {
                    if let Some(data) = tx_data_opt.as_ref() {
                        if let Ok(inputs) = parse_input_txids(&data.raw_tx) {
                            if !inputs.is_empty() {
                                stored_roots = inputs;
                            }
                        }
                    }
                }
                prune_beef_to_roots(&mut stored_beef, &stored_roots);

                // Every root a stored BEEF carries is read against the
                // wallet's headers of the moment, whoever called the walk
                // (ruling 7c): the caller's tracker, else the storage's own.
                let stored_beef_valid = if let Some(tracker) = read_check_tracker {
                    validate_stored_beef(&mut stored_beef, tracker, &txid).await
                } else {
                    stored_beef.verify_valid(true).valid
                };

                if stored_beef_valid {
                    merge_beef_linked(beef, &stored_beef);

                    for beef_tx in &stored_beef.txs {
                        processed_txids.insert(beef_tx.txid());
                    }

                    // Only a structurally complete stored graph can end the
                    // walk. An unsourced stored body is discarded above and
                    // its raw subject takes the recursive input path below.
                    if beef.find_txid(&txid).is_some() {
                        continue;
                    }
                }
            }
        }

        // Fix 3: Network fallback — if not found locally, try fetching from services
        let tx_data_opt = if tx_data_opt.is_none() {
            try_network_fallback(storage, &txid).await?
        } else {
            tx_data_opt
        };

        if let Some(tx_data) = tx_data_opt {
            // If we have a merkle proof, add both tx and proof - no need to recurse
            let bump_index = if let Some(merkle_path_bytes) = &tx_data.merkle_path {
                match MerklePath::from_binary(merkle_path_bytes) {
                    Ok(merkle_path) => {
                        // P0-1: a stored proof that was never checked
                        // against a tracker (written before migration 005,
                        // or merged by sync) is checked once, here, and
                        // demoted when the tracker refutes it; it carries
                        // no evidence, so the refutation alone decides.
                        let read_check = super::proof_root_checks::check_unchecked_stored_proof_on(
                            &mut *conn,
                            writer,
                            read_check_tracker,
                            &txid,
                            &merkle_path,
                        )
                        .await?;
                        // The stack's ruling 7c (2026-10-20): a stored bump
                        // enters the BEEF only when the wallet's headers of
                        // the moment carry its root, read for every walk
                        // through the caller's tracker or else the storage's
                        // own, never merged as stored. A bump whose root the
                        // headers DEFINITELY refuse (a reorg moved the block)
                        // is not attached: the transaction rides as raw
                        // bytes (BRC-62) and its parents are walked one level
                        // deeper, to ancestors whose roots the headers carry.
                        // The reference merges a stored bump unchecked unless
                        // its caller passes a `chainTracker`
                        // (ts-stack@edf6e03 getBeefForTransaction.ts:103-105,
                        // 270-272). A CHECKED row is never mutated here, and
                        // a tracker fault keeps the bump (the final BEEF
                        // verification decides); the stale row is demoted by
                        // the reorg or review task on the local header read.
                        use super::proof_root_checks::ReadCheck;
                        if read_check == ReadCheck::Demoted {
                            None
                        } else if read_check != ReadCheck::Confirmed
                            && stored_bump_refuted(read_check_tracker, &merkle_path, &txid).await
                        {
                            tracing::warn!(
                                txid = %txid,
                                height = merkle_path.block_height,
                                marker = "stale_proof_skipped",
                                "beef walk: the stored merkle proof no longer matches the chain; walking the raw-tx leg instead"
                            );
                            None
                        } else {
                            // P0-1d: merged with this txid's own leaf
                            // flagged, so the link survives the bytes.
                            let bump_index = merge_proof_for(beef, merkle_path, &txid);
                            if bump_index.is_none() {
                                tracing::warn!(
                                    txid = %txid,
                                    marker = "stored_proof_not_for_txid",
                                    "beef walk: the stored merkle path does not carry this txid; walking the raw-tx leg instead"
                                );
                            }
                            bump_index
                        }
                    }
                    Err(e) => {
                        // Continue without proof - will need to recurse to ancestors
                        tracing::warn!(
                            txid = %txid,
                            error = %e,
                            "Failed to parse merkle path — will recurse to ancestors"
                        );
                        None
                    }
                }
            } else {
                None
            };

            // Add the raw transaction to BEEF
            beef.merge_raw_tx(tx_data.raw_tx.clone(), bump_index);

            // If no merkle proof, we need to recurse to this transaction's inputs
            // so the recipient can trace back to proven transactions
            if bump_index.is_none() {
                // Parse the transaction to get its input txids
                // Children are one level deeper in the chain
                if let Ok(input_txids) = parse_input_txids(&tx_data.raw_tx) {
                    for input_txid in input_txids {
                        if !processed_txids.contains(&input_txid)
                            && !pending_txids.iter().any(|(t, _)| t == &input_txid)
                        {
                            pending_txids.push((input_txid, depth + 1));
                        }
                    }
                }
            }
        } else {
            // Fix 2: Direct inputs (depth 0) MUST be found — error if missing.
            // Deeper ancestors may be proven elsewhere, so just warn.
            if depth == 0 {
                return Err(Error::TransactionError(format!(
                    "Cannot find raw tx for direct input {} — BEEF will be incomplete",
                    txid
                )));
            }
            tracing::warn!(
                txid = %txid,
                depth = depth,
                "Cannot find raw tx for ancestor — BEEF may be incomplete"
            );
        }
    }

    Ok(())
}
#[cfg(test)]
#[path = "tests/beef_walk_stored.rs"]
mod beef_walk_stored;

#[cfg(test)]
#[path = "tests/beef_walk_ancestry.rs"]
mod beef_walk_ancestry;
