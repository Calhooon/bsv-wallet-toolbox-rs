use super::beef_storage::parse_input_txids;
use crate::error::Result;
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use chrono::Utc;
use sqlx::sqlite::{SqliteConnection, SqlitePool};
use sqlx::Row;
use std::collections::HashSet;

/// Validate a stored BEEF's merkle proofs against ChainTracker.
///
/// Returns `true` if the stored BEEF is valid (safe to merge), `false` if any
/// merkle root is invalid or the BEEF structure is corrupt. Invalid BEEFs are
/// discarded so the caller can fall through to individual tx+proof lookup and
/// network fallback paths.
///
/// Matches Go's `VerifyBeef()` call in `create_process_inputs.go:144-150`.
pub(super) async fn validate_stored_beef(
    stored_beef: &mut Beef,
    tracker: &dyn ChainTracker,
    txid: &str,
) -> bool {
    let validation = stored_beef.verify_valid(true);
    if !validation.valid {
        tracing::warn!(
            txid = %txid,
            detail = %describe_invalid_beef(stored_beef, &[txid.to_string()]),
            "Discarding stored input_beef: BEEF structure is invalid. \
             Will fall through to individual tx lookup / network fallback."
        );
        return false;
    }

    for (height, root) in &validation.roots {
        match tracker.is_valid_root_for_height(root, *height).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    txid = %txid,
                    height = %height,
                    root = %root,
                    "Discarding stored input_beef: invalid merkle root at height {}. \
                     Will fall through to individual tx lookup / network fallback.",
                    height,
                );
                return false;
            }
            Err(e) => {
                tracing::warn!(
                    txid = %txid,
                    height = %height,
                    error = %e,
                    "Discarding stored input_beef: ChainTracker error at height {}. \
                     Will fall through to fallback.",
                    height,
                );
                return false;
            }
        }
    }

    true
}

/// Mark the level-0 leaf whose hash is `txid` as a txid leaf. A BUMP merged
/// from a sibling's proof carries our transaction as a plain hash; flagging
/// it is what lets the BEEF's own linker and validator treat it as proven.
pub(in crate::storage::sqlx) fn flag_txid_leaf(bump: &mut MerklePath, txid: &str) {
    if let Some(level0) = bump.path.first_mut() {
        for leaf in level0.iter_mut() {
            if leaf.hash.as_deref() == Some(txid) {
                leaf.txid = true;
            }
        }
    }
}

/// Merge `txid`'s merkle path into `beef` with `txid`'s own leaf flagged, and
/// return the BUMP's index; `None` when the path does not carry `txid` at
/// level 0 (it is not this transaction's proof).
///
/// A stored path can carry its own transaction as a plain hash and its
/// sibling as the txid leaf: the provider's path for one of two transactions
/// mined side by side (P0-1d, the beta write soak of 2026-10-08: the row for
/// 65e78b0a flagged 867358fe). Linked by index alone, the link lives only in
/// memory: bsv-rs's `verify_valid` accepts it (it checks `contains`), the
/// bytes leave the wallet, and the next `merge_beef`'s strict linker drops it.
/// The reference flags the leaf it links (ts-stack@edf6e03
/// packages/sdk/src/transaction/Beef.ts:812-821, :1329-1341).
pub(in crate::storage::sqlx) fn merge_proof_for(
    beef: &mut Beef,
    mut merkle_path: MerklePath,
    txid: &str,
) -> Option<usize> {
    if !merkle_path.contains(txid) {
        return None;
    }
    flag_txid_leaf(&mut merkle_path, txid);
    // Two paths for one block combine and keep every flag set in either.
    Some(beef.merge_bump(merkle_path))
}

/// Link every transaction in `beef` that has no BUMP to a BUMP whose level-0
/// leaves carry its txid, and flag that leaf, as the reference's merge does
/// (ts-stack@edf6e03 packages/sdk/src/transaction/Beef.ts:1329-1341). bsv-rs
/// links only flagged leaves, so a BEEF that carries a proven transaction as
/// a sibling hash (the template BEEF of P0-1d, handed back as `inputBEEF`)
/// arrives with that transaction unproven and its parent demanded. Every
/// level-0 hash of a merkle path is a transaction of that block, so the link
/// claims nothing the path does not prove; the roots are still checked.
pub(in crate::storage::sqlx) fn link_txids_carried_by_bumps(beef: &mut Beef) {
    let unlinked: Vec<(usize, String)> = beef
        .txs
        .iter()
        .enumerate()
        .filter(|(_, tx)| tx.bump_index().is_none())
        .map(|(i, tx)| (i, tx.txid()))
        .collect();
    let mut linked = false;
    for (i, txid) in unlinked {
        if let Some(bump_idx) = beef.bumps.iter().position(|b| b.contains(&txid)) {
            flag_txid_leaf(&mut beef.bumps[bump_idx], &txid);
            beef.txs[i].set_bump_index(Some(bump_idx));
            linked = true;
        }
    }
    if linked {
        // A transaction that became proven moves ahead of its children.
        beef.sort_txs();
    }
}

/// `merge_beef`, then the reference's link of carried txids.
pub(super) fn merge_beef_linked(beef: &mut Beef, other: &Beef) {
    beef.merge_beef(other);
    link_txids_carried_by_bumps(beef);
}

/// Why a BEEF failed structural validation, in one line, and the rejected
/// bytes on disk. "BEEF structure is invalid" alone cost a seat a day of
/// diagnosis (LOW p25, 2026-09-04): the sorter knows exactly which inputs
/// are missing and which entries it could not place, so say so, and write
/// the hex to `beef-rejected-<millis>.hex` under `std::env::temp_dir()`
/// (`$TMPDIR`) for a repro.
pub(in crate::storage::sqlx) fn describe_invalid_beef(beef: &mut Beef, roots: &[String]) -> String {
    let sr = beef.sort_txs();
    let short = |v: &[String]| -> String {
        if v.is_empty() {
            "none".to_string()
        } else {
            v.iter()
                .map(|t| t.chars().take(16).collect::<String>())
                .collect::<Vec<_>>()
                .join(",")
        }
    };
    let bytes = beef.to_binary();
    let dump = std::env::temp_dir().join(format!(
        "beef-rejected-{}.hex",
        Utc::now().timestamp_millis()
    ));
    let dumped = match std::fs::write(&dump, hex::encode(&bytes)) {
        Ok(()) => dump.display().to_string(),
        Err(e) => format!("not written ({e})"),
    };
    format!(
        "roots [{}]; missing inputs [{}]; transactions with missing inputs [{}]; \
         not valid [{}]; txid-only [{}]; {} tx(s), {} bump(s), {} bytes; dump {}",
        short(roots),
        short(&sr.missing_inputs),
        short(&sr.with_missing_inputs),
        short(&sr.not_valid),
        short(&sr.txid_only),
        beef.txs.len(),
        beef.bumps.len(),
        bytes.len(),
        dumped
    )
}

/// The indices of `beef.txs` the `roots` reach: each root, and the parents of
/// every unproven transaction reached, stopping at a proven or a txid-only
/// entry. `txids` is `beef.txs`' txids, in order.
pub(super) fn needed_by_roots(beef: &Beef, txids: &[String], roots: &[String]) -> HashSet<usize> {
    let mut index: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::with_capacity(txids.len());
    for (i, t) in txids.iter().enumerate() {
        index.insert(t.as_str(), i);
    }

    let mut needed: HashSet<usize> = HashSet::new();
    let mut queue: std::collections::VecDeque<usize> = roots
        .iter()
        .filter_map(|r| index.get(r.as_str()).copied())
        .collect();

    while let Some(i) = queue.pop_front() {
        if !needed.insert(i) {
            continue;
        }
        let tx = &beef.txs[i];
        // Proven, or nothing to walk into: this branch terminates here.
        if tx.bump_index().is_some() || tx.is_txid_only() {
            continue;
        }
        let raw = match tx.raw_tx() {
            Some(r) => r.to_vec(),
            None => continue,
        };
        if let Ok(parents) = parse_input_txids(&raw) {
            for parent in parents {
                if let Some(&pi) = index.get(parent.as_str()) {
                    if !needed.contains(&pi) {
                        queue.push_back(pi);
                    }
                }
            }
        }
    }

    needed
}

/// Prunes a BEEF down to the transactions the given roots actually need.
///
/// A transaction that carries a BUMP is self-proving: its merkle path
/// establishes it is in a block, so none of its ancestors belong in the BEEF.
/// This walks the dependency graph from `roots` and stops at every proven
/// transaction, keeping the artifact to one root transaction plus only its
/// recursive proof dependencies, which is what the TypeScript builder emits.
///
/// Unused BUMPs are dropped with the transactions that referenced them and the
/// surviving bump indices are rediscovered on merge, so the result always
/// serializes with consistent indices.
///
/// Roots that are absent from `beef` are ignored. If that leaves nothing to
/// keep, `beef` is left untouched: pruning must never empty a BEEF.
pub(in crate::storage::sqlx) fn prune_beef_to_roots(beef: &mut Beef, roots: &[String]) {
    if beef.txs.is_empty() || roots.is_empty() {
        return;
    }

    // `BeefTx::txid()` re-hashes the raw bytes on every call, so index once.
    let txids: Vec<String> = beef.txs.iter().map(|tx| tx.txid()).collect();
    let needed = needed_by_roots(beef, &txids, roots);

    if needed.is_empty() || needed.len() == beef.txs.len() {
        return;
    }

    // Rebuild: bumps first so each merged transaction rediscovers its index.
    let mut pruned = Beef::with_version(beef.version);
    pruned.atomic_txid = beef.atomic_txid.clone();

    let mut kept: Vec<usize> = needed.into_iter().collect();
    kept.sort_unstable();

    let mut bump_indices: Vec<usize> = kept
        .iter()
        .filter_map(|&i| beef.txs[i].bump_index())
        .collect();
    bump_indices.sort_unstable();
    bump_indices.dedup();
    for bi in bump_indices {
        if let Some(bump) = beef.bumps.get(bi) {
            pruned.merge_bump(bump.clone());
        }
    }

    for i in kept {
        let tx = &beef.txs[i];
        if tx.is_txid_only() {
            pruned.merge_txid_only(txids[i].clone());
        } else if let Some(raw) = tx.raw_tx() {
            pruned.merge_raw_tx(raw.to_vec(), None);
        } else if let Some(t) = tx.tx() {
            pruned.merge_transaction(t.clone());
        }
    }
    // The rebuild links strictly; keep every link the original carried.
    link_txids_carried_by_bumps(&mut pruned);

    *beef = pruned;
}

/// Compacts a stored BEEF by upgrading unproven transactions with current
/// merkle proofs from the `proven_txs` table, then trimming unnecessary
/// ancestor transactions.
///
/// When a stored BEEF was created, some of its transactions may have been
/// unproven (awaiting mining). Over time, those transactions get mined and
/// their proofs are stored in `proven_txs`. This function:
///
/// 1. Finds unproven transactions in the BEEF
/// 2. Checks `proven_txs` for newly-available merkle proofs
/// 3. Upgrades those transactions with their BUMPs
/// 4. Trims ancestor transactions that are no longer needed (because their
///    dependents are now self-proving via BUMPs)
///
/// This can dramatically reduce BEEF size: a 200KB stored BEEF with long
/// unproven ancestor chains can shrink to a few KB once ancestors are proven.
pub(in crate::storage::sqlx) async fn compact_stored_beef(
    conn: &mut SqliteConnection,
    writer: Option<&SqlitePool>,
    beef: &mut Beef,
    chain_tracker: Option<&dyn ChainTracker>,
) -> Result<()> {
    // Collect unproven txids in the BEEF
    let unproven_txids: Vec<String> = beef
        .txs
        .iter()
        .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
        .map(|tx| tx.txid())
        .collect();

    if unproven_txids.is_empty() {
        return Ok(());
    }

    // Batch query proven_txs for all unproven txids
    // SQLite bind param limit is 999, batch in chunks of 400
    for chunk in unproven_txids.chunks(400) {
        let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query_str = format!(
            "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for txid in chunk {
            query = query.bind(txid);
        }

        let rows = query.fetch_all(&mut *conn).await?;

        for row in &rows {
            let txid: String = row.get("txid");
            let merkle_path_bytes: Vec<u8> = row.get("merkle_path");

            if let Ok(merkle_path) = MerklePath::from_binary(&merkle_path_bytes) {
                // P0-1: a stored proof that was never checked is checked
                // here, once, as in the walk; one the tracker refutes is
                // demoted. Compaction attaches only what the ingest would
                // store (P0-1c): with no tracker or a tracker fault the
                // ancestor stays a raw leg (its own ancestors are still in
                // the stored BEEF) and the row stays unchecked.
                if !super::proof_root_checks::check_unchecked_stored_proof_on(
                    &mut *conn,
                    writer,
                    chain_tracker,
                    &txid,
                    &merkle_path,
                )
                .await?
                .may_store()
                {
                    continue;
                }
                // P0-1d: linked with its own leaf flagged.
                let Some(bump_index) = merge_proof_for(beef, merkle_path, &txid) else {
                    continue;
                };
                if let Some(tx) = beef.find_txid_mut(&txid) {
                    tx.set_bump_index(Some(bump_index));
                }
            }
        }
    }

    // Trimming is deliberately left to the caller: only the caller knows which
    // transactions the BEEF has to prove. `prune_beef_to_roots` takes those
    // roots and stops at every BUMP. `Beef::trim_known_proven` is not used:
    // it walks from whichever transactions nothing else spends, so an unproven
    // input can be dropped merely because a proven sibling descends from it.

    Ok(())
}
#[cfg(test)]
#[path = "tests/beef_graph.rs"]
mod tests;
