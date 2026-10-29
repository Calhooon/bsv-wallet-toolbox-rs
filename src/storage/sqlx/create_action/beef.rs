use super::beef_graph::{
    describe_invalid_beef, merge_beef_linked, needed_by_roots, prune_beef_to_roots,
};
use super::beef_storage::get_tx_with_proof;
use super::beef_walk::beef_bfs_walk;
use super::inputs::ExtendedInput;
use super::AllocatedChangeInput;
use crate::error::{Error, Result};
use bsv_rs::transaction::beef_stream::display_hex;
use bsv_rs::transaction::{Beef, BeefTx, ChainTracker};
use sqlx::sqlite::{SqliteConnection, SqlitePool};
use std::collections::HashSet;

/// `trustSelf: 'known'`: a txid-only entry of the caller's `inputBEEF` that
/// names a transaction this wallet's storage holds is the wallet's own
/// evidence, so storage answers for it before `verify_valid` (ts-stack@edf6e03
/// wallet-toolbox `src/storage/methods/createAction.ts:876`, `906-911`,
/// `923-963`; the bytes and the proof from storage, `996-1002`). bsv-rs 0.4.2
/// reads a txid-only entry as valid only when the BEEF proves it; that is the
/// rule for a stranger's BEEF at a door, not for the wallet's known inputs.
///
/// Returns the caller's BEEF without the entries storage will answer for, and
/// their txids for the ancestor walk, which supplies each one's bytes and,
/// where storage holds it, its proof (or, for an unproven one, its stored
/// ancestry). An entry a BUMP of the caller's BEEF proves is kept as it came.
/// An entry no input reaches is dropped, as the reference's `beefForTxids`
/// drops it (`createAction.ts:893-896`). An entry an input reaches that
/// storage does not know is refused (`createAction.ts:934-937` for an
/// ancestor, `996-1000` for an input).
async fn resolve_known_txid_only_entries(
    conn: &mut SqliteConnection,
    user_beef: Beef,
    roots: &[String],
) -> Result<(Beef, Vec<String>)> {
    // Decided before any await: no borrow of a `BeefTx` (not `Sync`) may
    // live across one.
    let (kept, from_storage) = split_txid_only_entries(&user_beef, roots);
    for txid in &from_storage {
        if get_tx_with_proof(&mut *conn, txid).await?.is_none() {
            return Err(Error::ValidationError(format!(
                "inputBEEF: the txid-only entry {} is not known to this wallet: \
                 trustSelf 'known' vouches only for a transaction the wallet's \
                 storage holds; the BEEF must carry its bytes and its proof",
                txid
            )));
        }
    }
    Ok((kept, from_storage))
}

/// The caller's BEEF without the txid-only entries storage must answer for,
/// and their txids (`resolve_known_txid_only_entries`).
fn split_txid_only_entries(user_beef: &Beef, roots: &[String]) -> (Beef, Vec<String>) {
    if !user_beef.txs.iter().any(|tx| tx.is_txid_only()) {
        return (user_beef.clone(), Vec::new());
    }

    let txids: Vec<String> = user_beef.txs.iter().map(|tx| tx.txid()).collect();
    let needed = needed_by_roots(user_beef, &txids, roots);

    let mut kept = Beef::with_version(user_beef.version);
    for bump in &user_beef.bumps {
        kept.merge_bump(bump.clone());
    }
    let mut from_storage = Vec::new();
    for (i, tx) in user_beef.txs.iter().enumerate() {
        if tx.is_txid_only() {
            let txid = &txids[i];
            if user_beef.bumps.iter().any(|b| b.contains(txid)) {
                kept.merge_txid_only(txid.clone());
            } else if needed.contains(&i) {
                from_storage.push(txid.clone());
            }
        } else if let Some(raw) = tx.raw_tx() {
            kept.merge_raw_tx(raw.to_vec(), None);
        } else if let Some(t) = tx.tx() {
            kept.merge_transaction(t.clone());
        }
    }
    (kept, from_storage)
}

/// Builds input BEEF containing all input transactions with their merkle proofs.
///
/// Collects unique input txids from both user-provided inputs and allocated change inputs,
/// then recursively fetches all ancestor transactions until we reach transactions with
/// merkle proofs or can't find any more ancestors in storage.
///
/// This matches the TypeScript/Go `validateRequiredInputs` and `getBeefForTransaction` behavior:
/// 1. First merges user-provided inputBEEF (contains proofs for external inputs)
/// 2. Then adds storage transactions for known inputs
/// 3. Verifies BEEF against ChainTracker (merkle roots match block headers)
/// 4. Finally trims known_txids to txid-only format to reduce BEEF size
///
/// # Arguments
/// * `storage` - The storage backend to query
/// * `chain_tracker` - Optional chain tracker for merkle root verification (if None, skips verification)
/// * `extended_inputs` - User-provided inputs
/// * `change_inputs` - Allocated change inputs from storage
/// * `user_input_beef` - Optional user-provided BEEF with proofs for external inputs
/// * `known_txids` - TXIDs the recipient already has (will be trimmed to txid-only)
/// * `trust_self` - `trustSelf: 'known'`: the caller's txid-only entries are
///   resolved from storage (`resolve_known_txid_only_entries`)
/// * `read_check_tracker` - the walk's tracker when `chain_tracker` is `None`
///   (the storage's own): stored proofs are read against it, the assembled
///   roots are not
///
/// The BEEF is assembled from stored bytes only: no provider is asked
/// (`createAction` fetches nothing, bsv-wallet-cli #44). An ancestor whose
/// bytes are not stored, or a chain that reaches no stored proof, is refused,
/// naming the txid. `writer` is where the walk records its read checks; `None`
/// under the write lock (they were recorded by the pre-check).
///
/// # Returns
/// * `Ok(Some(beef_bytes))` - BEEF binary data if there are inputs
/// * `Ok(None)` - If there are no inputs
///
/// # Errors
/// * `ValidationError` - If BEEF structure is invalid or merkle roots don't match chain
/// * `TransactionError` - If a direct input (depth 0) cannot be found in any source
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_input_beef(
    conn: &mut SqliteConnection,
    writer: Option<&SqlitePool>,
    chain_tracker: Option<&dyn ChainTracker>,
    extended_inputs: &[ExtendedInput],
    change_inputs: &[AllocatedChangeInput],
    user_input_beef: Option<&[u8]>,
    known_txids: &[String],
    trust_self: bool,
    read_check_tracker: Option<&dyn ChainTracker>,
) -> Result<Option<Vec<u8>>> {
    // Collect unique input txids with their chain depth.
    // Depth tracks how far each txid is from the direct inputs (depth 0).
    // This matches the TypeScript reference which uses actual recursion depth,
    // not a flat counter — a tx with 10 inputs at depth 0 doesn't count as depth 10.
    let mut pending_txids: Vec<(String, usize)> = Vec::new();
    let mut processed_txids: HashSet<String> = HashSet::new();

    for input in extended_inputs {
        if !processed_txids.contains(&input.txid) {
            pending_txids.push((input.txid.clone(), 0));
        }
    }

    for input in change_inputs {
        if !processed_txids.contains(&input.txid)
            && !pending_txids.iter().any(|(t, _)| t == &input.txid)
        {
            pending_txids.push((input.txid.clone(), 0));
        }
    }

    if pending_txids.is_empty() {
        return Ok(None);
    }

    // Create BEEF structure (V2 format)
    let mut beef = Beef::new();

    // Gap #1: Merge user-provided inputBEEF FIRST
    // This contains proofs for external inputs not in our storage
    if let Some(input_beef_bytes) = user_input_beef {
        if !input_beef_bytes.is_empty() {
            // The caller's envelope was checked by the create validator
            // before any storage call (#416, #451).
            match Beef::from_binary(input_beef_bytes) {
                Ok(mut user_beef) => {
                    if trust_self {
                        let roots: Vec<String> =
                            pending_txids.iter().map(|(t, _)| t.clone()).collect();
                        let (kept, from_storage) =
                            resolve_known_txid_only_entries(&mut *conn, user_beef, &roots).await?;
                        user_beef = kept;
                        // The walk supplies each from storage; a root is
                        // already queued at depth 0.
                        for txid in from_storage {
                            if !pending_txids.iter().any(|(t, _)| t == &txid) {
                                pending_txids.push((txid, 1));
                            }
                        }
                    }
                    // P0-1d: a proven transaction the BEEF carries as a
                    // sibling hash is linked here; the walk never revisits
                    // a txid the caller's BEEF supplied.
                    merge_beef_linked(&mut beef, &user_beef);
                    // Mark txids from user BEEF as already processed
                    for tx in &user_beef.txs {
                        processed_txids.insert(tx.txid());
                    }
                }
                Err(e) => {
                    return Err(Error::ValidationError(format!(
                        "inputBEEF: invalid BEEF format: {}",
                        e
                    )));
                }
            }
        }
    }

    // The roots this BEEF has to prove: every direct input of the new
    // transaction. Captured before the walk drains the queue.
    let root_txids: Vec<String> = pending_txids.iter().map(|(t, _)| t.clone()).collect();

    // Delegate the BFS ancestor walk to the shared core function. No
    // storage: the walk asks no provider and finds no tracker of its own.
    beef_bfs_walk(
        &mut *conn,
        writer,
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        None,
        chain_tracker.or(read_check_tracker),
    )
    .await?;

    // One root transaction plus only its recursive proof dependencies: drop
    // everything a merged BEEF dragged along that no root reaches through an
    // unproven transaction.
    prune_beef_to_roots(&mut beef, &root_txids);

    // Validate every assembly before trimming known txids, including bodies
    // without BUMPs and calls with no tracker. Missing ancestry is an error,
    // and an ancestor this wallet does not store is named: createAction
    // fetches none (the monitor's backstop fetches proofs). The walk merges
    // children ahead of their parents; bsv-rs judges the order held
    // (BRC-62, parents ahead of their children, since 0.9.0), so the assembly is judged in the
    // order `to_binary` writes it.
    beef.sort_txs();
    let validation = beef.verify_valid(true);
    if !validation.valid {
        let missing = beef.sort_txs().missing_inputs;
        let not_stored = if missing.is_empty() {
            String::new()
        } else {
            format!(
                "ancestor(s) not stored, and createAction fetches none: [{}]; ",
                missing.join(",")
            )
        };
        return Err(Error::ValidationError(format!(
            "inputBEEF: BEEF structure is invalid: {}{}",
            not_stored,
            describe_invalid_beef(&mut beef, &root_txids)
        )));
    }

    if let Some(tracker) = chain_tracker {
        // Verify each merkle root against the ChainTracker
        for (height, root) in &validation.roots {
            match tracker.is_valid_root_for_height(root, *height).await {
                Ok(true) => {
                    // Root is valid, continue
                }
                Ok(false) => {
                    return Err(Error::ValidationError(format!(
                        "inputBEEF: invalid merkle root {} at height {}",
                        root, height
                    )));
                }
                Err(e) => {
                    // ChainTracker error (network, block not found, etc.)
                    // Match Go/TypeScript behavior: treat as validation failure
                    return Err(Error::ValidationError(format!(
                        "inputBEEF: failed to verify merkle root at height {}: {}",
                        height, e
                    )));
                }
            }
        }
    }

    // Gap #2: Trim known_txids to txid-only format
    // This reduces BEEF size when the recipient already has these transactions
    for known_txid in known_txids {
        beef.make_txid_only(known_txid);
    }

    // Serialize BEEF to binary
    let beef_bytes = beef.to_binary();

    // Only return BEEF if it contains data beyond the header
    // BEEF V2 header is 4 bytes (0x0100EFBE for V1 or 0x0200EFBE for V2)
    if beef_bytes.len() > 4 {
        Ok(Some(beef_bytes))
    } else {
        Ok(None)
    }
}

/// The node's word for the caller's `inputBEEF` (#426): its tip comes from
/// `tracker`, else the `services`' header service, asked only when an
/// unproven caller transaction is there to judge; its era policy from the
/// storage's `network`.
pub(super) struct CallerWord<'a> {
    pub(super) tracker: Option<&'a dyn ChainTracker>,
    pub(super) services: Option<std::sync::Arc<dyn crate::services::WalletServices>>,
    pub(super) network: &'a str,
}

/// The judged set of a posting route under the one-door rule (#426, #454).
/// Every unproven transaction in a chain has one judging door: bytes the
/// wallet received are judged at internalize under the node's word (#422),
/// bytes the wallet made at their own signing (#450), and the node judges
/// each at broadcast. So the posting routes judge nothing a second time:
///
/// - `createAction` judges the unproven raw transactions the caller's
///   `inputBEEF` brings that the assembly does not prove and the store does
///   not hold (`held`: a transaction with a row in `transactions`,
///   `proven_tx_reqs` or `proven_txs` was judged at its own door), the
///   same set whatever the caller asks back (every option assembles the
///   inputs, `returnTXIDOnly` too, #454, #463);
/// - `processAction` judges none: every transaction a post carries is the
///   store's, the posted roots (the wallet's own) and their stored ancestry
///   alike (the caller's bytes judged here at create, the wallet's earlier
///   transactions, what internalize received).
///
/// The roots and the structure of each chain stay the routes' structural
/// passes', unchanged.
fn judged_set(caller: &Beef, assembled: &Beef, held: &HashSet<String>) -> HashSet<String> {
    caller
        .txs
        .iter()
        .filter(|tx| !tx.is_txid_only())
        .map(BeefTx::txid)
        .filter(|txid| !held.contains(txid))
        .filter(|txid| {
            assembled
                .find_txid(txid)
                .is_some_and(|tx| !tx.is_txid_only() && tx.bump_index().is_none())
        })
        .collect()
}

/// The txids of `txids` the store holds: a row in `transactions`,
/// `proven_tx_reqs` or `proven_txs`, read on the pre-check's reader.
async fn store_holds(conn: &mut SqliteConnection, txids: &[String]) -> Result<HashSet<String>> {
    let mut held = HashSet::new();
    for txid in txids {
        let holds: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM transactions WHERE txid = ?1) \
             OR EXISTS(SELECT 1 FROM proven_tx_reqs WHERE txid = ?1) \
             OR EXISTS(SELECT 1 FROM proven_txs WHERE txid = ?1)",
        )
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
        if holds {
            held.insert(txid.clone());
        }
    }
    Ok(held)
}

/// Judge each input of every transaction in create's judged set
/// (`judged_set`) under the node's word (#426), on `assembled`: the BEEF
/// `build_input_beef` assembled for the pre-check, its structure and roots
/// already verified and no known txid trimmed, so every judged
/// transaction's ancestry is whole. An empty set asks no word and no tip.
pub(super) async fn judge_caller_beef(
    conn: &mut SqliteConnection,
    assembled: Option<&[u8]>,
    caller: Option<&[u8]>,
    word: &CallerWord<'_>,
) -> Result<()> {
    let (Some(assembled), Some(caller)) = (assembled, caller) else {
        return Ok(());
    };
    // The assembly parsed the caller's bytes already.
    let Ok(caller) = Beef::from_binary(caller) else {
        return Ok(());
    };
    let beef = Beef::from_binary(assembled)?;
    let raw: Vec<String> = judged_set(&caller, &beef, &HashSet::new())
        .into_iter()
        .collect();
    if raw.is_empty() {
        return Ok(());
    }
    let held = store_holds(conn, &raw).await?;
    let judged = judged_set(&caller, &beef, &held);
    if judged.is_empty() {
        return Ok(());
    }
    let tracker = match (word.tracker, word.services.as_deref()) {
        (Some(tracker), _) => tracker,
        (None, Some(services)) => services
            .get_chain_tracker()
            .await
            .map_err(super::beef_verification::header_fault)?,
        (None, None) => {
            return Err(super::beef_verification::header_fault(
                "no header service configured",
            ))
        }
    };
    super::beef_verification::verify_beef_word(assembled, tracker, word.network, &|txid| {
        judged.contains(&display_hex(txid))
    })
    .await
}

/// The gap in a change coin's stored ancestry: the BEEF for `txid` alone,
/// walked from stored bytes as `build_input_beef` walks it, is invalid.
/// Returns the ancestors it lacks (the coin's own txid when its bytes are
/// not stored, or when the walk reaches no proof with no input missing), or
/// `None` when the coin's BEEF is complete.
pub(super) async fn change_coin_gap(
    conn: &mut SqliteConnection,
    writer: Option<&SqlitePool>,
    tracker: Option<&dyn ChainTracker>,
    txid: &str,
) -> Result<Option<Vec<String>>> {
    let mut beef = Beef::new();
    let mut pending = vec![(txid.to_string(), 0)];
    let mut processed = HashSet::new();
    match beef_bfs_walk(
        &mut *conn,
        writer,
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        tracker,
    )
    .await
    {
        Ok(()) => {}
        // The coin's own transaction is not stored.
        Err(Error::TransactionError(_)) => return Ok(Some(vec![txid.to_string()])),
        Err(e) => return Err(e),
    }
    prune_beef_to_roots(&mut beef, &[txid.to_string()]);
    // Judged in the order `to_binary` writes, as the assembly above is.
    beef.sort_txs();
    if beef.verify_valid(true).valid {
        return Ok(None);
    }
    let missing = beef.sort_txs().missing_inputs;
    Ok(Some(if missing.is_empty() {
        vec![txid.to_string()]
    } else {
        missing
    }))
}
#[cfg(test)]
#[path = "tests/beef_inputs.rs"]
mod beef_inputs;

#[cfg(test)]
#[path = "tests/beef_options.rs"]
mod beef_options;

#[cfg(test)]
#[path = "tests/beef_ancestry.rs"]
mod beef_ancestry;

#[cfg(test)]
#[path = "tests/beef_stubs.rs"]
mod beef_stubs;
