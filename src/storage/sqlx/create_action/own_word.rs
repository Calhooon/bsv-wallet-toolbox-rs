//! The wallet's own newly signed transaction judged at its signing (#450):
//! the create door's `signAndProcess` before processAction's writes (the
//! reference's `signer/methods/createAction.ts:120`, before `processAction`
//! at :138), and `signAction`'s `process_action_internal` before its writer
//! (`signAction.ts:90`). The sources are the action's input BEEF, else the
//! store's bytes; an input with neither is skipped, as
//! `verifyUnlockScripts.ts:316-321` skips it. The word is the one verifier's
//! (`beef_verification::verify_own`), at the tip read before any writer.

use super::beef_storage::get_tx_with_proof;
use super::beef_verification::{header_fault, mining_tip, verify_own, SourceTx};
use crate::error::{Error, Result};
use crate::services::WalletServices;
use bsv_rs::transaction::beef_stream::ChainTip;
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath, Transaction};
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::sync::Arc;

/// The tip the own transaction is judged at: the tracker's (the caller's,
/// else the storage's own), else the services' header service, as the
/// pre-check's word reads it. A tracker fault or a missing tip holds with
/// `ServiceError`; with no header source configured there is no tip and the
/// word is not read (`None`).
pub(in crate::storage::sqlx) async fn own_word_tip(
    tracker: Option<&dyn ChainTracker>,
    services: Option<Arc<dyn WalletServices>>,
) -> Result<Option<ChainTip>> {
    match (tracker, services.as_deref()) {
        (Some(tracker), _) => mining_tip(tracker).await.map(Some),
        (None, Some(services)) => {
            let tracker = services.get_chain_tracker().await.map_err(header_fault)?;
            mining_tip(tracker).await.map(Some)
        }
        (None, None) => Ok(None),
    }
}

/// The source in `beef` when it carries the transaction's bytes, its proof
/// height from its BUMP.
fn beef_source(beef: &Beef, txid: &str) -> Option<SourceTx> {
    let entry = beef.find_txid(txid)?;
    let raw = match entry.raw_tx() {
        Some(raw) => raw.to_vec(),
        None => entry.tx()?.to_binary(),
    };
    let proven_height = entry
        .bump_index()
        .and_then(|index| beef.bumps.get(index))
        .map(|bump| u64::from(bump.block_height));
    Some(SourceTx { raw, proven_height })
}

/// Judge `raw_tx`, the wallet's own newly signed transaction, under the
/// node's word at `tip` on `network`. Bytes that do not parse are left to
/// processAction's own checks.
pub(in crate::storage::sqlx) async fn judge_own_tx(
    conn: &mut SqliteConnection,
    input_beef: Option<&[u8]>,
    raw_tx: &[u8],
    tip: ChainTip,
    network: &str,
) -> Result<()> {
    // Parsed and dropped before any await: bsv-rs's parsed types are not `Send`.
    let (mut sources, stored) = {
        let Ok(tx) = Transaction::from_binary(raw_tx) else {
            return Ok(());
        };
        let beef = input_beef.and_then(|bytes| Beef::from_binary(bytes).ok());
        let mut sources = HashMap::new();
        let mut stored = Vec::new();
        for input in &tx.inputs {
            let txid = input.get_source_txid()?;
            if sources.contains_key(&txid) || stored.contains(&txid) {
                continue;
            }
            match beef.as_ref().and_then(|beef| beef_source(beef, &txid)) {
                Some(source) => {
                    sources.insert(txid, source);
                }
                None => stored.push(txid),
            }
        }
        (sources, stored)
    };
    for txid in stored {
        match get_tx_with_proof(&mut *conn, &txid).await {
            Ok(Some(found)) => {
                let proven_height = match found.merkle_path {
                    Some(path) => Some(u64::from(MerklePath::from_binary(&path)?.block_height)),
                    None => None,
                };
                sources.insert(
                    txid,
                    SourceTx {
                        raw: found.raw_tx,
                        proven_height,
                    },
                );
            }
            // Neither bytes nor a live word: the input is skipped.
            Ok(None) | Err(Error::FailedAncestor { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    verify_own(raw_tx, &sources, tip, network)
}
