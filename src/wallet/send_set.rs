//! The post of a signed action and the transactions its call names in
//! `sendWith`, one function for both doors (`createAction` with
//! `signAndProcess`, and `signAction`).
//!
//! The process step queues the action and each named transaction that is
//! the caller's own `noSend` action (`sending`, its request `unsent`, every
//! request of the set under one batch). This post carries the action and
//! those named transactions in ONE post, as the reference's storage
//! `processAction` shares `[...sendWith, txid]` in one `postBeef`
//! (ts-stack 57c5f130 `storage/methods/processAction.ts` 124, 148;
//! `attemptToPostReqsToNetwork.ts` 51), and records each txid's word.
//! A named txid the process step did not move in this call (one already
//! `sending`, one not the caller's, one unknown) is not carried: its entry
//! is the process step's answer.

use std::collections::{HashMap, HashSet};

use bsv_rs::transaction::Beef;

use crate::services::{PostBeefResult, WalletServices};
use crate::storage::broadcast::{classify_broadcast_results, classify_for_txid, BroadcastOutcome};
use crate::storage::{FindProvenTxReqsArgs, SendWithResult, WalletStorageProvider};

/// The named transactions the process step moved to `sending` in this call:
/// an entry `sending` whose request carries the action's own batch.
async fn moved_in_this_call<S: WalletStorageProvider>(
    storage: &S,
    txid: &str,
    results: Option<&[SendWithResult]>,
) -> Vec<(String, Vec<u8>, Vec<u8>)> {
    let named: Vec<String> = results
        .unwrap_or_default()
        .iter()
        .filter(|entry| entry.txid != txid && entry.status == "sending")
        .map(|entry| entry.txid.clone())
        .collect();
    if named.is_empty() {
        return Vec::new();
    }
    let mut asked = named.clone();
    asked.push(txid.to_string());
    let reqs = match storage
        .find_proven_tx_reqs(FindProvenTxReqsArgs {
            txids: Some(asked),
            ..Default::default()
        })
        .await
    {
        Ok(reqs) => reqs,
        Err(e) => {
            tracing::warn!(txid = %txid, error = %e, "The named transactions' requests were not read: the action is posted alone, they stay queued");
            return Vec::new();
        }
    };
    let Some(batch) = reqs
        .iter()
        .find(|req| req.txid == txid)
        .and_then(|req| req.batch.clone())
    else {
        return Vec::new();
    };
    named
        .into_iter()
        .filter_map(|named| {
            let req = reqs
                .iter()
                .find(|req| req.txid == named && req.batch.as_deref() == Some(batch.as_str()))?;
            let raw = req.raw_tx.clone().filter(|raw| !raw.is_empty())?;
            let input = req.input_beef.clone().filter(|beef| !beef.is_empty())?;
            Some((named, raw, input))
        })
        .collect()
}

/// Keeps the transactions of `beef` that `roots` need: the roots and their
/// ancestors held in it (BRC-62: a BEEF carries its subjects' dependency
/// graph). BUMPs are left intact.
pub(super) fn keep_ancestor_closure(beef: &mut Beef, roots: &[String]) {
    let by_id: HashMap<String, usize> = beef
        .txs
        .iter()
        .enumerate()
        .map(|(i, t)| (t.txid(), i))
        .collect();
    let mut keep: HashSet<String> = HashSet::new();
    let mut stack = roots.to_vec();
    while let Some(id) = stack.pop() {
        if !keep.insert(id.clone()) {
            continue;
        }
        if let Some(tx) = by_id.get(&id).and_then(|&idx| beef.txs[idx].tx()) {
            for input in tx.inputs() {
                if let Ok(source) = input.get_source_txid() {
                    if by_id.contains_key(&source) {
                        stack.push(source);
                    }
                }
            }
        }
    }
    beef.txs.retain(|t| keep.contains(&t.txid()));
}

/// The action's broadcast BEEF with each moved named transaction and its
/// stored ancestry merged in, and the txids it names (the named ones in
/// call order, then the action). A named transaction whose bytes do not
/// merge is left out and stays queued for the send-waiting task.
fn the_set(
    txid: &str,
    beef_bytes: &[u8],
    named: Vec<(String, Vec<u8>, Vec<u8>)>,
) -> (Vec<u8>, Vec<String>) {
    let alone = || (beef_bytes.to_vec(), vec![txid.to_string()]);
    if named.is_empty() {
        return alone();
    }
    let Ok(mut beef) = Beef::from_binary(beef_bytes) else {
        return alone();
    };
    let mut txids = Vec::new();
    for (named, raw, input) in named {
        let mut merged = beef.clone();
        let fits = Beef::from_binary(&input)
            .map_err(|e| e.to_string())
            .and_then(|input| merged.merge_beef(&input).map_err(|e| e.to_string()))
            .and_then(|()| {
                merged
                    .merge_raw_tx(raw, None)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            });
        match fits {
            Ok(()) => {
                beef = merged;
                txids.push(named);
            }
            Err(e) => {
                tracing::warn!(txid = %named, error = %e, "A named transaction's stored bytes did not merge: it stays queued");
            }
        }
    }
    if txids.is_empty() {
        return alone();
    }
    txids.push(txid.to_string());
    keep_ancestor_closure(&mut beef, &txids);
    beef.sort_txs();
    if beef.txs.iter().all(|tx| !tx.is_txid_only()) {
        beef.version = bsv_rs::transaction::BEEF_V1;
    }
    match beef.to_binary() {
        Ok(bytes) => (bytes, txids),
        Err(e) => {
            tracing::warn!(txid = %txid, error = %e, "The set's BEEF was not written: the action is posted alone");
            alone()
        }
    }
}

/// Posts the signed action `txid` (its broadcast BEEF `beef_bytes`) with
/// every named transaction the process step moved to `sending` in this
/// call, in one post, then records each txid's word
/// (`update_transaction_status_after_post`, a store fault logged, never
/// the call's error) and marks each accepted entry `unproven`. A
/// broadcaster's word is never an error to the caller (bsv-stack-lean
/// #66): every outcome but a success leaves the transaction `sending` and
/// the monitor re-asks on the cadence.
pub(super) async fn post_action_with_named<S, V>(
    storage: &S,
    services: &V,
    txid: &str,
    beef_bytes: &[u8],
    send_with_results: &mut Option<Vec<SendWithResult>>,
) where
    S: WalletStorageProvider,
    V: WalletServices,
{
    let named = moved_in_this_call(storage, txid, send_with_results.as_deref()).await;
    let (beef, txids) = the_set(txid, beef_bytes, named);
    tracing::debug!(txid = %txid, txids = ?txids, beef_len = beef.len(), "Posting the action and its named transactions");
    let (words, fault): (Vec<PostBeefResult>, Option<String>) =
        match services.post_beef(&beef, &txids).await {
            Ok(words) => (words, None),
            Err(e) => (Vec::new(), Some(e.to_string())),
        };
    for posted in &txids {
        let outcome = outcome_of(posted, txids.len(), &words, fault.as_deref());
        super::wallet::record_post(
            storage,
            posted,
            &outcome,
            (&words, fault.as_deref()),
            send_with_results,
        )
        .await;
    }
}

/// One posted txid's outcome: the fault when no broadcaster answered, the
/// whole word for a post naming one txid, else the word selected for it.
fn outcome_of(
    posted: &str,
    named: usize,
    words: &[PostBeefResult],
    fault: Option<&str>,
) -> BroadcastOutcome {
    let outcome = match fault {
        Some(fault) => BroadcastOutcome::ServiceError {
            details: vec![fault.to_string()],
        },
        None if named == 1 => classify_broadcast_results(words),
        None => classify_for_txid(words, posted),
    };
    if matches!(outcome, BroadcastOutcome::Success) {
        tracing::info!(txid = %posted, "Transaction broadcast successfully");
    } else {
        tracing::warn!(txid = %posted, outcome = ?outcome, "Broadcast not taken: a hint, re-asked via SendWaitingTask");
    }
    outcome
}
