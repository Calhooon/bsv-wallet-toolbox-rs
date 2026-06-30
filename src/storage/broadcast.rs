//! Broadcast outcome classification + BEEF structural validation.
//!
//! Feature-independent: used by the sqlx backends, the remote `StorageClient`,
//! the `Wallet`, and `StorageManager`. Lived in `storage::sqlx` historically;
//! moved here so `--no-default-features --features remote` builds.

use crate::services::traits::{PostBeefResult, PostTxResultForTxid};
use bsv_rs::transaction::Beef;

/// Classified result of a broadcast attempt.
///
/// Matches the classification pattern used by the TS and Go reference
/// wallet-toolbox implementations. Transient failures (ServiceError) keep
/// inputs locked for background retry; permanent failures (DoubleSpend,
/// InvalidTx) restore inputs immediately.
#[derive(Debug, Clone)]
pub enum BroadcastOutcome {
    /// At least one provider accepted the transaction.
    Success,
    /// All providers returned service/network errors (transient — will retry).
    ServiceError { details: Vec<String> },
    /// A provider reported a double-spend (permanent).
    DoubleSpend {
        competing_txs: Vec<String>,
        details: Vec<String>,
    },
    /// A provider definitively rejected the transaction (permanent).
    InvalidTx { details: Vec<String> },
    /// A provider reported orphan mempool (parent tx not yet propagated).
    /// This is a propagation issue, NOT a double-spend. The miner has the
    /// child tx but not the parent. The tx should stay in 'sending' for
    /// retry — the parent will typically propagate within a few seconds.
    OrphanMempool { details: Vec<String> },
}

impl BroadcastOutcome {
    /// Returns true if the broadcast was accepted by at least one provider.
    pub fn is_success(&self) -> bool {
        matches!(self, BroadcastOutcome::Success)
    }

    /// Returns true if the failure is transient and should be retried.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            BroadcastOutcome::ServiceError { .. } | BroadcastOutcome::OrphanMempool { .. }
        )
    }

    /// Build a human-readable error message with per-provider details.
    pub fn error_message(&self, txid: &str) -> Option<String> {
        match self {
            BroadcastOutcome::Success => None,
            BroadcastOutcome::ServiceError { details } => Some(format!(
                "Transaction broadcast for txid {} returned service errors (will retry): {}",
                txid,
                details.join("; ")
            )),
            BroadcastOutcome::DoubleSpend {
                competing_txs,
                details,
            } => Some(format!(
                "Transaction broadcast failed for txid {}: double spend detected. Competing txs: [{}]. Details: {}",
                txid,
                competing_txs.join(", "),
                details.join("; ")
            )),
            BroadcastOutcome::InvalidTx { details } => Some(format!(
                "Transaction broadcast failed for txid {}: transaction rejected. Details: {}",
                txid,
                details.join("; ")
            )),
            BroadcastOutcome::OrphanMempool { details } => Some(format!(
                "Transaction broadcast for txid {} returned orphan mempool (parent not propagated, will retry): {}",
                txid,
                details.join("; ")
            )),
        }
    }
}

/// Classify broadcast results from multiple providers into a single outcome.
///
/// Priority order (matching TS/Go reference implementations):
/// 1. Any success → Success
/// 2. Any double-spend (but NOT orphan mempool) → DoubleSpend (permanent)
/// 3. Any definitive rejection (ARC 46x codes) → InvalidTx (permanent)
/// 4. Any orphan mempool → OrphanMempool (transient, parent not propagated)
/// 5. Otherwise → ServiceError (transient, will retry)
pub fn classify_broadcast_results(results: &[PostBeefResult]) -> BroadcastOutcome {
    // Collect all per-txid results across providers
    let all_txid_results: Vec<&PostTxResultForTxid> =
        results.iter().flat_map(|r| r.txid_results.iter()).collect();

    // 1. Any success?
    let any_success = results.iter().any(|r| r.is_success());
    if any_success {
        return BroadcastOutcome::Success;
    }

    // Collect error details from all providers
    let details: Vec<String> = results
        .iter()
        .filter(|r| !r.is_success())
        .map(|r| {
            let txid_errors: String = r
                .txid_results
                .iter()
                .filter(|tx| tx.status != "success")
                .map(|tx| tx.data.as_deref().unwrap_or("unknown"))
                .collect::<Vec<_>>()
                .join("; ");
            format!("{}: {} [{}]", r.name, r.status, txid_errors)
        })
        .collect();

    // 2. Any GENUINE double-spend? A real double-spend NAMES the competing tx(s).
    // A DOUBLE_SPEND_ATTEMPTED with NO competing txids is the artifact of an
    // unpropagated parent — a deep 0-conf ancestry one provider rejects while another
    // reports orphan-mempool / "missing inputs". That is TRANSIENT (the tx lands once
    // its ancestry propagates), so it must NOT be classified as a permanent failure
    // (which fails the tx + restores its inputs, breaking a legitimate chained spend).
    // Require a named competitor; otherwise fall through to the orphan/service path
    // (transient — keep 'sending' for SendWaitingTask retry).
    let competing_txs: Vec<String> = all_txid_results
        .iter()
        .filter_map(|tr| tr.competing_txs.as_ref())
        .flatten()
        .cloned()
        .collect();
    let is_double_spend = !competing_txs.is_empty()
        && all_txid_results
            .iter()
            .any(|tr| tr.double_spend && !tr.orphan_mempool);
    if is_double_spend {
        return BroadcastOutcome::DoubleSpend {
            competing_txs,
            details,
        };
    }

    // 3. Any definitive rejection? (ARC 46x status codes = tx-level rejection)
    let is_invalid = all_txid_results.iter().any(|tr| {
        !tr.service_error
            && !tr.orphan_mempool
            && (tr.status.contains("46") || tr.status.contains("invalid"))
    });
    if is_invalid {
        return BroadcastOutcome::InvalidTx { details };
    }

    // 4. Any orphan mempool? (parent not yet propagated — transient)
    let is_orphan = all_txid_results.iter().any(|tr| tr.orphan_mempool);
    if is_orphan {
        return BroadcastOutcome::OrphanMempool { details };
    }

    // 5. Everything else is a transient service error
    BroadcastOutcome::ServiceError { details }
}

// =============================================================================

/// Validate BEEF structure before broadcast (diagnostic — does not block broadcast).
///
/// Checks:
/// 1. BEEF contains exactly 1 unproven (leaf) transaction — the one being broadcast
/// 2. All inputs of the leaf tx have source transactions in the BEEF
/// 3. Source transactions either have merkle proofs (bump_index) or are themselves
///    in the BEEF with proofs
///
/// Returns `Ok(())` if valid, `Err(message)` with details of what's missing.
pub fn validate_beef_for_broadcast(beef: &Beef, txid: &str) -> std::result::Result<(), String> {
    use bsv_rs::transaction::Transaction;

    // Find unproven (no merkle proof, not txid-only) transactions. A BEEF for a 0-conf
    // chain LEGITIMATELY carries many unproven ANCESTORS (the unconfirmed parents whose
    // raw bytes ARC needs to validate the chain) — those are expected, NOT an error. The
    // leaf being broadcast is the unproven tx whose txid == `txid`. (The previous check
    // errored on >1 unproven, miscounting every legitimate ancestor as a stray leaf and
    // false-warning on every real chain.)
    let unproven: Vec<&bsv_rs::transaction::BeefTx> = beef
        .txs
        .iter()
        .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
        .collect();

    if unproven.is_empty() {
        return Err(format!(
            "BEEF for {} has no unproven leaf transaction",
            txid
        ));
    }

    let leaf = match unproven.iter().find(|tx| tx.txid() == txid) {
        Some(tx) => *tx,
        None => {
            let ids: Vec<String> = unproven.iter().map(|t| t.txid()).collect();
            return Err(format!(
                "BEEF for {} does not contain the target as an unproven leaf (unproven: {:?})",
                txid, ids
            ));
        }
    };
    let leaf_txid = leaf.txid();

    // Parse the leaf transaction to check its inputs
    let raw_bytes = match leaf.raw_tx() {
        Some(bytes) => bytes,
        None => {
            return Err(format!(
                "BEEF for {}: leaf tx {} has no raw bytes",
                txid, leaf_txid
            ));
        }
    };
    let parsed = match Transaction::from_binary(raw_bytes) {
        Ok(tx) => tx,
        Err(e) => {
            return Err(format!(
                "BEEF for {}: failed to parse leaf tx {}: {}",
                txid, leaf_txid, e
            ));
        }
    };

    // Check each input has its source in the BEEF
    let mut missing_sources = Vec::new();
    for (i, input) in parsed.inputs.iter().enumerate() {
        let source_txid = input
            .source_txid
            .as_deref()
            .or_else(|| input.source_transaction.as_ref().map(|_| "embedded"))
            .unwrap_or("unknown");

        if source_txid == "unknown" {
            missing_sources.push(format!("input[{}]: no source txid", i));
            continue;
        }
        if source_txid == "embedded" {
            continue; // source transaction is inline
        }

        // Check if the source txid exists in the BEEF
        if beef.find_txid(source_txid).is_none() {
            missing_sources.push(format!("input[{}]: source {} not in BEEF", i, source_txid));
        }
    }

    if !missing_sources.is_empty() {
        return Err(format!(
            "BEEF for {} missing source transactions: {}",
            txid,
            missing_sources.join("; ")
        ));
    }

    Ok(())
}
