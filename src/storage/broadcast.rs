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

/// `PostTxResultForTxid.status` a provider sets when the broadcaster gave a
/// terminal-fatal verdict for the transaction itself (Arcade `REJECTED` /
/// `DOUBLE_SPEND_ATTEMPTED`): definitive, never retried.
pub const STATUS_REJECTED: &str = "rejected";

/// Whether a per-txid result is a DEFINITIVE rejection of the transaction (as
/// opposed to a transient service fault, an orphan-mempool wait, or a
/// double-spend report that names its competitor).
///
/// A provider marks one by clearing `service_error` and putting either an HTTP
/// rejection code (`"465"`, `"461"`, ...), [`STATUS_REJECTED`] or `"invalid"`
/// in `status`. Bare `"error"` is deliberately NOT definitive: it is the
/// generic shape every provider used historically for transient faults, and a
/// definitive verdict must be explicit.
pub fn is_definitive_rejection(tr: &PostTxResultForTxid) -> bool {
    if tr.service_error || tr.orphan_mempool || tr.is_success() {
        return false;
    }
    let status = tr.status.trim();
    status.eq_ignore_ascii_case(STATUS_REJECTED)
        || status.eq_ignore_ascii_case("invalid")
        || status
            .parse::<u16>()
            .map(crate::services::providers::arc::status_codes::is_rejection)
            .unwrap_or(false)
}

/// Classify broadcast results from multiple providers into a single outcome.
///
/// Priority order (matching TS/Go reference implementations):
/// 1. Any success → Success
/// 2. Any double-spend that NAMES a competing tx (and is not orphan mempool)
///    → DoubleSpend (permanent)
/// 3. Any definitive rejection ([`is_definitive_rejection`]: ARC/Arcade 4xx
///    rejection codes such as 465 fee-too-low, Arcade `REJECTED` /
///    `DOUBLE_SPEND_ATTEMPTED`) → permanent. A rejection flagged `double_spend`
///    (Arcade `DOUBLE_SPEND_ATTEMPTED`, competitor unnamed) → DoubleSpend, so
///    its inputs are released only after per-input chain verification; every
///    other definitive rejection → InvalidTx.
/// 4. Any orphan mempool → OrphanMempool (transient, parent not propagated)
/// 5. Otherwise → ServiceError (transient, will retry)
///
/// A definitive rejection is NEVER a transient retry: `create_action` fails
/// the transaction, releases its inputs and returns the error to the caller,
/// instead of returning a phantom txid that `SendWaitingTask` re-submits.
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

    // 3. Any definitive rejection? (ARC/Arcade 4xx rejection codes, Arcade fatal
    // statuses.) Permanent: never a transient retry. An unnamed double-spend
    // verdict stays a DoubleSpend so its inputs are released only after
    // per-input chain verification, never blindly.
    let rejections: Vec<&PostTxResultForTxid> = all_txid_results
        .iter()
        .copied()
        .filter(|tr| is_definitive_rejection(tr))
        .collect();
    if !rejections.is_empty() {
        if rejections.iter().any(|tr| tr.double_spend) {
            return BroadcastOutcome::DoubleSpend {
                competing_txs,
                details,
            };
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_result(status: &str, service_error: bool) -> PostTxResultForTxid {
        PostTxResultForTxid {
            txid: "ab".repeat(32),
            status: status.to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(format!("status {}", status)),
            service_error,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }
    }

    fn provider(name: &str, tr: PostTxResultForTxid) -> PostBeefResult {
        PostBeefResult {
            name: name.to_string(),
            status: if tr.is_success() { "success" } else { "error" }.to_string(),
            txid_results: vec![tr],
            error: None,
            notes: vec![],
        }
    }

    #[test]
    fn arc_fee_too_low_465_is_a_definitive_rejection() {
        // The phantom-txid bug: ARC 465 used to be `service_error: true` and
        // therefore a transient "will retry" success. It is a permanent failure.
        let results = vec![provider("taal", tx_result("465", false))];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::InvalidTx { .. }),
            "got {:?}",
            outcome
        );
        assert!(!outcome.is_transient());
        let msg = outcome.error_message(&"ab".repeat(32)).unwrap();
        assert!(msg.contains("rejected"), "{}", msg);
        assert!(msg.contains("465"), "{}", msg);
    }

    #[test]
    fn every_arc_rejection_code_is_definitive_and_the_rest_are_not() {
        use crate::services::providers::arc::status_codes::is_rejection;
        for code in [
            400u16, 422, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 471, 472, 473,
        ] {
            assert!(
                is_rejection(code),
                "{} must be a definitive rejection",
                code
            );
            let outcome =
                classify_broadcast_results(&[provider("arc", tx_result(&code.to_string(), false))]);
            assert!(
                matches!(outcome, BroadcastOutcome::InvalidTx { .. }),
                "{}: got {:?}",
                code,
                outcome
            );
        }
        for code in [
            401u16, 403, 404, 408, 409, 413, 429, 470, 500, 502, 503, 504,
        ] {
            assert!(!is_rejection(code), "{} must stay transient", code);
        }
    }

    #[test]
    fn a_service_error_tagged_result_is_never_a_rejection_whatever_its_status() {
        // Belt and braces: the provider's own `service_error` flag wins.
        let results = vec![provider("arc", tx_result("465", true))];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::ServiceError { .. }),
            "got {:?}",
            outcome
        );
    }

    #[test]
    fn bare_error_status_stays_transient() {
        // The historical generic shape (status "error", service_error false) is
        // not an explicit verdict and must not fail a tx on its own.
        let results = vec![provider("arc", tx_result("error", false))];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::ServiceError { .. }),
            "got {:?}",
            outcome
        );
    }

    #[test]
    fn arcade_rejected_status_is_a_definitive_rejection() {
        let results = vec![provider("arcade", tx_result(STATUS_REJECTED, false))];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::InvalidTx { .. }),
            "got {:?}",
            outcome
        );
        assert!(is_definitive_rejection(&tx_result("REJECTED", false)));
        assert!(is_definitive_rejection(&tx_result("invalid", false)));
    }

    #[test]
    fn arcade_double_spend_attempted_without_competitor_is_a_verified_double_spend() {
        // Definitive, but its inputs may be consumed by an unnamed competitor:
        // classify as DoubleSpend so release goes through per-input chain
        // verification, never the blind InvalidTx restore.
        let mut tr = tx_result(STATUS_REJECTED, false);
        tr.double_spend = true;
        let outcome = classify_broadcast_results(&[provider("arcade", tr)]);
        match outcome {
            BroadcastOutcome::DoubleSpend { competing_txs, .. } => {
                assert!(competing_txs.is_empty());
            }
            other => panic!("expected DoubleSpend, got {:?}", other),
        }
    }

    #[test]
    fn classic_arc_unnamed_double_spend_stays_transient() {
        // Unchanged behaviour: a classic-ARC DOUBLE_SPEND_ATTEMPTED that names no
        // competitor (status "error") is the unpropagated-parent artifact.
        let mut tr = tx_result("error", false);
        tr.double_spend = true;
        let outcome = classify_broadcast_results(&[provider("taal", tr)]);
        assert!(
            matches!(outcome, BroadcastOutcome::ServiceError { .. }),
            "got {:?}",
            outcome
        );
    }

    #[test]
    fn success_anywhere_beats_a_rejection_elsewhere() {
        let results = vec![
            provider("taal", tx_result("465", false)),
            provider("gorillapool", tx_result("success", false)),
        ];
        assert!(classify_broadcast_results(&results).is_success());
    }

    #[test]
    fn orphan_and_rejection_together_is_a_rejection() {
        // A definitive verdict from one provider is not undone by another
        // provider still waiting on a parent.
        let mut orphan = tx_result("error", false);
        orphan.orphan_mempool = true;
        let results = vec![
            provider("gorillapool", orphan),
            provider("taal", tx_result("461", false)),
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::InvalidTx { .. }),
            "got {:?}",
            outcome
        );
    }

    #[test]
    fn a_successful_result_is_not_a_rejection_even_with_a_numeric_status() {
        let mut tr = tx_result("success", false);
        tr.status = "success".to_string();
        assert!(!is_definitive_rejection(&tr));
        let mut orphan = tx_result("465", false);
        orphan.orphan_mempool = true;
        assert!(!is_definitive_rejection(&orphan));
    }
}
