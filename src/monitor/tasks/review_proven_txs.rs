//! ReviewProvenTxs task: the lagged audit of recent stored proofs against
//! the canonical headers, the reference's `TaskReviewProvenTxs`.
//!
//! The header task names a reorg when it SEES the change; this task is the
//! net for what it did not see (a service that was down for a cycle, a
//! proof stored by another process, a one-shot process whose reorg queue
//! did not outlive it, a row written by `internalize_action` with no block
//! hash). Every run it reads the stored anchors within the last
//! `REVIEW_HEIGHTS` heights below the persisted proof gate, fetches the
//! canonical header per distinct height, fills an EMPTY stored block hash
//! whose root is canonical, and re-proves every anchor whose merkle root
//! disagrees (`reprove_anchor`: replace on a validated provider proof,
//! retain on faults, demote only on positive evidence). A height whose
//! header cannot be read is skipped (an unknown never reads as stale).
//!
//! While the gate is CLOSED (0: no header processed yet) the audit runs
//! nothing above height 0, i.e. it skips and says so: a height that has not
//! aged is never judged.
//!
//! Stated divergence (`docs/REORG-DIVERGENCES.md`): a fresh 12-height window
//! every 10 minutes, where the reference sweeps at least 100 blocks deep
//! from a persisted checkpoint.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{
    block_hash_of_header, merkle_root_of_header, reprove_anchor, stale_anchors_by_root,
    ReproveTally,
};
use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

/// How many heights below the proof gate the audit re-checks.
pub const REVIEW_HEIGHTS: u32 = 12;

/// What one review pass found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReviewReport {
    /// The highest height audited (the proof gate); 0 = skipped.
    pub max_height: u32,
    /// Stored anchors in the window.
    pub anchors: u32,
    /// Anchors whose stored root disagreed with the canonical root.
    pub stale: u32,
    /// Empty stored block hashes filled from the canonical header.
    pub hashes_backfilled: u32,
    /// The re-prove tally over the stale anchors.
    pub tally: ReproveTally,
}

/// Task that audits recent stored proofs against the canonical headers.
pub struct ReviewProvenTxsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
}

impl<S, V> ReviewProvenTxsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self { storage, services }
    }

    /// One audit pass over `[max_height - REVIEW_HEIGHTS + 1, max_height]`.
    pub async fn review(&self, max_height: u32) -> Result<ReviewReport> {
        let mut report = ReviewReport {
            max_height,
            ..Default::default()
        };
        if max_height == 0 {
            return Ok(report);
        }
        let min_height = max_height.saturating_sub(REVIEW_HEIGHTS.saturating_sub(1));
        let anchors = self
            .storage
            .find_proven_txs_in_heights(min_height, max_height)
            .await?;
        report.anchors = anchors.len() as u32;
        if anchors.is_empty() {
            return Ok(report);
        }
        let mut canonical_roots: HashMap<u32, String> = HashMap::new();
        let mut canonical_hashes: HashMap<u32, String> = HashMap::new();
        let mut heights: Vec<u32> = anchors.iter().map(|a| a.height).collect();
        heights.sort_unstable();
        heights.dedup();
        for height in heights {
            match self.services.get_header_for_height(height).await {
                Ok(bytes) => {
                    if let Some(root) = merkle_root_of_header(&bytes) {
                        canonical_roots.insert(height, root);
                    }
                    if let Some(hash) = block_hash_of_header(&bytes) {
                        canonical_hashes.insert(height, hash);
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        task = "review_proven_txs",
                        height,
                        error = %e,
                        "review: header unavailable; this height is skipped this pass"
                    );
                }
            }
        }
        // A row written without a header in hand: fill its block hash once
        // its root is the canonical one at its height.
        for anchor in anchors.iter().filter(|a| a.block_hash.is_empty()) {
            let canonical = match (
                canonical_roots.get(&anchor.height),
                canonical_hashes.get(&anchor.height),
            ) {
                (Some(root), Some(hash)) if root.eq_ignore_ascii_case(&anchor.merkle_root) => hash,
                _ => continue,
            };
            match self
                .storage
                .set_proven_tx_block_hash_if_empty(&anchor.txid, canonical)
                .await
            {
                Ok(true) => {
                    report.hashes_backfilled += 1;
                    tracing::info!(
                        task = "review_proven_txs",
                        txid = %anchor.txid,
                        height = anchor.height,
                        block_hash = %canonical,
                        marker = "proof_block_hash_backfilled",
                        "review: filled the stored proof's empty block hash from the canonical header"
                    );
                }
                Ok(false) => {}
                Err(e) => report
                    .tally
                    .errors
                    .push(format!("{}: backfill: {e}", anchor.txid)),
            }
        }
        let stale: Vec<_> = stale_anchors_by_root(&anchors, &canonical_roots)
            .into_iter()
            .cloned()
            .collect();
        report.stale = stale.len() as u32;
        for anchor in &stale {
            tracing::warn!(
                task = "review_proven_txs",
                txid = %anchor.txid,
                height = anchor.height,
                stored_root = %anchor.merkle_root,
                canonical_root = %canonical_roots.get(&anchor.height).cloned().unwrap_or_default(),
                marker = "reorg_stale_proof_found",
                "review: a stored proof disagrees with the canonical header"
            );
            let outcome =
                reprove_anchor(self.storage.as_ref(), self.services.as_ref(), anchor).await;
            report.tally.record(&anchor.txid, &outcome);
        }
        Ok(report)
    }
}

#[async_trait]
impl<S, V> MonitorTask for ReviewProvenTxsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "review_proven_txs"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(10 * 60)
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut result = TaskResult::new();
        let max_height = match self.storage.max_acceptable_proof_height().await {
            Ok(h) => h,
            Err(e) => {
                result.add_error(format!("review_proven_txs: proof gate unreadable: {e}"));
                return Ok(result);
            }
        };
        if max_height == 0 {
            tracing::info!(
                task = "review_proven_txs",
                marker = "reorg_review_skipped",
                "review: the proof gate is closed (no header processed yet); nothing above height 0 is audited this pass"
            );
            return Ok(result);
        }
        match self.review(max_height).await {
            Ok(report) => {
                if report.stale > 0
                    || report.hashes_backfilled > 0
                    || !report.tally.errors.is_empty()
                {
                    tracing::info!(
                        task = "review_proven_txs",
                        max_height,
                        anchors = report.anchors,
                        stale = report.stale,
                        hashes_backfilled = report.hashes_backfilled,
                        replaced = report.tally.replaced,
                        demoted = report.tally.demoted,
                        deferred = report.tally.deferred,
                        unchanged = report.tally.unchanged,
                        errors = report.tally.errors.len(),
                        marker = "reorg_review_pass",
                        "review: pass complete"
                    );
                }
                result.items_processed = report.stale + report.hashes_backfilled;
                for e in report.tally.errors {
                    result.add_error(e);
                }
            }
            Err(e) => result.add_error(format!("review_proven_txs failed: {e}")),
        }
        Ok(result)
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::services::mock::MockWalletServices;
    use crate::{StorageSqlx, WalletStorageWriter};

    async fn storage() -> Arc<StorageSqlx> {
        let s = StorageSqlx::in_memory().await.unwrap();
        s.migrate("review-test", &"0".repeat(64)).await.unwrap();
        s.make_available().await.unwrap();
        Arc::new(s)
    }

    /// F14: while the proof gate is CLOSED (0) the audit runs nothing above
    /// height 0: it skips (`reorg_review_skipped` in the log) and touches no
    /// row, even one whose root disagrees with the canonical header. Once
    /// the gate is open the same row is found stale.
    #[tokio::test]
    async fn a_closed_gate_audits_nothing_and_an_open_gate_finds_the_stale_row() {
        let storage = storage().await;
        let txid = "a".repeat(64);
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) VALUES (?, 5, 0, '', ?, X'00', X'00')",
        )
        .bind(&txid)
        .bind("aa".repeat(32))
        .execute(storage.pool())
        .await
        .unwrap();
        // The header service's root at height 5 (the synthetic header's,
        // all zeros) disagrees with the stored root.
        let services = Arc::new(MockWalletServices::builder().height(5).build());
        let task = ReviewProvenTxsTask::new(storage.clone(), services.clone());

        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 0);
        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0, "nothing audited while closed");
        assert!(result.errors.is_empty());
        assert_eq!(
            services.call_count("get_header_for_height"),
            0,
            "no header read"
        );
        let report = task.review(0).await.unwrap();
        assert_eq!(
            report,
            ReviewReport::default(),
            "height 0 is an empty audit"
        );

        storage.set_max_acceptable_proof_height(5).await.unwrap();
        let report = task.review(5).await.unwrap();
        assert_eq!((report.max_height, report.anchors, report.stale), (5, 1, 1));
        assert!(services.call_count("get_header_for_height") >= 1);
    }
}
