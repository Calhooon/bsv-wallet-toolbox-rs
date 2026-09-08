//! ReviewProvenTxs task (M19 R1): the lagged audit of recent stored proofs
//! against the canonical headers, the reference's `TaskReviewProvenTxs`.
//!
//! The header tracker names a reorg when it SEES the hash change; this task
//! is the backup for what it did not see (a service that was down for a
//! cycle, a proof stored by another process, a row written by
//! `internalize_action` with no block hash). Every run it reads the stored
//! anchors within the last `REVIEW_HEIGHTS` heights, fetches the canonical
//! header per distinct height, and re-proves every anchor whose merkle root
//! disagrees (`reprove_anchor`: replace or demote). A height whose header
//! cannot be read is skipped (an unknown never reads as stale).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{merkle_root_of_header, reprove_anchor, stale_anchors_by_root, ReproveTally};
use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

/// How many heights below the processed height the audit re-checks.
pub const REVIEW_HEIGHTS: u32 = 12;

/// Task that audits recent stored proofs against the canonical headers.
pub struct ReviewProvenTxsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    /// The header task's processed height (0 = none yet); the audit's upper
    /// bound, so a header that has not aged is never judged.
    processed_height: Arc<AtomicU32>,
}

impl<S, V> ReviewProvenTxsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    pub fn new(storage: Arc<S>, services: Arc<V>, processed_height: Arc<AtomicU32>) -> Self {
        Self {
            storage,
            services,
            processed_height,
        }
    }

    /// One audit pass over `[max_height - REVIEW_HEIGHTS + 1, max_height]`.
    pub async fn review(&self, max_height: u32) -> Result<(u32, ReproveTally)> {
        let min_height = max_height.saturating_sub(REVIEW_HEIGHTS.saturating_sub(1));
        let anchors = self
            .storage
            .find_proven_txs_in_heights(min_height, max_height)
            .await?;
        let mut tally = ReproveTally::default();
        if anchors.is_empty() {
            return Ok((0, tally));
        }
        let mut canonical: HashMap<u32, String> = HashMap::new();
        let mut heights: Vec<u32> = anchors.iter().map(|a| a.height).collect();
        heights.sort_unstable();
        heights.dedup();
        for height in heights {
            match self.services.get_header_for_height(height).await {
                Ok(bytes) => {
                    if let Some(root) = merkle_root_of_header(&bytes) {
                        canonical.insert(height, root);
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
        let stale: Vec<_> = stale_anchors_by_root(&anchors, &canonical)
            .into_iter()
            .cloned()
            .collect();
        let stale_count = stale.len() as u32;
        for anchor in &stale {
            tracing::warn!(
                task = "review_proven_txs",
                txid = %anchor.txid,
                height = anchor.height,
                stored_root = %anchor.merkle_root,
                canonical_root = %canonical.get(&anchor.height).cloned().unwrap_or_default(),
                marker = "reorg_stale_proof_found",
                "review: a stored proof disagrees with the canonical header"
            );
            let outcome = reprove_anchor(self.storage.as_ref(), self.services.as_ref(), anchor).await;
            tally.record(&anchor.txid, &outcome);
        }
        Ok((stale_count, tally))
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
        let mut max_height = self.processed_height.load(Ordering::SeqCst);
        if max_height == 0 {
            // No processed header yet (a fresh daemon): audit below the tip.
            match self.services.get_height().await {
                Ok(h) => max_height = h.saturating_sub(1),
                Err(e) => {
                    result.add_error(format!("review_proven_txs: no tip: {e}"));
                    return Ok(result);
                }
            }
        }
        match self.review(max_height).await {
            Ok((stale, tally)) => {
                if stale > 0 || !tally.errors.is_empty() {
                    tracing::info!(
                        task = "review_proven_txs",
                        max_height,
                        stale,
                        replaced = tally.replaced,
                        demoted = tally.demoted,
                        deferred = tally.deferred,
                        unchanged = tally.unchanged,
                        errors = tally.errors.len(),
                        marker = "reorg_review_pass",
                        "review: pass complete"
                    );
                }
                result.items_processed = stale;
                for e in tally.errors {
                    result.add_error(e);
                }
            }
            Err(e) => result.add_error(format!("review_proven_txs failed: {e}")),
        }
        Ok(result)
    }
}
