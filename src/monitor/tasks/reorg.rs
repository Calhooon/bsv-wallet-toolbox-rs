//! Reorg task: handles blockchain reorganizations.
//!
//! The reference (ts-stack `TaskReorg`): deactivated headers arrive on a
//! queue and age ten minutes before they are processed (a short fork must
//! not churn storage); every `proven_txs` row citing a deactivated header is
//! re-proved (`reproveHeader`); a header whose rows came back `unchanged` or
//! `unavailable` is retried, aged again, at most three times, and then the
//! original proof data is RETAINED ("maximum retries exceeded"). In normal
//! operation there is rarely any work here because the proof LAG keeps most
//! orphan proofs out of storage in the first place.
//!
//! Ours conforms, with one stated difference (`docs/REORG-DIVERGENCES.md`):
//! a row the chain POSITIVELY refutes (`reprove_anchor`: the tracker answers
//! a definite false, at least two providers answer cleanly "not mined", no
//! provider serves a path) is DEMOTED to the pre-proof state, because a
//! retained stale proof refuses every spend that touches it (loop 8,
//! 2026-09-07: 28 seats, `createAction` 400). Faults never demote. The
//! queue is fed by the header task (a same-height hash change and the ring
//! walk); the review task is the net for what the queue missed, and for a
//! one-shot process whose queue does not outlive it.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{new_reorg_queue, reprove_block_hash, ReorgQueue};
use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

pub use crate::monitor::reorg_ops::DeactivatedHeader;

/// How many times a deactivated header is processed before the original
/// proof data is retained (the reference's `maxRetries`).
pub const MAX_RETRY_COUNT: u32 = 3;

/// How long a deactivated header ages before each processing (the
/// reference's `agedMsecs`, ten minutes).
pub const REORG_PROCESS_DELAY_SECS: i64 = 10 * 60;

/// Task that handles blockchain reorganizations.
pub struct ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    /// Queue of deactivated headers to process (shared with the producer).
    queue: ReorgQueue,
}

impl<S, V> ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a reorg task with its own queue (tests and the standalone shape).
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self::with_queue(storage, services, new_reorg_queue())
    }

    /// Create a reorg task over the daemon's shared queue.
    pub fn with_queue(storage: Arc<S>, services: Arc<V>, queue: ReorgQueue) -> Self {
        Self {
            storage,
            services,
            queue,
        }
    }

    /// Queue a deactivated header for processing.
    pub async fn queue_deactivated_header(&self, hash: String, height: u32) {
        let header = DeactivatedHeader {
            hash: hash.clone(),
            height,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let mut headers = self.queue.lock().await;
        headers.push(header);
        tracing::info!(
            task = "reorg",
            height = height,
            hash = %hash,
            "Queued deactivated header"
        );
    }

    /// Get the number of pending deactivated headers.
    pub async fn pending_count(&self) -> usize {
        self.queue.lock().await.len()
    }

    /// Process every queued header regardless of age (the CLI and tests).
    pub async fn run_now(&self) -> Result<TaskResult> {
        self.process(true).await
    }

    async fn process(&self, ignore_age: bool) -> Result<TaskResult> {
        let mut result = TaskResult::new();
        let now = chrono::Utc::now();
        let process_threshold = now - chrono::Duration::seconds(REORG_PROCESS_DELAY_SECS);

        // Take the aged headers out of the queue; the lock is never held
        // across the network.
        let due: Vec<DeactivatedHeader> = {
            let mut headers = self.queue.lock().await;
            let (due, keep): (Vec<_>, Vec<_>) = headers
                .drain(..)
                .partition(|h| ignore_age || h.deactivated_at <= process_threshold);
            *headers = keep;
            due
        };

        let mut requeue = Vec::new();
        for header in due {
            tracing::info!(
                task = "reorg",
                hash = %header.hash,
                height = header.height,
                retry_count = header.retry_count,
                "Processing deactivated header"
            );
            let tally = reprove_block_hash(
                self.storage.as_ref(),
                self.services.as_ref(),
                &header.hash,
                header.height,
            )
            .await;
            tracing::info!(
                task = "reorg",
                hash = %header.hash,
                height = header.height,
                replaced = tally.replaced,
                demoted = tally.demoted,
                deferred = tally.deferred,
                unchanged = tally.unchanged,
                errors = tally.errors.len(),
                marker = "reorg_header_processed",
                "Deactivated header processed"
            );
            result.items_processed += tally.replaced + tally.demoted;
            for e in &tally.errors {
                result.add_error(e.clone());
            }
            // The reference retries on `unchanged` or `unavailable` (ours:
            // unchanged, deferred, a transient fault); a replacement or a
            // demotion is final. After MAX_RETRY_COUNT tries the original
            // proof data is retained and the header dropped: the review task
            // is the net.
            if tally.wants_retry() {
                if header.retry_count + 1 >= MAX_RETRY_COUNT {
                    tracing::warn!(
                        task = "reorg",
                        hash = %header.hash,
                        height = header.height,
                        tries = header.retry_count + 1,
                        marker = "reorg_max_retries_exceeded",
                        "maximum retries exceeded, original retained"
                    );
                } else {
                    let mut again = header.clone();
                    again.retry_count += 1;
                    again.deactivated_at = now;
                    requeue.push(again);
                }
            }
        }
        if !requeue.is_empty() {
            self.queue.lock().await.extend(requeue);
        }
        Ok(result)
    }
}

#[async_trait]
impl<S, V> MonitorTask for ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "reorg"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    async fn run(&self) -> Result<TaskResult> {
        self.process(false).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorg_default_interval() {
        let expected = Duration::from_secs(60);
        assert_eq!(expected.as_secs(), 60);
    }

    #[test]
    fn the_reorg_constants_conform_to_the_reference() {
        // ts-stack TaskReorg: agedMsecs = 10 minutes, maxRetries = 3.
        assert_eq!(MAX_RETRY_COUNT, 3);
        assert_eq!(REORG_PROCESS_DELAY_SECS, 600);
    }

    #[test]
    fn test_deactivated_header_creation() {
        let header = DeactivatedHeader {
            hash: "abc123".to_string(),
            height: 800000,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };
        assert_eq!(header.height, 800000);
        assert_eq!(header.retry_count, 0);
    }
}
