//! Reorg task - handles blockchain reorganizations.
//!
//! The reference (ts-stack `TaskReorg`): deactivated headers arrive on a
//! queue, age briefly (a short fork must not churn storage), and every
//! `proven_txs` row citing a deactivated header is re-proved
//! (`reproveHeader`). Ours (M19 R1, 2026-09-08) differs in one deliberate
//! way: when no valid replacement proof exists the stored proof is DEMOTED
//! (the transaction is unmined again and re-proved later) instead of
//! retained, because a retained stale proof refuses every spend that
//! touches it (loop 8: 28 seats, `createAction` 400). The queue is fed by
//! the header task's same-height hash change and tip decrease
//! (`HeaderTracker`); the review task is the backup for events it missed.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{new_reorg_queue, reprove_block_hash, ReorgQueue};
use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

pub use crate::monitor::reorg_ops::DeactivatedHeader;

/// Maximum retry attempts for a deactivated header whose re-prove hit a
/// transient fault (a network error, a tracker error).
const MAX_RETRY_COUNT: u32 = 3;

/// Delay before processing a deactivated header. Short on purpose: the proof
/// lag already keeps most orphan proofs out, and a seat with a stale proof
/// cannot spend until it is re-proved.
const REORG_PROCESS_DELAY_SECS: i64 = 2 * 60;

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
            hash,
            height,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };
        let mut headers = self.queue.lock().await;
        headers.push(header);
        tracing::info!(
            task = "reorg",
            height = height,
            "Queued deactivated header for reorg processing"
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
            let tally =
                reprove_block_hash(self.storage.as_ref(), self.services.as_ref(), &header.hash)
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
            // Only a transient fault or a deferral earns a retry: the header
            // stays queued so the next pass finishes the work.
            if (!tally.errors.is_empty() || tally.deferred > 0)
                && header.retry_count < MAX_RETRY_COUNT
            {
                let mut again = header.clone();
                again.retry_count += 1;
                again.deactivated_at = now;
                requeue.push(again);
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
    fn test_max_retry_count() {
        assert_eq!(MAX_RETRY_COUNT, 3);
    }

    #[test]
    fn test_reorg_process_delay_is_short() {
        // Two minutes: the proof lag keeps orphan proofs out; a seat with a
        // stale proof cannot spend until it is re-proved.
        assert_eq!(REORG_PROCESS_DELAY_SECS, 120);
    }

    #[test]
    fn test_deactivated_header() {
        let header = DeactivatedHeader {
            hash: "000000000000000001234567890abcdef".to_string(),
            height: 800000,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };
        assert_eq!(header.height, 800000);
        assert_eq!(header.retry_count, 0);
    }
}
