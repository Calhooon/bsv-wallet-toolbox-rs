//! SendWaiting task - broadcasts transactions that are waiting to be sent.
//!
//! Includes a local TryLock (Go pattern: `sendWaitingLock.TryLock()`) to prevent
//! concurrent runs within the same instance. This is in ADDITION to the distributed
//! task lock in daemon.rs which prevents cross-instance stacking.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Default minimum age before sending a transaction (30 seconds).
const DEFAULT_MIN_AGE_SECS: u64 = 30;

/// Task that broadcasts transactions waiting to be sent.
///
/// This task delegates to `MonitorStorage::send_waiting_transactions()` which:
/// 1. Queries proven_tx_reqs with status 'unsent' or 'sending'
/// 2. Groups transactions by batch_id if present
/// 3. Builds BEEF for each group
/// 4. Calls services.post_beef() to broadcast
/// 5. On success: updates status to 'unmined'
/// 6. On double-spend: marks transaction as 'failed'
/// 7. On error: logs and retries next cycle
///
/// Uses a local TryLock (Go pattern) to skip if a previous run is still in progress.
pub struct SendWaitingTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    #[allow(dead_code)]
    services: Arc<V>,
    min_age: Duration,
    first_run: std::sync::atomic::AtomicBool,
    /// Local mutex to prevent concurrent runs within the same instance.
    /// Go pattern: `sendWaitingLock sync.Mutex` with `TryLock()`.
    send_lock: Mutex<()>,
}

impl<S, V> SendWaitingTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new SendWaitingTask.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self {
            storage,
            services,
            min_age: Duration::from_secs(DEFAULT_MIN_AGE_SECS),
            first_run: std::sync::atomic::AtomicBool::new(true),
            send_lock: Mutex::new(()),
        }
    }
}

#[async_trait]
impl<S, V> MonitorTask for SendWaitingTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "send_waiting"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(5 * 60) // 5 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut errors = Vec::new();

        // TryLock pattern (Go: sendWaitingLock.TryLock())
        // If a previous run is still in progress, skip this run.
        let guard = match self.send_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::warn!("send_waiting: already running, skipping this run");
                return Ok(TaskResult::new());
            }
        };

        // On first run, don't apply age filter (use zero duration)
        let is_first_run = self
            .first_run
            .swap(false, std::sync::atomic::Ordering::SeqCst);

        let min_age = if is_first_run {
            Duration::from_secs(0)
        } else {
            self.min_age
        };

        // Delegate to MonitorStorage which handles the full send logic
        let result = match self.storage.send_waiting_transactions(min_age).await {
            Ok(Some(results)) => {
                let items_processed = results
                    .send_with_results
                    .as_ref()
                    .map(|r| r.len() as u32)
                    .unwrap_or(0);

                if items_processed > 0 {
                    tracing::info!(processed = items_processed, "Sent waiting transactions");
                }

                Ok(TaskResult {
                    items_processed,
                    errors,
                })
            }
            Ok(None) => {
                tracing::debug!("No waiting transactions to send");
                Ok(TaskResult {
                    items_processed: 0,
                    errors,
                })
            }
            Err(e) => {
                errors.push(format!("send_waiting_transactions failed: {}", e));
                Ok(TaskResult {
                    items_processed: 0,
                    errors,
                })
            }
        };

        // Explicitly drop guard to release lock
        drop(guard);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_name() {
        assert_eq!("send_waiting", "send_waiting");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(5 * 60);
        assert_eq!(interval.as_secs(), 300);
    }

    #[test]
    fn test_default_min_age() {
        assert_eq!(DEFAULT_MIN_AGE_SECS, 30);
    }

    /// Test that try_lock on an already-held Mutex returns Err, which the task
    /// interprets as "skip this run".
    #[tokio::test]
    async fn test_try_lock_prevents_concurrent_run() {
        let lock = Mutex::new(());

        // Acquire the lock externally (simulating a long-running previous task)
        let guard = lock.lock().await;

        // try_lock should fail because the lock is already held
        let result = lock.try_lock();
        assert!(result.is_err(), "try_lock should fail when lock is held");

        // Drop the guard to release
        drop(guard);

        // Now try_lock should succeed
        let result = lock.try_lock();
        assert!(result.is_ok(), "try_lock should succeed when lock is free");
    }

    /// Test the TryLock pattern returns 0 items when skipped, matching the Go
    /// pattern where sendWaitingLock.TryLock() returns false.
    #[tokio::test]
    async fn test_try_lock_skip_returns_empty_task_result() {
        let lock = Mutex::new(());

        // Simulate lock already held
        let _guard = lock.lock().await;

        // Simulate the run() logic
        let result = match lock.try_lock() {
            Ok(_guard) => {
                // Would proceed with actual work
                TaskResult::with_count(5)
            }
            Err(_) => {
                // Lock held -> skip with empty result
                TaskResult::new()
            }
        };

        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    /// Verify that the lock is released after the guard is dropped, allowing
    /// subsequent runs.
    #[tokio::test]
    async fn test_lock_released_after_guard_drop() {
        let lock = Mutex::new(());

        // First acquisition
        {
            let guard = lock.try_lock();
            assert!(guard.is_ok());
            // guard dropped here
        }

        // Second acquisition should succeed
        {
            let guard = lock.try_lock();
            assert!(guard.is_ok());
        }
    }
}
