//! Sync-when-idle task - triggers storage synchronization after idle periods.
//!
//! This task monitors wallet activity and triggers a storage sync when the wallet
//! has been idle for a configurable duration. This matches the TypeScript
//! `TaskSyncWhenIdle` from `@bsv/wallet-toolbox`.

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{MonitorTask, TaskResult};
use crate::Result;

/// Default idle threshold before triggering sync (2 minutes).
const DEFAULT_IDLE_THRESHOLD_SECS: u64 = 2 * 60;

/// Task that triggers storage synchronization when the wallet has been idle.
///
/// Tracks the last activity timestamp and, when the idle threshold is exceeded,
/// logs a sync attempt. The actual synchronization would be performed by the
/// Monitor daemon which has access to the storage sync layer.
///
/// This mirrors the TypeScript `TaskSyncWhenIdle` which currently acts as a
/// placeholder for future sync-on-idle behavior.
pub struct SyncWhenIdleTask {
    /// Unix timestamp (seconds) of last recorded activity.
    last_activity: AtomicU64,
    /// How long the wallet must be idle before triggering a sync.
    idle_threshold: Duration,
}

impl SyncWhenIdleTask {
    /// Create a new sync-when-idle task with default idle threshold (2 minutes).
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            last_activity: AtomicU64::new(now),
            idle_threshold: Duration::from_secs(DEFAULT_IDLE_THRESHOLD_SECS),
        }
    }

    /// Create a new sync-when-idle task with a custom idle threshold.
    pub fn with_threshold(idle_threshold: Duration) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            last_activity: AtomicU64::new(now),
            idle_threshold,
        }
    }

    /// Notify the task that wallet activity has occurred, resetting the idle timer.
    pub fn notify_activity(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_activity.store(now, Ordering::Relaxed);
    }

    /// Get the last activity timestamp (seconds since epoch).
    pub fn last_activity_timestamp(&self) -> u64 {
        self.last_activity.load(Ordering::Relaxed)
    }

    /// Check whether the wallet has been idle long enough to trigger a sync.
    fn is_idle(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = self.last_activity.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(last);
        elapsed >= self.idle_threshold.as_secs()
    }
}

impl Default for SyncWhenIdleTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MonitorTask for SyncWhenIdleTask {
    fn name(&self) -> &'static str {
        "sync_when_idle"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60)
    }

    async fn run(&self) -> Result<TaskResult> {
        if self.is_idle() {
            tracing::debug!(
                task = "sync_when_idle",
                idle_threshold_secs = self.idle_threshold.as_secs(),
                "Wallet idle, sync triggered"
            );
            // The actual sync operation would be performed by the Monitor daemon
            // which has access to the storage sync layer. This task only signals
            // that a sync should occur, matching the TypeScript placeholder behavior.
            Ok(TaskResult::with_count(1))
        } else {
            Ok(TaskResult::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_when_idle_name_and_interval() {
        let task = SyncWhenIdleTask::new();
        assert_eq!(task.name(), "sync_when_idle");
        assert_eq!(task.default_interval(), Duration::from_secs(60));
    }

    #[test]
    fn test_notify_activity_resets_timer() {
        let task = SyncWhenIdleTask::new();
        let before = task.last_activity_timestamp();

        // Notify activity
        task.notify_activity();
        let after = task.last_activity_timestamp();

        // After should be >= before (same second or later)
        assert!(after >= before);
    }

    #[tokio::test]
    async fn test_sync_when_idle_runs_when_idle() {
        // Create a task with a zero-second threshold so it's always "idle"
        let task = SyncWhenIdleTask::with_threshold(Duration::from_secs(0));

        // Set last_activity to the past so it appears idle
        task.last_activity.store(0, Ordering::Relaxed);

        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_sync_when_idle_skips_when_active() {
        // Create a task with a very long threshold
        let task = SyncWhenIdleTask::with_threshold(Duration::from_secs(999_999));

        // Notify activity right now
        task.notify_activity();

        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_sync_when_idle_default() {
        let task = SyncWhenIdleTask::default();
        assert_eq!(task.name(), "sync_when_idle");
        assert_eq!(
            task.idle_threshold,
            Duration::from_secs(DEFAULT_IDLE_THRESHOLD_SECS)
        );
    }
}
