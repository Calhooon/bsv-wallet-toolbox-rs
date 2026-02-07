//! FailAbandoned task - marks abandoned transactions as failed.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::storage::MonitorStorage;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Task that marks abandoned transactions as failed.
///
/// This task delegates to `MonitorStorage::abort_abandoned()` which:
/// 1. Queries transactions with status 'unsigned' or 'unprocessed' older than the timeout
/// 2. For each transaction, calls storage.abort_action() to release locked UTXOs
/// 3. Logs results
pub struct FailAbandonedTask<S>
where
    S: MonitorStorage + 'static,
{
    storage: Arc<S>,
    timeout: Duration,
}

impl<S> FailAbandonedTask<S>
where
    S: MonitorStorage + 'static,
{
    /// Create a new FailAbandonedTask with the given timeout.
    pub fn new(storage: Arc<S>, timeout: Duration) -> Self {
        Self { storage, timeout }
    }
}

#[async_trait]
impl<S> MonitorTask for FailAbandonedTask<S>
where
    S: MonitorStorage + 'static,
{
    fn name(&self) -> &'static str {
        "fail_abandoned"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(5 * 60) // 5 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut errors = Vec::new();
        let timeout = self.timeout;

        tracing::debug!(
            timeout_secs = timeout.as_secs(),
            "Checking for abandoned transactions"
        );

        // Delegate to MonitorStorage which handles querying across all users
        // and aborting stale transactions.
        match self.storage.abort_abandoned(timeout).await {
            Ok(()) => {
                tracing::debug!("Fail abandoned check completed");
            }
            Err(e) => {
                errors.push(format!("abort_abandoned failed: {}", e));
            }
        }

        Ok(TaskResult {
            items_processed: 0,
            errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_name() {
        assert_eq!("fail_abandoned", "fail_abandoned");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(5 * 60);
        assert_eq!(interval.as_secs(), 300);
    }
}
