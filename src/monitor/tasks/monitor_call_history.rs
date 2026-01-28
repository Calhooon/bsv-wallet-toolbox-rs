//! Monitor call history task - logs service call history.
//!
//! This task retrieves and logs the service call history from external services.
//! Used for monitoring the activity and performance of services used by the wallet.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::services::Services;
use crate::Result;

/// Task that monitors and logs service call history.
///
/// Periodically retrieves call history from all configured services
/// and logs statistics about successes, failures, and errors.
///
/// Note: This task requires `Services` specifically (not the generic `WalletServices` trait)
/// because `get_services_call_history` is only available on the concrete `Services` type.
pub struct MonitorCallHistoryTask {
    services: Arc<Services>,
}

impl MonitorCallHistoryTask {
    /// Create a new monitor call history task.
    pub fn new(services: Arc<Services>) -> Self {
        Self { services }
    }
}

#[async_trait]
impl MonitorTask for MonitorCallHistoryTask {
    fn name(&self) -> &'static str {
        "monitor_call_history"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(12 * 60) // 12 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        // Get service call history and reset counters
        let history = self.services.get_services_call_history(true);

        let mut total_calls = 0u64;
        let mut total_errors = 0u64;

        // Log history for each service type
        if let Some(merkle_path) = &history.get_merkle_path {
            for (name, provider) in &merkle_path.history_by_provider {
                let counts = &provider.total_counts;
                total_calls += counts.success + counts.failure + counts.error;
                total_errors += counts.error;
                tracing::info!(
                    task = "monitor_call_history",
                    service = "get_merkle_path",
                    provider = name,
                    success = counts.success,
                    failure = counts.failure,
                    error = counts.error,
                    "Service call history"
                );
            }
        }

        if let Some(raw_tx) = &history.get_raw_tx {
            for (name, provider) in &raw_tx.history_by_provider {
                let counts = &provider.total_counts;
                total_calls += counts.success + counts.failure + counts.error;
                total_errors += counts.error;
                tracing::info!(
                    task = "monitor_call_history",
                    service = "get_raw_tx",
                    provider = name,
                    success = counts.success,
                    failure = counts.failure,
                    error = counts.error,
                    "Service call history"
                );
            }
        }

        if let Some(post_beef) = &history.post_beef {
            for (name, provider) in &post_beef.history_by_provider {
                let counts = &provider.total_counts;
                total_calls += counts.success + counts.failure + counts.error;
                total_errors += counts.error;
                tracing::info!(
                    task = "monitor_call_history",
                    service = "post_beef",
                    provider = name,
                    success = counts.success,
                    failure = counts.failure,
                    error = counts.error,
                    "Service call history"
                );
            }
        }

        if let Some(utxo_status) = &history.get_utxo_status {
            for (name, provider) in &utxo_status.history_by_provider {
                let counts = &provider.total_counts;
                total_calls += counts.success + counts.failure + counts.error;
                total_errors += counts.error;
                tracing::info!(
                    task = "monitor_call_history",
                    service = "get_utxo_status",
                    provider = name,
                    success = counts.success,
                    failure = counts.failure,
                    error = counts.error,
                    "Service call history"
                );
            }
        }

        tracing::info!(
            task = "monitor_call_history",
            total_calls = total_calls,
            total_errors = total_errors,
            "Service call history summary"
        );

        Ok(TaskResult::with_count(total_calls as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_call_history_task_name() {
        use crate::services::Services;

        let services = Services::mainnet().unwrap();
        let task = MonitorCallHistoryTask::new(Arc::new(services));
        assert_eq!(task.name(), "monitor_call_history");
    }

    #[test]
    fn test_monitor_call_history_task_interval() {
        use crate::services::Services;

        let services = Services::mainnet().unwrap();
        let task = MonitorCallHistoryTask::new(Arc::new(services));
        assert_eq!(task.default_interval(), Duration::from_secs(12 * 60));
    }
}
