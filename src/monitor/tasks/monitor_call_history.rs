//! Monitor call history task - logs service call history.
//!
//! This task retrieves and logs the service call history from external services.
//! Used for monitoring the activity and performance of services used by the wallet.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::services::WalletServices;
use crate::Result;

/// Task that monitors and logs service call history.
///
/// Periodically retrieves call history from all configured services
/// and logs statistics about successes, failures, and errors.
///
/// This task is generic over `V: WalletServices`, allowing it to work with
/// any services implementation (concrete `Services`, mock, etc.). The
/// `get_services_call_history` method is available on the `WalletServices`
/// trait with a default that returns empty history.
pub struct MonitorCallHistoryTask<V: WalletServices> {
    services: Arc<V>,
}

impl<V: WalletServices> MonitorCallHistoryTask<V> {
    /// Create a new monitor call history task.
    pub fn new(services: Arc<V>) -> Self {
        Self { services }
    }
}

#[async_trait]
impl<V: WalletServices + 'static> MonitorTask for MonitorCallHistoryTask<V> {
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

        tracing::debug!("Monitor call history task executed");

        tracing::info!(
            task = "monitor_call_history",
            total_calls = total_calls,
            total_errors = total_errors,
            "Service call history summary"
        );

        Ok(TaskResult {
            items_processed: total_calls as u32,
            errors: vec![],
        })
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

    #[tokio::test]
    async fn test_monitor_call_history_task_run_returns_empty_on_fresh_services() {
        use crate::services::Services;

        let services = Arc::new(Services::mainnet().unwrap());
        let task = MonitorCallHistoryTask::new(services);
        let result = task.run().await.unwrap();
        // Fresh services should have zero calls logged
        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_services_call_history_default_is_empty() {
        use crate::services::ServicesCallHistory;

        // The default ServicesCallHistory should have all fields as None
        let history = ServicesCallHistory::default();
        assert!(history.get_merkle_path.is_none());
        assert!(history.get_raw_tx.is_none());
        assert!(history.post_beef.is_none());
        assert!(history.get_utxo_status.is_none());
    }
}
