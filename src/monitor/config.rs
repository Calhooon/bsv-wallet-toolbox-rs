//! Monitor configuration types.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Status update for a transaction event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStatusUpdate {
    /// Transaction ID.
    pub txid: String,
    /// New status of the transaction.
    pub status: String,
    /// Merkle root if available.
    pub merkle_root: Option<String>,
    /// Encoded merkle path if available.
    pub merkle_path: Option<String>,
    /// Block height if mined.
    pub block_height: Option<u32>,
    /// Block hash if mined.
    pub block_hash: Option<String>,
}

/// Configuration options for the Monitor daemon.
#[derive(Clone)]
pub struct MonitorOptions {
    /// Configuration for each task.
    pub tasks: TasksConfig,
    /// Minimum age for transactions to be considered abandoned.
    pub fail_abandoned_timeout: Duration,
    /// Callback invoked when a transaction has been broadcast.
    pub on_tx_broadcasted: Option<Arc<dyn Fn(TransactionStatusUpdate) + Send + Sync>>,
    /// Callback invoked when a transaction proof has been obtained.
    pub on_tx_proven: Option<Arc<dyn Fn(TransactionStatusUpdate) + Send + Sync>>,
}

impl std::fmt::Debug for MonitorOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitorOptions")
            .field("tasks", &self.tasks)
            .field("fail_abandoned_timeout", &self.fail_abandoned_timeout)
            .field("on_tx_broadcasted", &self.on_tx_broadcasted.as_ref().map(|_| "Some(<callback>)"))
            .field("on_tx_proven", &self.on_tx_proven.as_ref().map(|_| "Some(<callback>)"))
            .finish()
    }
}

impl Default for MonitorOptions {
    fn default() -> Self {
        Self {
            tasks: TasksConfig::default(),
            fail_abandoned_timeout: Duration::from_secs(5 * 60), // 5 minutes (matches TS/Go)
            on_tx_broadcasted: None,
            on_tx_proven: None,
        }
    }
}

/// Configuration for all monitor tasks.
#[derive(Debug, Clone)]
pub struct TasksConfig {
    /// Check for proofs task configuration.
    pub check_for_proofs: TaskConfig,
    /// Send waiting transactions task configuration.
    pub send_waiting: TaskConfig,
    /// Fail abandoned transactions task configuration.
    pub fail_abandoned: TaskConfig,
    /// UnFail transactions task configuration.
    pub unfail: TaskConfig,
    /// Clock tick task configuration.
    pub clock: TaskConfig,
    /// New header polling task configuration.
    pub new_header: TaskConfig,
    /// Blockchain reorganization task configuration.
    pub reorg: TaskConfig,
    /// Check no-send transactions task configuration.
    pub check_no_sends: TaskConfig,
    /// Review transaction status task configuration.
    pub review_status: TaskConfig,
    /// Purge old data task configuration.
    pub purge: TaskConfig,
    /// Monitor service call history task configuration.
    pub monitor_call_history: TaskConfig,
}

impl Default for TasksConfig {
    fn default() -> Self {
        Self {
            check_for_proofs: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(60), // 1 minute
                start_immediately: false,
            },
            send_waiting: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(5 * 60), // 5 minutes
                start_immediately: true,
            },
            fail_abandoned: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(5 * 60), // 5 minutes
                start_immediately: false,
            },
            unfail: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(10 * 60), // 10 minutes
                start_immediately: false,
            },
            clock: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(1), // 1 second
                start_immediately: true,
            },
            new_header: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(60), // 1 minute
                start_immediately: false,
            },
            reorg: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(60), // 1 minute
                start_immediately: false,
            },
            check_no_sends: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(86400), // 24 hours
                start_immediately: false,
            },
            review_status: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(900), // 15 minutes
                start_immediately: false,
            },
            purge: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(3600), // 1 hour
                start_immediately: false,
            },
            monitor_call_history: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(720), // 12 minutes
                start_immediately: false,
            },
        }
    }
}

/// Configuration for a single monitor task.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Whether this task is enabled.
    pub enabled: bool,
    /// How often to run this task.
    pub interval: Duration,
    /// Whether to run immediately on start.
    pub start_immediately: bool,
}

impl TaskConfig {
    /// Create a new enabled task config with the given interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            enabled: true,
            interval,
            start_immediately: false,
        }
    }

    /// Create a disabled task config.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(60),
            start_immediately: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = MonitorOptions::default();
        assert!(opts.tasks.check_for_proofs.enabled);
        assert!(opts.tasks.send_waiting.enabled);
        assert!(opts.tasks.fail_abandoned.enabled);
        assert!(opts.tasks.unfail.enabled);
        assert!(opts.tasks.clock.enabled);
        assert!(opts.tasks.new_header.enabled);
        assert!(opts.tasks.reorg.enabled);
        assert!(opts.tasks.check_no_sends.enabled);
        assert!(opts.tasks.review_status.enabled);
        assert!(opts.tasks.purge.enabled);
        assert!(opts.tasks.monitor_call_history.enabled);
        assert_eq!(opts.fail_abandoned_timeout, Duration::from_secs(5 * 60));
        assert!(opts.on_tx_broadcasted.is_none());
        assert!(opts.on_tx_proven.is_none());
    }

    #[test]
    fn test_default_task_intervals() {
        let opts = MonitorOptions::default();
        assert_eq!(opts.tasks.clock.interval, Duration::from_secs(1));
        assert!(opts.tasks.clock.start_immediately);
        assert_eq!(opts.tasks.new_header.interval, Duration::from_secs(60));
        assert!(!opts.tasks.new_header.start_immediately);
        assert_eq!(opts.tasks.reorg.interval, Duration::from_secs(60));
        assert_eq!(opts.tasks.check_no_sends.interval, Duration::from_secs(86400));
        assert_eq!(opts.tasks.review_status.interval, Duration::from_secs(900));
        assert_eq!(opts.tasks.purge.interval, Duration::from_secs(3600));
        assert_eq!(opts.tasks.monitor_call_history.interval, Duration::from_secs(720));
    }

    #[test]
    fn test_task_config_new() {
        let config = TaskConfig::new(Duration::from_secs(30));
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(30));
        assert!(!config.start_immediately);
    }

    #[test]
    fn test_task_config_disabled() {
        let config = TaskConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_transaction_status_update() {
        let update = TransactionStatusUpdate {
            txid: "abc123".to_string(),
            status: "completed".to_string(),
            merkle_root: Some("root".to_string()),
            merkle_path: Some("path".to_string()),
            block_height: Some(800000),
            block_hash: Some("hash".to_string()),
        };
        assert_eq!(update.txid, "abc123");
        assert_eq!(update.status, "completed");
        assert_eq!(update.block_height, Some(800000));
    }

    #[test]
    fn test_monitor_options_with_callbacks() {
        let opts = MonitorOptions {
            on_tx_broadcasted: Some(Arc::new(|_update| {
                // callback
            })),
            on_tx_proven: Some(Arc::new(|_update| {
                // callback
            })),
            ..MonitorOptions::default()
        };
        assert!(opts.on_tx_broadcasted.is_some());
        assert!(opts.on_tx_proven.is_some());
    }
}
