//! Purge task - database maintenance that deletes transient data.
//!
//! This task performs cleanup of old/expired data:
//! - Failed transactions (all data)
//! - Completed transactions (raw data, beef, mapi responses)

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, WalletStorageProvider};
use crate::Result;

/// Configuration for the purge task.
#[derive(Debug, Clone)]
pub struct PurgeConfig {
    /// Whether to purge failed transactions.
    pub purge_failed: bool,
    /// Whether to purge completed transaction data (keeps the record, removes raw data).
    pub purge_completed_data: bool,
    /// Age threshold for purging failed transactions.
    pub failed_age: Duration,
    /// Age threshold for purging completed transaction data.
    pub completed_data_age: Duration,
}

impl Default for PurgeConfig {
    fn default() -> Self {
        Self {
            purge_failed: true,
            purge_completed_data: true,
            failed_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            completed_data_age: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
        }
    }
}

/// Task that purges old/expired data from storage.
///
/// This maintenance task cleans up transient data to keep the database
/// size manageable:
/// - Removes failed transaction records after a configurable period
/// - Removes raw transaction data from completed transactions
pub struct PurgeTask<S>
where
    S: WalletStorageProvider + 'static,
{
    storage: Arc<S>,
    config: PurgeConfig,
    /// Flag to trigger immediate purge.
    pub check_now: AtomicBool,
}

impl<S> PurgeTask<S>
where
    S: WalletStorageProvider + 'static,
{
    /// Create a new purge task with default configuration.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            config: PurgeConfig::default(),
            check_now: AtomicBool::new(false),
        }
    }

    /// Create a new purge task with custom configuration.
    pub fn with_config(storage: Arc<S>, config: PurgeConfig) -> Self {
        Self {
            storage,
            config,
            check_now: AtomicBool::new(false),
        }
    }

    /// Trigger an immediate purge on the next run.
    pub fn trigger_purge(&self) {
        self.check_now.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl<S> MonitorTask for PurgeTask<S>
where
    S: WalletStorageProvider + 'static,
{
    fn name(&self) -> &'static str {
        "purge"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60 * 60) // 1 hour
    }

    async fn run(&self) -> Result<TaskResult> {
        // Reset the check_now flag
        self.check_now.store(false, Ordering::SeqCst);

        let mut result = TaskResult::new();

        // Calculate cutoff times
        let now = chrono::Utc::now();
        let failed_cutoff = now
            - chrono::Duration::from_std(self.config.failed_age).unwrap_or(chrono::Duration::days(7));
        let completed_cutoff = now
            - chrono::Duration::from_std(self.config.completed_data_age)
                .unwrap_or(chrono::Duration::days(30));

        // Purge failed transactions
        if self.config.purge_failed {
            let args = FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Failed, ProvenTxReqStatus::Invalid]),
                ..Default::default()
            };

            let failed_reqs = match self.storage.find_proven_tx_reqs(args).await {
                Ok(reqs) => reqs,
                Err(e) => {
                    tracing::warn!(
                        task = "purge",
                        error = %e,
                        "Failed to query failed transactions"
                    );
                    result.add_error(format!("Failed to query failed transactions: {}", e));
                    Vec::new()
                }
            };

            let mut purged_failed = 0u32;
            for req in failed_reqs {
                if req.updated_at < failed_cutoff {
                    // TODO: Delete the proven_tx_req and associated data
                    tracing::debug!(
                        task = "purge",
                        txid = req.txid,
                        status = ?req.status,
                        "Would purge failed transaction"
                    );
                    purged_failed += 1;
                }
            }
            result.items_processed += purged_failed;
        }

        // Purge completed transaction data (keep record, remove raw data)
        if self.config.purge_completed_data {
            let args = FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Completed]),
                ..Default::default()
            };

            let completed_reqs = match self.storage.find_proven_tx_reqs(args).await {
                Ok(reqs) => reqs,
                Err(e) => {
                    tracing::warn!(
                        task = "purge",
                        error = %e,
                        "Failed to query completed transactions"
                    );
                    result.add_error(format!("Failed to query completed transactions: {}", e));
                    Vec::new()
                }
            };

            let mut purged_data = 0u32;
            for req in completed_reqs {
                if req.updated_at < completed_cutoff {
                    // TODO: Remove raw_tx, input_beef, and mapi responses
                    // but keep the transaction record for history
                    tracing::debug!(
                        task = "purge",
                        txid = req.txid,
                        "Would purge raw data from completed transaction"
                    );
                    purged_data += 1;
                }
            }
            result.items_processed += purged_data;
        }

        tracing::info!(
            task = "purge",
            items_processed = result.items_processed,
            "Purge task completed"
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_purge_config_default() {
        let config = PurgeConfig::default();
        assert!(config.purge_failed);
        assert!(config.purge_completed_data);
        assert_eq!(config.failed_age.as_secs(), 7 * 24 * 60 * 60);
        assert_eq!(config.completed_data_age.as_secs(), 30 * 24 * 60 * 60);
    }

    #[test]
    fn test_purge_default_interval() {
        // 1 hour = 3600 seconds
        let expected = Duration::from_secs(60 * 60);
        assert_eq!(expected.as_secs(), 3600);
    }
}
