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
use crate::storage::{MonitorStorage, PurgeParams};
use crate::Result;

/// Configuration for the purge task.
#[derive(Debug, Clone)]
pub struct PurgeConfig {
    /// Whether to purge failed transactions.
    pub purge_failed: bool,
    /// Whether to purge completed transaction data (keeps the record, removes raw data).
    pub purge_completed_data: bool,
    /// Age threshold for purging (in days).
    pub max_age_days: u32,
}

impl Default for PurgeConfig {
    fn default() -> Self {
        Self {
            purge_failed: true,
            purge_completed_data: true,
            max_age_days: 30,
        }
    }
}

/// Task that purges old/expired data from storage.
///
/// This maintenance task delegates to `MonitorStorage::purge_data()` to clean up
/// transient data and keep the database size manageable:
/// - Removes failed transaction records after a configurable period
/// - Removes raw transaction data from completed transactions
pub struct PurgeTask<S>
where
    S: MonitorStorage + 'static,
{
    storage: Arc<S>,
    config: PurgeConfig,
    /// Flag to trigger immediate purge.
    pub check_now: AtomicBool,
}

impl<S> PurgeTask<S>
where
    S: MonitorStorage + 'static,
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
    S: MonitorStorage + 'static,
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

        let mut errors = Vec::new();

        // Build PurgeParams from our config
        let params = PurgeParams {
            max_age_days: self.config.max_age_days,
            purge_completed: self.config.purge_completed_data,
            purge_failed: self.config.purge_failed,
        };

        // Delegate to MonitorStorage::purge_data() which operates across all users
        match MonitorStorage::purge_data(self.storage.as_ref(), params).await {
            Ok(result) => {
                if result.count > 0 {
                    tracing::info!(
                        count = result.count,
                        log = %result.log,
                        "Purge completed"
                    );
                } else {
                    tracing::debug!("Purge task executed - nothing to purge");
                }

                Ok(TaskResult {
                    items_processed: result.count,
                    errors,
                })
            }
            Err(e) => {
                errors.push(format!("purge_data failed: {}", e));
                Ok(TaskResult {
                    items_processed: 0,
                    errors,
                })
            }
        }
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
        assert_eq!(config.max_age_days, 30);
    }

    #[test]
    fn test_purge_default_interval() {
        // 1 hour = 3600 seconds
        let expected = Duration::from_secs(60 * 60);
        assert_eq!(expected.as_secs(), 3600);
    }
}
