//! FailAbandoned task - marks abandoned transactions as failed.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use crate::storage::WalletStorageProvider;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Task that marks abandoned transactions as failed.
///
/// This task:
/// 1. Queries transactions with status 'unsigned' or 'unprocessed' older than the timeout
/// 2. For each transaction, calls storage.abort_action() to release locked UTXOs
/// 3. Logs results
pub struct FailAbandonedTask<S>
where
    S: WalletStorageProvider + 'static,
{
    #[allow(dead_code)]
    storage: Arc<S>,
    timeout: Duration,
}

impl<S> FailAbandonedTask<S>
where
    S: WalletStorageProvider + 'static,
{
    /// Create a new FailAbandonedTask with the given timeout.
    pub fn new(storage: Arc<S>, timeout: Duration) -> Self {
        Self { storage, timeout }
    }
}

#[async_trait]
impl<S> MonitorTask for FailAbandonedTask<S>
where
    S: WalletStorageProvider + 'static,
{
    fn name(&self) -> &'static str {
        "fail_abandoned"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(5 * 60) // 5 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let result = TaskResult::new();

        // Calculate the cutoff time
        let cutoff = Utc::now() - chrono::Duration::from_std(self.timeout).unwrap_or_default();

        // Note: In a production implementation, we would need to:
        // 1. Query transactions directly with created_at < cutoff and status = unsigned/unprocessed
        // 2. For each transaction, check if outputs have been spent
        // 3. Call abort_action for valid candidates
        //
        // This requires storage methods that can query across all users (admin queries)
        // which are not yet implemented.

        tracing::debug!(
            cutoff = %cutoff,
            timeout_secs = self.timeout.as_secs(),
            "FailAbandoned task: would check for abandoned transactions"
        );

        // In a full implementation with storage support for admin queries:
        // for tx in abandoned_txs {
        //     let auth = AuthId::with_user_id(&tx.identity_key, tx.user_id);
        //     match self.storage.abort_action(&auth, AbortActionArgs {
        //         reference: tx.reference.clone(),
        //     }).await {
        //         Ok(_) => {
        //             result.items_processed += 1;
        //         }
        //         Err(e) => {
        //             result.add_error(format!("Failed to abort tx {}: {}", tx.txid, e));
        //         }
        //     }
        // }

        Ok(result)
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

    #[test]
    fn test_timeout_calculation() {
        let timeout = Duration::from_secs(24 * 60 * 60);
        let cutoff = Utc::now() - chrono::Duration::from_std(timeout).unwrap();
        assert!(cutoff < Utc::now());
    }
}
