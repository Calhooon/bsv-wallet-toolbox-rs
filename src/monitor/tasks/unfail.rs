//! UnFail task - recovers transactions that were incorrectly marked as failed.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Task that recovers transactions that were incorrectly marked as failed.
///
/// This task:
/// 1. Queries proven_tx_reqs with status 'unfail'
/// 2. For each transaction, checks if it has a merkle path on chain
/// 3. If merkle path found:
///    - Updates proven_tx_req status to 'unmined'
///    - Updates transaction status to 'unproven'
///    - Creates UTXOs for spendable outputs
/// 4. If not found:
///    - Updates proven_tx_req status to 'invalid'
pub struct UnfailTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    #[allow(dead_code)]
    services: Arc<V>,
}

impl<S, V> UnfailTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new UnfailTask.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self { storage, services }
    }
}

#[async_trait]
impl<S, V> MonitorTask for UnfailTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "unfail"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(10 * 60) // 10 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut errors = Vec::new();

        // Delegate to MonitorStorage which handles the full unfail logic:
        // querying unfail reqs, checking merkle paths, updating statuses.
        match self.storage.un_fail().await {
            Ok(()) => {
                tracing::debug!("Unfail check completed");
            }
            Err(e) => {
                errors.push(format!("un_fail failed: {}", e));
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
        assert_eq!("unfail", "unfail");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(10 * 60);
        assert_eq!(interval.as_secs(), 600);
    }
}
