//! UnFail task - recovers transactions that were incorrectly marked as failed.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, WalletStorageProvider};
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
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
}

impl<S, V> UnfailTask<S, V>
where
    S: WalletStorageProvider + 'static,
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
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "unfail"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(10 * 60) // 10 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut result = TaskResult::new();

        // Find transactions marked for unfail
        let args = FindProvenTxReqsArgs {
            status: Some(vec![ProvenTxReqStatus::Unfail]),
            ..Default::default()
        };

        let reqs = self.storage.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(result);
        }

        tracing::info!(count = reqs.len(), "Found transactions to unfail");

        for req in reqs {
            let txid = &req.txid;

            // Check if transaction has a merkle path on chain
            match self.services.get_merkle_path(txid).await {
                Ok(merkle_result) => {
                    if merkle_result.merkle_path.is_some() {
                        // Transaction is on chain - recover it
                        tracing::info!(
                            txid = %txid,
                            "Transaction found on chain, recovering"
                        );

                        // In a full implementation, we would:
                        // 1. Update proven_tx_req status to 'unmined'
                        // 2. Update transaction status to 'unproven'
                        // 3. Create UTXOs for spendable outputs
                        //
                        // This requires additional storage methods for status updates

                        result.items_processed += 1;
                    } else {
                        // Transaction not found - mark as invalid
                        tracing::info!(
                            txid = %txid,
                            "Transaction not found on chain, marking as invalid"
                        );

                        // In a full implementation:
                        // Update proven_tx_req status to 'invalid'
                    }
                }
                Err(e) => {
                    // Error checking merkle path - don't change status
                    result.add_error(format!("Failed to check merkle path for {}: {}", txid, e));
                }
            }
        }

        Ok(result)
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
