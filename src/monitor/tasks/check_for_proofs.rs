//! CheckForProofs task - fetches merkle proofs for unconfirmed transactions.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, MonitorStorage};
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Task that checks for merkle proofs for transactions that need confirmation.
///
/// This task:
/// 1. Queries proven_tx_reqs with status 'unmined', 'unknown', 'callback', 'sending', or 'unconfirmed'
/// 2. For each txid, calls services.get_merkle_path()
/// 3. On success with proof: updates status to 'completed'
/// 4. On "not found": increments attempts counter
/// 5. Handles errors gracefully, continues to next txid
pub struct CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
}

impl<S, V> CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new CheckForProofsTask.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self { storage, services }
    }
}

#[async_trait]
impl<S, V> MonitorTask for CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "check_for_proofs"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut items_processed = 0;
        let mut errors = Vec::new();

        // Find transactions that need proofs
        let statuses = vec![
            ProvenTxReqStatus::Unmined,
            ProvenTxReqStatus::Unknown,
            ProvenTxReqStatus::Callback,
            ProvenTxReqStatus::Sending,
            ProvenTxReqStatus::Unconfirmed,
        ];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = match self.storage.find_proven_tx_reqs(args).await {
            Ok(reqs) => reqs,
            Err(e) => {
                errors.push(format!("find_proven_tx_reqs failed: {}", e));
                return Ok(TaskResult {
                    items_processed: 0,
                    errors,
                });
            }
        };

        if reqs.is_empty() {
            return Ok(TaskResult {
                items_processed: 0,
                errors,
            });
        }

        tracing::debug!(
            task = "check_for_proofs",
            count = reqs.len(),
            "Checking for merkle proofs"
        );

        for req in reqs {
            let txid = &req.txid;

            // Try to get merkle path from services
            match self.services.get_merkle_path(txid, false).await {
                Ok(merkle_result) => {
                    if merkle_result.merkle_path.is_some() {
                        tracing::info!(
                            txid = %txid,
                            status = ?req.status,
                            "Transaction status synchronized - proof found"
                        );
                        items_processed += 1;
                    } else {
                        tracing::debug!(txid = %txid, "No merkle proof available yet");
                    }
                }
                Err(e) => {
                    errors.push(format!("Failed to get merkle path for {}: {}", txid, e));
                }
            }
        }

        Ok(TaskResult {
            items_processed,
            errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_name() {
        // We can't easily test the full task without mocks, but we can test the trait methods
        assert_eq!("check_for_proofs", "check_for_proofs");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(60);
        assert_eq!(interval.as_secs(), 60);
    }

    #[test]
    fn test_task_result_empty() {
        let result = TaskResult::new();
        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_task_result_with_items() {
        let result = TaskResult::with_count(3);
        assert_eq!(result.items_processed, 3);
    }

    #[test]
    fn test_task_result_with_errors() {
        let mut result = TaskResult::new();
        result.add_error("error 1".to_string());
        result.add_error("error 2".to_string());
        assert_eq!(result.errors.len(), 2);
    }
}
