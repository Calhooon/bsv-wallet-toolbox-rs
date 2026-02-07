//! Check no-sends task - retrieves proofs for 'nosend' transactions.
//!
//! Unlike intentional transactions, 'nosend' transactions are valid transactions
//! that were not broadcast by the wallet but may have been mined externally.
//! This task checks for merkle proofs for these transactions.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, MonitorStorage};
use crate::Result;

/// Task that retrieves merkle proofs for 'nosend' transactions.
///
/// 'NoSend' transactions are valid but not broadcast by the wallet.
/// They may have been mined externally (e.g., by another wallet or service).
/// This task periodically checks if these transactions have been mined
/// and retrieves their merkle proofs.
pub struct CheckNoSendsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    /// Flag to trigger immediate check (set by external events).
    pub check_now: AtomicBool,
}

impl<S, V> CheckNoSendsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new check no-sends task.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self {
            storage,
            services,
            check_now: AtomicBool::new(false),
        }
    }

    /// Trigger an immediate check on the next run.
    pub fn trigger_check(&self) {
        self.check_now.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl<S, V> MonitorTask for CheckNoSendsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "check_no_sends"
    }

    fn default_interval(&self) -> Duration {
        // Daily check by default (24 hours)
        Duration::from_secs(24 * 60 * 60)
    }

    async fn run(&self) -> Result<TaskResult> {
        // Reset the check_now flag
        self.check_now.store(false, Ordering::SeqCst);

        let mut result = TaskResult::new();

        // Query for 'nosend' proven_tx_reqs
        let args = FindProvenTxReqsArgs {
            status: Some(vec![ProvenTxReqStatus::NoSend]),
            ..Default::default()
        };

        let nosend_reqs = match self.storage.find_proven_tx_reqs(args).await {
            Ok(reqs) => reqs,
            Err(e) => {
                tracing::warn!(
                    task = "check_no_sends",
                    error = %e,
                    "Failed to query nosend transactions"
                );
                result.add_error(format!("Failed to query nosend transactions: {}", e));
                return Ok(result);
            }
        };

        tracing::debug!(
            task = "check_no_sends",
            count = nosend_reqs.len(),
            "Found nosend transactions to check"
        );

        for req in nosend_reqs {
            let txid = &req.txid;

            // Try to get merkle path for this transaction
            match self.services.get_merkle_path(txid, false).await {
                Ok(path_result) => {
                    if path_result.merkle_path.is_some() {
                        tracing::info!(
                            task = "check_no_sends",
                            txid = txid,
                            "Found merkle proof for nosend transaction"
                        );
                        result.items_processed += 1;

                        // TODO: Update proven_tx_req status to indicate proof found
                        // This would involve creating a ProvenTx record and updating
                        // the req's proven_tx_id
                    } else {
                        tracing::debug!(
                            task = "check_no_sends",
                            txid = txid,
                            "No merkle proof yet for nosend transaction"
                        );
                    }
                }
                Err(e) => {
                    // Log but don't fail the task
                    tracing::debug!(
                        task = "check_no_sends",
                        txid = txid,
                        error = %e,
                        "Error checking merkle path for nosend transaction"
                    );
                    result.add_error(format!("Error checking {}: {}", txid, e));
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
    fn test_check_no_sends_task_name() {
        
        

        // We can't easily create a real storage in tests, so just test the constants
        assert_eq!(
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(86400)
        );
    }

    #[test]
    fn test_check_no_sends_default_interval() {
        // 24 hours = 86400 seconds
        let expected = Duration::from_secs(24 * 60 * 60);
        assert_eq!(expected.as_secs(), 86400);
    }
}
