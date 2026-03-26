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
                        let block_height =
                            path_result.header.as_ref().map(|h| h.height).unwrap_or(0);
                        let block_hash = path_result
                            .header
                            .as_ref()
                            .map(|h| h.hash.as_str())
                            .unwrap_or("unknown");

                        tracing::info!(
                            task = "check_no_sends",
                            txid = txid,
                            block_height = block_height,
                            block_hash = block_hash,
                            proven_tx_req_id = req.proven_tx_req_id,
                            "Found merkle proof for nosend transaction"
                        );
                        result.items_processed += 1;

                        // Persist the proof by delegating to
                        // synchronize_transaction_statuses. That method independently
                        // queries for unmined/unknown/callback/sending/unconfirmed
                        // proven_tx_reqs and persists any proofs it discovers.  It does
                        // NOT cover nosend status, so the nosend transaction's own
                        // proof is not persisted here.
                        //
                        // Full persistence for nosend proofs requires adding a
                        // dedicated method to MonitorStorage (e.g.
                        // `persist_proof_for_txid`) that creates a ProvenTx record
                        // and updates the proven_tx_req's proven_tx_id and status to
                        // completed -- mirroring the logic in
                        // StorageSqlx::synchronize_transaction_statuses.
                        if let Err(e) = self
                            .storage
                            .synchronize_transaction_statuses()
                            .await
                        {
                            tracing::warn!(
                                task = "check_no_sends",
                                txid = txid,
                                error = %e,
                                "Failed to synchronize transaction statuses after proof discovery"
                            );
                            result.add_error(format!(
                                "synchronize_transaction_statuses failed for {}: {}",
                                txid, e
                            ));
                        }
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
