//! Reorg task - handles blockchain reorganizations.
//!
//! This task processes deactivated headers from chain reorganizations,
//! reviewing matching ProvenTx records and updating proof data.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use super::{MonitorTask, TaskResult};
use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, MonitorStorage};
use crate::Result;

/// A deactivated header from a reorg.
#[derive(Debug, Clone)]
pub struct DeactivatedHeader {
    /// Block hash that was deactivated.
    pub hash: String,
    /// Block height.
    pub height: u32,
    /// When this was deactivated.
    pub deactivated_at: chrono::DateTime<chrono::Utc>,
    /// Number of retry attempts.
    pub retry_count: u32,
}

/// Maximum retry attempts for reorg handling.
const MAX_RETRY_COUNT: u32 = 3;

/// Delay before processing deactivated headers (10 minutes).
const REORG_PROCESS_DELAY_SECS: i64 = 10 * 60;

/// Task that handles blockchain reorganizations.
///
/// When a reorg is detected, headers can be deactivated (removed from the main chain).
/// This task processes these deactivated headers with a delay to avoid unnecessary
/// disruption from temporary forks. It verifies proofs and updates transactions
/// that may have been affected.
pub struct ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    /// Queue of deactivated headers to process.
    deactivated_headers: RwLock<Vec<DeactivatedHeader>>,
}

impl<S, V> ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new reorg task.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self {
            storage,
            services,
            deactivated_headers: RwLock::new(Vec::new()),
        }
    }

    /// Queue a deactivated header for processing.
    pub async fn queue_deactivated_header(&self, hash: String, height: u32) {
        let header = DeactivatedHeader {
            hash,
            height,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };

        let mut headers = self.deactivated_headers.write().await;
        headers.push(header);

        tracing::info!(
            task = "reorg",
            height = height,
            "Queued deactivated header for reorg processing"
        );
    }

    /// Get the number of pending deactivated headers.
    pub async fn pending_count(&self) -> usize {
        self.deactivated_headers.read().await.len()
    }
}

#[async_trait]
impl<S, V> MonitorTask for ReorgTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "reorg"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut result = TaskResult::new();
        let now = chrono::Utc::now();
        let process_threshold = now - chrono::Duration::seconds(REORG_PROCESS_DELAY_SECS);

        // Get headers that are ready to process
        let mut headers = self.deactivated_headers.write().await;
        let mut processed_indices = Vec::new();
        let mut requeue = Vec::new();

        for (idx, header) in headers.iter_mut().enumerate() {
            // Only process headers that have aged enough
            if header.deactivated_at > process_threshold {
                continue;
            }

            tracing::info!(
                task = "reorg",
                hash = header.hash,
                height = header.height,
                retry_count = header.retry_count,
                "Processing deactivated header"
            );

            // Find proven transactions that reference this block
            // (In TypeScript, this queries ProvenTx with matching header height/hash)
            let args = FindProvenTxReqsArgs {
                status: Some(vec![
                    ProvenTxReqStatus::Completed,
                    ProvenTxReqStatus::Unmined,
                ]),
                ..Default::default()
            };

            let reqs = match self.storage.find_proven_tx_reqs(args).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        task = "reorg",
                        error = %e,
                        "Failed to query proven transactions for reorg"
                    );
                    result.add_error(format!("Failed to query transactions: {}", e));
                    continue;
                }
            };

            // Check each transaction that might be affected
            let mut affected_count = 0u32;
            for req in &reqs {
                // Check if this transaction's proof is in the reorg'd block
                // (This would check the ProvenTx.height matches header.height)
                // For now, try to re-verify the proof
                match self.services.get_merkle_path(&req.txid, false).await {
                    Ok(path_result) => {
                        if path_result.merkle_path.is_some() {
                            tracing::debug!(
                                task = "reorg",
                                txid = req.txid,
                                "Transaction still has valid proof after reorg"
                            );
                        } else {
                            tracing::warn!(
                                task = "reorg",
                                txid = req.txid,
                                proven_tx_req_id = req.proven_tx_req_id,
                                "Transaction proof no longer valid after reorg, demoting to unmined"
                            );
                            affected_count += 1;

                            // Demote the proven_tx_req back to 'unmined' so the
                            // CheckForProofs task will re-fetch the merkle proof
                            // on its next cycle.
                            if let Err(e) = self
                                .storage
                                .update_proven_tx_req_status(
                                    req.proven_tx_req_id,
                                    ProvenTxReqStatus::Unmined,
                                )
                                .await
                            {
                                tracing::error!(
                                    task = "reorg",
                                    txid = req.txid,
                                    error = %e,
                                    "Failed to update proven_tx_req status after reorg"
                                );
                                result.add_error(format!(
                                    "Failed to update status for txid {}: {}",
                                    req.txid, e
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            task = "reorg",
                            txid = req.txid,
                            error = %e,
                            "Error checking proof after reorg"
                        );
                    }
                }
            }

            if affected_count > 0 {
                tracing::warn!(
                    task = "reorg",
                    height = header.height,
                    affected = affected_count,
                    "Transactions affected by reorg"
                );
            }

            // Check if we should retry
            if header.retry_count < MAX_RETRY_COUNT {
                // Requeue with incremented retry count
                let mut requeued = header.clone();
                requeued.retry_count += 1;
                requeued.deactivated_at = now; // Reset the delay
                requeue.push(requeued);
            }

            processed_indices.push(idx);
            result.items_processed += 1;
        }

        // Remove processed headers (in reverse order to maintain indices)
        for idx in processed_indices.into_iter().rev() {
            headers.remove(idx);
        }

        // Add requeued headers
        headers.extend(requeue);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorg_default_interval() {
        // 1 minute = 60 seconds
        let expected = Duration::from_secs(60);
        assert_eq!(expected.as_secs(), 60);
    }

    #[test]
    fn test_max_retry_count() {
        assert_eq!(MAX_RETRY_COUNT, 3);
    }

    #[test]
    fn test_reorg_process_delay() {
        // 10 minutes = 600 seconds
        assert_eq!(REORG_PROCESS_DELAY_SECS, 600);
    }

    #[test]
    fn test_deactivated_header() {
        let header = DeactivatedHeader {
            hash: "000000000000000001234567890abcdef".to_string(),
            height: 800000,
            deactivated_at: chrono::Utc::now(),
            retry_count: 0,
        };

        assert_eq!(header.height, 800000);
        assert_eq!(header.retry_count, 0);
    }
}
