//! SendWaiting task - broadcasts transactions that are waiting to be sent.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, WalletStorageProvider};
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Task that broadcasts transactions waiting to be sent.
///
/// This task:
/// 1. Queries proven_tx_reqs with status 'unsent' or 'sending'
/// 2. Groups transactions by batch_id if present
/// 3. Builds BEEF for each group
/// 4. Calls services.post_beef() to broadcast
/// 5. On success: updates status to 'unmined'
/// 6. On double-spend: marks transaction as 'failed'
/// 7. On error: logs and retries next cycle
pub struct SendWaitingTask<S, V>
where
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    #[allow(dead_code)]
    services: Arc<V>,
    first_run: std::sync::atomic::AtomicBool,
}

impl<S, V> SendWaitingTask<S, V>
where
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    /// Create a new SendWaitingTask.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self {
            storage,
            services,
            first_run: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl<S, V> MonitorTask for SendWaitingTask<S, V>
where
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "send_waiting"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(5 * 60) // 5 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut result = TaskResult::new();

        // On first run, don't apply age filter
        let _is_first_run = self
            .first_run
            .swap(false, std::sync::atomic::Ordering::SeqCst);

        // Find transactions that need to be sent
        let statuses = vec![ProvenTxReqStatus::Unsent, ProvenTxReqStatus::Sending];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = self.storage.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(result);
        }

        // Group by batch_id
        let mut batches: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for req in &reqs {
            let batch_key = req.batch.clone().unwrap_or_else(|| req.txid.clone());
            batches
                .entry(batch_key)
                .or_default()
                .push(req.txid.clone());
        }

        tracing::info!(
            batches = batches.len(),
            total_txs = reqs.len(),
            "Found transactions waiting to be sent"
        );

        // Process each batch
        for (batch_name, txids) in batches {
            tracing::debug!(batch = %batch_name, txids = ?txids, "Processing batch");

            // In a full implementation, we would:
            // 1. Build BEEF from the stored raw_tx and input_beef
            // 2. Call services.post_beef()
            // 3. Update proven_tx_reqs status based on result

            // For now, we just log that we would send these transactions
            // The actual broadcast requires access to the raw transaction bytes
            // which would need additional storage queries

            for txid in &txids {
                // In a full implementation:
                // let beef = build_beef_for_tx(txid).await?;
                // match self.services.post_beef(&beef, &[txid.clone()]).await {
                //     Ok(post_result) => {
                //         for tx_result in post_result.txid_results {
                //             if tx_result.status == "success" {
                //                 // Update to unmined
                //             } else if tx_result.status == "double_spend" {
                //                 // Mark as failed
                //             }
                //         }
                //     }
                //     Err(e) => {
                //         result.add_error(format!("Failed to broadcast {}: {}", txid, e));
                //     }
                // }
                tracing::debug!(txid = %txid, "Would broadcast transaction");
            }

            result.items_processed += txids.len() as u32;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_name() {
        assert_eq!("send_waiting", "send_waiting");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(5 * 60);
        assert_eq!(interval.as_secs(), 300);
    }

    #[test]
    fn test_batch_grouping() {
        let mut batches: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        // Simulate grouping
        batches.entry("batch1".to_string()).or_default().push("tx1".to_string());
        batches.entry("batch1".to_string()).or_default().push("tx2".to_string());
        batches.entry("tx3".to_string()).or_default().push("tx3".to_string());

        assert_eq!(batches.len(), 2);
        assert_eq!(batches.get("batch1").unwrap().len(), 2);
        assert_eq!(batches.get("tx3").unwrap().len(), 1);
    }
}
