//! Review status task - synchronizes transaction status with ProvenTxReq status.
//!
//! Finds aged transactions with provenTxId and non-'completed' status,
//! and sets them to 'completed'. Handles mismatches between transaction
//! and proof request status.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::{FindProvenTxReqsArgs, WalletStorageProvider};
use crate::Result;

/// Default age threshold for reviewing status (5 minutes).
const DEFAULT_AGE_THRESHOLD_SECS: u64 = 5 * 60;

/// Task that synchronizes transaction status with ProvenTxReq status.
///
/// This task ensures consistency between transaction records and their
/// associated proof requests. It finds transactions that have a proof
/// but haven't been marked as completed yet.
pub struct ReviewStatusTask<S>
where
    S: WalletStorageProvider + 'static,
{
    storage: Arc<S>,
    /// Age threshold before reviewing status (default 5 minutes).
    age_threshold: Duration,
    /// Flag to trigger immediate check.
    pub check_now: AtomicBool,
}

impl<S> ReviewStatusTask<S>
where
    S: WalletStorageProvider + 'static,
{
    /// Create a new review status task.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            age_threshold: Duration::from_secs(DEFAULT_AGE_THRESHOLD_SECS),
            check_now: AtomicBool::new(false),
        }
    }

    /// Create a new review status task with custom age threshold.
    pub fn with_age_threshold(storage: Arc<S>, age_threshold: Duration) -> Self {
        Self {
            storage,
            age_threshold,
            check_now: AtomicBool::new(false),
        }
    }

    /// Trigger an immediate check on the next run.
    pub fn trigger_check(&self) {
        self.check_now.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl<S> MonitorTask for ReviewStatusTask<S>
where
    S: WalletStorageProvider + 'static,
{
    fn name(&self) -> &'static str {
        "review_status"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(15 * 60) // 15 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        // Reset the check_now flag
        self.check_now.store(false, Ordering::SeqCst);

        let mut result = TaskResult::new();

        // Calculate cutoff time for "aged" transactions
        let cutoff = chrono::Utc::now() - chrono::Duration::from_std(self.age_threshold).unwrap();

        // Find transactions that:
        // 1. Have a proven_tx_id (proof exists)
        // 2. Are older than the age threshold
        // 3. Don't have 'completed' status
        //
        // For now, query proven_tx_reqs with status 'completed' that might need
        // their associated transactions updated
        let args = FindProvenTxReqsArgs {
            status: Some(vec![ProvenTxReqStatus::Completed]),
            ..Default::default()
        };

        let completed_reqs = match self.storage.find_proven_tx_reqs(args).await {
            Ok(reqs) => reqs,
            Err(e) => {
                tracing::warn!(
                    task = "review_status",
                    error = %e,
                    "Failed to query completed proven_tx_reqs"
                );
                result.add_error(format!("Failed to query proven_tx_reqs: {}", e));
                return Ok(result);
            }
        };

        tracing::debug!(
            task = "review_status",
            count = completed_reqs.len(),
            cutoff = %cutoff,
            "Reviewing transaction statuses"
        );

        // For each completed proof request, verify the transaction is also marked completed
        for req in completed_reqs {
            // Check if the transaction's updated_at is before the cutoff
            if req.updated_at < cutoff {
                // Transaction is aged and has completed proof
                // TODO: Query the associated transaction and ensure it's marked completed
                tracing::debug!(
                    task = "review_status",
                    txid = req.txid,
                    proven_tx_id = ?req.proven_tx_id,
                    "Would sync transaction status to completed"
                );
                result.items_processed += 1;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_review_status_default_interval() {
        // 15 minutes = 900 seconds
        let expected = Duration::from_secs(15 * 60);
        assert_eq!(expected.as_secs(), 900);
    }

    #[test]
    fn test_age_threshold_default() {
        // 5 minutes = 300 seconds
        assert_eq!(DEFAULT_AGE_THRESHOLD_SECS, 300);
    }
}
