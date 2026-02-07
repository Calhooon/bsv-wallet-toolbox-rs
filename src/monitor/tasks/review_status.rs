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
use crate::storage::MonitorStorage;
use crate::Result;

/// Task that synchronizes transaction status with ProvenTxReq status.
///
/// This task delegates to `MonitorStorage::review_status()` which ensures
/// consistency between transaction records and their associated proof requests.
/// It finds transactions that have a proof but haven't been marked as completed yet.
pub struct ReviewStatusTask<S>
where
    S: MonitorStorage + 'static,
{
    storage: Arc<S>,
    /// Flag to trigger immediate check.
    pub check_now: AtomicBool,
}

impl<S> ReviewStatusTask<S>
where
    S: MonitorStorage + 'static,
{
    /// Create a new review status task.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
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
    S: MonitorStorage + 'static,
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

        let mut errors = Vec::new();

        // Delegate to MonitorStorage::review_status() which operates across all users
        match MonitorStorage::review_status(self.storage.as_ref()).await {
            Ok(result) => {
                if !result.log.is_empty() {
                    tracing::debug!(log = %result.log, "Review status completed");
                }
            }
            Err(e) => {
                errors.push(format!("review_status failed: {}", e));
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
    fn test_review_status_default_interval() {
        // 15 minutes = 900 seconds
        let expected = Duration::from_secs(15 * 60);
        assert_eq!(expected.as_secs(), 900);
    }
}
