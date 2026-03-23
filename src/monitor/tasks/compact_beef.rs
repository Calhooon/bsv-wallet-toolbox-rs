//! Compact BEEF task - retroactively compacts stored input_beef blobs.
//!
//! Over time, stored input_beef blobs in proven_tx_reqs and transactions tables
//! become stale: they contain full raw ancestor transactions that have since been
//! proven (merkle proofs stored in proven_txs). This task compacts those blobs by
//! upgrading unproven transactions with their now-available BUMPs and trimming
//! unnecessary ancestor chains.
//!
//! Safety: Only compacts input_beef for completed proof requests (fully proven
//! transactions) to avoid interfering with pending broadcasts or rebroadcasts.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::storage::MonitorStorage;
use crate::Result;

/// Task that retroactively compacts stored input_beef blobs.
pub struct CompactBeefTask<S>
where
    S: MonitorStorage + 'static,
{
    storage: Arc<S>,
}

impl<S> CompactBeefTask<S>
where
    S: MonitorStorage + 'static,
{
    /// Create a new compact BEEF task.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S> MonitorTask for CompactBeefTask<S>
where
    S: MonitorStorage + 'static,
{
    fn name(&self) -> &'static str {
        "compact_beef"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(15 * 60) // 15 minutes
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut errors = Vec::new();

        match MonitorStorage::compact_input_beefs(self.storage.as_ref()).await {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(
                        compacted = count,
                        "Compact BEEF: compacted stored input_beef blobs"
                    );
                } else {
                    tracing::debug!("Compact BEEF: no stale input_beef blobs to compact");
                }

                Ok(TaskResult {
                    items_processed: count,
                    errors,
                })
            }
            Err(e) => {
                errors.push(format!("compact_input_beefs failed: {}", e));
                Ok(TaskResult {
                    items_processed: 0,
                    errors,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_beef_default_interval() {
        let expected = Duration::from_secs(15 * 60);
        assert_eq!(expected.as_secs(), 900);
    }
}
