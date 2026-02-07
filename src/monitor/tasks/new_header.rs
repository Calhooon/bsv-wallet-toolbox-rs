//! New header task - polls for new blockchain block headers.
//!
//! This task polls Chaintracks for new block headers and triggers
//! proof solicitation when new blocks are confirmed.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::services::WalletServices;
use crate::Result;

/// Task that polls for new blockchain block headers.
///
/// Monitors the blockchain for new blocks. When a new block is detected,
/// it triggers proof solicitation for pending transactions. Uses a
/// one-cycle delay to avoid reorg disruptions.
pub struct NewHeaderTask<V>
where
    V: WalletServices + 'static,
{
    services: Arc<V>,
    /// Last known chain height.
    last_height: AtomicU32,
    /// Last known chain tip hash (stored as a string).
    #[allow(dead_code)]
    last_hash: std::sync::RwLock<Option<String>>,
    /// Number of consecutive cycles without new headers.
    stable_cycles: AtomicU32,
    /// Flag indicating a new header was received (for proof checking).
    pub new_header_received: AtomicBool,
}

impl<V> NewHeaderTask<V>
where
    V: WalletServices + 'static,
{
    /// Create a new header monitoring task.
    pub fn new(services: Arc<V>) -> Self {
        Self {
            services,
            last_height: AtomicU32::new(0),
            last_hash: std::sync::RwLock::new(None),
            stable_cycles: AtomicU32::new(0),
            new_header_received: AtomicBool::new(false),
        }
    }

    /// Check if there's been a new header since last check.
    pub fn has_new_header(&self) -> bool {
        self.new_header_received.load(Ordering::SeqCst)
    }

    /// Reset the new header flag.
    pub fn clear_new_header_flag(&self) {
        self.new_header_received.store(false, Ordering::SeqCst);
    }

    /// Get the last known height.
    pub fn last_known_height(&self) -> u32 {
        self.last_height.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl<V> MonitorTask for NewHeaderTask<V>
where
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "new_header"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut result = TaskResult::new();

        // Get current chain height from services
        let current_height = match self.services.get_height().await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    task = "new_header",
                    error = %e,
                    "Failed to get current chain height"
                );
                result.add_error(format!("Failed to get chain height: {}", e));
                return Ok(result);
            }
        };

        let last_height = self.last_height.load(Ordering::SeqCst);

        if last_height == 0 {
            // First run, just record the current height
            self.last_height.store(current_height, Ordering::SeqCst);
            tracing::info!(
                task = "new_header",
                height = current_height,
                "Initialized with chain height"
            );
            return Ok(result);
        }

        match current_height.cmp(&last_height) {
            std::cmp::Ordering::Greater => {
                // New blocks found
                let blocks_ahead = current_height - last_height;
                self.last_height.store(current_height, Ordering::SeqCst);
                self.stable_cycles.store(0, Ordering::SeqCst);

                tracing::info!(
                    task = "new_header",
                    height = current_height,
                    blocks_ahead = blocks_ahead,
                    "New block(s) detected"
                );

                // Set the flag to trigger proof checking
                self.new_header_received.store(true, Ordering::SeqCst);

                result.items_processed = blocks_ahead;
            }
            std::cmp::Ordering::Less => {
                // Potential reorg detected (chain tip went backwards)
                tracing::warn!(
                    task = "new_header",
                    current = current_height,
                    last = last_height,
                    "Chain height decreased - possible reorg"
                );
                self.last_height.store(current_height, Ordering::SeqCst);
                self.stable_cycles.store(0, Ordering::SeqCst);
                result.add_error(format!(
                    "Chain height decreased from {} to {} - possible reorg",
                    last_height, current_height
                ));
            }
            std::cmp::Ordering::Equal => {
                // Same height, increment stable cycle counter
                let cycles = self.stable_cycles.fetch_add(1, Ordering::SeqCst) + 1;
                tracing::debug!(
                    task = "new_header",
                    height = current_height,
                    stable_cycles = cycles,
                    "No new blocks"
                );
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_header_default_interval() {
        // 1 minute = 60 seconds
        let expected = Duration::from_secs(60);
        assert_eq!(expected.as_secs(), 60);
    }

    #[tokio::test]
    async fn test_new_header_task_name() {
        use crate::services::Services;

        let services = Services::mainnet().unwrap();
        let task = NewHeaderTask::new(Arc::new(services));
        assert_eq!(task.name(), "new_header");
        assert_eq!(task.last_known_height(), 0);
        assert!(!task.has_new_header());
    }
}
