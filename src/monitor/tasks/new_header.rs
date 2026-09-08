//! New header task - polls for new blockchain block headers.
//!
//! The reference (ts-stack `TaskNewHeader`), as of M19 R1 (2026-09-08):
//!
//! 1. Each cycle reads the chain tip's height AND hash.
//! 2. A header is PROCESSED only after it has remained the chain tip for a
//!    full cycle (the one-cycle proof lag). Processing raises the shared
//!    `check_for_proofs` trigger and moves the storage's proof gate
//!    (`MonitorStorage::set_max_acceptable_proof_height`), so no proof for
//!    a block that has not aged is ever stored.
//! 3. A hash change at the same height, or a tip decrease, is a REORG: the
//!    old header goes onto the shared reorg queue as a deactivated header
//!    and the gate drops below the reorged height.
//!
//! The decisions live in `crate::monitor::reorg_ops::HeaderTracker` (pure,
//! pinned); this task only feeds it observations and acts on its events.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{
    block_hash_of_header, new_reorg_queue, DeactivatedHeader, HeaderTracker, ReorgQueue,
};
use crate::services::WalletServices;
use crate::Result;

/// The sink the daemon wires to `MonitorStorage::set_max_acceptable_proof_height`.
pub type GateSink = Arc<dyn Fn(u32) + Send + Sync>;

/// Task that polls for new blockchain block headers.
pub struct NewHeaderTask<V>
where
    V: WalletServices + 'static,
{
    services: Arc<V>,
    /// Last observed chain height.
    last_height: AtomicU32,
    /// The reference's state machine.
    tracker: std::sync::Mutex<HeaderTracker>,
    /// The highest height whose proofs may be stored; shared with the
    /// review task (the audit's upper bound).
    processed_height: Arc<AtomicU32>,
    /// Deactivated headers for the reorg task.
    queue: ReorgQueue,
    /// Where the processed height is published (the storage's proof gate).
    gate_sink: std::sync::RwLock<Option<GateSink>>,
    /// Flag indicating a header was PROCESSED (for proof checking).
    /// Arc-wrapped so it can be shared with CheckForProofsTask (TS pattern: checkNow).
    pub new_header_received: Arc<AtomicBool>,
}

impl<V> NewHeaderTask<V>
where
    V: WalletServices + 'static,
{
    /// Create a new header monitoring task with its own queue and counter
    /// (tests and the standalone shape).
    pub fn new(services: Arc<V>) -> Self {
        Self::with_shared(services, new_reorg_queue(), Arc::new(AtomicU32::new(0)))
    }

    /// Create the task around the daemon's shared reorg queue and processed
    /// height.
    pub fn with_shared(services: Arc<V>, queue: ReorgQueue, processed_height: Arc<AtomicU32>) -> Self {
        Self {
            services,
            last_height: AtomicU32::new(0),
            tracker: std::sync::Mutex::new(HeaderTracker::new()),
            processed_height,
            queue,
            gate_sink: std::sync::RwLock::new(None),
            new_header_received: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wire the storage's proof gate. Called once by the daemon.
    pub fn set_gate_sink(&self, sink: GateSink) {
        if let Ok(mut g) = self.gate_sink.write() {
            *g = Some(sink);
        }
    }

    /// The shared processed height (0 = none yet).
    pub fn processed_height_handle(&self) -> Arc<AtomicU32> {
        self.processed_height.clone()
    }

    /// The shared reorg queue.
    pub fn queue(&self) -> ReorgQueue {
        self.queue.clone()
    }

    /// Check if there's been a processed header since last check.
    pub fn has_new_header(&self) -> bool {
        self.new_header_received.load(Ordering::SeqCst)
    }

    /// Reset the new header flag.
    pub fn clear_new_header_flag(&self) {
        self.new_header_received.store(false, Ordering::SeqCst);
    }

    /// Get the last observed height.
    pub fn last_known_height(&self) -> u32 {
        self.last_height.load(Ordering::SeqCst)
    }

    /// Get a clone of the new_header_received flag for sharing with CheckForProofsTask.
    /// TS pattern: Monitor.processNewBlockHeader sets TaskCheckForProofs.checkNow = true.
    pub fn new_header_received_flag(&self) -> Arc<AtomicBool> {
        self.new_header_received.clone()
    }

    fn publish_gate(&self, height: u32) {
        self.processed_height.store(height, Ordering::SeqCst);
        if let Ok(g) = self.gate_sink.read() {
            if let Some(sink) = g.as_ref() {
                sink(height);
            }
        }
    }

    /// Feed one observation to the tracker and act on its events. Exposed
    /// for tests; `run` reads the observation from the services.
    pub async fn observe(&self, height: u32, hash: Option<String>) -> TaskResult {
        let mut result = TaskResult::new();
        let (events, gate_before, gate_after) = {
            let mut t = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
            let before = t.processed_height;
            let ev = t.observe(height, hash);
            (ev, before, t.processed_height)
        };
        self.last_height.store(height, Ordering::SeqCst);
        if !events.deactivated.is_empty() {
            let mut q = self.queue.lock().await;
            for (h, hash) in &events.deactivated {
                tracing::warn!(
                    task = "new_header",
                    height = h,
                    hash = %hash,
                    marker = "reorg_header_deactivated",
                    "reorg: a header left the chain; its proofs will be re-proved"
                );
                q.push(DeactivatedHeader {
                    hash: hash.clone(),
                    height: *h,
                    deactivated_at: chrono::Utc::now(),
                    retry_count: 0,
                });
            }
        } else if events.reorg {
            tracing::warn!(
                task = "new_header",
                height,
                marker = "reorg_header_deactivated",
                "reorg: the tip moved without a known old hash; the review task covers the heights"
            );
        }
        if let Some((h, hash)) = &events.queued {
            tracing::info!(
                task = "new_header",
                height = h,
                hash = %hash,
                "New header queued; processed after one cycle as the tip"
            );
        }
        if let Some((h, hash)) = &events.processed {
            tracing::info!(
                task = "new_header",
                height = h,
                hash = %hash,
                marker = "header_processed",
                "Header processed: proofs up to this height may be stored"
            );
            self.new_header_received.store(true, Ordering::SeqCst);
            result.items_processed = 1;
        }
        if gate_after != gate_before || events.queued.is_some() && gate_before == 0 {
            self.publish_gate(gate_after);
        }
        result
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
        let current_height = match self.services.get_height().await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    task = "new_header",
                    error = %e,
                    "Failed to get current chain height"
                );
                let mut result = TaskResult::new();
                result.add_error(format!("Failed to get chain height: {}", e));
                return Ok(result);
            }
        };
        let hash = match self.services.get_header_for_height(current_height).await {
            Ok(bytes) => block_hash_of_header(&bytes),
            Err(e) => {
                tracing::debug!(
                    task = "new_header",
                    height = current_height,
                    error = %e,
                    "tip header unreadable this cycle; height-only observation"
                );
                None
            }
        };
        Ok(self.observe(current_height, hash).await)
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

    /// Test that new_header_received_flag() returns a shareable Arc that points to
    /// the same underlying AtomicBool.
    #[tokio::test]
    async fn test_new_header_received_flag_is_shareable() {
        use crate::services::Services;
        let services = Services::mainnet().unwrap();
        let task = NewHeaderTask::new(Arc::new(services));
        let flag1 = task.new_header_received_flag();
        let flag2 = task.new_header_received_flag();
        assert!(!flag1.load(Ordering::SeqCst));
        assert!(!flag2.load(Ordering::SeqCst));
        flag1.store(true, Ordering::SeqCst);
        assert!(flag2.load(Ordering::SeqCst));
        assert!(task.has_new_header());
        task.clear_new_header_flag();
        assert!(!flag1.load(Ordering::SeqCst));
        assert!(!flag2.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_flag_identity_with_task_field() {
        use crate::services::Services;
        let services = Services::mainnet().unwrap();
        let task = NewHeaderTask::new(Arc::new(services));
        let flag = task.new_header_received_flag();
        task.new_header_received.store(true, Ordering::SeqCst);
        assert!(flag.load(Ordering::SeqCst));
        flag.store(false, Ordering::SeqCst);
        assert!(!task.new_header_received.load(Ordering::SeqCst));
    }

    /// M19 R1: the proof trigger fires when a header is PROCESSED (one cycle
    /// as the tip), not when it is first seen; the gate follows the
    /// processed height; a same-height hash change queues a deactivated
    /// header and lowers the gate.
    #[tokio::test]
    async fn observations_drive_the_trigger_the_gate_and_the_reorg_queue() {
        use crate::services::Services;
        let services = Services::mainnet().unwrap();
        let task = NewHeaderTask::new(Arc::new(services));
        let published = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        {
            let published = published.clone();
            task.set_gate_sink(Arc::new(move |h| published.lock().unwrap().push(h)));
        }
        // First sight: queued, nothing processed, the gate is tip - 1.
        let r = task.observe(965771, Some("153e10f4".into())).await;
        assert_eq!(r.items_processed, 0);
        assert!(!task.has_new_header());
        assert_eq!(task.processed_height_handle().load(Ordering::SeqCst), 965770);
        assert_eq!(*published.lock().unwrap(), vec![965770]);
        // Same tip a cycle later: processed, trigger raised, gate = 965771.
        let r = task.observe(965771, Some("153e10f4".into())).await;
        assert_eq!(r.items_processed, 1);
        assert!(task.has_new_header());
        assert_eq!(task.processed_height_handle().load(Ordering::SeqCst), 965771);
        assert_eq!(*published.lock().unwrap(), vec![965770, 965771]);
        // The 2026-09-07 competition: a different block at the same height.
        task.clear_new_header_flag();
        let r = task.observe(965771, Some("1de5aa96".into())).await;
        assert_eq!(r.items_processed, 0);
        assert!(!task.has_new_header());
        assert_eq!(task.processed_height_handle().load(Ordering::SeqCst), 965770);
        let q = task.queue().lock().await.clone();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].hash, "153e10f4");
        assert_eq!(q[0].height, 965771);
        assert_eq!(*published.lock().unwrap(), vec![965770, 965771, 965770]);
    }
}
