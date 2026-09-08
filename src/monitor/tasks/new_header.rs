//! New header task: polls the header service for the chain tip.
//!
//! The reference (ts-stack `TaskNewHeader`), conformed to since 0.3.66:
//!
//! 1. Each cycle reads the chain tip header (height AND hash) from the
//!    header service (chaintracks, then BHS; never a courier). An unreadable
//!    tip is an error for this cycle and no observation.
//! 2. The first header is queued. A LOWER height is an "old header": ignored
//!    (reverted), never a reorg, never a deactivation, never a gate change.
//!    A new height, or a different hash at the same height (a "reorg
//!    header"), queues that header.
//! 3. A queued header that is still the tip a full cycle later is PROCESSED:
//!    the shared `check_for_proofs` trigger is raised and the storage's
//!    persisted proof gate (`MonitorStorage::set_max_acceptable_proof_height`)
//!    is raised to its height. Nothing is accepted before the first PROCESS:
//!    the gate is closed (0) on a fresh database. `bsv-wallet tick` therefore
//!    opens the gate only on its SECOND run (the tracker's state is persisted
//!    with the gate, so the runs see each other).
//! 4. A reorg header deactivates the old hash. On every tip move the ring of
//!    the last 12 observed tips is reconciled against the header service,
//!    newest first, and every remembered hash that no longer matches is
//!    deactivated, stopping at the first match (the reference's
//!    `processReorg(deactivatedHeaders)` that our chaintracks does not push;
//!    `docs/REORG-DIVERGENCES.md`). Every deactivated header goes on the
//!    reorg queue.
//!
//! The decisions live in `crate::monitor::reorg_ops::HeaderTracker` (pure,
//! pinned); this task feeds it observations, reads the ring's canonical
//! hashes, persists the state and acts on the events.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::monitor::reorg_ops::{
    block_hash_of_header, new_reorg_queue, DeactivatedHeader, HeaderTracker, ReorgQueue,
};
use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

/// Task that polls for new blockchain block headers.
pub struct NewHeaderTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    /// The reference's state machine, loaded from storage on first use.
    tracker: std::sync::Mutex<Option<HeaderTracker>>,
    /// Deactivated headers for the reorg task.
    queue: ReorgQueue,
    /// Last observed chain height.
    last_height: AtomicU32,
    /// Flag indicating a header was PROCESSED (for proof checking).
    /// Arc-wrapped so it can be shared with CheckForProofsTask (TS pattern: checkNow).
    pub new_header_received: Arc<AtomicBool>,
}

impl<S, V> NewHeaderTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a header task with its own reorg queue (tests and the
    /// standalone shape).
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self::with_queue(storage, services, new_reorg_queue())
    }

    /// Create the task around the daemon's shared reorg queue.
    pub fn with_queue(storage: Arc<S>, services: Arc<V>, queue: ReorgQueue) -> Self {
        Self {
            storage,
            services,
            tracker: std::sync::Mutex::new(None),
            queue,
            last_height: AtomicU32::new(0),
            new_header_received: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The shared reorg queue.
    pub fn queue(&self) -> ReorgQueue {
        self.queue.clone()
    }

    /// Check if there's been a processed header since last check.
    pub fn has_new_header(&self) -> bool {
        self.new_header_received.load(Ordering::SeqCst)
    }

    /// Clear the new header flag (call after processing).
    pub fn clear_new_header_flag(&self) {
        self.new_header_received.store(false, Ordering::SeqCst);
    }

    /// Get the last observed height.
    pub fn last_known_height(&self) -> u32 {
        self.last_height.load(Ordering::SeqCst)
    }

    /// Get a clone of the Arc<AtomicBool> flag for sharing with CheckForProofsTask.
    /// TS pattern: Monitor.processNewBlockHeader sets TaskCheckForProofs.checkNow.
    pub fn new_header_received_flag(&self) -> Arc<AtomicBool> {
        self.new_header_received.clone()
    }

    async fn tracker(&self) -> HeaderTracker {
        if let Some(t) = self
            .tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return t;
        }
        match self.storage.load_header_tracker_state().await {
            Ok(Some(state)) => HeaderTracker::from_state(state),
            Ok(None) => HeaderTracker::new(),
            Err(e) => {
                tracing::warn!(task = "new_header", error = %e, "header tracker state unreadable; starting fresh");
                HeaderTracker::new()
            }
        }
    }

    /// Feed one observation (the tip's height and hash) to the tracker and
    /// act on its events. Exposed for tests; `run` reads the tip from the
    /// header service.
    pub async fn observe(&self, height: u32, hash: &str) -> TaskResult {
        let mut result = TaskResult::new();
        let mut tracker = self.tracker().await;
        let events = tracker.observe(height, hash);
        self.last_height.store(height, Ordering::SeqCst);

        if let Some((h, hash)) = &events.old_header {
            tracing::debug!(
                task = "new_header",
                height = h,
                hash = %hash,
                last = tracker.last_height(),
                "old header: a lower tip than the last one; ignored"
            );
        }

        let mut deactivated = events.deactivated.clone();
        if events.check_ring {
            // Reconcile the remembered tips against the header service,
            // newest first, stopping at the first match or the first
            // unreadable height.
            let mut canonical: Vec<(u32, Option<String>)> = Vec::new();
            for (h, remembered) in tracker.ring_below_tip_newest_first() {
                match self.services.get_header_for_height(h).await {
                    Ok(bytes) => {
                        let c = block_hash_of_header(&bytes);
                        let stop = c
                            .as_deref()
                            .map(|c| c.eq_ignore_ascii_case(&remembered))
                            .unwrap_or(true);
                        canonical.push((h, c));
                        if stop {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            task = "new_header",
                            height = h,
                            error = %e,
                            "ring check: header unreadable; the walk stops here (the review task covers it)"
                        );
                        canonical.push((h, None));
                        break;
                    }
                }
            }
            deactivated.extend(tracker.deactivate_mismatched(&canonical));
        }

        if !deactivated.is_empty() {
            let mut q = self.queue.lock().await;
            for (h, hash) in &deactivated {
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
        }

        if let Some((h, hash)) = &events.queued {
            tracing::info!(
                task = "new_header",
                height = h,
                hash = %hash,
                reorg_header = events.reorg_header,
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
            // The persisted gate only ever rises.
            match self.storage.max_acceptable_proof_height().await {
                Ok(gate) if gate >= *h => {}
                Ok(_) => {
                    if let Err(e) = self.storage.set_max_acceptable_proof_height(*h).await {
                        result.add_error(format!("could not raise the proof gate to {h}: {e}"));
                    }
                }
                Err(e) => result.add_error(format!("could not read the proof gate: {e}")),
            }
        }

        if let Err(e) = self
            .storage
            .save_header_tracker_state(tracker.state())
            .await
        {
            result.add_error(format!("could not persist the header tracker state: {e}"));
        }
        *self.tracker.lock().unwrap_or_else(|e| e.into_inner()) = Some(tracker);
        result
    }
}

#[async_trait]
impl<S, V> MonitorTask for NewHeaderTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "new_header"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    async fn run(&self) -> Result<TaskResult> {
        let tip = match self.services.get_chain_tip_header().await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    task = "new_header",
                    error = %e,
                    "Failed to read the chain tip header; no observation this cycle"
                );
                let mut result = TaskResult::new();
                result.add_error(format!("Failed to read the chain tip header: {}", e));
                return Ok(result);
            }
        };
        let hash = if tip.hash.is_empty() {
            block_hash_of_header(&tip.to_binary()).unwrap_or_default()
        } else {
            tip.hash.to_ascii_lowercase()
        };
        Ok(self.observe(tip.height, &hash).await)
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::services::mock::MockWalletServices;
    use crate::services::BlockHeader;
    use crate::{StorageSqlx, WalletStorageWriter};

    fn header(height: u32, hash: &str) -> BlockHeader {
        BlockHeader {
            version: 0x20000000,
            previous_hash: "p".repeat(64),
            merkle_root: "m".repeat(64),
            time: 1_700_000_000,
            bits: 0,
            nonce: 0,
            hash: hash.to_string(),
            height,
        }
    }

    async fn storage() -> Arc<StorageSqlx> {
        let s = StorageSqlx::in_memory().await.unwrap();
        s.migrate("test-wallet", &"0".repeat(64)).await.unwrap();
        s.make_available().await.unwrap();
        Arc::new(s)
    }

    #[tokio::test]
    async fn the_task_name_and_flag_are_shareable() {
        let task = NewHeaderTask::new(storage().await, Arc::new(MockWalletServices::new()));
        assert_eq!(task.name(), "new_header");
        assert_eq!(task.default_interval(), Duration::from_secs(60));
        let flag = task.new_header_received_flag();
        task.new_header_received.store(true, Ordering::SeqCst);
        assert!(flag.load(Ordering::SeqCst));
        task.clear_new_header_flag();
        assert!(!flag.load(Ordering::SeqCst));
    }

    /// F13: nothing is accepted before a header has survived a cycle. The
    /// gate is closed on the first observation and opens on the second.
    #[tokio::test]
    async fn the_gate_is_closed_until_the_second_observation() {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        services.set_tip_header(header(965771, "153e10f4"));
        let task = NewHeaderTask::new(storage.clone(), services.clone());
        let r = task.run().await.unwrap();
        assert_eq!(r.items_processed, 0);
        assert!(!task.has_new_header());
        assert_eq!(
            storage.max_acceptable_proof_height().await.unwrap(),
            0,
            "closed"
        );
        let r = task.run().await.unwrap();
        assert_eq!(r.items_processed, 1);
        assert!(task.has_new_header());
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965771);
    }

    /// The tracker's state is persisted: a NEW task instance on the same
    /// storage (a second `bsv-wallet tick`) processes the header the first
    /// one queued.
    #[tokio::test]
    async fn the_tracker_state_survives_a_new_task_instance() {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        services.set_tip_header(header(965771, "153e10f4"));
        let first = NewHeaderTask::new(storage.clone(), services.clone());
        first.run().await.unwrap();
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 0);
        let second = NewHeaderTask::new(storage.clone(), services.clone());
        let r = second.run().await.unwrap();
        assert_eq!(r.items_processed, 1, "the queued header was persisted");
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965771);
    }

    /// The 2026-09-07 competition: a different block at the same height
    /// deactivates the old hash (queued for the reorg task) and must itself
    /// survive a cycle; the gate does not move.
    #[tokio::test]
    async fn a_same_height_hash_change_queues_the_old_hash_and_the_replacement_is_processed_later()
    {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        let task = NewHeaderTask::new(storage.clone(), services.clone());
        task.observe(965771, "153e10f4").await;
        task.observe(965771, "153e10f4").await;
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965771);
        task.clear_new_header_flag();
        let r = task.observe(965771, "1de5aa96").await;
        assert_eq!(r.items_processed, 0);
        assert!(!task.has_new_header());
        let q = task.queue().lock().await.clone();
        assert_eq!(q.len(), 1);
        assert_eq!((q[0].height, q[0].hash.as_str()), (965771, "153e10f4"));
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965771);
        let r = task.observe(965771, "1de5aa96").await;
        assert_eq!(r.items_processed, 1);
        assert_eq!(
            task.queue().lock().await.len(),
            1,
            "nothing more deactivated"
        );
    }

    /// F6 re-pin: a tip decrease is an old header. No deactivation, no queue
    /// entry, no gate change.
    #[tokio::test]
    async fn a_tip_decrease_is_an_old_header() {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        let task = NewHeaderTask::new(storage.clone(), services.clone());
        task.observe(965773, "0851a554").await;
        task.observe(965773, "0851a554").await;
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965773);
        task.clear_new_header_flag();
        let r = task.observe(965772, "cc").await;
        assert_eq!(r.items_processed, 0);
        assert!(r.errors.is_empty());
        assert!(!task.has_new_header());
        assert!(task.queue().lock().await.is_empty());
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965773);
        let state = storage.load_header_tracker_state().await.unwrap().unwrap();
        assert_eq!(state.last, Some((965773, "0851a554".to_string())));
    }

    /// F9: the extend-reorg case. A@H is processed, then B'@H+1 arrives on
    /// top of A'@H: the ring walk asks the header service for H, finds A',
    /// and deactivates A.
    /// A header whose `hash` IS the hash of its bytes (what a real header
    /// service serves), distinct per `nonce`.
    fn header_at(height: u32, nonce: u32) -> BlockHeader {
        let mut h = header(height, "");
        h.nonce = nonce;
        h.hash = block_hash_of_header(&h.to_binary()).unwrap();
        h
    }

    #[tokio::test]
    async fn a_replaced_previous_tip_under_a_new_tip_is_deactivated_by_the_ring_walk() {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        let task = NewHeaderTask::new(storage.clone(), services.clone());
        let g = header_at(100, 1);
        let a = header_at(101, 2);
        let a_prime = header_at(101, 3);
        let b_prime = header_at(102, 4);
        let c_prime = header_at(103, 5);
        services.set_header_for_height(g.clone());
        services.set_header_for_height(a.clone());
        task.observe(100, &g.hash).await;
        task.observe(100, &g.hash).await;
        task.observe(101, &a.hash).await;
        task.observe(101, &a.hash).await;
        assert!(task.queue().lock().await.is_empty());
        // The fork: A' at 101 under B' at 102.
        services.set_header_for_height(a_prime.clone());
        task.observe(102, &b_prime.hash).await;
        let q = task.queue().lock().await.clone();
        assert_eq!(q.len(), 1);
        assert_eq!((q[0].height, q[0].hash.as_str()), (101, a.hash.as_str()));
        // The ring now remembers the canonical A' at 101 and the walk stops
        // at the first match: nothing more on the next tip.
        services.set_header_for_height(b_prime.clone());
        task.observe(103, &c_prime.hash).await;
        assert_eq!(task.queue().lock().await.len(), 1);
        let state = storage.load_header_tracker_state().await.unwrap().unwrap();
        assert!(state.ring.contains(&(101, a_prime.hash.clone())));
        assert!(!state.ring.contains(&(101, a.hash.clone())));
    }

    /// An unreadable tip is an error for the cycle and no observation.
    #[tokio::test]
    async fn an_unreadable_tip_observes_nothing() {
        let storage = storage().await;
        let services = Arc::new(MockWalletServices::new());
        services.set_tip_unavailable();
        let task = NewHeaderTask::new(storage.clone(), services.clone());
        let r = task.run().await.unwrap();
        assert_eq!(r.errors.len(), 1);
        assert!(storage.load_header_tracker_state().await.unwrap().is_none());
    }
}
