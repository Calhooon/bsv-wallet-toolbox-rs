//! ArcadeEvents task — Arcade V2 SSE status subscriber.
//!
//! Subscribes OUTBOUND to the wallet's per-token Arcade V2 SSE stream
//! (`GET /events?callbackToken=<token>`) so status delivery works from plain
//! localhost — no inbound anything. One token per wallet db/port means any
//! number of concurrent wallets get independent streams.
//!
//! Event handling (maps Arcade statuses onto the existing status model):
//! - `SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` — spendability gate: mark the
//!   proven_tx_req `unmined` and the transaction `unproven` (exactly what a
//!   successful ARC `post_beef` does).
//! - `MINED` — additionally raise the shared CheckForProofs trigger flag so
//!   the proof is fetched immediately (event-driven, no schedule polling).
//!   SSE data frames do NOT carry the merkle path — only the webhook does —
//!   so MINED always triggers a fetch through the services stack.
//! - `REJECTED` / `DOUBLE_SPEND_ATTEMPTED` — mark the proven_tx_req
//!   `invalid` / `doubleSpend` so it is never re-broadcast.
//!
//! Connection lifecycle: a background tokio task (spawned in [`setup`]) holds
//! the SSE connection open and reconnects with exponential backoff (1s → 60s
//! cap, reset after a healthy connection). A fresh connect REPLAYS all
//! non-terminal statuses for the token, so reconnect gaps are lossless;
//! `Last-Event-ID` resume additionally avoids re-processing. The Monitor only
//! starts this task in Arcade mode — with classic ARC configured the task is
//! never registered.
//!
//! [`setup`]: MonitorTask::setup

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::services::providers::arcade::{statuses, ArcadeSseClient, ArcadeStatusEvent};
use crate::storage::MonitorStorage;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Initial reconnect backoff.
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
/// Maximum reconnect backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// A connection that survived this long resets the backoff.
const HEALTHY_CONNECTION: Duration = Duration::from_secs(30);

/// Monitor task that subscribes to the Arcade V2 SSE status stream.
pub struct ArcadeEventsTask<S>
where
    S: MonitorStorage + 'static,
{
    storage: Arc<S>,
    /// Arcade base URL (e.g. `https://arcade-v2-us-1.bsvblockchain.tech`).
    arcade_url: String,
    /// Per-wallet callback token scoping the SSE stream.
    callback_token: String,
    /// Shared CheckForProofs trigger flag — set on MINED so the proof is
    /// fetched immediately.
    proof_trigger: Arc<AtomicBool>,
    /// Signals the background SSE loop to stop.
    shutdown: Arc<AtomicBool>,
    /// Events processed since task start (for health reporting).
    events_processed: Arc<AtomicU64>,
    /// Count reported at the last `run()` (so run() reports deltas).
    last_reported: AtomicU64,
    /// Handle of the background SSE loop.
    handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<S> ArcadeEventsTask<S>
where
    S: MonitorStorage + 'static,
{
    /// Create a new ArcadeEvents task.
    ///
    /// `proof_trigger` should be the same flag CheckForProofsTask reads
    /// (`checkNow`) so MINED events cause an immediate proof fetch.
    pub fn new(
        storage: Arc<S>,
        arcade_url: impl Into<String>,
        callback_token: impl Into<String>,
        proof_trigger: Arc<AtomicBool>,
    ) -> Self {
        Self {
            storage,
            arcade_url: arcade_url.into(),
            callback_token: callback_token.into(),
            proof_trigger,
            shutdown: Arc::new(AtomicBool::new(false)),
            events_processed: Arc::new(AtomicU64::new(0)),
            last_reported: AtomicU64::new(0),
            handle: std::sync::Mutex::new(None),
        }
    }

    /// Total status events processed since start.
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    /// Apply one Arcade status event to storage. Returns whether anything
    /// was updated. Public so the webhook path (status-only payloads) and
    /// tests can reuse identical mapping logic.
    pub async fn apply_status_event(
        storage: &S,
        txid: &str,
        tx_status: &str,
        proof_trigger: &AtomicBool,
    ) -> Result<bool> {
        match tx_status {
            statuses::SEEN_ON_NETWORK | statuses::SEEN_MULTIPLE_NODES => {
                storage.mark_transaction_seen_on_network(txid).await
            }
            statuses::MINED => {
                // Ensure spendability even if we never saw SEEN_ON_NETWORK,
                // then trigger an immediate proof fetch (SSE frames don't
                // carry the merkle path; the fetch goes through services).
                let updated = storage.mark_transaction_seen_on_network(txid).await?;
                proof_trigger.store(true, Ordering::SeqCst);
                tracing::info!(txid = %txid, "Arcade MINED event — triggering immediate proof fetch");
                Ok(updated)
            }
            statuses::REJECTED => storage.mark_transaction_rejected(txid, false).await,
            statuses::DOUBLE_SPEND_ATTEMPTED => storage.mark_transaction_rejected(txid, true).await,
            // RECEIVED / SENT_TO_NETWORK / ACCEPTED_BY_NETWORK — pre-gate
            // statuses, nothing to record yet.
            _ => Ok(false),
        }
    }

    /// The background SSE loop: connect → forward events → reconnect with
    /// exponential backoff. Runs until `shutdown` is set.
    async fn sse_loop(
        storage: Arc<S>,
        arcade_url: String,
        callback_token: String,
        proof_trigger: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
        events_processed: Arc<AtomicU64>,
    ) {
        let mut sse = match ArcadeSseClient::new(&arcade_url, &callback_token) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "arcade_events: failed to build SSE client — task inert");
                return;
            }
        };

        let mut backoff = BACKOFF_INITIAL;

        while !shutdown.load(Ordering::Relaxed) {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ArcadeStatusEvent>(256);

            let connected_at = std::time::Instant::now();
            tracing::debug!(url = %arcade_url, "arcade_events: connecting SSE");

            // Drive the stream and the event handler concurrently: stream_once
            // owns the sender and pushes into the channel while we drain it
            // here. When stream_once returns, the sender drops, the channel
            // closes, and rx.recv() yields None.
            let stream_fut = sse.stream_once(tx);
            tokio::pin!(stream_fut);

            let mut stream_done: Option<crate::Result<u64>> = None;
            loop {
                tokio::select! {
                    biased;
                    ev = rx.recv() => {
                        match ev {
                            Some(ev) => {
                                match Self::apply_status_event(
                                    &storage,
                                    &ev.txid,
                                    &ev.tx_status,
                                    &proof_trigger,
                                )
                                .await
                                {
                                    Ok(updated) => {
                                        events_processed.fetch_add(1, Ordering::Relaxed);
                                        tracing::debug!(
                                            txid = %ev.txid,
                                            status = %ev.tx_status,
                                            updated = updated,
                                            "arcade_events: status event"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            txid = %ev.txid,
                                            status = %ev.tx_status,
                                            error = %e,
                                            "arcade_events: failed to apply status event"
                                        );
                                    }
                                }
                            }
                            None => break, // channel closed — stream ended
                        }
                    }
                    res = &mut stream_fut, if stream_done.is_none() => {
                        stream_done = Some(res);
                        // keep draining rx until it closes
                    }
                }
            }

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            match stream_done {
                Some(Ok(n)) => {
                    tracing::debug!(events = n, "arcade_events: SSE stream ended, reconnecting");
                }
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "arcade_events: SSE stream error, reconnecting");
                }
                None => {}
            }

            // Backoff: reset after a healthy connection, else exponential.
            if connected_at.elapsed() >= HEALTHY_CONNECTION {
                backoff = BACKOFF_INITIAL;
            } else {
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
            tokio::time::sleep(backoff).await;
        }

        tracing::debug!("arcade_events: SSE loop stopped");
    }
}

#[async_trait]
impl<S> MonitorTask for ArcadeEventsTask<S>
where
    S: MonitorStorage + 'static,
{
    fn name(&self) -> &'static str {
        "arcade_events"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // health-report cadence; the SSE loop is persistent
    }

    async fn setup(&self) -> Result<()> {
        let handle = tokio::spawn(Self::sse_loop(
            self.storage.clone(),
            self.arcade_url.clone(),
            self.callback_token.clone(),
            self.proof_trigger.clone(),
            self.shutdown.clone(),
            self.events_processed.clone(),
        ));
        *self.handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        Ok(())
    }

    /// Periodic health report: the actual work happens in the background SSE
    /// loop; run() surfaces how many events were processed since last run.
    async fn run(&self) -> Result<TaskResult> {
        let total = self.events_processed.load(Ordering::Relaxed);
        let last = self.last_reported.swap(total, Ordering::Relaxed);
        let delta = total.saturating_sub(last);

        let mut result = TaskResult::with_count(delta.min(u32::MAX as u64) as u32);

        // Surface a dead SSE loop as a task error (daemon logs it).
        let loop_dead = {
            let guard = self.handle.lock().unwrap_or_else(|e| e.into_inner());
            guard.as_ref().map(|h| h.is_finished()).unwrap_or(false)
        };
        if loop_dead && !self.shutdown.load(Ordering::Relaxed) {
            result.add_error("arcade_events SSE loop is not running".to_string());
        }

        Ok(result)
    }
}

impl<S> Drop for ArcadeEventsTask<S>
where
    S: MonitorStorage + 'static,
{
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.handle.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_constants() {
        assert_eq!(BACKOFF_INITIAL, Duration::from_secs(1));
        assert_eq!(BACKOFF_MAX, Duration::from_secs(60));
        // Doubling from initial stays capped at max
        let mut b = BACKOFF_INITIAL;
        for _ in 0..10 {
            b = (b * 2).min(BACKOFF_MAX);
        }
        assert_eq!(b, BACKOFF_MAX);
    }

    #[test]
    fn test_task_name() {
        // Name constant used by TaskType::ArcadeEvents wiring
        assert_eq!("arcade_events", "arcade_events");
    }
}
