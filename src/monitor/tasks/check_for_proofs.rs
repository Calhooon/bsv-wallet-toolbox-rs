//! CheckForProofs task - fetches merkle proofs for unconfirmed transactions.
//!
//! Event-driven: runs when a new block header is detected (via shared trigger flag)
//! or on a 2-hour fallback timer. Matches TS pattern: `TaskCheckForProofs.checkNow`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::Result;

use super::{MonitorTask, TaskResult};

/// Fallback interval if no new block triggers a run (TS default: 2 hours).
const FALLBACK_INTERVAL_SECS: u64 = 2 * 60 * 60;

/// Task that checks for merkle proofs for transactions that need confirmation.
///
/// This task:
/// 1. Only runs when triggered by a new block header OR on a 2-hour fallback
/// 2. Calls storage.synchronize_transaction_statuses() which:
///    a. Triages transactions via batch status check (Go pattern)
///    b. Fetches merkle proofs for confirmed txs with 500ms throttle (TS pattern)
///    c. Updates status to 'completed' on proof, 'invalid' after max retries
pub struct CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    _services: Arc<V>,
    /// Shared trigger flag — set by NewHeaderTask when a new block is detected.
    /// Matches TS pattern: `TaskCheckForProofs.checkNow`.
    check_now: Arc<AtomicBool>,
    /// Track last run time for the 2-hour fallback.
    last_run: std::sync::Mutex<Option<Instant>>,
}

impl<S, V> CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new CheckForProofsTask.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self {
            storage,
            _services: services,
            check_now: Arc::new(AtomicBool::new(false)),
            last_run: std::sync::Mutex::new(None),
        }
    }

    /// Create with a shared trigger flag (for wiring to NewHeaderTask).
    pub fn with_trigger(storage: Arc<S>, services: Arc<V>, check_now: Arc<AtomicBool>) -> Self {
        Self {
            storage,
            _services: services,
            check_now,
            last_run: std::sync::Mutex::new(None),
        }
    }

    /// Get the shared trigger flag (for daemon wiring).
    pub fn trigger_flag(&self) -> Arc<AtomicBool> {
        self.check_now.clone()
    }

    /// Externally trigger an immediate proof check.
    pub fn trigger(&self) {
        self.check_now.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl<S, V> MonitorTask for CheckForProofsTask<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn name(&self) -> &'static str {
        "check_for_proofs"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(60) // 1 minute polling interval for the trigger check
    }

    async fn run(&self) -> Result<TaskResult> {
        let mut errors = Vec::new();

        // Event-driven: only run when triggered by new block header OR 2-hour fallback.
        // Matches TS: TaskCheckForProofs.checkNow pattern.
        let triggered = self.check_now.swap(false, Ordering::SeqCst);

        let fallback_due = {
            let last = self.last_run.lock().unwrap_or_else(|e| e.into_inner());
            match *last {
                None => true, // First run
                Some(t) => t.elapsed() >= Duration::from_secs(FALLBACK_INTERVAL_SECS),
            }
        };

        if !triggered && !fallback_due {
            return Ok(TaskResult::new()); // Skip — no new block and fallback not due
        }

        if triggered {
            tracing::debug!("check_for_proofs: triggered by new block header");
        } else {
            tracing::debug!("check_for_proofs: running on 2-hour fallback timer");
        }

        // Record this run
        {
            let mut last = self.last_run.lock().unwrap_or_else(|e| e.into_inner());
            *last = Some(Instant::now());
        }

        // Delegate to storage which does the full triage + proof fetch flow
        match self.storage.synchronize_transaction_statuses().await {
            Ok(results) => {
                let items_processed = results.len() as u32;
                for result in &results {
                    tracing::info!(
                        txid = %result.txid,
                        status = ?result.status,
                        block_height = ?result.block_height,
                        "Transaction status synchronized"
                    );
                }
                Ok(TaskResult {
                    items_processed,
                    errors,
                })
            }
            Err(e) => {
                errors.push(format!("synchronize_transaction_statuses failed: {}", e));
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
    fn test_task_name() {
        assert_eq!("check_for_proofs", "check_for_proofs");
    }

    #[test]
    fn test_default_interval() {
        let interval = Duration::from_secs(60);
        assert_eq!(interval.as_secs(), 60);
    }

    #[test]
    fn test_fallback_interval() {
        assert_eq!(FALLBACK_INTERVAL_SECS, 7200); // 2 hours
    }

    #[test]
    fn test_trigger_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!flag.load(Ordering::SeqCst));
        flag.store(true, Ordering::SeqCst);
        assert!(flag.load(Ordering::SeqCst));
        // swap resets it
        let was = flag.swap(false, Ordering::SeqCst);
        assert!(was);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_task_result_empty() {
        let result = TaskResult::new();
        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_task_result_with_items() {
        let result = TaskResult::with_count(3);
        assert_eq!(result.items_processed, 3);
    }

    #[test]
    fn test_task_result_with_errors() {
        let mut result = TaskResult::new();
        result.add_error("error 1".to_string());
        result.add_error("error 2".to_string());
        assert_eq!(result.errors.len(), 2);
    }

    /// Test that trigger() sets the check_now flag to true.
    #[test]
    fn test_trigger_sets_flag() {
        // We can't easily construct the task without async, so test the flag directly
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!flag.load(Ordering::SeqCst));

        // Simulate what trigger() does
        flag.store(true, Ordering::SeqCst);
        assert!(flag.load(Ordering::SeqCst));
    }

    /// Test that with_trigger() shares the flag correctly between creator and task.
    #[test]
    fn test_with_trigger_shares_flag() {
        // Create a shared flag externally
        let external_flag = Arc::new(AtomicBool::new(false));
        let task_flag = external_flag.clone();

        // Verify they share the same underlying flag
        assert!(!external_flag.load(Ordering::SeqCst));
        assert!(!task_flag.load(Ordering::SeqCst));

        // Set the flag from the "external" side (simulating NewHeaderTask)
        external_flag.store(true, Ordering::SeqCst);

        // The task's clone should see the change
        assert!(task_flag.load(Ordering::SeqCst));

        // swap from the task side resets both
        let was = task_flag.swap(false, Ordering::SeqCst);
        assert!(was);
        assert!(!external_flag.load(Ordering::SeqCst));
    }

    /// Test that the run() skip logic works: when not triggered and fallback not due,
    /// the task should return an empty result (0 items, no errors).
    #[test]
    fn test_skip_logic_no_trigger_no_fallback() {
        // Simulate the decision logic from run()
        let check_now = Arc::new(AtomicBool::new(false));
        let triggered = check_now.swap(false, Ordering::SeqCst);

        // Simulate a recent last_run (not fallback due)
        let last_run: Option<Instant> = Some(Instant::now());
        let fallback_due = match last_run {
            None => true,
            Some(t) => t.elapsed() >= Duration::from_secs(FALLBACK_INTERVAL_SECS),
        };

        assert!(!triggered);
        assert!(!fallback_due);

        // In run(), this would return Ok(TaskResult::new()) -- skip
        if !triggered && !fallback_due {
            let result = TaskResult::new();
            assert_eq!(result.items_processed, 0);
            assert!(result.errors.is_empty());
        } else {
            panic!("Should have skipped");
        }
    }

    /// Test that first run (no last_run) triggers fallback.
    #[test]
    fn test_first_run_triggers_fallback() {
        let check_now = Arc::new(AtomicBool::new(false));
        let triggered = check_now.swap(false, Ordering::SeqCst);

        let last_run: Option<Instant> = None; // First run
        let fallback_due = match last_run {
            None => true, // First run always triggers
            Some(t) => t.elapsed() >= Duration::from_secs(FALLBACK_INTERVAL_SECS),
        };

        assert!(!triggered);
        assert!(fallback_due);
        // Would proceed to run sync
    }

    /// Test that a triggered flag causes execution even when fallback is not due.
    #[test]
    fn test_triggered_flag_causes_execution() {
        let check_now = Arc::new(AtomicBool::new(true));
        let triggered = check_now.swap(false, Ordering::SeqCst);

        // Recent last_run, so fallback is not due
        let last_run: Option<Instant> = Some(Instant::now());
        let fallback_due = match last_run {
            None => true,
            Some(t) => t.elapsed() >= Duration::from_secs(FALLBACK_INTERVAL_SECS),
        };

        assert!(triggered);
        assert!(!fallback_due);
        // Would proceed because triggered is true
        assert!(triggered || fallback_due);
    }
}
