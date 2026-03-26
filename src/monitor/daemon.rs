//! Monitor daemon - the main task scheduler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::{Error, Result};

use super::config::{MonitorOptions, TaskConfig};
use super::tasks::{
    CheckForProofsTask, CheckNoSendsTask, ClockTask, CompactBeefTask, FailAbandonedTask,
    MonitorCallHistoryTask, MonitorTask, NewHeaderTask, PurgeTask, ReorgTask, ReviewStatusTask,
    SendWaitingTask, SyncWhenIdleTask, TaskResult, TaskType, UnfailTask,
};

/// Generate random bytes using the `rand` crate's thread-local CSPRNG.
fn rand_bytes(buf: &mut [u8]) {
    use rand::RngCore;
    rand::thread_rng().fill_bytes(buf);
}

/// Health status for an individual monitor task.
#[derive(Debug, Clone, Default)]
pub struct TaskHealth {
    /// When the task last completed a run (success or failure).
    pub last_run: Option<Instant>,
    /// The result of the last successful run, or `None` if the task has never succeeded.
    pub last_result: Option<TaskResult>,
    /// The error message from the last failed run, if any.
    pub last_error: Option<String>,
    /// Number of consecutive fatal errors (resets on success).
    pub consecutive_errors: u32,
}

/// Aggregate health status for the entire monitor daemon.
#[derive(Debug, Clone)]
pub struct MonitorHealth {
    /// Whether the monitor daemon is currently running.
    pub running: bool,
    /// Number of spawned tasks.
    pub task_count: usize,
    /// Per-task health status.
    pub tasks: HashMap<TaskType, TaskHealth>,
}

impl MonitorHealth {
    /// Returns `true` if all tasks have run at least once without consecutive errors.
    pub fn all_tasks_healthy(&self) -> bool {
        self.tasks
            .values()
            .all(|h| h.consecutive_errors == 0 && h.last_run.is_some())
    }
}

/// Background task scheduler for wallet transaction lifecycle management.
///
/// The `Monitor` daemon spawns and manages recurring background tasks using `tokio`.
/// It handles the full transaction lifecycle from broadcasting through proof
/// verification, with automatic cleanup and recovery.
///
/// # Tasks
///
/// | Task | Default Interval | Purpose |
/// |------|-----------------|--------|
/// | `clock` | 1 second | Track minute boundaries for scheduling |
/// | `check_for_proofs` | 1 minute | Fetch merkle proofs for unconfirmed transactions |
/// | `new_header` | 1 minute | Poll for new block headers |
/// | `reorg` | 1 minute | Handle blockchain reorganizations |
/// | `send_waiting` | 5 minutes | Broadcast pending transactions |
/// | `fail_abandoned` | 5 minutes | Mark stale transactions as failed |
/// | `unfail` | 10 minutes | Recover incorrectly failed transactions |
/// | `monitor_call_history` | 12 minutes | Log service call diagnostics |
/// | `review_status` | 15 minutes | Synchronize transaction and proof status |
/// | `purge` | 1 hour | Delete expired data |
/// | `check_no_sends` | 24 hours | Check for externally mined nosend transactions |
/// | `sync_when_idle` | 1 minute | Synchronize storage when wallet is idle |
///
/// # Type Parameters
///
/// - `S`: Storage backend implementing [`MonitorStorage`]
/// - `V`: Services backend implementing [`WalletServices`]
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::monitor::{Monitor, MonitorOptions, TaskConfig};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// let monitor = Monitor::new(Arc::new(storage), Arc::new(services));
///
/// // Start all enabled background tasks
/// monitor.start().await?;
///
/// // Or run all tasks once (useful for testing)
/// let results = monitor.run_once().await?;
///
/// // Stop when done
/// monitor.stop().await?;
/// ```
pub struct Monitor<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    options: MonitorOptions,
    running: Arc<AtomicBool>,
    task_handles: RwLock<HashMap<TaskType, JoinHandle<()>>>,
    /// Per-task health tracking, updated after each task run.
    task_health: Arc<RwLock<HashMap<TaskType, TaskHealth>>>,
    /// Unique identifier for this monitor instance, used for distributed task locking.
    instance_id: String,
}

impl<S, V> Monitor<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    /// Create a new Monitor with the given storage and services.
    pub fn new(storage: Arc<S>, services: Arc<V>) -> Self {
        Self::with_options(storage, services, MonitorOptions::default())
    }

    /// Create a new Monitor with custom options.
    pub fn with_options(storage: Arc<S>, services: Arc<V>, options: MonitorOptions) -> Self {
        // Generate a random 16-byte hex string as instance ID
        let mut bytes = [0u8; 16];
        rand_bytes(&mut bytes);
        let instance_id = hex::encode(bytes);

        Self {
            storage,
            services,
            options,
            running: Arc::new(AtomicBool::new(false)),
            task_handles: RwLock::new(HashMap::new()),
            task_health: Arc::new(RwLock::new(HashMap::new())),
            instance_id,
        }
    }

    /// Get this monitor instance's unique identifier.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Start the monitor daemon.
    ///
    /// This spawns background tasks for each enabled monitor task.
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(Error::StorageError(
                "Monitor is already running".to_string(),
            ));
        }

        // Set services on storage so storage-based operations (synchronize_transaction_statuses,
        // send_waiting_transactions, etc.) can access blockchain services.
        self.storage.set_services(self.services.clone());

        let mut handles = self.task_handles.write().await;

        // Start clock task
        if self.options.tasks.clock.enabled {
            let task = ClockTask::new();
            let handle =
                self.spawn_task(TaskType::Clock, Arc::new(task), &self.options.tasks.clock);
            handles.insert(TaskType::Clock, handle);
        }

        // Start new_header task
        if self.options.tasks.new_header.enabled {
            let task = NewHeaderTask::new(self.services.clone());
            let handle = self.spawn_task(
                TaskType::NewHeader,
                Arc::new(task),
                &self.options.tasks.new_header,
            );
            handles.insert(TaskType::NewHeader, handle);
        }

        // Start reorg task
        if self.options.tasks.reorg.enabled {
            let task = ReorgTask::new(self.storage.clone(), self.services.clone());
            let handle =
                self.spawn_task(TaskType::Reorg, Arc::new(task), &self.options.tasks.reorg);
            handles.insert(TaskType::Reorg, handle);
        }

        // Start check_for_proofs task
        if self.options.tasks.check_for_proofs.enabled {
            let task = CheckForProofsTask::new(self.storage.clone(), self.services.clone());
            let handle = self.spawn_task(
                TaskType::CheckForProofs,
                Arc::new(task),
                &self.options.tasks.check_for_proofs,
            );
            handles.insert(TaskType::CheckForProofs, handle);
        }

        // Start send_waiting task
        if self.options.tasks.send_waiting.enabled {
            let task = SendWaitingTask::new(self.storage.clone(), self.services.clone());
            let handle = self.spawn_task(
                TaskType::SendWaiting,
                Arc::new(task),
                &self.options.tasks.send_waiting,
            );
            handles.insert(TaskType::SendWaiting, handle);
        }

        // Start fail_abandoned task
        if self.options.tasks.fail_abandoned.enabled {
            let task =
                FailAbandonedTask::new(self.storage.clone(), self.options.fail_abandoned_timeout);
            let handle = self.spawn_task(
                TaskType::FailAbandoned,
                Arc::new(task),
                &self.options.tasks.fail_abandoned,
            );
            handles.insert(TaskType::FailAbandoned, handle);
        }

        // Start unfail task
        if self.options.tasks.unfail.enabled {
            let task = UnfailTask::new(self.storage.clone(), self.services.clone());
            let handle =
                self.spawn_task(TaskType::UnFail, Arc::new(task), &self.options.tasks.unfail);
            handles.insert(TaskType::UnFail, handle);
        }

        // Start check_no_sends task
        if self.options.tasks.check_no_sends.enabled {
            let task = CheckNoSendsTask::new(self.storage.clone(), self.services.clone());
            let handle = self.spawn_task(
                TaskType::CheckNoSends,
                Arc::new(task),
                &self.options.tasks.check_no_sends,
            );
            handles.insert(TaskType::CheckNoSends, handle);
        }

        // Start review_status task
        if self.options.tasks.review_status.enabled {
            let task = ReviewStatusTask::new(self.storage.clone());
            let handle = self.spawn_task(
                TaskType::ReviewStatus,
                Arc::new(task),
                &self.options.tasks.review_status,
            );
            handles.insert(TaskType::ReviewStatus, handle);
        }

        // Start purge task
        if self.options.tasks.purge.enabled {
            let task = PurgeTask::new(self.storage.clone());
            let handle =
                self.spawn_task(TaskType::Purge, Arc::new(task), &self.options.tasks.purge);
            handles.insert(TaskType::Purge, handle);
        }

        // Start compact_beef task
        if self.options.tasks.compact_beef.enabled {
            let task = CompactBeefTask::new(self.storage.clone());
            let handle = self.spawn_task(
                TaskType::CompactBeef,
                Arc::new(task),
                &self.options.tasks.compact_beef,
            );
            handles.insert(TaskType::CompactBeef, handle);
        }

        // Start sync_when_idle task
        if self.options.tasks.sync_when_idle.enabled {
            let task = SyncWhenIdleTask::new();
            let handle = self.spawn_task(
                TaskType::SyncWhenIdle,
                Arc::new(task),
                &self.options.tasks.sync_when_idle,
            );
            handles.insert(TaskType::SyncWhenIdle, handle);
        }

        // Start monitor_call_history task
        if self.options.tasks.monitor_call_history.enabled {
            let task = MonitorCallHistoryTask::new(self.services.clone());
            let handle = self.spawn_task(
                TaskType::MonitorCallHistory,
                Arc::new(task),
                &self.options.tasks.monitor_call_history,
            );
            handles.insert(TaskType::MonitorCallHistory, handle);
        }

        Ok(())
    }

    /// Stop the monitor daemon.
    ///
    /// This cancels all running background tasks.
    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(()); // Already stopped
        }

        let mut handles = self.task_handles.write().await;
        for (_, handle) in handles.drain() {
            handle.abort();
        }

        Ok(())
    }

    /// Check if the monitor is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Returns a snapshot of the monitor's health status.
    ///
    /// Includes per-task health information such as last run time,
    /// last result, and consecutive error counts.
    pub async fn health(&self) -> MonitorHealth {
        let tasks = self.task_health.read().await.clone();
        let task_count = self.task_handles.read().await.len();
        MonitorHealth {
            running: self.is_running(),
            task_count,
            tasks,
        }
    }

    /// Run all enabled tasks once (for testing).
    pub async fn run_once(&self) -> Result<HashMap<TaskType, TaskResult>> {
        let mut results = HashMap::new();

        if self.options.tasks.clock.enabled {
            let task = ClockTask::new();
            let result = task.run().await?;
            results.insert(TaskType::Clock, result);
        }

        if self.options.tasks.new_header.enabled {
            let task = NewHeaderTask::new(self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::NewHeader, result);
        }

        if self.options.tasks.reorg.enabled {
            let task = ReorgTask::new(self.storage.clone(), self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::Reorg, result);
        }

        if self.options.tasks.check_for_proofs.enabled {
            let task = CheckForProofsTask::new(self.storage.clone(), self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::CheckForProofs, result);
        }

        if self.options.tasks.send_waiting.enabled {
            let task = SendWaitingTask::new(self.storage.clone(), self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::SendWaiting, result);
        }

        if self.options.tasks.fail_abandoned.enabled {
            let task =
                FailAbandonedTask::new(self.storage.clone(), self.options.fail_abandoned_timeout);
            let result = task.run().await?;
            results.insert(TaskType::FailAbandoned, result);
        }

        if self.options.tasks.unfail.enabled {
            let task = UnfailTask::new(self.storage.clone(), self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::UnFail, result);
        }

        if self.options.tasks.check_no_sends.enabled {
            let task = CheckNoSendsTask::new(self.storage.clone(), self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::CheckNoSends, result);
        }

        if self.options.tasks.review_status.enabled {
            let task = ReviewStatusTask::new(self.storage.clone());
            let result = task.run().await?;
            results.insert(TaskType::ReviewStatus, result);
        }

        if self.options.tasks.purge.enabled {
            let task = PurgeTask::new(self.storage.clone());
            let result = task.run().await?;
            results.insert(TaskType::Purge, result);
        }

        if self.options.tasks.compact_beef.enabled {
            let task = CompactBeefTask::new(self.storage.clone());
            let result = task.run().await?;
            results.insert(TaskType::CompactBeef, result);
        }

        if self.options.tasks.sync_when_idle.enabled {
            let task = SyncWhenIdleTask::new();
            let result = task.run().await?;
            results.insert(TaskType::SyncWhenIdle, result);
        }

        if self.options.tasks.monitor_call_history.enabled {
            let task = MonitorCallHistoryTask::new(self.services.clone());
            let result = task.run().await?;
            results.insert(TaskType::MonitorCallHistory, result);
        }

        Ok(results)
    }

    /// Spawn a task with the given configuration.
    ///
    /// The task is spawned as a tokio background task. Before the first run,
    /// the task's optional `setup()` method is called. Each run acquires a
    /// distributed task lock (for multi-instance support) and logs results
    /// and any non-fatal errors encountered during execution.
    fn spawn_task(
        &self,
        task_type: TaskType,
        task: Arc<dyn MonitorTask>,
        config: &TaskConfig,
    ) -> JoinHandle<()> {
        let interval = config.interval;
        let start_immediately = config.start_immediately;
        let running = self.running.clone();
        let task_name = task_type.as_str();
        let storage = self.storage.clone();
        let instance_id = self.instance_id.clone();
        let health_map = self.task_health.clone();

        tokio::spawn(async move {
            // Run optional async setup phase before first run
            if let Err(e) = task.setup().await {
                tracing::error!(
                    task = task_name,
                    error = %e,
                    "Task setup failed"
                );
                return;
            }

            // Wait for initial delay if not starting immediately
            if !start_immediately {
                tokio::time::sleep(interval).await;
            }

            // Use 2x interval as TTL so locks expire if an instance crashes
            let lock_ttl = interval * 2;

            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Try to acquire the distributed task lock
                let acquired = match storage
                    .try_acquire_task_lock(task_name, &instance_id, lock_ttl)
                    .await
                {
                    Ok(acquired) => acquired,
                    Err(e) => {
                        tracing::warn!(
                            task = task_name,
                            error = %e,
                            "Failed to acquire task lock, skipping run"
                        );
                        tokio::time::sleep(interval).await;
                        continue;
                    }
                };

                if !acquired {
                    tracing::debug!(
                        task = task_name,
                        "Task lock held by another instance, skipping"
                    );
                    tokio::time::sleep(interval).await;
                    continue;
                }

                // Run the task and update health tracking
                match task.run().await {
                    Ok(result) => {
                        if result.items_processed > 0 {
                            tracing::info!(
                                task = task_name,
                                processed = result.items_processed,
                                errors = result.errors.len(),
                                "Task completed"
                            );
                        }
                        // Persistent error logging for non-fatal task errors
                        if !result.errors.is_empty() {
                            for error in &result.errors {
                                tracing::warn!(
                                    task = task_name,
                                    error = error.as_str(),
                                    "Task error"
                                );
                            }
                        }
                        // Record successful run in health state
                        let mut health = health_map.write().await;
                        let entry = health.entry(task_type).or_default();
                        entry.last_run = Some(Instant::now());
                        entry.last_result = Some(result);
                        entry.last_error = None;
                        entry.consecutive_errors = 0;
                    }
                    Err(e) => {
                        tracing::error!(
                            task = task_name,
                            error = %e,
                            "Task failed"
                        );
                        // Record failure in health state
                        let mut health = health_map.write().await;
                        let entry = health.entry(task_type).or_default();
                        entry.last_run = Some(Instant::now());
                        entry.last_error = Some(e.to_string());
                        entry.consecutive_errors += 1;
                    }
                }

                // Release the lock after task completes
                if let Err(e) = storage.release_task_lock(task_name, &instance_id).await {
                    tracing::warn!(
                        task = task_name,
                        error = %e,
                        "Failed to release task lock"
                    );
                }

                // Wait for next interval
                tokio::time::sleep(interval).await;
            }
        })
    }
}

impl<S, V> Drop for Monitor<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_monitor_options_default() {
        let opts = MonitorOptions::default();
        assert!(opts.tasks.check_for_proofs.enabled);
        assert!(opts.tasks.clock.enabled);
        assert!(opts.tasks.new_header.enabled);
        assert!(opts.tasks.reorg.enabled);
        assert!(opts.tasks.check_no_sends.enabled);
        assert!(opts.tasks.review_status.enabled);
        assert!(opts.tasks.purge.enabled);
        assert!(opts.tasks.monitor_call_history.enabled);
        assert!(opts.tasks.sync_when_idle.enabled);
        assert_eq!(opts.fail_abandoned_timeout, Duration::from_secs(5 * 60));
    }

    #[test]
    fn test_all_task_types_have_names() {
        let task_types = [
            TaskType::CheckForProofs,
            TaskType::SendWaiting,
            TaskType::FailAbandoned,
            TaskType::UnFail,
            TaskType::Clock,
            TaskType::CheckNoSends,
            TaskType::MonitorCallHistory,
            TaskType::NewHeader,
            TaskType::Purge,
            TaskType::Reorg,
            TaskType::ReviewStatus,
            TaskType::SyncWhenIdle,
        ];
        for tt in &task_types {
            assert!(!tt.as_str().is_empty());
        }
        assert_eq!(task_types.len(), 12);
    }

    #[test]
    fn test_monitor_instance_id_is_unique() {
        // Two calls to rand_bytes should produce different values
        let id1 = {
            let mut bytes = [0u8; 16];
            rand_bytes(&mut bytes);
            hex::encode(bytes)
        };
        let id2 = {
            let mut bytes = [0u8; 16];
            rand_bytes(&mut bytes);
            hex::encode(bytes)
        };
        assert_ne!(id1, id2);
        assert_eq!(id1.len(), 32); // 16 bytes = 32 hex chars
    }

    #[test]
    fn test_task_health_default() {
        let health = TaskHealth::default();
        assert!(health.last_run.is_none());
        assert!(health.last_result.is_none());
        assert!(health.last_error.is_none());
        assert_eq!(health.consecutive_errors, 0);
    }

    #[test]
    fn test_monitor_health_all_tasks_healthy() {
        // Empty monitor is considered healthy (no tasks to be unhealthy)
        let health = MonitorHealth {
            running: true,
            task_count: 0,
            tasks: HashMap::new(),
        };
        assert!(health.all_tasks_healthy());

        // Task with successful run is healthy
        let mut tasks = HashMap::new();
        tasks.insert(
            TaskType::Clock,
            TaskHealth {
                last_run: Some(Instant::now()),
                last_result: Some(TaskResult::new()),
                last_error: None,
                consecutive_errors: 0,
            },
        );
        let health = MonitorHealth {
            running: true,
            task_count: 1,
            tasks,
        };
        assert!(health.all_tasks_healthy());

        // Task with consecutive errors is unhealthy
        let mut tasks = HashMap::new();
        tasks.insert(
            TaskType::Clock,
            TaskHealth {
                last_run: Some(Instant::now()),
                last_result: None,
                last_error: Some("test error".to_string()),
                consecutive_errors: 3,
            },
        );
        let health = MonitorHealth {
            running: true,
            task_count: 1,
            tasks,
        };
        assert!(!health.all_tasks_healthy());

        // Task that has never run is unhealthy
        let mut tasks = HashMap::new();
        tasks.insert(TaskType::Clock, TaskHealth::default());
        let health = MonitorHealth {
            running: true,
            task_count: 1,
            tasks,
        };
        assert!(!health.all_tasks_healthy());
    }
}
