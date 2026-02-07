//! Monitor daemon - the main task scheduler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::services::WalletServices;
use crate::storage::MonitorStorage;
use crate::{Error, Result};

use super::config::{MonitorOptions, TaskConfig};
use super::tasks::{
    CheckForProofsTask, CheckNoSendsTask, ClockTask, FailAbandonedTask, MonitorTask,
    NewHeaderTask, PurgeTask, ReorgTask, ReviewStatusTask, SendWaitingTask, TaskResult, TaskType,
    UnfailTask,
};

/// The Monitor daemon schedules and runs background tasks for wallet maintenance.
///
/// It handles:
/// - Checking for merkle proofs for unconfirmed transactions
/// - Broadcasting pending transactions
/// - Failing abandoned transactions
/// - Recovering incorrectly failed transactions
/// - Tracking clock/minute boundaries
/// - Polling for new block headers
/// - Handling blockchain reorganizations
/// - Checking nosend transaction proofs
/// - Reviewing and synchronizing transaction status
/// - Purging expired data
/// - Monitoring service call history
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
        Self {
            storage,
            services,
            options,
            running: Arc::new(AtomicBool::new(false)),
            task_handles: RwLock::new(HashMap::new()),
        }
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
            let handle = self.spawn_task(
                TaskType::UnFail,
                Arc::new(task),
                &self.options.tasks.unfail,
            );
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

        // Note: MonitorCallHistory requires concrete Services type, not generic WalletServices.
        // It is not spawned here because the Monitor is generic over V: WalletServices.
        // Users who need this task should spawn it separately with a concrete Services instance.

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

        // Note: MonitorCallHistory requires concrete Services type.
        // It is skipped in run_once for the generic Monitor.

        Ok(results)
    }

    /// Spawn a task with the given configuration.
    ///
    /// The task is spawned as a tokio background task. Before the first run,
    /// the task's optional `setup()` method is called. Each run logs results
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

            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Run the task
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
                                // Try to log to storage for persistent error tracking.
                                // This is intentionally fire-and-forget (ok()) so that
                                // storage logging failures don't break the task loop.
                                // self.storage.log_monitor_event(task_name, error).await.ok();
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            task = task_name,
                            error = %e,
                            "Task failed"
                        );
                    }
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
        assert_eq!(
            opts.fail_abandoned_timeout,
            Duration::from_secs(5 * 60)
        );
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
        ];
        for tt in &task_types {
            assert!(!tt.as_str().is_empty());
        }
        assert_eq!(task_types.len(), 11);
    }
}
