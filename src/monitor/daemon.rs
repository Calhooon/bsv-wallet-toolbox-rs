//! Monitor daemon - the main task scheduler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::services::WalletServices;
use crate::storage::WalletStorageProvider;
use crate::{Error, Result};

use super::config::{MonitorOptions, TaskConfig};
use super::tasks::{
    CheckForProofsTask, FailAbandonedTask, MonitorTask, SendWaitingTask, TaskResult, TaskType,
    UnfailTask,
};

/// The Monitor daemon schedules and runs background tasks for wallet maintenance.
///
/// It handles:
/// - Checking for merkle proofs for unconfirmed transactions
/// - Broadcasting pending transactions
/// - Failing abandoned transactions
/// - Recovering incorrectly failed transactions
pub struct Monitor<S, V>
where
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    options: MonitorOptions,
    running: AtomicBool,
    task_handles: RwLock<HashMap<TaskType, JoinHandle<()>>>,
}

impl<S, V> Monitor<S, V>
where
    S: WalletStorageProvider + 'static,
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
            running: AtomicBool::new(false),
            task_handles: RwLock::new(HashMap::new()),
        }
    }

    /// Start the monitor daemon.
    ///
    /// This spawns background tasks for each enabled monitor task.
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(Error::StorageError("Monitor is already running".to_string()));
        }

        let mut handles = self.task_handles.write().await;

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

        Ok(results)
    }

    /// Spawn a task with the given configuration.
    fn spawn_task(
        &self,
        task_type: TaskType,
        task: Arc<dyn MonitorTask>,
        config: &TaskConfig,
    ) -> JoinHandle<()> {
        let interval = config.interval;
        let start_immediately = config.start_immediately;
        let running = self.running.load(Ordering::SeqCst);

        tokio::spawn(async move {
            // Wait for initial delay if not starting immediately
            if !start_immediately {
                tokio::time::sleep(interval).await;
            }

            loop {
                if !running {
                    break;
                }

                // Run the task
                match task.run().await {
                    Ok(result) => {
                        if result.items_processed > 0 {
                            tracing::info!(
                                task = task_type.as_str(),
                                processed = result.items_processed,
                                errors = result.errors.len(),
                                "Task completed"
                            );
                        }
                        if !result.errors.is_empty() {
                            for error in &result.errors {
                                tracing::warn!(
                                    task = task_type.as_str(),
                                    error = error.as_str(),
                                    "Task encountered error"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            task = task_type.as_str(),
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
    S: WalletStorageProvider + 'static,
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
        assert_eq!(opts.fail_abandoned_timeout, Duration::from_secs(24 * 60 * 60));
    }
}
