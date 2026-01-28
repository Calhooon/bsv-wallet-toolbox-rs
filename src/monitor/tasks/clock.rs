//! Clock task - tracks minute-level clock events.
//!
//! This task runs every second to check if the next minute boundary has been reached.
//! Primarily used for scheduling and logging periodic events at minute granularity.

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::{MonitorTask, TaskResult};
use crate::Result;

/// Task that tracks minute-level clock events.
///
/// Runs every second to check if a new minute has started.
/// When a minute boundary is crossed, it triggers minute-level events.
pub struct ClockTask {
    /// Last recorded minute (minutes since epoch).
    last_minute: AtomicU64,
}

impl ClockTask {
    /// Create a new clock task.
    pub fn new() -> Self {
        Self {
            last_minute: AtomicU64::new(0),
        }
    }

    /// Get the current minute since Unix epoch.
    fn current_minute() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        now.as_secs() / 60
    }
}

impl Default for ClockTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MonitorTask for ClockTask {
    fn name(&self) -> &'static str {
        "clock"
    }

    fn default_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn run(&self) -> Result<TaskResult> {
        let current = Self::current_minute();
        let last = self.last_minute.load(Ordering::Relaxed);

        if last == 0 {
            // First run, just record the current minute
            self.last_minute.store(current, Ordering::Relaxed);
            return Ok(TaskResult::new());
        }

        if current > last {
            // Minute boundary crossed
            let minutes_elapsed = current - last;
            self.last_minute.store(current, Ordering::Relaxed);

            tracing::debug!(
                task = "clock",
                minutes_elapsed = minutes_elapsed,
                current_minute = current,
                "Minute boundary crossed"
            );

            Ok(TaskResult::with_count(minutes_elapsed as u32))
        } else {
            Ok(TaskResult::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_clock_task_first_run() {
        let task = ClockTask::new();
        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert!(task.last_minute.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_clock_task_same_minute() {
        let task = ClockTask::new();
        // First run
        task.run().await.unwrap();
        // Immediate second run should show no new minutes
        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0);
    }

    #[test]
    fn test_clock_task_default() {
        let task = ClockTask::default();
        assert_eq!(task.name(), "clock");
        assert_eq!(task.default_interval(), Duration::from_secs(1));
    }
}
