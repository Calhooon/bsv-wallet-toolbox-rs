//! Monitor tasks.
//!
//! Each task implements the `MonitorTask` trait and performs a specific
//! background operation on the wallet storage.

mod check_for_proofs;
mod check_no_sends;
mod clock;
mod fail_abandoned;
mod monitor_call_history;
mod new_header;
mod purge;
mod reorg;
mod review_status;
mod send_waiting;
mod unfail;

pub use check_for_proofs::CheckForProofsTask;
pub use check_no_sends::CheckNoSendsTask;
pub use clock::ClockTask;
pub use fail_abandoned::FailAbandonedTask;
pub use monitor_call_history::MonitorCallHistoryTask;
pub use new_header::NewHeaderTask;
pub use purge::{PurgeConfig, PurgeTask};
pub use reorg::{DeactivatedHeader, ReorgTask};
pub use review_status::ReviewStatusTask;
pub use send_waiting::SendWaitingTask;
pub use unfail::UnfailTask;

use async_trait::async_trait;
use std::time::Duration;

use crate::Result;

/// Result of running a monitor task.
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    /// Number of items processed.
    pub items_processed: u32,
    /// List of errors encountered (non-fatal).
    pub errors: Vec<String>,
}

impl TaskResult {
    /// Create a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a result with processed count.
    pub fn with_count(count: u32) -> Self {
        Self {
            items_processed: count,
            errors: Vec::new(),
        }
    }

    /// Add an error to the result.
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }
}

/// Trait for monitor tasks.
#[async_trait]
pub trait MonitorTask: Send + Sync {
    /// Get the task name.
    fn name(&self) -> &'static str;

    /// Get the default interval for this task.
    fn default_interval(&self) -> Duration;

    /// Run the task once.
    async fn run(&self) -> Result<TaskResult>;
}

/// Task type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
    /// Check for merkle proofs.
    CheckForProofs,
    /// Send waiting transactions.
    SendWaiting,
    /// Fail abandoned transactions.
    FailAbandoned,
    /// UnFail transactions.
    UnFail,
    /// Clock tick (minute boundaries).
    Clock,
    /// Check for 'nosend' transaction proofs.
    CheckNoSends,
    /// Monitor service call history.
    MonitorCallHistory,
    /// Poll for new block headers.
    NewHeader,
    /// Purge old/expired data.
    Purge,
    /// Handle blockchain reorganizations.
    Reorg,
    /// Review and sync transaction status.
    ReviewStatus,
}

impl TaskType {
    /// Get the task name as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskType::CheckForProofs => "check_for_proofs",
            TaskType::SendWaiting => "send_waiting",
            TaskType::FailAbandoned => "fail_abandoned",
            TaskType::UnFail => "unfail",
            TaskType::Clock => "clock",
            TaskType::CheckNoSends => "check_no_sends",
            TaskType::MonitorCallHistory => "monitor_call_history",
            TaskType::NewHeader => "new_header",
            TaskType::Purge => "purge",
            TaskType::Reorg => "reorg",
            TaskType::ReviewStatus => "review_status",
        }
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_result_new() {
        let result = TaskResult::new();
        assert_eq!(result.items_processed, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_task_result_with_count() {
        let result = TaskResult::with_count(5);
        assert_eq!(result.items_processed, 5);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_task_result_add_error() {
        let mut result = TaskResult::new();
        result.add_error("test error".to_string());
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0], "test error");
    }

    #[test]
    fn test_task_type_as_str() {
        assert_eq!(TaskType::CheckForProofs.as_str(), "check_for_proofs");
        assert_eq!(TaskType::SendWaiting.as_str(), "send_waiting");
        assert_eq!(TaskType::FailAbandoned.as_str(), "fail_abandoned");
        assert_eq!(TaskType::UnFail.as_str(), "unfail");
        assert_eq!(TaskType::Clock.as_str(), "clock");
        assert_eq!(TaskType::CheckNoSends.as_str(), "check_no_sends");
        assert_eq!(TaskType::MonitorCallHistory.as_str(), "monitor_call_history");
        assert_eq!(TaskType::NewHeader.as_str(), "new_header");
        assert_eq!(TaskType::Purge.as_str(), "purge");
        assert_eq!(TaskType::Reorg.as_str(), "reorg");
        assert_eq!(TaskType::ReviewStatus.as_str(), "review_status");
    }
}
