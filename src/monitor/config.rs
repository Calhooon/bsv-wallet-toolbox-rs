//! Monitor configuration types.

use std::time::Duration;

/// Configuration options for the Monitor daemon.
#[derive(Debug, Clone)]
pub struct MonitorOptions {
    /// Configuration for each task.
    pub tasks: TasksConfig,
    /// Minimum age for transactions to be considered abandoned.
    pub fail_abandoned_timeout: Duration,
}

impl Default for MonitorOptions {
    fn default() -> Self {
        Self {
            tasks: TasksConfig::default(),
            fail_abandoned_timeout: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

/// Configuration for all monitor tasks.
#[derive(Debug, Clone)]
pub struct TasksConfig {
    /// Check for proofs task configuration.
    pub check_for_proofs: TaskConfig,
    /// Send waiting transactions task configuration.
    pub send_waiting: TaskConfig,
    /// Fail abandoned transactions task configuration.
    pub fail_abandoned: TaskConfig,
    /// UnFail transactions task configuration.
    pub unfail: TaskConfig,
}

impl Default for TasksConfig {
    fn default() -> Self {
        Self {
            check_for_proofs: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(60), // 1 minute
                start_immediately: false,
            },
            send_waiting: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(5 * 60), // 5 minutes
                start_immediately: true,
            },
            fail_abandoned: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(5 * 60), // 5 minutes
                start_immediately: false,
            },
            unfail: TaskConfig {
                enabled: true,
                interval: Duration::from_secs(10 * 60), // 10 minutes
                start_immediately: false,
            },
        }
    }
}

/// Configuration for a single monitor task.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Whether this task is enabled.
    pub enabled: bool,
    /// How often to run this task.
    pub interval: Duration,
    /// Whether to run immediately on start.
    pub start_immediately: bool,
}

impl TaskConfig {
    /// Create a new enabled task config with the given interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            enabled: true,
            interval,
            start_immediately: false,
        }
    }

    /// Create a disabled task config.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(60),
            start_immediately: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = MonitorOptions::default();
        assert!(opts.tasks.check_for_proofs.enabled);
        assert!(opts.tasks.send_waiting.enabled);
        assert!(opts.tasks.fail_abandoned.enabled);
        assert!(opts.tasks.unfail.enabled);
        assert_eq!(opts.fail_abandoned_timeout, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn test_task_config_new() {
        let config = TaskConfig::new(Duration::from_secs(30));
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(30));
        assert!(!config.start_immediately);
    }

    #[test]
    fn test_task_config_disabled() {
        let config = TaskConfig::disabled();
        assert!(!config.enabled);
    }
}
