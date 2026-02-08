//! Monitor Module
//!
//! Background task scheduler for monitoring and managing BSV wallet transaction lifecycle.
//!
//! The monitor provides a daemon-based task scheduler for running recurring background
//! operations on wallet storage. It handles transaction lifecycle management including:
//! - Proof verification (checking for merkle proofs)
//! - Broadcasting pending transactions
//! - Marking abandoned transactions as failed
//! - Recovering incorrectly failed transactions
//! - Tracking clock/minute boundaries
//! - Polling for new block headers
//! - Handling blockchain reorganizations
//! - Checking nosend transaction proofs
//! - Reviewing and synchronizing transaction status
//! - Purging expired data
//! - Monitoring service call history

mod config;
mod daemon;
pub mod tasks;

pub use config::{MonitorOptions, TaskConfig, TransactionStatusUpdate};
pub use daemon::{Monitor, MonitorHealth, TaskHealth};
pub use tasks::{MonitorTask, TaskResult};
