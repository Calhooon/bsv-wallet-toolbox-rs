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

mod config;
mod daemon;
pub mod tasks;

pub use config::{MonitorOptions, TaskConfig};
pub use daemon::Monitor;
pub use tasks::{MonitorTask, TaskResult};
