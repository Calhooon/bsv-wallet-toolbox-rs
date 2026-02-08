//! Wallet Manager Components
//!
//! This module provides higher-level wallet management abstractions on top of
//! the core storage, services, and wallet layers. These managers provide:
//!
//! - **WalletStorageManager**: Multi-storage synchronization with active/backup semantics
//! - **WalletSettingsManager**: Persistent wallet settings management
//! - **SimpleWalletManager**: Two-factor authentication with primary key + privileged key manager
//! - **CWIStyleWalletManager**: CWI-compatible multi-profile wallet manager
//! - **WalletPermissionsManager**: BRC-98/99 permission control
//! - **WalletAuthenticationManager**: WAB integration for authentication backends
//! - **WalletLogger**: Operation logging for debugging and diagnostics
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Managers                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  WalletStorageManager          - Multi-storage sync, active/backup│
//! │  WalletSettingsManager         - Settings persistence            │
//! │  SimpleWalletManager           - Primary key + PKM authentication│
//! │  CWIStyleWalletManager         - Multi-profile, password-based   │
//! │  WalletAuthenticationManager   - WAB authentication integration  │
//! │  WalletPermissionsManager      - BRC-98/99 permissions           │
//! │  WalletLogger                  - Operation logging               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                      Wallet + Storage + Services                │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Feature Parity
//!
//! These managers are designed for 1:1 parity with the TypeScript wallet-toolbox:
//! - `WalletStorageManager` matches TypeScript's storage synchronization logic
//! - `WalletSettingsManager` matches TypeScript's settings persistence pattern
//! - `SimpleWalletManager` matches TypeScript's two-factor authentication pattern
//! - `CWIStyleWalletManager` matches TypeScript's CWI multi-profile pattern
//! - `WalletLogger` matches TypeScript's WalletLogger interface

use serde::{Deserialize, Serialize};

mod settings_manager;
mod simple_wallet_manager;
mod storage_manager;

// CWI and permissions implementations
mod cwi_style_wallet_manager;
mod permissions_manager;

// Authentication manager
mod auth_manager;

// Re-export public types
pub use settings_manager::{
    Certifier, TrustSettings, WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig,
    WalletTheme, DEFAULT_SETTINGS, TESTNET_DEFAULT_SETTINGS,
};

pub use simple_wallet_manager::SimpleWalletManager;

pub use storage_manager::{ManagedStorage, WalletStorageManager};

pub use cwi_style_wallet_manager::{
    CWIStyleWalletManager, CWIStyleWalletManagerConfig, Profile,
    UmpToken, WalletSnapshot,
};

pub use permissions_manager::{
    BasketUsageType, CertificateUsageType, GroupedPermissions, PermissionRequest,
    PermissionRequestHandler, PermissionToken, PermissionUsageType, PermissionsModule,
    WalletPermissionsManager, WalletPermissionsManagerConfig,
};

pub use auth_manager::WalletAuthenticationManager;

// ============================================================================
// WalletLogger - Operation logging for debugging and diagnostics
// ============================================================================

/// Wallet operation logger for debugging and diagnostics.
/// Matches TypeScript's WalletLogger interface.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalletLogger {
    /// Current indentation level for nested groups.
    pub indent: u32,
    /// All log entries recorded by this logger.
    pub logs: Vec<WalletLogEntry>,
    /// Whether this logger is the origin logger (top-level).
    pub is_origin: bool,
    /// Whether any error has been logged.
    pub is_error: bool,
}

/// A single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletLogEntry {
    /// ISO 8601 timestamp of when the entry was created.
    pub timestamp: String,
    /// Log level (e.g., "info", "warn", "error", "debug").
    pub level: String,
    /// The log message.
    pub message: String,
    /// Indentation level at the time of logging.
    pub indent: u32,
}

impl WalletLogger {
    /// Create a new empty logger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start a new log group (increases indentation).
    pub fn group(&mut self, name: &str) {
        self.log("info", name);
        self.indent += 1;
    }

    /// End the current log group (decreases indentation).
    pub fn group_end(&mut self) {
        if self.indent > 0 {
            self.indent -= 1;
        }
    }

    /// Log a message at the given level.
    pub fn log(&mut self, level: &str, message: &str) {
        self.logs.push(WalletLogEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: level.to_string(),
            message: message.to_string(),
            indent: self.indent,
        });
    }

    /// Log an error message and mark the logger as having errors.
    pub fn error(&mut self, message: &str) {
        self.is_error = true;
        self.log("error", message);
    }

    /// Format all log entries as a human-readable string.
    pub fn to_log_string(&self) -> String {
        self.logs
            .iter()
            .map(|l| {
                format!(
                    "[{}] {}{}: {}",
                    l.timestamp,
                    "  ".repeat(l.indent as usize),
                    l.level,
                    l.message
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Serialize the logger to JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

// ============================================================================
// Setup helpers
// ============================================================================

/// Options for setting up a wallet with standard configuration.
#[derive(Debug, Clone)]
pub struct SetupWalletOptions {
    /// Optional root key for wallet initialization.
    pub root_key: Option<Vec<u8>>,
    /// Optional storage path for database persistence.
    pub storage_path: Option<String>,
    /// The blockchain network to use.
    pub chain: crate::chaintracks::Chain,
}

/// Create a wallet with standard configuration.
///
/// This is a convenience function that sets up storage, services, and creates
/// a wallet with sensible defaults. Currently a stub that logs the setup intent.
///
/// # Arguments
///
/// * `options` - Configuration options for the wallet setup
///
/// # Returns
///
/// Returns `Ok(())` on success.
pub async fn setup_wallet(options: SetupWalletOptions) -> crate::Result<()> {
    tracing::info!("Setting up wallet with {:?} chain", options.chain);
    // Stub implementation - full setup requires storage and services creation
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_logger_new() {
        let logger = WalletLogger::new();
        assert_eq!(logger.indent, 0);
        assert!(logger.logs.is_empty());
        assert!(!logger.is_origin);
        assert!(!logger.is_error);
    }

    #[test]
    fn test_wallet_logger_log() {
        let mut logger = WalletLogger::new();
        logger.log("info", "test message");
        assert_eq!(logger.logs.len(), 1);
        assert_eq!(logger.logs[0].level, "info");
        assert_eq!(logger.logs[0].message, "test message");
        assert_eq!(logger.logs[0].indent, 0);
    }

    #[test]
    fn test_wallet_logger_group() {
        let mut logger = WalletLogger::new();
        logger.group("outer");
        assert_eq!(logger.indent, 1);
        logger.log("info", "inside outer");
        assert_eq!(logger.logs[1].indent, 1);

        logger.group("inner");
        assert_eq!(logger.indent, 2);
        logger.log("debug", "inside inner");
        assert_eq!(logger.logs[3].indent, 2);

        logger.group_end();
        assert_eq!(logger.indent, 1);
        logger.group_end();
        assert_eq!(logger.indent, 0);
        // Extra group_end should not go below 0
        logger.group_end();
        assert_eq!(logger.indent, 0);
    }

    #[test]
    fn test_wallet_logger_error() {
        let mut logger = WalletLogger::new();
        assert!(!logger.is_error);
        logger.error("something went wrong");
        assert!(logger.is_error);
        assert_eq!(logger.logs[0].level, "error");
        assert_eq!(logger.logs[0].message, "something went wrong");
    }

    #[test]
    fn test_wallet_logger_to_log_string() {
        let mut logger = WalletLogger::new();
        logger.log("info", "start");
        logger.group("section");
        logger.log("debug", "detail");
        logger.group_end();

        let output = logger.to_log_string();
        assert!(output.contains("info: start"));
        assert!(output.contains("  debug: detail"));
    }

    #[test]
    fn test_wallet_logger_serialization() {
        let mut logger = WalletLogger::new();
        logger.log("info", "test");
        logger.is_origin = true;

        let json = logger.to_json();
        let parsed: WalletLogger = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.logs.len(), 1);
        assert!(parsed.is_origin);
        assert_eq!(parsed.logs[0].message, "test");
    }

    #[test]
    fn test_wallet_log_entry_serialization() {
        let entry = WalletLogEntry {
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            level: "info".to_string(),
            message: "hello".to_string(),
            indent: 2,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: WalletLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.timestamp, entry.timestamp);
        assert_eq!(parsed.level, entry.level);
        assert_eq!(parsed.message, entry.message);
        assert_eq!(parsed.indent, entry.indent);
    }

    #[test]
    fn test_setup_wallet_options() {
        let options = SetupWalletOptions {
            root_key: Some(vec![0x42; 32]),
            storage_path: Some("/tmp/test.db".to_string()),
            chain: crate::chaintracks::Chain::Main,
        };
        assert!(options.root_key.is_some());
        assert_eq!(options.storage_path, Some("/tmp/test.db".to_string()));
    }

    #[tokio::test]
    async fn test_setup_wallet_stub() {
        let options = SetupWalletOptions {
            root_key: None,
            storage_path: None,
            chain: crate::chaintracks::Chain::Test,
        };
        let result = setup_wallet(options).await;
        assert!(result.is_ok());
    }
}
