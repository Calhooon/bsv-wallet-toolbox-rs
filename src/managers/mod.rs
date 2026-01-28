//! Wallet Manager Components
//!
//! This module provides higher-level wallet management abstractions on top of
//! the core storage, services, and wallet layers. These managers provide:
//!
//! - **WalletStorageManager**: Multi-storage synchronization with active/backup semantics
//! - **WalletSettingsManager**: Persistent wallet settings management
//! - **SimpleWalletManager**: Two-factor authentication with primary key + privileged key manager
//! - **CWIStyleWalletManager**: CWI-compatible multi-profile wallet manager
//! - **WalletPermissionsManager**: BRC-98/99 permission control (stub)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Managers                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  WalletStorageManager     - Multi-storage sync, active/backup   │
//! │  WalletSettingsManager    - Settings persistence                │
//! │  SimpleWalletManager      - Primary key + PKM authentication    │
//! │  CWIStyleWalletManager    - Multi-profile, password-based       │
//! │  WalletPermissionsManager - BRC-98/99 permissions (stub)        │
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

mod settings_manager;
mod simple_wallet_manager;
mod storage_manager;

// Stub implementations (placeholders for full implementation)
mod cwi_style_wallet_manager;
mod permissions_manager;

// Re-export public types
pub use settings_manager::{
    Certifier, TrustSettings, WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig,
    WalletTheme, DEFAULT_SETTINGS, TESTNET_DEFAULT_SETTINGS,
};

pub use simple_wallet_manager::SimpleWalletManager;

pub use storage_manager::{ManagedStorage, WalletStorageManager};

pub use cwi_style_wallet_manager::{CWIStyleWalletManager, CWIStyleWalletManagerConfig, Profile};

pub use permissions_manager::{
    GroupedPermissions, PermissionRequest, PermissionToken, PermissionsModule,
    WalletPermissionsManager, WalletPermissionsManagerConfig,
};
