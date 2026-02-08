//! Storage layer for wallet state persistence.
//!
//! This module provides the storage abstractions and implementations for
//! persisting wallet state. It mirrors the TypeScript `@bsv/wallet-toolbox`
//! storage architecture.
//!
//! # Trait Hierarchy
//!
//! ```text
//! WalletStorageReader     - Read operations (findOutputs, listActions, etc.)
//!         ↑
//! WalletStorageWriter     - Write operations (createAction, insertCertificate, etc.)
//!         ↑
//! WalletStorageSync       - Sync operations (getSyncChunk, processSyncChunk)
//!         ↑
//! WalletStorageProvider   - Full provider interface
//! ```
//!
//! # Implementations
//!
//! - `StorageSqlx` - SQLite/MySQL storage using sqlx
//! - `StorageClient` - Remote storage via JSON-RPC to storage.babbage.systems
//! - `WalletStorageManager` - Orchestrates multiple providers with active/backup

pub mod entities;
mod traits;

// SQLx storage (SQLite/MySQL)
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub mod sqlx;

// Remote storage client
#[cfg(feature = "remote")]
pub mod client;

// Future implementations
// pub mod manager;

pub use traits::*;

// Re-export storage implementations
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use sqlx::StorageSqlx;

#[cfg(feature = "remote")]
pub use client::StorageClient;
