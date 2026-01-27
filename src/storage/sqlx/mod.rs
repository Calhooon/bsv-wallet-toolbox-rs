//! SQLx-based storage implementations.
//!
//! This module provides storage backends using SQLx for database access.
//! Currently supports SQLite, with MySQL support planned.
//!
//! # Features
//!
//! - `sqlite` (default) - SQLite storage backend
//! - `mysql` - MySQL storage backend (planned)
//!
//! # Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::storage::sqlx::StorageSqlx;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Open file-based SQLite database
//!     let storage = StorageSqlx::open("wallet.db").await?;
//!
//!     // Or use in-memory for testing
//!     let test_storage = StorageSqlx::in_memory().await?;
//!
//!     // Run migrations
//!     storage.migrate("my-wallet", &storage_identity_key).await?;
//!
//!     // Make storage available
//!     let settings = storage.make_available().await?;
//!
//!     Ok(())
//! }
//! ```

mod abort_action;
mod create_action;
mod internalize_action;
mod process_action;
mod storage_sqlx;
mod sync;

pub use storage_sqlx::{StorageSqlx, DEFAULT_MAX_OUTPUT_SCRIPT};
pub use sync::entity_names;
