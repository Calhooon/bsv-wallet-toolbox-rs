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
//! use bsv_wallet_toolbox_rs::storage::sqlx::StorageSqlx;
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
mod beef_verification;
mod broadcast_seen;
mod create_action;
mod internalize_action;
mod poisoned_chain;
mod process_action;
mod storage_sqlx;
mod sync;

pub use crate::storage::broadcast::{
    classify_broadcast_results, validate_beef_for_broadcast, BroadcastOutcome,
};
pub use beef_verification::{verify_beef_merkle_proofs, verify_txid_merkle_proof};
pub use broadcast_seen::{
    SqlxBroadcastMemory, MIGRATION_002_BROADCAST_SEEN_NAME, MIGRATION_002_BROADCAST_SEEN_SQL,
};
pub use poisoned_chain::{InternalizedPhantom, PoisonOutcome, PoisonReport, PoisonedTx};
pub use storage_sqlx::{
    RetireOutcome, StorageSqlx, UnprovenAdoption, ADOPT_UNPROVEN_TX_LIMIT,
    DEFAULT_MAX_OUTPUT_SCRIPT,
};
pub use sync::entity_names;
