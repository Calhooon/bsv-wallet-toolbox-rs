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
#[cfg(test)]
mod broadcaster_hint_tests;
#[cfg(test)]
mod change_tiebreak_tests;
mod create_action;
mod internalize_action;
use crate::wallet::label;
mod locked_inputs;
mod monitor_state;
mod poisoned_chain;
mod process_action;
#[cfg(test)]
mod proof_attempt_backstop_tests;
mod proof_root_checks;
#[cfg(test)]
mod proof_root_tests;
#[cfg(test)]
mod reorg_below_the_user_tests;
#[cfg(test)]
mod reorg_tests;
#[cfg(test)]
mod sequence_number_tests;
#[cfg(test)]
mod sibling_bump_tests;
mod storage_sqlx;
mod sync;
#[cfg(test)]
mod void_lineage_tests;

#[cfg(feature = "bench-support")]
pub(crate) use create_action::benchmark_allocate_change_input;

pub use crate::storage::broadcast::{
    classify_broadcast_results, validate_beef_for_broadcast, BroadcastOutcome,
};
pub use beef_verification::{
    refuse_invalid_beef_bytes, verify_beef_merkle_proofs, verify_txid_merkle_proof,
};
pub use broadcast_seen::{
    SqlxBroadcastMemory, MIGRATION_002_BROADCAST_SEEN_NAME, MIGRATION_002_BROADCAST_SEEN_SQL,
};
/// The A8 differential's seam for the fuzz target `a8_beef_bytes` (#556).
#[cfg(feature = "a8-beef-bytes")]
#[doc(hidden)]
pub use internalize_action::a8_differential::differential as a8_beef_bytes_differential;
pub use locked_inputs::{
    locked_input_backoff_minutes, LockedInputCheck, LockedInputReport, LockedInputVerdict,
    LOCKED_INPUT_BACKOFF_CAP_MINUTES, LOCKED_VERDICT_SPENT_HINT, LOCKED_VERDICT_SPENT_PROVEN,
    LOCKED_VERDICT_UNKNOWN, MIGRATION_003_LOCKED_INPUT_CHECKS_NAME,
    MIGRATION_003_LOCKED_INPUT_CHECKS_SQL,
};
pub use monitor_state::{
    MIGRATION_004_MONITOR_STATE_NAME, MIGRATION_004_MONITOR_STATE_SQL,
    MONITOR_STATE_KEY_HEADER_TRACKER, MONITOR_STATE_KEY_PROOF_GATE,
};
pub use poisoned_chain::{
    chain_knowledge, utxo_verdict, ChainKnowledge, InternalizedPhantom, PoisonOutcome,
    PoisonReport, PoisonedTx, UtxoVerdict, DEFAULT_ABSENCE_MINUTES,
};
pub use proof_root_checks::{
    MIGRATION_005_PROOF_ROOT_CHECKS_NAME, MIGRATION_005_PROOF_ROOT_CHECKS_SQL,
};
pub use storage_sqlx::{
    RetireOutcome, StorageSqlx, StorageSqlxOptions, UnprovenAdoption, ADOPT_UNPROVEN_TX_LIMIT,
    DEFAULT_MAX_OUTPUT_SCRIPT, STORE_BUSY_CEILING_MS,
};
pub use sync::entity_names;
#[cfg(test)]
mod script_pin_tests;
