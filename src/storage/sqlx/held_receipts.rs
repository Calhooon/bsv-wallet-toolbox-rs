//! Held receipts on an existing store (migration 006, the owner's ruling of
//! 2026-11-17, DECISIONS 29.1, and the keeper's reading 5 of section 30).
//!
//! The internalize door writes a received transaction with no checked proof
//! and no broadcaster's take `sending`. A store written before that change
//! holds one as `unproven`; the migration's one statement moves it to
//! `sending` on open. A taken one is not touched; running it again changes
//! nothing; a coin the wallet's own action already spent stays spent.

use sqlx::{Pool, Sqlite};

use crate::Result;

/// The data statement of migration 006.
pub(crate) const MIGRATION_006_HELD_RECEIPTS_SQL: &str =
    include_str!("migrations/006_held_receipts.sql");

/// Name of migration 006, as `StorageSqlx::migrate` reports it.
pub(crate) const MIGRATION_006_HELD_RECEIPTS_NAME: &str = "006_held_receipts";

/// Move every held receipt a store holds as `unproven` to `sending`
/// (migration 006; idempotent). Called on `make_available()`, after 002,
/// whose `broadcast_seen` the statement reads.
pub(crate) async fn move_held_receipts(pool: &Pool<Sqlite>) -> Result<()> {
    super::broadcast_seen::apply_migration_sql(
        pool,
        MIGRATION_006_HELD_RECEIPTS_NAME,
        MIGRATION_006_HELD_RECEIPTS_SQL,
    )
    .await
}
