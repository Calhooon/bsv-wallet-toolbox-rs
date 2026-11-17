//! Held receipts on an existing store (migration 006, the owner's ruling of
//! 2026-11-17, DECISIONS 29.1, and the keeper's reading 5 of section 30).
//!
//! The internalize door writes a received transaction with no checked proof
//! and no broadcaster's take `sending`. A store written before that change
//! holds one as `unproven`; the migration's one statement moves it to
//! `sending` on open. A taken one is not touched; running it again changes
//! nothing; a coin the wallet's own action already spent stays spent.

use sqlx::{Pool, Sqlite, SqliteConnection};

use crate::Result;

/// The request words that are written only after a broadcaster took the
/// transaction or a proof was checked: `unmined` (an instance's acceptance,
/// a status source's `known`/`mined`, or a checked proof the gate
/// deferred), `callback` and `unconfirmed` (no writer in this crate; a
/// synced store's word for a transaction a broadcaster answered for), and
/// `completed` (a proof stored). Every other word of `ProvenTxReqStatus`
/// (`unsent`, `sending`, `nosend`, `unprocessed`, `invalid`, `doubleSpend`,
/// `unfail`, `unknown`, `nonfinal` and the five words no writer here uses)
/// says no broadcaster has taken it.
pub(crate) const TAKEN_REQUEST_WORDS: &str = "'unmined', 'callback', 'unconfirmed', 'completed'";

/// The `broadcast_seen` words that record a take: an instance's
/// acceptance, a status source or push that saw it, a block that mined it.
pub(crate) const TAKEN_SEEN_WORDS: &str = "'accepted', 'seen', 'mined'";

/// Was `txid` taken: does its request carry one of [`TAKEN_REQUEST_WORDS`]
/// or `broadcast_seen` one of [`TAKEN_SEEN_WORDS`]? The one fact the door
/// and migration 006 read; a received transaction with no checked proof
/// that was not taken is held (`sending`).
pub(crate) async fn was_taken(conn: &mut SqliteConnection, txid: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM proven_tx_reqs WHERE txid = ?1 AND status IN ({TAKEN_REQUEST_WORDS})) \
         OR EXISTS (SELECT 1 FROM broadcast_seen WHERE txid = ?1 AND status IN ({TAKEN_SEEN_WORDS}))"
    );
    let taken: bool = sqlx::query_scalar(&sql)
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
    Ok(taken)
}

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

#[cfg(test)]
mod tests {
    use super::{MIGRATION_006_HELD_RECEIPTS_SQL, TAKEN_REQUEST_WORDS, TAKEN_SEEN_WORDS};

    /// The migration reads the same fact as the door: the same two lists.
    #[test]
    fn the_migration_reads_the_doors_taken_words() {
        let sql: String = MIGRATION_006_HELD_RECEIPTS_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            sql.contains(&format!("proven_tx_reqs WHERE status IN ({TAKEN_REQUEST_WORDS})")),
            "{sql}"
        );
        assert!(
            sql.contains(&format!("broadcast_seen WHERE status IN ({TAKEN_SEEN_WORDS})")),
            "{sql}"
        );
    }
}
