//! Held receipts (the owner's ruling of 2026-11-17, DECISIONS 29.1, and
//! the keeper's reading 5 of section 30): what makes a new received row
//! taken at the internalize door, and migration 006's own rule for rows a
//! store wrote before the door kept a record of every take.
//!
//! THE DOOR. A received transaction with no checked proof is taken when,
//! and only when, the door's own post drew an acceptance, the store holds a
//! record of a take for the txid (`broadcast_seen` `accepted`, `seen` or
//! `mined`), or a proof is stored for it (a `proven_txs` row). A request's
//! word alone is not evidence: `unmined` and `completed` are written on
//! paths no broadcaster took and no proof was checked. Not taken, the row is
//! held (`sending`).
//!
//! THE MIGRATION (006), its own rule. Rows written before the door recorded
//! its takes carry no record of them, so the statement keeps the request's
//! word for them: a received `unproven` row with no proof linked whose
//! request is `unmined`, `callback`, `unconfirmed` or `completed`, or whose
//! txid has a record of a take, is left as it is; one whose request carries
//! any other word (`unsent`, `sending`, ...) and no record moves to
//! `sending`. Running it again changes nothing; a coin the wallet's own
//! action already spent stays spent.

use sqlx::{Pool, Sqlite, SqliteConnection};

use crate::Result;

/// The `broadcast_seen` words that record a take: an instance's
/// acceptance, a status source or push that saw it, a block that mined it.
/// The one list the door and migration 006 both read.
pub(crate) const TAKEN_SEEN_WORDS: &str = "'accepted', 'seen', 'mined'";

/// The SQL condition "`txid_expr` was taken": a record of a take
/// ([`TAKEN_SEEN_WORDS`]) or a stored proof (a `proven_txs` row; a request
/// `completed` links one). Read by the door ([`was_taken`]) and the per-user
/// `review_status` heal.
pub(crate) fn taken_condition(txid_expr: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM broadcast_seen WHERE txid = {txid_expr} AND status IN ({TAKEN_SEEN_WORDS})) \
         OR EXISTS (SELECT 1 FROM proven_txs WHERE txid = {txid_expr}))"
    )
}

/// Was `txid` taken: a record of a take or a stored proof
/// ([`taken_condition`])? A received transaction with no checked proof that
/// was not taken is held (`sending`).
pub(crate) async fn was_taken(conn: &mut SqliteConnection, txid: &str) -> Result<bool> {
    let sql = format!("SELECT {}", taken_condition("?1"));
    let taken: bool = sqlx::query_scalar(&sql)
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
    Ok(taken)
}

/// Record a take the writer observed on its own connection, inside the
/// write that moves the row to `unproven`, so the row's word and the record
/// never disagree whichever services a host built: `provider` an instance's
/// name with `accepted`, or `chain` with `seen` for a status source that
/// holds the transaction. The ladder keeps a stronger record as it is.
pub(crate) async fn record_take_on(
    conn: &mut SqliteConnection,
    txid: &str,
    provider: &str,
    word: &str,
) -> Result<()> {
    super::broadcast_seen::record_broadcast_status_on(conn, txid, provider, word).await
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
    use super::{taken_condition, MIGRATION_006_HELD_RECEIPTS_SQL, TAKEN_SEEN_WORDS};

    /// The migration and the door read the same record of a take: the same
    /// `broadcast_seen` words.
    #[test]
    fn the_migration_and_the_door_read_the_same_record_words() {
        let sql: String = MIGRATION_006_HELD_RECEIPTS_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let record = format!("broadcast_seen WHERE status IN ({TAKEN_SEEN_WORDS})");
        assert!(sql.contains(&record), "{sql}");
        assert!(
            taken_condition("?1").contains(&format!(
                "broadcast_seen WHERE txid = ?1 AND status IN ({TAKEN_SEEN_WORDS})"
            )),
            "the door reads the same words"
        );
    }
}
