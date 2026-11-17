//! Held receipts (the owner's ruling of 2026-11-17, DECISIONS 29.1, and
//! the keeper's reading 5 of section 30): what makes a new received row
//! taken at the internalize door, and migration 006's own rule for rows a
//! store wrote before the door kept a record of every take.
//!
//! THE DOOR. A received transaction with no checked proof is taken when,
//! and only when, the door's own post drew an acceptance, the store holds a
//! record of a take for the txid (`broadcast_seen` `accepted`, `seen` or
//! `mined`), or the store holds a stored proof whose root was checked for
//! it (a `proven_txs` row that `proof_root_checks` records at its height
//! with its root; a row a sync chunk merged has no record). A request's
//! word alone is not read. Not taken, the row is held (`sending`). The
//! per-user `review_status` heals a received `sending` row on the same
//! evidence; the wallet's own outgoing row heals on its request's word.
//!
//! THE MIGRATION (006), its own rule. Rows written before the door recorded
//! its takes carry no record of them, so the statement keeps the request's
//! word for them: a received `unproven` row with no proof linked whose
//! request is `unmined`, `callback`, `unconfirmed` or `completed`, whose
//! txid has a record of a take, or whose txid has a stored proof whose root
//! was checked, is left as it is; one whose request carries any other word
//! (`unsent`, `sending`, ...) and neither moves to `sending`. Running it again changes nothing; a coin the wallet's own
//! action already spent stays spent.

use sqlx::{Pool, Sqlite, SqliteConnection};

use crate::Result;

/// The `broadcast_seen` words that record a take: an instance's
/// acceptance, a status source or push that saw it, a block that mined it.
/// The one list the door and migration 006 both read.
pub(crate) const TAKEN_SEEN_WORDS: &str = "'accepted', 'seen', 'mined'";

/// The SQL condition "`txid_expr` has a stored proof whose root was
/// checked": a `proven_txs` row of the txid that `proof_root_checks`
/// records at that row's height with that row's root. The definition of a
/// checked row is `proof_root_checks.rs`'s (migration 005), the one
/// `root_checked_on` reads, with the row's `merkle_root` column standing for
/// the root its stored path computes (SQL cannot compute it). A record is
/// written only by the store funnel and the read check, each after a chain
/// tracker confirmed the root; a row a sync chunk wrote has none.
pub(crate) fn checked_proof_condition(txid_expr: &str) -> String {
    format!("EXISTS (SELECT 1 FROM {CHECKED_PROOF_JOIN} WHERE p.txid = {txid_expr})")
}

/// The join of a stored proof with its root check: the one text the door
/// ([`checked_proof_condition`]) and migration 006 both read.
pub(crate) const CHECKED_PROOF_JOIN: &str = "proven_txs p JOIN proof_root_checks c \
     ON c.txid = p.txid AND c.height = p.height AND c.merkle_root = lower(p.merkle_root)";

/// The SQL condition "`txid_expr` was taken": a record of a take
/// ([`TAKEN_SEEN_WORDS`]) or a stored proof whose root was checked
/// ([`checked_proof_condition`]). Read by the door ([`was_taken`]) and the
/// per-user `review_status` heal.
pub(crate) fn taken_condition(txid_expr: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM broadcast_seen WHERE txid = {txid_expr} AND status IN ({TAKEN_SEEN_WORDS})) \
         OR {})",
        checked_proof_condition(txid_expr)
    )
}

/// Was `txid` taken: a record of a take or a stored proof whose root was
/// checked ([`taken_condition`])? A received transaction with no checked proof that
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

/// [`record_take_on`] for a status source that holds `txid`: `chain`,
/// `seen`.
pub(crate) async fn record_chain_seen_on(conn: &mut SqliteConnection, txid: &str) -> Result<()> {
    record_take_on(
        conn,
        txid,
        crate::services::broadcast_memory::BROADCAST_PROVIDER_CHAIN,
        crate::services::broadcast_memory::BROADCAST_STATUS_SEEN,
    )
    .await
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
    use super::{
        taken_condition, CHECKED_PROOF_JOIN, MIGRATION_006_HELD_RECEIPTS_SQL, TAKEN_SEEN_WORDS,
    };

    /// Migration 006's statement, its comment lines out.
    fn statement() -> String {
        MIGRATION_006_HELD_RECEIPTS_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `sql` with every run of whitespace one space.
    fn folded(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// The migration and the door read the same evidence: the same
    /// `broadcast_seen` words and the same stored proof whose root was
    /// checked.
    #[test]
    fn the_migration_and_the_door_read_the_same_evidence() {
        let sql = statement();
        let join = folded(CHECKED_PROOF_JOIN);
        assert!(
            folded(&sql).contains(&format!("SELECT p.txid FROM {join})")),
            "the migration reads the checked proof: {sql}"
        );
        assert!(
            folded(&taken_condition("?1"))
                .contains(&format!("SELECT 1 FROM {join} WHERE p.txid = ?1)")),
            "the door reads the same checked proof"
        );
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
