//! Which stored proofs had their root checked (migration 005, P0-1).
//!
//! A `proven_txs` row is CHECKED when `proof_root_checks` holds a record for
//! its txid at its height with the root its stored `merkle_path` computes.
//! The store funnel writes the record for every proof it stores (it is
//! reached only after a ChainTracker confirmed the root); rows written before
//! migration 005 and rows merged by sync have none. The BEEF walk checks an
//! unchecked row once, when it reads it, and demotes it when the tracker
//! refutes it.

use bsv_rs::transaction::{ChainTracker, MerklePath};
use sqlx::{Pool, Sqlite, SqliteConnection};

use crate::Result;

/// The migration that creates `proof_root_checks`.
pub const MIGRATION_005_PROOF_ROOT_CHECKS_SQL: &str =
    include_str!("migrations/005_proof_root_checks.sql");

/// Name of migration 005, as `StorageSqlx::migrate` reports it.
pub const MIGRATION_005_PROOF_ROOT_CHECKS_NAME: &str = "005_proof_root_checks";

/// Make sure the `proof_root_checks` table exists (migration 005;
/// idempotent). Called on `make_available()`, so a database created before
/// 005 gets the table the next time it is opened.
pub(crate) async fn ensure_proof_root_checks_schema(pool: &Pool<Sqlite>) -> Result<()> {
    super::broadcast_seen::apply_migration_sql(
        pool,
        MIGRATION_005_PROOF_ROOT_CHECKS_NAME,
        MIGRATION_005_PROOF_ROOT_CHECKS_SQL,
    )
    .await
}

/// Record that a ChainTracker confirmed `merkle_root` at `height` for `txid`.
pub(crate) async fn record_root_checked_on(
    conn: &mut SqliteConnection,
    txid: &str,
    height: u32,
    merkle_root: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO proof_root_checks (txid, height, merkle_root, checked_at) VALUES (?, ?, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET height = excluded.height, merkle_root = excluded.merkle_root, checked_at = excluded.checked_at",
    )
    .bind(txid)
    .bind(height as i64)
    .bind(merkle_root.to_ascii_lowercase())
    .bind(chrono::Utc::now())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Was `merkle_root` at `height` for `txid` confirmed by a ChainTracker?
pub(crate) async fn root_checked_on(
    conn: &mut SqliteConnection,
    txid: &str,
    height: u32,
    merkle_root: &str,
) -> Result<bool> {
    let found: Option<(i64,)> = sqlx::query_as(
        "SELECT 1 FROM proof_root_checks WHERE txid = ? AND height = ? AND merkle_root = ?",
    )
    .bind(txid)
    .bind(height as i64)
    .bind(merkle_root.to_ascii_lowercase())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(found.is_some())
}

/// Drop `txid`'s record (its proof row is being deleted).
pub(crate) async fn forget_root_check_on(conn: &mut SqliteConnection, txid: &str) -> Result<()> {
    sqlx::query("DELETE FROM proof_root_checks WHERE txid = ?")
        .bind(txid)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Is the stored proof `merkle_path` for `txid` a checked one? A path whose
/// root cannot be computed is never checked.
pub(crate) async fn stored_proof_checked_on(
    conn: &mut SqliteConnection,
    txid: &str,
    merkle_path: &MerklePath,
) -> Result<bool> {
    let Ok(root) = merkle_path.compute_root(Some(txid)) else {
        return Ok(false);
    };
    root_checked_on(conn, txid, merkle_path.block_height, &root).await
}

/// What the one-time read check of an UNCHECKED stored proof found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadCheck {
    /// The row already carries a record: nothing asked.
    AlreadyChecked,
    /// The tracker confirmed the root; the record was written.
    Confirmed,
    /// The tracker refuted the root; the row was demoted.
    Demoted,
    /// No tracker, a tracker fault, or a root that does not compute: the
    /// row is left as it was, unchecked, for the next read.
    Undecided,
}

/// Check an unchecked stored proof once against `tracker`: on a
/// confirmation record it, on a refutation demote the row (the transaction
/// is unproven again, its bytes kept; see `demote_stale_proof_on`). A row
/// that was never checked carries no evidence, so the tracker's refutation
/// alone demotes it; a CHECKED row that the tracker later refutes is a
/// reorg, the reorg and review tasks' business, and is not touched here.
///
/// Runs on the caller's connection inside a savepoint, so the demotion is
/// atomic with itself and with the caller's transaction when there is one.
pub(crate) async fn check_unchecked_stored_proof_on(
    conn: &mut SqliteConnection,
    tracker: Option<&dyn ChainTracker>,
    txid: &str,
    merkle_path: &MerklePath,
) -> Result<ReadCheck> {
    if stored_proof_checked_on(&mut *conn, txid, merkle_path).await? {
        return Ok(ReadCheck::AlreadyChecked);
    }
    let Some(tracker) = tracker else {
        return Ok(ReadCheck::Undecided);
    };
    let Ok(root) = merkle_path.compute_root(Some(txid)) else {
        return Ok(ReadCheck::Undecided);
    };
    match tracker
        .is_valid_root_for_height(&root, merkle_path.block_height)
        .await
    {
        Ok(true) => {
            record_root_checked_on(&mut *conn, txid, merkle_path.block_height, &root).await?;
            tracing::info!(
                txid = %txid,
                height = merkle_path.block_height,
                marker = "unchecked_proof_confirmed",
                "read check: a stored proof that was never checked is confirmed by the chain tracker"
            );
            Ok(ReadCheck::Confirmed)
        }
        Ok(false) => {
            let mut sp = sqlx::Connection::begin(&mut *conn).await?;
            super::storage_sqlx::demote_stale_proof_on(&mut sp, txid).await?;
            sp.commit().await?;
            tracing::warn!(
                txid = %txid,
                height = merkle_path.block_height,
                marker = "unchecked_proof_demoted",
                "read check: a stored proof that was never checked is refuted by the chain tracker; demoted, the transaction is unproven again"
            );
            Ok(ReadCheck::Demoted)
        }
        Err(e) => {
            tracing::debug!(
                txid = %txid,
                height = merkle_path.block_height,
                error = %e,
                "read check: the chain tracker could not answer; the unchecked proof is left for the next read"
            );
            Ok(ReadCheck::Undecided)
        }
    }
}
