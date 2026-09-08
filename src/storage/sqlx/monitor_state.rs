//! Persisted monitor state: the `monitor_state` table (migration
//! `004_monitor_state`).
//!
//! Two facts live here today, both shared by every `StorageSqlx` opened on
//! the same database file, in every process:
//!
//! - the proof LAG gate (`max_acceptable_proof_height`): the highest block
//!   height whose merkle proofs may be stored. Absent or `0` is CLOSED:
//!   every proof is deferred until the monitor's header task has processed
//!   a header (one that stayed the chain tip for a full cycle). The reorg
//!   review of 2026-09-08 found the previous in-memory gate set only on the
//!   monitor's own storage instance while the webhook and relay ingested on
//!   a second instance whose gate was 0 = open forever; a persisted row
//!   closes that (F3).
//! - the header tracker's state (`header_tracker`): the last observed tip,
//!   the queued header and the ring of recent tips, as JSON, so a one-shot
//!   process conforms to the same "survived a cycle" rule across runs.
//!
//! Divergences from the reference are recorded in `docs/REORG-DIVERGENCES.md`.

use sqlx::{Pool, Sqlite, SqliteConnection};

use crate::error::Result;
use crate::storage::traits::HeaderTrackerState;

/// Migration `004_monitor_state` (additive, idempotent).
pub const MIGRATION_004_MONITOR_STATE_SQL: &str = include_str!("migrations/004_monitor_state.sql");

/// Name of migration 004, as `StorageSqlx::migrate` reports it.
pub const MIGRATION_004_MONITOR_STATE_NAME: &str = "004_monitor_state";

/// The proof LAG gate's row key.
pub const MONITOR_STATE_KEY_PROOF_GATE: &str = "max_acceptable_proof_height";

/// The header tracker's row key (JSON in `text_value`).
pub const MONITOR_STATE_KEY_HEADER_TRACKER: &str = "header_tracker";

/// Make sure the `monitor_state` table exists (migration 004; idempotent).
pub(crate) async fn ensure_monitor_state_schema(pool: &Pool<Sqlite>) -> Result<()> {
    super::broadcast_seen::apply_migration_sql(
        pool,
        MIGRATION_004_MONITOR_STATE_NAME,
        MIGRATION_004_MONITOR_STATE_SQL,
    )
    .await
}

/// The proof LAG gate as stored, on an open connection: `0` when the row is
/// absent (CLOSED).
pub(crate) async fn read_proof_gate_on(conn: &mut SqliteConnection) -> Result<u32> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT value FROM monitor_state WHERE key = ?")
        .bind(MONITOR_STATE_KEY_PROOF_GATE)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(v,)| v.max(0) as u32).unwrap_or(0))
}

/// Write the proof LAG gate on an open connection.
pub(crate) async fn write_proof_gate_on(conn: &mut SqliteConnection, height: u32) -> Result<()> {
    sqlx::query(
        "INSERT INTO monitor_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(MONITOR_STATE_KEY_PROOF_GATE)
    .bind(height as i64)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The header tracker's persisted state, if any.
pub(crate) async fn read_header_tracker_on(
    conn: &mut SqliteConnection,
) -> Result<Option<HeaderTrackerState>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT text_value FROM monitor_state WHERE key = ?")
            .bind(MONITOR_STATE_KEY_HEADER_TRACKER)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((Some(text),)) = row else {
        return Ok(None);
    };
    match serde_json::from_str::<HeaderTrackerState>(&text) {
        Ok(state) => Ok(Some(state)),
        Err(e) => {
            // An unreadable row is a fresh start, never a fault: the tracker
            // re-queues the tip and the gate opens one cycle later.
            tracing::warn!(error = %e, "monitor_state: header tracker state unreadable; starting fresh");
            Ok(None)
        }
    }
}

/// Persist the header tracker's state.
pub(crate) async fn write_header_tracker_on(
    conn: &mut SqliteConnection,
    state: &HeaderTrackerState,
) -> Result<()> {
    let text = serde_json::to_string(state)
        .map_err(|e| crate::error::Error::StorageError(format!("header tracker state: {e}")))?;
    let last_height = state.last.as_ref().map(|(h, _)| *h as i64).unwrap_or(0);
    sqlx::query(
        "INSERT INTO monitor_state (key, value, text_value, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, text_value = excluded.text_value, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(MONITOR_STATE_KEY_HEADER_TRACKER)
    .bind(last_height)
    .bind(text)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MonitorStorage;
    use crate::{StorageSqlx, WalletStorageWriter};

    async fn table_exists(pool: &Pool<Sqlite>, name: &str) -> bool {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(name)
                .fetch_optional(pool)
                .await
                .unwrap();
        row.is_some()
    }

    /// Migration 004 lands on `migrate()` and, for a database created before
    /// it, on `make_available()` (the way every CLI command opens a wallet).
    /// A fresh table reads as a CLOSED gate.
    #[tokio::test]
    async fn migration_004_applies_on_open_of_an_existing_003_database_and_reads_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.db");
        let path = path.to_str().unwrap().to_string();
        {
            let storage = StorageSqlx::open(&path).await.unwrap();
            let version = storage
                .migrate("old-wallet", &"1".repeat(64))
                .await
                .unwrap();
            assert_eq!(version, MIGRATION_004_MONITOR_STATE_NAME);
            sqlx::query("DROP TABLE monitor_state")
                .execute(storage.pool())
                .await
                .unwrap();
            assert!(!table_exists(storage.pool(), "monitor_state").await);
            storage.pool().close().await;
        }
        let storage = StorageSqlx::open(&path).await.unwrap();
        assert!(!table_exists(storage.pool(), "monitor_state").await);
        storage.make_available().await.unwrap();
        assert!(table_exists(storage.pool(), "monitor_state").await);
        assert_eq!(
            storage.max_acceptable_proof_height().await.unwrap(),
            0,
            "closed"
        );
        storage
            .set_max_acceptable_proof_height(965771)
            .await
            .unwrap();
        assert_eq!(storage.max_acceptable_proof_height().await.unwrap(), 965771);
        assert!(storage.load_header_tracker_state().await.unwrap().is_none());
    }

    /// F3: ONE persisted gate. Two `StorageSqlx` opened on the same file
    /// (the daemon's monitor instance and its wallet/webhook instance) read
    /// the same row: set on one, seen by the other; and the tracker state
    /// rides the same table.
    #[tokio::test]
    async fn two_storages_on_one_file_share_the_gate_and_the_tracker_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.db");
        let path = path.to_str().unwrap().to_string();
        let monitor_side = StorageSqlx::open(&path).await.unwrap();
        monitor_side
            .migrate("shared-wallet", &"2".repeat(64))
            .await
            .unwrap();
        monitor_side.make_available().await.unwrap();
        let webhook_side = StorageSqlx::open(&path).await.unwrap();
        webhook_side.make_available().await.unwrap();

        assert_eq!(webhook_side.max_acceptable_proof_height().await.unwrap(), 0);
        monitor_side
            .set_max_acceptable_proof_height(500)
            .await
            .unwrap();
        assert_eq!(
            webhook_side.max_acceptable_proof_height().await.unwrap(),
            500
        );
        webhook_side
            .set_max_acceptable_proof_height(501)
            .await
            .unwrap();
        assert_eq!(
            monitor_side.max_acceptable_proof_height().await.unwrap(),
            501
        );

        let state = HeaderTrackerState {
            last: Some((501, "aa".into())),
            queued: Some((501, "aa".into())),
            ring: vec![(500, "bb".into()), (501, "aa".into())],
        };
        monitor_side
            .save_header_tracker_state(&state)
            .await
            .unwrap();
        assert_eq!(
            webhook_side.load_header_tracker_state().await.unwrap(),
            Some(state)
        );
    }

    /// An unreadable tracker row is a fresh start, never a fault.
    #[tokio::test]
    async fn a_garbled_tracker_row_reads_as_none() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("w", &"3".repeat(64)).await.unwrap();
        storage.make_available().await.unwrap();
        sqlx::query("INSERT INTO monitor_state (key, value, text_value) VALUES (?, 0, 'not json')")
            .bind(MONITOR_STATE_KEY_HEADER_TRACKER)
            .execute(storage.pool())
            .await
            .unwrap();
        assert!(storage.load_header_tracker_state().await.unwrap().is_none());
    }
}
