//! Persisted broadcast acceptance memory: the `broadcast_seen` and
//! `broadcast_prefs` tables (migration `002_broadcast_seen`).
//!
//! See [`crate::services::broadcast_memory`] for what the memory is for, the
//! status ladder and how `Services::post_beef` uses it.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use sqlx::{Pool, Row, Sqlite, SqliteConnection};
use tokio::sync::OnceCell;

use crate::error::{Error, Result};
use crate::services::broadcast_memory::{
    ladder_step, BroadcastMemory, BroadcastSeenRecord, BroadcastStatus, LadderStep,
    BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_REJECTED,
};

use super::storage_sqlx::StorageSqlx;

/// Migration `002_broadcast_seen` (additive, idempotent).
pub const MIGRATION_002_BROADCAST_SEEN_SQL: &str =
    include_str!("migrations/002_broadcast_seen.sql");

/// Name of migration 002, as `StorageSqlx::migrate` reports it.
pub const MIGRATION_002_BROADCAST_SEEN_NAME: &str = "002_broadcast_seen";

/// SQLite's default host-parameter cap is 32766 (999 before 3.32); `IN`
/// lists are chunked well under either.
const IN_CHUNK: usize = 500;

/// Parse a timestamp the way this database writes them: SQLite's
/// `CURRENT_TIMESTAMP` (`YYYY-MM-DD HH:MM:SS`, UTC), the same with
/// fractional seconds, or the RFC 3339 form sqlx binds a `DateTime<Utc>`
/// as. An unparseable value reads as the UNIX epoch: stale, never fresh.
pub(crate) fn parse_db_timestamp(text: &str) -> DateTime<Utc> {
    let text = text.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return dt.with_timezone(&Utc);
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Utc.from_utc_datetime(&naive);
        }
    }
    Utc.timestamp_opt(0, 0).single().unwrap_or_default()
}

/// Run every statement of one migration file: comment lines stripped,
/// statements split on `;`. Shared by `StorageSqlx::run_migrations` and the
/// lazy schema check of [`SqlxBroadcastMemory`].
pub(crate) async fn apply_migration_sql(pool: &Pool<Sqlite>, name: &str, sql: &str) -> Result<()> {
    // Remove comments and split by semicolons
    let sql_without_comments: String = sql
        .lines()
        .filter(|line| !line.trim().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    for statement in sql_without_comments.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement)
                .execute(pool)
                .await
                .map_err(|e| Error::MigrationError(format!("{}: {}: {}", name, e, statement)))?;
        }
    }

    Ok(())
}

/// Apply one ladder record on an open connection (inside the caller's
/// transaction). The internalize path records a mined proof through it.
pub(crate) async fn record_broadcast_status_on(
    conn: &mut SqliteConnection,
    txid: &str,
    provider: &str,
    status: &str,
) -> Result<()> {
    let incoming = BroadcastStatus::parse(status)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown broadcast status '{}'", status)))?;
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT status FROM broadcast_seen WHERE txid = ? AND provider = ?")
            .bind(txid)
            .bind(provider)
            .fetch_optional(&mut *conn)
            .await?;
    let existing = existing.and_then(|(s,)| BroadcastStatus::parse(&s));
    match ladder_step(existing, incoming) {
        LadderStep::Keep => {}
        LadderStep::Refresh => {
            sqlx::query(
                "UPDATE broadcast_seen SET seen_at = CURRENT_TIMESTAMP \
                 WHERE txid = ? AND provider = ?",
            )
            .bind(txid)
            .bind(provider)
            .execute(&mut *conn)
            .await?;
        }
        LadderStep::Set(status) => {
            sqlx::query(
                "INSERT INTO broadcast_seen (txid, provider, status, seen_at) \
                 VALUES (?, ?, ?, CURRENT_TIMESTAMP) \
                 ON CONFLICT(txid, provider) DO UPDATE SET \
                 status = excluded.status, seen_at = CURRENT_TIMESTAMP",
            )
            .bind(txid)
            .bind(provider)
            .bind(status.as_str())
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}

/// [`BroadcastMemory`] over a `StorageSqlx` pool.
///
/// The schema is ensured lazily, once per instance, before the first read
/// or write, so a database opened by any path (including one created before
/// migration 002) has the tables. Reads are one indexed query per call
/// (`txid IN (...)`, chunked).
pub struct SqlxBroadcastMemory {
    pool: Pool<Sqlite>,
    schema: OnceCell<()>,
}

impl SqlxBroadcastMemory {
    /// Wrap a pool.
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self {
            pool,
            schema: OnceCell::new(),
        }
    }

    /// Apply migration 002 once per instance (idempotent `IF NOT EXISTS`).
    pub async fn ensure_schema(&self) -> Result<()> {
        self.schema
            .get_or_try_init(|| async {
                apply_migration_sql(
                    &self.pool,
                    MIGRATION_002_BROADCAST_SEEN_NAME,
                    MIGRATION_002_BROADCAST_SEEN_SQL,
                )
                .await
            })
            .await
            .map(|_| ())
    }

    /// Every row of `txids` recorded under `provider` (plus the network
    /// rows), or under anyone when `provider` is `None`.
    async fn records_query(
        &self,
        provider: Option<&str>,
        txids: &[String],
    ) -> Result<Vec<BroadcastSeenRecord>> {
        let mut out = Vec::new();
        if txids.is_empty() {
            return Ok(out);
        }
        self.ensure_schema().await?;

        for chunk in txids.chunks(IN_CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql = match provider {
                Some(_) => format!(
                    "SELECT txid, provider, status, CAST(seen_at AS TEXT) AS seen_at \
                     FROM broadcast_seen WHERE provider IN (?, ?) AND txid IN ({})",
                    placeholders
                ),
                None => format!(
                    "SELECT txid, provider, status, CAST(seen_at AS TEXT) AS seen_at \
                     FROM broadcast_seen WHERE txid IN ({})",
                    placeholders
                ),
            };
            let mut query = sqlx::query(&sql);
            if let Some(provider) = provider {
                query = query.bind(provider).bind(BROADCAST_PROVIDER_NETWORK);
            }
            for txid in chunk {
                query = query.bind(txid);
            }
            for row in query.fetch_all(&self.pool).await? {
                let seen_at: String = row.get("seen_at");
                out.push(BroadcastSeenRecord {
                    txid: row.get("txid"),
                    provider: row.get("provider"),
                    status: row.get("status"),
                    seen_at: parse_db_timestamp(&seen_at),
                });
            }
        }

        Ok(out)
    }

    /// Every row for `txid` turns `rejected` (except `mined`), and the
    /// network pseudo-provider gets a `rejected` row of its own so no
    /// provider ever skips the txid again. The retire paths call this.
    pub async fn mark_rejected(&self, txid: &str) -> Result<()> {
        self.ensure_schema().await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE broadcast_seen SET status = ?, seen_at = CURRENT_TIMESTAMP \
             WHERE txid = ? AND status NOT IN ('mined', 'rejected')",
        )
        .bind(BROADCAST_STATUS_REJECTED)
        .bind(txid)
        .execute(&mut *tx)
        .await?;
        record_broadcast_status_on(
            &mut tx,
            txid,
            BROADCAST_PROVIDER_NETWORK,
            BROADCAST_STATUS_REJECTED,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl BroadcastMemory for SqlxBroadcastMemory {
    async fn broadcast_records(
        &self,
        provider: Option<&str>,
        txids: &[String],
    ) -> Result<Vec<BroadcastSeenRecord>> {
        self.records_query(provider, txids).await
    }

    async fn record_broadcast_status(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
    ) -> Result<()> {
        self.ensure_schema().await?;
        let mut tx = self.pool.begin().await?;
        record_broadcast_status_on(&mut tx, txid, provider, status).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_broadcast_status_many(
        &self,
        provider: &str,
        status: &str,
        txids: &[String],
    ) -> Result<()> {
        if txids.is_empty() {
            return Ok(());
        }
        self.ensure_schema().await?;
        let mut tx = self.pool.begin().await?;
        for txid in txids {
            record_broadcast_status_on(&mut tx, txid, provider, status).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_broadcast_pref(&self, key: &str) -> Result<Option<String>> {
        self.ensure_schema().await?;
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM broadcast_prefs WHERE key = ?")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(value,)| value))
    }

    async fn set_broadcast_pref(&self, key: &str, value: &str) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            "INSERT INTO broadcast_prefs (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl StorageSqlx {
    /// Make sure the broadcast tables exist (migration 002; idempotent).
    /// Called on `make_available` and `migrate`.
    pub(crate) async fn ensure_broadcast_schema(&self) -> Result<()> {
        self.sqlx_broadcast_memory().ensure_schema().await
    }

    /// Record `status` for `(txid, provider)` through the ladder
    /// ([`crate::services::ladder_step`]): idempotent, positives never
    /// downgraded, `mined` terminal.
    pub async fn record_broadcast_status(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
    ) -> Result<()> {
        self.sqlx_broadcast_memory()
            .record_broadcast_status(txid, provider, status)
            .await
    }

    /// Alias of [`StorageSqlx::record_broadcast_status`] (0.3.56 name).
    pub async fn record_broadcast_seen(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
    ) -> Result<()> {
        self.record_broadcast_status(txid, provider, status).await
    }

    /// [`StorageSqlx::record_broadcast_status`] that logs instead of
    /// failing: the memory is an optimization, never a reason for a storage
    /// write to fail.
    pub(crate) async fn record_broadcast_status_quiet(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
    ) {
        if let Err(e) = self.record_broadcast_status(txid, provider, status).await {
            tracing::warn!(
                txid = %txid,
                provider = %provider,
                status = %status,
                error = %e,
                "broadcast_seen: record failed"
            );
        }
    }

    /// Every memory row of `txid` turns `rejected` (except `mined`) and the
    /// network gets a `rejected` row. Logged, never failing.
    pub(crate) async fn mark_broadcast_rejected_quiet(&self, txid: &str) {
        if let Err(e) = self.sqlx_broadcast_memory().mark_rejected(txid).await {
            tracing::warn!(txid = %txid, error = %e, "broadcast_seen: mark rejected failed");
        }
    }

    /// The subset of `txids` that `provider` may skip: its own or the
    /// network's `seen` / `mined` rows, not vetoed by its own negative row.
    pub async fn broadcast_seen_for(
        &self,
        provider: &str,
        txids: &[String],
    ) -> Result<HashSet<String>> {
        self.sqlx_broadcast_memory()
            .broadcast_seen_for(provider, txids)
            .await
    }

    /// The subset of `txids` with network evidence from any provider.
    pub async fn broadcast_seen_any(&self, txids: &[String]) -> Result<HashSet<String>> {
        self.sqlx_broadcast_memory().broadcast_seen_any(txids).await
    }

    /// Every memory row of `txids` for `provider` (plus the network's), or
    /// of every provider when `None`.
    pub async fn broadcast_records(
        &self,
        provider: Option<&str>,
        txids: &[String],
    ) -> Result<Vec<BroadcastSeenRecord>> {
        self.sqlx_broadcast_memory()
            .broadcast_records(provider, txids)
            .await
    }

    /// The memory row for `(txid, provider)`, if any.
    pub async fn broadcast_status_of(
        &self,
        txid: &str,
        provider: &str,
    ) -> Result<Option<BroadcastSeenRecord>> {
        self.sqlx_broadcast_memory()
            .broadcast_status_of(txid, provider)
            .await
    }

    /// Read a broadcast preference.
    pub async fn get_broadcast_pref(&self, key: &str) -> Result<Option<String>> {
        self.sqlx_broadcast_memory().get_broadcast_pref(key).await
    }

    /// Write a broadcast preference.
    pub async fn set_broadcast_pref(&self, key: &str, value: &str) -> Result<()> {
        self.sqlx_broadcast_memory()
            .set_broadcast_pref(key, value)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::broadcast_memory::{
        BROADCAST_STATUS_ACCEPTED, BROADCAST_STATUS_MINED, BROADCAST_STATUS_SEEN,
        BROADCAST_STATUS_UNKNOWN, PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_ARCADE_V2,
        PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC,
    };
    use crate::storage::traits::{MonitorStorage, WalletStorageProvider, WalletStorageWriter};

    async fn table_exists(pool: &Pool<Sqlite>, table: &str) -> bool {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_optional(pool)
                .await
                .unwrap();
        row.is_some()
    }

    async fn migrated_in_memory() -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-broadcast-seen", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        storage
    }

    async fn row_of(storage: &StorageSqlx, txid: &str, provider: &str) -> (String, String) {
        sqlx::query_as(
            "SELECT status, CAST(seen_at AS TEXT) FROM broadcast_seen WHERE txid = ? AND provider = ?",
        )
        .bind(txid)
        .bind(provider)
        .fetch_one(storage.pool())
        .await
        .unwrap()
    }

    #[test]
    fn db_timestamps_parse_in_every_form_this_database_writes() {
        let sqlite = parse_db_timestamp("2026-09-02 23:12:00");
        assert_eq!(sqlite.to_rfc3339(), "2026-09-02T23:12:00+00:00");
        let fractional = parse_db_timestamp("2026-09-02 23:12:00.250");
        assert_eq!(fractional.timestamp_subsec_millis(), 250);
        let rfc = parse_db_timestamp("2026-09-02T23:12:00.123456+00:00");
        assert_eq!(rfc.timestamp(), sqlite.timestamp());
        assert_eq!(
            parse_db_timestamp("garbage").timestamp(),
            0,
            "unparseable is stale"
        );
    }

    #[tokio::test]
    async fn migration_002_applies_on_open_of_an_existing_001_only_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.db");
        let path = path.to_str().unwrap().to_string();

        // A database at the pre-0.3.56 schema: migrated, then the 002
        // tables dropped again (exactly what a wallet created before this
        // release looks like on disk).
        {
            let storage = StorageSqlx::open(&path).await.unwrap();
            let version = storage
                .migrate("old-wallet", &"1".repeat(64))
                .await
                .unwrap();
            assert_eq!(version, MIGRATION_002_BROADCAST_SEEN_NAME);
            sqlx::query("DROP TABLE broadcast_seen")
                .execute(storage.pool())
                .await
                .unwrap();
            sqlx::query("DROP TABLE broadcast_prefs")
                .execute(storage.pool())
                .await
                .unwrap();
            assert!(!table_exists(storage.pool(), "broadcast_seen").await);
            storage.pool().close().await;
        }

        // Re-open the way every CLI command does: open + make_available,
        // no migrate. The tables are back.
        let storage = StorageSqlx::open(&path).await.unwrap();
        assert!(!table_exists(storage.pool(), "broadcast_seen").await);
        storage.make_available().await.unwrap();
        assert!(table_exists(storage.pool(), "broadcast_seen").await);
        assert!(table_exists(storage.pool(), "broadcast_prefs").await);

        // And the memory works on it.
        let txid = "ab".repeat(32);
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        let seen = storage
            .broadcast_seen_for(PROVIDER_TAAL_ARC, std::slice::from_ref(&txid))
            .await
            .unwrap();
        assert!(seen.contains(&txid));

        // Running the full migration again on the upgraded database is a
        // no-op, not an error.
        let version = storage
            .migrate("old-wallet", &"1".repeat(64))
            .await
            .unwrap();
        assert_eq!(version, MIGRATION_002_BROADCAST_SEEN_NAME);
        storage.pool().close().await;
    }

    #[tokio::test]
    async fn record_broadcast_status_is_idempotent_and_mined_is_terminal() {
        let storage = migrated_in_memory().await;
        let txid = "cd".repeat(32);

        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        // A weaker report never downgrades.
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT provider, status FROM broadcast_seen WHERE txid = ?")
                .bind(&txid)
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![(
                PROVIDER_TAAL_ARC.to_string(),
                BROADCAST_STATUS_SEEN.to_string()
            )],
            "one row per (txid, provider), highest status"
        );

        // mined is never downgraded
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_MINED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_REJECTED)
            .await
            .unwrap();
        let (status, _) = row_of(&storage, &txid, PROVIDER_TAAL_ARC).await;
        assert_eq!(status, BROADCAST_STATUS_MINED);

        // An unknown status string is refused, not stored.
        assert!(storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, "bogus")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn negative_verdicts_persist_through_the_ladder_and_keep_the_absence_clock() {
        let storage = migrated_in_memory().await;
        let txid = "ee".repeat(32);
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_UNKNOWN)
            .await
            .unwrap();
        let (status, _) = row_of(&storage, &txid, PROVIDER_ARCADE_V2).await;
        assert_eq!(status, BROADCAST_STATUS_UNKNOWN);
        assert!(storage
            .broadcast_seen_for(PROVIDER_ARCADE_V2, std::slice::from_ref(&txid))
            .await
            .unwrap()
            .is_empty());

        // Backdate the absence, repeat it: the first observation stands.
        sqlx::query(
            "UPDATE broadcast_seen SET seen_at = datetime('now', '-45 minutes') \
             WHERE txid = ? AND provider = ?",
        )
        .bind(&txid)
        .bind(PROVIDER_ARCADE_V2)
        .execute(storage.pool())
        .await
        .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_UNKNOWN)
            .await
            .unwrap();
        let record = storage
            .broadcast_status_of(&txid, PROVIDER_ARCADE_V2)
            .await
            .unwrap()
            .expect("row");
        let age = (Utc::now() - record.seen_at).num_minutes();
        assert!(
            (44..=46).contains(&age),
            "first-absent-at kept: {} min",
            age
        );

        // A fresh seen supersedes the absence and refreshes the clock.
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        let record = storage
            .broadcast_status_of(&txid, PROVIDER_ARCADE_V2)
            .await
            .unwrap()
            .expect("row");
        assert_eq!(record.status, BROADCAST_STATUS_SEEN);
        assert!((Utc::now() - record.seen_at).num_seconds() < 5);

        // A rejection overrides seen.
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_REJECTED)
            .await
            .unwrap();
        let (status, _) = row_of(&storage, &txid, PROVIDER_ARCADE_V2).await;
        assert_eq!(status, BROADCAST_STATUS_REJECTED);
    }

    #[tokio::test]
    async fn mark_rejected_turns_every_row_and_adds_the_network_row() {
        let storage = migrated_in_memory().await;
        let txid = "ff".repeat(32);
        let mined = "fe".repeat(32);
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&txid, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&mined, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED)
            .await
            .unwrap();
        storage
            .sqlx_broadcast_memory()
            .mark_rejected(&txid)
            .await
            .unwrap();
        storage
            .sqlx_broadcast_memory()
            .mark_rejected(&mined)
            .await
            .unwrap();

        let rows = storage
            .broadcast_records(None, &[txid.clone(), mined.clone()])
            .await
            .unwrap();
        let mut statuses: Vec<(String, String, String)> = rows
            .iter()
            .map(|r| (r.txid.clone(), r.provider.clone(), r.status.clone()))
            .collect();
        statuses.sort();
        assert_eq!(
            statuses,
            vec![
                (
                    mined.clone(),
                    BROADCAST_PROVIDER_NETWORK.to_string(),
                    BROADCAST_STATUS_MINED.to_string()
                ),
                (
                    txid.clone(),
                    PROVIDER_ARCADE_V2.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
                (
                    txid.clone(),
                    PROVIDER_TAAL_ARC.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
                (
                    txid.clone(),
                    BROADCAST_PROVIDER_NETWORK.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
            ]
        );
        assert!(storage
            .broadcast_seen_any(std::slice::from_ref(&txid))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn seen_for_credits_the_provider_and_the_network_only() {
        let storage = migrated_in_memory().await;
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let d = "dd".repeat(32);
        storage
            .record_broadcast_status(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&d, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        let all = vec![a.clone(), b.clone(), c.clone(), d.clone()];

        let taal = storage
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &all)
            .await
            .unwrap();
        assert!(taal.contains(&a) && taal.contains(&b) && !taal.contains(&c));
        assert!(!taal.contains(&d), "accepted never skips");

        let gp = storage
            .broadcast_seen_for(PROVIDER_GORILLAPOOL_ARC, &all)
            .await
            .unwrap();
        assert!(!gp.contains(&a) && gp.contains(&b) && !gp.contains(&c));

        let any = storage.broadcast_seen_any(&all).await.unwrap();
        assert_eq!(any.len(), 2);

        assert!(storage
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &[])
            .await
            .unwrap()
            .is_empty());

        // The records carry the timestamps the staleness rule reads.
        let records = storage
            .broadcast_records(Some(PROVIDER_TAAL_ARC), &all)
            .await
            .unwrap();
        assert_eq!(records.len(), 3);
        for r in &records {
            assert!((Utc::now() - r.seen_at).num_seconds() < 5, "{:?}", r);
        }
    }

    #[tokio::test]
    async fn seen_queries_chunk_long_txid_lists() {
        let storage = migrated_in_memory().await;
        let txids: Vec<String> = (0..(IN_CHUNK * 2 + 7))
            .map(|i| format!("{:064x}", i))
            .collect();
        storage
            .sqlx_broadcast_memory()
            .record_broadcast_status_many(PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN, &txids)
            .await
            .unwrap();
        let seen = storage
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &txids)
            .await
            .unwrap();
        assert_eq!(seen.len(), txids.len());
        let any = storage.broadcast_seen_any(&txids).await.unwrap();
        assert_eq!(any.len(), txids.len());
    }

    #[tokio::test]
    async fn prefs_upsert() {
        let storage = migrated_in_memory().await;
        assert!(storage
            .get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
            .await
            .unwrap()
            .is_none());
        storage
            .set_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_TAAL_ARC)
            .await
            .unwrap();
        storage
            .set_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_GORILLAPOOL_ARC)
            .await
            .unwrap();
        assert_eq!(
            storage
                .get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
                .await
                .unwrap()
                .as_deref(),
            Some(PROVIDER_GORILLAPOOL_ARC)
        );
    }

    #[tokio::test]
    async fn storage_exposes_its_memory_and_seen_on_network_records_the_plane() {
        let storage = migrated_in_memory().await;
        let memory = WalletStorageProvider::broadcast_memory(&storage).expect("sqlx memory");

        let txid = "ef".repeat(32);
        // No tx rows: nothing to update, but the plane still gets credited.
        let updated = storage
            .mark_transaction_seen_on_network_by(&txid, PROVIDER_GORILLAPOOL_ARC)
            .await
            .unwrap();
        assert!(!updated);
        let seen = memory
            .broadcast_seen_for(PROVIDER_GORILLAPOOL_ARC, std::slice::from_ref(&txid))
            .await
            .unwrap();
        assert!(seen.contains(&txid));
        assert!(memory
            .broadcast_seen_for(PROVIDER_TAAL_ARC, std::slice::from_ref(&txid))
            .await
            .unwrap()
            .is_empty());

        // The provider-less variant credits the network (every provider).
        let other = "ee".repeat(32);
        storage
            .mark_transaction_seen_on_network(&other)
            .await
            .unwrap();
        assert!(memory
            .broadcast_seen_for(PROVIDER_TAAL_ARC, std::slice::from_ref(&other))
            .await
            .unwrap()
            .contains(&other));
    }
}
