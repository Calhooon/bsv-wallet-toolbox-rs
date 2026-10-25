//! SQLx-based storage provider implementation.
//!
//! This module provides a storage backend using SQLx with SQLite support.
//! It implements the `WalletStorageProvider` trait hierarchy.

use super::{
    abort_action, broadcast_seen, create_action, internalize_action, monitor_state, process_action,
    proof_root_checks, sync, BroadcastOutcome,
};
mod baskets;
mod beef_compaction;
#[cfg(test)]
mod certificate_tests;
mod certificates;
#[cfg(test)]
mod commission_tests;
mod commissions;
mod competitors;
#[cfg(test)]
mod core_tests;
mod labels;
mod list_outputs;
mod maintenance;
mod monitor;
#[cfg(test)]
mod monitor_event_tests;
mod monitor_events;
mod output_selection;
mod outputs;
#[cfg(test)]
mod proof_request_tests;
mod proof_requests;
mod proven_tx_reqs;
mod proven_txs;
mod reader;
mod retirement;
mod send_waiting;
mod settings;
mod stale_proofs;
mod sync_state;
mod tags;
#[cfg(test)]
mod transaction_tests;
mod transactions;
mod unfail;
#[cfg(test)]
mod unfail_empty_tests;
#[cfg(test)]
mod unfail_tests;
#[cfg(test)]
mod user_tests;
mod users;
#[cfg(test)]
mod validation_tests;
mod writer;
pub use proven_tx_reqs::{UnprovenAdoption, ADOPT_UNPROVEN_TX_LIMIT};
pub(crate) use proven_txs::{
    bump_leaf_index, store_validated_proof_on, StoreProofOutcome, ValidatedProofRow,
};
pub(crate) use retirement::RetireBasis;
pub use retirement::RetireOutcome;
use send_waiting::append_req_history_note;
pub(super) use stale_proofs::demote_stale_proof_on;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::locked_inputs::{LOCKED_VERDICT_SPENT_PROVEN, LOCKED_VERDICT_UNKNOWN};
use crate::error::{Error, Result, StoreBusyReason};
use crate::lock_utils::{lock_read, lock_write};
use crate::services::{GetRawTxResult, UtxoVerdict, WalletServices};
use crate::storage::entities::*;
use crate::storage::traits::*;

use crate::storage::broadcast::validate_beef_for_broadcast;
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, InternalizeActionArgs, ListActionsArgs, ListActionsResult,
    ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs, ListOutputsResult,
    RelinquishCertificateArgs, RelinquishOutputArgs,
};

use super::create_action::{
    beef_bfs_walk, parse_input_txids, prune_beef_to_roots, rebuild_beef_for_broadcast,
};

/// Every schema migration, in order. Each statement is `IF NOT EXISTS`, so
/// the whole list is idempotent on a database at any earlier schema.
const MIGRATIONS: &[(&str, &str)] = &[
    ("001_initial", include_str!("../migrations/001_initial.sql")),
    (
        super::broadcast_seen::MIGRATION_002_BROADCAST_SEEN_NAME,
        super::broadcast_seen::MIGRATION_002_BROADCAST_SEEN_SQL,
    ),
    (
        super::locked_inputs::MIGRATION_003_LOCKED_INPUT_CHECKS_NAME,
        super::locked_inputs::MIGRATION_003_LOCKED_INPUT_CHECKS_SQL,
    ),
    (
        super::monitor_state::MIGRATION_004_MONITOR_STATE_NAME,
        super::monitor_state::MIGRATION_004_MONITOR_STATE_SQL,
    ),
    (
        super::proof_root_checks::MIGRATION_005_PROOF_ROOT_CHECKS_NAME,
        super::proof_root_checks::MIGRATION_005_PROOF_ROOT_CHECKS_SQL,
    ),
];

/// Connections in the reader pool (sqlx's default pool size).
const READER_POOL_SIZE: u32 = 10;

/// The store's ceiling on waiting for the write lock, in milliseconds: the
/// `busy_timeout` a writer waits on another process's lock, and the writer
/// pool's `acquire_timeout` a caller in this process waits for the one writer
/// connection. Past it the call answers `Error::StoreBusy` having written
/// nothing.
pub const STORE_BUSY_CEILING_MS: u64 = 5000;

/// `BEGIN IMMEDIATE` on `pool`: the one sanctioned way to open a transaction
/// (`clippy.toml` disallows `Pool::begin` and `Pool::begin_with` elsewhere).
/// `pool` is a writer pool: a `PoolTimedOut` here is the writer not acquired
/// within the store's ceiling, `StoreBusy` (`WriterNotAcquired`).
#[allow(clippy::disallowed_methods)]
pub(crate) async fn begin_immediate(
    pool: &SqlitePool,
) -> Result<sqlx::Transaction<'static, Sqlite>> {
    pool.begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(write_tx_error)
}

/// Commits a write transaction (`write_tx`): a busy answer is `StoreBusy`,
/// any other failure `DatabaseError` (review 2 F4 of #44).
pub(crate) async fn commit(tx: sqlx::Transaction<'_, Sqlite>) -> Result<()> {
    tx.commit().await.map_err(write_tx_error)
}

/// A begin's or a commit's error: the writer not acquired within the
/// store's ceiling is `StoreBusy` (`WriterNotAcquired`), SQLite's busy is
/// `StoreBusy` (`SqliteBusy`), anything else `DatabaseError` with the error's
/// text, the `Database error:` these sites answered before #44 (a host's
/// 500, not the `SQLx error:` of the plain conversion).
fn write_tx_error(err: sqlx::Error) -> Error {
    if let sqlx::Error::PoolTimedOut = err {
        return Error::StoreBusy {
            reason: StoreBusyReason::WriterNotAcquired,
        };
    }
    match crate::error::sqlite_busy_code(&err) {
        Some(code) => Error::StoreBusy {
            reason: StoreBusyReason::SqliteBusy { code },
        },
        None => Error::DatabaseError(err.to_string()),
    }
}

/// The writer connection of `writer` (a writer pool) for single-statement
/// writes, acquired at the site so that its `PoolTimedOut` is
/// `StoreBusy` (`WriterNotAcquired`): the writer not acquired within the store's
/// ceiling, nothing written. Only the writer's timeout is busy; a reader-pool
/// timeout converts to the plain SQLx error.
pub(crate) async fn acquire_writer(
    writer: &SqlitePool,
) -> Result<sqlx::pool::PoolConnection<Sqlite>> {
    writer.acquire().await.map_err(writer_acquire_error)
}

/// A writer acquire's error: `PoolTimedOut` is `StoreBusy` (`WriterNotAcquired`), any
/// other error converts as every SQLx error does.
fn writer_acquire_error(err: sqlx::Error) -> Error {
    match err {
        sqlx::Error::PoolTimedOut => Error::StoreBusy {
            reason: StoreBusyReason::WriterNotAcquired,
        },
        other => other.into(),
    }
}

/// Default maximum length for output scripts stored in the outputs table.
/// Scripts longer than this will be retrieved from the raw transaction.
pub const DEFAULT_MAX_OUTPUT_SCRIPT: i32 = 10_000;

/// SQLite storage provider.
///
/// Implements the full `WalletStorageProvider` trait hierarchy using SQLx
/// with SQLite as the backend.
///
/// One store per file per process (bsv-wallet-cli #44). A clone is a second
/// handle over the same store: the same writer pool (one connection), the
/// same reader pool, the same in-flight pick reservations, the same settings,
/// identity, chain tracker, services, broadcast memory and trx tokens; a
/// change made through one handle is seen through every other. Give a
/// `Wallet` and a `Monitor` (or two `Wallet`s) clones of one store. Two
/// `StorageSqlx::new` over one file are two stores: two writer pools whose
/// writers meet in SQLite and wait on its busy handler (`busy_timeout`, then
/// `StoreBusy`) instead of queueing on one writer, and two reservation sets.
pub struct StorageSqlx {
    /// The reader pool: several connections, each `PRAGMA query_only = ON`,
    /// so a write sent here fails loudly instead of racing the writer.
    reader: Pool<Sqlite>,
    /// The writer pool: exactly one connection. Every write, and every
    /// write transaction (`write_tx`, `BEGIN IMMEDIATE`), runs here, so two
    /// writers in this process queue for the connection and never meet in
    /// SQLite (toolbox cli#44: `SQLITE_BUSY_SNAPSHOT`, 517).
    writer: Pool<Sqlite>,
    /// Cached settings (loaded on make_available).
    settings: Arc<std::sync::RwLock<Option<TableSettings>>>,
    /// Storage identity key (set during migration).
    storage_identity_key: Arc<std::sync::RwLock<String>>,
    /// Storage name (set during migration).
    storage_name: Arc<std::sync::RwLock<String>>,
    /// Optional ChainTracker for BEEF and proof verification.
    /// When set, create_action will verify BEEF merkle roots against the chain.
    /// When NOT set, no merkle proof is ever stored as proven (P0-1).
    chain_tracker: Arc<RwLock<Option<Arc<dyn ChainTracker>>>>,
    /// Optional WalletServices for blockchain operations.
    /// When set, storage can perform BEEF verification, broadcast transactions,
    /// validate UTXOs, and look up block headers.
    services: Arc<std::sync::RwLock<Option<Arc<dyn WalletServices>>>>,
    /// Active transaction tokens for TrxToken scope tracking.
    active_transactions: Arc<RwLock<HashMap<u64, ()>>>,
    /// In-memory task locks for multi-instance monitor support.
    /// Maps task_name -> (instance_id, expiry).
    #[allow(dead_code)]
    task_locks: Arc<RwLock<HashMap<String, (String, std::time::Instant)>>>,
    /// Persisted broadcast acceptance memory over the same pool (0.3.56):
    /// the `broadcast_seen` / `broadcast_prefs` tables.
    broadcast_memory: Arc<super::broadcast_seen::SqlxBroadcastMemory>,
    /// The change coins `createAction` pre-checks have picked in this
    /// process and not yet returned from (review 2 F1 of bsv-wallet-cli
    /// #44): a pre-check's dry run picks none of them.
    in_flight_picks: Arc<super::create_action::InFlightPicks>,
    /// Tests only: the next process step answers busy at its first write,
    /// inside its write transaction.
    #[cfg(test)]
    busy_next_process_action: Arc<std::sync::atomic::AtomicBool>,
    /// Tests only: the next status write after a broadcast answers busy
    /// before writing.
    #[cfg(test)]
    busy_next_status_write: Arc<std::sync::atomic::AtomicBool>,
    /// Tests only: the next descendants' retirement answers busy before
    /// writing.
    #[cfg(test)]
    busy_next_descendant_retire: Arc<std::sync::atomic::AtomicBool>,
    /// Tests only: the next certificate field insert answers busy, inside
    /// its write transaction.
    #[cfg(test)]
    busy_next_certificate_field: Arc<std::sync::atomic::AtomicU32>,
    /// Tests only: the next poisoned retirement is cut between its req
    /// write and its status write.
    #[cfg(test)]
    cut_next_retirement: Arc<std::sync::atomic::AtomicBool>,
}

/// A second handle over the same store: every field is shared, none copied
/// (`Pool` is an `Arc` inside; every other field is an `Arc`). The tests'
/// busy hooks are shared too: a hook armed through one handle fires at the
/// next matching step through any. `destroy()` through any handle drops the
/// tables of the one store, for every handle.
impl Clone for StorageSqlx {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            writer: self.writer.clone(),
            settings: Arc::clone(&self.settings),
            storage_identity_key: Arc::clone(&self.storage_identity_key),
            storage_name: Arc::clone(&self.storage_name),
            chain_tracker: Arc::clone(&self.chain_tracker),
            services: Arc::clone(&self.services),
            active_transactions: Arc::clone(&self.active_transactions),
            task_locks: Arc::clone(&self.task_locks),
            broadcast_memory: Arc::clone(&self.broadcast_memory),
            in_flight_picks: Arc::clone(&self.in_flight_picks),
            #[cfg(test)]
            busy_next_process_action: Arc::clone(&self.busy_next_process_action),
            #[cfg(test)]
            busy_next_status_write: Arc::clone(&self.busy_next_status_write),
            #[cfg(test)]
            busy_next_descendant_retire: Arc::clone(&self.busy_next_descendant_retire),
            #[cfg(test)]
            busy_next_certificate_field: Arc::clone(&self.busy_next_certificate_field),
            #[cfg(test)]
            cut_next_retirement: Arc::clone(&self.cut_next_retirement),
        }
    }
}

impl StorageSqlx {
    /// Create a new SQLite storage provider.
    ///
    /// # Arguments
    ///
    /// * `database_url` - SQLite database URL (e.g., "sqlite:wallet.db" or "sqlite::memory:")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let storage = StorageSqlx::new("sqlite:wallet.db").await?;
    /// ```
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::connect(database_url, READER_POOL_SIZE, None).await
    }

    /// The store over `database_url` with a reader pool of `reader_size`
    /// connections and, when given, that reader acquire timeout (sqlx's
    /// default otherwise). The writer is always one connection with the
    /// store's ceiling as its acquire timeout.
    async fn connect(
        database_url: &str,
        reader_size: u32,
        reader_acquire_timeout: Option<std::time::Duration>,
    ) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| Error::DatabaseError(e.to_string()))?
            .pragma("foreign_keys", "ON")
            .pragma("journal_mode", "WAL")
            .pragma("busy_timeout", STORE_BUSY_CEILING_MS.to_string())
            .pragma("synchronous", "NORMAL")
            .create_if_missing(true);
        // The writer first: it creates the file and sets WAL before any
        // query-only reader opens it. Both pools share the one options value,
        // so `sqlite::memory:` names one shared in-memory database for both.
        // The writer's acquire timeout is the store's ceiling on purpose: a
        // caller that cannot get the writer within it answers StoreBusy.
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(STORE_BUSY_CEILING_MS))
            .connect_with(options.clone())
            .await?;
        let mut reader = SqlitePoolOptions::new().max_connections(reader_size);
        if let Some(timeout) = reader_acquire_timeout {
            reader = reader.acquire_timeout(timeout);
        }
        let reader = reader
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("PRAGMA query_only = ON")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        let broadcast_memory = Arc::new(super::broadcast_seen::SqlxBroadcastMemory::with_pools(
            reader.clone(),
            writer.clone(),
        ));

        Ok(Self {
            reader,
            writer,
            settings: Default::default(),
            storage_identity_key: Default::default(),
            storage_name: Default::default(),
            chain_tracker: Default::default(),
            services: Default::default(),
            active_transactions: Default::default(),
            task_locks: Default::default(),
            broadcast_memory,
            in_flight_picks: Default::default(),
            #[cfg(test)]
            busy_next_process_action: Default::default(),
            #[cfg(test)]
            busy_next_status_write: Default::default(),
            #[cfg(test)]
            busy_next_descendant_retire: Default::default(),
            #[cfg(test)]
            busy_next_certificate_field: Default::default(),
            #[cfg(test)]
            cut_next_retirement: Default::default(),
        })
    }

    /// Cache `settings`, shared by every handle of this store. Readers get
    /// owned copies (`get_settings`), so replacing it frees nothing anyone
    /// holds.
    fn cache_settings(&self, settings: &TableSettings) -> Result<()> {
        *lock_write(&self.settings)? = Some(settings.clone());
        Ok(())
    }

    /// Set the ChainTracker for BEEF and proof verification.
    ///
    /// When set, `create_action` will verify BEEF merkle roots against the chain
    /// before returning. This matches TypeScript/Go behavior. A storage with no
    /// tracker stores no merkle proof: every ingest answers
    /// [`ProofIngestOutcome::TrackerUnavailable`] and the transaction stays
    /// unproven (P0-1). Wire one before the first proof can arrive.
    ///
    /// # Arguments
    /// * `tracker` - The chain tracker to use for verification
    ///
    /// # Example
    /// ```rust,ignore
    /// let storage = StorageSqlx::in_memory().await?;
    /// storage.set_chain_tracker(Arc::new(my_chaintracks)).await;
    /// ```
    pub async fn set_chain_tracker(&self, tracker: Arc<dyn ChainTracker>) {
        let mut ct = self.chain_tracker.write().await;
        *ct = Some(tracker);
    }

    /// Clear the ChainTracker (disable BEEF verification; from then on no
    /// merkle proof is stored).
    pub async fn clear_chain_tracker(&self) {
        let mut ct = self.chain_tracker.write().await;
        *ct = None;
    }

    /// Get a reference to the current ChainTracker, if set.
    pub(crate) async fn get_chain_tracker(&self) -> Option<Arc<dyn ChainTracker>> {
        let ct = self.chain_tracker.read().await;
        ct.clone()
    }

    /// Open an in-memory SQLite database.
    ///
    /// Useful for testing.
    pub async fn in_memory() -> Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Open a file-based SQLite database.
    ///
    /// Creates the database file if it doesn't exist.
    pub async fn open(path: &str) -> Result<Self> {
        let url = format!("sqlite:{}", path);
        Self::new(&url).await
    }

    /// The reader pool. Every connection is `PRAGMA query_only = ON`: a
    /// write sent here fails (`attempt to write a readonly database`). Writes
    /// go to [`writer`](Self::writer) or [`write_tx`](Self::write_tx).
    pub fn pool(&self) -> &SqlitePool {
        &self.reader
    }

    /// Tests only: the next process step (`process_action`, or the one inside
    /// `createAction`'s door) answers `StoreBusy { SqliteBusy { code: 5 } }`
    /// at its first write, inside its write transaction, which rolls back.
    #[cfg(test)]
    pub(crate) fn busy_next_process_action(&self) {
        self.busy_next_process_action
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests only: takes the busy `busy_next_process_action` armed.
    #[cfg(test)]
    pub(crate) fn take_busy_next_process_action(&self) -> bool {
        self.busy_next_process_action
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Tests only: the next status write after a broadcast
    /// (`update_transaction_status_after_broadcast`, or internalizeAction's
    /// req advance to `unmined`) answers busy, as a store held by another
    /// process would.
    #[cfg(test)]
    pub(crate) fn busy_next_status_write(&self) {
        self.busy_next_status_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests only: takes the busy `busy_next_status_write` armed.
    #[cfg(test)]
    pub(crate) fn take_busy_next_status_write(&self) -> bool {
        self.busy_next_status_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Tests only: the next `retire_poisoned_descendants` answers busy
    /// before its first write, as a store held by another process would.
    #[cfg(test)]
    pub(crate) fn busy_next_descendant_retire(&self) {
        self.busy_next_descendant_retire
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests only: takes the busy `busy_next_descendant_retire` armed.
    #[cfg(test)]
    pub(crate) fn take_busy_next_descendant_retire(&self) -> bool {
        self.busy_next_descendant_retire
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Tests only: the next certificate field insert
    /// (`insert_certificate_with_fields`) answers busy, inside the write
    /// transaction that inserted the certificate, which rolls back.
    #[cfg(test)]
    pub(crate) fn busy_next_certificate_field(&self) {
        self.busy_next_certificate_fields(1);
    }

    /// Tests only: the next `times` certificate field inserts answer busy,
    /// one per `insert_certificate_with_fields` call.
    #[cfg(test)]
    pub(crate) fn busy_next_certificate_fields(&self, times: u32) {
        self.busy_next_certificate_field
            .store(times, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests only: takes one of the busies `busy_next_certificate_fields`
    /// armed.
    #[cfg(test)]
    pub(crate) fn take_busy_next_certificate_field(&self) -> bool {
        // A compare-exchange loop rather than `fetch_update`: the newest
        // stable deprecates `fetch_update` for `try_update`, which the
        // crate's oldest supported toolchain does not have.
        use std::sync::atomic::Ordering::SeqCst;
        let mut n = self.busy_next_certificate_field.load(SeqCst);
        loop {
            let Some(next) = n.checked_sub(1) else {
                return false;
            };
            match self
                .busy_next_certificate_field
                .compare_exchange(n, next, SeqCst, SeqCst)
            {
                Ok(_) => return true,
                Err(now) => n = now,
            }
        }
    }

    /// Tests only: the next `retire_one_poisoned` stops between its first
    /// write (the req) and its second (the transaction's status) with a
    /// busy, as a process dying there would leave it.
    #[cfg(test)]
    pub(crate) fn cut_next_retirement(&self) {
        self.cut_next_retirement
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Tests only: takes the cut `cut_next_retirement` armed.
    #[cfg(test)]
    pub(crate) fn take_cut_next_retirement(&self) -> bool {
        self.cut_next_retirement
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// The in-flight pre-check picks of this process (`createAction`).
    pub(crate) fn in_flight_picks(&self) -> &Arc<super::create_action::InFlightPicks> {
        &self.in_flight_picks
    }

    /// The writer pool, exactly one connection: for single-statement writes.
    /// Never hold it (or a [`write_tx`](Self::write_tx)) across a service or
    /// chain-tracker await.
    pub fn writer(&self) -> &SqlitePool {
        &self.writer
    }

    /// Open a write transaction on the writer with `BEGIN IMMEDIATE`: the
    /// write lock is taken before the first read, so a contender (another
    /// process on the same file) waits `busy_timeout` instead of failing
    /// with `SQLITE_BUSY_SNAPSHOT`, and one in this process waits for the
    /// writer connection. The only way this crate opens a transaction.
    pub async fn write_tx(&self) -> Result<sqlx::Transaction<'static, Sqlite>> {
        begin_immediate(&self.writer).await
    }

    /// `create_action` with the host's offer (bsv-wallet-cli #44): the change
    /// coins are picked from the output ids in `offer` only (the dry-run pick
    /// and the locked set both); an offer that does not cover the action, or
    /// an empty one, refuses `InsufficientFunds`. The storage-level
    /// `Wallet::create_action_offering`.
    pub async fn create_action_offering(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
        offer: &[i64],
    ) -> Result<StorageCreateActionResult> {
        self.create_action_at_door(
            auth,
            args,
            crate::storage::CreateActionDoor {
                offer: Some(offer),
                sign_and_process: None,
            },
        )
        .await
        .map(|done| done.created)
        .map_err(crate::storage::DoorRefusal::into_error)
    }

    /// The writer connection for a single-statement write: see
    /// [`acquire_writer`]. Hold it for the one statement only.
    pub(crate) async fn writer_conn(&self) -> Result<sqlx::pool::PoolConnection<Sqlite>> {
        acquire_writer(&self.writer).await
    }

    /// The persisted broadcast acceptance memory over this storage's pool
    /// (`broadcast_seen` / `broadcast_prefs`). A shared handle: the same
    /// instance the wallet and the monitor attach to the services.
    pub fn sqlx_broadcast_memory(&self) -> Arc<super::broadcast_seen::SqlxBroadcastMemory> {
        Arc::clone(&self.broadcast_memory)
    }

    /// Run every migration, in order (see [`MIGRATIONS`]). Idempotent.
    async fn run_migrations(&self) -> Result<()> {
        for (name, sql) in MIGRATIONS {
            super::broadcast_seen::apply_migration_sql(&self.writer, name, sql).await?;
        }
        Ok(())
    }
}

use crate::storage::traits::{MonitorStorage, ProofIngestOutcome, TxSynchronizedStatus};
use std::time::Duration;

impl StorageSqlx {
    /// Get admin statistics about the storage.
    ///
    /// Returns counts of users, transactions, outputs, certificates, etc.
    pub async fn admin_stats(&self, _admin_identity_key: &str) -> Result<AdminStatsResult> {
        let users: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(self.pool())
            .await?;
        let transactions: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
            .fetch_one(self.pool())
            .await?;
        let outputs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outputs")
            .fetch_one(self.pool())
            .await?;
        let certificates: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM certificates WHERE is_deleted = 0")
                .fetch_one(self.pool())
                .await?;
        let proven_txs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_txs")
            .fetch_one(self.pool())
            .await?;
        let proven_tx_reqs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_tx_reqs")
            .fetch_one(self.pool())
            .await?;

        Ok(AdminStatsResult {
            users: users.0 as u32,
            transactions: transactions.0 as u32,
            outputs: outputs.0 as u32,
            certificates: certificates.0 as u32,
            proven_txs: proven_txs.0 as u32,
            proven_tx_reqs: proven_tx_reqs.0 as u32,
        })
    }
}

/// M19 R1: is a freshly validated proof the SAME anchor as the stored one?
///
/// A stored row written by `internalize_action` before 0.3.65 carries an
/// empty `block_hash` (the validated bump had none), so the comparison falls
/// back to height + merkle root there; with both hashes known, the hash and
/// the height decide (the root is implied by the hash).
pub(crate) fn same_proof_anchor(
    stored_hash: &str,
    stored_height: i64,
    stored_root: &str,
    new_hash: &str,
    new_height: u32,
    new_root: &str,
) -> bool {
    if stored_height != new_height as i64 {
        return false;
    }
    if !stored_hash.is_empty() && !new_hash.is_empty() {
        return stored_hash.eq_ignore_ascii_case(new_hash);
    }
    stored_root.eq_ignore_ascii_case(new_root)
}

/// The reference's `unprovenAttemptsLimitMain` / `unprovenAttemptsLimitTest`
/// (wallet-toolbox src/monitor/Monitor.ts:105-106). The reference writes a
/// req off once `attempts` is GREATER than the limit
/// (src/monitor/tasks/TaskCheckForProofs.ts:158); here, since 0.7.4
/// (bsv-stack-lean #66), past the limit is only a log line: a count is no
/// evidence and writes no word.
pub(crate) const PROOF_ATTEMPTS_LIMIT_MAIN: i64 = 144;
pub(crate) const PROOF_ATTEMPTS_LIMIT_TEST: i64 = 10;
