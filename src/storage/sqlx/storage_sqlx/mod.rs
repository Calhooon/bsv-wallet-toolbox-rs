//! SQLx-based storage provider implementation.
//!
//! This module provides a storage backend using SQLx with SQLite support.
//! It implements the `WalletStorageProvider` trait hierarchy.

use super::{
    abort_action, broadcast_seen, create_action, monitor_state, process_action, proof_root_checks,
    sync,
};
mod baskets;
#[cfg(test)]
mod certificate_tests;
mod certificates;
mod commissions;
mod competitors;
mod labels;
mod list_outputs;
mod maintenance;
mod monitor_events;
mod output_selection;
mod outputs;
#[cfg(test)]
mod proof_request_tests;
mod proof_requests;
mod proven_tx_reqs;
mod proven_txs;
mod retirement;
mod send_waiting;
mod settings;
mod stale_proofs;
mod sync_state;
mod tags;
mod transactions;
mod users;
pub(crate) use proof_requests::{PROOF_ATTEMPTS_LIMIT_MAIN, PROOF_ATTEMPTS_LIMIT_TEST};
pub use proven_tx_reqs::{UnprovenAdoption, ADOPT_UNPROVEN_TX_LIMIT};
pub(crate) use proven_txs::{
    bump_leaf_index, same_proof_anchor, store_validated_proof_on, StoreProofOutcome,
    ValidatedProofRow,
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

// =============================================================================
// WalletStorageReader Implementation
// =============================================================================

#[async_trait]
impl WalletStorageReader for StorageSqlx {
    fn is_available(&self) -> bool {
        lock_read(&self.settings)
            .map(|s| s.is_some())
            .unwrap_or(false)
    }

    fn get_settings(&self) -> TableSettings {
        // A copy taken under the lock; the defaults before make_available()
        // (or on a poisoned lock).
        lock_read(&self.settings)
            .ok()
            .and_then(|settings| settings.clone())
            .unwrap_or_default()
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        let guard = lock_read(&self.services)?;
        guard.clone().ok_or_else(|| {
            Error::InvalidOperation(
                "Must setServices first. Services are required for blockchain operations."
                    .to_string(),
            )
        })
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_certificates_internal(user_id, &args).await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_output_baskets_internal(user_id, &args).await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_outputs_internal(user_id, &args).await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.find_proven_tx_reqs_internal(&args).await
    }

    async fn find_transactions(
        &self,
        _args: FindTransactionsArgs,
    ) -> Result<Vec<TableTransaction>> {
        // TODO: Implement for SQLite storage
        Ok(vec![])
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        self.list_actions_impl(auth, args).await
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        self.list_certificates_impl(auth, args).await
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        self.list_outputs_impl(auth, args).await
    }
}

// =============================================================================
// WalletStorageWriter Implementation
// =============================================================================

#[async_trait]
impl WalletStorageWriter for StorageSqlx {
    async fn make_available(&self) -> Result<TableSettings> {
        self.make_available_impl().await
    }

    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String> {
        self.migrate_impl(storage_name, storage_identity_key).await
    }

    async fn destroy(&self) -> Result<()> {
        self.destroy_impl().await
    }

    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        self.find_or_insert_user_impl(identity_key).await
    }

    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::abort_action::abort_action_internal(self, user_id, args).await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        // Get the ChainTracker for BEEF verification (if set)
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());

        // Note: Locking scripts for change outputs are stored during process_action
        // when the signed transaction is processed.
        super::create_action::create_action_internal(self, tracker_ref, user_id, args).await
    }

    async fn create_action_at_door(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
        door: crate::storage::CreateActionDoor<'_>,
    ) -> std::result::Result<crate::storage::CreateActionDone, crate::storage::DoorRefusal> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());
        super::create_action::create_action_at_door_internal(self, tracker_ref, user_id, args, door)
            .await
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::process_action::process_action_internal(self, user_id, args).await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::internalize_action::internalize_action_internal(self, user_id, args).await
    }

    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        super::internalize_action::mark_internalized_tx_failed(self, txid).await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.insert_certificate_impl(auth, certificate).await
    }

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.insert_certificate_field_impl(auth, field).await
    }

    async fn insert_certificate_with_fields(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
        fields: Vec<TableCertificateField>,
    ) -> Result<i64> {
        self.insert_certificate_with_fields_impl(auth, certificate, fields)
            .await
    }

    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        self.relinquish_certificate_impl(auth, args).await
    }

    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        self.relinquish_output_impl(auth, args).await
    }

    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        outcome: &super::BroadcastOutcome,
    ) -> Result<()> {
        #[cfg(test)]
        if self.take_busy_next_status_write() {
            return Err(Error::StoreBusy {
                reason: StoreBusyReason::SqliteBusy { code: 5 },
            });
        }

        super::process_action::update_transaction_status_after_broadcast_internal(
            self, txid, outcome,
        )
        .await
    }

    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: chrono::DateTime<chrono::Utc>,
    ) -> Result<ReviewStatusResult> {
        self.review_status_for_user(auth, aged_limit).await
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        self.purge_data_for_user(auth, params).await
    }

    async fn begin_transaction(&self) -> Result<TrxToken> {
        let token = TrxToken::new();
        let mut active = self.active_transactions.write().await;
        active.insert(token.id(), ());
        Ok(token)
    }

    async fn commit_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
    }

    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
    }
}

// =============================================================================
// MonitorStorage Implementation
// =============================================================================

use crate::storage::traits::{MonitorStorage, ProofIngestOutcome, TxSynchronizedStatus};
use std::time::Duration;

/// Private helpers of `MonitorStorage::send_waiting_transactions`.

/// The competitor's word for our transaction: the proof path's evidence.

/// The proof pass over this wallet's own requests.

/// Private helpers for the attempt backstop of
/// `MonitorStorage::synchronize_transaction_statuses`.

#[async_trait]
impl MonitorStorage for StorageSqlx {
    async fn max_acceptable_proof_height(&self) -> Result<u32> {
        let mut conn = self.pool().acquire().await?;
        super::monitor_state::read_proof_gate_on(&mut conn).await
    }

    async fn set_max_acceptable_proof_height(&self, height: u32) -> Result<()> {
        let mut conn = self.writer_conn().await?;
        super::monitor_state::write_proof_gate_on(&mut conn, height).await
    }

    async fn load_header_tracker_state(&self) -> Result<Option<HeaderTrackerState>> {
        let mut conn = self.pool().acquire().await?;
        super::monitor_state::read_header_tracker_on(&mut conn).await
    }

    async fn save_header_tracker_state(&self, state: &HeaderTrackerState) -> Result<()> {
        let mut conn = self.writer_conn().await?;
        super::monitor_state::write_header_tracker_on(&mut conn, state).await
    }

    async fn set_proven_tx_block_hash_if_empty(
        &self,
        txid: &str,
        block_hash: &str,
    ) -> Result<bool> {
        self.set_proven_tx_block_hash_if_empty_impl(txid, block_hash)
            .await
    }

    async fn find_proven_txs_by_block_hash(&self, block_hash: &str) -> Result<Vec<ProvenTxAnchor>> {
        self.find_proven_txs_by_block_hash_impl(block_hash).await
    }

    async fn find_proven_txs_in_heights(
        &self,
        min_height: u32,
        max_height: u32,
    ) -> Result<Vec<ProvenTxAnchor>> {
        self.find_proven_txs_in_heights_impl(min_height, max_height)
            .await
    }

    async fn demote_stale_proof(&self, txid: &str) -> Result<bool> {
        self.demote_stale_proof_impl(txid).await
    }

    async fn announce_lost_proof(&self, txid: &str) -> Result<crate::storage::BroadcastOutcome> {
        self.announce_lost_proof_impl(txid).await
    }

    async fn note_lost_proof_refusal(&self, txid: &str, verdict: &str) -> Result<()> {
        self.note_lost_proof_refusal_impl(txid, verdict).await
    }

    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        // A competitor a broadcaster's word named is asked for first: its
        // checked proof is the one evidence for the double-spend word
        // (bsv-stack-lean #66). Then this wallet's own requests.
        let mut results = self.ask_queued_competitor_proofs().await?;
        results.extend(self.synchronize_own_reqs().await?);
        Ok(results)
    }

    async fn send_waiting_transactions(
        &self,
        min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        self.send_waiting_transactions_impl(min_transaction_age)
            .await
    }

    async fn abort_abandoned(&self, timeout: Duration) -> Result<()> {
        self.abort_abandoned_impl(timeout).await
    }

    async fn un_fail(&self) -> Result<()> {
        let now = chrono::Utc::now();

        // ---- Phase A: auto-unfail canary (the producer) ----
        //
        // Canary invariant: `failed` means never-on-chain. Every failed
        // transaction with an 'invalid'/'doubleSpend' req is periodically
        // re-verified against the chain, UNBOUNDED — a tx the network still
        // reports known/mined must never stop being re-checked. Backoff:
        // hourly for the first 24 checks, daily after (attempts counts
        // canary re-stamps) — bounded provider load without ever abandoning
        // a row.
        //
        // Without this sweep the explicit-'unfail' consumer below is dead
        // code: every demotion path writes 'invalid'/'doubleSpend', never
        // 'unfail', so a false-fail (a broadcast reject during an index
        // disagreement window — the 2026-07-28 btc-relay fuel-coin
        // incident) stayed `failed` forever while mined on-chain.
        // Production reference: rust-wallet-infra monitor auto-unfail
        // canary; TS TaskReviewDoubleSpends covers only the doubleSpend
        // half of this.
        //
        // julianday() on BOTH sides of the time compares: updated_at holds
        // RFC3339 'T' stamps (sqlx chrono) but CURRENT_TIMESTAMP defaults
        // write space-format — a raw string compare silently breaks the
        // backoff windows.
        //
        // The LEFT JOIN + OR arm: a 'doubleSpend' req whose transactions
        // row missed its 'failed' write (partial batch) must still be
        // re-checked.
        let candidates = sqlx::query(
            "SELECT DISTINCT r.proven_tx_req_id, r.txid, r.raw_tx \
             FROM proven_tx_reqs r \
             LEFT JOIN transactions t ON t.txid = r.txid \
             WHERE r.status IN ('invalid', 'doubleSpend') \
               AND (t.status = 'failed' OR r.status = 'doubleSpend') \
               AND julianday(r.updated_at) < julianday(?) \
               AND (r.attempts < 24 OR julianday(r.updated_at) < julianday(?)) \
             ORDER BY r.updated_at ASC \
             LIMIT 20",
        )
        .bind(now - chrono::Duration::hours(1))
        .bind(now - chrono::Duration::days(1))
        .fetch_all(self.pool())
        .await?;

        // Both phases are chain-evidence driven; with no work, don't
        // require services to be configured (parity with the pre-canary
        // no-op path).
        let unfail_reqs = self
            .find_proven_tx_reqs(FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Unfail]),
                ..Default::default()
            })
            .await?;
        if candidates.is_empty() && unfail_reqs.is_empty() {
            return Ok(());
        }
        let services = self.get_services()?;

        for row in &candidates {
            let req_id: i64 = row.get("proven_tx_req_id");
            let txid: String = row.get("txid");
            let raw_tx: Option<Vec<u8>> = row.get("raw_tx");
            if txid.is_empty() {
                continue;
            }
            // EVERY status source, not only the one that refused it
            // (Calgooon/zanaadu-v2#357): the status sources first, then each
            // broadcaster's own status read and a tracker-checked merkle
            // path. A failed transaction is usually failed on ONE
            // broadcaster's word; the one that accepted it is the likeliest
            // to know better.
            let status_word = match services
                .get_status_for_txids(std::slice::from_ref(&txid), false)
                .await
            {
                Ok(result) => result
                    .results
                    .iter()
                    .find(|d| d.txid == txid && (d.status == "known" || d.status == "mined"))
                    .map(|d| format!("status sources {}", d.status)),
                // Provider error: no evidence from these sources.
                Err(_) => None,
            };
            let evidence = match status_word {
                Some(word) => Some(word),
                None => Self::network_holds(services.as_ref(), &txid, None, false, true).await,
            };
            match evidence {
                Some(evidence) => {
                    self.recover_false_failed_req(req_id, &txid, raw_tx.as_deref(), now)
                        .await?;
                    tracing::warn!(
                        "un_fail canary: false-failed tx is on network ({}) — recovered txid={}",
                        evidence,
                        txid
                    );
                }
                None => {
                    // No source holds it — factually failed (so far).
                    // Re-stamp for the backoff; stays invalid, stays
                    // watched, forever.
                    self.restamp_false_fail_candidate(req_id, now).await?;
                }
            }
        }

        // ---- Phase B: explicit 'unfail' requests (the consumer) ----
        let reqs = unfail_reqs;

        if reqs.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "un_fail: processing {} transactions marked for unfail",
            reqs.len()
        );

        for req in &reqs {
            // Check if transaction has a merkle path on-chain
            match services.get_merkle_path(&req.txid, false).await {
                Ok(result) if result.merkle_path.is_some() => {
                    self.recover_false_failed_req(
                        req.proven_tx_req_id,
                        &req.txid,
                        req.raw_tx.as_deref(),
                        now,
                    )
                    .await?;
                }
                Ok(_) => {
                    // Positive network answer, no proof — back to 'invalid'.
                    // NOT terminal: the Phase A canary keeps re-checking
                    // 'invalid' reqs whose tx is failed, so a proof that
                    // shows up later still recovers it.
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;

                    tracing::debug!(
                        "un_fail: marked {} as invalid (not found on chain)",
                        req.txid
                    );
                }
                Err(e) => {
                    // Provider error — no evidence, no state change: hold at
                    // 'unfail' for the next pass. Terminalizing on an outage
                    // is the incident class where a multi-day provider
                    // outage turns a recovery request permanent-'invalid'.
                    tracing::warn!(
                        "un_fail: provider error for {} — holding at 'unfail': {}",
                        req.txid,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        self.review_status_impl().await
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        self.purge_data_impl(params).await
    }

    async fn compact_input_beefs(&self) -> Result<u32> {
        use bsv_rs::transaction::{Beef, MerklePath};

        // Find completed proven_tx_reqs with non-null input_beef.
        // Process in batches of 50 to avoid holding the connection too long.
        let rows: Vec<(i64, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, input_beef, raw_tx
            FROM proven_tx_reqs
            WHERE status = 'completed'
              AND input_beef IS NOT NULL
              AND LENGTH(input_beef) > 1000
            ORDER BY LENGTH(input_beef) DESC
            LIMIT 50
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut compacted = 0u32;

        for (req_id, beef_bytes, raw_tx) in &rows {
            let mut beef = match Beef::from_binary(beef_bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let original_size = beef_bytes.len();

            // Find unproven txids in this BEEF
            let unproven_txids: Vec<String> = beef
                .txs
                .iter()
                .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
                .map(|tx| tx.txid())
                .collect();

            if unproven_txids.is_empty() {
                continue;
            }

            // Check proven_txs for available proofs (batch query)
            let mut upgraded = 0u32;

            for chunk in unproven_txids.chunks(400) {
                let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query_str = format!(
                    "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
                    placeholders
                );

                let mut query = sqlx::query(&query_str);
                for txid in chunk {
                    query = query.bind(txid);
                }

                let proof_rows: Vec<(String, Vec<u8>)> = query
                    .fetch_all(self.pool())
                    .await?
                    .into_iter()
                    .map(|row| {
                        let txid: String = sqlx::Row::get(&row, "txid");
                        let mp: Vec<u8> = sqlx::Row::get(&row, "merkle_path");
                        (txid, mp)
                    })
                    .collect();

                let mut conn = self.pool().acquire().await?;
                let tracker = self.get_chain_tracker().await;
                for (txid, merkle_path_bytes) in &proof_rows {
                    if let Ok(merkle_path) = MerklePath::from_binary(merkle_path_bytes) {
                        // P0-1: a stored proof that was never checked is
                        // checked here, once; a refuted one is demoted. The
                        // rewritten BEEF is stored, so it takes only what
                        // the ingest would store (P0-1c): with no tracker or
                        // a tracker fault the proof is not merged and the
                        // row stays unchecked for a later pass.
                        if !super::proof_root_checks::check_unchecked_stored_proof_on(
                            &mut conn,
                            Some(self.writer()),
                            tracker.as_deref(),
                            txid,
                            &merkle_path,
                        )
                        .await?
                        .may_store()
                        {
                            continue;
                        }
                        // P0-1d: linked with its own leaf flagged, so the
                        // stored link survives the next merge.
                        let Some(bump_index) =
                            super::create_action::merge_proof_for(&mut beef, merkle_path, txid)
                        else {
                            continue;
                        };
                        if let Some(tx) = beef.find_txid_mut(txid) {
                            tx.set_bump_index(Some(bump_index));
                            upgraded += 1;
                        }
                    }
                }
            }

            if upgraded == 0 {
                continue;
            }

            // Now that ancestors carry proofs, drop everything the transaction's
            // own inputs no longer reach: a BUMP terminates its branch. Rooted at
            // the inputs, so an input that is still unproven keeps its history.
            // (`Beef::trim_known_proven` is root-blind: it walks from whichever
            // transactions nothing else spends, which can drop an unproven input
            // that a proven sibling happens to descend from.)
            if let Some(raw) = raw_tx {
                if let Ok(roots) = parse_input_txids(raw) {
                    prune_beef_to_roots(&mut beef, &roots);
                }
            }

            let new_bytes = beef.to_binary();
            let new_size = new_bytes.len();

            // Only update if we actually saved space
            if new_size < original_size {
                let now = chrono::Utc::now();
                sqlx::query(
                    "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                )
                .bind(&new_bytes)
                .bind(now)
                .bind(req_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;

                compacted += 1;
                tracing::debug!(
                    req_id = req_id,
                    original_kb = original_size / 1024,
                    new_kb = new_size / 1024,
                    savings_pct = ((original_size - new_size) * 100) / original_size,
                    "Compacted input_beef"
                );
            }
        }

        Ok(compacted)
    }

    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let now = std::time::Instant::now();
        let mut locks = self.task_locks.write().await;

        // Check if there's an existing lock
        if let Some((holder, expiry)) = locks.get(task_name) {
            if *expiry > now {
                // Lock is still valid
                if holder == instance_id {
                    // We already hold it - extend the TTL
                    locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
                    return Ok(true);
                }
                // Another instance holds it
                return Ok(false);
            }
            // Lock has expired - fall through to acquire
        }

        // Acquire the lock
        locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
        Ok(true)
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        let mut locks = self.task_locks.write().await;
        if let Some((holder, _)) = locks.get(task_name) {
            if holder == instance_id {
                locks.remove(task_name);
            }
        }
        Ok(())
    }

    async fn update_proven_tx_req_status(
        &self,
        proven_tx_req_id: i64,
        new_status: ProvenTxReqStatus,
    ) -> Result<()> {
        self.update_proven_tx_req_status_impl(proven_tx_req_id, new_status)
            .await
    }

    async fn mark_transaction_seen_on_network(&self, txid: &str) -> Result<bool> {
        self.mark_transaction_seen_on_network_by(
            txid,
            crate::services::broadcast_memory::BROADCAST_PROVIDER_NETWORK,
        )
        .await
    }

    async fn mark_transaction_seen_on_network_by(
        &self,
        txid: &str,
        provider: &str,
    ) -> Result<bool> {
        // The reporting plane has the tx: remember it for reduced sends.
        self.record_broadcast_status_quiet(
            txid,
            provider,
            crate::services::broadcast_memory::BROADCAST_STATUS_SEEN,
        )
        .await;

        let now = chrono::Utc::now();

        // Same transition a successful post_beef broadcast performs in
        // send_waiting_transactions: req → 'unmined' (awaiting proof),
        // tx → 'unproven' (spendable). Only lift PRE-broadcast statuses —
        // never demote 'completed'.
        let req_result = sqlx::query(
            "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? \
             WHERE txid = ? AND status IN ('unsent', 'sending', 'unknown', 'callback', 'unconfirmed')",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        let tx_result = sqlx::query(
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status = 'sending'",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        let updated = req_result.rows_affected() > 0 || tx_result.rows_affected() > 0;
        if updated {
            tracing::info!(txid = %txid, "marked seen-on-network: proven_tx_req → unmined, transaction → unproven");
        }
        Ok(updated)
    }

    async fn mark_transaction_rejected(&self, txid: &str, double_spend: bool) -> Result<bool> {
        self.mark_transaction_rejected_impl(txid, double_spend)
            .await
    }

    async fn mark_transaction_rejected_by(
        &self,
        txid: &str,
        provider: &str,
        double_spend: bool,
    ) -> Result<bool> {
        // A broadcaster's word is a hint (bsv-stack-lean #66; its tracker
        // charter, section 2): `rejected` is the node's rules run locally
        // and `conflicted` a mined competitor's checked proof, neither of
        // which this word is. The refusing broadcaster is remembered (it
        // never skips this txid as an ancestor again), the word goes on the
        // request's history, and the transaction keeps its word: the re-ask
        // and the proof task decide. A competitor the word names is queued
        // by `queue_competitor_proof_asks`. Until 0.7.4 the refusal was
        // final when no other source held the transaction (#357).
        self.record_broadcast_status_quiet(
            txid,
            provider,
            crate::services::broadcast_memory::BROADCAST_STATUS_REJECTED,
        )
        .await;
        self.note_on_req_history(
            txid,
            serde_json::json!({
                "when": chrono::Utc::now().to_rfc3339(),
                "what": "broadcasterRefusalHint",
                "provider": provider,
                "doubleSpend": double_spend,
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            provider = %provider,
            double_spend = double_spend,
            marker = "broadcaster_refusal_hint",
            "a broadcaster refused the transaction: a hint, no word written; the re-ask and the proof task decide"
        );
        Ok(false)
    }

    async fn queue_competitor_proof_asks(&self, txid: &str, competitors: &[String]) -> Result<()> {
        self.queue_competitors(txid, competitors).await.map(|_| ())
    }

    async fn ingest_push_proof(
        &self,
        txid: &str,
        merkle_path: &[u8],
        block_height: u32,
        block_hash: &str,
    ) -> Result<Option<ProofIngestOutcome>> {
        self.ingest_push_proof_impl(txid, merkle_path, block_height, block_hash)
            .await
    }
}

// =============================================================================
// Commission and Event Logging Methods
// =============================================================================

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

/// How long a broadcast-memory row counts as another source holding a
/// transaction one broadcaster refused (Calgooon/zanaadu-v2#357).
const MEMORY_EVIDENCE_MAX_AGE_SECS: i64 = 2 * 60 * 60;

/// Private helpers for the `MonitorStorage::un_fail` recovery path.
impl StorageSqlx {
    /// THE FINAL-VERSUS-TRANSIENT RULE for one broadcaster's refusal
    /// (Calgooon/zanaadu-v2#357), the memory half: the broadcast memory's
    /// evidence that a source other than `refusing` holds `txid`. Another
    /// broadcaster's `seen` / `mined` row or the network's / chain's
    /// `seen` / `mined` row always counts; another broadcaster's bare
    /// `accepted` row counts only when `accepted_counts` (a plain REJECTED:
    /// the transaction is valid to a broadcaster that took it). A conflict
    /// verdict passes `false`: two broadcasters can each take one side of a
    /// double spend, so only the network's word overrules it.
    async fn memory_holds(
        &self,
        txid: &str,
        refusing: Option<&str>,
        accepted_counts: bool,
    ) -> Option<String> {
        use crate::services::broadcast_memory::{is_global_provider, BroadcastStatus};
        let records = match self
            .broadcast_records(None, std::slice::from_ref(&txid.to_string()))
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::debug!(txid = %txid, error = %e, "broadcast memory unreadable: no memory evidence");
                return None;
            }
        };
        // A memory row is evidence only while fresh: a SEEN from hours ago
        // says nothing about a mempool that has since evicted the
        // transaction, and an unbounded row would hold an evicted
        // transaction (and its inputs) forever. Past the window only the
        // live read below decides.
        let fresh_after =
            chrono::Utc::now() - chrono::Duration::seconds(MEMORY_EVIDENCE_MAX_AGE_SECS);
        records.into_iter().find_map(|r| {
            if Some(r.provider.as_str()) == refusing || r.seen_at < fresh_after {
                return None;
            }
            let status = r.ladder_status()?;
            let counts = status.is_network_evidence()
                || (accepted_counts
                    && status == BroadcastStatus::Accepted
                    && !is_global_provider(&r.provider));
            counts.then(|| format!("{} {}", r.provider, status.as_str()))
        })
    }

    /// The rule's live half: whether the network holds `txid` by any source
    /// but `refusing`, asked now. `ask_status`: the status sources
    /// (`get_status_for_txids`, a `known` / `mined` answer under its merge
    /// rule). Always: every broadcaster's own status read
    /// (`get_broadcaster_statuses`, `seen` / `mined` only; a broadcaster's
    /// "accepted" is not the network's word). `ask_proof`: a merkle path,
    /// which `Services` serves only after the chain tracker accepted its
    /// root. A source that cannot answer is no evidence either way.
    async fn network_holds(
        services: &dyn WalletServices,
        txid: &str,
        refusing: Option<&str>,
        ask_status: bool,
        ask_proof: bool,
    ) -> Option<String> {
        let txids = [txid.to_string()];
        if ask_status {
            if let Ok(result) = services.get_status_for_txids(&txids, false).await {
                if let Some(detail) = result
                    .results
                    .iter()
                    .find(|d| d.txid == txid && (d.status == "known" || d.status == "mined"))
                {
                    return Some(format!("status sources {}", detail.status));
                }
            }
        }
        for (provider, status) in services.get_broadcaster_statuses(txid).await {
            if Some(provider.as_str()) != refusing && status.is_network_evidence() {
                return Some(format!("{} {}", provider, status.as_str()));
            }
        }
        if ask_proof {
            if let Ok(result) = services.get_merkle_path(txid, false).await {
                if result.merkle_path.is_some() {
                    return Some("a merkle path the chain tracker accepted".to_string());
                }
            }
        }
        None
    }

    /// Restore a false-failed transaction whose chain evidence says it is
    /// known/mined: req → 'unmined' (attempts 0, so
    /// `synchronize_transaction_statuses` re-polls it and completes it with
    /// a proof), tx → 'unproven', created outputs re-validated against the
    /// chain (is_utxo) before re-enabling, and the tx's consumed inputs
    /// re-marked spent (they were released when the tx was wrongly failed).
    /// Shared by the Phase A canary and Phase B explicit-'unfail' requests.
    async fn recover_false_failed_req(
        &self,
        req_id: i64,
        txid: &str,
        raw_tx: Option<&[u8]>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let services = self.get_services()?;

        sqlx::query(
            "UPDATE proven_tx_reqs SET status = 'unmined', attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(now)
        .bind(req_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        // 'completed' rows are already correct — never move them backward.
        sqlx::query(
            "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND status != 'completed'",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        // Restore outputs of this transaction, but validate each against
        // chain to avoid creating ghost UTXOs (outputs already spent
        // on-chain).
        let output_rows = sqlx::query(
            "SELECT output_id, vout, locking_script FROM outputs WHERE txid = ? AND spendable = 0",
        )
        .bind(txid)
        .fetch_all(self.pool())
        .await?;

        let mut restored = 0u32;
        for row in &output_rows {
            let output_id: i64 = row.get("output_id");
            let vout: i32 = row.get("vout");
            let locking_script: Option<Vec<u8>> = row.get("locking_script");
            let script = locking_script.as_deref().unwrap_or(&[]);
            match services.is_utxo(txid, vout as u32, script).await {
                UtxoVerdict::UnspentHint => {
                    sqlx::query(
                        "UPDATE outputs SET spendable = 1, updated_at = ? WHERE output_id = ?",
                    )
                    .bind(now)
                    .bind(output_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                    restored += 1;
                }
                UtxoVerdict::Spent | UtxoVerdict::SpentHint => {
                    tracing::debug!(
                        "un_fail: output {}:{} is in no unspent set, skipping",
                        txid,
                        vout
                    );
                }
                UtxoVerdict::Unknown => {
                    tracing::warn!(
                        "un_fail: output {}:{} could not be looked up (not a spend); left unspendable",
                        txid,
                        vout
                    );
                }
            }
        }

        self.remark_unfailed_inputs_spent(txid, raw_tx, now).await?;

        tracing::info!(
            "un_fail: restored transaction {} ({}/{} outputs validated as UTXOs)",
            txid,
            restored,
            output_rows.len()
        );
        Ok(())
    }

    /// Re-mark an unfailed transaction's consumed inputs as spent: every
    /// wallet-tracked output row at each of the tx's input outpoints becomes
    /// unspendable (chain truth — the outpoint is consumed), and `spent_by`
    /// is stamped with the row-owner's own transactions row for this txid
    /// where one exists and no competing spend is recorded. Same decision
    /// table as internalize's `mark_user_inputs_spent`, but cross-user (the
    /// monitor runs for all users, not one caller).
    async fn remark_unfailed_inputs_spent(
        &self,
        txid: &str,
        raw_tx: Option<&[u8]>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        // Resolve the raw tx: prefer the req's copy, fall back to the
        // transactions row. Without one the outpoints are unknowable — the
        // output restoration + is_utxo validation stays the safety net.
        let raw = match raw_tx {
            Some(r) if !r.is_empty() => r.to_vec(),
            _ => {
                let row = sqlx::query(
                    "SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1",
                )
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;
                match row.and_then(|r| r.get::<Option<Vec<u8>>, _>("raw_tx")) {
                    Some(r) if !r.is_empty() => r,
                    _ => {
                        tracing::warn!(
                            "un_fail: no raw_tx available for {} — cannot re-mark consumed inputs spent",
                            txid
                        );
                        return Ok(());
                    }
                }
            }
        };
        let tx = match bsv_rs::transaction::Transaction::from_binary(&raw) {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    "un_fail: raw_tx for {} does not parse ({:?}) — skipping input re-mark",
                    txid,
                    e
                );
                return Ok(());
            }
        };
        for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&tx) {
            let rows = sqlx::query(
                "SELECT output_id, user_id, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
            )
            .bind(&source_txid)
            .bind(vout as i32)
            .fetch_all(self.pool())
            .await?;
            for row in rows {
                let output_id: i64 = row.get("output_id");
                let row_user_id: i64 = row.get("user_id");
                let spendable: bool = row.get("spendable");
                let spent_by: Option<i64> = row.get("spent_by");

                // The row-owner's own transactions row for the spending
                // txid (None for foreign users without one — their
                // spent_by stays NULL, the FK references the owning
                // user's transactions scope).
                let user_tx_id: Option<i64> = sqlx::query(
                    "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
                )
                .bind(txid)
                .bind(row_user_id)
                .fetch_optional(self.pool())
                .await?
                .map(|r| r.get("transaction_id"));

                // Rows already accounted for are skipped, and a competing
                // recorded spend is never clobbered.
                if !spendable {
                    continue;
                }
                if let Some(existing) = spent_by {
                    if user_tx_id != Some(existing) {
                        continue;
                    }
                }
                if let Some(user_tx_id) = user_tx_id {
                    sqlx::query(
                        "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
                    )
                    .bind(user_tx_id)
                    .bind(now)
                    .bind(output_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                } else {
                    sqlx::query(
                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE output_id = ?",
                    )
                    .bind(now)
                    .bind(output_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Backoff re-stamp for a canary candidate with no chain evidence:
    /// attempts+1 and a fresh updated_at, no status change.
    async fn restamp_false_fail_candidate(
        &self,
        req_id: i64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(now)
        .bind(req_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tracker that answers every root valid.
    struct AnyRoot;

    #[async_trait]
    impl ChainTracker for AnyRoot {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
            Ok(true)
        }

        async fn current_height(
            &self,
        ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
            Ok(1)
        }
    }

    /// One store, two handles (bsv-wallet-cli #44: one store per file per
    /// process): the chain tracker, the services, the settings and the
    /// identity set through one handle are seen through the other, cleared
    /// as well; the two hold one broadcast memory and one writer.
    #[tokio::test]
    async fn the_tracker_services_and_settings_set_on_one_handle_are_seen_by_the_other() {
        let a = StorageSqlx::in_memory().await.unwrap();
        let b = a.clone();
        assert!(b.get_chain_tracker().await.is_none());
        assert!(b.get_services().is_err());

        let tracker: Arc<dyn ChainTracker> = Arc::new(AnyRoot);
        a.set_chain_tracker(tracker.clone()).await;
        let seen = b.get_chain_tracker().await.expect("set through a");
        assert!(Arc::ptr_eq(&seen, &tracker));
        b.clear_chain_tracker().await;
        assert!(a.get_chain_tracker().await.is_none(), "cleared through b");

        let services: Arc<dyn WalletServices> =
            Arc::new(crate::services::mock::MockWalletServices::new());
        b.set_services(services.clone());
        assert!(Arc::ptr_eq(&a.get_services().unwrap(), &services));

        a.migrate("shared", &"02".repeat(33)).await.unwrap();
        assert_eq!(b.storage_identity_key(), "02".repeat(33));
        assert_eq!(b.storage_name(), "shared");
        assert_eq!(b.get_settings().storage_name, "shared");
        let copy = b.get_settings();
        a.make_available().await.unwrap();
        b.make_available().await.unwrap();
        assert_eq!(copy.storage_identity_key, "02".repeat(33));
        assert_eq!(a.get_settings(), b.get_settings());

        assert!(Arc::ptr_eq(
            &a.sqlx_broadcast_memory(),
            &b.sqlx_broadcast_memory()
        ));
        let held = a.writer().acquire().await.unwrap();
        assert_eq!(b.writer().num_idle(), 0, "one writer connection");
        drop(held);
    }

    /// Review 4 G7 of #44: `get_settings`, `storage_identity_key` and
    /// `storage_name` lent references out of their locks through `unsafe`,
    /// so with shared handles a second handle's `make_available` (or
    /// `migrate`) could free a value another was reading. They return owned
    /// copies now, taken under the lock. Two handles: one replaces the
    /// settings, the identity and the name with changing values, over and
    /// over, while four threads read them through the other; every value
    /// read is one that was written, whole.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_through_one_handle_race_make_available_through_another() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g7.db").to_string_lossy().to_string();
        let a = StorageSqlx::open(&path).await.unwrap();
        let b = a.clone();
        let name = |i: usize| format!("store-{}", i % 7);
        let key = |i: usize| format!("{:02x}", i % 5).repeat(33);
        a.migrate(&name(0), &key(0)).await.unwrap();
        a.make_available().await.unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let b = b.clone();
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let names: HashSet<String> = (0..7).map(name).collect();
                    let keys: HashSet<String> = (0..5).map(key).collect();
                    let mut reads = 0u64;
                    while !done.load(std::sync::atomic::Ordering::SeqCst) {
                        let settings = b.get_settings();
                        assert!(names.contains(&settings.storage_name), "{settings:?}");
                        assert!(keys.contains(&settings.storage_identity_key));
                        assert!(names.contains(&b.storage_name()));
                        assert!(keys.contains(&b.storage_identity_key()));
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        for i in 1..300 {
            if i % 10 == 0 {
                a.migrate(&name(i), &key(i)).await.unwrap();
            } else {
                sqlx::query("UPDATE settings SET storage_name = ?, updated_at = ?")
                    .bind(name(i))
                    .bind(Utc::now())
                    .execute(a.writer())
                    .await
                    .unwrap();
            }
            a.make_available().await.unwrap();
            b.make_available().await.unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        for reader in readers {
            assert!(reader.join().expect("a reader saw a whole value") > 0);
        }
    }

    /// Review 2 F4 of #44: a begin that fails for anything but busy keeps
    /// the `Database error:` prefix these sites answered before #44. The
    /// pool's one connection is already inside a transaction, so SQLite
    /// refuses `BEGIN IMMEDIATE` (code 1, not busy).
    #[tokio::test]
    async fn a_non_busy_begin_failure_is_a_database_error() {
        let in_a_transaction = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("BEGIN").execute(&mut *conn).await?;
                    Ok(())
                })
            })
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let err = match begin_immediate(&in_a_transaction).await {
            Ok(_) => panic!("a transaction within a transaction"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::DatabaseError(_)), "{err:?}");
        assert!(err.to_string().starts_with("Database error: "), "{err}");
        assert!(
            err.to_string()
                .contains("cannot start a transaction within a transaction"),
            "{err}"
        );
    }

    /// The begin/commit mapping: busy is `StoreBusy`, anything else
    /// `DatabaseError` with the error's text.
    #[test]
    fn a_begin_or_commit_error_is_store_busy_only_when_busy() {
        let err = write_tx_error(sqlx::Error::PoolTimedOut);
        assert!(
            matches!(
                err,
                Error::StoreBusy {
                    reason: StoreBusyReason::WriterNotAcquired
                }
            ),
            "{err:?}"
        );
        let err = write_tx_error(sqlx::Error::Protocol("a broken pipe".to_string()));
        assert!(matches!(err, Error::DatabaseError(_)), "{err:?}");
        assert!(err.to_string().starts_with("Database error: "), "{err}");
        assert!(err.to_string().contains("a broken pipe"), "{err}");
    }

    /// Only a timeout acquiring the WRITER is busy: a reader pool of one,
    /// held, and a second read times out on the reader pool. That is the
    /// plain SQLx error, not `StoreBusy`.
    #[tokio::test]
    async fn a_reader_pool_timeout_is_not_store_busy() {
        let store = StorageSqlx::connect(
            "sqlite::memory:",
            1,
            Some(std::time::Duration::from_millis(200)),
        )
        .await
        .unwrap();
        let _held = store.pool().acquire().await.unwrap();

        let err = store.find_user("02ab").await.unwrap_err();
        assert!(matches!(err, Error::SqlxError(_)), "{err:?}");
        assert!(!err.to_string().starts_with("Store busy"), "{err}");
        assert!(err.to_string().contains("pool timed out"), "{err}");
    }

    /// The writer's single-statement path: with the writer held, a
    /// single-statement write waits the ceiling and answers
    /// `StoreBusy` (`WriterNotAcquired`) at its acquire.
    #[tokio::test]
    async fn the_writer_not_acquired_for_one_statement_is_store_busy() {
        let store = StorageSqlx::in_memory().await.unwrap();
        let held = store.write_tx().await.unwrap();

        let started = std::time::Instant::now();
        let err = store.log_monitor_event("busy", None).await.unwrap_err();
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(4500),
            "it waited the ceiling"
        );
        assert!(
            matches!(
                err,
                Error::StoreBusy {
                    reason: StoreBusyReason::WriterNotAcquired
                }
            ),
            "{err:?}"
        );
        held.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_un_fail_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no unfail transactions, should complete without error
        let result = storage.un_fail().await;
        assert!(result.is_ok());
    }

    // =============================================================================
    // Services Integration Tests
    // =============================================================================

    #[tokio::test]
    async fn test_get_services_without_set_returns_error() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // get_services without set_services should return error
        let result = storage.get_services();
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("setServices"),
                    "Error message should mention setServices: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_set_and_get_services() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Create a services instance
        let services = Arc::new(Services::new(Chain::Main).unwrap());

        // Set services
        storage.set_services(services.clone());

        // Get services should now work
        let retrieved = storage.get_services();
        assert!(retrieved.is_ok());

        // Verify we can call a method on the retrieved services
        let retrieved_services = retrieved.unwrap();
        let script_hash = retrieved_services.hash_output_script(&[0x76, 0xa9]);
        assert!(!script_hash.is_empty());
    }

    #[tokio::test]
    async fn test_set_services_replaces_previous() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Set first services instance
        let services1 = Arc::new(Services::new(Chain::Main).unwrap());
        storage.set_services(services1);

        // Verify first services
        let retrieved1 = storage.get_services().unwrap();
        let hash1 = retrieved1.hash_output_script(&[0x76]);

        // Set second services instance (replaces first)
        let services2 = Arc::new(Services::new(Chain::Test).unwrap());
        storage.set_services(services2);

        // Get services again - should still work (services were replaced)
        let retrieved2 = storage.get_services().unwrap();
        let hash2 = retrieved2.hash_output_script(&[0x76]);

        // Both should produce the same hash (hash_output_script is deterministic)
        assert_eq!(hash1, hash2);
    }

    // =============================================================================
    // TrxToken / Transaction Scope Tests
    // =============================================================================

    #[tokio::test]
    async fn test_begin_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        assert!(token.id() > 0);
    }

    #[tokio::test]
    async fn test_commit_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.commit_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rollback_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.rollback_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trx_token_uniqueness() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token1 = storage.begin_transaction().await.unwrap();
        let token2 = storage.begin_transaction().await.unwrap();
        assert_ne!(token1.id(), token2.id());
        storage.commit_transaction(token1).await.unwrap();
        storage.commit_transaction(token2).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.commit_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    #[tokio::test]
    async fn test_rollback_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.rollback_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    // =========================================================================
    // Task Lock Tests
    // =========================================================================

    #[tokio::test]
    async fn test_task_lock_acquire_and_release() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Acquire should succeed
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Release should succeed
        storage
            .release_task_lock("test_task", "instance_a")
            .await
            .unwrap();

        // After release, another instance can acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_contention() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Instance A acquires the lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Instance B should fail to acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!acquired);

        // Instance A can re-acquire (extend TTL)
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_expiry() {
        let storage = StorageSqlx::in_memory().await.unwrap();

        // Acquire with a very short TTL
        let ttl = Duration::from_millis(1);
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Instance B should now be able to acquire the expired lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_different_tasks() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Different tasks should have independent locks
        let acquired_a = storage
            .try_acquire_task_lock("task_one", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_a);

        let acquired_b = storage
            .try_acquire_task_lock("task_two", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_b);

        // Another instance should be blocked on task_one but not task_three
        let blocked = storage
            .try_acquire_task_lock("task_one", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!blocked);

        let ok = storage
            .try_acquire_task_lock("task_three", "instance_b", ttl)
            .await
            .unwrap();
        assert!(ok);
    }

    // =========================================================================
    // send_waiting_transactions doubleSpend + UTXO verification
    // =========================================================================

    // NOTE: A full integration test for send_waiting_transactions encountering a
    // doubleSpend with UTXO-verified rollback would require:
    //   1. A proven parent tx in `proven_txs` (with raw_tx + merkle_path for BEEF rebuild)
    //   2. A child tx in `transactions` + `proven_tx_reqs` with status='unprocessed'
    //   3. The child's raw_tx must be parseable and reference the parent's txid as input
    //   4. Outputs for the parent marked as spent_by the child
    //   5. Mock services: post_beef → double-spend, get_status_for_txids → no match,
    //      is_utxo → Sequence(true, false) for partial restore
    //
    // The core UTXO-verified rollback logic is already tested by:
    //   - test_double_spend_partial_utxo_verification (partial is_utxo results)
    //   - test_double_spend_restores_inputs (all inputs verified)
    //   - test_double_spend_no_services_keeps_inputs_locked (fail-safe)
    //
    // The send_waiting path reuses the same pattern inline. A full end-to-end
    // test is deferred because it requires BEEF rebuild infrastructure (proven_txs
    // with valid merkle paths) wired through the entire broadcast pipeline.

    // =========================================================================
    // validate_beef_for_broadcast Tests
    // =========================================================================

    /// Minimal valid coinbase-like transaction bytes (no real inputs).
    const MINIMAL_TX_BYTES: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, // version
        0x01, // input count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // prev txid (32 zero bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
        0xff, // vout (0xFFFFFFFF for coinbase)
        0x00, // script length
        0xff, 0xff, 0xff, 0xff, // sequence
        0x01, // output count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // satoshis
        0x00, // script length
        0x00, 0x00, 0x00, 0x00, // locktime
    ];

    /// Compute the txid (double-SHA256, reversed) of raw transaction bytes.
    fn test_compute_txid(raw_tx: &[u8]) -> String {
        use bsv_rs::primitives::{sha256d, to_hex};
        let hash = sha256d(raw_tx);
        let mut reversed = hash;
        reversed.reverse();
        to_hex(&reversed)
    }

    /// Test validate_beef_for_broadcast with a valid BEEF: 1 unproven leaf + 1 proven parent.
    #[test]
    fn test_validate_beef_for_broadcast_valid() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;

        // Build the parent tx (proven, coinbase-like)
        let parent_raw = MINIMAL_TX_BYTES.to_vec();
        let parent_txid = test_compute_txid(&parent_raw);
        let parent_bump = MerklePath::from_coinbase_txid(&parent_txid, height);

        // Build a child tx that spends the parent
        let parent_txid_bytes = bsv_rs::primitives::from_hex(&parent_txid).unwrap();
        let mut parent_txid_le = parent_txid_bytes;
        parent_txid_le.reverse(); // txid stored little-endian in raw tx

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&parent_txid_le); // prev txid (parent)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(parent_bump);
        beef.merge_raw_tx(parent_raw, Some(bump_index)); // parent (proven)
        beef.merge_raw_tx(child_raw, None); // child (unproven leaf)

        // Validation should pass: 1 unproven leaf whose input parent is in the BEEF
        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_ok(),
            "Expected valid BEEF, got error: {:?}",
            result.err()
        );
    }

    /// Test validate_beef_for_broadcast with 0 unproven transactions (all proven).
    #[test]
    fn test_validate_beef_for_broadcast_no_unproven() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;
        let raw_tx = MINIMAL_TX_BYTES.to_vec();
        let txid = test_compute_txid(&raw_tx);
        let bump = MerklePath::from_coinbase_txid(&txid, height);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(bump);
        beef.merge_raw_tx(raw_tx, Some(bump_index)); // proven — no unproven leaves

        let result = validate_beef_for_broadcast(&beef, &txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with no unproven leaf"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("no unproven leaf"),
            "Error should mention 'no unproven leaf', got: {}",
            err
        );
    }

    /// Test validate_beef_for_broadcast with a missing parent (input source not in BEEF).
    #[test]
    fn test_validate_beef_for_broadcast_missing_parent() {
        use bsv_rs::transaction::Beef;

        // Build a child tx that references a parent NOT in the BEEF.
        // Use a dummy parent txid that won't be in the BEEF.
        let fake_parent_txid = "ab".repeat(32);
        let fake_parent_bytes = bsv_rs::primitives::from_hex(&fake_parent_txid).unwrap();
        let mut fake_parent_le = fake_parent_bytes;
        fake_parent_le.reverse();

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&fake_parent_le); // prev txid (missing from BEEF)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        beef.merge_raw_tx(child_raw, None); // child only, no parent

        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with missing parent"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("missing source"),
            "Error should mention 'missing source', got: {}",
            err
        );
    }

    // =========================================================================
    // un_fail: auto-unfail canary (producer) + explicit-request consumer
    // =========================================================================

    mod un_fail_recovery {
        use super::*;
        use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServicesBuilder};
        use crate::services::traits::{
            GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail,
        };
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        use std::sync::Arc;

        async fn create_storage() -> StorageSqlx {
            let storage = StorageSqlx::in_memory().await.unwrap();
            storage
                .migrate("test-unfail", &"0".repeat(64))
                .await
                .unwrap();
            storage.make_available().await.unwrap();
            storage
        }

        async fn create_user(storage: &StorageSqlx) -> i64 {
            let (user, _) = storage
                .find_or_insert_user(
                    "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
                )
                .await
                .unwrap();
            user.user_id
        }

        /// A minimal spending tx consuming `source_txid:source_vout`;
        /// returns (raw bytes, txid).
        fn spending_tx(source_txid: &str, source_vout: u32) -> (Vec<u8>, String) {
            let mut tx = Transaction::new();
            tx.version = 1;
            tx.lock_time = 0;
            let mut input = TransactionInput::new(source_txid.to_string(), source_vout);
            input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
            tx.inputs.push(input);
            tx.outputs.push(TransactionOutput {
                satoshis: Some(9000),
                locking_script: LockingScript::from_hex(
                    "76a914000000000000000000000000000000000000000088ac",
                )
                .unwrap(),
                change: false,
            });
            let txid = tx.id();
            (tx.to_binary(), txid)
        }

        async fn insert_tx(storage: &StorageSqlx, user_id: i64, txid: &str, status: &str) -> i64 {
            let now = chrono::Utc::now();
            sqlx::query(
                "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, is_outgoing, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, 'unfail test tx', 0, 1, 0, 1, ?, ?)",
            )
            .bind(user_id)
            .bind(txid)
            .bind(status)
            .bind(format!("ref-{}", &txid[..8]))
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        async fn insert_output(
            storage: &StorageSqlx,
            user_id: i64,
            transaction_id: i64,
            txid: &str,
            vout: i32,
            spendable: bool,
        ) -> i64 {
            let now = chrono::Utc::now();
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, created_at, updated_at) \
                 VALUES (?, ?, ?, 1, ?, 9000, 'storage', 'change', 'P2PKH', ?, ?, ?)",
            )
            .bind(user_id)
            .bind(transaction_id)
            .bind(spendable)
            .bind(vout)
            .bind(txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        /// Insert a proven_tx_req row aged `hours_old` hours into the past
        /// (controls canary backoff eligibility).
        async fn insert_req_aged(
            storage: &StorageSqlx,
            txid: &str,
            status: &str,
            raw_tx: &[u8],
            hours_old: i64,
        ) -> i64 {
            let stamp = chrono::Utc::now() - chrono::Duration::hours(hours_old);
            sqlx::query(
                "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notify, notified, raw_tx, created_at, updated_at) \
                 VALUES (?, ?, 0, '{}', '{}', 0, ?, ?, ?)",
            )
            .bind(txid)
            .bind(status)
            .bind(raw_tx)
            .bind(stamp)
            .bind(stamp)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
        }

        fn status_response(txid: &str, status: &str) -> MockResponse<GetStatusForTxidsResult> {
            MockResponse::Success(GetStatusForTxidsResult {
                name: "mock".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.to_string(),
                    status: status.to_string(),
                    depth: Some(1),
                    ..Default::default()
                }],
            })
        }

        async fn req_state(storage: &StorageSqlx, txid: &str) -> (String, i64) {
            let row = sqlx::query("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            (row.get("status"), row.get::<i32, _>("attempts") as i64)
        }

        async fn tx_status(storage: &StorageSqlx, txid: &str) -> String {
            sqlx::query("SELECT status FROM transactions WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap()
                .get("status")
        }

        async fn output_state(storage: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
            let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
                .bind(output_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            (row.get("spendable"), row.get("spent_by"))
        }

        /// The incident shape (btc-relay 2026-07-28): a mined tx wrongly
        /// demoted to failed/invalid recovers via the canary — req →
        /// 'unmined' (attempts 0), tx → 'unproven', its de-credited change
        /// re-enabled, and the coin it consumed re-marked spent.
        #[tokio::test]
        async fn canary_recovers_mined_false_fail() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            // The consumed coin: released back to spendable when the
            // spender was wrongly failed.
            let prev_txid = "11".repeat(32);
            let prev_tx_id = insert_tx(&storage, user_id, &prev_txid, "completed").await;
            let prev_out = insert_output(&storage, user_id, prev_tx_id, &prev_txid, 0, true).await;

            // The false-failed spender: tx 'failed', change de-credited,
            // req 'invalid', aged past the 1h canary backoff.
            let (raw, txid) = spending_tx(&prev_txid, 0);
            let tx_id = insert_tx(&storage, user_id, &txid, "failed").await;
            let own_out = insert_output(&storage, user_id, tx_id, &txid, 0, false).await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;

            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "mined"))
                .is_utxo_response(MockResponse::Success(true))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            // Own change output re-enabled (is_utxo said true).
            assert_eq!(output_state(&storage, own_out).await, (true, None));
            // Consumed coin re-marked spent by this transaction.
            assert_eq!(output_state(&storage, prev_out).await, (false, Some(tx_id)));
        }

        /// No chain evidence → the row is re-stamped (attempts+1) but stays
        /// 'invalid' and watched; nothing else moves.
        #[tokio::test]
        async fn canary_restamps_unknown_and_keeps_watching() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "22".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;

            // Every source: the status sources, no broadcaster, no proof.
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 1));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// The hourly backoff: a row stamped within the last hour is not
        /// re-checked even when the chain would vouch for it.
        #[tokio::test]
        async fn canary_skips_recent_rows() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "33".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "mined"))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// An explicit 'unfail' request recovers when a merkle path exists,
        /// exercising the raw_tx-from-req input re-mark path.
        #[tokio::test]
        async fn explicit_unfail_recovers_with_proof() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "44".repeat(32);
            let prev_tx_id = insert_tx(&storage, user_id, &prev_txid, "completed").await;
            let prev_out = insert_output(&storage, user_id, prev_tx_id, &prev_txid, 0, true).await;

            let (raw, txid) = spending_tx(&prev_txid, 0);
            let tx_id = insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .is_utxo_response(MockResponse::Success(true))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, prev_out).await, (false, Some(tx_id)));
        }

        /// A provider outage must hold an explicit 'unfail' request at
        /// 'unfail' — terminalizing on missing evidence is the incident
        /// class where an outage makes a false-fail permanent.
        #[tokio::test]
        async fn explicit_unfail_holds_on_provider_error() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "55".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_merkle_path_response(MockResponse::Error(
                    MockErrorKind::ServiceError,
                    "provider outage".to_string(),
                ))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unfail".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        /// A positive no-proof answer moves an explicit 'unfail' request to
        /// 'invalid' — which the canary then keeps watching (not terminal).
        #[tokio::test]
        async fn explicit_unfail_no_proof_becomes_invalid() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;

            let prev_txid = "66".repeat(32);
            let (raw, txid) = spending_tx(&prev_txid, 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "unfail", &raw, 0).await;

            let mock = MockWalletServicesBuilder::default()
                .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                    name: Some("mock".to_string()),
                    merkle_path: None,
                    header: None,
                    error: None,
                    notes: vec![],
                }))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("invalid".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "failed");
        }

        // =================================================================
        // One broadcaster's refusal against every other source
        // (Calgooon/zanaadu-v2#357)
        // =================================================================

        use crate::monitor::ArcadeEventsTask;
        use crate::services::broadcast_memory::{
            BroadcastStatus, BROADCAST_STATUS_ACCEPTED, PROVIDER_ARCADE_V2,
            PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC,
        };
        use std::sync::atomic::{AtomicBool, Ordering};

        fn no_proof() -> MockResponse<GetMerklePathResult> {
            MockResponse::Success(GetMerklePathResult {
                name: Some("mock".to_string()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![],
            })
        }

        /// A broadcast transaction as the wallet holds it after a submit one
        /// broadcaster accepted: tx 'unproven', req 'unmined', its change
        /// spendable. Returns (txid, change output id).
        async fn seed_broadcast(storage: &StorageSqlx, seed: &str) -> (String, i64) {
            let user_id = create_user(storage).await;
            let (raw, txid) = spending_tx(&seed.repeat(32), 0);
            let tx_id = insert_tx(storage, user_id, &txid, "unproven").await;
            let change = insert_output(storage, user_id, tx_id, &txid, 0, true).await;
            insert_req_aged(storage, &txid, "unmined", &raw, 0).await;
            (txid, change)
        }

        async fn memory_status(storage: &StorageSqlx, txid: &str, provider: &str) -> String {
            storage
                .broadcast_status_of(txid, provider)
                .await
                .unwrap()
                .map(|r| r.status)
                .unwrap_or_default()
        }

        /// The incident (2026-10-07, the 1.9 MB post): Arcade 500 on submit,
        /// GorillaPool ARC accepted, then Arcade's stream said REJECTED 460
        /// "missing input source data" and the wallet failed a transaction
        /// that was MINED at 970030. Now the REJECTED is Arcade's own view:
        /// the transaction stays unproven with its change, and Arcade's later
        /// MINED takes it to the proof task.
        #[tokio::test]
        async fn arcade_rejected_after_gorillapool_accepted_stays_unproven_and_mined_proves_it() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a1").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();
            let trigger = AtomicBool::new(false);

            let applied =
                ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap();

            assert!(
                !applied,
                "a refusal another broadcaster overrules applies nothing"
            );
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
            // Arcade lacks it; GorillaPool's acceptance stands.
            assert_eq!(
                memory_status(&storage, &txid, PROVIDER_ARCADE_V2).await,
                "rejected"
            );
            assert_eq!(
                memory_status(&storage, &txid, PROVIDER_GORILLAPOOL_ARC).await,
                "accepted"
            );

            // Later MINED: the proof task is asked now.
            ArcadeEventsTask::apply_status_event(&storage, &txid, "MINED", &trigger)
                .await
                .unwrap();
            assert!(trigger.load(Ordering::SeqCst));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
        }

        /// No memory row, but the network holds it by a live read of another
        /// broadcaster (GorillaPool SEEN_ON_NETWORK): not final either.
        #[tokio::test]
        async fn refusal_overruled_by_another_broadcasters_live_status() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a2").await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_GORILLAPOOL_ARC.to_string(), BroadcastStatus::Seen),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// Every broadcaster refuses or never heard of it and the status
        /// sources do not know it: still hints (bsv-stack-lean #66). No
        /// word, the change counted; the re-ask and the proof task decide.
        /// Until 0.7.4 this was final: failed, req invalid, change dead.
        #[tokio::test]
        async fn a_refusal_from_every_source_is_still_a_hint() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "a3").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_TAAL_ARC.to_string(), BroadcastStatus::Unknown),
                    (
                        PROVIDER_GORILLAPOOL_ARC.to_string(),
                        BroadcastStatus::Rejected,
                    ),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// A conflict verdict (466, a competitor) is a hint like any other
        /// broadcaster's word (bsv-stack-lean #66): no word is written, the
        /// refusing broadcaster is remembered. Until 0.7.4 it was final
        /// unless the network held the transaction.
        #[tokio::test]
        async fn a_conflict_word_is_a_hint() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "a4").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_ACCEPTED)
                .await
                .unwrap();

            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, true)
                .await
                .unwrap();

            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(
                storage
                    .broadcast_status_of(&txid, PROVIDER_ARCADE_V2)
                    .await
                    .unwrap()
                    .map(|r| r.status),
                Some(crate::services::broadcast_memory::BROADCAST_STATUS_REJECTED.to_string())
            );
        }

        /// The canary asks EVERY source, not only the one that refused: the
        /// status sources and Arcade say nothing, GorillaPool says MINED.
        /// Recovered without a hand-set 'unfail'.
        #[tokio::test]
        async fn canary_recovers_on_another_broadcasters_mined() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;
            let (raw, txid) = spending_tx(&"a5".repeat(32), 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_GORILLAPOOL_ARC.to_string(), BroadcastStatus::Mined),
                ])
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// ... and a merkle path the chain tracker accepted is a source too.
        #[tokio::test]
        async fn canary_recovers_on_a_checked_merkle_path() {
            let storage = create_storage().await;
            let user_id = create_user(&storage).await;
            let (raw, txid) = spending_tx(&"a6".repeat(32), 0);
            insert_tx(&storage, user_id, &txid, "failed").await;
            insert_req_aged(&storage, &txid, "invalid", &raw, 2).await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(&txid, "unknown"))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));

            MonitorStorage::un_fail(&storage).await.unwrap();

            assert_eq!(req_state(&storage, &txid).await, ("unmined".into(), 0));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        // ============ LENS replay (not part of the lane) ============

        async fn age_memory(storage: &StorageSqlx, txid: &str, provider: &str, hours: i64) {
            sqlx::query(&format!(
                "UPDATE broadcast_seen SET seen_at = datetime('now', '-{} hours') WHERE txid = ? AND provider = ?",
                hours
            ))
            .bind(txid)
            .bind(provider)
            .execute(storage.writer())
            .await
            .unwrap();
        }

        fn evicted_everywhere(txid: &str) -> crate::services::mock::MockWalletServices {
            MockWalletServicesBuilder::default()
                .get_status_for_txids_response(status_response(txid, "unknown"))
                .get_merkle_path_response(no_proof())
                .broadcaster_statuses(vec![
                    (PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Rejected),
                    (PROVIDER_TAAL_ARC.to_string(), BroadcastStatus::Unknown),
                    (
                        PROVIDER_GORILLAPOOL_ARC.to_string(),
                        BroadcastStatus::Unknown,
                    ),
                ])
                .build()
        }

        /// LENS: GorillaPool SEEN six hours ago, evicted from every mempool
        /// since, then Arcade's REJECTED: a hint, no word, the change still
        /// counted (bsv-stack-lean #66). Until 0.7.4 the REJECTED was final
        /// here and the change written off.
        #[tokio::test]
        async fn lens_stale_seen_then_rejected_writes_no_word() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "b1").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            age_memory(&storage, &txid, PROVIDER_GORILLAPOOL_ARC, 6).await;
            WalletStorageProvider::set_services(&storage, Arc::new(evicted_everywhere(&txid)));
            let trigger = AtomicBool::new(false);
            assert!(
                !ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap()
            );
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(req_state(&storage, &txid).await.0, "unmined");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }

        /// LENS (corrected): a fresh SEEN still overrules (the incident).
        #[tokio::test]
        async fn lens_fresh_seen_still_overrules_rejected() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "b2").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            age_memory(&storage, &txid, PROVIDER_GORILLAPOOL_ARC, 1).await;
            WalletStorageProvider::set_services(&storage, Arc::new(evicted_everywhere(&txid)));
            assert!(!storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap());
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// LENS: every live source down (no broadcaster answers, status
        /// sources error), no memory row: the refusal is still a hint
        /// (bsv-stack-lean #66). Until 0.7.4 it was final, failing closed
        /// to `failed` as the reference does unconditionally.
        #[tokio::test]
        async fn lens_sources_down_refusal_is_a_hint() {
            let storage = create_storage().await;
            let (txid, _) = seed_broadcast(&storage, "b3").await;
            let mock = MockWalletServicesBuilder::default()
                .get_status_for_txids_response(MockResponse::Error(
                    crate::services::mock::MockErrorKind::NetworkError,
                    "down".into(),
                ))
                .build();
            WalletStorageProvider::set_services(&storage, Arc::new(mock));
            let applied = storage
                .mark_transaction_rejected_by(&txid, PROVIDER_ARCADE_V2, false)
                .await
                .unwrap();
            assert!(!applied);
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
        }

        /// LENS: the incident with SEEN (not ACCEPTED) from GorillaPool, then
        /// Arcade MINED: stays unproven, change visible, proof triggered.
        #[tokio::test]
        async fn lens_incident_with_seen_then_mined() {
            let storage = create_storage().await;
            let (txid, change) = seed_broadcast(&storage, "b4").await;
            storage
                .record_broadcast_status(&txid, PROVIDER_GORILLAPOOL_ARC, "seen")
                .await
                .unwrap();
            let trigger = AtomicBool::new(false);
            assert!(
                !ArcadeEventsTask::apply_status_event(&storage, &txid, "REJECTED", &trigger)
                    .await
                    .unwrap()
            );
            ArcadeEventsTask::apply_status_event(&storage, &txid, "MINED", &trigger)
                .await
                .unwrap();
            assert!(trigger.load(Ordering::SeqCst));
            assert_eq!(tx_status(&storage, &txid).await, "unproven");
            assert_eq!(output_state(&storage, change).await, (true, None));
        }
    }
}
