//! Wallet Storage Manager
//!
//! Provides multi-storage synchronization with active/backup semantics and
//! concurrency control through lock queues.
//!
//! The `WalletStorageManager` manages multiple `WalletStorageProvider` instances:
//! - One is designated as "active" for all write operations
//! - Others serve as backups for redundancy
//! - Conflicting actives are detected when storage providers disagree on which should be active
//!
//! # Lock Hierarchy
//!
//! The manager implements a lock queue system to prevent concurrent access issues:
//! - **Reader Lock**: For read-only operations (multiple readers allowed)
//! - **Writer Lock**: For write operations (exclusive with readers)
//! - **Sync Lock**: For synchronization operations (exclusive with all)
//! - **Provider Lock**: For StorageProvider-level operations (highest precedence)

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::lock_utils::{lock_read, lock_write};

use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, InternalizeActionArgs, ListActionsArgs,
    ListActionsResult, ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs,
    ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs,
};

use crate::error::{Error, Result};
use crate::services::WalletServices;
use chrono::{DateTime, Utc};

use crate::storage::{
    entities::{
        TableCertificate, TableCertificateField, TableOutput, TableOutputBasket, TableProvenTxReq,
        TableSettings, TableSyncState, TableTransaction, TableUser,
    },
    AuthId, FindCertificatesArgs, FindOutputBasketsArgs, FindOutputsArgs, FindProvenTxReqsArgs,
    FindTransactionsArgs, MonitorStorage, ProcessSyncChunkResult, PurgeParams, PurgeResults,
    RequestSyncChunkArgs, ReviewStatusResult, StorageCreateActionResult,
    StorageInternalizeActionResult, StorageProcessActionArgs, StorageProcessActionResults,
    SyncChunk, SyncMap, SyncOffset, TrxToken, TxSynchronizedStatus, WalletStorageInfo,
    WalletStorageProvider, WalletStorageReader, WalletStorageSync, WalletStorageWriter,
};

/// A wrapper around a storage provider with cached state.
#[derive(Clone)]
pub struct ManagedStorage {
    /// The underlying storage provider (also implements MonitorStorage).
    pub storage: Arc<dyn MonitorStorage>,
    /// Whether the storage is available and initialized.
    pub is_available: bool,
    /// Whether the storage supports full StorageProvider interface.
    pub is_storage_provider: bool,
    /// Cached settings from the storage.
    pub settings: Option<TableSettings>,
    /// Cached user record from the storage.
    pub user: Option<TableUser>,
}

impl ManagedStorage {
    /// Creates a new managed storage wrapper.
    pub fn new(storage: Arc<dyn MonitorStorage>) -> Self {
        let is_storage_provider = storage.is_storage_provider();
        Self {
            storage,
            is_available: false,
            is_storage_provider,
            settings: None,
            user: None,
        }
    }

    /// Whether the manager has opened this store: it answered
    /// `make_available` and the wallet's user row, both held here.
    fn is_open(&self) -> bool {
        self.is_available && self.settings.is_some() && self.user.is_some()
    }
}

/// Default lock acquisition timeout in seconds.
const LOCK_TIMEOUT_SECS: u64 = 30;

/// Lock queue for managing concurrent access.
/// Uses a Semaphore(1) for FIFO mutual exclusion with automatic release on drop.
/// This ensures locks are released even when tasks are aborted or panic.
type LockQueue = Arc<tokio::sync::Semaphore>;

/// Manages multiple storage providers with active/backup semantics.
///
/// # Features
///
/// - **Multi-storage**: Manages one active and multiple backup storage providers
/// - **Synchronization**: Syncs state from active to backups
/// - **Conflict detection**: Detects when providers disagree on active selection
/// - **Concurrency control**: Lock queues for reader/writer/sync operations
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
///
/// // Create manager with active and backup storages
/// let manager = WalletStorageManager::new(
///     identity_key.to_string(),
///     Some(active_storage),
///     Some(vec![backup_storage]),
/// );
///
/// // Initialize and make available
/// let settings = manager.make_available().await?;
///
/// // Use with locking
/// let result = manager.run_as_writer(|writer| async {
///     writer.create_action(&auth, args).await
/// }).await?;
/// ```
pub struct WalletStorageManager {
    /// All configured stores including active, backups, and conflicting.
    stores: RwLock<Vec<ManagedStorage>>,

    /// Whether makeAvailable has been called and storage is ready.
    is_available: RwLock<bool>,

    /// The current active store index (in stores vec).
    active_index: RwLock<Option<usize>>,

    /// Indices of backup stores.
    backup_indices: RwLock<Vec<usize>>,

    /// Indices of stores with conflicting active selection.
    conflicting_indices: RwLock<Vec<usize>>,

    /// Authentication identifier for the user.
    auth_id: RwLock<AuthId>,

    /// Configured services (shared with stores), async access.
    services: RwLock<Option<Arc<dyn WalletServices>>>,

    /// Cached services for synchronous `WalletStorageReader::get_services()`.
    /// Updated whenever `set_services()` is called.
    services_sync: std::sync::RwLock<Option<Arc<dyn WalletServices>>>,

    /// Cached settings from the active store for synchronous
    /// `WalletStorageReader::get_settings()`. Updated when `make_available()` completes.
    cached_settings: std::sync::RwLock<Option<TableSettings>>,

    /// Lock queue for reader operations.
    reader_locks: LockQueue,

    /// Lock queue for writer operations.
    writer_locks: LockQueue,

    /// Lock queue for sync operations.
    sync_locks: LockQueue,

    /// Lock queue for storage provider operations.
    #[allow(dead_code)]
    provider_locks: LockQueue,

    /// One `set_active` transition at a time; no other operation waits on it.
    set_active_lock: tokio::sync::Mutex<()>,

    /// One open (`make_available` past its answer when every store is
    /// open) at a time: a second open waits for the one running and then
    /// finds the stores it opened open. Never taken while a state lock is
    /// held; taken under the transition lock when a `set_active` opens.
    open_lock: tokio::sync::Mutex<()>,

    /// Set before any call of a `set_active` transition that can write a
    /// store's row, cleared when the transition completes or when the rows
    /// have been read again after its error: while set, the rows the manager
    /// holds may not be the stores' rows (a call dropped at an await, or a
    /// store's panic), so the manager answers its active as not enabled.
    rows_in_doubt: std::sync::atomic::AtomicBool,
}

const ENTITY_NAMES: [&str; 12] = [
    "outputBasket",
    "provenTx",
    "provenTxReq",
    "txLabel",
    "outputTag",
    "transaction",
    "output",
    "txLabelMap",
    "outputTagMap",
    "certificate",
    "certificateField",
    "commission",
];

fn entity_row_count(chunk: &SyncChunk, entity: &str) -> u32 {
    let len = |n: usize| n as u32;
    match entity {
        "outputBasket" => chunk.output_baskets.as_ref().map(|v| len(v.len())),
        "provenTx" => chunk.proven_txs.as_ref().map(|v| len(v.len())),
        "provenTxReq" => chunk.proven_tx_reqs.as_ref().map(|v| len(v.len())),
        "txLabel" => chunk.tx_labels.as_ref().map(|v| len(v.len())),
        "outputTag" => chunk.output_tags.as_ref().map(|v| len(v.len())),
        "transaction" => chunk.transactions.as_ref().map(|v| len(v.len())),
        "output" => chunk.outputs.as_ref().map(|v| len(v.len())),
        "txLabelMap" => chunk.tx_label_maps.as_ref().map(|v| len(v.len())),
        "outputTagMap" => chunk.output_tag_maps.as_ref().map(|v| len(v.len())),
        "certificate" => chunk.certificates.as_ref().map(|v| len(v.len())),
        "certificateField" => chunk.certificate_fields.as_ref().map(|v| len(v.len())),
        "commission" => chunk.commissions.as_ref().map(|v| len(v.len())),
        _ => None,
    }
    .unwrap_or(0)
}

/// Which `active_storage` a drained chunk's user row carries to the writer.
enum UserActiveStorage {
    /// The reader's: a push from the active store (backups, set_active).
    FromReader,
    /// The writer's own: merging state from a reader cannot change which
    /// store the wallet treats as active (the reference's syncFromReader).
    /// `None` (the manager holds no active for the writer's row) hands the
    /// writer no user row: the writer's row of the user is left as it was.
    Keep(Option<String>),
}

/// Whether two declared chains are one chain: this crate's stores declare
/// `mainnet` (or `testnet`), the reference's stores `main` (or `test`)
/// (ts-stack 11faa7d16 `storage/sync/syncFailure.ts` 6); every other word
/// is one chain with itself alone.
fn same_chain(a: &str, b: &str) -> bool {
    fn canonical(chain: &str) -> &str {
        match chain {
            "mainnet" => "main",
            "testnet" => "test",
            other => other,
        }
    }
    canonical(a) == canonical(b)
}

async fn drain_sync_chunks(
    reader: Arc<dyn MonitorStorage>,
    writer: Arc<dyn MonitorStorage>,
    identity_key: &str,
    user_active: UserActiveStorage,
) -> Result<(u32, u32)> {
    // The reader is made available before it is asked whether it is local
    // (in drain_chunks, around each chunk): a manager answers by its active
    // store, which it chooses there.
    let reader_settings = reader.make_available().await?;
    let drain = drain_chunks(reader, reader_settings, writer, identity_key, user_active);
    crate::storage::sync_scope::scope(drain).await
}

async fn drain_chunks(
    reader: Arc<dyn MonitorStorage>,
    reader_settings: TableSettings,
    writer: Arc<dyn MonitorStorage>,
    identity_key: &str,
    user_active: UserActiveStorage,
) -> Result<(u32, u32)> {
    let writer_settings = writer.make_available().await?;
    // Stores of one chain alone, before the writer's user or any chunk (the
    // reference's assertSyncNetwork).
    if !same_chain(&reader_settings.chain, &writer_settings.chain) {
        return Err(Error::SyncError(
            "Live sync requires matching declared network chains on both storage providers."
                .to_string(),
        ));
    }
    // A chunk never creates its user on the writer: the manager makes the
    // writer's user before any chunk, as the reference's
    // loadSyncRequest does with findOrInsertUser.
    writer.find_or_insert_user(identity_key).await?;

    let mut offsets: Vec<SyncOffset> = ENTITY_NAMES
        .iter()
        .map(|name| SyncOffset {
            name: (*name).to_string(),
            offset: 0,
        })
        .collect();

    let mut inserts = 0u32;
    let mut updates = 0u32;
    let mut sync_map = SyncMap::default();

    loop {
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: reader_settings.storage_identity_key.clone(),
            to_storage_identity_key: writer_settings.storage_identity_key.clone(),
            identity_key: identity_key.to_string(),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: offsets.clone(),
        };

        // A chunk from a local reader is the wallet's own rows: a local
        // writer takes its new proof rows as given. The reader is asked
        // before and after the chunk is read (a manager answers by its
        // active store, which another task may move meanwhile); a chunk is
        // the wallet's own only when both answers are local. Any other
        // chunk is validated as from another party: the writer validates
        // every proof row it does not hold as the same proof
        // (crate::storage::sync_scope).
        let local_before = reader.checks_received_beef_in_process().await;
        let mut chunk = reader.get_sync_chunk(args.clone()).await?;
        let local_after = reader.checks_received_beef_in_process().await;
        crate::storage::sync_scope::set_local_reader(local_before && local_after);
        // A sync writes into the writer's user row only an active the
        // manager itself holds. While it holds none (a row it could not
        // read) the chunk goes with no user row: neither an empty active
        // nor the reader's reaches the writer, and every other row of the
        // chunk is merged as before.
        match &user_active {
            UserActiveStorage::Keep(Some(local)) => {
                if let Some(user) = chunk.user.as_mut() {
                    user.active_storage = Some(local.clone());
                }
            }
            UserActiveStorage::Keep(None) => chunk.user = None,
            UserActiveStorage::FromReader => {}
        }

        let mut carried = 0u32;
        for off in offsets.iter_mut() {
            let rows = entity_row_count(&chunk, &off.name);
            off.offset += rows;
            carried += rows;
        }

        let result = writer
            .process_sync_chunk_mapped(args, chunk, &mut sync_map)
            .await?;
        // A failure the writer returns is read ahead of its counts and its
        // `done` (the reference's throwSyncResultError).
        if let Some(error) = result.error {
            return Err(Error::SyncError(error));
        }
        // An unfinished answer must advance the offsets: a chunk that carried
        // no row cannot be followed by another (the reference's
        // assertSyncProgress).
        if !result.done && carried == 0 {
            return Err(Error::InvalidOperation(
                "Sync provider reported an unfinished page without advancing its durable checkpoint."
                    .to_string(),
            ));
        }
        inserts += result.inserts;
        updates += result.updates;

        if result.done {
            break;
        }
    }

    Ok((inserts, updates))
}

impl WalletStorageManager {
    /// Creates a new storage manager.
    ///
    /// # Arguments
    ///
    /// * `identity_key` - The user's identity public key
    /// * `active` - Optional active storage provider
    /// * `backups` - Optional list of backup storage providers
    pub fn new(
        identity_key: String,
        active: Option<Arc<dyn MonitorStorage>>,
        backups: Option<Vec<Arc<dyn MonitorStorage>>>,
    ) -> Self {
        let mut stores = Vec::new();

        // Add active first (if provided)
        if let Some(a) = active {
            stores.push(ManagedStorage::new(a));
        }

        // Add backups
        if let Some(b) = backups {
            for storage in b {
                stores.push(ManagedStorage::new(storage));
            }
        }

        Self {
            stores: RwLock::new(stores),
            is_available: RwLock::new(false),
            active_index: RwLock::new(None),
            backup_indices: RwLock::new(Vec::new()),
            conflicting_indices: RwLock::new(Vec::new()),
            auth_id: RwLock::new(AuthId::new(identity_key)),
            services: RwLock::new(None),
            services_sync: std::sync::RwLock::new(None),
            cached_settings: std::sync::RwLock::new(None),
            reader_locks: Arc::new(tokio::sync::Semaphore::new(1)),
            writer_locks: Arc::new(tokio::sync::Semaphore::new(1)),
            sync_locks: Arc::new(tokio::sync::Semaphore::new(1)),
            provider_locks: Arc::new(tokio::sync::Semaphore::new(1)),
            set_active_lock: tokio::sync::Mutex::new(()),
            open_lock: tokio::sync::Mutex::new(()),
            rows_in_doubt: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Returns whether storage is available and ready.
    pub async fn is_available(&self) -> bool {
        *self.is_available.read().await
    }

    /// Returns whether at least one storage provider is configured.
    pub fn can_make_available(&self) -> bool {
        // Note: This needs to be synchronous for the trait impl
        // We'll check the stores length which is set at construction
        true // Will fail in make_available if no stores
    }

    /// Returns whether the active storage is enabled.
    ///
    /// The active is enabled only if:
    /// 1. An active storage is selected
    /// 2. Its storageIdentityKey matches the user's activeStorage selection
    /// 3. There are no conflicting actives
    /// 4. Every store's row of the wallet's user, as the manager holds it,
    ///    names it (a `set_active` that failed part way leaves them apart)
    /// 5. No `set_active` transition has begun to write without completing
    ///    or reading every store's row again (a call dropped at an await)
    pub async fn is_active_enabled(&self) -> bool {
        if self.rows_in_doubt() {
            return false;
        }
        let stores = self.stores.read().await;
        let active_index = self.active_index.read().await;
        let conflicting = self.conflicting_indices.read().await;

        if let Some(idx) = *active_index {
            if let Some(settings) = stores.get(idx).and_then(|s| s.settings.as_ref()) {
                let key = &settings.storage_identity_key;
                return conflicting.is_empty()
                    && stores.iter().all(|s| {
                        s.user.as_ref().and_then(|u| u.active_storage.as_ref()) == Some(key)
                    });
            }
        }
        false
    }

    fn rows_in_doubt(&self) -> bool {
        self.rows_in_doubt.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn set_rows_in_doubt(&self, in_doubt: bool) {
        self.rows_in_doubt
            .store(in_doubt, std::sync::atomic::Ordering::SeqCst);
    }

    /// Reads each store's row of the wallet's user again into the manager's
    /// cached rows: a sync may have moved a backup's row since the manager
    /// read it.
    async fn refresh_user_rows(&self) -> Result<()> {
        let identity_key = self.auth_id.read().await.identity_key.clone();
        let storages: Vec<_> = self
            .stores
            .read()
            .await
            .iter()
            .map(|s| s.storage.clone())
            .collect();
        let mut users = Vec::with_capacity(storages.len());
        for storage in &storages {
            users.push(storage.find_or_insert_user(&identity_key).await?.0);
        }
        let mut stores = self.stores.write().await;
        for (store, user) in stores.iter_mut().zip(users) {
            store.user = Some(user);
        }
        Ok(())
    }

    /// Whether every store's cached row of the wallet's user names `key`.
    async fn every_row_names(&self, key: &str) -> bool {
        self.stores
            .read()
            .await
            .iter()
            .all(|s| s.user.as_ref().and_then(|u| u.active_storage.as_deref()) == Some(key))
    }

    /// The active's settings when the manager is available and every store
    /// has been opened: `make_available` then calls no store.
    async fn opened_settings(&self) -> Option<TableSettings> {
        if !*self.is_available.read().await {
            return None;
        }
        let stores = self.stores.read().await;
        let active_idx = self.active_index.read().await;
        if !stores.iter().all(ManagedStorage::is_open) {
            return None;
        }
        (*active_idx)
            .and_then(|idx| stores.get(idx))
            .and_then(|store| store.settings.clone())
    }

    /// Initializes and makes the storage available.
    ///
    /// This method:
    /// 1. Validates all storage providers
    /// 2. Partitions them into active, backups, and conflicting
    /// 3. Sets up the user authentication
    ///
    /// # Returns
    ///
    /// The settings from the active storage.
    pub async fn make_available(&self) -> Result<TableSettings> {
        if let Some(settings) = self.opened_settings().await {
            return Ok(settings);
        }
        // One open at a time: the open lock is held across the stores'
        // calls (no state lock is), so a second open waits here and then
        // finds every store the open ahead of it opened open.
        let _open = self.open_lock.lock().await;
        if let Some(settings) = self.opened_settings().await {
            return Ok(settings);
        }

        // Reset state
        *self.active_index.write().await = None;
        *self.backup_indices.write().await = Vec::new();
        *self.conflicting_indices.write().await = Vec::new();

        let identity_key = self.auth_id.read().await.identity_key.clone();
        // The stores to initialize, copied out: no state lock is held while
        // a store answers.
        let pending: Vec<(usize, Arc<dyn MonitorStorage>)> = {
            let stores = self.stores.read().await;
            if stores.is_empty() {
                return Err(Error::InvalidArgument(
                    "Must add at least one storage provider".to_string(),
                ));
            }
            stores
                .iter()
                .enumerate()
                .filter(|(_, s)| !s.is_open())
                .map(|(i, s)| (i, s.storage.clone()))
                .collect()
        };

        // Initialize each store and find/create its user; the answers of the
        // stores that answered are written even when a later store fails.
        let mut answered = Vec::with_capacity(pending.len());
        let mut outcome = Ok(());
        for (i, storage) in pending {
            let settings = match storage.make_available().await {
                Ok(settings) => settings,
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            };
            match storage.find_or_insert_user(&identity_key).await {
                Ok((user, _)) => answered.push((i, storage, settings, user)),
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        {
            // Stores are only ever appended, so an index names the store it
            // was copied from while that store is the one at the index.
            let mut stores = self.stores.write().await;
            for (i, storage, settings, user) in answered {
                if let Some(store) = stores
                    .get_mut(i)
                    .filter(|s| Arc::ptr_eq(&s.storage, &storage))
                {
                    store.settings = Some(settings);
                    store.user = Some(user);
                    store.is_available = true;
                }
            }
        }
        outcome?;
        self.partition().await
    }

    /// Partitions the stores into active, backups and conflicting from the
    /// rows of the wallet's user the manager holds, and answers the active's
    /// settings: at open (`make_available`), after a completed `set_active`
    /// transition, and after the rows are read again following its error
    /// when every store was read and every row names one store.
    ///
    /// All or nothing against a dropped or panicking call: everything it
    /// writes is computed under the stores read lock; then, that lock still
    /// held, it takes, in this order, `active_index`, `backup_indices`,
    /// `conflicting_indices`, `auth_id` and `is_available` (each for write)
    /// and writes every field, `cached_settings` last, with no await between
    /// its writes. A call dropped while it waits on a lock has changed none
    /// of the manager's fields; once it holds them all it has no await left.
    /// It holds the stores read lock from its read of the rows through its
    /// last write: a cached row is written only under the stores write
    /// lock, so no row moves between a partition's read and its writes, and
    /// a partition that read older rows cannot write after one that read
    /// newer. It cannot wait on itself: no caller holds a state lock when it
    /// calls (`make_available` releases the stores before, `set_active` and
    /// the transition hold only the transition lock), it takes the stores
    /// ahead of the five as every other taker does, and a writer queued on
    /// the stores holds no other state lock while it waits.
    ///
    /// The manager's locks, in their one order: the transition lock
    /// (`set_active_lock`, one `set_active` at a time), then the open lock
    /// (`open_lock`, one open of the stores at a time), then the state locks.
    /// The two outer locks are the only ones held across store calls, by
    /// design; neither is ever taken while a state lock is held, and an open
    /// never takes the transition lock. A state lock (`stores`, `active_index`, `backup_indices`,
    /// `conflicting_indices`, `auth_id`, `is_available`, `services`) is held
    /// only to copy out what a function needs (the stores it will call,
    /// cloned; the indices; the auth) or to write the manager's own fields:
    /// never across a store's call, and never across a call that takes one
    /// of the manager's locks again. A taker of two state locks takes
    /// `stores` ahead of the five above and these in the order above
    /// (`is_active_enabled`, `get_stores`, `get_settings`, `get_active`,
    /// `get_backup_stores`, `get_conflicting_stores`, `make_available`'s
    /// answer when available, `checks_received_beef_in_process`, this
    /// function), and while it holds one awaits nothing but the next in that
    /// order. So every holder of a state lock releases it without waiting on
    /// a store or on a holder of an earlier lock in that order, a writer
    /// queued on a lock included.
    async fn partition(&self) -> Result<TableSettings> {
        let stores = self.stores.read().await;
        // Partition stores into active, backups, and conflicting
        let mut active_idx: Option<usize> = None;
        let mut backups_temp: Vec<usize> = Vec::new();

        for (i, store) in stores.iter().enumerate() {
            if active_idx.is_none() {
                // First store becomes default active
                active_idx = Some(i);
            } else {
                let user_active = store.user.as_ref().unwrap().active_storage.clone();
                let store_identity = store
                    .settings
                    .as_ref()
                    .unwrap()
                    .storage_identity_key
                    .clone();

                // Check if this store's user record selects it as active
                if user_active.as_ref() == Some(&store_identity) {
                    // Check if current active is not enabled
                    let current_active = &stores[active_idx.unwrap()];
                    let current_settings = current_active.settings.as_ref().unwrap();
                    let current_user = current_active.user.as_ref().unwrap();

                    if current_user.active_storage.as_ref()
                        != Some(&current_settings.storage_identity_key)
                    {
                        // Swap: this store should be active
                        backups_temp.push(active_idx.unwrap());
                        active_idx = Some(i);
                        continue;
                    }
                }
                backups_temp.push(i);
            }
        }

        // Now partition backups into actual backups and conflicting
        let mut backups: Vec<usize> = Vec::new();
        let mut conflicting: Vec<usize> = Vec::new();

        if let Some(active) = active_idx {
            let active_storage_identity = stores[active]
                .settings
                .as_ref()
                .unwrap()
                .storage_identity_key
                .clone();

            for idx in backups_temp {
                let user_active = stores[idx].user.as_ref().unwrap().active_storage.clone();
                if user_active.as_ref() != Some(&active_storage_identity) {
                    conflicting.push(idx);
                } else {
                    backups.push(idx);
                }
            }
        }

        let active = active_idx.map(|idx| {
            let user = stores[idx].user.as_ref().unwrap();
            let settings = stores[idx].settings.clone().unwrap();
            let is_active = user.active_storage.as_ref() == Some(&settings.storage_identity_key)
                && conflicting.is_empty();
            (user.user_id, is_active, settings)
        });

        // Every lock, in the order above, the stores still held; then every
        // write, no await.
        let mut active_index = self.active_index.write().await;
        let mut backup_indices = self.backup_indices.write().await;
        let mut conflicting_indices = self.conflicting_indices.write().await;
        let mut auth = self.auth_id.write().await;
        let mut is_available = self.is_available.write().await;
        *active_index = active_idx;
        *backup_indices = backups;
        *conflicting_indices = conflicting;
        if let Some((user_id, is_active, _)) = &active {
            auth.user_id = Some(*user_id);
            auth.is_active = Some(*is_active);
        }
        *is_available = true;

        // Return active settings and update cached_settings for sync access
        match active {
            Some((_, _, settings)) => {
                if let Ok(mut guard) = lock_write(&self.cached_settings) {
                    *guard = Some(settings.clone());
                }
                Ok(settings)
            }
            None => Err(Error::StorageNotAvailable),
        }
    }

    /// Gets the authentication identifier, initializing if needed.
    pub async fn get_auth(&self, must_be_active: bool) -> Result<AuthId> {
        if !*self.is_available.read().await {
            self.make_available().await?;
        }

        let auth = self.auth_id.read().await.clone();

        if must_be_active && !auth.is_active.unwrap_or(false) {
            return Err(Error::AccessDenied(
                "Operation requires active storage".to_string(),
            ));
        }

        Ok(auth)
    }

    /// Gets a reference to the active storage provider.
    async fn get_active(&self) -> Result<Arc<dyn MonitorStorage>> {
        let stores = self.stores.read().await;
        let active_idx = self.active_index.read().await;

        if let Some(idx) = *active_idx {
            if *self.is_available.read().await {
                return Ok(stores[idx].storage.clone());
            }
        }

        Err(Error::InvalidOperation(
            "Active storage not available. Call make_available first.".to_string(),
        ))
    }

    /// Acquires a lock from the specified queue with a timeout.
    ///
    /// Returns an error if the lock cannot be acquired within `LOCK_TIMEOUT_SECS` seconds.
    async fn acquire_lock(
        queue: &LockQueue,
        lock_name: &str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        match tokio::time::timeout(
            Duration::from_secs(LOCK_TIMEOUT_SECS),
            Arc::clone(queue).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_closed)) => Err(Error::LockTimeout(format!(
                "Semaphore closed for {} lock",
                lock_name
            ))),
            Err(_elapsed) => Err(Error::LockTimeout(format!(
                "Timed out after {}s waiting for {} lock",
                LOCK_TIMEOUT_SECS, lock_name
            ))),
        }
    }

    /// Runs a closure with reader lock.
    pub async fn run_as_reader<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let _reader_permit = Self::acquire_lock(&self.reader_locks, "reader").await?;

        if !*self.is_available.read().await {
            self.make_available().await?;
        }

        let active = self.get_active().await?;
        f(active).await
        // _reader_permit drops here — lock released automatically, even on abort/panic
    }

    /// Runs a closure with writer lock.
    pub async fn run_as_writer<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let _reader_permit = Self::acquire_lock(&self.reader_locks, "reader").await?;
        let _writer_permit = Self::acquire_lock(&self.writer_locks, "writer").await?;

        if !*self.is_available.read().await {
            self.make_available().await?;
        }

        let active = self.get_active().await?;
        f(active).await
        // permits drop here — locks released automatically, even on abort/panic
    }

    /// Runs a closure with sync lock.
    pub async fn run_as_sync<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let _reader_permit = Self::acquire_lock(&self.reader_locks, "reader").await?;
        let _writer_permit = Self::acquire_lock(&self.writer_locks, "writer").await?;
        let _sync_permit = Self::acquire_lock(&self.sync_locks, "sync").await?;

        if !*self.is_available.read().await {
            self.make_available().await?;
        }

        let active = self.get_active().await?;
        f(active).await
        // permits drop here — locks released automatically, even on abort/panic
    }

    /// Sets the wallet services for all storage providers.
    pub async fn set_services(&self, services: Arc<dyn WalletServices>) {
        let storages: Vec<_> = self
            .stores
            .read()
            .await
            .iter()
            .map(|s| s.storage.clone())
            .collect();
        for storage in storages {
            storage.set_services(services.clone());
        }
        // Update sync-accessible cache for WalletStorageReader::get_services()
        if let Ok(mut guard) = lock_write(&self.services_sync) {
            *guard = Some(services.clone());
        }
        *self.services.write().await = Some(services);
    }

    /// Gets the wallet services.
    pub async fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        self.services
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::InvalidOperation("Must call set_services first".to_string()))
    }

    /// Adds a new storage provider.
    pub async fn add_wallet_storage_provider(
        &self,
        provider: Arc<dyn MonitorStorage>,
    ) -> Result<()> {
        provider.make_available().await?;

        let services = self.services.read().await.clone();
        if let Some(services) = services {
            provider.set_services(services);
        }

        self.stores
            .write()
            .await
            .push(ManagedStorage::new(provider));
        *self.is_available.write().await = false;

        self.make_available().await?;
        Ok(())
    }

    /// Gets information about all configured stores.
    pub async fn get_stores(&self) -> Vec<WalletStorageInfo> {
        let stores = self.stores.read().await;
        let active_idx = self.active_index.read().await;
        let backup_indices = self.backup_indices.read().await;
        let conflicting_indices = self.conflicting_indices.read().await;
        let in_doubt = self.rows_in_doubt();

        let mut result = Vec::new();

        for (i, store) in stores.iter().enumerate() {
            let is_active = active_idx.map(|idx| idx == i).unwrap_or(false);
            let is_backup = backup_indices.contains(&i);
            let is_conflicting = conflicting_indices.contains(&i);

            let is_enabled = if is_active {
                if let (Some(settings), Some(user)) = (&store.settings, &store.user) {
                    user.active_storage.as_ref() == Some(&settings.storage_identity_key)
                        && conflicting_indices.is_empty()
                        && !in_doubt
                } else {
                    false
                }
            } else {
                false
            };

            result.push(WalletStorageInfo {
                is_active,
                is_enabled,
                is_backup,
                is_conflicting,
                user_id: store.user.as_ref().map(|u| u.user_id).unwrap_or(0),
                storage_identity_key: store
                    .settings
                    .as_ref()
                    .map(|s| s.storage_identity_key.clone())
                    .unwrap_or_default(),
                storage_name: store
                    .settings
                    .as_ref()
                    .map(|s| s.storage_name.clone())
                    .unwrap_or_default(),
                storage_class: "WalletStorageProvider".to_string(),
                endpoint_url: None,
            });
        }

        result
    }

    /// Gets the active storage's settings.
    pub async fn get_settings(&self) -> Result<TableSettings> {
        let stores = self.stores.read().await;
        let active_idx = self.active_index.read().await;

        if let Some(idx) = *active_idx {
            if let Some(settings) = &stores[idx].settings {
                return Ok(settings.clone());
            }
        }

        Err(Error::StorageNotAvailable)
    }

    /// Gets the active store's identity key.
    pub async fn get_active_store(&self) -> Result<String> {
        Ok(self.get_settings().await?.storage_identity_key)
    }

    /// Gets the backup store identity keys.
    pub async fn get_backup_stores(&self) -> Vec<String> {
        let stores = self.stores.read().await;
        let backup_indices = self.backup_indices.read().await;

        backup_indices
            .iter()
            .filter_map(|&idx| {
                stores
                    .get(idx)
                    .and_then(|s| s.settings.as_ref())
                    .map(|s| s.storage_identity_key.clone())
            })
            .collect()
    }

    /// Gets the conflicting store identity keys.
    pub async fn get_conflicting_stores(&self) -> Vec<String> {
        let stores = self.stores.read().await;
        let conflicting_indices = self.conflicting_indices.read().await;

        conflicting_indices
            .iter()
            .filter_map(|&idx| {
                stores
                    .get(idx)
                    .and_then(|s| s.settings.as_ref())
                    .map(|s| s.storage_identity_key.clone())
            })
            .collect()
    }

    /// The manager's sync entries serve its own identity alone, as the
    /// reference's StorageSyncReader and syncToWriter refuse another
    /// identity: every entry that takes an identity calls this before anything else.
    async fn check_sync_identity(&self, identity_key: &str) -> Result<()> {
        if identity_key != self.auth_id.read().await.identity_key {
            return Err(Error::AccessDenied("Identity key mismatch".to_string()));
        }
        Ok(())
    }

    /// The active store's user row's `active_storage`, as this manager holds it.
    async fn active_user_active_storage(&self) -> Option<String> {
        let idx = (*self.active_index.read().await)?;
        let stores = self.stores.read().await;
        stores.get(idx)?.user.as_ref()?.active_storage.clone()
    }

    /// Synchronizes from a reader storage to the active storage.
    pub async fn sync_from_reader(
        &self,
        identity_key: &str,
        reader: Arc<dyn MonitorStorage>,
    ) -> Result<SyncResult> {
        self.check_sync_identity(identity_key).await?;
        self.get_auth(false).await?;

        let reader_settings = reader.make_available().await?;
        let mut log = String::new();
        let local_active = self.active_user_active_storage().await;

        self.run_as_sync(|active| async move {
            let writer_settings = active.get_settings();
            log.push_str(&format!(
                "syncFromReader from {} to {}\n",
                reader_settings.storage_name, writer_settings.storage_name
            ));

            let (inserts, updates) = drain_sync_chunks(
                reader.clone(),
                active,
                identity_key,
                UserActiveStorage::Keep(local_active),
            )
            .await?;

            log.push_str(&format!(
                "syncFromReader complete: {} inserts, {} updates\n",
                inserts, updates
            ));

            Ok(SyncResult {
                inserts,
                updates,
                log,
            })
        })
        .await
    }

    /// Updates all backup storages from the active storage.
    pub async fn update_backups(&self) -> Result<String> {
        let auth = self.get_auth(true).await?;
        let mut log = String::new();

        let backup_indices = self.backup_indices.read().await.clone();
        // The backups, copied out: each sync takes the manager's locks
        // again (`run_as_sync`, `get_active`), so none is held across it.
        let backups: Vec<Arc<dyn MonitorStorage>> = {
            let stores = self.stores.read().await;
            backup_indices
                .iter()
                .filter_map(|&idx| stores.get(idx).map(|s| s.storage.clone()))
                .collect()
        };

        log.push_str(&format!(
            "BACKUP CURRENT ACTIVE TO {} STORES\n",
            backup_indices.len()
        ));

        // A `set_active` from another task can run between two of these
        // syncs: each backup is synced only while it is still one of the
        // manager's stores and not its active, as read under the sync lock.
        for backup in backups {
            match self
                .sync_to_writer_unless_active(&auth.identity_key, backup.clone())
                .await?
            {
                Some(sync_result) => log.push_str(&sync_result.log),
                None => log.push_str(&format!(
                    "syncToWriter passes over {}: no longer a backup\n",
                    backup.storage_name()
                )),
            }
        }

        Ok(log)
    }

    /// Synchronizes from the active storage to a writer storage.
    pub async fn sync_to_writer(
        &self,
        identity_key: &str,
        writer: Arc<dyn MonitorStorage>,
    ) -> Result<SyncResult> {
        self.sync_to_writer_from_active(identity_key, writer, false)
            .await
            .and_then(|synced| synced.ok_or(Error::StorageNotAvailable))
    }

    /// `sync_to_writer` for a backup: `None` (no sync) when the writer is
    /// the manager's active, or no longer one of its stores, once the sync
    /// lock is held.
    async fn sync_to_writer_unless_active(
        &self,
        identity_key: &str,
        writer: Arc<dyn MonitorStorage>,
    ) -> Result<Option<SyncResult>> {
        self.sync_to_writer_from_active(identity_key, writer, true)
            .await
    }

    async fn sync_to_writer_from_active(
        &self,
        identity_key: &str,
        writer: Arc<dyn MonitorStorage>,
        backups_only: bool,
    ) -> Result<Option<SyncResult>> {
        self.check_sync_identity(identity_key).await?;
        let writer_settings = writer.make_available().await?;
        let mut log = String::new();

        self.run_as_sync(|active| async move {
            if backups_only {
                let managed = self
                    .stores
                    .read()
                    .await
                    .iter()
                    .any(|s| Arc::ptr_eq(&s.storage, &writer));
                if !managed || Arc::ptr_eq(&active, &writer) {
                    return Ok(None);
                }
            }
            let reader_settings = active.get_settings();
            log.push_str(&format!(
                "syncToWriter from {} to {}\n",
                reader_settings.storage_name, writer_settings.storage_name
            ));

            let (inserts, updates) = drain_sync_chunks(
                active,
                writer.clone(),
                identity_key,
                UserActiveStorage::FromReader,
            )
            .await?;

            log.push_str(&format!(
                "syncToWriter complete: {} inserts, {} updates\n",
                inserts, updates
            ));

            Ok(Some(SyncResult {
                inserts,
                updates,
                log,
            }))
        })
        .await
    }

    /// Sets a new active storage from among the current stores.
    ///
    /// One transition at a time: a second call waits for the one running, on a
    /// lock no other operation of the manager takes. The call answers
    /// unchanged only when the store is the manager's active and every
    /// store's row of the wallet's user, read again from the store, names
    /// it; otherwise it backs up the active and writes the store's key to
    /// every store whose row does not name it. A transition that fails part
    /// way leaves the manager available on the active it had: the stores
    /// whose call succeeded already name the new active, in their rows and
    /// in the manager's cached rows, so while the rows disagree the manager
    /// does not answer its active as enabled, and a retry of the call, or a
    /// call back to the old active, runs the transition again. After an
    /// error the manager reads every store's row again (a store may have
    /// written its row and lost its answer); the row of a store that cannot
    /// be read is held as naming no active until a later call reads it.
    /// A call dropped once its transition has begun (a host's timeout), or
    /// one whose store panics, leaves the manager answering not enabled
    /// until a later call completes or reads every store's row again.
    pub async fn set_active(&self, storage_identity_key: &str) -> Result<String> {
        let _transition = self.set_active_lock.lock().await;
        // A call dropped or panicking once its transition had begun left the
        // mark set; what it left of the manager's fields is not trusted.
        let in_doubt_at_entry = self.rows_in_doubt();
        if !*self.is_available.read().await {
            self.make_available().await?;
        }

        // Find the store with matching identity key
        let stores = self.stores.read().await;
        let _new_active_idx = stores
            .iter()
            .position(|s| {
                s.settings
                    .as_ref()
                    .map(|settings| settings.storage_identity_key == storage_identity_key)
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "Storage {} not found in managed stores",
                    storage_identity_key
                ))
            })?;

        drop(stores);

        // The no-op is decided from the stores' rows, read again, and not
        // from the manager's own view of its active alone.
        self.refresh_user_rows().await?;
        let current_active = self.get_active_store().await?;
        if storage_identity_key == current_active
            && self.every_row_names(storage_identity_key).await
        {
            // Every row names the active: a partition over those rows keeps
            // it and answers it as enabled. With the mark set at entry the
            // partition runs whatever the manager's fields say, so they are
            // rebuilt from the rows just read before the mark is cleared.
            if in_doubt_at_entry || !self.auth_id.read().await.is_active.unwrap_or(false) {
                self.partition().await?;
            }
            // Every row was read back and names the active: nothing a
            // dropped transition left is in doubt.
            self.set_rows_in_doubt(false);
            return Ok(format!("setActive to {} unchanged\n", storage_identity_key));
        }

        // From here a store's row may be written (the backups' syncs carry
        // the active's row): until the transition completes, or the rows are
        // read again after its error, the manager answers not enabled. A
        // call dropped at any await below, or a store's panic, leaves the
        // mark set; the lock above is released with the dropped future.
        self.set_rows_in_doubt(true);
        let outcome = self.set_active_transition(storage_identity_key).await;
        if outcome.is_err() {
            // A store may have written its row and then failed to answer
            // (a storage client whose reply was lost): no cached row is
            // trusted after an error inside the transition. When every
            // store was read and every row names one store, the manager is
            // partitioned from those rows as at open: that store is its
            // active (the target, when the switch in fact completed and one
            // answer was lost; the old active, when nothing was written).
            // Otherwise it stays on the active it had, not enabled. The
            // condition that every store was read cannot by itself decide
            // the outcome: the row of a store that cannot be read is held as
            // naming no active, so `rows_name_one_store` is already false
            // then. It is kept as a safety net, should that hold ever change.
            if self.reread_user_rows_after_error().await && self.rows_name_one_store().await {
                // The partition reads no store; it fails only without an
                // active, which a manager with stores always has.
                let _ = self.partition().await;
            }
        }
        self.set_rows_in_doubt(false);
        outcome
    }

    /// Reads each store's row of the wallet's user again into the manager's
    /// cached rows, store by store; the row of a store that cannot be read
    /// is held as naming no active, so the manager does not answer its
    /// active as enabled until a later call reads it. Answers whether every
    /// store was read.
    async fn reread_user_rows_after_error(&self) -> bool {
        let identity_key = self.auth_id.read().await.identity_key.clone();
        let storages: Vec<_> = self
            .stores
            .read()
            .await
            .iter()
            .map(|s| s.storage.clone())
            .collect();
        let mut users = Vec::with_capacity(storages.len());
        for storage in &storages {
            users.push(storage.find_or_insert_user(&identity_key).await.ok());
        }
        let every_read = users.iter().all(Option::is_some);
        let mut stores = self.stores.write().await;
        for (store, user) in stores.iter_mut().zip(users) {
            match user {
                Some((user, _)) => store.user = Some(user),
                None => {
                    if let Some(user) = store.user.as_mut() {
                        user.active_storage = None;
                    }
                }
            }
        }
        every_read
    }

    /// Whether every store's held row names one store, and that store is one
    /// of the manager's.
    async fn rows_name_one_store(&self) -> bool {
        let stores = self.stores.read().await;
        let Some(key) = stores
            .iter()
            .next()
            .and_then(|s| s.user.as_ref())
            .and_then(|u| u.active_storage.clone())
        else {
            return false;
        };
        let managed = stores.iter().any(|s| {
            s.settings
                .as_ref()
                .is_some_and(|t| t.storage_identity_key == key)
        });
        drop(stores);
        managed && self.every_row_names(&key).await
    }

    async fn set_active_transition(&self, storage_identity_key: &str) -> Result<String> {
        let mut log = format!("setActive to {}\n", storage_identity_key);

        // Update backups first, then switch active
        log.push_str("BACKUP CURRENT ACTIVE STATE THEN SET NEW ACTIVE\n");
        log.push_str(&self.update_backups().await?);
        // The backups' rows as the backup left them.
        self.refresh_user_rows().await?;

        // Update the user record of every store whose row does not name the
        // new active, each by that store's own row of the wallet's user:
        // stores number their users independently. Each store and its
        // cached row are copied out under the stores read lock, which is
        // released before any store answers; the cached rows of the stores
        // whose call succeeded are written after, under the write lock, as
        // the reference's setActive sets each store's cached row (the
        // re-partition below reads them).
        let auth = self.get_auth(false).await?;
        let calls: Vec<(Arc<dyn MonitorStorage>, Option<TableUser>)> = self
            .stores
            .read()
            .await
            .iter()
            .map(|s| (s.storage.clone(), s.user.clone()))
            .collect();
        let mut done = Vec::with_capacity(calls.len());
        let mut outcome = Ok(());
        for (i, (storage, row)) in calls.into_iter().enumerate() {
            let Some(user) = row else {
                outcome = Err(Error::StorageNotAvailable);
                break;
            };
            if user.active_storage.as_deref() == Some(storage_identity_key) {
                continue;
            }
            let store_auth = AuthId::with_user_id(&auth.identity_key, user.user_id);
            if let Err(e) = storage.set_active(&store_auth, storage_identity_key).await {
                outcome = Err(e);
                break;
            }
            done.push(i);
        }

        let mut stores = self.stores.write().await;
        for i in done {
            if let Some(user) = stores.get_mut(i).and_then(|s| s.user.as_mut()) {
                user.active_storage = Some(storage_identity_key.to_string());
            }
        }
        drop(stores);
        outcome?;

        // Re-partition from the rows held: every store's row now names the
        // new active.
        self.partition().await?;

        Ok(log)
    }
}

/// Result from synchronization operations.
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Number of records inserted.
    pub inserts: u32,
    /// Number of records updated.
    pub updates: u32,
    /// Log of operations performed.
    pub log: String,
}

// Implement WalletStorageReader for WalletStorageManager (delegate to active)
#[async_trait]
impl WalletStorageReader for WalletStorageManager {
    fn is_available(&self) -> bool {
        // Sync check - will be properly checked in async methods
        true
    }

    fn get_settings(&self) -> TableSettings {
        // Settings are behind an async RwLock in the managed stores; a
        // std::sync::RwLock cache is populated when make_available()
        // completes. A copy taken under the lock; the defaults before.
        lock_read(&self.cached_settings)
            .ok()
            .and_then(|settings| settings.clone())
            .unwrap_or_default()
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        let guard = lock_read(&self.services_sync)?;
        guard.clone().ok_or_else(|| {
            Error::InvalidOperation(
                "Must call set_services first. Services are required for blockchain operations."
                    .to_string(),
            )
        })
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        self.run_as_reader(|active| async move { active.find_certificates(auth, args).await })
            .await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        self.run_as_reader(|active| async move { active.find_output_baskets(auth, args).await })
            .await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        self.run_as_reader(|active| async move { active.find_outputs(auth, args).await })
            .await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.run_as_reader(|active| async move { active.find_proven_tx_reqs(args).await })
            .await
    }

    async fn find_transactions(&self, args: FindTransactionsArgs) -> Result<Vec<TableTransaction>> {
        self.run_as_reader(|active| async move { active.find_transactions(args).await })
            .await
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        self.run_as_reader(|active| async move { active.list_actions(auth, args).await })
            .await
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        self.run_as_reader(|active| async move { active.list_certificates(auth, args).await })
            .await
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        self.run_as_reader(|active| async move { active.list_outputs(auth, args).await })
            .await
    }
}

#[async_trait]
impl WalletStorageWriter for WalletStorageManager {
    async fn make_available(&self) -> Result<TableSettings> {
        WalletStorageManager::make_available(self).await
    }

    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String> {
        self.run_as_writer(|active| async move {
            active.migrate(storage_name, storage_identity_key).await
        })
        .await
    }

    async fn destroy(&self) -> Result<()> {
        // Destroy all stores
        let storages: Vec<_> = self
            .stores
            .read()
            .await
            .iter()
            .map(|s| s.storage.clone())
            .collect();
        for storage in storages {
            storage.destroy().await?;
        }
        Ok(())
    }

    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        let auth = self.get_auth(false).await?;
        if identity_key != auth.identity_key {
            return Err(Error::AccessDenied("Identity key mismatch".to_string()));
        }

        self.run_as_writer(|active| async move { active.find_or_insert_user(identity_key).await })
            .await
    }

    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        self.run_as_writer(|active| async move { active.abort_action(auth, args).await })
            .await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        self.run_as_writer(|active| async move { active.create_action(auth, args).await })
            .await
    }

    async fn create_action_at_door(
        &self,
        auth: &AuthId,
        args: CreateActionArgs,
        door: crate::storage::CreateActionDoor<'_>,
    ) -> std::result::Result<crate::storage::CreateActionDone, crate::storage::DoorRefusal> {
        // The active storage's own door (one transaction on the SQL storage).
        self.run_as_writer(|active| async move {
            Ok(active.create_action_at_door(auth, args, door).await)
        })
        .await?
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        self.run_as_writer(|active| async move { active.process_action(auth, args).await })
            .await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        self.run_as_writer(|active| async move { active.internalize_action(auth, args).await })
            .await
    }

    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        let txid_owned = txid.to_string();
        self.run_as_writer(
            |active| async move { active.mark_internalized_tx_failed(&txid_owned).await },
        )
        .await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.run_as_writer(
            |active| async move { active.insert_certificate(auth, certificate).await },
        )
        .await
    }

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.run_as_writer(
            |active| async move { active.insert_certificate_field(auth, field).await },
        )
        .await
    }

    async fn insert_certificate_with_fields(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
        fields: Vec<TableCertificateField>,
    ) -> Result<i64> {
        self.run_as_writer(|active| async move {
            active
                .insert_certificate_with_fields(auth, certificate, fields)
                .await
        })
        .await
    }

    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        self.run_as_writer(|active| async move { active.relinquish_certificate(auth, args).await })
            .await
    }

    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        self.run_as_writer(|active| async move { active.relinquish_output(auth, args).await })
            .await
    }

    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        outcome: &crate::storage::broadcast::BroadcastOutcome,
    ) -> Result<()> {
        let txid_owned = txid.to_string();
        let outcome_owned = outcome.clone();
        self.run_as_writer(|active| async move {
            active
                .update_transaction_status_after_broadcast(&txid_owned, &outcome_owned)
                .await
        })
        .await
    }

    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: DateTime<Utc>,
    ) -> Result<ReviewStatusResult> {
        let auth = auth.clone();
        self.run_as_writer(|active| async move {
            WalletStorageWriter::review_status(active.as_ref(), &auth, aged_limit).await
        })
        .await
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        let auth = auth.clone();
        self.run_as_writer(|active| async move {
            WalletStorageWriter::purge_data(active.as_ref(), &auth, params).await
        })
        .await
    }
    async fn begin_transaction(&self) -> Result<TrxToken> {
        self.run_as_writer(|active| async move { active.begin_transaction().await })
            .await
    }

    async fn commit_transaction(&self, trx: TrxToken) -> Result<()> {
        self.run_as_writer(|active| async move { active.commit_transaction(trx).await })
            .await
    }

    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()> {
        self.run_as_writer(|active| async move { active.rollback_transaction(trx).await })
            .await
    }
}

#[async_trait]
impl WalletStorageSync for WalletStorageManager {
    async fn find_or_insert_sync_state(
        &self,
        auth: &AuthId,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)> {
        self.check_sync_identity(&auth.identity_key).await?;
        self.run_as_sync(|active| async move {
            active
                .find_or_insert_sync_state(auth, storage_identity_key, storage_name)
                .await
        })
        .await
    }

    async fn set_active(
        &self,
        auth: &AuthId,
        new_active_storage_identity_key: &str,
    ) -> Result<i64> {
        self.check_sync_identity(&auth.identity_key).await?;
        self.run_as_sync(|active| async move {
            active
                .set_active(auth, new_active_storage_identity_key)
                .await
        })
        .await
    }

    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        self.check_sync_identity(&args.identity_key).await?;
        self.run_as_sync(|active| async move { active.get_sync_chunk(args).await })
            .await
    }

    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        self.check_sync_identity(&args.identity_key).await?;
        self.run_as_sync(|active| async move { active.process_sync_chunk(args, chunk).await })
            .await
    }
}

#[async_trait]
impl WalletStorageProvider for WalletStorageManager {
    fn is_storage_provider(&self) -> bool {
        false // Manager is not a direct storage provider
    }

    /// The active store's answer: the manager calls its active store's
    /// `internalize_action`, so the active store's door is the one that
    /// checks. With no active store chosen yet the answer is false (the
    /// wallet checks). The locks are taken with `.await`, never blocking.
    async fn checks_received_beef_in_process(&self) -> bool {
        let active = {
            let stores = self.stores.read().await;
            let active_idx = self.active_index.read().await;
            match *active_idx {
                Some(idx) => stores.get(idx).map(|store| store.storage.clone()),
                None => None,
            }
        };
        match active {
            Some(store) => store.checks_received_beef_in_process().await,
            None => false,
        }
    }

    fn storage_identity_key(&self) -> String {
        // This is sync - use get_active_store() for async access
        unimplemented!("Use async get_active_store() method instead")
    }

    fn storage_name(&self) -> String {
        unimplemented!("Use async get_settings() method instead")
    }

    fn broadcast_memory(
        &self,
    ) -> Option<Arc<dyn crate::services::broadcast_memory::BroadcastMemory>> {
        // Sync trait method: never block on the async locks. When they are
        // contended there is simply no memory to hand out this time.
        let stores = self.stores.try_read().ok()?;
        let active = self.active_index.try_read().ok()?;
        let idx = (*active).or(if stores.is_empty() { None } else { Some(0) })?;
        stores
            .get(idx)
            .and_then(|store| store.storage.broadcast_memory())
    }

    fn set_services(&self, services: Arc<dyn WalletServices>) {
        // Use blocking write since this trait method is sync
        // Note: This may block if the lock is held by an async task
        let storages: Vec<_> = self
            .stores
            .blocking_read()
            .iter()
            .map(|s| s.storage.clone())
            .collect();
        for storage in storages {
            storage.set_services(services.clone());
        }
        // Update sync-accessible cache for WalletStorageReader::get_services()
        if let Ok(mut guard) = lock_write(&self.services_sync) {
            *guard = Some(services.clone());
        }
        *self.services.blocking_write() = Some(services);
    }
}

// =============================================================================
// MonitorStorage Implementation
// =============================================================================

#[async_trait]
impl MonitorStorage for WalletStorageManager {
    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        self.run_as_writer(|active| async move { active.synchronize_transaction_statuses().await })
            .await
    }

    // The reorg-resilience surface (0.3.66) forwards to the active storage,
    // so a monitor over the manager drives the same persisted proof gate,
    // tracker state and anchors the active storage's own funnel reads.
    async fn max_acceptable_proof_height(&self) -> Result<u32> {
        self.run_as_writer(|active| async move { active.max_acceptable_proof_height().await })
            .await
    }

    async fn set_max_acceptable_proof_height(&self, height: u32) -> Result<()> {
        self.run_as_writer(
            |active| async move { active.set_max_acceptable_proof_height(height).await },
        )
        .await
    }

    async fn load_header_tracker_state(
        &self,
    ) -> Result<Option<crate::storage::HeaderTrackerState>> {
        self.run_as_writer(|active| async move { active.load_header_tracker_state().await })
            .await
    }

    async fn save_header_tracker_state(
        &self,
        state: &crate::storage::HeaderTrackerState,
    ) -> Result<()> {
        let state = state.clone();
        self.run_as_writer(|active| async move { active.save_header_tracker_state(&state).await })
            .await
    }

    async fn find_proven_txs_by_block_hash(
        &self,
        block_hash: &str,
    ) -> Result<Vec<crate::storage::ProvenTxAnchor>> {
        let block_hash = block_hash.to_string();
        self.run_as_writer(|active| async move {
            active.find_proven_txs_by_block_hash(&block_hash).await
        })
        .await
    }

    async fn find_proven_txs_in_heights(
        &self,
        min_height: u32,
        max_height: u32,
    ) -> Result<Vec<crate::storage::ProvenTxAnchor>> {
        self.run_as_writer(|active| async move {
            active
                .find_proven_txs_in_heights(min_height, max_height)
                .await
        })
        .await
    }

    async fn demote_stale_proof(&self, txid: &str) -> Result<bool> {
        let txid = txid.to_string();
        self.run_as_writer(|active| async move { active.demote_stale_proof(&txid).await })
            .await
    }

    async fn announce_lost_proof(&self, txid: &str) -> Result<crate::storage::BroadcastOutcome> {
        let txid = txid.to_string();
        self.run_as_writer(|active| async move { active.announce_lost_proof(&txid).await })
            .await
    }

    async fn note_lost_proof_refusal(&self, txid: &str, verdict: &str) -> Result<()> {
        let txid = txid.to_string();
        let verdict = verdict.to_string();
        self.run_as_writer(
            |active| async move { active.note_lost_proof_refusal(&txid, &verdict).await },
        )
        .await
    }

    async fn set_proven_tx_block_hash_if_empty(
        &self,
        txid: &str,
        block_hash: &str,
    ) -> Result<bool> {
        let txid = txid.to_string();
        let block_hash = block_hash.to_string();
        self.run_as_writer(|active| async move {
            active
                .set_proven_tx_block_hash_if_empty(&txid, &block_hash)
                .await
        })
        .await
    }

    async fn send_waiting_transactions(
        &self,
        min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        self.run_as_writer(|active| async move {
            active.send_waiting_transactions(min_transaction_age).await
        })
        .await
    }

    async fn abort_abandoned(&self, timeout: Duration) -> Result<()> {
        self.run_as_writer(|active| async move { active.abort_abandoned(timeout).await })
            .await
    }

    async fn un_fail(&self) -> Result<()> {
        self.run_as_writer(|active| async move { active.un_fail().await })
            .await
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        self.run_as_writer(
            |active| async move { MonitorStorage::review_status(active.as_ref()).await },
        )
        .await
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        self.run_as_writer(|active| async move {
            MonitorStorage::purge_data(active.as_ref(), params).await
        })
        .await
    }

    async fn compact_input_beefs(&self) -> Result<u32> {
        self.run_as_writer(|active| async move {
            MonitorStorage::compact_input_beefs(active.as_ref()).await
        })
        .await
    }

    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool> {
        let tn = task_name.to_string();
        let iid = instance_id.to_string();
        self.run_as_writer(
            |active| async move { active.try_acquire_task_lock(&tn, &iid, ttl).await },
        )
        .await
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        let tn = task_name.to_string();
        let iid = instance_id.to_string();
        self.run_as_writer(|active| async move { active.release_task_lock(&tn, &iid).await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_managed_storage_new() {
        // Basic construction test - full tests require mock storage
    }

    #[test]
    fn test_sync_result() {
        let result = SyncResult {
            inserts: 10,
            updates: 5,
            log: "test".to_string(),
        };
        assert_eq!(result.inserts, 10);
        assert_eq!(result.updates, 5);
    }

    #[test]
    fn test_get_settings_returns_default_before_make_available() {
        // Before make_available() is called, get_settings() should return a
        // default TableSettings (via OnceLock fallback) instead of panicking.
        let manager = WalletStorageManager::new("02dummy_identity_key".to_string(), None, None);
        let settings = WalletStorageReader::get_settings(&manager);
        // Should return the OnceLock default rather than panicking
        assert_eq!(settings.settings_id, 1);
        assert_eq!(settings.chain, "mainnet");
        assert!(settings.storage_identity_key.is_empty());
    }

    #[test]
    fn test_get_settings_returns_cached_settings() {
        // When cached_settings is manually populated, get_settings() should
        // return the cached value.
        let manager = WalletStorageManager::new("02dummy_identity_key".to_string(), None, None);
        // Manually populate the cache (simulating what make_available does)
        {
            let mut guard =
                lock_write(&manager.cached_settings).expect("lock_write should succeed");
            *guard = Some(TableSettings {
                settings_id: 42,
                storage_identity_key: "cached_key".to_string(),
                storage_name: "cached_name".to_string(),
                chain: "testnet".to_string(),
                ..TableSettings::default()
            });
        }
        let settings = WalletStorageReader::get_settings(&manager);
        assert_eq!(settings.settings_id, 42);
        assert_eq!(settings.storage_identity_key, "cached_key");
        assert_eq!(settings.storage_name, "cached_name");
        assert_eq!(settings.chain, "testnet");
    }

    #[test]
    fn test_get_services_returns_error_before_set() {
        // Before set_services() is called, get_services() should return an
        // error instead of panicking.
        let manager = WalletStorageManager::new("02dummy_identity_key".to_string(), None, None);
        let result = WalletStorageReader::get_services(&manager);
        assert!(result.is_err());
        match result {
            Err(ref e) => {
                let err_msg = format!("{}", e);
                assert!(
                    err_msg.contains("set_services"),
                    "Error message should mention set_services, got: {}",
                    err_msg
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "storage_manager_partition_tests.rs"]
mod partition_tests;
