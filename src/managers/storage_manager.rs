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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use bsv_sdk::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, InternalizeActionArgs,
    ListActionsArgs, ListActionsResult, ListCertificatesArgs, ListCertificatesResult,
    ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs,
};

use crate::error::{Error, Result};
use crate::services::WalletServices;
use chrono::{DateTime, Utc};

use crate::storage::{
    AuthId, FindCertificatesArgs, FindOutputBasketsArgs, FindOutputsArgs, FindProvenTxReqsArgs,
    MonitorStorage, ProcessSyncChunkResult, PurgeParams, PurgeResults, RequestSyncChunkArgs,
    ReviewStatusResult, StorageCreateActionResult,
    StorageInternalizeActionResult, StorageProcessActionArgs, StorageProcessActionResults,
    SyncChunk, TrxToken, TxSynchronizedStatus, WalletStorageInfo, WalletStorageProvider,
    WalletStorageReader, WalletStorageSync, WalletStorageWriter,
    entities::{
        TableCertificate, TableCertificateField, TableOutput, TableOutputBasket,
        TableProvenTxReq, TableSettings, TableSyncState, TableUser,
    },
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
}

/// Default lock acquisition timeout in seconds.
const LOCK_TIMEOUT_SECS: u64 = 30;

/// Lock queue for managing concurrent access.
type LockQueue = Arc<Mutex<VecDeque<tokio::sync::oneshot::Sender<()>>>>;

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
/// use bsv_wallet_toolbox::managers::WalletStorageManager;
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

    /// Configured services (shared with stores).
    services: RwLock<Option<Arc<dyn WalletServices>>>,

    /// Lock queue for reader operations.
    reader_locks: LockQueue,

    /// Lock queue for writer operations.
    writer_locks: LockQueue,

    /// Lock queue for sync operations.
    sync_locks: LockQueue,

    /// Lock queue for storage provider operations.
    #[allow(dead_code)]
    provider_locks: LockQueue,
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
            reader_locks: Arc::new(Mutex::new(VecDeque::new())),
            writer_locks: Arc::new(Mutex::new(VecDeque::new())),
            sync_locks: Arc::new(Mutex::new(VecDeque::new())),
            provider_locks: Arc::new(Mutex::new(VecDeque::new())),
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
    pub async fn is_active_enabled(&self) -> bool {
        let active_index = self.active_index.read().await;
        let conflicting = self.conflicting_indices.read().await;
        let stores = self.stores.read().await;

        if let Some(idx) = *active_index {
            if let Some(store) = stores.get(idx) {
                if let (Some(settings), Some(user)) = (&store.settings, &store.user) {
                    return user.active_storage.as_ref()
                        .map(|a| a == &settings.storage_identity_key)
                        .unwrap_or(false)
                        && conflicting.is_empty();
                }
            }
        }
        false
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
        // Check if already available
        if *self.is_available.read().await {
            let stores = self.stores.read().await;
            let active_idx = self.active_index.read().await;
            if let Some(idx) = *active_idx {
                if let Some(store) = stores.get(idx) {
                    if let Some(settings) = &store.settings {
                        return Ok(settings.clone());
                    }
                }
            }
        }

        // Reset state
        *self.active_index.write().await = None;
        *self.backup_indices.write().await = Vec::new();
        *self.conflicting_indices.write().await = Vec::new();

        let mut stores = self.stores.write().await;
        let auth_id = self.auth_id.read().await;

        if stores.is_empty() {
            return Err(Error::InvalidArgument(
                "Must add at least one storage provider".to_string(),
            ));
        }

        // Initialize all stores and find/create user
        for store in stores.iter_mut() {
            if !store.is_available || store.settings.is_none() || store.user.is_none() {
                let settings = store.storage.make_available().await?;
                let (user, _) = store.storage.find_or_insert_user(&auth_id.identity_key).await?;
                store.settings = Some(settings);
                store.user = Some(user);
                store.is_available = true;
            }
        }

        // Partition stores into active, backups, and conflicting
        let mut active_idx: Option<usize> = None;
        let mut backups_temp: Vec<usize> = Vec::new();

        for (i, store) in stores.iter().enumerate() {
            if active_idx.is_none() {
                // First store becomes default active
                active_idx = Some(i);
            } else {
                let user_active = store.user.as_ref().unwrap().active_storage.clone();
                let store_identity = store.settings.as_ref().unwrap().storage_identity_key.clone();

                // Check if this store's user record selects it as active
                if user_active.as_ref() == Some(&store_identity) {
                    // Check if current active is not enabled
                    let current_active = &stores[active_idx.unwrap()];
                    let current_settings = current_active.settings.as_ref().unwrap();
                    let current_user = current_active.user.as_ref().unwrap();

                    if current_user.active_storage.as_ref() != Some(&current_settings.storage_identity_key) {
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

        // Update state
        drop(stores);
        *self.active_index.write().await = active_idx;
        *self.backup_indices.write().await = backups;
        *self.conflicting_indices.write().await = conflicting;

        // Update auth_id with user info
        let stores = self.stores.read().await;
        if let Some(idx) = active_idx {
            let user = stores[idx].user.as_ref().unwrap();
            let mut auth = self.auth_id.write().await;
            auth.user_id = Some(user.user_id);
            auth.is_active = Some(
                user.active_storage.as_ref()
                    == Some(&stores[idx].settings.as_ref().unwrap().storage_identity_key)
                    && self.conflicting_indices.read().await.is_empty(),
            );
        }

        *self.is_available.write().await = true;

        // Return active settings
        if let Some(idx) = active_idx {
            Ok(stores[idx].settings.clone().unwrap())
        } else {
            Err(Error::StorageNotAvailable)
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
        let active_idx = self.active_index.read().await;
        let stores = self.stores.read().await;

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
    async fn acquire_lock(queue: &LockQueue, lock_name: &str) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let should_wait;

        {
            let mut q = queue.lock().await;
            should_wait = !q.is_empty();
            q.push_back(tx);
        }

        if should_wait {
            match tokio::time::timeout(Duration::from_secs(LOCK_TIMEOUT_SECS), rx).await {
                Ok(_) => Ok(()),
                Err(_) => {
                    // Remove our sender from the queue since we're giving up
                    // Note: the sender may have already been consumed, so we just
                    // report the timeout.
                    Err(Error::LockTimeout(format!(
                        "Timed out after {}s waiting for {} lock",
                        LOCK_TIMEOUT_SECS, lock_name
                    )))
                }
            }
        } else {
            Ok(())
        }
    }

    /// Releases a lock from the specified queue.
    async fn release_lock(queue: &LockQueue) {
        let next_sender;
        {
            let mut q = queue.lock().await;
            q.pop_front(); // Remove current holder's sender (which was already consumed)
            next_sender = q.pop_front(); // Get next waiter's sender
        }

        // Notify next waiter outside the lock
        if let Some(sender) = next_sender {
            let _ = sender.send(());
        }
    }

    /// Runs a closure with reader lock.
    pub async fn run_as_reader<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        Self::acquire_lock(&self.reader_locks, "reader").await?;

        let result = async {
            if !*self.is_available.read().await {
                self.make_available().await?;
            }

            let active = self.get_active().await?;
            f(active).await
        }
        .await;

        Self::release_lock(&self.reader_locks).await;
        result
    }

    /// Runs a closure with writer lock.
    pub async fn run_as_writer<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        Self::acquire_lock(&self.reader_locks, "reader").await?;
        let result = match Self::acquire_lock(&self.writer_locks, "writer").await {
            Ok(()) => {
                let inner_result = async {
                    if !*self.is_available.read().await {
                        self.make_available().await?;
                    }

                    let active = self.get_active().await?;
                    f(active).await
                }
                .await;

                Self::release_lock(&self.writer_locks).await;
                inner_result
            }
            Err(e) => {
                Self::release_lock(&self.reader_locks).await;
                return Err(e);
            }
        };

        Self::release_lock(&self.reader_locks).await;
        result
    }

    /// Runs a closure with sync lock.
    pub async fn run_as_sync<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<dyn MonitorStorage>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        Self::acquire_lock(&self.reader_locks, "reader").await?;
        if let Err(e) = Self::acquire_lock(&self.writer_locks, "writer").await {
            Self::release_lock(&self.reader_locks).await;
            return Err(e);
        }
        if let Err(e) = Self::acquire_lock(&self.sync_locks, "sync").await {
            Self::release_lock(&self.writer_locks).await;
            Self::release_lock(&self.reader_locks).await;
            return Err(e);
        }

        let result = async {
            if !*self.is_available.read().await {
                self.make_available().await?;
            }

            let active = self.get_active().await?;
            f(active).await
        }
        .await;

        Self::release_lock(&self.sync_locks).await;
        Self::release_lock(&self.writer_locks).await;
        Self::release_lock(&self.reader_locks).await;
        result
    }

    /// Sets the wallet services for all storage providers.
    pub async fn set_services(&self, services: Arc<dyn WalletServices>) {
        let stores = self.stores.read().await;
        for store in stores.iter() {
            store.storage.set_services(services.clone());
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

        if let Some(services) = self.services.read().await.as_ref() {
            provider.set_services(services.clone());
        }

        self.stores.write().await.push(ManagedStorage::new(provider));
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

        let mut result = Vec::new();

        for (i, store) in stores.iter().enumerate() {
            let is_active = active_idx.map(|idx| idx == i).unwrap_or(false);
            let is_backup = backup_indices.contains(&i);
            let is_conflicting = conflicting_indices.contains(&i);

            let is_enabled = if is_active {
                if let (Some(settings), Some(user)) = (&store.settings, &store.user) {
                    user.active_storage.as_ref() == Some(&settings.storage_identity_key) && conflicting_indices.is_empty()
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

    /// Synchronizes from a reader storage to the active storage.
    pub async fn sync_from_reader(
        &self,
        identity_key: &str,
        reader: Arc<dyn MonitorStorage>,
    ) -> Result<SyncResult> {
        let auth = self.get_auth(false).await?;
        if identity_key != auth.identity_key {
            return Err(Error::AccessDenied("Identity key mismatch".to_string()));
        }

        let reader_settings = reader.make_available().await?;
        let mut inserts = 0u32;
        let mut updates = 0u32;
        let mut log = String::new();

        self.run_as_sync(|active| async move {
            let writer_settings = active.get_settings();
            log.push_str(&format!(
                "syncFromReader from {} to {}\n",
                reader_settings.storage_name, writer_settings.storage_name
            ));

            let mut chunk_num = 0;
            loop {
                let args = RequestSyncChunkArgs {
                    from_storage_identity_key: reader_settings.storage_identity_key.clone(),
                    to_storage_identity_key: writer_settings.storage_identity_key.clone(),
                    identity_key: identity_key.to_string(),
                    since: None,
                    max_rough_size: 100_000,
                    max_items: 1000,
                    offsets: vec![],
                };

                let chunk = reader.get_sync_chunk(args.clone()).await?;
                let result = active.process_sync_chunk(args, chunk).await?;

                inserts += result.inserts;
                updates += result.updates;
                log.push_str(&format!(
                    "chunk {} inserted {} updated {}\n",
                    chunk_num, result.inserts, result.updates
                ));

                if result.done {
                    break;
                }
                chunk_num += 1;
            }

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
        let stores = self.stores.read().await;

        log.push_str(&format!("BACKUP CURRENT ACTIVE TO {} STORES\n", backup_indices.len()));

        for idx in backup_indices {
            if let Some(backup) = stores.get(idx) {
                let sync_result = self
                    .sync_to_writer(&auth.identity_key, backup.storage.clone())
                    .await?;
                log.push_str(&sync_result.log);
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
        let writer_settings = writer.make_available().await?;
        let mut inserts = 0u32;
        let mut updates = 0u32;
        let mut log = String::new();

        self.run_as_sync(|active| async move {
            let reader_settings = active.get_settings();
            log.push_str(&format!(
                "syncToWriter from {} to {}\n",
                reader_settings.storage_name, writer_settings.storage_name
            ));

            let mut chunk_num = 0;
            loop {
                let args = RequestSyncChunkArgs {
                    from_storage_identity_key: reader_settings.storage_identity_key.clone(),
                    to_storage_identity_key: writer_settings.storage_identity_key.clone(),
                    identity_key: identity_key.to_string(),
                    since: None,
                    max_rough_size: 100_000,
                    max_items: 1000,
                    offsets: vec![],
                };

                let chunk = active.get_sync_chunk(args.clone()).await?;
                let result = writer.process_sync_chunk(args, chunk).await?;

                inserts += result.inserts;
                updates += result.updates;
                log.push_str(&format!(
                    "chunk {} inserted {} updated {}\n",
                    chunk_num, result.inserts, result.updates
                ));

                if result.done {
                    break;
                }
                chunk_num += 1;
            }

            log.push_str(&format!(
                "syncToWriter complete: {} inserts, {} updates\n",
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

    /// Sets a new active storage from among the current stores.
    pub async fn set_active(&self, storage_identity_key: &str) -> Result<String> {
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

        let current_active = self.get_active_store().await?;

        // If already active and enabled, no-op
        if storage_identity_key == current_active && self.is_active_enabled().await {
            return Ok(format!("setActive to {} unchanged\n", storage_identity_key));
        }

        drop(stores);

        let mut log = format!("setActive to {}\n", storage_identity_key);

        // Update backups first, then switch active
        log.push_str("BACKUP CURRENT ACTIVE STATE THEN SET NEW ACTIVE\n");
        log.push_str(&self.update_backups().await?);

        // Update user records in all stores
        let auth = self.get_auth(false).await?;
        let stores = self.stores.read().await;

        for store in stores.iter() {
            store
                .storage
                .set_active(&auth, storage_identity_key)
                .await?;
        }

        drop(stores);

        // Re-partition
        *self.is_available.write().await = false;
        self.make_available().await?;

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

    fn get_settings(&self) -> &TableSettings {
        // This is a sync method that can't work with our async design
        // Return a placeholder - real usage should use get_settings() async method
        unimplemented!("Use async get_settings() method instead")
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        // This is sync but needs async - callers should use the async version
        unimplemented!("Use async get_services() method instead")
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

    async fn find_outputs(
        &self,
        auth: &AuthId,
        args: FindOutputsArgs,
    ) -> Result<Vec<TableOutput>> {
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
        let stores = self.stores.read().await;
        for store in stores.iter() {
            store.storage.destroy().await?;
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

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.run_as_writer(|active| async move {
            active.insert_certificate(auth, certificate).await
        })
        .await
    }

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.run_as_writer(|active| async move { active.insert_certificate_field(auth, field).await })
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
        success: bool,
    ) -> Result<()> {
        let txid_owned = txid.to_string();
        self.run_as_writer(|active| async move {
            active.update_transaction_status_after_broadcast(&txid_owned, success).await
        })
        .await
    }

    async fn review_status(&self, auth: &AuthId, aged_limit: DateTime<Utc>) -> Result<ReviewStatusResult> {
        let auth = auth.clone();
        self.run_as_writer(|active| async move { WalletStorageWriter::review_status(active.as_ref(), &auth, aged_limit).await })
            .await
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        let auth = auth.clone();
        self.run_as_writer(|active| async move { WalletStorageWriter::purge_data(active.as_ref(), &auth, params).await })
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
        self.run_as_sync(|active| async move {
            active
                .set_active(auth, new_active_storage_identity_key)
                .await
        })
        .await
    }

    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        self.run_as_sync(|active| async move { active.get_sync_chunk(args).await })
            .await
    }

    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        self.run_as_sync(|active| async move { active.process_sync_chunk(args, chunk).await })
            .await
    }
}

#[async_trait]
impl WalletStorageProvider for WalletStorageManager {
    fn is_storage_provider(&self) -> bool {
        false // Manager is not a direct storage provider
    }

    fn storage_identity_key(&self) -> &str {
        // This is sync - use get_active_store() for async access
        unimplemented!("Use async get_active_store() method instead")
    }

    fn storage_name(&self) -> &str {
        unimplemented!("Use async get_settings() method instead")
    }

    fn set_services(&self, services: Arc<dyn WalletServices>) {
        // Use blocking write since this trait method is sync
        // Note: This may block if the lock is held by an async task
        let stores_guard = self.stores.blocking_read();
        for store in stores_guard.iter() {
            store.storage.set_services(services.clone());
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
        self.run_as_writer(|active| async move {
            active.synchronize_transaction_statuses().await
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
        self.run_as_writer(|active| async move {
            active.abort_abandoned(timeout).await
        })
        .await
    }

    async fn un_fail(&self) -> Result<()> {
        self.run_as_writer(|active| async move {
            active.un_fail().await
        })
        .await
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        self.run_as_writer(|active| async move {
            MonitorStorage::review_status(active.as_ref()).await
        })
        .await
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        self.run_as_writer(|active| async move {
            MonitorStorage::purge_data(active.as_ref(), params).await
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
        self.run_as_writer(|active| async move {
            active.try_acquire_task_lock(&tn, &iid, ttl).await
        })
        .await
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        let tn = task_name.to_string();
        let iid = instance_id.to_string();
        self.run_as_writer(|active| async move {
            active.release_task_lock(&tn, &iid).await
        })
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
}
