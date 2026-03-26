//! Storage provider traits.
//!
//! These traits mirror the TypeScript `WalletStorageProvider` interface hierarchy
//! from `@bsv/wallet-toolbox`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, InternalizeActionArgs, InternalizeActionResult,
    ListActionsArgs, ListActionsResult, ListCertificatesArgs, ListCertificatesResult,
    ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs,
};

use crate::error::Result;
use crate::services::WalletServices;
use crate::storage::entities::*;

// =============================================================================
// Transaction Token
// =============================================================================

/// Global counter for TrxToken ID generation.
static NEXT_TRX_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque transaction scope token.
///
/// When provided to storage operations, they execute within the same database
/// transaction. Mirrors the TypeScript `TrxToken` interface from
/// `WalletStorage.interfaces.ts`.
///
/// Callers obtain a token via [`WalletStorageWriter::begin_transaction`],
/// and must eventually call [`WalletStorageWriter::commit_transaction`] or
/// [`WalletStorageWriter::rollback_transaction`] to close the scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrxToken {
    id: u64,
}

impl TrxToken {
    /// Create a new unique TrxToken.
    pub(crate) fn new() -> Self {
        Self {
            id: NEXT_TRX_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Get the unique ID of this token.
    pub fn id(&self) -> u64 {
        self.id
    }
}

// =============================================================================
// Authentication
// =============================================================================

/// Authentication identifier for storage operations.
///
/// Every storage operation is performed in the context of an authenticated user.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthId {
    /// The identity public key (hex) of the authenticated user.
    pub identity_key: String,
    /// The user's ID in the storage system (populated after lookup).
    pub user_id: Option<i64>,
    /// Whether this user is the active user for the storage.
    pub is_active: Option<bool>,
}

impl AuthId {
    /// Create a new AuthId with just the identity key.
    pub fn new(identity_key: impl Into<String>) -> Self {
        Self {
            identity_key: identity_key.into(),
            user_id: None,
            is_active: None,
        }
    }

    /// Create an AuthId with user_id already known.
    pub fn with_user_id(identity_key: impl Into<String>, user_id: i64) -> Self {
        Self {
            identity_key: identity_key.into(),
            user_id: Some(user_id),
            is_active: None,
        }
    }
}

// =============================================================================
// Query Arguments
// =============================================================================

/// Pagination parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paged {
    /// Number of items to skip.
    pub offset: Option<u32>,
    /// Maximum number of items to return.
    pub limit: Option<u32>,
}

/// Base arguments for paginated queries with optional since filter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindSincePagedArgs {
    /// Only return items updated after this time.
    pub since: Option<DateTime<Utc>>,
    /// Pagination parameters.
    pub paged: Option<Paged>,
    /// Order results descending by updated_at.
    pub order_descending: Option<bool>,
}

/// Arguments for finding certificates.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindCertificatesArgs {
    /// Base query parameters.
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    /// Filter by user ID.
    pub user_id: Option<i64>,
    /// Filter by certifier identity keys.
    pub certifiers: Option<Vec<String>>,
    /// Filter by certificate types.
    pub types: Option<Vec<String>>,
    /// Include certificate field values.
    pub include_fields: Option<bool>,
}

/// Arguments for finding output baskets.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindOutputBasketsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub user_id: Option<i64>,
    pub name: Option<String>,
}

/// Arguments for finding outputs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindOutputsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub user_id: Option<i64>,
    pub basket_id: Option<i64>,
    pub txid: Option<String>,
    pub vout: Option<u32>,
    /// Exclude locking script from results (for efficiency).
    pub no_script: Option<bool>,
    /// Filter by transaction status.
    pub tx_status: Option<Vec<TransactionStatus>>,
}

/// Arguments for finding proven transaction requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindProvenTxReqsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub status: Option<Vec<ProvenTxReqStatus>>,
    pub txids: Option<Vec<String>>,
}

/// Arguments for finding transactions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindTransactionsArgs {
    #[serde(flatten)]
    pub base: FindSincePagedArgs,
    pub status: Option<Vec<TransactionStatus>>,
    pub no_raw_tx: Option<bool>,
}

// =============================================================================
// Storage Results
// =============================================================================

/// Result from createAction storage operation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StorageCreateActionResult {
    /// BEEF data for inputs (if needed).
    pub input_beef: Option<Vec<u8>>,
    /// Input details for the transaction.
    #[serde(default)]
    pub inputs: Vec<StorageCreateTransactionInput>,
    /// Output details for the transaction.
    #[serde(default)]
    pub outputs: Vec<StorageCreateTransactionOutput>,
    /// Change output vouts for noSend transactions.
    pub no_send_change_output_vouts: Option<Vec<u32>>,
    /// Derivation prefix for key derivation.
    #[serde(default)]
    pub derivation_prefix: String,
    /// Transaction version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Transaction locktime.
    #[serde(default)]
    pub lock_time: u32,
    /// Unique reference for this action.
    #[serde(default)]
    pub reference: String,
}

fn default_version() -> u32 {
    1
}

/// Input details from storage for transaction creation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StorageCreateTransactionInput {
    pub vin: u32,
    pub source_txid: String,
    pub source_vout: u32,
    pub source_satoshis: u64,
    pub source_locking_script: String,
    pub source_transaction: Option<Vec<u8>>,
    pub unlocking_script_length: u32,
    #[serde(default)]
    pub provided_by: StorageProvidedBy,
    #[serde(default)]
    pub input_type: String,
    pub spending_description: Option<String>,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
}

/// Output details from storage for transaction creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageCreateTransactionOutput {
    pub vout: u32,
    pub satoshis: u64,
    pub locking_script: String,
    pub provided_by: StorageProvidedBy,
    pub purpose: Option<String>,
    pub derivation_suffix: Option<String>,
    pub basket: Option<String>,
    pub tags: Vec<String>,
    pub output_description: Option<String>,
    pub custom_instructions: Option<String>,
}

/// Indicates who provided the input/output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StorageProvidedBy {
    #[default]
    You,
    Storage,
    YouAndStorage,
}

/// Arguments for processAction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProcessActionArgs {
    pub is_new_tx: bool,
    pub is_send_with: bool,
    pub is_no_send: bool,
    pub is_delayed: bool,
    pub reference: Option<String>,
    pub txid: Option<String>,
    /// Raw transaction bytes - serializes as JSON array of numbers (not hex string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_tx: Option<Vec<u8>>,
    pub send_with: Vec<String>,
}

/// Results from processAction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProcessActionResults {
    pub send_with_results: Option<Vec<SendWithResult>>,
    pub not_delayed_results: Option<Vec<ReviewActionResult>>,
    pub log: Option<String>,
}

/// Result of sending a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendWithResult {
    pub txid: String,
    pub status: String,
}

/// Result of reviewing an action (non-delayed broadcast).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewActionResult {
    pub txid: String,
    pub status: ReviewActionResultStatus,
    pub competing_txs: Option<Vec<String>>,
    pub competing_beef: Option<Vec<u8>>,
}

/// Status of a reviewed action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReviewActionResultStatus {
    Success,
    DoubleSpend,
    ServiceError,
    InvalidTx,
}

/// Result from internalizeAction with storage-specific details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageInternalizeActionResult {
    /// Base result from SDK.
    #[serde(flatten)]
    pub base: InternalizeActionResult,
    /// True if internalizing outputs on an existing transaction.
    pub is_merge: bool,
    /// TXID of the transaction being internalized.
    pub txid: String,
    /// Net change in balance for user.
    pub satoshis: i64,
    /// SendWith results if applicable.
    pub send_with_results: Option<Vec<SendWithResult>>,
    /// Review results if non-delayed broadcast.
    pub not_delayed_results: Option<Vec<ReviewActionResult>>,
}

// =============================================================================
// Sync Types
// =============================================================================

/// Arguments for requesting a sync chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestSyncChunkArgs {
    /// Storage identity key of the source.
    pub from_storage_identity_key: String,
    /// Storage identity key of the destination.
    pub to_storage_identity_key: String,
    /// Identity key of the user being synced.
    pub identity_key: String,
    /// Only include items updated after this time.
    pub since: Option<DateTime<Utc>>,
    /// Rough size limit for the response.
    pub max_rough_size: u32,
    /// Maximum number of items to return.
    pub max_items: u32,
    /// Offsets for each entity type.
    pub offsets: Vec<SyncOffset>,
}

/// Offset for a specific entity type during sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncOffset {
    pub name: String,
    pub offset: u32,
}

/// A chunk of data for synchronization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncChunk {
    pub from_storage_identity_key: String,
    pub to_storage_identity_key: String,
    pub user_identity_key: String,

    pub user: Option<TableUser>,
    pub proven_txs: Option<Vec<TableProvenTx>>,
    pub proven_tx_reqs: Option<Vec<TableProvenTxReq>>,
    pub output_baskets: Option<Vec<TableOutputBasket>>,
    pub tx_labels: Option<Vec<TableTxLabel>>,
    pub output_tags: Option<Vec<TableOutputTag>>,
    pub transactions: Option<Vec<TableTransaction>>,
    pub tx_label_maps: Option<Vec<TableTxLabelMap>>,
    pub commissions: Option<Vec<TableCommission>>,
    pub outputs: Option<Vec<TableOutput>>,
    pub output_tag_maps: Option<Vec<TableOutputTagMap>>,
    pub certificates: Option<Vec<TableCertificate>>,
    pub certificate_fields: Option<Vec<TableCertificateField>>,
}

/// Result of processing a sync chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessSyncChunkResult {
    /// Whether sync is complete.
    pub done: bool,
    /// Maximum updated_at time seen.
    pub max_updated_at: Option<DateTime<Utc>>,
    /// Number of records updated.
    pub updates: u32,
    /// Number of records inserted.
    pub inserts: u32,
    /// Error if any occurred.
    pub error: Option<String>,
}

// =============================================================================
// Storage Operation Types
// =============================================================================

/// Parameters for purging old data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeParams {
    pub max_age_days: u32,
    pub purge_completed: bool,
    pub purge_failed: bool,
}

/// Results from a purge operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeResults {
    pub count: u32,
    pub log: String,
}

/// Result from reviewing storage status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewStatusResult {
    pub log: String,
}

/// Result from admin statistics query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminStatsResult {
    pub users: u32,
    pub transactions: u32,
    pub outputs: u32,
    pub certificates: u32,
    pub proven_txs: u32,
    pub proven_tx_reqs: u32,
}

// =============================================================================
// Wallet Storage Reader
// =============================================================================

/// Read-only storage operations.
///
/// This trait provides all read operations for wallet storage.
#[async_trait]
pub trait WalletStorageReader: Send + Sync {
    /// Check if storage is available and ready.
    fn is_available(&self) -> bool;

    /// Get current settings.
    fn get_settings(&self) -> &TableSettings;

    /// Get the wallet services instance.
    ///
    /// Returns an error if services have not been set via `set_services`.
    /// This is required for operations that need blockchain access:
    /// - BEEF verification via ChainTracker
    /// - Transaction broadcasting via postBeef
    /// - UTXO validation via isUtxo
    /// - Block header lookups
    fn get_services(&self) -> Result<Arc<dyn WalletServices>>;

    /// Find certificates matching criteria.
    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>>;

    /// Find output baskets matching criteria.
    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>>;

    /// Find outputs matching criteria.
    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>>;

    /// Find proven transaction requests.
    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>>;

    /// Find transactions matching criteria.
    async fn find_transactions(&self, args: FindTransactionsArgs) -> Result<Vec<TableTransaction>>;

    /// List actions (transactions) for the user.
    async fn list_actions(&self, auth: &AuthId, args: ListActionsArgs)
        -> Result<ListActionsResult>;

    /// List certificates for the user.
    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult>;

    /// List outputs for the user.
    async fn list_outputs(&self, auth: &AuthId, args: ListOutputsArgs)
        -> Result<ListOutputsResult>;
}

// =============================================================================
// Wallet Storage Writer
// =============================================================================

/// Write operations for wallet storage.
///
/// Extends `WalletStorageReader` with write capabilities.
#[async_trait]
pub trait WalletStorageWriter: WalletStorageReader {
    /// Initialize storage and return settings.
    async fn make_available(&self) -> Result<TableSettings>;

    /// Run database migrations.
    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String>;

    /// Destroy the storage (delete all data).
    async fn destroy(&self) -> Result<()>;

    /// Find or create a user by identity key.
    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)>;

    /// Abort an in-progress action.
    async fn abort_action(&self, auth: &AuthId, args: AbortActionArgs)
        -> Result<AbortActionResult>;

    /// Create a new action (transaction).
    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult>;

    /// Process an action after signing.
    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults>;

    /// Internalize an external action.
    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult>;

    /// Mark an internalized transaction as failed after broadcast failure.
    ///
    /// Sets tx status to 'failed', marks created outputs as unspendable,
    /// and sets proven_tx_req to 'invalid'. This prevents ghost UTXOs from
    /// poisoning subsequent transactions.
    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()>;

    /// Insert a certificate.
    async fn insert_certificate(&self, auth: &AuthId, certificate: TableCertificate)
        -> Result<i64>;

    /// Insert a certificate field.
    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64>;

    /// Relinquish (release) a certificate.
    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64>;

    /// Relinquish (release) an output.
    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64>;

    /// Update transaction status after broadcast attempt.
    ///
    /// This method is called by the wallet layer after attempting to broadcast a transaction.
    /// It updates the transaction and proven_tx_req statuses based on whether the broadcast
    /// succeeded or failed.
    ///
    /// On success: Sets transaction status to 'unproven' and proven_tx_req to 'unmined'.
    /// On failure: Sets transaction status to 'failed' and restores spent inputs to spendable.
    ///
    /// # Arguments
    /// * `txid` - The transaction ID
    /// * `success` - Whether the broadcast succeeded
    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        success: bool,
    ) -> Result<()>;

    /// Review storage status and clean up aged items.
    ///
    /// Checks for proven_tx_reqs and transactions that may need attention,
    /// and returns a log of what was found/processed.
    ///
    /// # Arguments
    /// * `auth` - The authenticated user
    /// * `aged_limit` - Items older than this are considered aged
    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: DateTime<Utc>,
    ) -> Result<ReviewStatusResult>;

    /// Purge old data from storage.
    ///
    /// Removes completed and/or failed records older than the specified age.
    ///
    /// # Arguments
    /// * `auth` - The authenticated user
    /// * `params` - Parameters controlling what to purge
    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults>;

    /// Begin a new storage transaction scope.
    ///
    /// Returns a [`TrxToken`] that can be passed to subsequent storage operations
    /// to group them into a single atomic unit of work. The caller must eventually
    /// call [`commit_transaction`](Self::commit_transaction) or
    /// [`rollback_transaction`](Self::rollback_transaction) to close the scope.
    ///
    /// Mirrors TypeScript `beginStorageTransaction()`.
    async fn begin_transaction(&self) -> Result<TrxToken>;

    /// Commit a previously begun transaction scope.
    ///
    /// All operations performed under this [`TrxToken`] are made permanent.
    /// Returns an error if the token is unknown (already committed/rolled back
    /// or never created).
    async fn commit_transaction(&self, trx: TrxToken) -> Result<()>;

    /// Roll back a previously begun transaction scope.
    ///
    /// All operations performed under this [`TrxToken`] are discarded.
    /// Returns an error if the token is unknown (already committed/rolled back
    /// or never created).
    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()>;
}

// =============================================================================
// Wallet Storage Sync
// =============================================================================

/// Synchronization operations between storages.
///
/// Extends `WalletStorageWriter` with sync capabilities.
#[async_trait]
pub trait WalletStorageSync: WalletStorageWriter {
    /// Find or create a sync state record.
    async fn find_or_insert_sync_state(
        &self,
        auth: &AuthId,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)>;

    /// Set the active storage for a user.
    async fn set_active(&self, auth: &AuthId, new_active_storage_identity_key: &str)
        -> Result<i64>;

    /// Get a chunk of data for synchronization.
    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk>;

    /// Process a received sync chunk.
    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult>;
}

// =============================================================================
// Wallet Storage Provider
// =============================================================================

/// Full storage provider interface.
///
/// This is the complete interface that storage implementations must provide.
/// It combines all read, write, and sync operations.
#[async_trait]
pub trait WalletStorageProvider: WalletStorageSync {
    /// Returns true if this can be extended to the full StorageProvider interface.
    fn is_storage_provider(&self) -> bool {
        true
    }

    /// Get the storage identity key.
    fn storage_identity_key(&self) -> &str;

    /// Get the storage name.
    fn storage_name(&self) -> &str;

    /// Set the wallet services instance.
    ///
    /// This must be called before any operations that require blockchain access:
    /// - createAction (BEEF verification)
    /// - processAction (BEEF verification, nLockTime checks)
    /// - internalizeAction (BEEF verification, header lookups)
    /// - Network broadcasting
    ///
    /// # Arguments
    /// * `services` - The wallet services to use for blockchain operations
    fn set_services(&self, services: Arc<dyn WalletServices>);
}

// =============================================================================
// Storage Info
// =============================================================================

/// Information about a configured storage provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletStorageInfo {
    pub is_active: bool,
    pub is_enabled: bool,
    pub is_backup: bool,
    pub is_conflicting: bool,
    pub user_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub storage_class: String,
    pub endpoint_url: Option<String>,
}

// =============================================================================
// BEEF Verification
// =============================================================================

/// Mode for BEEF merkle proof verification.
///
/// Controls how BEEF (Background Evaluation Extended Format) transactions
/// are verified against the blockchain when internalizing or creating actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BeefVerificationMode {
    /// Verify all BEEF merkle proofs against the chain.
    /// Returns error if any proof is invalid.
    #[default]
    Strict,

    /// Skip verification for transactions already known to this wallet.
    /// Only verify merkle proofs for new/unknown txids.
    TrustKnown,

    /// Disable BEEF verification entirely.
    /// Use when no ChainTracker is available or verification is handled elsewhere.
    Disabled,
}

// =============================================================================
// Monitor Storage
// =============================================================================

use std::time::Duration;

/// Status result for a synchronized transaction.
///
/// Returned by `synchronize_transaction_statuses` to report the outcome
/// for each transaction that was checked.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxSynchronizedStatus {
    /// Transaction ID.
    pub txid: String,
    /// New status after synchronization.
    pub status: ProvenTxReqStatus,
    /// Block height if mined.
    pub block_height: Option<u32>,
    /// Block hash if mined.
    pub block_hash: Option<String>,
    /// Merkle root if mined.
    pub merkle_root: Option<String>,
    /// Encoded merkle path if mined.
    pub merkle_path: Option<Vec<u8>>,
}

/// Storage operations used by the monitor daemon.
///
/// This trait provides dedicated methods for background monitoring tasks.
/// Each method encapsulates the full logic for a monitor operation:
/// querying the database, calling external services, and updating records.
///
/// The trait mirrors Go's `MonitoredStorage` interface.
#[async_trait]
pub trait MonitorStorage: WalletStorageProvider {
    /// Synchronize transaction statuses by fetching merkle proofs.
    ///
    /// This method:
    /// 1. Queries proven_tx_reqs with status: unmined, unknown, callback, sending, unconfirmed
    /// 2. For each transaction, fetches merkle path from services
    /// 3. On success with proof: updates proven_tx_req to completed, updates transaction status
    /// 4. On not found: increments attempts counter
    /// 5. Marks transactions as invalid after max attempts exceeded
    ///
    /// # Returns
    ///
    /// A list of transactions that were synchronized with their new statuses.
    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>>;

    /// Send transactions that are waiting to be broadcast.
    ///
    /// This method:
    /// 1. Queries proven_tx_reqs with status: unsent or sending
    /// 2. Groups transactions by batch_id
    /// 3. For each batch, builds BEEF and broadcasts via services
    /// 4. On success: updates status to unmined
    /// 5. On double-spend: marks as failed
    ///
    /// # Arguments
    ///
    /// * `min_transaction_age` - Only send transactions older than this duration
    ///
    /// # Returns
    ///
    /// Process action results for any transactions that were sent.
    async fn send_waiting_transactions(
        &self,
        min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>>;

    /// Abort transactions that have been abandoned.
    ///
    /// This method finds transactions in 'unsigned' or 'unprocessed' status
    /// that are older than the configured timeout and aborts them to release
    /// locked UTXOs.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Transactions older than this are considered abandoned
    async fn abort_abandoned(&self, timeout: Duration) -> Result<()>;

    /// Attempt to recover transactions incorrectly marked as failed.
    ///
    /// This method:
    /// 1. Queries proven_tx_reqs with status: unfail
    /// 2. For each, checks if transaction has a merkle path on-chain
    /// 3. If found: restores the transaction as unproven
    /// 4. If not found: marks as invalid
    async fn un_fail(&self) -> Result<()>;

    /// Review and synchronize transaction statuses.
    ///
    /// This is the monitor-level review_status that operates across all users
    /// without requiring an AuthId. It checks for proven_tx_reqs and transactions
    /// that may need status corrections.
    async fn review_status(&self) -> Result<ReviewStatusResult>;

    /// Purge old data from storage.
    ///
    /// This is the monitor-level purge that operates across all users
    /// without requiring an AuthId. It removes old completed and/or failed
    /// records based on the provided parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters controlling what to purge
    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults>;

    /// Compact stored input_beef blobs by upgrading unproven transactions
    /// with now-available merkle proofs and trimming unnecessary ancestors.
    ///
    /// This retroactively compacts input_beef blobs in `proven_tx_reqs` for
    /// completed transactions. Only processes completed proof requests to
    /// avoid interfering with pending broadcasts.
    ///
    /// # Returns
    ///
    /// The number of input_beef blobs that were compacted.
    async fn compact_input_beefs(&self) -> Result<u32> {
        // Default: no-op
        Ok(0)
    }

    /// Try to acquire a distributed lock for a monitor task.
    ///
    /// Used for multi-instance support: when multiple monitor daemons run
    /// against the same storage, only one should execute each task at a time.
    ///
    /// # Arguments
    ///
    /// * `task_name` - The name of the task to lock (e.g., "check_for_proofs")
    /// * `instance_id` - The unique ID of the monitor instance requesting the lock
    /// * `ttl` - Time-to-live for the lock; it auto-expires after this duration
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the lock was acquired, `Ok(false)` if another instance holds it.
    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        // Default: always acquire (single-instance mode)
        let _ = (task_name, instance_id, ttl);
        Ok(true)
    }

    /// Release a previously acquired task lock.
    ///
    /// # Arguments
    ///
    /// * `task_name` - The name of the task to unlock
    /// * `instance_id` - The unique ID of the monitor instance releasing the lock
    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        // Default: no-op (single-instance mode)
        let _ = (task_name, instance_id);
        Ok(())
    }

    /// Update the status of a proven transaction request.
    ///
    /// Used by the reorg task to demote a completed or other-status proven_tx_req
    /// back to a retriable status (e.g., `Unmined`) when a blockchain reorganization
    /// invalidates its proof. The `CheckForProofs` task will then re-fetch the proof
    /// on its next cycle.
    ///
    /// The default implementation logs a warning but performs no update. Storage
    /// backends that support direct status updates (e.g., `StorageSqlx`) should
    /// override this method.
    ///
    /// # Arguments
    ///
    /// * `proven_tx_req_id` - The ID of the proven_tx_req to update
    /// * `new_status` - The new status to set
    async fn update_proven_tx_req_status(
        &self,
        proven_tx_req_id: i64,
        new_status: ProvenTxReqStatus,
    ) -> Result<()> {
        // Default: log warning only. Concrete backends should override.
        tracing::warn!(
            proven_tx_req_id = proven_tx_req_id,
            new_status = ?new_status,
            "update_proven_tx_req_status called on storage that does not override this method"
        );
        Ok(())
    }
}
