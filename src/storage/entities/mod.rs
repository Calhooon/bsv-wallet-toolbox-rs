//! Database entity definitions.
//!
//! These structs represent the 18 tables in the wallet storage schema.
//! They mirror the TypeScript and Go implementations for cross-SDK compatibility.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// =============================================================================
// Transaction Status
// =============================================================================

/// Status of a transaction in the wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionStatus {
    /// Transaction is complete and confirmed.
    Completed,
    /// Transaction is unprocessed (not yet signed/broadcast).
    Unprocessed,
    /// Transaction is being sent to the network.
    Sending,
    /// Transaction is broadcast but not yet proven.
    Unproven,
    /// Transaction is unsigned.
    Unsigned,
    /// Transaction is marked as no-send (not to be broadcast).
    NoSend,
    /// Transaction has a non-final nLockTime.
    NonFinal,
    /// Transaction failed.
    Failed,
    /// Transaction is marked for unfailing (retry after failure).
    Unfail,
}

impl TransactionStatus {
    /// Returns the status as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionStatus::Completed => "completed",
            TransactionStatus::Unprocessed => "unprocessed",
            TransactionStatus::Sending => "sending",
            TransactionStatus::Unproven => "unproven",
            TransactionStatus::Unsigned => "unsigned",
            TransactionStatus::NoSend => "nosend",
            TransactionStatus::NonFinal => "nonfinal",
            TransactionStatus::Failed => "failed",
            TransactionStatus::Unfail => "unfail",
        }
    }
}

/// Status of a proven transaction request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProvenTxReqStatus {
    /// Request is pending.
    Pending,
    /// Request is in progress.
    InProgress,
    /// Request completed successfully.
    Completed,
    /// Request failed.
    Failed,
    /// Request was not found.
    NotFound,
    /// Transaction is waiting to be sent.
    Unsent,
    /// Transaction is currently being sent.
    Sending,
    /// Transaction was sent but not yet mined.
    Unmined,
    /// Transaction status is unknown.
    Unknown,
    /// Waiting for callback confirmation.
    Callback,
    /// Transaction is unconfirmed on chain.
    Unconfirmed,
    /// Marked for unfail processing.
    Unfail,
    /// Transaction should not be sent.
    NoSend,
    /// Transaction is invalid.
    Invalid,
    /// Transaction is a double spend.
    DoubleSpend,
}

// =============================================================================
// Core Tables
// =============================================================================

/// User record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableUser {
    pub user_id: i64,
    pub identity_key: String,
    pub active_storage: Option<String>,
    /// Created timestamp. Server may return as snake_case or camelCase.
    #[serde(alias = "created_at")]
    pub created_at: DateTime<Utc>,
    /// Updated timestamp. Server may return as snake_case or camelCase.
    #[serde(alias = "updated_at")]
    pub updated_at: DateTime<Utc>,
}

/// Wallet settings (singleton).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSettings {
    /// Settings ID (optional since remote servers may not return it).
    #[serde(default = "default_settings_id")]
    pub settings_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub chain: String,
    pub max_output_script: i32,
    /// Database type (e.g., "MySQL", "SQLite"). Optional field from remote storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dbtype: Option<String>,
    /// Created timestamp. Server may return as snake_case or camelCase.
    #[serde(alias = "created_at")]
    pub created_at: DateTime<Utc>,
    /// Updated timestamp. Server may return as snake_case or camelCase.
    #[serde(alias = "updated_at")]
    pub updated_at: DateTime<Utc>,
}

fn default_settings_id() -> i64 {
    1
}

impl Default for TableSettings {
    fn default() -> Self {
        Self {
            settings_id: 1,
            storage_identity_key: String::new(),
            storage_name: String::new(),
            chain: "mainnet".to_string(),
            max_output_script: 10_000,
            dbtype: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

/// Transaction record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTransaction {
    pub transaction_id: i64,
    pub user_id: i64,
    pub txid: Option<String>,
    pub status: TransactionStatus,
    pub reference: String,
    pub description: String,
    pub satoshis: i64,
    pub version: i32,
    pub lock_time: i64,
    pub raw_tx: Option<Vec<u8>>,
    pub input_beef: Option<Vec<u8>>,
    pub is_outgoing: bool,
    pub proof_txid: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Output (UTXO) record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutput {
    pub output_id: i64,
    pub user_id: i64,
    pub transaction_id: i64,
    pub basket_id: Option<i64>,
    pub txid: String,
    pub vout: i32,
    pub satoshis: i64,
    pub locking_script: Option<Vec<u8>>,
    pub script_length: i32,
    pub script_offset: i32,
    pub output_type: String,
    /// Who provided this output: "you", "storage", or "you-and-storage".
    pub provided_by: String,
    /// Purpose of this output.
    pub purpose: Option<String>,
    /// Description of this output.
    pub output_description: Option<String>,
    /// Transaction ID that spent this output.
    pub spent_by: Option<i64>,
    /// Sequence number for spending input.
    pub sequence_number: Option<u32>,
    /// Description of how this output was spent.
    pub spending_description: Option<String>,
    pub spendable: bool,
    pub change: bool,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
    pub custom_instructions: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Output basket for organizing outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputBasket {
    pub basket_id: i64,
    pub user_id: i64,
    pub name: String,
    pub number_of_desired_utxos: i32,
    pub minimum_desired_utxo_value: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Output tag for labeling outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputTag {
    pub tag_id: i64,
    pub user_id: i64,
    pub tag: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Many-to-many mapping between outputs and tags.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableOutputTagMap {
    pub output_tag_map_id: i64,
    pub output_id: i64,
    pub tag_id: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Transaction label.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTxLabel {
    pub label_id: i64,
    pub user_id: i64,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Many-to-many mapping between transactions and labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableTxLabelMap {
    pub tx_label_map_id: i64,
    pub transaction_id: i64,
    pub label_id: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Proof Tables
// =============================================================================

/// Proven transaction with Merkle proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableProvenTx {
    pub proven_tx_id: i64,
    pub txid: String,
    pub height: i64,
    pub index: i64,
    pub block_hash: String,
    pub merkle_root: String,
    pub merkle_path: Vec<u8>,
    pub raw_tx: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request for a Merkle proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableProvenTxReq {
    pub proven_tx_req_id: i64,
    pub txid: String,
    pub status: ProvenTxReqStatus,
    pub attempts: i32,
    pub history: String,
    /// Whether notifications have been sent for this request.
    pub notified: bool,
    /// JSON string matching TS ProvenTxReqNotifyApi. Default "".
    #[serde(default)]
    pub notify: String,
    /// Raw transaction bytes.
    pub raw_tx: Option<Vec<u8>>,
    /// Input BEEF data for this request.
    pub input_beef: Option<Vec<u8>>,
    pub proven_tx_id: Option<i64>,
    /// Batch identifier for grouping transactions to broadcast together.
    pub batch: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Certificate Tables
// =============================================================================

/// Identity certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCertificate {
    pub certificate_id: i64,
    pub user_id: i64,
    pub cert_type: String,
    pub serial_number: String,
    pub certifier: String,
    pub subject: String,
    pub verifier: Option<String>,
    pub revocation_outpoint: String,
    pub signature: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Certificate field value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCertificateField {
    pub certificate_field_id: i64,
    pub certificate_id: i64,
    pub user_id: i64,
    pub field_name: String,
    pub field_value: String,
    pub master_key: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Sync Tables
// =============================================================================

/// Synchronization state between storages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSyncState {
    pub sync_state_id: i64,
    pub user_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub status: String,
    pub init: bool,
    pub ref_num: String,
    pub sync_map: String,
    pub when_last_sync_started: Option<DateTime<Utc>>,
    /// Total satoshis tracked for this sync state. Optional, matches TS/Go implementations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub satoshis: Option<i64>,
    pub error_local: Option<String>,
    pub error_other: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// =============================================================================
// Other Tables
// =============================================================================

/// Commission record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCommission {
    pub commission_id: i64,
    pub user_id: i64,
    pub transaction_id: i64,
    pub satoshis: i64,
    pub payer_locking_script: Vec<u8>,
    pub key_offset: String,
    pub is_redeemed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Monitor event record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMonitorEvent {
    pub event_id: i64,
    pub event_type: String,
    pub event_data: String,
    pub created_at: DateTime<Utc>,
}
