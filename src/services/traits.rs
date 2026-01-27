//! Wallet services trait and result types.
//!
//! Defines the `WalletServices` trait that providers implement, along with
//! all the result types for service methods.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use bsv_sdk::transaction::ChainTracker;
use crate::{Error, Result};

/// Main trait for wallet service operations.
///
/// Provides methods for interacting with blockchain services:
/// - Transaction retrieval and broadcasting
/// - Merkle proof retrieval
/// - UTXO status checking
/// - Script history queries
/// - Exchange rates
#[async_trait]
pub trait WalletServices: Send + Sync {
    /// Get the chain tracker for header validation.
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker>;

    /// Get the current blockchain height.
    async fn get_height(&self) -> Result<u32>;

    /// Get block header for a specific height.
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>>;

    /// Get a block header by its hash.
    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader>;

    /// Get raw transaction bytes by txid.
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult>;

    /// Get merkle path proof for a transaction.
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult>;

    /// Post BEEF transaction to miners.
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>>;

    /// Get UTXO status for a script hash.
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult>;

    /// Get status for multiple transaction IDs.
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult>;

    /// Get transaction history for a script hash.
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult>;

    /// Get BSV/USD exchange rate.
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;

    /// Hash an output script to the format expected by getUtxoStatus.
    fn hash_output_script(&self, script: &[u8]) -> String;

    /// Check if a specific output is a UTXO.
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool>;

    /// Check if nLockTime is final.
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool>;
}

// =============================================================================
// Block Header
// =============================================================================

/// Block header information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block version.
    pub version: u32,

    /// Hash of the previous block.
    pub previous_hash: String,

    /// Merkle root of transactions.
    pub merkle_root: String,

    /// Block timestamp.
    pub time: u32,

    /// Difficulty target bits.
    pub bits: u32,

    /// Nonce value.
    pub nonce: u32,

    /// Block hash.
    pub hash: String,

    /// Block height (if known).
    pub height: u32,
}

impl BlockHeader {
    /// Serialize header to 80 bytes (standard block header format).
    pub fn to_binary(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(80);

        // Version (4 bytes, little-endian)
        result.extend_from_slice(&self.version.to_le_bytes());

        // Previous hash (32 bytes, reversed)
        if let Ok(prev_hash) = hex::decode(&self.previous_hash) {
            let mut reversed: Vec<u8> = prev_hash.into_iter().rev().collect();
            reversed.resize(32, 0);
            result.extend_from_slice(&reversed);
        } else {
            result.extend_from_slice(&[0u8; 32]);
        }

        // Merkle root (32 bytes, reversed)
        if let Ok(merkle) = hex::decode(&self.merkle_root) {
            let mut reversed: Vec<u8> = merkle.into_iter().rev().collect();
            reversed.resize(32, 0);
            result.extend_from_slice(&reversed);
        } else {
            result.extend_from_slice(&[0u8; 32]);
        }

        // Time (4 bytes, little-endian)
        result.extend_from_slice(&self.time.to_le_bytes());

        // Bits (4 bytes, little-endian)
        result.extend_from_slice(&self.bits.to_le_bytes());

        // Nonce (4 bytes, little-endian)
        result.extend_from_slice(&self.nonce.to_le_bytes());

        result
    }
}

// =============================================================================
// Get Raw Tx Result
// =============================================================================

/// Result of getting a raw transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRawTxResult {
    /// Provider name that returned the result.
    pub name: String,

    /// Transaction ID.
    pub txid: String,

    /// Raw transaction bytes.
    #[serde(with = "serde_bytes_opt")]
    pub raw_tx: Option<Vec<u8>>,

    /// Error if retrieval failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

mod serde_bytes_opt {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(b) => serializer.serialize_str(&hex::encode(b)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => hex::decode(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

// =============================================================================
// Get Merkle Path Result
// =============================================================================

/// Result of getting a merkle path proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMerklePathResult {
    /// Provider name.
    pub name: Option<String>,

    /// Merkle path as serialized BUMP format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merkle_path: Option<String>,

    /// Block header containing this transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<BlockHeader>,

    /// Error if retrieval failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Notes about the retrieval process.
    #[serde(default)]
    pub notes: Vec<HashMap<String, serde_json::Value>>,
}

// =============================================================================
// Post BEEF Result
// =============================================================================

/// Result of posting a BEEF transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostBeefResult {
    /// Provider name.
    pub name: String,

    /// Overall status: "success" or "error".
    pub status: String,

    /// Results for each transaction ID.
    #[serde(default)]
    pub txid_results: Vec<PostTxResultForTxid>,

    /// Error if overall post failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Notes about the posting process.
    #[serde(default)]
    pub notes: Vec<HashMap<String, serde_json::Value>>,
}

impl PostBeefResult {
    /// Check if the post was successful.
    pub fn is_success(&self) -> bool {
        self.status == "success"
    }
}

/// Result for a single transaction in a BEEF post.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostTxResultForTxid {
    /// Transaction ID.
    pub txid: String,

    /// Status: "success" or "error".
    pub status: String,

    /// Whether this is a double-spend attempt.
    #[serde(default)]
    pub double_spend: bool,

    /// Competing transactions (if double-spend).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub competing_txs: Option<Vec<String>>,

    /// Additional data from the service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    /// Whether this was a service error (not transaction error).
    #[serde(default)]
    pub service_error: bool,

    /// Block hash (if mined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,

    /// Block height (if mined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u32>,

    /// Notes about this transaction.
    #[serde(default)]
    pub notes: Vec<HashMap<String, serde_json::Value>>,
}

impl PostTxResultForTxid {
    /// Check if the transaction was posted successfully.
    pub fn is_success(&self) -> bool {
        self.status == "success"
    }
}

// =============================================================================
// UTXO Status
// =============================================================================

/// Format for UTXO status output parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GetUtxoStatusOutputFormat {
    /// Little-endian script hash (default).
    #[default]
    HashLE,
    /// Big-endian script hash.
    HashBE,
    /// Raw script bytes.
    Script,
}

/// Result of checking UTXO status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetUtxoStatusResult {
    /// Provider name.
    pub name: String,

    /// Status: "success" or "error".
    pub status: String,

    /// Whether the output is a UTXO.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_utxo: Option<bool>,

    /// Details about UTXOs found for this script hash.
    #[serde(default)]
    pub details: Vec<UtxoDetail>,

    /// Error if check failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Details about a UTXO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UtxoDetail {
    /// Transaction ID.
    pub txid: String,

    /// Output index.
    pub index: u32,

    /// Satoshi value.
    pub satoshis: u64,

    /// Block height (if confirmed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

// =============================================================================
// Transaction Status
// =============================================================================

/// Result of getting status for multiple transaction IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStatusForTxidsResult {
    /// Provider name.
    pub name: String,

    /// Status: "success" or "error".
    pub status: String,

    /// Error if retrieval failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Status for each requested txid.
    #[serde(default)]
    pub results: Vec<TxStatusDetail>,
}

/// Status detail for a single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxStatusDetail {
    /// Transaction ID.
    pub txid: String,

    /// Status: "unknown", "known", "mined".
    pub status: String,

    /// Confirmation depth (if mined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
}

// =============================================================================
// Script Hash History
// =============================================================================

/// Result of getting script hash history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetScriptHashHistoryResult {
    /// Provider name.
    pub name: String,

    /// Status: "success" or "error".
    pub status: String,

    /// Error if retrieval failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Transaction history for this script hash.
    #[serde(default)]
    pub history: Vec<ScriptHistoryItem>,
}

/// A single item in script history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptHistoryItem {
    /// Transaction ID.
    pub txid: String,

    /// Block height (if confirmed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

// =============================================================================
// Exchange Rate
// =============================================================================

/// BSV exchange rate information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BsvExchangeRate {
    /// When this rate was fetched.
    pub timestamp: DateTime<Utc>,

    /// Base currency (usually "USD").
    pub base: String,

    /// Exchange rate (USD per BSV).
    pub rate: f64,
}

impl BsvExchangeRate {
    /// Create a new exchange rate.
    pub fn new(rate: f64) -> Self {
        Self {
            timestamp: Utc::now(),
            base: "USD".to_string(),
            rate,
        }
    }

    /// Check if the rate is stale (older than the given milliseconds).
    pub fn is_stale(&self, max_age_msecs: u64) -> bool {
        let age = Utc::now() - self.timestamp;
        age.num_milliseconds() as u64 > max_age_msecs
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Compute SHA256 hash of data.
pub fn sha256(data: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute double SHA256 hash (as used in Bitcoin).
pub fn double_sha256(data: &[u8]) -> Vec<u8> {
    sha256(&sha256(data))
}

/// Compute double SHA256 and return as big-endian hex (txid format).
pub fn txid_from_raw_tx(raw_tx: &[u8]) -> String {
    let hash = double_sha256(raw_tx);
    // Reverse to get big-endian (txid format)
    let reversed: Vec<u8> = hash.into_iter().rev().collect();
    hex::encode(reversed)
}

/// Validate that a computed txid matches expected.
pub fn validate_txid(raw_tx: &[u8], expected_txid: &str) -> Result<()> {
    let computed = txid_from_raw_tx(raw_tx);
    if computed != expected_txid {
        return Err(Error::ValidationError(format!(
            "Computed txid {} doesn't match expected {}",
            computed, expected_txid
        )));
    }
    Ok(())
}

/// Validate script hash format (64 hex characters).
pub fn validate_script_hash(hash: &str) -> Result<()> {
    if hash.len() != 64 {
        return Err(Error::InvalidArgument(format!(
            "Script hash must be 64 hex characters, got {}",
            hash.len()
        )));
    }
    if !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::InvalidArgument(
            "Script hash must be valid hex".to_string(),
        ));
    }
    Ok(())
}

/// Convert script hash format.
pub fn convert_script_hash(
    output: &str,
    format: Option<GetUtxoStatusOutputFormat>,
) -> Result<String> {
    let format = format.unwrap_or_default();

    match format {
        GetUtxoStatusOutputFormat::HashLE => {
            // Already in LE format, reverse to BE for API
            let bytes = hex::decode(output)
                .map_err(|e| Error::InvalidArgument(format!("Invalid hex: {}", e)))?;
            let reversed: Vec<u8> = bytes.into_iter().rev().collect();
            Ok(hex::encode(reversed))
        }
        GetUtxoStatusOutputFormat::HashBE => {
            // Already in BE format
            Ok(output.to_string())
        }
        GetUtxoStatusOutputFormat::Script => {
            // Hash the script and return BE
            let script_bytes = hex::decode(output)
                .map_err(|e| Error::InvalidArgument(format!("Invalid hex: {}", e)))?;
            let hash = sha256(&script_bytes);
            let reversed: Vec<u8> = hash.into_iter().rev().collect();
            Ok(hex::encode(reversed))
        }
    }
}
