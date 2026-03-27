//! Wallet services trait and result types.
//!
//! Defines the `WalletServices` trait that providers implement, along with
//! all the result types for service methods.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{Error, Result};
use bsv_rs::transaction::{ChainTracker, Transaction};

use super::collection::ServiceCallHistory;

// =============================================================================
// nLockTime Input Types
// =============================================================================

/// Pre-extracted data for nLockTime finality check.
///
/// This struct contains the minimal data needed to check transaction finality,
/// extracted from a Transaction before the async call. This avoids Send/Sync
/// issues with Transaction's RefCell fields.
///
/// Use the `From` implementations to create this from various transaction formats.
#[derive(Debug, Clone)]
pub struct NLockTimeInput {
    /// The nLockTime value from the transaction.
    pub lock_time: u32,
    /// Whether all inputs have sequence == 0xFFFFFFFF (max sequence).
    /// If true, the transaction is immediately final regardless of nLockTime.
    pub all_sequences_final: bool,
}

impl NLockTimeInput {
    /// Create from a raw nLockTime value only.
    ///
    /// Since we don't have sequence information, `all_sequences_final` is set to false
    /// and finality will be determined solely by the lock_time value.
    pub fn from_lock_time(lock_time: u32) -> Self {
        Self {
            lock_time,
            all_sequences_final: false,
        }
    }

    /// Create from a Transaction reference.
    ///
    /// Extracts nLockTime and checks if all inputs have final sequences.
    pub fn from_transaction(tx: &Transaction) -> Self {
        const MAX_SEQUENCE: u32 = 0xFFFFFFFF;
        Self {
            lock_time: tx.lock_time,
            all_sequences_final: tx.inputs.iter().all(|i| i.sequence == MAX_SEQUENCE),
        }
    }

    /// Create from raw transaction bytes.
    ///
    /// Parses the transaction and extracts nLockTime and sequence info.
    ///
    /// # Errors
    /// Returns an error if the bytes cannot be parsed as a valid transaction.
    pub fn from_raw_tx(bytes: &[u8]) -> crate::Result<Self> {
        let tx = Transaction::from_binary(bytes).map_err(|e| {
            crate::Error::InvalidArgument(format!("Failed to parse transaction bytes: {}", e))
        })?;
        Ok(Self::from_transaction(&tx))
    }

    /// Create from a hex-encoded transaction string.
    ///
    /// Decodes the hex and parses the transaction.
    ///
    /// # Errors
    /// Returns an error if the hex is invalid or cannot be parsed as a transaction.
    pub fn from_hex_tx(hex: &str) -> crate::Result<Self> {
        let bytes = hex::decode(hex).map_err(|e| {
            crate::Error::InvalidArgument(format!("Invalid hex transaction: {}", e))
        })?;
        Self::from_raw_tx(&bytes)
    }
}

/// Aggregated call history across all service types.
///
/// Used for diagnostics and monitoring of service provider performance.
/// The default implementation returns empty history.
#[derive(Debug, Clone, Default)]
pub struct ServicesCallHistory {
    /// Version of the history format.
    pub version: u32,
    /// Call history for getMerklePath service.
    pub get_merkle_path: Option<ServiceCallHistory>,
    /// Call history for getRawTx service.
    pub get_raw_tx: Option<ServiceCallHistory>,
    /// Call history for postBeef service.
    pub post_beef: Option<ServiceCallHistory>,
    /// Call history for getUtxoStatus service.
    pub get_utxo_status: Option<ServiceCallHistory>,
    /// Call history for getStatusForTxids service.
    pub get_status_for_txids: Option<ServiceCallHistory>,
    /// Call history for getScriptHashHistory service.
    pub get_script_hash_history: Option<ServiceCallHistory>,
}

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
    ///
    /// # Arguments
    /// * `txid` - Transaction hash for which raw transaction bytes are requested
    /// * `use_next` - If true, skip to next service before starting service requests cycle
    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult>;

    /// Get merkle path proof for a transaction.
    ///
    /// # Arguments
    /// * `txid` - Transaction hash for which proof is requested
    /// * `use_next` - If true, skip to next service before starting service requests cycle
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult>;

    /// Post BEEF transaction to miners.
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>>;

    /// Get UTXO status for a script hash.
    ///
    /// # Arguments
    /// * `output` - Script hash or output to check
    /// * `output_format` - Format of the output parameter
    /// * `outpoint` - Optional specific outpoint (txid.vout)
    /// * `use_next` - If true, skip to next service before starting service requests cycle
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult>;

    /// Get status for multiple transaction IDs.
    ///
    /// # Arguments
    /// * `txids` - List of transaction IDs to check
    /// * `use_next` - If true, skip to next service before starting service requests cycle
    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult>;

    /// Get transaction history for a script hash.
    ///
    /// # Arguments
    /// * `hash` - Script hash to get history for
    /// * `use_next` - If true, skip to next service before starting service requests cycle
    async fn get_script_hash_history(
        &self,
        hash: &str,
        use_next: bool,
    ) -> Result<GetScriptHashHistoryResult>;

    /// Get BSV/USD exchange rate.
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;

    /// Get fiat exchange rate between currencies.
    ///
    /// Returns the exchange rate of `currency` per `base`.
    /// If `base` is not specified, USD is used as the base.
    ///
    /// # Arguments
    /// * `currency` - Target currency (USD, GBP, or EUR)
    /// * `base` - Base currency (defaults to USD if None)
    ///
    /// # Returns
    /// The exchange rate (units of currency per unit of base), or 0.0 if rate not available.
    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64>;

    /// Hash an output script to the format expected by getUtxoStatus.
    fn hash_output_script(&self, script: &[u8]) -> String;

    /// Check if a specific output is a UTXO.
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool>;

    /// Check if nLockTime is final (raw nLockTime value).
    ///
    /// This is the simple version that accepts just the raw u32 nLockTime value.
    /// For Transaction objects or raw bytes, use `n_lock_time_is_final_for_tx`.
    ///
    /// # Arguments
    /// * `n_lock_time` - The raw nLockTime value from a transaction
    ///
    /// # Returns
    /// * `true` if the nLockTime allows the transaction to be mined now
    /// * `false` if the transaction is locked until a future time/block
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool>;

    /// Check if a transaction's nLockTime is final.
    ///
    /// This extended version uses pre-extracted data that includes both the
    /// nLockTime value and sequence information for BIP 68 finality checks.
    ///
    /// Use `NLockTimeInput::from_transaction()`, `NLockTimeInput::from_raw_tx()`,
    /// or `NLockTimeInput::from_hex_tx()` to create the input from various formats.
    ///
    /// # Arguments
    /// * `input` - Pre-extracted nLockTime data (see `NLockTimeInput`)
    ///
    /// # Returns
    /// * `true` if the transaction can be mined now (final)
    /// * `false` if the transaction is time-locked
    ///
    /// # Finality Rules
    /// 1. If all inputs have sequence = 0xFFFFFFFF, transaction is immediately final
    /// 2. If nLockTime >= 500,000,000: it's a Unix timestamp, final if in the past
    /// 3. If nLockTime < 500,000,000: it's a block height, final if current height > nLockTime
    ///
    /// # Example
    /// ```rust,ignore
    /// use bsv_wallet_toolbox_rs::services::{NLockTimeInput, WalletServices};
    ///
    /// // From a Transaction
    /// let input = NLockTimeInput::from_transaction(&tx);
    /// let is_final = services.n_lock_time_is_final_for_tx(input).await?;
    ///
    /// // From raw bytes
    /// let input = NLockTimeInput::from_raw_tx(&raw_tx_bytes)?;
    /// let is_final = services.n_lock_time_is_final_for_tx(input).await?;
    ///
    /// // From hex
    /// let input = NLockTimeInput::from_hex_tx("0100000001...")?;
    /// let is_final = services.n_lock_time_is_final_for_tx(input).await?;
    /// ```
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool>;

    /// Get BEEF for a transaction, building it from raw tx and merkle path.
    ///
    /// This method retrieves the raw transaction and merkle proof, then
    /// assembles them into BEEF (Background Evaluation Extended Format).
    ///
    /// # Arguments
    /// * `txid` - The transaction ID to get BEEF for
    /// * `known_txids` - TXIDs that should be included as TxIDOnly (trimmed)
    ///
    /// # Returns
    /// * `Ok(GetBeefResult)` - The BEEF data and metadata
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult>;

    /// Get aggregated service call history for diagnostics.
    ///
    /// Returns call statistics for all service types. If `reset` is true,
    /// the counters are reset after reading.
    ///
    /// The default implementation returns empty history, which is appropriate
    /// for mock implementations or services that don't track call history.
    ///
    /// # Arguments
    /// * `reset` - If true, reset the call counters after reading
    fn get_services_call_history(&self, _reset: bool) -> ServicesCallHistory {
        ServicesCallHistory::default()
    }
}

// =============================================================================
// Get BEEF Result
// =============================================================================

/// Result of getting BEEF for a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBeefResult {
    /// Provider name that returned the result.
    pub name: String,

    /// Transaction ID.
    pub txid: String,

    /// BEEF bytes (if successful).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beef: Option<Vec<u8>>,

    /// Whether the transaction has a merkle proof.
    pub has_proof: bool,

    /// Error if retrieval failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
            Some(s) => hex::decode(&s).map(Some).map_err(serde::de::Error::custom),
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
// Fiat Currency Types
// =============================================================================

/// Supported fiat currencies for exchange rate conversions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FiatCurrency {
    /// US Dollar
    USD,
    /// British Pound
    GBP,
    /// Euro
    EUR,
}

impl FiatCurrency {
    /// Parse a currency string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "USD" => Some(FiatCurrency::USD),
            "GBP" => Some(FiatCurrency::GBP),
            "EUR" => Some(FiatCurrency::EUR),
            _ => None,
        }
    }

    /// Get the currency code as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            FiatCurrency::USD => "USD",
            FiatCurrency::GBP => "GBP",
            FiatCurrency::EUR => "EUR",
        }
    }
}

impl std::fmt::Display for FiatCurrency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for FiatCurrency {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        FiatCurrency::parse(s).ok_or_else(|| format!("Invalid currency: {}", s))
    }
}

/// Fiat exchange rates with USD as base.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiatExchangeRates {
    /// When these rates were fetched.
    pub timestamp: DateTime<Utc>,

    /// Base currency (always USD).
    pub base: FiatCurrency,

    /// Exchange rates (currency per base unit).
    pub rates: HashMap<FiatCurrency, f64>,
}

impl Default for FiatExchangeRates {
    fn default() -> Self {
        let mut rates = HashMap::new();
        rates.insert(FiatCurrency::USD, 1.0);
        rates.insert(FiatCurrency::EUR, 0.85);
        rates.insert(FiatCurrency::GBP, 0.79);

        Self {
            timestamp: Utc::now(),
            base: FiatCurrency::USD,
            rates,
        }
    }
}

impl FiatExchangeRates {
    /// Create new fiat exchange rates.
    pub fn new(rates: HashMap<FiatCurrency, f64>) -> Self {
        Self {
            timestamp: Utc::now(),
            base: FiatCurrency::USD,
            rates,
        }
    }

    /// Check if the rates are stale (older than the given milliseconds).
    pub fn is_stale(&self, max_age_msecs: u64) -> bool {
        let age = Utc::now() - self.timestamp;
        age.num_milliseconds() as u64 > max_age_msecs
    }

    /// Get the exchange rate from one currency to another.
    /// Returns currency units per base unit.
    pub fn get_rate(&self, currency: FiatCurrency, base: Option<FiatCurrency>) -> Option<f64> {
        let base = base.unwrap_or(FiatCurrency::USD);

        // Get both rates relative to USD (our internal base)
        let currency_rate = self.rates.get(&currency)?;
        let base_rate = self.rates.get(&base)?;

        // Convert: currency per base = (currency/USD) / (base/USD)
        Some(currency_rate / base_rate)
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
