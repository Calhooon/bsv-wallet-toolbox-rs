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

    /// The chain tip header (height AND hash) from the header service
    /// (chaintracks, then BHS; never a courier). The monitor's header task
    /// observes the tip through this, so the height and the hash always come
    /// from one source. The default is for backends without a header
    /// service.
    async fn get_chain_tip_header(&self) -> Result<BlockHeader> {
        Err(crate::error::Error::ServiceError(
            "get_chain_tip_header: no header service configured".to_string(),
        ))
    }

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

    /// Attach a [`BroadcastMemory`](crate::services::BroadcastMemory) so
    /// `post_beef` can send each provider only what it has not already seen
    /// and try the last accepting provider first.
    ///
    /// The default ignores the memory (a services backend that does not do
    /// its own provider fan-out has nothing to reduce). `Services` overrides
    /// it; the `Wallet`, the `Monitor` and `StorageSqlx::set_services` call
    /// it with the storage's persisted memory.
    fn set_broadcast_memory(
        &self,
        memory: std::sync::Arc<dyn crate::services::broadcast_memory::BroadcastMemory>,
    ) {
        let _ = memory;
    }

    /// The attached broadcast memory, if any.
    fn broadcast_memory(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::services::broadcast_memory::BroadcastMemory>> {
        None
    }

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
///
/// `Default` is the all-zero header: a provider that knows only part of a
/// header (Arcade's status document gives height, hash and, recomputed from
/// the BUMP, merkle root, but no version/time/bits/nonce/previous hash) fills
/// what it knows and leaves the rest zeroed. Consumers of a merkle path
/// result read height, hash and merkle root only.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// What ONE provider said about a transaction's merkle path, derived from
/// the per-provider notes (`what` + `name`) every provider and the
/// `Services` ladder write into `notes`. The reorg re-prove decides on these
/// (F4/F7 of the reorg review): a stored proof is demoted only on POSITIVE
/// evidence, at least two providers that answered cleanly "not mined",
/// never on a fault (a 429, a timeout, a 5xx, a tracker error) and never on
/// a path the tracker refuted (retained and retried instead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderVerdict {
    /// The provider answered cleanly and holds no proof: a 404, an empty
    /// body for an unmined transaction, or a status below MINED.
    NotMined { provider: String },
    /// The provider served a path the ladder accepted.
    Proof { provider: String },
    /// The provider served a path the chain tracker refuted.
    Refuted { provider: String },
    /// The provider (or the tracker validating its answer, or the header
    /// resolution) errored.
    Fault { provider: String, error: String },
    /// The provider knows the transaction but served no usable path and no
    /// verdict (a record with an empty merkle path); counts as nothing.
    Inconclusive { provider: String },
}

/// Note kinds that are a clean "not mined" answer.
pub const NOTE_NOT_MINED: &[&str] = &[
    "getMerklePathNotFound",
    "getMerklePathNoData",
    "getMerklePathNotMined",
];

/// Note kinds that are a fault (the answer is unusable, not evidence).
pub const NOTE_FAULT: &[&str] = &[
    "getMerklePathBadStatus",
    "getMerklePathServiceError",
    "getMerklePathError",
    "getMerklePathTrackerError",
    "getMerklePathHeaderUnresolved",
    "getMerklePathBadProof",
    "getMerklePathMultiple",
    "getMerklePathHeightMismatch",
];

/// The note kind the ladder writes when the chain tracker refutes a served
/// path.
pub const NOTE_REFUTED: &str = "getMerklePathInvalidRoot";

/// The note kind a provider writes when it served a path.
pub const NOTE_PROOF: &str = "getMerklePathSuccess";

/// Note kinds that are inconclusive (known, no path).
pub const NOTE_INCONCLUSIVE: &[&str] = &["getMerklePathNoPath"];

/// Build one per-provider note (`what`, `name`, `when`, optional `error`).
pub fn merkle_path_note(
    provider: &str,
    what: &str,
    error: Option<&str>,
) -> HashMap<String, serde_json::Value> {
    let mut note = HashMap::new();
    note.insert(
        "what".to_string(),
        serde_json::Value::String(what.to_string()),
    );
    note.insert(
        "name".to_string(),
        serde_json::Value::String(provider.to_string()),
    );
    note.insert(
        "when".to_string(),
        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
    );
    if let Some(e) = error {
        note.insert(
            "error".to_string(),
            serde_json::Value::String(e.to_string()),
        );
    }
    note
}

impl GetMerklePathResult {
    /// One verdict per provider name, in first-seen order. Precedence when a
    /// provider left several notes: `Refuted` over `Fault` over `NotMined`
    /// over `Proof` over `Inconclusive` (a served path that later failed
    /// validation is a refutation, not a proof). Notes of unknown kinds are
    /// ignored.
    pub fn provider_verdicts(&self) -> Vec<ProviderVerdict> {
        fn rank(v: &ProviderVerdict) -> u8 {
            match v {
                ProviderVerdict::Refuted { .. } => 5,
                ProviderVerdict::Fault { .. } => 4,
                ProviderVerdict::NotMined { .. } => 3,
                ProviderVerdict::Proof { .. } => 2,
                ProviderVerdict::Inconclusive { .. } => 1,
            }
        }
        let mut order: Vec<String> = Vec::new();
        let mut best: HashMap<String, ProviderVerdict> = HashMap::new();
        for note in &self.notes {
            let Some(what) = note.get("what").and_then(|v| v.as_str()) else {
                continue;
            };
            let provider = note
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let error = note
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or(what)
                .to_string();
            let verdict = if what == NOTE_REFUTED {
                ProviderVerdict::Refuted {
                    provider: provider.clone(),
                }
            } else if NOTE_FAULT.contains(&what) {
                ProviderVerdict::Fault {
                    provider: provider.clone(),
                    error,
                }
            } else if NOTE_NOT_MINED.contains(&what) {
                ProviderVerdict::NotMined {
                    provider: provider.clone(),
                }
            } else if what == NOTE_PROOF {
                ProviderVerdict::Proof {
                    provider: provider.clone(),
                }
            } else if NOTE_INCONCLUSIVE.contains(&what) {
                ProviderVerdict::Inconclusive {
                    provider: provider.clone(),
                }
            } else {
                continue;
            };
            match best.get(&provider) {
                Some(existing) if rank(existing) >= rank(&verdict) => {}
                Some(_) => {
                    best.insert(provider, verdict);
                }
                None => {
                    order.push(provider.clone());
                    best.insert(provider, verdict);
                }
            }
        }
        order
            .into_iter()
            .filter_map(|name| best.remove(&name))
            .collect()
    }

    /// The providers that answered cleanly "not mined" (distinct names).
    pub fn not_mined_witnesses(&self) -> Vec<String> {
        self.provider_verdicts()
            .into_iter()
            .filter_map(|v| match v {
                ProviderVerdict::NotMined { provider } => Some(provider),
                _ => None,
            })
            .collect()
    }

    /// The providers whose served path the chain tracker refuted.
    pub fn refuted_witnesses(&self) -> Vec<String> {
        self.provider_verdicts()
            .into_iter()
            .filter_map(|v| match v {
                ProviderVerdict::Refuted { provider } => Some(provider),
                _ => None,
            })
            .collect()
    }

    /// The provider faults, as `name: error`.
    pub fn faults(&self) -> Vec<String> {
        self.provider_verdicts()
            .into_iter()
            .filter_map(|v| match v {
                ProviderVerdict::Fault { provider, error } => Some(format!("{provider}: {error}")),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod merkle_path_verdict_tests {
    use super::*;

    fn result(notes: Vec<HashMap<String, serde_json::Value>>) -> GetMerklePathResult {
        GetMerklePathResult {
            name: Some("Services".into()),
            merkle_path: None,
            header: None,
            error: None,
            notes,
        }
    }

    #[test]
    fn a_clean_negative_is_a_witness_and_a_provider_error_is_a_fault() {
        let r = result(vec![
            merkle_path_note("WoC", "getMerklePathNotFound", None),
            merkle_path_note("Bitails", "getMerklePathBadStatus", Some("HTTP 429")),
            merkle_path_note("Arcade", "getMerklePathNotMined", None),
            merkle_path_note("TAAL", "getMerklePathNoPath", None),
        ]);
        assert_eq!(
            r.not_mined_witnesses(),
            vec!["WoC".to_string(), "Arcade".to_string()]
        );
        assert_eq!(r.faults(), vec!["Bitails: HTTP 429".to_string()]);
        assert!(r.refuted_witnesses().is_empty());
        assert_eq!(r.provider_verdicts().len(), 4);
    }

    #[test]
    fn a_served_path_the_tracker_refuted_is_a_refutation_not_a_proof() {
        let r = result(vec![
            merkle_path_note("WoC", "getMerklePathSuccess", None),
            merkle_path_note("WoC", "getMerklePathInvalidRoot", None),
            merkle_path_note("Bitails", "getMerklePathSuccess", None),
            merkle_path_note("Bitails", "getMerklePathTrackerError", Some("timeout")),
        ]);
        assert_eq!(r.refuted_witnesses(), vec!["WoC".to_string()]);
        assert_eq!(r.faults(), vec!["Bitails: timeout".to_string()]);
        assert!(r.not_mined_witnesses().is_empty());
    }

    #[test]
    fn one_provider_never_counts_twice_and_unknown_notes_are_ignored() {
        let r = result(vec![
            merkle_path_note("WoC", "getMerklePathNotFound", None),
            merkle_path_note("WoC", "getMerklePathNoData", None),
            merkle_path_note("WoC", "somethingElse", None),
        ]);
        assert_eq!(r.not_mined_witnesses(), vec!["WoC".to_string()]);
        assert_eq!(r.provider_verdicts().len(), 1);
    }
}

// =============================================================================
// Post BEEF Result
// =============================================================================

/// How a provider actually delivered one broadcast: what went over the wire
/// and which txids the provider took. Feeds the per-broadcast log line and
/// the [`BroadcastMemory`](crate::services::BroadcastMemory).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostBeefDelivery {
    /// The send omitted transactions the provider had already seen (EF of
    /// the subject alone, or an EF batch of the unseen ancestors).
    pub reduced: bool,
    /// Bytes actually sent (all attempts, including a full-package
    /// fallback).
    pub bytes_sent: usize,
    /// A reduced send was refused for what read as a missing parent and the
    /// full package was sent once more.
    pub fallback_full: bool,
    /// txids the provider accepted in this delivery: the subject plus every
    /// unproven transaction it took in the same package. Empty on failure.
    pub accepted_txids: Vec<String>,
}

impl PostBeefDelivery {
    /// A conventional full-package delivery: `bytes` sent, nothing omitted,
    /// `accepted` recorded only when `result` is a success.
    pub fn full_package(bytes: usize, result: &PostBeefResult, accepted: &[String]) -> Self {
        Self {
            reduced: false,
            bytes_sent: bytes,
            fallback_full: false,
            accepted_txids: if result.is_success() {
                accepted.to_vec()
            } else {
                Vec::new()
            },
        }
    }
}

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

    /// Whether this is an orphan mempool condition (parent not yet propagated).
    /// This is NOT a double-spend — the miner has the child but not the parent.
    #[serde(default)]
    pub orphan_mempool: bool,

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
///
/// `merkle_path` / `block_height` / `block_hash` are populated only by a
/// status provider whose answer ALREADY carries the proof (Arcade's `MINED`
/// document does; WhatsOnChain's and Bitails' batch status answers do not).
/// A caller that finds them set can record the proof without a second
/// `getMerklePath` round trip. They are a hint, never truth: the proof still
/// goes through the same ChainTracker validation as any other provider's.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TxStatusDetail {
    /// Transaction ID.
    pub txid: String,

    /// Status: "unknown", "known", "mined".
    pub status: String,

    /// Confirmation depth (if mined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,

    /// BRC-74 BUMP merkle path (hex), when the status answer carried it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merkle_path: Option<String>,

    /// Height of the block containing the transaction, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u32>,

    /// Hash of the block containing the transaction, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,
}

impl TxStatusDetail {
    /// A status detail with no proof attached (the shape every batch status
    /// provider but Arcade returns).
    pub fn new(txid: impl Into<String>, status: impl Into<String>, depth: Option<u32>) -> Self {
        Self {
            txid: txid.into(),
            status: status.into(),
            depth,
            ..Default::default()
        }
    }
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
