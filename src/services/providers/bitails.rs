//! Bitails service provider.
//!
//! Provides access to Bitails API for:
//! - Raw transaction retrieval
//! - Merkle proof retrieval (TSC format)
//! - Transaction broadcasting
//! - Script hash history
//!
//! # API Endpoints
//!
//! - Mainnet: `https://api.bitails.io/`
//! - Testnet: `https://test-api.bitails.io/`

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::chaintracks::Chain;
use crate::services::traits::{
    sha256, validate_txid, BlockHeader, GetMerklePathResult, GetRawTxResult,
    GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, PostBeefResult,
    PostTxResultForTxid, TxStatusDetail, UtxoDetail,
};
#[cfg(feature = "break-glass-script-history")]
use crate::services::traits::{
    validate_script_hash, GetScriptHashHistoryResult, ScriptHistoryItem,
};
use crate::{Error, Result};

/// Bitails mainnet API URL.
pub const BITAILS_MAINNET_URL: &str = "https://api.bitails.io/";

/// Bitails testnet API URL.
pub const BITAILS_TESTNET_URL: &str = "https://test-api.bitails.io/";

/// Error codes returned by Bitails.
pub mod error_codes {
    /// Transaction already in mempool.
    pub const ALREADY_IN_MEMPOOL: &str = "-27";
    /// Double spend or missing inputs (same error code in Bitails).
    pub const DOUBLE_SPEND_OR_MISSING_INPUTS: &str = "-25";
    /// Connection refused.
    pub const ECONNREFUSED: &str = "ECONNREFUSED";
    /// Connection reset.
    pub const ECONNRESET: &str = "ECONNRESET";
}

/// Configuration for Bitails provider.
#[derive(Debug, Clone, Default)]
pub struct BitailsConfig {
    /// API key for authentication (optional).
    pub api_key: Option<String>,

    /// Request timeout in seconds.
    pub timeout_secs: Option<u64>,
}

impl BitailsConfig {
    /// Create config with API key.
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            api_key: Some(api_key.into()),
            timeout_secs: None,
        }
    }
}

/// The length at which an unspent list that does not hold the outpoint is
/// treated as possibly truncated (a page of a longer set) and so as "could
/// not look". A wallet's output scripts hold one or a few outputs; this is
/// a guard on the negative, not a limit on anything.
pub const BITAILS_UNSPENT_PAGE: usize = 100;

/// Bitails service provider.
pub struct Bitails {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    chain: Chain,
    api_key: Option<String>,
}

impl Bitails {
    /// Create a new Bitails provider.
    pub fn new(chain: Chain, config: BitailsConfig) -> Result<Self> {
        let base_url = match chain {
            Chain::Main => BITAILS_MAINNET_URL.to_string(),
            Chain::Test => BITAILS_TESTNET_URL.to_string(),
        };

        let timeout = config.timeout_secs.unwrap_or(30);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url,
            chain,
            api_key: config.api_key,
        })
    }

    /// The provider at another API base: a local fixture standing in for
    /// the explorer, so a test can count the requests that reach it.
    #[cfg(test)]
    pub(crate) fn with_base_url(chain: Chain, base_url: &str) -> Self {
        let mut bitails = Self::new(chain, BitailsConfig::default()).expect("client");
        bitails.base_url = format!("{}/", base_url.trim_end_matches('/'));
        bitails
    }

    /// The API base for this provider's chain (with its trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get HTTP headers.
    fn get_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "application/json".parse().unwrap());

        if let Some(ref api_key) = self.api_key {
            if !api_key.is_empty() {
                headers.insert("Authorization", api_key.parse().unwrap());
            }
        }

        headers
    }

    // =========================================================================
    // Raw Transaction
    // =========================================================================

    /// Get raw transaction by txid.
    ///
    /// Break-glass (Rule 28, T9): a transaction's bytes. Our own storage
    /// holds our own transactions; for a foreign ancestor the sender's BEEF
    /// did not carry, no header, proof or index of ours has them. The
    /// answer is self-verifying: the bytes are bound to the txid before
    /// they are returned. A 404 is "no such transaction"; every other
    /// failure is an error ("could not look").
    pub async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        let url = format!("{}tx/{}/hex", self.base_url, txid);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let hex_str = response
                    .text()
                    .await
                    .map_err(|e| Error::NetworkError(format!("Failed to read response: {}", e)))?;

                let raw_tx = hex::decode(hex_str.trim())
                    .map_err(|e| Error::ValidationError(format!("Failed to decode hex: {}", e)))?;

                // Validate txid
                validate_txid(&raw_tx, txid)?;

                Ok(GetRawTxResult {
                    name: "Bitails".to_string(),
                    txid: txid.to_string(),
                    raw_tx: Some(raw_tx),
                    error: None,
                    could_not_look: false,
                })
            }
            StatusCode::NOT_FOUND => Ok(GetRawTxResult {
                name: "Bitails".to_string(),
                txid: txid.to_string(),
                raw_tx: None,
                error: None,
                could_not_look: false,
            }),
            status => Err(Error::ServiceError(format!(
                "Bitails getRawTx failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Merkle Path
    // =========================================================================

    /// Get merkle path proof for a transaction.
    ///
    /// Break-glass (Rule 28, T7): a transaction's inclusion proof. For a
    /// transaction we received, the BEEF's own BUMP is the proof; for one
    /// we broadcast through Arcade, the BUMP in Arcade's MINED document
    /// is. This explorer is asked only as the courier of a proof nobody
    /// pushed to us; `Services::get_merkle_path` checks its root against
    /// the header service before it is returned, and asks no explorer when
    /// no header service is configured.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        let url = format!("{}tx/{}/proof/tsc", self.base_url, txid);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let data: BitailsTscProof = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse proof: {}", e)))?;

                // Convert to standard format
                Ok(GetMerklePathResult {
                    name: Some("BitailsTsc".to_string()),
                    merkle_path: Some(serde_json::to_string(&data).unwrap_or_default()),
                    header: None, // Would need to fetch from hash_to_header
                    error: None,
                    notes: vec![make_note("getMerklePathSuccess")],
                })
            }
            StatusCode::NOT_FOUND => Ok(GetMerklePathResult {
                name: Some("BitailsTsc".to_string()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![make_note("getMerklePathNotFound")],
            }),
            status => Ok(GetMerklePathResult {
                name: Some("BitailsTsc".to_string()),
                merkle_path: None,
                header: None,
                error: Some(format!("HTTP {}", status)),
                notes: vec![make_note("getMerklePathBadStatus")],
            }),
        }
    }

    // =========================================================================
    // Transaction Broadcasting
    // =========================================================================

    /// Broadcast multiple raw transactions.
    ///
    /// A broadcast is a write, not a read: Rule 28's test (what header,
    /// proof or index answers the question) does not apply. This explorer
    /// is a last rung of `postBeef`, behind Arcade and the ARC broadcasters.
    pub async fn post_raws(&self, raws: &[String]) -> Result<Vec<BitailsBroadcastResult>> {
        let url = format!("{}tx/broadcast/multi", self.base_url);

        let body = serde_json::json!({ "raws": raws });

        let response = self
            .client
            .post(&url)
            .headers(self.get_headers())
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                let results: Vec<BitailsBroadcastResult> = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse response: {}", e)))?;
                Ok(results)
            }
            status => Err(Error::ServiceError(format!(
                "Bitails broadcast failed with status {}",
                status
            ))),
        }
    }

    /// Broadcast a single raw transaction.
    pub async fn broadcast(&self, raw_tx: &[u8]) -> Result<PostTxResultForTxid> {
        let raw_hex = hex::encode(raw_tx);
        let txid = crate::services::traits::txid_from_raw_tx(raw_tx);

        let results = self.post_raws(std::slice::from_ref(&raw_hex)).await?;

        if results.len() != 1 {
            return Ok(PostTxResultForTxid {
                txid: txid.clone(),
                status: "error".to_string(),
                double_spend: false,
                orphan_mempool: false,
                competing_txs: None,
                data: Some(format!("Expected 1 result, got {}", results.len())),
                service_error: true,
                block_hash: None,
                block_height: None,
                notes: vec![make_note("postRawsResultCount")],
            });
        }

        let result = &results[0];

        // Verify txid matches
        if let Some(ref returned_txid) = result.txid {
            if returned_txid != &txid {
                return Ok(PostTxResultForTxid {
                    txid: txid.clone(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(format!("txid mismatch: {} != {}", returned_txid, txid)),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note("postRawsTxidMismatch")],
                });
            }
        }

        // Check for errors
        if let Some(ref error) = result.error {
            let code = &error.code;
            let message = &error.message;

            match code.as_str() {
                error_codes::ALREADY_IN_MEMPOOL => {
                    return Ok(PostTxResultForTxid {
                        txid,
                        status: "success".to_string(),
                        double_spend: false,
                        orphan_mempool: false,
                        competing_txs: None,
                        data: Some("already-in-mempool".to_string()),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawsSuccessAlreadyInMempool")],
                    });
                }
                error_codes::DOUBLE_SPEND_OR_MISSING_INPUTS => {
                    // -25 can be either double spend or missing inputs.
                    // Double-spend has "double" or "mempool conflict" in message.
                    // Missing inputs (orphan mempool) is a propagation issue, NOT double-spend.
                    let is_double_spend = message.to_lowercase().contains("double")
                        || message.to_lowercase().contains("mempool conflict");
                    let is_orphan = !is_double_spend;
                    return Ok(PostTxResultForTxid {
                        txid,
                        status: "error".to_string(),
                        double_spend: is_double_spend,
                        orphan_mempool: is_orphan,
                        competing_txs: None,
                        data: Some(format!("code={}, msg={}", code, message)),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(if is_double_spend {
                            "postRawsErrorDoubleSpend"
                        } else {
                            "postRawsErrorMissingInputs"
                        })],
                    });
                }
                _ => {
                    return Ok(PostTxResultForTxid {
                        txid,
                        status: "error".to_string(),
                        double_spend: false,
                        orphan_mempool: false,
                        competing_txs: None,
                        data: Some(format!("code={}, msg={}", code, message)),
                        service_error: true,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawsError")],
                    });
                }
            }
        }

        Ok(PostTxResultForTxid {
            txid,
            status: "success".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: None,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![make_note("postRawsSuccess")],
        })
    }

    /// Post BEEF transaction.
    ///
    /// Parses the BEEF to extract raw transactions and broadcasts each one
    /// via the raw transaction endpoint.
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        use bsv_rs::transaction::Beef;

        let mut result = PostBeefResult {
            name: "Bitails".to_string(),
            status: "success".to_string(),
            txid_results: Vec::new(),
            error: None,
            notes: vec![make_note("postBeef")],
        };

        // Parse the BEEF to extract raw transactions
        let parsed_beef = match Beef::from_binary(beef) {
            Ok(b) => b,
            Err(e) => {
                result.status = "error".to_string();
                result.error = Some(format!("Failed to parse BEEF: {}", e));
                return Ok(result);
            }
        };

        // Broadcast each requested txid
        for txid in txids {
            // Find the transaction in the BEEF
            let beef_tx = parsed_beef.find_txid(txid);
            let raw_tx = beef_tx.and_then(|btx| btx.tx()).map(|tx| tx.to_binary());

            let tx_result = match raw_tx {
                Some(tx_bytes) => {
                    // Broadcast the raw transaction
                    match self.broadcast(&tx_bytes).await {
                        Ok(broadcast_result) => broadcast_result,
                        Err(e) => {
                            let err_msg = e.to_string();
                            PostTxResultForTxid {
                                txid: txid.clone(),
                                status: "error".to_string(),
                                double_spend: false,
                                orphan_mempool: false,
                                competing_txs: None,
                                data: Some(err_msg),
                                service_error: true,
                                block_hash: None,
                                block_height: None,
                                notes: vec![make_note("postBeefBroadcastError")],
                            }
                        }
                    }
                }
                None => PostTxResultForTxid {
                    txid: txid.clone(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(format!("Transaction {} not found in BEEF", txid)),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note("postBeefTxNotFound")],
                },
            };

            if tx_result.status != "success" {
                result.status = "error".to_string();
            }
            result.txid_results.push(tx_result);
        }

        Ok(result)
    }

    // =========================================================================
    // Block Headers
    // =========================================================================

    /// Get block header by hash.
    ///
    /// Break-glass (Rule 28, T4): a header by hash, the block a TSC proof
    /// names. The header service holds every header; this is asked only
    /// under `break_glass_explorer_headers`, when the header service gave
    /// none and nothing else we run holds it.
    ///
    /// The route is `block/{hash}`: the raw 80-byte header hex with the
    /// height beside it (the shape of the header service's own Bitails
    /// courier, rust-chaintracks@62cf619 `src/couriers.rs:143-205`). The
    /// bytes must hash to the hash asked for, so the answer is bound to the
    /// question; the height is Bitails' word.
    pub async fn get_block_header_by_hash(&self, hash: &str) -> Result<Option<BlockHeader>> {
        let url = format!("{}block/{}", self.base_url, hash);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let block: BitailsBlock = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse block: {}", e)))?;

                let header_bytes = hex::decode(block.header.trim()).map_err(|e| {
                    Error::ValidationError(format!("Failed to decode header hex: {}", e))
                })?;

                if header_bytes.len() != 80 {
                    return Err(Error::ValidationError(format!(
                        "Invalid header length: {}",
                        header_bytes.len()
                    )));
                }

                // Parse 80-byte header
                let mut header = parse_block_header(&header_bytes, hash)?;
                header.height = block.height;
                Ok(Some(header))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "getBlockHeader failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // UTXO Status
    // =========================================================================

    /// Get UTXO status for a script hash.
    ///
    /// Break-glass (Rule 28, T10): is an output unspent. Headers and proofs
    /// prove inclusion, never that an output is unspent, and our own
    /// outputs table knows only the spends we made, so no header, proof or
    /// index of ours answers it. This explorer is WhatsOnChain's second:
    /// `Services::get_utxo_status` takes a negative only from both.
    ///
    /// Anything but a 200 carrying an `unspent` list is "could not look"
    /// (`status: "error"`), never an empty set. A list of
    /// [`BITAILS_UNSPENT_PAGE`] entries or more that does not hold the
    /// outpoint may be one page of a longer set, so it is "could not look"
    /// too.
    ///
    /// The route (`scripthash/{hash}/unspent`) and its field names are not
    /// in the TypeScript reference, which asks WhatsOnChain alone. They are
    /// exercised here against a local fixture only; until one read of the
    /// live service confirms them, a mismatch shows as "could not look" and
    /// no negative is ever confirmed, which is the safe side.
    pub async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult> {
        let could_not_look = |error: String| GetUtxoStatusResult {
            name: "Bitails".to_string(),
            status: "error".to_string(),
            is_utxo: None,
            details: Vec::new(),
            error: Some(error),
        };

        // Convert output to script hash BE format
        let script_hash = crate::services::traits::convert_script_hash(output, output_format)?;

        let url = format!("{}scripthash/{}/unspent", self.base_url, script_hash);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        if response.status() != StatusCode::OK {
            return Ok(could_not_look(format!("HTTP {}", response.status())));
        }

        let data: BitailsUnspentResponse = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse unspent set: {}", e)))?;

        let details: Vec<UtxoDetail> = data
            .unspent
            .iter()
            .map(|u| UtxoDetail {
                txid: u.txid.clone(),
                index: u.vout,
                satoshis: u.satoshis,
                height: u.blockheight,
            })
            .collect();

        // Check if specific outpoint is a UTXO ("txid.vout")
        let is_utxo = match outpoint.and_then(|o| o.split_once('.')) {
            Some((op_txid, op_vout)) => {
                let op_vout: u32 = op_vout.parse().unwrap_or(u32::MAX);
                details
                    .iter()
                    .any(|d| d.txid == op_txid && d.index == op_vout)
            }
            None => !details.is_empty(),
        };

        if !is_utxo && details.len() >= BITAILS_UNSPENT_PAGE {
            return Ok(could_not_look(format!(
                "the unspent list holds {} entries and may be one page of more",
                details.len()
            )));
        }

        Ok(GetUtxoStatusResult {
            name: "Bitails".to_string(),
            status: "success".to_string(),
            is_utxo: Some(is_utxo),
            details,
            error: None,
        })
    }

    // =========================================================================
    // Script Hash History
    // =========================================================================

    /// Get transaction history for a script hash.
    ///
    /// Break-glass (Rule 28, T14): a chain scan, every transaction that
    /// touched a script. No header, proof or index of ours answers it and
    /// no wallet path needs it; built only under `break-glass-script-history`.
    #[cfg(feature = "break-glass-script-history")]
    pub async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        validate_script_hash(hash)?;

        // Reverse hash from LE to BE
        let hash_bytes = hex::decode(hash)
            .map_err(|e| Error::InvalidArgument(format!("Invalid hash hex: {}", e)))?;
        let reversed: Vec<u8> = hash_bytes.into_iter().rev().collect();
        let hash_be = hex::encode(&reversed);

        let url = format!("{}address/scripthash/{}/history", self.base_url, hash_be);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let data: Vec<BitailsHistoryItem> = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse history: {}", e)))?;

                let history = data
                    .into_iter()
                    .map(|h| ScriptHistoryItem {
                        txid: h.txid,
                        height: h.height,
                    })
                    .collect();

                Ok(GetScriptHashHistoryResult {
                    name: "Bitails".to_string(),
                    status: "success".to_string(),
                    error: None,
                    history,
                })
            }
            StatusCode::NOT_FOUND => Ok(GetScriptHashHistoryResult {
                name: "Bitails".to_string(),
                status: "success".to_string(),
                error: None,
                history: Vec::new(),
            }),
            status => Err(Error::ServiceError(format!(
                "getScriptHashHistory failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Transaction Status
    // =========================================================================

    /// Get status for multiple transaction IDs.
    ///
    /// Break-glass (Rule 28, T12): is a transaction mined, known to the
    /// mempool, or unknown. "Mined" is a proof we hold or a broadcaster
    /// pushes; "known to the mempool" and "unknown" have no header or proof
    /// answer, so the explorers are asked, each the other's fallback.
    ///
    /// The tip is not read here (T15): the header service holds it. A
    /// transaction this index places in a block is reported at depth 1, "in
    /// a block by this index's word"; the proof, checked against the header
    /// service's root, is what says mined.
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        let mut results = Vec::new();

        for txid in txids {
            match self.get_tx_info(txid).await? {
                Some(info) => {
                    let (status, depth) = if info.block_height.is_some() {
                        ("mined".to_string(), Some(1))
                    } else {
                        ("known".to_string(), Some(0))
                    };

                    results.push(TxStatusDetail {
                        txid: txid.clone(),
                        status,
                        depth,
                        ..Default::default()
                    });
                }
                None => {
                    results.push(TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
                        ..Default::default()
                    });
                }
            }
        }

        Ok(GetStatusForTxidsResult {
            name: "Bitails".to_string(),
            status: "success".to_string(),
            error: None,
            results,
        })
    }

    /// Get transaction info.
    ///
    /// The request behind `get_status_for_txids` (Rule 28, T12; the
    /// break-glass reason is written there).
    async fn get_tx_info(&self, txid: &str) -> Result<Option<BitailsTxInfo>> {
        let url = format!("{}tx/{}", self.base_url, txid);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let data: BitailsTxInfo = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse tx info: {}", e)))?;
                Ok(Some(data))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "getTxInfo failed with status {}",
                status
            ))),
        }
    }
}

// =============================================================================
// API Response Types
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
struct BitailsTscProof {
    index: u32,
    #[serde(rename = "txOrId")]
    tx_or_id: String,
    target: String,
    nodes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct BitailsBroadcastResult {
    txid: Option<String>,
    error: Option<BitailsBroadcastError>,
}

#[derive(Debug, Deserialize)]
struct BitailsBroadcastError {
    code: String,
    message: String,
}

#[cfg(feature = "break-glass-script-history")]
#[derive(Debug, Deserialize)]
struct BitailsHistoryItem {
    txid: String,
    height: Option<u32>,
}

/// A block by hash: the raw 80-byte header hex and the height.
#[derive(Debug, Deserialize)]
struct BitailsBlock {
    height: u32,
    header: String,
}

/// The unspent set of a script hash. `unspent` is required: a body without
/// it is a parse fault ("could not look"), never an empty set.
#[derive(Debug, Deserialize)]
struct BitailsUnspentResponse {
    unspent: Vec<BitailsUnspentItem>,
}

#[derive(Debug, Deserialize)]
struct BitailsUnspentItem {
    txid: String,
    vout: u32,
    satoshis: u64,
    blockheight: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BitailsTxInfo {
    txid: String,
    #[serde(rename = "blockHash")]
    block_hash: Option<String>,
    #[serde(rename = "blockHeight")]
    block_height: Option<u32>,
}

// =============================================================================
// Helper Functions
// =============================================================================

fn make_note(what: &str) -> HashMap<String, serde_json::Value> {
    let mut note = HashMap::new();
    note.insert(
        "what".to_string(),
        serde_json::Value::String(what.to_string()),
    );
    note.insert(
        "name".to_string(),
        serde_json::Value::String("Bitails".to_string()),
    );
    note.insert(
        "when".to_string(),
        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
    );
    note
}

/// Parse 80-byte block header.
/// Parse an 80-byte header, bound to the block hash it was asked for by:
/// the double SHA-256 of the bytes, reversed, must be `hash`.
fn parse_block_header(data: &[u8], hash: &str) -> Result<BlockHeader> {
    if data.len() != 80 {
        return Err(Error::ValidationError(format!(
            "Invalid header length: {}",
            data.len()
        )));
    }

    let mut computed = sha256(&sha256(data));
    computed.reverse();
    let computed = hex::encode(computed);
    if !computed.eq_ignore_ascii_case(hash) {
        return Err(Error::ValidationError(format!(
            "header bytes hash to {}, not the block hash asked for ({})",
            computed, hash
        )));
    }

    let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let previous_hash = hex::encode(data[4..36].iter().rev().copied().collect::<Vec<u8>>());
    let merkle_root = hex::encode(data[36..68].iter().rev().copied().collect::<Vec<u8>>());
    let time = u32::from_le_bytes([data[68], data[69], data[70], data[71]]);
    let bits = u32::from_le_bytes([data[72], data[73], data[74], data[75]]);
    let nonce = u32::from_le_bytes([data[76], data[77], data[78], data[79]]);

    Ok(BlockHeader {
        version,
        previous_hash,
        merkle_root,
        time,
        bits,
        nonce,
        hash: hash.to_string(),
        height: 0, // Not in the 80 bytes; the caller sets it
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitails_url_construction() {
        let bitails = Bitails::new(Chain::Main, BitailsConfig::default()).unwrap();
        assert_eq!(bitails.base_url, BITAILS_MAINNET_URL);

        let bitails = Bitails::new(Chain::Test, BitailsConfig::default()).unwrap();
        assert_eq!(bitails.base_url, BITAILS_TESTNET_URL);
    }

    #[test]
    fn test_config_with_api_key() {
        let config = BitailsConfig::with_api_key("test-key");
        assert_eq!(config.api_key, Some("test-key".to_string()));
    }

    #[test]
    fn test_parse_block_header() {
        // Genesis block header (mainnet)
        let header_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let header_bytes = hex::decode(header_hex).unwrap();
        let hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

        let header = parse_block_header(&header_bytes, hash).unwrap();

        assert_eq!(header.version, 1);
        assert_eq!(header.nonce, 2083236893);
        assert_eq!(header.hash, hash);
    }
}
