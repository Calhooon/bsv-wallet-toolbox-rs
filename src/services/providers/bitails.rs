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
use std::sync::RwLock;
use std::time::Duration;

use crate::chaintracks::Chain;
use crate::services::traits::{
    validate_script_hash, validate_txid, BlockHeader, GetMerklePathResult, GetRawTxResult,
    GetScriptHashHistoryResult, GetStatusForTxidsResult, PostBeefResult, PostTxResultForTxid,
    ScriptHistoryItem, TxStatusDetail,
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

/// Bitails service provider.
pub struct Bitails {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    chain: Chain,
    api_key: Option<String>,
    #[allow(dead_code)]
    root_cache: RwLock<HashMap<u32, String>>,
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
            root_cache: RwLock::new(HashMap::new()),
        })
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
                })
            }
            StatusCode::NOT_FOUND => Ok(GetRawTxResult {
                name: "Bitails".to_string(),
                txid: txid.to_string(),
                raw_tx: None,
                error: None,
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

        let results = self.post_raws(&[raw_hex.clone()]).await?;

        if results.len() != 1 {
            return Ok(PostTxResultForTxid {
                txid: txid.clone(),
                status: "error".to_string(),
                double_spend: false,
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
                        competing_txs: None,
                        data: Some("already-in-mempool".to_string()),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawsSuccessAlreadyInMempool")],
                    });
                }
                error_codes::DOUBLE_SPEND_OR_MISSING_INPUTS => {
                    // -25 can be either double spend or missing inputs
                    // Check message for more context
                    let is_double_spend = message.to_lowercase().contains("double")
                        || message.to_lowercase().contains("mempool conflict");
                    return Ok(PostTxResultForTxid {
                        txid,
                        status: "error".to_string(),
                        double_spend: is_double_spend,
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

    /// Get current chain height.
    pub async fn get_current_height(&self) -> Result<u32> {
        let url = format!("{}network/info", self.base_url);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "getCurrentHeight failed with status {}",
                response.status()
            )));
        }

        let data: BitailsNetworkInfo = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse network info: {}", e)))?;

        Ok(data.blocks)
    }

    /// Get block header by hash.
    pub async fn get_block_header_by_hash(&self, hash: &str) -> Result<Option<BlockHeader>> {
        let url = format!("{}block/{}/header/raw", self.base_url, hash);

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

                let header_bytes = hex::decode(hex_str.trim()).map_err(|e| {
                    Error::ValidationError(format!("Failed to decode header hex: {}", e))
                })?;

                if header_bytes.len() != 80 {
                    return Err(Error::ValidationError(format!(
                        "Invalid header length: {}",
                        header_bytes.len()
                    )));
                }

                // Parse 80-byte header
                let header = parse_block_header(&header_bytes, hash)?;
                Ok(Some(header))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "getBlockHeader failed with status {}",
                status
            ))),
        }
    }

    /// Get latest block info.
    pub async fn get_latest_block(&self) -> Result<(String, u32)> {
        let url = format!("{}block/latest", self.base_url);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "getLatestBlock failed with status {}",
                response.status()
            )));
        }

        let data: BitailsBlockInfo = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse block info: {}", e)))?;

        Ok((data.hash, data.height))
    }

    // =========================================================================
    // Script Hash History
    // =========================================================================

    /// Get transaction history for a script hash.
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
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        let tip_height = self.get_current_height().await?;

        let mut results = Vec::new();

        for txid in txids {
            match self.get_tx_info(txid).await? {
                Some(info) => {
                    let (status, depth) = if let Some(height) = info.block_height {
                        let depth = if tip_height >= height {
                            (tip_height - height) + 1
                        } else {
                            0
                        };
                        ("mined".to_string(), Some(depth))
                    } else {
                        ("known".to_string(), Some(0))
                    };

                    results.push(TxStatusDetail {
                        txid: txid.clone(),
                        status,
                        depth,
                    });
                }
                None => {
                    results.push(TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
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

    // =========================================================================
    // Additional Height / Header Methods
    // =========================================================================

    /// Get current blockchain height.
    ///
    /// Uses the chain info endpoint to retrieve the current block count.
    pub async fn current_height(&self) -> Result<u32> {
        let url = format!("{}network/info", self.base_url);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Bitails chain_info: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Bitails chain_info: HTTP {}",
                response.status()
            )));
        }

        let data: BitailsNetworkInfo = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Bitails parse: {}", e)))?;

        Ok(data.blocks)
    }

    /// Get block header by height.
    pub async fn get_header_by_height(&self, height: u32) -> Result<BlockHeader> {
        let url = format!("{}block/header/{}", self.base_url, height);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Bitails header: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Bitails header: HTTP {}",
                response.status()
            )));
        }

        response
            .json::<BlockHeader>()
            .await
            .map_err(|e| Error::ServiceError(format!("Bitails header parse: {}", e)))
    }

    /// Validate merkle root for height (lookup header and compare).
    pub async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool> {
        let header = self.get_header_by_height(height).await?;
        Ok(header.merkle_root == root)
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BitailsNetworkInfo {
    blocks: u32,
    headers: Option<u32>,
    bestblockhash: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BitailsBlockInfo {
    hash: String,
    height: u32,
    time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BitailsHistoryItem {
    txid: String,
    height: Option<u32>,
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
fn parse_block_header(data: &[u8], hash: &str) -> Result<BlockHeader> {
    if data.len() != 80 {
        return Err(Error::ValidationError(format!(
            "Invalid header length: {}",
            data.len()
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
        height: 0, // Height not available from raw header
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
