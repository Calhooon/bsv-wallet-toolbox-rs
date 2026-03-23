//! WhatsOnChain service provider.
//!
//! Provides access to WhatsOnChain API for:
//! - Raw transaction retrieval
//! - Merkle proof retrieval (TSC format)
//! - UTXO status checking
//! - Script hash history
//! - Transaction broadcasting
//! - Exchange rate information
//!
//! # API Endpoints
//!
//! - Mainnet: `https://api.whatsonchain.com/v1/bsv/main`
//! - Testnet: `https://api.whatsonchain.com/v1/bsv/test`

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use tokio::time::sleep;

use crate::chaintracks::Chain;
use crate::lock_utils::{lock_read, lock_write};
use crate::services::traits::{
    validate_txid, BlockHeader, BsvExchangeRate, GetMerklePathResult, GetRawTxResult,
    GetScriptHashHistoryResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat,
    GetUtxoStatusResult, PostBeefResult, PostTxResultForTxid, ScriptHistoryItem, TxStatusDetail,
    UtxoDetail,
};
use crate::{Error, Result};

/// Base URL for WhatsOnChain mainnet API.
pub const WOC_MAINNET_URL: &str = "https://api.whatsonchain.com/v1/bsv/main";

/// Base URL for WhatsOnChain testnet API.
pub const WOC_TESTNET_URL: &str = "https://api.whatsonchain.com/v1/bsv/test";

/// Configuration for WhatsOnChain provider.
#[derive(Debug, Clone, Default)]
pub struct WhatsOnChainConfig {
    /// API key for authenticated access (optional).
    pub api_key: Option<String>,

    /// Request timeout in seconds.
    pub timeout_secs: Option<u64>,
}

impl WhatsOnChainConfig {
    /// Create with API key.
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            api_key: Some(api_key.into()),
            timeout_secs: None,
        }
    }
}

/// WhatsOnChain service provider.
pub struct WhatsOnChain {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    chain: Chain,
    api_key: Option<String>,
    exchange_rate: RwLock<Option<BsvExchangeRate>>,
}

impl WhatsOnChain {
    /// Create a new WhatsOnChain provider.
    pub fn new(chain: Chain, config: WhatsOnChainConfig) -> Result<Self> {
        let base_url = match chain {
            Chain::Main => WOC_MAINNET_URL.to_string(),
            Chain::Test => WOC_TESTNET_URL.to_string(),
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
            exchange_rate: RwLock::new(None),
        })
    }

    /// Get HTTP headers including optional API key.
    fn get_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "application/json".parse().unwrap());

        if let Some(ref api_key) = self.api_key {
            if !api_key.is_empty() {
                headers.insert(
                    "Authorization",
                    format!("Bearer {}", api_key).parse().unwrap(),
                );
            }
        }

        headers
    }

    /// Make a GET request with retry on rate limiting.
    async fn get_with_retry(&self, url: &str) -> Result<reqwest::Response> {
        let mut retries = 0;
        loop {
            let response = self
                .client
                .get(url)
                .headers(self.get_headers())
                .send()
                .await
                .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

            if response.status() == StatusCode::TOO_MANY_REQUESTS && retries < 2 {
                retries += 1;
                sleep(Duration::from_secs(2)).await;
                continue;
            }

            return Ok(response);
        }
    }

    // =========================================================================
    // Raw Transaction
    // =========================================================================

    /// Get raw transaction by txid.
    pub async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        let url = format!("{}/tx/{}/hex", self.base_url, txid);

        let response = self.get_with_retry(&url).await?;

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
                    name: "WoC".to_string(),
                    txid: txid.to_string(),
                    raw_tx: Some(raw_tx),
                    error: None,
                })
            }
            StatusCode::NOT_FOUND => Ok(GetRawTxResult {
                name: "WoC".to_string(),
                txid: txid.to_string(),
                raw_tx: None,
                error: None,
            }),
            status => Err(Error::ServiceError(format!(
                "WoC getRawTx failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Merkle Path (TSC Proof)
    // =========================================================================

    /// Get merkle path proof for a transaction.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        let url = format!("{}/tx/{}/proof/tsc", self.base_url, txid);

        let response = self.get_with_retry(&url).await?;

        match response.status() {
            StatusCode::OK => {
                let body = response
                    .text()
                    .await
                    .map_err(|e| Error::NetworkError(format!("Failed to read response: {}", e)))?;

                if body.is_empty() {
                    // Unmined transaction
                    return Ok(GetMerklePathResult {
                        name: Some("WoCTsc".to_string()),
                        merkle_path: None,
                        header: None,
                        error: None,
                        notes: vec![make_note("getMerklePathNoData")],
                    });
                }

                // Parse TSC proof(s)
                let proofs: Vec<WocTscProof> = serde_json::from_str(&body).unwrap_or_else(|_| {
                    // Try single object
                    serde_json::from_str::<WocTscProof>(&body)
                        .map(|p| vec![p])
                        .unwrap_or_default()
                });

                if proofs.is_empty() {
                    return Ok(GetMerklePathResult {
                        name: Some("WoCTsc".to_string()),
                        merkle_path: None,
                        header: None,
                        error: None,
                        notes: vec![make_note("getMerklePathNoData")],
                    });
                }

                // We only handle single proof for now
                if proofs.len() != 1 {
                    return Ok(GetMerklePathResult {
                        name: Some("WoCTsc".to_string()),
                        merkle_path: None,
                        header: None,
                        error: Some("Multiple proofs not supported".to_string()),
                        notes: vec![make_note("getMerklePathMultiple")],
                    });
                }

                let proof = &proofs[0];

                // Convert TSC proof to merkle path format
                // Note: Full implementation requires fetching block header for height
                // For now, return the raw proof data
                Ok(GetMerklePathResult {
                    name: Some("WoCTsc".to_string()),
                    merkle_path: Some(serde_json::to_string(proof).unwrap_or_default()),
                    header: None, // Would need to fetch from hash_to_header
                    error: None,
                    notes: vec![make_note("getMerklePathSuccess")],
                })
            }
            StatusCode::NOT_FOUND => Ok(GetMerklePathResult {
                name: Some("WoCTsc".to_string()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![make_note("getMerklePathNotFound")],
            }),
            status => Ok(GetMerklePathResult {
                name: Some("WoCTsc".to_string()),
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

    /// Post a raw transaction.
    pub async fn post_raw_tx(&self, raw_tx_hex: &str) -> Result<PostTxResultForTxid> {
        let url = format!("{}/tx/raw", self.base_url);

        // Compute txid
        let raw_tx = hex::decode(raw_tx_hex)
            .map_err(|e| Error::InvalidArgument(format!("Invalid hex: {}", e)))?;
        let txid = crate::services::traits::txid_from_raw_tx(&raw_tx);

        let body = serde_json::json!({ "txhex": raw_tx_hex });

        let mut retries = 0;
        loop {
            let response = self
                .client
                .post(&url)
                .headers(self.get_headers())
                .header("Content-Type", "application/json")
                .header("Accept", "text/plain")
                .json(&body)
                .send()
                .await;

            match response {
                Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS && retries < 5 => {
                    retries += 1;
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
                Ok(resp) if resp.status().is_success() => {
                    return Ok(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: "success".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: None,
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawTxSuccess")],
                    });
                }
                Ok(resp) => {
                    let body = resp.text().await.unwrap_or_default();
                    let (double_spend, data) = if body.contains("mempool-conflict") {
                        (true, Some("txn-mempool-conflict".to_string()))
                    } else if body.contains("Missing inputs") {
                        (true, Some("Missing inputs".to_string()))
                    } else if body.contains("already-in-mempool")
                        || body.contains("already in the mempool")
                    {
                        return Ok(PostTxResultForTxid {
                            txid: txid.clone(),
                            status: "success".to_string(),
                            double_spend: false,
                            competing_txs: None,
                            data: Some("already-in-mempool".to_string()),
                            service_error: false,
                            block_hash: None,
                            block_height: None,
                            notes: vec![make_note("postRawTxSuccessAlreadyInMempool")],
                        });
                    } else {
                        (false, Some(body))
                    };

                    return Ok(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: "error".to_string(),
                        double_spend,
                        competing_txs: None,
                        data,
                        service_error: !double_spend,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawTxError")],
                    });
                }
                Err(e) => {
                    return Ok(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: "error".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: Some(e.to_string()),
                        service_error: true,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note("postRawTxCatch")],
                    });
                }
            }
        }
    }

    /// Post BEEF transaction (extracts raw txs and broadcasts sequentially).
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        use bsv_rs::transaction::Beef;

        let mut result = PostBeefResult {
            name: "WoC".to_string(),
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
        for (i, txid) in txids.iter().enumerate() {
            if i > 0 {
                // Give WoC time to propagate between transactions
                sleep(Duration::from_secs(1)).await;
            }

            // Find the transaction in the BEEF
            let beef_tx = parsed_beef.find_txid(txid);
            let raw_tx = beef_tx.and_then(|btx| btx.tx()).map(|tx| tx.to_binary());

            let tx_result = match raw_tx {
                Some(tx_bytes) => {
                    // Broadcast the raw transaction
                    let tx_hex = hex::encode(&tx_bytes);
                    match self.post_raw_tx(&tx_hex).await {
                        Ok(broadcast_result) => broadcast_result,
                        Err(e) => {
                            let err_msg = e.to_string();
                            let is_double_spend = err_msg.contains("txn-mempool-conflict")
                                || err_msg.contains("already in the mempool")
                                || err_msg.contains("already known");
                            PostTxResultForTxid {
                                txid: txid.clone(),
                                status: if is_double_spend { "success" } else { "error" }
                                    .to_string(),
                                double_spend: is_double_spend,
                                competing_txs: None,
                                data: Some(err_msg),
                                service_error: !is_double_spend,
                                block_hash: None,
                                block_height: None,
                                notes: vec![make_note("broadcastError")],
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
                    notes: vec![make_note("txNotInBeef")],
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
    // UTXO Status
    // =========================================================================

    /// Get UTXO status for a script hash.
    pub async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult> {
        // Convert output to script hash BE format
        let script_hash = crate::services::traits::convert_script_hash(output, output_format)?;

        let url = format!("{}/script/{}/unspent/all", self.base_url, script_hash);

        let response = self.get_with_retry(&url).await?;

        if !response.status().is_success() {
            return Ok(GetUtxoStatusResult {
                name: "WoC".to_string(),
                status: "error".to_string(),
                is_utxo: None,
                details: Vec::new(),
                error: Some(format!("HTTP {}", response.status())),
            });
        }

        let data: WocUtxoStatusResponse = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse UTXO response: {}", e)))?;

        let details: Vec<UtxoDetail> = data
            .result
            .iter()
            .map(|u| UtxoDetail {
                txid: u.tx_hash.clone(),
                index: u.tx_pos,
                satoshis: u.value,
                height: Some(u.height),
            })
            .collect();

        // Check if specific outpoint is a UTXO
        let is_utxo = if let Some(outpoint_str) = outpoint {
            // Parse outpoint "txid.vout" format
            let parts: Vec<&str> = outpoint_str.split('.').collect();
            if parts.len() == 2 {
                let op_txid = parts[0];
                let op_vout: u32 = parts[1].parse().unwrap_or(u32::MAX);
                details
                    .iter()
                    .any(|d| d.txid == op_txid && d.index == op_vout)
            } else {
                !details.is_empty()
            }
        } else {
            !details.is_empty()
        };

        Ok(GetUtxoStatusResult {
            name: "WoC".to_string(),
            status: "success".to_string(),
            is_utxo: Some(is_utxo),
            details,
            error: None,
        })
    }

    // =========================================================================
    // Transaction Status
    // =========================================================================

    /// Get status for multiple transaction IDs.
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        let url = format!("{}/txs/status", self.base_url);

        let body = serde_json::json!({ "txids": txids });

        let response = self
            .client
            .post(&url)
            .headers(self.get_headers())
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            return Ok(GetStatusForTxidsResult {
                name: "WoC".to_string(),
                status: "error".to_string(),
                error: Some(format!("HTTP {}", response.status())),
                results: Vec::new(),
            });
        }

        let data: Vec<WocTxStatus> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse status response: {}", e)))?;

        let results: Vec<TxStatusDetail> = txids
            .iter()
            .map(|txid| {
                let d = data.iter().find(|d| d.txid == *txid);
                match d {
                    None => TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
                    },
                    Some(d) if d.error.as_deref() == Some("unknown") => TxStatusDetail {
                        txid: txid.clone(),
                        status: "unknown".to_string(),
                        depth: None,
                    },
                    Some(d) if d.confirmations.is_none() => TxStatusDetail {
                        txid: txid.clone(),
                        status: "known".to_string(),
                        depth: Some(0),
                    },
                    Some(d) => TxStatusDetail {
                        txid: txid.clone(),
                        status: "mined".to_string(),
                        depth: d.confirmations,
                    },
                }
            })
            .collect();

        Ok(GetStatusForTxidsResult {
            name: "WoC".to_string(),
            status: "success".to_string(),
            error: None,
            results,
        })
    }

    // =========================================================================
    // Script Hash History
    // =========================================================================

    /// Get confirmed transaction history for a script hash.
    pub async fn get_script_hash_confirmed_history(
        &self,
        hash: &str,
    ) -> Result<Vec<ScriptHistoryItem>> {
        // Reverse hash from LE to BE for WoC
        let hash_bytes = hex::decode(hash)
            .map_err(|e| Error::InvalidArgument(format!("Invalid hash hex: {}", e)))?;
        let reversed: Vec<u8> = hash_bytes.into_iter().rev().collect();
        let hash_be = hex::encode(&reversed);

        let url = format!("{}/script/{}/confirmed/history", self.base_url, hash_be);

        let response = self.get_with_retry(&url).await?;

        match response.status() {
            StatusCode::OK => {
                let data: WocScriptHistoryResponse = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse history: {}", e)))?;

                Ok(data
                    .result
                    .into_iter()
                    .map(|h| ScriptHistoryItem {
                        txid: h.tx_hash,
                        height: h.height,
                    })
                    .collect())
            }
            StatusCode::NOT_FOUND => Ok(Vec::new()),
            status => Err(Error::ServiceError(format!(
                "getScriptHashHistory failed with status {}",
                status
            ))),
        }
    }

    /// Get unconfirmed transaction history for a script hash.
    pub async fn get_script_hash_unconfirmed_history(
        &self,
        hash: &str,
    ) -> Result<Vec<ScriptHistoryItem>> {
        // Reverse hash from LE to BE for WoC
        let hash_bytes = hex::decode(hash)
            .map_err(|e| Error::InvalidArgument(format!("Invalid hash hex: {}", e)))?;
        let reversed: Vec<u8> = hash_bytes.into_iter().rev().collect();
        let hash_be = hex::encode(&reversed);

        let url = format!("{}/script/{}/unconfirmed/history", self.base_url, hash_be);

        let response = self.get_with_retry(&url).await?;

        match response.status() {
            StatusCode::OK => {
                let data: WocScriptHistoryResponse = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse history: {}", e)))?;

                Ok(data
                    .result
                    .into_iter()
                    .map(|h| ScriptHistoryItem {
                        txid: h.tx_hash,
                        height: h.height,
                    })
                    .collect())
            }
            StatusCode::NOT_FOUND => Ok(Vec::new()),
            status => Err(Error::ServiceError(format!(
                "getScriptHashHistory failed with status {}",
                status
            ))),
        }
    }

    /// Get full transaction history (confirmed + unconfirmed) for a script hash.
    pub async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        let mut history = self.get_script_hash_confirmed_history(hash).await?;
        let unconfirmed = self.get_script_hash_unconfirmed_history(hash).await?;
        history.extend(unconfirmed);

        Ok(GetScriptHashHistoryResult {
            name: "WoC".to_string(),
            status: "success".to_string(),
            error: None,
            history,
        })
    }

    // =========================================================================
    // Block Headers
    // =========================================================================

    /// Get block header by hash.
    pub async fn get_block_header_by_hash(&self, hash: &str) -> Result<Option<BlockHeader>> {
        let url = format!("{}/block/{}/header", self.base_url, hash);

        let response = self.get_with_retry(&url).await?;

        match response.status() {
            StatusCode::OK => {
                let data: WocBlockHeader = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse header: {}", e)))?;

                Ok(Some(data.into_block_header()))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "getBlockHeader failed with status {}",
                status
            ))),
        }
    }

    /// Get chain info (including current height).
    pub async fn get_chain_info(&self) -> Result<WocChainInfo> {
        let url = format!("{}/chain/info", self.base_url);

        let response = self.get_with_retry(&url).await?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "getChainInfo failed with status {}",
                response.status()
            )));
        }

        response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse chain info: {}", e)))
    }

    // =========================================================================
    // Exchange Rate
    // =========================================================================

    /// Update and return BSV exchange rate.
    pub async fn update_bsv_exchange_rate(&self, update_msecs: u64) -> Result<f64> {
        // Check cached rate
        {
            let rate = lock_read(&self.exchange_rate)?;
            if let Some(ref r) = *rate {
                if !r.is_stale(update_msecs) {
                    return Ok(r.rate);
                }
            }
        }

        let url = format!("{}/exchangerate", self.base_url);

        let response = self.get_with_retry(&url).await?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "getExchangeRate failed with status {}",
                response.status()
            )));
        }

        let data: WocExchangeRate = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse exchange rate: {}", e)))?;

        if data.currency != "USD" {
            return Err(Error::ServiceError(
                "Unexpected currency in exchange rate".to_string(),
            ));
        }

        // Update cache
        {
            let mut rate = lock_write(&self.exchange_rate)?;
            *rate = Some(BsvExchangeRate::new(data.rate));
        }

        Ok(data.rate)
    }
}

// =============================================================================
// API Response Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct WocTscProof {
    index: u32,
    nodes: Vec<String>,
    target: String,
    #[serde(rename = "txOrId")]
    tx_or_id: String,
}

impl Serialize for WocTscProof {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("WocTscProof", 4)?;
        s.serialize_field("index", &self.index)?;
        s.serialize_field("nodes", &self.nodes)?;
        s.serialize_field("target", &self.target)?;
        s.serialize_field("txOrId", &self.tx_or_id)?;
        s.end()
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocUtxoStatusResponse {
    script: String,
    result: Vec<WocUtxoItem>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocUtxoItem {
    height: u32,
    tx_pos: u32,
    tx_hash: String,
    value: u64,
    #[serde(rename = "isSpentInMempoolTx")]
    is_spent_in_mempool_tx: Option<bool>,
    status: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocTxStatus {
    txid: String,
    blockhash: Option<String>,
    blockheight: Option<u32>,
    blocktime: Option<u64>,
    confirmations: Option<u32>,
    error: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocScriptHistoryResponse {
    script: Option<String>,
    result: Vec<WocScriptHistoryItem>,
    error: Option<String>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WocScriptHistoryItem {
    tx_hash: String,
    height: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocBlockHeader {
    hash: String,
    size: Option<u32>,
    height: u32,
    version: u32,
    #[serde(rename = "versionHex")]
    version_hex: Option<String>,
    merkleroot: String,
    time: u32,
    mediantime: Option<u32>,
    nonce: u32,
    bits: serde_json::Value, // Can be number or hex string
    difficulty: Option<f64>,
    chainwork: Option<String>,
    previousblockhash: Option<String>,
    nextblockhash: Option<String>,
    confirmations: Option<u32>,
    txcount: Option<u32>,
}

impl WocBlockHeader {
    fn into_block_header(self) -> BlockHeader {
        let bits = match self.bits {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0) as u32,
            serde_json::Value::String(s) => u32::from_str_radix(&s, 16).unwrap_or(0),
            _ => 0,
        };

        BlockHeader {
            version: self.version,
            previous_hash: self.previousblockhash.unwrap_or_else(|| "0".repeat(64)),
            merkle_root: self.merkleroot,
            time: self.time,
            bits,
            nonce: self.nonce,
            hash: self.hash,
            height: self.height,
        }
    }
}

/// Chain info response from WoC.
#[derive(Debug, Deserialize)]
pub struct WocChainInfo {
    pub chain: String,
    pub blocks: u32,
    pub headers: u32,
    pub bestblockhash: String,
    pub difficulty: f64,
    pub mediantime: u64,
    pub verificationprogress: f64,
    pub pruned: bool,
    pub chainwork: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WocExchangeRate {
    rate: f64,
    time: u64,
    currency: String,
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
        serde_json::Value::String("WoC".to_string()),
    );
    note.insert(
        "when".to_string(),
        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
    );
    note
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_woc_url_construction() {
        let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default()).unwrap();
        assert_eq!(woc.base_url, WOC_MAINNET_URL);

        let woc = WhatsOnChain::new(Chain::Test, WhatsOnChainConfig::default()).unwrap();
        assert_eq!(woc.base_url, WOC_TESTNET_URL);
    }

    #[test]
    fn test_config_with_api_key() {
        let config = WhatsOnChainConfig::with_api_key("test-key");
        assert_eq!(config.api_key, Some("test-key".to_string()));
    }
}
