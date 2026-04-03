//! ARC (mAPI) service provider.
//!
//! Provides transaction broadcasting via ARC API:
//! - TAAL mainnet: `https://arc.taal.com`
//! - TAAL testnet: `https://arc-test.taal.com`
//! - GorillaPool: `https://arc.gorillapool.io`
//!
//! Supports BEEF format transaction broadcasting with callbacks for
//! proof delivery and double-spend notifications.

use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::services::traits::{GetMerklePathResult, PostBeefResult, PostTxResultForTxid};
use crate::{Error, Result};

/// TAAL ARC mainnet URL.
pub const ARC_TAAL_MAINNET: &str = "https://arc.taal.com";

/// TAAL ARC testnet URL.
pub const ARC_TAAL_TESTNET: &str = "https://arc-test.taal.com";

/// GorillaPool ARC URL.
pub const ARC_GORILLAPOOL: &str = "https://arc.gorillapool.io";

/// Custom ARC HTTP status codes.
pub mod status_codes {
    /// Transaction not in extended format.
    pub const NOT_EXTENDED_FORMAT: u16 = 460;
    /// Fee too low.
    pub const FEE_TOO_LOW: u16 = 465;
    /// Cumulative fee validation failed.
    pub const CUMULATIVE_FEE_FAILED: u16 = 473;
}

/// Configuration for ARC provider.
#[derive(Debug, Clone, Default)]
pub struct ArcConfig {
    /// API key/token for authentication.
    pub api_key: Option<String>,

    /// Deployment ID for request tracking.
    pub deployment_id: Option<String>,

    /// Callback URL for proof/double-spend notifications.
    pub callback_url: Option<String>,

    /// Authentication token for callback endpoint.
    pub callback_token: Option<String>,

    /// Wait-for header value (e.g., "SEEN_ON_NETWORK").
    pub wait_for: Option<String>,

    /// Additional headers to include.
    pub headers: Option<HashMap<String, String>>,

    /// Request timeout in seconds.
    pub timeout_secs: Option<u64>,
}

impl ArcConfig {
    /// Create config with API key.
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            api_key: Some(api_key.into()),
            ..Default::default()
        }
    }

    /// Set callback URL for notifications.
    pub fn with_callback(mut self, url: impl Into<String>, token: Option<String>) -> Self {
        self.callback_url = Some(url.into());
        self.callback_token = token;
        self
    }

    /// Set deployment ID.
    pub fn with_deployment_id(mut self, id: impl Into<String>) -> Self {
        self.deployment_id = Some(id.into());
        self
    }
}

/// ARC service provider.
pub struct Arc {
    client: Client,
    name: String,
    url: String,
    api_key: Option<String>,
    deployment_id: String,
    callback_url: Option<String>,
    callback_token: Option<String>,
    wait_for: Option<String>,
    additional_headers: Option<HashMap<String, String>>,
}

impl Arc {
    /// Create a new ARC provider.
    pub fn new(
        url: impl Into<String>,
        config: Option<ArcConfig>,
        name: Option<&str>,
    ) -> Result<Self> {
        let url = url.into();
        let config = config.unwrap_or_default();

        let timeout = config.timeout_secs.unwrap_or(30);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;

        let deployment_id = config
            .deployment_id
            .unwrap_or_else(|| format!("rust-wallet-toolbox-{}", Uuid::new_v4()));

        Ok(Self {
            client,
            name: name.unwrap_or("ARC").to_string(),
            url,
            api_key: config.api_key,
            deployment_id,
            callback_url: config.callback_url,
            callback_token: config.callback_token,
            wait_for: config.wait_for,
            additional_headers: config.headers,
        })
    }

    /// Create TAAL mainnet provider.
    pub fn taal_mainnet(config: Option<ArcConfig>) -> Result<Self> {
        Self::new(ARC_TAAL_MAINNET, config, Some("arcTaal"))
    }

    /// Create TAAL testnet provider.
    pub fn taal_testnet(config: Option<ArcConfig>) -> Result<Self> {
        Self::new(ARC_TAAL_TESTNET, config, Some("arcTaalTest"))
    }

    /// Create GorillaPool provider.
    pub fn gorillapool(config: Option<ArcConfig>) -> Result<Self> {
        Self::new(ARC_GORILLAPOOL, config, Some("arcGorillaPool"))
    }

    /// Get the provider name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get request headers.
    fn get_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Accept", "application/json".parse().unwrap());
        headers.insert("XDeployment-ID", self.deployment_id.parse().unwrap());

        if let Some(ref api_key) = self.api_key {
            if !api_key.is_empty() {
                headers.insert(
                    "Authorization",
                    format!("Bearer {}", api_key).parse().unwrap(),
                );
            }
        }

        if let Some(ref url) = self.callback_url {
            headers.insert("X-CallbackUrl", url.parse().unwrap());
        }

        if let Some(ref token) = self.callback_token {
            headers.insert("X-CallbackToken", token.parse().unwrap());
        }

        if let Some(ref wait_for) = self.wait_for {
            headers.insert("X-WaitFor", wait_for.parse().unwrap());
        }

        if let Some(ref additional) = self.additional_headers {
            for (key, value) in additional {
                if let (Ok(name), Ok(val)) = (
                    reqwest::header::HeaderName::try_from(key.as_str()),
                    reqwest::header::HeaderValue::from_str(value),
                ) {
                    headers.insert(name, val);
                }
            }
        }

        headers
    }

    // =========================================================================
    // Transaction Broadcasting
    // =========================================================================

    /// Post a raw transaction (can be raw, EF, or BEEF v1 format).
    pub async fn post_raw_tx(
        &self,
        raw_tx_hex: &str,
        txids: Option<&[String]>,
    ) -> Result<PostTxResultForTxid> {
        let url = format!("{}/v1/tx", self.url);

        // Determine txid - use last provided txid or compute from raw
        let txid = if let Some(ids) = txids {
            ids.last()
                .cloned()
                .unwrap_or_else(|| compute_txid_from_hex(raw_tx_hex))
        } else {
            compute_txid_from_hex(raw_tx_hex)
        };

        let body = serde_json::json!({ "rawTx": raw_tx_hex });

        let response = self
            .client
            .post(&url)
            .headers(self.get_headers())
            .timeout(Duration::from_secs(30))
            .json(&body)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let data: ArcResponse = resp.json().await.map_err(|e| {
                    Error::ServiceError(format!("Failed to parse ARC response: {}", e))
                })?;

                tracing::debug!(
                    name = %self.name,
                    tx_status = %data.tx_status,
                    txid = %data.txid,
                    extra_info = ?data.extra_info,
                    competing_txs = ?data.competing_txs,
                    hex_len = raw_tx_hex.len(),
                    "ARC response"
                );

                let is_double_spend = data.tx_status == "DOUBLE_SPEND_ATTEMPTED"
                    || data.tx_status == "SEEN_IN_ORPHAN_MEMPOOL";

                if is_double_spend {
                    Ok(PostTxResultForTxid {
                        txid: data.txid,
                        status: "error".to_string(),
                        double_spend: true,
                        competing_txs: data.competing_txs,
                        data: Some(format!(
                            "{} {}",
                            data.tx_status,
                            data.extra_info.unwrap_or_default()
                        )),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(&self.name, "postRawTxDoubleSpend")],
                    })
                } else {
                    Ok(PostTxResultForTxid {
                        txid: data.txid,
                        status: "success".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: Some(format!(
                            "{} {}",
                            data.tx_status,
                            data.extra_info.unwrap_or_default()
                        )),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(&self.name, "postRawTxSuccess")],
                    })
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                let error_msg = match status.as_u16() {
                    status_codes::NOT_EXTENDED_FORMAT => {
                        "ARC expects transaction in extended format".to_string()
                    }
                    status_codes::FEE_TOO_LOW | status_codes::CUMULATIVE_FEE_FAILED => {
                        "ARC rejected transaction: fee too low".to_string()
                    }
                    401 | 403 => "ARC: unauthorized".to_string(),
                    _ => format!("ARC error: HTTP {} - {}", status, body),
                };

                Ok(PostTxResultForTxid {
                    txid,
                    status: "error".to_string(),
                    double_spend: false,
                    competing_txs: None,
                    data: Some(error_msg),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postRawTxError")],
                })
            }
            Err(e) => Ok(PostTxResultForTxid {
                txid,
                status: "error".to_string(),
                double_spend: false,
                competing_txs: None,
                data: Some(format!("Request failed: {}", e)),
                service_error: true,
                block_hash: None,
                block_height: None,
                notes: vec![make_note(&self.name, "postRawTxCatch")],
            }),
        }
    }

    /// Post BEEF transaction.
    ///
    /// ARC accepts BEEF v1 format. If the beef is v2 and can be downgraded,
    /// it will be converted automatically.
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        let mut result = PostBeefResult {
            name: self.name.clone(),
            status: "success".to_string(),
            txid_results: Vec::new(),
            error: None,
            notes: Vec::new(),
        };

        // Strategy: use EF (Extended Format) when all BEEF ancestors are proven
        // (have merkle proofs), which avoids ARC BEEF parsing bugs. But when the
        // BEEF contains unproven ancestors (e.g., internalized txs not yet on-chain,
        // or deep unconfirmed chains), send the full BEEF so ARC can process the
        // entire ancestor chain. EF only embeds direct parent data — it cannot
        // convey multi-level unproven ancestry.
        let post_result = {
            use bsv_rs::transaction::Beef;
            match Beef::from_binary(beef) {
                Ok(beef_parsed) => {
                    // Extract the new (unproven) transaction data upfront so we
                    // don't hold borrows across await points.
                    let new_tx_cloned = beef_parsed
                        .txs
                        .iter()
                        .rev()
                        .find(|btx| btx.bump_index().is_none() && !btx.is_txid_only())
                        .and_then(|btx| btx.tx().cloned());
                    let new_txid = new_tx_cloned.as_ref().map(|tx| tx.id());

                    let unproven_ancestor_count = beef_parsed
                        .txs
                        .iter()
                        .filter(|btx| {
                            btx.bump_index().is_none()
                                && !btx.is_txid_only()
                                && Some(btx.txid()) != new_txid
                        })
                        .count();

                    // Send full BEEF when there are unproven ancestors, up to
                    // a reasonable limit. ARC chokes on very large BEEFs (500+ txs)
                    // but handles moderate ones fine. Full BEEF is essential for:
                    // - Phantom UTXOs (internalized tx never broadcast)
                    // - Short unconfirmed chains (10-50 txs from rapid payments)
                    // For extreme chains (>100 unproven), skip to EF and hope
                    // the direct parents are already in the mempool.
                    let use_full_beef = unproven_ancestor_count > 0
                        && unproven_ancestor_count <= 100;

                    if use_full_beef {
                        tracing::debug!(
                            name = %self.name,
                            unproven_ancestors = unproven_ancestor_count,
                            total_txs = beef_parsed.txs.len(),
                            "BEEF has unproven ancestors — trying full BEEF first"
                        );
                        result.notes.push(make_note(&self.name, "postBeefFull"));
                        let beef_hex = hex::encode(beef);
                        let beef_result =
                            self.post_raw_tx(&beef_hex, Some(txids)).await?;

                        if beef_result.status == "success"
                            || beef_result.double_spend
                            || beef_result
                                .data
                                .as_ref()
                                .map(|d| d.contains("already"))
                                .unwrap_or(false)
                        {
                            beef_result
                        } else {
                            // Full BEEF rejected — fall back to EF (works when
                            // the direct parent is already in the mempool).
                            tracing::info!(
                                name = %self.name,
                                beef_error = ?beef_result.data,
                                "Full BEEF failed — falling back to EF"
                            );
                            result.notes.push(make_note(&self.name, "postBeefFallbackEF"));
                            match new_tx_cloned.clone() {
                                Some(mut fallback_tx) => {
                                    let mut hydrated = true;
                                    for input in &mut fallback_tx.inputs {
                                        if let Ok(parent_txid) =
                                            input.get_source_txid()
                                        {
                                            if let Some(parent_btx) =
                                                beef_parsed.find_txid(&parent_txid)
                                            {
                                                if let Some(parent_tx) = parent_btx.tx()
                                                {
                                                    input.source_transaction = Some(
                                                        Box::new(parent_tx.clone()),
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        hydrated = false;
                                        break;
                                    }
                                    if hydrated {
                                        match fallback_tx.to_hex_ef() {
                                            Ok(ef_hex) => {
                                                self.post_raw_tx(&ef_hex, Some(txids))
                                                    .await?
                                            }
                                            Err(_) => beef_result,
                                        }
                                    } else {
                                        beef_result
                                    }
                                }
                                None => beef_result,
                            }
                        }
                    } else {
                        // All ancestors are proven — safe to use EF.
                        // EF embeds parent UTXO data inline for script validation.
                        match new_tx_cloned {
                            Some(mut new_tx) => {
                                let mut hydrated = true;
                                for input in &mut new_tx.inputs {
                                    if let Ok(parent_txid) = input.get_source_txid() {
                                        if let Some(parent_btx) =
                                            beef_parsed.find_txid(&parent_txid)
                                        {
                                            if let Some(parent_tx) = parent_btx.tx() {
                                                input.source_transaction =
                                                    Some(Box::new(parent_tx.clone()));
                                                continue;
                                            }
                                        }
                                    }
                                    hydrated = false;
                                    break;
                                }

                                if hydrated {
                                    match new_tx.to_hex_ef() {
                                        Ok(ef_hex) => {
                                            tracing::debug!(
                                                name = %self.name,
                                                ef_len = ef_hex.len(),
                                                num_inputs = new_tx.inputs.len(),
                                                "Posting as EF (all ancestors proven)"
                                            );
                                            result
                                                .notes
                                                .push(make_note(&self.name, "postBeefAsEF"));
                                            self.post_raw_tx(&ef_hex, Some(txids)).await?
                                        }
                                        Err(e) => {
                                            tracing::warn!(name = %self.name, error = %e, "EF serialization failed — falling back to BEEF");
                                            let beef_hex = hex::encode(beef);
                                            self.post_raw_tx(&beef_hex, Some(txids)).await?
                                        }
                                    }
                                } else {
                                    tracing::warn!(name = %self.name, "Hydration failed — falling back to BEEF");
                                    let beef_hex = hex::encode(beef);
                                    self.post_raw_tx(&beef_hex, Some(txids)).await?
                                }
                            }
                            None => {
                                let beef_hex = hex::encode(beef);
                                self.post_raw_tx(&beef_hex, Some(txids)).await?
                            }
                        }
                    }
                }
                Err(_) => {
                    // Can't parse BEEF — send as-is
                    let beef_hex = hex::encode(beef);
                    self.post_raw_tx(&beef_hex, Some(txids)).await?
                }
            }
        };

        result.status = post_result.status.clone();
        result.txid_results.push(post_result.clone());

        // For additional txids, query their status
        for txid in txids.iter().skip(1) {
            if post_result.txid == *txid {
                continue;
            }

            match self.get_tx_data(txid).await {
                Ok(Some(data)) => {
                    let status = if data.tx_status == "SEEN_ON_NETWORK"
                        || data.tx_status == "STORED"
                        || data.tx_status == "MINED"
                    {
                        "success"
                    } else {
                        result.status = "error".to_string();
                        "error"
                    };

                    result.txid_results.push(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: status.to_string(),
                        double_spend: data.tx_status == "DOUBLE_SPEND_ATTEMPTED",
                        competing_txs: data.competing_txs,
                        data: Some(data.tx_status),
                        service_error: false,
                        block_hash: data.block_hash,
                        block_height: data.block_height,
                        notes: vec![make_note(&self.name, "postBeefGetTxDataSuccess")],
                    });
                }
                Ok(None) => {
                    result.status = "error".to_string();
                    result.txid_results.push(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: "error".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: Some("Transaction not found".to_string()),
                        service_error: true,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(&self.name, "postBeefGetTxDataNotFound")],
                    });
                }
                Err(e) => {
                    result.status = "error".to_string();
                    result.txid_results.push(PostTxResultForTxid {
                        txid: txid.clone(),
                        status: "error".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: Some(format!("Query failed: {}", e)),
                        service_error: true,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(&self.name, "postBeefGetTxDataError")],
                    });
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Transaction Query
    // =========================================================================

    /// Get transaction data/status from ARC.
    ///
    /// This only works for recently submitted transactions.
    pub async fn get_tx_data(&self, txid: &str) -> Result<Option<ArcTxInfo>> {
        let url = format!("{}/v1/tx/{}", self.url, txid);

        let response = self
            .client
            .get(&url)
            .headers(self.get_headers())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            StatusCode::OK => {
                let data: ArcTxInfo = response
                    .json()
                    .await
                    .map_err(|e| Error::ServiceError(format!("Failed to parse response: {}", e)))?;
                Ok(Some(data))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "ARC getTxData failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Merkle Path
    // =========================================================================

    /// Get merkle path from ARC (if available).
    ///
    /// ARC returns merkle paths for mined transactions that it knows about.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        match self.get_tx_data(txid).await? {
            Some(data) if !data.merkle_path.is_empty() => Ok(GetMerklePathResult {
                name: Some(self.name.clone()),
                merkle_path: Some(data.merkle_path),
                header: None,
                error: None,
                notes: vec![make_note(&self.name, "getMerklePathSuccess")],
            }),
            Some(_) => Ok(GetMerklePathResult {
                name: Some(self.name.clone()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![make_note(&self.name, "getMerklePathNoPath")],
            }),
            None => Ok(GetMerklePathResult {
                name: Some(self.name.clone()),
                merkle_path: None,
                header: None,
                error: None,
                notes: vec![make_note(&self.name, "getMerklePathNotFound")],
            }),
        }
    }
}

// =============================================================================
// API Response Types
// =============================================================================

/// ARC broadcast response.
#[derive(Debug, Deserialize)]
struct ArcResponse {
    txid: String,
    #[serde(rename = "extraInfo")]
    extra_info: Option<String>,
    #[serde(rename = "txStatus")]
    tx_status: String,
    #[serde(rename = "competingTxs")]
    competing_txs: Option<Vec<String>>,
}

/// ARC transaction info response.
#[derive(Debug, Clone, Deserialize)]
pub struct ArcTxInfo {
    /// HTTP status code.
    pub status: Option<u16>,

    /// Status title.
    pub title: Option<String>,

    /// Block hash if mined.
    #[serde(rename = "blockHash")]
    pub block_hash: Option<String>,

    /// Block height if mined.
    #[serde(rename = "blockHeight")]
    pub block_height: Option<u32>,

    /// Competing transactions.
    #[serde(rename = "competingTxs")]
    pub competing_txs: Option<Vec<String>>,

    /// Additional info.
    #[serde(rename = "extraInfo")]
    pub extra_info: Option<String>,

    /// Merkle path (BUMP format hex).
    #[serde(rename = "merklePath", default)]
    pub merkle_path: String,

    /// Timestamp.
    pub timestamp: Option<String>,

    /// Transaction ID.
    pub txid: String,

    /// Transaction status.
    #[serde(rename = "txStatus")]
    pub tx_status: String,
}

/// ARC API error response.
#[derive(Debug, Deserialize)]
pub struct ArcApiError {
    /// Error type.
    #[serde(rename = "type")]
    pub error_type: Option<String>,

    /// Error title.
    pub title: Option<String>,

    /// HTTP status.
    pub status: Option<u16>,

    /// Error detail.
    pub detail: Option<String>,

    /// Instance identifier.
    pub instance: Option<String>,

    /// Transaction ID.
    pub txid: Option<String>,

    /// Extra info.
    #[serde(rename = "extraInfo")]
    pub extra_info: Option<String>,
}

// =============================================================================
// Helper Functions
// =============================================================================

fn make_note(provider: &str, what: &str) -> HashMap<String, serde_json::Value> {
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
    note
}

fn compute_txid_from_hex(hex_str: &str) -> String {
    if let Ok(bytes) = hex::decode(hex_str) {
        crate::services::traits::txid_from_raw_tx(&bytes)
    } else {
        "invalid".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arc_url_construction() {
        let arc = Arc::taal_mainnet(None).unwrap();
        assert!(arc.url.contains("taal.com"));
        assert_eq!(arc.name, "arcTaal");

        let arc = Arc::gorillapool(None).unwrap();
        assert!(arc.url.contains("gorillapool.io"));
        assert_eq!(arc.name, "arcGorillaPool");
    }

    #[test]
    fn test_config_with_api_key() {
        let config = ArcConfig::with_api_key("test-key");
        assert_eq!(config.api_key, Some("test-key".to_string()));
    }

    #[test]
    fn test_config_with_callback() {
        let config = ArcConfig::default()
            .with_callback("https://example.com/callback", Some("secret".to_string()));
        assert_eq!(
            config.callback_url,
            Some("https://example.com/callback".to_string())
        );
        assert_eq!(config.callback_token, Some("secret".to_string()));
    }
}
