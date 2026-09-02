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
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use uuid::Uuid;

use crate::services::providers::arcade::{
    beef_to_ef_batch_skipping, missing_parent_hint, EfBatchEntry,
};
use crate::services::traits::{
    GetMerklePathResult, PostBeefDelivery, PostBeefResult, PostTxResultForTxid,
};
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

    /// Whether an HTTP status from an ARC-family broadcaster (classic ARC,
    /// Arcade V2) is a DEFINITIVE rejection of the submitted transaction, as
    /// opposed to a transient fault of the service or of our access to it.
    ///
    /// Definitive (re-submitting the same bytes can never succeed):
    /// * `400` bad request / `422` unprocessable — the submission itself was
    ///   rejected at validation;
    /// * `460..=469` — ARC's transaction-level rejections: not extended format,
    ///   unlocking scripts, inputs, malformed, outputs, fees (465), conflicts,
    ///   BEEF validation, merkle roots;
    /// * `471..=473` — frozen (policy / consensus) and cumulative-fee failures.
    ///
    /// Transient (kept for retry through `SendWaitingTask`): `401`/`403`
    /// (our credentials), `404` (route), `408`/`429` (load), `409` (ARC's
    /// generic error, ambiguous), `413` (this provider's size limit), `5xx`.
    pub fn is_rejection(code: u16) -> bool {
        matches!(code, 400 | 422 | 460..=469 | 471..=473)
    }

    /// ARC's `469 Merkle Roots validation failed` whose body says the
    /// provider's OWN verification timed out (`timed out` / `timeout`; e.g.
    /// GorillaPool's "couldn't verify Merkle Roots ... BEEF verification
    /// timed out" after 7 s, 2026-09-01) is a provider-side timeout, not a
    /// verdict on the transaction: transient, the loop moves on and the tx
    /// is never marked invalid. Every other 469 body is definitive.
    pub fn is_transient_beef_validation_timeout(code: u16, body: &str) -> bool {
        if code != 469 {
            return false;
        }
        let lower = body.to_ascii_lowercase();
        lower.contains("timed out") || lower.contains("timeout") || lower.contains("time out")
    }
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

                Ok(self.classify_arc_response(data))
            }
            Ok(resp) => {
                let code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                Ok(self.classify_arc_http_error(txid, code, &body))
            }
            Err(e) => Ok(self.request_failed(txid, &e.to_string(), "postRawTxCatch")),
        }
    }

    /// Map a 2xx ARC body (a single submit, or one item of a `/v1/txs`
    /// batch) to a per-txid result.
    fn classify_arc_response(&self, data: ArcResponse) -> PostTxResultForTxid {
        let is_double_spend = data.tx_status == "DOUBLE_SPEND_ATTEMPTED";
        let is_orphan_mempool = data.tx_status == "SEEN_IN_ORPHAN_MEMPOOL";

        if is_double_spend {
            PostTxResultForTxid {
                txid: data.txid,
                status: "error".to_string(),
                double_spend: true,
                orphan_mempool: false,
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
            }
        } else if is_orphan_mempool {
            PostTxResultForTxid {
                txid: data.txid,
                status: "error".to_string(),
                double_spend: false,
                orphan_mempool: true,
                competing_txs: None,
                data: Some(format!(
                    "{} {}",
                    data.tx_status,
                    data.extra_info.unwrap_or_default()
                )),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![make_note(&self.name, "postRawTxOrphanMempool")],
            }
        } else {
            // Canonical @bsv/sdk + @bsv/wallet-toolbox semantics: a 200 from ARC
            // with a non-error txStatus means ARC ACCEPTED the tx (it is in ARC's
            // mempool / the network has it). DOUBLE_SPEND_ATTEMPTED and *ORPHAN*
            // are handled above as their own outcomes; EVERY other status (RECEIVED
            // / QUEUED / ANNOUNCED_TO_NETWORK / REQUESTED_BY_NETWORK / SENT_TO_NETWORK
            // / SEEN_ON_NETWORK / STORED / MINED) is SUCCESS — matching
            // ts-sdk/src/transaction/broadcasters/ARC.ts and
            // wallet-toolbox/src/services/providers/ARC.ts (which only treat
            // DOUBLE_SPEND/ORPHAN as errors).
            //
            // The previous narrowing to only SEEN_ON_NETWORK/STORED/MINED reported
            // a legitimately-accepted tx as a transient service_error, which
            // (a) tripped the full-BEEF→EF fallback below for 0-conf-ancestry BEEFs
            // (EF drops the unproven parent → ARC orphans the child → phantom tx),
            // and (b) left the tx unrecorded so a CHAINED createAction saw
            // "Insufficient funds: have 0". Cross-provider federation (broadcast to
            // every reachable ARC, not just the first to accept) is the
            // re-broadcast/monitor layer's responsibility, NOT this success classifier.
            PostTxResultForTxid {
                txid: data.txid,
                status: "success".to_string(),
                double_spend: false,
                orphan_mempool: false,
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
            }
        }
    }

    /// Map a non-2xx ARC status and body to a per-txid result: a definitive
    /// rejection for the codes in [`status_codes::is_rejection`], a transient
    /// `service_error` for everything else, including a `469` whose body says
    /// the provider's own BEEF verification timed out
    /// ([`status_codes::is_transient_beef_validation_timeout`]).
    fn classify_arc_http_error(&self, txid: String, code: u16, body: &str) -> PostTxResultForTxid {
        let status = StatusCode::from_u16(code)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| code.to_string());

        let error_msg = match code {
            status_codes::NOT_EXTENDED_FORMAT => {
                "ARC expects transaction in extended format".to_string()
            }
            status_codes::FEE_TOO_LOW | status_codes::CUMULATIVE_FEE_FAILED => {
                format!(
                    "ARC rejected transaction: fee too low (HTTP {}) - {}",
                    code, body
                )
            }
            401 | 403 => "ARC: unauthorized".to_string(),
            c if status_codes::is_rejection(c) => {
                format!("ARC rejected transaction: HTTP {} - {}", status, body)
            }
            _ => format!("ARC error: HTTP {} - {}", status, body),
        };

        // ARC's 469 "BEEF validation failed" with a body that says the
        // verification TIMED OUT (GorillaPool, 2026-09-01: "couldn't verify
        // Merkle Roots ... BEEF verification timed out" after 7 s) is a
        // provider-side timeout, not a verdict on the transaction: the same
        // bytes are accepted by the next provider in ~0.3 s. Transient, so
        // the loop moves on and the tx is never marked invalid. Every other
        // 469 body stays a definitive rejection.
        if status_codes::is_transient_beef_validation_timeout(code, body) {
            tracing::warn!(
                name = %self.name,
                txid = %txid,
                code,
                body = %body,
                "ARC BEEF verification timed out on the provider side (469): transient, not a rejection"
            );
            return PostTxResultForTxid {
                txid,
                status: "error".to_string(),
                double_spend: false,
                orphan_mempool: false,
                competing_txs: None,
                data: Some(format!(
                    "ARC BEEF verification timed out (HTTP {}) - {}",
                    code, body
                )),
                service_error: true,
                block_hash: None,
                block_height: None,
                notes: vec![make_note(&self.name, "postRawTxVerificationTimeout")],
            };
        }

        // A transaction-level rejection (465 fee too low, 461 script,
        // 462 inputs, ...) is DEFINITIVE: the same bytes can never be
        // accepted, so it must not be reported as a transient
        // `service_error` that `classify_broadcast_results` turns into a
        // phantom "will retry" success. The numeric code goes in
        // `status` so the classifier (and any log) sees exactly why.
        if status_codes::is_rejection(code) {
            tracing::warn!(
                name = %self.name,
                txid = %txid,
                code,
                body = %body,
                "ARC definitively rejected the transaction"
            );
            return PostTxResultForTxid {
                txid,
                status: code.to_string(),
                double_spend: false,
                orphan_mempool: false,
                competing_txs: None,
                data: Some(error_msg),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![make_note(&self.name, "postRawTxRejected")],
            };
        }

        PostTxResultForTxid {
            txid,
            status: "error".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(error_msg),
            service_error: true,
            block_hash: None,
            block_height: None,
            notes: vec![make_note(&self.name, "postRawTxError")],
        }
    }

    /// A transient result for a request that never got an HTTP answer.
    fn request_failed(&self, txid: String, error: &str, note: &str) -> PostTxResultForTxid {
        self.service_error_result(txid, format!("Request failed: {}", error), note)
    }

    /// A transient (`service_error`) result carrying `detail`.
    fn service_error_result(
        &self,
        txid: String,
        detail: String,
        note: &str,
    ) -> PostTxResultForTxid {
        PostTxResultForTxid {
            txid,
            status: "error".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(detail),
            service_error: true,
            block_hash: None,
            block_height: None,
            notes: vec![make_note(&self.name, note)],
        }
    }

    /// Submit an EF batch to `POST /v1/txs`.
    ///
    /// Request: `application/json`, an array of `{"rawTx": <ef hex>}` (the
    /// `TransactionRequest` item of ARC's `pkg/api/arc.yaml`; the same body
    /// the ts-sdk `ARC.broadcastMany` sends). Response: ARC's handler
    /// (`POSTTransactions`) answers `200` with a BARE JSON array mixing
    /// `TransactionResponse` objects (`txid`, `txStatus`, ...) and inline
    /// error objects (`status` >= 400, `title`, `detail`, `txid`); the
    /// OpenAPI wrapper `{"transactions": [...]}` is accepted too.
    ///
    /// Returns one classified result per item (by the item's txid, or by
    /// position when an item carries none). A non-2xx answer is classified
    /// for the subject alone.
    async fn post_ef_batch(&self, entries: &[EfBatchEntry], subject: &str) -> ArcBatchPost {
        let url = format!("{}/v1/txs", self.url);
        let body: Vec<serde_json::Value> = entries
            .iter()
            .map(|e| serde_json::json!({ "rawTx": hex::encode(&e.ef) }))
            .collect();

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
                let code = resp.status().as_u16();
                let value: serde_json::Value = match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        return ArcBatchPost {
                            http_code: Some(code),
                            items: vec![self.service_error_result(
                                subject.to_string(),
                                format!("Failed to parse ARC batch response: {}", e),
                                "postBeefBatchParseError",
                            )],
                        }
                    }
                };
                let items_json: Vec<serde_json::Value> = match value {
                    serde_json::Value::Array(a) => a,
                    serde_json::Value::Object(mut o) => o
                        .remove("transactions")
                        .and_then(|t| match t {
                            serde_json::Value::Array(a) => Some(a),
                            _ => None,
                        })
                        .unwrap_or_default(),
                    _ => Vec::new(),
                };

                let mut items = Vec::with_capacity(items_json.len());
                for (i, item) in items_json.into_iter().enumerate() {
                    let parsed: ArcBatchItem = match serde_json::from_value(item) {
                        Ok(p) => p,
                        Err(e) => {
                            let txid = entries
                                .get(i)
                                .map(|e| e.txid.clone())
                                .unwrap_or_else(|| "unknown".to_string());
                            items.push(self.service_error_result(
                                txid,
                                format!("Unparseable ARC batch item: {}", e),
                                "postBeefBatchItemError",
                            ));
                            continue;
                        }
                    };
                    let txid = parsed
                        .txid
                        .clone()
                        .filter(|t| !t.is_empty())
                        .or_else(|| entries.get(i).map(|e| e.txid.clone()))
                        .unwrap_or_else(|| "unknown".to_string());
                    tracing::debug!(
                        name = %self.name,
                        txid = %txid,
                        tx_status = ?parsed.tx_status,
                        status = ?parsed.status,
                        detail = ?parsed.detail,
                        "ARC batch item"
                    );
                    let result = match parsed.tx_status.filter(|s| !s.is_empty()) {
                        Some(tx_status) => self.classify_arc_response(ArcResponse {
                            txid,
                            extra_info: parsed.extra_info,
                            tx_status,
                            competing_txs: parsed.competing_txs,
                        }),
                        None => match parsed.status {
                            Some(c) if c >= 400 => {
                                let detail = parsed
                                    .detail
                                    .or(parsed.title)
                                    .or(parsed.extra_info)
                                    .unwrap_or_default();
                                self.classify_arc_http_error(txid, c, &detail)
                            }
                            _ => self.service_error_result(
                                txid,
                                "ARC batch item carried neither txStatus nor an error status"
                                    .to_string(),
                                "postBeefBatchItemError",
                            ),
                        },
                    };
                    items.push(result);
                }
                ArcBatchPost {
                    http_code: Some(code),
                    items,
                }
            }
            Ok(resp) => {
                let code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                ArcBatchPost {
                    http_code: Some(code),
                    items: vec![self.classify_arc_http_error(subject.to_string(), code, &body)],
                }
            }
            Err(e) => ArcBatchPost {
                http_code: None,
                items: vec![self.request_failed(
                    subject.to_string(),
                    &e.to_string(),
                    "postBeefBatchCatch",
                )],
            },
        }
    }

    /// Post BEEF transaction.
    ///
    /// ARC accepts BEEF v1 format. If the beef is v2 and can be downgraded,
    /// it will be converted automatically.
    ///
    /// The full package every time (no seen set); see [`Arc::post_beef_seen`]
    /// for the reduced send a broadcast memory enables.
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef_seen(beef, txids, &HashSet::new())
            .await
            .map(|(result, _)| result)
    }

    /// Broadcast a BEEF, sending this ARC only what it has not already seen.
    ///
    /// `seen` is the provider's seen set (txids it accepted or reported
    /// itself, or that are mined) from a
    /// [`BroadcastMemory`](crate::services::BroadcastMemory). For a subject
    /// with unproven ancestors ([`reduced_send_plan`]):
    ///
    /// | unproven ancestors in `seen` | what is sent                                  |
    /// |------------------------------|-----------------------------------------------|
    /// | none                         | the full BEEF (`/v1/tx`), exactly as before   |
    /// | all                          | the subject alone as EF (`/v1/tx`)            |
    /// | some                         | EF batch: unseen ancestors + subject (`/v1/txs`) |
    ///
    /// A reduced send that is refused for what reads as a missing parent (an
    /// orphan-mempool verdict, a 4xx whose text matches
    /// [`missing_parent_hint`], an ancestor rejected inside the batch, or a
    /// batch endpoint this ARC does not serve) is retried ONCE with the full
    /// BEEF, and the fallback is logged. An EF child is never sent to a
    /// provider whose seen set does not cover its unproven parents.
    ///
    /// Logs the ARC round trip at info (the time a caller waits on
    /// `createAction` is mostly this) with the bytes actually sent and
    /// whether the send was reduced.
    pub async fn post_beef_seen(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
    ) -> Result<(PostBeefResult, PostBeefDelivery)> {
        let started = std::time::Instant::now();
        let mut delivery = PostBeefDelivery::default();
        let result = self.post_beef_inner(beef, txids, seen, &mut delivery).await;
        let outcome = match &result {
            Ok(r) => r.status.clone(),
            Err(_) => "error".to_string(),
        };
        tracing::info!(
            name = %self.name,
            subject = %txids.last().cloned().unwrap_or_default(),
            beef_bytes = beef.len(),
            bytes = delivery.bytes_sent,
            reduced = delivery.reduced,
            fallback_full = delivery.fallback_full,
            txs = txids.len(),
            http_ms = started.elapsed().as_millis(),
            outcome = %outcome,
            "ARC submit timing"
        );
        result.map(|r| (r, delivery))
    }

    async fn post_beef_inner(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
        delivery: &mut PostBeefDelivery,
    ) -> Result<PostBeefResult> {
        let mut result = PostBeefResult {
            name: self.name.clone(),
            status: "success".to_string(),
            txid_results: Vec::new(),
            error: None,
            notes: Vec::new(),
        };
        let subject = txids.last().cloned().unwrap_or_default();

        // Strategy: use EF (Extended Format) when all BEEF ancestors are proven
        // (have merkle proofs), which avoids ARC BEEF parsing bugs. But when the
        // BEEF contains unproven ancestors (e.g., internalized txs not yet on-chain,
        // or deep unconfirmed chains), send the full BEEF so ARC can process the
        // entire ancestor chain. EF only embeds direct parent data — it cannot
        // convey multi-level unproven ancestry. The one exception is the
        // seen-set rule below (`reduced_send_plan`).
        let (post_result, accepted_on_success) = {
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

                    let unproven_ancestors: Vec<String> = beef_parsed
                        .txs
                        .iter()
                        .filter(|btx| {
                            btx.bump_index().is_none()
                                && !btx.is_txid_only()
                                && Some(btx.txid()) != new_txid
                        })
                        .map(|btx| btx.txid())
                        .collect();

                    // ANY unproven ancestor ⇒ full BEEF (no cap) unless this ARC has
                    // already seen the ancestors. Full BEEF is the ONLY correct path
                    // for unconfirmed ancestry a provider does not know — it carries
                    // the whole 0-conf chain so ARC can validate it. (There is no
                    // valid EF fallback for an unproven parent: EF inlines only a
                    // parent's output, not the parent tx, so ARC would orphan the
                    // child. A pathologically huge BEEF that ARC rejects fails the
                    // same way under EF, so the cap only ever hurt.)
                    if unproven_ancestors.is_empty() {
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
                                            delivery.bytes_sent += ef_hex.len() / 2;
                                            (
                                                self.post_raw_tx(&ef_hex, Some(txids)).await?,
                                                vec![subject.clone()],
                                            )
                                        }
                                        Err(e) => {
                                            tracing::warn!(name = %self.name, error = %e, "EF serialization failed — falling back to BEEF");
                                            (
                                                self.post_full_beef(beef, txids, delivery).await?,
                                                vec![subject.clone()],
                                            )
                                        }
                                    }
                                } else {
                                    tracing::warn!(name = %self.name, "Hydration failed — falling back to BEEF");
                                    (
                                        self.post_full_beef(beef, txids, delivery).await?,
                                        vec![subject.clone()],
                                    )
                                }
                            }
                            None => (
                                self.post_full_beef(beef, txids, delivery).await?,
                                vec![subject.clone()],
                            ),
                        }
                    } else {
                        match reduced_send_plan(&unproven_ancestors, seen) {
                            SendPlan::Full => {
                                tracing::debug!(
                                    name = %self.name,
                                    unproven_ancestors = unproven_ancestors.len(),
                                    total_txs = beef_parsed.txs.len(),
                                    "BEEF has unproven ancestors none of which this ARC has seen — trying full BEEF first"
                                );
                                result.notes.push(make_note(&self.name, "postBeefFull"));
                                // Full BEEF carries the entire unconfirmed ancestry, so ARC can
                                // validate the whole 0-conf chain. NEVER downgrade to EF here: EF
                                // inlines only a direct parent's *output* and cannot convey an
                                // unproven *parent transaction*, so for any 0-conf ancestor ARC
                                // would orphan/reject the child — the phantom-tx bug. A transient
                                // full-BEEF failure classifies upstream as ServiceError/OrphanMempool
                                // and is retried as full BEEF by the SendWaiting task; a real
                                // rejection (double-spend / invalid) classifies permanently. This
                                // matches @bsv/wallet-toolbox, which always posts full BEEF and never
                                // EF-downgrades.
                                //
                                // The ONE exception is the seen-set rule (0.3.56, the other two
                                // arms of this match): an EF child may go to a provider whose
                                // BroadcastMemory seen set covers EVERY unproven parent (this
                                // ARC accepted or reported them itself, or they are mined), and
                                // an EF batch may carry the still-unseen ancestors with it in
                                // dependency order. Never an EF child to a provider whose seen
                                // set does not cover its unproven parents; and a reduced send
                                // that is refused for what reads as a missing parent is retried
                                // once with this full BEEF (`post_reduced`).
                                //
                                // On success ARC has every unproven transaction of the package:
                                // its handler (`getTxDataFromHex`) appends each RawTx-format
                                // ancestor of a BEEF to the submitted set, not just the subject.
                                let mut accepted = vec![subject.clone()];
                                accepted.extend(unproven_ancestors.iter().cloned());
                                (self.post_full_beef(beef, txids, delivery).await?, accepted)
                            }
                            SendPlan::EfSubject | SendPlan::EfBatch { .. } => {
                                self.post_reduced(
                                    beef,
                                    txids,
                                    seen,
                                    &unproven_ancestors,
                                    &mut result.notes,
                                    delivery,
                                )
                                .await?
                            }
                        }
                    }
                }
                Err(_) => {
                    // Can't parse BEEF — send as-is
                    (
                        self.post_full_beef(beef, txids, delivery).await?,
                        vec![subject.clone()],
                    )
                }
            }
        };

        result.status = post_result.status.clone();
        result.txid_results.push(post_result.clone());
        if post_result.is_success() {
            delivery.accepted_txids = accepted_on_success
                .into_iter()
                .filter(|t| !t.is_empty())
                .collect();
        }

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
                        orphan_mempool: data.tx_status == "SEEN_IN_ORPHAN_MEMPOOL",
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
                        orphan_mempool: false,
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
                        orphan_mempool: false,
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

    /// The full package: the whole BEEF, hex-encoded, to `/v1/tx`.
    async fn post_full_beef(
        &self,
        beef: &[u8],
        txids: &[String],
        delivery: &mut PostBeefDelivery,
    ) -> Result<PostTxResultForTxid> {
        delivery.bytes_sent += beef.len();
        let beef_hex = hex::encode(beef);
        self.post_raw_tx(&beef_hex, Some(txids)).await
    }

    /// The reduced send (see [`Arc::post_beef_seen`]): the subject alone as
    /// EF when every unproven ancestor is in `seen`, otherwise an EF batch
    /// of the unseen ancestors plus the subject. Falls back to the full BEEF
    /// once when the reduced send is refused for what reads as a missing
    /// parent.
    ///
    /// Returns the subject's result and the txids this ARC accepted.
    async fn post_reduced(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
        unproven_ancestors: &[String],
        notes: &mut Vec<HashMap<String, serde_json::Value>>,
        delivery: &mut PostBeefDelivery,
    ) -> Result<(PostTxResultForTxid, Vec<String>)> {
        let subject = txids.last().cloned().unwrap_or_default();
        let full_accepted = |accepted: bool| -> Vec<String> {
            if accepted {
                let mut all = vec![subject.clone()];
                all.extend(unproven_ancestors.iter().cloned());
                all
            } else {
                Vec::new()
            }
        };

        let batch = match beef_to_ef_batch_skipping(beef, seen) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    name = %self.name,
                    txid = %subject,
                    error = %e,
                    "EF batch conversion failed — falling back to full BEEF"
                );
                notes.push(make_note(&self.name, "postBeefFull"));
                let r = self.post_full_beef(beef, txids, delivery).await?;
                let accepted = full_accepted(r.is_success());
                return Ok((r, accepted));
            }
        };
        delivery.reduced = true;

        let (first, first_accepted, refused) =
            if batch.entries.len() == 1 && batch.entries[0].txid == subject {
                // Every unproven ancestor is known to this ARC: the subject alone.
                let ef = &batch.entries[0].ef;
                tracing::debug!(
                    name = %self.name,
                    txid = %subject,
                    skipped = batch.skipped.len(),
                    ef_len = ef.len(),
                    "Posting subject as EF (every unproven ancestor already seen by this ARC)"
                );
                notes.push(make_note(&self.name, "postBeefReducedEF"));
                delivery.bytes_sent += ef.len();
                let r = self.post_raw_tx(&hex::encode(ef), Some(txids)).await?;
                let refused = r.orphan_mempool
                    || (!r.is_success() && missing_parent_hint(r.data.as_deref().unwrap_or("")));
                let accepted = if r.is_success() {
                    vec![subject.clone()]
                } else {
                    Vec::new()
                };
                (r, accepted, refused)
            } else {
                // Some ancestors are unseen: they travel with the subject, in
                // dependency order, through the batch endpoint.
                let bytes: usize = batch.entries.iter().map(|e| e.ef.len()).sum();
                tracing::debug!(
                    name = %self.name,
                    txid = %subject,
                    txs = batch.entries.len(),
                    skipped = batch.skipped.len(),
                    bytes,
                    "Posting EF batch of unseen ancestors plus subject to /v1/txs"
                );
                notes.push(make_note(&self.name, "postBeefReducedBatch"));
                delivery.bytes_sent += bytes;
                let post = self.post_ef_batch(&batch.entries, &subject).await;
                let subject_result = post
                    .items
                    .iter()
                    .find(|r| r.txid == subject)
                    .cloned()
                    .unwrap_or_else(|| {
                        self.service_error_result(
                            subject.clone(),
                            "ARC batch response did not include the subject".to_string(),
                            "postBeefBatchNoSubject",
                        )
                    });
                let endpoint_unsupported = matches!(post.http_code, Some(404 | 405 | 415 | 501));
                let refused = subject_result.orphan_mempool
                    || endpoint_unsupported
                    || post.items.iter().any(|r| {
                        !r.is_success()
                            && (missing_parent_hint(r.data.as_deref().unwrap_or(""))
                                || (r.txid != subject
                                    && crate::storage::broadcast::is_definitive_rejection(r)))
                    });
                let accepted: Vec<String> = if subject_result.is_success() {
                    post.items
                        .iter()
                        .filter(|r| r.is_success())
                        .map(|r| r.txid.clone())
                        .collect()
                } else {
                    Vec::new()
                };
                (subject_result, accepted, refused)
            };

        if !refused {
            return Ok((first, first_accepted));
        }

        tracing::warn!(
            name = %self.name,
            txid = %subject,
            sent = batch.entries.len(),
            skipped = batch.skipped.len(),
            verdict = %first.data.as_deref().unwrap_or(""),
            "ARC refused the reduced send (missing parent?); retrying once with the full BEEF"
        );
        notes.push(make_note(&self.name, "postBeefFallbackFull"));
        delivery.fallback_full = true;
        let r = self.post_full_beef(beef, txids, delivery).await?;
        let accepted = full_accepted(r.is_success());
        Ok((r, accepted))
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

/// What a classic ARC provider sends for a subject with unproven ancestors,
/// given which of those ancestors it has already seen
/// ([`reduced_send_plan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendPlan {
    /// No ancestor is known to the provider (or there is no memory): the
    /// full BEEF, exactly the pre-memory behavior.
    Full,
    /// Every unproven ancestor is known (or there is none): the subject
    /// alone, as EF, to `/v1/tx`.
    EfSubject,
    /// Some ancestors are known: an EF batch of the unseen ones, in
    /// dependency order, plus the subject, to `/v1/txs`.
    EfBatch {
        /// The unproven ancestors the provider has not seen, in the order
        /// they appear in the BEEF.
        unseen: Vec<String>,
    },
}

/// Decide the send for `unproven_ancestors` against the provider's `seen`
/// set. Never plans an EF child for a provider whose seen set does not cover
/// its unproven parents: those parents travel in the batch.
pub fn reduced_send_plan(unproven_ancestors: &[String], seen: &HashSet<String>) -> SendPlan {
    if unproven_ancestors.is_empty() {
        return SendPlan::EfSubject;
    }
    let unseen: Vec<String> = unproven_ancestors
        .iter()
        .filter(|txid| !seen.contains(*txid))
        .cloned()
        .collect();
    if unseen.len() == unproven_ancestors.len() {
        SendPlan::Full
    } else if unseen.is_empty() {
        SendPlan::EfSubject
    } else {
        SendPlan::EfBatch { unseen }
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

/// One item of ARC's `/v1/txs` answer: a `TransactionResponse` (has
/// `txStatus`) or an inline error object (`status` >= 400 with `title` /
/// `detail`). Every field is optional so either shape parses.
#[derive(Debug, Default, Deserialize)]
struct ArcBatchItem {
    #[serde(default)]
    txid: Option<String>,
    #[serde(rename = "txStatus", default)]
    tx_status: Option<String>,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    detail: Option<String>,
    #[serde(rename = "extraInfo", default)]
    extra_info: Option<String>,
    #[serde(rename = "competingTxs", default)]
    competing_txs: Option<Vec<String>>,
}

/// Outcome of one `POST /v1/txs` call: the HTTP code (`None` when the
/// request never got an answer) and one classified result per item.
struct ArcBatchPost {
    http_code: Option<u16>,
    items: Vec<PostTxResultForTxid>,
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

    fn set(items: &[&String]) -> HashSet<String> {
        items.iter().map(|s| (*s).clone()).collect()
    }

    #[test]
    fn transient_469_only_when_the_body_says_timeout() {
        use status_codes::is_transient_beef_validation_timeout;
        // The GorillaPool shape, 2026-09-01.
        assert!(is_transient_beef_validation_timeout(
            469,
            r#"{"detail":"BEEF validation failed: couldn't verify Merkle Roots: BEEF verification timed out","status":469}"#
        ));
        assert!(is_transient_beef_validation_timeout(
            469,
            "request timed out"
        ));
        assert!(is_transient_beef_validation_timeout(469, "Timeout"));
        // A real merkle-root verdict stays definitive.
        assert!(!is_transient_beef_validation_timeout(
            469,
            "BEEF validation failed: merkle root mismatch at height 800000"
        ));
        assert!(!is_transient_beef_validation_timeout(469, ""));
        // Other codes never qualify, whatever the body says.
        assert!(!is_transient_beef_validation_timeout(465, "timed out"));
        assert!(!is_transient_beef_validation_timeout(468, "timeout"));
        assert!(!is_transient_beef_validation_timeout(500, "timed out"));
    }

    #[test]
    fn transient_469_classifies_as_service_error_and_other_469_as_rejection() {
        let arc = Arc::taal_mainnet(None).unwrap();
        let txid = "ab".repeat(32);

        let timeout = arc.classify_arc_http_error(
            txid.clone(),
            469,
            "BEEF validation failed: couldn't verify Merkle Roots: BEEF verification timed out",
        );
        assert!(
            timeout.service_error,
            "a provider-side timeout is transient"
        );
        assert_eq!(timeout.status, "error");
        assert!(!crate::storage::broadcast::is_definitive_rejection(
            &timeout
        ));
        let outcome = crate::storage::broadcast::classify_broadcast_results(&[PostBeefResult {
            name: "arc".to_string(),
            status: "error".to_string(),
            txid_results: vec![timeout],
            error: None,
            notes: vec![],
        }]);
        assert!(
            matches!(
                outcome,
                crate::storage::broadcast::BroadcastOutcome::ServiceError { .. }
            ),
            "got {:?}",
            outcome
        );

        let verdict =
            arc.classify_arc_http_error(txid, 469, "BEEF validation failed: merkle root mismatch");
        assert!(!verdict.service_error, "a real 469 verdict is definitive");
        assert_eq!(verdict.status, "469");
        assert!(crate::storage::broadcast::is_definitive_rejection(&verdict));
    }

    #[test]
    fn send_plan_decision_table() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let ancestors = vec![a.clone(), b.clone(), c.clone()];

        // none seen -> full BEEF (the pre-memory behavior)
        assert_eq!(
            reduced_send_plan(&ancestors, &HashSet::new()),
            SendPlan::Full
        );
        let other = set(&[&"dd".repeat(32)]);
        assert_eq!(reduced_send_plan(&ancestors, &other), SendPlan::Full);

        // all seen -> EF of the subject alone
        assert_eq!(
            reduced_send_plan(&ancestors, &set(&[&a, &b, &c])),
            SendPlan::EfSubject
        );
        // a superset is fine too
        assert_eq!(
            reduced_send_plan(&ancestors, &set(&[&a, &b, &c, &"dd".repeat(32)])),
            SendPlan::EfSubject
        );

        // partially seen -> batch of the unseen, in BEEF order
        assert_eq!(
            reduced_send_plan(&ancestors, &set(&[&b])),
            SendPlan::EfBatch {
                unseen: vec![a.clone(), c.clone()]
            }
        );
        assert_eq!(
            reduced_send_plan(&ancestors, &set(&[&a, &c])),
            SendPlan::EfBatch {
                unseen: vec![b.clone()]
            }
        );

        // no unproven ancestors at all -> EF (the existing all-proven path)
        assert_eq!(reduced_send_plan(&[], &HashSet::new()), SendPlan::EfSubject);
    }

    #[test]
    fn batch_item_shapes_parse() {
        // ARC `TransactionResponse`
        let ok: ArcBatchItem = serde_json::from_value(serde_json::json!({
            "txid": "ab".repeat(32),
            "txStatus": "SEEN_ON_NETWORK",
            "status": 200,
            "title": "OK",
            "timestamp": "2026-09-01T00:00:00Z",
            "blockHash": "",
            "blockHeight": 0,
            "merklePath": ""
        }))
        .unwrap();
        assert_eq!(ok.tx_status.as_deref(), Some("SEEN_ON_NETWORK"));
        assert_eq!(ok.status, Some(200));

        // ARC inline error object
        let err: ArcBatchItem = serde_json::from_value(serde_json::json!({
            "type": "https://arc.bitcoinsv.com/errors/462",
            "title": "Invalid inputs",
            "status": 462,
            "detail": "Transaction is invalid because the inputs are non-existent or spent",
            "txid": "cd".repeat(32)
        }))
        .unwrap();
        assert!(err.tx_status.is_none());
        assert_eq!(err.status, Some(462));
        assert!(err.detail.unwrap().contains("inputs"));
    }
}
