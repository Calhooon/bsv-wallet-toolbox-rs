//! Arcade V2 (Teranode broadcaster) service provider.
//!
//! Arcade V2 (github.com/bsv-blockchain/arcade) is the next-generation BSV
//! transaction broadcaster for Teranode. It differs from classic ARC in three
//! load-bearing ways (all empirically verified against the live endpoint):
//!
//! 1. **EF-only submit** — Arcade rejects ALL BEEF flavors (V1, V2, Atomic) and
//!    raw transactions whose inputs it can't source. Every unmined transaction in
//!    a BEEF's ancestry must be individually converted to Extended Format
//!    (BRC-30) and submitted (dependency order; Arcade dedupes re-submissions).
//!    See [`beef_to_ef_batch`].
//! 2. **Always-async** — submit returns `202 RECEIVED`; there is no
//!    `X-WaitForStatus`. Verdicts arrive later via SSE (`GET
//!    /events?callbackToken=`), webhooks (`X-CallbackUrl`, public-HTTPS-only),
//!    or polling (`GET /tx/{txid}`).
//! 3. **Per-token SSE stream** — submitting with `X-CallbackToken` +
//!    `X-FullStatusUpdates: true` scopes an SSE stream that replays all
//!    non-terminal statuses on a fresh connect (race-free) and supports
//!    `Last-Event-ID` resume. See [`ArcadeSseClient`].
//!
//! Status lifecycle: `RECEIVED → SENT_TO_NETWORK → ACCEPTED_BY_NETWORK →
//! SEEN_ON_NETWORK → SEEN_MULTIPLE_NODES → MINED`; fatal statuses are
//! `REJECTED` and `DOUBLE_SPEND_ATTEMPTED`. Gate spendability on
//! `SEEN_ON_NETWORK` (~3s, reliable); `SEEN_MULTIPLE_NODES` is erratic (>20s
//! observed) and should only ever be treated as an async upgrade.
//!
//! # Configuration
//!
//! Arcade mode is an **explicit** configuration choice — the toolbox never
//! guesses from URL substrings. Set
//! [`ServicesOptions::with_arcade`](crate::services::ServicesOptions::with_arcade)
//! (which sets `arcade_v2 = true`) to register the Arcade broadcaster as the
//! first postBeef provider.

use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::services::traits::{PostBeefResult, PostTxResultForTxid};
use crate::{Error, Result};

/// Live Arcade V2 mainnet endpoint (verified 2026-07-10).
pub const ARCADE_V2_MAINNET: &str = "https://arcade-v2-us-1.bsvblockchain.tech";

/// Arcade transaction statuses, in lifecycle order.
pub mod statuses {
    /// Accepted by Arcade, queued for propagation.
    pub const RECEIVED: &str = "RECEIVED";
    /// Sent to the Teranode network.
    pub const SENT_TO_NETWORK: &str = "SENT_TO_NETWORK";
    /// Accepted by the network.
    pub const ACCEPTED_BY_NETWORK: &str = "ACCEPTED_BY_NETWORK";
    /// Seen on the network (~3s after submit; the reliable spendability gate).
    pub const SEEN_ON_NETWORK: &str = "SEEN_ON_NETWORK";
    /// Seen by multiple nodes (erratic timing — async upgrade only).
    pub const SEEN_MULTIPLE_NODES: &str = "SEEN_MULTIPLE_NODES";
    /// Mined into a block. The webhook payload for this status carries
    /// `blockHash`, `blockHeight` and `merklePath`.
    pub const MINED: &str = "MINED";
    /// Fatal: rejected (also how double-spends/mempool orphans surface).
    pub const REJECTED: &str = "REJECTED";
    /// Fatal: double spend attempted.
    pub const DOUBLE_SPEND_ATTEMPTED: &str = "DOUBLE_SPEND_ATTEMPTED";
}

/// Rank an Arcade status within the lifecycle (higher = further along).
/// Fatal statuses rank 0 — compare with [`is_fatal_status`] first.
pub fn arcade_status_rank(status: &str) -> u8 {
    match status {
        statuses::RECEIVED => 1,
        statuses::SENT_TO_NETWORK => 2,
        statuses::ACCEPTED_BY_NETWORK => 3,
        statuses::SEEN_ON_NETWORK => 4,
        statuses::SEEN_MULTIPLE_NODES => 5,
        statuses::MINED => 6,
        _ => 0,
    }
}

/// Whether an Arcade status is terminal-fatal (do not build on this tx).
pub fn is_fatal_status(status: &str) -> bool {
    matches!(
        status,
        statuses::REJECTED | statuses::DOUBLE_SPEND_ATTEMPTED
    )
}

/// Configuration for the Arcade V2 provider.
#[derive(Debug, Clone, Default)]
pub struct ArcadeConfig {
    /// Public-HTTPS webhook URL for async status POSTs (`X-CallbackUrl`).
    /// Arcade is SSRF-guarded: plain localhost URLs are rejected server-side.
    /// The MINED webhook payload includes the merkle path.
    pub callback_url: Option<String>,

    /// Token sent as `X-CallbackToken`. Authenticates the webhook AND scopes
    /// the `GET /events?callbackToken=` SSE stream.
    pub callback_token: Option<String>,

    /// Skip Arcade-side fee validation (`X-SkipFeeValidation`).
    pub skip_fee_validation: bool,

    /// Skip Arcade-side script validation (`X-SkipScriptValidation`).
    pub skip_script_validation: bool,

    /// Additional headers to include on submits.
    pub headers: Option<HashMap<String, String>>,

    /// Request timeout in seconds for submit/status calls (not SSE).
    pub timeout_secs: Option<u64>,
}

impl ArcadeConfig {
    /// Create a config with a callback token (SSE scoping).
    pub fn with_callback_token(token: impl Into<String>) -> Self {
        Self {
            callback_token: Some(token.into()),
            ..Default::default()
        }
    }

    /// Set the webhook callback URL (must be public HTTPS).
    pub fn with_callback_url(mut self, url: impl Into<String>) -> Self {
        self.callback_url = Some(url.into());
        self
    }
}

/// Arcade V2 service provider (EF-only broadcaster).
pub struct Arcade {
    client: Client,
    name: String,
    url: String,
    config: ArcadeConfig,
}

impl Arcade {
    /// Create a new Arcade V2 provider.
    pub fn new(
        url: impl Into<String>,
        config: Option<ArcadeConfig>,
        name: Option<&str>,
    ) -> Result<Self> {
        let url = url.into();
        let config = config.unwrap_or_default();

        let timeout = config.timeout_secs.unwrap_or(30);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            name: name.unwrap_or("ArcadeV2").to_string(),
            url: url.trim_end_matches('/').to_string(),
            config,
        })
    }

    /// Create a provider for the live mainnet endpoint.
    pub fn mainnet(config: Option<ArcadeConfig>) -> Result<Self> {
        Self::new(ARCADE_V2_MAINNET, config, Some("ArcadeV2"))
    }

    /// Get the provider name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Base URL (no trailing slash).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The configured callback token, if any.
    pub fn callback_token(&self) -> Option<&str> {
        self.config.callback_token.as_deref()
    }

    /// Headers for submit requests.
    fn submit_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
        // REQUIRED to receive non-terminal statuses (SEEN_ON_NETWORK etc.) on
        // the SSE stream / webhook. Default delivers terminal statuses only.
        headers.insert("X-FullStatusUpdates", "true".parse().unwrap());

        if let Some(ref token) = self.config.callback_token {
            if let Ok(v) = token.parse() {
                headers.insert("X-CallbackToken", v);
            }
        }
        if let Some(ref url) = self.config.callback_url {
            if let Ok(v) = url.parse() {
                headers.insert("X-CallbackUrl", v);
            }
        }
        if self.config.skip_fee_validation {
            headers.insert("X-SkipFeeValidation", "true".parse().unwrap());
        }
        if self.config.skip_script_validation {
            headers.insert("X-SkipScriptValidation", "true".parse().unwrap());
        }
        if let Some(ref additional) = self.config.headers {
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

    /// Post a BEEF by converting its unproven ancestry to EF and submitting.
    ///
    /// - Multiple unproven txs → binary-concatenated EF to `POST /txs`
    ///   (`application/octet-stream`; the ONLY format `/txs` accepts).
    /// - Single unproven tx → binary EF to `POST /tx`.
    /// - Nothing unproven → nothing to submit (success, noted).
    ///
    /// Arcade responds `202` and dedupes ancestors (`duplicates` count).
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        let mut result = PostBeefResult {
            name: self.name.clone(),
            status: "success".to_string(),
            txid_results: Vec::new(),
            error: None,
            notes: Vec::new(),
        };

        let (efs, subject_txid) = match beef_to_ef_batch(beef) {
            Ok(v) => v,
            Err(e) => {
                // Can't convert: report a service error so the caller can fail
                // over to a BEEF-capable provider.
                result.status = "error".to_string();
                result
                    .notes
                    .push(make_note(&self.name, "postBeefEfConversionError"));
                let txid = txids.last().cloned().unwrap_or_default();
                result.txid_results.push(PostTxResultForTxid {
                    txid,
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(format!("EF conversion failed: {}", e)),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefEfConversionError")],
                });
                return Ok(result);
            }
        };

        if efs.is_empty() {
            // Every transaction in the BEEF is already proven — nothing to broadcast.
            result
                .notes
                .push(make_note(&self.name, "postBeefAllProven"));
            for txid in txids {
                result.txid_results.push(PostTxResultForTxid {
                    txid: txid.clone(),
                    status: "success".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some("already proven; nothing to submit".to_string()),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefAllProven")],
                });
            }
            return Ok(result);
        }

        let submit_outcome = if efs.len() == 1 {
            self.post_single_ef(&efs[0], &subject_txid).await
        } else {
            self.post_ef_batch(&efs, &subject_txid).await
        };

        match submit_outcome {
            Ok(SubmitOutcome::Accepted { note }) => {
                result.notes.push(make_note(&self.name, &note));
                let mut reported: Vec<String> = txids.to_vec();
                if !reported.contains(&subject_txid) {
                    reported.push(subject_txid.clone());
                }
                for txid in reported {
                    result.txid_results.push(PostTxResultForTxid {
                        txid,
                        status: "success".to_string(),
                        double_spend: false,
                        orphan_mempool: false,
                        competing_txs: None,
                        data: Some(note.clone()),
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![make_note(&self.name, &note)],
                    });
                }
            }
            Ok(SubmitOutcome::Fatal {
                tx_status,
                double_spend,
            }) => {
                result.status = "error".to_string();
                result
                    .notes
                    .push(make_note(&self.name, "postBeefFatalStatus"));
                result.txid_results.push(PostTxResultForTxid {
                    txid: subject_txid.clone(),
                    status: "error".to_string(),
                    double_spend,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(tx_status),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefFatalStatus")],
                });
            }
            Ok(SubmitOutcome::ServiceError { detail }) => {
                result.status = "error".to_string();
                result
                    .notes
                    .push(make_note(&self.name, "postBeefServiceError"));
                result.txid_results.push(PostTxResultForTxid {
                    txid: subject_txid.clone(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(detail),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefServiceError")],
                });
            }
            Err(e) => {
                result.status = "error".to_string();
                result.notes.push(make_note(&self.name, "postBeefCatch"));
                result.txid_results.push(PostTxResultForTxid {
                    txid: subject_txid.clone(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(format!("Request failed: {}", e)),
                    service_error: true,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefCatch")],
                });
            }
        }

        Ok(result)
    }

    /// Submit one EF binary to `POST /tx`.
    async fn post_single_ef(&self, ef: &[u8], subject_txid: &str) -> Result<SubmitOutcome> {
        let url = format!("{}/tx", self.url);
        let response = self
            .client
            .post(&url)
            .headers(self.submit_headers())
            .body(ef.to_vec())
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let data: ArcadeSubmitResponse = resp.json().await.map_err(|e| {
                    Error::ServiceError(format!("Failed to parse Arcade response: {}", e))
                })?;
                tracing::debug!(
                    name = %self.name,
                    txid = %data.txid,
                    tx_status = %data.tx_status,
                    "Arcade /tx response"
                );
                // Resubmission of a known tx returns its CURRENT status —
                // including fatal ones.
                if is_fatal_status(&data.tx_status) {
                    Ok(SubmitOutcome::Fatal {
                        double_spend: data.tx_status == statuses::DOUBLE_SPEND_ATTEMPTED,
                        tx_status: data.tx_status,
                    })
                } else {
                    Ok(SubmitOutcome::Accepted {
                        note: format!("postTxEf:{}", data.tx_status),
                    })
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(name = %self.name, %status, body = %body, txid = %subject_txid, "Arcade /tx error");
                Ok(SubmitOutcome::ServiceError {
                    detail: format!("Arcade error: HTTP {} - {}", status, body),
                })
            }
            Err(e) => Ok(SubmitOutcome::ServiceError {
                detail: format!("Request failed: {}", e),
            }),
        }
    }

    /// Submit multiple EF binaries as binary concat to `POST /txs`.
    async fn post_ef_batch(&self, efs: &[Vec<u8>], subject_txid: &str) -> Result<SubmitOutcome> {
        let url = format!("{}/txs", self.url);
        let total_len: usize = efs.iter().map(|e| e.len()).sum();
        let mut body = Vec::with_capacity(total_len);
        for ef in efs {
            body.extend_from_slice(ef);
        }

        let response = self
            .client
            .post(&url)
            .headers(self.submit_headers())
            .body(body)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let data: ArcadeBatchResponse = resp.json().await.map_err(|e| {
                    Error::ServiceError(format!("Failed to parse Arcade batch response: {}", e))
                })?;
                tracing::debug!(
                    name = %self.name,
                    submitted = data.submitted,
                    duplicates = data.duplicates,
                    total = data.total,
                    txid = %subject_txid,
                    "Arcade /txs response"
                );
                // /txs is summary-only (no per-tx results, upstream issue #210).
                // 202 means the batch was accepted; verdicts arrive via SSE/webhook.
                Ok(SubmitOutcome::Accepted {
                    note: format!(
                        "postTxsEfBatch:submitted={},duplicates={},total={}",
                        data.submitted, data.duplicates, data.total
                    ),
                })
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(name = %self.name, %status, body = %body, txid = %subject_txid, "Arcade /txs error");
                Ok(SubmitOutcome::ServiceError {
                    detail: format!("Arcade batch error: HTTP {} - {}", status, body),
                })
            }
            Err(e) => Ok(SubmitOutcome::ServiceError {
                detail: format!("Request failed: {}", e),
            }),
        }
    }

    /// Query the current status of a transaction (`GET /tx/{txid}`).
    ///
    /// Only works for transactions Arcade has seen.
    pub async fn get_tx_status(&self, txid: &str) -> Result<Option<ArcadeTxInfo>> {
        let url = format!("{}/tx/{}", self.url, txid);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;

        match response.status() {
            reqwest::StatusCode::OK => {
                let data: ArcadeTxInfo = response.json().await.map_err(|e| {
                    Error::ServiceError(format!("Failed to parse Arcade tx info: {}", e))
                })?;
                Ok(Some(data))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => Err(Error::ServiceError(format!(
                "Arcade getTxStatus failed with status {}",
                status
            ))),
        }
    }

    /// Health check (`GET /health`).
    pub async fn health(&self) -> Result<bool> {
        let url = format!("{}/health", self.url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Request failed: {}", e)))?;
        if !response.status().is_success() {
            return Ok(false);
        }
        let v: serde_json::Value = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Failed to parse health: {}", e)))?;
        Ok(v.get("healthy").and_then(|h| h.as_bool()).unwrap_or(false))
    }
}

/// Internal outcome classification for a submit call.
enum SubmitOutcome {
    Accepted {
        note: String,
    },
    Fatal {
        tx_status: String,
        double_spend: bool,
    },
    ServiceError {
        detail: String,
    },
}

// =============================================================================
// BEEF → EF conversion
// =============================================================================

/// Convert a BEEF into Extended Format (BRC-30) binaries for Arcade V2.
///
/// Arcade V2 only accepts EF — it rejects BEEF and cannot look up spent parent
/// outputs for raw transactions. Every unproven (unmined) transaction in the
/// BEEF must be submitted itself, since interior chain txs otherwise never
/// reach the network. Source satoshis/scripts come from the BEEF's own
/// ancestry: the BEEF parser does NOT link `input.source_transaction`, so this
/// function links each input from the BEEF's own tx map before EF-encoding.
///
/// Returns `(efs, subject_txid)` — EF binaries for all unproven txs in
/// dependency order (parents before children), and the txid of the BEEF's
/// subject (last-sorted) transaction. `efs` is empty when every transaction is
/// already proven.
pub fn beef_to_ef_batch(beef: &[u8]) -> Result<(Vec<Vec<u8>>, String)> {
    use bsv_rs::transaction::{Beef, Transaction};

    let mut beef = Beef::from_binary(beef)
        .map_err(|e| Error::ServiceError(format!("BEEF parse failed: {}", e)))?;
    // Sort into dependency order: parents before children.
    beef.sort_txs();

    // txid → parsed transaction, for linking input sources one level deep.
    // Parsed BEEF transactions have no sources linked themselves, so the
    // clones stay flat (no recursive blowup).
    let mut tx_map: HashMap<String, Transaction> = HashMap::with_capacity(beef.txs.len());
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            tx_map.insert(btx.txid(), tx.clone());
        }
    }

    let mut efs = Vec::new();
    let mut subject_txid = String::new();

    for btx in &beef.txs {
        let txid = btx.txid();
        subject_txid = txid.clone();

        if btx.has_proof() {
            // Already mined — provides source data for children, nothing to broadcast.
            continue;
        }

        let tx = btx.tx().ok_or_else(|| {
            Error::ServiceError(format!(
                "txid-only BEEF entry {} has no transaction data for EF conversion",
                txid
            ))
        })?;

        let mut tx = tx.clone();
        for input in &mut tx.inputs {
            if input.source_transaction.is_some() {
                continue;
            }
            let src_txid = input.get_source_txid().map_err(|e| {
                Error::ServiceError(format!("input in {} has no source txid: {}", txid, e))
            })?;
            let src = tx_map.get(&src_txid).ok_or_else(|| {
                Error::ServiceError(format!(
                    "source tx {} for {} not present in BEEF",
                    src_txid, txid
                ))
            })?;
            input.source_transaction = Some(Box::new(src.clone()));
        }

        let ef = tx
            .to_ef()
            .map_err(|e| Error::ServiceError(format!("EF conversion for {}: {}", txid, e)))?;
        efs.push(ef);
    }

    Ok((efs, subject_txid))
}

// =============================================================================
// SSE status stream
// =============================================================================

/// A parsed Server-Sent-Events frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The `id:` field (Arcade uses a nanosecond timestamp).
    pub id: Option<String>,
    /// The `event:` field (Arcade uses `status`).
    pub event: Option<String>,
    /// The `data:` field(s), joined with `\n` when multi-line.
    pub data: String,
}

/// Incremental SSE frame parser.
///
/// Feed raw byte chunks as they arrive; complete frames are returned as soon
/// as their terminating blank line has been seen. Handles frames split across
/// chunk boundaries, multi-line `data:`, `\r\n` line endings, and `:` comment
/// lines (keep-alives).
#[derive(Debug, Default)]
pub struct SseFrameParser {
    buf: String,
    cur_id: Option<String>,
    cur_event: Option<String>,
    cur_data: Vec<String>,
}

impl SseFrameParser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a chunk of bytes; returns any frames completed by this chunk.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        // SSE is UTF-8; tolerate invalid sequences rather than dropping the stream.
        self.buf.push_str(&String::from_utf8_lossy(chunk));

        let mut events = Vec::new();
        // Process complete lines only; keep the trailing partial line buffered.
        while let Some(newline_pos) = self.buf.find('\n') {
            let line: String = self.buf.drain(..=newline_pos).collect();
            let line = line.trim_end_matches(['\n', '\r']);

            if line.is_empty() {
                // Blank line = frame boundary.
                if !self.cur_data.is_empty() || self.cur_id.is_some() || self.cur_event.is_some() {
                    events.push(SseEvent {
                        id: self.cur_id.take(),
                        event: self.cur_event.take(),
                        data: self.cur_data.join("\n"),
                    });
                    self.cur_data.clear();
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix(':') {
                // Comment / keep-alive.
                let _ = rest;
                continue;
            }
            let (field, value) = match line.split_once(':') {
                Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
                None => (line, ""),
            };
            match field {
                "id" => self.cur_id = Some(value.to_string()),
                "event" => self.cur_event = Some(value.to_string()),
                "data" => self.cur_data.push(value.to_string()),
                _ => {} // ignore unknown fields (retry:, etc.)
            }
        }
        events
    }
}

/// A transaction status update from the Arcade SSE stream.
#[derive(Debug, Clone, Deserialize)]
pub struct ArcadeStatusEvent {
    /// Transaction ID.
    pub txid: String,
    /// Arcade status (see [`statuses`]).
    #[serde(rename = "txStatus")]
    pub tx_status: String,
    /// Event timestamp.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// SSE event id (for `Last-Event-ID` resume). Not part of the JSON
    /// payload; populated from the SSE frame.
    #[serde(skip)]
    pub event_id: Option<String>,
}

/// SSE client for `GET /events?callbackToken=<token>`.
///
/// A fresh connect REPLAYS all non-terminal statuses for the token, so
/// connecting after submit is race-free. `Last-Event-ID` resume is supported
/// and tracked automatically across [`ArcadeSseClient::stream_once`] calls.
pub struct ArcadeSseClient {
    client: Client,
    base_url: String,
    token: String,
    /// Last observed SSE event id; sent as `Last-Event-ID` on reconnect.
    pub last_event_id: Option<String>,
}

impl ArcadeSseClient {
    /// Create a new SSE client for the given Arcade base URL and callback token.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        // No total-request timeout: SSE connections are long-lived.
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            last_event_id: None,
        })
    }

    /// The `/events` URL this client connects to.
    pub fn events_url(&self) -> String {
        format!("{}/events?callbackToken={}", self.base_url, self.token)
    }

    /// Connect once and forward parsed status events into `tx` until the
    /// stream ends or errors. Returns the number of status events delivered.
    /// The sender is dropped on return, closing the receiver side.
    ///
    /// The caller is expected to loop with backoff around this (the fresh
    /// connect replay makes reconnects lossless for non-terminal statuses).
    pub async fn stream_once(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ArcadeStatusEvent>,
    ) -> Result<u64> {
        let mut req = self
            .client
            .get(self.events_url())
            .header("Accept", "text/event-stream");
        if let Some(ref id) = self.last_event_id {
            req = req.header("Last-Event-ID", id.clone());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("SSE connect failed: {}", e)))?;

        if !resp.status().is_success() {
            return Err(Error::ServiceError(format!(
                "SSE connect failed: HTTP {}",
                resp.status()
            )));
        }

        let mut parser = SseFrameParser::new();
        let mut stream = resp.bytes_stream();
        let mut delivered = 0u64;

        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| Error::NetworkError(format!("SSE stream error: {}", e)))?;
            for frame in parser.push(&chunk) {
                if let Some(ref id) = frame.id {
                    self.last_event_id = Some(id.clone());
                }
                // Arcade tags status frames `event: status`; be liberal and
                // accept untagged data frames too.
                if frame.data.is_empty() {
                    continue;
                }
                match serde_json::from_str::<ArcadeStatusEvent>(&frame.data) {
                    Ok(mut ev) => {
                        ev.event_id = frame.id.clone();
                        if tx.send(ev).await.is_err() {
                            // Receiver dropped — stop streaming.
                            return Ok(delivered);
                        }
                        delivered += 1;
                    }
                    Err(e) => {
                        tracing::debug!(data = %frame.data, error = %e, "Unparseable SSE data frame");
                    }
                }
            }
        }

        Ok(delivered)
    }
}

// =============================================================================
// API response types
// =============================================================================

/// Response to `POST /tx`.
#[derive(Debug, Deserialize)]
struct ArcadeSubmitResponse {
    txid: String,
    #[serde(rename = "txStatus")]
    tx_status: String,
}

/// Response to `POST /txs` (summary only — no per-tx results).
#[derive(Debug, Deserialize)]
struct ArcadeBatchResponse {
    #[serde(default)]
    duplicates: u64,
    #[serde(default)]
    submitted: u64,
    #[serde(default)]
    total: u64,
}

/// Response to `GET /tx/{txid}`.
#[derive(Debug, Clone, Deserialize)]
pub struct ArcadeTxInfo {
    /// Transaction ID.
    pub txid: String,
    /// Current Arcade status.
    #[serde(rename = "txStatus")]
    pub tx_status: String,
    /// Timestamp of the last status change.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Extra info, if any.
    #[serde(rename = "extraInfo", default)]
    pub extra_info: Option<String>,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_rank_ordering() {
        assert!(
            arcade_status_rank(statuses::RECEIVED) < arcade_status_rank(statuses::SENT_TO_NETWORK)
        );
        assert!(
            arcade_status_rank(statuses::SEEN_ON_NETWORK)
                < arcade_status_rank(statuses::SEEN_MULTIPLE_NODES)
        );
        assert!(
            arcade_status_rank(statuses::SEEN_MULTIPLE_NODES) < arcade_status_rank(statuses::MINED)
        );
        assert_eq!(arcade_status_rank(statuses::REJECTED), 0);
        assert_eq!(arcade_status_rank("UNKNOWN_FUTURE_STATUS"), 0);
    }

    #[test]
    fn test_fatal_statuses() {
        assert!(is_fatal_status(statuses::REJECTED));
        assert!(is_fatal_status(statuses::DOUBLE_SPEND_ATTEMPTED));
        assert!(!is_fatal_status(statuses::MINED));
        assert!(!is_fatal_status(statuses::SEEN_ON_NETWORK));
    }

    #[test]
    fn test_sse_parser_single_frame() {
        let mut p = SseFrameParser::new();
        let events = p.push(
            b"id: 1751884800000000000\nevent: status\ndata: {\"txid\":\"aa\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n",
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_deref(), Some("1751884800000000000"));
        assert_eq!(events[0].event.as_deref(), Some("status"));
        assert!(events[0].data.contains("SEEN_ON_NETWORK"));
    }

    #[test]
    fn test_sse_parser_split_across_chunks() {
        let mut p = SseFrameParser::new();
        let events = p.push(b"id: 1\nevent: stat");
        assert!(events.is_empty());
        let events = p.push(b"us\ndata: {\"txid\":\"bb\",\"txStatus\":\"MINED\"}\n");
        assert!(events.is_empty()); // no blank line yet
        let events = p.push(b"\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("status"));
        assert!(events[0].data.contains("MINED"));
    }

    #[test]
    fn test_sse_parser_multiple_frames_and_comments() {
        let mut p = SseFrameParser::new();
        let raw = b": keep-alive\n\nid: 1\nevent: status\ndata: {\"a\":1}\n\nid: 2\nevent: status\ndata: {\"a\":2}\n\n";
        let events = p.push(raw);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id.as_deref(), Some("1"));
        assert_eq!(events[1].id.as_deref(), Some("2"));
    }

    #[test]
    fn test_sse_parser_crlf_and_multiline_data() {
        let mut p = SseFrameParser::new();
        let events = p.push(b"data: line1\r\ndata: line2\r\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line1\nline2");
    }

    #[test]
    fn test_events_url() {
        let c = ArcadeSseClient::new("https://example.test/", "tok123").unwrap();
        assert_eq!(
            c.events_url(),
            "https://example.test/events?callbackToken=tok123"
        );
    }

    #[test]
    fn test_config_builders() {
        let cfg = ArcadeConfig::with_callback_token("t").with_callback_url("https://cb.example");
        assert_eq!(cfg.callback_token.as_deref(), Some("t"));
        assert_eq!(cfg.callback_url.as_deref(), Some("https://cb.example"));
    }

    #[test]
    fn test_arcade_url_trailing_slash_trimmed() {
        let a = Arcade::new("https://example.test/", None, None).unwrap();
        assert_eq!(a.url(), "https://example.test");
        assert_eq!(a.name(), "ArcadeV2");
    }
}
