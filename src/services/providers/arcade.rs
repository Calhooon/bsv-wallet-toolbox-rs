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
//! Arcade is also a READ path. `GET /tx/{txid}` answers `MINED` with
//! `blockHeight`, `blockHash` and `merklePath` (BUMP) for a mined
//! transaction, so [`Arcade::get_merkle_path`] serves proofs and
//! [`Arcade::get_status_for_txids`] serves batch triage from our own
//! broadcaster instead of a third-party indexer. Both are registered FIRST in
//! their service collections when Arcade is configured, with WhatsOnChain and
//! Bitails kept behind them as failover.
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

use bsv_rs::transaction::MerklePath;
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::services::traits::{
    BlockHeader, GetMerklePathResult, GetStatusForTxidsResult, PostBeefDelivery, PostBeefResult,
    PostTxResultForTxid, TxStatusDetail,
};
use crate::{Error, Result};

/// How many `GET /tx/{txid}` status calls a batch triage runs at once.
///
/// Arcade answers a single-txid status in a few milliseconds; the bound is
/// there to keep a several-hundred-txid wallet from opening a connection per
/// transaction, not because Arcade is fragile.
pub const ARCADE_STATUS_CONCURRENCY: usize = 8;

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

    /// Headers for read requests (`GET /tx/{txid}`).
    ///
    /// The same authentication the broadcaster sends: the `X-CallbackToken`
    /// and any additional configured headers (where an API key or
    /// `Authorization` lives). Submit-only headers (content type, status
    /// verbosity, validation skips, callback URL) have no meaning on a GET
    /// and are not sent.
    fn read_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(ref token) = self.config.callback_token {
            if let Ok(v) = token.parse() {
                headers.insert("X-CallbackToken", v);
            }
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
    ///
    /// The full batch every time (no seen set); see [`Arcade::post_beef_seen`]
    /// for the reduced send a broadcast memory enables.
    pub async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef_seen(beef, txids, &HashSet::new())
            .await
            .map(|(result, _)| result)
    }

    /// [`Arcade::post_beef`] with this Arcade's seen set (txids it already
    /// accepted or reported, or that are mined, from a
    /// [`BroadcastMemory`](crate::services::BroadcastMemory)): unproven
    /// transactions in `seen` stay out of the EF batch, the subject is always
    /// sent ([`beef_to_ef_batch_skipping`]).
    ///
    /// If Arcade refuses the reduced batch for what reads as a missing parent
    /// (a 4xx whose text matches [`missing_parent_hint`], or a `REJECTED`
    /// verdict, which is also how Arcade surfaces mempool orphans), the full
    /// batch is sent ONCE more and the fallback is logged. The verdict
    /// handling stays asynchronous (SSE / webhook / polling) exactly as for
    /// a full submit.
    ///
    /// Returns the result plus the [`PostBeefDelivery`]: bytes actually sent,
    /// whether the send was reduced, and every txid the batch handed Arcade
    /// (all of them on a `202`; `/txs` duplicates are txids Arcade already
    /// had).
    pub async fn post_beef_seen(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
    ) -> Result<(PostBeefResult, PostBeefDelivery)> {
        let mut result = PostBeefResult {
            name: self.name.clone(),
            status: "success".to_string(),
            txid_results: Vec::new(),
            error: None,
            notes: Vec::new(),
        };
        let mut delivery = PostBeefDelivery::default();

        // Timing is logged at info: where a wallet's broadcast spends its time
        // (EF conversion vs the Arcade round trip) is the number a caller
        // waits on, so it must be observable without a debugger.
        let submit_started = std::time::Instant::now();
        let batch = match beef_to_ef_batch_skipping(beef, seen) {
            Ok(b) => b,
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
                return Ok((result, delivery));
            }
        };

        if batch.entries.is_empty() {
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
            return Ok((result, delivery));
        }

        let ef_ms = submit_started.elapsed().as_millis();
        delivery.reduced = !batch.skipped.is_empty();
        let mut delivered = batch;
        let mut submit_outcome = self
            .submit_ef_batch(&delivered, ef_ms, delivery.reduced, &mut delivery)
            .await;

        if delivery.reduced && reads_as_missing_parent(&submit_outcome) {
            match beef_to_ef_batch_skipping(beef, &HashSet::new()) {
                Ok(full) => {
                    tracing::warn!(
                        name = %self.name,
                        txid = %delivered.subject_txid,
                        skipped = delivered.skipped.len(),
                        refusal = %describe_outcome(&submit_outcome),
                        "Arcade refused the reduced EF batch (missing parent?); retrying once with the full batch"
                    );
                    result
                        .notes
                        .push(make_note(&self.name, "postBeefFallbackFull"));
                    delivery.fallback_full = true;
                    submit_outcome = self.submit_ef_batch(&full, 0, false, &mut delivery).await;
                    delivered = full;
                }
                Err(e) => {
                    tracing::warn!(
                        name = %self.name,
                        txid = %delivered.subject_txid,
                        error = %e,
                        "Arcade: the full-batch fallback could not be built; reporting the reduced verdict"
                    );
                }
            }
        }
        let subject_txid = delivered.subject_txid.clone();

        match submit_outcome {
            Ok(SubmitOutcome::Accepted { note }) => {
                delivery.accepted_txids =
                    delivered.entries.iter().map(|e| e.txid.clone()).collect();
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
                // Terminal-fatal verdict (REJECTED / DOUBLE_SPEND_ATTEMPTED):
                // DEFINITIVE. `status` carries the explicit rejection marker so
                // `classify_broadcast_results` fails the tx permanently instead
                // of scheduling a retry that can never succeed.
                result.status = "error".to_string();
                result
                    .notes
                    .push(make_note(&self.name, "postBeefFatalStatus"));
                result.txid_results.push(PostTxResultForTxid {
                    txid: subject_txid.clone(),
                    status: crate::storage::broadcast::STATUS_REJECTED.to_string(),
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
            Ok(SubmitOutcome::Rejected { code, detail }) => {
                // HTTP-level rejection of the submission (465 fee too low, 4xx
                // validation): DEFINITIVE, the same bytes can never be accepted.
                result.status = "error".to_string();
                result.notes.push(make_note(&self.name, "postBeefRejected"));
                result.txid_results.push(PostTxResultForTxid {
                    txid: subject_txid.clone(),
                    status: code.to_string(),
                    double_spend: false,
                    orphan_mempool: false,
                    competing_txs: None,
                    data: Some(detail),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![make_note(&self.name, "postBeefRejected")],
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

        Ok((result, delivery))
    }

    /// Submit one EF batch: a single EF to `POST /tx`, several as a binary
    /// concat to `POST /txs`. Logs the submit timing at info and adds the
    /// bytes sent to `delivery`.
    async fn submit_ef_batch(
        &self,
        batch: &EfBatch,
        ef_ms: u128,
        reduced: bool,
        delivery: &mut PostBeefDelivery,
    ) -> Result<SubmitOutcome> {
        let ef_bytes: usize = batch.entries.iter().map(|e| e.ef.len()).sum();
        delivery.bytes_sent += ef_bytes;
        let http_started = std::time::Instant::now();
        let outcome = if batch.entries.len() == 1 {
            self.post_single_ef(&batch.entries[0].ef, &batch.subject_txid)
                .await
        } else {
            self.post_ef_batch(&batch.entries, &batch.subject_txid)
                .await
        };
        tracing::info!(
            name = %self.name,
            txid = %batch.subject_txid,
            txs = batch.entries.len(),
            skipped = batch.skipped.len(),
            reduced,
            bytes = ef_bytes,
            ef_ms,
            http_ms = http_started.elapsed().as_millis(),
            outcome = %describe_outcome(&outcome),
            "Arcade submit timing"
        );
        outcome
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
                    extra_info = ?data.extra_info,
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
                Ok(classify_http_error("Arcade error", status, body))
            }
            Err(e) => Ok(SubmitOutcome::ServiceError {
                detail: format!("Request failed: {}", e),
            }),
        }
    }

    /// Submit multiple EF binaries as binary concat to `POST /txs`.
    async fn post_ef_batch(
        &self,
        entries: &[EfBatchEntry],
        subject_txid: &str,
    ) -> Result<SubmitOutcome> {
        let url = format!("{}/txs", self.url);
        let total_len: usize = entries.iter().map(|e| e.ef.len()).sum();
        let mut body = Vec::with_capacity(total_len);
        for entry in entries {
            body.extend_from_slice(&entry.ef);
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
                // A batch rejection is the subject's rejection too: an invalid
                // ancestor can never make the subject valid.
                Ok(classify_http_error("Arcade batch error", status, body))
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
            .headers(self.read_headers())
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

    /// Merkle path for `txid` from Arcade's own status document
    /// (`GET /tx/{txid}`).
    ///
    /// A `MINED` document carries `merklePath` (BUMP), `blockHeight` and
    /// `blockHash` since arcade v0.10.1, so a wallet broadcasting through
    /// Arcade never has to ask a third-party indexer for the proof.
    ///
    /// Every answer that is not a usable proof (any non-`MINED` status
    /// (`SEEN_ON_NETWORK`, `RECEIVED`, `REJECTED`, ...), a 404, a `MINED`
    /// document with no or partial enrichment, an unparseable BUMP, a
    /// transport failure) comes back as the same "no proof yet" result an
    /// unmined transaction gets from WhatsOnChain: `merkle_path: None`, never
    /// an `Err`, so the collection moves straight on to the next provider.
    ///
    /// The proof is a HINT, never truth. The caller recomputes the root and
    /// validates it against its own headers exactly as it does for every
    /// other provider.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        let info = match self.get_tx_status(txid).await {
            Ok(Some(info)) => info,
            Ok(None) => return Ok(self.no_merkle_path("getMerklePathNotFound", None)),
            Err(e) => {
                return Ok(self.no_merkle_path("getMerklePathServiceError", Some(e.to_string())))
            }
        };

        if info.tx_status != statuses::MINED {
            return Ok(self.no_merkle_path("getMerklePathNotMined", None));
        }

        let Some((bytes, bump)) = status_proof(&info) else {
            return Ok(self.no_merkle_path("getMerklePathNoPath", None));
        };

        let block_height = info.block_height.unwrap_or(bump.block_height);
        if block_height != bump.block_height {
            tracing::warn!(
                txid = %txid,
                doc_height = block_height,
                bump_height = bump.block_height,
                "Arcade status document height disagrees with its own BUMP; dropping the proof"
            );
            return Ok(self.no_merkle_path("getMerklePathHeightMismatch", None));
        }

        // The root of the block this BUMP proves. Recomputed (never taken on
        // trust) and handed on as the header's merkle root so the caller
        // stores the real value instead of a zero placeholder; the caller
        // validates it against its own headers before latching anything.
        let merkle_root = match bump.compute_root(Some(txid)) {
            Ok(root) => root,
            Err(e) => {
                return Ok(self.no_merkle_path("getMerklePathBadProof", Some(e.to_string())));
            }
        };

        // `blockHash` may legitimately be absent (upstream enrichment is
        // `omitempty`): it is informational, validation is root-vs-height
        // against our own headers.
        let block_hash = info.block_hash.clone().unwrap_or_default();

        Ok(GetMerklePathResult {
            name: Some(self.name.clone()),
            merkle_path: Some(hex::encode(&bytes)),
            header: Some(BlockHeader {
                height: block_height,
                hash: block_hash,
                merkle_root,
                ..Default::default()
            }),
            error: None,
            notes: vec![make_note(&self.name, "getMerklePathSuccess")],
        })
    }

    /// Batch triage: the current status of each txid, from Arcade's own
    /// `GET /tx/{txid}` documents.
    ///
    /// One call per txid, [`ARCADE_STATUS_CONCURRENCY`] in flight at a time.
    /// A `MINED` answer carries the proof with it (`merkle_path` /
    /// `block_height` / `block_hash` on the detail), so a caller triaging a
    /// wallet's unmined set can record the proofs it finds without a second
    /// round trip per transaction.
    ///
    /// A txid Arcade has never seen (404) is reported `unknown`: that is an
    /// answer, not a failure. A txid whose own call failed at the transport
    /// is also reported `unknown` and never fails the batch. Only a batch in
    /// which EVERY call failed at the transport comes back as an error, so
    /// the collection falls through to the next status provider instead of
    /// mistaking silence for "nothing is mined".
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        if txids.is_empty() {
            return Ok(GetStatusForTxidsResult {
                name: self.name.clone(),
                status: "success".to_string(),
                error: None,
                results: Vec::new(),
            });
        }

        let answers: HashMap<String, std::result::Result<Option<ArcadeTxInfo>, String>> =
            futures_util::stream::iter(txids.iter().cloned().map(|txid| async move {
                let answer = self.get_tx_status(&txid).await.map_err(|e| e.to_string());
                (txid, answer)
            }))
            .buffer_unordered(ARCADE_STATUS_CONCURRENCY)
            .collect()
            .await;

        let mut unreachable = 0usize;
        let mut last_error = None;
        let mut results = Vec::with_capacity(txids.len());

        for txid in txids {
            match answers.get(txid) {
                Some(Ok(Some(info))) => results.push(status_detail(txid, info)),
                Some(Ok(None)) => results.push(TxStatusDetail::new(txid, "unknown", None)),
                Some(Err(e)) => {
                    unreachable += 1;
                    last_error = Some(e.clone());
                    tracing::debug!(
                        txid = %txid,
                        error = %e,
                        "Arcade status call failed for one txid; reported unknown, batch continues"
                    );
                    results.push(TxStatusDetail::new(txid, "unknown", None));
                }
                None => results.push(TxStatusDetail::new(txid, "unknown", None)),
            }
        }

        if unreachable == txids.len() {
            return Ok(GetStatusForTxidsResult {
                name: self.name.clone(),
                status: "error".to_string(),
                error: Some(format!(
                    "Arcade could not answer any of {} status calls: {}",
                    txids.len(),
                    last_error.unwrap_or_else(|| "unknown error".to_string())
                )),
                results: Vec::new(),
            });
        }

        Ok(GetStatusForTxidsResult {
            name: self.name.clone(),
            status: "success".to_string(),
            error: None,
            results,
        })
    }

    /// A "no proof from here" merkle path result (never an error the
    /// collection could mistake for a verdict).
    fn no_merkle_path(&self, what: &str, error: Option<String>) -> GetMerklePathResult {
        GetMerklePathResult {
            name: Some(self.name.clone()),
            merkle_path: None,
            header: None,
            error,
            notes: vec![make_note(&self.name, what)],
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
    /// A terminal-fatal transaction status in a 2xx body (REJECTED /
    /// DOUBLE_SPEND_ATTEMPTED). Definitive.
    Fatal {
        tx_status: String,
        double_spend: bool,
    },
    /// An HTTP rejection of the submission itself (465 fee too low, 4xx
    /// validation — see `arc::status_codes::is_rejection`). Definitive.
    Rejected {
        code: u16,
        detail: String,
    },
    /// A fault of the service or of our access to it. Transient.
    ServiceError {
        detail: String,
    },
}

/// Map a non-2xx submit response to a definitive rejection or a transient
/// service error, by the same code table classic ARC uses.
fn classify_http_error(prefix: &str, status: reqwest::StatusCode, body: String) -> SubmitOutcome {
    let code = status.as_u16();
    if crate::services::providers::arc::status_codes::is_rejection(code) {
        SubmitOutcome::Rejected {
            code,
            detail: format!("{} rejected: HTTP {} - {}", prefix, status, body),
        }
    } else {
        SubmitOutcome::ServiceError {
            detail: format!("{}: HTTP {} - {}", prefix, status, body),
        }
    }
}

// =============================================================================
// BEEF → EF conversion
// =============================================================================

/// One transaction of an EF batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EfBatchEntry {
    /// Transaction id.
    pub txid: String,
    /// The transaction in Extended Format (BRC-30).
    pub ef: Vec<u8>,
}

/// The EF batch a BEEF converts to; see [`beef_to_ef_batch_skipping`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EfBatch {
    /// EFs to submit, in dependency order (parents before children). The
    /// subject, when unproven, is always last.
    pub entries: Vec<EfBatchEntry>,
    /// The txid of the BEEF's subject (last-sorted) transaction.
    pub subject_txid: String,
    /// Unproven transactions left out because they were in the skip set
    /// (never the subject), in dependency order.
    pub skipped: Vec<String>,
}

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
    let batch = beef_to_ef_batch_skipping(beef, &HashSet::new())?;
    Ok((
        batch.entries.into_iter().map(|e| e.ef).collect(),
        batch.subject_txid,
    ))
}

/// [`beef_to_ef_batch`] minus the unproven transactions in `skip`: the ones
/// the target broadcaster has already accepted or seen (its
/// [`BroadcastMemory`](crate::services::BroadcastMemory) seen set).
///
/// The skip applies at the "emit this EF" decision only: every transaction
/// of the BEEF still goes into the source map, so a child of a skipped parent
/// links its inputs and EF-encodes exactly as before. The subject (the
/// last-sorted transaction) is ALWAYS emitted, even when it is in `skip`.
pub fn beef_to_ef_batch_skipping(beef: &[u8], skip: &HashSet<String>) -> Result<EfBatch> {
    use bsv_rs::transaction::{Beef, Transaction};

    let mut beef = Beef::from_binary(beef)
        .map_err(|e| Error::ServiceError(format!("BEEF parse failed: {}", e)))?;
    // Sort into dependency order: parents before children.
    beef.sort_txs();

    // txid → parsed transaction, for linking input sources one level deep.
    // Parsed BEEF transactions have no sources linked themselves, so the
    // clones stay flat (no recursive blowup). Skipped transactions stay in
    // the map: their children still need them as sources.
    let mut tx_map: HashMap<String, Transaction> = HashMap::with_capacity(beef.txs.len());
    for btx in &beef.txs {
        if let Some(tx) = btx.tx() {
            tx_map.insert(btx.txid(), tx.clone());
        }
    }

    let subject_txid = beef.txs.last().map(|btx| btx.txid()).unwrap_or_default();
    let mut batch = EfBatch {
        entries: Vec::new(),
        subject_txid: subject_txid.clone(),
        skipped: Vec::new(),
    };

    for btx in &beef.txs {
        let txid = btx.txid();

        if btx.has_proof() {
            // Already mined — provides source data for children, nothing to broadcast.
            continue;
        }

        if txid != subject_txid && skip.contains(&txid) {
            // The broadcaster already has it; its outputs still source children.
            batch.skipped.push(txid);
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
        batch.entries.push(EfBatchEntry { txid, ef });
    }

    Ok(batch)
}

/// Whether a broadcaster's refusal text reads as "I do not have the parent":
/// it mentions `missing`, `parent`, `orphan` or `inputs` (case-insensitive).
/// Drives the one-shot full-package fallback after a reduced send.
pub fn missing_parent_hint(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    ["missing", "parent", "orphan", "inputs"]
        .iter()
        .any(|needle| lower.contains(needle))
}

/// Whether a reduced Arcade submit was refused for what reads as a missing
/// parent: an HTTP rejection whose text matches [`missing_parent_hint`], or a
/// `REJECTED` verdict (Arcade surfaces mempool orphans as `REJECTED`; a
/// `DOUBLE_SPEND_ATTEMPTED` is never a parent problem).
fn reads_as_missing_parent(outcome: &Result<SubmitOutcome>) -> bool {
    match outcome {
        Ok(SubmitOutcome::Rejected { detail, .. }) => missing_parent_hint(detail),
        Ok(SubmitOutcome::Fatal {
            tx_status,
            double_spend,
        }) => !double_spend && tx_status == statuses::REJECTED,
        _ => false,
    }
}

/// A one-word label for a submit outcome (logs).
fn describe_outcome(outcome: &Result<SubmitOutcome>) -> &'static str {
    match outcome {
        Ok(SubmitOutcome::Accepted { .. }) => "accepted",
        Ok(SubmitOutcome::Fatal { .. }) => "fatal",
        Ok(SubmitOutcome::Rejected { .. }) => "rejected",
        Ok(SubmitOutcome::ServiceError { .. }) => "service_error",
        Err(_) => "error",
    }
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
///
/// Since arcade v0.10.1 (upstream #259), MINED/IMMUTABLE frames additionally
/// carry `blockHash`, `blockHeight` and `merklePath` (BUMP hex) — the same
/// enriched shape as the webhook callback body. Enrichment is best-effort
/// (`omitempty` upstream): all three fields default to `None`, so frames from
/// older instances and non-mined frames parse unchanged.
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
    /// Block hash — present on MINED/IMMUTABLE frames (arcade ≥ v0.10.1).
    #[serde(rename = "blockHash", default)]
    pub block_hash: Option<String>,
    /// Block height — present on MINED/IMMUTABLE frames (arcade ≥ v0.10.1).
    #[serde(rename = "blockHeight", default)]
    pub block_height: Option<u32>,
    /// BRC-74 BUMP merkle path (hex) — present on MINED/IMMUTABLE frames
    /// (arcade ≥ v0.10.1, best-effort). A hint, never truth: consumers must
    /// SPV-verify against their own headers before latching.
    #[serde(rename = "merklePath", default)]
    pub merkle_path: Option<String>,
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
    #[serde(rename = "extraInfo", default)]
    extra_info: Option<String>,
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
///
/// A `MINED` document additionally carries `blockHash`, `blockHeight` and
/// `merklePath` (BUMP), the same enriched shape as the MINED SSE frame and
/// the webhook callback body (arcade >= v0.10.1). Enrichment is best-effort
/// (`omitempty` upstream), so all three default to `None` and documents from
/// older instances or for unmined transactions parse unchanged.
#[derive(Debug, Clone, Default, Deserialize)]
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
    /// Block hash, present on MINED documents (arcade >= v0.10.1).
    #[serde(rename = "blockHash", default)]
    pub block_hash: Option<String>,
    /// Block height, present on MINED documents (arcade >= v0.10.1).
    #[serde(rename = "blockHeight", default)]
    pub block_height: Option<u32>,
    /// BRC-74 BUMP merkle path, present on MINED documents (arcade >=
    /// v0.10.1, best-effort). A hint, never truth: consumers must SPV-verify
    /// against their own headers before latching.
    #[serde(rename = "merklePath", default)]
    pub merkle_path: Option<String>,
}

/// The byte decodings of an Arcade merkle path worth trying, in order.
///
/// Arcade serializes the BUMP as hex. Base64 is tried as a fallback because
/// the field is an `omitempty` string upstream and has been seen base64 in
/// other ARC-family payloads. Both candidates are offered rather than the
/// first that decodes, so a base64 string that happens to be legal hex still
/// gets its second chance at the BUMP parse.
pub(crate) fn decode_bump_candidates(encoded: &str) -> Vec<Vec<u8>> {
    if encoded.is_empty() {
        return Vec::new();
    }
    let mut candidates = Vec::with_capacity(2);
    if let Ok(bytes) = hex::decode(encoded) {
        candidates.push(bytes);
    }
    if let Ok(bytes) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded) {
        candidates.push(bytes);
    }
    candidates
}

/// The proof material in a status document: `(BUMP bytes, parsed BUMP)`.
///
/// `None` unless the merkle path is present, non-empty, decodable and parses
/// as a BUMP, AND the block height is present. Partial enrichment (upstream
/// is best-effort) falls back to the fetch path rather than latching half an
/// answer: the same rule the SSE inline-proof path applies.
pub(crate) fn status_proof(info: &ArcadeTxInfo) -> Option<(Vec<u8>, MerklePath)> {
    info.block_height?;
    for bytes in decode_bump_candidates(info.merkle_path.as_deref()?) {
        if let Ok(bump) = MerklePath::from_binary(&bytes) {
            return Some((bytes, bump));
        }
    }
    None
}

/// Map one Arcade status document onto a [`TxStatusDetail`], carrying the
/// proof when the document has one.
pub(crate) fn status_detail(txid: &str, info: &ArcadeTxInfo) -> TxStatusDetail {
    if info.tx_status == statuses::MINED {
        // Arcade reports MINED without a confirmation count. Depth 1 is the
        // honest floor: the transaction is in a block.
        let mut detail = TxStatusDetail::new(txid, "mined", Some(1));
        if let Some((bytes, bump)) = status_proof(info) {
            let height = info.block_height.unwrap_or(bump.block_height);
            if height == bump.block_height {
                detail.merkle_path = Some(hex::encode(&bytes));
                detail.block_height = Some(height);
                detail.block_hash = info.block_hash.clone();
            } else {
                tracing::warn!(
                    txid = %txid,
                    doc_height = height,
                    bump_height = bump.block_height,
                    "Arcade status document height disagrees with its own BUMP; proof not carried"
                );
            }
        }
        return detail;
    }
    if is_fatal_status(&info.tx_status) || arcade_status_rank(&info.tx_status) == 0 {
        // REJECTED / DOUBLE_SPEND_ATTEMPTED / anything unrecognised: no
        // proof is coming, and the transaction is not in the mempool.
        return TxStatusDetail::new(txid, "unknown", None);
    }
    // RECEIVED .. SEEN_MULTIPLE_NODES: known to the network, not yet mined.
    TxStatusDetail::new(txid, "known", Some(0))
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

    // =========================================================================
    // Status document -> proof / triage
    // =========================================================================

    fn mined_doc(txid: &str, height: u32) -> ArcadeTxInfo {
        ArcadeTxInfo {
            txid: txid.to_string(),
            tx_status: statuses::MINED.to_string(),
            block_hash: Some("bb".repeat(32)),
            block_height: Some(height),
            merkle_path: Some(MerklePath::from_coinbase_txid(txid, height).to_hex()),
            ..Default::default()
        }
    }

    #[test]
    fn test_decode_bump_candidates_hex_and_base64() {
        let txid = "aa".repeat(32);
        let bytes = MerklePath::from_coinbase_txid(&txid, 850_000).to_binary();

        let from_hex = decode_bump_candidates(&hex::encode(&bytes));
        assert!(from_hex.contains(&bytes), "hex encoding must decode");

        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
        let from_b64 = decode_bump_candidates(&b64);
        assert!(from_b64.contains(&bytes), "base64 encoding must decode");

        assert!(decode_bump_candidates("").is_empty());
    }

    #[test]
    fn test_status_proof_needs_height_and_parseable_path() {
        let txid = "aa".repeat(32);
        let height = 850_000u32;

        let (bytes, bump) = status_proof(&mined_doc(&txid, height)).expect("proof");
        assert_eq!(bump.block_height, height);
        assert_eq!(
            bytes,
            MerklePath::from_coinbase_txid(&txid, height).to_binary()
        );

        // Partial enrichment: height missing.
        let mut no_height = mined_doc(&txid, height);
        no_height.block_height = None;
        assert!(status_proof(&no_height).is_none());

        // Partial enrichment: path missing.
        let mut no_path = mined_doc(&txid, height);
        no_path.merkle_path = None;
        assert!(status_proof(&no_path).is_none());

        // Garbage that is legal hex but not a BUMP.
        let mut garbage = mined_doc(&txid, height);
        garbage.merkle_path = Some("dead".to_string());
        assert!(status_proof(&garbage).is_none());
    }

    #[test]
    fn test_status_proof_accepts_a_base64_merkle_path() {
        let txid = "cc".repeat(32);
        let height = 851_000u32;
        let bytes = MerklePath::from_coinbase_txid(&txid, height).to_binary();

        let mut doc = mined_doc(&txid, height);
        doc.merkle_path = Some(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &bytes,
        ));

        let (decoded, bump) = status_proof(&doc).expect("base64 proof");
        assert_eq!(decoded, bytes);
        assert_eq!(bump.block_height, height);
    }

    #[test]
    fn test_status_detail_mined_carries_the_proof() {
        let txid = "aa".repeat(32);
        let height = 850_000u32;
        let detail = status_detail(&txid, &mined_doc(&txid, height));

        assert_eq!(detail.status, "mined");
        assert_eq!(detail.depth, Some(1));
        assert_eq!(detail.block_height, Some(height));
        assert_eq!(detail.block_hash.as_deref(), Some("bb".repeat(32).as_str()));
        assert_eq!(
            detail.merkle_path.as_deref(),
            Some(
                MerklePath::from_coinbase_txid(&txid, height)
                    .to_hex()
                    .as_str()
            )
        );
    }

    #[test]
    fn test_status_detail_height_mismatch_drops_the_proof() {
        let txid = "aa".repeat(32);
        let mut doc = mined_doc(&txid, 850_000);
        doc.block_height = Some(850_001); // disagrees with its own BUMP

        let detail = status_detail(&txid, &doc);
        assert_eq!(detail.status, "mined");
        assert!(
            detail.merkle_path.is_none(),
            "an incoherent document must not carry a proof"
        );
    }

    #[test]
    fn test_status_detail_in_flight_is_known_and_fatal_is_unknown() {
        let txid = "aa".repeat(32);

        for status in [
            statuses::RECEIVED,
            statuses::SENT_TO_NETWORK,
            statuses::ACCEPTED_BY_NETWORK,
            statuses::SEEN_ON_NETWORK,
            statuses::SEEN_MULTIPLE_NODES,
        ] {
            let doc = ArcadeTxInfo {
                txid: txid.clone(),
                tx_status: status.to_string(),
                ..Default::default()
            };
            let detail = status_detail(&txid, &doc);
            assert_eq!(detail.status, "known", "{} is in the mempool", status);
            assert_eq!(detail.depth, Some(0));
            assert!(detail.merkle_path.is_none());
        }

        for status in [
            statuses::REJECTED,
            statuses::DOUBLE_SPEND_ATTEMPTED,
            "SOME_FUTURE_STATUS",
        ] {
            let doc = ArcadeTxInfo {
                txid: txid.clone(),
                tx_status: status.to_string(),
                ..Default::default()
            };
            assert_eq!(status_detail(&txid, &doc).status, "unknown", "{}", status);
        }
    }
}
