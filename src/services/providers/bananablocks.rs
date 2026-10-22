//! BananaBlocks service provider: GorillaPool's independent explorer, the
//! third of the services' ladders beside WhatsOnChain and Bitails (the
//! owner's word of 2026-10-22, bsv-wallet-cli#50: each chain scanner has its
//! own rate limits, so when one fails the ladder goes to the next).
//!
//! Provides, live-probed 2026-10-22:
//! - `GET {base}/tx/{txid}/hex`: the raw transaction (text hex); the bytes
//!   must hash to the txid
//! - `GET {base}/txo/{txid}/{vout}/spend`: `{"spent":false}` or
//!   `{"spent":true,"spentTxid":<64 hex>}`, an outpoint's spend status
//! - `GET {base}/tx/{txid}`: `block_hash` / `block_height` once mined
//! - `GET {base}/tx/{txid}/proof`: a TSC proof whose `target` is the block's
//!   merkle root (`"targetType":"merkleRoot"`); `/proof/tsc` is no route
//!
//! No key; 60 requests a minute per provider (a token bucket), and a 429 is
//! a fault returned at once with its Retry-After, never slept on
//! ([`RateGate`]).
//!
//! # API Endpoints
//!
//! - Mainnet: `https://bananablocks.com/api/v1`
//! - Testnet: `https://test.bananablocks.com/api/v1`

use reqwest::{Client, StatusCode};
use std::time::Duration;

use crate::services::providers::pace::RateGate;
use crate::services::traits::{
    merkle_path_note, validate_txid, GetMerklePathResult, GetRawTxResult, GetStatusForTxidsResult,
    GetUtxoStatusResult, TxStatusDetail, NOTE_PROOF,
};
use crate::services::Chain;
use crate::{Error, Result};
use bsv_rs::primitives::sha256d;

/// BananaBlocks mainnet API base.
pub const BANANABLOCKS_MAINNET_URL: &str = "https://bananablocks.com/api/v1";

/// BananaBlocks testnet API base.
pub const BANANABLOCKS_TESTNET_URL: &str = "https://test.bananablocks.com/api/v1";

/// BananaBlocks' published budget: requests a minute, no key.
pub const BANANABLOCKS_PER_MINUTE: u32 = 60;

/// The name this provider answers under (results, notes, call history).
pub const BANANABLOCKS_NAME: &str = "BananaBlocks";

/// How long a 429 without a Retry-After closes the rung: BananaBlocks'
/// budget is a minute's.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);

/// What BananaBlocks says about one outpoint's spend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutpointSpend {
    /// `{"spent": false}`: no spender this index can see.
    Unspent,
    /// `{"spent": true, "spentTxid": ...}`: the named spender (a word; its
    /// bytes bind it).
    SpentBy(String),
}

/// BananaBlocks service provider.
pub struct BananaBlocks {
    client: Client,
    base_url: String,
    gate: RateGate,
}

impl BananaBlocks {
    /// The provider for a chain at its default API base.
    pub fn new(chain: Chain) -> Result<Self> {
        let base = match chain {
            Chain::Main => BANANABLOCKS_MAINNET_URL,
            Chain::Test => BANANABLOCKS_TESTNET_URL,
        };
        Self::with_base(base, None)
    }

    /// The provider at another API base (a mirror, or a local fixture in a
    /// test). `timeout_secs` defaults to 30, as every explorer's.
    pub fn with_base(base_url: &str, timeout_secs: Option<u64>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs.unwrap_or(30)))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            gate: RateGate::new(BANANABLOCKS_NAME, DEFAULT_RETRY_AFTER)
                .with_per_minute(BANANABLOCKS_PER_MINUTE),
        })
    }

    /// The API base (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// One GET through the gate: no request inside a Retry-After window or
    /// with no token left; a 429 closes the rung and is the fault returned.
    async fn get(&self, url: &str) -> Result<(StatusCode, String)> {
        self.gate.admit()?;
        let response = self
            .client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("BananaBlocks request failed: {}", e)))?;
        let status = response.status();
        self.gate.check(status, response.headers())?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::NetworkError(format!("BananaBlocks read failed: {}", e)))?;
        Ok((status, body))
    }

    // =========================================================================
    // Raw Transaction
    // =========================================================================

    /// A transaction's bytes by txid (Rule 28, T8 and T9: a courier of a
    /// foreign ancestor's bytes). The answer is self-verifying: bytes that
    /// do not hash to the txid are a fault. A 404 is "no such transaction";
    /// every other failure is an error ("could not look").
    pub async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        let (status, body) = self
            .get(&format!("{}/tx/{}/hex", self.base_url, txid))
            .await?;
        match status {
            StatusCode::OK => {
                let raw_tx = hex::decode(body.trim()).map_err(|e| {
                    Error::ValidationError(format!("BananaBlocks raw tx hex: {}", e))
                })?;
                validate_txid(&raw_tx, txid)?;
                Ok(GetRawTxResult {
                    name: BANANABLOCKS_NAME.to_string(),
                    txid: txid.to_string(),
                    raw_tx: Some(raw_tx),
                    error: None,
                    could_not_look: false,
                })
            }
            StatusCode::NOT_FOUND => Ok(GetRawTxResult {
                name: BANANABLOCKS_NAME.to_string(),
                txid: txid.to_string(),
                raw_tx: None,
                error: None,
                could_not_look: false,
            }),
            status => Err(Error::ServiceError(format!(
                "BananaBlocks getRawTx failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Outpoint spend
    // =========================================================================

    /// An outpoint's spend status from `/txo/{txid}/{vout}/spend`. Only a
    /// 200 with a boolean `spent` answers: `false` is a clean unspent,
    /// `true` with a well-formed `spentTxid` names the spender. A 404 (the
    /// index does not know the transaction), a 429, a 5xx, a malformed
    /// body, a `spent: true` without a 64-hex spender (or naming the
    /// outpoint's own transaction) are faults, never "unspent".
    pub async fn get_spend(&self, txid: &str, vout: u32) -> Result<OutpointSpend> {
        let (status, body) = self
            .get(&format!("{}/txo/{}/{}/spend", self.base_url, txid, vout))
            .await?;
        parse_spend_body(status, &body, txid)
    }

    /// The outpoint question of `getUtxoStatus` (Rule 28, T10). BananaBlocks
    /// answers an outpoint directly; it has no script's unspent set, so a
    /// question without an outpoint is not one it answers (the ladder does
    /// not ask it; asked directly, the answer is `status: "error"`, never
    /// a negative). `{"spent": false}` is a positive; a named spender is a
    /// negative (which the ladder takes only with a second explorer's).
    pub async fn get_utxo_status(&self, outpoint: Option<&str>) -> Result<GetUtxoStatusResult> {
        let Some(outpoint) = outpoint else {
            return Ok(GetUtxoStatusResult {
                name: BANANABLOCKS_NAME.to_string(),
                status: "error".to_string(),
                is_utxo: None,
                details: Vec::new(),
                error: Some(
                    "BananaBlocks answers an outpoint, not a script's unspent set".to_string(),
                ),
            });
        };
        let (txid, vout) = parse_outpoint(outpoint)?;
        let is_utxo = match self.get_spend(txid, vout).await? {
            OutpointSpend::Unspent => true,
            OutpointSpend::SpentBy(_) => false,
        };
        Ok(GetUtxoStatusResult {
            name: BANANABLOCKS_NAME.to_string(),
            status: "success".to_string(),
            is_utxo: Some(is_utxo),
            details: Vec::new(),
            error: None,
        })
    }

    // =========================================================================
    // Merkle Path
    // =========================================================================

    /// A transaction's inclusion proof (Rule 28, T6 and T7: a courier of a
    /// proof nobody pushed to us; `Services::get_merkle_path` checks its
    /// root against the header service before it is returned).
    ///
    /// `GET /tx/{txid}` first: a 404 means this index does not know the
    /// transaction, which says nothing about the chain (inconclusive, never
    /// "not mined"); no block is a clean "not mined". For a mined
    /// transaction `GET /tx/{txid}/proof` serves a TSC proof whose target is
    /// the merkle root; its nodes must fold to that root, and it is handed
    /// on with the block's hash as its target, so the ladder resolves the
    /// header from the header service as for every TSC courier.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        let (status, body) = self.get(&format!("{}/tx/{}", self.base_url, txid)).await?;
        let block = match status {
            StatusCode::OK => parse_tx_block(&body, txid)?,
            StatusCode::NOT_FOUND => {
                return Ok(no_path(None, "getMerklePathNotIndexed"));
            }
            status => {
                return Err(Error::ServiceError(format!(
                    "BananaBlocks tx failed with status {}",
                    status
                )))
            }
        };
        let Some((block_hash, _height)) = block else {
            return Ok(no_path(None, "getMerklePathNotMined"));
        };

        let (status, body) = self
            .get(&format!("{}/tx/{}/proof", self.base_url, txid))
            .await?;
        if status != StatusCode::OK {
            return Err(Error::ServiceError(format!(
                "BananaBlocks proof failed with status {} for a transaction it places in block {}",
                status, block_hash
            )));
        }
        let proof = parse_tsc_proof(&body, txid, &block_hash)?;
        Ok(GetMerklePathResult {
            name: Some(BANANABLOCKS_NAME.to_string()),
            merkle_path: Some(proof),
            header: None,
            error: None,
            notes: vec![merkle_path_note(BANANABLOCKS_NAME, NOTE_PROOF, None)],
        })
    }

    // =========================================================================
    // Transaction Status
    // =========================================================================

    /// Status for txids (Rule 28, T11 and T12), one `GET /tx/{txid}` each:
    /// in a block is `mined` at depth 1 (the tip is the header service's,
    /// T15), no block is `known`, a 404 is `unknown`. The index lags the
    /// mempool, so the ladder treats this provider's `unknown` as a gap and
    /// its `known` as a word (the ladder's `is_chain_index` is
    /// false). A fault stops the batch: the txids not yet looked at are left
    /// out (no word), and a batch where nothing was looked at is the fault.
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        let mut results = Vec::new();
        let mut fault: Option<Error> = None;
        for txid in txids {
            let answer = match self.get(&format!("{}/tx/{}", self.base_url, txid)).await {
                Ok((StatusCode::OK, body)) => parse_tx_block(&body, txid).map(|b| match b {
                    Some(_) => ("mined", Some(1)),
                    None => ("known", Some(0)),
                }),
                Ok((StatusCode::NOT_FOUND, _)) => Ok(("unknown", None)),
                Ok((status, _)) => Err(Error::ServiceError(format!(
                    "BananaBlocks tx failed with status {}",
                    status
                ))),
                Err(e) => Err(e),
            };
            match answer {
                Ok((status, depth)) => results.push(TxStatusDetail {
                    txid: txid.clone(),
                    status: status.to_string(),
                    depth,
                    ..Default::default()
                }),
                Err(e) => {
                    fault = Some(e);
                    break;
                }
            }
        }
        if results.is_empty() {
            if let Some(e) = fault {
                return Err(e);
            }
        }
        Ok(GetStatusForTxidsResult {
            name: BANANABLOCKS_NAME.to_string(),
            status: "success".to_string(),
            error: fault.map(|e| format!("stopped early: {}", e)),
            results,
        })
    }
}

/// `txid.vout` as the services' outpoints are written.
fn parse_outpoint(outpoint: &str) -> Result<(&str, u32)> {
    let (txid, vout) = outpoint
        .split_once('.')
        .ok_or_else(|| Error::InvalidArgument(format!("outpoint {outpoint}: not txid.vout")))?;
    if !is_hex64(txid) {
        return Err(Error::InvalidArgument(format!(
            "outpoint {outpoint}: not a txid"
        )));
    }
    let vout = vout
        .parse::<u32>()
        .map_err(|_| Error::InvalidArgument(format!("outpoint {outpoint}: not a vout")))?;
    Ok((txid, vout))
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A char-boundary-safe excerpt of a body for a fault message.
fn excerpt(body: &str) -> &str {
    let mut cut = body.len().min(120);
    while !body.is_char_boundary(cut) {
        cut -= 1;
    }
    &body[..cut]
}

/// The `/txo/{txid}/{vout}/spend` rule (the overlay's
/// `parse_bananablocks_spent`, in production since 2026-09-04): only a 200
/// with a boolean `spent` answers.
pub(crate) fn parse_spend_body(
    status: StatusCode,
    body: &str,
    outpoint_txid: &str,
) -> Result<OutpointSpend> {
    if status != StatusCode::OK {
        return Err(Error::ServiceError(format!(
            "BananaBlocks spend: HTTP {}: {}",
            status,
            excerpt(body)
        )));
    }
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        Error::ServiceError(format!(
            "BananaBlocks spend: bad json: {}: {}",
            e,
            excerpt(body)
        ))
    })?;
    match v.get("spent").and_then(serde_json::Value::as_bool) {
        Some(false) => Ok(OutpointSpend::Unspent),
        Some(true) => match v.get("spentTxid").and_then(|t| t.as_str()) {
            Some(t) if is_hex64(t) && !t.eq_ignore_ascii_case(outpoint_txid) => {
                Ok(OutpointSpend::SpentBy(t.to_ascii_lowercase()))
            }
            _ => Err(Error::ServiceError(format!(
                "BananaBlocks spend: spent:true without a well-formed spentTxid: {}",
                excerpt(body)
            ))),
        },
        None => Err(Error::ServiceError(format!(
            "BananaBlocks spend: no boolean `spent` (schema drift?): {}",
            excerpt(body)
        ))),
    }
}

/// `GET /tx/{txid}` → the block `(hash, height)` it is in, `None` when it is
/// in none. A body for another txid, or a half-given block, is a fault.
pub(crate) fn parse_tx_block(body: &str, txid: &str) -> Result<Option<(String, u32)>> {
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        Error::ServiceError(format!(
            "BananaBlocks tx: bad json: {}: {}",
            e,
            excerpt(body)
        ))
    })?;
    match v.get("txid").and_then(|t| t.as_str()) {
        Some(t) if t.eq_ignore_ascii_case(txid) => {}
        _ => {
            return Err(Error::ServiceError(format!(
                "BananaBlocks tx: the body is not about {}: {}",
                txid,
                excerpt(body)
            )))
        }
    }
    let hash = v
        .get("block_hash")
        .and_then(|h| h.as_str())
        .filter(|h| !h.is_empty());
    let height = v
        .get("block_height")
        .and_then(|h| h.as_u64())
        .filter(|h| *h > 0);
    match (hash, height) {
        (None, None) => Ok(None),
        (Some(hash), Some(height)) if is_hex64(hash) && height <= u64::from(u32::MAX) => {
            Ok(Some((hash.to_ascii_lowercase(), height as u32)))
        }
        _ => Err(Error::ServiceError(format!(
            "BananaBlocks tx: a malformed block: {}",
            excerpt(body)
        ))),
    }
}

/// The `/tx/{txid}/proof` TSC body, checked and handed on with the block's
/// hash as its target. Its `txOrId` must be the txid, its nodes 64 hex (or
/// `*`), and they must fold to its target merkle root.
pub(crate) fn parse_tsc_proof(body: &str, txid: &str, block_hash: &str) -> Result<String> {
    let bad =
        |why: &str| Error::ServiceError(format!("BananaBlocks proof: {}: {}", why, excerpt(body)));
    let v: serde_json::Value = serde_json::from_str(body).map_err(|_| bad("bad json"))?;
    let index = v
        .get("index")
        .and_then(|i| i.as_u64())
        .ok_or_else(|| bad("no index"))?;
    match v.get("txOrId").and_then(|t| t.as_str()) {
        Some(t) if t.eq_ignore_ascii_case(txid) => {}
        _ => return Err(bad("txOrId is not the txid")),
    }
    if v.get("targetType").and_then(|t| t.as_str()) != Some("merkleRoot") {
        return Err(bad("target is not a merkle root"));
    }
    let root = v
        .get("target")
        .and_then(|t| t.as_str())
        .filter(|t| is_hex64(t))
        .ok_or_else(|| bad("no merkle root"))?;
    let nodes: Vec<String> = v
        .get("nodes")
        .and_then(|n| n.as_array())
        .ok_or_else(|| bad("no nodes"))?
        .iter()
        .map(|n| match n.as_str() {
            Some(s) if s == "*" || is_hex64(s) => Ok(s.to_ascii_lowercase()),
            _ => Err(bad("a node is not a hash")),
        })
        .collect::<Result<_>>()?;
    if fold_tsc(txid, index, &nodes)? != root.to_ascii_lowercase() {
        return Err(bad("the nodes do not fold to the merkle root"));
    }
    Ok(serde_json::json!({
        "index": index,
        "txOrId": txid.to_ascii_lowercase(),
        "target": block_hash,
        "nodes": nodes,
    })
    .to_string())
}

/// Fold a TSC path to its root (display-order hex).
fn fold_tsc(txid: &str, mut index: u64, nodes: &[String]) -> Result<String> {
    let to_internal = |h: &str| -> Result<Vec<u8>> {
        let mut b = hex::decode(h).map_err(|e| Error::ServiceError(e.to_string()))?;
        b.reverse();
        Ok(b)
    };
    let mut current = to_internal(txid)?;
    for node in nodes {
        let sibling = if node == "*" {
            current.clone()
        } else {
            to_internal(node)?
        };
        let mut joined = Vec::with_capacity(64);
        if index & 1 == 1 {
            joined.extend_from_slice(&sibling);
            joined.extend_from_slice(&current);
        } else {
            joined.extend_from_slice(&current);
            joined.extend_from_slice(&sibling);
        }
        current = sha256d(&joined).to_vec();
        index >>= 1;
    }
    current.reverse();
    Ok(hex::encode(current))
}

fn no_path(error: Option<String>, what: &str) -> GetMerklePathResult {
    GetMerklePathResult {
        name: Some(BANANABLOCKS_NAME.to_string()),
        merkle_path: None,
        header: None,
        error,
        notes: vec![merkle_path_note(BANANABLOCKS_NAME, what, None)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::traits::ProviderVerdict;
    use std::time::Instant;

    const OUT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const SPENDER: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    async fn fixture() -> (mockito::ServerGuard, BananaBlocks) {
        let server = mockito::Server::new_async().await;
        let bb = BananaBlocks::with_base(&server.url(), None).unwrap();
        (server, bb)
    }

    async fn spend_route(
        server: &mut mockito::ServerGuard,
        status: usize,
        body: &str,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/txo/{OUT}/0/spend").as_str())
            .with_status(status)
            .with_body(body)
            .create_async()
            .await
    }

    /// The live shapes (2026-10-22): `spent:false` is a clean unspent, and
    /// `spent:true` with a 64-hex `spentTxid` names the spender (lowered).
    #[tokio::test]
    async fn the_spend_answers_unspent_and_names_a_spender() {
        let (mut server, bb) = fixture().await;
        let _m = spend_route(
            &mut server,
            200,
            &format!(r#"{{"spent":false,"txid":"{OUT}","vout":0}}"#),
        )
        .await;
        assert_eq!(bb.get_spend(OUT, 0).await.unwrap(), OutpointSpend::Unspent);

        let (mut server, bb) = fixture().await;
        let _m = spend_route(
            &mut server,
            200,
            &format!(
                r#"{{"spendScope":"confirmed","spent":true,"spentTxid":"{}","txid":"{OUT}","vout":0}}"#,
                SPENDER.to_ascii_uppercase()
            ),
        )
        .await;
        assert_eq!(
            bb.get_spend(OUT, 0).await.unwrap(),
            OutpointSpend::SpentBy(SPENDER.to_string())
        );
    }

    /// Every other spend answer is a fault ("could not look"), never
    /// "unspent": a 404 (the index does not know the transaction), a 5xx,
    /// a malformed body, no boolean `spent`, `spent:true` without a
    /// spender, a spender that is not 64 hex, one naming the outpoint's own
    /// transaction.
    #[tokio::test]
    async fn every_other_spend_answer_is_a_fault() {
        let not_hex = format!(r#"{{"spent":true,"spentTxid":"{}"}}"#, "zz".repeat(32));
        let short = r#"{"spent":true,"spentTxid":"abcd"}"#.to_string();
        let itself = format!(r#"{{"spent":true,"spentTxid":"{OUT}"}}"#);
        for (status, body) in [
            (404, r#"{"error":"Transaction not found"}"#.to_string()),
            (500, "boom".to_string()),
            (502, String::new()),
            (200, "<html>".to_string()),
            (200, "{}".to_string()),
            (200, r#"{"spent":"false"}"#.to_string()),
            (200, r#"{"spent":true}"#.to_string()),
            (200, not_hex),
            (200, short),
            (200, itself),
        ] {
            let (mut server, bb) = fixture().await;
            let _m = spend_route(&mut server, status, &body).await;
            let answer = bb.get_spend(OUT, 0).await;
            assert!(answer.is_err(), "{status} {body}: {answer:?}");
            let utxo = bb.get_utxo_status(Some(&format!("{OUT}.0"))).await;
            assert!(utxo.is_err(), "{status} {body}: {utxo:?}");
        }
    }

    /// A 429 is a fault returned at once with its Retry-After, never slept
    /// on; the next call inside the window makes no request.
    #[tokio::test]
    async fn a_429_is_a_fault_at_once_and_the_rung_is_skipped_until_retry_after() {
        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", format!("/txo/{OUT}/0/spend").as_str())
            .with_status(429)
            .with_header("retry-after", "17")
            .expect(1)
            .create_async()
            .await;
        let started = Instant::now();
        match bb.get_spend(OUT, 0).await {
            Err(Error::RateLimited {
                provider,
                retry_after_secs,
            }) => {
                assert_eq!(provider, BANANABLOCKS_NAME);
                assert_eq!(retry_after_secs, 17);
            }
            other => panic!("a 429 is a rate-limit fault: {other:?}"),
        }
        match bb.get_raw_tx(OUT).await {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert!((16..=17).contains(&retry_after_secs)),
            other => panic!("inside Retry-After the rung is skipped: {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "no sleep: {:?}",
            started.elapsed()
        );
        route.assert_async().await;
    }

    /// 60 a minute: the burst is 30, and the 31st call at once is a
    /// rate-limit fault without a request.
    #[tokio::test]
    async fn the_provider_paces_itself_at_sixty_a_minute() {
        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", format!("/txo/{OUT}/0/spend").as_str())
            .with_status(200)
            .with_body(r#"{"spent":false}"#)
            .expect(30)
            .create_async()
            .await;
        for _ in 0..30 {
            assert_eq!(bb.get_spend(OUT, 0).await.unwrap(), OutpointSpend::Unspent);
        }
        assert!(matches!(
            bb.get_spend(OUT, 0).await,
            Err(Error::RateLimited { .. })
        ));
        route.assert_async().await;
    }

    /// The outpoint question: `spent:false` is a positive, a spender a
    /// negative; a script's question (no outpoint) is not one it answers,
    /// and asks nothing.
    #[tokio::test]
    async fn the_utxo_answer_is_by_outpoint_only() {
        let (mut server, bb) = fixture().await;
        let _m = spend_route(&mut server, 200, r#"{"spent":false}"#).await;
        let r = bb.get_utxo_status(Some(&format!("{OUT}.0"))).await.unwrap();
        assert_eq!((r.status.as_str(), r.is_utxo), ("success", Some(true)));

        let (mut server, bb) = fixture().await;
        let _m = spend_route(
            &mut server,
            200,
            &format!(r#"{{"spent":true,"spentTxid":"{SPENDER}"}}"#),
        )
        .await;
        let r = bb.get_utxo_status(Some(&format!("{OUT}.0"))).await.unwrap();
        assert_eq!((r.status.as_str(), r.is_utxo), ("success", Some(false)));

        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let r = bb.get_utxo_status(None).await.unwrap();
        assert_eq!((r.status.as_str(), r.is_utxo), ("error", None));
        assert!(bb.get_utxo_status(Some("nonsense")).await.is_err());
        route.assert_async().await;
    }

    /// A one-input transaction and its txid.
    fn a_tx() -> (String, String) {
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        let mut input = TransactionInput::new(OUT.to_string(), 0);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        tx.inputs.push(input);
        tx.outputs.push(TransactionOutput {
            satoshis: Some(1),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        (tx.id(), tx.to_hex())
    }

    /// Bytes that hash to the txid are served; bytes that do not are a
    /// fault, never served; a 404 is "no such transaction"; a 5xx a fault.
    #[tokio::test]
    async fn raw_tx_bytes_must_hash_to_the_txid() {
        let (txid, hex) = a_tx();
        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{txid}/hex").as_str())
            .with_status(200)
            .with_body(format!("{hex}\n"))
            .create_async()
            .await;
        let r = bb.get_raw_tx(&txid).await.unwrap();
        assert_eq!(r.raw_tx, Some(hex::decode(&hex).unwrap()));
        assert_eq!(r.name, BANANABLOCKS_NAME);

        let other = "33".repeat(32);
        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(200)
            .with_body(&hex)
            .create_async()
            .await;
        assert!(bb.get_raw_tx(&other).await.is_err(), "bytes of another tx");

        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(404)
            .with_body(r#"{"error":"Transaction not found"}"#)
            .create_async()
            .await;
        let r = bb.get_raw_tx(&other).await.unwrap();
        assert!(r.raw_tx.is_none() && !r.could_not_look && r.error.is_none());

        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(503)
            .create_async()
            .await;
        assert!(bb.get_raw_tx(&other).await.is_err());
    }

    /// A two-transaction block: txid `a` at index 0, `b` its sibling.
    fn two_tx_block() -> (String, String, String) {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let root = fold_tsc(&a, 0, std::slice::from_ref(&b)).unwrap();
        (a, b, root)
    }

    fn tx_body(txid: &str, block: Option<(&str, u32)>) -> String {
        match block {
            Some((hash, height)) => format!(
                r#"{{"txid":"{txid}","block_hash":"{hash}","block_height":{height},"confirmations":3}}"#
            ),
            None => format!(r#"{{"txid":"{txid}","confirmations":0}}"#),
        }
    }

    fn verdicts(r: &GetMerklePathResult) -> Vec<ProviderVerdict> {
        r.provider_verdicts()
    }

    /// The live proof shape (`targetType: merkleRoot`) is checked (its
    /// nodes fold to its root) and handed on with the block's hash as its
    /// target, for the ladder to resolve against the header service.
    #[tokio::test]
    async fn a_mined_transactions_proof_is_handed_on_with_the_block_hash() {
        let (a, b, root) = two_tx_block();
        let block_hash = "cc".repeat(32);
        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, Some((&block_hash, 900_000))))
            .create_async()
            .await;
        let _proof = server
            .mock("GET", format!("/tx/{a}/proof").as_str())
            .with_status(200)
            .with_body(format!(
                r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["{b}"]}}"#
            ))
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        let mp: serde_json::Value =
            serde_json::from_str(r.merkle_path.as_deref().unwrap()).unwrap();
        assert_eq!(mp["target"], block_hash);
        assert_eq!(mp["txOrId"], a);
        assert_eq!(mp["index"], 0);
        assert_eq!(mp["nodes"], serde_json::json!([b]));
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::Proof {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
    }

    /// A proof whose nodes do not fold to its root, or for another txid,
    /// or of another target type, is a fault; so is a 404 at `/proof` for
    /// a transaction the index placed in a block.
    #[tokio::test]
    async fn a_proof_that_does_not_hold_is_a_fault() {
        let (a, b, root) = two_tx_block();
        let block_hash = "cc".repeat(32);
        let wrong = "dd".repeat(32);
        for (status, body) in [
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{wrong}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":1,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{b}","targetType":"merkleRoot","target":"{root}","nodes":["{a}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"header","target":"{root}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["xyz"]}}"#
                ),
            ),
            (404, r#"{"error":"Transaction not confirmed"}"#.to_string()),
        ] {
            let (mut server, bb) = fixture().await;
            let _tx = server
                .mock("GET", format!("/tx/{a}").as_str())
                .with_status(200)
                .with_body(tx_body(&a, Some((&block_hash, 900_000))))
                .create_async()
                .await;
            let _proof = server
                .mock("GET", format!("/tx/{a}/proof").as_str())
                .with_status(status)
                .with_body(&body)
                .create_async()
                .await;
            let r = bb.get_merkle_path(&a).await;
            assert!(r.is_err(), "{status} {body}: {r:?}");
        }
    }

    /// No block is a clean "not mined"; a 404 at `/tx` says only that this
    /// index does not know the transaction: inconclusive, never "not
    /// mined" (a reorg re-prove never demotes on it). No proof is asked
    /// for either.
    #[tokio::test]
    async fn not_mined_and_not_indexed_are_told_apart() {
        let (a, _, _) = two_tx_block();
        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, None))
            .create_async()
            .await;
        let proof = server
            .mock("GET", format!("/tx/{a}/proof").as_str())
            .expect(0)
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        assert!(r.merkle_path.is_none());
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::NotMined {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
        proof.assert_async().await;

        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(404)
            .with_body(r#"{"error":"Transaction not found"}"#)
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        assert!(r.merkle_path.is_none());
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::Inconclusive {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
    }

    /// `/tx/{txid}` per txid: in a block is mined at depth 1 (no tip read),
    /// no block is known, a 404 unknown. A fault stops the batch and the
    /// txids not looked at are left out; a fault on the first is the error.
    #[tokio::test]
    async fn the_status_answers_and_a_fault_leaves_the_rest_out() {
        let (a, b, _) = two_tx_block();
        let c = "ee".repeat(32);
        let d = "ff".repeat(32);
        let (mut server, bb) = fixture().await;
        let _a = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, Some((&"cc".repeat(32), 900_000))))
            .create_async()
            .await;
        let _b = server
            .mock("GET", format!("/tx/{b}").as_str())
            .with_status(200)
            .with_body(tx_body(&b, None))
            .create_async()
            .await;
        let _c = server
            .mock("GET", format!("/tx/{c}").as_str())
            .with_status(404)
            .create_async()
            .await;
        let _d = server
            .mock("GET", format!("/tx/{d}").as_str())
            .with_status(429)
            .with_header("retry-after", "5")
            .create_async()
            .await;
        let txids = vec![a.clone(), b.clone(), c.clone(), d.clone(), a.clone()];
        let r = bb.get_status_for_txids(&txids).await.unwrap();
        let got: Vec<(&str, &str, Option<u32>)> = r
            .results
            .iter()
            .map(|d| (d.txid.as_str(), d.status.as_str(), d.depth))
            .collect();
        assert_eq!(
            got,
            vec![
                (a.as_str(), "mined", Some(1)),
                (b.as_str(), "known", Some(0)),
                (c.as_str(), "unknown", None),
            ]
        );
        assert!(r
            .error
            .as_deref()
            .is_some_and(|e| e.contains("Rate limited")));

        let (mut server, bb) = fixture().await;
        let _a = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(500)
            .create_async()
            .await;
        assert!(bb
            .get_status_for_txids(std::slice::from_ref(&a))
            .await
            .is_err());
    }

    #[test]
    fn the_tsc_fold_matches_the_live_block() {
        // Block 972169, txid c375f88e… at index 1, as the 2026-10-22 probe
        // served it: the path folds to the header service's merkle root.
        let nodes: Vec<String> = [
            "bc0016e76dda341b06e625c41a1d3a313fdf1eedc58f1254b8ba2564422c1763",
            "0bdf27ab5f900d8a3f2342c12ad7bb752b57dd803cbddf1b137eb80589f23604",
            "84699274058213409e91dddfd5a1707388ce5c4415c110e789c9a46ddc6438ba",
            "30b54bed547e704b448cabf8d4a767449cbef235524240cd31a603563d8f1141",
            "fce35d818426c5fca55c0e37910df80f51f99c1890cda9644efc3dcf67757c36",
            "e4c8d64c64a2bb59fdafc3a453bfabec04bc1827e15316d2051b52e646b720ea",
            "5d77c1a44f469d03aae0a677c77e5015be3b643363a09626d82645653b81778e",
            "70806389e388214fc269dbd2c4a9e58011ada124c8501c26e4a69c5490075cf1",
            "0e996b88155cd8e10e694374eaecec0e8310359d3d7b70a0477e864d6e16c517",
            "fa0a715902e46ba19935b54daa163d041c071719f95770021c84876ab6cc5cf9",
            "4a29b4ff46d188e901b7203e1d74bbc9efc1f89eb2ab1d7d655dd31ec491a15f",
            "ef4eba8c0fab7e7179e57fbcb8ed480250aee9c35c3b72e37b4e4f7981fdf0c0",
            "d82f7a0829d70b35a192426de0d41e6ed34062cdc356fffa66c59f9765371314",
            "56c96ac4d7041fa2b3c622d3709791f11881054bc14d496e3cc99aca45aa891d",
            "222c3c0f643aca0b2ab6a1e0ffaa3593562a47a177cf4413a770d4414faf1b2f",
            "b5b99ec3ba26d0c561994a652710b415d69ea6da6f736468975273fbce12ec66",
            "3af74674dc467f3ccd5f982d30e66bf561bc78b7484dccad47ac74c900d57645",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            fold_tsc(
                "c375f88e925f504dbe440102b368087009db1fc70e55382fc9cdbc15b1c82076",
                1,
                &nodes
            )
            .unwrap(),
            "79f8aaee78f918ac6588ec3f308732549e61caacf9fb9489d46938f834b2a557"
        );
    }
}
