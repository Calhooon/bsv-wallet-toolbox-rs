//! Chaintracks HTTP service client.
//!
//! Provides block header lookups and merkle root validation via a remote
//! Chaintracks server. Also implements the `ChainTracker` trait from bsv-sdk.
//!
//! Every request passes the ladder's Retry-After gate, the explorers' own
//! ([`RateGate`]; rule 28 read by the keeper on 2026-10-31, #148): a 429
//! from the header service, ours included, closes the rung until its
//! Retry-After passes, and a call inside that window is
//! [`Error::RateLimited`] without a request.

use std::sync::Arc;
use std::time::Duration;

use crate::services::providers::http_client::Client;
use crate::services::providers::pace::{shared_key, RateGate};
use crate::services::{traits::BlockHeader, Chain};
use crate::{Error, Result};
use async_trait::async_trait;
use bsv_rs::transaction::beef_stream::ChainTip;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use serde::{Deserialize, Serialize};

/// Chaintracks service client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaintracksConfig {
    /// Chaintracks API URL (e.g. `https://api.calhouninfra.com`).
    pub url: String,
    /// Optional API key.
    pub api_key: Option<String>,
}

/// Generic response frame from Chaintracks API.
///
/// All endpoints return `{"status": "success"|"error", "value": T}`.
#[derive(Debug, Deserialize)]
pub struct ResponseFrame<T> {
    pub status: String,
    pub value: Option<T>,
}

impl<T> ResponseFrame<T> {
    fn is_success(&self) -> bool {
        self.status == "success"
    }
}

/// Block header as returned by the Chaintracks API (camelCase JSON).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CtBlockHeader {
    pub version: u32,
    pub previous_hash: String,
    pub merkle_root: String,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    pub height: u32,
    pub hash: String,
}

impl From<CtBlockHeader> for BlockHeader {
    fn from(ct: CtBlockHeader) -> Self {
        BlockHeader {
            version: ct.version,
            previous_hash: ct.previous_hash,
            merkle_root: ct.merkle_root,
            time: ct.time,
            bits: ct.bits,
            nonce: ct.nonce,
            height: ct.height,
            hash: ct.hash,
        }
    }
}

/// How long a 429 with no Retry-After closes the rung: WhatsOnChain's and
/// Bitails' default.
const CHAINTRACKS_DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

/// Chaintracks HTTP service client.
///
/// Provides block header lookups via a remote Chaintracks server.
/// Follows the same provider pattern as `BlockHeaderService` (BHS).
pub struct ChaintracksServiceClient {
    client: Client,
    config: ChaintracksConfig,
    chain: tokio::sync::OnceCell<Chain>,
    /// The process's gate for this base and key ([`RateGate::shared`]).
    gate: Arc<RateGate>,
}

impl ChaintracksServiceClient {
    /// Create a new Chaintracks service client.
    pub fn new(config: ChaintracksConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        let key = shared_key("Chaintracks", &config.url, config.api_key.as_deref());
        Self {
            client,
            config,
            chain: tokio::sync::OnceCell::new(),
            gate: RateGate::shared(&key, Self::gate),
        }
    }

    fn gate() -> RateGate {
        RateGate::new("Chaintracks", CHAINTRACKS_DEFAULT_RETRY_AFTER)
    }

    /// A client for `url` with a gate of its own reading now from `clock`:
    /// a fixture server is recycled, so a gate keyed by its URL would carry
    /// one test's 429 into the next.
    #[cfg(test)]
    pub(crate) fn at_clock(url: &str, clock: crate::services::providers::pace::Clock) -> Self {
        let gate = Arc::new(Self::gate().with_clock(clock));
        Self {
            gate,
            ..Self::from_url(url)
        }
    }

    /// Create from a URL string.
    pub fn from_url(url: &str) -> Self {
        Self::new(ChaintracksConfig {
            url: url.to_string(),
            api_key: None,
        })
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.config.url.trim_end_matches('/'), path)
    }

    fn build_request(&self, url: &str) -> crate::services::providers::http_client::RequestBuilder {
        let mut req = self.client.get(url);
        if let Some(ref key) = self.config.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        req
    }

    /// One GET through the gate: no request inside a Retry-After window,
    /// and a 429 closes the rung. `what` names the call in a send fault.
    async fn get(&self, url: &str, what: &str) -> std::result::Result<reqwest::Response, Fault> {
        self.gate.admit().map_err(Fault::Unread)?;
        let response = self.build_request(url).send().await.map_err(|e| {
            Fault::Unread(Error::NetworkError(format!("Chaintracks {}: {}", what, e)))
        })?;
        self.gate
            .check(response.status(), response.headers())
            .map_err(Fault::Unread)?;
        Ok(response)
    }

    /// Get the current blockchain height.
    pub async fn get_present_height(&self) -> Result<u32> {
        Ok(self.present_height().await?)
    }

    async fn present_height(&self) -> std::result::Result<u32, Fault> {
        let url = self.build_url("/getPresentHeight");
        let response = self.get(&url, "getPresentHeight").await?;

        if !response.status().is_success() {
            return Err(Fault::Unread(Error::ServiceError(format!(
                "Chaintracks getPresentHeight failed: HTTP {}",
                response.status()
            ))));
        }

        let frame: ResponseFrame<u32> = response.json().await.map_err(|e| {
            Fault::Unread(Error::ServiceError(format!(
                "Chaintracks parse error: {}",
                e
            )))
        })?;

        if !frame.is_success() {
            return Err(Fault::Refused(Error::ServiceError(format!(
                "Chaintracks getPresentHeight: status={}",
                frame.status
            ))));
        }

        frame.value.ok_or_else(|| {
            Fault::Refused(Error::ServiceError(
                "Chaintracks getPresentHeight: missing value".to_string(),
            ))
        })
    }

    /// Find block header by height.
    pub async fn find_header_for_height(&self, height: u32) -> Result<BlockHeader> {
        Ok(self.header_for_height(height).await?)
    }

    async fn header_for_height(&self, height: u32) -> std::result::Result<BlockHeader, Fault> {
        let url = self.build_url(&format!("/findHeaderHexForHeight?height={}", height));
        let response = self.get(&url, "findHeaderHexForHeight").await?;

        if !response.status().is_success() {
            return Err(Fault::Unread(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForHeight failed: HTTP {}",
                response.status()
            ))));
        }

        let frame: ResponseFrame<CtBlockHeader> = response.json().await.map_err(|e| {
            Fault::Unread(Error::ServiceError(format!(
                "Chaintracks header parse: {}",
                e
            )))
        })?;

        if !frame.is_success() {
            return Err(Fault::Refused(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForHeight: status={}",
                frame.status
            ))));
        }

        frame.value.map(BlockHeader::from).ok_or_else(|| {
            Fault::Refused(Error::NotFound {
                entity: "BlockHeader".to_string(),
                id: height.to_string(),
            })
        })
    }

    /// Find the chain tip header.
    pub async fn find_chain_tip_header(&self) -> Result<BlockHeader> {
        Ok(self.chain_tip_header().await?)
    }

    async fn chain_tip_header(&self) -> std::result::Result<BlockHeader, Fault> {
        let url = self.build_url("/findChainTipHeaderHex");
        let response = self.get(&url, "findChainTipHeaderHex").await?;

        if !response.status().is_success() {
            return Err(Fault::Unread(Error::ServiceError(format!(
                "Chaintracks findChainTipHeaderHex failed: HTTP {}",
                response.status()
            ))));
        }

        let frame: ResponseFrame<CtBlockHeader> = response.json().await.map_err(|e| {
            Fault::Unread(Error::ServiceError(format!("Chaintracks tip parse: {}", e)))
        })?;

        if !frame.is_success() {
            return Err(Fault::Refused(Error::ServiceError(format!(
                "Chaintracks findChainTipHeaderHex: status={}",
                frame.status
            ))));
        }

        frame.value.map(BlockHeader::from).ok_or_else(|| {
            Fault::Refused(Error::ServiceError("Chaintracks: no chain tip".to_string()))
        })
    }

    /// Find block header by block hash.
    pub async fn find_header_for_block_hash(&self, hash: &str) -> Result<BlockHeader> {
        let url = self.build_url(&format!("/findHeaderHexForBlockHash?hash={}", hash));
        let response = self.get(&url, "findHeaderHexForBlockHash").await?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForBlockHash failed: HTTP {}",
                response.status()
            )));
        }

        let frame: ResponseFrame<CtBlockHeader> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Chaintracks header parse: {}", e)))?;

        if !frame.is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForBlockHash: status={}",
                frame.status
            )));
        }

        frame
            .value
            .map(BlockHeader::from)
            .ok_or_else(|| Error::NotFound {
                entity: "BlockHeader".to_string(),
                id: hash.to_string(),
            })
    }

    /// Validate a merkle root for a given block height.
    pub async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool> {
        Ok(self.root_is_valid(root, height).await?)
    }

    async fn root_is_valid(&self, root: &str, height: u32) -> std::result::Result<bool, Fault> {
        let header = self.header_for_height(height).await?;
        Ok(header.merkle_root == root)
    }

    // Network metadata is stable for this service. Header context is read anew
    // for each verification: one tip and one range of at most eleven headers.
    /// The network, read once per client (`/getChain`) and kept.
    async fn verifier_chain(&self) -> std::result::Result<Chain, Fault> {
        let chain = self
            .chain
            .get_or_try_init(|| async {
                self.gate.admit().map_err(Fault::Unread)?;
                let response = self
                    .build_request(&self.build_url("/getChain"))
                    .send()
                    .await
                    .map_err(|e| Fault::Unread(e.into()))?;
                self.gate
                    .check(response.status(), response.headers())
                    .map_err(Fault::Unread)?;
                let frame: ResponseFrame<Chain> = response.error_for_status()?.json().await?;
                if !frame.is_success() {
                    return Err(Fault::Refused(Error::ServiceError(
                        "Chaintracks getChain failed".into(),
                    )));
                }
                frame.value.ok_or_else(|| {
                    Fault::Refused(Error::ServiceError(
                        "Chaintracks getChain: missing value".into(),
                    ))
                })
            })
            .await?;
        Ok(*chain)
    }

    async fn verifier_tip(&self) -> std::result::Result<ChainTip, Fault> {
        let chain = self.verifier_chain().await?;
        // The activation heights are bsv-rs's ChainParams (#440, the node's
        // chainparams.cpp); the CSV heights, which ChainParams does not carry,
        // the node's (bsv-script-lean docs/PARAMETERS.md:566: chainparams.cpp
        // :1046, :1342, :1470). Regtest's are the SV node's defaults; a
        // fixture with other heights (Teranode's 100 and 200) builds its own
        // tip. The regtest hold of #439 (`REGTEST_TIP_HELD`) lifted at beta.4.
        let (params, csv_height) = match chain {
            Chain::Main => (bsv_rs::block::ChainParams::mainnet(), 419_328),
            Chain::Test => (bsv_rs::block::ChainParams::testnet(), 770_112),
            Chain::Regtest => (bsv_rs::block::ChainParams::regtest(), 576),
        };
        let tip = self.chain_tip_header().await?;
        let start = tip.height.saturating_sub(10);
        let count = tip.height - start + 1;
        let url = self.build_url(&format!("/getHeaders?height={start}&count={count}"));
        self.gate.admit().map_err(Fault::Unread)?;
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Fault::Unread(e.into()))?;
        self.gate
            .check(response.status(), response.headers())
            .map_err(Fault::Unread)?;
        let frame: ResponseFrame<String> = response.error_for_status()?.json().await?;
        if !frame.is_success() {
            return Err(Fault::Refused(Error::ServiceError(
                "Chaintracks getHeaders failed".into(),
            )));
        }
        let bytes = frame
            .value
            .and_then(|s| hex::decode(s).ok())
            .ok_or_else(|| {
                Fault::Refused(Error::ServiceError(
                    "Chaintracks getHeaders: missing or invalid header bytes".into(),
                ))
            })?;
        let median_time_past =
            median_time_past(&bytes, &tip.to_binary(), count as usize).map_err(Fault::Refused)?;
        // A historical pre-CSV timestamp lock requires a candidate block
        // time. A header service cannot supply that future fact; hold it.
        Ok(crate::services::chain_tip(
            &params,
            tip.height,
            median_time_past,
            csv_height,
            None,
        ))
    }
}

/// The median time past of a connected header range that ends at the tip:
/// `bytes` holds `count` headers of 80 bytes, each naming the previous one's
/// hash, the last equal to `tip_bytes`; the middle timestamp of the range.
fn median_time_past(bytes: &[u8], tip_bytes: &[u8], count: usize) -> Result<u32> {
    if bytes.len() != count * 80 || &bytes[bytes.len() - 80..] != tip_bytes {
        return Err(Error::ServiceError(
            "Chaintracks getHeaders: range does not end at the tip".into(),
        ));
    }
    let mut previous: Option<[u8; 32]> = None;
    let mut times = Vec::with_capacity(count);
    for header in bytes.as_chunks::<80>().0 {
        if previous.is_some_and(|hash| header[4..36] != hash) {
            return Err(Error::ServiceError(
                "Chaintracks getHeaders: disconnected range".into(),
            ));
        }
        previous = Some(bsv_rs::primitives::sha256d(header));
        times.push(u32::from_le_bytes([
            header[68], header[69], header[70], header[71],
        ]));
    }
    times.sort_unstable();
    Ok(times[times.len() / 2])
}

/// A fault of one read and its class (#554, #565), one class per fault
/// kind whatever the route: the service was not read (`Unread`: the
/// Retry-After window, a send fault, a timeout, an HTTP error status, a
/// body that fails to decode), or it answered a frame the client refuses
/// after decoding (`Refused`: a status other than success, a missing
/// value, header bytes that are not hex, a range that does not end at the
/// tip). The error is the client's, its text the same whatever the class.
enum Fault {
    Unread(Error),
    Refused(Error),
}

impl From<Fault> for Error {
    fn from(fault: Fault) -> Self {
        match fault {
            Fault::Unread(e) | Fault::Refused(e) => e,
        }
    }
}

/// reqwest's own fault (a send fault, a timeout, a status
/// `error_for_status` refused, a body that fails to decode) is unread.
impl From<reqwest::Error> for Fault {
    fn from(e: reqwest::Error) -> Self {
        Fault::Unread(e.into())
    }
}

/// The one mapping of the client's three `ChainTracker` reads (the root
/// check, the height read, the tip read) into the tracker's error class
/// (#554): an `Unread` fault (the Retry-After window, a send fault, a
/// timeout, an HTTP error status, a body that fails to decode) is
/// `NetworkError`; a `Refused` one (a frame the client refuses after
/// decoding: a non-hex header, a missing field, a status other than
/// success) is `Other`. The text is the client's Display, unchanged.
fn tracker_fault(fault: Fault) -> ChainTrackerError {
    match fault {
        Fault::Unread(e) => ChainTrackerError::NetworkError(e.to_string()),
        Fault::Refused(e) => ChainTrackerError::Other(e.to_string()),
    }
}

#[async_trait]
impl ChainTracker for ChaintracksServiceClient {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        self.root_is_valid(root, height)
            .await
            .map_err(tracker_fault)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        self.present_height().await.map_err(tracker_fault)
    }

    async fn mining_tip(&self) -> std::result::Result<Option<ChainTip>, ChainTrackerError> {
        self.verifier_tip().await.map(Some).map_err(tracker_fault)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_range() -> (BlockHeader, Vec<u8>) {
        let mut tip = BlockHeader::default();
        let mut bytes = Vec::new();
        for i in 0..11 {
            let header = BlockHeader {
                version: 1,
                previous_hash: tip.hash.clone(),
                merkle_root: "11".repeat(32),
                // Permuted times make taking the tip's time fail the test.
                time: 1_700_000_000 + ((i * 7) % 11),
                bits: 0x1d00ffff,
                nonce: i,
                height: 900_000 + i,
                hash: String::new(),
            };
            let raw = header.to_binary();
            let hash =
                bsv_rs::transaction::beef_stream::display_hex(&bsv_rs::primitives::sha256d(&raw));
            tip = BlockHeader { hash, ..header };
            bytes.extend(raw);
        }
        (tip, bytes)
    }

    async fn answer(
        server: &mut crate::services::MockServer,
        route: &str,
        value: serde_json::Value,
        count: usize,
    ) -> mockito::Mock {
        server
            .mock("GET", route)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::json!({"status": "success", "value": value}).to_string())
            .expect(count)
            .create_async()
            .await
    }

    fn tip_json(tip: &BlockHeader) -> serde_json::Value {
        serde_json::json!({
            "version": tip.version, "previousHash": tip.previous_hash,
            "merkleRoot": tip.merkle_root, "time": tip.time, "bits": tip.bits,
            "nonce": tip.nonce, "height": tip.height, "hash": tip.hash
        })
    }

    #[tokio::test]
    async fn mining_context_uses_one_tip_and_one_range_with_cached_network() {
        for (chain, params, csv_height) in [
            ("main", bsv_rs::block::ChainParams::mainnet(), 419_328),
            ("test", bsv_rs::block::ChainParams::testnet(), 770_112),
            ("regtest", bsv_rs::block::ChainParams::regtest(), 576),
        ] {
            let mut server = crate::services::mock_server().await;
            let (tip, bytes) = header_range();
            let network = answer(&mut server, "/getChain", serde_json::json!(chain), 1).await;
            let tip_read = answer(&mut server, "/findChainTipHeaderHex", tip_json(&tip), 2).await;
            let range = answer(
                &mut server,
                "/getHeaders?height=900000&count=11",
                serde_json::json!(hex::encode(bytes)),
                2,
            )
            .await;
            let client = ChaintracksServiceClient::from_url(&server.url());
            for _ in 0..2 {
                assert_eq!(
                    client.mining_tip().await.unwrap(),
                    Some(crate::services::chain_tip(
                        &params,
                        tip.height,
                        1_700_000_005,
                        csv_height,
                        None
                    ))
                );
            }
            network.assert_async().await;
            tip_read.assert_async().await;
            range.assert_async().await;
        }
    }

    #[tokio::test]
    async fn mining_context_holds_on_a_short_or_disconnected_range() {
        for corruption in 0..3 {
            let mut server = crate::services::mock_server().await;
            let (tip, mut bytes) = header_range();
            match corruption {
                0 => {
                    bytes.pop();
                }
                1 => {
                    bytes[80 + 4] ^= 1;
                }
                _ => {
                    bytes[800 + 68] ^= 1;
                }
            }
            let network = answer(&mut server, "/getChain", serde_json::json!("main"), 1).await;
            let tip_read = answer(&mut server, "/findChainTipHeaderHex", tip_json(&tip), 1).await;
            let range = answer(
                &mut server,
                "/getHeaders?height=900000&count=11",
                serde_json::json!(hex::encode(bytes)),
                1,
            )
            .await;
            let client = ChaintracksServiceClient::from_url(&server.url());
            assert!(client.mining_tip().await.is_err());
            network.assert_async().await;
            tip_read.assert_async().await;
            range.assert_async().await;
        }
    }

    #[test]
    fn test_chaintracks_config() {
        let config = ChaintracksConfig {
            url: "https://api.calhouninfra.com".to_string(),
            api_key: None,
        };
        assert!(config.url.contains("calhouninfra"));
    }

    #[test]
    fn test_chaintracks_from_url() {
        let ct = ChaintracksServiceClient::from_url("https://api.calhouninfra.com");
        assert_eq!(ct.config.url, "https://api.calhouninfra.com");
    }

    /// One gate per base and key in the process: every `Services` built for
    /// one Chaintracks spends one Retry-After window (no request is made).
    #[test]
    fn clients_for_one_base_and_key_share_one_gate() {
        let config = |api_key: Option<&str>| ChaintracksConfig {
            url: "http://127.0.0.1:9/chaintracks-gate-key".to_string(),
            api_key: api_key.map(str::to_string),
        };
        let a = ChaintracksServiceClient::new(config(None));
        let b = ChaintracksServiceClient::new(config(None));
        let keyed = ChaintracksServiceClient::new(config(Some("k")));
        assert!(Arc::ptr_eq(&a.gate, &b.gate));
        assert!(!Arc::ptr_eq(&a.gate, &keyed.gate));
    }

    #[test]
    fn test_response_frame_success() {
        let json = r#"{"status":"success","value":937627}"#;
        let frame: ResponseFrame<u32> = serde_json::from_str(json).unwrap();
        assert!(frame.is_success());
        assert_eq!(frame.value, Some(937627));
    }

    #[test]
    fn test_ct_block_header_deser() {
        let json = r#"{
            "version": 536870912,
            "previousHash": "0000000000000000abc123",
            "merkleRoot": "def456",
            "time": 1700000000,
            "bits": 402917821,
            "nonce": 12345,
            "height": 937627,
            "hash": "000000000000000099887766"
        }"#;
        let ct: CtBlockHeader = serde_json::from_str(json).unwrap();
        assert_eq!(ct.height, 937627);

        let header: BlockHeader = ct.into();
        assert_eq!(header.height, 937627);
        assert_eq!(header.previous_hash, "0000000000000000abc123");
    }
}
