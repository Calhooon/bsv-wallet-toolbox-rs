//! The chain tracker the wallet checks merkle roots with: a ChainTracks
//! header service and an in-memory cache of confirmed roots.
//!
//! No explorer in the proof path (P0-1c, bsv-stack-lean #48; the owner's
//! rule of 2026-09-15, "nothing routine through WhatsOnChain"). By default
//! the header service's answer is the answer: `Ok(true)` is true, a
//! definite `Ok(false)` is false, and an error is an error (unable to
//! verify), never a verdict. Before P0-1c WhatsOnChain was asked whenever
//! the header service failed or refuted, and its root was taken.
//!
//! Break-glass: [`FallbackChainTracker::with_break_glass_explorers`] (from
//! `ServicesOptions::break_glass_explorer_headers`, off by default) asks
//! two explorers, WhatsOnChain and Bitails, for the block at the height,
//! only when the header service gave no answer, and logs every such call at
//! warn level (marker `break_glass_explorer_header`). An explorer never
//! overrules the header service's definite answer, break-glass or not.
//!
//! Rule 28 (T5): the question is the merkle root at a height; the header
//! service holds it, and with the header service unreachable nothing else
//! we run does, so this is the irreducible case and a break-glass read.
//! One explorer's word is never a verified root: `true` needs both
//! explorers naming the asked root, `false` needs both naming one other
//! root, and anything else (one fault, a disagreement) is an error, unable
//! to verify. Only a root both named, or the header service confirmed, is
//! cached. No proof of work or difficulty rule is checked on an explorer's
//! header here: two words agreeing is the whole of the check, which is why
//! the setting is break-glass.

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::RwLock;

use super::chaintracks_client::ChaintracksServiceClient;

/// WoC block header response (block-by-height endpoint).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct WocBlockByHeight {
    merkleroot: Option<String>,
    hash: Option<String>,
    height: Option<u32>,
    confirmations: Option<u32>,
}

/// Bitails block response (block-by-height endpoint): the raw 80-byte
/// header hex in `header`. The shape is the header service's own Bitails
/// courier's (rust-chaintracks@62cf619 `src/couriers.rs:143-205`).
#[derive(Debug, Deserialize)]
struct BitailsBlockByHeight {
    hash: String,
    height: u32,
    header: String,
}

/// The merkle root an 80-byte header carries, in display order, once the
/// bytes are bound to the block hash the explorer claimed for them (the
/// double SHA-256 of the bytes, reversed).
fn merkle_root_of_header(header: &[u8], claimed_hash: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    if header.len() != 80 {
        return Err(format!("header is {} bytes, not 80", header.len()));
    }
    let mut hash = Sha256::digest(Sha256::digest(header)).to_vec();
    hash.reverse();
    let computed = hex::encode(hash);
    if !computed.eq_ignore_ascii_case(claimed_hash) {
        return Err(format!(
            "claimed hash {} but the header bytes hash to {}",
            claimed_hash, computed
        ));
    }
    let mut root = header[36..68].to_vec();
    root.reverse();
    Ok(hex::encode(root))
}

/// Thread-safe in-memory cache for verified merkle roots.
///
/// Maps block height to lowercase merkle root hex. Evicts the lowest
/// height when at capacity (oldest blocks are least likely to be re-queried).
struct RootCache {
    inner: RwLock<HashMap<u32, String>>,
    max_entries: usize,
}

impl RootCache {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    fn get(&self, height: u32) -> Option<String> {
        self.inner.read().ok()?.get(&height).cloned()
    }

    fn insert(&self, height: u32, root: String) {
        if let Ok(mut map) = self.inner.write() {
            if map.len() >= self.max_entries && !map.contains_key(&height) {
                if let Some(&min_height) = map.keys().min() {
                    map.remove(&min_height);
                }
            }
            map.insert(height, root);
        }
    }
}

/// Chain tracker over a ChainTracks header service.
///
/// Logic for `is_valid_root_for_height`:
/// 1. Check the cache (positives only): on a hit, compare and return.
/// 2. Ask the header service. `Ok(true)`: cache and return true. `Ok(false)`:
///    return false.
/// 3. The header service gave no answer: `Err(ChainTrackerError::NetworkError)`,
///    unless break-glass is on, when WhatsOnChain and Bitails are both asked
///    (logged at warn): both name the asked root, true (cached); both name
///    one other root, false; one fails or they disagree, `Err`.
pub struct FallbackChainTracker {
    primary: ChaintracksServiceClient,
    /// The API bases of WhatsOnChain and Bitails, present only under
    /// break-glass.
    break_glass_explorers: Option<(String, String)>,
    client: Client,
    cache: RootCache,
}

impl FallbackChainTracker {
    /// Access the underlying ChaintracksServiceClient for header lookups
    /// that don't go through the `ChainTracker` trait (e.g. `find_header_for_height`).
    pub fn primary(&self) -> &ChaintracksServiceClient {
        &self.primary
    }

    /// A tracker over the header service alone: no explorer is ever asked.
    pub fn new(primary: ChaintracksServiceClient) -> Self {
        Self::build(primary, None)
    }

    /// Break-glass: a tracker that asks WhatsOnChain at `woc_base_url` (e.g.
    /// `https://api.whatsonchain.com/v1/bsv/main`) and Bitails at
    /// `bitails_base_url` (e.g. `https://api.bitails.io/`) when the header
    /// service gives no answer, logging every such call at warn level.
    /// Never the default; never consulted against a definite answer; a
    /// verdict only when the two explorers agree.
    pub fn with_break_glass_explorers(
        primary: ChaintracksServiceClient,
        woc_base_url: impl Into<String>,
        bitails_base_url: impl Into<String>,
    ) -> Self {
        Self::build(
            primary,
            Some((
                woc_base_url.into().trim_end_matches('/').to_string(),
                bitails_base_url.into().trim_end_matches('/').to_string(),
            )),
        )
    }

    fn build(
        primary: ChaintracksServiceClient,
        break_glass_explorers: Option<(String, String)>,
    ) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self {
            primary,
            break_glass_explorers,
            client,
            cache: RootCache::new(1000),
        }
    }

    /// Is the break-glass explorer fallback on?
    pub fn break_glass_explorers(&self) -> bool {
        self.break_glass_explorers.is_some()
    }

    /// Break-glass: Bitails' block-by-height API. The header bytes are
    /// bound to the block hash and the height Bitails claims for them
    /// before their merkle root is read.
    async fn bitails_root_for_height(
        &self,
        bitails_base_url: &str,
        height: u32,
    ) -> Result<String, String> {
        let url = format!("{}/block/height/{}", bitails_base_url, height);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Bitails fallback request error: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            return Err(format!("Bitails fallback HTTP {}", status));
        }

        let block: BitailsBlockByHeight = response
            .json()
            .await
            .map_err(|e| format!("Bitails fallback parse error: {}", e))?;
        if block.height != height {
            return Err(format!(
                "Bitails: asked for height {} and was answered for {}",
                height, block.height
            ));
        }
        let bytes = hex::decode(&block.header)
            .map_err(|e| format!("Bitails header hex at height {}: {}", height, e))?;
        merkle_root_of_header(&bytes, &block.hash)
            .map_err(|e| format!("Bitails header at height {}: {}", height, e))
    }

    /// Break-glass (Rule 28, T5): WhatsOnChain's block-by-height API, asked
    /// for the merkle root at a height only when the header service, which
    /// holds it, gave no answer. Its word alone is never a verified root.
    async fn woc_root_for_height(&self, woc_base_url: &str, height: u32) -> Result<String, String> {
        let url = format!("{}/block/height/{}", woc_base_url, height);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("WoC fallback request error: {}", e))?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(format!("WoC: block not found at height {}", height));
        }
        if !status.is_success() {
            return Err(format!("WoC fallback HTTP {}", status));
        }

        let header: WocBlockByHeight = response
            .json()
            .await
            .map_err(|e| format!("WoC fallback parse error: {}", e))?;

        header
            .merkleroot
            .ok_or_else(|| format!("WoC: missing merkleroot at height {}", height))
    }
}

#[async_trait]
impl ChainTracker for FallbackChainTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        // 1. Check cache
        if let Some(cached_root) = self.cache.get(height) {
            return Ok(cached_root.eq_ignore_ascii_case(root));
        }

        // 2. The header service.
        let primary_error =
            match ChaintracksServiceClient::is_valid_root_for_height(&self.primary, root, height)
                .await
            {
                Ok(true) => {
                    self.cache.insert(height, root.to_lowercase());
                    return Ok(true);
                }
                // A definite answer from the header service stands; no
                // explorer is asked to overrule it.
                Ok(false) => return Ok(false),
                Err(e) => e,
            };

        // 3. No answer. Without break-glass that is the answer: unable to
        // verify, never an explorer's verdict.
        let Some((woc_base_url, bitails_base_url)) = self.break_glass_explorers.as_ref() else {
            return Err(ChainTrackerError::NetworkError(format!(
                "header service gave no answer for height {}: {}",
                height, primary_error
            )));
        };
        // Break-glass (Rule 28, T5): the merkle root at a height. The
        // header service holds it and is unreachable; nothing else we run
        // does. One explorer's word is never a verified root, so both are
        // asked and a verdict needs them to agree.
        tracing::warn!(
            height,
            marker = "break_glass_explorer_header",
            error = %primary_error,
            "break-glass: the header service gave no answer; asking WhatsOnChain and Bitails for the merkle root"
        );
        let unable = |why: String| {
            tracing::warn!(
                height,
                marker = "break_glass_explorer_header",
                "break-glass: unable to verify the root at height {}: chaintracks: {}; {}",
                height,
                primary_error,
                why
            );
            ChainTrackerError::NetworkError(format!(
                "unable to verify the root at height {}: chaintracks: {}; {}",
                height, primary_error, why
            ))
        };
        let woc_root = self
            .woc_root_for_height(woc_base_url, height)
            .await
            .map_err(|e| unable(format!("WoC: {}", e)))?;
        let bitails_root = self
            .bitails_root_for_height(bitails_base_url, height)
            .await
            .map_err(|e| {
                unable(format!(
                    "WoC named a root and no second explorer confirmed it: {}",
                    e
                ))
            })?;
        if !woc_root.eq_ignore_ascii_case(&bitails_root) {
            return Err(unable(format!(
                "the explorers disagree: WoC names {} and Bitails names {}",
                woc_root, bitails_root
            )));
        }
        let valid = woc_root.eq_ignore_ascii_case(root);
        if valid {
            self.cache.insert(height, root.to_lowercase());
        }
        Ok(valid)
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        // Delegate to primary — height lookups don't have the same gap problem
        self.primary
            .get_present_height()
            .await
            .map_err(|e| ChainTrackerError::NetworkError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::providers::chaintracks_client::ChaintracksConfig;

    fn make_primary(url: &str) -> ChaintracksServiceClient {
        ChaintracksServiceClient::new(ChaintracksConfig {
            url: url.to_string(),
            api_key: None,
        })
    }

    /// A break-glass tracker: WhatsOnChain at `woc_url` and Bitails at
    /// `woc_url/bitails` (one fixture server, two bases) behind the header
    /// service.
    fn make_tracker(ct_url: &str, woc_url: &str) -> FallbackChainTracker {
        FallbackChainTracker::with_break_glass_explorers(
            make_primary(ct_url),
            woc_url,
            format!("{}/bitails", woc_url),
        )
    }

    /// An 80-byte header carrying `merkle_root` (display hex), and its hash.
    fn header_with_root(merkle_root: &str) -> (String, String) {
        use sha2::{Digest, Sha256};
        let mut bytes = vec![0u8; 80];
        bytes[0..4].copy_from_slice(&536870912u32.to_le_bytes());
        let mut root = hex::decode(merkle_root).expect("a 64-hex root");
        root.reverse();
        bytes[36..68].copy_from_slice(&root);
        let mut hash = Sha256::digest(Sha256::digest(&bytes)).to_vec();
        hash.reverse();
        (hex::encode(bytes), hex::encode(hash))
    }

    // Helper: mock Bitails /block/height/{height} endpoint (under /bitails).
    async fn mock_bitails_header(
        server: &mut mockito::ServerGuard,
        height: u32,
        merkle_root: &str,
    ) -> mockito::Mock {
        let (header, hash) = header_with_root(merkle_root);
        server
            .mock("GET", format!("/bitails/block/height/{}", height).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"hash":"{}","height":{},"header":"{}"}}"#,
                hash, height, header
            ))
            .create_async()
            .await
    }

    // Helper: mock ChainTracks /findHeaderHexForHeight endpoint.
    async fn mock_ct_header(
        server: &mut mockito::ServerGuard,
        height: u32,
        merkle_root: &str,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/findHeaderHexForHeight?height={}", height).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"status":"success","value":{{"version":536870912,"previousHash":"{}","merkleRoot":"{}","time":1700000000,"bits":402917821,"nonce":12345,"height":{},"hash":"{}"}}}}"#,
                "0".repeat(64),
                merkle_root,
                height,
                "0".repeat(64)
            ))
            .create_async()
            .await
    }

    // Helper: mock ChainTracks returning success but no value (sync gap).
    async fn mock_ct_not_found(server: &mut mockito::ServerGuard, height: u32) -> mockito::Mock {
        server
            .mock(
                "GET",
                format!("/findHeaderHexForHeight?height={}", height).as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":"success"}"#)
            .create_async()
            .await
    }

    // Helper: mock ChainTracks returning HTTP error.
    async fn mock_ct_error(server: &mut mockito::ServerGuard, height: u32) -> mockito::Mock {
        server
            .mock(
                "GET",
                format!("/findHeaderHexForHeight?height={}", height).as_str(),
            )
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await
    }

    // Helper: mock WoC /block/height/{height} endpoint.
    async fn mock_woc_header(
        server: &mut mockito::ServerGuard,
        height: u32,
        merkle_root: &str,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/block/height/{}", height).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"merkleroot":"{}","hash":"{}","height":{},"confirmations":100}}"#,
                merkle_root,
                "0".repeat(64),
                height
            ))
            .create_async()
            .await
    }

    // Helper: mock WoC returning 404.
    async fn mock_woc_not_found(server: &mut mockito::ServerGuard, height: u32) -> mockito::Mock {
        server
            .mock("GET", format!("/block/height/{}", height).as_str())
            .with_status(404)
            .with_body("Not Found")
            .create_async()
            .await
    }

    // Helper: mock WoC returning 429.
    async fn mock_woc_rate_limited(
        server: &mut mockito::ServerGuard,
        height: u32,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/block/height/{}", height).as_str())
            .with_status(429)
            .with_body("Too Many Requests")
            .create_async()
            .await
    }

    /// A 64-hex merkle root named by a tag.
    fn r(tag: &str) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(tag.as_bytes()))
    }

    /// Rule 28 witness (T5): under break-glass, with the header service
    /// giving no answer, one explorer naming the asked root is not a
    /// verified root. It is not `true`, and it is not cached: when the
    /// header service is back, its answer is the answer.
    #[tokio::test]
    async fn one_explorers_root_is_never_a_verified_root() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;
        let asked = r("the root one explorer names");

        let ct_down = mock_ct_error(&mut ct_server, 700).await;
        let _woc = mock_woc_header(&mut woc_server, 700, &asked).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let alone = tracker.is_valid_root_for_height(&asked, 700).await;
        assert!(
            alone.is_err(),
            "one explorer's word is unable to verify, never a verdict: {alone:?}"
        );

        // A Bitails header whose bytes do not hash to the hash it claims
        // is no second word either.
        let (header, _hash) = header_with_root(&asked);
        let forged = woc_server
            .mock("GET", "/bitails/block/height/700")
            .with_status(200)
            .with_body(format!(
                r#"{{"hash":"{}","height":700,"header":"{}"}}"#,
                "0".repeat(64),
                header
            ))
            .create_async()
            .await;
        let unbound = tracker.is_valid_root_for_height(&asked, 700).await;
        assert!(unbound.is_err(), "{unbound:?}");
        forged.remove_async().await;

        ct_down.remove_async().await;
        let _ct_back = mock_ct_header(&mut ct_server, 700, &r("the header service's root")).await;
        let back = tracker.is_valid_root_for_height(&asked, 700).await;
        assert!(
            matches!(back, Ok(false)),
            "nothing was cached from the explorer: {back:?}"
        );
    }

    // =========================================================================
    // Test 1: Primary succeeds, root matches
    // =========================================================================

    #[tokio::test]
    async fn test_primary_succeeds_root_matches() {
        let mut ct_server = mockito::Server::new_async().await;
        let woc_server = mockito::Server::new_async().await;
        let root = "abc123def456";

        let _m = mock_ct_header(&mut ct_server, 800000, root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height(root, 800000).await;

        assert!(result.unwrap());
    }

    // =========================================================================
    // Test 2: Primary succeeds, root mismatch
    // =========================================================================

    #[tokio::test]
    async fn test_primary_succeeds_root_mismatch() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_header(&mut ct_server, 800000, "real_root").await;
        // WoC also returns the real root (not what caller asked for)
        let _woc = mock_woc_header(&mut woc_server, 800000, "real_root").await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("wrong_root", 800000).await;

        assert!(!result.unwrap());
    }

    // =========================================================================
    // Test 3: Primary fails (error), fallback succeeds
    // =========================================================================

    #[tokio::test]
    async fn test_primary_fails_fallback_succeeds() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;
        let root = r("abc123def456");

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_header(&mut woc_server, 943495, &root).await;
        let _bitails = mock_bitails_header(&mut woc_server, 943495, &root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height(&root, 943495).await;

        assert!(result.unwrap());
    }

    // =========================================================================
    // Test 4: Primary fails, fallback succeeds, root mismatch
    // =========================================================================

    #[tokio::test]
    async fn test_primary_fails_fallback_root_mismatch() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_header(&mut woc_server, 943495, &r("actual_root")).await;
        let _bitails = mock_bitails_header(&mut woc_server, 943495, &r("actual_root")).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker
            .is_valid_root_for_height(&r("wrong_root"), 943495)
            .await;

        // Two explorers naming one other root are the negative.
        assert!(!result.unwrap());
    }

    // =========================================================================
    // Test 5: Both fail: an error, never a verdict (F2 of the reorg review)
    // =========================================================================

    #[tokio::test]
    async fn test_both_fail_returns_an_error_not_a_verdict() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_not_found(&mut woc_server, 943495).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("any_root", 943495).await;

        // A double outage used to answer Ok(false), which every caller read
        // as "the chain refutes this root".
        assert!(
            matches!(result, Err(ChainTrackerError::NetworkError(_))),
            "got {result:?}"
        );
    }

    // =========================================================================
    // Test 5b: the four arms, named (F2b of the reorg review)
    // =========================================================================

    /// Under break-glass: (a) primary true; (b) primary definite false:
    /// false, WoC never asked; (c) primary definite false + WoC down: false;
    /// (d) primary error + both explorers agree: compare; (d2) primary
    /// error + the explorers disagree: Err; (e) primary error + WoC fails:
    /// Err.
    #[tokio::test]
    async fn the_four_arms_of_the_fallback_tracker() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        // (a)
        let _a = mock_ct_header(&mut ct_server, 1, "root_a").await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(t.is_valid_root_for_height("root_a", 1).await.unwrap());

        // (b) primary says the real root is "real": the asked root is
        // refuted, and WhatsOnChain is never asked, even when it would name
        // the asked root (P0-1c: an explorer never overrules the header
        // service's definite answer, break-glass or not).
        let _b_ct = mock_ct_header(&mut ct_server, 2, "real_b").await;
        let _b_woc = mock_woc_header(&mut woc_server, 2, "real_b").await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(!t.is_valid_root_for_height("wrong_b", 2).await.unwrap());
        let _b2_ct = mock_ct_header(&mut ct_server, 3, "stale_c").await;
        let b2_woc = woc_server
            .mock("GET", "/block/height/3")
            .with_status(200)
            .with_body(r#"{"merkleroot":"asked_c"}"#)
            .expect(0)
            .create_async()
            .await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(!t.is_valid_root_for_height("asked_c", 3).await.unwrap());
        b2_woc.assert_async().await;

        // (c) primary definite false, WoC rate limited: false stands.
        let _c_ct = mock_ct_header(&mut ct_server, 4, "real_d").await;
        let _c_woc = mock_woc_rate_limited(&mut woc_server, 4).await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(!t.is_valid_root_for_height("wrong_d", 4).await.unwrap());

        // (d) primary error, both explorers answer and agree: compare.
        let _d_ct = mock_ct_error(&mut ct_server, 5).await;
        let _d_woc = mock_woc_header(&mut woc_server, 5, &r("root_e")).await;
        let _d_bitails = mock_bitails_header(&mut woc_server, 5, &r("root_e")).await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(t.is_valid_root_for_height(&r("root_e"), 5).await.unwrap());
        assert!(!t.is_valid_root_for_height(&r("other_e"), 5).await.unwrap());

        // (d2) primary error, the explorers disagree: Err, whichever of
        // them names the asked root.
        let _d2_ct = mock_ct_error(&mut ct_server, 7).await;
        let _d2_woc = mock_woc_header(&mut woc_server, 7, &r("root_f")).await;
        let _d2_bitails = mock_bitails_header(&mut woc_server, 7, &r("root_g")).await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        for asked in [r("root_f"), r("root_g"), r("root_h")] {
            assert!(matches!(
                t.is_valid_root_for_height(&asked, 7).await,
                Err(ChainTrackerError::NetworkError(_))
            ));
        }

        // (e) primary error, WoC error: Err.
        let _e_ct = mock_ct_error(&mut ct_server, 6).await;
        let _e_woc = mock_woc_rate_limited(&mut woc_server, 6).await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(matches!(
            t.is_valid_root_for_height("any", 6).await,
            Err(ChainTrackerError::NetworkError(_))
        ));
    }

    /// P0-1c witness (bsv-stack-lean #48): the tracker `Services` builds
    /// from a `chaintracks_url` takes an explorer's root. The base builds it
    /// as `FallbackChainTracker::new(primary, None)` (`services.rs:495`), the
    /// same arms as here with WhatsOnChain's real URL; the mock stands in for
    /// it. With the header service down the explorer's root is accepted, and
    /// the explorer overrules the header service's own definite answer. The
    /// rule (CLAUDE.md rule 21, the owner's rule of 2026-09-15): no explorer
    /// in the proof path. Down is an error; a definite answer stands.
    #[tokio::test]
    async fn the_default_tracker_never_takes_an_explorers_root() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        // The header service is down; the explorer names the asked root.
        let _a_ct = mock_ct_error(&mut ct_server, 900).await;
        let _a_woc = mock_woc_header(&mut woc_server, 900, "explorer_root").await;
        let t = default_tracker(&ct_server.url(), &woc_server.url());
        let down = t.is_valid_root_for_height("explorer_root", 900).await;
        assert!(
            down.is_err(),
            "the header service down is an error, never an explorer's verdict: {down:?}"
        );

        // The header service answers a different root; the explorer names
        // the asked one.
        let _b_ct = mock_ct_header(&mut ct_server, 901, "header_service_root").await;
        let _b_woc = mock_woc_header(&mut woc_server, 901, "explorer_root").await;
        let t = default_tracker(&ct_server.url(), &woc_server.url());
        let refuted = t.is_valid_root_for_height("explorer_root", 901).await;
        assert!(
            matches!(refuted, Ok(false)),
            "the header service's definite answer stands: {refuted:?}"
        );
    }

    /// The tracker `Services` builds by default. It holds no explorer URL,
    /// so the explorer mock (`_woc_url`) is never reachable from it.
    fn default_tracker(ct_url: &str, _woc_url: &str) -> FallbackChainTracker {
        let t = FallbackChainTracker::new(make_primary(ct_url));
        assert!(!t.break_glass_explorers());
        t
    }

    // =========================================================================
    // Test 6: Cache hit — no provider calls on second request
    // =========================================================================

    #[tokio::test]
    async fn test_cache_hit_skips_providers() {
        let mut ct_server = mockito::Server::new_async().await;
        let woc_server = mockito::Server::new_async().await;
        let root = "cached_root_123";

        // Only allow one hit on ChainTracks
        let _ct = ct_server
            .mock("GET", format!("/findHeaderHexForHeight?height={}", 800000).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"status":"success","value":{{"version":536870912,"previousHash":"{}","merkleRoot":"{}","time":1700000000,"bits":402917821,"nonce":12345,"height":{},"hash":"{}"}}}}"#,
                "0".repeat(64), root, 800000, "0".repeat(64)
            ))
            .expect_at_most(1)
            .create_async()
            .await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());

        // First call: hits ChainTracks, caches result
        let r1 = tracker.is_valid_root_for_height(root, 800000).await;
        assert!(r1.unwrap());

        // Second call: served from cache, no HTTP call
        let r2 = tracker.is_valid_root_for_height(root, 800000).await;
        assert!(r2.unwrap());
    }

    // =========================================================================
    // Test 7: Cache stores fallback results
    // =========================================================================

    #[tokio::test]
    async fn test_cache_stores_fallback_results() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;
        let root = r("fallback_root");

        // ChainTracks has a gap; both explorers have the header
        let _ct = mock_ct_not_found(&mut ct_server, 943495).await;
        let _woc = woc_server
            .mock("GET", format!("/block/height/{}", 943495).as_str())
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"merkleroot":"{}","hash":"{}","height":{},"confirmations":100}}"#,
                root,
                "0".repeat(64),
                943495
            ))
            .expect_at_most(1)
            .create_async()
            .await;
        let _bitails = mock_bitails_header(&mut woc_server, 943495, &root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());

        // First call: CT fails, both explorers name the root, result cached
        let r1 = tracker.is_valid_root_for_height(&root, 943495).await;
        assert!(r1.unwrap());

        // Second call: served from cache
        let r2 = tracker.is_valid_root_for_height(&root, 943495).await;
        assert!(r2.unwrap());
    }

    // =========================================================================
    // Test 8: Primary returns not-found (sync gap) triggers fallback
    // =========================================================================

    #[tokio::test]
    async fn test_primary_not_found_triggers_fallback() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;
        let root = r("gap_root");

        // ChainTracks sync gap: {"status":"success"} with no value
        let _ct = mock_ct_not_found(&mut ct_server, 943495).await;
        let _woc = mock_woc_header(&mut woc_server, 943495, &root).await;
        let _bitails = mock_bitails_header(&mut woc_server, 943495, &root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height(&root, 943495).await;

        assert!(result.unwrap());
    }

    // =========================================================================
    // Test 9: WoC rate limited (429) behind a primary error: an error
    // =========================================================================

    #[tokio::test]
    async fn test_woc_rate_limited_behind_a_primary_error_is_an_error() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_rate_limited(&mut woc_server, 943495).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("any_root", 943495).await;

        // A 429 is a fault, never "the chain refutes this root".
        assert!(matches!(result, Err(ChainTrackerError::NetworkError(_))));
    }

    // =========================================================================
    // Test 10: Cache miss for different root at same height
    // =========================================================================

    #[tokio::test]
    async fn test_cache_mismatch_different_root() {
        let mut ct_server = mockito::Server::new_async().await;
        let woc_server = mockito::Server::new_async().await;
        let real_root = "real_root_abc";

        let _ct = mock_ct_header(&mut ct_server, 800000, real_root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());

        // Cache the real root
        let r1 = tracker.is_valid_root_for_height(real_root, 800000).await;
        assert!(r1.unwrap());

        // Query with wrong root — cache returns false without hitting providers
        let r2 = tracker.is_valid_root_for_height("wrong_root", 800000).await;
        assert!(!r2.unwrap());
    }

    // =========================================================================
    // Test: current_height delegates to primary
    // =========================================================================

    #[tokio::test]
    async fn test_current_height_delegates_to_primary() {
        let mut ct_server = mockito::Server::new_async().await;
        let woc_server = mockito::Server::new_async().await;

        let _m = ct_server
            .mock("GET", "/getPresentHeight")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":"success","value":943500}"#)
            .create_async()
            .await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let height = tracker.current_height().await.unwrap();
        assert_eq!(height, 943500);
    }

    // =========================================================================
    // Test: RootCache eviction
    // =========================================================================

    #[test]
    fn test_root_cache_eviction() {
        let cache = RootCache::new(3);
        cache.insert(100, "root_100".to_string());
        cache.insert(200, "root_200".to_string());
        cache.insert(300, "root_300".to_string());

        // At capacity — inserting height 400 should evict height 100 (lowest)
        cache.insert(400, "root_400".to_string());

        assert!(cache.get(100).is_none());
        assert_eq!(cache.get(200), Some("root_200".to_string()));
        assert_eq!(cache.get(400), Some("root_400".to_string()));
    }

    // =========================================================================
    // Test: cache is case-insensitive
    // =========================================================================

    #[tokio::test]
    async fn test_cache_case_insensitive() {
        let mut ct_server = mockito::Server::new_async().await;
        let woc_server = mockito::Server::new_async().await;
        let root = "AbCdEf123456";

        // First call with exact case — primary succeeds, caches lowercase
        let _ct = mock_ct_header(&mut ct_server, 800000, root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let r1 = tracker.is_valid_root_for_height(root, 800000).await;
        assert!(r1.unwrap());

        // Second call with different case — cache hit, case-insensitive match
        let r2 = tracker
            .is_valid_root_for_height("abcdef123456", 800000)
            .await;
        assert!(r2.unwrap());
    }
}
