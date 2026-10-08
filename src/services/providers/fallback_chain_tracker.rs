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
//! Break-glass: [`FallbackChainTracker::with_break_glass_woc`] (from
//! `ServicesOptions::break_glass_explorer_headers`, off by default) asks
//! WhatsOnChain's block-by-height API only when the header service gave no
//! answer, and logs every such call at warn level (marker
//! `break_glass_explorer_header`). An explorer never overrules the header
//! service's definite answer, break-glass or not.
//! Positives are cached; nothing else is.

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
///    unless break-glass is on, when WhatsOnChain is asked (logged at warn);
///    when it answers, compare (cache a match); when it fails, `Err`.
pub struct FallbackChainTracker {
    primary: ChaintracksServiceClient,
    /// WhatsOnChain's API base, present only under break-glass.
    break_glass_woc_base_url: Option<String>,
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
    /// `https://api.whatsonchain.com/v1/bsv/main`) when the header service
    /// gives no answer, logging every such call at warn level. Never the
    /// default; never consulted against a definite answer.
    pub fn with_break_glass_woc(
        primary: ChaintracksServiceClient,
        woc_base_url: impl Into<String>,
    ) -> Self {
        Self::build(primary, Some(woc_base_url.into()))
    }

    fn build(primary: ChaintracksServiceClient, break_glass_woc_base_url: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self {
            primary,
            break_glass_woc_base_url,
            client,
            cache: RootCache::new(1000),
        }
    }

    /// Is the break-glass explorer fallback on?
    pub fn break_glass_woc(&self) -> bool {
        self.break_glass_woc_base_url.is_some()
    }

    /// Break-glass: WhatsOnChain's block-by-height API.
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
        let Some(woc_base_url) = self.break_glass_woc_base_url.as_deref() else {
            return Err(ChainTrackerError::NetworkError(format!(
                "header service gave no answer for height {}: {}",
                height, primary_error
            )));
        };
        tracing::warn!(
            height,
            marker = "break_glass_explorer_header",
            error = %primary_error,
            "break-glass: the header service gave no answer; asking WhatsOnChain for the merkle root"
        );
        match self.woc_root_for_height(woc_base_url, height).await {
            Ok(woc_root) => {
                let valid = woc_root.eq_ignore_ascii_case(root);
                if valid {
                    self.cache.insert(height, root.to_lowercase());
                }
                Ok(valid)
            }
            // Nobody answered: an outage is an error, never a verdict.
            Err(woc_error) => {
                tracing::warn!(
                    "Both ChainTracks and WoC failed for height {}: chaintracks: {}; WoC: {}",
                    height,
                    primary_error,
                    woc_error
                );
                Err(ChainTrackerError::NetworkError(format!(
                    "both chaintracks and WoC failed for height {}: chaintracks: {}; WoC: {}",
                    height, primary_error, woc_error
                )))
            }
        }
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

    /// A break-glass tracker: WhatsOnChain at `woc_url` behind the header service.
    fn make_tracker(ct_url: &str, woc_url: &str) -> FallbackChainTracker {
        FallbackChainTracker::with_break_glass_woc(make_primary(ct_url), woc_url)
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
        let root = "abc123def456";

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_header(&mut woc_server, 943495, root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height(root, 943495).await;

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
        let _woc = mock_woc_header(&mut woc_server, 943495, "actual_root").await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("wrong_root", 943495).await;

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
    /// (d) primary error + WoC answers: compare; (e) primary error + WoC
    /// fails: Err.
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

        // (d) primary error, WoC answers: compare.
        let _d_ct = mock_ct_error(&mut ct_server, 5).await;
        let _d_woc = mock_woc_header(&mut woc_server, 5, "root_e").await;
        let t = make_tracker(&ct_server.url(), &woc_server.url());
        assert!(t.is_valid_root_for_height("root_e", 5).await.unwrap());
        assert!(!t.is_valid_root_for_height("other_e", 5).await.unwrap());

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
        assert!(!t.break_glass_woc());
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
        let root = "fallback_root";

        // ChainTracks has a gap — WoC has the header
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

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());

        // First call: CT fails, WoC succeeds, result cached
        let r1 = tracker.is_valid_root_for_height(root, 943495).await;
        assert!(r1.unwrap());

        // Second call: served from cache
        let r2 = tracker.is_valid_root_for_height(root, 943495).await;
        assert!(r2.unwrap());
    }

    // =========================================================================
    // Test 8: Primary returns not-found (sync gap) triggers fallback
    // =========================================================================

    #[tokio::test]
    async fn test_primary_not_found_triggers_fallback() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;
        let root = "gap_root";

        // ChainTracks sync gap: {"status":"success"} with no value
        let _ct = mock_ct_not_found(&mut ct_server, 943495).await;
        let _woc = mock_woc_header(&mut woc_server, 943495, root).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height(root, 943495).await;

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
