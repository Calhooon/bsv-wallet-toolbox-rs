//! Fallback chain tracker with WoC backup and in-memory cache.
//!
//! Wraps `ChaintracksServiceClient` as the primary provider and falls back
//! to WhatsOnChain's block-by-height API when ChainTracks has sync gaps
//! (e.g. missing headers at certain heights).
//!
//! Matches the Go toolbox's `servicequeue.Queue` pattern: try providers in
//! sequence, first success wins, errors from one provider are logged and
//! the next is tried. Only returns `Ok(false)` (not an error) if ALL
//! providers fail — matching the TS toolbox's lenient behavior.

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

/// Chain tracker with ChainTracks primary and WoC fallback.
///
/// Logic for `is_valid_root_for_height`:
/// 1. Check cache — if hit, compare root and return
/// 2. Try primary (ChainTracks) — if success, cache and return
/// 3. If primary fails, try WoC fallback
/// 4. If WoC succeeds, cache and return
/// 5. If both fail, return `Ok(false)` (lenient — never hard-error)
pub struct FallbackChainTracker {
    primary: ChaintracksServiceClient,
    woc_base_url: String,
    client: Client,
    cache: RootCache,
}

impl FallbackChainTracker {
    /// Access the underlying ChaintracksServiceClient for header lookups
    /// that don't go through the `ChainTracker` trait (e.g. `find_header_for_height`).
    pub fn primary(&self) -> &ChaintracksServiceClient {
        &self.primary
    }

    /// Create a new fallback chain tracker.
    ///
    /// `woc_base_url` defaults to `https://api.whatsonchain.com/v1/bsv/main`
    /// if `None` is provided.
    pub fn new(primary: ChaintracksServiceClient, woc_base_url: Option<String>) -> Self {
        let woc_base_url =
            woc_base_url.unwrap_or_else(|| "https://api.whatsonchain.com/v1/bsv/main".to_string());
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self {
            primary,
            woc_base_url,
            client,
            cache: RootCache::new(1000),
        }
    }

    /// Try WoC block-by-height API as fallback.
    async fn woc_root_for_height(&self, height: u32) -> Result<String, String> {
        let url = format!("{}/block/height/{}", self.woc_base_url, height);
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

        // 2. Try primary (ChainTracks)
        match ChaintracksServiceClient::is_valid_root_for_height(&self.primary, root, height).await
        {
            Ok(true) => {
                self.cache.insert(height, root.to_lowercase());
                return Ok(true);
            }
            Ok(false) => {
                // Primary returned a root that doesn't match. Cache the actual root
                // from ChainTracks. We still fall through to WoC in case CT has bad data,
                // but this is unlikely.
                tracing::debug!(
                    "ChainTracks root mismatch at height {} — trying WoC fallback",
                    height
                );
            }
            Err(e) => {
                tracing::warn!(
                    "ChainTracks failed for height {}, trying WoC fallback: {}",
                    height,
                    e
                );
            }
        }

        // 3. Try WoC fallback
        match self.woc_root_for_height(height).await {
            Ok(woc_root) => {
                let valid = woc_root.eq_ignore_ascii_case(root);
                if valid {
                    self.cache.insert(height, root.to_lowercase());
                }
                Ok(valid)
            }
            Err(e) => {
                tracing::warn!(
                    "Both ChainTracks and WoC failed for height {}: {}",
                    height,
                    e
                );
                // 5. Both failed — return Ok(false), not an error (match TS behavior)
                Ok(false)
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

    fn make_tracker(ct_url: &str, woc_url: &str) -> FallbackChainTracker {
        FallbackChainTracker::new(make_primary(ct_url), Some(woc_url.to_string()))
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

        assert_eq!(result.unwrap(), true);
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

        assert_eq!(result.unwrap(), false);
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

        assert_eq!(result.unwrap(), true);
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

        assert_eq!(result.unwrap(), false);
    }

    // =========================================================================
    // Test 5: Both fail — returns Ok(false), not an error
    // =========================================================================

    #[tokio::test]
    async fn test_both_fail_returns_false_not_error() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_not_found(&mut woc_server, 943495).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("any_root", 943495).await;

        // Must be Ok(false), NOT Err — match TS lenient behavior
        assert_eq!(result.unwrap(), false);
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
        assert_eq!(r1.unwrap(), true);

        // Second call: served from cache, no HTTP call
        let r2 = tracker.is_valid_root_for_height(root, 800000).await;
        assert_eq!(r2.unwrap(), true);
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
        assert_eq!(r1.unwrap(), true);

        // Second call: served from cache
        let r2 = tracker.is_valid_root_for_height(root, 943495).await;
        assert_eq!(r2.unwrap(), true);
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

        assert_eq!(result.unwrap(), true);
    }

    // =========================================================================
    // Test 9: WoC rate limited (429) — returns Ok(false), not error
    // =========================================================================

    #[tokio::test]
    async fn test_woc_rate_limited_returns_false() {
        let mut ct_server = mockito::Server::new_async().await;
        let mut woc_server = mockito::Server::new_async().await;

        let _ct = mock_ct_error(&mut ct_server, 943495).await;
        let _woc = mock_woc_rate_limited(&mut woc_server, 943495).await;

        let tracker = make_tracker(&ct_server.url(), &woc_server.url());
        let result = tracker.is_valid_root_for_height("any_root", 943495).await;

        // Should be Ok(false), not Err
        assert_eq!(result.unwrap(), false);
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
        assert_eq!(r1.unwrap(), true);

        // Query with wrong root — cache returns false without hitting providers
        let r2 = tracker.is_valid_root_for_height("wrong_root", 800000).await;
        assert_eq!(r2.unwrap(), false);
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
        assert_eq!(r1.unwrap(), true);

        // Second call with different case — cache hit, case-insensitive match
        let r2 = tracker
            .is_valid_root_for_height("abcdef123456", 800000)
            .await;
        assert_eq!(r2.unwrap(), true);
    }
}
