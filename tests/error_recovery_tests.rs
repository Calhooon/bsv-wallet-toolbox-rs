//! Error recovery and adaptive timeout tests for the services layer.
//!
//! These tests verify:
//! - HTTP error handling (500, 429, connection failures)
//! - JSON error response parsing
//! - All-provider failure scenarios
//! - Adaptive timeout defaults, calculation, and bounds
//! - EMA convergence
//! - Partial/truncated response handling

use std::time::Duration;

use bsv_wallet_toolbox_rs::services::{
    collection::{AdaptiveTimeoutConfig, ServiceCall, ServiceCollection},
    mock::{MockErrorKind, MockResponse, MockWalletServices},
    Arc as ArcProvider, ArcConfig, WalletServices,
};
use bsv_wallet_toolbox_rs::Error;

// =============================================================================
// Test 1: Service 500 error via mockito (ARC provider)
// =============================================================================

#[tokio::test]
async fn test_service_500_error() {
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error": "Internal Server Error"}"#)
        .create_async()
        .await;

    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let beef = vec![0x01, 0x00, 0x00, 0x00]; // minimal bytes
    let txids = vec!["a".repeat(64)];

    let result = arc.post_beef(&beef, &txids).await.unwrap();

    // ARC wraps HTTP errors as PostBeefResult with error status, not Err()
    assert_eq!(result.status, "error");
    assert!(!result.txid_results.is_empty());
    assert!(result.txid_results[0].service_error);
    assert!(result.txid_results[0]
        .data
        .as_ref()
        .unwrap()
        .contains("500"));

    mock.assert_async().await;
}

// =============================================================================
// Test 2: Service 429 rate limit via mockito (ARC provider)
// =============================================================================

#[tokio::test]
async fn test_service_429_rate_limit() {
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(429)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error": "Too Many Requests", "detail": "Rate limit exceeded"}"#)
        .create_async()
        .await;

    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let beef = vec![0x01, 0x00, 0x00, 0x00];
    let txids = vec!["b".repeat(64)];

    let result = arc.post_beef(&beef, &txids).await.unwrap();

    // ARC wraps 429 as service error in the result
    assert_eq!(result.status, "error");
    assert!(!result.txid_results.is_empty());
    assert!(result.txid_results[0].service_error);
    assert!(result.txid_results[0]
        .data
        .as_ref()
        .unwrap()
        .contains("429"));

    mock.assert_async().await;
}

// =============================================================================
// Test 3: Connection failure (unreachable host)
// =============================================================================

#[tokio::test]
async fn test_connection_failure() {
    // Use a non-routable address that will fail to connect
    let config = ArcConfig {
        timeout_secs: Some(1), // Short timeout to fail fast
        ..Default::default()
    };
    let arc = ArcProvider::new(
        "http://192.0.2.1:1", // TEST-NET-1 (RFC 5737), unreachable
        Some(config),
        Some("testArc"),
    )
    .unwrap();

    let beef = vec![0x01, 0x00, 0x00, 0x00];
    let txids = vec!["c".repeat(64)];

    let result = arc.post_beef(&beef, &txids).await.unwrap();

    // ARC catches reqwest errors and wraps them in PostTxResultForTxid
    assert_eq!(result.status, "error");
    assert!(!result.txid_results.is_empty());
    assert!(result.txid_results[0].service_error);
    // The data should contain some form of connection/request failure message
    let data = result.txid_results[0].data.as_ref().unwrap();
    assert!(
        data.contains("Request failed") || data.contains("error") || data.contains("timed out"),
        "Expected connection failure message, got: {}",
        data
    );
}

// =============================================================================
// Test 4: JSON error response from service (valid JSON but error content)
// =============================================================================

#[tokio::test]
async fn test_json_error_response() {
    let mut server = mockito::Server::new_async().await;

    // Return a valid JSON response but with error status codes
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(465) // FEE_TOO_LOW
        .with_header("content-type", "application/json")
        .with_body(r#"{"type":"error","title":"Fee too low","status":465,"detail":"Transaction fee is below the minimum threshold","txid":"","extraInfo":""}"#)
        .create_async()
        .await;

    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let beef = vec![0x01, 0x00, 0x00, 0x00];
    let txids = vec!["d".repeat(64)];

    let result = arc.post_beef(&beef, &txids).await.unwrap();

    assert_eq!(result.status, "error");
    assert!(!result.txid_results.is_empty());
    assert!(result.txid_results[0].service_error);
    let data = result.txid_results[0].data.as_ref().unwrap();
    assert!(
        data.contains("fee too low") || data.contains("Fee") || data.contains("465"),
        "Expected fee-related error, got: {}",
        data
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 5: All providers fail in ServiceCollection
// =============================================================================

#[tokio::test]
async fn test_all_providers_fail_collection() {
    let provider1 = std::sync::Arc::new(
        MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "provider1: connection refused".to_string(),
            ))
            .build(),
    );
    let provider2 = std::sync::Arc::new(
        MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::ServiceError,
                "provider2: service unavailable (503)".to_string(),
            ))
            .build(),
    );
    let provider3 = std::sync::Arc::new(
        MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "provider3: DNS resolution failed".to_string(),
            ))
            .build(),
    );

    let mut collection = ServiceCollection::<std::sync::Arc<MockWalletServices>>::new("postBeef");
    collection.add("provider1", std::sync::Arc::clone(&provider1));
    collection.add("provider2", std::sync::Arc::clone(&provider2));
    collection.add("provider3", std::sync::Arc::clone(&provider3));

    let beef = vec![0x01, 0x02];
    let txids = vec!["tx1".to_string()];

    let mut last_error: Option<Error> = None;

    // Try all providers
    for _ in 0..collection.count() {
        let service = collection.current_service().unwrap().clone();
        match service.post_beef(&beef, &txids).await {
            Ok(_) => {
                let mut call = ServiceCall::new();
                call.mark_success(None);
                let name = collection.current_name().unwrap().to_string();
                collection.add_call_success(&name, call);
                last_error = None;
                break;
            }
            Err(e) => {
                let mut call = ServiceCall::new();
                call.mark_error(&e.to_string(), "ERROR");
                let name = collection.current_name().unwrap().to_string();
                collection.add_call_error(&name, call);
                last_error = Some(e);
                collection.next();
            }
        }
    }

    // All providers should have failed
    assert!(last_error.is_some(), "Expected all providers to fail");

    // Verify all three providers were tried
    let history = collection.get_call_history(false);
    assert_eq!(history.history_by_provider.len(), 3);

    for (name, h) in &history.history_by_provider {
        assert_eq!(
            h.total_counts.success, 0,
            "Provider {} should have 0 successes",
            name
        );
        assert!(
            h.total_counts.failure > 0,
            "Provider {} should have failures",
            name
        );
        assert!(
            h.total_counts.error > 0,
            "Provider {} should have errors",
            name
        );
    }
}

// =============================================================================
// Test 6: Adaptive timeout defaults
// =============================================================================

#[test]
fn test_adaptive_timeout_defaults() {
    let config = AdaptiveTimeoutConfig::default();

    assert_eq!(config.min_timeout_ms, 5_000);
    assert_eq!(config.max_timeout_ms, 60_000);
    assert!((config.multiplier - 2.0).abs() < f64::EPSILON);
    assert_eq!(config.initial_timeout_ms, 30_000);

    // Verify ServiceCollection uses these defaults
    let collection = ServiceCollection::<String>::new("test");
    let timeout = collection.get_current_timeout();
    assert_eq!(timeout, Duration::from_millis(30_000));

    // avg_response_ms should be None initially
    assert!(collection.avg_response_ms().is_none());
}

// =============================================================================
// Test 7: Adaptive timeout calculation
// =============================================================================

#[test]
fn test_adaptive_timeout_calculation() {
    let config = AdaptiveTimeoutConfig {
        min_timeout_ms: 1_000,
        max_timeout_ms: 20_000,
        multiplier: 2.0,
        initial_timeout_ms: 5_000,
    };
    let collection = ServiceCollection::<String>::with_timeout_config("test", config);

    // Before any data: returns initial_timeout_ms
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(5_000)
    );

    // Record first sample: 3000ms
    // avg = 3000 (first sample becomes avg)
    // timeout = 3000 * 2.0 = 6000, clamped to [1000, 20000] => 6000
    collection.record_response_time(3_000);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(6_000)
    );

    // Record second sample: 5000ms
    // EMA = 3000 * 0.7 + 5000 * 0.3 = 2100 + 1500 = 3600
    // timeout = 3600 * 2.0 = 7200
    collection.record_response_time(5_000);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(7_200)
    );

    // Record third sample: 1000ms
    // EMA = 3600 * 0.7 + 1000 * 0.3 = 2520 + 300 = 2820
    // timeout = 2820 * 2.0 = 5640
    collection.record_response_time(1_000);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(5_640)
    );
}

// =============================================================================
// Test 8: Timeout bounds enforcement
// =============================================================================

#[test]
fn test_timeout_bounds() {
    let config = AdaptiveTimeoutConfig {
        min_timeout_ms: 2_000,
        max_timeout_ms: 10_000,
        multiplier: 2.0,
        initial_timeout_ms: 5_000,
    };
    let collection = ServiceCollection::<String>::with_timeout_config("test", config);

    // Test minimum bound: very fast response
    // Record 100ms => avg = 100, timeout = 100 * 2.0 = 200, clamped to min 2000
    collection.record_response_time(100);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(2_000),
        "Timeout should be clamped to min_timeout_ms"
    );

    // Test maximum bound: very slow response
    // Reset by creating a new collection
    let config = AdaptiveTimeoutConfig {
        min_timeout_ms: 2_000,
        max_timeout_ms: 10_000,
        multiplier: 2.0,
        initial_timeout_ms: 5_000,
    };
    let collection = ServiceCollection::<String>::with_timeout_config("test", config);

    // Record 50000ms => avg = 50000, timeout = 50000 * 2.0 = 100000, clamped to max 10000
    collection.record_response_time(50_000);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(10_000),
        "Timeout should be clamped to max_timeout_ms"
    );

    // Multiplier edge case: multiplier of 1.0 with response exactly at max
    let config = AdaptiveTimeoutConfig {
        min_timeout_ms: 1_000,
        max_timeout_ms: 15_000,
        multiplier: 1.0,
        initial_timeout_ms: 5_000,
    };
    let collection = ServiceCollection::<String>::with_timeout_config("test", config);

    // Record 15000ms => avg = 15000, timeout = 15000 * 1.0 = 15000, equals max
    collection.record_response_time(15_000);
    assert_eq!(
        collection.get_current_timeout(),
        Duration::from_millis(15_000)
    );
}

// =============================================================================
// Test 9: EMA calculation accuracy
// =============================================================================

#[test]
fn test_ema_calculation() {
    let collection = ServiceCollection::<String>::new("test");

    // No data initially
    assert!(collection.avg_response_ms().is_none());

    // First sample: becomes the average
    collection.record_response_time(1000);
    let avg = collection.avg_response_ms().unwrap();
    assert!(
        (avg - 1000.0).abs() < 0.01,
        "First sample should become the average, got {}",
        avg
    );

    // Second sample (same value): EMA should stay at 1000
    // EMA = 1000 * 0.7 + 1000 * 0.3 = 700 + 300 = 1000
    collection.record_response_time(1000);
    let avg = collection.avg_response_ms().unwrap();
    assert!(
        (avg - 1000.0).abs() < 0.01,
        "EMA with same value should stay same, got {}",
        avg
    );

    // Third sample (much larger): verify 70/30 weighting
    // EMA = 1000 * 0.7 + 5000 * 0.3 = 700 + 1500 = 2200
    collection.record_response_time(5000);
    let avg = collection.avg_response_ms().unwrap();
    assert!(
        (avg - 2200.0).abs() < 1.0,
        "EMA should be 2200 (70/30 weighted), got {}",
        avg
    );

    // Fourth sample (smaller): verify EMA converges downward
    // EMA = 2200 * 0.7 + 200 * 0.3 = 1540 + 60 = 1600
    collection.record_response_time(200);
    let avg = collection.avg_response_ms().unwrap();
    assert!(
        (avg - 1600.0).abs() < 1.0,
        "EMA should be 1600, got {}",
        avg
    );

    // Convergence test: many samples of 500ms should converge toward 500
    for _ in 0..50 {
        collection.record_response_time(500);
    }
    let avg = collection.avg_response_ms().unwrap();
    assert!(
        (avg - 500.0).abs() < 5.0,
        "After 50 samples of 500ms, EMA should converge near 500, got {}",
        avg
    );
}

// =============================================================================
// Test 10: Partial/truncated response handling via mockito
// =============================================================================

#[tokio::test]
async fn test_partial_truncated_response() {
    let mut server = mockito::Server::new_async().await;

    // Return a 200 OK but with invalid/truncated JSON that cannot be parsed
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"txid": "abc123", "txStat"#) // Truncated JSON
        .create_async()
        .await;

    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let beef = vec![0x01, 0x00, 0x00, 0x00];
    let txids = vec!["e".repeat(64)];

    // ARC should handle truncated JSON gracefully rather than panicking
    let result = arc.post_beef(&beef, &txids).await;

    // Should be an error (failed to parse JSON) or a result with error status
    match result {
        Ok(beef_result) => {
            // If it somehow returns Ok, it should indicate an error in the result
            assert!(
                beef_result.status == "error"
                    || beef_result.txid_results.iter().any(|r| r.service_error),
                "Truncated response should be treated as error"
            );
        }
        Err(e) => {
            // Error variant is also acceptable for truncated responses
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("parse")
                    || err_msg.contains("JSON")
                    || err_msg.contains("Service")
                    || err_msg.contains("error"),
                "Error should indicate parse/JSON failure, got: {}",
                err_msg
            );
        }
    }

    mock.assert_async().await;
}
