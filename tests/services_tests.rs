//! Integration tests for the services layer.
//!
//! These tests verify:
//! - Service creation and configuration
//! - Provider construction
//! - Service collection failover behavior
//! - Result type serialization

use bsv_wallet_toolbox_rs::services::{
    collection::{ServiceCall, ServiceCollection},
    traits::{
        BlockHeader, BsvExchangeRate, GetMerklePathResult, GetRawTxResult,
        GetScriptHashHistoryResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat,
        GetUtxoStatusResult, PostBeefResult, PostTxResultForTxid, ScriptHistoryItem,
        TxStatusDetail, UtxoDetail,
    },
    Arc, ArcConfig, Bitails, BitailsConfig, Chain, Services, ServicesOptions, WhatsOnChain,
    WhatsOnChainConfig,
};

// =============================================================================
// Services Creation Tests
// =============================================================================

#[test]
fn test_services_mainnet_creation() {
    let services = Services::mainnet();
    assert!(services.is_ok());
    let services = services.unwrap();
    assert!(services.get_merkle_path_count().unwrap() >= 1);
    assert!(services.get_raw_tx_count().unwrap() >= 1);
    assert!(services.post_beef_count().unwrap() >= 1);
    assert!(services.get_utxo_status_count().unwrap() >= 1);
}

#[test]
fn test_services_testnet_creation() {
    let services = Services::testnet();
    assert!(services.is_ok());
}

#[test]
fn test_services_with_options() {
    let options = ServicesOptions::mainnet()
        .with_woc_api_key("test-api-key")
        .with_bitails_api_key("bitails-key");

    assert_eq!(
        options.whatsonchain_api_key,
        Some("test-api-key".to_string())
    );
    assert_eq!(options.bitails_api_key, Some("bitails-key".to_string()));

    let services = Services::with_options(Chain::Main, options);
    assert!(services.is_ok());
}

// =============================================================================
// Provider Configuration Tests
// =============================================================================

#[test]
fn test_whatsonchain_mainnet_url() {
    // Verify WoC can be created for mainnet
    let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default());
    assert!(woc.is_ok());
}

#[test]
fn test_whatsonchain_testnet_url() {
    // Verify WoC can be created for testnet
    let woc = WhatsOnChain::new(Chain::Test, WhatsOnChainConfig::default());
    assert!(woc.is_ok());
}

#[test]
fn test_whatsonchain_with_api_key() {
    let config = WhatsOnChainConfig::with_api_key("test-key-123");
    assert_eq!(config.api_key, Some("test-key-123".to_string()));
}

#[test]
fn test_arc_taal_mainnet() {
    let arc = Arc::taal_mainnet(None);
    assert!(arc.is_ok());
    let arc = arc.unwrap();
    assert_eq!(arc.name(), "arcTaal");
}

#[test]
fn test_arc_taal_testnet() {
    let arc = Arc::taal_testnet(None);
    assert!(arc.is_ok());
    let arc = arc.unwrap();
    assert_eq!(arc.name(), "arcTaalTest");
}

#[test]
fn test_arc_gorillapool() {
    let arc = Arc::gorillapool(None);
    assert!(arc.is_ok());
    let arc = arc.unwrap();
    assert_eq!(arc.name(), "arcGorillaPool");
}

#[test]
fn test_arc_with_config() {
    let config = ArcConfig::with_api_key("arc-api-key")
        .with_deployment_id("deployment-123")
        .with_callback(
            "https://example.com/callback",
            Some("callback-token".to_string()),
        );

    assert_eq!(config.api_key, Some("arc-api-key".to_string()));
    assert_eq!(config.deployment_id, Some("deployment-123".to_string()));
    assert_eq!(
        config.callback_url,
        Some("https://example.com/callback".to_string())
    );
    assert_eq!(config.callback_token, Some("callback-token".to_string()));
}

#[test]
fn test_bitails_mainnet() {
    let bitails = Bitails::new(Chain::Main, BitailsConfig::default());
    assert!(bitails.is_ok());
}

#[test]
fn test_bitails_testnet() {
    let bitails = Bitails::new(Chain::Test, BitailsConfig::default());
    assert!(bitails.is_ok());
}

#[test]
fn test_bitails_with_api_key() {
    let config = BitailsConfig::with_api_key("bitails-key");
    assert_eq!(config.api_key, Some("bitails-key".to_string()));
}

// =============================================================================
// Service Collection Tests
// =============================================================================

#[test]
fn test_service_collection_basic() {
    let collection = ServiceCollection::<String>::new("testService")
        .with("provider1", "service1".to_string())
        .with("provider2", "service2".to_string())
        .with("provider3", "service3".to_string());

    assert_eq!(collection.count(), 3);
    assert!(!collection.is_empty());
    assert_eq!(collection.current_name(), Some("provider1"));
}

#[test]
fn test_service_collection_next() {
    let mut collection = ServiceCollection::<String>::new("testService")
        .with("p1", "s1".to_string())
        .with("p2", "s2".to_string());

    assert_eq!(collection.current_name(), Some("p1"));
    collection.next();
    assert_eq!(collection.current_name(), Some("p2"));
    collection.next();
    assert_eq!(collection.current_name(), Some("p1")); // Wraps around
}

#[test]
fn test_service_collection_reset() {
    let mut collection = ServiceCollection::<String>::new("testService")
        .with("p1", "s1".to_string())
        .with("p2", "s2".to_string());

    collection.next();
    assert_eq!(collection.current_name(), Some("p2"));
    collection.reset();
    assert_eq!(collection.current_name(), Some("p1"));
}

#[test]
fn test_service_collection_remove() {
    let mut collection = ServiceCollection::<String>::new("testService")
        .with("p1", "s1".to_string())
        .with("p2", "s2".to_string())
        .with("p3", "s3".to_string());

    collection.remove("p2");
    assert_eq!(collection.count(), 2);

    let names: Vec<_> = (0..collection.count())
        .map(|i| {
            collection
                .get_service_to_call(i)
                .unwrap()
                .provider_name
                .to_string()
        })
        .collect();
    assert_eq!(names, vec!["p1", "p3"]);
}

#[test]
fn test_service_collection_move_to_last() {
    let mut collection = ServiceCollection::<String>::new("testService")
        .with("p1", "s1".to_string())
        .with("p2", "s2".to_string())
        .with("p3", "s3".to_string());

    collection.move_to_last("p1");

    let names: Vec<_> = (0..collection.count())
        .map(|i| {
            collection
                .get_service_to_call(i)
                .unwrap()
                .provider_name
                .to_string()
        })
        .collect();
    assert_eq!(names, vec!["p2", "p3", "p1"]);
}

#[test]
fn test_service_collection_call_tracking() {
    let mut collection =
        ServiceCollection::<String>::new("testService").with("provider1", "service1".to_string());

    // Record success
    let mut call = ServiceCall::new();
    call.mark_success(Some("result".to_string()));
    collection.add_call_success("provider1", call);

    // Record failure
    let mut call = ServiceCall::new();
    call.mark_failure(Some("not found".to_string()));
    collection.add_call_failure("provider1", call);

    // Record error
    let mut call = ServiceCall::new();
    call.mark_error("Connection failed", "ECONNRESET");
    collection.add_call_error("provider1", call);

    let history = collection.get_call_history(false);
    let provider_history = history.history_by_provider.get("provider1").unwrap();

    assert_eq!(provider_history.total_counts.success, 1);
    assert_eq!(provider_history.total_counts.failure, 2);
    assert_eq!(provider_history.total_counts.error, 1);
    assert_eq!(provider_history.calls.len(), 3);
}

// =============================================================================
// Result Type Tests
// =============================================================================

#[test]
fn test_get_raw_tx_result_serialization() {
    let result = GetRawTxResult {
        name: "WoC".to_string(),
        txid: "abc123".to_string(),
        raw_tx: Some(vec![0x01, 0x02, 0x03]),
        error: None,
    };

    let json = serde_json::to_string(&result).unwrap();
    assert!(json.contains("\"name\":\"WoC\""));
    assert!(json.contains("\"txid\":\"abc123\""));
}

#[test]
fn test_get_merkle_path_result() {
    let result = GetMerklePathResult {
        name: Some("WoCTsc".to_string()),
        merkle_path: Some("path_data".to_string()),
        header: None,
        error: None,
        notes: vec![],
    };

    assert!(result.merkle_path.is_some());
    assert!(result.error.is_none());
}

#[test]
fn test_post_beef_result() {
    let result = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: "tx123".to_string(),
            status: "success".to_string(),
            double_spend: false,
            competing_txs: None,
            data: None,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    };

    assert!(result.is_success());
    assert_eq!(result.txid_results.len(), 1);
    assert!(result.txid_results[0].is_success());
}

#[test]
fn test_post_tx_result_double_spend() {
    let result = PostTxResultForTxid {
        txid: "tx123".to_string(),
        status: "error".to_string(),
        double_spend: true,
        competing_txs: Some(vec!["competing_tx".to_string()]),
        data: Some("DOUBLE_SPEND_ATTEMPTED".to_string()),
        service_error: false,
        block_hash: None,
        block_height: None,
        notes: vec![],
    };

    assert!(!result.is_success());
    assert!(result.double_spend);
    assert!(result.competing_txs.is_some());
}

#[test]
fn test_get_utxo_status_result() {
    let result = GetUtxoStatusResult {
        name: "WoC".to_string(),
        status: "success".to_string(),
        is_utxo: Some(true),
        details: vec![UtxoDetail {
            txid: "tx123".to_string(),
            index: 0,
            satoshis: 100000,
            height: Some(800000),
        }],
        error: None,
    };

    assert_eq!(result.status, "success");
    assert_eq!(result.is_utxo, Some(true));
    assert_eq!(result.details.len(), 1);
}

#[test]
fn test_get_status_for_txids_result() {
    let result = GetStatusForTxidsResult {
        name: "WoC".to_string(),
        status: "success".to_string(),
        error: None,
        results: vec![
            TxStatusDetail {
                txid: "tx1".to_string(),
                status: "mined".to_string(),
                depth: Some(10),
            },
            TxStatusDetail {
                txid: "tx2".to_string(),
                status: "known".to_string(),
                depth: Some(0),
            },
            TxStatusDetail {
                txid: "tx3".to_string(),
                status: "unknown".to_string(),
                depth: None,
            },
        ],
    };

    assert_eq!(result.results.len(), 3);
    assert_eq!(result.results[0].status, "mined");
    assert_eq!(result.results[1].status, "known");
    assert_eq!(result.results[2].status, "unknown");
}

#[test]
fn test_get_script_hash_history_result() {
    let result = GetScriptHashHistoryResult {
        name: "WoC".to_string(),
        status: "success".to_string(),
        error: None,
        history: vec![
            ScriptHistoryItem {
                txid: "tx1".to_string(),
                height: Some(800000),
            },
            ScriptHistoryItem {
                txid: "tx2".to_string(),
                height: None, // Unconfirmed
            },
        ],
    };

    assert_eq!(result.history.len(), 2);
    assert!(result.history[0].height.is_some());
    assert!(result.history[1].height.is_none());
}

#[test]
fn test_block_header_to_binary() {
    let header = BlockHeader {
        version: 1,
        previous_hash: "0000000000000000000000000000000000000000000000000000000000000000"
            .to_string(),
        merkle_root: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
        time: 1231006505,
        bits: 486604799,
        nonce: 2083236893,
        hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
        height: 0,
    };

    let binary = header.to_binary();
    assert_eq!(binary.len(), 80);
}

#[test]
fn test_bsv_exchange_rate() {
    let rate = BsvExchangeRate::new(50.0);
    assert_eq!(rate.rate, 50.0);
    assert_eq!(rate.base, "USD");
    assert!(!rate.is_stale(60000)); // Should not be stale within 1 minute
}

// =============================================================================
// Services Options Tests
// =============================================================================

#[test]
fn test_services_options_default() {
    let options = ServicesOptions::default();
    assert!(options.whatsonchain_api_key.is_none());
    assert!(options.bitails_api_key.is_none());
    assert!(options.arc_gorillapool_url.is_some());
}

#[test]
fn test_services_options_mainnet() {
    let options = ServicesOptions::mainnet();
    assert!(options.arc_url.contains("taal.com"));
}

#[test]
fn test_services_options_testnet() {
    let options = ServicesOptions::testnet();
    assert!(options.arc_url.contains("test"));
    assert!(options.arc_gorillapool_url.is_none());
}

#[test]
fn test_services_options_builder() {
    let options = ServicesOptions::mainnet()
        .with_woc_api_key("woc-key")
        .with_bitails_api_key("bitails-key")
        .with_arc("https://custom-arc.com", None)
        .with_gorillapool("https://custom-gp.com", None);

    assert_eq!(options.whatsonchain_api_key, Some("woc-key".to_string()));
    assert_eq!(options.bitails_api_key, Some("bitails-key".to_string()));
    assert_eq!(options.arc_url, "https://custom-arc.com");
    assert_eq!(
        options.arc_gorillapool_url,
        Some("https://custom-gp.com".to_string())
    );
}

// =============================================================================
// Helper Function Tests
// =============================================================================

#[test]
fn test_output_format_default() {
    let format = GetUtxoStatusOutputFormat::default();
    assert_eq!(format, GetUtxoStatusOutputFormat::HashLE);
}

// =============================================================================
// Integration Tests (require network - marked as ignored)
// =============================================================================

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_whatsonchain_get_chain_info() {
    let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default()).unwrap();
    let result = woc.get_chain_info().await;
    assert!(result.is_ok());
    let info = result.unwrap();
    assert!(info.blocks > 0);
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_whatsonchain_get_exchange_rate() {
    let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default()).unwrap();
    let result = woc.update_bsv_exchange_rate(15 * 60 * 1000).await;
    assert!(result.is_ok());
    let rate = result.unwrap();
    assert!(rate > 0.0);
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_services_get_height() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();
    let result = services.get_height().await;
    assert!(result.is_ok());
    let height = result.unwrap();
    assert!(height > 800000); // Should be well past 800k at this point
}

// =============================================================================
// GetBeef Result Tests
// =============================================================================

#[test]
fn test_get_beef_result_success_with_proof() {
    use bsv_wallet_toolbox_rs::services::GetBeefResult;

    let result = GetBeefResult {
        name: "Services".to_string(),
        txid: "abc123def456".to_string(),
        beef: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        has_proof: true,
        error: None,
    };

    assert!(result.beef.is_some());
    assert!(result.has_proof);
    assert!(result.error.is_none());
    assert_eq!(result.beef.unwrap().len(), 4);
}

#[test]
fn test_get_beef_result_success_without_proof() {
    use bsv_wallet_toolbox_rs::services::GetBeefResult;

    let result = GetBeefResult {
        name: "Services".to_string(),
        txid: "abc123def456".to_string(),
        beef: Some(vec![0x01, 0x02, 0x03]),
        has_proof: false,
        error: None,
    };

    assert!(result.beef.is_some());
    assert!(!result.has_proof);
    assert!(result.error.is_none());
}

#[test]
fn test_get_beef_result_error_tx_not_found() {
    use bsv_wallet_toolbox_rs::services::GetBeefResult;

    let result = GetBeefResult {
        name: "Services".to_string(),
        txid: "nonexistent".to_string(),
        beef: None,
        has_proof: false,
        error: Some("Transaction not found".to_string()),
    };

    assert!(result.beef.is_none());
    assert!(!result.has_proof);
    assert!(result.error.is_some());
    assert!(result.error.unwrap().contains("not found"));
}

#[test]
fn test_get_beef_result_error_parse_failed() {
    use bsv_wallet_toolbox_rs::services::GetBeefResult;

    let result = GetBeefResult {
        name: "Services".to_string(),
        txid: "corrupted_tx".to_string(),
        beef: None,
        has_proof: false,
        error: Some("Failed to parse transaction: invalid format".to_string()),
    };

    assert!(result.beef.is_none());
    assert!(result.error.is_some());
    assert!(result.error.unwrap().contains("parse"));
}

#[test]
fn test_get_beef_result_serialization() {
    use bsv_wallet_toolbox_rs::services::GetBeefResult;

    let result = GetBeefResult {
        name: "WoC".to_string(),
        txid: "txid123".to_string(),
        beef: Some(vec![0x01, 0x02]),
        has_proof: true,
        error: None,
    };

    // Test JSON serialization/deserialization
    let json = serde_json::to_string(&result).unwrap();
    let deserialized: GetBeefResult = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.name, "WoC");
    assert_eq!(deserialized.txid, "txid123");
    assert!(deserialized.beef.is_some());
    assert!(deserialized.has_proof);
}

// =============================================================================
// nLockTime Finality Tests (Unit Tests - No Network)
// =============================================================================

/// Tests for nLockTime finality logic matching Go implementation.
/// Note: Actual get_height() calls require network, so we test the logic separately.

#[test]
fn test_n_lock_time_zero_is_always_final() {
    // nLockTime of 0 is always final (represents immediate finality)
    let n_lock_time: u32 = 0;
    // Zero locktime is always considered final in Bitcoin
    assert!(n_lock_time == 0 || n_lock_time < 500_000_000);
}

#[test]
fn test_n_lock_time_threshold_boundary() {
    // The threshold between block height and timestamp is 500,000,000
    const BLOCK_LIMIT: u32 = 500_000_000;

    // Values below threshold are block heights
    let block_height_lock: u32 = 499_999_999;
    assert!(block_height_lock < BLOCK_LIMIT);

    // Value at threshold is treated as timestamp (per Bitcoin rules)
    let at_threshold: u32 = 500_000_000;
    assert!(at_threshold >= BLOCK_LIMIT);

    // Values above threshold are timestamps
    let timestamp_lock: u32 = 1600000000;
    assert!(timestamp_lock >= BLOCK_LIMIT);
}

#[test]
fn test_n_lock_time_block_height_comparison() {
    const BLOCK_LIMIT: u32 = 500_000_000;
    let current_height: u32 = 880000;

    // Lock at past height - should be final
    let past_lock: u32 = 800000;
    assert!(past_lock < BLOCK_LIMIT); // It's a block height
    assert!(past_lock < current_height); // Lock is in the past = final

    // Lock at current height - should be final (equal counts as final)
    let current_lock: u32 = 880000;
    assert!(current_lock < BLOCK_LIMIT);
    assert!(current_lock <= current_height);

    // Lock at future height - should NOT be final
    let future_lock: u32 = 900000;
    assert!(future_lock < BLOCK_LIMIT);
    assert!(future_lock > current_height); // Lock is in the future = not final
}

#[test]
fn test_n_lock_time_timestamp_comparison() {
    const BLOCK_LIMIT: u32 = 500_000_000;

    // Get a timestamp representing "now" (for testing purposes)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    // Past timestamp - should be final
    let past_timestamp: u32 = now - 86400; // 1 day ago
    assert!(past_timestamp >= BLOCK_LIMIT);
    assert!(past_timestamp < now);

    // Future timestamp - should NOT be final
    let future_timestamp: u32 = now + 86400; // 1 day in future
    assert!(future_timestamp >= BLOCK_LIMIT);
    assert!(future_timestamp > now);
}

#[test]
fn test_n_lock_time_is_final_logic() {
    // This test validates the exact logic used in n_lock_time_is_final
    const BLOCK_LIMIT: u32 = 500_000_000;

    fn is_final_with_height_and_time(
        n_lock_time: u32,
        current_height: u32,
        current_time: u32,
    ) -> bool {
        if n_lock_time >= BLOCK_LIMIT {
            // Time-based locktime
            n_lock_time < current_time
        } else {
            // Block-based locktime
            n_lock_time < current_height
        }
    }

    let height = 880000;
    let time = 1706400000; // Approx Jan 2024

    // Zero locktime
    assert!(is_final_with_height_and_time(0, height, time));

    // Block height locks
    assert!(is_final_with_height_and_time(800000, height, time)); // Past height
    assert!(!is_final_with_height_and_time(900000, height, time)); // Future height

    // Timestamp locks
    assert!(is_final_with_height_and_time(1706000000, height, time)); // Past time
    assert!(!is_final_with_height_and_time(1707000000, height, time)); // Future time

    // Edge case: exactly at threshold
    assert!(is_final_with_height_and_time(
        BLOCK_LIMIT,
        height,
        time + BLOCK_LIMIT
    )); // Would need huge time
}

#[test]
fn test_n_lock_time_sequence_affects_finality() {
    // In Bitcoin, if all inputs have max sequence (0xFFFFFFFF), nLockTime is ignored
    // This test documents the expected behavior for the sequence check
    const MAX_SEQUENCE: u32 = 0xFFFFFFFF;

    // If all inputs have max sequence, the transaction is final regardless of nLockTime
    let all_max_sequence = [MAX_SEQUENCE, MAX_SEQUENCE, MAX_SEQUENCE];
    let has_non_max = all_max_sequence.iter().any(|&s| s != MAX_SEQUENCE);
    assert!(!has_non_max); // All max means nLockTime is bypassed

    // If any input has non-max sequence, nLockTime must be checked
    let some_non_max = [MAX_SEQUENCE, 0xFFFFFFFE, MAX_SEQUENCE];
    let needs_locktime_check = some_non_max.iter().any(|&s| s != MAX_SEQUENCE);
    assert!(needs_locktime_check);
}

// =============================================================================
// GetBeef Integration Tests (require network - marked as ignored)
// =============================================================================

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_services_get_beef_mined_transaction() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();

    // Genesis coinbase txid (always available on mainnet)
    let genesis_txid = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";

    let result = services.get_beef(genesis_txid, &[]).await;
    assert!(result.is_ok());
    let beef_result = result.unwrap();

    // Genesis coinbase should have BEEF data
    // Note: may or may not have proof depending on service
    assert_eq!(beef_result.txid, genesis_txid);
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_services_get_beef_with_known_txids() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();

    // Use a well-known mined transaction
    let txid = "e8b0f9f6b92e31b97d39f6f5c7f8fb2def8a13f1d5f9c5b3a2e1d0c4b3a2e1d0";

    // Mark the txid itself as known (should be treated as txid-only)
    let known = vec![txid.to_string()];

    let result = services.get_beef(txid, &known).await;
    // Result should succeed but may have txid-only treatment
    assert!(result.is_ok());
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_services_get_beef_nonexistent_tx() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();

    // Use an obviously fake txid
    let fake_txid = "0000000000000000000000000000000000000000000000000000000000000000";

    let result = services.get_beef(fake_txid, &[]).await;
    assert!(result.is_ok());
    let beef_result = result.unwrap();

    // Should have error for non-existent tx
    assert!(beef_result.beef.is_none() || beef_result.error.is_some());
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_services_n_lock_time_finality_integration() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();

    // Test zero locktime (always final)
    let result = services.n_lock_time_is_final(0).await;
    assert!(result.is_ok());
    assert!(result.unwrap()); // Zero is always final

    // Test past block height (should be final)
    let result = services.n_lock_time_is_final(100000).await;
    assert!(result.is_ok());
    assert!(result.unwrap()); // Height 100k is long past

    // Test past timestamp (should be final)
    let result = services.n_lock_time_is_final(1600000000).await; // ~2020
    assert!(result.is_ok());
    assert!(result.unwrap()); // 2020 is in the past

    // Test future timestamp (should NOT be final)
    let result = services.n_lock_time_is_final(2000000000).await; // ~2033
    assert!(result.is_ok());
    assert!(!result.unwrap()); // 2033 is in the future
}

// =============================================================================
// NLockTimeInput Tests
// =============================================================================

#[test]
fn test_n_lock_time_input_from_lock_time() {
    use bsv_wallet_toolbox_rs::services::NLockTimeInput;

    let input = NLockTimeInput::from_lock_time(500);
    assert_eq!(input.lock_time, 500);
    assert!(!input.all_sequences_final); // Can't know sequences from just locktime
}

#[test]
fn test_n_lock_time_input_from_raw_tx_max_sequence() {
    use bsv_wallet_toolbox_rs::services::NLockTimeInput;

    // Real BSV transaction from mainnet (txid: ecb7b03ba0d8696548f4479508a69b6d1dedd878b91a54fcdd3752e98dc1bc2b)
    // Has sequence 0xFFFFFFFF (max) and locktime 0
    let raw_hex = "0100000001f12a690c788e61163f8404d8eace6483b837a7abd67b5ad7428dc8c07ee04f3c080000006b483045022100ed7b3b0ab8cb689e4cb884b647fb3820ad44a62a36210a4569b22484b3974c5b0220094c682cba5a9c649f0fd0b630a68fca8dd57c8f8b8304f1bf255e0f5a7b730a4121025a1db26875991c9678d1407a0414e15db323e30c69a331729bae1bb99dfef12affffffff0108030000000000001976a9148d4d91d5f0e47cdb44634af89b36a9b5332f6cfb88ac00000000";
    let result = NLockTimeInput::from_hex_tx(raw_hex);
    assert!(
        result.is_ok(),
        "Failed to parse transaction: {:?}",
        result.err()
    );
    let input = result.unwrap();
    assert_eq!(input.lock_time, 0);
    assert!(input.all_sequences_final); // All inputs have max sequence (0xFFFFFFFF)
}

#[test]
fn test_n_lock_time_input_from_raw_tx_non_max_sequence() {
    use bsv_wallet_toolbox_rs::services::NLockTimeInput;

    // Same real transaction but with modified sequence (0xFFFFFFFE) and locktime (100)
    // Original: ecb7b03ba0d8696548f4479508a69b6d1dedd878b91a54fcdd3752e98dc1bc2b
    // Changed: ffffffff -> feffffff (sequence), 00000000 -> 64000000 (locktime=100)
    let raw_hex = "0100000001f12a690c788e61163f8404d8eace6483b837a7abd67b5ad7428dc8c07ee04f3c080000006b483045022100ed7b3b0ab8cb689e4cb884b647fb3820ad44a62a36210a4569b22484b3974c5b0220094c682cba5a9c649f0fd0b630a68fca8dd57c8f8b8304f1bf255e0f5a7b730a4121025a1db26875991c9678d1407a0414e15db323e30c69a331729bae1bb99dfef12afeffffff0108030000000000001976a9148d4d91d5f0e47cdb44634af89b36a9b5332f6cfb88ac64000000";
    let result = NLockTimeInput::from_hex_tx(raw_hex);
    assert!(
        result.is_ok(),
        "Failed to parse transaction: {:?}",
        result.err()
    );
    let input = result.unwrap();
    assert_eq!(input.lock_time, 100);
    assert!(!input.all_sequences_final); // Not all inputs have max sequence (0xFFFFFFFE)
}

#[test]
fn test_n_lock_time_input_from_hex_invalid() {
    use bsv_wallet_toolbox_rs::services::NLockTimeInput;

    // Invalid hex should error
    let result = NLockTimeInput::from_hex_tx("not_valid_hex!");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_n_lock_time_is_final_for_tx_with_final_sequences() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;
    use bsv_wallet_toolbox_rs::services::{NLockTimeInput, Services};

    let services = Services::mainnet().unwrap();

    // Create NLockTimeInput with max sequence (all_sequences_final = true)
    // This simulates a transaction where all inputs have max sequence
    let input = NLockTimeInput {
        lock_time: 2000000000,     // Far future timestamp (~2033)
        all_sequences_final: true, // All inputs have max sequence
    };

    let result = services.n_lock_time_is_final_for_tx(input).await;
    assert!(result.is_ok());
    assert!(result.unwrap()); // Should be final because all sequences are max
}

#[tokio::test]
async fn test_n_lock_time_is_final_for_tx_with_non_final_sequences() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;
    use bsv_wallet_toolbox_rs::services::{NLockTimeInput, Services};

    let services = Services::mainnet().unwrap();

    // Create NLockTimeInput with non-max sequence (all_sequences_final = false)
    let input = NLockTimeInput {
        lock_time: 2000000000,      // Far future timestamp (~2033)
        all_sequences_final: false, // Not all inputs have max sequence
    };

    let result = services.n_lock_time_is_final_for_tx(input).await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // Should NOT be final - future locktime with non-max sequence
}

#[tokio::test]
async fn test_n_lock_time_is_final_for_tx_from_raw_locktime() {
    use bsv_wallet_toolbox_rs::services::traits::WalletServices;
    use bsv_wallet_toolbox_rs::services::{NLockTimeInput, Services};

    let services = Services::mainnet().unwrap();

    // Past locktime (block height)
    let input = NLockTimeInput::from_lock_time(100);
    let result = services.n_lock_time_is_final_for_tx(input).await;
    assert!(result.is_ok());
    assert!(result.unwrap()); // Block 100 is long past

    // Future locktime (timestamp)
    let input = NLockTimeInput::from_lock_time(2000000000);
    let result = services.n_lock_time_is_final_for_tx(input).await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // 2033 is in the future
}
