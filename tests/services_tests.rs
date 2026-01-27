//! Integration tests for the services layer.
//!
//! These tests verify:
//! - Service creation and configuration
//! - Provider construction
//! - Service collection failover behavior
//! - Result type serialization

use bsv_wallet_toolbox::services::{
    collection::{ServiceCall, ServiceCollection},
    traits::{
        BlockHeader, BsvExchangeRate, GetMerklePathResult, GetRawTxResult,
        GetScriptHashHistoryResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat,
        GetUtxoStatusResult, PostBeefResult, PostTxResultForTxid, ScriptHistoryItem,
        TxStatusDetail, UtxoDetail,
    },
    Arc, ArcConfig, Bitails, BitailsConfig, Chain, Services,
    ServicesOptions, WhatsOnChain, WhatsOnChainConfig,
};

// =============================================================================
// Services Creation Tests
// =============================================================================

#[test]
fn test_services_mainnet_creation() {
    let services = Services::mainnet();
    assert!(services.is_ok());
    let services = services.unwrap();
    assert!(services.get_merkle_path_count() >= 1);
    assert!(services.get_raw_tx_count() >= 1);
    assert!(services.post_beef_count() >= 1);
    assert!(services.get_utxo_status_count() >= 1);
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

    assert_eq!(options.whatsonchain_api_key, Some("test-api-key".to_string()));
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
        .with_callback("https://example.com/callback", Some("callback-token".to_string()));

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
        .map(|i| collection.get_service_to_call(i).unwrap().provider_name.to_string())
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
        .map(|i| collection.get_service_to_call(i).unwrap().provider_name.to_string())
        .collect();
    assert_eq!(names, vec!["p2", "p3", "p1"]);
}

#[test]
fn test_service_collection_call_tracking() {
    let mut collection = ServiceCollection::<String>::new("testService")
        .with("provider1", "service1".to_string());

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
        merkle_root: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
            .to_string(),
        time: 1231006505,
        bits: 486604799,
        nonce: 2083236893,
        hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
            .to_string(),
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
    use bsv_wallet_toolbox::services::traits::WalletServices;

    let services = Services::mainnet().unwrap();
    let result = services.get_height().await;
    assert!(result.is_ok());
    let height = result.unwrap();
    assert!(height > 800000); // Should be well past 800k at this point
}
