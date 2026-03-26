//! Double-spend detection tests
//!
//! Tests for handling double-spend scenarios during transaction broadcast.
//! These tests verify the data structures used to represent double-spend
//! results without requiring network calls or database access.

use std::collections::HashMap;

use bsv_wallet_toolbox::{PostBeefResult, PostTxResultForTxid};

// =============================================================================
// PostBeefResult double-spend field tests
// =============================================================================

#[tokio::test]
async fn test_post_beef_result_double_spend_fields() {
    // Create a PostBeefResult with double_spend indicators on a per-txid basis
    let competing =
        vec!["aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233".to_string()];

    let txid_result = PostTxResultForTxid {
        txid: "11223344556677881122334455667788112233445566778811223344556677ab".to_string(),
        status: "error".to_string(),
        double_spend: true,
        competing_txs: Some(competing.clone()),
        data: Some("DOUBLE_SPEND_ATTEMPTED".to_string()),
        service_error: false,
        block_hash: None,
        block_height: None,
        notes: Vec::new(),
    };

    // Verify the double_spend flag and competing_txs are populated
    assert!(txid_result.double_spend);
    assert!(!txid_result.is_success());
    assert_eq!(txid_result.competing_txs.as_ref().unwrap().len(), 1);
    assert_eq!(txid_result.competing_txs.as_ref().unwrap()[0], competing[0]);
    assert!(
        !txid_result.service_error,
        "Double-spend is not a service error"
    );

    // Wrap in a PostBeefResult
    let result = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "error".to_string(),
        txid_results: vec![txid_result],
        error: None,
        notes: Vec::new(),
    };

    assert!(!result.is_success());
    assert_eq!(result.txid_results.len(), 1);
    assert!(result.txid_results[0].double_spend);
}

#[tokio::test]
async fn test_post_beef_result_success() {
    // Create a successful PostBeefResult with no double_spend
    let txid_result = PostTxResultForTxid {
        txid: "ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00".to_string(),
        status: "success".to_string(),
        double_spend: false,
        competing_txs: None,
        data: Some("SEEN_ON_NETWORK".to_string()),
        service_error: false,
        block_hash: None,
        block_height: None,
        notes: Vec::new(),
    };

    assert!(txid_result.is_success());
    assert!(!txid_result.double_spend);
    assert!(txid_result.competing_txs.is_none());

    let result = PostBeefResult {
        name: "arcGorillaPool".to_string(),
        status: "success".to_string(),
        txid_results: vec![txid_result],
        error: None,
        notes: Vec::new(),
    };

    assert!(result.is_success());
    assert!(result.error.is_none());
}

#[tokio::test]
async fn test_post_beef_result_serialization() {
    // Verify PostBeefResult serializes/deserializes correctly, including double_spend fields
    let mut note = HashMap::new();
    note.insert(
        "what".to_string(),
        serde_json::Value::String("postBeefDoubleSpend".to_string()),
    );

    let txid_result = PostTxResultForTxid {
        txid: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string(),
        status: "error".to_string(),
        double_spend: true,
        competing_txs: Some(vec![
            "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        ]),
        data: Some("DOUBLE_SPEND_ATTEMPTED extra info".to_string()),
        service_error: false,
        block_hash: None,
        block_height: None,
        notes: vec![note],
    };

    let result = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "error".to_string(),
        txid_results: vec![txid_result],
        error: None,
        notes: Vec::new(),
    };

    // Serialize to JSON
    let json = serde_json::to_string(&result).expect("Should serialize PostBeefResult");

    // Verify key fields are present in JSON
    assert!(json.contains("\"doubleSpend\":true") || json.contains("\"double_spend\":true"));
    assert!(json.contains("competingTxs") || json.contains("competing_txs"));

    // Deserialize back
    let deserialized: PostBeefResult =
        serde_json::from_str(&json).expect("Should deserialize PostBeefResult");

    assert_eq!(deserialized.name, "arcTaal");
    assert!(!deserialized.is_success());
    assert_eq!(deserialized.txid_results.len(), 1);
    assert!(deserialized.txid_results[0].double_spend);
    assert_eq!(
        deserialized.txid_results[0]
            .competing_txs
            .as_ref()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn test_transaction_status_values() {
    // Verify all expected TransactionStatus variants can be serialized/deserialized
    use bsv_wallet_toolbox::storage::entities::{ProvenTxReqStatus, TransactionStatus};

    // Test TransactionStatus variants and their string representations
    let statuses = vec![
        (TransactionStatus::NoSend, "nosend"),
        (TransactionStatus::Unsigned, "unsigned"),
        (TransactionStatus::Unprocessed, "unprocessed"),
        (TransactionStatus::Sending, "sending"),
        (TransactionStatus::Unproven, "unproven"),
        (TransactionStatus::Completed, "completed"),
        (TransactionStatus::Failed, "failed"),
        (TransactionStatus::NonFinal, "nonfinal"),
        (TransactionStatus::Unfail, "unfail"),
    ];

    for (status, expected_str) in &statuses {
        assert_eq!(
            status.as_str(),
            *expected_str,
            "Status string mismatch for {:?}",
            status
        );

        // Roundtrip via serde
        let json = serde_json::to_string(status).expect("Should serialize TransactionStatus");
        let deserialized: TransactionStatus =
            serde_json::from_str(&json).expect("Should deserialize TransactionStatus");
        assert_eq!(*status, deserialized, "Roundtrip mismatch for {:?}", status);
    }

    // Test ProvenTxReqStatus::DoubleSpend variant specifically
    let ds_status = ProvenTxReqStatus::DoubleSpend;
    let json = serde_json::to_string(&ds_status).expect("Should serialize DoubleSpend status");
    let deserialized: ProvenTxReqStatus =
        serde_json::from_str(&json).expect("Should deserialize DoubleSpend status");
    assert_eq!(deserialized, ProvenTxReqStatus::DoubleSpend);

    // Verify all ProvenTxReqStatus variants roundtrip
    let ptx_statuses = vec![
        ProvenTxReqStatus::Pending,
        ProvenTxReqStatus::InProgress,
        ProvenTxReqStatus::Completed,
        ProvenTxReqStatus::Failed,
        ProvenTxReqStatus::NotFound,
        ProvenTxReqStatus::Unsent,
        ProvenTxReqStatus::Sending,
        ProvenTxReqStatus::Unmined,
        ProvenTxReqStatus::Unknown,
        ProvenTxReqStatus::Callback,
        ProvenTxReqStatus::Unconfirmed,
        ProvenTxReqStatus::Unfail,
        ProvenTxReqStatus::NoSend,
        ProvenTxReqStatus::Invalid,
        ProvenTxReqStatus::DoubleSpend,
    ];

    for status in &ptx_statuses {
        let json = serde_json::to_string(status).expect("Should serialize ProvenTxReqStatus");
        let deserialized: ProvenTxReqStatus =
            serde_json::from_str(&json).expect("Should deserialize ProvenTxReqStatus");
        assert_eq!(*status, deserialized, "Roundtrip mismatch for {:?}", status);
    }
}

#[tokio::test]
async fn test_double_spend_result_handling() {
    // Create a PostBeefResult indicating double-spend and verify it can be
    // pattern-matched and checked programmatically.
    let result = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "error".to_string(),
        txid_results: vec![
            // First txid: successful
            PostTxResultForTxid {
                txid: "aaaa".repeat(16),
                status: "success".to_string(),
                double_spend: false,
                competing_txs: None,
                data: Some("SEEN_ON_NETWORK".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: Vec::new(),
            },
            // Second txid: double-spend
            PostTxResultForTxid {
                txid: "bbbb".repeat(16),
                status: "error".to_string(),
                double_spend: true,
                competing_txs: Some(vec!["cccc".repeat(16)]),
                data: Some("DOUBLE_SPEND_ATTEMPTED".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: Vec::new(),
            },
            // Third txid: service error (not double-spend)
            PostTxResultForTxid {
                txid: "dddd".repeat(16),
                status: "error".to_string(),
                double_spend: false,
                competing_txs: None,
                data: Some("Request failed: timeout".to_string()),
                service_error: true,
                block_hash: None,
                block_height: None,
                notes: Vec::new(),
            },
        ],
        error: None,
        notes: Vec::new(),
    };

    // Check overall result
    assert!(!result.is_success());

    // Find double-spend results
    let double_spends: Vec<&PostTxResultForTxid> = result
        .txid_results
        .iter()
        .filter(|r| r.double_spend)
        .collect();
    assert_eq!(double_spends.len(), 1);
    assert_eq!(double_spends[0].txid, "bbbb".repeat(16));

    // Find service errors
    let service_errors: Vec<&PostTxResultForTxid> = result
        .txid_results
        .iter()
        .filter(|r| r.service_error)
        .collect();
    assert_eq!(service_errors.len(), 1);
    assert_eq!(service_errors[0].txid, "dddd".repeat(16));

    // Find successes
    let successes: Vec<&PostTxResultForTxid> = result
        .txid_results
        .iter()
        .filter(|r| r.is_success())
        .collect();
    assert_eq!(successes.len(), 1);
    assert_eq!(successes[0].txid, "aaaa".repeat(16));

    // Verify a successful result with block info
    let mined_result = PostTxResultForTxid {
        txid: "eeee".repeat(16),
        status: "success".to_string(),
        double_spend: false,
        competing_txs: None,
        data: Some("MINED".to_string()),
        service_error: false,
        block_hash: Some(
            "0000000000000000000123456789abcdef0123456789abcdef0123456789abcd".to_string(),
        ),
        block_height: Some(800_000),
        notes: Vec::new(),
    };

    assert!(mined_result.is_success());
    assert!(!mined_result.double_spend);
    assert_eq!(mined_result.block_height, Some(800_000));
}
