//! BEEF format edge case tests
//!
//! Tests for BEEF (Background Evaluation Extended Format) data structures
//! and result types, focusing on edge cases and serialization.

use std::collections::HashMap;

use bsv_wallet_toolbox::services::GetBeefResult;
use bsv_wallet_toolbox::storage::BeefVerificationMode;
use bsv_wallet_toolbox::{PostBeefResult, PostTxResultForTxid};

// =============================================================================
// BEEF edge case tests
// =============================================================================

#[tokio::test]
async fn test_beef_empty_bytes() {
    // Empty BEEF data should fail to parse via bsv-sdk's Beef::from_binary.
    // We verify this indirectly by testing that GetBeefResult can represent
    // a failure case with an error message.
    let result = GetBeefResult {
        name: "WoC".to_string(),
        txid: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        beef: None,
        has_proof: false,
        error: Some("Empty BEEF data".to_string()),
    };

    assert!(result.beef.is_none());
    assert!(!result.has_proof);
    assert!(result.error.is_some());
    assert_eq!(result.error.as_ref().unwrap(), "Empty BEEF data");

    // Verify serialization roundtrip
    let json = serde_json::to_string(&result).expect("Should serialize GetBeefResult");
    let deserialized: GetBeefResult =
        serde_json::from_str(&json).expect("Should deserialize GetBeefResult");
    assert_eq!(deserialized.name, "WoC");
    assert!(deserialized.beef.is_none());
    assert!(deserialized.error.is_some());
}

#[tokio::test]
async fn test_beef_invalid_version() {
    // BEEF v1 starts with 0100BEEF, v2 with 0200BEEF.
    // An invalid version byte should be flagged. We test that the result
    // types can represent this scenario.
    let invalid_beef_bytes = vec![0x99, 0x00, 0xBE, 0xEF, 0x00]; // Invalid version 0x99

    let result = GetBeefResult {
        name: "Bitails".to_string(),
        txid: "aa".repeat(32),
        beef: Some(invalid_beef_bytes.clone()),
        has_proof: false,
        error: Some("Invalid BEEF version: 0x99".to_string()),
    };

    assert!(result.beef.is_some());
    assert_eq!(result.beef.as_ref().unwrap().len(), 5);
    assert!(!result.has_proof);
    assert!(result
        .error
        .as_ref()
        .unwrap()
        .contains("Invalid BEEF version"));

    // Verify the BEEF bytes are preserved through serialization
    let json = serde_json::to_string(&result).expect("Should serialize");
    let deserialized: GetBeefResult = serde_json::from_str(&json).expect("Should deserialize");
    assert_eq!(deserialized.beef.as_ref().unwrap(), &invalid_beef_bytes);
}

#[tokio::test]
async fn test_beef_result_types() {
    // Verify PostBeefResult can represent various broadcast outcomes:
    // success, error, partial success, double-spend, service error.

    // 1. Full success
    let success = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: "aa".repeat(32),
            status: "success".to_string(),
            double_spend: false,
            competing_txs: None,
            data: Some("SEEN_ON_NETWORK".to_string()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: Vec::new(),
        }],
        error: None,
        notes: Vec::new(),
    };
    assert!(success.is_success());

    // 2. Overall error with error message
    let error = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "error".to_string(),
        txid_results: Vec::new(),
        error: Some("Connection refused".to_string()),
        notes: Vec::new(),
    };
    assert!(!error.is_success());
    assert!(error.error.is_some());

    // 3. Partial success (one tx succeeds, another fails)
    let partial = PostBeefResult {
        name: "arcGorillaPool".to_string(),
        status: "error".to_string(),
        txid_results: vec![
            PostTxResultForTxid {
                txid: "aa".repeat(32),
                status: "success".to_string(),
                double_spend: false,
                competing_txs: None,
                data: None,
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: Vec::new(),
            },
            PostTxResultForTxid {
                txid: "bb".repeat(32),
                status: "error".to_string(),
                double_spend: false,
                competing_txs: None,
                data: Some("Transaction not found".to_string()),
                service_error: true,
                block_hash: None,
                block_height: None,
                notes: Vec::new(),
            },
        ],
        error: None,
        notes: Vec::new(),
    };
    assert!(!partial.is_success());
    assert_eq!(partial.txid_results.len(), 2);
    assert!(partial.txid_results[0].is_success());
    assert!(!partial.txid_results[1].is_success());

    // 4. Already mined result
    let mined = PostTxResultForTxid {
        txid: "cc".repeat(32),
        status: "success".to_string(),
        double_spend: false,
        competing_txs: None,
        data: Some("MINED".to_string()),
        service_error: false,
        block_hash: Some("00".repeat(32)),
        block_height: Some(800_000),
        notes: Vec::new(),
    };
    assert!(mined.is_success());
    assert_eq!(mined.block_height, Some(800_000));
}

#[tokio::test]
async fn test_atomic_beef_missing_fields() {
    // Test handling of incomplete atomic BEEF data by constructing a
    // GetBeefResult with missing proof info and verifying its state.

    // Case 1: BEEF with no proof (unproven transaction)
    let no_proof = GetBeefResult {
        name: "WoC".to_string(),
        txid: "dd".repeat(32),
        beef: Some(vec![0x01, 0x00, 0xBE, 0xEF]),
        has_proof: false,
        error: None,
    };
    assert!(no_proof.beef.is_some());
    assert!(!no_proof.has_proof);
    assert!(no_proof.error.is_none());

    // Case 2: BEEF retrieval failed entirely
    let failed = GetBeefResult {
        name: "Bitails".to_string(),
        txid: "ee".repeat(32),
        beef: None,
        has_proof: false,
        error: Some("Service unavailable".to_string()),
    };
    assert!(failed.beef.is_none());
    assert!(!failed.has_proof);

    // Case 3: Successful BEEF with proof
    let with_proof = GetBeefResult {
        name: "WoC".to_string(),
        txid: "ff".repeat(32),
        beef: Some(vec![0x01, 0x00, 0xBE, 0xEF, 0x01, 0x02, 0x03]),
        has_proof: true,
        error: None,
    };
    assert!(with_proof.beef.is_some());
    assert!(with_proof.has_proof);
    assert!(with_proof.error.is_none());

    // Verify all three serialize cleanly
    for result in [&no_proof, &failed, &with_proof] {
        let json = serde_json::to_string(result).expect("Should serialize GetBeefResult");
        let roundtrip: GetBeefResult =
            serde_json::from_str(&json).expect("Should deserialize GetBeefResult");
        assert_eq!(roundtrip.name, result.name);
        assert_eq!(roundtrip.txid, result.txid);
        assert_eq!(roundtrip.has_proof, result.has_proof);
    }
}

#[tokio::test]
async fn test_beef_broadcast_result_serialization() {
    // Verify BEEF-related types serialize correctly with all optional fields.
    let mut note1 = HashMap::new();
    note1.insert(
        "what".to_string(),
        serde_json::Value::String("postBeefV2Detected".to_string()),
    );
    note1.insert(
        "name".to_string(),
        serde_json::Value::String("arcTaal".to_string()),
    );
    note1.insert(
        "when".to_string(),
        serde_json::Value::String("2026-01-15T10:30:00Z".to_string()),
    );

    let mut note2 = HashMap::new();
    note2.insert(
        "what".to_string(),
        serde_json::Value::String("postBeefV2ToV1".to_string()),
    );

    let result = PostBeefResult {
        name: "arcTaal".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: "ab".repeat(32),
            status: "success".to_string(),
            double_spend: false,
            competing_txs: None,
            data: Some("STORED additional-info".to_string()),
            service_error: false,
            block_hash: Some("00".repeat(32)),
            block_height: Some(850_123),
            notes: vec![note1.clone()],
        }],
        error: None,
        notes: vec![note1, note2],
    };

    let json = serde_json::to_string_pretty(&result).expect("Should serialize PostBeefResult");

    // Verify the JSON contains expected fields
    assert!(json.contains("\"name\":"));
    assert!(json.contains("arcTaal"));
    assert!(json.contains("postBeefV2Detected"));
    assert!(json.contains("postBeefV2ToV1"));

    // Deserialize and verify
    let deserialized: PostBeefResult =
        serde_json::from_str(&json).expect("Should deserialize PostBeefResult");
    assert!(deserialized.is_success());
    assert_eq!(deserialized.notes.len(), 2);
    assert_eq!(deserialized.txid_results.len(), 1);
    assert_eq!(deserialized.txid_results[0].block_height, Some(850_123));

    // Test GetBeefResult serialization with optional fields
    let beef_result = GetBeefResult {
        name: "WoC".to_string(),
        txid: "cc".repeat(32),
        beef: Some(vec![1, 0, 0xBE, 0xEF, 0x00, 0x01]),
        has_proof: true,
        error: None,
    };

    let json2 = serde_json::to_string(&beef_result).expect("Should serialize GetBeefResult");
    // Error field should be omitted (skip_serializing_if = "Option::is_none")
    assert!(!json2.contains("\"error\""));

    let roundtrip: GetBeefResult =
        serde_json::from_str(&json2).expect("Should deserialize GetBeefResult");
    assert_eq!(roundtrip.txid, "cc".repeat(32));
    assert!(roundtrip.has_proof);
    assert!(roundtrip.error.is_none());

    // Test BeefVerificationMode serialization roundtrips
    let modes = vec![
        (BeefVerificationMode::Strict, "\"strict\""),
        (BeefVerificationMode::TrustKnown, "\"trustKnown\""),
        (BeefVerificationMode::Disabled, "\"disabled\""),
    ];

    for (mode, expected_json) in modes {
        let json = serde_json::to_string(&mode).expect("Should serialize BeefVerificationMode");
        assert_eq!(json, expected_json, "JSON mismatch for {:?}", mode);

        let roundtrip: BeefVerificationMode =
            serde_json::from_str(&json).expect("Should deserialize BeefVerificationMode");
        assert_eq!(roundtrip, mode, "Roundtrip mismatch for {:?}", mode);
    }

    // Verify default is Strict
    assert_eq!(
        BeefVerificationMode::default(),
        BeefVerificationMode::Strict
    );
}
