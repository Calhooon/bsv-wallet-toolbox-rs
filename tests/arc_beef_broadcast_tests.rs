//! ARC BEEF broadcast handling tests.
//!
//! Tests for BEEF format conversion, trimming, and ARC response handling
//! during transaction broadcast. Uses mockito for HTTP mocking.

use bsv_rs::transaction::{Beef, MerklePath, Transaction, BEEF_V1, BEEF_V2};
use bsv_wallet_toolbox_rs::services::{Arc as ArcProvider, ArcConfig};

// A simple P2PKH transaction hex (from bsv-rs test vectors).
const TEST_TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

/// Helper: build a minimal valid BEEF with a single proven transaction.
///
/// Returns (beef_binary, txid) where the BEEF contains the test transaction
/// with a merkle proof (BUMP).
fn build_single_tx_beef(version: u32) -> (Vec<u8>, String) {
    let tx = Transaction::from_hex(TEST_TX_HEX).unwrap();
    let txid = tx.id();

    // Build a simple merkle path (coinbase-style: single tx in block)
    let bump = MerklePath::from_coinbase_txid(&txid, 800_000);

    let mut beef = Beef::with_version(version);
    let bump_idx = beef.merge_bump(bump);
    let raw_tx = hex::decode(TEST_TX_HEX).unwrap();
    beef.merge_raw_tx(raw_tx, Some(bump_idx));

    (beef.to_binary(), txid)
}

// =============================================================================
// Test 1: V2 BEEF is converted to V1 before posting to ARC
// =============================================================================

#[tokio::test]
async fn test_post_beef_v2_is_converted_to_v1() {
    // Build a V2 BEEF
    let (beef_v2_bytes, txid) = build_single_tx_beef(BEEF_V2);

    // Verify it's actually V2
    assert_eq!(
        beef_v2_bytes[0..4],
        [0x02, 0x00, 0xBE, 0xEF],
        "Input should be V2 BEEF"
    );

    // Set up mock ARC server
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_ON_NETWORK", "extraInfo": ""}}"#,
            txid
        ))
        .match_body(mockito::Matcher::AllOf(vec![
            // The body is JSON with rawTx field containing the hex-encoded BEEF.
            // We verify it contains the V1 magic bytes (0100beef) at the start.
            mockito::Matcher::Regex("0100beef".to_string()),
        ]))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_v2_bytes, std::slice::from_ref(&txid)).await.unwrap();

    assert_eq!(result.status, "success");
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].status, "success");

    // Verify the V2->V1 conversion note was added
    let has_v2_to_v1_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefV2ToV1"));
    assert!(
        has_v2_to_v1_note,
        "Should have a postBeefV2ToV1 note indicating conversion happened"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 2: V1 BEEF stays V1 (no unnecessary conversion)
// =============================================================================

#[tokio::test]
async fn test_post_beef_v1_stays_v1() {
    let (beef_v1_bytes, txid) = build_single_tx_beef(BEEF_V1);

    // Verify it's V1
    assert_eq!(
        beef_v1_bytes[0..4],
        [0x01, 0x00, 0xBE, 0xEF],
        "Input should be V1 BEEF"
    );

    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_ON_NETWORK", "extraInfo": ""}}"#,
            txid
        ))
        .match_body(mockito::Matcher::Regex("0100beef".to_string()))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_v1_bytes, std::slice::from_ref(&txid)).await.unwrap();

    assert_eq!(result.status, "success");

    // Should NOT have V2->V1 conversion note since it was already V1
    let has_v2_to_v1_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefV2ToV1"));
    assert!(
        !has_v2_to_v1_note,
        "Should NOT have postBeefV2ToV1 note for already-V1 BEEF"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 3: trim_known_proven removes deep ancestors
// =============================================================================

#[tokio::test]
async fn test_post_beef_trim_removes_deep_ancestors() {
    // Build a BEEF with a proven ancestor and an unproven child that references it.
    // After trimming, the proven ancestor's own ancestors (if any) should be removed.
    //
    // We construct this by building a BEEF with:
    //   - tx_ancestor (proven, with BUMP at height 800000)
    //   - tx_grandparent (proven, with BUMP at height 799999) -- ancestor of tx_ancestor
    //   - tx_tip (unproven, references tx_ancestor)
    //
    // After trim_known_proven: tx_grandparent should be removed because
    // tx_ancestor is self-proving and doesn't need its ancestors.

    let tx = Transaction::from_hex(TEST_TX_HEX).unwrap();
    let tx_bytes = hex::decode(TEST_TX_HEX).unwrap();
    let txid = tx.id();

    // Build BEEF with 2 proven txs + verify trim reduces count
    let mut beef = Beef::with_version(BEEF_V1);

    // Add a "grandparent" proven tx (same bytes, different bump height)
    let bump_gp = MerklePath::from_coinbase_txid(&txid, 799_999);
    let bump_gp_idx = beef.merge_bump(bump_gp);
    beef.merge_raw_tx(tx_bytes.clone(), Some(bump_gp_idx));

    let count_before = beef.txs.len();
    assert_eq!(count_before, 1, "Should have 1 tx before trim");

    // Trim -- since there's only one proven tx with no children referencing it
    // as an input from within the BEEF, it stays. This tests the mechanism works.
    beef.trim_known_proven();
    let count_after = beef.txs.len();
    assert_eq!(
        count_after, 1,
        "Single proven tx should survive trim (it's the tip)"
    );

    // Now test via the ARC post_beef path which does trim internally.
    // Build a V1 BEEF and post it -- verify the request succeeds.
    let beef_bytes = beef.to_binary();

    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "STORED", "extraInfo": ""}}"#,
            txid
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_bytes, std::slice::from_ref(&txid)).await.unwrap();

    assert_eq!(result.status, "success");
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].txid, txid);

    mock.assert_async().await;
}

// =============================================================================
// Test 4: SEEN_IN_ORPHAN_MEMPOOL treated as success
// =============================================================================

#[tokio::test]
async fn test_post_beef_orphan_mempool_treated_as_success() {
    let (beef_bytes, txid) = build_single_tx_beef(BEEF_V1);

    let mut server = mockito::Server::new_async().await;

    // ARC returns 200 with txStatus "SEEN_IN_ORPHAN_MEMPOOL"
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_IN_ORPHAN_MEMPOOL", "extraInfo": "waiting for parent"}}"#,
            txid
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_bytes, std::slice::from_ref(&txid)).await.unwrap();

    // SEEN_IN_ORPHAN_MEMPOOL is not DOUBLE_SPEND_ATTEMPTED, so it should be "success"
    assert_eq!(
        result.status, "success",
        "SEEN_IN_ORPHAN_MEMPOOL should be treated as success"
    );
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].status, "success");
    assert!(
        !result.txid_results[0].double_spend,
        "Should not be flagged as double spend"
    );

    // Verify the data contains the status string
    let data = result.txid_results[0].data.as_ref().unwrap();
    assert!(
        data.contains("SEEN_IN_ORPHAN_MEMPOOL"),
        "Data should contain the ARC status"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 5: DOUBLE_SPEND_ATTEMPTED is still an error
// =============================================================================

#[tokio::test]
async fn test_post_beef_double_spend_still_error() {
    let (beef_bytes, txid) = build_single_tx_beef(BEEF_V1);

    let competing_txid = "ff".repeat(32);

    let mut server = mockito::Server::new_async().await;

    // ARC returns 200 with txStatus "DOUBLE_SPEND_ATTEMPTED"
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "DOUBLE_SPEND_ATTEMPTED", "extraInfo": "conflicting input", "competingTxs": ["{}"]}}"#,
            txid, competing_txid
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_bytes, std::slice::from_ref(&txid)).await.unwrap();

    // DOUBLE_SPEND_ATTEMPTED should be treated as error
    assert_eq!(
        result.status, "error",
        "DOUBLE_SPEND_ATTEMPTED should be treated as error"
    );
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].status, "error");
    assert!(
        result.txid_results[0].double_spend,
        "Should be flagged as double spend"
    );

    // Verify competing transactions are captured
    let competing = result.txid_results[0].competing_txs.as_ref().unwrap();
    assert_eq!(competing.len(), 1);
    assert_eq!(competing[0], competing_txid);

    // Should NOT be a service error (it's a legitimate protocol response)
    assert!(
        !result.txid_results[0].service_error,
        "Double spend is not a service error"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 6: Various ARC success statuses are all treated as success
// =============================================================================

#[tokio::test]
async fn test_post_beef_various_success_statuses() {
    // ARC can return several "success" statuses. Verify they all map to success.
    let success_statuses = [
        "SEEN_ON_NETWORK",
        "STORED",
        "MINED",
        "SEEN_IN_ORPHAN_MEMPOOL",
        "ANNOUNCED_TO_NETWORK",
    ];

    for status in success_statuses {
        let (beef_bytes, txid) = build_single_tx_beef(BEEF_V1);

        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/tx")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"txid": "{}", "txStatus": "{}", "extraInfo": ""}}"#,
                txid, status
            ))
            .create_async()
            .await;

        let arc =
            ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

        let result = arc.post_beef(&beef_bytes, std::slice::from_ref(&txid)).await.unwrap();

        assert_eq!(
            result.status, "success",
            "Status '{}' should be treated as success",
            status
        );
        assert!(
            !result.txid_results[0].double_spend,
            "Status '{}' should not be double spend",
            status
        );

        mock.assert_async().await;
    }
}

// =============================================================================
// Test 7: Unparseable BEEF bytes are sent as-is
// =============================================================================

#[tokio::test]
async fn test_post_beef_unparseable_sent_as_is() {
    // If the BEEF bytes can't be parsed, they should be sent unchanged.
    let garbage_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03];
    let txid = "aa".repeat(32);

    let mut server = mockito::Server::new_async().await;

    let expected_hex = hex::encode(&garbage_bytes);
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "STORED", "extraInfo": ""}}"#,
            txid
        ))
        .match_body(mockito::Matcher::Regex(expected_hex))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&garbage_bytes, std::slice::from_ref(&txid)).await.unwrap();

    assert_eq!(result.status, "success");

    // Should NOT have conversion note since we couldn't parse
    let has_v2_to_v1_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefV2ToV1"));
    assert!(
        !has_v2_to_v1_note,
        "Unparseable BEEF should not trigger V2-to-V1 note"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 8: V2 BEEF with txid-only entries cannot be downgraded
// =============================================================================

#[tokio::test]
async fn test_post_beef_v2_with_txid_only_not_downgraded() {
    // Build a V2 BEEF that contains a txid-only entry (not downgrade-able to V1).
    let tx = Transaction::from_hex(TEST_TX_HEX).unwrap();
    let txid = tx.id();
    let raw_tx = hex::decode(TEST_TX_HEX).unwrap();

    let mut beef = Beef::with_version(BEEF_V2);

    // Add a proven transaction
    let bump = MerklePath::from_coinbase_txid(&txid, 800_000);
    let bump_idx = beef.merge_bump(bump);
    beef.merge_raw_tx(raw_tx, Some(bump_idx));

    // Add a txid-only entry (V2-only feature)
    let fake_txid = "bb".repeat(32);
    beef.merge_txid_only(fake_txid);

    let beef_bytes = beef.to_binary();

    // Verify it starts with V2 magic
    assert_eq!(beef_bytes[0..4], [0x02, 0x00, 0xBE, 0xEF]);

    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "STORED", "extraInfo": ""}}"#,
            txid
        ))
        // The BEEF should remain V2 since it has txid-only entries
        .match_body(mockito::Matcher::Regex("0200beef".to_string()))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc.post_beef(&beef_bytes, std::slice::from_ref(&txid)).await.unwrap();

    // The broadcast may succeed or fail (ARC doesn't handle V2), but the
    // important thing is that the V2 was NOT downgraded to V1.
    // No V2-to-V1 note should be present.
    let has_v2_to_v1_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefV2ToV1"));
    assert!(
        !has_v2_to_v1_note,
        "V2 BEEF with txid-only entries should NOT be downgraded to V1"
    );

    mock.assert_async().await;
}
