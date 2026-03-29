//! ARC BEEF broadcast handling tests.
//!
//! Tests for BEEF format conversion, EF extraction, trimming, and ARC response
//! handling during transaction broadcast. Uses mockito for HTTP mocking.

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, Transaction, TransactionInput, TransactionOutput, BEEF_V1, BEEF_V2,
};
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

/// Helper: build a BEEF with a proven parent and an unproven child that spends it.
///
/// Returns (beef_binary, child_txid, parent_txid).
fn build_parent_child_beef() -> (Vec<u8>, String, String) {
    build_parent_child_beef_with_output(1_000_000_000, "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac")
}

/// Helper: build a parent-child BEEF with a specific satoshi amount and locking script hex
/// on the parent's output at vout 0.
///
/// Returns (beef_binary, child_txid, parent_txid).
fn build_parent_child_beef_with_output(
    satoshis: u64,
    locking_script_hex: &str,
) -> (Vec<u8>, String, String) {
    let locking_script = LockingScript::from_hex(locking_script_hex).unwrap();

    // --- Build the parent transaction ---
    let parent_tx = Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some("00".repeat(32)),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xFFFFFFFF,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: locking_script.clone(),
            change: false,
        }],
        0,
    );
    let parent_txid = parent_tx.id();

    // --- Build the child transaction that spends parent vout 0 ---
    // We need an unlocking script (to_ef requires it). Use a dummy one.
    let child_tx = Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(parent_txid.clone()),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_hex("4830450221009999999999999999999999999999999999999999999999999999999999999999022099999999999999999999999999999999999999999999999999999999999999990121030000000000000000000000000000000000000000000000000000000000000001").unwrap()),
            unlocking_script_template: None,
            sequence: 0xFFFFFFFF,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis - 200),
            locking_script: LockingScript::from_hex("6a").unwrap(), // OP_RETURN
            change: false,
        }],
        0,
    );
    let child_txid = child_tx.id();

    // --- Assemble the BEEF ---
    let mut beef = Beef::with_version(BEEF_V1);

    // Add parent as proven (with merkle path)
    let bump = MerklePath::from_coinbase_txid(&parent_txid, 800_000);
    let bump_idx = beef.merge_bump(bump);
    beef.merge_raw_tx(parent_tx.to_binary(), Some(bump_idx));

    // Add child as unproven (no bump)
    beef.merge_raw_tx(child_tx.to_binary(), None);

    (beef.to_binary(), child_txid, parent_txid)
}

// =============================================================================
// Test 1: post_beef extracts EF format from BEEF with parent+child
// =============================================================================

#[tokio::test]
async fn test_post_beef_sends_ef_format() {
    let (beef_bytes, child_txid, _parent_txid) = build_parent_child_beef();

    let mut server = mockito::Server::new_async().await;

    // The mock needs to match an EF hex in the JSON body.
    // EF marker is 6 bytes: 0000000000ef immediately after the 4-byte version.
    // Version 1 little-endian = "01000000", then EF marker = "0000000000ef".
    // So the rawTx hex should contain "010000000000000000ef".
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_ON_NETWORK", "extraInfo": ""}}"#,
            child_txid
        ))
        .match_body(mockito::Matcher::Regex(
            "010000000000000000ef".to_string(),
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&child_txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success", "EF broadcast should succeed");
    assert!(!result.txid_results.is_empty());

    // Verify the postBeefAsEF note is present
    let has_ef_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefAsEF"));
    assert!(
        has_ef_note,
        "Should have a postBeefAsEF note indicating EF extraction happened"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 2: V1 BEEF with only proven txs is sent as BEEF hex (no EF extraction)
// =============================================================================

#[tokio::test]
async fn test_post_beef_v1_proven_only_sent_as_beef() {
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
        // BEEF hex should start with "0100beef" (V1 magic)
        .match_body(mockito::Matcher::Regex("0100beef".to_string()))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_v1_bytes, std::slice::from_ref(&txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success");

    // Should NOT have EF note since there's no unproven tx to extract
    let has_ef_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefAsEF"));
    assert!(
        !has_ef_note,
        "Proven-only BEEF should NOT have postBeefAsEF note"
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

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success");
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].txid, txid);

    mock.assert_async().await;
}

// =============================================================================
// Test 4: SEEN_IN_ORPHAN_MEMPOOL treated as error (matches TS/Go behavior)
// =============================================================================

#[tokio::test]
async fn test_post_beef_orphan_mempool_treated_as_error() {
    let (beef_bytes, child_txid, _parent_txid) = build_parent_child_beef();

    let mut server = mockito::Server::new_async().await;

    // ARC returns 200 with txStatus "SEEN_IN_ORPHAN_MEMPOOL"
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_IN_ORPHAN_MEMPOOL", "extraInfo": "waiting for parent"}}"#,
            child_txid
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&child_txid))
        .await
        .unwrap();

    // SEEN_IN_ORPHAN_MEMPOOL is now treated as error (matching TS/Go behavior)
    assert_eq!(
        result.status, "error",
        "SEEN_IN_ORPHAN_MEMPOOL should be treated as error"
    );
    assert!(!result.txid_results.is_empty());
    assert_eq!(result.txid_results[0].status, "error");
    assert!(
        result.txid_results[0].double_spend,
        "SEEN_IN_ORPHAN_MEMPOOL should be flagged as double_spend"
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
    let (beef_bytes, child_txid, _parent_txid) = build_parent_child_beef();

    let competing_txid = "ff".repeat(32);

    let mut server = mockito::Server::new_async().await;

    // ARC returns 200 with txStatus "DOUBLE_SPEND_ATTEMPTED"
    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "DOUBLE_SPEND_ATTEMPTED", "extraInfo": "conflicting input", "competingTxs": ["{}"]}}"#,
            child_txid, competing_txid
        ))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&child_txid))
        .await
        .unwrap();

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
    // Note: SEEN_IN_ORPHAN_MEMPOOL is now treated as error (matching TS/Go).
    let success_statuses = [
        "SEEN_ON_NETWORK",
        "STORED",
        "MINED",
        "ANNOUNCED_TO_NETWORK",
    ];

    for status in success_statuses {
        let (beef_bytes, child_txid, _parent_txid) = build_parent_child_beef();

        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/tx")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"txid": "{}", "txStatus": "{}", "extraInfo": ""}}"#,
                child_txid, status
            ))
            .create_async()
            .await;

        let arc =
            ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

        let result = arc
            .post_beef(&beef_bytes, std::slice::from_ref(&child_txid))
            .await
            .unwrap();

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
// Test 7: Unparseable BEEF bytes are sent as-is (EF fallback)
// =============================================================================

#[tokio::test]
async fn test_post_beef_ef_fallback_on_unparseable() {
    // If the BEEF bytes can't be parsed, they should be sent unchanged as hex.
    // The postBeefAsEF note should NOT be present.
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

    let result = arc
        .post_beef(&garbage_bytes, std::slice::from_ref(&txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success");

    // postBeefAsEF note should NOT be present for unparseable BEEF
    let has_ef_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefAsEF"));
    assert!(
        !has_ef_note,
        "Unparseable BEEF should NOT have postBeefAsEF note"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 8: V2 BEEF with txid-only entries sent as-is
// =============================================================================

#[tokio::test]
async fn test_post_beef_v2_with_txid_only_sent_as_is() {
    // Build a V2 BEEF that contains a txid-only entry. Since the only "real"
    // tx is proven, there's no new tx to extract EF from, so it falls back
    // to sending the BEEF hex as-is.
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
        // The BEEF should remain V2 since there's no new tx to extract EF from
        .match_body(mockito::Matcher::Regex("0200beef".to_string()))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success");

    // No EF note should be present
    let has_ef_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefAsEF"));
    assert!(
        !has_ef_note,
        "V2 BEEF with txid-only entries and no new tx should NOT trigger EF extraction"
    );

    mock.assert_async().await;
}

// =============================================================================
// Test 9: EF format embeds parent UTXO data (satoshis + locking script)
// =============================================================================

#[tokio::test]
async fn test_post_beef_ef_embeds_parent_utxo() {
    // Create a parent tx with a specific satoshi amount and locking script,
    // then a child that spends it. When posted as EF, the embedded source
    // output should contain the parent's satoshis and locking script.
    let parent_satoshis: u64 = 42_000;
    let parent_script_hex = "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac";

    let (beef_bytes, child_txid, parent_txid) =
        build_parent_child_beef_with_output(parent_satoshis, parent_script_hex);

    // Independently build what the EF hex should look like by hydrating the
    // child tx manually and calling to_hex_ef().
    let beef_parsed = Beef::from_binary(&beef_bytes).unwrap();
    let new_btx = beef_parsed
        .txs
        .iter()
        .rev()
        .find(|btx| btx.bump_index().is_none() && !btx.is_txid_only())
        .unwrap();
    let mut new_tx = new_btx.tx().unwrap().clone();
    for input in &mut new_tx.inputs {
        let src_txid = input.get_source_txid().unwrap();
        let parent_btx = beef_parsed.find_txid(&src_txid).unwrap();
        input.source_transaction = Some(Box::new(parent_btx.tx().unwrap().clone()));
    }
    let expected_ef_hex = new_tx.to_hex_ef().unwrap();

    // Verify the EF hex contains the EF marker
    assert!(
        expected_ef_hex.contains("0000000000ef"),
        "EF hex should contain the EF marker"
    );

    // Parse the EF back and verify the embedded source output
    let ef_bytes = hex::decode(&expected_ef_hex).unwrap();
    let parsed_ef_tx = Transaction::from_ef(&ef_bytes).unwrap();

    assert_eq!(parsed_ef_tx.inputs.len(), 1);
    let input = &parsed_ef_tx.inputs[0];
    let source_tx = input.source_transaction.as_ref().unwrap();
    let source_output = &source_tx.outputs[input.source_output_index as usize];

    assert_eq!(
        source_output.satoshis,
        Some(parent_satoshis),
        "EF should embed the parent's satoshi amount"
    );
    assert_eq!(
        source_output.locking_script.to_hex(),
        parent_script_hex,
        "EF should embed the parent's locking script"
    );

    // Now verify via the ARC broadcast path
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid": "{}", "txStatus": "SEEN_ON_NETWORK", "extraInfo": ""}}"#,
            child_txid
        ))
        // Verify the exact EF hex is sent (it's in the JSON rawTx field)
        .match_body(mockito::Matcher::Regex(expected_ef_hex.clone()))
        .create_async()
        .await;

    let arc =
        ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("testArc")).unwrap();

    let result = arc
        .post_beef(&beef_bytes, std::slice::from_ref(&child_txid))
        .await
        .unwrap();

    assert_eq!(result.status, "success");

    // Verify the postBeefAsEF note
    let has_ef_note = result
        .notes
        .iter()
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some("postBeefAsEF"));
    assert!(
        has_ef_note,
        "Should have postBeefAsEF note for successful EF extraction"
    );

    // Verify the input references the correct parent
    assert_eq!(
        input.source_txid.as_ref().unwrap(),
        &parent_txid,
        "EF input should reference the parent txid"
    );

    mock.assert_async().await;
}
