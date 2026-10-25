use super::*;
use crate::storage::traits::WalletStorageWriter;

// =============================================================================
// Tests for BEEF Gaps (matching Go tests)
// =============================================================================

#[tokio::test]
async fn test_build_input_beef_return_txid_only() {
    // Gap #3: When return_txid_only is true, should return None
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let txid = "3333333333333333333333333333333333333333333333333333333333333333";
    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // With return_txid_only = true, should return None regardless of inputs
    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        true, // return_txid_only = true
        false,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_build_input_beef_with_known_txids() {
    // Gap #2: Known txids should be trimmed to txid-only format
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Sample raw transaction
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let merkle_path = synth_bump(100_000, &txid_of(&raw_tx)).to_binary();
    let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 5000000000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // Build BEEF with the txid marked as known
    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification
        &extended_inputs,
        &change_inputs,
        None,
        &[txid.to_string()], // This txid is known to recipient
        false,
        false,
        None,
    )
    .await
    .unwrap();

    // Should still return BEEF (but with txid-only for known tx)
    assert!(result.is_some());
    let beef_bytes = result.unwrap();

    // Parse the BEEF and verify the tx is txid-only
    let beef = Beef::from_binary(&beef_bytes).unwrap();
    let beef_tx = beef.find_txid(txid).unwrap();
    assert!(
        beef_tx.is_txid_only(),
        "Known txid should be converted to txid-only"
    );
}

#[tokio::test]
async fn test_build_input_beef_with_user_input_beef() {
    // Gap #1: User-provided inputBEEF should be merged first
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Create a user-provided BEEF with a transaction
    let user_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let user_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Build user's BEEF
    let mut user_beef = Beef::new();
    let idx = user_beef.merge_bump(synth_bump(100_000, user_txid));
    user_beef.merge_raw_tx(user_raw_tx, Some(idx));
    let user_beef_bytes = user_beef.to_binary();

    // Input references the tx from user BEEF (not in storage)
    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: user_txid.to_string(),
        vout: 0,
        satoshis: 5000000000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // Build BEEF with user-provided inputBEEF
    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification
        &extended_inputs,
        &change_inputs,
        Some(&user_beef_bytes), // User provides BEEF for external input
        &[],
        false,
        false,
        None,
    )
    .await
    .unwrap();

    // Should return BEEF containing the user's transaction
    assert!(result.is_some());
    let beef_bytes = result.unwrap();

    // Parse and verify the tx is included
    let beef = Beef::from_binary(&beef_bytes).unwrap();
    assert!(
        beef.find_txid(user_txid).is_some(),
        "User-provided transaction should be in the BEEF"
    );
}

#[tokio::test]
async fn test_build_input_beef_user_beef_invalid() {
    // Gap #1: Invalid user inputBEEF should return error
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: "4444444444444444444444444444444444444444444444444444444444444444".to_string(),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // Provide invalid BEEF bytes
    let invalid_beef = vec![0x00, 0x01, 0x02, 0x03];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification
        &extended_inputs,
        &change_inputs,
        Some(&invalid_beef),
        &[],
        false,
        false,
        None,
    )
    .await;

    // The streaming reader names the byte: the version word 0x03020100
    // is neither BEEF version, at offset 0.
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidBeef {
                offset: 0,
                kind: bsv_rs::transaction::Kind::BadVersion
            }
        ),
        "{err}"
    );
}

// =============================================================================
// Tests for BEEF Verification against ChainTracker (Gap #4)
// =============================================================================

/// The caller's inputBEEF is a stranger's bytes: a transaction with no
/// input in it is refused as invalid bytes, at its offset, by kind.
#[tokio::test]
async fn test_build_input_beef_refuses_a_transaction_with_no_input_at_its_offset() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Version 1, no input, one OP_TRUE output, lock time 0.
    let mut raw = 1u32.to_le_bytes().to_vec();
    raw.push(0);
    raw.push(1);
    raw.extend_from_slice(&1_000u64.to_le_bytes());
    raw.extend_from_slice(&[1, 0x51]);
    raw.extend_from_slice(&0u32.to_le_bytes());
    let mut beef = Beef::new();
    beef.merge_raw_tx(raw, None);
    let bytes = beef.to_binary();

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: "44".repeat(32),
        vout: 0,
        satoshis: 1000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let mut conn = storage.pool().acquire().await.unwrap();
    let err = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &[],
        Some(&bytes),
        &[],
        false,
        false,
        None,
    )
    .await
    .expect_err("a transaction with no input is invalid bytes");
    // The version (4), the two counts (1 + 1), the format byte (1).
    assert_eq!(err.to_string(), "Invalid BEEF at byte 7: NoInputs");
    assert!(matches!(
        err,
        Error::InvalidBeef {
            offset: 7,
            kind: bsv_rs::transaction::Kind::NoInputs
        }
    ));
}

#[tokio::test]
async fn test_build_input_beef_with_valid_chain_tracker() {
    use bsv_rs::transaction::AlwaysValidChainTracker;

    // Test that BEEF verification passes with AlwaysValidChainTracker
    // We use unproven transactions (no merkle path) to avoid structural validation issues
    // with fake merkle paths in test data
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    // Create an unproven transaction (no merkle path needed)
    let now = Utc::now();
    let chain = synth_chain(2);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 100_000).await;
    let raw_tx = chain[1].0.clone();
    let txid = chain[1].1.as_str();

    sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref123', 0, 5000000000, 1, 0, 'Test tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 5000000000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // Use AlwaysValidChainTracker with an unproven child and proven parent
    // This tests that the verification code path completes successfully
    let tracker = AlwaysValidChainTracker::new(100000);

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        Some(&tracker),
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        false,
        None,
    )
    .await
    .unwrap();

    // Should return Some BEEF data (verification passes)
    assert!(result.is_some());
    let beef_bytes = result.unwrap();
    assert!(beef_bytes.len() > 4);
}

#[tokio::test]
async fn test_build_input_beef_refuses_unsourced_body_when_proof_is_malformed() {
    use bsv_rs::transaction::MockChainTracker;

    // An unreadable proof leaves an unsourced transaction, which must
    // fail structural validation even though no BUMP could be attached.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Sample raw transaction with a fake merkle path (will fail to parse, no bump added)
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let merkle_path = hex::decode("a086010001020002").unwrap(); // Invalid/incomplete merkle path
    let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 5000000000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let tracker = MockChainTracker::new(100000);

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        Some(&tracker),
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        false,
        None,
    )
    .await;

    assert!(matches!(result, Err(Error::ValidationError(_))));
}

#[tokio::test]
async fn test_build_input_beef_valid_structure_without_tracker() {
    // Structure is checked without a tracker; header roots need a tracker.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Sample raw transaction
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let merkle_path = synth_bump(100_000, &txid_of(&raw_tx)).to_binary();
    let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

    let extended_inputs = vec![ExtendedInput {
        vin: 0,
        txid: txid.to_string(),
        vout: 0,
        satoshis: 5000000000,
        locking_script: vec![],
        unlocking_script_length: 107,
        input_description: None,
        output: None,
    }];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    // Without a tracker only the root lookup is skipped.
    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None,
        &extended_inputs,
        &change_inputs,
        None,
        &[],
        false,
        false,
        None,
    )
    .await
    .unwrap();

    // The valid structure still returns BEEF without a root lookup.
    assert!(result.is_some());
    let beef_bytes = result.unwrap();
    assert!(beef_bytes.len() > 4);
}
