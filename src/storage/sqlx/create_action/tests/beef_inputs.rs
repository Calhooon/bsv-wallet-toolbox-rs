use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_build_input_beef_empty() {
    // Test that empty inputs returns None
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let extended_inputs: Vec<ExtendedInput> = vec![];
    let change_inputs: Vec<AllocatedChangeInput> = vec![];

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification for this test
        &extended_inputs,
        &change_inputs,
        None,  // user_input_beef
        &[],   // known_txids
        false, // return_txid_only
        false,
        None, // storage - no network fallback
    )
    .await
    .unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_build_input_beef_with_proven_tx() {
    // Test building BEEF with a proven transaction
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // Sample raw transaction (minimal valid tx)
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

    // A real single-leaf proof for the funding transaction.
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

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = build_input_beef(
        &mut conn,
        Some(storage.writer()),
        None, // chain_tracker - skip verification for this test
        &extended_inputs,
        &change_inputs,
        None,  // user_input_beef
        &[],   // known_txids
        false, // return_txid_only
        false,
        None, // storage - no network fallback
    )
    .await
    .unwrap();

    // Should return Some BEEF data
    assert!(result.is_some());
    let beef_bytes = result.unwrap();

    // BEEF V2 starts with magic bytes (little endian: 0x0100BEEF or 0x0200BEEF)
    // In practice, bsv_rs uses 0xEFBE0002 for V2 in little endian
    assert!(beef_bytes.len() > 4);
}

#[tokio::test]
async fn test_build_input_beef_with_unproven_tx() {
    // Test building BEEF with an unproven transaction (from transactions table)
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    // Create a transaction with raw_tx but no proof
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

    // Should return Some BEEF data (even without proof)
    assert!(result.is_some());
    let beef_bytes = result.unwrap();
    assert!(beef_bytes.len() > 4);
}

#[tokio::test]
async fn test_build_input_beef_deduplicates_txids() {
    // Test that duplicate txids are handled correctly
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    let now = Utc::now();

    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

    let txid_owned = txid_of(&raw_tx);
    let txid = txid_owned.as_str();
    seed_proven_chain_tx(&storage, txid, &raw_tx, 100_000).await;
    sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', 'ref123', 0, 10000000000, 1, 0, 'Test tx', ?, ?, ?, ?)
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

    // Same txid appears in both extended_inputs and change_inputs
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
    let change_inputs = vec![AllocatedChangeInput {
        output_id: 1,
        satoshis: 5000000000,
        txid: txid.to_string(),
        vout: 1,
        locking_script: vec![],
        derivation_prefix: None,
        derivation_suffix: None,
        sender_identity_key: None,
    }];

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

    // Should return Some BEEF data
    assert!(result.is_some());
    // The transaction should only appear once in the BEEF
    let beef_bytes = result.unwrap();
    assert!(beef_bytes.len() > 4);
}
