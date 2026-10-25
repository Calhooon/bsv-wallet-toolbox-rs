use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_get_tx_with_proof_from_proven_txs() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let txid = "3333333333333333333333333333333333333333333333333333333333333333";
    let raw_tx = vec![1, 2, 3, 4, 5];
    let merkle_path = vec![6, 7, 8, 9, 10];

    seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = get_tx_with_proof(&mut conn, txid).await.unwrap();

    assert!(result.is_some());
    let tx_data = result.unwrap();
    assert_eq!(tx_data.raw_tx, raw_tx);
    assert_eq!(tx_data.merkle_path, Some(merkle_path));
}

#[tokio::test]
async fn test_get_tx_with_proof_from_transactions() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

    let now = Utc::now();
    let txid = "4444444444444444444444444444444444444444444444444444444444444444";
    let raw_tx = vec![11, 12, 13, 14, 15];

    sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref456', 0, 1000, 1, 0, 'Test tx', ?, ?, ?, ?)
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

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = get_tx_with_proof(&mut conn, txid).await.unwrap();

    assert!(result.is_some());
    let tx_data = result.unwrap();
    assert_eq!(tx_data.raw_tx, raw_tx);
    assert!(tx_data.merkle_path.is_none()); // No proof from transactions table
}

#[tokio::test]
async fn test_get_tx_with_proof_not_found() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = get_tx_with_proof(&mut conn, "nonexistent_txid")
        .await
        .unwrap();

    assert!(result.is_none());
}

#[test]
fn test_parse_input_txids_coinbase() {
    // Coinbase transaction (input txid is all zeros) - should return empty
    let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

    let txids = parse_input_txids(&raw_tx).unwrap();
    assert!(txids.is_empty()); // Coinbase inputs (all zeros) should be excluded
}

#[test]
fn test_parse_input_txids_single_input() {
    // Transaction with one non-coinbase input
    // version(4) + input_count(1) + txid(32) + vout(4) + script_len(1) + script + seq(4) + ...
    let mut raw_tx = vec![];
    raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    raw_tx.push(0x01); // 1 input
                       // Input txid (will be reversed when parsed)
    let input_txid_bytes =
        hex::decode("1111111111111111111111111111111111111111111111111111111111111111").unwrap();
    raw_tx.extend_from_slice(&input_txid_bytes);
    raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
    raw_tx.push(0x00); // empty script
    raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    raw_tx.push(0x00); // 0 outputs
    raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let txids = parse_input_txids(&raw_tx).unwrap();
    assert_eq!(txids.len(), 1);
    assert_eq!(
        txids[0],
        "1111111111111111111111111111111111111111111111111111111111111111"
    );
}

#[test]
fn test_parse_input_txids_multiple_inputs() {
    // Transaction with two non-coinbase inputs
    let mut raw_tx = vec![];
    raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    raw_tx.push(0x02); // 2 inputs

    // First input
    let input1_txid =
        hex::decode("1111111111111111111111111111111111111111111111111111111111111111").unwrap();
    raw_tx.extend_from_slice(&input1_txid);
    raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
    raw_tx.push(0x00); // empty script
    raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence

    // Second input
    let input2_txid =
        hex::decode("2222222222222222222222222222222222222222222222222222222222222222").unwrap();
    raw_tx.extend_from_slice(&input2_txid);
    raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // vout 1
    raw_tx.push(0x00); // empty script
    raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence

    raw_tx.push(0x00); // 0 outputs
    raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    let txids = parse_input_txids(&raw_tx).unwrap();
    assert_eq!(txids.len(), 2);
    assert_eq!(
        txids[0],
        "1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(
        txids[1],
        "2222222222222222222222222222222222222222222222222222222222222222"
    );
}

#[test]
fn test_parse_input_txids_empty_tx() {
    // Very short transaction (too short to parse)
    let raw_tx = vec![0x01, 0x00];
    let txids = parse_input_txids(&raw_tx).unwrap();
    assert!(txids.is_empty());
}

#[test]
fn test_read_var_int_for_beef() {
    // Single byte
    let data = vec![0x05];
    let mut offset = 0;
    assert_eq!(read_var_int_for_beef(&data, &mut offset).unwrap(), 5);
    assert_eq!(offset, 1);

    // Two byte (0xfd prefix)
    let data = vec![0xfd, 0x00, 0x01];
    let mut offset = 0;
    assert_eq!(read_var_int_for_beef(&data, &mut offset).unwrap(), 256);
    assert_eq!(offset, 3);
}
