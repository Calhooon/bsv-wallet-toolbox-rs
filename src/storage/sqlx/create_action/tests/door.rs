use super::*;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::wallet::{CreateActionOptions, CreateActionOutput};
use sqlx::Row;

// =========================================================================
// Integration tests
// =========================================================================

#[tokio::test]
async fn test_create_action_basic() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    // Create a user
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    // Create a simple action with one output
    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: locking_script.clone(),
            satoshis: 42000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user.user_id, args).await;

    // Should fail due to insufficient funds (no change inputs available)
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Insufficient funds"));
}

#[tokio::test]
async fn test_create_action_no_send() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test noSend transaction".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(false),
            accept_delayed_broadcast: None,
            trust_self: None,
            return_txid_only: None,
            known_txids: None,
            no_send_change: None,
            send_with: None,
            randomize_outputs: None,
        }),
    };

    let result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    // For noSend, we should get change vouts
    assert!(result.no_send_change_output_vouts.is_some());
}

#[tokio::test]
async fn test_create_action_with_version_and_locktime() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test with version/locktime".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: Some(500000),
        version: Some(2),
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    assert_eq!(result.version, 2);
    assert_eq!(result.lock_time, 500000);
}

#[tokio::test]
async fn test_raw_tx_stored_at_create_time() {
    // Verify that after create_action + process_action, the raw_tx is stored
    // on the transactions table (not NULL).
    use crate::storage::traits::{AuthId, StorageProcessActionArgs, WalletStorageWriter};
    use bsv_rs::primitives::sha256d;

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test raw_tx storage".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: locking_script.clone(),
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let create_result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    // Build a raw tx with proper script length varint encoding.
    // Must match the number and locking scripts of outputs from create_action.
    let total_outputs = create_result.outputs.len();
    let mut raw_tx = vec![];
    raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
    raw_tx.push(0x01); // 1 input (dummy coinbase)
    raw_tx.extend_from_slice(&[0u8; 32]); // txid
    raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // vout
    raw_tx.push(0x00); // empty unlocking script
    raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
    raw_tx.push(total_outputs as u8); // output count
    for output in &create_result.outputs {
        // satoshis (8 bytes LE)
        raw_tx.extend_from_slice(&output.satoshis.to_le_bytes());
        // locking script with varint length prefix
        let script_bytes = hex::decode(&output.locking_script).unwrap_or_default();
        raw_tx.push(script_bytes.len() as u8);
        raw_tx.extend_from_slice(&script_bytes);
    }
    raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

    // Compute txid from the raw_tx
    let mut txid_bytes = sha256d(&raw_tx).to_vec();
    txid_bytes.reverse();
    let txid = hex::encode(&txid_bytes);

    // Inject input_beef so process_action doesn't reject us
    let dummy_beef = vec![0x00, 0x01, 0x00];
    sqlx::query("UPDATE transactions SET input_beef = ? WHERE reference = ?")
        .bind(&dummy_beef)
        .bind(&create_result.reference)
        .execute(storage.writer())
        .await
        .unwrap();

    let process_args = StorageProcessActionArgs {
        is_new_tx: true,
        is_send_with: false,
        is_no_send: true,
        is_delayed: false,
        reference: Some(create_result.reference.clone()),
        txid: Some(txid.clone()),
        raw_tx: Some(raw_tx.clone()),
        send_with: vec![],
    };

    let auth_id = AuthId::with_user_id("02user_identity_key", user.user_id);
    let _process_result = storage
        .process_action(&auth_id, process_args)
        .await
        .unwrap();

    // Now verify that raw_tx is stored on the transactions table
    let row = sqlx::query("SELECT raw_tx FROM transactions WHERE txid = ?")
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();

    let stored_raw_tx: Option<Vec<u8>> = row.get("raw_tx");
    assert!(
        stored_raw_tx.is_some(),
        "raw_tx must NOT be NULL on the transactions table after process_action"
    );
    assert_eq!(
        stored_raw_tx.unwrap(),
        raw_tx,
        "stored raw_tx must match the signed transaction"
    );
}
#[cfg(test)]
#[path = "trust_self_known/mod.rs"]
mod trust_self_known;
