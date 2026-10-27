//! The BRC-177 `nosend` module at the door (#312, ruling 19.1 (a')). The
//! port has no BRC-177 capability, so a label starting `p nosend ` is refused
//! with the reference's text on a store without it
//! (ts-stack@edf6e03 `createNoSendExpiryAction.ts:201-203`) and writes no
//! row, as `0177.md:53` asks. The prefix is exact (`brc177NoSendExpiry.ts:5`,
//! matched by `startsWith` at `:119`): anything else is ordinary metadata.

use super::super::create_action_internal;
use super::super::test_support::seed_change_output;
use crate::error::Error;
use crate::storage::sqlx::StorageSqlx;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};

/// The reference's refusal, byte for byte.
const REFUSAL: &str = "Active storage does not support BRC-177 noSend expiry";

async fn funded_store() -> (StorageSqlx, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    seed_change_output(&storage, user.user_id, 100_000).await;
    (storage, user.user_id)
}

fn no_send_args(labels: &[&str]) -> CreateActionArgs {
    CreateActionArgs {
        description: "A BRC-177 label test".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac")
                .unwrap(),
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(labels.iter().map(|l| l.to_string()).collect()),
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
    }
}

async fn transaction_rows(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

/// The door refuses `labels` with the reference's text and writes no row.
async fn assert_refused(labels: &[&str]) {
    let (storage, user_id) = funded_store().await;
    let before = transaction_rows(&storage).await;
    let result = create_action_internal(&storage, None, user_id, no_send_args(labels)).await;
    match result {
        Err(Error::InvalidOperation(message)) => assert_eq!(message, REFUSAL, "{labels:?}"),
        Err(other) => panic!("{labels:?}: refused with another error: {other}"),
        Ok(_) => panic!("{labels:?}: the action was created"),
    }
    assert_eq!(transaction_rows(&storage).await, before, "{labels:?}");
}

/// The door creates the action as before.
async fn assert_accepted(labels: &[&str]) {
    let (storage, user_id) = funded_store().await;
    let before = transaction_rows(&storage).await;
    create_action_internal(&storage, None, user_id, no_send_args(labels))
        .await
        .unwrap_or_else(|e| panic!("{labels:?}: {e}"));
    assert_eq!(transaction_rows(&storage).await, before + 1, "{labels:?}");
}

#[tokio::test]
async fn nosend_expiry_seconds_label_is_refused_and_writes_no_row() {
    assert_refused(&["p nosend expiry seconds 300"]).await;
}

#[tokio::test]
async fn nosend_expiry_never_label_is_refused() {
    assert_refused(&["p nosend expiry never"]).await;
}

#[tokio::test]
async fn nosend_batch_label_is_refused() {
    assert_refused(&["p nosend batch"]).await;
}

#[tokio::test]
async fn two_nosend_expiry_labels_are_refused() {
    assert_refused(&[
        "p nosend expiry seconds 300",
        "p nosend expiry blockheight 900000",
    ])
    .await;
}

#[tokio::test]
async fn nosend_label_beside_ordinary_labels_is_refused() {
    assert_refused(&["invoice", "p nosend expiry timestamp 1900000000"]).await;
}

#[tokio::test]
async fn labels_outside_the_exact_prefix_are_accepted() {
    assert_accepted(&["p nosendx"]).await;
    assert_accepted(&["nosend expiry seconds 300"]).await;
    assert_accepted(&["p other"]).await;
}

/// The reference matches the label as the SDK normalizes it, trimmed and
/// lowercased (ts-stack@edf6e03 `sdk/src/wallet/validationHelpers.ts:411`,
/// validateLabel), ahead of the prefix check.
#[tokio::test]
async fn nosend_label_is_matched_trimmed_and_lowercased() {
    assert_refused(&["P NoSend expiry seconds 300"]).await;
    assert_refused(&["  p nosend batch"]).await;
}

/// `p nosend ` trims to `p nosend`, outside the prefix, as in the reference.
#[tokio::test]
async fn bare_module_word_is_accepted() {
    assert_accepted(&["p nosend "]).await;
}
