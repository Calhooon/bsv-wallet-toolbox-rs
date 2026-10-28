//! The create description bound through `WalletInterface` (#383, #407).
//! ts-stack@edf6e03 validationHelpers.ts:755 calls `validateStringLength`
//! (348-356): 5 to 2000 UTF-8 bytes, untrimmed, with the reference's text.

use super::super::test_support::seed_change_output;
use super::nosend_tests::{no_send_args, transaction_rows};
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::{StorageSqlx, Wallet};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{CreateActionArgs, WalletInterface};
use bsv_rs::WalletRefusalClass;

const TOO_SHORT: &str = "Validation error: The description parameter must be at least 5 length.";
const TOO_LONG: &str =
    "Validation error: The description parameter must be no more than 2000 length.";

pub(super) async fn wallet() -> Wallet<StorageSqlx, MockWalletServices> {
    let storage = StorageSqlx::in_memory().await.unwrap();
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("description-tests", &identity)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    seed_change_output(&storage, user.user_id, 100_000).await;
    Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap()
}

fn described(description: &str) -> CreateActionArgs {
    let mut args = no_send_args(&[]);
    args.description = description.to_string();
    args
}

pub(super) async fn assert_refused(args: CreateActionArgs, expected: &str) {
    let wallet = wallet().await;
    let before = transaction_rows(wallet.storage()).await;
    match WalletInterface::create_action(&wallet, args, None).await {
        Err(bsv_rs::Error::WalletRefusal { class, message }) => {
            assert_eq!(class, WalletRefusalClass::InvalidParameter);
            assert_eq!(message, expected);
        }
        other => panic!("expected an invalid-parameter refusal, got {other:?}"),
    }
    assert_eq!(transaction_rows(wallet.storage()).await, before);
}

async fn assert_accepted(description: &str) {
    let wallet = wallet().await;
    let before = transaction_rows(wallet.storage()).await;
    let result = WalletInterface::create_action(&wallet, described(description), None)
        .await
        .unwrap();
    assert!(result.signable_transaction.is_some());
    assert_eq!(transaction_rows(wallet.storage()).await, before + 1);
    let stored: String =
        sqlx::query_scalar("SELECT description FROM transactions WHERE is_outgoing = 1")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(stored, description);
}

#[tokio::test]
async fn create_description_four_bytes_refuses_with_reference_text() {
    assert_refused(described("abcd"), TOO_SHORT).await;
    assert_refused(described("éé"), TOO_SHORT).await;
}

#[tokio::test]
async fn create_description_2001_bytes_refuses_with_reference_text() {
    assert_refused(described(&"a".repeat(2001)), TOO_LONG).await;
    assert_refused(described(&format!("{}a", "é".repeat(1000))), TOO_LONG).await;
}

#[tokio::test]
async fn create_description_five_bytes_is_accepted() {
    assert_accepted("abcde").await;
    assert_accepted("ééa").await;
}

#[tokio::test]
async fn create_description_2000_bytes_is_accepted() {
    assert_accepted(&"a".repeat(2000)).await;
    assert_accepted(&"é".repeat(1000)).await;
}

#[tokio::test]
async fn create_description_whitespace_is_counted_and_preserved() {
    assert_accepted(" abc ").await;
    assert_accepted("     ").await;
    assert_refused(described(&format!(" {} ", "a".repeat(1999))), TOO_LONG).await;
}

#[tokio::test]
async fn create_description_refuses_ahead_of_an_invalid_output() {
    let mut args = described("abcd");
    args.outputs.as_mut().unwrap()[0].locking_script.clear();
    assert_refused(args, TOO_SHORT).await;
}
