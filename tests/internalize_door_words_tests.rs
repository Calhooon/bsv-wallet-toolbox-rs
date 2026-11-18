//! What the internalize door reads and writes of each instance's answer to
//! its post of a held presentation (the fix round of the held payment's
//! answer, DECISIONS 29.1 and reading 10 of section 30): a take is read per
//! txid; the door's notes on a request name the door and the presenting
//! user, one per instance; each instance's words are cleaned and bounded
//! where they are written and where they are read; the message carries
//! this call's words alone; a store with no services says so.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::WalletInterface;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "ZG9vci13b3Jkcw==";
const SUFFIX: &str = "d29yZHM=";
const ORIGIN: &str = "test.local";

/// One instance's answer: `status` for `txid` with `data`.
fn answer(name: &str, overall: &str, txid: &str, status: &str, data: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: overall.to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: status.to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(data.to_string()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

/// A payment to `root` and its txid.
fn payment(root: &PrivateKey) -> (bsv_rs::wallet::InternalizeActionArgs, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, SUFFIX);
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    (
        common::args(beef, common::payment(&sender, PREFIX, SUFFIX)),
        txid,
    )
}

async fn scalar(wallet: &TestWallet, sql: &str, txid: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(txid)
        .fetch_optional(wallet.storage().pool())
        .await
        .unwrap()
}

/// ITEM 5: an instance whose answer says success for ANOTHER txid did not
/// take this one: the payment is held, no take recorded under its name.
#[tokio::test]
async fn a_success_for_another_txid_is_no_take() {
    let root = PrivateKey::random();
    let (args, txid) = payment(&root);
    let other = "ee".repeat(32);
    let wallet = common::wallet_posting(
        "door-words",
        &root,
        MockResponse::Success(vec![answer("arcade", "success", &other, "success", "")]),
    )
    .await;
    let got = common::answer_word(wallet.internalize_action(args, Some(ORIGIN)).await, "other");
    let row = scalar(
        &wallet,
        "SELECT status FROM transactions WHERE txid = ?",
        &txid,
    )
    .await;
    let take = scalar(
        &wallet,
        "SELECT status FROM broadcast_seen WHERE txid = ? AND status = 'accepted'",
        &txid,
    )
    .await;
    println!("ITEM5 answer {got} row {row:?} take {take:?}");
    assert_eq!((got, row.as_deref(), take), ("held", Some("sending"), None));
}
