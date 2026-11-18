//! The take recorded by the wallet's own post (the held word, DECISIONS
//! 29.1): `process_action`'s success arm moves every row of the txid that
//! is neither `failed` nor `completed` to `unproven` and records the take in
//! the same write, each accepting instance's name with `accepted`, as the
//! internalize door and send-waiting record theirs. So with a host's own
//! `WalletServices`, which records nothing, a later receipt of that txid by
//! another user of the store with no proof reads it taken.
//!
//! Real SQLite storage shared by two wallets, mock broadcasters and
//! trackers; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

const PREFIX: &str = "b3duLXBvc3Q=";
const ORIGIN: &str = "test.local";

#[tokio::test]
async fn a_receipt_of_a_txid_the_wallets_own_post_took_reads_taken() {
    // One store, two users: A pays C from a coin A received.
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("own-post", &"1".repeat(64)).await.unwrap();
    store.make_available().await.unwrap();
    let root_a = PrivateKey::random();
    let root_c = PrivateKey::random();
    // A host's services that accept every post and record nothing.
    let wallet_a = Wallet::new(
        Some(root_a.clone()),
        store.clone(),
        MockWalletServices::new(),
    )
    .await
    .unwrap();

    // A receives a payment its post takes, then spends it to C.
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root_a, &sender, PREFIX, "ZnVuZA==");
    let (funding, _) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let answer = wallet_a
        .internalize_action(
            common::args(
                funding.clone(),
                common::payment(&sender, PREFIX, "ZnVuZA=="),
            ),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(common::answer_word(answer, "A's funding"), "accepted");
    let a_key = root_a.public_key();
    let c_key = common::derived_key(&root_c, &a_key, PREFIX, "cGF5");
    let made = wallet_a
        .create_action(
            CreateActionArgs {
                description: "A pays C".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: common::p2pkh_bytes(&c_key),
                    satoshis: 1_000,
                    output_description: "a payment to C".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: Some(vec!["underReceivedRule".to_string()]),
                options: Some(CreateActionOptions {
                    accept_delayed_broadcast: Some(false),
                    ..Default::default()
                }),
            },
            Some(ORIGIN),
        )
        .await
        .expect("A's own action, posted and accepted");
    let own = hex::encode(made.txid.expect("txid"));
    let (raw, vout): (Vec<u8>, i64) = sqlx::query_as(
        "SELECT t.raw_tx, o.vout FROM transactions t JOIN outputs o \
         ON o.transaction_id = t.transaction_id WHERE t.txid = ? AND o.satoshis = 1000",
    )
    .bind(&own)
    .fetch_one(store.pool())
    .await
    .unwrap();
    let recorded: Vec<(String, String)> =
        sqlx::query_as("SELECT provider, status FROM broadcast_seen WHERE txid = ?")
            .bind(&own)
            .fetch_all(store.pool())
            .await
            .unwrap();

    // C receives A's transaction with no proof.
    // C's wallet, built now: the store holds one services handle, the last
    // wallet's, and C's posts find no instance answering.
    let wallet_c = Wallet::new(
        Some(root_c.clone()),
        store.clone(),
        MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "no instance answered".into(),
            ))
            .build(),
    )
    .await
    .unwrap();
    let mut atomic = Beef::from_binary(&funding).unwrap();
    atomic.merge_raw_tx(raw, None).unwrap();
    let mut payment = common::payment(&a_key, PREFIX, "cGF5");
    payment.output_index = u32::try_from(vout).unwrap();
    let answer = wallet_c
        .internalize_action(
            common::args(atomic.to_binary_atomic(&own).unwrap(), payment),
            Some(ORIGIN),
        )
        .await;
    let answer = common::answer_word(answer, "C's receipt");
    let c_user = store
        .find_or_insert_user(&root_c.public_key().to_hex())
        .await
        .unwrap()
        .0
        .user_id;
    let c_row: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND user_id = ?")
            .bind(&own)
            .bind(c_user)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let c_posts = wallet_c.services().call_count("post_beef");
    println!(
        "A's own post recorded {recorded:?}; C's receipt: answer {answer} row {c_row} posts {c_posts}"
    );
    assert_eq!(
        recorded,
        vec![("MockProvider".to_string(), "accepted".to_string())],
        "the take of A's own post, recorded in its write"
    );
    assert_eq!(
        (answer, c_row.as_str(), c_posts),
        ("accepted", "unproven", 0)
    );
}
