//! A run behind a note for hosts on rows earlier releases wrote:
//! `createAction` with an output whose basket is `default`, what this line
//! answers and the row it writes. (The restore of a received wallet payment
//! with its sender is run through a signed spend in
//! `internalize_sender_spend_tests.rs`.)
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    WalletInterface,
};

use common::{other_lock, paying_derived, payment};

const PREFIX: &str = "bm90ZS1wcmVmaXg=";
const SUFFIX: &str = "bm90ZS1zdWZmaXg=";

fn args(tx: Vec<u8>, sender: &str) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, sender)],
        description: "a payment to restore".to_string(),
        labels: None,
        seek_permission: None,
    }
}

#[tokio::test]
async fn create_action_admits_an_output_into_default_as_a_custom_row() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-restore-notes-tests", &root, &txid).await;
    wallet
        .internalize_action(args(tx, &sender.to_hex()), "test.local")
        .await
        .expect("the payment funds the wallet");
    let answer = wallet
        .create_action(
            CreateActionArgs {
                description: "an output into default".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
                    satoshis: 1_000,
                    output_description: "an output".to_string(),
                    basket: Some("default".to_string()),
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                // The coin is a received payment whose transaction is unproven.
                labels: Some(vec!["underReceivedRule".to_string()]),
                options: Some(CreateActionOptions {
                    no_send: Some(true),
                    randomize_outputs: Some(false),
                    ..Default::default()
                }),
            },
            "test.local",
        )
        .await
        .map(|r| r.txid.is_some())
        .map_err(|e| e.to_string());
    let rows: Vec<(i64, Option<String>, String, bool)> = sqlx::query_as(
        "SELECT o.satoshis, b.name, o.type, o.change FROM outputs o \
         LEFT JOIN output_baskets b ON b.basket_id = o.basket_id \
         WHERE o.satoshis = 1000 ORDER BY o.output_id",
    )
    .fetch_all(wallet.storage().writer())
    .await
    .unwrap();
    println!("createAction, basket default: {answer:?} rows={rows:?}");
    // Admitted on this line: the output is written as a custom row in the
    // default basket, not change.
    assert_eq!(answer, Ok(true));
    assert_eq!(
        rows,
        vec![(
            1_000,
            Some("default".to_string()),
            "custom".to_string(),
            false
        )]
    );
}
