//! Two runs behind notes for hosts on rows earlier releases wrote.
//!
//! - A received wallet payment an earlier release reclassified as a basket
//!   insertion (derivation and sender cleared) is restored by a wallet
//!   payment naming its prefix, its suffix AND the sender identity key the
//!   row held: the door derives with the remittance's sender as counterparty
//!   (the wallet itself when it is empty), so the prefix and suffix alone
//!   derive another key and are refused.
//! - `createAction` with an output whose basket is `default`: what this
//!   line answers and the row it writes.
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
async fn a_reclassified_received_payment_is_restored_only_with_its_sender() {
    let root = PrivateKey::random();
    let sender_key = PrivateKey::random().public_key();
    let sender = sender_key.to_hex();
    let (tx, txid) = paying_derived(&root, &sender_key, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-restore-notes-tests", &root, &txid).await;
    wallet
        .internalize_action(args(tx.clone(), &sender), "test.local")
        .await
        .expect("the payment");
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    let pool = wallet.storage().writer();
    // The row an earlier release wrote for a basket insertion into "tokens".
    sqlx::query(
        "INSERT INTO output_baskets (user_id, name, created_at, updated_at) \
         SELECT user_id, 'tokens', created_at, updated_at FROM output_baskets WHERE name = 'default'",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'tokens'), \
         type = 'custom', change = 0, derivation_prefix = NULL, derivation_suffix = NULL, \
         sender_identity_key = NULL WHERE txid = ? AND vout = 0",
    )
    .bind(&txid)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(wallet.balance().await.unwrap().total, 0);

    let without_sender = wallet
        .internalize_action(args(tx.clone(), ""), "test.local")
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    println!("prefix and suffix, no sender: {without_sender:?}");
    assert!(without_sender
        .as_ref()
        .is_err_and(|e| e.starts_with("wallet error: Derivation mismatch for output 0")));

    let restored = wallet
        .internalize_action(args(tx, &sender), "test.local")
        .await
        .expect("prefix, suffix and sender restore it");
    assert!(restored.accepted);
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
}

#[tokio::test]
async fn create_action_with_an_output_in_the_default_basket() {
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
                labels: None,
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
