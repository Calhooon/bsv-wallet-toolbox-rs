//! On the merge path (a transaction the wallet already holds), one existing
//! output named by both a wallet payment and a basket insertion is refused
//! before any row is written, in either order, as the reference's
//! `linkExistingOutputs` refuses it (`The outputs parameter must be unique
//! outputIndex values`, ts-stack 4d0ea63c8). Nothing wider: one output named
//! twice by the same protocol is admitted on the merge path as before.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

use common::{auth, paying_derived, payment};

const PREFIX: &str = "aW5kZXgtcHJlZml4";
const SUFFIX: &str = "aW5kZXgtc3VmZml4";
const UNIQUE: &str = "The outputs parameter must be unique outputIndex values";

type W = Wallet<StorageSqlx, MockWalletServices>;

fn insertion(basket: &str) -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: basket.to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

fn args(tx: Vec<u8>, outputs: Vec<InternalizeOutput>) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs,
        description: "outputs to merge".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// A wallet that already holds output 0 of a payment to it, inserted into
/// `tokens`; the BEEF and the payment output naming it.
async fn held() -> (W, PrivateKey, Vec<u8>, String, InternalizeOutput) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-output-index-tests", &root, &txid).await;
    wallet
        .internalize_action(args(tx.clone(), vec![insertion("tokens")]), "test.local")
        .await
        .expect("the insertion");
    let pay = payment(0, PREFIX, SUFFIX, &sender.to_hex());
    (wallet, root, tx, txid, pay)
}

type State = (i64, String, bool, bool, Option<i64>, Option<String>, i64);

async fn state(wallet: &W, txid: &str) -> State {
    sqlx::query_as(
        "SELECT o.basket_id, o.type, o.change, o.spendable, o.spent_by, o.derivation_prefix, \
         t.satoshis FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.txid = ? AND o.vout = 0",
    )
    .bind(txid)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

#[tokio::test]
async fn a_payment_and_an_insertion_of_one_held_output_are_refused_in_either_order() {
    for door in ["wallet", "storage"] {
        for payment_first in [true, false] {
            let (wallet, root, tx, txid, pay) = held().await;
            let outputs = if payment_first {
                vec![pay, insertion("tokens")]
            } else {
                vec![insertion("tokens"), pay]
            };
            let before = state(&wallet, &txid).await;
            let balance = wallet.balance().await.unwrap().total;
            let answer = common::internalize_at(door, &wallet, &root, args(tx, outputs)).await;
            println!("{door}, payment first {payment_first}: {answer:?}");
            let expected = match door {
                "wallet" => format!("wallet error: Validation error: {UNIQUE}"),
                _ => format!("Validation error: {UNIQUE}"),
            };
            assert_eq!(answer, Err(expected));
            assert_eq!(state(&wallet, &txid).await, before, "the row is unmoved");
            assert_eq!(wallet.balance().await.unwrap().total, balance);
        }
    }
}

#[tokio::test]
async fn one_held_output_named_twice_by_one_protocol_is_admitted_as_before() {
    let (wallet, root, tx, txid, pay) = held().await;
    let result = wallet
        .storage()
        .internalize_action(
            &auth(&wallet, &root).await,
            args(tx.clone(), vec![pay.clone(), pay]),
        )
        .await
        .unwrap();
    println!(
        "two payments of one held output: accepted={} merge={} satoshis={} row={:?}",
        result.base.accepted,
        result.is_merge,
        result.satoshis,
        state(&wallet, &txid).await
    );
    assert!(result.base.accepted);

    let (wallet, _root, tx, _txid, _pay) = held().await;
    let answer = wallet
        .internalize_action(
            args(tx, vec![insertion("tokens"), insertion("tokens")]),
            "test.local",
        )
        .await;
    assert!(answer.unwrap().accepted);
}
