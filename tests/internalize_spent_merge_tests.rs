//! A wallet payment merged over an output the wallet already holds leaves
//! `spendable` and `spent_by` as the row holds them, as the reference's
//! `mergeWalletPaymentForOutput` does: a spent output stays spent and the
//! balance does not move; an unspent one becomes managed change.
//!
//! The last test runs what a host does about a row an earlier release wrote:
//! a spent output a payment merge made spendable (it counts in the balance
//! with `spent_by` set), found by `spendable = 1 AND spent_by IS NOT NULL`.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    BasketInsertion, InternalizeActionArgs, InternalizeOutput, RelinquishOutputArgs,
    WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{paying_derived, payment};

const PREFIX: &str = "c3BlbnQtcHJlZml4";
const SUFFIX: &str = "c3BlbnQtc3VmZml4";

type W = Wallet<StorageSqlx, MockWalletServices>;

fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "an output to merge".to_string(),
        labels: None,
        seek_permission: None,
    }
}

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

/// A wallet holding output 0 of a payment to it as a custom row in
/// `tokens`; spent by its own transaction when `spent`. Returns the wallet,
/// the BEEF, its txid and the payment's sender.
async fn custom_row(spent: bool) -> (W, Vec<u8>, String, String) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-spent-merge-tests", &root, &txid).await;
    wallet
        .internalize_action(args(tx.clone(), insertion("tokens")), "test.local")
        .await
        .expect("the insertion");
    if spent {
        sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = transaction_id WHERE txid = ? AND vout = 0",
        )
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    }
    (wallet, tx, txid, sender.to_hex())
}

async fn spend_state(wallet: &W, txid: &str) -> (bool, Option<i64>, bool, String) {
    sqlx::query_as(
        "SELECT spendable, spent_by, change, type FROM outputs WHERE txid = ? AND vout = 0",
    )
    .bind(txid)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

#[tokio::test]
async fn a_payment_merge_over_a_spent_row_leaves_it_spent() {
    let (wallet, tx, txid, sender) = custom_row(true).await;
    let (_, spent_by, _, _) = spend_state(&wallet, &txid).await;
    assert!(spent_by.is_some());
    let answer = wallet
        .internalize_action(args(tx, payment(0, PREFIX, SUFFIX, &sender)), "test.local")
        .await;
    assert!(answer.unwrap().accepted);
    let (spendable, after_spent_by, change, kind) = spend_state(&wallet, &txid).await;
    println!("spent row after a payment merge: spendable={spendable} spent_by={after_spent_by:?} change={change} type={kind}");
    assert!(!spendable, "a spent output stays spent");
    assert_eq!(after_spent_by, spent_by);
    assert_eq!(
        wallet.balance().await.unwrap().total,
        0,
        "the balance does not move"
    );
}

#[tokio::test]
async fn a_payment_merge_over_an_unspent_row_makes_it_managed_change() {
    let (wallet, tx, txid, sender) = custom_row(false).await;
    let answer = wallet
        .internalize_action(args(tx, payment(0, PREFIX, SUFFIX, &sender)), "test.local")
        .await;
    assert!(answer.unwrap().accepted);
    assert_eq!(
        spend_state(&wallet, &txid).await,
        (true, None, true, "P2PKH".to_string())
    );
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
}

/// A row an earlier release wrote: spent, then made spendable by a payment
/// merge. It counts in the balance; the host finds it by `spendable = 1 AND
/// spent_by IS NOT NULL`, and a relinquish of it from the default basket
/// takes it out of the balance (as does writing `spendable = 0` on it).
#[tokio::test]
async fn a_spent_row_an_earlier_merge_made_spendable_is_found_and_relinquished() {
    let (wallet, _tx, txid, _sender) = custom_row(true).await;
    let pool = wallet.storage().writer();
    sqlx::query(
        "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE name = 'default'), \
         type = 'P2PKH', change = 1, spendable = 1 WHERE txid = ? AND vout = 0",
    )
    .bind(&txid)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(wallet.balance().await.unwrap().total, 9_500, "counted");

    let found: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT user_id, txid, vout FROM outputs WHERE spendable = 1 AND spent_by IS NOT NULL",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    println!("found: {found:?}");
    let (user_id,): (i64,) = sqlx::query_as("SELECT user_id FROM users")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(found, vec![(user_id, txid.clone(), 0)]);

    let relinquished = wallet
        .relinquish_output(
            RelinquishOutputArgs {
                basket: "default".to_string(),
                output: bsv_rs::wallet::Outpoint::from_string(&format!("{txid}.0")).unwrap(),
            },
            "test.local",
        )
        .await;
    println!("relinquish: {relinquished:?}");
    assert!(relinquished.unwrap().relinquished);
    assert_eq!(
        wallet.balance().await.unwrap().total,
        0,
        "out of the balance"
    );
}
