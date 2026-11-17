//! `internalizeAction` merging into a transaction the store already holds,
//! as the reference's storage method has it (ts-stack@4d0ea63c8
//! `storage/methods/internalizeAction.ts`):
//!
//! - a wallet payment naming an existing custom output converts it to
//!   managed change and leaves its `spendable` as the row has it
//!   (`mergeWalletPaymentForOutput`, :795-810): an unspent row joins the
//!   balance, a spent one stays spent and out of it;
//! - one existing output named by both a wallet payment and a basket
//!   insertion is refused before any row moves, `The outputs parameter must
//!   be unique outputIndex values` (`linkExistingOutputs`, :345-359).
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/insertion.rs"]
mod insertion;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;

use common::{args, beef_paying, p2pkh_bytes};
use insertion::*;

/// A BRC-29 payment of 4500 satoshis to `root` from a random sender:
/// its atomic BEEF, its txid and the sender.
fn paying(root: &PrivateKey) -> (Vec<u8>, String, PublicKey) {
    let sender = PrivateKey::random().public_key();
    let derived = common::derived_key(root, &sender, PREFIX, SUFFIX);
    let lock = LockingScript::from_binary(&p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = beef_paying(lock);
    (bytes, txid, sender)
}

/// The row's `spendable` and `spent_by`.
async fn spent_state(wallet: &TestWallet, txid: &str) -> (i64, Option<i64>) {
    sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0")
        .bind(txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// A wallet holding `root`'s payment as a custom row in `tokens`.
async fn held_as_custom(root: &PrivateKey) -> (TestWallet, Vec<u8>, String, PublicKey) {
    let (bytes, txid, sender) = paying(root);
    let wallet = wallet(root, &txid).await;
    let inserted = at_wallet(&wallet, args(bytes.clone(), insertion(0, "tokens"))).await;
    assert_eq!(inserted, Ok(true));
    (wallet, bytes, txid, sender)
}

/// A payment merge over a custom row this store holds as spent converts the
/// row to managed change and leaves it spent: the balance does not move.
#[tokio::test]
async fn a_payment_merge_over_a_spent_row_leaves_it_spent() {
    let root = PrivateKey::random();
    let (wallet, bytes, txid, sender) = held_as_custom(&root).await;
    sqlx::query("UPDATE outputs SET spendable = 0 WHERE txid = ?")
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    assert_eq!(balance(&wallet).await, 0);

    let payment = common::payment(&sender, PREFIX, SUFFIX);
    assert_eq!(at_wallet(&wallet, args(bytes, payment)).await, Ok(true));

    let (basket, kind, change, ..) = row(&wallet, &txid, 0).await;
    assert_eq!(
        (basket.as_deref(), kind.as_str(), change),
        (Some("default"), "P2PKH", 1)
    );
    assert_eq!(spent_state(&wallet, &txid).await, (0, None));
    assert_eq!(balance(&wallet).await, 0);
}

/// A payment merge over an unspent custom row converts it to managed
/// change, spendable, and the balance counts it.
#[tokio::test]
async fn a_payment_merge_over_an_unspent_row_makes_it_change() {
    let root = PrivateKey::random();
    let (wallet, bytes, txid, sender) = held_as_custom(&root).await;
    assert_eq!(balance(&wallet).await, 0);

    let payment = common::payment(&sender, PREFIX, SUFFIX);
    assert_eq!(at_wallet(&wallet, args(bytes, payment)).await, Ok(true));

    let (basket, kind, change, ..) = row(&wallet, &txid, 0).await;
    assert_eq!(
        (basket.as_deref(), kind.as_str(), change),
        (Some("default"), "P2PKH", 1)
    );
    assert_eq!(spent_state(&wallet, &txid).await, (1, None));
    assert_eq!(balance(&wallet).await, 4500);
}
