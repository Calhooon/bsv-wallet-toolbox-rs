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

use bsv_rs::wallet::{InternalizeActionArgs, InternalizeOutput};
use bsv_wallet_toolbox_rs::WalletStorageWriter;
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

fn outputs(tx: Vec<u8>, outputs: Vec<InternalizeOutput>) -> InternalizeActionArgs {
    let mut a = args(tx, outputs[0].clone());
    a.outputs = outputs;
    a
}

/// The storage door's answer: whether it merged and the satoshis it
/// reports, or the refusal's text.
async fn at_store(wallet: &TestWallet, a: InternalizeActionArgs) -> Result<(bool, i64), String> {
    let auth = storage_auth(wallet).await;
    wallet
        .storage()
        .internalize_action(&auth, a)
        .await
        .map(|r| (r.is_merge, r.satoshis))
        .map_err(|e| e.to_string())
}

/// The transaction row's satoshis and its output's basket, type and change.
async fn held(wallet: &TestWallet, txid: &str) -> (i64, Option<String>, String, i64) {
    let satoshis = sqlx::query_scalar("SELECT satoshis FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    let (basket, kind, change, ..) = row(wallet, txid, 0).await;
    (satoshis, basket, kind, change)
}

const UNIQUE: &str = "Validation error: The outputs parameter must be unique outputIndex values";

/// One existing output named by a payment and an insertion, in either
/// order, at either door: refused, the row, the transaction's satoshis and
/// the balance unmoved.
#[tokio::test]
async fn one_existing_output_named_by_a_payment_and_an_insertion_is_refused() {
    let root = PrivateKey::random();
    for (order, door) in [
        ("payment then insertion", "storage"),
        ("insertion then payment", "storage"),
        ("payment then insertion", "wallet"),
        ("insertion then payment", "wallet"),
    ] {
        let (wallet, bytes, txid, sender) = held_as_custom(&root).await;
        let before = held(&wallet, &txid).await;
        let payment = common::payment(&sender, PREFIX, SUFFIX);
        let named = match order {
            "payment then insertion" => vec![payment, insertion(0, "tokens")],
            _ => vec![insertion(0, "tokens"), payment],
        };
        let a = outputs(bytes, named);
        let answer = match door {
            "storage" => at_store(&wallet, a).await.map(|_| true),
            _ => at_wallet(&wallet, a).await.map_err(|(_, text)| text),
        };
        assert_eq!(answer, Err(UNIQUE.to_string()), "{order} at {door}");
        assert_eq!(held(&wallet, &txid).await, before, "{order} at {door}");
        assert_eq!(balance(&wallet).await, 0, "{order} at {door}");
    }
}

/// What the reference does not refuse is not refused: two payments of one
/// existing output are each counted in the reported satoshis (the
/// reference's `computeWalletPaymentBalance`, :408-420), one row converted.
#[tokio::test]
async fn two_payments_of_one_existing_output_are_admitted() {
    let root = PrivateKey::random();
    let (wallet, bytes, txid, sender) = held_as_custom(&root).await;
    let payment = common::payment(&sender, PREFIX, SUFFIX);
    let answer = at_store(&wallet, outputs(bytes, vec![payment.clone(), payment])).await;
    assert_eq!(answer, Ok((true, 9000)));
    let (_, basket, kind, change) = held(&wallet, &txid).await;
    assert_eq!(
        (basket.as_deref(), kind.as_str(), change),
        (Some("default"), "P2PKH", 1)
    );
    assert_eq!(balance(&wallet).await, 4500);
}

/// Two insertions of one existing output into its own basket are not
/// refused either: the row stays in its basket.
#[tokio::test]
async fn two_insertions_of_one_existing_output_are_admitted() {
    let root = PrivateKey::random();
    let (wallet, bytes, txid, _) = held_as_custom(&root).await;
    let named = vec![insertion(0, "tokens"), insertion(0, "tokens")];
    assert_eq!(
        at_store(&wallet, outputs(bytes, named)).await,
        Ok((true, 0))
    );
    let (_, basket, kind, change) = held(&wallet, &txid).await;
    assert_eq!(
        (basket.as_deref(), kind.as_str(), change),
        (Some("tokens"), "custom", 0)
    );
}
