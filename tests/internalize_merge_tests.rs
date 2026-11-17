//! `internalizeAction` merging into a transaction the store already holds,
//! as the reference's storage method has it (ts-stack@4d0ea63c8
//! `storage/methods/internalizeAction.ts`):
//!
//! - a wallet payment naming an existing custom output converts it to
//!   managed change and leaves its `spendable` as the row has it
//!   (`mergeWalletPaymentForOutput`, :795-810): an unspent row joins the
//!   balance, a spent one stays spent and out of it;
//! - a request naming one output more than once, whatever the protocols,
//!   is refused before anything is written or broadcast, `The outputs
//!   parameter must be unique outputIndex values`, on this path as on a new
//!   transaction: the reference refuses one existing output named by a
//!   payment and an insertion with that sentence (`linkExistingOutputs`,
//!   :345-359), and every other pair fails there on the outputs table's
//!   unique key (`transactionId`, `vout`, `userId`) inside the merge's
//!   transaction (:610, `storeNew*ForOutput` :764-792, :829-866), so the
//!   whole merge writes nothing. (On a new transaction the reference meets
//!   the same key only after it recorded the transaction, with its
//!   proven-transaction row where the BEEF carried a proof and its proof
//!   request where it did not, and the inputs it spends marked spent,
//!   :663-739; this port refuses ahead of that.)
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
use bsv_rs::WalletRefusalClass;
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

const UNIQUE: &str = "Validation error: The outputs parameter must be unique outputIndex values";

/// Everything a refused call could have moved: every output and
/// transaction row, every request and label, and the balance.
async fn snapshot(wallet: &TestWallet) -> (String, u64, usize) {
    let rows: String = sqlx::query_scalar(
        "SELECT COALESCE((SELECT group_concat(quote(output_id) || quote(basket_id) \
         || quote(spendable) || quote(change) || quote(vout) || quote(satoshis) \
         || quote(provided_by) || quote(purpose) || quote(type) || quote(txid) \
         || quote(sender_identity_key) || quote(derivation_prefix) \
         || quote(derivation_suffix) || quote(custom_instructions) || quote(spent_by) \
         || quote(updated_at), ';') FROM outputs), '') || '#' || \
         COALESCE((SELECT group_concat(quote(transaction_id) || quote(status) \
         || quote(satoshis) || quote(txid) || quote(updated_at), ';') FROM transactions), '') \
         || '#' || (SELECT COUNT(*) FROM proven_tx_reqs) || '#' || \
         (SELECT COUNT(*) FROM tx_labels) || '#' || (SELECT COUNT(*) FROM output_baskets)",
    )
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap();
    let posted = wallet.services().call_count("post_beef");
    (rows, balance(wallet).await, posted)
}

/// One output named twice by one request: every pair of protocols, in
/// both orders, on a transaction the store does not hold and on one it
/// holds, at both doors: refused with the sentence, the class through the
/// wallet `InvalidParameter`, every row and the balance as before the call,
/// nothing broadcast.
#[tokio::test]
async fn one_output_named_twice_is_refused_whatever_the_protocols() {
    let root = PrivateKey::random();
    let pairs = [
        "payment and payment",
        "insertion and insertion",
        "payment and insertion",
        "insertion and payment",
    ];
    for held_before in [false, true] {
        for pair in pairs {
            for door in ["storage", "wallet"] {
                let (wallet, bytes, sender) = if held_before {
                    let (wallet, bytes, _, sender) = held_as_custom(&root).await;
                    (wallet, bytes, sender)
                } else {
                    let (bytes, txid, sender) = paying(&root);
                    (wallet(&root, &txid).await, bytes, sender)
                };
                let payment = common::payment(&sender, PREFIX, SUFFIX);
                let named = match pair {
                    "payment and payment" => vec![payment.clone(), payment],
                    "insertion and insertion" => {
                        vec![insertion(0, "tokens"), insertion(0, "tokens")]
                    }
                    "payment and insertion" => vec![payment, insertion(0, "tokens")],
                    _ => vec![insertion(0, "tokens"), payment],
                };
                let before = snapshot(&wallet).await;
                let a = outputs(bytes, named);
                let answer = match door {
                    "storage" => at_storage(&wallet, a)
                        .await
                        .map(|ok| format!("accepted {ok}")),
                    _ => at_wallet(&wallet, a)
                        .await
                        .map(|ok| format!("accepted {ok}")),
                };
                let case = format!("{pair}, held {held_before}, at {door}");
                println!("{case}: {answer:?}");
                assert_eq!(
                    answer,
                    Err((WalletRefusalClass::InvalidParameter, UNIQUE.to_string())),
                    "{case}"
                );
                assert_eq!(snapshot(&wallet).await, before, "{case}");
            }
        }
    }
}
