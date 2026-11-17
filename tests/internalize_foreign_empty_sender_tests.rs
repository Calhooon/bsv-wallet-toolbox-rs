//! A received payment's change row whose stored sender identity key is the
//! empty string, as a damaged store would hold it: the signer reads the
//! empty string as the wallet itself and derives a key that does not unlock
//! the coin. A `createAction` that selects it, signed in the call or by a
//! deferred `signAction`, is refused before anything is broadcast, and the
//! coin is left as the door leaves any coin whose signing failed.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::collections::HashMap;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{other_lock, paying_derived, payment};

type W = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "Zm9yZWlnbi1wcmVmaXg=";
const SUFFIX: &str = "Zm9yZWlnbi1zdWZmaXg=";

/// A wallet holding one coin, a payment from another party internalized
/// with that party's key, its row's sender then set to the empty string.
async fn damaged() -> (W, String) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-foreign-empty-sender-tests", &root, &txid).await;
    wallet
        .internalize_action(
            InternalizeActionArgs {
                tx,
                outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
                description: "a payment from another party".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .expect("the payment");
    sqlx::query("UPDATE outputs SET sender_identity_key = '' WHERE txid = ? AND vout = 0")
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    (wallet, txid)
}

/// The coin's spendable flag and spender, and every action's status.
async fn state(wallet: &W, txid: &str) -> ((bool, Option<i64>), Vec<String>) {
    let coin =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0")
            .bind(txid)
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap();
    let statuses: Vec<(String,)> =
        sqlx::query_as("SELECT status FROM transactions WHERE txid IS NULL OR txid != ?")
            .bind(txid)
            .fetch_all(wallet.storage().writer())
            .await
            .unwrap();
    (coin, statuses.into_iter().map(|(s,)| s).collect())
}

/// A `createAction` of 1000 satoshis to another key, to be broadcast:
/// signed in the call, or (`deferred`) by a `signAction` after.
async fn spend(wallet: &W, deferred: bool) -> Result<(), String> {
    let created = wallet
        .create_action(
            CreateActionArgs {
                description: "a spend of the received coin".to_string(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
                    satoshis: 1_000,
                    output_description: "a payment".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: Some(CreateActionOptions {
                    sign_and_process: Some(!deferred),
                    accept_delayed_broadcast: Some(false),
                    ..Default::default()
                }),
            },
            "test.local",
        )
        .await
        .map_err(|e| format!("createAction: {e}"))?;
    if !deferred {
        return Ok(());
    }
    let signable = created
        .signable_transaction
        .expect("a signable transaction");
    wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: String::from_utf8(signable.reference).unwrap(),
                options: None,
            },
            "test.local",
        )
        .await
        .map(|_| ())
        .map_err(|e| format!("signAction: {e}"))
}

#[tokio::test]
async fn a_received_coin_with_an_empty_stored_sender_is_refused_before_any_broadcast() {
    for deferred in [false, true] {
        let (wallet, txid) = damaged().await;
        let posts = wallet.services().call_count("post_beef");
        let answer = spend(&wallet, deferred).await;
        let after = state(&wallet, &txid).await;
        println!(
            "deferred {deferred}: {answer:?}; broadcasts {}; coin (spendable, spent_by) and actions {after:?}; balance {}",
            wallet.services().call_count("post_beef") - posts,
            wallet.balance().await.unwrap().total
        );
        let call = if deferred {
            "signAction"
        } else {
            "createAction"
        };
        assert!(
            answer.as_ref().is_err_and(|e| e.starts_with(&format!(
                "{call}: wallet error: Signing failed: Signing error: Derived pubkey hash160 "
            ))),
            "refused at signing"
        );
        let actions: Vec<String> = if deferred {
            vec!["failed".into()]
        } else {
            vec![]
        };
        assert_eq!(
            after.1, actions,
            "nothing written, or the deferred action failed"
        );
        assert_eq!(
            wallet.services().call_count("post_beef"),
            posts,
            "nothing broadcast"
        );
        assert_eq!(after.0, (true, None), "the coin is released");
    }
}
