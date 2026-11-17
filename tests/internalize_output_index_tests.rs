//! A request whose outputs name one output index more than once is refused
//! before anything is written or broadcast, whatever the protocols, on a new
//! transaction and on the merge path alike, at both doors, with `The outputs
//! parameter must be unique outputIndex values`. The reference ends every
//! such request without a write (ts-stack 4d0ea63c8: `linkExistingOutputs`
//! for a payment and an insertion of a held output, the outputs table's
//! unique key for every other pair).
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{paying_derived, payment};

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

/// A wallet paid at output 0 of a transaction, the BEEF and the payment
/// output naming it; with `held`, the wallet already holds that output,
/// inserted into `tokens`.
async fn setup(held: bool) -> (W, PrivateKey, Vec<u8>, InternalizeOutput) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let wallet = common::wallet("internalize-output-index-tests", &root, &txid).await;
    if held {
        wallet
            .internalize_action(args(tx.clone(), vec![insertion("tokens")]), "test.local")
            .await
            .expect("the insertion");
    }
    let pay = payment(0, PREFIX, SUFFIX, &sender.to_hex());
    (wallet, root, tx, pay)
}

/// Every row of the tables an internalization writes, as text.
async fn rows(wallet: &W) -> String {
    let mut all = String::new();
    for table in [
        "outputs",
        "transactions",
        "proven_tx_reqs",
        "output_baskets",
        "output_tags_map",
        "tx_labels_map",
    ] {
        let columns: Vec<(String,)> =
            sqlx::query_as(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .fetch_all(wallet.storage().writer())
                .await
                .unwrap();
        let quoted: Vec<String> = columns.iter().map(|(c,)| format!("quote({c})")).collect();
        let lines: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT {} FROM {table} ORDER BY rowid",
            quoted.join(" || '|' || ")
        ))
        .fetch_all(wallet.storage().writer())
        .await
        .unwrap();
        all.push_str(table);
        for (line,) in lines {
            all.push_str(&format!("\n{line}"));
        }
        all.push('\n');
    }
    all
}

#[tokio::test]
async fn one_output_index_named_twice_is_refused_for_every_pair_and_writes_nothing() {
    let pairs = [
        "payment, payment",
        "insertion, insertion",
        "payment, insertion",
        "insertion, payment",
    ];
    let mut wrong = Vec::new();
    for held in [false, true] {
        for door in ["wallet", "storage"] {
            for pair in pairs {
                let (wallet, root, tx, pay) = setup(held).await;
                let ins = insertion("tokens");
                let outputs = match pair {
                    "payment, payment" => vec![pay.clone(), pay],
                    "insertion, insertion" => vec![ins.clone(), ins],
                    "payment, insertion" => vec![pay, ins],
                    _ => vec![ins, pay],
                };
                let before = rows(&wallet).await;
                let balance = wallet.balance().await.unwrap().total;
                let posts = wallet.services().call_count("post_beef");
                let answer = common::internalize_at(door, &wallet, &root, args(tx, outputs)).await;
                let after = rows(&wallet).await;
                let balance_after = wallet.balance().await.unwrap().total;
                let posts_after = wallet.services().call_count("post_beef");
                println!(
                    "held {held}, {door} door, {pair}: {answer:?}; rows moved {}; balance {balance} -> {balance_after}; broadcasts {}",
                    before != after,
                    posts_after - posts
                );
                let expected = match door {
                    "wallet" => format!("wallet error: Validation error: {UNIQUE}"),
                    _ => format!("Validation error: {UNIQUE}"),
                };
                if answer != Err(expected)
                    || before != after
                    || balance != balance_after
                    || posts != posts_after
                {
                    wrong.push(format!("held {held}, {door} door, {pair}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "not refused cleanly: {wrong:?}");
}
