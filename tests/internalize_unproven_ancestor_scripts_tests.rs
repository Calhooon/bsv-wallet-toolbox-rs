//! `internalizeAction` runs every input of every transaction of the
//! received BEEF that the BEEF does not prove, the subject's unproven
//! ancestors as well as the subject, at the one door that checks the
//! received BEEF in the wallet's process: the wallet's over a local store
//! (the store's door), the storage door itself, and a wallet whose storage
//! is not local (the wallet's door, storage never called on a refusal).
//!
//! - a subject whose own input passes, spending an unproven parent whose
//!   input does not satisfy the output it spends: refused at the parent's
//!   input, nothing written, every row the store held as before;
//! - a subject spending a chain of two unproven ancestors whose inputs all
//!   pass: admitted.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

use common::forwarding::Forwarding;
use common::{
    accepted, after_genesis, anyone_lock, anyone_unlock, derived_key, internalize_at, p2pkh_bytes,
    paying_derived, payment,
};

const PREFIX: &str = "YW5jZXN0b3Jz";
const SUFFIX: &str = "Y29pbg==";
const SECOND: &str = "c2Vjb25k";

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// A transaction spending `prev`:0 with `unlock` into one output of
/// `satoshis` locked by `lock`.
fn spending(
    prev: &str,
    unlock: UnlockingScript,
    satoshis: u64,
    lock: LockingScript,
) -> Transaction {
    let mut tx = Transaction::new();
    tx.version = 1;
    let mut input = TransactionInput::new(prev.to_string(), 0);
    input.unlocking_script = Some(unlock);
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: lock,
        change: false,
    });
    tx
}

/// A payment to the key `root` derives for `sender` under `suffix`, its
/// own input passing, spending `unproven` ancestors in a chain from a
/// parent proven after Genesis; every ancestor's output locked by
/// `anyone_lock`. The unproven ancestor next to the proven one spends it
/// with `first_unlock`; the others with `anyone_unlock`. The AtomicBEEF,
/// the payment's txid, and the first unproven ancestor's txid.
fn paying_through(
    unproven: usize,
    first_unlock: UnlockingScript,
    root: &PrivateKey,
    sender: &PublicKey,
    suffix: &str,
) -> (Vec<u8>, String, String) {
    let proven = spending(&"5a".repeat(32), anyone_unlock(), 30_000, anyone_lock());
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(
        &proven.id(),
        after_genesis(13),
    ));
    beef.merge_raw_tx(proven.to_binary(), Some(bump));
    let mut prev = proven.id();
    let mut satoshis = 30_000;
    let mut first = String::new();
    for i in 0..unproven {
        satoshis -= 500;
        let unlock = if i == 0 {
            first_unlock.clone()
        } else {
            anyone_unlock()
        };
        let tx = spending(&prev, unlock, satoshis, anyone_lock());
        prev = tx.id();
        if i == 0 {
            first = prev.clone();
        }
        beef.merge_raw_tx(tx.to_binary(), None);
    }
    let lock = p2pkh_bytes(&derived_key(root, sender, PREFIX, suffix));
    let payment = spending(
        &prev,
        anyone_unlock(),
        satoshis - 500,
        LockingScript::from_binary(&lock).unwrap(),
    );
    let txid = payment.id();
    beef.merge_raw_tx(payment.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid, first)
}

fn args(tx: Vec<u8>, sender: &PublicKey, suffix: &str) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, suffix, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// Every row of `table`, every column quoted, in key order.
async fn rows(wallet: &TestWallet, table: &str, key: &str) -> Vec<String> {
    let columns: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT name FROM pragma_table_info('{table}') ORDER BY cid"
    ))
    .fetch_all(wallet.storage().writer())
    .await
    .unwrap();
    let row = columns
        .iter()
        .map(|c| format!("quote({c})"))
        .collect::<Vec<_>>()
        .join(" || '|' || ");
    sqlx::query_scalar(&format!("SELECT {row} FROM {table} ORDER BY {key}"))
        .fetch_all(wallet.storage().writer())
        .await
        .unwrap()
}

/// What the store holds: every output, transaction and request row.
async fn store(wallet: &TestWallet) -> [Vec<String>; 3] {
    [
        rows(wallet, "outputs", "output_id").await,
        rows(wallet, "transactions", "transaction_id").await,
        rows(wallet, "proven_tx_reqs", "proven_tx_req_id").await,
    ]
}

/// A wallet of `root` over a local store, holding one received coin.
async fn holding(root: &PrivateKey, sender: &PublicKey) -> TestWallet {
    let (tx, coin) = paying_derived(root, sender, PREFIX, SUFFIX);
    let wallet = common::wallet("ancestor-scripts-tests", root, &coin).await;
    assert_eq!(
        internalize_at("wallet", &wallet, root, args(tx, sender, SUFFIX)).await,
        Ok(true),
        "the coin is received"
    );
    wallet
}

/// A wallet of `root` whose storage is not local, and the count of
/// internalize calls its storage receives.
async fn forwarding(
    root: &PrivateKey,
    txid: &str,
) -> (Wallet<Forwarding, MockWalletServices>, Arc<AtomicUsize>) {
    let inner = StorageSqlx::in_memory().await.unwrap();
    inner
        .migrate("ancestor-scripts-forwarding", &root.public_key().to_hex())
        .await
        .unwrap();
    inner.make_available().await.unwrap();
    let internalized = Arc::new(AtomicUsize::new(0));
    let storage = Forwarding {
        inner,
        internalized: internalized.clone(),
    };
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build();
    let wallet = Wallet::new(Some(root.clone()), storage, services)
        .await
        .unwrap();
    (wallet, internalized)
}

/// The refusal's whole text at `door`.
fn refusal(door: &str, offset: u64, input: u32, txid: &str, why: &str) -> String {
    let prefix = if door == "storage" {
        ""
    } else {
        "wallet error: "
    };
    format!("{prefix}Validation error: spend refused at offset {offset}, input {input} of {txid}: {why}")
}

/// The interpreter's sentence for `anyone_lock` spent with no script: its
/// second drop finds the stack empty.
const NOTHING_TO_DROP: &str = "OP_DROP requires at least one item to be on the stack.";

/// The BEEF offset of the unproven parent's input (after the BUMP and the
/// proven ancestor).
const PARENT_INPUT_AT: u64 = 176;

#[tokio::test]
async fn an_unproven_parent_whose_input_fails_is_refused_with_nothing_written() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let empty = UnlockingScript::from_binary(&[]).unwrap();
    for door in ["wallet", "storage"] {
        let (tx, _, parent) = paying_through(1, empty.clone(), &root, &sender, SECOND);
        let wallet = holding(&root, &sender).await;
        let before = store(&wallet).await;
        let answer = internalize_at(door, &wallet, &root, args(tx, &sender, SECOND)).await;
        let after = store(&wallet).await;
        println!(
            "{door}, a failing unproven parent: {answer:?}; rows (outputs, transactions, \
             requests) {:?} -> {:?}; unchanged {}",
            before.each_ref().map(Vec::len),
            after.each_ref().map(Vec::len),
            before == after
        );
        assert_eq!(
            answer,
            Err(refusal(door, PARENT_INPUT_AT, 0, &parent, NOTHING_TO_DROP)),
            "{door}"
        );
        assert_eq!(
            after, before,
            "{door}: nothing written, every row as before"
        );
    }
    let (tx, txid, parent) = paying_through(1, empty, &root, &sender, SECOND);
    let (wallet, internalized) = forwarding(&root, &txid).await;
    let answer = wallet
        .internalize_action(args(tx, &sender, SECOND), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    let calls = internalized.load(Ordering::SeqCst);
    println!(
        "forwarding, a failing unproven parent: {answer:?}; storage internalize calls {calls}"
    );
    assert_eq!(
        answer,
        Err(refusal(
            "wallet",
            PARENT_INPUT_AT,
            0,
            &parent,
            NOTHING_TO_DROP
        ))
    );
    assert_eq!(calls, 0, "the storage is never called");
}

#[tokio::test]
async fn a_chain_of_two_unproven_ancestors_that_pass_is_admitted() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    for door in ["wallet", "storage"] {
        let (tx, txid, _) = paying_through(2, anyone_unlock(), &root, &sender, SUFFIX);
        let wallet = common::wallet("ancestor-scripts-honest", &root, &txid).await;
        let answer = internalize_at(door, &wallet, &root, args(tx, &sender, SUFFIX)).await;
        println!("{door}, two unproven ancestors: {answer:?}");
        assert_eq!(answer, Ok(true), "{door}");
    }
    let (tx, txid, _) = paying_through(2, anyone_unlock(), &root, &sender, SUFFIX);
    let (wallet, internalized) = forwarding(&root, &txid).await;
    let answer = wallet
        .internalize_action(args(tx, &sender, SUFFIX), "test.local")
        .await;
    println!("forwarding, two unproven ancestors: {answer:?}");
    assert!(answer.unwrap().accepted);
    assert_eq!(internalized.load(Ordering::SeqCst), 1);
}
