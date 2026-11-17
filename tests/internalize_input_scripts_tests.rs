//! `internalizeAction` runs the unlocking script of every input of a
//! received transaction against the output it spends, at the one door
//! that checks the received BEEF in the wallet's process: the wallet's
//! over a local store (the store's door), the storage door itself, and a
//! wallet whose storage is not local (the wallet's door, storage never
//! called on a refusal).
//!
//! The witnesses hold a coin of the wallet's (a payment it received,
//! unmined, its parent proven after Genesis) and hand the door a
//! transaction spending it:
//!
//! - one input whose unlocking script does not satisfy the coin's P2PKH:
//!   refused, nothing written, the coin's row exactly as before;
//! - two inputs, input 0 verifying and input 1 not: refused at input
//!   1;
//! - an empty unlocking script: refused;
//! - a basket insertion in place of a payment: refused the same way;
//! - the subject carrying its own checked proof: its scripts are not run
//!   (a mined transaction is the chain's word), so it is admitted.
//!
//! And the honest forms stay admitted: a spend of an unmined parent the
//! BEEF carries, a parent of a kind the wallet does not sign for, a sweep
//! of 800 inputs (`beef_link_budget_tests`), a transaction the wallet's own
//! `createAction` and `signAction` built (`internalize_sender_spend_tests`).

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

use common::forwarding::Forwarding;
use common::{
    accepted, after_genesis, anyone_lock, anyone_unlock, derived_key, internalize_at, p2pkh_bytes,
    paying_derived, payment,
};

const PREFIX: &str = "aW5wdXQtc2NyaXB0cw==";
const SUFFIX: &str = "Y29pbg==";
const SECOND: &str = "c2Vjb25k";

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// How the subject's input of the wallet's coin is unlocked.
#[derive(Clone, Copy, Debug)]
enum Unlock {
    /// One empty push: not a signature for the coin's P2PKH.
    EmptyPush,
    /// No script at all.
    Empty,
}

impl Unlock {
    fn script(self) -> UnlockingScript {
        match self {
            Unlock::EmptyPush => UnlockingScript::from_hex("00").unwrap(),
            Unlock::Empty => UnlockingScript::from_binary(&[]).unwrap(),
        }
    }
}

/// What the subject is internalized as.
#[derive(Clone, Copy, Debug)]
enum As {
    Payment,
    Insertion,
}

/// The subject's shape.
#[derive(Clone, Copy, Debug)]
struct Case {
    unlock: Unlock,
    /// A verifying input of a proven parent ahead of the coin's input.
    two_inputs: bool,
    /// The subject carries its own BUMP.
    proven: bool,
    protocol: As,
}

const ONE_INPUT: Case = Case {
    unlock: Unlock::EmptyPush,
    two_inputs: false,
    proven: false,
    protocol: As::Payment,
};

/// A wallet of `root` holding one received coin: the AtomicBEEF that paid
/// it and the coin's txid (vout 0, 9,500 satoshis).
async fn holding(root: &PrivateKey, sender: &PublicKey) -> (TestWallet, Vec<u8>, String) {
    let (tx, txid) = paying_derived(root, sender, PREFIX, SUFFIX);
    let wallet = common::wallet("input-scripts-tests", root, &txid).await;
    let args = InternalizeActionArgs {
        tx: tx.clone(),
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    };
    assert_eq!(
        internalize_at("wallet", &wallet, root, args).await,
        Ok(true),
        "the coin is received"
    );
    (wallet, tx, txid)
}

/// The subject of `case`, spending the coin `coin`:0 carried in `paid`
/// (and, with `two_inputs`, ahead of it an `anyone_lock` output of a proven
/// parent), paying 9,000 satoshis to the key `root` derives for `sender`
/// under `SECOND`: its AtomicBEEF and txid.
fn subject(
    case: Case,
    paid: &[u8],
    coin: &str,
    root: &PrivateKey,
    sender: &PublicKey,
) -> (Vec<u8>, String) {
    let mut beef = Beef::from_binary(paid).unwrap();
    let mut tx = Transaction::new();
    tx.version = 1;
    if case.two_inputs {
        let mut funding = Transaction::new();
        let mut input = TransactionInput::new("77".repeat(32), 0);
        input.unlocking_script = Some(anyone_unlock());
        funding.inputs.push(input);
        funding.outputs.push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: anyone_lock(),
            change: false,
        });
        let funding_txid = funding.id();
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(
            &funding_txid,
            after_genesis(7),
        ));
        beef.merge_raw_tx(funding.to_binary(), Some(bump));
        let mut input = TransactionInput::new(funding_txid, 0);
        input.unlocking_script = Some(anyone_unlock());
        tx.inputs.push(input);
    }
    let mut input = TransactionInput::new(coin.to_string(), 0);
    input.unlocking_script = Some(case.unlock.script());
    tx.inputs.push(input);
    let lock = p2pkh_bytes(&derived_key(root, sender, PREFIX, SECOND));
    tx.outputs.push(TransactionOutput {
        satoshis: Some(9_000),
        locking_script: LockingScript::from_binary(&lock).unwrap(),
        change: false,
    });
    let txid = tx.id();
    if case.proven {
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&txid, after_genesis(9)));
        beef.merge_raw_tx(tx.to_binary(), Some(bump));
    } else {
        beef.merge_raw_tx(tx.to_binary(), None);
    }
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

fn args(case: Case, tx: Vec<u8>, sender: &PublicKey) -> InternalizeActionArgs {
    let output = match case.protocol {
        As::Payment => payment(0, PREFIX, SECOND, &sender.to_hex()),
        As::Insertion => InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "witnesses".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        },
    };
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "a received transaction".to_string(),
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

/// The coin's `spendable` and `spent_by`.
async fn coin_row(wallet: &TestWallet, txid: &str) -> (bool, Option<i64>) {
    sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0")
        .bind(txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// The answer of `case` at `door` (`wallet` over a local store, or
/// `storage`), and the store before and after.
async fn at_local(door: &str, case: Case) -> (Result<bool, String>, String, bool) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (wallet, paid, coin) = holding(&root, &sender).await;
    let (tx, txid) = subject(case, &paid, &coin, &root, &sender);
    let before = store(&wallet).await;
    let coin_before = coin_row(&wallet, &coin).await;
    let answer = internalize_at(door, &wallet, &root, args(case, tx, &sender)).await;
    let after = store(&wallet).await;
    let coin_after = coin_row(&wallet, &coin).await;
    println!(
        "{door} {case:?}: {answer:?}; coin (spendable, spent_by) {coin_before:?} -> {coin_after:?}; \
         rows (outputs, transactions, requests) {:?} -> {:?}",
        before.each_ref().map(Vec::len),
        after.each_ref().map(Vec::len)
    );
    (answer, txid, before == after && coin_before == coin_after)
}

/// The answer of `case` at a wallet whose storage is not local, and the
/// number of internalize calls the storage received.
async fn at_forwarding(case: Case) -> (Result<bool, String>, String, usize) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (local, paid, coin) = holding(&root, &sender).await;
    drop(local);
    let (tx, txid) = subject(case, &paid, &coin, &root, &sender);
    let inner = StorageSqlx::in_memory().await.unwrap();
    inner
        .migrate("input-scripts-forwarding", &root.public_key().to_hex())
        .await
        .unwrap();
    inner.make_available().await.unwrap();
    let internalized = Arc::new(AtomicUsize::new(0));
    let storage = Forwarding {
        inner,
        internalized: internalized.clone(),
    };
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(&txid)]))
        .build();
    let wallet = Wallet::new(Some(root), storage, services).await.unwrap();
    let answer = wallet
        .internalize_action(args(case, tx, &sender), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    let calls = internalized.load(Ordering::SeqCst);
    println!("forwarding {case:?}: {answer:?}; storage internalize calls {calls}");
    (answer, txid, calls)
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

/// The interpreter's sentence for a P2PKH spent by an empty push.
const NOT_A_SIGNATURE: &str = "OP_EQUALVERIFY requires the top two stack items to be equal.";
/// The interpreter's sentence for a P2PKH spent with an empty script.
const EMPTY_STACK: &str = "OP_DUP requires at least one item to be on the stack.";

/// The BEEF offset of the coin's input in the one-input subject (after the
/// BUMP, the proven parent and the coin's transaction).
const ONE_INPUT_AT: u64 = 263;
/// The same in the two-input subject (a second BUMP and parent ahead).
const SECOND_INPUT_AT: u64 = 433;

/// Each refused case, its input, its offset in the BEEF, its sentence.
async fn refused_at_every_door(case: Case, input: u32, offset: u64, why: &str) {
    for door in ["wallet", "storage"] {
        let (answer, txid, unchanged) = at_local(door, case).await;
        assert_eq!(
            answer,
            Err(refusal(door, offset, input, &txid, why)),
            "{door}"
        );
        assert!(
            unchanged,
            "{door}: nothing written, the coin's row as before"
        );
    }
    let (answer, txid, calls) = at_forwarding(case).await;
    assert_eq!(answer, Err(refusal("wallet", offset, input, &txid, why)));
    assert_eq!(calls, 0, "the storage is never called");
}

#[tokio::test]
async fn an_input_that_does_not_satisfy_the_coin_it_spends_is_refused() {
    refused_at_every_door(ONE_INPUT, 0, ONE_INPUT_AT, NOT_A_SIGNATURE).await;
}

#[tokio::test]
async fn two_inputs_one_failing_are_refused_at_the_failing_one() {
    let case = Case {
        two_inputs: true,
        ..ONE_INPUT
    };
    refused_at_every_door(case, 1, SECOND_INPUT_AT, NOT_A_SIGNATURE).await;
}

#[tokio::test]
async fn an_empty_unlocking_script_is_refused() {
    let case = Case {
        unlock: Unlock::Empty,
        ..ONE_INPUT
    };
    refused_at_every_door(case, 0, ONE_INPUT_AT, EMPTY_STACK).await;
}

#[tokio::test]
async fn a_basket_insertion_is_refused_as_a_payment_is() {
    let case = Case {
        protocol: As::Insertion,
        ..ONE_INPUT
    };
    refused_at_every_door(case, 0, ONE_INPUT_AT, NOT_A_SIGNATURE).await;
}

/// The 0.14 line's rule: a transaction the BEEF proves (its BUMP, whose
/// root the tracker confirmed) is not run, at every door.
#[tokio::test]
async fn a_subject_carrying_its_own_checked_proof_is_not_run() {
    let case = Case {
        proven: true,
        ..ONE_INPUT
    };
    for door in ["wallet", "storage"] {
        let (answer, _, _) = at_local(door, case).await;
        assert_eq!(answer, Ok(true), "{door}");
    }
    let (answer, _, calls) = at_forwarding(case).await;
    assert_eq!(answer, Ok(true));
    assert_eq!(calls, 1);
}

/// A payment to the key `root` derives for `sender` under `SUFFIX`
/// spending an unmined parent the BEEF carries whole, itself spending a
/// parent proven after Genesis; each parent output locked by
/// `anyone_lock`, a kind the wallet does not sign for.
fn paying_through_an_unmined_parent(root: &PrivateKey, sender: &PublicKey) -> (Vec<u8>, String) {
    let spending = |prev: &str, satoshis: u64, lock: LockingScript| {
        let mut tx = Transaction::new();
        tx.version = 1;
        let mut input = TransactionInput::new(prev.to_string(), 0);
        input.unlocking_script = Some(anyone_unlock());
        tx.inputs.push(input);
        tx.outputs.push(TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: lock,
            change: false,
        });
        tx
    };
    let proven = spending(&"88".repeat(32), 20_000, anyone_lock());
    let unmined = spending(&proven.id(), 19_500, anyone_lock());
    let lock = p2pkh_bytes(&derived_key(root, sender, PREFIX, SUFFIX));
    let payment = spending(
        &unmined.id(),
        19_000,
        LockingScript::from_binary(&lock).unwrap(),
    );
    let txid = payment.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(
        &proven.id(),
        after_genesis(11),
    ));
    beef.merge_raw_tx(proven.to_binary(), Some(bump));
    beef.merge_raw_tx(unmined.to_binary(), None);
    beef.merge_raw_tx(payment.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// Honest: a payment whose input spends an unmined parent the BEEF
/// carries, of a kind the wallet does not sign for, at both local doors
/// and at a wallet whose storage is not local.
#[tokio::test]
async fn a_spend_of_an_unmined_parent_of_a_foreign_lock_is_admitted() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let args = |tx| InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    };
    for door in ["wallet", "storage"] {
        let (tx, txid) = paying_through_an_unmined_parent(&root, &sender);
        let wallet = common::wallet("input-scripts-honest", &root, &txid).await;
        let answer = internalize_at(door, &wallet, &root, args(tx)).await;
        println!("{door}, through an unmined parent: {answer:?}");
        assert_eq!(answer, Ok(true), "{door}");
    }
    let (tx, txid) = paying_through_an_unmined_parent(&root, &sender);
    let inner = StorageSqlx::in_memory().await.unwrap();
    inner
        .migrate(
            "input-scripts-honest-forwarding",
            &root.public_key().to_hex(),
        )
        .await
        .unwrap();
    inner.make_available().await.unwrap();
    let internalized = Arc::new(AtomicUsize::new(0));
    let storage = Forwarding {
        inner,
        internalized: internalized.clone(),
    };
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(&txid)]))
        .build();
    let wallet = Wallet::new(Some(root.clone()), storage, services)
        .await
        .unwrap();
    let answer = wallet.internalize_action(args(tx), "test.local").await;
    println!("forwarding, through an unmined parent: {answer:?}");
    assert!(answer.unwrap().accepted);
    assert_eq!(internalized.load(Ordering::SeqCst), 1);
}

/// A payment spending an `anyone_lock` output of a parent proven at
/// `height`: its AtomicBEEF and txid.
fn paying_from_a_parent_proven_at(
    height: u32,
    root: &PrivateKey,
    sender: &PublicKey,
) -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("99".repeat(32), 0);
    input.unlocking_script = Some(anyone_unlock());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: anyone_lock(),
        change: false,
    });
    let mut tx = Transaction::new();
    tx.version = 1;
    let mut input = TransactionInput::new(parent.id(), 0);
    input.unlocking_script = Some(anyone_unlock());
    tx.inputs.push(input);
    let lock = p2pkh_bytes(&derived_key(root, sender, PREFIX, SUFFIX));
    tx.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: LockingScript::from_binary(&lock).unwrap(),
        change: false,
    });
    let txid = tx.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent.id(), height));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(tx.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// The 0.14 line's word: a coin its parent's BUMP proves below mainnet's
/// Genesis height is not judged, and refused, at both local doors; the
/// same spend of a coin proven at Genesis is admitted.
#[tokio::test]
async fn a_spend_of_a_coin_proven_before_genesis_is_not_judged() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let args = |tx| InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    };
    for door in ["wallet", "storage"] {
        let (tx, txid) = paying_from_a_parent_proven_at(600_000, &root, &sender);
        let wallet = common::wallet("input-scripts-pre-genesis", &root, &txid).await;
        let before = store(&wallet).await;
        let answer = internalize_at(door, &wallet, &root, args(tx)).await;
        println!("{door}, a coin proven at 600000: {answer:?}");
        assert_eq!(
            answer,
            Err(refusal(
                door,
                PRE_GENESIS_INPUT_AT,
                0,
                &txid,
                "input 0 not judged: coin height 600000 is before Genesis height 620538."
            ))
        );
        assert_eq!(store(&wallet).await, before, "{door}: nothing written");
        let (tx, txid) = paying_from_a_parent_proven_at(after_genesis(0), &root, &sender);
        let wallet = common::wallet("input-scripts-at-genesis", &root, &txid).await;
        assert_eq!(
            internalize_at(door, &wallet, &root, args(tx)).await,
            Ok(true)
        );
    }
}

/// The BEEF offset of the input in `paying_from_a_parent_proven_at`.
const PRE_GENESIS_INPUT_AT: u64 = 176;
