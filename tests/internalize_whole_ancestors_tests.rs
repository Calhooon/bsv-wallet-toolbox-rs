//! The storage door's `internalizeAction` reads a received AtomicBEEF only
//! when every transaction it carries is whole: an entry carried by txid
//! alone is refused before any row is written, whether or not the store
//! holds that transaction, as the reference's storage reads it
//! (`ab.verify(chainTracker, false)`).
//!
//! A wallet on its own local store (`StorageSqlx`), whose services' chain
//! tracker confirms every root; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

use common::{accepted, derived_key, p2pkh_bytes, payment, stored_outputs};

const PREFIX: &str = "d2hvbGUtcHJlZml4";
const ONE: &str = "b25lLXN1ZmZpeA==";
const SECOND: &str = "c2Vjb25kLXN1ZmZpeA==";

type W = Wallet<StorageSqlx, MockWalletServices>;

/// A wallet of `root` on a local in-memory store whose services' tracker
/// confirms every root.
async fn wallet(root: &PrivateKey) -> W {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate(
            "internalize-whole-ancestors-tests",
            &root.public_key().to_hex(),
        )
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let services = MockWalletServices::builder()
        .roots_valid(true)
        .post_beef_response(MockResponse::Success(vec![accepted(&"00".repeat(32))]))
        .build();
    Wallet::new(Some(root.clone()), storage, services)
        .await
        .unwrap()
}

/// The P2PKH of the key `root` derives for `sender` under `suffix`.
fn lock(root: &PrivateKey, sender: &PublicKey, suffix: &str) -> LockingScript {
    LockingScript::from_binary(&p2pkh_bytes(&derived_key(root, sender, PREFIX, suffix))).unwrap()
}

/// A transaction spending `txid:0` into one output of `satoshis` locked by
/// `lock`.
fn spending(txid: &str, satoshis: u64, lock: LockingScript) -> Transaction {
    let mut tx = Transaction::new();
    tx.version = 1;
    let mut input = TransactionInput::new(txid.to_string(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: lock,
        change: false,
    });
    tx
}

/// An AtomicBEEF of `child`, carrying its parent `parent` with the parent's
/// BUMP at `height`: whole when `whole`, else by txid alone.
fn beef_of(child: &Transaction, parent: &Transaction, height: u32, whole: bool) -> Vec<u8> {
    let parent_txid = parent.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, height));
    if whole {
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
    } else {
        beef.merge_txid_only(parent_txid);
    }
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary_atomic(&child.id()).unwrap()
}

fn args(tx: Vec<u8>, suffix: &str, sender: &PublicKey) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, suffix, &sender.to_hex())],
        description: "a payment whose ancestry is read".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// The rows a payment writes: transactions, outputs, proof requests.
async fn rows(wallet: &W) -> (i64, i64, i64) {
    let count = |table: &'static str| async move {
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap()
    };
    (
        count("transactions").await,
        stored_outputs(wallet).await,
        count("proven_tx_reqs").await,
    )
}

async fn internalize(wallet: &W, args: InternalizeActionArgs) -> Result<bool, String> {
    wallet
        .internalize_action(args, "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string())
}

fn carried_by_txid_alone(txid: &str) -> String {
    format!(
        "wallet error: Validation error: The tx parameter must be valid AtomicBEEF: \
         transaction {txid} is carried by txid alone"
    )
}

/// A parent from outside the wallet: one input, one output to no party.
fn foreign_parent() -> Transaction {
    spending(&"44".repeat(32), 10_000, common::other_lock())
}

#[tokio::test]
async fn a_parent_unknown_to_the_store_carried_by_txid_alone_is_refused() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let wallet = wallet(&root).await;
    let parent = foreign_parent();
    let child = spending(&parent.id(), 9_500, lock(&root, &sender, ONE));
    let before = (rows(&wallet).await, wallet.balance().await.unwrap().total);
    let answer = internalize(
        &wallet,
        args(beef_of(&child, &parent, 100_003, false), ONE, &sender),
    )
    .await;
    let after = (rows(&wallet).await, wallet.balance().await.unwrap().total);
    println!(
        "(i) parent unknown, by txid alone: {answer:?}; rows and balance {before:?} -> {after:?}"
    );
    assert_eq!(answer, Err(carried_by_txid_alone(&parent.id())));
    assert_eq!(after, before, "nothing written, the balance unmoved");
}

#[tokio::test]
async fn a_parent_the_store_holds_carried_by_txid_alone_is_refused() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let wallet = wallet(&root).await;
    // The store holds the parent: a payment of its own, read whole.
    let grandparent = foreign_parent();
    let parent = spending(&grandparent.id(), 9_800, lock(&root, &sender, ONE));
    let held = internalize(
        &wallet,
        args(beef_of(&parent, &grandparent, 100_004, true), ONE, &sender),
    )
    .await;
    assert_eq!(held, Ok(true), "the parent is held");
    let child = spending(&parent.id(), 9_500, lock(&root, &sender, SECOND));
    let before = (rows(&wallet).await, wallet.balance().await.unwrap().total);
    let answer = internalize(
        &wallet,
        args(beef_of(&child, &parent, 100_005, false), SECOND, &sender),
    )
    .await;
    let after = (rows(&wallet).await, wallet.balance().await.unwrap().total);
    println!(
        "(ii) parent held, by txid alone: {answer:?}; rows and balance {before:?} -> {after:?}"
    );
    assert_eq!(answer, Err(carried_by_txid_alone(&parent.id())));
    assert_eq!(after, before, "nothing written, the balance unmoved");
}

#[tokio::test]
async fn a_beef_with_every_ancestor_whole_is_admitted() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let wallet = wallet(&root).await;
    let parent = foreign_parent();
    let child = spending(&parent.id(), 9_500, lock(&root, &sender, ONE));
    let answer = internalize(
        &wallet,
        args(beef_of(&child, &parent, 100_003, true), ONE, &sender),
    )
    .await;
    let after = (rows(&wallet).await, wallet.balance().await.unwrap().total);
    println!("every ancestor whole: {answer:?}; rows and balance {after:?}");
    assert_eq!(answer, Ok(true));
    assert_eq!(after.0 .1, 1, "one output stored");
    assert_eq!(after.1, 9_500, "the balance counts the payment");
}
