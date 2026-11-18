//! The network a local store's `internalizeAction` door judges a received
//! transaction's input scripts under: the calling wallet's `Chain`, not the
//! store's own `chain` word (which `migrate` writes as `mainnet`). A store
//! called with no wallet judges under its own word.
//!
//! The witness is a coin proven at height 700,000: after Genesis on
//! mainnet (620,538), before it on testnet (1,344,302).

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::InternalizeActionArgs;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{Chain, StorageSqlx, Wallet, WalletOptions, WalletStorageWriter};

use common::{
    accepted, anyone_lock, anyone_unlock, derived_key, internalize_at, p2pkh_bytes, payment,
};

const PREFIX: &str = "bmV0d29yaw==";
const SUFFIX: &str = "Y29pbg==";

/// After mainnet's Genesis, before testnet's.
const PROOF_HEIGHT: u32 = 700_000;

/// The BEEF offset of the payment's input (after the BUMP and the parent).
const INPUT_AT: u64 = 176;

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// A payment to the key `root` derives for `sender`, spending an
/// `anyone_lock` output of a parent proven at `PROOF_HEIGHT`: its
/// AtomicBEEF and txid.
fn paying(root: &PrivateKey, sender: &PublicKey) -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("6b".repeat(32), 0);
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
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent.id(), PROOF_HEIGHT));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(tx.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// A wallet of `root` on `chain` over a fresh local store (whose own word
/// is `mainnet`), its mock broadcaster accepting `txid`.
async fn wallet_on(chain: Chain, root: &PrivateKey, txid: &str) -> TestWallet {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("wallet-network-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build();
    Wallet::with_chain(
        Some(root.clone()),
        storage,
        services,
        WalletOptions::default(),
        chain,
    )
    .await
    .unwrap()
}

fn args(tx: Vec<u8>, sender: &PublicKey) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// The count of output, transaction and request rows.
async fn counts(wallet: &TestWallet) -> [i64; 3] {
    let mut counts = [0; 3];
    for (count, table) in counts
        .iter_mut()
        .zip(["outputs", "transactions", "proven_tx_reqs"])
    {
        *count = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap();
    }
    counts
}

/// A testnet wallet over a local store: the coin is judged under
/// testnet's heights, before Genesis there, so not judged and refused,
/// nothing written.
#[tokio::test]
async fn a_testnet_wallet_over_a_local_store_is_judged_under_testnet() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying(&root, &sender);
    let wallet = wallet_on(Chain::Test, &root, &txid).await;
    let before = counts(&wallet).await;
    let answer = internalize_at("wallet", &wallet, &root, args(tx, &sender)).await;
    let after = counts(&wallet).await;
    println!(
        "testnet wallet, a coin proven at {PROOF_HEIGHT}: {answer:?}; rows {before:?} -> {after:?}"
    );
    assert_eq!(
        answer,
        Err(format!(
            "wallet error: Validation error: spend refused at offset {INPUT_AT}, input 0 of \
             {txid}: input 0 not judged: coin height {PROOF_HEIGHT} is before Genesis height \
             1344302."
        ))
    );
    assert_eq!(after, before, "nothing written");
}

/// A mainnet wallet over a local store admits the same coin (after
/// mainnet's Genesis); so does the store's own door called with no wallet,
/// under the store's own word (`mainnet`).
#[tokio::test]
async fn a_mainnet_wallet_and_a_store_alone_judge_under_mainnet() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying(&root, &sender);
    let wallet = wallet_on(Chain::Main, &root, &txid).await;
    let answer = internalize_at("wallet", &wallet, &root, args(tx.clone(), &sender)).await;
    println!("mainnet wallet: {answer:?}");
    assert_eq!(answer, Ok(true));
    let wallet = wallet_on(Chain::Test, &root, &txid).await;
    let answer = internalize_at("storage", &wallet, &root, args(tx, &sender)).await;
    println!("the store's own door, no wallet in the call: {answer:?}");
    assert_eq!(answer, Ok(true));
}
