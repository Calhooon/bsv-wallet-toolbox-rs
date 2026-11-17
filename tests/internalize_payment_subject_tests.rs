//! `Wallet::internalize_action`'s BRC-29 derivation check reads the BEEF's
//! subject without linking its ancestry.
//!
//! A payment output is admitted only when the key derived from its
//! remittance is the key its P2PKH script pays. The check reads the subject
//! transaction's own outputs (`Transaction::from_beef_subject`), so it runs
//! whatever the ancestry's size or shape: a sweep of one parent past bsv-rs
//! 0.8.11's default link budget, or a parent carried as a txid-only entry.
//! A BEEF with a payment output and no readable subject is refused before
//! anything is stored.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::{hash160, PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, InternalizeActionArgs, InternalizeOutput, KeyDeriverApi, ProtoWallet, Protocol,
    SecurityLevel, WalletInterface, WalletPayment,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

/// Outputs of the one parent the sweep spends: past the 702 P2PKH outputs
/// bsv-rs 0.8.11 states as the default link budget's single-sweep reach.
const SWEPT: u32 = 800;
const PREFIX: &str = "c3dlZXAtcHJlZml4";
const SUFFIX: &str = "c3dlZXAtc3VmZml4";

fn p2pkh(pubkey: &PublicKey) -> LockingScript {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(&pubkey.to_compressed()));
    script.extend([0x88, 0xac]);
    LockingScript::from_binary(&script).unwrap()
}

fn other_lock() -> LockingScript {
    LockingScript::from_hex("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap()
}

/// The key the recipient `root` derives for the remittance from `sender`.
fn derived_lock(root: &PrivateKey, sender: &PublicKey) -> LockingScript {
    let key = ProtoWallet::new(Some(root.clone()))
        .key_deriver()
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &format!("{PREFIX} {SUFFIX}"),
            &Counterparty::Other(sender.clone()),
            true,
        )
        .unwrap();
    p2pkh(&key)
}

fn input(txid: &str, vout: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid.to_string(), vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    input
}

/// A proven parent of `outputs` P2PKH outputs.
fn proven_parent(outputs: u32) -> Transaction {
    let mut parent = Transaction::new();
    parent.inputs.push(input(&"11".repeat(32), 0));
    for _ in 0..outputs {
        parent.outputs.push(TransactionOutput {
            satoshis: Some(1_000),
            locking_script: other_lock(),
            change: false,
        });
    }
    parent
}

/// A child spending outputs 0 to `spent - 1` of `parent_txid` into one
/// output locked by `lock`.
fn child(parent_txid: &str, spent: u32, lock: LockingScript) -> Transaction {
    let mut child = Transaction::new();
    child.version = 1;
    for vout in 0..spent {
        child.inputs.push(input(parent_txid, vout));
    }
    child.outputs.push(TransactionOutput {
        satoshis: Some(u64::from(spent) * 1_000 - 500),
        locking_script: lock,
        change: false,
    });
    child
}

/// An AtomicBEEF of a proven parent of `SWEPT` outputs and a child sweeping
/// every one of them into one output locked by `lock`.
fn sweep_beef(lock: LockingScript) -> (Vec<u8>, String) {
    let parent = proven_parent(SWEPT);
    let parent_txid = parent.id();
    let child = child(&parent_txid, SWEPT, lock);
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_001));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

/// An AtomicBEEF whose subject's one parent is a txid-only entry, proven by
/// a BUMP the BEEF carries.
fn txid_only_parent_beef(lock: LockingScript) -> (Vec<u8>, String) {
    let parent_txid = proven_parent(1).id();
    let child = child(&parent_txid, 1, lock);
    let txid = child.id();
    let mut beef = Beef::new();
    beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_002));
    beef.merge_txid_only(parent_txid);
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary_atomic(&txid).unwrap(), txid)
}

fn accepted(txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: "arc".to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "success".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: None,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

async fn wallet(root: &PrivateKey, txid: &str) -> Wallet<StorageSqlx, MockWalletServices> {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    let identity = root.public_key().to_hex();
    storage
        .migrate("internalize-payment-subject-tests", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build();
    Wallet::new(Some(root.clone()), storage, services)
        .await
        .expect("wallet")
}

fn payment_args(tx: Vec<u8>, sender: &PublicKey) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "wallet payment".to_string(),
            payment_remittance: Some(WalletPayment {
                derivation_prefix: PREFIX.to_string(),
                derivation_suffix: SUFFIX.to_string(),
                sender_identity_key: sender.to_hex(),
            }),
            insertion_remittance: None,
        }],
        description: "a payment to check".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn stored_outputs(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// Internalizes a payment built by `beef` and returns the answer and the
/// number of outputs stored after it.
async fn internalize(
    lock_derives: bool,
    beef: fn(LockingScript) -> (Vec<u8>, String),
) -> (bsv_rs::Result<bool>, i64) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let lock = if lock_derives {
        derived_lock(&root, &sender)
    } else {
        other_lock()
    };
    let (bytes, txid) = beef(lock);
    let wallet = wallet(&root, &txid).await;
    let answer = wallet
        .internalize_action(payment_args(bytes, &sender), "test.local")
        .await
        .map(|r| r.accepted);
    let stored = stored_outputs(&wallet).await;
    (answer, stored)
}

fn assert_derivation_mismatch(answer: bsv_rs::Result<bool>, stored: i64) {
    match answer {
        Err(bsv_rs::Error::WalletError(text)) => assert!(
            text.starts_with("Derivation mismatch for output 0: derived "),
            "{text}"
        ),
        other => panic!("not refused for the derivation: {other:?}"),
    }
    assert_eq!(stored, 0, "nothing is stored");
}

/// The sweep is past the linking door's budget, and the payment's key does
/// not derive from its remittance: refused for the derivation.
#[tokio::test]
async fn a_sweep_past_the_link_budget_with_a_payment_key_that_does_not_derive_is_refused() {
    let (beef, txid) = sweep_beef(other_lock());
    assert!(matches!(
        Transaction::from_beef(&beef, Some(&txid)),
        Err(bsv_rs::Error::BeefResourceLimit(_))
    ));
    let (answer, stored) = internalize(false, sweep_beef).await;
    assert_derivation_mismatch(answer, stored);
}

/// The same sweep paying the derived key: admitted, its output stored.
#[tokio::test]
async fn a_sweep_past_the_link_budget_with_a_payment_key_that_derives_is_stored() {
    let (answer, stored) = internalize(true, sweep_beef).await;
    assert!(answer.expect("the payment is internalized"));
    assert_eq!(stored, 1, "the payment output is stored");
}

/// A subject whose parent is a txid-only entry, paying a key that does not
/// derive: refused for the derivation.
#[tokio::test]
async fn a_txid_only_parent_with_a_payment_key_that_does_not_derive_is_refused() {
    let (beef, _) = txid_only_parent_beef(other_lock());
    assert!(
        Transaction::from_beef(&beef, None).is_ok(),
        "the shape links"
    );
    let (answer, stored) = internalize(false, txid_only_parent_beef).await;
    assert_derivation_mismatch(answer, stored);
}

/// The same shape paying the derived key: admitted, its output stored.
#[tokio::test]
async fn a_txid_only_parent_with_a_payment_key_that_derives_is_stored() {
    let (answer, stored) = internalize(true, txid_only_parent_beef).await;
    assert!(answer.expect("the payment is internalized"));
    assert_eq!(stored, 1, "the payment output is stored");
}

/// Bytes with no readable subject and a payment output: refused before
/// anything is stored, saying the subject cannot be read.
#[tokio::test]
async fn a_payment_in_bytes_with_no_readable_subject_is_refused_before_storage() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (good, _) = txid_only_parent_beef(derived_lock(&root, &sender));
    // An AtomicBEEF naming a subject it does not carry.
    let mut absent = good.clone();
    absent[4..36].copy_from_slice(&[0x5a; 32]);
    for bytes in [vec![0xde, 0xad, 0xbe, 0xef], absent] {
        let wallet = wallet(&root, &"00".repeat(32)).await;
        let answer = wallet
            .internalize_action(payment_args(bytes, &sender), "test.local")
            .await;
        match answer {
            Err(bsv_rs::Error::WalletError(text)) => assert!(
                text.starts_with(
                    "Cannot read the BEEF's subject transaction to check payment outputs: "
                ),
                "{text}"
            ),
            other => panic!(
                "not refused for the subject: {:?}",
                other.map(|r| r.accepted)
            ),
        }
        assert_eq!(stored_outputs(&wallet).await, 0, "nothing is stored");
    }
}
