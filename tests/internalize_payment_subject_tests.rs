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

mod common;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, MerklePath, Transaction};
use bsv_rs::wallet::{InternalizeActionArgs, InternalizeOutput, WalletInterface, WalletPayment};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};

use common::{child, other_lock, p2pkh_bytes, proven_parent, stored_outputs, sweep_beef};

const PREFIX: &str = "c3dlZXAtcHJlZml4";
const SUFFIX: &str = "c3dlZXAtc3VmZml4";

/// The P2PKH of the key the recipient `root` derives for the remittance
/// from `sender`.
fn derived_lock(root: &PrivateKey, sender: &PublicKey) -> LockingScript {
    let key = common::derived_key(root, sender, PREFIX, SUFFIX);
    LockingScript::from_binary(&p2pkh_bytes(&key)).unwrap()
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

async fn wallet(root: &PrivateKey, txid: &str) -> Wallet<StorageSqlx, MockWalletServices> {
    common::wallet("internalize-payment-subject-tests", root, txid).await
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
/// anything is stored, saying the subject cannot be read. The whole text a
/// host reads is pinned for each input: bytes that are not a BEEF, and an
/// AtomicBEEF naming a subject it does not carry.
#[tokio::test]
async fn a_payment_in_bytes_with_no_readable_subject_is_refused_before_storage() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (good, _) = txid_only_parent_beef(derived_lock(&root, &sender));
    // An AtomicBEEF naming a subject it does not carry.
    let mut absent = good.clone();
    absent[4..36].copy_from_slice(&[0x5a; 32]);
    let cases = [
        (
            vec![0xde, 0xad, 0xbe, 0xef],
            "Cannot read the BEEF's subject transaction to check payment outputs: \
             BEEF error: Invalid BEEF version: 0xEFBEADDE"
                .to_string(),
        ),
        (
            absent,
            format!(
                "Cannot read the BEEF's subject transaction to check payment outputs: \
                 transaction error: Transaction {} not found in BEEF",
                "5a".repeat(32)
            ),
        ),
    ];
    for (bytes, expected) in cases {
        let wallet = wallet(&root, &"00".repeat(32)).await;
        let answer = wallet
            .internalize_action(payment_args(bytes, &sender), "test.local")
            .await;
        match answer {
            Err(e @ bsv_rs::Error::WalletError(_)) => {
                assert_eq!(e.to_string(), format!("wallet error: {expected}"))
            }
            other => panic!(
                "not refused for the subject: {:?}",
                other.map(|r| r.accepted)
            ),
        }
        assert_eq!(stored_outputs(&wallet).await, 0, "nothing is stored");
    }
}
