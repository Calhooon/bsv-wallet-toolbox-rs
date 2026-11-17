//! One received-BEEF check, two doors: each refusal answers the same text
//! through a wallet on a local store (the store's door checks) and through
//! a wallet on a storage that does not say its own door checks in the
//! wallet's process (the wallet's door checks, storage never called):
//! malformed bytes, an invalid structure, an entry carried by txid alone,
//! a refuted root, a tracker fault, no tracker; and, with no tracker,
//! malformed bytes and an invalid structure answer their own texts at both
//! doors (the tracker is resolved after the structure, at both).
//!
//! Mock services, or services with no tracker; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{Services, StorageSqlx, Wallet, WalletServices, WalletStorageWriter};

use common::forwarding::Forwarding;
use common::{accepted, beef_paying_with, derived_key, p2pkh_bytes, payment};

const PREFIX: &str = "dGV4dHMtcHJlZml4";
const SUFFIX: &str = "dGV4dHMtc3VmZml4";
const REFUSAL: &str = "wallet error: Validation error: ";
const WALLET_ERROR: &str = "wallet error: ";

fn lock(root: &PrivateKey, sender: &PublicKey) -> LockingScript {
    LockingScript::from_binary(&p2pkh_bytes(&derived_key(root, sender, PREFIX, SUFFIX))).unwrap()
}

/// The BEEFs of the cases, for the wallet of `root` paid by `sender`.
fn whole(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    beef_paying_with(lock(root, sender), true).0
}

fn by_txid_alone(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    beef_paying_with(lock(root, sender), false).0
}

/// A whole BEEF cut by its last byte.
fn malformed(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    let mut bytes = whole(root, sender);
    bytes.pop();
    bytes
}

/// A child whose parent the BEEF does not carry and no BUMP proves: the
/// bytes read clean, the structure does not.
fn missing_parent(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    let mut child = Transaction::new();
    let mut input = TransactionInput::new("44".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock(root, sender),
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary_atomic(&txid).unwrap()
}

/// A parent carried by txid alone that no BUMP of the BEEF proves (the BUMP
/// proves another transaction).
fn by_txid_alone_unproven(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    by_txid_alone_beside(root, sender, true)
}

/// A parent carried by txid alone in a BEEF that carries no BUMP.
fn by_txid_alone_no_bump(root: &PrivateKey, sender: &PublicKey) -> Vec<u8> {
    by_txid_alone_beside(root, sender, false)
}

fn by_txid_alone_beside(root: &PrivateKey, sender: &PublicKey, other_bump: bool) -> Vec<u8> {
    let parent_txid = "55".repeat(32);
    let mut child = Transaction::new();
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock(root, sender),
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    if other_bump {
        beef.merge_bump(MerklePath::from_coinbase_txid(&"66".repeat(32), 100_003));
    }
    beef.merge_txid_only(parent_txid);
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary_atomic(&txid).unwrap()
}

/// A wallet payment of output 0; or, when `insert`, a basket insertion of
/// it (no payment: the wallet reads no subject before the check).
fn args(tx: Vec<u8>, sender: &PublicKey, insert: bool) -> InternalizeActionArgs {
    let output = if insert {
        InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "received".to_string(),
                custom_instructions: None,
                tags: None,
            }),
        }
    } else {
        payment(0, PREFIX, SUFFIX, &sender.to_hex())
    };
    InternalizeActionArgs {
        tx,
        outputs: vec![output],
        description: "a payment read at both doors".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn store(root: &PrivateKey) -> StorageSqlx {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("received-beef-texts", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    storage
}

/// The answer of one internalize of the case `beef` through a wallet on a
/// local store and through a wallet on a forwarding storage, each with
/// services from `services`; and the forwarding storage's internalize calls.
async fn at_both_doors<V, F>(
    beef: fn(&PrivateKey, &PublicKey) -> Vec<u8>,
    services: F,
) -> (Result<bool, String>, Result<bool, String>, usize)
where
    V: WalletServices + Send + Sync + 'static,
    F: Fn() -> V,
{
    at_both_doors_as(beef, services, false).await
}

async fn at_both_doors_as<V, F>(
    beef: fn(&PrivateKey, &PublicKey) -> Vec<u8>,
    services: F,
    insert: bool,
) -> (Result<bool, String>, Result<bool, String>, usize)
where
    V: WalletServices + Send + Sync + 'static,
    F: Fn() -> V,
{
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let local = Wallet::new(Some(root.clone()), store(&root).await, services())
        .await
        .unwrap();
    let at_store = local
        .internalize_action(args(beef(&root, &sender), &sender, insert), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());

    let internalized = Arc::new(AtomicUsize::new(0));
    let forwarding = Forwarding {
        inner: store(&root).await,
        internalized: internalized.clone(),
    };
    let wallet = Wallet::new(Some(root.clone()), forwarding, services())
        .await
        .unwrap();
    let at_wallet = wallet
        .internalize_action(args(beef(&root, &sender), &sender, insert), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    (at_store, at_wallet, internalized.load(Ordering::SeqCst))
}

fn mock(
    configure: fn(
        bsv_wallet_toolbox_rs::services::mock::MockWalletServicesBuilder,
    ) -> bsv_wallet_toolbox_rs::services::mock::MockWalletServicesBuilder,
) -> impl Fn() -> MockWalletServices {
    move || {
        configure(MockWalletServices::builder())
            .post_beef_response(MockResponse::Success(vec![accepted(&"00".repeat(32))]))
            .build()
    }
}

fn no_tracker() -> Services {
    Services::mainnet().unwrap()
}

/// Asserts one text at both doors, starting `REFUSAL` then `starts`, the
/// forwarding storage never called.
fn same_refusal(
    case: &str,
    answers: (Result<bool, String>, Result<bool, String>, usize),
    starts: &str,
) {
    same_refusal_after(case, answers, &format!("{REFUSAL}{starts}"))
}

/// Asserts one text at both doors, starting `starts`, the forwarding
/// storage never called.
fn same_refusal_after(
    case: &str,
    answers: (Result<bool, String>, Result<bool, String>, usize),
    starts: &str,
) {
    let (at_store, at_wallet, calls) = answers;
    println!("{case}: local store {at_store:?}; wallet door {at_wallet:?}; storage calls {calls}");
    assert_eq!(at_store, at_wallet, "{case}: one text at both doors");
    assert!(
        at_wallet.as_ref().is_err_and(|e| e.starts_with(starts)),
        "{case}: {at_wallet:?}"
    );
    assert_eq!(calls, 0, "{case}: storage is never called");
}

/// Malformed bytes: a basket insertion meets the streaming reader's
/// refusal at both doors; a wallet payment meets the wallet's read of its
/// subject before the check, at both doors, as at 0.13.4.
#[tokio::test]
async fn malformed_bytes_answer_one_text_at_both_doors() {
    for (case, answers) in [
        (
            "malformed bytes, insertion",
            at_both_doors_as(malformed, mock(|b| b), true).await,
        ),
        (
            "malformed bytes, insertion, no tracker",
            at_both_doors_as(malformed, no_tracker, true).await,
        ),
    ] {
        same_refusal_after(
            case,
            answers,
            &format!("{WALLET_ERROR}Invalid BEEF at byte "),
        );
    }
    let answers = at_both_doors(malformed, mock(|b| b)).await;
    same_refusal_after(
        "malformed bytes, payment",
        answers,
        &format!(
            "{WALLET_ERROR}Cannot read the BEEF's subject transaction to check payment outputs: "
        ),
    );
}

#[tokio::test]
async fn an_invalid_structure_answers_one_text_at_both_doors() {
    let answers = at_both_doors(missing_parent, mock(|b| b)).await;
    same_refusal("invalid structure", answers, "BEEF structure is invalid");
    let answers = at_both_doors(missing_parent, no_tracker).await;
    same_refusal(
        "invalid structure, no tracker",
        answers,
        "BEEF structure is invalid",
    );
}

#[tokio::test]
async fn an_entry_carried_by_txid_alone_answers_one_text_at_both_doors() {
    let answers = at_both_doors(by_txid_alone, mock(|b| b)).await;
    same_refusal(
        "an entry by txid alone a BUMP proves",
        answers,
        "The tx parameter must be valid AtomicBEEF: transaction ",
    );
    // One no BUMP proves is refused earlier, as an invalid BEEF.
    let answers = at_both_doors(by_txid_alone_unproven, mock(|b| b)).await;
    same_refusal(
        "an entry by txid alone no BUMP proves, beside a BUMP of another",
        answers,
        "BEEF structure is invalid",
    );
    let answers = at_both_doors(by_txid_alone_no_bump, mock(|b| b)).await;
    same_refusal("an entry by txid alone in a BEEF with no BUMP", answers, "");
}

#[tokio::test]
async fn a_refuted_root_answers_one_text_at_both_doors() {
    let answers = at_both_doors(whole, mock(|b| b.roots_valid(false))).await;
    same_refusal("refuted root", answers, "Invalid merkle root ");
}

#[tokio::test]
async fn a_tracker_fault_answers_one_text_at_both_doors() {
    let answers = at_both_doors(whole, mock(|b| b.root_fault("the header service is down"))).await;
    let text = format!(
        "ChainTracker verification failed at height {}: network error: the header service is down",
        common::PAYING_PROOF_HEIGHT
    );
    same_refusal("tracker fault", answers, &text);
}

#[tokio::test]
async fn no_tracker_answers_one_text_at_both_doors() {
    let answers = at_both_doors(whole, no_tracker).await;
    same_refusal(
        "no tracker",
        answers,
        "No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker",
    );
}

#[tokio::test]
async fn a_whole_beef_with_confirmed_roots_is_admitted_at_both_doors() {
    let (at_store, at_wallet, calls) = at_both_doors(whole, mock(|b| b)).await;
    println!(
        "confirmed: local store {at_store:?}; wallet door {at_wallet:?}; storage calls {calls}"
    );
    assert_eq!((at_store, at_wallet, calls), (Ok(true), Ok(true), 1));
}
