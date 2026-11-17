//! The wallet door's own check of a received AtomicBEEF's merkle roots.
//!
//! A wallet whose storage does not say its own door checks a received BEEF
//! in the wallet's process (`checks_received_beef_in_process`, false by
//! default: a storage client, a host's own storage type, a manager whose
//! active store is one) verifies the BEEF's roots with its own services'
//! chain tracker before it calls storage, and refuses with the storage
//! door's texts; with no tracker on its services it refuses with the
//! storage door's no-tracker sentence, storage never called. A wallet on a
//! local store leaves the check to the store's door: the tracker is read
//! the same number of times as before.
//!
//! The storage here is a host's own type behind the wallet's storage
//! traits that forwards and overrides no provided method: it answers every
//! internalize accepted without checking anything and counts the calls;
//! every other method is a local in-memory store's. Nothing touches the
//! network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletServices};

use common::forwarding::Forwarding;
use common::{accepted, beef_paying_with, derived_key, p2pkh_bytes, payment, wallet_on_manager};

const PREFIX: &str = "d2FsbGV0LWRvb3I=";
const SUFFIX: &str = "cm9vdHM=";

const NO_TRACKER: &str = "wallet error: Validation error: No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker";

/// An AtomicBEEF paying the key `root` derives for `sender`, whose parent
/// is carried whole with a BUMP at `common::PAYING_PROOF_HEIGHT`.
fn paying_with_whole_parent(root: &PrivateKey, sender: &bsv_rs::primitives::PublicKey) -> Vec<u8> {
    beef_paying_with(lock(root, sender), true).0
}

/// The same payment with its parent carried by txid alone, beside the
/// parent's BUMP.
fn paying_with_parent_by_txid_alone(
    root: &PrivateKey,
    sender: &bsv_rs::primitives::PublicKey,
) -> Vec<u8> {
    beef_paying_with(lock(root, sender), false).0
}

fn lock(root: &PrivateKey, sender: &bsv_rs::primitives::PublicKey) -> LockingScript {
    LockingScript::from_binary(&p2pkh_bytes(&derived_key(root, sender, PREFIX, SUFFIX))).unwrap()
}

fn args_for(tx: Vec<u8>, sender: &str) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, sender)],
        description: "a payment with roots to check".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn local_store(root: &PrivateKey) -> StorageSqlx {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("wallet-root-check-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    storage
}

/// A wallet on a forwarding storage, with `services`: the answer
/// to one internalize of `tx` and the number of internalize calls the
/// storage received.
async fn on_forwarding<V: WalletServices + Send + Sync + 'static>(
    services: V,
    whole_parent: bool,
) -> (std::result::Result<bool, String>, usize) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let tx = if whole_parent {
        paying_with_whole_parent(&root, &sender)
    } else {
        paying_with_parent_by_txid_alone(&root, &sender)
    };
    let internalized = Arc::new(AtomicUsize::new(0));
    let storage = Forwarding {
        inner: local_store(&root).await,
        internalized: internalized.clone(),
    };
    let wallet = Wallet::new(Some(root), storage, services).await.unwrap();
    let answer = wallet
        .internalize_action(args_for(tx, &sender.to_hex()), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    (answer, internalized.load(Ordering::SeqCst))
}

fn mock_services(roots_valid: bool) -> MockWalletServices {
    MockWalletServices::builder()
        .roots_valid(roots_valid)
        .post_beef_response(MockResponse::Success(vec![accepted(&"00".repeat(32))]))
        .build()
}

#[tokio::test]
async fn a_forwarding_storage_with_a_refuting_tracker_is_refused_at_the_wallet_door() {
    let (answer, calls) = on_forwarding(mock_services(false), true).await;
    println!("forwarding, refuting tracker: {answer:?}, storage internalize calls {calls}");
    assert!(
        answer.as_ref().is_err_and(|e| e
            .starts_with("wallet error: Validation error: Invalid merkle root ")
            && e.ends_with(&format!(" at height {}", common::PAYING_PROOF_HEIGHT))),
        "{answer:?}"
    );
    assert_eq!(calls, 0, "storage is never called");
}

#[tokio::test]
async fn a_forwarding_storage_with_a_confirming_tracker_is_called_once() {
    let (answer, calls) = on_forwarding(mock_services(true), true).await;
    println!("forwarding, confirming tracker: {answer:?}, storage internalize calls {calls}");
    assert_eq!(answer, Ok(true), "the storage's answer");
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn a_forwarding_storage_with_no_tracker_on_the_services_is_refused() {
    let services = bsv_wallet_toolbox_rs::Services::mainnet().unwrap();
    let (answer, calls) = on_forwarding(services, true).await;
    println!("forwarding, services with no tracker: {answer:?}, storage internalize calls {calls}");
    assert_eq!(answer, Err(NO_TRACKER.to_string()));
    assert_eq!(calls, 0, "storage is never called");
}

#[tokio::test]
async fn a_forwarding_storage_refuses_an_entry_carried_by_txid_alone() {
    let (answer, calls) = on_forwarding(mock_services(true), false).await;
    println!("forwarding, txid-only parent: {answer:?}, storage internalize calls {calls}");
    assert!(
        answer.as_ref().is_err_and(|e| e.starts_with(
            "wallet error: Validation error: The tx parameter must be valid AtomicBEEF: transaction "
        ) && e.ends_with(" is carried by txid alone")),
        "{answer:?}"
    );
    assert_eq!(calls, 0, "storage is never called");
}

/// A wallet on a `WalletStorageManager` whose active store is the
/// forwarding storage: the manager answers for its active store, so the
/// wallet checks the roots; refuted, the storage is never called;
/// confirmed, it is called once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manager_over_a_forwarding_storage_checks_at_the_wallet_door() {
    for roots_valid in [false, true] {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let tx = paying_with_whole_parent(&root, &sender);
        let internalized = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(Forwarding {
            inner: local_store(&root).await,
            internalized: internalized.clone(),
        });
        let manager = bsv_wallet_toolbox_rs::WalletStorageManager::new(
            root.public_key().to_hex(),
            Some(storage),
            None,
        );
        manager.make_available().await.unwrap();
        assert!(!manager.checks_received_beef_in_process().await);
        let wallet = wallet_on_manager(&root, manager, mock_services(roots_valid));
        let answer = wallet
            .internalize_action(args_for(tx, &sender.to_hex()), "test.local")
            .await
            .map(|r| r.accepted)
            .map_err(|e| e.to_string());
        let calls = internalized.load(Ordering::SeqCst);
        println!(
            "manager over a forwarding storage, roots_valid {roots_valid}: {answer:?}, storage internalize calls {calls}"
        );
        if roots_valid {
            assert_eq!((answer, calls), (Ok(true), 1));
        } else {
            assert!(
                answer.as_ref().is_err_and(
                    |e| e.starts_with("wallet error: Validation error: Invalid merkle root ")
                ),
                "{answer:?}"
            );
            assert_eq!(calls, 0, "storage is never called");
        }
    }
}

/// A wallet on its own local store: one internalize reads the services'
/// chain tracker once (at the storage door), as at 0.13.4, refuted or
/// confirmed, whole parent or txid-only parent. The storage door admits
/// only a BEEF whose every transaction is whole and whose roots the tracker
/// confirms.
#[tokio::test]
async fn a_local_store_reads_the_tracker_as_before() {
    for (roots_valid, whole_parent) in [(true, true), (false, true), (true, false), (false, false)]
    {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let tx = if whole_parent {
            paying_with_whole_parent(&root, &sender)
        } else {
            paying_with_parent_by_txid_alone(&root, &sender)
        };
        let storage = local_store(&root).await;
        assert!(storage.checks_received_beef_in_process().await);
        let wallet = Wallet::new(Some(root), storage, mock_services(roots_valid))
            .await
            .unwrap();
        let before = wallet.services().call_count("get_chain_tracker");
        let answer = wallet
            .internalize_action(args_for(tx, &sender.to_hex()), "test.local")
            .await
            .map(|r| r.accepted)
            .map_err(|e| e.to_string());
        let reads = wallet.services().call_count("get_chain_tracker") - before;
        println!(
            "local store, roots_valid {roots_valid}, whole parent {whole_parent}: {answer:?}, tracker reads {reads}"
        );
        assert_eq!(reads, 1, "one read of the tracker per internalize");
        assert_eq!(answer.is_ok(), roots_valid && whole_parent, "{answer:?}");
        if !whole_parent {
            assert!(
                answer
                    .as_ref()
                    .is_err_and(|e| e.ends_with(" is carried by txid alone")),
                "{answer:?}"
            );
        }
    }
}
