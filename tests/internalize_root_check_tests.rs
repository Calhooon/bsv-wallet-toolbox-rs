//! `internalizeAction` verifies an AtomicBEEF's merkle roots whatever the
//! host installed.
//!
//! The storage door verifies with the storage's own chain tracker when the
//! host installed one, else with the services' chain tracker (the one
//! `Wallet::new` hands the storage); with neither the request is refused
//! before anything is stored.
//!
//! Real SQLite storage, a mock broadcaster and mock trackers; nothing
//! touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::MockChainTracker;
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

use common::{accepted, paying_derived, payment, stored_outputs};

const PREFIX: &str = "cm9vdC1wcmVmaXg=";
const SUFFIX: &str = "cm9vdC1zdWZmaXg=";

type W = Wallet<StorageSqlx, MockWalletServices>;

/// A wallet built as documented (`Wallet::new`), whose services' chain
/// tracker answers `roots_valid` for every root, and an AtomicBEEF paying
/// it whose parent's BUMP no chain holds.
async fn setup(roots_valid: bool) -> (W, InternalizeActionArgs) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("internalize-root-check-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let services = MockWalletServices::builder()
        .roots_valid(roots_valid)
        .post_beef_response(MockResponse::Success(vec![accepted(&txid)]))
        .build();
    let wallet = Wallet::new(Some(root), storage, services).await.unwrap();
    let args = InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment with roots to check".to_string(),
        labels: None,
        seek_permission: None,
    };
    (wallet, args)
}

async fn stored_transactions(wallet: &W) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_bump_the_services_tracker_refutes_is_refused_and_nothing_is_stored() {
    let (wallet, args) = setup(false).await;
    let answer = wallet.internalize_action(args, "test.local").await;
    println!("refuted root, default Wallet::new: {answer:?}");
    assert!(answer.is_err(), "a root no chain holds is refused");
    assert_eq!(stored_outputs(&wallet).await, 0);
    assert_eq!(stored_transactions(&wallet).await, 0);
    assert_eq!(wallet.balance().await.unwrap().total, 0);
}

#[tokio::test]
async fn a_bump_the_services_tracker_confirms_is_admitted() {
    let (wallet, args) = setup(true).await;
    let answer = wallet.internalize_action(args, "test.local").await;
    assert!(answer.unwrap().accepted);
    assert_eq!(wallet.balance().await.unwrap().total, 9_500);
}

#[tokio::test]
async fn the_storages_own_tracker_is_asked_before_the_services() {
    // Own tracker confirms, services refute: admitted, as before.
    let (wallet, args) = setup(false).await;
    wallet
        .storage()
        .set_chain_tracker(Arc::new(MockChainTracker::always_valid(1_000_000)))
        .await;
    let answer = wallet.internalize_action(args, "test.local").await;
    assert!(answer.unwrap().accepted);

    // Own tracker refutes, services confirm: refused, as before.
    let (wallet, args) = setup(true).await;
    let mut liar = MockChainTracker::new(1_000_000);
    liar.add_root(100_003, "aa".repeat(32));
    wallet.storage().set_chain_tracker(Arc::new(liar)).await;
    let answer = wallet.internalize_action(args, "test.local").await;
    println!("own tracker refutes: {answer:?}");
    assert!(answer.is_err());
    assert_eq!(stored_outputs(&wallet).await, 0);
}

#[tokio::test]
async fn with_no_tracker_anywhere_the_storage_door_refuses() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, _txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let identity = root.public_key().to_hex();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("internalize-root-check-tests", &identity)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let auth = bsv_wallet_toolbox_rs::AuthId::with_user_id(&identity, user.user_id);
    let args = InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment with roots to check".to_string(),
        labels: None,
        seek_permission: None,
    };
    let answer = storage.internalize_action(&auth, args).await;
    let text = match answer {
        Err(e) => e.to_string(),
        Ok(_) => panic!("admitted with no tracker anywhere"),
    };
    println!("no tracker anywhere: {text}");
    assert!(text.contains("No chain tracker is available to verify the BEEF"));
    let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(storage.writer())
        .await
        .unwrap();
    let transactions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.writer())
        .await
        .unwrap();
    assert_eq!((outputs, transactions), (0, 0));
}

/// A wallet built on `Services` with no header service (`chaintracks_url`
/// unset) and no tracker on its storage: the services have no chain tracker,
/// so the wallet door refuses before anything is stored or broadcast.
#[tokio::test]
async fn a_wallet_whose_services_have_no_tracker_refuses() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, _txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("internalize-root-check-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let services = bsv_wallet_toolbox_rs::Services::mainnet().unwrap();
    let wallet = Wallet::new(Some(root), storage, services).await.unwrap();
    let args = InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment with roots to check".to_string(),
        labels: None,
        seek_permission: None,
    };
    let answer = wallet
        .internalize_action(args, "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    println!("services with no tracker: {answer:?}");
    assert_eq!(
        answer,
        Err("wallet error: Validation error: No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker".to_string())
    );
    let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    assert_eq!(outputs, 0);
}
