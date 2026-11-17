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
    let text = answer.map(|r| r.accepted).map_err(|e| e.to_string());
    assert!(
        text.as_ref()
            .is_err_and(|e| e.starts_with("wallet error: Validation error: Invalid merkle root ")),
        "a root no chain holds is refused: {text:?}"
    );
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
    liar.add_root(common::PAYING_PROOF_HEIGHT, "aa".repeat(32));
    wallet.storage().set_chain_tracker(Arc::new(liar)).await;
    let answer = wallet.internalize_action(args, "test.local").await;
    println!("own tracker refutes: {answer:?}");
    let text = answer.map(|r| r.accepted).map_err(|e| e.to_string());
    assert!(
        text.as_ref()
            .is_err_and(|e| e.starts_with("wallet error: Validation error: Invalid merkle root ")),
        "{text:?}"
    );
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

/// The answer of a wallet built on `services` with no tracker on its
/// storage, and the outputs stored after it.
async fn internalized_on(services: bsv_wallet_toolbox_rs::Services) -> (Result<bool, String>, i64) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let (tx, _txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("internalize-root-check-tests", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
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
    let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    (answer, outputs)
}

const NO_TRACKER: &str = "wallet error: Validation error: No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker";

/// A wallet built on `Services::mainnet()` (no `chaintracks_url`) and no
/// tracker on its storage: the services have no chain tracker, so the
/// wallet door refuses before anything is stored or broadcast.
#[tokio::test]
async fn a_wallet_whose_services_have_no_tracker_refuses() {
    let (answer, outputs) =
        internalized_on(bsv_wallet_toolbox_rs::Services::mainnet().unwrap()).await;
    println!("services with no tracker: {answer:?}");
    assert_eq!(answer, Err(NO_TRACKER.to_string()));
    assert_eq!(outputs, 0);
}

/// A `bhs_url` alone gives the services no chain tracker: refused the same
/// way. Nothing is asked of the header service (the refusal comes before).
#[tokio::test]
async fn a_wallet_whose_services_have_a_bhs_url_alone_refuses() {
    let options =
        bsv_wallet_toolbox_rs::ServicesOptions::mainnet().with_bhs_url("http://127.0.0.1:9");
    let services =
        bsv_wallet_toolbox_rs::Services::with_options(bsv_wallet_toolbox_rs::Chain::Main, options)
            .unwrap();
    let (answer, outputs) = internalized_on(services).await;
    println!("services with a bhs_url alone: {answer:?}");
    assert_eq!(answer, Err(NO_TRACKER.to_string()));
    assert_eq!(outputs, 0);
}

/// A storage with a tracker installed (`StorageSqlx::set_chain_tracker`)
/// and no services admits a BEEF whose roots the tracker confirms.
#[tokio::test]
async fn a_storage_with_a_tracker_and_no_services_admits_a_confirmed_beef() {
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
    storage
        .set_chain_tracker(Arc::new(MockChainTracker::always_valid(1_000_000)))
        .await;
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
    println!("own tracker, no services: {answer:?}");
    assert!(answer.unwrap().base.accepted);
    let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs")
        .fetch_one(storage.writer())
        .await
        .unwrap();
    assert_eq!(outputs, 1);
}
