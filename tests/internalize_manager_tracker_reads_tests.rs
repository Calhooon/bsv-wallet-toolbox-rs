//! A pin of today's answer: a wallet on a `WalletStorageManager` whose
//! active store is a local `StorageSqlx` reads its services' chain tracker
//! twice for one internalize. The manager answers `is_storage_provider()`
//! false whatever its active store, so the wallet door checks the BEEF's
//! roots, and the active store's own door checks them again.
//!
//! A mock tracker that confirms every root; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    StorageSqlx, Wallet, WalletStorageManager, WalletStorageProvider, WalletStorageWriter,
};

use common::{accepted, paying_derived, payment};

const PREFIX: &str = "bWFuYWdlci1wcmVmaXg=";
const SUFFIX: &str = "bWFuYWdlci1zdWZmaXg=";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wallet_on_a_manager_over_a_local_store_reads_the_tracker_twice() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let identity = root.public_key().to_hex();
    let store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    store
        .migrate("manager-tracker-reads", &"3".repeat(64))
        .await
        .unwrap();
    store.make_available().await.unwrap();
    let manager = WalletStorageManager::new(identity, Some(store), None);
    manager.make_available().await.unwrap();
    assert!(!manager.is_storage_provider(), "the manager answers false");

    let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);
    let services = MockWalletServices::builder()
        .roots_valid(true)
        .post_beef_response(MockResponse::Success(vec![accepted(&txid)]))
        .build();
    // The wallet is built on a thread of its own holding the runtime's
    // handle; the runtime's workers drive the store's I/O.
    let handle = tokio::runtime::Handle::current();
    let key = root.clone();
    let wallet = std::thread::spawn(move || {
        let _entered = handle.enter();
        futures::executor::block_on(Wallet::new(Some(key), manager, services))
    })
    .join()
    .unwrap()
    .unwrap();

    let before = wallet.services().call_count("get_chain_tracker");
    let answer = wallet
        .internalize_action(
            InternalizeActionArgs {
                tx,
                outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
                description: "a payment through a manager".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    let reads = wallet.services().call_count("get_chain_tracker") - before;
    println!("manager over a local store: {answer:?}, tracker reads {reads}");
    assert_eq!(answer, Ok(true));
    assert_eq!(reads, 2, "the wallet door and the active store's door");
}
