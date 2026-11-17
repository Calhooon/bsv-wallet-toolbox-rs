//! A wallet on a `WalletStorageManager` whose active store is a local
//! `StorageSqlx`: the active store says its own door checks a received
//! BEEF in the wallet's process (`checks_received_beef_in_process`), so the
//! manager says so too and the wallet leaves the check to that door. One
//! internalize reads the tracker once and checks each root once, and
//! answers as a wallet directly on the store answers, with the tracker on
//! the services or with a tracker installed on the store alone.
//!
//! Mock trackers; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use bsv_rs::wallet::{InternalizeActionArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    Services, StorageSqlx, Wallet, WalletServices, WalletStorageManager, WalletStorageWriter,
};

use common::{accepted, paying_derived, payment, wallet_on_manager};

const PREFIX: &str = "bWFuYWdlci1wcmVmaXg=";
const SUFFIX: &str = "bWFuYWdlci1zdWZmaXg=";

/// A tracker installed on the store: answers `valid` for every root and
/// counts the roots it is asked.
struct CountingTracker {
    valid: bool,
    checks: Arc<AtomicUsize>,
}

#[async_trait]
impl ChainTracker for CountingTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(self.valid)
    }
    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(100_010)
    }
}

async fn local_store(name: &str) -> Arc<StorageSqlx> {
    let store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    store.migrate(name, &"3".repeat(64)).await.unwrap();
    store.make_available().await.unwrap();
    store
}

fn mock_services(roots_valid: bool, txid: &str) -> MockWalletServices {
    MockWalletServices::builder()
        .roots_valid(roots_valid)
        .post_beef_response(MockResponse::Success(vec![accepted(txid)]))
        .build()
}

fn args(tx: Vec<u8>, sender: &str) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, sender)],
        description: "a payment through a manager".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn internalize<S, V>(
    wallet: &Wallet<S, V>,
    tx: Vec<u8>,
    sender: &str,
) -> std::result::Result<bool, String>
where
    S: bsv_wallet_toolbox_rs::WalletStorageProvider + Send + Sync + 'static,
    V: WalletServices + Send + Sync + 'static,
{
    wallet
        .internalize_action(args(tx, sender), "test.local")
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string())
}

/// One internalize on a manager over a local store, roots confirmed or
/// refuted by the services' tracker: the answer, the tracker reads and the
/// root checks; and the answer of a wallet directly on a local store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wallet_on_a_manager_over_a_local_store_reads_the_tracker_once() {
    for roots_valid in [true, false] {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let (tx, txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);

        let manager = WalletStorageManager::new(
            root.public_key().to_hex(),
            Some(local_store("manager-tracker-reads").await),
            None,
        );
        manager.make_available().await.unwrap();
        let wallet = wallet_on_manager(&root, manager, mock_services(roots_valid, &txid));
        let before = wallet.services().call_count("get_chain_tracker");
        let roots_before = wallet.services().root_check_count();
        let answer = internalize(&wallet, tx.clone(), &sender.to_hex()).await;
        let reads = wallet.services().call_count("get_chain_tracker") - before;
        let checks = wallet.services().root_check_count() - roots_before;

        let direct = Wallet::new(
            Some(root.clone()),
            StorageSqlx::clone(&*local_store("direct-tracker-reads").await),
            mock_services(roots_valid, &txid),
        )
        .await
        .unwrap();
        let direct_answer = internalize(&direct, tx, &sender.to_hex()).await;
        println!(
            "manager over a local store, roots_valid {roots_valid}: {answer:?}, tracker reads {reads}, root checks {checks}; directly on the store: {direct_answer:?}"
        );
        assert_eq!(reads, 1, "the active store's door alone reads the tracker");
        assert_eq!(checks, 1, "one root, checked once");
        assert_eq!(answer, direct_answer);
        assert_eq!(answer.is_ok(), roots_valid, "{answer:?}");
    }
}

/// A tracker installed on the store and none on the services: a wallet on a
/// manager over that store answers as a wallet directly on it (the store's
/// door reads the store's tracker), confirmed or refuted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wallet_on_a_manager_with_the_tracker_on_the_store_alone() {
    for valid in [true, false] {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let (tx, _txid) = paying_derived(&root, &sender, PREFIX, SUFFIX);

        let manager_checks = Arc::new(AtomicUsize::new(0));
        let store = local_store("manager-store-tracker").await;
        store
            .set_chain_tracker(Arc::new(CountingTracker {
                valid,
                checks: manager_checks.clone(),
            }))
            .await;
        let manager = WalletStorageManager::new(root.public_key().to_hex(), Some(store), None);
        manager.make_available().await.unwrap();
        let wallet = wallet_on_manager(&root, manager, Services::mainnet().unwrap());
        let answer = internalize(&wallet, tx.clone(), &sender.to_hex()).await;

        let direct_checks = Arc::new(AtomicUsize::new(0));
        let store = local_store("direct-store-tracker").await;
        store
            .set_chain_tracker(Arc::new(CountingTracker {
                valid,
                checks: direct_checks.clone(),
            }))
            .await;
        let direct = Wallet::new(
            Some(root.clone()),
            StorageSqlx::clone(&*store),
            Services::mainnet().unwrap(),
        )
        .await
        .unwrap();
        let direct_answer = internalize(&direct, tx, &sender.to_hex()).await;
        let (on_manager, on_store) = (
            manager_checks.load(Ordering::SeqCst),
            direct_checks.load(Ordering::SeqCst),
        );
        println!(
            "tracker on the store alone, valid {valid}: manager {answer:?} (root checks {on_manager}); directly on the store {direct_answer:?} (root checks {on_store})"
        );
        assert_eq!(answer, direct_answer);
        assert_eq!(answer.is_ok(), valid, "{answer:?}");
        assert_eq!((on_manager, on_store), (1, 1));
    }
}
