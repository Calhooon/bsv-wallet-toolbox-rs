//! The wallet door's own check of a received AtomicBEEF's merkle roots.
//!
//! A wallet whose storage is not a local store in its own process
//! (`WalletStorageProvider::is_storage_provider` false: a storage client, a
//! storage manager) verifies the BEEF's roots with its own services' chain
//! tracker before it calls storage, and refuses with the storage door's
//! texts; with no tracker on its services it refuses with the storage
//! door's no-tracker sentence, storage never called. A wallet on a local
//! store leaves the check to the store's door: the tracker is read the same
//! number of times as before.
//!
//! The storage that is not local is a double behind the wallet's storage
//! traits: it answers every internalize accepted without checking anything
//! and counts the calls; every other method is a local in-memory store's.
//! Nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{InternalizeActionArgs, InternalizeActionResult, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::{
    AbortActionArgs, AbortActionResult, ListActionsArgs, ListActionsResult, ListCertificatesArgs,
    ListCertificatesResult, ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs,
    RelinquishOutputArgs, Result, StorageSqlx, Wallet, WalletServices,
};
use chrono::{DateTime, Utc};

use common::{accepted, derived_key, p2pkh_bytes, paying_derived, payment};

const PREFIX: &str = "d2FsbGV0LWRvb3I=";
const SUFFIX: &str = "cm9vdHM=";

const NO_TRACKER: &str = "wallet error: Validation error: No chain tracker is available to verify the BEEF: install one on the storage or configure the services' chain tracker";

/// A storage that is not a local store: every internalize is answered
/// accepted without any check and counted; everything else is `inner`'s.
struct NotLocal {
    inner: StorageSqlx,
    internalized: Arc<AtomicUsize>,
}

#[async_trait]
impl WalletStorageReader for NotLocal {
    fn is_available(&self) -> bool {
        self.inner.is_available()
    }
    fn get_settings(&self) -> TableSettings {
        self.inner.get_settings()
    }
    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        self.inner.get_services()
    }
    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        self.inner.find_certificates(auth, args).await
    }
    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        self.inner.find_output_baskets(auth, args).await
    }
    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        self.inner.find_outputs(auth, args).await
    }
    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.inner.find_proven_tx_reqs(args).await
    }
    async fn find_transactions(&self, args: FindTransactionsArgs) -> Result<Vec<TableTransaction>> {
        self.inner.find_transactions(args).await
    }
    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        self.inner.list_actions(auth, args).await
    }
    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        self.inner.list_certificates(auth, args).await
    }
    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        self.inner.list_outputs(auth, args).await
    }
}

#[async_trait]
impl WalletStorageWriter for NotLocal {
    async fn make_available(&self) -> Result<TableSettings> {
        self.inner.make_available().await
    }
    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String> {
        self.inner.migrate(storage_name, storage_identity_key).await
    }
    async fn destroy(&self) -> Result<()> {
        self.inner.destroy().await
    }
    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        self.inner.find_or_insert_user(identity_key).await
    }
    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        self.inner.abort_action(auth, args).await
    }
    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        self.inner.create_action(auth, args).await
    }
    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        self.inner.process_action(auth, args).await
    }
    async fn internalize_action(
        &self,
        _auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        self.internalized.fetch_add(1, Ordering::SeqCst);
        let txid = Beef::from_binary(&args.tx)
            .ok()
            .and_then(|b| b.atomic_txid)
            .unwrap_or_default();
        Ok(StorageInternalizeActionResult {
            base: InternalizeActionResult { accepted: true },
            is_merge: false,
            txid,
            satoshis: 0,
            send_with_results: None,
            not_delayed_results: None,
        })
    }
    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        self.inner.mark_internalized_tx_failed(txid).await
    }
    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.inner.insert_certificate(auth, certificate).await
    }
    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.inner.insert_certificate_field(auth, field).await
    }
    async fn insert_certificate_with_fields(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
        fields: Vec<TableCertificateField>,
    ) -> Result<i64> {
        self.inner
            .insert_certificate_with_fields(auth, certificate, fields)
            .await
    }
    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        self.inner.relinquish_certificate(auth, args).await
    }
    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        self.inner.relinquish_output(auth, args).await
    }
    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        outcome: &BroadcastOutcome,
    ) -> Result<()> {
        self.inner
            .update_transaction_status_after_broadcast(txid, outcome)
            .await
    }
    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: DateTime<Utc>,
    ) -> Result<ReviewStatusResult> {
        WalletStorageWriter::review_status(&self.inner, auth, aged_limit).await
    }
    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        WalletStorageWriter::purge_data(&self.inner, auth, params).await
    }
    async fn begin_transaction(&self) -> Result<TrxToken> {
        self.inner.begin_transaction().await
    }
    async fn commit_transaction(&self, trx: TrxToken) -> Result<()> {
        self.inner.commit_transaction(trx).await
    }
    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()> {
        self.inner.rollback_transaction(trx).await
    }
}

#[async_trait]
impl WalletStorageSync for NotLocal {
    async fn find_or_insert_sync_state(
        &self,
        auth: &AuthId,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)> {
        self.inner
            .find_or_insert_sync_state(auth, storage_identity_key, storage_name)
            .await
    }
    async fn set_active(&self, auth: &AuthId, key: &str) -> Result<i64> {
        self.inner.set_active(auth, key).await
    }
    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        self.inner.get_sync_chunk(args).await
    }
    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        self.inner.process_sync_chunk(args, chunk).await
    }
}

impl WalletStorageProvider for NotLocal {
    fn is_storage_provider(&self) -> bool {
        false
    }
    fn storage_identity_key(&self) -> String {
        self.inner.storage_identity_key()
    }
    fn storage_name(&self) -> String {
        self.inner.storage_name()
    }
    fn set_services(&self, services: Arc<dyn WalletServices>) {
        self.inner.set_services(services)
    }
}

/// An AtomicBEEF paying the key `root` derives for `sender`, whose parent
/// is carried whole with a BUMP at height 100_003.
fn paying_with_whole_parent(root: &PrivateKey, sender: &bsv_rs::primitives::PublicKey) -> Vec<u8> {
    let lock = LockingScript::from_binary(&p2pkh_bytes(&derived_key(root, sender, PREFIX, SUFFIX)))
        .unwrap();
    let mut parent = Transaction::new();
    let mut parent_input = TransactionInput::new("33".repeat(32), 0);
    parent_input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(parent_input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: common::other_lock(),
        change: false,
    });
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    child.version = 1;
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 100_003));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary_atomic(&txid).unwrap()
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

/// A wallet on a storage that is not local, with `services`: the answer
/// to one internalize of `tx` and the number of internalize calls the
/// storage received.
async fn on_not_local<V: WalletServices + Send + Sync + 'static>(
    services: V,
    whole_parent: bool,
) -> (std::result::Result<bool, String>, usize) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let tx = if whole_parent {
        paying_with_whole_parent(&root, &sender)
    } else {
        paying_derived(&root, &sender, PREFIX, SUFFIX).0
    };
    let internalized = Arc::new(AtomicUsize::new(0));
    let storage = NotLocal {
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
async fn not_local_storage_with_a_refuting_tracker_is_refused_at_the_wallet_door() {
    let (answer, calls) = on_not_local(mock_services(false), true).await;
    println!("not local, refuting tracker: {answer:?}, storage internalize calls {calls}");
    assert!(
        answer.as_ref().is_err_and(|e| e
            .starts_with("wallet error: Validation error: Invalid merkle root ")
            && e.ends_with(" at height 100003")),
        "{answer:?}"
    );
    assert_eq!(calls, 0, "storage is never called");
}

#[tokio::test]
async fn not_local_storage_with_a_confirming_tracker_is_called_once() {
    let (answer, calls) = on_not_local(mock_services(true), true).await;
    println!("not local, confirming tracker: {answer:?}, storage internalize calls {calls}");
    assert_eq!(answer, Ok(true), "the storage's answer");
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn not_local_storage_with_no_tracker_on_the_services_is_refused() {
    let services = bsv_wallet_toolbox_rs::Services::mainnet().unwrap();
    let (answer, calls) = on_not_local(services, true).await;
    println!("not local, services with no tracker: {answer:?}, storage internalize calls {calls}");
    assert_eq!(answer, Err(NO_TRACKER.to_string()));
    assert_eq!(calls, 0, "storage is never called");
}

#[tokio::test]
async fn not_local_storage_refuses_an_entry_carried_by_txid_alone() {
    let (answer, calls) = on_not_local(mock_services(true), false).await;
    println!("not local, txid-only parent: {answer:?}, storage internalize calls {calls}");
    assert!(
        answer.as_ref().is_err_and(|e| e.starts_with(
            "wallet error: Validation error: The tx parameter must be valid AtomicBEEF: transaction "
        ) && e.ends_with(" is carried by txid alone")),
        "{answer:?}"
    );
    assert_eq!(calls, 0, "storage is never called");
}

/// A wallet on its own local store: one internalize reads the services'
/// chain tracker once (at the storage door), as at 0.13.4, refuted or
/// confirmed, whole parent or txid-only parent.
#[tokio::test]
async fn a_local_store_reads_the_tracker_as_before() {
    for (roots_valid, whole_parent) in [(true, true), (false, true), (true, false), (false, false)]
    {
        let root = PrivateKey::random();
        let sender = PrivateKey::random().public_key();
        let tx = if whole_parent {
            paying_with_whole_parent(&root, &sender)
        } else {
            paying_derived(&root, &sender, PREFIX, SUFFIX).0
        };
        let storage = local_store(&root).await;
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
        assert_eq!(answer.is_ok(), roots_valid, "{answer:?}");
    }
}
