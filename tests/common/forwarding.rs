//! A host's own storage type behind the wallet's storage traits that
//! forwards and overrides no provided method: every internalize is answered
//! accepted without any check and counted; every other method is a local
//! in-memory store's. Shared by the tests of the wallet door's check.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{InternalizeActionArgs, InternalizeActionResult};
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::{
    AbortActionArgs, AbortActionResult, ListActionsArgs, ListActionsResult, ListCertificatesArgs,
    ListCertificatesResult, ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs,
    RelinquishOutputArgs, Result, StorageSqlx, WalletServices,
};
use chrono::{DateTime, Utc};

/// A host's own storage type that forwards: every internalize is answered
/// accepted without any check and counted; everything else is `inner`'s.
/// It overrides no provided method of the storage traits.
pub struct Forwarding {
    pub inner: StorageSqlx,
    pub internalized: Arc<AtomicUsize>,
}

#[async_trait]
impl WalletStorageReader for Forwarding {
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
impl WalletStorageWriter for Forwarding {
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
impl WalletStorageSync for Forwarding {
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

impl WalletStorageProvider for Forwarding {
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

#[async_trait]
impl MonitorStorage for Forwarding {
    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        self.inner.synchronize_transaction_statuses().await
    }
    async fn send_waiting_transactions(
        &self,
        min_transaction_age: std::time::Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        self.inner
            .send_waiting_transactions(min_transaction_age)
            .await
    }
    async fn abort_abandoned(&self, timeout: std::time::Duration) -> Result<()> {
        self.inner.abort_abandoned(timeout).await
    }
    async fn un_fail(&self) -> Result<()> {
        self.inner.un_fail().await
    }
    async fn review_status(&self) -> Result<ReviewStatusResult> {
        MonitorStorage::review_status(&self.inner).await
    }
    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        MonitorStorage::purge_data(&self.inner, params).await
    }
}
