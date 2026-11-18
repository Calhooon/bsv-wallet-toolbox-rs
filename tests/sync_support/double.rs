//! A store behind the storage traits that forwards to a local store, with
//! hooks: it can answer as a store that is not local (from the start,
//! from a chunk on, or until a chunk is served), serve scripted chunks as a reader, answer every chunk
//! with a fixed result, refuse `set_active`, write its row and then lose
//! the answer, write its row and then never answer or panic, refuse to
//! read its user row or never answer that read, and record its
//! `set_active` calls and count its user reads. Shared by the sync manager tests.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::wallet::InternalizeActionArgs;
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::{
    AbortActionArgs, AbortActionResult, Error, ListActionsArgs, ListActionsResult,
    ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs, ListOutputsResult,
    RelinquishCertificateArgs, RelinquishOutputArgs, Result, StorageSqlx, WalletServices,
};
use chrono::{DateTime, Utc};

/// A forwarding store with hooks (see the module's head).
pub struct Double {
    pub inner: StorageSqlx,
    /// Answer `checks_received_beef_in_process` false, as a storage client.
    pub not_local: bool,
    /// Answer local only while at most this many chunks have been served.
    pub local_through: Option<usize>,
    /// Answer local only once at least this many chunks have been served.
    pub local_from: Option<usize>,
    /// Chunks served as a reader.
    pub served: AtomicUsize,
    /// As a reader, serve these in order (then an empty chunk), when set.
    pub script: Mutex<Option<std::collections::VecDeque<SyncChunk>>>,
    /// Every `process_sync_chunk_mapped` answers this, when set.
    pub answer: Mutex<Option<ProcessSyncChunkResult>>,
    /// Chunks handed to this store as a writer.
    pub chunks: AtomicUsize,
    /// `set_active` refuses, when set.
    pub fail_set_active: AtomicBool,
    /// `set_active` writes the row and then answers an error, when set.
    pub lose_set_active_answer: AtomicBool,
    /// A lost answer also sets `refuse_user_reads`, when set.
    pub refuse_reads_after_lost_answer: AtomicBool,
    /// `set_active` writes the row and then never answers, when set.
    pub hang_after_write: AtomicBool,
    /// `set_active` writes the row and then panics, when set.
    pub panic_after_write: AtomicBool,
    /// `find_or_insert_user` refuses, when set.
    pub refuse_user_reads: AtomicBool,
    /// `find_or_insert_user` never answers, when set.
    pub hang_user_reads: AtomicBool,
    /// `find_or_insert_user` calls.
    pub user_reads: AtomicUsize,
    /// How long each `set_active` call takes before it answers.
    pub set_active_delay_ms: u64,
    /// `<storage name> enter|leave <key>` per `set_active` call.
    pub events: Arc<Mutex<Vec<String>>>,
}

impl Double {
    pub fn new(inner: StorageSqlx) -> Self {
        Self {
            inner,
            not_local: false,
            local_through: None,
            local_from: None,
            served: AtomicUsize::new(0),
            script: Mutex::new(None),
            answer: Mutex::new(None),
            chunks: AtomicUsize::new(0),
            fail_set_active: AtomicBool::new(false),
            lose_set_active_answer: AtomicBool::new(false),
            refuse_reads_after_lost_answer: AtomicBool::new(false),
            hang_after_write: AtomicBool::new(false),
            panic_after_write: AtomicBool::new(false),
            refuse_user_reads: AtomicBool::new(false),
            hang_user_reads: AtomicBool::new(false),
            user_reads: AtomicUsize::new(0),
            set_active_delay_ms: 0,
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl WalletStorageReader for Double {
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
impl WalletStorageWriter for Double {
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
        self.user_reads.fetch_add(1, Ordering::SeqCst);
        if self.hang_user_reads.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.refuse_user_reads.load(Ordering::SeqCst) {
            let name = self.inner.storage_name();
            return Err(Error::StorageError(format!("{name} refused the user read")));
        }
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
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        self.inner.internalize_action(auth, args).await
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
impl WalletStorageSync for Double {
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
        let name = self.inner.storage_name();
        self.events
            .lock()
            .unwrap()
            .push(format!("{name} enter {key}"));
        tokio::time::sleep(std::time::Duration::from_millis(self.set_active_delay_ms)).await;
        let r = if self.fail_set_active.load(Ordering::SeqCst) {
            Err(Error::StorageError(format!("{name} refused set_active")))
        } else if self.hang_after_write.load(Ordering::SeqCst) {
            self.inner.set_active(auth, key).await?;
            std::future::pending::<()>().await;
            unreachable!()
        } else if self.panic_after_write.load(Ordering::SeqCst) {
            self.inner.set_active(auth, key).await?;
            panic!("{name} panicked after its write");
        } else if self.lose_set_active_answer.load(Ordering::SeqCst) {
            self.inner.set_active(auth, key).await?;
            if self.refuse_reads_after_lost_answer.load(Ordering::SeqCst) {
                self.refuse_user_reads.store(true, Ordering::SeqCst);
            }
            Err(Error::StorageError(format!("{name} answer lost")))
        } else {
            self.inner.set_active(auth, key).await
        };
        self.events
            .lock()
            .unwrap()
            .push(format!("{name} leave {key}"));
        r
    }
    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        let scripted = self.script.lock().unwrap().as_mut().map(|chunks| {
            chunks.pop_front().unwrap_or_else(|| SyncChunk {
                from_storage_identity_key: args.from_storage_identity_key.clone(),
                to_storage_identity_key: args.to_storage_identity_key.clone(),
                user_identity_key: args.identity_key.clone(),
                ..Default::default()
            })
        });
        let chunk = match scripted {
            Some(chunk) => Ok(chunk),
            None => self.inner.get_sync_chunk(args).await,
        };
        self.served.fetch_add(1, Ordering::SeqCst);
        chunk
    }
    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        self.inner.process_sync_chunk(args, chunk).await
    }
    async fn process_sync_chunk_mapped(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
        sync_map: &mut SyncMap,
    ) -> Result<ProcessSyncChunkResult> {
        self.chunks.fetch_add(1, Ordering::SeqCst);
        if let Some(answer) = self.answer.lock().unwrap().clone() {
            return Ok(answer);
        }
        self.inner
            .process_sync_chunk_mapped(args, chunk, sync_map)
            .await
    }
}

#[async_trait]
impl WalletStorageProvider for Double {
    async fn checks_received_beef_in_process(&self) -> bool {
        let served = self.served.load(Ordering::SeqCst);
        !self.not_local
            && self.local_through.is_none_or(|n| served <= n)
            && self.local_from.is_none_or(|n| served >= n)
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

#[async_trait]
impl MonitorStorage for Double {
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
