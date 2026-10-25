// =============================================================================
// WalletStorageWriter Implementation
// =============================================================================

use super::{
    async_trait, AbortActionArgs, AbortActionResult, AuthId, ChainTracker, CreateActionDone,
    CreateActionDoor, DoorRefusal, Error, InternalizeActionArgs, PurgeParams, PurgeResults,
    RelinquishCertificateArgs, RelinquishOutputArgs, Result, ReviewStatusResult,
    StorageCreateActionResult, StorageInternalizeActionResult, StorageProcessActionArgs,
    StorageProcessActionResults, StorageSqlx, StoreBusyReason, TableCertificate,
    TableCertificateField, TableSettings, TableUser, TrxToken, Utc, WalletStorageWriter,
};

#[async_trait]
impl WalletStorageWriter for StorageSqlx {
    async fn make_available(&self) -> Result<TableSettings> {
        self.make_available_impl().await
    }

    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String> {
        self.migrate_impl(storage_name, storage_identity_key).await
    }

    async fn destroy(&self) -> Result<()> {
        self.destroy_impl().await
    }

    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        self.find_or_insert_user_impl(identity_key).await
    }

    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::abort_action::abort_action_internal(self, user_id, args).await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        // Get the ChainTracker for BEEF verification (if set)
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());

        // Note: Locking scripts for change outputs are stored during process_action
        // when the signed transaction is processed.
        super::create_action::create_action_internal(self, tracker_ref, user_id, args).await
    }

    async fn create_action_at_door(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
        door: crate::storage::CreateActionDoor<'_>,
    ) -> std::result::Result<crate::storage::CreateActionDone, crate::storage::DoorRefusal> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());
        super::create_action::create_action_at_door_internal(self, tracker_ref, user_id, args, door)
            .await
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::process_action::process_action_internal(self, user_id, args).await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::internalize_action::internalize_action_internal(self, user_id, args).await
    }

    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        super::internalize_action::mark_internalized_tx_failed(self, txid).await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.insert_certificate_impl(auth, certificate).await
    }

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.insert_certificate_field_impl(auth, field).await
    }

    async fn insert_certificate_with_fields(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
        fields: Vec<TableCertificateField>,
    ) -> Result<i64> {
        self.insert_certificate_with_fields_impl(auth, certificate, fields)
            .await
    }

    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        self.relinquish_certificate_impl(auth, args).await
    }

    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        self.relinquish_output_impl(auth, args).await
    }

    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        outcome: &super::BroadcastOutcome,
    ) -> Result<()> {
        #[cfg(test)]
        if self.take_busy_next_status_write() {
            return Err(Error::StoreBusy {
                reason: StoreBusyReason::SqliteBusy { code: 5 },
            });
        }

        super::process_action::update_transaction_status_after_broadcast_internal(
            self, txid, outcome,
        )
        .await
    }

    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: chrono::DateTime<chrono::Utc>,
    ) -> Result<ReviewStatusResult> {
        self.review_status_for_user(auth, aged_limit).await
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        self.purge_data_for_user(auth, params).await
    }

    async fn begin_transaction(&self) -> Result<TrxToken> {
        let token = TrxToken::new();
        let mut active = self.active_transactions.write().await;
        active.insert(token.id(), ());
        Ok(token)
    }

    async fn commit_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
    }

    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
    }
}
