// =============================================================================
// WalletStorageReader Implementation
// =============================================================================

use super::{
    async_trait, lock_read, Arc, AuthId, Error, FindCertificatesArgs, FindOutputBasketsArgs,
    FindOutputsArgs, FindProvenTxReqsArgs, FindTransactionsArgs, ListActionsArgs,
    ListActionsResult, ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs,
    ListOutputsResult, Result, StorageSqlx, TableCertificate, TableOutput, TableOutputBasket,
    TableProvenTxReq, TableSettings, TableTransaction, WalletServices, WalletStorageReader,
};

#[async_trait]
impl WalletStorageReader for StorageSqlx {
    fn is_available(&self) -> bool {
        lock_read(&self.settings)
            .map(|s| s.is_some())
            .unwrap_or(false)
    }

    fn get_settings(&self) -> TableSettings {
        // A copy taken under the lock; the defaults before make_available()
        // (or on a poisoned lock).
        lock_read(&self.settings)
            .ok()
            .and_then(|settings| settings.clone())
            .unwrap_or_default()
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        let guard = lock_read(&self.services)?;
        guard.clone().ok_or_else(|| {
            Error::InvalidOperation(
                "Must setServices first. Services are required for blockchain operations."
                    .to_string(),
            )
        })
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_certificates_internal(user_id, &args).await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_output_baskets_internal(user_id, &args).await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_outputs_internal(user_id, &args).await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.find_proven_tx_reqs_internal(&args).await
    }

    async fn find_transactions(
        &self,
        _args: FindTransactionsArgs,
    ) -> Result<Vec<TableTransaction>> {
        // TODO: Implement for SQLite storage
        Ok(vec![])
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        self.list_actions_impl(auth, args).await
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        self.list_certificates_impl(auth, args).await
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        self.list_outputs_impl(auth, args).await
    }
}
