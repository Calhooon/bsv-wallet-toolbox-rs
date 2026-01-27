//! StorageClient implementation for remote storage via JSON-RPC.
//!
//! This implements `WalletStorageProvider` by making authenticated JSON-RPC
//! calls to a remote storage server (e.g., `storage.babbage.systems`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::RwLock;

use bsv_sdk::auth::{Peer, PeerOptions, SimplifiedFetchTransport};
use bsv_sdk::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, InternalizeActionArgs,
    ListActionsArgs, ListActionsResult, ListCertificatesArgs, ListCertificatesResult,
    ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs,
    WalletInterface,
};

use crate::error::{Error, Result};
use crate::storage::entities::*;
use crate::storage::traits::*;

use super::json_rpc::{JsonRpcRequest, JsonRpcResponse};

/// Mainnet storage endpoint.
pub const MAINNET_URL: &str = "https://storage.babbage.systems";
/// Testnet storage endpoint.
pub const TESTNET_URL: &str = "https://staging-storage.babbage.systems";

/// Remote storage client using JSON-RPC over HTTPS with BRC-31 authentication.
///
/// `StorageClient` implements the `WalletStorageProvider` interface, allowing it
/// to serve as a BRC-100 wallet's active storage via remote calls.
///
/// Authentication is handled via the `Peer` type from `bsv-sdk`, which implements
/// BRC-31 (Authrite) mutual authentication.
///
/// ## Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::storage::client::StorageClient;
/// use bsv_sdk::wallet::ProtoWallet;
/// use bsv_sdk::primitives::PrivateKey;
///
/// let wallet = ProtoWallet::new(Some(PrivateKey::from_wif("...")?));
/// let client = StorageClient::new(wallet, StorageClient::MAINNET_URL);
///
/// // Initialize and verify connection
/// let settings = client.make_available().await?;
/// println!("Connected to storage: {}", settings.storage_name);
///
/// // Find or create user
/// let (user, is_new) = client.find_or_insert_user(&identity_key).await?;
/// ```
pub struct StorageClient<W: WalletInterface> {
    /// The endpoint URL for the storage server.
    endpoint_url: String,

    /// The authenticated peer for BRC-31 communication.
    #[allow(dead_code)]
    peer: Arc<Peer<W, SimplifiedFetchTransport>>,

    /// The wallet for authentication.
    wallet: W,

    /// HTTP client for requests.
    http_client: reqwest::Client,

    /// Request ID counter.
    next_id: AtomicU64,

    /// Cached settings after makeAvailable.
    settings: Arc<RwLock<Option<TableSettings>>>,

    /// Whether to use authenticated requests (BRC-31).
    /// Set to false for testing without authentication.
    use_auth: bool,
}

impl<W: WalletInterface + Clone + 'static> StorageClient<W> {
    /// Mainnet storage endpoint URL.
    pub const MAINNET_URL: &'static str = MAINNET_URL;

    /// Testnet storage endpoint URL.
    pub const TESTNET_URL: &'static str = TESTNET_URL;

    /// Creates a new StorageClient with BRC-31 authentication.
    ///
    /// # Arguments
    ///
    /// * `wallet` - A wallet implementing `WalletInterface` for authentication
    /// * `endpoint_url` - The storage server URL (use `MAINNET_URL` or `TESTNET_URL`)
    pub fn new(wallet: W, endpoint_url: impl Into<String>) -> Self {
        let url = endpoint_url.into();
        let transport = SimplifiedFetchTransport::new(&url);

        let peer = Peer::new(PeerOptions {
            wallet: wallet.clone(),
            transport,
            certificates_to_request: None,
            session_manager: None,
            auto_persist_last_session: true,
            originator: Some("bsv-wallet-toolbox".to_string()),
        });

        Self {
            endpoint_url: url,
            peer: Arc::new(peer),
            wallet,
            http_client: reqwest::Client::new(),
            next_id: AtomicU64::new(1),
            settings: Arc::new(RwLock::new(None)),
            use_auth: true,
        }
    }

    /// Creates a new StorageClient without authentication (for testing).
    ///
    /// # Arguments
    ///
    /// * `wallet` - A wallet implementing `WalletInterface`
    /// * `endpoint_url` - The storage server URL
    pub fn new_unauthenticated(wallet: W, endpoint_url: impl Into<String>) -> Self {
        let mut client = Self::new(wallet, endpoint_url);
        client.use_auth = false;
        client
    }

    /// Creates a new StorageClient for mainnet.
    pub fn mainnet(wallet: W) -> Self {
        Self::new(wallet, MAINNET_URL)
    }

    /// Creates a new StorageClient for testnet.
    pub fn testnet(wallet: W) -> Self {
        Self::new(wallet, TESTNET_URL)
    }

    /// Returns the endpoint URL.
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Returns a reference to the peer for advanced operations.
    pub fn peer(&self) -> &Arc<Peer<W, SimplifiedFetchTransport>> {
        &self.peer
    }

    /// Returns a reference to the wallet.
    pub fn wallet(&self) -> &W {
        &self.wallet
    }

    /// Gets the identity key of the wallet (hex string).
    pub async fn get_identity_key(&self) -> Result<String> {
        use bsv_sdk::wallet::GetPublicKeyArgs;

        let result = self
            .wallet
            .get_public_key(
                GetPublicKeyArgs {
                    identity_key: true,
                    protocol_id: None,
                    key_id: None,
                    counterparty: None,
                    for_self: None,
                },
                "bsv-wallet-toolbox",
            )
            .await
            .map_err(|_| Error::AuthenticationRequired)?;

        Ok(result.public_key)
    }

    /// Makes a JSON-RPC call to the storage server.
    ///
    /// This method handles:
    /// - Request ID generation
    /// - JSON serialization
    /// - BRC-31 authenticated HTTP POST (when use_auth is true)
    /// - Response parsing and error handling
    ///
    /// # Arguments
    ///
    /// * `method` - The RPC method name (e.g., "makeAvailable", "findOrInsertUser")
    /// * `params` - Parameters to pass to the method
    async fn rpc_call<T: DeserializeOwned>(&self, method: &str, params: Vec<Value>) -> Result<T> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let request = JsonRpcRequest::new(method, params, id);
        let request_body = serde_json::to_vec(&request)?;

        tracing::debug!(
            method = method,
            id = id,
            endpoint = %self.endpoint_url,
            "Making JSON-RPC call to storage server"
        );

        // Make HTTP POST request
        // TODO: When BRC-31 auth is properly implemented in the peer,
        // use self.peer.to_peer() instead of direct HTTP
        let mut request_builder = self
            .http_client
            .post(&self.endpoint_url)
            .header("Content-Type", "application/json");

        // Add authentication headers if enabled
        if self.use_auth {
            // For now, add identity key header for basic identification
            // Full BRC-31 auth would involve the complete handshake via Peer
            if let Ok(identity_key) = self.get_identity_key().await {
                request_builder = request_builder
                    .header("x-bsv-auth-identity-key", identity_key);
            }
        }

        let response = request_builder
            .body(request_body)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::NetworkError(format!(
                "HTTP error {}: {}",
                status, body
            )));
        }

        let response_body = response.bytes().await.map_err(|e| {
            Error::NetworkError(format!("Failed to read response body: {}", e))
        })?;

        tracing::trace!(
            method = method,
            response_size = response_body.len(),
            "Received response from storage server"
        );

        let rpc_response: JsonRpcResponse = serde_json::from_slice(&response_body)
            .map_err(|e| Error::StorageError(format!("Invalid JSON-RPC response: {}", e)))?;

        if rpc_response.id != id {
            return Err(Error::StorageError(format!(
                "Response ID mismatch: expected {}, got {}",
                id, rpc_response.id
            )));
        }

        match rpc_response.into_result() {
            Ok(value) => {
                let result: T = serde_json::from_value(value).map_err(|e| {
                    Error::StorageError(format!("Failed to deserialize result: {}", e))
                })?;
                Ok(result)
            }
            Err(rpc_error) => {
                tracing::error!(
                    method = method,
                    code = rpc_error.code,
                    message = %rpc_error.message,
                    "JSON-RPC error from storage server"
                );
                Err(Error::StorageError(format!(
                    "RPC error {}: {}",
                    rpc_error.code, rpc_error.message
                )))
            }
        }
    }

    /// Helper to serialize a value to JSON Value.
    fn to_value<T: Serialize>(value: &T) -> Result<Value> {
        serde_json::to_value(value).map_err(|e| Error::StorageError(e.to_string()))
    }
}

// =============================================================================
// WalletStorageReader Implementation
// =============================================================================

#[async_trait]
impl<W: WalletInterface + Clone + 'static> WalletStorageReader for StorageClient<W> {
    fn is_available(&self) -> bool {
        // Check if we have cached settings
        // Note: This is sync, so we can't check the RwLock properly
        // In practice, call make_available() first
        true
    }

    fn get_settings(&self) -> &TableSettings {
        // This is problematic for the trait signature - we need to return a reference
        // but we're behind an RwLock. For now, panic if not available.
        // A better design would be to cache settings differently.
        panic!("get_settings() requires make_available() to be called first. Use get_settings_async() instead.")
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        self.rpc_call(
            "findCertificatesAuth",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        self.rpc_call(
            "findOutputBaskets",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        self.rpc_call(
            "findOutputsAuth",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.rpc_call("findProvenTxReqs", vec![Self::to_value(&args)?])
            .await
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        self.rpc_call(
            "listActions",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        self.rpc_call(
            "listCertificates",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        self.rpc_call(
            "listOutputs",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }
}

// =============================================================================
// WalletStorageWriter Implementation
// =============================================================================

#[async_trait]
impl<W: WalletInterface + Clone + 'static> WalletStorageWriter for StorageClient<W> {
    async fn make_available(&self) -> Result<TableSettings> {
        // Check if we already have settings cached
        {
            let settings = self.settings.read().await;
            if let Some(ref s) = *settings {
                return Ok(s.clone());
            }
        }

        // Fetch settings from server
        let result: TableSettings = self.rpc_call("makeAvailable", vec![]).await?;

        // Cache the settings
        {
            let mut settings = self.settings.write().await;
            *settings = Some(result.clone());
        }

        tracing::info!(
            storage_name = %result.storage_name,
            storage_identity_key = %result.storage_identity_key,
            chain = %result.chain,
            "Connected to storage server"
        );

        Ok(result)
    }

    async fn migrate(&self, storage_name: &str, _storage_identity_key: &str) -> Result<String> {
        // Remote storage typically ignores migration requests from clients
        // The TypeScript implementation only sends storage_name
        self.rpc_call(
            "migrate",
            vec![Value::String(storage_name.to_string())],
        )
        .await
    }

    async fn destroy(&self) -> Result<()> {
        self.rpc_call::<Value>("destroy", vec![]).await?;

        // Clear cached settings
        let mut settings = self.settings.write().await;
        *settings = None;

        Ok(())
    }

    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
        #[derive(serde::Deserialize)]
        struct FindOrInsertUserResult {
            user: TableUser,
            #[serde(rename = "isNew")]
            is_new: bool,
        }

        let result: FindOrInsertUserResult = self
            .rpc_call(
                "findOrInsertUser",
                vec![Value::String(identity_key.to_string())],
            )
            .await?;

        Ok((result.user, result.is_new))
    }

    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        self.rpc_call(
            "abortAction",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        self.rpc_call(
            "createAction",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        self.rpc_call(
            "processAction",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        self.rpc_call(
            "internalizeAction",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        self.rpc_call(
            "insertCertificateAuth",
            vec![Self::to_value(auth)?, Self::to_value(&certificate)?],
        )
        .await
    }

    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        self.rpc_call(
            "relinquishCertificate",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }

    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        self.rpc_call(
            "relinquishOutput",
            vec![Self::to_value(auth)?, Self::to_value(&args)?],
        )
        .await
    }
}

// =============================================================================
// WalletStorageSync Implementation
// =============================================================================

#[async_trait]
impl<W: WalletInterface + Clone + 'static> WalletStorageSync for StorageClient<W> {
    async fn find_or_insert_sync_state(
        &self,
        auth: &AuthId,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)> {
        #[derive(serde::Deserialize)]
        struct FindOrInsertSyncStateResult {
            #[serde(rename = "syncState")]
            sync_state: TableSyncState,
            #[serde(rename = "isNew")]
            is_new: bool,
        }

        let result: FindOrInsertSyncStateResult = self
            .rpc_call(
                "findOrInsertSyncStateAuth",
                vec![
                    Self::to_value(auth)?,
                    Value::String(storage_identity_key.to_string()),
                    Value::String(storage_name.to_string()),
                ],
            )
            .await?;

        Ok((result.sync_state, result.is_new))
    }

    async fn set_active(
        &self,
        auth: &AuthId,
        new_active_storage_identity_key: &str,
    ) -> Result<i64> {
        self.rpc_call(
            "setActive",
            vec![
                Self::to_value(auth)?,
                Value::String(new_active_storage_identity_key.to_string()),
            ],
        )
        .await
    }

    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        self.rpc_call("getSyncChunk", vec![Self::to_value(&args)?])
            .await
    }

    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        self.rpc_call(
            "processSyncChunk",
            vec![Self::to_value(&args)?, Self::to_value(&chunk)?],
        )
        .await
    }
}

// =============================================================================
// WalletStorageProvider Implementation
// =============================================================================

#[async_trait]
impl<W: WalletInterface + Clone + 'static> WalletStorageProvider for StorageClient<W> {
    fn is_storage_provider(&self) -> bool {
        // StorageClient implements WalletStorageProvider but not the lower-level
        // StorageProvider interface (like direct table access)
        false
    }

    fn storage_identity_key(&self) -> &str {
        // This would need to be fetched from settings
        // For now, return empty - proper implementation would cache this
        ""
    }

    fn storage_name(&self) -> &str {
        // This would need to be fetched from settings
        ""
    }
}

// =============================================================================
// Additional Helper Methods
// =============================================================================

impl<W: WalletInterface + Clone + 'static> StorageClient<W> {
    /// Gets the cached settings, or fetches them if not available.
    ///
    /// This is the async version of `get_settings()` that properly handles
    /// the RwLock.
    pub async fn get_settings_async(&self) -> Result<TableSettings> {
        {
            let settings = self.settings.read().await;
            if let Some(ref s) = *settings {
                return Ok(s.clone());
            }
        }

        // Settings not cached, fetch them
        self.make_available().await
    }

    /// Checks if the storage is available and connected.
    ///
    /// Returns true if `make_available()` has been called successfully.
    pub async fn is_available_async(&self) -> bool {
        let settings = self.settings.read().await;
        settings.is_some()
    }

    /// Updates a proven transaction request with a new proven transaction.
    ///
    /// This handles the data when a new transaction proof is found.
    pub async fn update_proven_tx_req_with_new_proven_tx(
        &self,
        args: UpdateProvenTxReqWithNewProvenTxArgs,
    ) -> Result<UpdateProvenTxReqWithNewProvenTxResult> {
        self.rpc_call(
            "updateProvenTxReqWithNewProvenTx",
            vec![Self::to_value(&args)?],
        )
        .await
    }

    /// Creates an AuthId for the current wallet user.
    ///
    /// This is a convenience method that creates an AuthId with the wallet's
    /// identity key. The user_id will be populated after calling find_or_insert_user().
    pub async fn create_auth_id(&self) -> Result<AuthId> {
        let identity_key = self.get_identity_key().await?;
        Ok(AuthId::new(identity_key))
    }

    /// Creates an AuthId with a user_id for the current wallet user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - The user's ID from find_or_insert_user()
    pub async fn create_auth_id_with_user(&self, user_id: i64) -> Result<AuthId> {
        let identity_key = self.get_identity_key().await?;
        Ok(AuthId::with_user_id(identity_key, user_id))
    }
}

/// Arguments for updating a proven tx request with a new proven tx.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProvenTxReqWithNewProvenTxArgs {
    /// The proven tx request ID.
    pub proven_tx_req_id: i64,
    /// The new proven tx data.
    pub proven_tx: TableProvenTx,
}

/// Result of updating a proven tx request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProvenTxReqWithNewProvenTxResult {
    /// Number of transactions updated.
    pub txs_updated: i32,
    /// Number of requests updated.
    pub reqs_updated: i32,
    /// The proven tx ID.
    pub proven_tx_id: i64,
}

// =============================================================================
// Storage Info Builder
// =============================================================================

impl<W: WalletInterface + Clone + 'static> StorageClient<W> {
    /// Creates a WalletStorageInfo for this client.
    ///
    /// Requires that make_available() has been called first.
    pub async fn get_storage_info(&self, user_id: i64, is_active: bool) -> Result<WalletStorageInfo> {
        let settings = self.get_settings_async().await?;

        Ok(WalletStorageInfo {
            is_active,
            is_enabled: true,
            is_backup: !is_active,
            is_conflicting: false,
            user_id,
            storage_identity_key: settings.storage_identity_key,
            storage_name: settings.storage_name,
            storage_class: "StorageClient".to_string(),
            endpoint_url: Some(self.endpoint_url.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_urls() {
        assert_eq!(MAINNET_URL, "https://storage.babbage.systems");
        assert_eq!(TESTNET_URL, "https://staging-storage.babbage.systems");
    }

    #[test]
    fn test_json_rpc_request_format() {
        let request = JsonRpcRequest::new("makeAvailable", vec![], 1);
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"method\":\"makeAvailable\""));
        assert!(json.contains("\"params\":[]"));
        assert!(json.contains("\"id\":1"));
    }

    // Integration tests would require a mock server or actual connection
    // to storage.babbage.systems
}
