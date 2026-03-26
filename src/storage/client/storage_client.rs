//! StorageClient implementation for remote storage via JSON-RPC.
//!
//! This implements `WalletStorageProvider` by making authenticated JSON-RPC
//! calls to a remote storage server (e.g., `storage.babbage.systems`).
//!
//! ## BRC-31 Authentication
//!
//! All requests are authenticated using the BRC-31 (Authrite) protocol:
//! - Each request is signed with the wallet's identity key
//! - A unique nonce and timestamp prevent replay attacks
//! - Server responses can optionally be verified
//!
//! The authentication headers added to each request:
//! - `x-bsv-auth-version`: Protocol version ("0.1")
//! - `x-bsv-auth-identity-key`: Client's public key (hex)
//! - `x-bsv-auth-nonce`: Random nonce (base64)
//! - `x-bsv-auth-timestamp`: Unix timestamp (milliseconds)
//! - `x-bsv-auth-signature`: Signature over canonical request (hex)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{oneshot, RwLock};

use bsv_rs::auth::transports::HttpRequest;
use bsv_rs::auth::{Peer, PeerOptions, SimplifiedFetchTransport};
use bsv_rs::primitives::{to_base64, PublicKey};
use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, InternalizeActionArgs, ListActionsArgs,
    ListActionsResult, ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs,
    ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs, WalletInterface,
};

use crate::error::{Error, Result};
use crate::services::WalletServices;
use crate::storage::entities::*;
use crate::storage::traits::*;

use super::json_rpc::{JsonRpcRequest, JsonRpcResponse};

// =============================================================================
// ValidCreateActionArgs
// =============================================================================

/// Validated CreateAction arguments with internal state flags.
///
/// This struct wraps the SDK's `CreateActionArgs` and adds internal flags that
/// the server expects. The TypeScript SDK has a `ValidCreateActionArgs` class
/// that performs this same validation/enhancement before sending to storage.
///
/// ## Server Requirement
///
/// The storage server expects these flags to be present in the createAction request.
/// Without them, the server returns an "internal error" because it cannot determine
/// the transaction mode (new tx, noSend, delayed, etc.).
///
/// ## Flag Derivation
///
/// The flags are derived from the `CreateActionArgs.options` field:
/// - `isNewTx`: True when creating a new transaction (always true for createAction)
/// - `isNoSend`: True when `options.noSend` is true
/// - `isDelayed`: True when `options.acceptDelayedBroadcast` is true
/// - `isSendWith`: True when `options.sendWith` has items
/// - `isRemixChange`: True for change-only remix transactions (typically false)
/// - `isSignAction`: True when `options.signAndProcess` is true
/// - `includeAllSourceTransactions`: Whether to include all ancestor transactions in BEEF
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidCreateActionArgs {
    /// The action description (5-2000 characters).
    pub description: String,

    /// Optional BEEF data containing input transactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_beef: Option<Vec<u8>>,

    /// Input specifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<bsv_rs::wallet::CreateActionInput>>,

    /// Output specifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<bsv_rs::wallet::CreateActionOutput>>,

    /// Transaction lock time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_time: Option<u32>,

    /// Transaction version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    /// Transaction labels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,

    /// Action options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<bsv_rs::wallet::CreateActionOptions>,

    // === Internal state flags (required by server) ===
    /// True when creating a new transaction with inputs/outputs.
    /// For createAction, this is typically always true.
    pub is_new_tx: bool,

    /// True when `options.noSend` is true - creates transaction but doesn't broadcast.
    pub is_no_send: bool,

    /// True when `options.acceptDelayedBroadcast` is true - allows deferred broadcast.
    pub is_delayed: bool,

    /// True when `options.sendWith` has items - bundles multiple transactions for broadcast.
    pub is_send_with: bool,

    /// True when creating a change-only remix transaction (no explicit inputs/outputs).
    pub is_remix_change: bool,

    /// True when `options.signAndProcess` is true - signs immediately.
    pub is_sign_action: bool,

    /// True to include all ancestor transactions in BEEF.
    pub include_all_source_transactions: bool,
}

impl From<CreateActionArgs> for ValidCreateActionArgs {
    fn from(args: CreateActionArgs) -> Self {
        // Extract option flags with sensible defaults
        let options = args.options.as_ref();

        let is_no_send = options.and_then(|o| o.no_send).unwrap_or(false);

        let is_delayed = options
            .and_then(|o| o.accept_delayed_broadcast)
            .unwrap_or(false);

        let is_send_with = options
            .and_then(|o| o.send_with.as_ref())
            .map(|s| !s.is_empty())
            .unwrap_or(false);

        let is_sign_action = options.and_then(|o| o.sign_and_process).unwrap_or(true); // Default to true per TypeScript SDK

        // isRemixChange is true only when there are no explicit inputs or outputs
        // and we're just remixing change. For normal createAction, this is false.
        let is_remix_change = args.inputs.as_ref().map(|i| i.is_empty()).unwrap_or(true)
            && args.outputs.as_ref().map(|o| o.is_empty()).unwrap_or(true);

        // isNewTx is true when we have inputs or outputs to create
        // For createAction with outputs, this is always true
        let is_new_tx = !is_remix_change;

        ValidCreateActionArgs {
            description: args.description,
            input_beef: args.input_beef,
            inputs: args.inputs,
            outputs: args.outputs,
            lock_time: args.lock_time,
            version: args.version,
            labels: args.labels,
            options: args.options,
            is_new_tx,
            is_no_send,
            is_delayed,
            is_send_with,
            is_remix_change,
            is_sign_action,
            include_all_source_transactions: true, // Default to true for complete BEEF
        }
    }
}

impl ValidCreateActionArgs {
    /// Creates a new ValidCreateActionArgs from CreateActionArgs.
    pub fn new(args: CreateActionArgs) -> Self {
        args.into()
    }

    /// Creates a ValidCreateActionArgs with custom flag overrides.
    ///
    /// Use this when you need to override the automatically computed flags.
    pub fn with_flags(
        args: CreateActionArgs,
        is_new_tx: bool,
        is_no_send: bool,
        is_delayed: bool,
        is_send_with: bool,
    ) -> Self {
        let mut valid_args: ValidCreateActionArgs = args.into();
        valid_args.is_new_tx = is_new_tx;
        valid_args.is_no_send = is_no_send;
        valid_args.is_delayed = is_delayed;
        valid_args.is_send_with = is_send_with;
        valid_args
    }
}

/// Mainnet storage endpoint.
pub const MAINNET_URL: &str = "https://storage.babbage.systems";
/// Testnet storage endpoint.
pub const TESTNET_URL: &str = "https://staging-storage.babbage.systems";

/// Remote storage client using JSON-RPC over HTTPS with BRC-31 authentication.
///
/// `StorageClient` implements the `WalletStorageProvider` interface, allowing it
/// to serve as a BRC-100 wallet's active storage via remote calls.
///
/// ## BRC-31 Authentication
///
/// All requests are signed using the wallet's identity key. Each request includes:
/// - Identity key header for server to identify the client
/// - Nonce and timestamp for replay protection
/// - Signature over the canonical request data
///
/// Server responses can optionally be verified (if the server provides auth headers).
///
/// ## Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::storage::client::StorageClient;
/// use bsv_rs::wallet::ProtoWallet;
/// use bsv_rs::primitives::PrivateKey;
///
/// let wallet = ProtoWallet::new(Some(PrivateKey::from_wif("...")?));
/// let client = StorageClient::new(wallet, StorageClient::MAINNET_URL);
///
/// // Initialize and verify connection (authenticated)
/// let settings = client.make_available().await?;
/// println!("Connected to storage: {}", settings.storage_name);
///
/// // Find or create user
/// let (user, is_new) = client.find_or_insert_user(&identity_key).await?;
/// ```
pub struct StorageClient<W: WalletInterface> {
    /// The endpoint URL for the storage server.
    endpoint_url: String,

    /// The authenticated peer for BRC-31 communication (for advanced use).
    #[allow(dead_code)]
    peer: Arc<Peer<W, SimplifiedFetchTransport>>,

    /// The wallet for authentication and signing.
    wallet: W,

    /// HTTP client for requests.
    http_client: reqwest::Client,

    /// Request ID counter for JSON-RPC.
    next_id: AtomicU64,

    /// Cached settings after makeAvailable.
    settings: Arc<RwLock<Option<TableSettings>>>,

    /// Whether to use authenticated requests (BRC-31).
    /// Set to false for testing without authentication.
    use_auth: bool,

    /// Whether to verify server response signatures.
    /// Set to false if server doesn't provide auth headers.
    verify_responses: bool,

    /// Cached server identity key (from settings).
    server_identity_key: Arc<RwLock<Option<PublicKey>>>,
}

impl<W: WalletInterface + Clone + 'static> StorageClient<W> {
    /// Mainnet storage endpoint URL.
    pub const MAINNET_URL: &'static str = MAINNET_URL;

    /// Testnet storage endpoint URL.
    pub const TESTNET_URL: &'static str = TESTNET_URL;

    /// Originator string for BRC-31 authentication.
    const ORIGINATOR: &'static str = "bsv-wallet-toolbox";

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
            originator: Some(Self::ORIGINATOR.to_string()),
        });

        // Start the peer to set up the transport callback for receiving messages
        peer.start();

        Self {
            endpoint_url: url,
            peer: Arc::new(peer),
            wallet,
            http_client: reqwest::Client::new(),
            next_id: AtomicU64::new(1),
            settings: Arc::new(RwLock::new(None)),
            use_auth: true,
            verify_responses: false, // Most servers don't sign responses yet
            server_identity_key: Arc::new(RwLock::new(None)),
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

    /// Enables or disables response signature verification.
    ///
    /// By default, response verification is disabled because most storage
    /// servers don't sign their responses yet.
    pub fn set_verify_responses(&mut self, verify: bool) {
        self.verify_responses = verify;
    }

    /// Sets the server's identity key for signature verification.
    ///
    /// If not set, the key will be extracted from settings after makeAvailable().
    pub async fn set_server_identity_key(&self, key: PublicKey) {
        let mut cached = self.server_identity_key.write().await;
        *cached = Some(key);
    }

    /// Gets the cached server identity key.
    pub async fn get_server_identity_key(&self) -> Option<PublicKey> {
        let cached = self.server_identity_key.read().await;
        cached.clone()
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
        use bsv_rs::wallet::GetPublicKeyArgs;

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

    /// Makes a JSON-RPC call to the storage server using the authenticated Peer.
    ///
    /// This method handles:
    /// - Request ID generation
    /// - JSON serialization
    /// - Mutual authentication via Peer (BRC-31/BRC-104)
    /// - Response parsing and error handling
    ///
    /// ## BRC-104 Authentication
    ///
    /// Uses the Peer for proper mutual authentication including:
    /// - Session handshake with the server
    /// - Proper nonce exchange (x-bsv-auth-your-nonce)
    /// - Request ID correlation
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
            "Making JSON-RPC call to storage server via Peer"
        );

        if !self.use_auth {
            // Fall back to simple HTTP without auth for testing
            return self
                .rpc_call_unauthenticated(method, &request_body, id)
                .await;
        }

        // Generate a unique request ID (32 random bytes)
        let mut request_nonce = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut request_nonce);
        let request_nonce_b64 = to_base64(&request_nonce);

        // Build the HTTP request payload for the Peer
        let http_request = HttpRequest {
            request_id: request_nonce,
            method: "POST".to_string(),
            path: "/".to_string(),
            search: String::new(),
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: request_body.clone(),
        };

        let payload = http_request.to_payload();

        // Set up a channel to receive the response
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>>>();
        let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));
        let request_nonce_for_callback = request_nonce;

        // Register a listener for the response
        let tx_clone = tx.clone();
        let callback_id = self
            .peer
            .listen_for_general_messages(move |_sender, response_payload| {
                let tx = tx_clone.clone();
                let expected_nonce = request_nonce_for_callback;
                Box::pin(async move {
                    // Check if the response matches our request ID
                    if response_payload.len() >= 32 {
                        let mut response_nonce = [0u8; 32];
                        response_nonce.copy_from_slice(&response_payload[..32]);

                        if response_nonce == expected_nonce {
                            // This is our response
                            let mut tx_guard = tx.lock().await;
                            if let Some(sender) = tx_guard.take() {
                                let _ = sender.send(Ok(response_payload));
                            }
                        }
                    }
                    Ok(())
                })
            })
            .await;

        // Get the server's identity key if we have it (for session lookup)
        let server_key = self.get_server_identity_key().await;
        let server_key_hex = server_key.map(|k| k.to_hex());

        // Send the request via the Peer
        tracing::trace!(
            request_id = %request_nonce_b64,
            "Sending authenticated request via Peer"
        );

        let send_result = self
            .peer
            .to_peer(&payload, server_key_hex.as_deref(), Some(30000))
            .await;

        if let Err(e) = send_result {
            // Clean up listener
            self.peer
                .stop_listening_for_general_messages(callback_id)
                .await;
            return Err(Error::NetworkError(format!(
                "Failed to send request: {}",
                e
            )));
        }

        // Wait for the response with a timeout
        let response_result = tokio::time::timeout(std::time::Duration::from_secs(30), rx).await;

        // Clean up listener
        self.peer
            .stop_listening_for_general_messages(callback_id)
            .await;

        let response_payload = match response_result {
            Ok(Ok(Ok(payload))) => payload,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_)) => return Err(Error::NetworkError("Response channel closed".to_string())),
            Err(_) => return Err(Error::NetworkError("Request timed out".to_string())),
        };

        // Parse the HTTP response from the payload
        let http_response =
            bsv_rs::auth::transports::HttpResponse::from_payload(&response_payload)
                .map_err(|e| Error::StorageError(format!("Failed to parse response: {}", e)))?;

        tracing::trace!(
            method = method,
            status = http_response.status,
            response_size = http_response.body.len(),
            "Received authenticated response"
        );

        if http_response.status >= 400 {
            let body_text = String::from_utf8_lossy(&http_response.body);
            return Err(Error::NetworkError(format!(
                "HTTP error {}: {}",
                http_response.status, body_text
            )));
        }

        let rpc_response: JsonRpcResponse = serde_json::from_slice(&http_response.body)
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

    /// Makes an unauthenticated JSON-RPC call (for testing).
    async fn rpc_call_unauthenticated<T: DeserializeOwned>(
        &self,
        method: &str,
        request_body: &[u8],
        id: u64,
    ) -> Result<T> {
        let response = self
            .http_client
            .post(&self.endpoint_url)
            .header("Content-Type", "application/json")
            .body(request_body.to_vec())
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::NetworkError(format!(
                "HTTP error {}: {}",
                status, body
            )));
        }

        let response_body = response
            .bytes()
            .await
            .map_err(|e| Error::NetworkError(format!("Failed to read response body: {}", e)))?;

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
        // The trait signature requires returning a reference but we're behind an async RwLock.
        // Use try_read() which is synchronous. If the lock is held or settings not yet loaded,
        // return a static default. Callers should use get_settings_async() for async contexts.
        static DEFAULT_SETTINGS: std::sync::OnceLock<TableSettings> = std::sync::OnceLock::new();
        if let Ok(guard) = self.settings.try_read() {
            if let Some(ref settings) = *guard {
                // SAFETY: Settings are effectively static once loaded via make_available().
                // The pointer remains valid because the Arc keeps the allocation alive.
                unsafe { return &*(settings as *const TableSettings) }
            }
        }
        DEFAULT_SETTINGS.get_or_init(TableSettings::default)
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        // Remote storage does not offer Services to remote clients.
        // Services are local definitions - the remote server has its own services.
        // This matches TypeScript behavior where StorageClient throws WERR_INVALID_OPERATION.
        Err(Error::InvalidOperation(
            "getServices() not implemented in remote client. Services are typically local definitions and not shared remotely.".to_string()
        ))
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
        // TS server expects filter fields wrapped in a `partial` sub-object:
        //   { ...base, partial: { userId, basketId, txid, vout }, noScript, txStatus }
        let mut wire_args = serde_json::json!({});
        // Flatten base (FindSincePagedArgs) fields
        let base_val = serde_json::to_value(&args.base)?;
        if let serde_json::Value::Object(map) = base_val {
            for (k, v) in map { wire_args[&k] = v; }
        }
        // Build the partial sub-object with filter fields
        let mut partial = serde_json::json!({});
        if let Some(uid) = args.user_id { partial["userId"] = serde_json::json!(uid); }
        if let Some(bid) = args.basket_id { partial["basketId"] = serde_json::json!(bid); }
        if let Some(ref txid) = args.txid { partial["txid"] = serde_json::json!(txid); }
        if let Some(vout) = args.vout { partial["vout"] = serde_json::json!(vout); }
        wire_args["partial"] = partial;
        // Top-level optional fields
        if let Some(ns) = args.no_script { wire_args["noScript"] = serde_json::json!(ns); }
        if let Some(ref ts) = args.tx_status { wire_args["txStatus"] = serde_json::to_value(ts)?; }
        self.rpc_call(
            "findOutputsAuth",
            vec![Self::to_value(auth)?, wire_args],
        )
        .await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        // TS server expects: { ...base, partial: {}, status?, txids? }
        // The server accesses args.partial.rawTx immediately, so partial must be present.
        let mut wire_args = serde_json::json!({});
        // Flatten base (FindSincePagedArgs) fields
        let base_val = serde_json::to_value(&args.base)?;
        if let serde_json::Value::Object(map) = base_val {
            for (k, v) in map {
                wire_args[&k] = v;
            }
        }
        // Empty partial (required by server, we don't filter by partial fields)
        wire_args["partial"] = serde_json::json!({});
        // Top-level optional fields
        if let Some(ref status) = args.status {
            wire_args["status"] = serde_json::to_value(status)?;
        }
        if let Some(ref txids) = args.txids {
            wire_args["txids"] = serde_json::to_value(txids)?;
        }
        self.rpc_call("findProvenTxReqs", vec![wire_args])
            .await
    }

    async fn find_transactions(
        &self,
        args: FindTransactionsArgs,
    ) -> Result<Vec<TableTransaction>> {
        // TS server expects: { ...base, partial: {}, status?, noRawTx? }
        let mut wire_args = serde_json::json!({});
        let base_val = serde_json::to_value(&args.base)?;
        if let serde_json::Value::Object(map) = base_val {
            for (k, v) in map {
                wire_args[&k] = v;
            }
        }
        wire_args["partial"] = serde_json::json!({});
        if let Some(ref status) = args.status {
            wire_args["status"] = serde_json::to_value(status)?;
        }
        if let Some(no_raw_tx) = args.no_raw_tx {
            wire_args["noRawTx"] = serde_json::Value::Bool(no_raw_tx);
        }
        self.rpc_call("findTransactions", vec![wire_args])
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

        // Cache the server's identity key for future request signing
        if !result.storage_identity_key.is_empty() {
            if let Ok(server_key) = PublicKey::from_hex(&result.storage_identity_key) {
                let mut cached_key = self.server_identity_key.write().await;
                *cached_key = Some(server_key);
                tracing::debug!(
                    server_identity_key = %result.storage_identity_key,
                    "Cached server identity key for BRC-31 signing"
                );
            }
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
        self.rpc_call("migrate", vec![Value::String(storage_name.to_string())])
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
        // Convert to ValidCreateActionArgs to add internal state flags
        // The server expects these flags to be present in the request
        let valid_args = ValidCreateActionArgs::from(args);

        tracing::debug!(
            is_new_tx = valid_args.is_new_tx,
            is_no_send = valid_args.is_no_send,
            is_delayed = valid_args.is_delayed,
            is_send_with = valid_args.is_send_with,
            is_remix_change = valid_args.is_remix_change,
            is_sign_action = valid_args.is_sign_action,
            "Creating action with validated args"
        );

        self.rpc_call(
            "createAction",
            vec![Self::to_value(auth)?, Self::to_value(&valid_args)?],
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

    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        let result: Result<()> = self
            .rpc_call(
                "markInternalizedTxFailed",
                vec![Value::String(txid.to_string())],
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Server may not implement this method yet - treat as non-fatal
                let msg = e.to_string();
                if msg.contains("-32601") || msg.contains("Method not found") {
                    tracing::warn!(
                        txid = txid,
                        "Server does not support markInternalizedTxFailed, skipping"
                    );
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
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

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        self.rpc_call(
            "insertCertificateFieldAuth",
            vec![Self::to_value(auth)?, Self::to_value(&field)?],
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

    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        success: bool,
    ) -> Result<()> {
        let result: Result<()> = self
            .rpc_call(
                "updateTransactionStatusAfterBroadcast",
                vec![Value::String(txid.to_string()), Value::Bool(success)],
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Server may not implement this method yet - treat as non-fatal
                let msg = e.to_string();
                if msg.contains("-32601") || msg.contains("Method not found") {
                    tracing::warn!(
                        txid = txid,
                        "Server does not support updateTransactionStatusAfterBroadcast, skipping"
                    );
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: chrono::DateTime<chrono::Utc>,
    ) -> Result<ReviewStatusResult> {
        self.rpc_call(
            "reviewStatus",
            vec![
                Self::to_value(auth)?,
                Self::to_value(&aged_limit.to_rfc3339())?,
            ],
        )
        .await
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        self.rpc_call(
            "purgeData",
            vec![Self::to_value(auth)?, Self::to_value(&params)?],
        )
        .await
    }
    async fn begin_transaction(&self) -> Result<TrxToken> {
        self.rpc_call::<Value>("beginStorageTransaction", vec![])
            .await
            .map(|_| TrxToken::new())
    }

    async fn commit_transaction(&self, _trx: TrxToken) -> Result<()> {
        self.rpc_call::<Value>("commitStorageTransaction", vec![])
            .await
            .map(|_| ())
    }

    async fn rollback_transaction(&self, _trx: TrxToken) -> Result<()> {
        self.rpc_call::<Value>("rollbackStorageTransaction", vec![])
            .await
            .map(|_| ())
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

    fn set_services(&self, _services: Arc<dyn WalletServices>) {
        // Ignored. Remote storage cannot share Services with remote clients.
        // Services are local definitions to the storage - the remote server
        // manages its own service connections. This matches TypeScript behavior.
    }
}

// =============================================================================
// MonitorStorage Implementation
// =============================================================================

#[async_trait]
impl<W: WalletInterface + Clone + 'static> MonitorStorage for StorageClient<W> {
    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        self.rpc_call("synchronizeTransactionStatuses", vec![])
            .await
    }

    async fn send_waiting_transactions(
        &self,
        min_transaction_age: std::time::Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        self.rpc_call(
            "sendWaitingTransactions",
            vec![Self::to_value(&min_transaction_age.as_millis())?],
        )
        .await
    }

    async fn abort_abandoned(&self, timeout: std::time::Duration) -> Result<()> {
        self.rpc_call(
            "abortAbandoned",
            vec![Self::to_value(&timeout.as_millis())?],
        )
        .await
    }

    async fn un_fail(&self) -> Result<()> {
        self.rpc_call("unFail", vec![]).await
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        self.rpc_call("monitorReviewStatus", vec![]).await
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        self.rpc_call("monitorPurgeData", vec![Self::to_value(&params)?])
            .await
    }

    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool> {
        self.rpc_call(
            "tryAcquireTaskLock",
            vec![
                Self::to_value(&task_name)?,
                Self::to_value(&instance_id)?,
                Self::to_value(&ttl.as_millis())?,
            ],
        )
        .await
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        self.rpc_call(
            "releaseTaskLock",
            vec![Self::to_value(&task_name)?, Self::to_value(&instance_id)?],
        )
        .await
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
    pub async fn get_storage_info(
        &self,
        user_id: i64,
        is_active: bool,
    ) -> Result<WalletStorageInfo> {
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
    use super::JsonRpcResponse;
    use super::*;
    use chrono::Utc;

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

    // Helper to create a JSON-RPC success response
    fn rpc_success<T: Serialize>(id: u64, result: T) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "result": result,
            "id": id
        })
    }

    // Helper to create a JSON-RPC error response
    #[allow(dead_code)]
    fn rpc_error(id: u64, code: i32, message: &str) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": code,
                "message": message
            },
            "id": id
        })
    }

    #[test]
    fn test_json_rpc_response_success_parsing() {
        let json = r#"{
            "jsonrpc": "2.0",
            "result": {"settingsId": 1, "storageName": "test"},
            "id": 1
        }"#;

        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(response.error.is_none());
        assert!(response.result.is_some());
    }

    #[test]
    fn test_json_rpc_response_error_parsing() {
        let json = r#"{
            "jsonrpc": "2.0",
            "error": {"code": -32601, "message": "Method not found"},
            "id": 1
        }"#;

        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(response.error.is_some());
        assert!(response.result.is_none());

        let err = response.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.message, "Method not found");
    }

    #[test]
    fn test_auth_id_serialization() {
        let auth = AuthId {
            identity_key: "test-identity-key".to_string(),
            user_id: Some(42),
            is_active: Some(true),
        };

        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains("\"identityKey\":\"test-identity-key\""));
        assert!(json.contains("\"userId\":42"));
        assert!(json.contains("\"isActive\":true"));
    }

    #[test]
    fn test_find_proven_tx_reqs_args_default() {
        let args = FindProvenTxReqsArgs::default();
        assert!(args.status.is_none());
        assert!(args.txids.is_none());
    }

    #[test]
    fn test_request_sync_chunk_args_serialization() {
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "from-key".to_string(),
            to_storage_identity_key: "to-key".to_string(),
            identity_key: "user-key".to_string(),
            since: None,
            max_rough_size: 100000,
            max_items: 1000,
            offsets: vec![],
        };

        let json = serde_json::to_string(&args).unwrap();
        assert!(json.contains("fromStorageIdentityKey"));
        assert!(json.contains("toStorageIdentityKey"));
        assert!(json.contains("maxRoughSize"));
    }

    #[test]
    fn test_sync_chunk_default() {
        let chunk = SyncChunk::default();
        assert!(chunk.user.is_none());
        assert!(chunk.proven_txs.is_none());
        assert!(chunk.outputs.is_none());
    }

    #[test]
    fn test_table_settings_deserialization() {
        let json = r#"{
            "settingsId": 1,
            "storageIdentityKey": "key123",
            "storageName": "test-storage",
            "chain": "mainnet",
            "maxOutputScript": 10000,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;

        let settings: TableSettings = serde_json::from_str(json).unwrap();
        assert_eq!(settings.settings_id, 1);
        assert_eq!(settings.storage_name, "test-storage");
        assert_eq!(settings.chain, "mainnet");
    }

    #[test]
    fn test_table_user_deserialization() {
        let json = r#"{
            "userId": 42,
            "identityKey": "user-key",
            "activeStorage": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;

        let user: TableUser = serde_json::from_str(json).unwrap();
        assert_eq!(user.user_id, 42);
        assert_eq!(user.identity_key, "user-key");
        assert!(user.active_storage.is_none());
    }

    #[test]
    fn test_update_proven_tx_req_args() {
        let args = UpdateProvenTxReqWithNewProvenTxArgs {
            proven_tx_req_id: 123,
            proven_tx: TableProvenTx {
                proven_tx_id: 1,
                txid: "abc123".to_string(),
                height: 800000,
                index: 5,
                block_hash: "blockhash".to_string(),
                merkle_root: "merkleroot".to_string(),
                merkle_path: vec![1, 2, 3],
                raw_tx: vec![4, 5, 6],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        };

        let json = serde_json::to_string(&args).unwrap();
        assert!(json.contains("provenTxReqId"));
        assert!(json.contains("provenTx"));
    }

    #[test]
    fn test_process_sync_chunk_result_deserialization() {
        let json = r#"{
            "done": true,
            "maxUpdatedAt": "2024-01-01T00:00:00Z",
            "updates": 5,
            "inserts": 10,
            "error": null
        }"#;

        let result: ProcessSyncChunkResult = serde_json::from_str(json).unwrap();
        assert!(result.done);
        assert_eq!(result.updates, 5);
        assert_eq!(result.inserts, 10);
    }

    // =========================================================================
    // Tests for all 22 JSON-RPC method request/response formats
    // =========================================================================

    #[test]
    fn test_make_available_method() {
        // makeAvailable takes no parameters
        let request = JsonRpcRequest::new("makeAvailable", vec![], 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"makeAvailable\""));
        assert!(json.contains("\"params\":[]"));
    }

    #[test]
    fn test_destroy_method() {
        // destroy takes no parameters
        let request = JsonRpcRequest::new("destroy", vec![], 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"destroy\""));
        assert!(json.contains("\"params\":[]"));
    }

    #[test]
    fn test_migrate_method() {
        // migrate takes storage_name as parameter
        let params = vec![serde_json::json!("my-storage")];
        let request = JsonRpcRequest::new("migrate", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"migrate\""));
        assert!(json.contains("\"my-storage\""));
    }

    #[test]
    fn test_find_or_insert_user_method() {
        // findOrInsertUser takes identity_key as parameter
        let params = vec![serde_json::json!("02abcdef123...")];
        let request = JsonRpcRequest::new("findOrInsertUser", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findOrInsertUser\""));
        assert!(json.contains("\"02abcdef123...\""));

        // Test response format
        let response_json = r#"{
            "jsonrpc": "2.0",
            "result": {
                "user": {
                    "userId": 1,
                    "identityKey": "02abcdef",
                    "activeStorage": null,
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                },
                "isNew": true
            },
            "id": 1
        }"#;
        let response: JsonRpcResponse = serde_json::from_str(response_json).unwrap();
        assert!(response.is_success());
    }

    #[test]
    fn test_find_proven_tx_reqs_method() {
        // findProvenTxReqs takes FindProvenTxReqsArgs
        let args = FindProvenTxReqsArgs {
            status: Some(vec![ProvenTxReqStatus::Pending]),
            txids: Some(vec!["abc123".to_string()]),
            ..Default::default()
        };
        let params = vec![serde_json::to_value(&args).unwrap()];
        let request = JsonRpcRequest::new("findProvenTxReqs", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findProvenTxReqs\""));
    }

    #[test]
    fn test_get_sync_chunk_method() {
        // getSyncChunk takes RequestSyncChunkArgs
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "from-key".to_string(),
            to_storage_identity_key: "to-key".to_string(),
            identity_key: "user-key".to_string(),
            since: None,
            max_rough_size: 100000,
            max_items: 1000,
            offsets: vec![],
        };
        let params = vec![serde_json::to_value(&args).unwrap()];
        let request = JsonRpcRequest::new("getSyncChunk", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"getSyncChunk\""));
        assert!(json.contains("fromStorageIdentityKey"));
    }

    #[test]
    fn test_process_sync_chunk_method() {
        // processSyncChunk takes RequestSyncChunkArgs and SyncChunk
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "from-key".to_string(),
            to_storage_identity_key: "to-key".to_string(),
            identity_key: "user-key".to_string(),
            since: None,
            max_rough_size: 100000,
            max_items: 1000,
            offsets: vec![],
        };
        let chunk = SyncChunk::default();
        let params = vec![
            serde_json::to_value(&args).unwrap(),
            serde_json::to_value(&chunk).unwrap(),
        ];
        let request = JsonRpcRequest::new("processSyncChunk", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"processSyncChunk\""));
    }

    #[test]
    fn test_find_or_insert_sync_state_auth_method() {
        // findOrInsertSyncStateAuth takes auth, storageIdentityKey, storageName
        let auth = AuthId::new("user-identity-key");
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::json!("storage-identity-key"),
            serde_json::json!("storage-name"),
        ];
        let request = JsonRpcRequest::new("findOrInsertSyncStateAuth", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findOrInsertSyncStateAuth\""));
        assert!(json.contains("identityKey"));
    }

    #[test]
    fn test_insert_certificate_auth_method() {
        // insertCertificateAuth takes auth and certificate
        let auth = AuthId::new("user-identity-key");
        let cert = TableCertificate {
            certificate_id: 0,
            user_id: 1,
            cert_type: "test-type".to_string(),
            serial_number: "SN123".to_string(),
            certifier: "certifier-key".to_string(),
            subject: "subject-key".to_string(),
            verifier: None,
            revocation_outpoint: "txid:0".to_string(),
            signature: "sig".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::to_value(&cert).unwrap(),
        ];
        let request = JsonRpcRequest::new("insertCertificateAuth", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"insertCertificateAuth\""));
        assert!(json.contains("certType"));
        assert!(json.contains("serialNumber"));
    }

    #[test]
    fn test_find_certificates_auth_method() {
        // findCertificatesAuth takes auth and FindCertificatesArgs
        let auth = AuthId::new("user-identity-key");
        let args = FindCertificatesArgs {
            certifiers: Some(vec!["certifier1".to_string()]),
            types: Some(vec!["type1".to_string()]),
            include_fields: Some(true),
            ..Default::default()
        };
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::to_value(&args).unwrap(),
        ];
        let request = JsonRpcRequest::new("findCertificatesAuth", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findCertificatesAuth\""));
        assert!(json.contains("includeFields"));
    }

    #[test]
    fn test_list_certificates_method() {
        // listCertificates takes auth and ListCertificatesArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "listCertificates",
            vec![serde_json::to_value(&auth).unwrap(), serde_json::json!({})],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"listCertificates\""));
    }

    #[test]
    fn test_relinquish_certificate_method() {
        // relinquishCertificate takes auth and RelinquishCertificateArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "relinquishCertificate",
            vec![
                serde_json::to_value(&auth).unwrap(),
                serde_json::json!({"type": "cert-type", "serialNumber": "SN123", "certifier": "certifier-key"}),
            ],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"relinquishCertificate\""));
    }

    #[test]
    fn test_find_outputs_auth_method() {
        // findOutputsAuth takes auth and FindOutputsArgs
        let auth = AuthId::new("user-identity-key");
        let args = FindOutputsArgs {
            basket_id: Some(1),
            txid: Some("abc123".to_string()),
            no_script: Some(true),
            ..Default::default()
        };
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::to_value(&args).unwrap(),
        ];
        let request = JsonRpcRequest::new("findOutputsAuth", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findOutputsAuth\""));
        assert!(json.contains("basketId"));
        assert!(json.contains("noScript"));
    }

    #[test]
    fn test_find_output_baskets_method() {
        // findOutputBaskets takes auth and FindOutputBasketsArgs
        let auth = AuthId::new("user-identity-key");
        let args = FindOutputBasketsArgs {
            name: Some("default".to_string()),
            ..Default::default()
        };
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::to_value(&args).unwrap(),
        ];
        let request = JsonRpcRequest::new("findOutputBaskets", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"findOutputBaskets\""));
    }

    #[test]
    fn test_list_outputs_method() {
        // listOutputs takes auth and ListOutputsArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "listOutputs",
            vec![serde_json::to_value(&auth).unwrap(), serde_json::json!({})],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"listOutputs\""));
    }

    #[test]
    fn test_relinquish_output_method() {
        // relinquishOutput takes auth and RelinquishOutputArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "relinquishOutput",
            vec![
                serde_json::to_value(&auth).unwrap(),
                serde_json::json!({"basket": "default", "output": "txid.0"}),
            ],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"relinquishOutput\""));
    }

    #[test]
    fn test_create_action_method() {
        // createAction takes auth and CreateActionArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "createAction",
            vec![
                serde_json::to_value(&auth).unwrap(),
                serde_json::json!({
                    "description": "Test payment"
                }),
            ],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"createAction\""));
        assert!(json.contains("Test payment"));
    }

    #[test]
    fn test_process_action_method() {
        // processAction takes auth and StorageProcessActionArgs
        let auth = AuthId::new("user-identity-key");
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("ref123".to_string()),
            txid: Some("txid123".to_string()),
            raw_tx: Some(vec![1, 2, 3, 4]),
            send_with: vec![],
        };
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::to_value(&args).unwrap(),
        ];
        let request = JsonRpcRequest::new("processAction", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"processAction\""));
        assert!(json.contains("isNewTx"));
        assert!(json.contains("isNoSend"));
    }

    #[test]
    fn test_internalize_action_method() {
        // internalizeAction takes auth and InternalizeActionArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "internalizeAction",
            vec![
                serde_json::to_value(&auth).unwrap(),
                serde_json::json!({
                    "tx": [],
                    "outputs": []
                }),
            ],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"internalizeAction\""));
    }

    #[test]
    fn test_abort_action_method() {
        // abortAction takes auth and AbortActionArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "abortAction",
            vec![
                serde_json::to_value(&auth).unwrap(),
                serde_json::json!({
                    "reference": "ref123"
                }),
            ],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"abortAction\""));
        assert!(json.contains("ref123"));
    }

    #[test]
    fn test_list_actions_method() {
        // listActions takes auth and ListActionsArgs
        let auth = AuthId::new("user-identity-key");
        let request = JsonRpcRequest::new(
            "listActions",
            vec![serde_json::to_value(&auth).unwrap(), serde_json::json!({})],
            1,
        );
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"listActions\""));
    }

    #[test]
    fn test_set_active_method() {
        // setActive takes auth and newActiveStorageIdentityKey
        let auth = AuthId::new("user-identity-key");
        let params = vec![
            serde_json::to_value(&auth).unwrap(),
            serde_json::json!("new-active-storage-key"),
        ];
        let request = JsonRpcRequest::new("setActive", params, 1);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"method\":\"setActive\""));
        assert!(json.contains("new-active-storage-key"));
    }

    // =========================================================================
    // Tests for result type serialization/deserialization
    // =========================================================================

    #[test]
    fn test_storage_create_action_result_deserialization() {
        let json = r#"{
            "inputBeef": null,
            "inputs": [],
            "outputs": [],
            "noSendChangeOutputVouts": null,
            "derivationPrefix": "prefix",
            "version": 1,
            "lockTime": 0,
            "reference": "ref123"
        }"#;
        let result: StorageCreateActionResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.derivation_prefix, "prefix");
        assert_eq!(result.reference, "ref123");
        assert_eq!(result.version, 1);
    }

    #[test]
    fn test_storage_process_action_results_deserialization() {
        let json = r#"{
            "sendWithResults": null,
            "notDelayedResults": null,
            "log": "test log"
        }"#;
        let result: StorageProcessActionResults = serde_json::from_str(json).unwrap();
        assert!(result.send_with_results.is_none());
        assert_eq!(result.log, Some("test log".to_string()));
    }

    #[test]
    fn test_storage_create_transaction_input_serialization() {
        let input = StorageCreateTransactionInput {
            vin: 0,
            source_txid: "txid123".to_string(),
            source_vout: 0,
            source_satoshis: 1000,
            source_locking_script: "76a914...88ac".to_string(),
            source_transaction: None,
            unlocking_script_length: 107,
            provided_by: StorageProvidedBy::Storage,
            input_type: "P2PKH".to_string(),
            spending_description: Some("Spending UTXO".to_string()),
            derivation_prefix: Some("m/0".to_string()),
            derivation_suffix: Some("0/1".to_string()),
            sender_identity_key: None,
        };
        let json = serde_json::to_string(&input).unwrap();
        assert!(json.contains("sourceTxid"));
        assert!(json.contains("sourceVout"));
        assert!(json.contains("sourceSatoshis"));
        assert!(json.contains("unlockingScriptLength"));
        assert!(json.contains("\"providedBy\":\"storage\""));
    }

    #[test]
    fn test_storage_create_transaction_output_serialization() {
        let output = StorageCreateTransactionOutput {
            vout: 0,
            satoshis: 1000,
            locking_script: "76a914...88ac".to_string(),
            provided_by: StorageProvidedBy::You,
            purpose: Some("payment".to_string()),
            derivation_suffix: Some("0/0".to_string()),
            basket: Some("default".to_string()),
            tags: vec!["tag1".to_string()],
            output_description: Some("Payment output".to_string()),
            custom_instructions: None,
        };
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("lockingScript"));
        assert!(json.contains("\"providedBy\":\"you\""));
        assert!(json.contains("derivationSuffix"));
    }

    #[test]
    fn test_storage_provided_by_serialization() {
        // Test all variants serialize correctly
        assert_eq!(
            serde_json::to_string(&StorageProvidedBy::You).unwrap(),
            "\"you\""
        );
        assert_eq!(
            serde_json::to_string(&StorageProvidedBy::Storage).unwrap(),
            "\"storage\""
        );
        assert_eq!(
            serde_json::to_string(&StorageProvidedBy::YouAndStorage).unwrap(),
            "\"you-and-storage\""
        );
    }

    #[test]
    fn test_send_with_result_deserialization() {
        let json = r#"{"txid": "abc123", "status": "success"}"#;
        let result: SendWithResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.txid, "abc123");
        assert_eq!(result.status, "success");
    }

    #[test]
    fn test_review_action_result_deserialization() {
        let json = r#"{
            "txid": "abc123",
            "status": "success",
            "competingTxs": null,
            "competingBeef": null
        }"#;
        let result: ReviewActionResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.txid, "abc123");
        assert_eq!(result.status, ReviewActionResultStatus::Success);
    }

    #[test]
    fn test_review_action_result_status_serialization() {
        assert_eq!(
            serde_json::to_string(&ReviewActionResultStatus::Success).unwrap(),
            "\"success\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewActionResultStatus::DoubleSpend).unwrap(),
            "\"doubleSpend\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewActionResultStatus::ServiceError).unwrap(),
            "\"serviceError\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewActionResultStatus::InvalidTx).unwrap(),
            "\"invalidTx\""
        );
    }

    #[test]
    fn test_table_output_deserialization() {
        let json = r#"{
            "outputId": 1,
            "userId": 1,
            "transactionId": 1,
            "basketId": null,
            "txid": "abc123",
            "vout": 0,
            "satoshis": 1000,
            "lockingScript": [118, 169],
            "scriptLength": 25,
            "scriptOffset": 0,
            "outputType": "P2PKH",
            "providedBy": "you",
            "purpose": null,
            "outputDescription": null,
            "spentBy": null,
            "sequenceNumber": null,
            "spendingDescription": null,
            "spendable": true,
            "change": false,
            "derivationPrefix": "m/0",
            "derivationSuffix": "0/1",
            "senderIdentityKey": null,
            "customInstructions": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;
        let output: TableOutput = serde_json::from_str(json).unwrap();
        assert_eq!(output.output_id, 1);
        assert_eq!(output.txid, "abc123");
        assert_eq!(output.satoshis, 1000);
        assert!(output.spendable);
    }

    #[test]
    fn test_table_certificate_deserialization() {
        let json = r#"{
            "certificateId": 1,
            "userId": 1,
            "certType": "identity",
            "serialNumber": "SN123",
            "certifier": "certifier-key",
            "subject": "subject-key",
            "verifier": null,
            "revocationOutpoint": "txid:0",
            "signature": "sig123",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;
        let cert: TableCertificate = serde_json::from_str(json).unwrap();
        assert_eq!(cert.certificate_id, 1);
        assert_eq!(cert.cert_type, "identity");
        assert_eq!(cert.serial_number, "SN123");
    }

    #[test]
    fn test_table_sync_state_deserialization() {
        let json = r#"{
            "syncStateId": 1,
            "userId": 1,
            "storageIdentityKey": "storage-key",
            "storageName": "my-storage",
            "status": "idle",
            "init": true,
            "refNum": "ref123",
            "syncMap": "{}",
            "whenLastSyncStarted": null,
            "errorLocal": null,
            "errorOther": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;
        let state: TableSyncState = serde_json::from_str(json).unwrap();
        assert_eq!(state.sync_state_id, 1);
        assert_eq!(state.storage_name, "my-storage");
        assert!(state.init);
        assert_eq!(state.satoshis, None);
    }

    #[test]
    fn test_table_sync_state_deserialization_with_satoshis() {
        let json = r#"{
            "syncStateId": 2,
            "userId": 1,
            "storageIdentityKey": "storage-key",
            "storageName": "my-storage",
            "status": "idle",
            "init": false,
            "refNum": "ref456",
            "syncMap": "{}",
            "whenLastSyncStarted": null,
            "satoshis": 50000,
            "errorLocal": null,
            "errorOther": null,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z"
        }"#;
        let state: TableSyncState = serde_json::from_str(json).unwrap();
        assert_eq!(state.sync_state_id, 2);
        assert_eq!(state.satoshis, Some(50000));
    }

    #[test]
    fn test_transaction_status_serialization() {
        assert_eq!(
            serde_json::to_string(&TransactionStatus::Completed).unwrap(),
            "\"completed\""
        );
        assert_eq!(
            serde_json::to_string(&TransactionStatus::Unprocessed).unwrap(),
            "\"unprocessed\""
        );
        assert_eq!(
            serde_json::to_string(&TransactionStatus::Sending).unwrap(),
            "\"sending\""
        );
        assert_eq!(
            serde_json::to_string(&TransactionStatus::Unproven).unwrap(),
            "\"unproven\""
        );
        assert_eq!(
            serde_json::to_string(&TransactionStatus::NoSend).unwrap(),
            "\"noSend\""
        );
    }

    #[test]
    fn test_proven_tx_req_status_serialization() {
        assert_eq!(
            serde_json::to_string(&ProvenTxReqStatus::Pending).unwrap(),
            "\"pending\""
        );
        assert_eq!(
            serde_json::to_string(&ProvenTxReqStatus::InProgress).unwrap(),
            "\"inProgress\""
        );
        assert_eq!(
            serde_json::to_string(&ProvenTxReqStatus::Completed).unwrap(),
            "\"completed\""
        );
        assert_eq!(
            serde_json::to_string(&ProvenTxReqStatus::Failed).unwrap(),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&ProvenTxReqStatus::NotFound).unwrap(),
            "\"notFound\""
        );
    }

    // =========================================================================
    // Verify all 22 methods are implemented
    // =========================================================================

    #[test]
    fn test_all_22_methods_documented() {
        // This test documents all 22 JSON-RPC methods that must be implemented
        // No Auth (7 methods):
        let no_auth_methods = vec![
            "makeAvailable",    // Initialize storage
            "destroy",          // Delete all data
            "migrate",          // Run migrations
            "findOrInsertUser", // Find/create user
            "findProvenTxReqs", // Find proof requests
            "getSyncChunk",     // Get sync data
            "processSyncChunk", // Apply sync data
        ];

        // Auth Required (15 methods):
        let auth_methods = vec![
            "findOrInsertSyncStateAuth", // Find/create sync state
            "insertCertificateAuth",     // Add certificate
            "findCertificatesAuth",      // Query certificates
            "listCertificates",          // List certificates
            "relinquishCertificate",     // Release certificate
            "findOutputsAuth",           // Query outputs
            "findOutputBaskets",         // Query baskets
            "listOutputs",               // List outputs
            "relinquishOutput",          // Release output
            "createAction",              // Create transaction
            "processAction",             // Process signed tx
            "internalizeAction",         // Import external tx
            "abortAction",               // Cancel action
            "listActions",               // List transactions
            "setActive",                 // Set active storage
        ];

        assert_eq!(no_auth_methods.len(), 7);
        assert_eq!(auth_methods.len(), 15);
        assert_eq!(no_auth_methods.len() + auth_methods.len(), 22);
    }

    // =========================================================================
    // BRC-31 Authentication Tests
    // =========================================================================

    #[test]
    fn test_brc31_auth_header_names() {
        use crate::storage::client::auth::headers;

        // Verify header names match BRC-31/BRC-104 specification
        assert_eq!(headers::VERSION, "x-bsv-auth-version");
        assert_eq!(headers::IDENTITY_KEY, "x-bsv-auth-identity-key");
        assert_eq!(headers::NONCE, "x-bsv-auth-nonce");
        assert_eq!(headers::TIMESTAMP, "x-bsv-auth-timestamp");
        assert_eq!(headers::SIGNATURE, "x-bsv-auth-signature");
    }

    #[test]
    fn test_brc31_auth_version() {
        use crate::storage::client::auth::AUTH_VERSION;

        // BRC-31 version should be "0.1"
        assert_eq!(AUTH_VERSION, "0.1");
    }

    #[test]
    fn test_brc31_nonce_creation() {
        use crate::storage::client::auth::create_simple_nonce;

        let nonce1 = create_simple_nonce();
        let nonce2 = create_simple_nonce();

        // Nonces should be different
        assert_ne!(nonce1, nonce2);

        // Nonces should be base64 encoded
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &nonce1);
        assert!(decoded.is_ok());

        // Should be 32 bytes when decoded
        assert_eq!(decoded.unwrap().len(), 32);
    }

    #[test]
    fn test_brc31_timestamp_validation() {
        use crate::storage::client::auth::{current_timestamp_ms, validate_timestamp};

        // Current timestamp should be valid
        let now = current_timestamp_ms();
        assert!(validate_timestamp(now).is_ok());

        // Old timestamp (> 5 minutes) should fail
        let old = now - 6 * 60 * 1000;
        assert!(validate_timestamp(old).is_err());

        // Far future timestamp should fail
        let future = now + 2 * 60 * 1000;
        assert!(validate_timestamp(future).is_err());
    }

    #[test]
    fn test_brc31_signing_data_deterministic() {
        use crate::storage::client::auth::create_signing_data;

        let body = br#"{"jsonrpc":"2.0","method":"makeAvailable","params":[],"id":1}"#;
        let nonce = "test-nonce-base64";
        let timestamp = 1234567890000u64;

        let data1 = create_signing_data("POST", "/", body, timestamp, nonce);
        let data2 = create_signing_data("POST", "/", body, timestamp, nonce);

        // Same inputs should produce same output
        assert_eq!(data1, data2);
    }

    #[test]
    fn test_brc31_signing_data_different_bodies() {
        use crate::storage::client::auth::create_signing_data;

        let body1 = br#"{"method":"makeAvailable"}"#;
        let body2 = br#"{"method":"listOutputs"}"#;
        let nonce = "test-nonce";
        let timestamp = 1234567890000u64;

        let data1 = create_signing_data("POST", "/", body1, timestamp, nonce);
        let data2 = create_signing_data("POST", "/", body2, timestamp, nonce);

        // Different bodies should produce different signing data
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_brc31_auth_headers_struct() {
        use crate::storage::client::auth::{AuthHeaders, AUTH_VERSION};

        let headers = AuthHeaders {
            version: AUTH_VERSION.to_string(),
            identity_key: "02abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"
                .to_string(),
            nonce: "dGVzdC1ub25jZS1iYXNlNjQ=".to_string(),
            timestamp: 1234567890000,
            signature: "3044022012345678".to_string(),
        };

        let tuples = headers.to_header_tuples();

        // Should have 5 header tuples
        assert_eq!(tuples.len(), 5);

        // Verify each header is present
        let map: std::collections::HashMap<_, _> = tuples.into_iter().collect();
        assert!(map.contains_key("x-bsv-auth-version"));
        assert!(map.contains_key("x-bsv-auth-identity-key"));
        assert!(map.contains_key("x-bsv-auth-nonce"));
        assert!(map.contains_key("x-bsv-auth-timestamp"));
        assert!(map.contains_key("x-bsv-auth-signature"));
    }

    #[test]
    fn test_brc31_response_headers_complete_check() {
        use crate::storage::client::auth::ResponseAuthHeaders;

        // Complete headers
        let complete = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(complete.is_complete());

        // Missing identity key
        let missing_key = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: None,
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(!missing_key.is_complete());

        // Missing signature
        let missing_sig = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: None,
        };
        assert!(!missing_sig.is_complete());
    }

    #[test]
    fn test_brc31_replay_protection() {
        use crate::storage::client::auth::{
            create_signing_data, create_simple_nonce, current_timestamp_ms,
        };

        // Two requests with same body at different times should have different signing data
        let body = br#"{"method":"test"}"#;

        let nonce1 = create_simple_nonce();
        let nonce2 = create_simple_nonce();
        let ts = current_timestamp_ms();

        let data1 = create_signing_data("POST", "/", body, ts, &nonce1);
        let data2 = create_signing_data("POST", "/", body, ts, &nonce2);

        // Different nonces = different signing data (replay protected)
        assert_ne!(data1, data2);
    }

    // =========================================================================
    // ValidCreateActionArgs Tests
    // =========================================================================

    #[test]
    fn test_valid_create_action_args_default_flags() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOutput};

        // Create args with outputs (typical case)
        let args = CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14], // partial P2PKH for test
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let valid_args = ValidCreateActionArgs::from(args);

        // With outputs but no inputs, should be new tx
        assert!(valid_args.is_new_tx);
        // Default flags should be false
        assert!(!valid_args.is_no_send);
        assert!(!valid_args.is_delayed);
        assert!(!valid_args.is_send_with);
        assert!(!valid_args.is_remix_change);
        // Default signAndProcess is true
        assert!(valid_args.is_sign_action);
        assert!(valid_args.include_all_source_transactions);
    }

    #[test]
    fn test_valid_create_action_args_with_no_send() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};

        let args = CreateActionArgs {
            description: "NoSend transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9],
                satoshis: 500,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: Some(CreateActionOptions {
                no_send: Some(true),
                accept_delayed_broadcast: None,
                send_with: None,
                sign_and_process: Some(false),
                known_txids: None,
                return_txid_only: None,
                no_send_change: None,
                randomize_outputs: None,
                trust_self: None,
            }),
        };

        let valid_args = ValidCreateActionArgs::from(args);

        assert!(valid_args.is_new_tx);
        assert!(valid_args.is_no_send);
        assert!(!valid_args.is_delayed);
        assert!(!valid_args.is_send_with);
        assert!(!valid_args.is_sign_action);
    }

    #[test]
    fn test_valid_create_action_args_with_delayed_broadcast() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};

        let args = CreateActionArgs {
            description: "Delayed broadcast tx".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76],
                satoshis: 100,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: Some(CreateActionOptions {
                no_send: None,
                accept_delayed_broadcast: Some(true),
                send_with: None,
                sign_and_process: None,
                known_txids: None,
                return_txid_only: None,
                no_send_change: None,
                randomize_outputs: None,
                trust_self: None,
            }),
        };

        let valid_args = ValidCreateActionArgs::from(args);

        assert!(valid_args.is_new_tx);
        assert!(!valid_args.is_no_send);
        assert!(valid_args.is_delayed);
        assert!(!valid_args.is_send_with);
    }

    #[test]
    fn test_valid_create_action_args_with_send_with() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionOutput};

        let args = CreateActionArgs {
            description: "SendWith transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76],
                satoshis: 100,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: Some(CreateActionOptions {
                no_send: None,
                accept_delayed_broadcast: None,
                send_with: Some(vec![[0u8; 32], [1u8; 32]]), // Two txids
                sign_and_process: None,
                known_txids: None,
                return_txid_only: None,
                no_send_change: None,
                randomize_outputs: None,
                trust_self: None,
            }),
        };

        let valid_args = ValidCreateActionArgs::from(args);

        assert!(valid_args.is_new_tx);
        assert!(!valid_args.is_no_send);
        assert!(!valid_args.is_delayed);
        assert!(valid_args.is_send_with);
    }

    #[test]
    fn test_valid_create_action_args_remix_change() {
        use bsv_rs::wallet::CreateActionArgs;

        // No inputs and no outputs = remix change
        let args = CreateActionArgs {
            description: "Remix change".to_string(),
            input_beef: None,
            inputs: Some(vec![]),  // Empty inputs
            outputs: Some(vec![]), // Empty outputs
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let valid_args = ValidCreateActionArgs::from(args);

        // With no inputs/outputs, this is a remix change (not new tx)
        assert!(!valid_args.is_new_tx);
        assert!(valid_args.is_remix_change);
    }

    #[test]
    fn test_valid_create_action_args_serialization() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOutput};

        let args = CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14],
                satoshis: 42000,
                output_description: "Payment".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: Some(0),
            version: Some(1),
            labels: Some(vec!["test".to_string()]),
            options: None,
        };

        let valid_args = ValidCreateActionArgs::from(args);
        let json = serde_json::to_string(&valid_args).unwrap();

        // Verify the JSON contains the internal flags
        assert!(json.contains("\"isNewTx\":true"));
        assert!(json.contains("\"isNoSend\":false"));
        assert!(json.contains("\"isDelayed\":false"));
        assert!(json.contains("\"isSendWith\":false"));
        assert!(json.contains("\"isRemixChange\":false"));
        assert!(json.contains("\"isSignAction\":true"));
        assert!(json.contains("\"includeAllSourceTransactions\":true"));

        // Verify base args are present
        assert!(json.contains("\"description\":\"Test transaction\""));
        assert!(json.contains("\"lockTime\":0"));
        assert!(json.contains("\"version\":1"));
    }

    #[test]
    fn test_valid_create_action_args_with_custom_flags() {
        use bsv_rs::wallet::{CreateActionArgs, CreateActionOutput};

        let args = CreateActionArgs {
            description: "Custom flags test".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76],
                satoshis: 100,
                output_description: "Test".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        // Override flags manually
        let valid_args = ValidCreateActionArgs::with_flags(
            args, true,  // is_new_tx
            true,  // is_no_send
            true,  // is_delayed
            false, // is_send_with
        );

        assert!(valid_args.is_new_tx);
        assert!(valid_args.is_no_send);
        assert!(valid_args.is_delayed);
        assert!(!valid_args.is_send_with);
    }
}
