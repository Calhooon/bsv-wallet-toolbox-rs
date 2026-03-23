//! Remote storage client for JSON-RPC over HTTPS.
//!
//! This module provides `StorageClient`, which implements the `WalletStorageProvider`
//! interface via JSON-RPC calls to a remote server (e.g., `storage.babbage.systems`).
//!
//! ## Features
//!
//! - JSON-RPC 2.0 over HTTPS
//! - BRC-31 (Authrite) mutual authentication
//! - Request signing with identity key
//! - Response signature verification
//! - Full implementation of `WalletStorageProvider` trait
//!
//! ## BRC-31 Authentication
//!
//! All requests are authenticated using the BRC-31 protocol:
//! - Requests are signed with the wallet's identity key
//! - Nonce and timestamp provide replay protection
//! - Server responses can optionally be verified
//!
//! ## Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::storage::client::StorageClient;
//! use bsv_rs::wallet::ProtoWallet;
//! use bsv_rs::primitives::PrivateKey;
//!
//! // Create a wallet for authentication
//! let wallet = ProtoWallet::new(Some(PrivateKey::from_wif("...")?));
//!
//! // Create client for mainnet storage
//! let client = StorageClient::new(wallet, StorageClient::MAINNET_URL);
//!
//! // Initialize connection (authenticated via BRC-31)
//! let settings = client.make_available().await?;
//! println!("Connected to: {}", settings.storage_name);
//!
//! // Create an AuthId for the current user
//! let identity_key = client.get_identity_key().await?;
//! let (user, is_new) = client.find_or_insert_user(&identity_key).await?;
//! let auth = client.create_auth_id_with_user(user.user_id).await?;
//!
//! // List outputs for the user
//! use bsv_rs::wallet::ListOutputsArgs;
//! let outputs = client.list_outputs(&auth, ListOutputsArgs::default()).await?;
//! ```

pub mod auth;
mod json_rpc;
mod storage_client;

pub use auth::{
    create_auth_headers, create_simple_nonce, current_timestamp_ms, headers as auth_headers,
    validate_timestamp, verify_response_auth, AuthHeaders, AuthVerificationResult,
    ResponseAuthHeaders, AUTH_PROTOCOL_ID, AUTH_VERSION,
};
pub use json_rpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};
pub use storage_client::{
    StorageClient, UpdateProvenTxReqWithNewProvenTxArgs, UpdateProvenTxReqWithNewProvenTxResult,
    ValidCreateActionArgs, MAINNET_URL, TESTNET_URL,
};
