//! Remote storage client for JSON-RPC over HTTPS.
//!
//! This module provides `StorageClient`, which implements the `WalletStorageProvider`
//! interface via JSON-RPC calls to a remote server (e.g., `storage.babbage.systems`).
//!
//! ## Features
//!
//! - JSON-RPC 2.0 over HTTPS
//! - BRC-31 (Authrite) authentication via `bsv-sdk` auth module
//! - Full implementation of `WalletStorageProvider` trait
//!
//! ## Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::storage::client::StorageClient;
//! use bsv_sdk::wallet::ProtoWallet;
//! use bsv_sdk::primitives::PrivateKey;
//!
//! // Create a wallet for authentication
//! let wallet = ProtoWallet::new(Some(PrivateKey::from_wif("...")?));
//!
//! // Create client for mainnet storage
//! let client = StorageClient::new(wallet, StorageClient::MAINNET_URL);
//!
//! // Initialize connection
//! let settings = client.make_available().await?;
//! println!("Connected to: {}", settings.storage_name);
//!
//! // Create an AuthId for the current user
//! let identity_key = client.get_identity_key().await?;
//! let (user, is_new) = client.find_or_insert_user(&identity_key).await?;
//! let auth = client.create_auth_id_with_user(user.user_id).await?;
//!
//! // List outputs for the user
//! use bsv_sdk::wallet::ListOutputsArgs;
//! let outputs = client.list_outputs(&auth, ListOutputsArgs::default()).await?;
//! ```

mod json_rpc;
mod storage_client;

pub use json_rpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};
pub use storage_client::{
    StorageClient, UpdateProvenTxReqWithNewProvenTxArgs, UpdateProvenTxReqWithNewProvenTxResult,
    MAINNET_URL, TESTNET_URL,
};
