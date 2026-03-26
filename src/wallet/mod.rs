//! Wallet Module
//!
//! This module provides the main [`Wallet`] struct that implements the full
//! [`WalletInterface`] trait from `bsv_rs::wallet`. The Wallet combines:
//!
//! - **ProtoWallet**: For cryptographic operations (key derivation, signing, encryption)
//! - **Storage**: For persistent state (UTXOs, transactions, certificates)
//! - **Services**: For blockchain interaction (broadcasting, merkle proofs, UTXO status)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Wallet<S, V>                             │
//! │  (Implements WalletInterface with full storage and services)    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ProtoWallet     │    Storage (S)     │    Services (V)         │
//! │  - Key derivation│    - UTXOs         │    - Broadcasting       │
//! │  - Signing       │    - Transactions  │    - Merkle proofs      │
//! │  - Encryption    │    - Certificates  │    - UTXO status        │
//! │  - HMAC          │    - Labels/Tags   │    - Chain height       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
//! use bsv_rs::primitives::PrivateKey;
//! use bsv_rs::wallet::WalletInterface;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create storage backend
//!     let storage = StorageSqlx::open("wallet.db").await?;
//!     storage.migrate("my-wallet", &storage_identity_key).await?;
//!     storage.make_available().await?;
//!
//!     // Create services for blockchain interaction
//!     let services = Services::mainnet();
//!
//!     // Create wallet with root key
//!     let root_key = PrivateKey::random();
//!     let wallet = Wallet::new(Some(root_key), storage, services).await?;
//!
//!     // Use WalletInterface methods
//!     let result = wallet.get_public_key(args, "app.example.com").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Method Categories
//!
//! The `Wallet` implements all 28 methods from `WalletInterface`:
//!
//! ## Cryptographic Operations (delegated to ProtoWallet)
//! - `get_public_key` - Get identity or derived public key
//! - `encrypt` / `decrypt` - AES-GCM encryption with derived keys
//! - `create_hmac` / `verify_hmac` - HMAC-SHA256 operations
//! - `create_signature` / `verify_signature` - ECDSA signatures
//! - `reveal_counterparty_key_linkage` / `reveal_specific_key_linkage` - Key linkage revelation
//!
//! ## Action Operations (coordinated with storage and services)
//! - `create_action` - Create new Bitcoin transaction
//! - `sign_action` - Sign previously created transaction
//! - `abort_action` - Cancel transaction in progress
//! - `list_actions` - List transactions matching labels
//! - `internalize_action` - Import external transaction
//!
//! ## Output Operations (delegated to storage)
//! - `list_outputs` - List spendable outputs in a basket
//! - `relinquish_output` - Remove output from basket tracking
//!
//! ## Certificate Operations (delegated to storage)
//! - `acquire_certificate` - Acquire identity certificate
//! - `list_certificates` - List certificates by certifier/type
//! - `prove_certificate` - Prove certificate fields to verifier
//! - `relinquish_certificate` - Remove certificate from wallet
//!
//! ## Discovery Operations
//! - `discover_by_identity_key` - Find certificates by identity key
//! - `discover_by_attributes` - Find certificates by attributes
//!
//! ## Chain/Status Operations
//! - `is_authenticated` / `wait_for_authentication` - Authentication status
//! - `get_height` - Current blockchain height
//! - `get_header_for_height` - Block header at height
//! - `get_network` - Network (mainnet/testnet)
//! - `get_version` - Wallet version string

mod certificate_issuance;
pub mod lookup;
mod signer;
#[allow(clippy::module_inception)]
mod wallet;

pub use lookup::{
    dedup_certificates, HttpLookupResolver, OverlayCertificate, OverlayLookupResolver,
};
pub use signer::{ScriptType, SignerInput, UnlockingScriptTemplate, WalletSigner};
pub use wallet::{
    PendingTransaction, PrivilegedKeyManager, UtxoInfo, Wallet, WalletBalance, WalletOptions,
};
