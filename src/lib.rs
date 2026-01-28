//! BSV Wallet Toolbox
//!
//! Rust implementation of `@bsv/wallet-toolbox`, providing storage and services
//! for BSV wallets. Built on top of `bsv-sdk` which provides cryptographic
//! primitives, transaction building, and the `WalletInterface` trait.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    bsv-wallet-toolbox                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Wallet (implements WalletInterface with full storage/services) │
//! ├───────────────┬──────────────────────┬──────────────────────────┤
//! │  WalletSigner │ WalletStorageManager │ Services │ Monitor       │
//! ├───────────────┴──────────────────────┴──────────┴───────────────┤
//! │  Storage: StorageSqlx (SQLite/MySQL) | StorageClient (Remote)   │
//! └─────────────────────────────────────────────────────────────────┘
//!                                 │
//!                                 ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        bsv-sdk                                   │
//! │  primitives | script | transaction | wallet (ProtoWallet)       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - `sqlite` (default) - SQLite storage backend
//! - `mysql` - MySQL storage backend
//! - `remote` - Remote storage via StorageClient (storage.babbage.systems)
//! - `full` - All features enabled
//!
//! # Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
//! use bsv_sdk::wallet::WalletInterface;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Open local SQLite storage
//!     let storage = StorageSqlx::open("wallet.db").await?;
//!
//!     // Configure mainnet services
//!     let services = Services::mainnet();
//!
//!     // Create wallet with root key
//!     let wallet = Wallet::new(Some(root_key), storage, services).await?;
//!
//!     // Use WalletInterface methods
//!     let outputs = wallet.list_outputs(args, "app.example.com").await?;
//!
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod storage;
pub mod chaintracks;
pub mod services;
pub mod wallet;
pub mod monitor;
// NOTE: managers module not yet implemented. Would include:
//   - WalletStorageManager (multi-storage sync)
//   - WalletPermissionsManager (BRC-98/99 permissions)
//   - SimpleWalletManager / CWIStyleWalletManager
// See MIGRATION_PLAN.md Phase 5 for details
// pub mod managers;

pub use error::{Error, Result};

// Re-export storage types
pub use storage::{
    AuthId,
    WalletStorageProvider,
    WalletStorageReader,
    WalletStorageSync,
    WalletStorageWriter,
};

// Re-export StorageSqlx when sqlite or mysql feature is enabled
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use storage::StorageSqlx;

// Re-export StorageClient when remote feature is enabled
#[cfg(feature = "remote")]
pub use storage::StorageClient;

// Re-export commonly used bsv-sdk types
pub use bsv_sdk::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, CreateActionResult,
    InternalizeActionArgs, InternalizeActionResult, ListActionsArgs, ListActionsResult,
    ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs, ListOutputsResult,
    RelinquishCertificateArgs, RelinquishOutputArgs, WalletInterface,
};

// Re-export Chaintracks types
pub use chaintracks::{
    Chaintracks, ChaintracksClient, ChaintracksInfo, ChaintracksManagement,
    ChaintracksOptions, ChaintracksStorage, BaseBlockHeader, LiveBlockHeader,
    HeightRange, InsertHeaderResult,
};

// Re-export Services types
pub use services::{
    Chain, Services, ServicesOptions,
    WalletServices, NLockTimeInput,
    GetMerklePathResult, GetRawTxResult, PostBeefResult, PostTxResultForTxid,
    GetUtxoStatusResult, GetUtxoStatusOutputFormat, GetStatusForTxidsResult,
    GetScriptHashHistoryResult, ScriptHistoryItem, UtxoDetail, TxStatusDetail,
    BlockHeader, BsvExchangeRate, FiatCurrency, FiatExchangeRates,
    ServiceCollection, ServiceCallHistory,
    WhatsOnChain, WhatsOnChainConfig,
    Arc, ArcConfig,
    Bitails, BitailsConfig,
};

// Re-export Wallet types
pub use wallet::{Wallet, WalletOptions, WalletSigner};

// Re-export Monitor types
pub use monitor::{Monitor, MonitorOptions, MonitorTask, TaskConfig, TaskResult};
