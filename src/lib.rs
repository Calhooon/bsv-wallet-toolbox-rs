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
//! use bsv_wallet_toolbox_rs::{Wallet, StorageSqlx, Services};
//! use bsv_rs::wallet::WalletInterface;
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

pub mod chaintracks;
pub mod error;
pub mod lock_utils;
pub mod managers;
pub mod monitor;
pub mod services;
pub mod storage;
pub mod tsc_proof;
pub mod wallet;

pub use error::{Error, Result};

// Re-export storage types
pub use storage::{
    AuthId, MonitorStorage, WalletStorageProvider, WalletStorageReader, WalletStorageSync,
    WalletStorageWriter,
};

// Re-export StorageSqlx and BroadcastOutcome when sqlite or mysql feature is enabled
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use storage::{classify_broadcast_results, BroadcastOutcome, StorageSqlx};

// Re-export StorageClient when remote feature is enabled
#[cfg(feature = "remote")]
pub use storage::StorageClient;

// Re-export commonly used bsv-sdk types
pub use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, CreateActionArgs, CreateActionResult,
    InternalizeActionArgs, InternalizeActionResult, ListActionsArgs, ListActionsResult,
    ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs, ListOutputsResult,
    RelinquishCertificateArgs, RelinquishOutputArgs, WalletInterface,
};

// Re-export Chaintracks types
pub use chaintracks::{
    BaseBlockHeader, Chaintracks, ChaintracksClient, ChaintracksInfo, ChaintracksManagement,
    ChaintracksOptions, ChaintracksStorage, HeightRange, InsertHeaderResult, LiveBlockHeader,
};

// Re-export Services types
pub use services::{
    AdaptiveTimeoutConfig, Arc, ArcConfig, BhsConfig, Bitails, BitailsConfig, BlockHeader,
    BlockHeaderService, BsvExchangeRate, Chain, FallbackChainTracker, FiatCurrency,
    FiatExchangeRates, GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult,
    GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput,
    PostBeefResult, PostTxResultForTxid, ScriptHistoryItem, ServiceCallHistory, ServiceCollection,
    Services, ServicesOptions, TxStatusDetail, UtxoDetail, WalletServices, WhatsOnChain,
    WhatsOnChainConfig,
};

// Re-export Wallet types
pub use wallet::{
    HttpLookupResolver, OverlayCertificate, OverlayLookupResolver, ScriptType, SignerInput,
    UnlockingScriptTemplate, UtxoInfo, Wallet, WalletBalance, WalletOptions, WalletSigner,
};

// Re-export Monitor types
pub use monitor::{
    Monitor, MonitorOptions, MonitorTask, TaskConfig, TaskResult, TransactionStatusUpdate,
};

// Re-export Managers types
pub use managers::{
    setup_wallet,
    // Permissions manager
    BasketUsageType,
    // CWI-style wallet manager
    CWIStyleWalletManager,
    CWIStyleWalletManagerConfig,
    CertificateUsageType,
    // Settings manager
    Certifier,
    GroupedPermissions,
    // Storage manager
    ManagedStorage,
    PermissionRequest,
    PermissionRequestHandler,
    PermissionToken,
    PermissionUsageType,
    PermissionsModule,
    Profile,
    // Setup helpers
    SetupWalletOptions,
    // Simple wallet manager
    SimpleWalletManager,
    TrustSettings,
    UmpToken,
    // Authentication manager
    WalletAuthenticationManager,
    WalletLogEntry,
    // Logger
    WalletLogger,
    WalletPermissionsManager,
    WalletPermissionsManagerConfig,
    WalletSettings,
    WalletSettingsManager,
    WalletSettingsManagerConfig,
    WalletSnapshot,
    WalletStorageManager,
    WalletTheme,
    DEFAULT_SETTINGS,
    TESTNET_DEFAULT_SETTINGS,
};
