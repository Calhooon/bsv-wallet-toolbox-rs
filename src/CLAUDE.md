# src/ - BSV Wallet Toolbox Core
> Rust implementation of storage and services for BSV wallets

## Overview

This is the main source directory for `bsv-wallet-toolbox`, a Rust port of the TypeScript `@bsv/wallet-toolbox`. It provides wallet storage backends, blockchain header tracking, and services that implement the `WalletInterface` trait from `bsv-sdk`. The library supports multiple storage backends (SQLite, MySQL, remote) and includes the Chaintracks system for block header management.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    bsv-wallet-toolbox                           │
├─────────────────────────────────────────────────────────────────┤
│  Wallet (implements WalletInterface with full storage/services) │
├───────────────┬──────────────────────┬──────────────────────────┤
│  WalletSigner │ WalletStorageManager │ Services │ Monitor       │
├───────────────┴──────────────────────┴──────────┴───────────────┤
│  Storage: StorageSqlx (SQLite/MySQL) | StorageClient (Remote)   │
├─────────────────────────────────────────────────────────────────┤
│  Chaintracks: Block header tracking with bulk/live storage      │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        bsv-sdk                                   │
│  primitives | script | transaction | wallet (ProtoWallet)       │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `lib.rs` | 144 | Crate root with module declarations, re-exports, and crate-level documentation |
| `error.rs` | 127 | Error types using `thiserror` with variants for storage, auth, service, transaction, sync, and validation errors |

## Modules

| Module | Purpose |
|--------|---------|
| `storage/` | Wallet storage layer with traits and implementations (SQLite, MySQL, remote) |
| `chaintracks/` | Block header tracking system with two-tier bulk/live storage |
| `services/` | External service providers (WhatsOnChain, ARC, Bitails, BHS) for blockchain operations |
| `wallet/` | Full `Wallet` implementation with `WalletSigner` for transaction signing |
| `monitor/` | Transaction monitoring daemon with background tasks for syncing and reprocessing |
| `managers/` | Higher-level wallet management: storage sync, settings, authentication, permissions |

## Key Exports

### Error Handling

```rust
pub use error::{Error, Result};
```

- `Error` - Enum with categorized error variants (storage, auth, service, transaction, sync, validation)
- `Result<T>` - Type alias for `std::result::Result<T, Error>`

### Storage Types

```rust
pub use storage::{
    AuthId,                   // Authentication identifier for storage operations
    WalletStorageProvider,    // Full storage interface (read + write + sync)
    WalletStorageReader,      // Read-only operations (find_*, list_*)
    WalletStorageWriter,      // Write operations (create_action, insert_certificate)
    WalletStorageSync,        // Sync operations (get_sync_chunk, process_sync_chunk)
};
```

### Storage Implementations (feature-gated)

```rust
#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use storage::StorageSqlx;  // Local database storage

#[cfg(feature = "remote")]
pub use storage::StorageClient; // Remote storage via JSON-RPC
```

### Chaintracks Types

```rust
pub use chaintracks::{
    Chaintracks,          // Main orchestrator
    ChaintracksClient,    // Read-only client trait
    ChaintracksInfo,      // System status information
    ChaintracksManagement,// Management trait (destroy, validate, export)
    ChaintracksOptions,   // Configuration options
    ChaintracksStorage,   // Storage trait for headers
    BaseBlockHeader,      // Header without height (as from network)
    LiveBlockHeader,      // Header with chain tracking fields
    HeightRange,          // Range of block heights
    InsertHeaderResult,   // Result of header insertion
};
```

### Services Types

```rust
pub use services::{
    // Core types
    Chain,                      // Network identifier (Main, Test)
    Services,                   // Main services orchestrator
    ServicesOptions,            // Configuration options
    WalletServices,             // Service provider trait
    NLockTimeInput,             // Input for nLockTime-based time queries

    // Result types for blockchain operations
    GetMerklePathResult,        // Merkle path for transaction proof
    GetRawTxResult,             // Raw transaction data
    PostBeefResult,             // BEEF transaction broadcast result
    PostTxResultForTxid,        // Transaction broadcast result
    GetUtxoStatusResult,        // UTXO status information
    GetUtxoStatusOutputFormat,  // Output format for UTXO queries
    GetStatusForTxidsResult,    // Status for multiple transactions
    GetScriptHashHistoryResult, // Script hash history
    ScriptHistoryItem,          // Individual history item
    UtxoDetail,                 // UTXO details
    TxStatusDetail,             // Transaction status details
    BlockHeader,                // Block header with height and hash
    BsvExchangeRate,            // Exchange rate information
    FiatCurrency,               // Fiat currency type (USD, EUR, etc.)
    FiatExchangeRates,          // Collection of fiat exchange rates

    // Service collection and history
    ServiceCollection,          // Collection of service providers
    ServiceCallHistory,         // History of service calls

    // Provider implementations
    WhatsOnChain, WhatsOnChainConfig,  // WhatsOnChain API provider
    Arc, ArcConfig,                     // ARC transaction processor
    Bitails, BitailsConfig,             // Bitails API provider
    BlockHeaderService, BhsConfig,      // Block Header Service provider
};
```

### Wallet Types

```rust
pub use wallet::{
    Wallet,          // Full wallet implementation with WalletInterface
    WalletOptions,   // Configuration options for wallet creation
    WalletSigner,    // Transaction signing component
};
```

### Monitor Types

```rust
pub use monitor::{
    Monitor,                    // Transaction monitoring daemon
    MonitorOptions,             // Configuration for monitor initialization
    MonitorTask,                // Individual background task definition
    TaskConfig,                 // Configuration for task execution
    TaskResult,                 // Result from task execution
    TransactionStatusUpdate,    // Status update notification for monitored transactions
};
```

### Managers Types

```rust
pub use managers::{
    // Settings manager - Persistent wallet settings
    Certifier,                     // Certificate issuer configuration
    TrustSettings,                 // Trust configuration for signing
    WalletSettings,                // Complete wallet settings
    WalletSettingsManager,         // Settings persistence manager
    WalletSettingsManagerConfig,   // Configuration options
    WalletTheme,                   // UI theme (Light, Dark, System)
    DEFAULT_SETTINGS,              // Default mainnet settings
    TESTNET_DEFAULT_SETTINGS,      // Default testnet settings

    // Storage manager - Multi-storage synchronization
    ManagedStorage,                // Individual storage with role
    WalletStorageManager,          // Active/backup storage sync

    // Simple wallet manager - Two-factor authentication
    SimpleWalletManager,           // Primary key + privileged key manager

    // CWI-style wallet manager - Multi-profile support
    CWIStyleWalletManager,         // CWI-compatible manager
    CWIStyleWalletManagerConfig,   // Configuration options
    Profile,                       // User profile definition
    UmpToken,                      // User media policy token
    WalletSnapshot,                // Serializable wallet state snapshot

    // Permissions manager - BRC-98/99 permission control (stub)
    GroupedPermissions,            // Grouped permission definitions
    PermissionRequest,             // Permission request structure
    PermissionToken,               // Permission token for operations
    PermissionsModule,             // Permission module trait
    WalletPermissionsManager,      // Permission manager
    WalletPermissionsManagerConfig,// Configuration options

    // Authentication manager - WAB integration
    WalletAuthenticationManager,   // Web Authentication Bridge manager (skeleton)

    // Logger - Structured wallet logging
    WalletLogger,                  // Logger interface for wallet operations
    WalletLogEntry,                // Individual log entry

    // Setup helpers - Convenience wallet initialization
    SetupWalletOptions,            // Options for setup_wallet()
    setup_wallet,                  // Helper to create a fully configured wallet
};
```

### Re-exported bsv-sdk Types

The library re-exports commonly used types from `bsv-sdk::wallet`:
- Action types: `AbortActionArgs`, `CreateActionArgs`, `InternalizeActionArgs`
- List types: `ListActionsArgs`, `ListOutputsArgs`, `ListCertificatesArgs`
- Results: `ListActionsResult`, `ListOutputsResult`, `ListCertificatesResult`, `AbortActionResult`, `CreateActionResult`, `InternalizeActionResult`
- Relinquish types: `RelinquishCertificateArgs`, `RelinquishOutputArgs`
- Core trait: `WalletInterface`

## Feature Flags

| Feature | Description |
|---------|-------------|
| `sqlite` (default) | SQLite storage backend via `StorageSqlx` |
| `mysql` | MySQL storage backend via `StorageSqlx` |
| `remote` | Remote storage via `StorageClient` (connects to storage.babbage.systems) |
| `full` | All features enabled |

## Error Categories

The `Error` enum in `error.rs` organizes errors by category:

| Category | Variants | Description |
|----------|----------|-------------|
| Storage | `StorageNotAvailable`, `StorageError`, `DatabaseError`, `MigrationError`, `NotFound`, `Duplicate` | Database and storage operations |
| Authentication | `AuthenticationRequired`, `InvalidIdentityKey`, `UserNotFound`, `AccessDenied` | User authentication |
| Service | `ServiceError`, `NetworkError`, `BroadcastFailed`, `NoServicesAvailable` | External service calls |
| Transaction | `TransactionError`, `InvalidTransactionStatus`, `InsufficientFunds` | Transaction processing |
| Validation | `ValidationError`, `InvalidArgument`, `InvalidOperation` | Input validation and operation errors |
| Sync | `SyncError`, `SyncConflict` | Multi-storage synchronization |
| Wrapped | `SdkError`, `JsonError`, `IoError`, `SqlxError` (sqlite feature), `HttpError` | Errors from dependencies (`bsv_sdk`, `serde_json`, `std::io`, `sqlx`, `reqwest`) |

## Usage

### Basic Wallet with SQLite Storage

```rust
use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
use bsv_sdk::wallet::WalletInterface;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open local SQLite storage
    let storage = StorageSqlx::open("wallet.db").await?;

    // Configure mainnet services
    let services = Services::mainnet();

    // Create wallet with root key
    let wallet = Wallet::new(Some(root_key), storage, services).await?;

    // Use WalletInterface methods
    let outputs = wallet.list_outputs(args, "app.example.com").await?;

    Ok(())
}
```

### Block Header Tracking with Chaintracks

```rust
use bsv_wallet_toolbox::chaintracks::{Chaintracks, ChaintracksOptions};

let options = ChaintracksOptions::default_mainnet();
let chaintracks = Chaintracks::new(options).await?;
chaintracks.make_available().await?;

let tip = chaintracks.find_chain_tip_header().await?;
println!("Chain tip: {} at height {}", tip.hash, tip.height);
```

### Using Services for Blockchain Operations

```rust
use bsv_wallet_toolbox::{Services, ServicesOptions, Chain, WalletServices};

// Create mainnet services with default providers
let services = Services::mainnet();

// Or configure with custom options
let options = ServicesOptions {
    chain: Chain::Main,
    ..Default::default()
};
let services = Services::new(options);

// Get a raw transaction
let tx_result = services.get_raw_tx(&txid, false).await?;

// Broadcast a transaction
let beef_result = services.post_beef(&beef_bytes, &[txid]).await?;

// Get merkle path for SPV verification
let merkle = services.get_merkle_path(&txid, false).await?;

// Check UTXO status
let utxo_status = services.get_utxo_status(&output_script, "script", None).await?;
```

## Storage Trait Hierarchy

The storage layer uses a trait hierarchy that mirrors the TypeScript implementation:

```
WalletStorageReader     ← Read operations (find_outputs, list_actions, etc.)
        ↑
WalletStorageWriter     ← Write operations (create_action, insert_certificate, etc.)
        ↑
WalletStorageSync       ← Sync operations (get_sync_chunk, process_sync_chunk)
        ↑
WalletStorageProvider   ← Full provider interface with identity/name
```

## Related Documentation

- [storage/CLAUDE.md](./storage/CLAUDE.md) - Storage layer details, entity definitions, trait implementations
- [chaintracks/CLAUDE.md](./chaintracks/CLAUDE.md) - Block header tracking system, storage backends, ingestors
- [services/CLAUDE.md](./services/CLAUDE.md) - External service providers, traits, and blockchain operations
- [wallet/CLAUDE.md](./wallet/CLAUDE.md) - Wallet implementation, signing, and WalletInterface
- [monitor/CLAUDE.md](./monitor/CLAUDE.md) - Transaction monitoring daemon and background tasks
- [managers/CLAUDE.md](./managers/CLAUDE.md) - Higher-level wallet management components

## Development Notes

### Cross-SDK Compatibility

Entity structures in `storage/entities/` are designed for cross-SDK compatibility with the TypeScript and Go implementations. Field names and types should match the other SDKs when possible.

### Authentication Model

All storage operations require an `AuthId` containing the user's identity public key (hex string). The storage layer looks up or creates the user and validates access permissions.

### Async Runtime

All storage and chaintracks operations are async and require a Tokio runtime. The library uses `async_trait` for async trait methods.
