# Storage Module
> Abstractions and implementations for persisting BSV wallet state

## Overview

This module provides the storage layer for the wallet toolbox, defining traits for reading, writing, and synchronizing wallet data. It mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture, enabling cross-SDK compatibility. The module supports multiple storage backends through a layered trait hierarchy, allowing both local (SQLite) and remote (JSON-RPC) storage implementations.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root; exports traits and conditionally compiles storage backends |
| `traits.rs` | Core trait definitions and associated types for storage operations |

## Submodules

| Submodule | Feature Flag | Purpose |
|-----------|--------------|---------|
| `entities/` | Always | Database entity structs for the 18-table wallet schema |
| `sqlx/` | `sqlite` or `mysql` | Local database storage using SQLx |
| `client/` | `remote` | Remote storage via JSON-RPC to storage.babbage.systems |

## Trait Hierarchy

The storage system uses a layered trait design where each level extends the previous:

```text
WalletStorageReader     - Read-only operations (find, list)
        ↑
WalletStorageWriter     - Read + write operations (create, insert, abort)
        ↑
WalletStorageSync       - Read + write + sync operations (get/process chunks)
        ↑
WalletStorageProvider   - Full provider interface with identity
```

### WalletStorageReader

Read-only operations for querying wallet state:

| Method | Description |
|--------|-------------|
| `is_available()` | Check if storage is ready |
| `get_settings()` | Get current `TableSettings` |
| `find_certificates()` | Query certificates with filters |
| `find_output_baskets()` | Query output baskets |
| `find_outputs()` | Query UTXOs with filters |
| `find_proven_tx_reqs()` | Query proof requests |
| `list_actions()` | List transactions for a user |
| `list_certificates()` | List certificates for a user |
| `list_outputs()` | List outputs for a user |

### WalletStorageWriter

Write operations (extends `WalletStorageReader`):

| Method | Description |
|--------|-------------|
| `make_available()` | Initialize storage, return settings |
| `migrate()` | Run database migrations |
| `destroy()` | Delete all storage data |
| `find_or_insert_user()` | Get or create user by identity key |
| `abort_action()` | Cancel an in-progress action |
| `create_action()` | Create a new transaction action |
| `process_action()` | Process action after signing |
| `internalize_action()` | Import an external transaction |
| `insert_certificate()` | Add a certificate |
| `relinquish_certificate()` | Release a certificate |
| `relinquish_output()` | Release an output |

### WalletStorageSync

Synchronization operations (extends `WalletStorageWriter`):

| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state record |
| `set_active()` | Set active storage for a user |
| `get_sync_chunk()` | Get data chunk for sync |
| `process_sync_chunk()` | Apply received sync chunk |

### WalletStorageProvider

Full provider interface (extends `WalletStorageSync`):

| Method | Description |
|--------|-------------|
| `is_storage_provider()` | Always returns `true` |
| `storage_identity_key()` | Get storage's identity key |
| `storage_name()` | Get storage's display name |

## Key Types

### Authentication

```rust
pub struct AuthId {
    pub identity_key: String,      // User's identity public key (hex)
    pub user_id: Option<i64>,      // Database user ID (after lookup)
    pub is_active: Option<bool>,   // Whether user is active
}

impl AuthId {
    pub fn new(identity_key: impl Into<String>) -> Self;
    pub fn with_user_id(identity_key: impl Into<String>, user_id: i64) -> Self;
}
```

Every storage operation requires an `AuthId` to identify the authenticated user.

### Storage Info

```rust
pub struct WalletStorageInfo {
    pub is_active: bool,
    pub is_enabled: bool,
    pub is_backup: bool,
    pub is_conflicting: bool,
    pub user_id: i64,
    pub storage_identity_key: String,
    pub storage_name: String,
    pub storage_class: String,
    pub endpoint_url: Option<String>,
}
```

Provides metadata about a configured storage provider instance.

### Query Arguments

| Type | Purpose |
|------|---------|
| `Paged` | Pagination with `offset` and `limit` |
| `FindSincePagedArgs` | Base for paginated queries with `since` timestamp |
| `FindCertificatesArgs` | Filter by certifiers, types, include fields |
| `FindOutputBasketsArgs` | Filter by user, name |
| `FindOutputsArgs` | Filter by basket, txid, vout, tx_status |
| `FindProvenTxReqsArgs` | Filter by status, txids |

### Result Types

| Type | Purpose |
|------|---------|
| `StorageCreateActionResult` | Result of creating a transaction action |
| `StorageCreateTransactionInput` | Input details for transaction creation |
| `StorageCreateTransactionOutput` | Output details for transaction creation |
| `StorageProcessActionResults` | Results after processing signed action |
| `StorageInternalizeActionResult` | Result of importing external transaction |
| `SendWithResult` | Status of a sent transaction |
| `ReviewActionResult` | Result of non-delayed broadcast |

### Sync Types

| Type | Purpose |
|------|---------|
| `RequestSyncChunkArgs` | Parameters for requesting sync data |
| `SyncOffset` | Offset tracking for each entity type |
| `SyncChunk` | Data payload for synchronization |
| `ProcessSyncChunkResult` | Result with insert/update counts |

### Enums

| Enum | Values | Purpose |
|------|--------|---------|
| `StorageProvidedBy` | `You`, `Storage`, `YouAndStorage` | Who provided an input/output |
| `ReviewActionResultStatus` | `Success`, `DoubleSpend`, `ServiceError`, `InvalidTx` | Broadcast result |

### Storage Info Type

| Type | Purpose |
|------|---------|
| `WalletStorageInfo` | Metadata about a configured storage provider (active, enabled, backup status) |

## Storage Implementations

### StorageSqlx (feature: `sqlite` or `mysql`)

Local database storage using SQLx. Supports SQLite (default) and MySQL.

```rust
use bsv_wallet_toolbox::storage::StorageSqlx;

// File-based SQLite
let storage = StorageSqlx::open("wallet.db").await?;

// In-memory for testing
let storage = StorageSqlx::in_memory().await?;

// Initialize
storage.migrate("my-wallet", &storage_identity_key).await?;
let settings = storage.make_available().await?;
```

See `sqlx/CLAUDE.md` for detailed documentation.

### StorageClient (feature: `remote`)

Remote storage via JSON-RPC to `storage.babbage.systems`. Uses BRC-31 (Authrite) authentication.

```rust
use bsv_wallet_toolbox::storage::StorageClient;
use bsv_sdk::wallet::ProtoWallet;

let wallet = ProtoWallet::new(Some(private_key));
let client = StorageClient::new(wallet, StorageClient::MAINNET_URL);

let settings = client.make_available().await?;
```

See `client/CLAUDE.md` for detailed documentation.

## Entities

The `entities` submodule defines structs for the 18-table wallet schema:

### Core Tables

| Entity | Purpose |
|--------|---------|
| `TableUser` | User identity and active storage |
| `TableSettings` | Storage configuration (singleton) |
| `TableTransaction` | Transaction records |
| `TableOutput` | UTXOs with locking scripts |
| `TableOutputBasket` | Output organization groups |
| `TableOutputTag` | Labels for outputs |
| `TableOutputTagMap` | Output-to-tag mapping |
| `TableTxLabel` | Labels for transactions |
| `TableTxLabelMap` | Transaction-to-label mapping |

### Proof Tables

| Entity | Purpose |
|--------|---------|
| `TableProvenTx` | Transactions with Merkle proofs |
| `TableProvenTxReq` | Pending proof requests |

### Certificate Tables

| Entity | Purpose |
|--------|---------|
| `TableCertificate` | Identity certificates |
| `TableCertificateField` | Encrypted certificate field values |

### Other Tables

| Entity | Purpose |
|--------|---------|
| `TableSyncState` | Sync state between storages |
| `TableCommission` | Commission tracking |
| `TableMonitorEvent` | Event log |

### Status Enums

```rust
pub enum TransactionStatus {
    Completed,    // Confirmed on chain
    Unprocessed,  // Not yet signed/broadcast
    Sending,      // Being broadcast
    Unproven,     // Broadcast but no proof yet
    Unsigned,     // Awaiting signature
    NoSend,       // Intentionally not broadcast
    NonFinal,     // Has future nLockTime
    Failed,       // Failed to process
    Unfail,       // Marked for retry after failure
}

pub enum ProvenTxReqStatus {
    Pending,      // Awaiting processing
    InProgress,   // Being processed
    Completed,    // Proof obtained
    Failed,       // Proof failed
    NotFound,     // Transaction not found
}
```

## Usage Patterns

### Basic Read Operations

```rust
use bsv_wallet_toolbox::storage::{AuthId, FindOutputsArgs, WalletStorageReader};

// Create auth for user
let auth = AuthId::new("02abc123...");

// Find spendable outputs
let args = FindOutputsArgs {
    user_id: Some(user.user_id),
    tx_status: Some(vec![TransactionStatus::Completed]),
    ..Default::default()
};
let outputs = storage.find_outputs(&auth, args).await?;
```

### Creating Transactions

```rust
use bsv_wallet_toolbox::storage::{WalletStorageWriter, StorageProcessActionArgs};
use bsv_sdk::wallet::CreateActionArgs;

// Create action (returns unsigned transaction template)
let create_result = storage.create_action(&auth, CreateActionArgs {
    description: "Payment".to_string(),
    outputs: vec![/* ... */],
    ..Default::default()
}).await?;

// After signing externally...
let process_result = storage.process_action(&auth, StorageProcessActionArgs {
    is_new_tx: true,
    reference: Some(create_result.reference),
    txid: Some(signed_txid),
    raw_tx: Some(signed_tx_bytes),
    ..Default::default()
}).await?;
```

### Synchronizing Between Storages

```rust
use bsv_wallet_toolbox::storage::{WalletStorageSync, RequestSyncChunkArgs};

// Get chunk from source storage
let args = RequestSyncChunkArgs {
    from_storage_identity_key: source.storage_identity_key().to_string(),
    to_storage_identity_key: dest.storage_identity_key().to_string(),
    identity_key: auth.identity_key.clone(),
    since: None,
    max_rough_size: 100_000,
    max_items: 1000,
    offsets: vec![],
};

let chunk = source.get_sync_chunk(args.clone()).await?;

// Apply to destination
let result = dest.process_sync_chunk(args, chunk).await?;
println!("Synced {} inserts, {} updates", result.inserts, result.updates);
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `sqlite` | Enable SQLite storage via StorageSqlx |
| `mysql` | Enable MySQL storage via StorageSqlx |
| `remote` | Enable remote storage via StorageClient |

## Related

- `entities/CLAUDE.md` - Detailed entity documentation
- `sqlx/CLAUDE.md` - SQLx storage implementation details
- `client/CLAUDE.md` - Remote storage client details
- `../chaintracks/CLAUDE.md` - Chain tracking for proofs
