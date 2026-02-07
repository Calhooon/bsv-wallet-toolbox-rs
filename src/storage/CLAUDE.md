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
| `entities/` | Always | Database entity structs for the wallet schema (18 table types) |
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
        ↑
MonitorStorage          - Background monitoring operations
```

### WalletStorageReader

Read-only operations for querying wallet state:

| Method | Description |
|--------|-------------|
| `is_available()` | Check if storage is ready |
| `get_settings()` | Get current `TableSettings` |
| `get_services()` | Get `WalletServices` instance (for BEEF verification, broadcasting, etc.) |
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
| `insert_certificate_field()` | Add a certificate field value |
| `relinquish_certificate()` | Release a certificate |
| `relinquish_output()` | Release an output |
| `update_transaction_status_after_broadcast()` | Update tx/proven_tx_req status after broadcast attempt |
| `review_status()` | Review storage status, clean up aged items |
| `purge_data()` | Remove old completed/failed records |

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
| `set_services()` | Set `WalletServices` instance (required before blockchain operations) |

### MonitorStorage

Background monitoring operations (extends `WalletStorageProvider`). Used by the monitor daemon for transaction lifecycle management:

| Method | Description |
|--------|-------------|
| `synchronize_transaction_statuses()` | Fetch merkle proofs for unmined/pending transactions |
| `send_waiting_transactions()` | Broadcast transactions in unsent/sending status |
| `abort_abandoned()` | Cancel stale unsigned/unprocessed transactions |
| `un_fail()` | Attempt recovery of incorrectly failed transactions |

This trait mirrors Go's `MonitoredStorage` interface and encapsulates the full logic for each operation: querying, calling external services, and updating records.

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
| `ReviewStatusResult` | Log output from `review_status()` |
| `PurgeParams` | Parameters for `purge_data()` (max_age_days, purge_completed, purge_failed) |
| `PurgeResults` | Count and log from `purge_data()` |
| `AdminStatsResult` | Aggregate statistics (users, transactions, outputs, etc.) |

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
| `BeefVerificationMode` | `Strict`, `TrustKnown`, `Disabled` | BEEF merkle proof verification mode |

### BEEF Verification

```rust
pub enum BeefVerificationMode {
    Strict,     // Verify all BEEF merkle proofs (default)
    TrustKnown, // Skip verification for known transactions
    Disabled,   // Disable verification entirely
}
```

Controls how BEEF (Background Evaluation Extended Format) transactions are verified against the blockchain when internalizing or creating actions.

### Monitor Types

| Type | Purpose |
|------|---------|
| `TxSynchronizedStatus` | Result from `synchronize_transaction_statuses` with txid, status, and optional merkle proof data |

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

### WalletStorageManager (in `managers/` module)

Orchestrates multiple storage providers with active/backup semantics. Implemented in `src/managers/storage_manager.rs`, not in the `storage/` submodule. Implements all storage traits by delegating via `run_as_writer`.

## Entities

The `entities` submodule defines structs for the wallet schema (18 table types):

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
| `TableProvenTx` | Transactions with Merkle proofs (height, block_hash, merkle_path) |
| `TableProvenTxReq` | Proof requests with status tracking, attempts, history, and optional batch grouping |

### Certificate Tables

| Entity | Purpose |
|--------|---------|
| `TableCertificate` | Identity certificates |
| `TableCertificateField` | Encrypted certificate field values |

### Sync Tables

| Entity | Purpose |
|--------|---------|
| `TableSyncState` | Sync state between storages (tracks init, ref_num, sync_map, errors) |

### Other Tables

| Entity | Purpose |
|--------|---------|
| `TableCommission` | Commission tracking with payer locking script and key offset |
| `TableMonitorEvent` | Event log for monitoring (event_type, event_data) |

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
    Unsent,       // Waiting to be sent
    Sending,      // Currently being sent
    Unmined,      // Sent but not yet mined
    Unknown,      // Status is unknown
    Callback,     // Waiting for callback confirmation
    Unconfirmed,  // Unconfirmed on chain
    Unfail,       // Marked for unfail processing
    NoSend,       // Should not be sent
    Invalid,      // Transaction is invalid
    DoubleSpend,  // Transaction is a double spend
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

### Monitor Operations

```rust
use bsv_wallet_toolbox::storage::MonitorStorage;
use std::time::Duration;

// Synchronize transaction statuses (fetch merkle proofs)
let statuses = storage.synchronize_transaction_statuses().await?;
for status in &statuses {
    println!("TX {} -> {:?}", status.txid, status.status);
}

// Send waiting transactions (older than 30 seconds)
let results = storage.send_waiting_transactions(Duration::from_secs(30)).await?;

// Abort abandoned transactions (older than 1 hour)
storage.abort_abandoned(Duration::from_secs(3600)).await?;

// Attempt to recover incorrectly failed transactions
storage.un_fail().await?;
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
- `../monitor/CLAUDE.md` - Monitor daemon using MonitorStorage trait
