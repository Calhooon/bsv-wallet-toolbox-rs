# SQLx Storage Module
> SQLite-based persistent storage implementation for wallet state.

## Overview

This module provides a production-ready storage backend for BSV wallet state using SQLx with SQLite. It implements the full `WalletStorageProvider` trait hierarchy, enabling persistent storage of transactions, outputs, certificates, and synchronization state. The implementation mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module definition and public exports |
| `storage_sqlx.rs` | Complete `StorageSqlx` implementation (~2698 lines) |
| `create_action.rs` | Transaction creation implementation (~3296 lines) |
| `process_action.rs` | Signed transaction processing (~1274 lines) |
| `abort_action.rs` | Transaction abort/cancellation (~1249 lines) |
| `internalize_action.rs` | External transaction internalization (~1247 lines) |
| `sync.rs` | Multi-storage synchronization (~2553 lines) |
| `migrations/001_initial.sql` | Initial schema with 16 tables |

## Key Exports

### `StorageSqlx`
The main storage provider struct implementing all wallet storage traits.

```rust
pub struct StorageSqlx {
    pool: Pool<Sqlite>,                              // SQLx connection pool
    settings: std::sync::RwLock<Option<TableSettings>>,
    storage_identity_key: std::sync::RwLock<String>,
    storage_name: std::sync::RwLock<String>,
    chain_tracker: RwLock<Option<Arc<dyn ChainTracker>>>,  // For BEEF verification
}
```

**Constructor methods:**
- `new(database_url: &str)` - Create from SQLite URL (e.g., `"sqlite:wallet.db"`)
- `in_memory()` - Create in-memory database (useful for testing)
- `open(path: &str)` - Open file-based database (creates if not exists)

**ChainTracker methods:**
- `set_chain_tracker(tracker)` - Set ChainTracker for BEEF verification
- `clear_chain_tracker()` - Disable BEEF verification

### `DEFAULT_MAX_OUTPUT_SCRIPT`
Constant defining maximum script length stored inline (10,000 bytes). Scripts longer than this are retrieved from raw transactions.

### `entity_names`
Module containing entity name constants for sync operations (exported from `sync.rs`).

## Trait Implementations

`StorageSqlx` implements the full trait hierarchy defined in `src/storage/traits.rs`:

```
WalletStorageReader     - Read operations
        ↑
WalletStorageWriter     - Write operations
        ↑
WalletStorageSync       - Sync operations
        ↑
WalletStorageProvider   - Full provider interface
```

### WalletStorageReader Methods
| Method | Description |
|--------|-------------|
| `is_available()` | Check if storage is initialized |
| `get_settings()` | Get cached settings |
| `find_certificates()` | Query certificates by certifier/type |
| `find_output_baskets()` | Query output baskets by name |
| `find_outputs()` | Query outputs with filters |
| `find_proven_tx_reqs()` | Query proof requests by status |
| `list_actions()` | List transactions with labels, inputs, outputs |
| `list_certificates()` | List certificates with field values and keyring |
| `list_outputs()` | List spendable outputs with tags and labels |

### WalletStorageWriter Methods
| Method | Description |
|--------|-------------|
| `make_available()` | Initialize and load settings |
| `migrate()` | Run schema migrations |
| `destroy()` | Drop all tables |
| `find_or_insert_user()` | Get or create user by identity key |
| `insert_certificate()` | Insert new certificate |
| `relinquish_certificate()` | Soft-delete certificate |
| `relinquish_output()` | Remove output from basket |
| `abort_action()` | Abort pending transaction, release locked outputs |
| `create_action()` | Create new transaction with inputs/outputs |
| `process_action()` | Process signed transaction (partial - delegates to internal) |
| `internalize_action()` | Internalize external transaction into wallet |

### WalletStorageSync Methods
| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state |
| `set_active()` | Set user's active storage |
| `get_sync_chunk()` | Get data chunk for synchronization |
| `process_sync_chunk()` | Apply received sync chunk with upsert logic |

## Abort Action Implementation

The `abort_action.rs` module cancels pending transactions and releases locked UTXOs:

### Core Function
```rust
pub async fn abort_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: AbortActionArgs,
) -> Result<AbortActionResult>
```

### Functionality
| Feature | Description |
|---------|-------------|
| Transaction lookup | Finds by reference or txid (if 64 hex chars) |
| Status validation | Only abortable statuses: unsigned, unprocessed, nosend, nonfinal, unfail |
| Outgoing check | Must be an outgoing transaction |
| Output protection | Fails if transaction outputs have been spent |
| UTXO release | Sets `spendable=true`, `spent_by=NULL` for locked outputs |
| Status update | Sets transaction status to 'failed' |

### Abortable vs Non-Abortable Statuses
| Abortable | Non-Abortable |
|-----------|---------------|
| unsigned, unprocessed, nosend, nonfinal, unfail | completed, failed, sending, unproven |

## Process Action Implementation

The `process_action.rs` module handles signed transactions after `create_action`:

### Core Function
```rust
pub async fn process_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: StorageProcessActionArgs,
) -> Result<StorageProcessActionResults>
```

### Functionality (1:1 Parity with Go/TypeScript)
| Feature | Description |
|---------|-------------|
| Args validation | Validates reference, txid, raw_tx for new transactions |
| txid validation | Computes double SHA256 hash and validates against provided txid |
| Transaction lookup | Finds transaction by reference, validates isOutgoing |
| inputBEEF validation | Ensures transaction has inputBEEF (not already processed) |
| Status validation | Transaction must be 'unsigned' or 'unprocessed' |
| Script verification | Validates output locking scripts match raw_tx |
| Script offset parsing | Extracts script offsets from raw transaction |
| DB updates | Updates transaction, outputs, creates proven_tx_req |
| Status determination | nosend/delayed/immediate modes |
| Batch support | Generates batch ID for multiple txids |
| Re-broadcast | Supports is_new_tx=false for re-broadcasting |

### Status Determination
| Condition | Transaction Status | ProvenTxReq Status |
|-----------|-------------------|-------------------|
| is_no_send && !is_send_with | nosend | nosend |
| is_delayed | unprocessed | unsent |
| immediate | unprocessed → unproven | unprocessed → unmined |

## Internalize Action Implementation

The `internalize_action.rs` module allows a wallet to take ownership of outputs in external transactions:

### Core Function
```rust
pub async fn internalize_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: InternalizeActionArgs,
) -> Result<StorageInternalizeActionResult>
```

### Protocols
| Protocol | Description |
|----------|-------------|
| `wallet payment` | Adds output to wallet's change balance in "default" basket |
| `basket insertion` | Custom output in specified basket, no balance effect |

### Functionality
| Feature | Description |
|---------|-------------|
| AtomicBEEF parsing | Parses and validates BEEF format with atomic_txid |
| Output extraction | Extracts satoshis and locking scripts from transaction |
| Merge support | Updates existing transaction if txid already exists |
| Status validation | Only completed/unproven/nosend transactions can be merged |
| Balance tracking | Calculates net satoshi changes for balance updates |
| Label support | Adds labels to transaction during internalization |
| Tag support | Adds tags to outputs for basket insertions |
| ProvenTxReq creation | Creates proof request if transaction lacks proof |

### Merge Behavior
| Scenario | Balance Change |
|----------|---------------|
| New wallet payment | +satoshis |
| Existing change output → wallet payment | 0 (ignored) |
| Existing non-change → wallet payment | +satoshis |
| Change output → basket insertion | -satoshis |

## Sync Implementation

The `sync.rs` module enables multi-storage synchronization:

### Core Functions
```rust
pub async fn get_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
) -> Result<SyncChunk>

pub async fn process_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
    chunk: SyncChunk,
) -> Result<ProcessSyncChunkResult>
```

### Entity Names
Constants for sync offsets (exported as `entity_names` module):
- `outputBasket`, `provenTx`, `provenTxReq`, `txLabel`, `outputTag`
- `transaction`, `output`, `txLabelMap`, `outputTagMap`
- `certificate`, `certificateField`, `commission`

### get_sync_chunk Features
| Feature | Description |
|---------|-------------|
| Dependency order | Processes entities in foreign key dependency order |
| Offset-based resumption | Uses offsets to continue from previous chunk |
| Size limiting | Tracks rough size to stay under `max_rough_size` |
| Item limiting | Respects `max_items` constraint |
| Since filtering | Only includes entities updated after `since` timestamp |

### process_sync_chunk Features
| Feature | Description |
|---------|-------------|
| Upsert logic | INSERT if new, UPDATE if chunk.updated_at > local.updated_at |
| ID translation | Maps source IDs to local IDs for foreign keys |
| Empty detection | Returns `done: true` when chunk is empty (sync complete) |
| Change tracking | Counts inserts and updates for progress reporting |

## Create Action Implementation

The `create_action.rs` module provides full transaction creation functionality:

### Core Function
```rust
pub async fn create_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: CreateActionArgs,
) -> Result<StorageCreateActionResult>
```

### Constants
| Constant | Value | Description |
|----------|-------|-------------|
| `MAX_SATOSHIS` | 2,100,000,000,000,000 | Total BTC supply in satoshis |
| `MAX_POSSIBLE_SATOSHIS` | 2,099,999,999,999,999 | Sentinel for "use max" |
| `DEFAULT_FEE_RATE_SAT_PER_KB` | 10 | Default fee rate |
| `P2PKH_LOCKING_SCRIPT_LENGTH` | 25 | Standard P2PKH output size |
| `P2PKH_UNLOCKING_SCRIPT_LENGTH` | 107 | Standard P2PKH input size |
| `MIN_DESCRIPTION_LENGTH` | 5 | Minimum description chars |
| `MAX_DESCRIPTION_LENGTH` | 2000 | Maximum description chars |
| `MAX_LABEL_LENGTH` | 300 | Maximum label chars |

### Features
- **Validation**: Description, labels, inputs, outputs, noSendChange
- **Fee calculation**: Accurate transaction size estimation
- **Change generation**: Automatic UTXO management with target counts
- **Input/output tracking**: Full database state management
- **Labels and tags**: Support for transaction/output organization

### BEEF Building (1:1 Parity with Go/TypeScript)

The `build_input_beef` function constructs BEEF with full Go/TypeScript parity:
- User inputBEEF merging (merged first before storage transactions)
- Recursive ancestor lookup until proven transactions
- knownTxids trimming to txid-only format
- returnTXIDOnly support

### Internal Types
`ExtendedInput`, `ExtendedOutput`, `GenerateChangeParams`, `AllocatedChangeInput`, `ChangeOutput`

## Database Schema

The module creates 16 tables via `migrations/001_initial.sql`:

| Category | Tables |
|----------|--------|
| Core | `settings`, `users`, `transactions`, `outputs`, `output_baskets` |
| Proofs | `proven_txs`, `proven_tx_reqs` |
| Certificates | `certificates`, `certificate_fields` |
| Labels/Tags | `tx_labels`, `tx_labels_map`, `output_tags`, `output_tags_map` |
| Other | `commissions`, `sync_states`, `monitor_events` |

## Usage

### Basic Setup
```rust
use bsv_wallet_toolbox::storage::sqlx::StorageSqlx;

let storage = StorageSqlx::open("wallet.db").await?;  // or in_memory()
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

let identity_key = "03abc..."; // 66-char hex public key
let (user, is_new) = storage.find_or_insert_user(&identity_key).await?;
let auth = AuthId::with_user_id(&identity_key, user.user_id);
```

### Transaction Operations
```rust
// Create transaction
let result = storage.create_action(&auth, CreateActionArgs {
    description: "Send payment".to_string(),
    outputs: Some(vec![CreateActionOutput { ... }]),
    labels: Some(vec!["payment".to_string()]),
    ..Default::default()
}).await?;

// Abort pending transaction
storage.abort_action(&auth, AbortActionArgs { reference: "ref".to_string() }).await?;

// Internalize external transaction
storage.internalize_action(&auth, InternalizeActionArgs { tx: beef_bytes, ... }).await?;
```

### Query Operations
```rust
// List transactions
let actions = storage.list_actions(&auth, ListActionsArgs { labels: vec![], ... }).await?;

// List outputs
let outputs = storage.list_outputs(&auth, ListOutputsArgs { basket: "default".to_string(), ... }).await?;

// Find certificates
let certs = storage.find_certificates(&auth, FindCertificatesArgs { ... }).await?;
```

## Feature Flags

Feature `sqlite` (default) enables SQLite support. MySQL (`mysql`) is planned but not yet implemented.

## Implementation Notes

### Soft Deletes
Certificates and baskets use `is_deleted` flag for soft deletes rather than actual row removal. This preserves history for sync operations.

### Dynamic Query Building
The `find_*` and `list_*` methods build SQL dynamically based on provided filter arguments. Parameters are bound safely to prevent SQL injection.

### Settings Caching
Settings are loaded once via `make_available()` and cached in an `RwLock`. The `get_settings()` method returns a reference to cached data.

### Unsafe Pointer Casts
The trait signatures require `&self` returns but internal state uses `RwLock`. The implementation uses controlled unsafe pointer casts (`storage_sqlx.rs:895`, `storage_sqlx.rs:2134-2140`) as a workaround. This is safe because settings don't change after `make_available()`.

### Fully Implemented Methods
- `list_actions()` - Full support for labels, inputs, outputs, pagination
- `list_certificates()` - Full support for filters, fields, keyring
- `list_outputs()` - Full support for baskets, tags, locking scripts
- `create_action()` - Full transaction creation via `create_action.rs`
- `process_action()` - Signed transaction processing via `process_action.rs` (1:1 parity with Go/TypeScript)
- `abort_action()` - Full abort implementation via `abort_action.rs`
- `internalize_action()` - Full external transaction internalization via `internalize_action.rs`
- `get_sync_chunk()` - Full sync chunk retrieval via `sync.rs`
- `process_sync_chunk()` - Full sync chunk processing with upsert logic via `sync.rs`

## Tests

Total: ~130 tests across all modules.

| Module | Tests | Key Coverage |
|--------|-------|--------------|
| `abort_action.rs` | 19 | Status validation, UTXO release, lookup by txid |
| `process_action.rs` | 35 | txid computation, VarInt parsing, script offsets, Go test parity |
| `internalize_action.rs` | 11 | Wallet payment, basket insertion, merge scenarios |
| `sync.rs` | 9 | Chunk retrieval, upsert logic, ID translation, roundtrip |
| `storage_sqlx.rs` | 11 | CRUD operations, list methods, certificate filters |
| `create_action.rs` | 45 | Validation, fee calculation, BEEF building, Go test parity |

Run with:
```bash
cargo test --features sqlite storage::sqlx
```

## Related

- `../traits.rs` - Trait definitions (`WalletStorageReader`, `WalletStorageWriter`, etc.)
- `../entities.rs` - Table entity structs (`TableUser`, `TableOutput`, etc.)
- `../client/` - Remote storage client (alternative implementation)
- `../../error.rs` - Error types used by this module
