# SQLx Storage Module
> SQLite-based persistent storage implementation for wallet state.

## Overview

This module provides a production-ready storage backend for BSV wallet state using SQLx with SQLite. It implements the full `WalletStorageProvider` trait hierarchy, enabling persistent storage of transactions, outputs, certificates, and synchronization state. The implementation mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module definition and public exports (44 lines) |
| `storage_sqlx.rs` | Complete `StorageSqlx` implementation (3303 lines) |
| `create_action.rs` | Transaction creation implementation (3296 lines) |
| `process_action.rs` | Signed transaction processing (1274 lines) |
| `abort_action.rs` | Transaction abort/cancellation (1249 lines) |
| `internalize_action.rs` | External transaction internalization (1282 lines) |
| `sync.rs` | Multi-storage synchronization (2554 lines) |
| `beef_verification.rs` | BEEF merkle proof verification (256 lines) |
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
- `get_chain_tracker()` - Internal: get current ChainTracker if set

### `DEFAULT_MAX_OUTPUT_SCRIPT`
Constant defining maximum script length stored inline (10,000 bytes). Scripts longer than this are retrieved from raw transactions.

### `entity_names`
Module containing entity name constants for sync operations (exported from `sync.rs`).

### BEEF Verification Functions
Exported from `beef_verification.rs`:
- `verify_beef_merkle_proofs(beef, chain_tracker, mode, known_txids)` - Verify all merkle proofs in a BEEF
- `verify_txid_merkle_proof(beef, txid, chain_tracker)` - Verify a single transaction's merkle proof

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
        +
MonitorStorage          - Background monitoring operations
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
| `process_action()` | Process signed transaction |
| `internalize_action()` | Internalize external transaction into wallet |

### WalletStorageSync Methods
| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state |
| `set_active()` | Set user's active storage |
| `get_sync_chunk()` | Get data chunk for synchronization |
| `process_sync_chunk()` | Apply received sync chunk with upsert logic |

### MonitorStorage Methods
| Method | Description |
|--------|-------------|
| `synchronize_transaction_statuses()` | Find transactions needing proof sync (unmined, unknown, callback, sending, unconfirmed) |
| `send_waiting_transactions()` | Find unsent transactions ready for broadcast (unsent, sending status) |
| `abort_abandoned()` | Abort unsigned/unprocessed transactions older than timeout |
| `un_fail()` | Process transactions marked for unfail retry |

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
- Args validation (reference, txid, raw_tx)
- txid validation via double SHA256 hash
- Transaction lookup by reference with isOutgoing validation
- inputBEEF validation (ensures not already processed)
- Status validation (must be 'unsigned' or 'unprocessed')
- Script verification and offset parsing from raw_tx
- DB updates: transaction, outputs, proven_tx_req
- Status determination: nosend/delayed/immediate modes
- Batch support and re-broadcast (is_new_tx=false)

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
- AtomicBEEF parsing with atomic_txid validation
- BEEF verification against ChainTracker (if set)
- Output extraction (satoshis, locking scripts)
- Merge support for existing transactions (completed/unproven/nosend)
- Balance tracking with net satoshi change calculation
- Label and tag support for transaction/output organization
- ProvenTxReq creation for unproven transactions

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

### Key Features
- **get_sync_chunk**: Dependency-ordered entity processing, offset-based resumption, size/item limiting, since filtering
- **process_sync_chunk**: Upsert logic (INSERT/UPDATE by updated_at), ID translation for foreign keys, empty detection, change tracking

## BEEF Verification Implementation

The `beef_verification.rs` module provides BEEF (Background Evaluation Extended Format) merkle proof verification:

### Core Functions
```rust
pub async fn verify_beef_merkle_proofs(
    beef: &mut Beef,
    chain_tracker: &dyn ChainTracker,
    mode: BeefVerificationMode,
    known_txids: &HashSet<String>,
) -> Result<bool>

pub async fn verify_txid_merkle_proof(
    beef: &Beef,
    txid: &str,
    chain_tracker: &dyn ChainTracker,
) -> Result<bool>
```

### Verification Modes
| Mode | Description |
|------|-------------|
| `Strict` | Verify all merkle roots against chain (default) |
| `TrustKnown` | Skip verification for known txids |
| `Disabled` | Skip all verification |

### Usage
- Called by `internalize_action` to verify incoming transaction proofs
- Called by `create_action` to verify user-provided inputBEEF
- Requires `ChainTracker` to be set via `storage.set_chain_tracker()`
- Returns `Ok(true)` if valid, `Ok(false)` if no proofs to verify, `Err` if invalid

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
- Validation: description, labels, inputs, outputs, noSendChange
- Fee calculation with accurate transaction size estimation
- Change generation with automatic UTXO management
- Full input/output database state management
- Labels and tags for transaction/output organization

### BEEF Building (1:1 Parity with Go/TypeScript)
- User inputBEEF merging, recursive ancestor lookup until proven transactions
- knownTxids trimming to txid-only format, returnTXIDOnly support

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

```rust
use bsv_wallet_toolbox::storage::sqlx::StorageSqlx;

// Setup
let storage = StorageSqlx::open("wallet.db").await?;  // or in_memory()
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

let identity_key = "03abc..."; // 66-char hex public key
let (user, is_new) = storage.find_or_insert_user(&identity_key).await?;
let auth = AuthId::with_user_id(&identity_key, user.user_id);

// Transaction operations
let result = storage.create_action(&auth, CreateActionArgs { ... }).await?;
storage.abort_action(&auth, AbortActionArgs { reference: "ref".to_string() }).await?;
storage.internalize_action(&auth, InternalizeActionArgs { tx: beef_bytes, ... }).await?;

// Query operations
let actions = storage.list_actions(&auth, ListActionsArgs { ... }).await?;
let outputs = storage.list_outputs(&auth, ListOutputsArgs { basket: "default".to_string(), ... }).await?;
let certs = storage.find_certificates(&auth, FindCertificatesArgs { ... }).await?;
```

## Feature Flags

Feature `sqlite` (default) enables SQLite support. MySQL is planned but not yet implemented.

## Implementation Notes

### Soft Deletes
Certificates and baskets use `is_deleted` flag for soft deletes rather than actual row removal. This preserves history for sync operations.

### Dynamic Query Building
The `find_*` and `list_*` methods build SQL dynamically based on provided filter arguments. Parameters are bound safely to prevent SQL injection.

### Settings Caching
Settings are loaded once via `make_available()` and cached in an `RwLock`. The `get_settings()` method returns a reference to cached data.

### Unsafe Pointer Casts
The trait signatures require `&self` returns but internal state uses `RwLock`. The implementation uses controlled unsafe pointer casts as a workaround. This is safe because settings don't change after `make_available()`.

### MonitorStorage Integration
The `MonitorStorage` trait enables background task integration. Full monitor functionality requires services access, handled externally by monitor tasks.

### Commission Tracking
- `insert_commission(user_id, transaction_id, satoshis, locking_script, key_offset)` - Record commission
- `get_unredeemed_commissions(user_id)` - Query unredeemed commissions
- `redeem_commission(commission_id)` - Mark commission as redeemed

### Monitor Events
- `log_monitor_event(event, details)` - Record monitor event with optional JSON data
- `get_monitor_events(event_type, limit)` - Query events by type with limit
- `cleanup_monitor_events(older_than)` - Remove events older than retention period

## Tests

Total: 142 tests across all modules.

| Module | Tests | Key Coverage |
|--------|-------|--------------|
| `create_action.rs` | 45 | Validation, fee calculation, BEEF building, Go test parity |
| `process_action.rs` | 35 | txid computation, VarInt parsing, script offsets, Go test parity |
| `abort_action.rs` | 19 | Status validation, UTXO release, lookup by txid |
| `storage_sqlx.rs` | 19 | CRUD operations, list methods, certificate filters, monitor ops |
| `internalize_action.rs` | 11 | Wallet payment, basket insertion, merge scenarios |
| `sync.rs` | 9 | Chunk retrieval, upsert logic, ID translation, roundtrip |
| `beef_verification.rs` | 4 | Verification modes, empty BEEF handling, mode serialization |

Run with:
```bash
cargo test --features sqlite storage::sqlx
```

## Related

- `../traits.rs` - Trait definitions (`WalletStorageReader`, `WalletStorageWriter`, `MonitorStorage`)
- `../entities/` - Table entity structs (`TableUser`, `TableOutput`, `TransactionStatus`)
- `../client/` - Remote storage client (alternative implementation)