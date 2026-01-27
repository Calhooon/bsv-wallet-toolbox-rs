# SQLx Storage Module
> SQLite-based persistent storage implementation for wallet state.

## Overview

This module provides a production-ready storage backend for BSV wallet state using SQLx with SQLite. It implements the full `WalletStorageProvider` trait hierarchy, enabling persistent storage of transactions, outputs, certificates, and synchronization state. The implementation mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module definition and public exports |
| `storage_sqlx.rs` | Complete `StorageSqlx` implementation (~2673 lines) |
| `create_action.rs` | Transaction creation implementation (~2088 lines) |
| `process_action.rs` | Signed transaction processing (~1250 lines) |
| `migrations/001_initial.sql` | Initial schema with 16 tables |

## Key Exports

### `StorageSqlx`
The main storage provider struct implementing all wallet storage traits.

```rust
pub struct StorageSqlx {
    pool: Pool<Sqlite>,          // SQLx connection pool
    settings: RwLock<Option<TableSettings>>,
    storage_identity_key: RwLock<String>,
    storage_name: RwLock<String>,
}
```

**Constructor methods:**
- `new(database_url: &str)` - Create from SQLite URL (e.g., `"sqlite:wallet.db"`)
- `in_memory()` - Create in-memory database (useful for testing)
- `open(path: &str)` - Open file-based database (creates if not exists)

### `DEFAULT_MAX_OUTPUT_SCRIPT`
Constant defining maximum script length stored inline (10,000 bytes). Scripts longer than this are retrieved from raw transactions.

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
| `abort_action()` | Abort transaction (stub) |
| `create_action()` | Create new transaction with inputs/outputs |
| `process_action()` | Process transaction (stub) |
| `internalize_action()` | Internalize external tx (stub) |

### WalletStorageSync Methods
| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state |
| `set_active()` | Set user's active storage |
| `get_sync_chunk()` | Get data chunk for sync (stub) |
| `process_sync_chunk()` | Apply sync chunk (stub) |

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

### Tests (35 total)
- 17 unit tests: txid computation, var_int parsing, script offsets, args validation, status determination
- 18 integration tests: all scenarios from Go's provider_process_action_test.go

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

### Internal Types
- `ExtendedInput` - Input with associated output context
- `ExtendedOutput` - Output with basket/tag metadata
- `GenerateChangeParams` - Parameters for change output generation
- `AllocatedChangeInput` - Selected change input for spending
- `ChangeOutput` - Generated change output details

## Database Schema

The module creates 16 tables via `migrations/001_initial.sql`:

### Core Tables
| Table | Purpose |
|-------|---------|
| `settings` | Singleton storage configuration |
| `users` | Wallet users by identity key |
| `transactions` | Transaction records |
| `outputs` | UTXOs and spent outputs |
| `output_baskets` | Output organization containers |

### Proof Tables
| Table | Purpose |
|-------|---------|
| `proven_txs` | Transactions with merkle proofs |
| `proven_tx_reqs` | Pending proof requests |

### Certificate Tables
| Table | Purpose |
|-------|---------|
| `certificates` | Identity certificates |
| `certificate_fields` | Certificate field values |

### Labeling Tables
| Table | Purpose |
|-------|---------|
| `tx_labels` | Transaction labels |
| `tx_labels_map` | Transaction-to-label mapping |
| `output_tags` | Output tags |
| `output_tags_map` | Output-to-tag mapping |

### Other Tables
| Table | Purpose |
|-------|---------|
| `commissions` | Transaction commissions |
| `sync_states` | Multi-storage sync state |
| `monitor_events` | System monitoring events |

## Usage

### Basic Setup
```rust
use bsv_wallet_toolbox::storage::sqlx::StorageSqlx;

// File-based storage
let storage = StorageSqlx::open("wallet.db").await?;

// Run migrations (creates tables)
storage.migrate("my-wallet", &storage_identity_key).await?;

// Make storage operational
let settings = storage.make_available().await?;

// Storage is now ready for use
assert!(storage.is_available());
```

### Testing with In-Memory Database
```rust
let storage = StorageSqlx::in_memory().await?;
storage.migrate("test", "00000...").await?;
storage.make_available().await?;

// Use storage...

// Cleanup (optional - memory is freed on drop)
storage.destroy().await?;
```

### User Management
```rust
let identity_key = "03abc..."; // 66-char hex public key

// Find or create user
let (user, is_new) = storage.find_or_insert_user(&identity_key).await?;

// User gets a default basket automatically
```

### Querying Outputs
```rust
use bsv_wallet_toolbox::storage::traits::{AuthId, FindOutputsArgs};

let auth = AuthId::with_user_id(&identity_key, user.user_id);

// Find all spendable outputs
let args = FindOutputsArgs {
    tx_status: Some(vec![TransactionStatus::Completed]),
    ..Default::default()
};

let outputs = storage.find_outputs(&auth, args).await?;
```

### Listing Actions (Transactions)
```rust
use bsv_sdk::wallet::ListActionsArgs;

let result = storage.list_actions(&auth, ListActionsArgs {
    labels: vec!["payment".to_string()],
    include_labels: Some(true),
    include_inputs: Some(true),
    include_outputs: Some(true),
    limit: Some(10),
    offset: Some(0),
    ..Default::default()
}).await?;

// Access transactions with full details
for action in result.actions {
    println!("Tx: {} satoshis", action.satoshis);
}
```

### Listing Outputs
```rust
use bsv_sdk::wallet::ListOutputsArgs;

let result = storage.list_outputs(&auth, ListOutputsArgs {
    basket: "default".to_string(),
    tags: Some(vec!["payment".to_string()]),
    include_tags: Some(true),
    include_locking_scripts: Some(true),
    limit: Some(10),
    ..Default::default()
}).await?;
```

### Certificate Operations
```rust
// Insert certificate
let cert_id = storage.insert_certificate(&auth, certificate).await?;

// Find certificates by certifier
let args = FindCertificatesArgs {
    certifiers: Some(vec!["03certifier_key...".to_string()]),
    ..Default::default()
};
let certs = storage.find_certificates(&auth, args).await?;

// Soft-delete certificate
storage.relinquish_certificate(&auth, RelinquishCertificateArgs {
    certificate_type: "...",
    certifier: "...",
    serial_number: "...",
}).await?;
```

### Creating Transactions
```rust
use bsv_sdk::wallet::{CreateActionArgs, CreateActionOutput};

let result = storage.create_action(&auth, CreateActionArgs {
    description: "Send payment".to_string(),
    outputs: Some(vec![CreateActionOutput {
        locking_script: recipient_script,
        satoshis: 10000,
        output_description: "Payment to recipient".to_string(),
        basket: Some("payments".to_string()),
        tags: Some(vec!["outgoing".to_string()]),
        ..Default::default()
    }]),
    labels: Some(vec!["payment".to_string()]),
    ..Default::default()
}).await?;

// Result contains inputs, outputs, and derivation info for signing
```

## Feature Flags

This module is conditionally compiled:

```toml
[features]
default = ["sqlite"]
sqlite = ["sqlx/sqlite"]
mysql = ["sqlx/mysql"]  # Planned, not yet implemented
```

## Implementation Notes

### Soft Deletes
Certificates and baskets use `is_deleted` flag for soft deletes rather than actual row removal. This preserves history for sync operations.

### Dynamic Query Building
The `find_*` and `list_*` methods build SQL dynamically based on provided filter arguments. Parameters are bound safely to prevent SQL injection.

### Settings Caching
Settings are loaded once via `make_available()` and cached in an `RwLock`. The `get_settings()` method returns a reference to cached data.

### Unsafe Pointer Casts
The trait signatures require `&self` returns but internal state uses `RwLock`. The implementation uses controlled unsafe pointer casts (`storage_sqlx.rs:895`, `storage_sqlx.rs:2134-2140`) as a workaround. This is safe because settings don't change after `make_available()`.

### Stub Methods
Several methods return placeholder results marked with `// TODO`:
- `abort_action()` - Returns `{ aborted: false }`
- `internalize_action()` - Returns error
- `get_sync_chunk()`, `process_sync_chunk()` - Return minimal defaults

### Fully Implemented Methods
- `list_actions()` - Full support for labels, inputs, outputs, pagination
- `list_certificates()` - Full support for filters, fields, keyring
- `list_outputs()` - Full support for baskets, tags, locking scripts
- `create_action()` - Full transaction creation via `create_action.rs`
- `process_action()` - Full signed transaction processing via `process_action.rs` (1:1 parity with Go/TypeScript)

## Tests

### process_action.rs Tests (`process_action.rs:538-1250`)
```rust
// Unit tests
#[test] fn test_compute_txid()                    // Double SHA256 txid computation
#[test] fn test_validate_txid_matches_raw_tx_*()  // txid validation (success/failure)
#[test] fn test_read_var_int_*()                  // VarInt parsing
#[test] fn test_parse_tx_script_offsets_*()       // Script offset extraction
#[test] fn test_validate_process_action_args_*()  // Args validation (all cases)
#[test] fn test_determine_statuses_*()            // Status determination (4 modes)
#[test] fn test_generate_batch_id()               // Batch ID generation

// Integration tests (match Go tests)
#[tokio::test] async fn test_process_action_missing_reference()           // Reference not found
#[tokio::test] async fn test_process_action_invalid_txid()                // txid mismatch
#[tokio::test] async fn test_process_action_with_nosend()                 // NoSend mode
#[tokio::test] async fn test_process_action_with_delayed()                // Delayed mode
#[tokio::test] async fn test_process_action_immediate_broadcast()         // Immediate mode
#[tokio::test] async fn test_process_action_verify_tx_updated()           // DB verification
#[tokio::test] async fn test_process_action_verify_proven_tx_req_created()// ProvenTxReq creation
#[tokio::test] async fn test_process_action_already_processed()           // Status error
#[tokio::test] async fn test_process_action_twice_with_is_new_tx_false()  // Re-broadcast
#[tokio::test] async fn test_process_action_is_new_tx_false_for_unstored()// Unknown tx
#[tokio::test] async fn test_process_action_missing_input_beef()          // inputBEEF check
#[tokio::test] async fn test_process_action_not_outgoing()                // isOutgoing check
#[tokio::test] async fn test_process_action_with_send_with_batch()        // Batch creation
#[tokio::test] async fn test_process_action_send_with_overrides_no_send() // SendWith priority
#[tokio::test] async fn test_process_action_locking_script_mismatch()     // Script validation
#[tokio::test] async fn test_process_action_outputs_updated_with_offsets()// Output updates
#[tokio::test] async fn test_process_action_proven_tx_req_status_modes()  // All status modes
```

### storage_sqlx.rs Tests (`storage_sqlx.rs:2144-2673`)
```rust
#[tokio::test]
async fn test_in_memory_storage()           // Migration and availability
async fn test_find_or_insert_user()         // User creation and lookup
async fn test_find_outputs()                // Output querying
async fn test_list_actions_empty()          // Empty action list
async fn test_list_actions_with_data()      // Action listing with data
async fn test_list_actions_with_labels()    // Label filtering
async fn test_list_outputs_empty()          // Empty output list
async fn test_list_outputs_nonexistent_basket() // Missing basket handling
async fn test_list_certificates_empty()     // Empty certificate list
async fn test_list_certificates_with_data() // Certificate with fields
async fn test_list_certificates_with_filters() // Certifier/type filtering
```

### create_action.rs Tests (`create_action.rs:1371-2087`)
```rust
#[test]
fn test_var_int_size()                      // VarInt encoding sizes
fn test_calculate_transaction_size()        // TX size calculation
fn test_random_derivation()                 // Random derivation paths
fn test_validate_description_too_short()    // Description validation
fn test_validate_description_too_long()     // Description validation
fn test_validate_description_valid()        // Description validation
fn test_validate_empty_label()              // Label validation
fn test_validate_label_too_long()           // Label validation
fn test_validate_output_empty_locking_script() // Output validation
fn test_validate_output_satoshis_too_high() // Satoshi limits
fn test_validate_output_description_too_short() // Output description
fn test_validate_output_empty_basket()      // Basket validation
fn test_validate_output_empty_tag()         // Tag validation
fn test_validate_valid_output()             // Valid output case
fn test_validate_max_possible_satoshis()    // Sentinel value
fn test_validate_input_missing_unlocking_script() // Input validation
fn test_validate_input_unlocking_script_length_mismatch() // Length check
fn test_validate_duplicate_input_outpoints() // Duplicate detection

#[tokio::test]
async fn test_create_action_basic()         // Basic creation (insufficient funds)
async fn test_create_action_with_labels()   // Labels support
async fn test_create_action_with_tags_and_basket() // Tags and baskets
async fn test_create_action_no_send()       // NoSend mode
async fn test_create_action_multiple_outputs() // Multiple outputs
async fn test_create_action_with_version_and_locktime() // Version/locktime
```

Run with:
```bash
cargo test --features sqlite storage::sqlx
```

## Related

- `../traits.rs` - Trait definitions (`WalletStorageReader`, `WalletStorageWriter`, etc.)
- `../entities.rs` - Table entity structs (`TableUser`, `TableOutput`, etc.)
- `../client/` - Remote storage client (alternative implementation)
- `../../error.rs` - Error types used by this module
