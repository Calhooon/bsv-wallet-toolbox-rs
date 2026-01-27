# SQLx Storage Module
> SQLite-based persistent storage implementation for wallet state.

## Overview

This module provides a production-ready storage backend for BSV wallet state using SQLx with SQLite. It implements the full `WalletStorageProvider` trait hierarchy, enabling persistent storage of transactions, outputs, certificates, and synchronization state. The implementation mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module definition and public exports |
| `storage_sqlx.rs` | Complete `StorageSqlx` implementation (~1400 lines) |
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
| `list_actions()` | List transactions (stub) |
| `list_certificates()` | List certificates (stub) |
| `list_outputs()` | List outputs (stub) |

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
| `create_action()` | Create transaction (stub) |
| `process_action()` | Process transaction (stub) |
| `internalize_action()` | Internalize external tx (stub) |

### WalletStorageSync Methods
| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state |
| `set_active()` | Set user's active storage |
| `get_sync_chunk()` | Get data chunk for sync (stub) |
| `process_sync_chunk()` | Apply sync chunk (stub) |

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
The `find_*` methods build SQL dynamically based on provided filter arguments. Parameters are bound safely to prevent SQL injection.

### Settings Caching
Settings are loaded once via `make_available()` and cached in an `RwLock`. The `get_settings()` method returns a reference to cached data.

### Unsafe Pointer Casts
The trait signatures require `&self` returns but internal state uses `RwLock`. The implementation uses controlled unsafe pointer casts (`storage_sqlx.rs:895`, `storage_sqlx.rs:1305`) as a workaround. This is safe because settings don't change after `make_available()`.

### Stub Methods
Several methods return placeholder results marked with `// TODO`:
- `list_actions()`, `list_certificates()`, `list_outputs()` - return empty results
- `abort_action()` - returns `{ aborted: false }`
- `create_action()`, `process_action()`, `internalize_action()` - return errors
- `get_sync_chunk()`, `process_sync_chunk()` - return minimal defaults

## Tests

The module includes unit tests (`storage_sqlx.rs:1316-1379`):

```rust
#[tokio::test]
async fn test_in_memory_storage()      // Migration and availability
async fn test_find_or_insert_user()    // User creation and lookup
async fn test_find_outputs()           // Output querying
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
