# Chaintracks Storage Backends
> Storage implementations for BSV blockchain header tracking

## Overview

This module provides storage backends for the Chaintracks block header tracking system. Storage backends handle persistence and retrieval of blockchain headers, supporting both "live" headers (recent, mutable, fork-tracking) and "bulk" headers (historical, immutable). Two implementations are available:

- **MemoryStorage**: In-memory backend for testing, development, and mobile clients
- **SqliteStorage**: SQLite-based persistent storage (requires `sqlite` or `mysql` feature)

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 11 | Module exports; conditionally re-exports storage implementations |
| `memory.rs` | 1195 | In-memory storage implementation (`MemoryStorage`) with 31 tests |
| `sqlite.rs` | 1917 | SQLite-based persistent storage (`SqliteStorage`) - feature-gated, 37 tests |

## Key Exports

### `MemoryStorage`

An in-memory implementation of the `ChaintracksStorage` trait.

```rust
pub struct MemoryStorage {
    chain: Chain,
    live_height_threshold: u32,
    reorg_height_threshold: u32,
    headers: RwLock<HashMap<i64, LiveBlockHeader>>,
    hash_to_id: RwLock<HashMap<String, i64>>,
    height_to_id: RwLock<HashMap<u32, i64>>,
    merkle_to_id: RwLock<HashMap<String, i64>>,  // Merkle root index for active headers
    next_id: RwLock<i64>,
    tip_id: RwLock<Option<i64>>,
}
```

**Constructors:**

| Method | Description |
|--------|-------------|
| `new(chain: Chain)` | Create storage with default thresholds (2000 live, 400 reorg) |
| `with_thresholds(chain, live_threshold, reorg_threshold)` | Create with custom thresholds |

**Use cases:**
- Unit and integration testing
- Development environments
- Mobile/embedded clients (ephemeral data acceptable)
- Short-lived processes where persistence isn't needed

### `SqliteStorage`

A SQLite-based persistent storage implementation (available with `sqlite` or `mysql` feature).

```rust
pub struct SqliteStorage {
    pool: Pool<Sqlite>,
    chain: Chain,
    live_height_threshold: u32,
    reorg_height_threshold: u32,
    available: RwLock<bool>,
}
```

**Constructors:**

| Method | Description |
|--------|-------------|
| `new(database_url, chain)` | Create storage connecting to SQLite database |
| `with_thresholds(database_url, chain, live_threshold, reorg_threshold)` | Create with custom thresholds |
| `in_memory(chain)` | Create with in-memory SQLite database (for testing) |

**Additional Methods:**

| Method | Description |
|--------|-------------|
| `pool()` | Get reference to the underlying SQLx connection pool |
| `header_count()` | Get total number of stored headers (async) |
| `live_header_exists(hash)` | Optimized existence check for a header by hash |
| `find_headers_for_height_less_than_or_equal_sorted(height, limit)` | Find headers at or below height, sorted ascending |
| `delete_live_headers_by_ids(ids)` | Delete headers by their IDs |
| `set_chain_tip_by_id(id, is_tip)` | Set the chain tip flag for a header |
| `set_active_by_id(id, is_active)` | Set the active flag for a header |
| `insert_headers_batch(headers)` | Batch insert for bulk ingestion (10k+ headers) |
| `update_chain_tip_to_highest()` | Set chain tip to header with highest height |
| `get_headers_by_height_range(start, end)` | Get active headers in a height range |
| `get_headers_at_height(height)` | Get all headers at a height (including forks) |
| `get_active_headers()` | Get all active (main chain) headers |
| `get_fork_headers()` | Get all inactive (fork) headers |
| `find_children(parent_hash)` | Find headers that build on a given hash |
| `mark_headers_inactive_above_height(height)` | Mark headers at or above height as inactive |

**Use cases:**
- Production deployments requiring persistence
- Desktop applications
- Server-side wallet services
- Long-running processes

**Database Schema:**

```sql
CREATE TABLE chaintracks_live_headers (
    header_id INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_header_id INTEGER,
    previous_hash TEXT NOT NULL,
    height INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    is_chain_tip INTEGER NOT NULL DEFAULT 0,
    hash TEXT NOT NULL UNIQUE,
    chain_work TEXT NOT NULL,
    version INTEGER NOT NULL,
    merkle_root TEXT NOT NULL,
    time INTEGER NOT NULL,
    bits INTEGER NOT NULL,
    nonce INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

**Indexes:**
- `idx_live_headers_height` on `height`
- `idx_live_headers_active` on `is_active`
- `idx_live_headers_tip` on `is_chain_tip`
- `idx_live_headers_merkle` on `merkle_root` (partial: where `is_active = 1`)

## Public Helper Methods (MemoryStorage)

In addition to trait implementations, `MemoryStorage` provides utility methods:

| Method | Description |
|--------|-------------|
| `header_count()` | Get total number of stored headers |
| `get_headers_at_height(height)` | Get all headers at a height (including forks) |
| `get_active_headers()` | Get all headers on the main chain |
| `get_fork_headers()` | Get all headers on fork chains (inactive) |
| `find_children(parent_hash)` | Find headers that build on a given hash |
| `insert_headers_batch(headers)` | Insert multiple headers sequentially |

## Implemented Traits

Both `MemoryStorage` and `SqliteStorage` implement three storage traits from `crate::chaintracks`:

### `ChaintracksStorageQuery` (read operations)

| Method | Description |
|--------|-------------|
| `chain()` | Returns the `Chain` (Main/Test) |
| `live_height_threshold()` | Returns threshold for live header retention |
| `reorg_height_threshold()` | Returns max reorg depth tracked |
| `find_chain_tip_header()` | Get current chain tip as `LiveBlockHeader` |
| `find_chain_tip_hash()` | Get current chain tip hash string |
| `find_header_for_height(height)` | Find header at specific height |
| `find_live_header_for_block_hash(hash)` | Find live header by block hash |
| `find_live_header_for_merkle_root(merkle_root)` | Find active header by merkle root (uses index) |
| `get_headers_bytes(height, count)` | Get serialized 80-byte headers |
| `get_live_headers()` | Get all stored live headers (sorted by height desc) |
| `get_available_height_ranges()` | Returns empty (no bulk storage) |
| `find_live_height_range()` | Get min/max height range of active headers |
| `find_common_ancestor(header1, header2)` | Find common ancestor for fork resolution |
| `find_reorg_depth(new_header)` | Calculate reorg depth from current tip |

### `ChaintracksStorageIngest` (write operations)

| Method | MemoryStorage | SqliteStorage |
|--------|---------------|---------------|
| `insert_header(header)` | Insert header, returns `InsertHeaderResult` | Same |
| `prune_live_block_headers(tip_height)` | Remove old inactive headers | Same (with FK handling) |
| `migrate_live_to_bulk(count)` | No-op (no bulk storage) | No-op |
| `delete_older_live_block_headers(max_height)` | Delete headers at or below height | Same (with FK handling) |
| `make_available()` | No-op (always available) | Sets available flag to true |
| `migrate_latest()` | No-op (no migrations needed) | Creates database tables and indexes |
| `drop_all_data()` | Clear all stored headers | DELETE all rows |
| `destroy()` | Alias for `drop_all_data()` | Same |

### `ChaintracksStorage` (full interface)

| Method | MemoryStorage | SqliteStorage |
|--------|---------------|---------------|
| `storage_type()` | Returns `"memory"` | Returns `"sqlite"` |
| `is_available()` | Always returns `true` | Returns value set by `make_available()` |

## Usage

### Basic Usage (MemoryStorage)

```rust
use bsv_wallet_toolbox::chaintracks::{Chain, MemoryStorage, ChaintracksStorage};

// Create storage for testnet
let storage = MemoryStorage::new(Chain::Test);

// Storage is immediately available
assert!(storage.is_available().await);
assert_eq!(storage.storage_type(), "memory");
```

### Basic Usage (SqliteStorage)

```rust
use bsv_wallet_toolbox::chaintracks::{Chain, SqliteStorage, ChaintracksStorage};

// Create persistent storage
let storage = SqliteStorage::new("sqlite:chaintracks.db", Chain::Main).await?;

// Run migrations to create tables
storage.migrate_latest().await?;

// Mark as available
storage.make_available().await?;

assert!(storage.is_available().await);
assert_eq!(storage.storage_type(), "sqlite");
```

### Custom Thresholds

```rust
// MemoryStorage: for mobile clients with shorter retention
let storage = MemoryStorage::with_thresholds(
    Chain::Main,
    500,   // live_height_threshold: keep fewer headers
    100,   // reorg_height_threshold: expect smaller reorgs
);

// SqliteStorage: with custom thresholds
let storage = SqliteStorage::with_thresholds(
    "sqlite:chaintracks.db",
    Chain::Main,
    500,
    100,
).await?;
```

## Header Insertion Logic

When `insert_header` is called, the following logic executes:

1. **Duplicate check**: If hash already exists, returns `{ dupe: true }`
2. **ID allocation**: Assigns unique `header_id`
3. **Chain work calculation**: Computes chain work from `bits` if not set
4. **Previous header linking**: Finds `previous_header_id` from `previous_hash`
5. **Tip determination**: Header becomes tip if height > current tip height
6. **Reorg detection**: If new tip doesn't extend old tip, handles chain reorganization
7. **Index updates**: Updates `hash_to_id`, `height_to_id`, and `merkle_to_id` maps

Returns `InsertHeaderResult`:
```rust
InsertHeaderResult {
    added: bool,           // Successfully stored
    dupe: bool,            // Was duplicate
    no_prev: bool,         // Previous header not found
    no_tip: bool,          // No prior tip existed (first header)
    is_active_tip: bool,   // Became chain tip
    reorg_depth: u32,      // Blocks replaced if reorg
    prior_tip: Option<LiveBlockHeader>,
    deactivated_headers: Vec<LiveBlockHeader>,  // Headers removed from active chain
}
```

## Reorg Handling

When a reorg is detected (new tip doesn't extend current tip), `handle_reorg` executes:

1. **Find common ancestor**: Walk back from both tips until hashes match
2. **Deactivate old chain**: Mark headers from old tip to ancestor as `is_active = false`
3. **Activate new chain**: Mark headers from new tip to ancestor as `is_active = true`
4. **Update indexes**: Remove old chain from `height_to_id` and `merkle_to_id`, add new chain
5. **Return deactivated headers**: For notification/rollback purposes

## Pruning Behavior

Two methods manage header cleanup:

**`prune_live_block_headers(tip_height)`**: Removes old *inactive* (fork) headers
- Calculates threshold as `tip_height - live_height_threshold`
- Only removes headers with `is_active = false` below threshold
- Active chain headers are preserved regardless of height
- Returns count of pruned headers

**`delete_older_live_block_headers(max_height)`**: Force-deletes by height
- Removes *all* headers at or below `max_height` (active or not)
- Updates all indexes (`hash_to_id`, `height_to_id`, `merkle_to_id`)
- Use with caution as it can break chain continuity

## Batch Insert for Bulk Ingestion

The `SqliteStorage::insert_headers_batch()` method is optimized for the bulk ingestor, which needs to insert 10,000+ headers at a time during initial sync.

```rust
// Create batch of headers (typically from CDN or WoC)
let headers: Vec<LiveBlockHeader> = fetch_bulk_headers().await?;

// Batch insert with transaction
let inserted = storage.insert_headers_batch(&headers).await?;

// Update chain tip after batch
let tip = storage.update_chain_tip_to_highest().await?;
```

**Key features:**
- Uses SQLite transaction for atomicity and performance
- Automatic duplicate detection (skips existing hashes)
- Previous header ID linking for chain integrity
- Chain work calculation for headers missing it
- Does NOT update chain tip during insert (call `update_chain_tip_to_highest()` after)

**Performance characteristics:**
- 1,000 headers: ~100ms
- Duplicate checking done in chunks of 500 for memory efficiency
- Single transaction commit at the end

## Go Implementation Parity

The following methods match the Go `StorageQueries` interface:

| Go Method | Rust Method |
|-----------|-------------|
| `LiveHeaderExists(hash)` | `live_header_exists(hash)` |
| `GetLiveHeaderByHash(hash)` | `find_live_header_for_block_hash(hash)` |
| `GetActiveTipLiveHeader()` | `find_chain_tip_header()` |
| `SetChainTipByID(id, bool)` | `set_chain_tip_by_id(id, bool)` |
| `SetActiveByID(id, bool)` | `set_active_by_id(id, bool)` |
| `InsertNewLiveHeader(header)` | `insert_header(header)` |
| `CountLiveHeaders()` | `header_count()` |
| `GetLiveHeaderByHeight(height)` | `find_header_for_height(height)` |
| `FindLiveHeightRange()` | `find_live_height_range()` |
| `FindHeadersForHeightLessThanOrEqualSorted(height, limit)` | `find_headers_for_height_less_than_or_equal_sorted(height, limit)` |
| `DeleteLiveHeadersByIDs(ids)` | `delete_live_headers_by_ids(ids)` |

## Limitations

### MemoryStorage

1. **No persistence**: Data lost on process restart
2. **No bulk storage**: `get_available_height_ranges()` returns empty, `migrate_live_to_bulk()` is no-op
3. **Memory bound**: All headers stored in RAM
4. **Single process**: Cannot share state across processes

### SqliteStorage

1. **No bulk storage**: Like MemoryStorage, `get_available_height_ranges()` returns empty, `migrate_live_to_bulk()` is no-op
2. **Feature-gated**: Requires `sqlite` or `mysql` feature flag
3. **Initialization required**: Must call `migrate_latest()` before use and `make_available()` to enable
4. **Connection overhead**: Database connections have startup cost compared to memory storage

## Feature Flags

SqliteStorage is conditionally compiled based on feature flags in `Cargo.toml`:

```rust
// In mod.rs
#[cfg(any(feature = "sqlite", feature = "mysql"))]
mod sqlite;

#[cfg(any(feature = "sqlite", feature = "mysql"))]
pub use sqlite::SqliteStorage;
```

Enable with:
```bash
cargo build --features sqlite
# or
cargo build --features mysql
```

## Testing

The module includes 68 total tests (31 in memory.rs, 37 in sqlite.rs) covering:

- Basic insertion and retrieval
- Chain growth and tip tracking
- Duplicate detection
- Hash and merkle root lookups
- Header byte serialization
- Height range queries
- Pruning inactive headers
- Deletion by height
- Common ancestor finding
- Batch insertion
- Sorted live headers
- Fork header tracking
- Children finding
- Reorg depth calculation
- Storage type and availability

### MemoryStorage Tests (31 tests)

| Category | Tests |
|----------|-------|
| Basic CRUD | `test_memory_storage_basic`, `test_duplicate_detection`, `test_hash_lookup`, `test_merkle_root_lookup`, `test_merkle_root_lookup_inactive` |
| Chain operations | `test_chain_growth`, `test_common_ancestor_same_chain`, `test_find_reorg_depth`, `test_no_prev_header` |
| Batch operations | `test_batch_insert`, `test_get_live_headers_sorted` |
| Queries | `test_headers_bytes_serialization`, `test_headers_bytes_multiple`, `test_live_height_range`, `test_get_headers_at_height`, `test_available_height_ranges` |
| Active/fork headers | `test_get_active_headers`, `test_get_fork_headers_empty`, `test_find_children` |
| Cleanup | `test_pruning`, `test_delete_older`, `test_drop_all_data`, `test_destroy` |
| No-ops | `test_migrate_live_to_bulk`, `test_make_available`, `test_migrate_latest` |
| Configuration | `test_storage_type`, `test_is_available`, `test_chain_accessor`, `test_thresholds`, `test_default_thresholds` |

### SqliteStorage Tests (37 tests)

| Category | Tests |
|----------|-------|
| Basic CRUD | `test_insert_header`, `test_duplicate_detection`, `test_find_by_hash`, `test_find_by_height`, `test_find_merkle_root` |
| Chain operations | `test_chain_growth`, `test_common_ancestor_detection`, `test_reorg_handling`, `test_reorg_depth_calculation` |
| Batch operations | `test_batch_insert`, `test_batch_insert_empty`, `test_batch_insert_with_duplicates`, `test_batch_insert_large` (1000 headers) |
| Go parity methods | `test_live_header_exists`, `test_find_headers_for_height_less_than_or_equal_sorted`, `test_find_headers_with_limit`, `test_delete_live_headers_by_ids`, `test_delete_empty_ids`, `test_set_chain_tip_by_id`, `test_set_active_by_id` |
| Height queries | `test_get_headers_by_height_range`, `test_get_headers_at_height`, `test_find_live_height_range` |
| Active/fork headers | `test_get_active_headers`, `test_get_fork_headers`, `test_find_children`, `test_mark_headers_inactive_above_height` |
| Chain tip | `test_update_chain_tip_to_highest`, `test_update_chain_tip_empty_storage` |
| Edge cases | `test_empty_database_queries`, `test_headers_bytes_multiple` |
| Cleanup | `test_prune_inactive`, `test_drop_all_data`, `test_destroy` |
| Configuration | `test_storage_type`, `test_is_available`, `test_get_headers_bytes` |

Run tests with:
```bash
# MemoryStorage tests (always available, 31 tests)
cargo test --lib chaintracks::storage::memory

# SqliteStorage tests (requires feature, 37 tests)
cargo test --lib chaintracks::storage::sqlite --features sqlite

# All storage tests
cargo test --lib chaintracks::storage
```

## Internal Dependencies

- **`crate::lock_utils`**: Both `MemoryStorage` and `SqliteStorage` use `lock_read`/`lock_write` helpers for `std::sync::RwLock` access with error handling
- **`crate::chaintracks`**: Imports `calculate_work`, `BlockHeader`, `Chain`, `ChaintracksStorage`, `ChaintracksStorageIngest`, `ChaintracksStorageQuery`, `HeightRange`, `InsertHeaderResult`, `LiveBlockHeader`
- **`tracing`**: Both backends use `debug!`, `info!`, `warn!` for structured logging
- **`chrono::Utc`**: SqliteStorage uses `Utc::now()` for `created_at`/`updated_at` timestamps
- **`sqlx`**: SqliteStorage uses `Pool<Sqlite>`, `SqlitePool`, `Row` trait for database access

## Related

- [`../CLAUDE.md`](../CLAUDE.md) - Parent Chaintracks module documentation
- [`../traits.rs`](../traits.rs) - Defines `ChaintracksStorage`, `ChaintracksStorageQuery`, `ChaintracksStorageIngest` traits
- [`../types.rs`](../types.rs) - Defines `Chain`, `LiveBlockHeader`, `BlockHeader`, `HeightRange`, `InsertHeaderResult`

## Origin

Ported from TypeScript and Go implementations:
- TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/`
  - `ChaintracksStorageNoDb.ts` → `memory.rs`
- Go: `pkg/services/chaintracks/gormstorage/`
  - Used as reference for `sqlite.rs` implementation
