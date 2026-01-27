# Chaintracks Storage Backends
> Storage implementations for BSV blockchain header tracking

## Overview

This module provides storage backends for the Chaintracks block header tracking system. Storage backends handle persistence and retrieval of blockchain headers, supporting both "live" headers (recent, mutable, fork-tracking) and "bulk" headers (historical, immutable). Currently implements an in-memory backend suitable for testing, development, and mobile clients.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports; re-exports `MemoryStorage` |
| `memory.rs` | In-memory storage implementation (`MemoryStorage`) |

## Key Exports

### `MemoryStorage`

The primary export is `MemoryStorage`, an in-memory implementation of the `ChaintracksStorage` trait.

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

## Public Helper Methods

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

`MemoryStorage` implements three storage traits from `crate::chaintracks`:

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

| Method | Description |
|--------|-------------|
| `insert_header(header)` | Insert header, returns `InsertHeaderResult` |
| `prune_live_block_headers(tip_height)` | Remove old inactive headers |
| `migrate_live_to_bulk(count)` | No-op (no bulk storage) |
| `delete_older_live_block_headers(max_height)` | Delete headers at or below height |
| `make_available()` | No-op (always available) |
| `migrate_latest()` | No-op (no migrations needed) |
| `drop_all_data()` | Clear all stored headers |
| `destroy()` | Alias for `drop_all_data()` |

### `ChaintracksStorage` (full interface)

| Method | Description |
|--------|-------------|
| `storage_type()` | Returns `"memory"` |
| `is_available()` | Always returns `true` |

## Internal Data Structures

`MemoryStorage` uses five `RwLock`-protected HashMaps for concurrent access:

```
┌─────────────────────────────────────────────────────────────┐
│                     MemoryStorage                            │
├─────────────────────────────────────────────────────────────┤
│  headers: HashMap<i64, LiveBlockHeader>                      │
│           └── Primary storage, indexed by header_id          │
│                                                              │
│  hash_to_id: HashMap<String, i64>                           │
│              └── Block hash → header_id lookup               │
│                                                              │
│  height_to_id: HashMap<u32, i64>                            │
│                └── Height → header_id (active chain only)    │
│                                                              │
│  merkle_to_id: HashMap<String, i64>                         │
│                └── Merkle root → header_id (active only)     │
│                                                              │
│  next_id: i64                                                │
│           └── Next header_id to allocate                     │
│                                                              │
│  tip_id: Option<i64>                                        │
│          └── Current chain tip header_id                     │
└─────────────────────────────────────────────────────────────┘
```

## Usage

### Basic Usage

```rust
use bsv_wallet_toolbox::chaintracks::{Chain, MemoryStorage, ChaintracksStorage};

// Create storage for testnet
let storage = MemoryStorage::new(Chain::Test);

// Storage is immediately available
assert!(storage.is_available().await);
assert_eq!(storage.storage_type(), "memory");
```

### Inserting Headers

```rust
use bsv_wallet_toolbox::chaintracks::{
    Chain, MemoryStorage, LiveBlockHeader,
    ChaintracksStorageIngest, ChaintracksStorageQuery
};

let storage = MemoryStorage::new(Chain::Main);

let header = LiveBlockHeader {
    version: 1,
    previous_hash: "0".repeat(64),  // Genesis has no parent
    merkle_root: "4a5e1e...".to_string(),
    time: 1231006505,
    bits: 0x1d00ffff,
    nonce: 2083236893,
    height: 0,
    hash: "000000000019d6...".to_string(),
    chain_work: "1".to_string(),
    is_chain_tip: false,    // Set by insert
    is_active: false,       // Set by insert
    header_id: 0,           // Assigned by insert
    previous_header_id: None,
};

let result = storage.insert_header(header).await?;

if result.added {
    println!("Header added successfully");
    if result.is_active_tip {
        println!("This is now the chain tip");
    }
}
```

### Querying Headers

```rust
// Find by height
if let Some(header) = storage.find_header_for_height(100).await? {
    println!("Block 100 hash: {}", header.hash);
}

// Find by hash
if let Some(live_header) = storage.find_live_header_for_block_hash(&hash).await? {
    println!("Found header at height {}", live_header.height);
}

// Get current tip
if let Some(tip) = storage.find_chain_tip_header().await? {
    println!("Chain tip: {} at height {}", tip.hash, tip.height);
}
```

### Custom Thresholds

```rust
// For mobile clients: shorter retention
let storage = MemoryStorage::with_thresholds(
    Chain::Main,
    500,   // live_height_threshold: keep fewer headers
    100,   // reorg_height_threshold: expect smaller reorgs
);
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

## Concurrency

All internal state uses `RwLock` for thread-safe access:
- Multiple readers can query concurrently
- Writers get exclusive access
- `async_trait` enables use in async contexts

Note: Lock acquisition is synchronous (uses `unwrap()`). In high-contention scenarios, consider the SQLite backend (when implemented).

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

## Limitations

1. **No persistence**: Data lost on process restart
2. **No bulk storage**: `get_available_height_ranges()` returns empty, `migrate_live_to_bulk()` is no-op
3. **Memory bound**: All headers stored in RAM
4. **Single process**: Cannot share state across processes

For persistent storage, a SQLite backend is planned (see `mod.rs` TODO comment).

## Testing

The module includes comprehensive tests (~500 lines) covering:

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

Example test:
```rust
#[tokio::test]
async fn test_chain_growth() {
    let storage = MemoryStorage::new(Chain::Test);

    // Insert genesis
    let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
    storage.insert_header(genesis).await.unwrap();

    // Insert block 1
    let block1 = create_test_header(1, "hash_0", "hash_1");
    let result = storage.insert_header(block1).await.unwrap();
    assert!(result.added);
    assert!(result.is_active_tip);
    assert_eq!(result.reorg_depth, 0);

    // Verify chain
    let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
    assert_eq!(tip.height, 1);
}
```

Run tests with:
```bash
cargo test --lib chaintracks::storage
```

## Related

- [`../CLAUDE.md`](../CLAUDE.md) - Parent Chaintracks module documentation (if exists)
- [`../traits.rs`](../traits.rs) - Defines `ChaintracksStorage`, `ChaintracksStorageQuery`, `ChaintracksStorageIngest` traits
- [`../types.rs`](../types.rs) - Defines `Chain`, `LiveBlockHeader`, `BlockHeader`, `HeightRange`, `InsertHeaderResult`

## TypeScript Origin

Ported from TypeScript implementation:
- Source: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/`
- Equivalent: `ChaintracksStorageNoDb.ts` → `memory.rs`
