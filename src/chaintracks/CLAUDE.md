# Chaintracks Module

> Block header tracking system for BSV blockchain with two-tier storage architecture

## Overview

Chaintracks is a Rust port of the TypeScript Chaintracks implementation, providing blockchain header synchronization and validation. It uses a two-tier storage architecture: **bulk storage** for immutable historical headers (height-indexed) and **live storage** for recent mutable headers that track forks and reorgs. The system coordinates bulk ingestors (for historical data from CDN/WhatsOnChain) with live ingestors (real-time WebSocket/polling) to maintain a synchronized view of the blockchain.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module entry point; re-exports all public types, traits, ingestors, and `ChainTracker` from bsv-sdk |
| `types.rs` | Core data structures: `Chain`, `BlockHeader`, `LiveBlockHeader`, `HeightRange`, `calculate_work()` |
| `traits.rs` | Trait definitions: `ChaintracksClient`, `ChaintracksStorage`, `BulkIngestor`, `LiveIngestor`, `ChaintracksOptions` |
| `chaintracks.rs` | Main `Chaintracks` orchestrator implementing client and management interfaces |
| `storage/mod.rs` | Storage backend module; exports `MemoryStorage` and `SqliteStorage` (feature-gated) |
| `storage/memory.rs` | In-memory storage implementation with reorg handling, fork tracking, and batch operations |
| `storage/sqlite.rs` | SQLite-based persistent storage (requires `sqlite` or `mysql` feature) |
| `ingestors/mod.rs` | Ingestor module; re-exports all bulk and live ingestor implementations with helper types |
| `ingestors/bulk_cdn.rs` | CDN-based bulk header downloads from Babbage Systems |
| `ingestors/bulk_woc.rs` | WhatsOnChain API bulk header fetching (fallback) |
| `ingestors/live_polling.rs` | Polling-based live header detection via WOC API |
| `ingestors/live_websocket.rs` | WebSocket-based real-time header streaming via WOC |

## Architecture

```
                    ┌──────────────────┐
                    │   Chaintracks    │
                    │  (orchestrator)  │
                    └────────┬─────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  BulkIngestor   │ │  LiveIngestor   │ │    Storage      │
│  (CDN, WoC)     │ │ (WS, Polling)   │ │ (Memory, SQLite)│
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### Two-Tier Storage

- **Live Storage**: Recent headers (default: last 2000 blocks) in mutable storage. Tracks forks, handles reorgs, maintains `is_active` and `is_chain_tip` flags.
- **Bulk Storage**: Historical headers migrated from live storage. Immutable, height-indexed for fast lookups.

## Key Exports

### Enums

- **`Chain`**: Network identifier (`Main`, `Test`)

### Structs - Block Headers

| Struct | Description |
|--------|-------------|
| `BaseBlockHeader` | Raw header without height/hash (80 bytes when serialized) |
| `BlockHeader` | Header with computed hash and height |
| `LiveBlockHeader` | Extended header with chain tracking: `chain_work`, `is_chain_tip`, `is_active`, `header_id`, `previous_header_id` |

### Structs - Results and Ranges

| Struct | Description |
|--------|-------------|
| `HeightRange` | Inclusive height range with utility methods (`count`, `contains`, `overlaps`, `merge`, `subtract`) |
| `InsertHeaderResult` | Insert operation result with reorg detection flags |
| `ChaintracksInfo` | System status information |
| `ReorgEvent` | Reorg notification with `depth`, `old_tip`, `new_tip`, `deactivated_headers` |
| `BulkSyncResult` | Bulk synchronization result |

### Configuration

- **`ChaintracksOptions`**: Configuration struct with fields:
  - `chain`: `Chain` (Main/Test)
  - `live_height_threshold`: Headers kept in live storage (default: 2000)
  - `reorg_height_threshold`: Max reorg depth to track (default: 400)
  - `add_live_recursion_limit`: Max recursive lookups (default: 36)
  - `batch_insert_limit`: Batch size for inserts (default: 400)
  - `bulk_migration_chunk_size`: Migration chunk size (default: 500)

### Traits

| Trait | Purpose |
|-------|---------|
| `ChaintracksClient` | Read-only client API: query headers, subscribe to events |
| `ChaintracksManagement` | Management API: destroy, validate, export |
| `ChaintracksStorageQuery` | Storage read operations |
| `ChaintracksStorageIngest` | Storage write operations |
| `ChaintracksStorage` | Full storage provider (combines Query + Ingest) |
| `BulkIngestor` | Historical header fetching interface |
| `LiveIngestor` | Real-time header streaming interface |

### Callback Types

- **`HeaderCallback`**: `Box<dyn Fn(BlockHeader) + Send + Sync>` - New header notification
- **`ReorgCallback`**: `Box<dyn Fn(ReorgEvent) + Send + Sync>` - Reorg notification

### Re-exported Types

- **`ChainTracker`**: Re-exported from `bsv_sdk::transaction::ChainTracker` for convenience

### Utility Functions

- **`calculate_work(bits: u32) -> String`**: Calculate chain work from difficulty bits (compact target format). Returns 64-character hex string representing relative work value.

### Storage Implementations

- **`MemoryStorage`**: In-memory storage suitable for testing/development/mobile. Data lost on restart. Features:
  - Reorg handling with `handle_reorg()` internal method
  - Hash/height/merkle root indexing via `HashMap`
  - Batch insert support via `insert_headers_batch()`
  - Fork tracking with `get_fork_headers()`, `get_active_headers()`, `find_children()`
  - Custom threshold configuration via `with_thresholds()`

- **`SqliteStorage`**: Persistent SQLite-based storage (requires `sqlite` or `mysql` feature). Features:
  - Full persistence across restarts
  - Reorg handling with chain deactivation tracking
  - Efficient indexes on hash, height, merkle root, and active/tip flags
  - Constructors: `new(database_url, chain)`, `in_memory(chain)`, `with_thresholds()`
  - Schema auto-created via `migrate_latest()`
  - Table: `chaintracks_live_headers` with foreign key on `previous_header_id`
  - Pruning support for inactive headers below threshold

### Ingestor Implementations

#### Bulk Ingestors (Historical Data)

| Ingestor | Description |
|----------|-------------|
| `BulkCdnIngestor` | Downloads headers from Babbage CDN (`DEFAULT_CDN_URL`). Fast, preferred method. |
| `BulkWocIngestor` | Uses WhatsOnChain API. Slower but reliable fallback. Supports API key for higher rate limits. |

**Options structs**: `BulkCdnOptions`, `BulkWocOptions` - both have `mainnet()` and `testnet()` constructors.

**CDN Types**:
- `BulkHeaderFileInfo` - Metadata for a single CDN header file (file name, height range, count, hash)
- `BulkHeaderFilesInfo` - CDN file listing response (files array, headers per file, last updated)

**WOC Types**:
- `WocChainInfo` - Chain information from WOC API
- `WocHeaderResponse` - Header response from WOC API
- `WocHeaderByteFileLinks` - Links to header byte files

#### Live Ingestors (Real-time Updates)

| Ingestor | Description |
|----------|-------------|
| `LivePollingIngestor` | Polls WOC `/block/headers` at intervals (default: 60s). Simple and reliable. |
| `LiveWebSocketIngestor` | Connects to WOC WebSocket for instant notifications. Lower latency, requires persistent connection. Auto-reconnects on failure. |

**Options structs**: `LivePollingOptions`, `LiveWebSocketOptions` - configurable poll intervals, timeouts, API keys, and reconnection behavior.

**Polling Types**:
- `WocGetHeadersHeader` - Header response from WOC `/block/headers` endpoint

**WebSocket Types**:
- `WocWsBlockHeader` - Block header from WebSocket message
- `WocWsMessage` - WebSocket message wrapper

#### Helper Functions

- `woc_header_to_block_header(&WocGetHeadersHeader) -> BlockHeader` - Convert WOC polling API response
- `ws_header_to_block_header(&WocWsBlockHeader) -> BlockHeader` - Convert WOC WebSocket message

## Usage

### Basic Setup (Memory Storage)

```rust
use bsv_wallet_toolbox::chaintracks::{
    Chaintracks, ChaintracksOptions, Chain, MemoryStorage
};

// Create storage and options
let storage = Box::new(MemoryStorage::new(Chain::Main));
let options = ChaintracksOptions::default_mainnet();

// Create and initialize
let chaintracks = Chaintracks::new(options, storage);
chaintracks.make_available().await?;

// Query chain tip
let tip = chaintracks.find_chain_tip_header().await?;
println!("Chain tip: {} at height {}", tip.hash, tip.height);
```

### Persistent Setup (SQLite Storage)

Requires `sqlite` or `mysql` feature enabled.

```rust
use bsv_wallet_toolbox::chaintracks::{
    Chaintracks, ChaintracksOptions, Chain, SqliteStorage
};

// File-based SQLite
let storage = SqliteStorage::new("sqlite:chaintracks.db", Chain::Main).await?;
storage.migrate_latest().await?;  // Create tables

// Or in-memory SQLite (for testing)
let storage = SqliteStorage::in_memory(Chain::Test).await?;
storage.migrate_latest().await?;

let options = ChaintracksOptions::default_mainnet();
let chaintracks = Chaintracks::new(options, Box::new(storage));
chaintracks.make_available().await?;
```

### Querying Headers

```rust
// By height
let header = chaintracks.find_header_for_height(100000).await?;

// By hash
let header = chaintracks.find_header_for_block_hash("abc123...").await?;

// Multiple headers as hex (10 headers starting at height 100000)
let headers_hex = chaintracks.get_headers(100000, 10).await?;

// Verify merkle root belongs to a specific height
let is_valid = chaintracks.is_valid_root_for_height(merkle_root, height).await?;
```

### Subscribing to Events

```rust
// Subscribe to new headers and reorgs
let sub_id = chaintracks.subscribe_headers(Box::new(|header| { /* ... */ })).await?;
let reorg_sub = chaintracks.subscribe_reorgs(Box::new(|event| { /* ... */ })).await?;

chaintracks.start_listening().await?;
chaintracks.unsubscribe(&sub_id).await?;
```

### Working with HeightRange

```rust
let range = HeightRange::new(100, 200);
range.count();       // 101
range.contains(150); // true
range.overlaps(&other_range);
range.merge(&adjacent_range);   // Some(merged) if adjacent/overlapping
range.subtract(&other_range);   // Vec of remaining ranges
```

## Block Header Serialization

Headers serialize to exactly 80 bytes:

| Bytes | Field | Encoding |
|-------|-------|----------|
| 0-3 | version | little-endian u32 |
| 4-35 | previous_hash | 32 bytes (reversed hex) |
| 36-67 | merkle_root | 32 bytes (reversed hex) |
| 68-71 | time | little-endian u32 |
| 72-75 | bits | little-endian u32 |
| 76-79 | nonce | little-endian u32 |

Use `BaseBlockHeader::to_bytes()` for serialization. Block hashes are computed as double SHA256 of the 80-byte header, reversed for display (Bitcoin convention).

## Reorg Handling

When a new header arrives that doesn't extend the current tip:

1. `find_common_ancestor()` locates the fork point
2. `find_reorg_depth()` calculates blocks being replaced
3. `InsertHeaderResult` contains:
   - `reorg_depth`: Number of blocks replaced
   - `deactivated_headers`: Headers no longer on active chain
   - `prior_tip`: Previous chain tip before reorg

Subscribers receive `ReorgEvent` notifications with full context.

## Implementation Status

| Component | Status |
|-----------|--------|
| Core types and traits | Complete |
| Chaintracks orchestrator | Complete (partial ingestor integration) |
| MemoryStorage | Complete |
| SqliteStorage | Complete (feature-gated: `sqlite` or `mysql`) |
| BulkCdnIngestor | Complete |
| BulkWocIngestor | Complete |
| LivePollingIngestor | Complete |
| LiveWebSocketIngestor | Complete |
| Full ingestor orchestration | Partial (headers can be added via `add_header()`) |

## Ingestor Usage

### Bulk Ingestors

```rust
// CDN (fast, preferred)
let cdn = BulkCdnIngestor::mainnet()?;
let headers = cdn.fetch_headers(0, HeightRange::new(0, 1000), None, &[]).await?;

// WhatsOnChain (fallback, supports API key)
let woc = BulkWocIngestor::new(BulkWocOptions::mainnet().with_api_key("key"))?;
let height = woc.get_present_height().await?;
```

### Live Ingestors

```rust
// Polling (simple, reliable)
let polling = LivePollingIngestor::new(
    LivePollingOptions::mainnet().with_poll_interval(30)
)?;
let mut headers = vec![];
polling.start_listening(&mut headers).await?;

// WebSocket (low latency)
let ws = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet())?;
ws.start_listening(&mut headers).await?;
ws.stop_listening();
```

## API Constants

| Constant | Value |
|----------|-------|
| `DEFAULT_CDN_URL` | `https://bsv-headers.babbage.systems/` |
| `LEGACY_CDN_URL` | `https://cdn.projectbabbage.com/blockheaders/` |
| `WOC_API_URL_MAIN` | `https://api.whatsonchain.com/v1/bsv/main` |
| `WOC_API_URL_TEST` | `https://api.whatsonchain.com/v1/bsv/test` |
| `WOC_WS_URL_MAIN` | `wss://socket-v2.whatsonchain.com/websocket/blockHeaders` |
| `WOC_WS_URL_TEST` | `wss://socket-v2-testnet.whatsonchain.com/websocket/blockHeaders` |

## Module Re-exports

The `mod.rs` exposes a convenient `ingestor` submodule and top-level re-exports:

```rust
// Via ingestor submodule
use bsv_wallet_toolbox::chaintracks::ingestor::{BulkCdnIngestor, LiveWebSocketIngestor};

// Or directly from chaintracks
use bsv_wallet_toolbox::chaintracks::{BulkCdnIngestor, LiveWebSocketIngestor};
```

## Feature Flags

| Feature | Enables |
|---------|---------|
| `sqlite` | `SqliteStorage` for persistent header storage |
| `mysql` | `SqliteStorage` (same implementation, shared feature gate) |

## SQLite Database Schema

The `SqliteStorage` creates a single table with the following structure:

```sql
CREATE TABLE chaintracks_live_headers (
    header_id INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_header_id INTEGER,          -- FK to parent header
    previous_hash TEXT NOT NULL,
    height INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0, -- 1 if on active chain
    is_chain_tip INTEGER NOT NULL DEFAULT 0, -- 1 if current tip
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

**Indexes**:
- `idx_live_headers_height` on `height`
- `idx_live_headers_active` on `is_active`
- `idx_live_headers_tip` on `is_chain_tip`
- `idx_live_headers_merkle` on `merkle_root` where `is_active = 1`

## Related

- Original TypeScript: `wallet-toolbox/src/services/chaintracker/chaintracks/`
