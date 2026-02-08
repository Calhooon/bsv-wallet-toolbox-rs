# Chaintracks Module

> Block header tracking system for BSV blockchain with two-tier storage architecture

## Overview

Chaintracks is a Rust port of the TypeScript Chaintracks implementation, providing blockchain header synchronization and validation. It uses a two-tier storage architecture: **bulk storage** for immutable historical headers (height-indexed) and **live storage** for recent mutable headers that track forks and reorgs. The system coordinates bulk ingestors (for historical data from CDN/WhatsOnChain) with live ingestors (real-time WebSocket/polling) to maintain a synchronized view of the blockchain.

## Files

| File | Lines | Tests | Purpose |
|------|-------|-------|---------|
| `mod.rs` | 57 | 0 | Module entry point; re-exports all public types, traits, ingestors, and `ChainTracker` from bsv-sdk |
| `types.rs` | 393 | 4 | Core data structures: `Chain`, `BaseBlockHeader`, `BlockHeader`, `LiveBlockHeader`, `InsertHeaderResult`, `HeightRange`, `ChaintracksInfo`, `calculate_work()` |
| `traits.rs` | 312 | 0 | Trait definitions: `ChaintracksClient`, `ChaintracksManagement`, `ChaintracksStorage`, `BulkIngestor`, `LiveIngestor`, `ChaintracksOptions`, `ReorgEvent`, `BulkSyncResult`, callback types |
| `chaintracks.rs` | 935 | 11 | Main `Chaintracks` orchestrator implementing `ChaintracksClient` + `ChaintracksManagement` traits, background sync task, header processing pipeline |
| `storage/mod.rs` | 12 | 0 | Storage backend module; exports `MemoryStorage` and `SqliteStorage` (feature-gated) |
| `storage/memory.rs` | 1196 | 30 | In-memory storage implementation with reorg handling, fork tracking, and batch operations |
| `storage/sqlite.rs` | 1918 | 35 | SQLite-based persistent storage with batch insert, reorg handling, and bulk operations (requires `sqlite` or `mysql` feature) |
| `ingestors/mod.rs` | 191 | 16 | Ingestor module; re-exports all bulk and live ingestor implementations with helper types and integration tests |
| `ingestors/bulk_cdn.rs` | 691 | 14 | CDN-based bulk header downloads from Babbage Systems |
| `ingestors/bulk_woc.rs` | 782 | 14 | WhatsOnChain API bulk header fetching (fallback) |
| `ingestors/live_polling.rs` | 590 | 12 | Polling-based live header detection via WOC API |
| `ingestors/live_websocket.rs` | 846 | 14 | WebSocket-based real-time header streaming via WOC |

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

- **`Chain`**: Network identifier (`Main`, `Test`). Default: `Main`. Serializes to `"main"`/`"test"`.

### Structs - Block Headers

| Struct | Description |
|--------|-------------|
| `BaseBlockHeader` | Raw header without height/hash (80 bytes when serialized). Methods: `to_bytes()`, `to_block_header_at_height()` |
| `BlockHeader` | Header with computed hash and height. Implements `From<BaseBlockHeader>`, `From<LiveBlockHeader>` |
| `LiveBlockHeader` | Extended header with chain tracking: `chain_work`, `is_chain_tip`, `is_active`, `header_id`, `previous_header_id` |

### Structs - Results and Ranges

| Struct | Description |
|--------|-------------|
| `HeightRange` | Inclusive height range with utility methods (`count`, `contains`, `overlaps`, `merge`, `subtract`) |
| `InsertHeaderResult` | Insert operation result with reorg detection flags (`added`, `dupe`, `is_active_tip`, `reorg_depth`, `prior_tip`, `deactivated_headers`, `no_prev`, `bad_prev`, `no_active_ancestor`, `no_tip`) |
| `ChaintracksInfo` | System status information (chain, storage type, ingestor counts, tip height, live range, sync state) |
| `ReorgEvent` | Reorg notification with `depth`, `old_tip`, `new_tip`, `deactivated_headers` |
| `BulkSyncResult` | Bulk synchronization result with `live_headers` and `done` flag |

### Configuration

- **`ChaintracksOptions`**: Configuration struct with fields:
  - `chain`: `Chain` (Main/Test)
  - `live_height_threshold`: Headers kept in live storage (default: 2000)
  - `reorg_height_threshold`: Max reorg depth to track (default: 400)
  - `add_live_recursion_limit`: Max recursive lookups (default: 36)
  - `batch_insert_limit`: Batch size for inserts (default: 400)
  - `bulk_migration_chunk_size`: Migration chunk size (default: 500)
  - `require_ingestors`: If true, validate ingestor config on `start_background_sync()` (default: false)
  - `readonly`: If true, block all write operations (`add_header`, `start_listening`, `destroy`) (default: false)
  - Constructors: `default_mainnet()`, `default_testnet()`

### Traits

| Trait | Extends | Purpose |
|-------|---------|---------|
| `ChaintracksClient` | — | Client API: query headers, add headers, subscribe to events (17 methods) |
| `ChaintracksManagement` | `ChaintracksClient` | Management API: `destroy`, `validate`, `export_bulk_headers` |
| `ChaintracksStorageQuery` | — | Storage read operations (13 methods) |
| `ChaintracksStorageIngest` | `ChaintracksStorageQuery` | Storage write operations (7 methods) |
| `ChaintracksStorage` | `ChaintracksStorageIngest` | Full storage provider: `storage_type()`, `is_available()` |
| `BulkIngestor` | — | Historical header fetching: `get_present_height`, `synchronize`, `fetch_headers`, `set_storage`, `shutdown` |
| `LiveIngestor` | — | Real-time header streaming: `get_header_by_hash`, `start_listening`, `stop_listening`, `set_storage`, `shutdown` |

### Callback Types

- **`HeaderCallback`**: `Box<dyn Fn(BlockHeader) + Send + Sync>` - New header notification
- **`ReorgCallback`**: `Box<dyn Fn(ReorgEvent) + Send + Sync>` - Reorg notification

### Chaintracks Struct Methods

| Method | Description |
|--------|-------------|
| `new(options, storage)` | Create new instance with options and storage backend |
| `make_available()` | Initialize storage and prepare for use |
| `set_bulk_ingestor_count(count)` | Set number of bulk ingestors (for status reporting) |
| `set_live_ingestor_count(count)` | Set number of live ingestors (for status reporting) |
| `process_pending_headers()` | Drain base header queue, compute hashes/heights, insert into storage, fire reorg callbacks |
| `start_background_sync()` | Start background sync; validates ingestor config if `require_ingestors` is set, sets listening flag. Idempotent (no-op if already running) |
| `stop_background_sync()` | Stop background sync and clear listening flag. Aborts background task. Idempotent |
| `is_background_syncing()` | Check if background sync is currently running (non-async, uses `AtomicBool`) |
| `validate_ingestor_config()` | Check ingestor counts and log warnings if none configured (does not error) |

The struct also implements `ChaintracksClient` and `ChaintracksManagement` traits.

#### Readonly Mode

When `ChaintracksOptions::readonly` is set to `true`, write operations return `Error::InvalidOperation`:
- `add_header()` - blocked
- `start_listening()` - blocked
- `destroy()` - blocked

Read-only queries (`find_header_for_height`, `get_info`, `current_height`, etc.) still work normally.

### Re-exported Types

- **`ChainTracker`**: Re-exported from `bsv_sdk::transaction::ChainTracker` for convenience

### Utility Functions

- **`calculate_work(bits: u32) -> String`**: Calculate chain work from difficulty bits (compact target format). Returns 64-character hex string representing relative work value. Uses u128 proxy for 2^256 division with overflow protection.

### Storage Implementations

#### MemoryStorage

In-memory storage suitable for testing/development/mobile. Data lost on restart.

| Method | Description |
|--------|-------------|
| `new(chain)` | Create with default thresholds (2000 live, 400 reorg) |
| `with_thresholds(chain, live, reorg)` | Create with custom thresholds |
| `header_count()` | Get total number of stored headers |
| `get_headers_at_height(height)` | Get all headers at a height (including forks) |
| `get_active_headers()` | Get all active chain headers |
| `get_fork_headers()` | Get all inactive/fork headers |
| `find_children(parent_hash)` | Find headers building on a given hash |
| `insert_headers_batch(headers)` | Insert multiple headers in order |

Internal indexes: hash→id, height→id, merkle→id via `HashMap`. Reorg handling via `handle_reorg()` which walks from both tips to common ancestor, deactivating old chain and activating new.

#### SqliteStorage

Persistent SQLite-based storage (requires `sqlite` or `mysql` feature).

| Method | Description |
|--------|-------------|
| `new(database_url, chain)` | Create with SQLite connection URL |
| `in_memory(chain)` | Create in-memory database (for testing) |
| `with_thresholds(url, chain, live, reorg)` | Create with custom thresholds |
| `pool()` | Get underlying `sqlx::Pool<Sqlite>` |
| `header_count()` | Get total header count (async) |
| `live_header_exists(hash)` | Optimized existence check by hash |
| `find_headers_for_height_less_than_or_equal_sorted(height, limit)` | Bulk query with limit |
| `delete_live_headers_by_ids(ids)` | Delete by IDs, handles FK constraints |
| `set_chain_tip_by_id(header_id, is_chain_tip)` | Update chain tip flag |
| `set_active_by_id(header_id, is_active)` | Update active flag |
| `insert_headers_batch(headers)` | Transactional batch insert with duplicate detection (optimized for 10k+ headers) |
| `update_chain_tip_to_highest()` | Set chain tip to highest active header (call after batch insert) |
| `get_headers_by_height_range(start, end)` | Active headers in height range |
| `get_headers_at_height(height)` | All headers at height (including forks) |
| `get_active_headers()` | All active chain headers |
| `get_fork_headers()` | All inactive/fork headers |
| `find_children(parent_hash)` | Headers building on a given hash |
| `mark_headers_inactive_above_height(height)` | Bulk deactivation for reorg handling |

Schema auto-created via `migrate_latest()`. FK constraint handling on deletes clears `previous_header_id` references before deletion.

### Ingestor Implementations

#### Bulk Ingestors (Historical Data)

| Ingestor | Description |
|----------|-------------|
| `BulkCdnIngestor` | Downloads headers from Babbage CDN (`DEFAULT_CDN_URL`). Fast, preferred method. Caches file listing. |
| `BulkWocIngestor` | Uses WhatsOnChain API. Slower but reliable fallback. Supports API key for higher rate limits. Caches chain info with configurable TTL. |

**Options structs**: `BulkCdnOptions`, `BulkWocOptions` - both have `mainnet()` and `testnet()` constructors.

**CDN Types**:
- `BulkHeaderFileInfo` - Metadata for a single CDN header file (file name, height range, count, hash, chain, source URL)
- `BulkHeaderFilesInfo` - CDN file listing response (files array, headers per file, last updated)

**WOC Types**:
- `WocChainInfo` - Chain information from WOC API (chain, blocks, headers, best block hash, difficulty)
- `WocHeaderResponse` - Header response from WOC API (full block header with confirmations, size, chain work)
- `WocHeaderByteFileLinks` - Links to header byte files

#### Live Ingestors (Real-time Updates)

| Ingestor | Description |
|----------|-------------|
| `LivePollingIngestor` | Polls WOC `/block/headers` at intervals (default: 60s). Simple and reliable. Broadcasts via `tokio::sync::broadcast`. |
| `LiveWebSocketIngestor` | Connects to WOC WebSocket for instant notifications. Lower latency, requires persistent connection. Auto-reconnects with configurable max attempts and delay. |

**Options structs**: `LivePollingOptions`, `LiveWebSocketOptions` - configurable poll intervals, timeouts, API keys, and reconnection behavior. Builder methods: `with_poll_interval()`, `with_api_key()`, `with_idle_timeout()`.

**Polling Types**:
- `WocGetHeadersHeader` - Header response from WOC `/block/headers` endpoint

**WebSocket Types**:
- `WocWsBlockHeader` - Block header from WebSocket message (bits as `u32`, not hex string)
- `WocWsMessage` - WebSocket message wrapper (untagged enum: `HeaderData`, `TypedMessage`, `Connect`, `Empty`)
- `WocPubData` - Published header data wrapper
- `WsError` - WebSocket-specific error types (`ConnectionFailed`, `MessageParseFailed`, `IdleTimeout`, `Stopped`)

**Both live ingestors provide**:
- `subscribe() -> broadcast::Receiver<LiveBlockHeader>` - Subscribe to new header notifications
- `is_running() -> bool` - Check running state

#### Helper Functions

- `woc_header_to_block_header(&WocGetHeadersHeader) -> BlockHeader` - Convert WOC polling API response (parses bits from hex string)
- `ws_header_to_block_header(&WocWsBlockHeader) -> BlockHeader` - Convert WOC WebSocket message (bits already u32)

## Usage

### Basic Setup (Memory Storage)

```rust
use bsv_wallet_toolbox::chaintracks::{
    Chaintracks, ChaintracksOptions, Chain, MemoryStorage
};

let storage = Box::new(MemoryStorage::new(Chain::Main));
let options = ChaintracksOptions::default_mainnet();
let chaintracks = Chaintracks::new(options, storage);
chaintracks.make_available().await?;

let tip = chaintracks.find_chain_tip_header().await?;
```

### Persistent Setup (SQLite Storage)

Requires `sqlite` or `mysql` feature enabled.

```rust
use bsv_wallet_toolbox::chaintracks::{
    Chaintracks, ChaintracksOptions, Chain, SqliteStorage
};

let storage = SqliteStorage::new("sqlite:chaintracks.db", Chain::Main).await?;
storage.migrate_latest().await?;  // Create tables

let options = ChaintracksOptions::default_mainnet();
let chaintracks = Chaintracks::new(options, Box::new(storage));
chaintracks.make_available().await?;
```

### Background Sync Lifecycle

```rust
chaintracks.start_background_sync().await?;
assert!(chaintracks.is_background_syncing());

// ... headers processed automatically every 500ms ...

chaintracks.stop_background_sync().await?;
```

### Subscribing to Events

```rust
let sub_id = chaintracks.subscribe_headers(Box::new(|header| { /* ... */ })).await?;
let reorg_sub = chaintracks.subscribe_reorgs(Box::new(|event| { /* ... */ })).await?;

chaintracks.start_listening().await?;
chaintracks.unsubscribe(&sub_id).await?;
```

### Ingestor Usage

```rust
// CDN bulk (fast, preferred)
let cdn = BulkCdnIngestor::mainnet()?;
let headers = cdn.fetch_headers(0, HeightRange::new(0, 1000), None, &[]).await?;

// WOC bulk (fallback, supports API key)
let woc = BulkWocIngestor::new(BulkWocOptions::mainnet().with_api_key("key"))?;

// Polling live (simple, reliable)
let polling = LivePollingIngestor::new(LivePollingOptions::mainnet().with_poll_interval(30))?;
let mut headers = vec![];
polling.start_listening(&mut headers).await?;

// WebSocket live (low latency)
let ws = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet())?;
ws.start_listening(&mut headers).await?;
ws.stop_listening();
```

## Block Header Serialization

Headers serialize to exactly 80 bytes:

| Bytes | Field | Encoding |
|-------|-------|----------|
| 0-3 | version | little-endian u32 |
| 4-35 | previous_hash | 32 bytes hex |
| 36-67 | merkle_root | 32 bytes hex |
| 68-71 | time | little-endian u32 |
| 72-75 | bits | little-endian u32 |
| 76-79 | nonce | little-endian u32 |

Use `BaseBlockHeader::to_bytes()` for serialization. Block hashes are computed as double SHA256 of the 80-byte header, reversed for display (Bitcoin convention).

## Reorg Handling

When a new header arrives that doesn't extend the current tip:

1. `find_common_ancestor()` locates the fork point by walking back from both headers
2. Old chain headers are marked `is_active = false`, new chain marked `is_active = true`
3. `InsertHeaderResult` contains:
   - `reorg_depth`: Number of blocks replaced
   - `deactivated_headers`: Headers no longer on active chain
   - `prior_tip`: Previous chain tip before reorg
4. Subscribers receive `ReorgEvent` notifications with full context

Both `MemoryStorage` and `SqliteStorage` implement reorg handling internally.

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

```rust
// Via ingestor submodule
use bsv_wallet_toolbox::chaintracks::ingestor::{BulkCdnIngestor, LiveWebSocketIngestor};

// Or directly from chaintracks
use bsv_wallet_toolbox::chaintracks::{BulkCdnIngestor, LiveWebSocketIngestor};

// ChainTracker from bsv-sdk
use bsv_wallet_toolbox::chaintracks::ChainTracker;
```

## Feature Flags

| Feature | Enables |
|---------|---------|
| `sqlite` | `SqliteStorage` for persistent header storage |
| `mysql` | `SqliteStorage` (same implementation, shared feature gate) |

## SQLite Database Schema

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
- `idx_live_headers_merkle` on `merkle_root` where `is_active = 1` (partial index)

## Implementation Status

| Component | Status |
|-----------|--------|
| Core types and traits | Complete |
| Chaintracks orchestrator | Complete (partial ingestor integration) |
| MemoryStorage | Complete (30 tests) |
| SqliteStorage | Complete (35 tests, feature-gated: `sqlite` or `mysql`) |
| BulkCdnIngestor | Complete (14 tests) |
| BulkWocIngestor | Complete (14 tests) |
| LivePollingIngestor | Complete (12 tests) |
| LiveWebSocketIngestor | Complete (14 tests) |
| Background sync lifecycle | Complete (`start/stop_background_sync()`, `process_pending_headers()`) |
| Header processing pipeline | Complete (queue → hash computation → parent lookup → storage insert → reorg detection → subscriber notification) |
| Readonly mode | Complete (blocks writes when `readonly: true`) |
| Full ingestor orchestration | Partial (headers added via `add_header()`, ingestor counts tracked; actual ingestor spawning deferred) |

## Related

- Original TypeScript: `wallet-toolbox/src/services/chaintracker/chaintracks/`
- Sub-module docs: `storage/CLAUDE.md`, `ingestors/CLAUDE.md`
