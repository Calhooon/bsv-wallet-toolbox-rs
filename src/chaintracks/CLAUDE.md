# Chaintracks Module

> Block header tracking system for BSV blockchain with two-tier storage architecture

## Overview

Chaintracks is a Rust port of the TypeScript Chaintracks implementation, providing blockchain header synchronization and validation. It uses a two-tier storage architecture: **bulk storage** for immutable historical headers (height-indexed) and **live storage** for recent mutable headers that track forks and reorgs. The system coordinates bulk ingestors (for historical data from CDN/WhatsOnChain) with live ingestors (real-time WebSocket/polling) to maintain a synchronized view of the blockchain.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module entry point; re-exports all public types, traits, and ingestors |
| `types.rs` | Core data structures: `Chain`, `BlockHeader`, `LiveBlockHeader`, `HeightRange`, etc. |
| `traits.rs` | Trait definitions: `ChaintracksClient`, `ChaintracksStorage`, `BulkIngestor`, `LiveIngestor` |
| `chaintracks.rs` | Main `Chaintracks` orchestrator implementing client and management interfaces |
| `storage/mod.rs` | Storage backend module; exports `MemoryStorage` |
| `storage/memory.rs` | In-memory storage implementation with reorg handling |
| `ingestors/mod.rs` | Ingestor module; re-exports all bulk and live ingestor implementations |
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
│  (CDN, WoC)     │ │ (WS, Polling)   │ │    (Memory)     │
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

### Storage Implementations

- **`MemoryStorage`**: In-memory storage suitable for testing/development/mobile. Data lost on restart. Features reorg handling, hash/height/merkle root indexing, and batch insert support.

### Ingestor Implementations

#### Bulk Ingestors (Historical Data)

| Ingestor | Description |
|----------|-------------|
| `BulkCdnIngestor` | Downloads headers from Babbage CDN (`DEFAULT_CDN_URL`). Fast, preferred method. |
| `BulkWocIngestor` | Uses WhatsOnChain API. Slower but reliable fallback. Supports API key for higher rate limits. |

**Options structs**: `BulkCdnOptions`, `BulkWocOptions` - both have `mainnet()` and `testnet()` constructors.

#### Live Ingestors (Real-time Updates)

| Ingestor | Description |
|----------|-------------|
| `LivePollingIngestor` | Polls WOC `/block/headers` at intervals (default: 60s). Simple and reliable. |
| `LiveWebSocketIngestor` | Connects to WOC WebSocket for instant notifications. Lower latency, requires persistent connection. Auto-reconnects on failure. |

**Options structs**: `LivePollingOptions`, `LiveWebSocketOptions` - configurable poll intervals, timeouts, API keys, and reconnection behavior.

#### Helper Functions

- `woc_header_to_block_header()` - Convert WOC polling API response to `BlockHeader`
- `ws_header_to_block_header()` - Convert WOC WebSocket message to `BlockHeader`
- `calculate_work()` - Calculate chain work from difficulty bits

## Usage

### Basic Setup

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

### Querying Headers

```rust
// By height
if let Some(header) = chaintracks.find_header_for_height(100000).await? {
    println!("Block at 100000: {}", header.hash);
}

// By hash
if let Some(header) = chaintracks.find_header_for_block_hash("abc123...").await? {
    println!("Found block at height {}", header.height);
}

// Multiple headers as hex
let headers_hex = chaintracks.get_headers(100000, 10).await?;  // 10 headers starting at 100000
```

### Merkle Root Validation

```rust
// Verify a merkle root belongs to a specific height
let is_valid = chaintracks.is_valid_root_for_height(
    "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    0  // genesis block
).await?;
```

### Subscribing to Events

```rust
// Subscribe to new headers
let sub_id = chaintracks.subscribe_headers(Box::new(|header| {
    println!("New header: {} at height {}", header.hash, header.height);
})).await?;

// Subscribe to reorgs
let reorg_sub = chaintracks.subscribe_reorgs(Box::new(|event| {
    println!("Reorg of depth {}: {} -> {}",
        event.depth, event.old_tip.hash, event.new_tip.hash);
})).await?;

// Start listening for new headers
chaintracks.start_listening().await?;

// Later: unsubscribe
chaintracks.unsubscribe(&sub_id).await?;
```

### Working with HeightRange

```rust
use bsv_wallet_toolbox::chaintracks::HeightRange;

let range = HeightRange::new(100, 200);
assert_eq!(range.count(), 101);
assert!(range.contains(150));

// Merge adjacent ranges
let r1 = HeightRange::new(100, 150);
let r2 = HeightRange::new(151, 200);
let merged = r1.merge(&r2);  // Some(HeightRange { low: 100, high: 200 })

// Subtract ranges
let r1 = HeightRange::new(100, 200);
let r2 = HeightRange::new(130, 170);
let remaining = r1.subtract(&r2);  // [HeightRange(100,129), HeightRange(171,200)]
```

### Custom Storage Implementation

```rust
use async_trait::async_trait;
use bsv_wallet_toolbox::chaintracks::{
    ChaintracksStorage, ChaintracksStorageQuery, ChaintracksStorageIngest,
    Chain, LiveBlockHeader, BlockHeader, HeightRange, InsertHeaderResult
};

struct MyStorage { /* ... */ }

#[async_trait]
impl ChaintracksStorageQuery for MyStorage {
    fn chain(&self) -> Chain { Chain::Main }
    fn live_height_threshold(&self) -> u32 { 2000 }
    fn reorg_height_threshold(&self) -> u32 { 400 }
    // ... implement query methods
}

#[async_trait]
impl ChaintracksStorageIngest for MyStorage {
    // ... implement ingest methods
}

#[async_trait]
impl ChaintracksStorage for MyStorage {
    fn storage_type(&self) -> &str { "custom" }
    async fn is_available(&self) -> bool { true }
}
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
| Chaintracks orchestrator | Complete |
| MemoryStorage | Complete |
| BulkCdnIngestor | Complete |
| BulkWocIngestor | Complete |
| LivePollingIngestor | Complete |
| LiveWebSocketIngestor | Complete |
| SQLite storage | Planned |

## Ingestor Usage

### CDN Bulk Ingestor

```rust
use bsv_wallet_toolbox::chaintracks::{BulkCdnIngestor, BulkCdnOptions};

// Create for mainnet (default CDN URL)
let ingestor = BulkCdnIngestor::mainnet()?;

// Or with custom options
let options = BulkCdnOptions {
    cdn_url: "https://custom-cdn.example.com/".to_string(),
    timeout_secs: 120,
    ..BulkCdnOptions::mainnet()
};
let ingestor = BulkCdnIngestor::new(options)?;

// Fetch headers for a range
let headers = ingestor.fetch_headers(0, HeightRange::new(0, 1000), None, &[]).await?;
```

### WhatsOnChain Bulk Ingestor

```rust
use bsv_wallet_toolbox::chaintracks::{BulkWocIngestor, BulkWocOptions};

// With API key for higher rate limits
let ingestor = BulkWocIngestor::new(
    BulkWocOptions::mainnet().with_api_key("your-api-key")
)?;

// Get current chain height
let height = ingestor.get_chain_tip_height().await?;
```

### Live Polling Ingestor

```rust
use bsv_wallet_toolbox::chaintracks::{LivePollingIngestor, LivePollingOptions};

let ingestor = LivePollingIngestor::new(
    LivePollingOptions::mainnet().with_poll_interval(30)  // Poll every 30 seconds
)?;

// Subscribe to new headers
let mut receiver = ingestor.subscribe();

// Start listening
let mut headers = vec![];
ingestor.start_listening(&mut headers).await?;

// Receive headers asynchronously
while let Ok(header) = receiver.recv().await {
    println!("New block: {}", header.height);
}
```

### Live WebSocket Ingestor

```rust
use bsv_wallet_toolbox::chaintracks::{LiveWebSocketIngestor, LiveWebSocketOptions};

let ingestor = LiveWebSocketIngestor::new(
    LiveWebSocketOptions::mainnet()
        .with_idle_timeout(60_000)
        .with_api_key("your-api-key")
)?;

// Subscribe to headers
let mut receiver = ingestor.subscribe();

// Start listening (spawns WebSocket connection)
let mut headers = vec![];
ingestor.start_listening(&mut headers).await?;

// Handle incoming headers
tokio::spawn(async move {
    while let Ok(header) = receiver.recv().await {
        println!("WebSocket: new block {} at height {}",
            &header.hash[..16], header.height);
    }
});

// Later: stop
ingestor.stop_listening();
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

## Related

- `storage/CLAUDE.md` - Storage implementation details
- `ingestors/CLAUDE.md` - Ingestor implementation details
- Original TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/`
