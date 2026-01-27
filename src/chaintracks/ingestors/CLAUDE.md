# Ingestors Module

> Block header fetching implementations for historical and real-time data sources

## Overview

This module provides four ingestor implementations for fetching BSV blockchain headers from external sources. **Bulk ingestors** download historical headers in batch from CDN or API endpoints, while **live ingestors** monitor for new blocks in real-time via WebSocket or polling. The Chaintracks orchestrator coordinates these ingestors to build and maintain a synchronized header chain.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module entry point; re-exports all ingestors, options, and utility functions |
| `bulk_cdn.rs` | CDN-based bulk ingestor using Babbage Systems header files |
| `bulk_woc.rs` | WhatsOnChain API-based bulk ingestor (fallback) |
| `live_polling.rs` | Polling-based live ingestor using WOC REST API |
| `live_websocket.rs` | WebSocket-based live ingestor using WOC streaming API |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      BulkIngestor Trait                         │
│  get_present_height() | synchronize() | fetch_headers()         │
└───────────────────────┬─────────────────────┬───────────────────┘
                        │                     │
            ┌───────────▼──────────┐ ┌────────▼──────────┐
            │   BulkCdnIngestor    │ │  BulkWocIngestor  │
            │ (Babbage CDN files)  │ │  (WOC API/files)  │
            └──────────────────────┘ └───────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      LiveIngestor Trait                         │
│  start_listening() | stop_listening() | get_header_by_hash()    │
└───────────────────────┬─────────────────────┬───────────────────┘
                        │                     │
            ┌───────────▼──────────┐ ┌────────▼──────────┐
            │ LivePollingIngestor  │ │LiveWebSocketIngest│
            │  (REST API polling)  │ │  (WS streaming)   │
            └──────────────────────┘ └───────────────────┘
```

## Key Exports

### Bulk Ingestors

| Type | Description |
|------|-------------|
| `BulkCdnIngestor` | Downloads binary header files from Babbage CDN; fast and preferred for historical sync |
| `BulkCdnOptions` | Configuration: `chain`, `cdn_url`, `json_resource`, `timeout_secs`, `user_agent` |
| `BulkWocIngestor` | Uses WhatsOnChain API for headers; slower but reliable fallback with chain tip info |
| `BulkWocOptions` | Configuration: `chain`, `api_key`, `timeout_secs`, `enable_cache`, `chain_info_ttl_ms` |

### Live Ingestors

| Type | Description |
|------|-------------|
| `LivePollingIngestor` | Polls WOC `/block/headers` at intervals; simple, battery-friendly |
| `LivePollingOptions` | Configuration: `chain`, `api_key`, `poll_interval_secs`, `idle_wait_ms` |
| `LiveWebSocketIngestor` | Connects to WOC WebSocket for instant notifications; low latency |
| `LiveWebSocketOptions` | Configuration: `chain`, `api_key`, `idle_timeout_ms`, `ping_interval_ms`, `max_reconnect_attempts` |

### Response Types

| Type | Description |
|------|-------------|
| `BulkHeaderFileInfo` | CDN file metadata: `file_name`, `from_height`, `to_height`, `count`, `chain` |
| `BulkHeaderFilesInfo` | CDN listing response: `files`, `headers_per_file`, `last_updated` |
| `WocChainInfo` | Chain status: `chain`, `blocks`, `headers`, `best_block_hash` |
| `WocHeaderResponse` | Full header from WOC REST API |
| `WocGetHeadersHeader` | Header from `/block/headers` endpoint |
| `WocWsBlockHeader` | Header from WebSocket stream (numeric `bits` field) |
| `WocWsMessage` | WebSocket message envelope (untagged enum) |

### Utility Functions

| Function | Description |
|----------|-------------|
| `woc_header_to_block_header()` | Convert `WocGetHeadersHeader` to `BlockHeader` |
| `ws_header_to_block_header()` | Convert `WocWsBlockHeader` to `BlockHeader` |

### Constants

| Constant | Value |
|----------|-------|
| `DEFAULT_CDN_URL` | `https://bsv-headers.babbage.systems/` |
| `LEGACY_CDN_URL` | `https://cdn.projectbabbage.com/blockheaders/` |
| `WOC_API_URL_MAIN` | `https://api.whatsonchain.com/v1/bsv/main` |
| `WOC_API_URL_TEST` | `https://api.whatsonchain.com/v1/bsv/test` |
| `WOC_WS_URL_MAIN` | `wss://socket-v2.whatsonchain.com/websocket/blockHeaders` |
| `WOC_WS_URL_TEST` | `wss://socket-v2-testnet.whatsonchain.com/websocket/blockHeaders` |

## Usage

### Quick Start with Default Ingestors

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{
    BulkCdnIngestor, BulkWocIngestor,
    LivePollingIngestor, LiveWebSocketIngestor,
};

// Bulk ingestors for historical sync
let cdn_ingestor = BulkCdnIngestor::mainnet()?;      // Fast CDN download
let woc_ingestor = BulkWocIngestor::mainnet()?;      // API fallback

// Live ingestors for real-time updates
let polling = LivePollingIngestor::mainnet()?;       // Simple polling
let websocket = LiveWebSocketIngestor::mainnet()?;   // Low latency
```

### CDN Bulk Sync

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{BulkCdnIngestor, BulkCdnOptions};
use bsv_wallet_toolbox::chaintracks::{Chain, HeightRange};

// Custom options
let options = BulkCdnOptions {
    chain: Chain::Main,
    cdn_url: "https://bsv-headers.babbage.systems/".to_string(),
    json_resource: "mainNetBlockHeaders.json".to_string(),
    timeout_secs: 120,
    ..Default::default()
};

let ingestor = BulkCdnIngestor::new(options)?;

// Fetch headers in a range
let headers = ingestor.fetch_headers(
    0,                              // before: current sync point
    HeightRange::new(0, 99999),     // fetch_range
    None,                           // bulk_range (optional)
    &[],                            // prior_live_headers
).await?;

println!("Fetched {} headers", headers.len());
```

### WhatsOnChain Bulk Sync with API Key

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{BulkWocIngestor, BulkWocOptions};

let options = BulkWocOptions::mainnet()
    .with_api_key("your-woc-api-key");

let ingestor = BulkWocIngestor::new(options)?;

// Get current chain tip
let height = ingestor.get_chain_tip_height().await?;
println!("Chain tip at height {}", height);

// Fetch specific header by hash
if let Some(header) = ingestor.get_header_by_hash("000000000019d6689c...").await? {
    println!("Genesis block: height={}", header.height);
}
```

### Polling for New Blocks

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{LivePollingIngestor, LivePollingOptions};

let options = LivePollingOptions::mainnet()
    .with_poll_interval(30)  // Check every 30 seconds
    .with_api_key("your-woc-api-key");

let ingestor = LivePollingIngestor::new(options)?;

// Subscribe to new headers
let mut receiver = ingestor.subscribe();

// Start polling
let mut live_headers = vec![];
ingestor.start_listening(&mut live_headers).await?;

// Receive notifications in another task
tokio::spawn(async move {
    while let Ok(header) = receiver.recv().await {
        println!("New block: height={}, hash={}", header.height, &header.hash[..16]);
    }
});

// Later: stop
ingestor.stop_listening();
```

### WebSocket Real-Time Updates

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{LiveWebSocketIngestor, LiveWebSocketOptions};

let options = LiveWebSocketOptions::mainnet()
    .with_idle_timeout(120_000)  // 2 minute idle timeout
    .with_api_key("your-woc-api-key");

let ingestor = LiveWebSocketIngestor::new(options)?;

// Subscribe before starting
let mut receiver = ingestor.subscribe();

// Start WebSocket connection with auto-reconnect
let mut live_headers = vec![];
ingestor.start_listening(&mut live_headers).await?;

// Headers arrive instantly via WebSocket
while let Ok(header) = receiver.recv().await {
    println!("Block via WebSocket: height={}", header.height);
}
```

### Header Format Conversion

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{
    WocGetHeadersHeader, woc_header_to_block_header,
    WocWsBlockHeader, ws_header_to_block_header,
};

// From REST API (bits as hex string)
let woc_header = WocGetHeadersHeader {
    hash: "000000000019d6689c...".to_string(),
    height: 0,
    bits: "1d00ffff".to_string(),  // Hex string
    // ... other fields
};
let block_header = woc_header_to_block_header(&woc_header);

// From WebSocket (bits as u32)
let ws_header = WocWsBlockHeader {
    hash: "000000000019d6689c...".to_string(),
    height: 0,
    bits: 486604799,  // Numeric
    // ... other fields
};
let block_header = ws_header_to_block_header(&ws_header);
```

## Implementation Details

### Binary Header Format

Headers are stored as 80-byte binary blobs in CDN files:

| Bytes | Field | Encoding |
|-------|-------|----------|
| 0-3 | version | little-endian u32 |
| 4-35 | previous_hash | 32 bytes raw |
| 36-67 | merkle_root | 32 bytes raw |
| 68-71 | time | little-endian u32 |
| 72-75 | bits | little-endian u32 |
| 76-79 | nonce | little-endian u32 |

Both `BulkCdnIngestor` and `BulkWocIngestor` include `deserialize_header()` and `compute_block_hash()` methods that parse this format and compute double-SHA256 hashes.

### CDN File Naming

CDN files follow the pattern: `{from_height}_{to_height}_headers.bin`

Example: `0_99999_headers.bin` contains headers for heights 0-99999 (100,000 headers, 8MB).

The JSON index file (e.g., `mainNetBlockHeaders.json`) lists available files with their height ranges.

### WebSocket Protocol

The WOC WebSocket uses a custom protocol:

1. Connect to `wss://socket-v2.whatsonchain.com/websocket/blockHeaders`
2. Send `{}` to initiate subscription
3. Receive messages with type codes:
   - Type 5/6: Subscription confirmations
   - Type 7: Data delivery or errors
4. Block headers arrive in `pub.data` or `data.data` fields
5. Send periodic pings to maintain connection

### Error Handling and Reconnection

Live ingestors handle transient failures gracefully:

- **Polling**: Continues on fetch errors, logs warnings
- **WebSocket**: Auto-reconnects with configurable attempts and delays
  - `max_reconnect_attempts`: Default 10
  - `reconnect_delay_ms`: Default 5000ms between attempts
  - `idle_timeout_ms`: Reconnect if no messages for 100s

### Choosing an Ingestor

| Use Case | Recommended Ingestor |
|----------|---------------------|
| Initial historical sync | `BulkCdnIngestor` (fast, parallel downloads) |
| CDN unavailable | `BulkWocIngestor` (reliable fallback) |
| Mobile/battery-conscious | `LivePollingIngestor` (configurable interval) |
| Low-latency trading | `LiveWebSocketIngestor` (instant notifications) |
| Development/testing | `LivePollingIngestor` (simpler to debug) |

## Related

- [`../CLAUDE.md`](../CLAUDE.md) - Parent Chaintracks module documentation
- [`../traits.rs`](../traits.rs) - `BulkIngestor` and `LiveIngestor` trait definitions
- [`../types.rs`](../types.rs) - `BlockHeader`, `LiveBlockHeader`, `HeightRange` types
