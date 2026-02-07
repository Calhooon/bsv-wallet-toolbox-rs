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
┌─────────────────────────────────────────────────────────────────────┐
│                       BulkIngestor Trait                            │
│  get_present_height() | synchronize() | fetch_headers()             │
│  set_storage() | shutdown()                                         │
└───────────────────────┬─────────────────────────┬───────────────────┘
                        │                         │
            ┌───────────▼──────────┐   ┌──────────▼──────────┐
            │   BulkCdnIngestor    │   │  BulkWocIngestor    │
            │ (Babbage CDN files)  │   │  (WOC API/files)    │
            └──────────────────────┘   └─────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       LiveIngestor Trait                            │
│  get_header_by_hash() | start_listening() | stop_listening()        │
│  set_storage() | shutdown()                                         │
└───────────────────────┬─────────────────────────┬───────────────────┘
                        │                         │
            ┌───────────▼──────────┐   ┌──────────▼──────────┐
            │ LivePollingIngestor  │   │LiveWebSocketIngestor│
            │  (REST API polling)  │   │  (WS streaming)     │
            └──────────────────────┘   └─────────────────────┘
```

## Key Exports

### Bulk Ingestors

| Type | Description |
|------|-------------|
| `BulkCdnIngestor` | Downloads binary header files from Babbage CDN; fast and preferred for historical sync |
| `BulkCdnOptions` | Configuration: `chain`, `cdn_url`, `json_resource`, `max_per_file`, `timeout_secs`, `user_agent` |
| `BulkWocIngestor` | Uses WhatsOnChain API for headers; slower but reliable fallback with chain tip info |
| `BulkWocOptions` | Configuration: `chain`, `api_key`, `timeout_secs`, `enable_cache`, `chain_info_ttl_ms`, `idle_wait_ms` |

### Live Ingestors

| Type | Description |
|------|-------------|
| `LivePollingIngestor` | Polls WOC `/block/headers` at intervals; simple, battery-friendly. Has `subscribe()` and `is_running()` methods |
| `LivePollingOptions` | Configuration: `chain`, `api_key`, `poll_interval_secs`, `timeout_secs`, `idle_wait_ms` |
| `LiveWebSocketIngestor` | Connects to WOC WebSocket for instant notifications; low latency. Has `subscribe()`, `is_running()`, and `get_errors()` methods |
| `LiveWebSocketOptions` | Configuration: `chain`, `api_key`, `idle_timeout_ms`, `ping_interval_ms`, `max_reconnect_attempts`, `reconnect_delay_ms`, `http_timeout_secs` |

### Response Types

| Type | Description |
|------|-------------|
| `BulkHeaderFileInfo` | CDN file metadata: `file_name`, `from_height`, `to_height`, `count`, `file_hash`, `chain`, `source_url` |
| `BulkHeaderFilesInfo` | CDN listing response: `files`, `headers_per_file`, `last_updated` |
| `WocChainInfo` | Chain status: `chain`, `blocks`, `headers`, `best_block_hash`, `difficulty`, `median_time` |
| `WocHeaderResponse` | Full header from WOC REST API (uses `previousblockhash`/`nextblockhash` field names) |
| `WocHeaderByteFileLinks` | WOC header byte file listing: `files` (Vec of URL strings) |
| `WocGetHeadersHeader` | Header from `/block/headers` endpoint (`bits` as hex string, `previous_block_hash`, `n_tx`, `num_tx`) |
| `WocWsBlockHeader` | Header from WebSocket stream (`bits` as numeric u32) |
| `WocWsMessage` | WebSocket message envelope (untagged enum: `HeaderData`, `TypedMessage`, `Connect`, `Empty`) |
| `WocPubData` | Published header data wrapper containing optional `WocWsBlockHeader` |

### Utility Functions

| Function | Description |
|----------|-------------|
| `woc_header_to_block_header()` | Convert `WocGetHeadersHeader` to `BlockHeader` (parses hex `bits` string) |
| `ws_header_to_block_header()` | Convert `WocWsBlockHeader` to `BlockHeader` (uses numeric `bits` directly) |

### Constants

| Constant | Value | Exported |
|----------|-------|----------|
| `DEFAULT_CDN_URL` | `https://bsv-headers.babbage.systems/` | Yes |
| `LEGACY_CDN_URL` | `https://cdn.projectbabbage.com/blockheaders/` | Yes |
| `WOC_API_URL_MAIN` | `https://api.whatsonchain.com/v1/bsv/main` | Yes |
| `WOC_API_URL_TEST` | `https://api.whatsonchain.com/v1/bsv/test` | Yes |
| `WOC_WS_URL_MAIN` | `wss://socket-v2.whatsonchain.com/websocket/blockHeaders` | Yes |
| `WOC_WS_URL_TEST` | `wss://socket-v2-testnet.whatsonchain.com/websocket/blockHeaders` | Yes |

Note: `WOC_WS_HISTORY_URL_MAIN` and `WOC_WS_HISTORY_URL_TEST` exist in `live_websocket.rs` but are `#[allow(dead_code)]` and not re-exported.

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
use bsv_wallet_toolbox::chaintracks::HeightRange;

let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet())?;

// Fetch headers in a range
let headers = ingestor.fetch_headers(
    0,                              // before: current sync point
    HeightRange::new(0, 99999),     // fetch_range
    None,                           // bulk_range (optional)
    &[],                            // prior_live_headers
).await?;
```

### WhatsOnChain Bulk Sync with API Key

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{BulkWocIngestor, BulkWocOptions};

let options = BulkWocOptions::mainnet().with_api_key("your-woc-api-key");
let ingestor = BulkWocIngestor::new(options)?;

// Get current chain tip
let height = ingestor.get_chain_tip_height().await?;

// Fetch header by hash
if let Some(header) = ingestor.get_header_by_hash("000000000019d6689c...").await? {
    println!("Genesis block: height={}", header.height);
}

// Fetch header byte file links (for binary download)
let links = ingestor.get_header_byte_file_links().await?;
```

### Polling for New Blocks

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{LivePollingIngestor, LivePollingOptions};

let options = LivePollingOptions::mainnet()
    .with_poll_interval(30)  // Check every 30 seconds
    .with_api_key("your-woc-api-key");

let ingestor = LivePollingIngestor::new(options)?;

// Subscribe to new headers (broadcast channel)
let mut receiver = ingestor.subscribe();

// Start polling
let mut live_headers = vec![];
ingestor.start_listening(&mut live_headers).await?;

// Receive notifications in another task
tokio::spawn(async move {
    while let Ok(header) = receiver.recv().await {
        println!("New block: height={}", header.height);
    }
});

// Check status and stop
assert!(ingestor.is_running());
ingestor.stop_listening();
```

### WebSocket Real-Time Updates

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{LiveWebSocketIngestor, LiveWebSocketOptions};

let options = LiveWebSocketOptions::mainnet()
    .with_idle_timeout(120_000)  // 2 minute idle timeout
    .with_api_key("your-woc-api-key");

let ingestor = LiveWebSocketIngestor::new(options)?;

let mut receiver = ingestor.subscribe();
let mut live_headers = vec![];
ingestor.start_listening(&mut live_headers).await?;

// Headers arrive instantly via WebSocket
while let Ok(header) = receiver.recv().await {
    println!("Block via WebSocket: height={}", header.height);
}

// Check errors if needed
let errors = ingestor.get_errors().await;
```

### Header Format Conversion

```rust
use bsv_wallet_toolbox::chaintracks::ingestors::{
    WocGetHeadersHeader, woc_header_to_block_header,
    WocWsBlockHeader, ws_header_to_block_header,
};

// From REST API (bits as hex string "1d00ffff")
let block_header = woc_header_to_block_header(&woc_header);

// From WebSocket (bits as u32: 486604799)
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

Both `BulkCdnIngestor` and `BulkWocIngestor` include `deserialize_header()` and `compute_block_hash()` methods that parse this format and compute double-SHA256 hashes (reversed for Bitcoin display convention).

### CDN File Naming

CDN files follow the pattern: `{from_height}_{to_height}_headers.bin`

Example: `0_99999_headers.bin` contains headers for heights 0-99999 (100,000 headers, 8MB).

The JSON index file (e.g., `mainNetBlockHeaders.json` or `testNetBlockHeaders.json`) lists available files with their height ranges. Files can optionally specify a `source_url` override and `file_hash` for integrity verification.

### WOC Header Byte File Links

`BulkWocIngestor` can also download binary header files via `get_header_byte_file_links()`, which fetches URLs from the `/block/headers/resources` endpoint. File links are parsed from URLs using the `{from}_{to}_headers.bin` naming convention, with a special `latest` file for the most recent headers.

### WebSocket Protocol

The WOC WebSocket uses a custom protocol:

1. Connect to `wss://socket-v2.whatsonchain.com/websocket/blockHeaders`
2. Send `{}` to initiate subscription
3. Receive messages as `WocWsMessage` (untagged enum):
   - `Connect`: Initial connection info
   - `TypedMessage` with type codes: 3 (unsubscribe), 5 (subscribed), 6 (confirm), 7 (data/error)
   - `HeaderData`: Block headers in `pub.data` or `data.data` fields
   - `Empty`: Ping response
4. Send periodic pings (`"ping"`) at `ping_interval_ms` intervals
5. Automatic reconnection on connection loss

### Error Handling and Reconnection

Live ingestors handle transient failures gracefully:

- **Polling**: Continues on fetch errors, logs warnings, detects new blocks by comparing against last seen header hashes
- **WebSocket**: Auto-reconnects with configurable attempts and delays
  - `max_reconnect_attempts`: Default 10
  - `reconnect_delay_ms`: Default 5000ms between attempts
  - `idle_timeout_ms`: Reconnect if no messages for 100s (default)
  - `WsError` enum tracks: `ConnectionFailed`, `MessageParseFailed`, `IdleTimeout`, `Stopped`
  - Error history available via `get_errors()` method

### BulkWocIngestor Additional Methods

Beyond the `BulkIngestor` trait, `BulkWocIngestor` provides:

- `get_chain_tip_height()` / `get_chain_tip_hash()` - Current chain state (cached via `chain_info_ttl_ms`)
- `get_header_by_hash(hash)` - Lookup individual headers
- `get_recent_headers()` - Last ~10 blocks from `/block/headers`
- `get_header_byte_file_links()` - Binary file download URLs

### Choosing an Ingestor

| Use Case | Recommended Ingestor |
|----------|---------------------|
| Initial historical sync | `BulkCdnIngestor` (fast, parallel downloads) |
| CDN unavailable | `BulkWocIngestor` (reliable fallback) |
| Mobile/battery-conscious | `LivePollingIngestor` (configurable interval) |
| Low-latency trading | `LiveWebSocketIngestor` (instant notifications) |
| Development/testing | `LivePollingIngestor` (simpler to debug) |

## Internal Types (Not Exported)

- `FileLink` (bulk_woc.rs) - Parsed file link with URL, file name, optional height range, and `is_latest` flag
- `WsError` (live_websocket.rs) - WebSocket-specific error enum
- `block_header_to_live_header()` (live_polling.rs, live_websocket.rs) - Converts `BlockHeader` to `LiveBlockHeader` with default chain work and flags

## Related

- [`../CLAUDE.md`](../CLAUDE.md) - Parent Chaintracks module documentation
- [`../traits.rs`](../traits.rs) - `BulkIngestor` and `LiveIngestor` trait definitions
- [`../types.rs`](../types.rs) - `BlockHeader`, `LiveBlockHeader`, `HeightRange`, `Chain` types
