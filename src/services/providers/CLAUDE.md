# Providers - BSV Blockchain Service Providers
> HTTP client implementations for BSV blockchain APIs

## Overview

This module contains concrete implementations of blockchain service providers that enable wallet operations. Each provider wraps a specific blockchain API to provide:

- Transaction retrieval and broadcasting
- Merkle proof retrieval for SPV verification
- UTXO and script hash history queries
- Block header information

The providers are designed to be used through the `WalletServices` trait defined in `src/services/traits.rs`.

## Providers

| Provider | Purpose | Networks | Primary Use |
|----------|---------|----------|-------------|
| `WhatsOnChain` | Full-featured blockchain explorer API | Mainnet, Testnet | UTXO queries, raw tx retrieval, script history |
| `Arc` | Transaction broadcasting (mAPI) | TAAL, GorillaPool | BEEF transaction broadcasting with callbacks |
| `Bitails` | Alternative merkle proof provider | Mainnet, Testnet | TSC merkle proofs, tx history |
| `BlockHeaderService` | Block Header Service (BHS) | Mainnet, Testnet | Header lookups, merkle root validation |

## Provider Details

### WhatsOnChain (`whatsonchain.rs`)

Primary blockchain data provider with the most comprehensive feature set.

**Struct:** `WhatsOnChain`

**Configuration:** `WhatsOnChainConfig`
- `api_key: Option<String>` - Bearer token for authenticated access
- `timeout_secs: Option<u64>` - Request timeout (default: 30s)

**Factory Methods:**
```rust
WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default())?;
WhatsOnChain::new(Chain::Test, WhatsOnChainConfig::with_api_key("key"))?;
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `get_raw_tx(txid)` | Retrieve raw transaction bytes |
| `get_merkle_path(txid)` | Get TSC merkle proof |
| `post_raw_tx(hex)` | Broadcast raw transaction (with retry on 429) |
| `post_beef(beef, txids)` | Parse BEEF, extract raw txs, broadcast sequentially with 1s delay |
| `get_utxo_status(output, format, outpoint)` | Check UTXO status for script hash |
| `get_status_for_txids(txids)` | Batch query transaction statuses (POST) |
| `get_script_hash_history(hash)` | Get confirmed + unconfirmed tx history |
| `get_script_hash_confirmed_history(hash)` | Get confirmed tx history only |
| `get_script_hash_unconfirmed_history(hash)` | Get mempool tx history only |
| `get_block_header_by_hash(hash)` | Get parsed block header |
| `get_chain_info()` | Get chain state (height, best block, etc.) |
| `update_bsv_exchange_rate(update_msecs)` | Get cached BSV/USD rate |

**API Endpoints:**
- Mainnet: `https://api.whatsonchain.com/v1/bsv/main`
- Testnet: `https://api.whatsonchain.com/v1/bsv/test`

**Rate Limiting:** Built-in retry with 2-second backoff on HTTP 429 responses (up to 2 retries for GET, 5 for POST).

**BEEF Handling:** Parses BEEF via `Beef::from_binary()`, finds each txid with `find_txid()`, extracts raw tx bytes, and broadcasts each via `post_raw_tx` with 1-second delays between transactions.

---

### Arc (`arc.rs`)

Transaction broadcasting service implementing the ARC (mAPI) protocol. Preferred for BEEF transaction broadcasting.

**Struct:** `Arc`

**Configuration:** `ArcConfig`
- `api_key: Option<String>` - Bearer token authentication
- `deployment_id: Option<String>` - Request tracking ID (auto-generated UUID if not set)
- `callback_url: Option<String>` - URL for proof/double-spend notifications
- `callback_token: Option<String>` - Auth token for callback endpoint
- `wait_for: Option<String>` - Wait header (e.g., "SEEN_ON_NETWORK")
- `headers: Option<HashMap<String, String>>` - Additional custom headers
- `timeout_secs: Option<u64>` - Request timeout (default: 30s)

**Factory Methods:**
```rust
Arc::taal_mainnet(Some(ArcConfig::with_api_key("key")))?;
Arc::taal_testnet(config)?;
Arc::gorillapool(config)?;
Arc::new("https://custom-arc.example.com", config, Some("customArc"))?;

let config = ArcConfig::with_api_key("key")
    .with_callback("https://myserver.com/callback", Some("secret"))
    .with_deployment_id("my-app-v1");
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `post_raw_tx(hex, txids)` | Broadcast raw/EF/BEEF v1 transaction |
| `post_beef(beef, txids)` | Broadcast BEEF with auto V2-to-V1 conversion and multi-txid support |
| `get_tx_data(txid)` | Query transaction status (recent txs only) |
| `get_merkle_path(txid)` | Get BUMP merkle path if available (via `get_tx_data`) |

**API Endpoints:**
- TAAL Mainnet: `https://arc.taal.com`
- TAAL Testnet: `https://arc-test.taal.com`
- GorillaPool: `https://arc.gorillapool.io`

**Custom Status Codes:**
| Code | Constant | Meaning |
|------|----------|---------|
| 460 | `NOT_EXTENDED_FORMAT` | Transaction not in extended format |
| 465 | `FEE_TOO_LOW` | Fee below minimum |
| 473 | `CUMULATIVE_FEE_FAILED` | Cumulative fee validation failed |

**BEEF V2-to-V1 Conversion:** `post_beef` detects BEEF v2 (prefix `0200BEEF`), parses via `Beef::from_binary()`, checks if all txs have full data (no txid-only entries), and downgrades to v1 by setting `parsed_beef.version = BEEF_V1` before posting.

**Response Types:**
- `ArcTxInfo` - Transaction info: `txid`, `tx_status`, `merkle_path`, `block_hash`, `block_height`, `competing_txs`, `timestamp`, `extra_info`
- `ArcApiError` - Error detail: `error_type`, `title`, `status`, `detail`, `instance`, `txid`, `extra_info`

---

### Bitails (`bitails.rs`)

Alternative provider focused on merkle proofs and transaction history.

**Struct:** `Bitails`

**Configuration:** `BitailsConfig`
- `api_key: Option<String>` - Optional API key
- `timeout_secs: Option<u64>` - Request timeout (default: 30s)

**Factory Method:**
```rust
Bitails::new(Chain::Main, BitailsConfig::default())?;
Bitails::new(Chain::Test, BitailsConfig::with_api_key("key"))?;
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `get_raw_tx(txid)` | Retrieve raw transaction bytes (with txid validation) |
| `get_merkle_path(txid)` | Get TSC format merkle proof |
| `broadcast(raw_tx)` | Broadcast single raw transaction (via `post_raws`) |
| `post_raws(raws)` | Broadcast multiple raw transactions (multi endpoint) |
| `post_beef(beef, txids)` | Parse BEEF, extract raw txs, broadcast each individually |
| `get_current_height()` | Get blockchain height from network/info |
| `current_height()` | Alias for `get_current_height()` |
| `get_block_header_by_hash(hash)` | Get and parse 80-byte raw header |
| `get_header_by_height(height)` | Get block header by height (JSON) |
| `get_latest_block()` | Get latest block hash and height |
| `get_script_hash_history(hash)` | Get transaction history for script hash |
| `get_status_for_txids(txids)` | Get tx statuses with depth (queries each via `get_tx_info`) |
| `is_valid_root_for_height(root, height)` | Validate merkle root for a block height |

**API Endpoints:**
- Mainnet: `https://api.bitails.io/`
- Testnet: `https://test-api.bitails.io/`

**Error Codes:**
| Code | Constant | Meaning |
|------|----------|---------|
| "-27" | `ALREADY_IN_MEMPOOL` | Transaction already known (treated as success) |
| "-25" | `DOUBLE_SPEND_OR_MISSING_INPUTS` | Double spend or missing input |
| "ECONNREFUSED" | `ECONNREFUSED` | Connection refused |
| "ECONNRESET" | `ECONNRESET` | Connection reset |

**Internal Features:**
- `root_cache: RwLock<HashMap<u32, String>>` - Caches merkle roots by block height
- `parse_block_header()` - Parses 80-byte raw headers into `BlockHeader` structs

---

### BlockHeaderService (`bhs.rs`)

Dedicated block header lookup service for production header data and merkle root validation.

**Struct:** `BlockHeaderService`

**Configuration:** `BhsConfig`
- `url: String` - BHS API URL
- `api_key: Option<String>` - Optional Bearer token

**Factory Methods:**
```rust
BlockHeaderService::new(BhsConfig::mainnet());
BlockHeaderService::new(BhsConfig::testnet());
BlockHeaderService::from_url("https://custom-bhs.example.com");
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `current_height()` | Get current blockchain tip height |
| `chain_header_by_height(height)` | Get block header by height (JSON `BlockHeader`) |
| `is_valid_root_for_height(root, height)` | Validate merkle root for a given block height |
| `find_chain_tip_header()` | Get the chain tip header |

**API Endpoints:**
- Mainnet: `https://bhs.babbage.systems`
- Testnet: `https://bhs-test.babbage.systems`

**API Paths:**
- `/api/v1/chain/tip/height` - Current height (plain text)
- `/api/v1/chain/header/byHeight?height=N` - Header by height (JSON)
- `/api/v1/chain/header/tip` - Chain tip header (JSON)

## Common Result Types

All providers return standardized result types from `src/services/traits.rs`:

| Type | Purpose |
|------|---------|
| `GetRawTxResult` | Raw transaction with provider name and optional error |
| `GetMerklePathResult` | Merkle path/BUMP format with notes |
| `PostBeefResult` | Broadcast result with per-txid statuses |
| `PostTxResultForTxid` | Single tx broadcast status with double-spend detection |
| `GetUtxoStatusResult` | UTXO status with details array |
| `GetStatusForTxidsResult` | Batch txid status results |
| `GetScriptHashHistoryResult` | Transaction history for script |
| `BlockHeader` | Parsed header with version, previous_hash, merkle_root, time, bits, nonce, hash, height |

## Usage Patterns

### Basic Transaction Retrieval
```rust
let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default())?;
let result = woc.get_raw_tx("abc123...").await?;
if let Some(raw_tx) = result.raw_tx {
    // Process transaction bytes
}
```

### BEEF Broadcasting with Callbacks
```rust
let config = ArcConfig::with_api_key("your-api-key")
    .with_callback("https://yourserver.com/callback", Some("secret-token"));
let arc = Arc::taal_mainnet(Some(config))?;

let result = arc.post_beef(&beef_bytes, &["txid1".to_string()]).await?;
for tx_result in result.txid_results {
    if tx_result.double_spend {
        // Handle double-spend
    }
}
```

### Block Header Validation via BHS
```rust
let bhs = BlockHeaderService::new(BhsConfig::mainnet());
let height = bhs.current_height().await?;
let header = bhs.chain_header_by_height(height).await?;
let valid = bhs.is_valid_root_for_height(&merkle_root, height).await?;
```

### Exchange Rate with Caching
```rust
let woc = WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default())?;
// Rate cached for specified duration (milliseconds)
let rate = woc.update_bsv_exchange_rate(60_000).await?;
println!("BSV/USD: {}", rate);
```

## Implementation Notes

### Script Hash Format
- WhatsOnChain and Bitails expect **big-endian** script hashes
- Internal functions convert from little-endian to big-endian automatically
- Use `validate_script_hash()` from traits.rs to validate 64-char hex format

### BEEF Support
- **Arc**: Full BEEF v1 support; automatic V2-to-V1 downgrade when all txs have full data
- **WhatsOnChain**: Parses BEEF via `Beef::from_binary()`, extracts raw txs, broadcasts sequentially
- **Bitails**: Same BEEF parsing approach; broadcasts each extracted tx individually

### Transaction ID Computation
All providers use `txid_from_raw_tx()` from traits.rs which computes:
1. Double SHA256 of raw transaction bytes
2. Byte reversal to big-endian (txid display format)

### Notes System
All providers attach diagnostic notes to results using `make_note()`:
```rust
HashMap {
    "what": "getMerklePathSuccess",
    "name": "WoC",
    "when": "2024-01-15T10:30:00Z"
}
```

## Testing

Each provider includes unit tests for configuration and URL construction:

```bash
# Run all provider tests
cargo test services::providers

# Run specific provider tests
cargo test services::providers::whatsonchain
cargo test services::providers::arc
cargo test services::providers::bitails
cargo test services::providers::bhs
```

## Related Documentation

- `../traits.rs` - `WalletServices` trait and result type definitions
- `../mod.rs` - Service module organization
- `../../chaintracks/` - Chain and block header tracking
