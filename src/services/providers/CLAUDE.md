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

## Provider Details

### WhatsOnChain (`whatsonchain.rs`)

Primary blockchain data provider with the most comprehensive feature set.

**Struct:** `WhatsOnChain`

**Configuration:** `WhatsOnChainConfig`
- `api_key: Option<String>` - Bearer token for authenticated access
- `timeout_secs: Option<u64>` - Request timeout (default: 30s)

**Factory Methods:**
```rust
// Create for mainnet or testnet
WhatsOnChain::new(Chain::Main, WhatsOnChainConfig::default())?;
WhatsOnChain::new(Chain::Test, WhatsOnChainConfig::with_api_key("key"))?;
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `get_raw_tx(txid)` | Retrieve raw transaction bytes |
| `get_merkle_path(txid)` | Get TSC merkle proof |
| `post_raw_tx(hex)` | Broadcast raw transaction |
| `post_beef(beef, txids)` | Broadcast BEEF (limited support) |
| `get_utxo_status(output, format, outpoint)` | Check UTXO status for script hash |
| `get_status_for_txids(txids)` | Batch query transaction statuses |
| `get_script_hash_history(hash)` | Get confirmed + unconfirmed tx history |
| `get_script_hash_confirmed_history(hash)` | Get confirmed tx history only |
| `get_script_hash_unconfirmed_history(hash)` | Get mempool tx history only |
| `get_block_header_by_hash(hash)` | Get parsed block header |
| `get_chain_info()` | Get chain state (height, best block, etc.) |
| `update_bsv_exchange_rate(update_msecs)` | Get cached BSV/USD rate |

**API Endpoints:**
- Mainnet: `https://api.whatsonchain.com/v1/bsv/main`
- Testnet: `https://api.whatsonchain.com/v1/bsv/test`

**Rate Limiting:** Built-in retry with 2-second backoff on HTTP 429 responses.

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
// Pre-configured endpoints
Arc::taal_mainnet(Some(ArcConfig::with_api_key("key")))?;
Arc::taal_testnet(config)?;
Arc::gorillapool(config)?;

// Custom endpoint
Arc::new("https://custom-arc.example.com", config, Some("customArc"))?;

// With callbacks
let config = ArcConfig::with_api_key("key")
    .with_callback("https://myserver.com/callback", Some("secret"))
    .with_deployment_id("my-app-v1");
```

**Capabilities:**
| Method | Description |
|--------|-------------|
| `post_raw_tx(hex, txids)` | Broadcast raw/EF/BEEF v1 transaction |
| `post_beef(beef, txids)` | Broadcast BEEF with multi-txid support |
| `get_tx_data(txid)` | Query transaction status (recent txs only) |
| `get_merkle_path(txid)` | Get BUMP merkle path if available |

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

**Response Types:**
- `ArcTxInfo` - Transaction info including `txid`, `tx_status`, `merkle_path`, `block_hash`, `block_height`, `competing_txs`
- `ArcApiError` - Detailed error information

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
| `get_raw_tx(txid)` | Retrieve raw transaction bytes |
| `get_merkle_path(txid)` | Get TSC format merkle proof |
| `broadcast(raw_tx)` | Broadcast single transaction |
| `post_raws(raws)` | Broadcast multiple transactions |
| `post_beef(beef, txids)` | BEEF broadcast (not fully implemented) |
| `get_current_height()` | Get blockchain height |
| `get_block_header_by_hash(hash)` | Get 80-byte raw header |
| `get_latest_block()` | Get latest block hash and height |
| `get_script_hash_history(hash)` | Get transaction history |
| `get_status_for_txids(txids)` | Get transaction statuses with depth |
| `get_tx_info(txid)` | Get transaction metadata |

**API Endpoints:**
- Mainnet: `https://api.bitails.io/`
- Testnet: `https://test-api.bitails.io/`

**Error Codes:**
| Code | Constant | Meaning |
|------|----------|---------|
| "-27" | `ALREADY_IN_MEMPOOL` | Transaction already known |
| "-25" | `DOUBLE_SPEND_OR_MISSING_INPUTS` | Double spend or missing input |
| "ECONNREFUSED" | `ECONNREFUSED` | Connection refused |
| "ECONNRESET" | `ECONNRESET` | Connection reset |

**Internal Features:**
- `root_cache: RwLock<HashMap<u32, String>>` - Caches merkle roots by block height

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
| `BlockHeader` | Parsed 80-byte header with height |

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

### Script Hash History Query
```rust
let bitails = Bitails::new(Chain::Main, BitailsConfig::default())?;
// Note: script hash must be 64 hex chars (SHA256 of locking script)
let history = bitails.get_script_hash_history("abc123...").await?;
for item in history.history {
    println!("txid: {}, height: {:?}", item.txid, item.height);
}
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
- **Arc**: Full BEEF v1 support; v2 detection with conversion needed
- **WhatsOnChain**: Limited BEEF support; prefers raw tx broadcasting
- **Bitails**: BEEF parsing not fully implemented

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
```

## Related Documentation

- `../traits.rs` - `WalletServices` trait and result type definitions
- `../mod.rs` - Service module organization
- `../../chaintracks/` - Chain and block header tracking
