# src/services/ - Blockchain Services Layer
> Multi-provider blockchain API abstraction with automatic failover

## Overview

The services module provides a unified interface for interacting with BSV blockchain services. It coordinates multiple service providers (WhatsOnChain, ARC, Bitails) with automatic failover and call tracking. The system uses a collection-based pattern where each operation type maintains an ordered list of providers that are tried sequentially until one succeeds.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Services                                 │
│  (Main orchestrator implementing WalletServices trait)          │
├─────────────────────────────────────────────────────────────────┤
│                    ServiceCollection<T>                          │
│  (Per-operation failover with call history tracking)            │
├───────────────┬───────────────┬───────────────┬─────────────────┤
│ WhatsOnChain  │     ARC       │    Bitails    │                 │
│ - Raw TX      │ - BEEF Post   │ - Raw TX      │                 │
│ - Merkle Path │ - Merkle Path │ - Merkle Path │                 │
│ - UTXO Status │ - Tx Status   │ - Tx Status   │                 │
│ - Script Hist │               │ - Script Hist │                 │
│ - Exchange $  │               │               │                 │
└───────────────┴───────────────┴───────────────┴─────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with re-exports, `ServicesOptions` configuration, and `Chain` re-export from chaintracks |
| `traits.rs` | `WalletServices` trait definition and all result types (`GetRawTxResult`, `PostBeefResult`, etc.) |
| `services.rs` | `Services` struct implementing `WalletServices` with multi-provider orchestration |
| `collection.rs` | `ServiceCollection<S>` generic failover container with call history tracking |
| `providers/` | Individual provider implementations (WhatsOnChain, ARC, Bitails) |

## Key Types

### Services (services.rs:48)

Main orchestrator coordinating all blockchain operations:

```rust
pub struct Services {
    pub chain: Chain,
    pub options: ServicesOptions,
    pub whatsonchain: Arc<WhatsOnChain>,
    pub arc_taal: Arc<Arc>,
    pub arc_gorillapool: Option<Arc<Arc>>,
    pub bitails: Arc<Bitails>,
    // Internal service collections for each operation...
}
```

**Factory Methods:**
- `Services::mainnet()` - Create with mainnet defaults
- `Services::testnet()` - Create with testnet configuration
- `Services::with_options(chain, options)` - Create with custom options

**Key Methods:**
- `get_raw_tx(txid)` - Retrieve raw transaction bytes
- `get_merkle_path(txid)` - Get merkle proof for SPV verification
- `post_beef(beef, txids)` - Broadcast BEEF-format transaction
- `get_utxo_status(output, format, outpoint)` - Check if output is unspent
- `get_status_for_txids(txids)` - Check confirmation status of transactions
- `get_script_hash_history(hash)` - Get transaction history for script
- `get_bsv_exchange_rate()` - Get cached USD/BSV rate
- `get_services_call_history(reset)` - Get diagnostics for all service calls

### WalletServices Trait (traits.rs:22)

Core trait that `Services` implements:

```rust
#[async_trait]
pub trait WalletServices: Send + Sync {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker>;
    async fn get_height(&self) -> Result<u32>;
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult>;
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult>;
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>>;
    async fn get_utxo_status(&self, output: &str, format: Option<GetUtxoStatusOutputFormat>, outpoint: Option<&str>) -> Result<GetUtxoStatusResult>;
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult>;
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult>;
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;
    fn hash_output_script(&self, script: &[u8]) -> String;
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool>;
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool>;
}
```

### ServiceCollection (collection.rs:21)

Generic failover container maintaining ordered providers:

```rust
pub struct ServiceCollection<S> {
    pub service_name: String,
    services: Vec<NamedService<S>>,
    index: usize,
    // Call history tracking...
}
```

**Key Methods:**
- `add(name, service)` - Add provider to collection
- `next()` - Advance to next provider (wraps around)
- `move_to_last(name)` - De-prioritize failing provider
- `add_call_success/failure/error(provider, call)` - Record call outcome
- `get_call_history(reset)` - Get statistics, optionally reset counters

### ServicesOptions (mod.rs:64)

Configuration for service providers:

```rust
pub struct ServicesOptions {
    pub whatsonchain_api_key: Option<String>,
    pub bitails_api_key: Option<String>,
    pub arc_url: String,
    pub arc_config: Option<ArcConfig>,
    pub arc_gorillapool_url: Option<String>,
    pub arc_gorillapool_config: Option<ArcConfig>,
    pub bsv_update_msecs: u64,  // 15 min default
    pub fiat_update_msecs: u64, // 15 min default
}
```

## Result Types (traits.rs)

| Type | Purpose |
|------|---------|
| `GetRawTxResult` | Raw transaction bytes with provider name |
| `GetMerklePathResult` | Merkle proof in TSC/BUMP format |
| `PostBeefResult` | Broadcast result with per-txid status |
| `PostTxResultForTxid` | Single transaction broadcast result |
| `GetUtxoStatusResult` | UTXO check with details list |
| `GetStatusForTxidsResult` | Batch transaction status check |
| `GetScriptHashHistoryResult` | Transaction history for address |
| `BlockHeader` | Parsed block header with all fields |
| `BsvExchangeRate` | Cached exchange rate with staleness check |

## Provider Priority by Operation

The `Services` constructor sets up provider priority for each operation:

| Operation | Provider Order |
|-----------|----------------|
| `get_merkle_path` | WhatsOnChain → Bitails |
| `get_raw_tx` | WhatsOnChain → Bitails |
| `post_beef` | GorillaPool ARC → TAAL ARC → Bitails → WhatsOnChain |
| `get_utxo_status` | WhatsOnChain |
| `get_status_for_txids` | WhatsOnChain → Bitails |
| `get_script_hash_history` | WhatsOnChain → Bitails |

## Providers

### WhatsOnChain (providers/whatsonchain.rs)

Primary data provider for most operations:
- **Mainnet:** `https://api.whatsonchain.com/v1/bsv/main`
- **Testnet:** `https://api.whatsonchain.com/v1/bsv/test`
- Optional API key for rate limit bypass
- Auto-retry on 429 (rate limited)
- Exchange rate caching

### ARC (providers/arc.rs)

Transaction broadcast service (mAPI):
- **TAAL Mainnet:** `https://arc.taal.com`
- **TAAL Testnet:** `https://arc-test.taal.com`
- **GorillaPool:** `https://arc.gorillapool.io`
- Native BEEF v1 support
- Callback URLs for proof delivery
- Double-spend detection

### Bitails (providers/bitails.rs)

Alternative provider:
- **Mainnet:** `https://api.bitails.io/`
- **Testnet:** `https://test-api.bitails.io/`
- TSC merkle proofs
- Multi-transaction broadcast
- Script hash history

## Helper Functions (traits.rs)

```rust
pub fn sha256(data: &[u8]) -> Vec<u8>;
pub fn double_sha256(data: &[u8]) -> Vec<u8>;
pub fn txid_from_raw_tx(raw_tx: &[u8]) -> String;
pub fn validate_txid(raw_tx: &[u8], expected: &str) -> Result<()>;
pub fn validate_script_hash(hash: &str) -> Result<()>;
pub fn convert_script_hash(output: &str, format: Option<GetUtxoStatusOutputFormat>) -> Result<String>;
```

## Usage Examples

### Create Services with Defaults

```rust
use bsv_wallet_toolbox::services::{Services, Chain};

let services = Services::mainnet()?;
let raw_tx = services.get_raw_tx("txid...").await?;
```

### Custom Configuration

```rust
use bsv_wallet_toolbox::services::{Services, ServicesOptions, Chain};

let options = ServicesOptions::mainnet()
    .with_woc_api_key("your-api-key")
    .with_bitails_api_key("bitails-key");

let services = Services::with_options(Chain::Main, options)?;
```

### Post BEEF Transaction

```rust
let beef_bytes: Vec<u8> = /* BEEF-encoded transaction */;
let txids = vec!["txid1".to_string(), "txid2".to_string()];

let results = services.post_beef(&beef_bytes, &txids).await?;
for result in results {
    if result.is_success() {
        println!("Broadcast via {}", result.name);
    }
}
```

### Check UTXO Status

```rust
let script_hash = services.hash_output_script(&locking_script);
let status = services.get_utxo_status(&script_hash, None, Some("txid.0")).await?;

if status.is_utxo.unwrap_or(false) {
    println!("Output is unspent!");
}
```

### Get Service Diagnostics

```rust
let history = services.get_services_call_history(true); // true = reset counters

for (name, provider_history) in &history.get_raw_tx.unwrap().history_by_provider {
    println!("{}: {} success, {} failures",
        name,
        provider_history.total_counts.success,
        provider_history.total_counts.failure
    );
}
```

## PostBeefMode (services.rs:24)

Controls broadcast behavior:

```rust
pub enum PostBeefMode {
    UntilSuccess,  // Default: try providers until one succeeds
    PromiseAll,    // Broadcast to all providers in parallel
}
```

## Error Handling

All methods return `Result<T>` with these error variants:
- `Error::NoServicesAvailable` - No providers configured for operation
- `Error::NetworkError` - HTTP request failed
- `Error::ServiceError` - Provider returned error
- `Error::ValidationError` - Invalid txid, script hash, etc.
- `Error::NotFound` - Resource not found (block header, etc.)

## Call History Tracking

Each `ServiceCollection` tracks:
- Recent calls (up to 32) with timing
- Total counts (success/failure/error)
- Reset interval counts for monitoring
- Provider-specific statistics

Use `get_services_call_history(reset)` to retrieve and optionally reset counters.

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent module overview
- [providers/CLAUDE.md](./providers/CLAUDE.md) - Provider implementation details
- [../chaintracks/CLAUDE.md](../chaintracks/CLAUDE.md) - Block header tracking (provides `Chain` type)
