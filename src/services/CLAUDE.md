# src/services/ - Blockchain Services Layer
> Multi-provider blockchain API abstraction with automatic failover

## Overview

The services module provides a unified interface for interacting with BSV blockchain services. It coordinates multiple service providers (WhatsOnChain, ARC, Bitails, BHS) with automatic failover and call tracking. The system uses a collection-based pattern where each operation type maintains an ordered list of providers that are tried sequentially until one succeeds.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Services                                 │
│  (Main orchestrator implementing WalletServices trait)          │
├─────────────────────────────────────────────────────────────────┤
│                    ServiceCollection<T>                          │
│  (Per-operation failover with call history tracking)            │
├───────────────┬───────────────┬───────────────┬─────────────────┤
│ WhatsOnChain  │     ARC       │    Bitails    │      BHS        │
│ - Raw TX      │ - BEEF Post   │ - Raw TX      │ - Height        │
│ - Merkle Path │ - Merkle Path │ - Merkle Path │ - Header by Ht  │
│ - UTXO Status │ - Tx Status   │ - Tx Status   │ - Merkle Root   │
│ - Script Hist │               │ - Script Hist │   Validation    │
│ - Exchange $  │               │ - Height      │ - Chain Tip     │
│ - Chain Info  │               │               │                 │
└───────────────┴───────────────┴───────────────┴─────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with re-exports, `ServicesOptions` configuration, fiat currency types, and `Chain` re-export from chaintracks |
| `traits.rs` | `WalletServices` trait definition, `NLockTimeInput` type, and all result types (`GetRawTxResult`, `PostBeefResult`, `GetBeefResult`, etc.) |
| `services.rs` | `Services` struct implementing `WalletServices` with multi-provider orchestration |
| `collection.rs` | `ServiceCollection<S>` generic failover container with call history tracking |
| `providers/` | Individual provider implementations (WhatsOnChain, ARC, Bitails, BHS) |

## Key Types

### Services (services.rs:49)

Main orchestrator coordinating all blockchain operations:

```rust
pub struct Services {
    pub chain: Chain,
    pub options: ServicesOptions,
    pub whatsonchain: StdArc<WhatsOnChain>,
    pub arc_taal: StdArc<Arc>,
    pub arc_gorillapool: Option<StdArc<Arc>>,
    pub bitails: StdArc<Bitails>,
    pub bhs: Option<StdArc<BlockHeaderService>>,
    pub post_beef_mode: PostBeefMode,
    // Internal RwLock-protected service collections for each operation...
    // Cached exchange rates (BSV and fiat)...
}
```

**Factory Methods:**
- `Services::new(chain)` - Create with chain-appropriate defaults
- `Services::mainnet()` - Create with mainnet defaults
- `Services::testnet()` - Create with testnet configuration
- `Services::with_options(chain, options)` - Create with custom options

**Key Methods:**
- `get_raw_tx(txid, use_next)` - Retrieve raw transaction bytes
- `get_merkle_path(txid, use_next)` - Get merkle proof for SPV verification
- `get_beef(txid, known_txids)` - Build BEEF from raw tx and merkle path
- `post_beef(beef, txids)` - Broadcast BEEF-format transaction
- `get_utxo_status(output, format, outpoint, use_next)` - Check if output is unspent
- `get_status_for_txids(txids, use_next)` - Check confirmation status of transactions
- `get_script_hash_history(hash, use_next)` - Get transaction history for script
- `get_bsv_exchange_rate()` - Get cached USD/BSV rate
- `get_fiat_exchange_rate(currency, base)` - Get fiat exchange rate between currencies
- `get_height()` - Get current blockchain height (BHS -> WoC -> Bitails failover)
- `hash_to_header(hash)` - Get block header by hash
- `n_lock_time_is_final(n_lock_time)` - Check if raw nLockTime value allows mining
- `n_lock_time_is_final_for_tx(input)` - Check nLockTime finality with sequence info
- `get_services_call_history(reset)` - Get diagnostics for all service calls
- `set_post_beef_mode(mode)` - Configure broadcast behavior

**Provider Count Methods:**
- `get_merkle_path_count()` - Number of merkle path providers
- `get_raw_tx_count()` - Number of raw tx providers
- `post_beef_count()` - Number of post beef providers
- `get_utxo_status_count()` - Number of UTXO status providers

### WalletServices Trait (traits.rs:93)

Core trait that `Services` implements:

```rust
#[async_trait]
pub trait WalletServices: Send + Sync {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker>;
    async fn get_height(&self) -> Result<u32>;
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>>;
    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader>;
    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult>;
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult>;
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>>;
    async fn get_utxo_status(&self, output: &str, format: Option<GetUtxoStatusOutputFormat>,
                             outpoint: Option<&str>, use_next: bool) -> Result<GetUtxoStatusResult>;
    async fn get_status_for_txids(&self, txids: &[String], use_next: bool) -> Result<GetStatusForTxidsResult>;
    async fn get_script_hash_history(&self, hash: &str, use_next: bool) -> Result<GetScriptHashHistoryResult>;
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;
    async fn get_fiat_exchange_rate(&self, currency: FiatCurrency, base: Option<FiatCurrency>) -> Result<f64>;
    fn hash_output_script(&self, script: &[u8]) -> String;
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool>;
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool>;
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool>;
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult>;
}
```

**`use_next` Parameter:** Several methods accept a `use_next: bool` parameter. When `true`, the service collection skips to the next provider before starting the failover cycle. This is useful for retrying with alternate providers when the current one returned incomplete results.

### NLockTimeInput (traits.rs:26)

Pre-extracted data for nLockTime finality checks:

```rust
pub struct NLockTimeInput {
    pub lock_time: u32,
    pub all_sequences_final: bool,
}
```

**Factory Methods:**
- `NLockTimeInput::from_lock_time(u32)` - From raw nLockTime value
- `NLockTimeInput::from_transaction(&Transaction)` - From Transaction reference
- `NLockTimeInput::from_raw_tx(&[u8])` - From raw transaction bytes
- `NLockTimeInput::from_hex_tx(&str)` - From hex-encoded transaction

**Finality Rules:**
1. If all inputs have sequence = 0xFFFFFFFF, transaction is immediately final
2. If nLockTime >= 500,000,000: Unix timestamp, final if in the past
3. If nLockTime < 500,000,000: block height, final if current height > nLockTime

### ServiceCollection (collection.rs:21)

Generic failover container maintaining ordered providers:

```rust
pub struct ServiceCollection<S> {
    pub service_name: String,
    services: Vec<NamedService<S>>,
    index: usize,
    since: DateTime<Utc>,
    history_by_provider: HashMap<String, ProviderCallHistoryInternal>,
}
```

**Key Methods:**
- `new(name)` - Create new collection
- `add(name, service)` / `with(name, service)` - Add provider to collection
- `remove(name)` - Remove provider by name
- `count()` / `is_empty()` - Collection size queries
- `current_name()` / `current_service()` - Get current provider
- `next()` - Advance to next provider (wraps around)
- `reset()` - Return to first provider
- `move_to_last(name)` - De-prioritize failing provider
- `service_to_call()` - Get current service with call metadata
- `all_services_to_call()` - Get all services for parallel operations
- `all_services_owned()` - Get owned copies (avoids lock contention)
- `all_services_from_current()` - Get services in round-robin order from current index
- `clone_collection()` - Clone with fresh history (requires `S: Clone`)
- `add_call_success/failure/error(provider, call)` - Record call outcome
- `get_call_history(reset)` - Get statistics, optionally reset counters

### SharedServiceCollection (collection.rs:474)

Thread-safe wrapper for concurrent access:

```rust
pub struct SharedServiceCollection<S>(pub Arc<RwLock<ServiceCollection<S>>>);
```

### ServicesOptions (mod.rs:70)

Configuration for service providers:

```rust
pub struct ServicesOptions {
    pub whatsonchain_api_key: Option<String>,
    pub bitails_api_key: Option<String>,
    pub arc_url: String,
    pub arc_config: Option<ArcConfig>,
    pub arc_gorillapool_url: Option<String>,
    pub arc_gorillapool_config: Option<ArcConfig>,
    pub bhs_url: Option<String>,
    pub bhs_api_key: Option<String>,
    pub bsv_update_msecs: u64,       // 15 min default
    pub fiat_update_msecs: u64,      // 24 hour default
    pub fiat_exchange_rates: FiatExchangeRates,
}
```

**Builder Methods:**
- `ServicesOptions::mainnet()` / `testnet()` - Create with defaults
- `with_woc_api_key(key)` - Set WhatsOnChain API key
- `with_bitails_api_key(key)` - Set Bitails API key
- `with_arc(url, config)` - Set TAAL ARC configuration
- `with_gorillapool(url, config)` - Set GorillaPool ARC configuration
- `with_bhs_url(url)` - Set Block Header Service URL
- `with_bhs_api_key(key)` - Set Block Header Service API key
- `with_bhs(url, api_key)` - Set BHS URL and API key together

### FiatCurrency (traits.rs:657)

Supported fiat currencies for exchange rate conversions:

```rust
pub enum FiatCurrency {
    USD,
    GBP,
    EUR,
}
```

**Methods:**
- `FiatCurrency::parse(s)` - Parse from string (case-insensitive)
- `as_str()` - Get currency code as string
- Implements `FromStr` and `Display`

### FiatExchangeRates (traits.rs:703)

Fiat exchange rates with USD as base:

```rust
pub struct FiatExchangeRates {
    pub timestamp: DateTime<Utc>,
    pub base: FiatCurrency,
    pub rates: HashMap<FiatCurrency, f64>,
}
```

**Methods:**
- `FiatExchangeRates::new(rates)` - Create with given rates
- `FiatExchangeRates::default()` - Create with default rates (USD=1.0, EUR=0.85, GBP=0.79)
- `is_stale(max_age_msecs)` - Check if rates need refresh
- `get_rate(currency, base)` - Get exchange rate between currencies

## Result Types (traits.rs)

| Type | Purpose |
|------|---------|
| `GetRawTxResult` | Raw transaction bytes with provider name |
| `GetMerklePathResult` | Merkle proof in TSC/BUMP format with optional header |
| `GetBeefResult` | BEEF data with txid, proof status, and error info |
| `PostBeefResult` | Broadcast result with per-txid status |
| `PostTxResultForTxid` | Single transaction broadcast result with double-spend detection |
| `GetUtxoStatusResult` | UTXO check with details list |
| `UtxoDetail` | Individual UTXO info (txid, index, satoshis, height) |
| `GetStatusForTxidsResult` | Batch transaction status check |
| `TxStatusDetail` | Single transaction status (unknown/known/mined with depth) |
| `GetScriptHashHistoryResult` | Transaction history for address |
| `ScriptHistoryItem` | Single history entry (txid, height) |
| `BlockHeader` | Parsed block header with all fields and `to_binary()` method |
| `BsvExchangeRate` | Cached exchange rate with `is_stale()` check |

## Call Tracking Types (collection.rs)

| Type | Purpose |
|------|---------|
| `ServiceCall` | Individual call metadata (timing, success, error) |
| `ServiceCallError` | Error details (message, code) |
| `ServiceToCall` | Service reference with call metadata for operations |
| `CallCounts` | Statistics (success/failure/error counts with time range) |
| `ProviderCallHistory` | Per-provider call history and statistics |
| `ServiceCallHistory` | Complete history for a service collection |
| `ServicesCallHistory` | Aggregated history across all service types |

## Provider Priority by Operation

The `Services` constructor sets up provider priority for each operation:

| Operation | Provider Order |
|-----------|----------------|
| `get_merkle_path` | WhatsOnChain -> Bitails |
| `get_raw_tx` | WhatsOnChain -> Bitails |
| `post_beef` | GorillaPool ARC -> TAAL ARC -> Bitails -> WhatsOnChain |
| `get_utxo_status` | WhatsOnChain |
| `get_status_for_txids` | WhatsOnChain -> Bitails |
| `get_script_hash_history` | WhatsOnChain -> Bitails |
| `get_height` | BHS (if configured) -> WhatsOnChain -> Bitails (not via ServiceCollection) |

Note: `get_height` uses direct failover (not ServiceCollection-based) trying BHS first, then WhatsOnChain's `get_chain_info()`, then Bitails' `current_height()`.

## Providers

### WhatsOnChain (providers/whatsonchain.rs)

Primary data provider for most operations:
- **Mainnet:** `https://api.whatsonchain.com/v1/bsv/main`
- **Testnet:** `https://api.whatsonchain.com/v1/bsv/test`
- Optional API key for rate limit bypass
- Auto-retry on 429 (rate limited)
- Exchange rate caching
- Chain info for height queries

### ARC (providers/arc.rs)

Transaction broadcast service (mAPI):
- **TAAL Mainnet:** `https://arc.taal.com`
- **TAAL Testnet:** `https://arc-test.taal.com`
- **GorillaPool:** `https://arc.gorillapool.io`
- Native BEEF v1 support with automatic V2-to-V1 conversion
- Callback URLs for proof delivery
- Double-spend detection
- Merkle path retrieval (implements `MerklePathService` via `get_tx_data`)

### Bitails (providers/bitails.rs)

Alternative provider:
- **Mainnet:** `https://api.bitails.io/`
- **Testnet:** `https://test-api.bitails.io/`
- TSC merkle proofs
- Multi-transaction broadcast
- Script hash history
- Block header by hash and by height
- Current height endpoint

### BlockHeaderService (providers/bhs.rs)

Dedicated block header lookup service:
- **Mainnet:** `https://bhs.babbage.systems`
- **Testnet:** `https://bhs-test.babbage.systems`
- Current chain height
- Header by height
- Merkle root validation
- Chain tip header
- Optional Bearer token authentication

## Helper Functions (traits.rs)

```rust
pub fn sha256(data: &[u8]) -> Vec<u8>;
pub fn double_sha256(data: &[u8]) -> Vec<u8>;
pub fn txid_from_raw_tx(raw_tx: &[u8]) -> String;
pub fn validate_txid(raw_tx: &[u8], expected: &str) -> Result<()>;
pub fn validate_script_hash(hash: &str) -> Result<()>;
pub fn convert_script_hash(output: &str, format: Option<GetUtxoStatusOutputFormat>) -> Result<String>;
```

## PostBeefMode (services.rs:26)

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
- `Error::InvalidArgument` - Invalid input parameter

## Call History Tracking

Each `ServiceCollection` tracks:
- Recent calls (up to 32) with timing and outcomes
- Total counts (success/failure/error) since creation
- Reset interval counts for monitoring windows
- Provider-specific statistics

Constants:
- `MAX_CALL_HISTORY = 32` - Maximum recent calls per provider
- `MAX_RESET_COUNTS = 32` - Maximum reset intervals to keep

Use `get_services_call_history(reset)` to retrieve and optionally reset counters.

## Internal Service Traits (services.rs)

The `Services` struct uses internal traits to abstract provider capabilities:

```rust
trait MerklePathService { async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult>; }
trait RawTxService { async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult>; }
trait PostBeefService { async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult>; }
trait UtxoStatusService { async fn get_utxo_status(...) -> Result<GetUtxoStatusResult>; }
trait StatusForTxidsService { async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult>; }
trait ScriptHashHistoryService { async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult>; }
```

These are implemented by the provider types and used via type-erased `Arc<dyn Trait>` in service collections.

**Trait implementations by provider:**

| Trait | WhatsOnChain | ARC | Bitails |
|-------|:---:|:---:|:---:|
| `MerklePathService` | Y | Y | Y |
| `RawTxService` | Y | - | Y |
| `PostBeefService` | Y | Y | Y |
| `UtxoStatusService` | Y | - | - |
| `StatusForTxidsService` | Y | - | Y |
| `ScriptHashHistoryService` | Y | - | Y |

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent module overview
- [providers/CLAUDE.md](./providers/CLAUDE.md) - Provider implementation details
- [../chaintracks/CLAUDE.md](../chaintracks/CLAUDE.md) - Block header tracking (provides `Chain` type)
