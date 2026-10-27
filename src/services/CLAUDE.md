# src/services/ - Blockchain Services Layer
> Multi-provider blockchain API abstraction with automatic failover

## Overview

The services module provides a unified interface for interacting with BSV blockchain services. It coordinates multiple service providers (WhatsOnChain, ARC, Bitails, BHS) with automatic failover and call tracking. The system uses a collection-based pattern where each operation type maintains an ordered list of providers that are tried sequentially until one succeeds.

## Test and bench isolation (#182)

Every service HTTP client is imported from private `providers/http_client.rs`. Under
`cfg(test)` or `feature = "bench-support"`, it checks the built request URL
before execution and permits only `127.0.0.1`, `localhost`, and `::1`.
Other hosts return `Error::NetworkError` naming the host, #182, and rule 28.
Redirects pass the same check, proxy configuration is disabled, and
`localhost` resolution is pinned to loopback addresses. The guard covers
all providers, the explorer header fallback, Arcade SSE, and fiat rates.
Changing a provider URL or post mode cannot disable it.

Constructors can still describe production defaults in these builds, so
configuration and ordering tests keep their existing assertions. A request
to those defaults is refused before any connection. Use `MockWalletServices`
for pure wallet tests, or give the existing fixture seams loopback URLs.
The production configuration (neither test nor `bench-support`) re-exports
the original reqwest types and keeps their behavior and public API.

`network_isolation_tests.rs` captures a default request without sending it
and checks the URL policy and redirects. `network_post_tests.rs` exercises
both post modes, configured providers, and hostile proxy environments in
child processes. `tests/network_isolation_tests.rs` runs those witnesses
against the library compiled as an external dependency with `bench-support`.
Cargo does not set `cfg(test)` on that dependency, so integration isolation
coverage must enable the feature. Keep the lane's proxy prefix on every
`cargo test` as well.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Services                                 │
│  (Main orchestrator implementing WalletServices trait)          │
├─────────────────────────────────────────────────────────────────┤
│                    ServiceCollection<T>                          │
│  (Per-operation failover with call history + adaptive timeouts) │
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

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 183 | Module root with re-exports, `ServicesOptions` config, `Chain` re-export |
| `traits.rs` | 884 | `WalletServices` trait, `NLockTimeInput`, all result types, helper functions |
| `services.rs` | 1210 | `Services` struct implementing `WalletServices` with multi-provider orchestration |
| `collection.rs` | 762 | `ServiceCollection<S>` failover container with call history and adaptive timeouts |
| `mock.rs` | 1396 | `MockWalletServices` for testing with configurable responses and call tracking |
| `providers/` | - | Individual provider implementations (WhatsOnChain, ARC, Bitails, BHS) |

## Key Types

### Services (services.rs:71)

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

**Factory Methods (all return `Result<Self>`):**
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
- `get_utxo_leads(script_hash, format)` - The union of every explorer's unspent list for a script: leads for discovery, never a verdict (0.12.0)
- `get_status_for_txids(txids, use_next)` - Check confirmation status of transactions
- `get_script_hash_history(hash, use_next)` - Get transaction history for script (feature `break-glass-script-history`, off by default: a chain scan)
- `get_bsv_exchange_rate()` - Get cached USD/BSV rate (via WhatsOnChain)
- `get_fiat_exchange_rate(currency, base)` - Get fiat exchange rate (auto-refreshes from API)
- `get_height()` - Get current blockchain height (BHS -> WoC -> Bitails failover)
- `hash_to_header(hash)` - Get block header by hash from the Chaintracks header service; WoC -> Bitails only under `break_glass_explorer_headers` (off by default, every call logged at warn)
- `n_lock_time_is_final(n_lock_time)` - Check if raw nLockTime value allows mining
- `n_lock_time_is_final_for_tx(input)` - Check nLockTime finality with sequence info
- `get_services_call_history(reset)` - Get diagnostics for all service calls
- `set_post_beef_mode(mode)` - Configure broadcast behavior

**Provider Count Methods:**
- `get_merkle_path_count()` - Number of merkle path providers
- `get_raw_tx_count()` - Number of raw tx providers
- `post_beef_count()` - Number of post beef providers
- `get_utxo_status_count()` - Number of UTXO status providers

### WalletServices Trait (traits.rs:117)

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
    #[cfg(feature = "break-glass-script-history")]
    async fn get_script_hash_history(&self, hash: &str, use_next: bool) -> Result<GetScriptHashHistoryResult>;
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;
    async fn get_fiat_exchange_rate(&self, currency: FiatCurrency, base: Option<FiatCurrency>) -> Result<f64>;
    fn hash_output_script(&self, script: &[u8]) -> String;
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> UtxoVerdict; // UnspentHint (two explorers say unspent, no bound spender) | Spent (by proof only) | SpentHint (two explorers agree, unproven) | Unknown (could not look)
    async fn is_utxo_with_spender(&self, txid: &str, vout: u32, locking_script: &[u8]) -> (UtxoVerdict, Option<BoundSpender>); // the same pass, the spender spend_evidence bound by bytes { txid, confirmed } beside the verdict (#44); a named spender its bytes cannot bear out is (Unknown, None), and the binding needs no header service: bytes alone give (SpentHint, Some { confirmed: false }) (#96); default None
    async fn get_utxo_leads(&self, script_hash: &str, format: Option<GetUtxoStatusOutputFormat>) -> Result<UtxoLeads>; // leads, not a verdict; `partial` names a list cut short (10 pages of 1000 an explorer); default Err(NoExplorerAnswered)
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool>;
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool>;
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult>;
    fn get_services_call_history(&self, _reset: bool) -> ServicesCallHistory { .. } // default impl
}
```

**`use_next` Parameter:** Several methods accept a `use_next: bool` parameter. When `true`, the service collection skips to the next provider before starting the failover cycle. This is useful for retrying with alternate providers when the current one returned incomplete results.

### NLockTimeInput (traits.rs:28)

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

### ServiceCollection (collection.rs:47)

Generic failover container maintaining ordered providers with adaptive timeout support:

```rust
pub struct ServiceCollection<S> {
    pub service_name: String,
    services: Vec<NamedService<S>>,
    index: usize,
    since: DateTime<Utc>,
    history_by_provider: HashMap<String, ProviderCallHistoryInternal>,
    timeout_config: AdaptiveTimeoutConfig,
    avg_response_ms: AtomicU64,  // EMA of response times
}
```

**Key Methods:**
- `new(name)` - Create new collection with default timeout config
- `with_timeout_config(name, config)` - Create with custom adaptive timeout
- `add(name, service)` / `with(name, service)` - Add provider to collection
- `remove(name)` - Remove provider by name
- `count()` / `is_empty()` - Collection size queries
- `current_name()` / `current_service()` - Get current provider
- `next()` - Advance to next provider (wraps around)
- `reset()` - Return to first provider
- `move_to_last(name)` - De-prioritize failing provider
- `service_to_call()` / `get_service_to_call(index)` - Get service with call metadata
- `all_services_to_call()` - Get all services for parallel operations
- `all_services_owned()` - Get owned copies (avoids lock contention, requires `S: Clone`)
- `all_services_from_current()` - Get services in round-robin order from current index (requires `S: Clone`)
- `clone_collection()` - Clone with fresh history (requires `S: Clone`)
- `add_call_success/failure/error(provider, call)` - Record call outcome
- `get_call_history(reset)` - Get statistics, optionally reset counters

**Adaptive Timeout Methods:**
- `timeout_config()` / `set_timeout_config(config)` - Get/set timeout config
- `get_current_timeout()` - Compute timeout from EMA (clamped to min/max)
- `record_response_time(elapsed_ms)` - Update EMA (0.7 old + 0.3 new)
- `avg_response_ms()` - Get current average response time (None if no data)

### AdaptiveTimeoutConfig (collection.rs:21)

Configuration for dynamic timeout adjustment based on response times:

```rust
pub struct AdaptiveTimeoutConfig {
    pub min_timeout_ms: u64,       // Default: 5,000
    pub max_timeout_ms: u64,       // Default: 60,000
    pub multiplier: f64,           // Default: 2.0
    pub initial_timeout_ms: u64,   // Default: 30,000
}
```

Timeout = clamp(avg_response_ms * multiplier, min, max). Uses `initial_timeout_ms` when no response data is available.

### SharedServiceCollection (collection.rs:587)

Thread-safe wrapper for concurrent access:

```rust
pub struct SharedServiceCollection<S>(pub Arc<RwLock<ServiceCollection<S>>>);
```

Methods: `new(collection)`, `read()` -> `Result<RwLockReadGuard>`, `write()` -> `Result<RwLockWriteGuard>`. Uses `lock_utils::lock_read/lock_write` for safe locking. Implements `Clone`.

### ServicesOptions (mod.rs:61)

Configuration for service providers:

```rust
pub struct ServicesOptions {
    pub whatsonchain_api_key: Option<String>,
    pub bitails_api_key: Option<String>,
    pub bananablocks_base: Option<String>, // None = the chain's default, "" = off
    pub arc_url: String,
    pub arc_config: Option<ArcConfig>,
    pub arc_gorillapool_url: Option<String>,
    pub arc_gorillapool_config: Option<ArcConfig>,
    pub bhs_url: Option<String>,
    pub bhs_api_key: Option<String>,
    pub bsv_update_msecs: u64,       // 15 min default
    pub fiat_update_msecs: u64,      // 24 hour default
    pub fiat_exchange_rates: FiatExchangeRates,
    pub timeout_config: AdaptiveTimeoutConfig,
}
```

**Builder Methods:**
- `ServicesOptions::mainnet()` / `testnet()` - Create with defaults
- `with_woc_api_key(key)` - Set WhatsOnChain API key
- `with_bitails_api_key(key)` - Set Bitails API key
- `with_bananablocks_base(url)` - BananaBlocks API base (default per chain; empty = off)
- `without_bananablocks()` - Leave BananaBlocks out of every ladder
- `with_arc(url, config)` - Set TAAL ARC configuration
- `with_gorillapool(url, config)` - Set GorillaPool ARC configuration
- `with_bhs_url(url)` - Set Block Header Service URL
- `with_bhs_api_key(key)` - Set Block Header Service API key
- `with_bhs(url, api_key)` - Set BHS URL and API key together
- `with_timeout_config(config)` - Set adaptive timeout configuration

### FiatCurrency (traits.rs:701)

Supported fiat currencies for exchange rate conversions:

```rust
pub enum FiatCurrency { USD, GBP, EUR }
```

**Methods:**
- `FiatCurrency::parse(s)` - Parse from string (case-insensitive)
- `as_str()` - Get currency code as string
- Implements `FromStr` and `Display`

### FiatExchangeRates (traits.rs:747)

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

## MockWalletServices (mock.rs:161)

Full `WalletServices` implementation for testing with configurable responses, call tracking, and sequence support.

```rust
pub struct MockWalletServices {
    chain_tracker: MockChainTracker,
    height: u32,
    post_beef_response: Mutex<MockResponse<Vec<PostBeefResult>>>,
    get_raw_tx_response: Mutex<MockResponse<GetRawTxResult>>,
    get_merkle_path_response: Mutex<MockResponse<GetMerklePathResult>>,
    // ... responses for each method
    call_history: Mutex<Vec<MockCallRecord>>,
    call_counts: Mutex<HashMap<String, usize>>,
}
```

**Construction:**
- `MockWalletServices::new()` - All-success defaults (height=880,000)
- `MockWalletServices::builder()` - Builder pattern for custom responses

**Builder Methods (MockWalletServicesBuilder):**
- `height(u32)` - Set blockchain height
- `post_beef_success()` / `post_beef_network_error(msg)` / `post_beef_double_spend(txid, competing)` / `post_beef_already_known(txid)` / `post_beef_service_unavailable()` / `post_beef_rate_limited()` - Preset post_beef responses
- `post_beef_response(MockResponse)` / `get_raw_tx_response(MockResponse)` / `get_merkle_path_response(MockResponse)` / `get_utxo_status_response(MockResponse)` / `get_status_for_txids_response(MockResponse)` / `get_script_hash_history_response(MockResponse)` - Custom responses
- `get_raw_tx_error(kind, msg)` - Shorthand for error response

**Call Inspection:**
- `call_history()` - Get all `MockCallRecord` entries
- `call_count(method)` - Count calls to a specific method
- `total_calls()` - Total calls across all methods
- `reset_history()` - Clear call records and counters

**MockResponse Enum:**
- `MockResponse::Success(T)` - Return value directly
- `MockResponse::Error(MockErrorKind, String)` - Return an error
- `MockResponse::Sequence(Vec<MockResponse<T>>)` - Ordered responses; repeats last when exhausted

**MockErrorKind:** `NetworkError`, `ServiceError`, `BroadcastFailed`, `NoServicesAvailable`, `ValidationError`, `InvalidArgument`, `NotFound`, `TransactionError`, `Internal`

**Helper Constructors:**
- `success_post_beef_result(name, txids)` - Create successful PostBeefResult
- `error_post_beef_result(name, error_msg)` - Create failed PostBeefResult
- `double_spend_post_beef_result(name, txid, competing_tx)` - Create double-spend result
- `already_known_post_beef_result(name, txid)` - Create "already known" result

## Result Types (traits.rs)

| Type | Purpose |
|------|---------|
| `GetRawTxResult` | Raw transaction bytes with provider name |
| `GetMerklePathResult` | Merkle proof in TSC/BUMP format with optional header |
| `GetBeefResult` | BEEF data with txid, proof status, and error info |
| `PostBeefResult` | Broadcast result with per-txid status (`is_success()` helper) |
| `PostTxResultForTxid` | Single transaction broadcast result with double-spend detection (`is_success()` helper) |
| `GetUtxoStatusResult` | UTXO check with details list |
| `UtxoDetail` | Individual UTXO info (txid, index, satoshis, height) |
| `GetStatusForTxidsResult` | Batch transaction status check |
| `TxStatusDetail` | Single transaction status (unknown/known/mined with depth) |
| `GetScriptHashHistoryResult` | Transaction history for address |
| `ScriptHistoryItem` | Single history entry (txid, height) |
| `BlockHeader` | Parsed block header with all fields and `to_binary()` method |
| `BsvExchangeRate` | Cached exchange rate with `is_stale()` check |
| `GetUtxoStatusOutputFormat` | Enum: `HashLE` (default), `HashBE`, `Script` |

## Call Tracking Types (collection.rs)

| Type | Purpose |
|------|---------|
| `ServiceCall` | Individual call metadata (timing, success, error) |
| `ServiceCallError` | Error details (message, code) |
| `ServiceToCall` | Service reference with call metadata for operations |
| `CallCounts` | Statistics (success/failure/error counts with time range) |
| `ProviderCallHistory` | Per-provider call history and statistics |
| `ServiceCallHistory` | Complete history for a service collection |
| `ServicesCallHistory` | Aggregated history across all service types (traits.rs:91) |

## Provider Priority by Operation

The `Services` constructor sets up provider priority for each operation:

| Operation | Provider Order |
|-----------|----------------|
| `get_merkle_path` | ArcadeV2 (opt-in, first) -> WhatsOnChain -> BananaBlocks -> Bitails |
| `get_raw_tx` | WhatsOnChain -> BananaBlocks -> Bitails |
| `post_beef` | ArcadeV2 (opt-in, first) -> TAAL ARC -> GorillaPool ARC -> Bitails -> WhatsOnChain; with a `BroadcastMemory` attached, the last accepting provider is tried first (never ahead of Arcade unless it is Arcade) |
| `get_utxo_status` | WhatsOnChain -> BananaBlocks -> Bitails, a rotating start, asked until two agree or none is left; an unspent only from two, a not-listed only from two, anything else `status: "error"` (could not look); BananaBlocks answers an outpoint only (not asked a script's question), and its `spent: false` counts only for an output whose script is the asked one |
| `get_status_for_txids` | ArcadeV2 (opt-in, first) -> WhatsOnChain -> Bitails -> BananaBlocks (not a chain index: its index lags the mempool); each provider after the first is asked about every txid no earlier provider placed as `mined` (`unknown` is a gap, not a verdict, and a broadcaster's `known` is its word, not the chain's: a chain index's `mined` or `known` replaces it); absence stands only when two chain indexes (WhatsOnChain and Bitails) both answer `unknown`, and then it stands even against a broadcaster's `known` (an Arcade-held phantom, which the reconciler's alive check acts on; the respend gate's own broadcaster check refuses while Arcade still answers SEEN); one chain index's `unknown` never stands alone, as a clean answer or against any provider's `known`, in either order; every other `unknown` (BananaBlocks' 404 included) is left out and the answer's `error` says `could not look: no chain index answered <txids>` |
| `get_script_hash_history` | WhatsOnChain -> Bitails (feature `break-glass-script-history`, off by default: a chain scan) |
| `get_height` | Chaintracks tip header -> BHS -> error; no explorer (not via ServiceCollection) |

**The ladder's rule (0.12.0; the owner's word of 2026-10-22, bsv-wallet-cli#50).** Every chain question that leaves our services asks WhatsOnChain, BananaBlocks and Bitails as each other's fallbacks, because each scanner has its own rate limits. A fault (a 429, a timeout, a 5xx, a malformed body, bytes that do not hash to the txid) is "could not look": it moves the question to the next rung within the same call, before any retry round, and is never read as "unspent" or "absent". A negative, "unspent" as much as "not listed" or "absent", needs a second provider's clean answer (`UTXO_POSITIVE_PROVIDERS = UTXO_NEGATIVE_PROVIDERS = 2`: two of the three), because the callers that act on an unspent (`un_fail`, `poisoned_chain`, `locked_inputs`, bsv-wallet-cli's `probe_input_spend`) restore or release coins; a spender named and bound by its own bytes stands on one, and `is_utxo` asks for one even after two unspent answers. No provider sleeps on a 429: it is `Error::RateLimited` at once, carrying the Retry-After, recorded in the call history with code `RATE_LIMITED`, and that rung is skipped without a request until Retry-After passes (the utxo ladder's retry round, the only one, re-asks only a transient fault: a 5xx, a 408, a timeout or a transport failure, never a 429, a 404 that is an answer, a malformed body or a script mismatch; `FaultKind`). Rate gates are the process's, one per host and key (`RateGate::shared`), so several `Services` in one process share one budget; Bitails has one too (0.12.0, a 429 is never slept on; not exposed, unlike WhatsOnChain's and BananaBlocks' `rate_gate()`). BananaBlocks paces itself per process with a token bucket, a burst of 30 refilled at 0.5/s, so 60 in the first minute and 30 a minute sustained, under BananaBlocks' 80; `ServicesOptions::with_bananablocks_base(url)` moves it, `without_bananablocks()` (or an empty base) turns it off. `spend_evidence` asks WhatsOnChain's `/tx/{txid}/{vout}/spent` and then BananaBlocks' `/txo/{txid}/{vout}` `spent_txid`, each name bound by the spender's own bytes; a `/txo` the utxo ladder already read is reused, not asked again, so an unspent verdict costs one BananaBlocks request when it answers; after a fault `spend_evidence` asks it once more (up to three with a 5xx retried in the ladder). WhatsOnChain's and Bitails' unspent-set reads report a body cut short as `NetworkError` (transient) and a malformed body as `ServiceError` (deterministic).

**Leads vs verdicts (0.12.0, bsv-wallet-cli's `sync`, its review's M1).** `get_utxo_status` and `is_utxo` give a VERDICT ("this output is unspent"), which callers act on by restoring or releasing coins, so an unspent needs two explorers' clean answers and a gap is "could not look". `get_utxo_leads(script_hash, format)` answers a different question, discovery: which outputs might pay this script. A lead (`UtxoLead { txid, vout, satoshis, listed_by }`) is acted on only after the caller verifies the output by its transaction's bytes and its proof against the header service (bsv-wallet-cli's receive does), so one explorer's list is enough to look and the union of every explorer's list, keyed by outpoint, is the fullest look. Every lead source is asked through its shared rate gate, every page of it (WhatsOnChain `/script/{hash}/unspent/all`, then `?token=` its `nextPageToken`; Bitails `/scripthash/{hash}/unspent?limit=1000&from=N`, read on while a page is full; and BananaBlocks `/address/{address}/utxos?limit=1000`, then `&cursor=` while `hasMore`, only when the script is the exact P2PKH template `76a914<20>88ac` handed as itself, `GetUtxoStatusOutputFormat::Script`: BananaBlocks lists by address, never by script hash, and an item whose `script_type` is given and is not `p2pkh` is left out). The limits: 1000 outputs a page, at most `LEAD_PAGES_MAX` = 10 pages (10,000 outputs) an explorer; an explorer cut short (the cap reached while it had more, or a later page that could not be read: a transport fault, a 429, a status) keeps the pages it gave, stays in `answered` and is named in `partial`. `answered` means "answered", never "listed everything"; `partial` means "cut short": an output past its pages may exist. A list holding a txid that is not 64 hex characters, on any page, is that explorer's malformed answer (a deterministic fault, none of its leads taken), as is a later page that was read and does not parse; a fault moves on at once and is recorded in `faults` (a transient one is asked once more after the 2 s retry round; a 429 or a deterministic one is not); `answered` names who answered (an empty list is an answer); `Error::NoExplorerAnswered { faults }` ("Could not look: no explorer answered: ...") only when none did. A lead is never `UtxoVerdict::UnspentHint`, and nothing in the crate stores, releases or spends on one.

Note: `get_height` asks the header service only (Rule 28): the Chaintracks tip header's height, then BHS, then an error. No explorer is asked.

Note: when Arcade is configured (`ServicesOptions::with_arcade`), a wallet's
own broadcaster is also its proof and status source. Arcade's `GET /tx/{txid}`
answers `MINED` with `merklePath`, `blockHeight` and `blockHash`, so ONE call
per txid serves both the batch triage and the proof, and the third-party
indexers are only reached for what Arcade cannot answer. Ordering is unchanged
when Arcade is not configured.

## Providers

### WhatsOnChain (providers/whatsonchain.rs)

Primary data provider for most operations:
- **Mainnet:** `https://api.whatsonchain.com/v1/bsv/main`
- **Testnet:** `https://api.whatsonchain.com/v1/bsv/test`
- Optional API key for rate limit bypass (sent as `woc-api-key` and `Authorization`)
- A 429 is `Error::RateLimited` at once, never slept on; the rung is skipped until Retry-After passes
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
- One status verdict (`arc_status_verdict`) for the single submit, a batch
  item and an ancestor's status: REJECTED / INVALID / MALFORMED definitive,
  DOUBLE_SPEND_ATTEMPTED a double spend, any ORPHAN word an orphan wait,
  MINED_IN_STALE_BLOCK and an undefined word transient, every other ARC or
  Arcade word accepted; a 2xx never decides by itself

### Arcade V2 (providers/arcade.rs)

Teranode broadcaster, and (when configured) the wallet's first-party read
path:
- EF-only submit, always-async, per-token SSE stream (see the module docs)
- One judgment of a status word, `arcade_verdict`, for the submit answer,
  the status document and the SSE frame / webhook body alike (arcade@1ae1208
  `models/transaction.go:89-114`, all twelve words, any letter case):
  REJECTED, DOUBLE_SPEND_ATTEMPTED and any ORPHAN word fail; every other
  defined word is accepted as its hint; an undefined word is an invalid
  answer (transient). HTTP 400 with body `status` 476 (non-final) is
  transient; `reorg_unmined` / `reorg_reanchor` (`arcade_reorg_marker`) are
  re-asks, never a status change by themselves. Pinned by
  `tests/vectors/arcade_status_verdicts.json`
- `GET /tx/{txid}` status document: `txStatus` plus, on `MINED` /
  `IMMUTABLE`, `merklePath` (BUMP), `blockHeight` and `blockHash`
  (arcade >= v0.10.1)
- Merkle path retrieval (implements `MerklePathService`): a `MINED` or
  `IMMUTABLE` document serves the proof; anything else is a soft "no proof
  yet" so the collection moves on
- Batch triage (implements `StatusForTxidsService`): one `GET /tx/{txid}` per
  txid, `ARCADE_STATUS_CONCURRENCY` in flight, the proof carried on each
  `mined` detail. A 404 or a single failed call is reported `unknown`, never a
  failed batch; only a batch where every call failed comes back as an error
- Reads send the same auth as submits (`X-CallbackToken` plus configured
  headers)

### Bitails (providers/bitails.rs)

Alternative provider:
- **Mainnet:** `https://api.bitails.io/`
- **Testnet:** `https://test-api.bitails.io/`
- TSC merkle proofs
- Multi-transaction broadcast
- Script hash history
- Block header by hash and by height
- Current height endpoint

### BananaBlocks (providers/bananablocks.rs, 0.12.0)

GorillaPool's explorer, the third rung of the read ladders:
- **Mainnet:** `https://bananablocks.com/api/v1`
- **Testnet:** `https://test.bananablocks.com/api/v1`
- Raw tx (`/tx/{txid}/hex`, bound to the txid), output and spend (`/txo/{txid}/{vout}`, bound to the asked script by its hash), TSC proof (`/tx/{txid}/proof`, target the merkle root), tx status (`/tx/{txid}`), an address's unspent list (`/address/{address}/utxos?limit=1000`, paged by `cursor`, leads only: `list_address_unspent`)
- No key; a token bucket per process, a burst of 30 refilled at 0.5/s, so 60 in the first minute and 30 a minute sustained, under BananaBlocks' 80; a 429 closes the rung for its Retry-After
- Probe notes (shapes, the mempool gap): `docs/providers/bananablocks.md`

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

The hashes are bsv-rs's (`bsv_rs::primitives::{sha256, sha256d}`); the crate keeps no copy.

```rust
pub fn txid_from_raw_tx(raw_tx: &[u8]) -> String;
pub fn validate_txid(raw_tx: &[u8], expected: &str) -> Result<()>;
pub fn validate_script_hash(hash: &str) -> Result<()>;
pub fn convert_script_hash(output: &str, format: Option<GetUtxoStatusOutputFormat>) -> Result<String>;
```

## PostBeefMode (services.rs:30)

Controls broadcast behavior:

```rust
pub enum PostBeefMode {
    UntilSuccess,  // Default: try providers until one succeeds
    PromiseAll,    // Broadcast to all providers in parallel
}
```

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

| Trait | WhatsOnChain | ARC | Arcade | Bitails | BananaBlocks |
|-------|:---:|:---:|:---:|:---:|:---:|
| `MerklePathService` | Y | Y | Y | Y | Y |
| `RawTxService` | Y | - | - | Y | Y |
| `PostBeefService` | Y | Y | Y | Y | - |
| `UtxoStatusService` | Y | - | - | Y | Y (outpoint only) |
| `StatusForTxidsService` | Y | - | Y | Y | Y |
| `ScriptHashHistoryService` | Y | - | - | Y | - |

## Error Handling

All methods return `Result<T>` with these error variants:
- `Error::NoServicesAvailable` - No providers configured for operation
- `Error::RateLimited { provider, retry_after_secs }` - A provider's 429 (or its own pace): a fault, never an answer
- `Error::NetworkError` - HTTP request failed
- `Error::ServiceError` - Provider returned error
- `Error::BroadcastFailed` - Transaction broadcast explicitly failed
- `Error::ValidationError` - Invalid txid, script hash, etc.
- `Error::NotFound` - Resource not found (block header, etc.)
- `Error::InvalidArgument` - Invalid input parameter
- `Error::TransactionError` - Transaction parsing/processing error
- `Error::Internal` - Internal error

## Call History Tracking

Each `ServiceCollection` tracks:
- Recent calls (up to 32) with timing and outcomes
- Total counts (success/failure/error) since creation
- Reset interval counts for monitoring windows
- Provider-specific statistics
- Adaptive timeout EMA for response times

Constants:
- `MAX_CALL_HISTORY = 32` - Maximum recent calls per provider
- `MAX_RESET_COUNTS = 32` - Maximum reset intervals to keep

Use `get_services_call_history(reset)` to retrieve and optionally reset counters.

## Tests

- `services.rs`: 5 unit tests (creation, options, fiat rates, currency parsing)
- `collection.rs`: 11 unit tests (basic operations, remove, move_to_last, call history, adaptive timeouts, EMA convergence)
- `mock.rs`: 15 integration tests (failover, double-spend, sequences, error categorization, call tracking)
- Bench seam: `benchmark_support::local_explorer_services` builds the real ladders (`Services::with_explorers`) with every explorer at a fixture URL; it and the `with_base_url` constructors it uses outside tests compile only under the non-default `bench-support` feature.

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent module overview
- [providers/CLAUDE.md](./providers/CLAUDE.md) - Provider implementation details

## Broadcast Acceptance Memory (broadcast_memory.rs, 0.3.56)

`BroadcastMemory` (persisted by `StorageSqlx`, `InMemoryBroadcastMemory` for tests) remembers per
postBeef provider which txids it already accepted / saw / mined. `Services::post_beef` consults it
when one is attached (`WalletServices::set_broadcast_memory`; the `Wallet`, the `Monitor` and
`StorageSqlx::set_services` attach the storage's memory automatically):

- Classic ARC (`arc.rs`, `reduced_send_plan`): every unproven ancestor seen -> subject alone as EF
  (`/v1/tx`); some seen -> JSON EF batch of the unseen ancestors + subject (`/v1/txs`); none seen
  -> full BEEF, exactly as before. A reduced send refused for what reads as a missing parent
  (`missing_parent_hint`, orphan verdict, rejected ancestor, unsupported batch endpoint) is retried
  ONCE with the full BEEF. `469` bodies saying the verification timed out are transient.
- Arcade (`arcade.rs`, `beef_to_ef_batch_skipping`): seen unproven txs stay out of the EF batch
  (the subject is always sent); a missing-parent refusal / `REJECTED` retries once with the full batch.
- On success the accepting provider's txids are recorded (`accepted`) and it becomes the sticky
  provider (`broadcast_prefs.last_accepted_provider`). SSE/webhook seen reports record `seen` for
  the reporting plane; proofs record `mined` for `network` (counts for every provider).
- No memory attached = full package, static order: nothing changes for existing callers.
