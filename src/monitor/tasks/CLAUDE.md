# src/monitor/tasks/ - Background Monitor Tasks
> Scheduled background tasks for transaction lifecycle management

## Overview

This module defines the monitor task system that runs periodic background operations on wallet storage. Each task implements the `MonitorTask` trait and handles a specific aspect of wallet management: proof fetching, transaction broadcasting, status synchronization, blockchain monitoring, database maintenance, and error recovery. Tasks are designed to be run by the monitor daemon at configurable intervals.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Monitor Daemon                                      │
│               (Schedules and runs tasks at configured intervals)              │
├──────────────────────────────────────────────────────────────────────────────┤
│                           MonitorTask Trait                                   │
│              name() | default_interval() | setup() | run()                    │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────────┤
│ Blockchain   │ Transaction  │ Status &     │ Maintenance  │ Service          │
│ Monitoring   │ Processing   │ Recovery     │              │ Monitoring       │
├──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┤
│ NewHeader    │ SendWaiting  │ ReviewStatus │ Purge        │ MonitorCall      │
│ (60s)        │ (5min)       │ (15min)      │ (1hr)        │ History (12min)  │
│              │              │              │              │                  │
│ Clock        │ CheckFor     │ UnFail       │ FailAbandoned│ SyncWhenIdle     │
│ (1s)         │ Proofs (60s) │ (10min)      │ (5min)       │ (60s)            │
│              │              │              │              │                  │
│ Reorg        │ CheckNo      │              │              │                  │
│ (60s)        │ Sends (24hr) │              │              │                  │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────────┘
         │               │               │               │               │
         ▼               ▼               ▼               ▼               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  MonitorStorage                       │         WalletServices                │
│  (find_proven_tx_reqs, abort_abandoned│         (get_merkle_path, post_beef,  │
│   purge_data, review_status,          │          get_height, etc.)            │
│   send_waiting_transactions, un_fail, │                                       │
│   synchronize_transaction_statuses,   │                                       │
│   update_proven_tx_req_status)        │                                       │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 186 | Module root with `MonitorTask` trait, `TaskResult`, and `TaskType` enum (12 variants) |
| `check_for_proofs.rs` | 165 | `CheckForProofsTask` - fetches merkle proofs for unconfirmed transactions |
| `check_no_sends.rs` | 199 | `CheckNoSendsTask` - retrieves proofs for 'nosend' transactions |
| `clock.rs` | 113 | `ClockTask` - tracks minute-level clock events |
| `fail_abandoned.rs` | 91 | `FailAbandonedTask` - marks abandoned transactions as failed |
| `monitor_call_history.rs` | 181 | `MonitorCallHistoryTask<V>` - generic service call statistics logger |
| `new_header.rs` | 181 | `NewHeaderTask` - polls for new blockchain block headers |
| `purge.rs` | 154 | `PurgeTask` - database maintenance, deletes expired data |
| `reorg.rs` | 279 | `ReorgTask` - handles blockchain reorganizations with status demotion |
| `review_status.rs` | 96 | `ReviewStatusTask` - synchronizes transaction and proof status |
| `send_waiting.rs` | 138 | `SendWaitingTask` - broadcasts transactions waiting to be sent |
| `sync_when_idle.rs` | 177 | `SyncWhenIdleTask` - triggers sync after idle periods |
| `unfail.rs` | 95 | `UnfailTask` - recovers incorrectly failed transactions |

## Key Exports

### MonitorTask Trait (mod.rs:67-85)

The core trait that all monitor tasks implement:

```rust
#[async_trait]
pub trait MonitorTask: Send + Sync {
    /// Get the task name (e.g., "check_for_proofs").
    fn name(&self) -> &'static str;

    /// Get the default interval for this task.
    fn default_interval(&self) -> Duration;

    /// Optional async setup phase called before first run.
    /// Override for initialization that requires async operations
    /// (e.g., loading state from storage). Default is a no-op.
    async fn setup(&self) -> Result<()> { Ok(()) }

    /// Run the task once, returning processed count and any errors.
    async fn run(&self) -> Result<TaskResult>;
}
```

### TaskResult (mod.rs:38-64)

Result structure returned by task execution. Derives `Default`.

```rust
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    pub items_processed: u32,
    pub errors: Vec<String>,
}
```

**Methods:**
- `TaskResult::new()` - Create empty result (same as `Default::default()`)
- `TaskResult::with_count(n)` - Create result with processed count
- `add_error(msg)` - Record a non-fatal error

### TaskType Enum (mod.rs:88-114)

Identifies task types for scheduling and configuration. Implements `Display` and has an `as_str()` method.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
    CheckForProofs,      // "check_for_proofs"
    SendWaiting,         // "send_waiting"
    FailAbandoned,       // "fail_abandoned"
    UnFail,              // "unfail"
    Clock,               // "clock"
    CheckNoSends,        // "check_no_sends"
    MonitorCallHistory,  // "monitor_call_history"
    NewHeader,           // "new_header"
    Purge,               // "purge"
    Reorg,               // "reorg"
    ReviewStatus,        // "review_status"
    SyncWhenIdle,        // "sync_when_idle"
}
```

**Methods:**
- `as_str() -> &'static str` - Get the task name as a string
- `fmt::Display` - Delegates to `as_str()`

### Task Implementations

| Task | Export | Default Interval | Category |
|------|--------|------------------|----------|
| `CheckForProofsTask<S, V>` | `check_for_proofs::CheckForProofsTask` | 60 seconds | Transaction |
| `CheckNoSendsTask<S, V>` | `check_no_sends::CheckNoSendsTask` | 24 hours | Transaction |
| `ClockTask` | `clock::ClockTask` | 1 second | Blockchain |
| `FailAbandonedTask<S>` | `fail_abandoned::FailAbandonedTask` | 5 minutes | Maintenance |
| `MonitorCallHistoryTask<V>` | `monitor_call_history::MonitorCallHistoryTask` | 12 minutes | Service |
| `NewHeaderTask<V>` | `new_header::NewHeaderTask` | 60 seconds | Blockchain |
| `PurgeTask<S>` | `purge::PurgeTask` | 1 hour | Maintenance |
| `ReorgTask<S, V>` | `reorg::ReorgTask` | 60 seconds | Blockchain |
| `ReviewStatusTask<S>` | `review_status::ReviewStatusTask` | 15 minutes | Status |
| `SendWaitingTask<S, V>` | `send_waiting::SendWaitingTask` | 5 minutes | Transaction |
| `UnfailTask<S, V>` | `unfail::UnfailTask` | 10 minutes | Status |
| `SyncWhenIdleTask` | `sync_when_idle::SyncWhenIdleTask` | 60 seconds | Service |

### Additional Exports

```rust
// Purge configuration
pub use purge::PurgeConfig;

// Reorg helper types
pub use reorg::DeactivatedHeader;
```

## Task Details

### Blockchain Monitoring Tasks

#### NewHeaderTask (new_header.rs)

Polls for new blockchain block headers and triggers proof checking.

**Type Parameters:**
- `V: WalletServices` - Service provider for chain height lookups

**Constructor:**
```rust
NewHeaderTask::new(services: Arc<V>) -> Self
```

**State:**
- `last_height: AtomicU32` - Last known chain height
- `last_hash: RwLock<Option<String>>` - Last known chain tip hash (`#[allow(dead_code)]`)
- `stable_cycles: AtomicU32` - Consecutive cycles without new headers
- `new_header_received: AtomicBool` - Flag for other tasks to check

**Public Methods:**
- `has_new_header()` - Check if new header detected since last check
- `clear_new_header_flag()` - Reset the new header flag
- `last_known_height()` - Get the last recorded height

**Behavior:**
1. Calls `services.get_height()` to get current chain height
2. On first run: records initial height
3. If height increased: sets `new_header_received` flag, logs blocks ahead
4. If height decreased: logs potential reorg warning, records as error
5. If same height: increments stable cycle counter

#### ClockTask (clock.rs)

Tracks minute-level clock events for scheduling coordination.

**Constructor:**
```rust
ClockTask::new() -> Self
ClockTask::default() -> Self  // Implements Default
```

**State:**
- `last_minute: AtomicU64` - Last recorded minute since Unix epoch

**Behavior:**
1. Calculates current minute since epoch (`SystemTime / 60`)
2. On first run: records current minute
3. If minute boundary crossed: logs elapsed minutes, returns count
4. Otherwise: returns zero items processed

#### ReorgTask (reorg.rs)

Handles blockchain reorganizations by re-verifying affected proofs.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend
- `V: WalletServices` - Service provider for proof verification

**Constructor:**
```rust
ReorgTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Constants:**
- `MAX_RETRY_COUNT: u32 = 3` - Maximum reprocessing attempts
- `REORG_PROCESS_DELAY_SECS: i64 = 600` - 10 minute delay before processing

**Additional Export:**
```rust
pub struct DeactivatedHeader {
    pub hash: String,
    pub height: u32,
    pub deactivated_at: DateTime<Utc>,
    pub retry_count: u32,
}
```

**Public Methods:**
- `queue_deactivated_header(hash, height)` - Queue a header for processing (async)
- `pending_count()` - Get number of pending headers (async)

**Behavior:**
1. Processes deactivated headers after 10-minute delay (avoids temporary forks)
2. Queries `Completed` and `Unmined` proven_tx_reqs for potentially affected transactions
3. Re-verifies merkle proofs via `services.get_merkle_path()`
4. If proof no longer valid: demotes proven_tx_req status to `Unmined` via `storage.update_proven_tx_req_status()` so `CheckForProofsTask` will re-fetch the proof
5. Retries up to 3 times, resetting the delay between attempts

### Transaction Processing Tasks

#### CheckForProofsTask (check_for_proofs.rs)

Fetches merkle proofs for transactions that have been broadcast but not yet confirmed.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend
- `V: WalletServices` - Service provider for merkle path lookups

**Constructor:**
```rust
CheckForProofsTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Behavior:**
1. Queries `proven_tx_reqs` with status: `Unmined`, `Unknown`, `Callback`, `Sending`, or `Unconfirmed`
2. For each transaction, calls `services.get_merkle_path(txid, false)`
3. On success with proof: logs success, increments processed count
4. On no proof: logs debug message (will retry next cycle)
5. On error: records error in `TaskResult.errors`, continues to next txid

#### CheckNoSendsTask (check_no_sends.rs)

Retrieves proofs for 'nosend' transactions that may have been mined externally.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend
- `V: WalletServices` - Service provider for merkle path lookups

**Constructor:**
```rust
CheckNoSendsTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**State:**
- `check_now: AtomicBool` - Flag to trigger immediate check

**Public Methods:**
- `trigger_check()` - Set flag for immediate check on next run

**Behavior:**
1. Resets `check_now` flag
2. Queries `proven_tx_reqs` with status: `NoSend`
3. For each transaction, checks for merkle proof via `get_merkle_path(txid, false)`
4. If proof found: logs success with block height/hash, increments processed count, calls `storage.synchronize_transaction_statuses()` to persist proof state (note: `synchronize_transaction_statuses` covers unmined/unknown/callback/sending/unconfirmed statuses but does not directly persist nosend proofs — full nosend proof persistence requires a dedicated `MonitorStorage` method)
5. If not found: logs debug, will retry on next daily cycle

#### SendWaitingTask (send_waiting.rs)

Broadcasts transactions that are ready to be sent to the network.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend
- `V: WalletServices` - Service provider for transaction broadcast

**Constructor:**
```rust
SendWaitingTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Constants:**
- `DEFAULT_MIN_AGE_SECS: u64 = 30` - Minimum age before sending (skipped on first run)

**State:**
- `min_age: Duration` - Minimum age filter (default 30s)
- `first_run: AtomicBool` - Tracks whether this is the first execution

**Behavior:**
1. On first run: uses zero-duration age filter for immediate processing
2. Delegates to `storage.send_waiting_transactions(min_age)`
3. Returns count from `send_with_results` in the response
4. On error: records error, returns zero items processed

### Status & Recovery Tasks

#### ReviewStatusTask (review_status.rs)

Synchronizes transaction status with ProvenTxReq status.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend

**Constructor:**
```rust
ReviewStatusTask::new(storage: Arc<S>) -> Self
```

**State:**
- `check_now: AtomicBool` - Flag to trigger immediate check

**Public Methods:**
- `trigger_check()` - Set flag for immediate check

**Behavior:**
1. Resets `check_now` flag
2. Delegates to `MonitorStorage::review_status()` which ensures consistency between transaction records and their associated proof requests
3. Logs the result log if non-empty

#### UnfailTask (unfail.rs)

Recovers transactions that were incorrectly marked as failed.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend
- `V: WalletServices` - Service provider for merkle path lookups

**Constructor:**
```rust
UnfailTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Behavior:**
1. Delegates to `storage.un_fail()` which handles the full unfail logic: querying unfail reqs, checking merkle paths, updating statuses
2. On error: records error in result

### Maintenance Tasks

#### PurgeTask (purge.rs)

Database maintenance that deletes transient/expired data.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend

**Configuration:**
```rust
pub struct PurgeConfig {
    pub purge_failed: bool,           // Whether to purge failed txs (default: true)
    pub purge_completed_data: bool,   // Whether to purge completed tx data (default: true)
    pub max_age_days: u32,            // Age threshold in days (default: 30)
}
```

**Constructor:**
```rust
PurgeTask::new(storage: Arc<S>) -> Self  // Uses default config
PurgeTask::with_config(storage: Arc<S>, config: PurgeConfig) -> Self
```

**State:**
- `check_now: AtomicBool` - Flag to trigger immediate purge

**Public Methods:**
- `trigger_purge()` - Set flag for immediate purge

**Behavior:**
1. Resets `check_now` flag
2. Builds `PurgeParams` from config (`max_age_days`, `purge_completed`, `purge_failed`)
3. Delegates to `MonitorStorage::purge_data(params)` which operates across all users
4. Returns `result.count` as items processed; logs `result.log` if any items purged

#### FailAbandonedTask (fail_abandoned.rs)

Marks abandoned transactions as failed to release locked UTXOs.

**Type Parameters:**
- `S: MonitorStorage` - Storage backend

**Constructor:**
```rust
FailAbandonedTask::new(storage: Arc<S>, timeout: Duration) -> Self
```

**Parameters:**
- `timeout: Duration` - How long before a transaction is considered abandoned

**Behavior:**
1. Delegates to `storage.abort_abandoned(timeout)` which queries transactions with status 'unsigned' or 'unprocessed' older than the timeout and calls `abort_action` for each
2. On error: records error in result

### Service Monitoring Tasks

#### MonitorCallHistoryTask (monitor_call_history.rs)

Logs service call statistics for monitoring and debugging.

**Type Parameters:**
- `V: WalletServices` - Service provider (works with any implementation, not just concrete `Services`)

**Constructor:**
```rust
MonitorCallHistoryTask::new(services: Arc<V>) -> Self
```

**Behavior:**
1. Calls `services.get_services_call_history(true)` (true = reset counters)
2. Logs per-provider statistics for each service type:
   - `get_merkle_path` - Proof lookups
   - `get_raw_tx` - Transaction retrieval
   - `post_beef` - Transaction broadcast
   - `get_utxo_status` - UTXO status checks
3. Tracks total_calls and total_errors across all providers
4. Returns total_calls as `items_processed`

#### SyncWhenIdleTask (sync_when_idle.rs)

Triggers storage synchronization after idle periods. Mirrors the TypeScript `TaskSyncWhenIdle` from `@bsv/wallet-toolbox`. Fully wired into `mod.rs` with `TaskType::SyncWhenIdle` variant.

**No generics** — standalone task with no storage or service dependencies.

**Constants:**
- `DEFAULT_IDLE_THRESHOLD_SECS: u64 = 120` - 2 minute idle threshold

**Constructor:**
```rust
SyncWhenIdleTask::new() -> Self               // Default 2-minute threshold
SyncWhenIdleTask::with_threshold(Duration) -> Self  // Custom threshold
SyncWhenIdleTask::default() -> Self           // Implements Default (same as new())
```

**State:**
- `last_activity: AtomicU64` - Unix timestamp of last recorded activity
- `idle_threshold: Duration` - How long wallet must be idle before sync

**Public Methods:**
- `notify_activity()` - Reset idle timer (call on wallet activity)
- `last_activity_timestamp()` - Get last activity time (seconds since epoch)

**Behavior:**
1. Checks if elapsed time since `last_activity` exceeds `idle_threshold`
2. If idle: returns `TaskResult::with_count(1)` signaling sync should occur
3. If active: returns empty result
4. Actual sync is performed by the Monitor daemon, not this task

## Transaction Status Flow

```
                    ┌──────────────────┐
                    │    Unsigned      │
                    └────────┬─────────┘
                             │ (signed)
                    ┌────────▼─────────┐
                    │   Unprocessed    │◄─────────────────┐
                    └────────┬─────────┘                  │
                             │ (processed)                │
              ┌──────────────┴──────────────┐             │
              ▼                             ▼             │
     ┌──────────────┐              ┌──────────────┐      │
     │    Unsent    │              │   NoSend     │      │
     └──────┬───────┘              └──────┬───────┘      │
            │ (SendWaiting)               │ (CheckNoSends)
     ┌──────▼───────┐                     │              │
     │   Sending    │                     │              │
     └──────┬───────┘                     │              │
            │ (broadcast success)         │              │
     ┌──────▼───────┐◄────────────────────┘              │
     │   Unmined    │                                    │
     └──────┬───────┘                                    │
            │ (CheckForProofs gets proof)                │
     ┌──────▼───────┐                                    │
     │  Unproven    │                                    │
     └──────┬───────┘                                    │
            │ (proof stored)                             │
     ┌──────▼───────┐         ┌──────────┐              │
     │  Completed   │         │  Failed  │──────────────┘
     └──────────────┘         └────┬─────┘  (UnfailTask recovers
            │                      │         if tx found on-chain)
            │                 (UnfailTask marks invalid
            │                  if tx not on-chain)
            │                      │
     ┌──────▼───────┐         ┌────▼─────┐
     │   (Purge)    │         │ Invalid  │
     │ removes data │         └────┬─────┘
     └──────────────┘              │
                              ┌────▼─────┐
                              │ (Purge)  │
                              │ deletes  │
                              └──────────┘
```

## ProvenTxReqStatus Values Used

| Task | Queries Statuses | Updates To |
|------|------------------|------------|
| `CheckForProofsTask` | `Unmined`, `Unknown`, `Callback`, `Sending`, `Unconfirmed` | `Completed` (on proof) |
| `CheckNoSendsTask` | `NoSend` | Calls `synchronize_transaction_statuses()` on proof discovery |
| `SendWaitingTask` | via `send_waiting_transactions()` | `Unmined` (success), `Failed` (double-spend) |
| `FailAbandonedTask` | via `abort_abandoned()` | Calls `abort_action` on stale unsigned/unprocessed txs |
| `UnfailTask` | via `un_fail()` | `Unmined` (on-chain), `Invalid` (not found) |
| `PurgeTask` | via `purge_data()` | Deletes/clears data |
| `ReviewStatusTask` | via `review_status()` | Syncs transaction status |
| `ReorgTask` | `Completed`, `Unmined` | Demotes to `Unmined` via `update_proven_tx_req_status()` if proof invalidated |

## Generic Type Constraints

Tasks use different type constraints based on their needs:

**Storage only (`MonitorStorage`):**
```rust
S: MonitorStorage + 'static
```
Used by: `FailAbandonedTask`, `PurgeTask`, `ReviewStatusTask`

**Both storage and services:**
```rust
S: MonitorStorage + 'static
V: WalletServices + 'static
```
Used by: `CheckForProofsTask`, `CheckNoSendsTask`, `ReorgTask`, `SendWaitingTask`, `UnfailTask`

**Services only (`WalletServices`):**
```rust
V: WalletServices + 'static
```
Used by: `NewHeaderTask`, `MonitorCallHistoryTask`

**No generics:**
- `ClockTask` - No external dependencies
- `SyncWhenIdleTask` - No external dependencies

## Tests

- `mod.rs`: 4 unit tests — `TaskResult` constructors and `TaskType::as_str()` for all 12 variants
- `check_for_proofs.rs`: 5 unit tests — `TaskResult` methods
- `check_no_sends.rs`: 2 unit tests — interval constants
- `clock.rs`: 3 tests (2 async) — first-run, same-minute, and default behavior
- `fail_abandoned.rs`: 2 unit tests — name and interval
- `monitor_call_history.rs`: 4 tests (1 async) — name, interval, run on fresh services, `ServicesCallHistory` default
- `new_header.rs`: 2 tests (1 async) — interval and task initialization with `Services::mainnet()`
- `purge.rs`: 2 unit tests — `PurgeConfig` defaults and interval
- `reorg.rs`: 4 unit tests — interval, `MAX_RETRY_COUNT`, delay constant, `DeactivatedHeader` struct
- `review_status.rs`: 1 unit test — interval
- `send_waiting.rs`: 3 unit tests — name, interval, `DEFAULT_MIN_AGE_SECS`
- `sync_when_idle.rs`: 5 tests (2 async) — name/interval, activity reset, idle/active behavior, defaults
- `unfail.rs`: 2 unit tests — name and interval

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent monitor module
- [../../CLAUDE.md](../../CLAUDE.md) - Source root documentation
- [../../storage/CLAUDE.md](../../storage/CLAUDE.md) - Storage layer and `MonitorStorage` trait
- [../../services/CLAUDE.md](../../services/CLAUDE.md) - Services layer and `WalletServices` trait
- [../../storage/entities/CLAUDE.md](../../storage/entities/CLAUDE.md) - Entity definitions including `ProvenTxReqStatus`
