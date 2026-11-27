# src/monitor/ - Background Task Scheduler
> Background daemon for monitoring and managing BSV wallet transaction lifecycle

## Overview

The monitor module provides a daemon-based task scheduler for running recurring background operations on wallet storage. It handles transaction lifecycle management including proof verification, transaction broadcasting, abandoned transaction cleanup, recovery of incorrectly failed transactions, blockchain reorganization handling, and database maintenance. The module is designed to run alongside a wallet instance, performing maintenance tasks at configurable intervals. It supports multi-instance deployments via distributed task locking.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Monitor<S, V>                            │
│  (S: MonitorStorage, V: WalletServices)                         │
├─────────────────────────────────────────────────────────────────┤
│  start() / stop() / run_once() / is_running() / health()        │
│  instance_id() - unique per Monitor for distributed locking     │
│  Manages task lifecycle, scheduling, and health tracking        │
├─────────────────────────────────────────────────────────────────┤
│                        TasksConfig (12 tasks)                    │
│  enabled | interval | start_immediately (per task)              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Core Tasks     │  Extended Tasks │  Maintenance Tasks          │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ CheckForProofs  │ CheckNoSends    │ Purge (1 hour)              │
│   (1 min)       │   (24 hours)    │ ReviewStatus (15 min)       │
│ SendWaiting     │ NewHeader       │ MonitorCallHistory (12 min) │
│   (5 min)       │   (1 min)       │ SyncWhenIdle (1 min)        │
│ FailAbandoned   │ Reorg           │                             │
│   (5 min)       │   (1 min)       │                             │
│ Unfail          │ Clock           │                             │
│   (10 min)      │   (1 sec)       │                             │
└─────────────────┴─────────────────┴─────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
              MonitorStorage         WalletServices
            (find_proven_tx_reqs,    (get_merkle_path,
             try_acquire_task_lock,   get_height,
             release_task_lock)       post_beef)
```

### Multi-Instance Support

Each `Monitor` generates a random 16-byte hex `instance_id` on creation. Before each task run, the daemon acquires a distributed task lock via `storage.try_acquire_task_lock(task_name, instance_id, ttl)`. The TTL is set to 2x the task interval so locks auto-expire if an instance crashes. After the task completes, the lock is released via `storage.release_task_lock()`.

### Callbacks

`MonitorOptions` supports optional event callbacks:
- `on_tx_broadcasted` - Invoked when a transaction has been broadcast
- `on_tx_proven` - Invoked when a transaction proof has been obtained

Both receive a `TransactionStatusUpdate` with txid, status, and optional proof data.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with re-exports of `Monitor`, `MonitorHealth`, `TaskHealth`, `MonitorOptions`, `TaskConfig`, `TransactionStatusUpdate`, `MonitorTask`, and `TaskResult` |
| `config.rs` | Configuration types: `MonitorOptions`, `TasksConfig`, `TaskConfig`, and `TransactionStatusUpdate` |
| `daemon.rs` | Main `Monitor` struct with health tracking, distributed locking, and background task execution via tokio |

## Submodules

| Submodule | Purpose |
|-----------|---------|
| `tasks/` | Individual task implementations, each implementing `MonitorTask` trait (12 task files + `mod.rs` with trait/types) |

## Key Types

### Monitor

The main daemon struct that schedules and runs background tasks.

```rust
pub struct Monitor<S, V>
where
    S: MonitorStorage + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    options: MonitorOptions,
    running: Arc<AtomicBool>,
    task_handles: RwLock<HashMap<TaskType, JoinHandle<()>>>,
    task_health: Arc<RwLock<HashMap<TaskType, TaskHealth>>>,
    instance_id: String,
}
```

**Methods:**
- `new(storage, services)` - Create with default options
- `with_options(storage, services, options)` - Create with custom configuration
- `start()` - Spawn all 12 enabled tasks as background tokio tasks
- `stop()` - Cancel all running tasks
- `is_running()` - Check daemon status
- `run_once()` - Execute all enabled tasks once (useful for testing)
- `health()` - Returns `MonitorHealth` snapshot with per-task health info
- `instance_id()` - Get the unique instance identifier

### MonitorHealth

Aggregate health status for the entire monitor daemon.

```rust
pub struct MonitorHealth {
    pub running: bool,
    pub task_count: usize,
    pub tasks: HashMap<TaskType, TaskHealth>,
}
```

**Methods:**
- `all_tasks_healthy()` - Returns `true` if all tasks have run at least once with zero consecutive errors

### TaskHealth

Health status for an individual monitor task.

```rust
pub struct TaskHealth {
    pub last_run: Option<Instant>,
    pub last_result: Option<TaskResult>,
    pub last_error: Option<String>,
    pub consecutive_errors: u32,
}
```

Implements `Default` (all `None`/zero). Updated after each task run: success resets `consecutive_errors` to 0, failure increments it.

### MonitorOptions

Configuration for the daemon.

```rust
pub struct MonitorOptions {
    pub tasks: TasksConfig,
    pub fail_abandoned_timeout: Duration,  // Default: 5 minutes
    pub on_tx_broadcasted: Option<Arc<dyn Fn(TransactionStatusUpdate) + Send + Sync>>,
    pub on_tx_proven: Option<Arc<dyn Fn(TransactionStatusUpdate) + Send + Sync>>,
}
```

Has a custom `Debug` impl that prints `"Some(<callback>)"` for non-None callbacks.

### TransactionStatusUpdate

Status update payload passed to monitor event callbacks.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStatusUpdate {
    pub txid: String,
    pub status: String,
    pub merkle_root: Option<String>,
    pub merkle_path: Option<String>,
    pub block_height: Option<u32>,
    pub block_hash: Option<String>,
}
```

### TasksConfig

Configuration for all 12 tasks with individual `TaskConfig` entries.

```rust
pub struct TasksConfig {
    pub check_for_proofs: TaskConfig,    // Default: 1 min, not immediate
    pub send_waiting: TaskConfig,         // Default: 5 min, starts immediately
    pub fail_abandoned: TaskConfig,       // Default: 5 min, not immediate
    pub unfail: TaskConfig,               // Default: 10 min, not immediate
    pub clock: TaskConfig,                // Default: 1 sec, starts immediately
    pub new_header: TaskConfig,           // Default: 1 min, not immediate
    pub reorg: TaskConfig,                // Default: 1 min, not immediate
    pub check_no_sends: TaskConfig,       // Default: 24 hours, not immediate
    pub review_status: TaskConfig,        // Default: 15 min, not immediate
    pub purge: TaskConfig,                // Default: 1 hour, not immediate
    pub monitor_call_history: TaskConfig, // Default: 12 min, not immediate
    pub sync_when_idle: TaskConfig,       // Default: 1 min, not immediate
}
```

### TaskConfig

Configuration for a single task.

```rust
pub struct TaskConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub start_immediately: bool,
}
```

**Constructors:**
- `TaskConfig::new(interval)` - Create enabled task with given interval
- `TaskConfig::disabled()` - Create disabled task

### MonitorTask Trait

Interface for background tasks.

```rust
#[async_trait]
pub trait MonitorTask: Send + Sync {
    fn name(&self) -> &'static str;
    fn default_interval(&self) -> Duration;
    async fn setup(&self) -> Result<()> { Ok(()) }  // optional, called before first run
    async fn run(&self) -> Result<TaskResult>;
}
```

The `setup()` method is called once before the first `run()` invocation. It has a default no-op implementation. Tasks can override it to perform async initialization (e.g., loading state from storage). If `setup()` returns an error, the task is not started.

### TaskResult

Result of a task execution.

```rust
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    pub items_processed: u32,
    pub errors: Vec<String>,  // Non-fatal errors
}
```

**Constructors:**
- `TaskResult::new()` - Create empty result
- `TaskResult::with_count(count)` - Create result with processed count

**Methods:**
- `add_error(error)` - Add a non-fatal error to the result

### TaskType

Enum identifying each of the 12 task types for tracking. Implements `Display` (delegates to `as_str()`).

```rust
pub enum TaskType {
    CheckForProofs,
    SendWaiting,
    FailAbandoned,
    UnFail,
    Clock,
    CheckNoSends,
    MonitorCallHistory,
    NewHeader,
    Purge,
    Reorg,
    ReviewStatus,
    SyncWhenIdle,
}
```

## Tasks

### Core Tasks

#### CheckForProofsTask

Fetches merkle proofs for unconfirmed transactions.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Storage queries | `find_proven_tx_reqs` with status: unmined, unknown, callback, sending, unconfirmed |
| Service calls | `get_merkle_path()` for each txid |

**Workflow:**
0. The adoption (`StorageSqlx::adopt_unproven_transactions`): a transaction that links no proof, reads `completed` or `unproven`, beside no request or a proof-eligible one, whose txid has a proof row, is linked to it; it is written `completed` (and its request with it) only where the store holds a root-check record of that proof, else the link alone, the status and the request left (the review's pass, or step 2's ask through the funnel, checks the root). No header is read. The link step takes every such row in one write a pass; a row with no request at all and no proof row gets an `unmined` request (at most `ADOPT_UNPROVEN_TX_LIMIT`, 200, a pass).
1. Query proven_tx_reqs with pending confirmation statuses
2. For each transaction, call `services.get_merkle_path()`
3. If proof found, update status to 'completed'
4. If not found, increment attempts counter
5. Continue on errors, collecting them in `TaskResult.errors`

#### SendWaitingTask

Broadcasts transactions waiting to be sent.

| Property | Value |
|----------|-------|
| Default interval | 5 minutes |
| Starts immediately | Yes |
| Storage queries | `find_proven_tx_reqs` with status: unsent, sending |
| Service calls | `post_beef()` in full implementation |

**Workflow:**
1. Query proven_tx_reqs with unsent/sending status
2. Group transactions by `batch` field (or use txid as key if no batch)
3. For each batch, build BEEF and broadcast
4. On success: update status to 'unmined'
5. On double-spend: mark as 'failed'
6. On error: log and retry next cycle

#### FailAbandonedTask

Marks abandoned transactions as failed to release locked UTXOs.

| Property | Value |
|----------|-------|
| Default interval | 5 minutes |
| Timeout | Configurable via `MonitorOptions.fail_abandoned_timeout` (default 5 minutes) |

**Workflow:**
1. Calculate cutoff time based on configured timeout
2. Query transactions with status 'unsigned' or 'unprocessed' older than cutoff
3. For each abandoned transaction, call `storage.abort_action()` to release UTXOs
4. Log results

#### UnfailTask

Recovers transactions that were incorrectly marked as failed.

| Property | Value |
|----------|-------|
| Default interval | 10 minutes |
| Storage queries | `find_proven_tx_reqs` with status: unfail |
| Service calls | `get_merkle_path()` for each txid |

**Workflow:**
1. Query proven_tx_reqs with 'unfail' status
2. For each transaction, check if it has a merkle path on chain
3. If proof found: update status to 'unmined', restore UTXOs
4. If not found: mark as 'invalid'

**The reopen of a recovered transaction's outputs** (`recover_false_failed_req`, `reopen_output_if_unspent_here`): an output is set spendable only when a provider answers it unspent (`is_utxo`), its row records no spender, and no transaction the store holds that is not failed spends it by its bytes (`StorageSqlx::stored_spenders`, `src/storage/sqlx/stored_spenders.rs`), nor as a spender recorded on any row of the outpoint, and the store holds no unread row that could hide a spender: a transaction neither failed nor completed, with no bytes in `transactions.raw_tx` or its proof row's `proven_txs.raw_tx`, whose `input_beef` carries the output's own transaction whole or its txid's 32 bytes (`knownTxids`), or is NULL or empty (`returnTXIDOnly`, a row a sync brought with none), or any such row when the store does not hold the output's transaction's bytes: what it can hide is a spend of the output as an input from outside the store, whose source travels in its `input_beef`. A row of the output's own user that an output row names as its spender (an action created and not yet signed that took a coin of the store) is not counted for that output, whatever its `input_beef` (`stored_spenders_of_output`), only by order: the output's row has a lower `output_id` than every output row of the action's own (`createAction` looks up the caller's inputs and writes the action's outputs in one write, records itself on every coin of the store it takes and refuses its own output that is not spendable), or the output's transaction is an outgoing action with a higher `transaction_id` and a `created_at` not older than the action's (its txid did not exist when the action was made). Such an action made before the user's row of the output was written (it took the output as an input from outside the store) is counted. Two limits: the second arm trusts a transaction row's stamp, which a sync carries from its reader (from a store built on the reference, the arrival's); and the arm by row ids reads an output row written before the action as held then, which an output row of the user's own action not yet signed (no txid to be looked up by) was not (#864). Another user's such row, an action with no output row of its own, and a row a sync brought (no output names it) are counted. An action whose inputs all come from elsewhere carries none of those bytes and is not counted either; a completed row (its spend is mined, read by the provider's word), a completed row a purge cleared without a proof link included, is not counted. Otherwise the output stays not spendable: a coin withheld, never one offered on a guess. **The retry:** an output withheld only for an unread row is recorded in `monitor_events` (event `outputWithheld`) and every later pass tries at most 20 records, the longest untried leading (`retry_withheld_outputs`): the store's reads before any provider call (no provider call while an unread row is held), the record renewed while still withheld, or the provider answers a hint (`UtxoVerdict::SpentHint`: never written as a spend) or nothing, dropped only once the output is reopened, a spender is found among the store's transactions, a provider answers a proven spend (`UtxoVerdict::Spent`), or the output's transaction is failed. No purge reaches a record: `purge_data` and `cleanup_monitor_events` leave the event `outputWithheld` out. Records are written by this task's reopen and by the send of a `noSend` transaction (`stored_spenders::release_sent_change`). Load per pass: at most 20 searches and 20 `is_utxo` lookups beyond the canary's, so at the default 10-minute interval at most 2,880 lookups a day whatever the number of records; a record whose provider never answers, or whose unread row never ends, is asked for as long as it stands. With no monitor, or this task off, nothing is tried and a withheld output stays not spendable. One search per output the task would reopen (the canary takes at most 20 requests a pass; the explicit `unfail` requests are not bounded): a scan of the bytes of every transaction not failed (no index), on a reader, outside the writer; inside one `BEGIN IMMEDIATE` the row is read again and the store's transactions must be as the search read them, else the search runs once more. Measured on a file store in the test profile (`stored_spenders_cost`, 128-byte transactions): about 5 ms at 10,000 and 55 ms at 100,000, the same on a reader or the writer.

### Extended Tasks

#### ClockTask

Tracks minute-level clock events for scheduling purposes.

| Property | Value |
|----------|-------|
| Default interval | 1 second |
| Starts immediately | Yes |
| State | `last_minute: AtomicU64` |

**Workflow:**
1. Run every second to check if a new minute has started
2. On minute boundary crossing, log the event and return count of elapsed minutes
3. Primarily used for scheduling minute-granularity events

#### NewHeaderTask

Polls for new blockchain block headers.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Service calls | `get_height()` |
| State | `last_height`, `last_hash`, `stable_cycles`, `new_header_received` flag |

**Workflow:**
1. Query current chain height from services
2. Compare with last known height
3. If new blocks detected, set `new_header_received` flag for proof checking
4. If height decreased, log potential reorg warning
5. Track stable cycles without new blocks

**Public Methods:**
- `has_new_header()` - Check if new header since last check
- `clear_new_header_flag()` - Reset the flag after processing
- `last_known_height()` - Get last recorded chain height

#### CheckNoSendsTask

Retrieves proofs for 'nosend' transactions (not broadcast by wallet but may be mined externally).

| Property | Value |
|----------|-------|
| Default interval | 24 hours |
| Storage queries | `find_proven_tx_reqs` with status: nosend |
| Service calls | `get_merkle_path()` for each txid |
| State | `check_now: AtomicBool` flag for immediate trigger |

**Workflow:**
1. Query proven_tx_reqs with 'nosend' status
2. For each transaction, check if it has been mined externally
3. If proof found, update status accordingly
4. Continue on errors, collecting them in result

#### ReorgTask

Handles blockchain reorganizations by processing deactivated headers.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Process delay | 10 minutes (to avoid temporary fork disruption) |
| Max retry count | 3 |
| State | `deactivated_headers: RwLock<Vec<DeactivatedHeader>>` |

**Types:**
```rust
pub struct DeactivatedHeader {
    pub hash: String,
    pub height: u32,
    pub deactivated_at: DateTime<Utc>,
    pub retry_count: u32,
}
```

**Workflow:**
1. Process queued deactivated headers that have aged past the delay threshold
2. Query transactions that may reference the reorg'd block
3. Re-verify merkle proofs for affected transactions
4. Update transaction status if proof no longer valid
5. Requeue with incremented retry count if under max retries

**Public Methods:**
- `queue_deactivated_header(hash, height)` - Add header to processing queue
- `pending_count()` - Get number of pending headers

### Maintenance Tasks

#### PurgeTask

Database maintenance that deletes transient/expired data.

| Property | Value |
|----------|-------|
| Default interval | 1 hour |
| Failed tx age | 7 days (configurable) |
| Completed data age | 30 days (configurable) |
| State | `check_now: AtomicBool` flag |

**Configuration:**
```rust
pub struct PurgeConfig {
    pub purge_failed: bool,           // Default: true
    pub purge_completed_data: bool,   // Default: true
    pub failed_age: Duration,         // Default: 7 days
    pub completed_data_age: Duration, // Default: 30 days
}
```

**Workflow:**
1. Query failed/invalid transactions older than `failed_age`
2. Delete old failed transaction records entirely
3. Query completed transactions older than `completed_data_age`
4. Remove raw_tx, input_beef, mapi responses (keep record for history)

**Constructors:**
- `PurgeTask::new(storage)` - Create with default config
- `PurgeTask::with_config(storage, config)` - Create with custom config

#### ReviewStatusTask

Synchronizes transaction status with ProvenTxReq status.

| Property | Value |
|----------|-------|
| Default interval | 15 minutes |
| Age threshold | 5 minutes (configurable) |
| State | `check_now: AtomicBool` flag |

**Workflow:**
1. Find transactions with completed proofs that are older than age threshold
2. Verify associated transaction records are also marked completed
3. Sync status for any mismatches
4. Storage check 1 (`MonitorStorage::review_status`): a transaction beside an `invalid` request is failed only when the store holds no proof row for it; a proof whose root the store's chain tracker confirms (or a store-checked one) stands and its request is completed; a refuted unchecked proof is demoted and asked again; no tracker answer writes nothing (at most 100 header reads per pass). The tracker is asked with no lock held; the pass's one write reads each candidate again before any arm writes (the root record, the completion, the demotion) and leaves one whose transaction another path failed meanwhile, whichever way the tracker answered (its request is not completed nor moved, so check 3 does not write a failed transaction `completed`). A txid with any `failed` transaction row is not a candidate (no header read for it): the unfail task takes its request after its hour and, with services that hold the transaction, recovers every row not `completed` to `unproven`; the proof task completes them. Without the unfail task or services the failed row stays failed. The candidates are taken by the proof row's number after where the last pass stopped (the `monitor_state` key `invalid_beside_proof_cursor`), coming round, and nothing of a request left `invalid` is written (not its `updated_at`, so the purge, the sync and the unfail task read its age as before): one that never leaves holds no slot for good, a candidate behind N others (counting those put ahead meanwhile) is looked at within ceil((N + 1) / 100) passes. The same pass, cursor and bound take check 3's unchecked proofs: a `completed` request linking a proof row this store has not checked, beside a transaction of its txid not `completed`; confirmed: the root recorded and every row of the txid `completed` and linked in the pass's one write after the re-read (the request still `completed` with the same link and path, a row still not `completed`); refuted: demoted, the request `unmined` (the wallet's own released `failed` row of the txid `unproven` again: an outgoing row, the request not `doubleSpend`, its bytes hashing to the txid, an output row of its user's free at one of its input outpoints, those free rows spent by it again, the note `refutedProofRecoversFailedRow`; any other `failed` row stays `failed`, its link cleared, and where every row of the txid then reads `failed` the request is written `invalid` with the note `refutedProofBesideFailedRow`); no answer: nothing written. Check 3 itself completes only beside a proof with this store's root-check record, with no header read, in one write that reads each row again. The same pass, cursor and bound also take a transaction not `completed` that links a proof row (whether it is checked is read by the root its path computes; a checked one completes with no header read, and beside a `failed` row of the txid is left to the unfail task), beside no request or a request whose word no proof, send or unfail task asks (`invalid` beside a `failed` row, `doubleSpend`, `nosend`, `notFound`): confirmed, recorded and every row of the txid and the request `completed` and linked, the request's history gaining the note `linkedProofCompletesRequest` (its former word under `was`; logged with the marker `linked_proof_completes_request`); refuted, demoted as everywhere on this line (the proof row and its record removed), the links cleared and the status left, a `failed` row recovered as above, an `unmined` request made where the txid holds none and a row of it does not read `failed`, a request the txid holds keeping its word (beside `nosend` the check of `noSend` transactions asks; beside an ended word nothing asks and the row keeps its status until a proof comes by another road); no answer, nothing written. A row that never leaves the candidates (a path that does not parse or computes no root; a linked row whose proof is checked beside a `failed` row) takes none of the pass's 100 places and costs no header read. A sync's merge and the proof task's adoption leave such a row: each writes `completed` only on a proof this store checked, else the held status with the link.

**Constructors:**
- `ReviewStatusTask::new(storage)` - Create with default threshold
- `ReviewStatusTask::with_age_threshold(storage, threshold)` - Custom threshold

#### MonitorCallHistoryTask

Logs service call history for monitoring and diagnostics.

| Property | Value |
|----------|-------|
| Default interval | 12 minutes |

**Workflow:**
1. Call `services.get_services_call_history(true)` to get and reset counters
2. Log success/failure/error counts for each service type:
   - `get_merkle_path`
   - `get_raw_tx`
   - `post_beef`
   - `get_utxo_status`
3. Log total summary

#### SyncWhenIdleTask

Triggers storage synchronization after idle periods. Mirrors the TypeScript `TaskSyncWhenIdle`.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Idle threshold | 2 minutes (configurable) |
| State | `last_activity: AtomicU64` |

**Workflow:**
1. Track last wallet activity timestamp via `notify_activity()`
2. Each run, check if idle threshold has been exceeded
3. If idle, log sync trigger and return `items_processed = 1`
4. If active, return empty result (no-op)

**Constructors:**
- `SyncWhenIdleTask::new()` - Create with default 2-minute threshold
- `SyncWhenIdleTask::with_threshold(duration)` - Create with custom idle threshold

**Public Methods:**
- `notify_activity()` - Reset idle timer (call on wallet activity)
- `last_activity_timestamp()` - Get last activity unix timestamp

## Usage

### Basic Usage with Default Options

```rust
use bsv_wallet_toolbox::monitor::{Monitor, MonitorOptions};
use std::sync::Arc;

// Create monitor with wallet storage and services
let monitor = Monitor::new(
    Arc::new(storage),
    Arc::new(services),
);

// Start all 12 background tasks
monitor.start().await?;

// Check health
let health = monitor.health().await;
assert!(health.running);

// Later, stop the monitor
monitor.stop().await?;
```

### Custom Configuration with Callbacks

```rust
use bsv_wallet_toolbox::monitor::{Monitor, MonitorOptions, TaskConfig};
use std::sync::Arc;
use std::time::Duration;

let options = MonitorOptions {
    tasks: {
        let mut t = TasksConfig::default();
        t.unfail.enabled = false;
        t.check_for_proofs.interval = Duration::from_secs(30);
        t
    },
    fail_abandoned_timeout: Duration::from_secs(12 * 60 * 60), // 12 hours
    on_tx_broadcasted: Some(Arc::new(|update| {
        println!("Broadcast: {} -> {}", update.txid, update.status);
    })),
    on_tx_proven: Some(Arc::new(|update| {
        println!("Proven: {} at height {:?}", update.txid, update.block_height);
    })),
};

let monitor = Monitor::with_options(
    Arc::new(storage),
    Arc::new(services),
    options,
);
```

### Testing with run_once

```rust
// Run all enabled tasks once without starting the daemon
let results = monitor.run_once().await?;

for (task_type, result) in results {
    println!("{}: processed {} items, {} errors",
        task_type, result.items_processed, result.errors.len());
}
```

## Default Task Intervals

| Task | Interval | Start Immediately |
|------|----------|-------------------|
| clock | 1 second | Yes |
| check_for_proofs | 1 minute | No |
| new_header | 1 minute | No |
| reorg | 1 minute | No |
| sync_when_idle | 1 minute | No |
| send_waiting | 5 minutes | Yes |
| fail_abandoned | 5 minutes | No |
| unfail | 10 minutes | No |
| monitor_call_history | 12 minutes | No |
| review_status | 15 minutes | No |
| purge | 1 hour | No |
| check_no_sends | 24 hours | No |

## Dependencies

- `tokio` - Async runtime for task spawning and scheduling
- `async_trait` - Async trait support for `MonitorTask`
- `chrono` - Time calculations for abandoned transaction detection and age thresholds
- `tracing` - Structured logging for task execution
- `serde` - Serialization for `TransactionStatusUpdate`
- `rand` - CSPRNG for generating unique instance IDs
- `hex` - Encoding instance IDs as hex strings

## Logging

The monitor uses `tracing` for structured logging:

- `info` level: Task completion with items processed, new blocks detected, transaction recovery
- `warn` level: Non-fatal errors, chain height decrease (potential reorg), proof invalidation, lock acquisition failures
- `error` level: Fatal task failures, task setup failures
- `debug` level: Detailed task progress, individual transaction processing, no-op cycles, lock skips (held by another instance)

## Implementation Notes

### Thread Safety

The `Monitor` struct uses:
- `Arc<AtomicBool>` for the running flag, shared with spawned tasks for lock-free status checks
- `RwLock<HashMap>` for task handles and health tracking to allow concurrent reads with exclusive writes
- `Arc<RwLock<HashMap<TaskType, TaskHealth>>>` for per-task health, updated after each run

Individual tasks use:
- `AtomicBool` for `check_now` flags (immediate trigger)
- `AtomicU32`/`AtomicU64` for counters and heights
- `RwLock` for queued data (e.g., deactivated headers in ReorgTask)

### Graceful Shutdown

When `stop()` is called or the `Monitor` is dropped:
1. The `running` flag is set to `false`
2. All spawned task handles are aborted
3. Tasks will complete their current iteration before stopping

### Task Lifecycle

Each spawned task follows this lifecycle:
1. `setup()` is called (fails the task if it returns an error)
2. If `start_immediately` is false, wait for the configured interval
3. Enter run loop:
   a. Acquire distributed task lock (skip run if held by another instance)
   b. Call `run()`, update health tracking
   c. Release task lock
   d. Sleep for interval, repeat

### Error Handling

Tasks distinguish between:
- **Fatal errors**: Returned as `Err(Error)`, logged at error level, increments `consecutive_errors` in health
- **Non-fatal errors**: Added to `TaskResult.errors`, logged at warn level, task continues, resets `consecutive_errors`

This allows the monitor to continue operating even when individual transactions fail to process.

### Task Coordination

Some tasks coordinate via shared flags:
- `NewHeaderTask.new_header_received` - Signals proof checking tasks that new blocks arrived
- `PurgeTask.check_now` / `ReviewStatusTask.check_now` / `CheckNoSendsTask.check_now` - Allow external triggering

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main source directory overview
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer and `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer and `WalletServices` trait
