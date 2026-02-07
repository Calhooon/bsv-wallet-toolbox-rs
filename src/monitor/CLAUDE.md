# src/monitor/ - Background Task Scheduler
> Background daemon for monitoring and managing BSV wallet transaction lifecycle

## Overview

The monitor module provides a daemon-based task scheduler for running recurring background operations on wallet storage. It handles transaction lifecycle management including proof verification, transaction broadcasting, abandoned transaction cleanup, recovery of incorrectly failed transactions, blockchain reorganization handling, and database maintenance. The module is designed to run alongside a wallet instance, performing maintenance tasks at configurable intervals.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Monitor<S, V>                            │
│  (S: WalletStorageProvider, V: WalletServices)                  │
├─────────────────────────────────────────────────────────────────┤
│  start() / stop() / run_once() / is_running()                   │
│  Manages task lifecycle and scheduling                          │
├─────────────────────────────────────────────────────────────────┤
│                        TasksConfig                               │
│  enabled | interval | start_immediately (per task)              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Core Tasks     │  Extended Tasks │  Maintenance Tasks          │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ CheckForProofs  │ CheckNoSends    │ Purge (1 hour)              │
│   (1 min)       │   (24 hours)    │ ReviewStatus (15 min)       │
│ SendWaiting     │ NewHeader       │ MonitorCallHistory (12 min) │
│   (5 min)       │   (1 min)       │                             │
│ FailAbandoned   │ Reorg           │                             │
│   (5 min)       │   (1 min)       │                             │
│ Unfail          │ Clock           │                             │
│   (10 min)      │   (1 sec)       │                             │
└─────────────────┴─────────────────┴─────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            WalletStorageProvider    WalletServices
            (find_proven_tx_reqs)    (get_merkle_path)
```

### Callbacks

`MonitorOptions` supports optional event callbacks:
- `on_tx_broadcasted` - Invoked when a transaction has been broadcast
- `on_tx_proven` - Invoked when a transaction proof has been obtained

Both receive a `TransactionStatusUpdate` with txid, status, and optional proof data.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with re-exports of `Monitor`, `MonitorOptions`, `TaskConfig`, `TransactionStatusUpdate`, `MonitorTask`, and `TaskResult` |
| `config.rs` | Configuration types: `MonitorOptions`, `TasksConfig`, `TaskConfig`, and `TransactionStatusUpdate` |
| `daemon.rs` | Main `Monitor` struct that spawns and manages background task execution via tokio |

## Submodules

| Submodule | Purpose |
|-----------|---------|
| `tasks/` | Individual task implementations, each implementing `MonitorTask` trait |

## Key Types

### Monitor

The main daemon struct that schedules and runs background tasks.

```rust
pub struct Monitor<S, V>
where
    S: WalletStorageProvider + 'static,
    V: WalletServices + 'static,
{
    storage: Arc<S>,
    services: Arc<V>,
    options: MonitorOptions,
    running: AtomicBool,
    task_handles: RwLock<HashMap<TaskType, JoinHandle<()>>>,
}
```

**Methods:**
- `new(storage, services)` - Create with default options
- `with_options(storage, services, options)` - Create with custom configuration
- `start()` - Spawn all enabled tasks as background tokio tasks
- `stop()` - Cancel all running tasks
- `is_running()` - Check daemon status
- `run_once()` - Execute all enabled tasks once (useful for testing)

**Note:** `MonitorCallHistoryTask` requires a concrete `Services` type (not the generic `WalletServices` trait), so it is **not** spawned by `start()` or executed by `run_once()`. Users who need this task should spawn it separately with a concrete `Services` instance.

### MonitorOptions

Configuration for the daemon.

```rust
pub struct MonitorOptions {
    pub tasks: TasksConfig,
    pub fail_abandoned_timeout: Duration,  // Default: 24 hours
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

Configuration for all 11 tasks with individual `TaskConfig` entries.

```rust
pub struct TasksConfig {
    pub check_for_proofs: TaskConfig,   // Default: 1 min, not immediate
    pub send_waiting: TaskConfig,        // Default: 5 min, starts immediately
    pub fail_abandoned: TaskConfig,      // Default: 5 min, not immediate
    pub unfail: TaskConfig,              // Default: 10 min, not immediate
    pub clock: TaskConfig,               // Default: 1 sec, starts immediately
    pub new_header: TaskConfig,          // Default: 1 min, not immediate
    pub reorg: TaskConfig,               // Default: 1 min, not immediate
    pub check_no_sends: TaskConfig,      // Default: 24 hours, not immediate
    pub review_status: TaskConfig,       // Default: 15 min, not immediate
    pub purge: TaskConfig,               // Default: 1 hour, not immediate
    pub monitor_call_history: TaskConfig, // Default: 12 min, not immediate
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

Enum identifying each task type for tracking. Implements `Display` (delegates to `as_str()`).

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
}
```

## Tasks

### Core Tasks (Configured via TasksConfig)

#### CheckForProofsTask

Fetches merkle proofs for unconfirmed transactions.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Storage queries | `find_proven_tx_reqs` with status: unmined, unknown, callback, sending, unconfirmed |
| Service calls | `get_merkle_path()` for each txid |

**Workflow:**
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
| Timeout | Configurable via `MonitorOptions.fail_abandoned_timeout` (default 24 hours) |

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

**Constructors:**
- `ReviewStatusTask::new(storage)` - Create with default threshold
- `ReviewStatusTask::with_age_threshold(storage, threshold)` - Custom threshold

#### MonitorCallHistoryTask

Logs service call history for monitoring and diagnostics.

| Property | Value |
|----------|-------|
| Default interval | 12 minutes |
| Service type | Requires concrete `Services` (not generic `WalletServices`) |

**Note:** This task is **not** spawned by `Monitor.start()` or included in `Monitor.run_once()` because it requires a concrete `Services` type. Users must spawn it separately.

**Workflow:**
1. Call `services.get_services_call_history(true)` to get and reset counters
2. Log success/failure/error counts for each service type:
   - `get_merkle_path`
   - `get_raw_tx`
   - `post_beef`
   - `get_utxo_status`
3. Log total summary

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

// Start background tasks
monitor.start().await?;

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

## Logging

The monitor uses `tracing` for structured logging:

- `info` level: Task completion with items processed, new blocks detected, transaction recovery
- `warn` level: Non-fatal errors, chain height decrease (potential reorg), proof invalidation
- `error` level: Fatal task failures, task setup failures
- `debug` level: Detailed task progress, individual transaction processing, no-op cycles

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main source directory overview
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer and `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer and `WalletServices` trait

## Implementation Notes

### Thread Safety

The `Monitor` struct uses:
- `AtomicBool` for the running flag to allow lock-free status checks
- `RwLock<HashMap>` for task handles to allow concurrent reads with exclusive writes

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
3. Enter run loop: call `run()`, log results, sleep for interval, repeat

### Error Handling

Tasks distinguish between:
- **Fatal errors**: Returned as `Err(Error)`, logged at error level
- **Non-fatal errors**: Added to `TaskResult.errors`, logged at warn level, task continues

This allows the monitor to continue operating even when individual transactions fail to process.

### Task Coordination

Some tasks coordinate via shared flags:
- `NewHeaderTask.new_header_received` - Signals proof checking tasks that new blocks arrived
- `PurgeTask.check_now` / `ReviewStatusTask.check_now` / `CheckNoSendsTask.check_now` - Allow external triggering
