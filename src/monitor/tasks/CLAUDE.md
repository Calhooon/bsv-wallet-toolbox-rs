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
│                   name() | default_interval() | run()                         │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────────┤
│ Blockchain   │ Transaction  │ Status &     │ Maintenance  │ Service          │
│ Monitoring   │ Processing   │ Recovery     │              │ Monitoring       │
├──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┤
│ NewHeader    │ SendWaiting  │ ReviewStatus │ Purge        │ MonitorCall      │
│ (60s)        │ (5min)       │ (15min)      │ (1hr)        │ History (12min)  │
│              │              │              │              │                  │
│ Clock        │ CheckFor     │ UnFail       │ FailAbandoned│                  │
│ (1s)         │ Proofs (60s) │ (10min)      │ (5min)       │                  │
│              │              │              │              │                  │
│ Reorg        │ CheckNo      │              │              │                  │
│ (60s)        │ Sends (24hr) │              │              │                  │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────────┘
         │               │               │               │               │
         ▼               ▼               ▼               ▼               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  WalletStorageProvider              │         WalletServices                  │
│  (find_proven_tx_reqs,              │         (get_merkle_path, post_beef,    │
│   abort_action, etc.)               │          get_height, etc.)              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with `MonitorTask` trait, `TaskResult`, and `TaskType` enum |
| `check_for_proofs.rs` | `CheckForProofsTask` - fetches merkle proofs for unconfirmed transactions |
| `check_no_sends.rs` | `CheckNoSendsTask` - retrieves proofs for 'nosend' transactions |
| `clock.rs` | `ClockTask` - tracks minute-level clock events |
| `fail_abandoned.rs` | `FailAbandonedTask` - marks abandoned transactions as failed |
| `monitor_call_history.rs` | `MonitorCallHistoryTask` - logs service call statistics |
| `new_header.rs` | `NewHeaderTask` - polls for new blockchain block headers |
| `purge.rs` | `PurgeTask` - database maintenance, deletes expired data |
| `reorg.rs` | `ReorgTask` - handles blockchain reorganizations |
| `review_status.rs` | `ReviewStatusTask` - synchronizes transaction and proof status |
| `send_waiting.rs` | `SendWaitingTask` - broadcasts transactions waiting to be sent |
| `unfail.rs` | `UnfailTask` - recovers incorrectly failed transactions |

## Key Exports

### MonitorTask Trait (mod.rs:64-75)

The core trait that all monitor tasks implement:

```rust
#[async_trait]
pub trait MonitorTask: Send + Sync {
    /// Get the task name (e.g., "check_for_proofs").
    fn name(&self) -> &'static str;

    /// Get the default interval for this task.
    fn default_interval(&self) -> Duration;

    /// Run the task once, returning processed count and any errors.
    async fn run(&self) -> Result<TaskResult>;
}
```

### TaskResult (mod.rs:35-62)

Result structure returned by task execution:

```rust
pub struct TaskResult {
    /// Number of items processed.
    pub items_processed: u32,
    /// List of errors encountered (non-fatal).
    pub errors: Vec<String>,
}
```

**Methods:**
- `TaskResult::new()` - Create empty result
- `TaskResult::with_count(n)` - Create result with processed count
- `add_error(msg)` - Record a non-fatal error

### TaskType Enum (mod.rs:77-102)

Identifies task types for scheduling and configuration:

```rust
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
}
```

### Task Implementations

| Task | Export | Default Interval | Category |
|------|--------|------------------|----------|
| `CheckForProofsTask<S, V>` | `check_for_proofs::CheckForProofsTask` | 60 seconds | Transaction |
| `CheckNoSendsTask<S, V>` | `check_no_sends::CheckNoSendsTask` | 24 hours | Transaction |
| `ClockTask` | `clock::ClockTask` | 1 second | Blockchain |
| `FailAbandonedTask<S>` | `fail_abandoned::FailAbandonedTask` | 5 minutes | Maintenance |
| `MonitorCallHistoryTask` | `monitor_call_history::MonitorCallHistoryTask` | 12 minutes | Service |
| `NewHeaderTask<V>` | `new_header::NewHeaderTask` | 60 seconds | Blockchain |
| `PurgeTask<S>` | `purge::PurgeTask` | 1 hour | Maintenance |
| `ReorgTask<S, V>` | `reorg::ReorgTask` | 60 seconds | Blockchain |
| `ReviewStatusTask<S>` | `review_status::ReviewStatusTask` | 15 minutes | Status |
| `SendWaitingTask<S, V>` | `send_waiting::SendWaitingTask` | 5 minutes | Transaction |
| `UnfailTask<S, V>` | `unfail::UnfailTask` | 10 minutes | Status |

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

**Purpose:** Detect new blocks and coordinate proof fetching for pending transactions.

**Type Parameters:**
- `V: WalletServices` - Service provider for chain height lookups

**Constructor:**
```rust
NewHeaderTask::new(services: Arc<V>) -> Self
```

**State:**
- `last_height: AtomicU32` - Last known chain height
- `last_hash: RwLock<Option<String>>` - Last known chain tip hash
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
4. If height decreased: logs potential reorg warning
5. If same height: increments stable cycle counter

#### ClockTask (clock.rs)

Tracks minute-level clock events for scheduling coordination.

**Purpose:** Provide minute granularity timing for other tasks and logging.

**Constructor:**
```rust
ClockTask::new() -> Self
ClockTask::default() -> Self
```

**State:**
- `last_minute: AtomicU64` - Last recorded minute since Unix epoch

**Behavior:**
1. Calculates current minute since epoch
2. On first run: records current minute
3. If minute boundary crossed: logs elapsed minutes, returns count
4. Otherwise: returns zero items processed

#### ReorgTask (reorg.rs)

Handles blockchain reorganizations by re-verifying affected proofs.

**Purpose:** Maintain proof validity when the blockchain reorganizes.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend
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
- `queue_deactivated_header(hash, height)` - Queue a header for processing
- `pending_count()` - Get number of pending headers

**Behavior:**
1. Processes deactivated headers after 10-minute delay (avoids temporary forks)
2. For each header, queries potentially affected transactions
3. Re-verifies merkle proofs via `services.get_merkle_path()`
4. Logs affected transactions that lost valid proofs
5. Retries up to 3 times with delay reset between attempts

### Transaction Processing Tasks

#### CheckForProofsTask (check_for_proofs.rs)

Fetches merkle proofs for transactions that have been broadcast but not yet confirmed.

**Purpose:** Monitor unconfirmed transactions and obtain merkle proofs once mined.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend
- `V: WalletServices` - Service provider for merkle path lookups

**Constructor:**
```rust
CheckForProofsTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Behavior:**
1. Queries `proven_tx_reqs` with status: `Unmined`, `Unknown`, `Callback`, `Sending`, or `Unconfirmed`
2. For each transaction, calls `services.get_merkle_path(txid)`
3. On success with proof: logs success, increments processed count
4. On "not found": logs debug message (will retry next cycle)
5. On error: records error in `TaskResult.errors`, continues to next txid

#### CheckNoSendsTask (check_no_sends.rs)

Retrieves proofs for 'nosend' transactions that may have been mined externally.

**Purpose:** Track transactions the wallet didn't broadcast but may have been mined by another party.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend
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
1. Queries `proven_tx_reqs` with status: `NoSend`
2. For each transaction, checks for merkle proof
3. If proof found: logs success (would update status to indicate proof found)
4. If not found: logs debug, will retry on next daily cycle

#### SendWaitingTask (send_waiting.rs)

Broadcasts transactions that are ready to be sent to the network.

**Purpose:** Ensure all signed transactions get broadcast, with automatic retry on failures.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend
- `V: WalletServices` - Service provider for transaction broadcast

**Constructor:**
```rust
SendWaitingTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**State:**
- `first_run: AtomicBool` - Tracks whether this is the first execution

**Behavior:**
1. Queries `proven_tx_reqs` with status: `Unsent` or `Sending`
2. Groups transactions by `batch_id` (or uses `txid` as key for unbatched)
3. For each batch:
   - Would build BEEF from stored `raw_tx` and `input_beef`
   - Would call `services.post_beef()` to broadcast
   - On success: update status to `Unmined`
   - On double-spend: mark as `Failed`
   - On error: log and retry next cycle

### Status & Recovery Tasks

#### ReviewStatusTask (review_status.rs)

Synchronizes transaction status with ProvenTxReq status.

**Purpose:** Ensure consistency between transaction records and proof requests.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend

**Constants:**
- `DEFAULT_AGE_THRESHOLD_SECS: u64 = 300` - 5 minute default age threshold

**Constructor:**
```rust
ReviewStatusTask::new(storage: Arc<S>) -> Self
ReviewStatusTask::with_age_threshold(storage: Arc<S>, age_threshold: Duration) -> Self
```

**State:**
- `age_threshold: Duration` - How old before reviewing status
- `check_now: AtomicBool` - Flag to trigger immediate check

**Public Methods:**
- `trigger_check()` - Set flag for immediate check

**Behavior:**
1. Queries completed `proven_tx_reqs` older than age threshold
2. For each, verifies associated transaction is marked completed
3. Would sync mismatched statuses

#### UnfailTask (unfail.rs)

Recovers transactions that were incorrectly marked as failed.

**Purpose:** Allow recovery of transactions that succeeded on-chain but were marked failed due to errors.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend
- `V: WalletServices` - Service provider for merkle path lookups

**Constructor:**
```rust
UnfailTask::new(storage: Arc<S>, services: Arc<V>) -> Self
```

**Behavior:**
1. Queries `proven_tx_reqs` with status: `Unfail`
2. For each transaction, checks if it has a merkle path on-chain
3. If merkle path found (transaction succeeded):
   - Would update `proven_tx_req` status to `Unmined`
   - Would update transaction status to `Unproven`
   - Would create UTXOs for spendable outputs
4. If not found (transaction truly failed):
   - Would update `proven_tx_req` status to `Invalid`
5. On lookup error: records error, does not change status

### Maintenance Tasks

#### PurgeTask (purge.rs)

Database maintenance that deletes transient/expired data.

**Purpose:** Keep database size manageable by removing old data.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend

**Configuration:**
```rust
pub struct PurgeConfig {
    pub purge_failed: bool,           // Whether to purge failed txs (default: true)
    pub purge_completed_data: bool,   // Whether to purge completed tx data (default: true)
    pub failed_age: Duration,         // Age for failed tx purge (default: 7 days)
    pub completed_data_age: Duration, // Age for completed data purge (default: 30 days)
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
1. If `purge_failed` enabled:
   - Queries `Failed` and `Invalid` transactions older than `failed_age`
   - Would delete entire records
2. If `purge_completed_data` enabled:
   - Queries `Completed` transactions older than `completed_data_age`
   - Would remove raw_tx, input_beef, mapi responses (keeps record)

#### FailAbandonedTask (fail_abandoned.rs)

Marks abandoned transactions as failed to release locked UTXOs.

**Purpose:** Clean up stale transactions that were never completed.

**Type Parameters:**
- `S: WalletStorageProvider` - Storage backend

**Constructor:**
```rust
FailAbandonedTask::new(storage: Arc<S>, timeout: Duration) -> Self
```

**Parameters:**
- `timeout: Duration` - How long before a transaction is considered abandoned

**Behavior:**
1. Calculates cutoff time: `now - timeout`
2. Would query transactions with status `Unsigned` or `Unprocessed` older than cutoff
3. For each abandoned transaction:
   - Call `storage.abort_action()` to release locked UTXOs

### Service Monitoring Tasks

#### MonitorCallHistoryTask (monitor_call_history.rs)

Logs service call statistics for monitoring and debugging.

**Purpose:** Track success/failure rates of external service calls.

**Note:** Requires concrete `Services` type (not generic `WalletServices` trait).

**Constructor:**
```rust
MonitorCallHistoryTask::new(services: Arc<Services>) -> Self
```

**Behavior:**
1. Calls `services.get_services_call_history(true)` (true = reset counters)
2. Logs per-provider statistics for each service type:
   - `get_merkle_path` - Proof lookups
   - `get_raw_tx` - Transaction retrieval
   - `post_beef` - Transaction broadcast
   - `get_utxo_status` - UTXO status checks
3. Logs summary of total calls and errors

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

## Usage

### Creating Tasks

```rust
use std::sync::Arc;
use std::time::Duration;
use bsv_wallet_toolbox::monitor::tasks::{
    CheckForProofsTask, CheckNoSendsTask, ClockTask, FailAbandonedTask,
    MonitorCallHistoryTask, NewHeaderTask, PurgeTask, PurgeConfig,
    ReorgTask, ReviewStatusTask, SendWaitingTask, UnfailTask,
    MonitorTask, TaskResult,
};

// Blockchain monitoring tasks
let new_header = NewHeaderTask::new(Arc::clone(&services));
let clock = ClockTask::new();
let reorg = ReorgTask::new(Arc::clone(&storage), Arc::clone(&services));

// Transaction processing tasks
let check_proofs = CheckForProofsTask::new(Arc::clone(&storage), Arc::clone(&services));
let check_no_sends = CheckNoSendsTask::new(Arc::clone(&storage), Arc::clone(&services));
let send_waiting = SendWaitingTask::new(Arc::clone(&storage), Arc::clone(&services));

// Status & recovery tasks
let review_status = ReviewStatusTask::new(Arc::clone(&storage));
let unfail = UnfailTask::new(Arc::clone(&storage), Arc::clone(&services));

// Maintenance tasks
let purge = PurgeTask::with_config(Arc::clone(&storage), PurgeConfig {
    purge_failed: true,
    purge_completed_data: true,
    failed_age: Duration::from_secs(7 * 24 * 60 * 60),
    completed_data_age: Duration::from_secs(30 * 24 * 60 * 60),
});
let fail_abandoned = FailAbandonedTask::new(
    Arc::clone(&storage),
    Duration::from_secs(24 * 60 * 60),
);

// Service monitoring (requires concrete Services type)
let monitor_calls = MonitorCallHistoryTask::new(Arc::clone(&services));
```

### Running a Task

```rust
async fn run_task<T: MonitorTask>(task: &T) -> Result<()> {
    println!("Running task: {}", task.name());

    let result = task.run().await?;

    println!("Processed {} items", result.items_processed);
    for error in &result.errors {
        eprintln!("Non-fatal error: {}", error);
    }

    Ok(())
}
```

### Task Scheduling Pattern

```rust
use tokio::time::{interval, Duration};

async fn schedule_task<T: MonitorTask>(task: Arc<T>) {
    let mut interval = interval(task.default_interval());

    loop {
        interval.tick().await;

        match task.run().await {
            Ok(result) => {
                tracing::info!(
                    task = task.name(),
                    processed = result.items_processed,
                    errors = result.errors.len(),
                    "Task completed"
                );
            }
            Err(e) => {
                tracing::error!(task = task.name(), error = %e, "Task failed");
            }
        }
    }
}
```

### Triggering Immediate Checks

Several tasks support immediate triggering:

```rust
// Trigger proof check when new header received
if new_header_task.has_new_header() {
    check_no_sends_task.trigger_check();
    new_header_task.clear_new_header_flag();
}

// Trigger purge manually
purge_task.trigger_purge();

// Queue reorg header for processing
reorg_task.queue_deactivated_header(hash, height).await;
```

## ProvenTxReqStatus Values Used

| Task | Queries Statuses | Updates To |
|------|------------------|------------|
| `CheckForProofsTask` | `Unmined`, `Unknown`, `Callback`, `Sending`, `Unconfirmed` | `Completed` (on proof) |
| `CheckNoSendsTask` | `NoSend` | Updates when proof found |
| `SendWaitingTask` | `Unsent`, `Sending` | `Unmined` (success), `Failed` (double-spend) |
| `FailAbandonedTask` | N/A (queries transactions) | Calls `abort_action` |
| `UnfailTask` | `Unfail` | `Unmined` (on-chain), `Invalid` (not found) |
| `PurgeTask` | `Failed`, `Invalid`, `Completed` | Deletes/clears data |
| `ReviewStatusTask` | `Completed` | Syncs transaction status |
| `ReorgTask` | `Completed`, `Unmined` | Re-verifies proofs |

## Generic Type Constraints

Tasks use different type constraints based on their needs:

**Storage only:**
```rust
S: WalletStorageProvider + 'static
```
Used by: `FailAbandonedTask`, `PurgeTask`, `ReviewStatusTask`

**Services only:**
```rust
V: WalletServices + 'static
```
Used by: `NewHeaderTask`

**Both storage and services:**
```rust
S: WalletStorageProvider + 'static
V: WalletServices + 'static
```
Used by: `CheckForProofsTask`, `CheckNoSendsTask`, `ReorgTask`, `SendWaitingTask`, `UnfailTask`

**No generics (concrete types only):**
- `ClockTask` - No external dependencies
- `MonitorCallHistoryTask` - Requires concrete `Services` type

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent monitor module
- [../../CLAUDE.md](../../CLAUDE.md) - Source root documentation
- [../../storage/CLAUDE.md](../../storage/CLAUDE.md) - Storage layer and `WalletStorageProvider` trait
- [../../services/CLAUDE.md](../../services/CLAUDE.md) - Services layer and `WalletServices` trait
- [../../storage/entities/CLAUDE.md](../../storage/entities/CLAUDE.md) - Entity definitions including `ProvenTxReqStatus`
