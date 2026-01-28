# src/monitor/tasks/ - Background Monitor Tasks
> Scheduled background tasks for transaction lifecycle management

## Overview

This module defines the monitor task system that runs periodic background operations on wallet storage. Each task implements the `MonitorTask` trait and handles a specific aspect of transaction lifecycle management: proof fetching, transaction broadcasting, abandoned transaction cleanup, and failed transaction recovery. Tasks are designed to be run by the monitor daemon at configurable intervals.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Monitor Daemon                              │
│  (Schedules and runs tasks at configured intervals)             │
├─────────────────────────────────────────────────────────────────┤
│                      MonitorTask Trait                           │
│  name() | default_interval() | run()                            │
├───────────────┬───────────────┬───────────────┬─────────────────┤
│ CheckForProofs│ SendWaiting   │ FailAbandoned │    UnFail       │
│ (60s default) │ (5min default)│ (5min default)│ (10min default) │
│               │               │               │                 │
│ Fetches merkle│ Broadcasts    │ Aborts stale  │ Recovers        │
│ proofs for    │ unsent txs    │ unsigned txs  │ incorrectly     │
│ unconfirmed   │ via BEEF      │ to free UTXOs │ failed txs      │
└───────────────┴───────────────┴───────────────┴─────────────────┘
         │               │               │               │
         ▼               ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│  WalletStorageProvider    │     WalletServices                   │
│  (find_proven_tx_reqs,    │     (get_merkle_path,                │
│   abort_action, etc.)     │      post_beef, etc.)                │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with `MonitorTask` trait, `TaskResult`, and `TaskType` enum |
| `check_for_proofs.rs` | `CheckForProofsTask` - fetches merkle proofs for unconfirmed transactions |
| `send_waiting.rs` | `SendWaitingTask` - broadcasts transactions waiting to be sent |
| `fail_abandoned.rs` | `FailAbandonedTask` - marks abandoned transactions as failed |
| `unfail.rs` | `UnfailTask` - recovers incorrectly failed transactions |

## Key Exports

### MonitorTask Trait (mod.rs:50-61)

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

### TaskResult (mod.rs:21-48)

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

### TaskType Enum (mod.rs:63-86)

Identifies task types for scheduling and configuration:

```rust
pub enum TaskType {
    CheckForProofs,   // "check_for_proofs"
    SendWaiting,      // "send_waiting"
    FailAbandoned,    // "fail_abandoned"
    UnFail,           // "unfail"
}
```

### Task Implementations

| Task | Export | Default Interval |
|------|--------|------------------|
| `CheckForProofsTask<S, V>` | `check_for_proofs::CheckForProofsTask` | 60 seconds |
| `SendWaitingTask<S, V>` | `send_waiting::SendWaitingTask` | 5 minutes |
| `FailAbandonedTask<S>` | `fail_abandoned::FailAbandonedTask` | 5 minutes |
| `UnfailTask<S, V>` | `unfail::UnfailTask` | 10 minutes |

## Task Details

### CheckForProofsTask (check_for_proofs.rs)

Fetches merkle proofs for transactions that have been broadcast but not yet confirmed.

**Purpose:** Monitor unconfirmed transactions and obtain merkle proofs once they are included in a block.

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

### SendWaitingTask (send_waiting.rs)

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
- `first_run: AtomicBool` - Tracks whether this is the first execution (affects age filtering)

**Behavior:**
1. Queries `proven_tx_reqs` with status: `Unsent` or `Sending`
2. Groups transactions by `batch_id` (or uses `txid` as key for unbatched)
3. For each batch:
   - Would build BEEF from stored `raw_tx` and `input_beef`
   - Would call `services.post_beef()` to broadcast
   - On success: update status to `Unmined`
   - On double-spend: mark as `Failed`
   - On error: log and retry next cycle

**Note:** Current implementation logs intent but does not execute full broadcast logic (requires additional storage access for raw transaction bytes).

### FailAbandonedTask (fail_abandoned.rs)

Marks abandoned transactions as failed to release locked UTXOs.

**Purpose:** Clean up stale transactions that were never completed (e.g., user abandoned the signing flow).

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
   - Log results

**Note:** Requires admin-level storage queries (across all users) which are not yet fully implemented.

### UnfailTask (unfail.rs)

Recovers transactions that were incorrectly marked as failed.

**Purpose:** Allow recovery of transactions that actually succeeded on-chain but were marked failed due to temporary errors.

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
     └──────┬───────┘              └──────────────┘      │
            │ (SendWaiting broadcasts)                    │
     ┌──────▼───────┐                                    │
     │   Sending    │                                    │
     └──────┬───────┘                                    │
            │ (broadcast success)                        │
     ┌──────▼───────┐                                    │
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
                                   │         if tx found on-chain)
                              (UnfailTask marks invalid
                               if tx not on-chain)
                                   │
                              ┌────▼─────┐
                              │ Invalid  │
                              └──────────┘
```

## Usage

### Creating Tasks

```rust
use std::sync::Arc;
use std::time::Duration;
use bsv_wallet_toolbox::monitor::tasks::{
    CheckForProofsTask, SendWaitingTask, FailAbandonedTask, UnfailTask,
    MonitorTask, TaskResult,
};

// Create tasks with storage and services
let check_proofs = CheckForProofsTask::new(
    Arc::clone(&storage),
    Arc::clone(&services),
);

let send_waiting = SendWaitingTask::new(
    Arc::clone(&storage),
    Arc::clone(&services),
);

let fail_abandoned = FailAbandonedTask::new(
    Arc::clone(&storage),
    Duration::from_secs(24 * 60 * 60), // 24 hour timeout
);

let unfail = UnfailTask::new(
    Arc::clone(&storage),
    Arc::clone(&services),
);
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

## ProvenTxReqStatus Values Used

| Task | Queries Statuses | Updates To |
|------|------------------|------------|
| `CheckForProofsTask` | `Unmined`, `Unknown`, `Callback`, `Sending`, `Unconfirmed` | `Completed` (on proof) |
| `SendWaitingTask` | `Unsent`, `Sending` | `Unmined` (on success), `Failed` (on double-spend) |
| `FailAbandonedTask` | N/A (queries transactions directly) | Calls `abort_action` |
| `UnfailTask` | `Unfail` | `Unmined` (if on-chain), `Invalid` (if not) |

## Generic Type Constraints

All tasks require their storage parameter to implement `WalletStorageProvider`:

```rust
S: WalletStorageProvider + 'static
```

Tasks that interact with blockchain services also require:

```rust
V: WalletServices + 'static
```

Both traits are re-exported from the `storage` and `services` modules respectively.

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent monitor module (when available)
- [../../CLAUDE.md](../../CLAUDE.md) - Source root documentation
- [../../storage/CLAUDE.md](../../storage/CLAUDE.md) - Storage layer and `WalletStorageProvider` trait
- [../../services/CLAUDE.md](../../services/CLAUDE.md) - Services layer and `WalletServices` trait
- [../../storage/entities/CLAUDE.md](../../storage/entities/CLAUDE.md) - Entity definitions including `ProvenTxReqStatus`
