# src/monitor/ - Background Task Scheduler
> Background daemon for monitoring and managing BSV wallet transaction lifecycle

## Overview

The monitor module provides a daemon-based task scheduler for running recurring background operations on wallet storage. It handles transaction lifecycle management including proof verification, transaction broadcasting, abandoned transaction cleanup, and recovery of incorrectly failed transactions. The module is designed to run alongside a wallet instance, performing maintenance tasks at configurable intervals.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Monitor<S, V>                            │
│  (S: WalletStorageProvider, V: WalletServices)                  │
├─────────────────────────────────────────────────────────────────┤
│  start() / stop() / run_once()                                  │
│  Manages task lifecycle and scheduling                          │
├─────────────────────────────────────────────────────────────────┤
│                        TaskConfig                                │
│  enabled | interval | start_immediately                         │
├───────────────┬───────────────┬───────────────┬─────────────────┤
│ CheckForProofs│ SendWaiting   │ FailAbandoned │ UnfailTask      │
│   (1 min)     │   (5 min)     │   (5 min)     │   (10 min)      │
└───────────────┴───────────────┴───────────────┴─────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            WalletStorageProvider    WalletServices
            (find_proven_tx_reqs)    (get_merkle_path)
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module root with re-exports of `Monitor`, `MonitorOptions`, `TaskConfig`, `MonitorTask`, and `TaskResult` |
| `config.rs` | Configuration types: `MonitorOptions`, `TasksConfig`, and `TaskConfig` with default intervals |
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

### MonitorOptions

Configuration for the daemon.

```rust
pub struct MonitorOptions {
    pub tasks: TasksConfig,
    pub fail_abandoned_timeout: Duration,  // Default: 24 hours
}
```

### TasksConfig

Configuration for all tasks with individual `TaskConfig` entries.

```rust
pub struct TasksConfig {
    pub check_for_proofs: TaskConfig,  // Default: 1 min, not immediate
    pub send_waiting: TaskConfig,       // Default: 5 min, starts immediately
    pub fail_abandoned: TaskConfig,     // Default: 5 min, not immediate
    pub unfail: TaskConfig,             // Default: 10 min, not immediate
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
    async fn run(&self) -> Result<TaskResult>;
}
```

### TaskResult

Result of a task execution.

```rust
pub struct TaskResult {
    pub items_processed: u32,
    pub errors: Vec<String>,  // Non-fatal errors
}
```

### TaskType

Enum identifying each task type for tracking.

```rust
pub enum TaskType {
    CheckForProofs,
    SendWaiting,
    FailAbandoned,
    UnFail,
}
```

## Tasks

### CheckForProofsTask

Fetches merkle proofs for unconfirmed transactions.

| Property | Value |
|----------|-------|
| Default interval | 1 minute |
| Storage queries | `find_proven_tx_reqs` with status: unmined, unknown, callback, sending, unconfirmed |
| Service calls | `get_merkle_path()` for each txid |

**Workflow:**
1. Query proven_tx_reqs with pending confirmation statuses
2. For each transaction, call `services.get_merkle_path()`
3. If proof found, log success (full implementation would update status to 'completed')
4. If not found, increment attempts counter
5. Continue on errors, collecting them in `TaskResult.errors`

### SendWaitingTask

Broadcasts transactions waiting to be sent.

| Property | Value |
|----------|-------|
| Default interval | 5 minutes |
| Starts immediately | Yes |
| Storage queries | `find_proven_tx_reqs` with status: unsent, sending |
| Service calls | Would call `post_beef()` in full implementation |

**Workflow:**
1. Query proven_tx_reqs with unsent/sending status
2. Group transactions by `batch_id` (or use txid as key if no batch)
3. For each batch, build BEEF and broadcast
4. On success: update status to 'unmined'
5. On double-spend: mark as 'failed'
6. On error: log and retry next cycle

### FailAbandonedTask

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

### UnfailTask

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

### Custom Configuration

```rust
use bsv_wallet_toolbox::monitor::{Monitor, MonitorOptions, TaskConfig};
use std::time::Duration;

let mut options = MonitorOptions::default();

// Disable the unfail task
options.tasks.unfail.enabled = false;

// Run proof checking more frequently
options.tasks.check_for_proofs.interval = Duration::from_secs(30);

// Set shorter abandoned timeout
options.fail_abandoned_timeout = Duration::from_secs(12 * 60 * 60); // 12 hours

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
| check_for_proofs | 1 minute | No |
| send_waiting | 5 minutes | Yes |
| fail_abandoned | 5 minutes | No |
| unfail | 10 minutes | No |

## Dependencies

- `tokio` - Async runtime for task spawning and scheduling
- `async_trait` - Async trait support for `MonitorTask`
- `chrono` - Time calculations for abandoned transaction detection
- `tracing` - Structured logging for task execution

## Logging

The monitor uses `tracing` for structured logging:

- `info` level: Task completion with items processed
- `warn` level: Non-fatal errors from task execution
- `error` level: Fatal task failures
- `debug` level: Detailed task progress and individual transaction processing

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main source directory overview
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer and `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer and `WalletServices` trait
- [./tasks/CLAUDE.md](./tasks/CLAUDE.md) - Detailed task implementations

## Implementation Notes

### Thread Safety

The `Monitor` struct uses:
- `AtomicBool` for the running flag to allow lock-free status checks
- `RwLock<HashMap>` for task handles to allow concurrent reads with exclusive writes

### Graceful Shutdown

When `stop()` is called or the `Monitor` is dropped:
1. The `running` flag is set to `false`
2. All spawned task handles are aborted
3. Tasks will complete their current iteration before stopping

### Error Handling

Tasks distinguish between:
- **Fatal errors**: Returned as `Err(Error)`, logged at error level
- **Non-fatal errors**: Added to `TaskResult.errors`, logged at warn level, task continues

This allows the monitor to continue operating even when individual transactions fail to process.
