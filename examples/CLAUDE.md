# Examples

> Runnable demos showing how to configure core wallet toolbox components.

## Overview

Three standalone examples demonstrating configuration and initialization of the main subsystems: Services (blockchain providers), Chaintracks (block header tracking), and Monitor (background task scheduler). All examples are synchronous print-based demos (no async runtime required) that show builder patterns and default configurations.

## Running

```bash
cargo run --example basic_wallet       # Services + ServicesOptions configuration
cargo run --example chaintracks_demo   # Chaintracks block header options
cargo run --example monitor_demo       # Monitor daemon task scheduling
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `basic_wallet.rs` | 47 | Services creation for mainnet/testnet, `ServicesOptions` builder pattern |
| `chaintracks_demo.rs` | 61 | `ChaintracksOptions` for mainnet/testnet, custom thresholds, readonly mode |
| `monitor_demo.rs` | 95 | `MonitorOptions` defaults, all 11 `TaskConfig` intervals, custom/disabled tasks |

## basic_wallet.rs

Demonstrates the `Services` orchestrator and `ServicesOptions` builder:

- `Services::mainnet()` / `Services::testnet()` - Create default provider sets
- `Services::with_options(Chain, ServicesOptions)` - Custom configuration
- `ServicesOptions::default().with_bhs_url().with_bhs_api_key()` - Builder methods
- `ServicesOptions::mainnet().with_woc_api_key().with_bitails_api_key().with_bhs()` - Full builder chain

**Key imports:** `bsv_wallet_toolbox::{Chain, Services, ServicesOptions}`

## chaintracks_demo.rs

Demonstrates `ChaintracksOptions` configuration:

- `ChaintracksOptions::default_mainnet()` / `default_testnet()` - Network defaults
- Struct update syntax for customization: `ChaintracksOptions { readonly: true, live_height_threshold: 500, ..default_mainnet() }`
- Inspectable fields: `chain`, `live_height_threshold`, `reorg_height_threshold`, `batch_insert_limit`, `bulk_migration_chunk_size`, `require_ingestors`, `readonly`

**Key imports:** `bsv_wallet_toolbox::chaintracks::ChaintracksOptions`

## monitor_demo.rs

Demonstrates `MonitorOptions` and per-task `TaskConfig`:

- `MonitorOptions::default()` - Shows all 11 task default intervals and flags
- `TaskConfig::new(Duration)` - Custom interval with defaults (enabled, no immediate start)
- `TaskConfig::disabled()` - Disable a task entirely

**Tasks shown with default intervals:**
`clock`, `check_for_proofs`, `new_header`, `reorg`, `send_waiting`, `fail_abandoned`, `unfail`, `monitor_call_history`, `review_status`, `purge`, `check_no_sends`

Each `TaskConfig` exposes: `enabled`, `interval` (Duration), `start_immediately`

**Key imports:** `bsv_wallet_toolbox::monitor::{MonitorOptions, TaskConfig}`

## Cargo.toml Registration

All three examples are declared as `[[example]]` entries in the root `Cargo.toml` (lines 80-90).

## Notes

- These examples are configuration-only demos; they do not perform I/O or require a database
- `basic_wallet.rs` returns `Result` (Services creation is fallible); the other two use plain `fn main()`
- None require the async tokio runtime since they only inspect configuration structs
- The examples use public API types re-exported from `bsv_wallet_toolbox` (see `src/lib.rs`)

## Related

- [Root CLAUDE.md](../CLAUDE.md) - Project overview, build commands, architecture
- [src/services/CLAUDE.md](../src/services/CLAUDE.md) - Services module details
- [src/chaintracks/CLAUDE.md](../src/chaintracks/CLAUDE.md) - Chaintracks module details
- [src/monitor/CLAUDE.md](../src/monitor/CLAUDE.md) - Monitor daemon details
