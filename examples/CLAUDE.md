# Examples

> Runnable demos showing how to configure core wallet toolbox components.

## Overview

Three examples. Two are standalone demos of configuration and initialization of the main subsystems: Services (blockchain providers) and Monitor (background task scheduler), synchronous and print-based (no async runtime required), showing builder patterns and default configurations. The third, `a8_census.rs`, is a profiling harness: it runs a tokio runtime and drives the wallet's doors against an in-memory SQLite store and the mock services.

## Running

```bash
cargo run --example basic_wallet       # Services + ServicesOptions configuration
cargo run --example monitor_demo       # Monitor daemon task scheduling
cargo run --release --example a8_census -- <scene> <size> <iterations>  # One census scene in a loop
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `basic_wallet.rs` | 62 | Services creation for mainnet/testnet, `ServicesOptions` builder pattern |
| `monitor_demo.rs` | 95 | `MonitorOptions` defaults, all 11 `TaskConfig` intervals, custom/disabled tasks |
| `a8_census.rs` | 302 | The A8 census scenes for a profiler: createAction, the post, internalize and the large BEEF path in a loop (`benches/README.md`) |

## basic_wallet.rs

Demonstrates the `Services` orchestrator and `ServicesOptions` builder:

- `Services::mainnet()` - Arcade at the crate's public `ARCADE_V2_MAINNET`; `Services::testnet()` refuses: no Arcade testnet endpoint is named (#317)
- `Services::with_options(Chain, ServicesOptions::mainnet().with_arcade(url, None))` - Arcade, the default mode, at `ARCADE_URL` (a loopback placeholder when unset)
- `ServicesOptions::default().with_arcade().with_bhs_url().with_bhs_api_key()` - Builder methods
- `ServicesOptions::mainnet().with_woc_api_key().with_bitails_api_key().with_bhs()` - Full builder chain

**Key imports:** `bsv_wallet_toolbox::{Chain, Services, ServicesOptions}`

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

`basic_wallet` and `monitor_demo` are declared as `[[example]]` entries in the root `Cargo.toml`; `a8_census.rs` is found by Cargo's example discovery (no entry, so `Cargo.toml` is unchanged).

## Notes

- `basic_wallet.rs` and `monitor_demo.rs` are configuration-only demos; they do not perform I/O or require a database
- `a8_census.rs` does: it seeds an in-memory SQLite store through the bench fixture (`benches/common`, by `#[path]`), runs a tokio runtime, and posts only to `MockWalletServices`
- `basic_wallet.rs` returns `Result` (Services creation is fallible); the other two use plain `fn main()`
- The two demos need no async runtime since they only inspect configuration structs
- The examples use public API types re-exported from `bsv_wallet_toolbox` (see `src/lib.rs`)

## Related

- [Root CLAUDE.md](../CLAUDE.md) - Project overview, build commands, architecture
- [src/services/CLAUDE.md](../src/services/CLAUDE.md) - Services module details
- [src/monitor/CLAUDE.md](../src/monitor/CLAUDE.md) - Monitor daemon details
