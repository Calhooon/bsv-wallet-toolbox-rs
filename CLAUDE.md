# BSV Wallet Toolbox (Rust)

Rust implementation of the BSV wallet toolbox (`@bsv/wallet-toolbox` / `go-wallet-toolbox`).
Provides storage, services, and monitoring for BSV wallets, built on top of `bsv-sdk`.

**Crate:** `bsv-wallet-toolbox` v0.1.0 | **Edition:** 2021 | **License:** MIT
**Stats:** 69 source files, ~51k lines of Rust

## Build & Test

```bash
cargo build                          # Default (sqlite feature)
cargo build --features full          # All features (sqlite + mysql + remote)
cargo test --lib                     # 533 unit tests
cargo test --test services_tests     # 58 services integration tests (169 total across 9 test files)
cargo test                           # All tests (702 total: 533 unit + 169 integration)
cargo test -- --test-threads=1       # If tests conflict on shared resources
cargo clippy -- -D warnings          # Lint (0 warnings, 0 errors)
cargo fmt --all -- --check           # Format check
cargo doc --no-deps --open           # Generate docs
CARGO_TARGET_DIR=~/bsv/targets/<name> DOOR_BENCH_SAMPLES=10 cargo bench --bench door --features bench-support  # Door/explorer p50/p99; the bench needs CARGO_TARGET_DIR or CRITERION_HOME in its own env (benches/README.md)
cd fuzz && cargo +nightly fuzz run <target> -- -max_total_time=60 -timeout=10  # One of twelve fuzz targets on outside bytes, with a budget, in the foreground (docs/FUZZ.md)
```

**Feature flags:** `sqlite` (default), `mysql`, `remote`, `full` (all three)

**bsv-rs dependency:** crates.io `bsv-rs` pinned exactly (`=0.9.0-beta.3` on beta since #419, `=0.9.0-beta.1` from #184; main asks `0.8.0`, a caret range, until beta merges into it) with `full` and `http` features. The pin is the owner's word (epic #80).

## Architecture

```
src/
├── lib.rs                  # Public API re-exports (159 lines)
├── error.rs                # thiserror Error enum (166 lines)
├── storage/                # Persistence layer
│   ├── traits.rs           # WalletStorageReader → Writer → Sync → Provider → MonitorStorage (892 lines)
│   ├── entities/mod.rs     # 18 table entity structs (single file, camelCase serde)
│   ├── sqlx/               # SQLite/MySQL impl (StorageSqlx)
│   │   ├── storage_sqlx/   # Provider core, entity repositories, monitor routines
│   │   ├── create_action/ (the door in mod.rs; allocation, change, fees, signing, validation, inputs, outputs, tracker, beef* and broadcast modules; #61), process_action.rs, abort_action.rs
│   │   ├── internalize_action.rs, beef_verification.rs, sync.rs
│   │   └── mod.rs
│   └── client/             # Remote JSON-RPC impl (StorageClient)
│       ├── storage_client.rs, auth.rs, json_rpc.rs
│       └── mod.rs
├── services/               # Blockchain service abstraction
│   ├── services.rs         # Services orchestrator (WoC, ARC, Bitails, BHS)
│   ├── traits.rs           # WalletServices trait
│   ├── collection.rs       # ServiceCollection<S> with failover
│   └── providers/          # WhatsOnChain, ARC, Bitails, BHS
├── wallet/                 # Full WalletInterface (28 methods)
│   ├── wallet.rs           # Wallet<S, V> generic struct (3314 lines)
│   ├── signer.rs           # BIP-143 sighash + P2PKH/P2PK signing (993 lines)
│   ├── certificate_issuance.rs  # BRC-104 protocol (1090 lines)
│   ├── lookup.rs           # OverlayLookupResolver trait + HttpLookupResolver (610 lines)
│   └── mod.rs
├── monitor/                # Background task daemon
│   ├── daemon.rs           # Monitor<S, V> task scheduler
│   ├── config.rs           # MonitorOptions, TaskConfig
│   └── tasks/              # 15 recurring tasks
│       ├── check_for_proofs.rs, send_waiting.rs, fail_abandoned.rs
│       ├── unfail.rs, clock.rs, new_header.rs, reorg.rs
│       ├── check_no_sends.rs, review_status.rs, purge.rs
│       ├── monitor_call_history.rs, arcade_events.rs, compact_beef.rs
│       ├── review_proven_txs.rs
│       └── sync_when_idle.rs
└── managers/               # Higher-level orchestration
    ├── mod.rs              # WalletLogger, setup_wallet() helper
    ├── storage_manager.rs  # Multi-storage sync with lock hierarchy (1311 lines)
    ├── cwi_style_wallet_manager.rs  # PBKDF2 multi-profile (760 lines)
    ├── simple_wallet_manager.rs     # 2FA auth (336 lines)
    ├── settings_manager.rs          # Persistent settings (354 lines)
    ├── permissions_manager.rs       # BRC-98/99 full enforcement with DPACP/DBAP/DCAP/DSAP (1978 lines)
    └── auth_manager.rs              # WAB integration wrapper (58 lines)
```

Each subdirectory has its own `CLAUDE.md` with detailed API docs. The dataflow across the machines (the doors, the writer, the ladders, the proof path, the monitor, the status words) is `docs/ARCHITECTURE.md`.

## Key Patterns

**Trait hierarchy (storage):** Reader < Writer < Sync < Provider < MonitorStorage.
Adding methods to any storage trait requires updating ALL 3 implementors:
- `StorageSqlx` (src/storage/sqlx/storage_sqlx/mod.rs) - SQL queries
- `WalletStorageManager` (src/managers/storage_manager.rs) - delegates via `run_as_writer`
- `StorageClient` (src/storage/client/storage_client.rs) - uses `rpc_call`

**Async everywhere:** All I/O is async (tokio). Traits use `#[async_trait]`.

**Entity wire format:** All entity structs use `#[serde(rename_all = "camelCase")]` for cross-SDK compatibility with TypeScript/Go implementations. All 18 entities defined in a single file: `src/storage/entities/mod.rs`.

**Service failover:** `ServiceCollection<S>` wraps multiple providers, tries each in order, tracks call history.

**Error handling:** `thiserror`-based `Error` enum in `error.rs`, crate-wide `Result<T>` alias. At the `WalletInterface` boundary, request refusals become bsv-rs's `WalletRefusal { class, message }`; hosts match `WalletRefusalClass`, never the message. The message keeps the toolbox Display text, and the SDK displays that message alone. `StoreBusy` stays `WalletBusy`, `ReviewActions` stays `WalletReviewActions`, and `SdkError` passes through. Other faults stay `WalletError`. The response-wait timeout carries `TransportFault::MayHaveBeenReceived`; BEEF verification preserves typed tracker transport faults, including status and rate-limit wait. Direct sign, internalize and certificate-lookup guards also retain their reference classes; the abort reference guards override the shared transaction-status class without changing their text. The variant, reference class, text table and remaining direct SDK faults are in `src/wallet/CLAUDE.md`.

**Generic types:** `Wallet<S, V>` where `S: WalletStorageProvider`, `V: WalletServices`. `Monitor<S, V>` where `S: MonitorStorage`, `V: WalletServices`.

## Critical Status Values

Transaction lifecycle: `nosend` | `unsigned` | `unprocessed` | `sending` | `unproven` | `completed` | `failed`
- Immediate broadcast uses `"sending"` status (not `"unproven"`)
- ProvenTxReq uses `"unprocessed"` (not `"unmined"`) for immediate broadcast

## Known Issues / Gotchas

- `bsv-rs` is pinned exactly from crates.io; a pin move is the owner's word and lands with the CLI's pin of the same version
- The live internalize verifier still uses the SDK default script mode (#422); the required `script_pin_tests` verdicts expose that gap. Its private caller must supply `get_settings().chain` before the verifier can distinguish mainnet's proven-coin era from testnet/regtest's tip era without guessing a network.
- `StorageSqlx::pool()` is the READER pool (`query_only`): writes go to `writer()` (one connection) or `write_tx()` (`BEGIN IMMEDIATE`); never await a service or tracker while holding either. `clippy.toml` bans every other `begin` (bsv-wallet-cli #44)
- One store per file per process: give a `Wallet` and a `Monitor` clones of ONE `StorageSqlx` (`impl Clone` shares the pools and every field); never open the same file twice in one process (two writer pools meet on SQLite's busy handler)
- `ChainTrackerError` must be imported separately: `use bsv_sdk::transaction::ChainTrackerError;`
- `target >= u128::MAX` triggers clippy `absurd_extreme_comparisons` - use `==` instead
- `WalletPermissionsManager` provides full BRC-98/99 permission enforcement (1978 lines)
- `WalletAuthenticationManager` provides WAB integration (58 lines)
- Doc tests are `ignored` (need runtime setup); real tests are in lib + `tests/`
- 0 clippy warnings with `-D warnings` flag
- CI uses `-D warnings` (deny) for clippy - passes clean

## Test Vectors

Cross-implementation test vectors live in `test_vectors/`:
- `storage/` - create_action, list_outputs, list_actions validation
- `transactions/` - merkle_path verification
- `keys/` - BRC-29 derivation, test users (Alice/Bob with known keys)

Key constants: `MaxPaginationLimit: 10000`, `DefaultLimit: 100`, `MaxSatoshis: 2_100_000_000_000_000`

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`):
- **Test:** ubuntu + macos, stable + beta Rust, unit + integration + doc tests
- **Clippy:** `-D warnings` (deny all warnings)
- **Rustfmt:** format check
- **Docs:** build with `-Dwarnings`

## Cross-SDK Parity

This crate maintains compatibility with:
- TypeScript: `@bsv/wallet-toolbox` (reference implementation)
- Go: `go-wallet-toolbox`

Shared: test vectors, entity field names (camelCase), API method signatures, status enums.

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `bsv-rs` | =0.9.0-beta.3 (crates.io, `full` + `http`) | Primitives, transactions, WalletInterface trait |
| `tokio` | 1 | Async runtime (full features) |
| `sqlx` | 0.8 | SQLite/MySQL database |
| `reqwest` | 0.12 | HTTP client |
| `serde` / `serde_json` | 1 | Serialization |
| `thiserror` | 1.0 | Error derive |
| `async-trait` | 0.1 | Async trait methods |
| `ring` | 0.17 | PBKDF2 (CWI manager) |
| `hex` | various | Hex encoding (hashing is bsv-rs's) |

## Workflow Tips

- Run `cargo test --lib` for fast iteration (5s) vs full `cargo test` (slower with integration tests)
- Each `src/` subdirectory has its own `CLAUDE.md` - check those before modifying a module
- When adding a new storage method, grep for an existing similar method to see the pattern in all 3 implementors
- The `tests/services_tests.rs` file (31KB) contains integration tests that use `mockito` for HTTP mocking
- `examples/basic_wallet.rs` is a minimal skeleton showing ProtoWallet creation
