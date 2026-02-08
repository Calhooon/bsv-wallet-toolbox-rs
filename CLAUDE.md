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
```

**Feature flags:** `sqlite` (default), `mysql`, `remote`, `full` (all three)

**bsv-sdk dependency:** Local path `../rust-sdk` with `full` and `http` features.
You MUST have `../rust-sdk` checked out for the project to compile.

## Architecture

```
src/
├── lib.rs                  # Public API re-exports (159 lines)
├── error.rs                # thiserror Error enum (166 lines)
├── storage/                # Persistence layer
│   ├── traits.rs           # WalletStorageReader → Writer → Sync → Provider → MonitorStorage (892 lines)
│   ├── entities/mod.rs     # 18 table entity structs (single file, camelCase serde)
│   ├── sqlx/               # SQLite/MySQL impl (StorageSqlx)
│   │   ├── storage_sqlx.rs # Main impl + CRUD
│   │   ├── create_action.rs, process_action.rs, abort_action.rs
│   │   ├── internalize_action.rs, beef_verification.rs, sync.rs
│   │   └── mod.rs
│   └── client/             # Remote JSON-RPC impl (StorageClient)
│       ├── storage_client.rs, auth.rs, json_rpc.rs
│       └── mod.rs
├── chaintracks/            # Block header tracking
│   ├── chaintracks.rs      # Main orchestrator
│   ├── traits.rs           # ChaintracksClient, ChaintracksManagement, storage/ingestor traits
│   ├── types.rs            # BaseBlockHeader, LiveBlockHeader, etc.
│   ├── storage/            # Memory + SQLite backends
│   └── ingestors/          # Bulk CDN/WoC + Live polling/WebSocket
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
│   └── tasks/              # 12 recurring tasks
│       ├── check_for_proofs.rs, send_waiting.rs, fail_abandoned.rs
│       ├── unfail.rs, clock.rs, new_header.rs, reorg.rs
│       ├── check_no_sends.rs, review_status.rs, purge.rs
│       ├── monitor_call_history.rs
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

Each subdirectory has its own `CLAUDE.md` with detailed API docs.

## Key Patterns

**Trait hierarchy (storage):** Reader < Writer < Sync < Provider < MonitorStorage.
Adding methods to any storage trait requires updating ALL 3 implementors:
- `StorageSqlx` (src/storage/sqlx/storage_sqlx.rs) - SQL queries
- `WalletStorageManager` (src/managers/storage_manager.rs) - delegates via `run_as_writer`
- `StorageClient` (src/storage/client/storage_client.rs) - uses `rpc_call`

**Async everywhere:** All I/O is async (tokio). Traits use `#[async_trait]`.

**Entity wire format:** All entity structs use `#[serde(rename_all = "camelCase")]` for cross-SDK compatibility with TypeScript/Go implementations. All 18 entities defined in a single file: `src/storage/entities/mod.rs`.

**Service failover:** `ServiceCollection<S>` wraps multiple providers, tries each in order, tracks call history.

**Error handling:** `thiserror`-based `Error` enum in `error.rs`, crate-wide `Result<T>` alias. Errors categorized: storage, authentication, service, transaction, validation, sync, wrapped.

**Generic types:** `Wallet<S, V>` where `S: WalletStorageProvider`, `V: WalletServices`. `Monitor<S, V>` where `S: MonitorStorage`, `V: WalletServices`.

## Critical Status Values

Transaction lifecycle: `nosend` | `unsigned` | `unprocessed` | `sending` | `unproven` | `completed` | `failed`
- Immediate broadcast uses `"sending"` status (not `"unproven"`)
- ProvenTxReq uses `"unprocessed"` (not `"unmined"`) for immediate broadcast

## Known Issues / Gotchas

- `bsv-sdk` is a local path dep - must have `../rust-sdk` checked out
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
| `bsv-sdk` | 0.2.0 (local) | Primitives, transactions, WalletInterface trait |
| `tokio` | 1 | Async runtime (full features) |
| `sqlx` | 0.8 | SQLite/MySQL database |
| `reqwest` | 0.12 | HTTP client |
| `serde` / `serde_json` | 1 | Serialization |
| `thiserror` | 1.0 | Error derive |
| `async-trait` | 0.1 | Async trait methods |
| `ring` | 0.17 | PBKDF2 (CWI manager) |
| `tokio-tungstenite` | 0.24 | WebSocket (live ingestors) |
| `sha2` / `ripemd` / `hex` | various | Crypto helpers (Chaintracks) |

## Workflow Tips

- Run `cargo test --lib` for fast iteration (5s) vs full `cargo test` (slower with integration tests)
- Each `src/` subdirectory has its own `CLAUDE.md` - check those before modifying a module
- When adding a new storage method, grep for an existing similar method to see the pattern in all 3 implementors
- The `tests/services_tests.rs` file (31KB) contains integration tests that use `mockito` for HTTP mocking
- `examples/basic_wallet.rs` is a minimal skeleton showing ProtoWallet creation
