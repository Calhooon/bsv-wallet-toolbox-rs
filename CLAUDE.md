# BSV Wallet Toolbox (Rust)

Rust implementation of the BSV wallet toolbox (`@bsv/wallet-toolbox` / `go-wallet-toolbox`).
Provides storage, services, and monitoring for BSV wallets, built on top of `bsv-sdk`.

**Crate:** `bsv-wallet-toolbox` v0.1.0 | **Edition:** 2021 | **License:** MIT
**Stats:** 64 source files, ~44k lines of Rust

## Build & Test

```bash
cargo build                          # Default (sqlite feature)
cargo build --features full          # All features (sqlite + mysql + remote)
cargo test --lib                     # 427 unit tests
cargo test --test services_tests     # 58 integration tests
cargo test                           # All tests (485 total)
cargo test -- --test-threads=1       # If tests conflict on shared resources
cargo clippy                         # Lint (62 warnings, no errors)
cargo fmt --all -- --check           # Format check
cargo doc --no-deps --open           # Generate docs
```

**Feature flags:** `sqlite` (default), `mysql`, `remote`, `full` (all three)

**bsv-sdk dependency:** Local path `../rust-sdk` with `full` and `http` features.
You MUST have `../rust-sdk` checked out for the project to compile.

## Architecture

```
src/
├── lib.rs                  # Public API re-exports (145 lines)
├── error.rs                # thiserror Error enum (131 lines)
├── storage/                # Persistence layer
│   ├── traits.rs           # WalletStorageReader → Writer → Sync → Provider → MonitorStorage (811 lines)
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
│   ├── wallet.rs           # Wallet<S, V> generic struct (2582 lines)
│   ├── signer.rs           # BIP-143 sighash + P2PKH/P2PK signing (966 lines)
│   ├── certificate_issuance.rs  # BRC-104 protocol (1095 lines)
│   └── mod.rs
├── monitor/                # Background task daemon
│   ├── daemon.rs           # Monitor<S, V> task scheduler
│   ├── config.rs           # MonitorOptions, TaskConfig
│   └── tasks/              # 11 recurring tasks
│       ├── check_for_proofs.rs, send_waiting.rs, fail_abandoned.rs
│       ├── unfail.rs, clock.rs, new_header.rs, reorg.rs
│       ├── check_no_sends.rs, review_status.rs, purge.rs
│       └── monitor_call_history.rs
└── managers/               # Higher-level orchestration
    ├── mod.rs              # WalletLogger, setup_wallet() helper
    ├── storage_manager.rs  # Multi-storage sync with lock hierarchy (1131 lines)
    ├── cwi_style_wallet_manager.rs  # PBKDF2 multi-profile (709 lines)
    ├── simple_wallet_manager.rs     # 2FA auth (336 lines)
    ├── settings_manager.rs          # Persistent settings (339 lines)
    ├── permissions_manager.rs       # BRC-98/99 (stub - does NOT enforce) (600 lines)
    └── auth_manager.rs              # WAB integration (skeleton) (56 lines)
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
- `WalletPermissionsManager` is a stub - it does NOT enforce permissions
- `WalletAuthenticationManager` is a skeleton (56 lines)
- Doc tests are `ignored` (need runtime setup); real tests are in lib + `tests/`
- 62 clippy warnings (dead_code for scaffolding types, minor style) - no errors
- CI uses `-D warnings` (deny) for clippy - local warnings won't pass CI as-is
- Scaffolding types (`PrivilegedKeyManager`, `WalletLogger`, `LookupResolver`) generate dead_code warnings - expected, they're for future integration

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
