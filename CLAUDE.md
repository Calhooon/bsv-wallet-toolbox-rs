# BSV Wallet Toolbox (Rust)

Rust implementation of the BSV wallet toolbox (`@bsv/wallet-toolbox` / `go-wallet-toolbox`).
Provides storage, services, and monitoring for BSV wallets, built on top of `bsv-sdk`.

## Build & Test

```bash
cargo build                          # Default (sqlite feature)
cargo build --features full          # All features (sqlite + mysql + remote)
cargo test                           # Unit tests (429) + integration tests (51)
cargo test -- --test-threads=1       # If tests conflict on shared resources
cargo clippy                         # Lint (62 warnings, mostly minor - no errors)
cargo doc --open                     # Generate docs
```

**Feature flags:** `sqlite` (default), `mysql`, `remote`, `full`

**bsv-sdk dependency:** Local path `../rust-sdk` with `full` and `http` features.

## Architecture

```
src/
├── lib.rs              # Public API re-exports
├── error.rs            # Error enum (thiserror)
├── storage/            # Persistence layer
│   ├── traits.rs       # WalletStorageReader → Writer → Sync → Provider → MonitorStorage
│   ├── entities/       # 18 table entity structs (camelCase serde)
│   ├── sqlx/           # SQLite/MySQL impl (StorageSqlx)
│   └── client/         # Remote JSON-RPC impl (StorageClient)
├── chaintracks/        # Block header tracking
│   ├── chaintracks.rs  # Main orchestrator
│   ├── storage/        # Memory + SQLite backends
│   └── ingestors/      # Bulk CDN/WoC + Live polling/WebSocket
├── services/           # Blockchain service abstraction
│   ├── services.rs     # Services orchestrator
│   ├── collection.rs   # ServiceCollection with failover
│   └── providers/      # WhatsOnChain, ARC, Bitails, BHS
├── wallet/             # Full WalletInterface (28 methods)
│   ├── wallet.rs       # Wallet<S, V> generic struct
│   ├── signer.rs       # BIP-143 sighash + P2PKH/P2PK signing
│   └── certificate_issuance.rs  # BRC-104 protocol
├── monitor/            # Background task daemon
│   ├── daemon.rs       # Task scheduler
│   └── tasks/          # 11 recurring tasks (proofs, broadcast, cleanup)
└── managers/           # Higher-level orchestration
    ├── storage_manager.rs    # Multi-storage sync with lock hierarchy
    ├── cwi_style_wallet_manager.rs  # PBKDF2 multi-profile
    ├── settings_manager.rs   # Persistent settings
    ├── permissions_manager.rs # BRC-98/99 (stub - does NOT enforce)
    └── auth_manager.rs       # WAB integration (skeleton)
```

Each subdirectory has its own `CLAUDE.md` with detailed API docs.

## Key Patterns

**Trait hierarchy (storage):** Reader < Writer < Sync < Provider < MonitorStorage.
Adding methods to `WalletStorageWriter` requires updating ALL 3 implementors:
- `StorageSqlx` (src/storage/sqlx/storage_sqlx.rs)
- `WalletStorageManager` (src/managers/storage_manager.rs) - delegates via `run_as_writer`
- `StorageClient` (src/storage/client/storage_client.rs) - uses `rpc_call`

**Async everywhere:** All I/O is async (tokio). Traits use `#[async_trait]`.

**Entity wire format:** All entity structs use `#[serde(rename_all = "camelCase")]` for cross-SDK compatibility with TypeScript/Go implementations.

**Service failover:** `ServiceCollection<S>` wraps multiple providers, tries each in order, tracks call history.

**Error handling:** `thiserror`-based `Error` enum in `error.rs`, crate-wide `Result<T>` alias.

## Critical Status Values

Transaction lifecycle: `nosend` | `unsigned` | `unprocessed` | `sending` | `unproven` | `completed` | `failed`
- Immediate broadcast uses `"sending"` status (not `"unproven"`)
- ProvenTxReq uses `"unprocessed"` (not `"unmined"`) for immediate broadcast

## Known Issues / Gotchas

- `bsv-sdk` is a local path dep - must have `../rust-sdk` checked out
- `ChainTrackerError` must be imported separately from `ChainTracker`
- `target >= u128::MAX` triggers clippy `absurd_extreme_comparisons` - use `==`
- `WalletPermissionsManager` is a stub - it does NOT enforce permissions
- Doc tests are `ignored` (need runtime setup); real tests are in lib + `tests/`
- 62 clippy warnings (dead_code, minor style) - no errors

## Test Vectors

Cross-implementation test vectors live in `test_vectors/`:
- `storage/` - create_action, list_outputs, list_actions validation
- `transactions/` - merkle_path verification
- `keys/` - BRC-29 derivation, test users (Alice/Bob with known keys)

## Cross-SDK Parity

This crate maintains compatibility with:
- TypeScript: `@bsv/wallet-toolbox` (reference implementation)
- Go: `go-wallet-toolbox`

Shared: test vectors, entity field names (camelCase), API method signatures, status enums.
