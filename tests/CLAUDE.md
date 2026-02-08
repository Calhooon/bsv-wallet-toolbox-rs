# Integration Tests

> End-to-end and cross-SDK integration tests for the BSV Wallet Toolbox.

## Overview

This directory contains 9 integration test files (~6,700 lines total) covering concurrent storage safety, chain reorganizations, monitor daemon lifecycle, service error handling, BEEF format edge cases, double-spend detection, error type construction, and cross-SDK test vector validation. Tests use in-memory SQLite databases for speed and isolation, with `mockito` for HTTP mocking and `MockWalletServices` for service layer stubs.

## Running Tests

```bash
cargo test                           # All tests (unit + integration)
cargo test --test services_tests     # Single integration test file
cargo test --test concurrent_tests   # Concurrent storage tests
cargo test --test test_vectors       # Cross-SDK test vector tests
cargo test -- --test-threads=1       # Serialize tests sharing resources
cargo test -- --ignored              # Run network-dependent tests only
```

Most integration tests require the `sqlite` feature (default). The `valid_create_action_args` module in `test_vectors.rs` requires the `remote` feature.

## Files

| File | Lines | Tests | Feature Gate | Purpose |
|------|-------|-------|--------------|---------|
| `concurrent_tests.rs` | 1053 | 10 | `sqlite` | Thread safety and concurrent access to `StorageSqlx` |
| `reorg_tests.rs` | 765 | 10 | `sqlite` | Chain reorganization handling via `ReorgTask` |
| `monitor_tests.rs` | 527 | 8 | `sqlite` | Monitor daemon lifecycle, task execution, callbacks |
| `error_recovery_tests.rs` | 495 | 10 | none | Service layer HTTP errors, adaptive timeouts, EMA |
| `services_tests.rs` | 949 | ~58 | none | Service creation, provider config, result types, nLockTime |
| `test_vectors.rs` | 2059 | ~40 | none/`remote` | Cross-SDK test vector validation (7 vector files) |
| `beef_edge_cases.rs` | 309 | 5 | none | BEEF format edge cases and serialization roundtrips |
| `double_spend_tests.rs` | 309 | 5 | none | Double-spend detection data structures and status enums |
| `error_path_tests.rs` | 250 | 5 | none | Error variant construction, Display, conversions |

## Test Categories

### Concurrent Storage Tests (`concurrent_tests.rs`)

Tests thread safety of `StorageSqlx` and `WalletStorageManager` under concurrent access. All tests use `StorageSqlx::in_memory()` with a migrated schema.

**Helper functions:**
- `setup_storage()` - Creates in-memory storage with a test user, returns `(Arc<StorageSqlx>, AuthId)`
- `insert_transaction()` - Direct SQL insert into `transactions` table
- `insert_output()` - Direct SQL insert into `outputs` table (includes NOT NULL `provided_by`, `purpose`)
- `insert_proven_tx_req()` - Direct SQL insert into `proven_tx_reqs` (includes NOT NULL `raw_tx`)

**Key tests:**
- `test_concurrent_create_action_both_succeed` - Two threads inserting transactions + outputs simultaneously; verifies no data corruption
- `test_concurrent_list_outputs_during_create_action` - 4 concurrent `list_outputs` calls return consistent results
- `test_concurrent_internalize_action_graceful_failure` - Two threads race to insert same txid; verifies idempotency
- `test_concurrent_abort_and_process_exactly_one_wins` - Racing `abort_action` calls; exactly one succeeds
- `test_race_broadcast_status_vs_abort` - `update_transaction_status_after_broadcast` vs `abort_action` on "sending" tx
- `test_storage_manager_lock_queue_fifo_ordering` - 5 concurrent `insert_certificate` calls succeed; SQLite serializes writes
- `test_concurrent_certificate_insert_and_relinquish` - Races certificate insert vs `relinquish_certificate`; verifies soft-delete
- `test_parallel_create_action_competing_for_same_utxo` - Atomic `UPDATE ... WHERE spent_by IS NULL` prevents double-spend
- `test_lock_queue_concurrent_operations_complete` - `WalletStorageManager::run_as_writer` serializes access; second writer blocked
- `test_reader_writer_interleaving` - 5 readers + 5 writers complete without blocking

### Reorg Tests (`reorg_tests.rs`)

Tests `ReorgTask` behavior when blockchain blocks are deactivated. Uses a custom `MockServices` with configurable merkle path responses.

**Mock setup:**
- `MockServices::with_valid_proof()` - Returns a valid `GetMerklePathResult`
- `MockServices::with_no_proof()` - Returns empty merkle path (simulates proof gone after reorg)
- Implements full `WalletServices` trait with stub methods

**Helper functions:**
- `setup_monitor_storage()` - Creates in-memory storage with test user
- `insert_proven_tx_req()` - Direct SQL insert into `proven_tx_reqs`
- `insert_transaction()` - Direct SQL insert into `transactions`
- `insert_proven_tx()` - Direct SQL insert into `proven_txs` with block hash/height

**Key tests:**
- `test_reorg_single_block_at_tip` - Queues one deactivated header; verifies 10-minute delay before processing
- `test_reorg_depth_3_blocks` - Three deactivated headers at different heights
- `test_reorg_transaction_proof_reverification` - Completed tx in reorged block; verifies `FindProvenTxReqsArgs` query
- `test_reorg_transaction_in_both_chains_no_status_change` - Tx confirmed in both old and new chain stays "completed"
- `test_reorg_delay_is_respected` - Confirms recently queued headers are not processed (10-min delay)
- `test_reorg_retry_count_mechanism` - `DeactivatedHeader.retry_count` increments; max 3 retries
- `test_reorg_pending_count_accuracy` - Verifies `pending_count()` increments correctly for 5 queued headers
- `test_reorg_empty_queue_noop` - Empty queue produces 0 items processed, 0 errors, 0 service calls
- `test_reorg_concurrent_queue_access` - 20 concurrent `queue_deactivated_header` calls; verifies thread safety
- `test_reorg_completed_proof_reverification_setup` - 5 txs with proofs, queue 3-block reorg; verifies task metadata (`name()`, `default_interval()`)

### Monitor Tests (`monitor_tests.rs`)

Integration tests for `Monitor<S, V>` lifecycle and task execution. Uses `MockWalletServices` from the `services::mock` module.

**Helper functions:**
- `setup_storage_and_services()` - Creates in-memory storage + mock services, calls `set_services()`
- `all_tasks_disabled()` - `MonitorOptions` with all 11 tasks disabled via `TaskConfig::disabled()`

**Key tests:**
- `start_stop_lifecycle` - `Monitor::start()` sets `is_running`, `Monitor::stop()` clears it
- `double_start_error` - Second `start()` returns error containing "already running"
- `run_once_empty_storage` - All tasks run on empty DB with 0 items processed, no errors; expects >= 10 task results
- `fail_abandoned_integration` - Inserts old "unsigned" tx (1 hour ago), runs `FailAbandonedTask` with 1s timeout, verifies status becomes "failed"
- `check_for_proofs_integration` - Inserts "unmined" `ProvenTxReq`, mock returns merkle path, verifies `items_processed > 0`
- `send_waiting_integration` - Inserts "unsent" `ProvenTxReq`, mock returns broadcast success; verifies no fatal errors
- `custom_task_config` - Enables only `clock` task; verifies only that task appears in results
- `monitor_options_callbacks` - Wires `on_tx_broadcasted` and `on_tx_proven` callbacks; verifies invocation via `TransactionStatusUpdate`

### Error Recovery Tests (`error_recovery_tests.rs`)

Tests service layer error handling, adaptive timeouts, and EMA convergence. Uses `mockito` for HTTP mocking and `MockWalletServices` for mock providers.

**Key tests:**
- `test_service_500_error` - ARC provider wraps HTTP 500 as `PostBeefResult` with `service_error: true`
- `test_service_429_rate_limit` - 429 response wrapped similarly
- `test_connection_failure` - Unreachable host (RFC 5737 TEST-NET) produces service error
- `test_json_error_response` - ARC-specific status 465 (FEE_TOO_LOW)
- `test_all_providers_fail_collection` - 3 mock providers all fail; verifies `ServiceCollection` failover + call history
- `test_adaptive_timeout_defaults` - `AdaptiveTimeoutConfig` defaults: min=5s, max=60s, multiplier=2.0, initial=30s
- `test_adaptive_timeout_calculation` - EMA-based timeout: first sample, then 70/30 weighted updates
- `test_timeout_bounds` - Min/max clamping enforced
- `test_ema_calculation` - EMA convergence: 50 samples of 500ms converges within 5ms of target
- `test_partial_truncated_response` - Truncated JSON body handled gracefully (no panic)

### Services Tests (`services_tests.rs`)

Broad integration tests for service creation, provider configuration, collection behavior, result type serialization, and nLockTime finality logic.

**Categories:**
- **Service creation:** `Services::mainnet()`, `Services::testnet()`, `Services::with_options()`
- **Provider config:** `WhatsOnChain`, `Arc` (Taal/GorillaPool), `Bitails` construction and API key setup
- **Collection ops:** `ServiceCollection` add/next/reset/remove/move_to_last, call tracking
- **Result types:** Serialization of `GetRawTxResult`, `GetMerklePathResult`, `PostBeefResult`, `GetUtxoStatusResult`, `GetStatusForTxidsResult`, `GetScriptHashHistoryResult`, `GetBeefResult`
- **Services options:** `ServicesOptions::default()`, `mainnet()`, `testnet()`, builder pattern with `with_woc_api_key`, `with_arc`, `with_gorillapool`
- **nLockTime:** Block height vs timestamp threshold (500,000,000), sequence finality, `NLockTimeInput::from_hex_tx` parsing real BSV transactions, `NLockTimeInput::from_lock_time`, `n_lock_time_is_final_for_tx` with final/non-final sequences
- **Network tests (ignored):** `test_whatsonchain_get_chain_info`, `test_whatsonchain_get_exchange_rate`, `test_services_get_height`, `test_services_get_beef_*`, `test_services_n_lock_time_finality_integration`

### Test Vectors (`test_vectors.rs`)

Cross-SDK test vector validation ensuring Rust implementation matches TypeScript (`@bsv/wallet-toolbox`) and Go (`go-wallet-toolbox`).

**Vector files used (from `test_vectors/`):**

| File | Module | Tests |
|------|--------|-------|
| `storage/create_action/validation.json` | `create_action_validation` | 14 validation error cases (description, labels, outputs, inputs, duplicates) |
| `storage/create_action/defaults.json` | `create_action_defaults` | Default args structure, P2PKH script, BRC-29 custom instructions |
| `storage/list_outputs/validation.json` | `list_outputs_validation` | Empty basket, valid paging, full args, non-existent basket |
| `storage/list_actions/validation.json` | `list_actions_validation` | Labels, max pagination, default wallet args, all includes |
| `keys/brc29.json` | `brc29_key_derivation` | Key pair consistency, ECDH symmetry, mainnet/testnet addresses |
| `keys/test_users.json` | `test_users` | Alice/Bob key pairs, storage config keys, pagination defaults |
| `transactions/merkle_path.json` | `merkle_path` | TSC proof to MerklePath conversion (even/odd index, duplicate marker, errors) |

**Key cross-vector tests:**
- `tv_brc29_self_and_counterparty_produce_same_address` - BRC-29 symmetry: sender(forCounterparty) == recipient(forSelf)
- `tv_alice_is_brc29_sender` - Alice's keys in `test_users.json` match sender in `brc29.json`
- `tv_create_action_defaults_match_validation_valid_case` - Default args from `defaults.json` match valid case in `validation.json`
- `tv_pagination_constants_consistent` - `MaxPaginationLimit: 10000` consistent across all vector files

**Feature-gated module:**
- `valid_create_action_args` (requires `remote` feature) - Verifies `ValidCreateActionArgs::from()` flag derivation matches test vectors

### BEEF Edge Cases (`beef_edge_cases.rs`)

Tests `GetBeefResult`, `PostBeefResult`, `PostTxResultForTxid`, and `BeefVerificationMode` data structures. Covers serialization roundtrips, optional field handling (`skip_serializing_if`), and all verification modes (`Strict`, `TrustKnown`, `Disabled`).

**Key tests:**
- `test_beef_empty_bytes` - `GetBeefResult` with `beef: None`, error message, serialization roundtrip
- `test_beef_invalid_version` - Invalid BEEF version byte (0x99), bytes preserved through serde
- `test_beef_result_types` - Full success, overall error, partial success (mixed txid results), already-mined result
- `test_atomic_beef_missing_fields` - Three cases: no proof, failed retrieval, successful with proof; all serialize cleanly
- `test_beef_broadcast_result_serialization` - `PostBeefResult` with notes, `GetBeefResult` `skip_serializing_if` for error, `BeefVerificationMode` all 3 variants roundtrip, default is `Strict`

### Double-Spend Tests (`double_spend_tests.rs`)

Tests double-spend detection structures: `PostTxResultForTxid.double_spend`, `competing_txs`, `TransactionStatus` enum variants, and `ProvenTxReqStatus::DoubleSpend`. Verifies serde roundtrips for all status variants.

**Key tests:**
- `test_post_beef_result_double_spend_fields` - Double-spend flag, competing_txs populated, `service_error` is false
- `test_post_beef_result_success` - Successful broadcast with no double-spend
- `test_post_beef_result_serialization` - JSON roundtrip preserving `doubleSpend` and `competingTxs` fields
- `test_transaction_status_values` - All 9 `TransactionStatus` variants roundtrip (`nosend`..`unfail`); all 15 `ProvenTxReqStatus` variants roundtrip
- `test_double_spend_result_handling` - Mixed result (success + double-spend + service error); pattern-matching to filter each category

### Error Path Tests (`error_path_tests.rs`)

Verifies all `Error` enum variants can be constructed and produce useful `Display` messages. Tests `Error::from` conversions for `serde_json::Error` and `std::io::Error`. Confirms `Error` is `Send + Sync`.

**Key tests:**
- `test_error_variants_exist` - Constructs all expected variants: `StorageNotAvailable`, `StorageError`, `DatabaseError`, `MigrationError`, `NotFound`, `Duplicate`, `AuthenticationRequired`, `InvalidIdentityKey`, `UserNotFound`, `AccessDenied`, `ServiceError`, `NetworkError`, `BroadcastFailed`, `NoServicesAvailable`, `TransactionError`, `InvalidTransactionStatus`, `InsufficientFunds`, `ValidationError`, `InvalidArgument`, `InvalidOperation`, `SyncError`, `SyncConflict`, `LockTimeout`, `HttpError`, `Internal`
- `test_error_display` - `NotFound` includes entity+id, `InsufficientFunds` includes both amounts, Debug works
- `test_error_conversion_from_sdk` - `Error::Internal` implements `std::error::Error`, can be matched
- `test_error_conversion_from_json` - `serde_json::Error` converts to `Error::JsonError`
- `test_validation_error_construction` - `ValidationError`, `InvalidArgument`, `InvalidOperation` messages; `Error` is `Send + Sync`; `std::io::Error` converts to `Error::IoError`

## Common Patterns

**In-memory storage setup:**
```rust
let storage = StorageSqlx::in_memory().await.unwrap();
storage.migrate("test-storage", &"0".repeat(64)).await.unwrap();
storage.make_available().await.unwrap();
let (user, _) = storage.find_or_insert_user(&"a".repeat(66)).await.unwrap();
let auth = AuthId::with_user_id(&"a".repeat(66), user.user_id);
```

**Direct SQL inserts:** Many tests bypass the storage API and insert directly via `sqlx::query()` to set up specific database states. Required NOT NULL columns: `outputs.provided_by`, `outputs.purpose`, `proven_tx_reqs.raw_tx`.

**Mock services:** Two approaches are used:
1. `MockWalletServices::builder()` - Builder pattern from `services::mock` module (monitor/error recovery tests)
2. Custom `MockServices` struct implementing `WalletServices` trait directly (reorg tests)

**Test vector loading:** `load_test_vectors()` reads JSON from `test_vectors/` relative to `CARGO_MANIFEST_DIR`.

## Related

- [Root CLAUDE.md](../CLAUDE.md) - Project overview, build commands, architecture
- [Storage CLAUDE.md](../src/storage/CLAUDE.md) - Storage trait hierarchy and entity details
- [Services CLAUDE.md](../src/services/CLAUDE.md) - Service providers and collection failover
- [Monitor CLAUDE.md](../src/monitor/CLAUDE.md) - Monitor daemon and task types
- [Test vectors](../test_vectors/) - Shared cross-SDK validation data
