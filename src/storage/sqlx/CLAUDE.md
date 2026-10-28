# SQLx Storage Module
> SQLite-based persistent storage implementation for wallet state.

## Overview

The stored sendWith path in `process_action.rs` resolves user-scoped handles, rebuilds BEEF before the writer, commits `unsent`/`sending`, posts without the writer, then records every instance's word (the `processActionPost` note on `proven_tx_reqs.history`, `record_stored_post_words`: `reqStatus` and `attempts` as the request stands before the status writes, the per-instance `words`, the reference's four counts, `serviceError` when no broadcaster answered; capped at 32 notes). The one-call composition (`create_action` with `signAndProcess`) posts from the wallet and hands the per-instance words to `update_transaction_status_after_post`, which writes the same note before the status writes (#187, the owner's ruling 18.5 (a)); signed noSend outputs retain their spendable flag as at the reference pin (#66, B3).

This module provides a production-ready storage backend for BSV wallet state using SQLx with SQLite. It implements the full `WalletStorageProvider` trait hierarchy, enabling persistent storage of transactions, outputs, certificates, and synchronization state. The implementation mirrors the TypeScript `@bsv/wallet-toolbox` storage architecture.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module definition and public exports (44 lines) |
| `storage_sqlx/mod.rs` | StorageSqlx, shared pools and settings, pragmas, writer discipline |
| `storage_sqlx/reader.rs`, `storage_sqlx/writer.rs`, `storage_sqlx/monitor.rs` | One implementation per storage trait, delegating to the entity modules |
| `storage_sqlx/users.rs`, `storage_sqlx/settings.rs`, `storage_sqlx/sync_state.rs` | User records, storage configuration, and synchronization state |
| `storage_sqlx/outputs.rs`, `storage_sqlx/output_selection.rs`, `storage_sqlx/list_outputs.rs` | Output queries, allocation, and wallet output listing |
| `storage_sqlx/baskets.rs`, `storage_sqlx/labels.rs`, `storage_sqlx/tags.rs` | Output baskets and transaction/output labels |
| `storage_sqlx/transactions.rs` | Wallet action listing, raw transaction reads, and abandoned actions |
| `storage_sqlx/certificates.rs` | Certificate queries, fields, atomic insertion, and relinquishing |
| `storage_sqlx/proven_tx_reqs.rs` | Proof-request queries, adoption, and status updates |
| `storage_sqlx/proven_txs.rs`, `storage_sqlx/stale_proofs.rs` | Validated proof storage, anchors, and stale-proof demotion |
| `storage_sqlx/proof_requests.rs`, `storage_sqlx/beef_compaction.rs` | The monitor proof-fetch pass, attempt accounting, and input BEEF compaction |
| `storage_sqlx/send_waiting.rs`, `storage_sqlx/competitors.rs`, `storage_sqlx/retirement.rs` | Broadcast retries and hints, competitor proof checks, and retirement |
| `storage_sqlx/maintenance.rs`, `storage_sqlx/unfail.rs` | Status repair, purge, and false-failure recovery |
| `storage_sqlx/commissions.rs`, `storage_sqlx/monitor_events.rs` | Commissions and monitor event persistence |
| `storage_sqlx/core_tests.rs`, `storage_sqlx/certificate_tests.rs`, `storage_sqlx/commission_tests.rs`, `storage_sqlx/monitor_event_tests.rs`, `storage_sqlx/proof_request_tests.rs`, `storage_sqlx/transaction_tests.rs`, `storage_sqlx/unfail_tests.rs`, `storage_sqlx/unfail_empty_tests.rs`, `storage_sqlx/user_tests.rs`, `storage_sqlx/validation_tests.rs` | Existing scenarios split by subject; entity modules also retain their tests |
| `create_action/mod.rs` | Private door, write transaction, signing callback, and result assembly |
| `create_action/allocation.rs` | Change-coin selection, reservations, and precheck |
| `create_action/must_spend.rs` | The offering call's must-spend list: the listed checks and the named coin's allocation ahead of the pick (#353) |
| `create_action/change.rs` | Change planning, funding, and starvation loop |
| `create_action/fees.rs` | Transaction sizes and fee calculations |
| `create_action/signing.rs` | Caller signature commitments and funding permission |
| `create_action/validation.rs` | Create-action argument validation |
| `create_action/inputs.rs` | Caller input preparation and spent-input writes |
| `create_action/outputs.rs` | Output preparation, action records, baskets, labels, and tags |
| `create_action/tracker.rs` | Precheck tracker answers, recording tracker, and cached tracker |
| `create_action/beef.rs` | Input BEEF assembly, known entries, and ancestry gaps |
| `create_action/beef_graph.rs` | Proof linking, graph pruning, and stored BEEF validation/compaction |
| `create_action/beef_storage.rs` | Stored transaction/proof lookup and raw-transaction parsing |
| `create_action/beef_walk.rs` | Shared ancestor walk |
| `create_action/broadcast.rs` | Broadcast BEEF rebuild and provider fallback |
| `create_action/tests/` | Existing unit and integration cases by concern, shared helpers, and door scenarios |
| `process_action.rs` | Signed transaction processing (1884 lines) |
| `abort_action.rs` | Transaction abort/cancellation (#165): refuses `unproven` and every stored accepting word; `sending` with no acceptance stays abortable (inputs released, own outputs dead, memory `rejected`, descendants retired); the abort makes no header-service or explorer call (`wallet.md:98`, `tracker.md:25`, broadcaster B4 (d)) |
| `internalize_action.rs` | External transaction internalization (1343 lines) |
| `label.rs` | Label normalization as validateLabel leaves a label, ECMAScript's trim and lowercase, for both doors and list filters (#379, #392) |
| `sync.rs` | Multi-storage synchronization (2713 lines) |
| `beef_verification.rs` | BEEF merkle proof verification (989 lines) |
| `migrations/001_initial.sql` | Initial schema with 16 tables (284 lines) |
| `migrations/002_broadcast_seen.sql` | `broadcast_seen` + `broadcast_prefs` (broadcast acceptance memory, 0.3.56); applied on `migrate()` AND `make_available()` |
| `migrations/003_locked_input_checks.sql` | `locked_input_checks` (inputs THE RELEASE RULE kept locked, re-checked with backoff, 0.3.59); applied on `migrate()` AND `make_available()` |
| `broadcast_seen.rs` | `SqlxBroadcastMemory` (`BroadcastMemory` over the pool), `StorageSqlx::{record_broadcast_seen, broadcast_seen_for, broadcast_seen_any, get/set_broadcast_pref}` |

## Key Exports

### `StorageSqlx`
The main storage provider struct implementing all wallet storage traits.
`storage::sqlx::StorageSqlxOptions { rng_seed: Option<[u8; 32]> }` configures `new_with_options` and `open_with_options`: `None` uses OS entropy, and `Some(seed)` repeats references and change derivations from the store's shared stream; no environment variable supplies the seed.

```rust
// impl Clone by hand: every field shared, none copied
pub struct StorageSqlx {
    reader: Pool<Sqlite>,                            // several connections, PRAGMA query_only = ON
    writer: Pool<Sqlite>,                            // exactly one connection: every write
    settings: Arc<std::sync::RwLock<Option<TableSettings>>>,
    storage_identity_key: Arc<std::sync::RwLock<String>>,
    storage_name: Arc<std::sync::RwLock<String>>,
    chain_tracker: Arc<RwLock<Option<Arc<dyn ChainTracker>>>>,  // For BEEF verification
    services: Arc<std::sync::RwLock<Option<Arc<dyn WalletServices>>>>,
    active_transactions: Arc<RwLock<HashMap<u64, ()>>>,         // TrxToken scope tracking
    task_locks: Arc<RwLock<HashMap<String, (String, Instant)>>>, // Multi-instance task locking
    broadcast_memory: Arc<SqlxBroadcastMemory>,
    in_flight_picks: Arc<InFlightPicks>,             // createAction's in-flight reservations
    // #[cfg(test)] busy hooks: Arc<AtomicBool> each
    // #[cfg(test)] writer_acquires: Arc<AtomicUsize>, one per store, shared by clones; counts writer handouts (#244)
}
```

**One store per file per process (the CLI review of 2026-10-21).** `StorageSqlx` is `Clone`: a clone is a second handle over the same store, the same writer pool (one connection), the same reader pool, and every other field behind the same `Arc` (settings, identity key and name, chain tracker, services, broadcast memory, in-flight reservations, trx tokens, task locks, the tests' busy hooks), so a change through one handle is seen through every other. Give a `Wallet` and a `Monitor` (or two `Wallet`s) clones of ONE store: `let monitor_store = Arc::new(store.clone()); Wallet::new(key, store, services)`. Never `StorageSqlx::open` the same file twice in one process: that is two stores, two writer pools whose writers meet in SQLite and wait on its busy handler (`busy_timeout`, then `StoreBusy { SqliteBusy }`) instead of queueing on one writer, and two reservation sets, so a coin one reserves is not held for the other's pick (the bounded re-check catches the collision, `StoreBusy { MovedUnderUs }` the second time). A new field must be shared the same way (an `Arc`, cloned in `impl Clone`). The cached settings, identity key and name are written only when the value differs (`cache_settings`, `replace_if_changed`): `get_settings`, `storage_identity_key` and `storage_name` lend references out of their locks, shared by every handle.

**Constructor methods:**
- `new(database_url: &str)` - Create from SQLite URL (e.g., `"sqlite:wallet.db"`)
- `in_memory()` - Create in-memory database (useful for testing)
- `open(path: &str)` - Open file-based database (creates if not exists)
- `pool()` - The READER pool (`query_only`: a write here fails `attempt to write a readonly database`)
- `writer()` - The writer pool, one connection: single-statement writes
- `write_tx()` - A write transaction on the writer, `BEGIN IMMEDIATE`; the only way to open one (`clippy.toml` disallows `Pool::begin`, `Pool::begin_with`, `Connection::begin`, `Acquire::begin`)

**Write lock rules (bsv-wallet-cli #44):** every write goes to `writer()` or a `write_tx()`. Nothing awaits a service or a chain tracker while holding either: ask the network, then write. The BEEF walk reads on a reader connection and writes (a fetched proof, a read check) through the `writer` it is passed; never call it with the writer held (the pool has one connection: it would wait on itself). `create_action` is one step (option B): before the lock it checks the caller's `inputBEEF` roots, picks the change coins on a reader as the allocation would (`ChangePick::DryRun`, marking nothing, never a coin another call of this process has reserved: `InFlightPicks` / `PickReservation`, released when the call returns) and assembles their BEEF once (a coin whose BEEF cannot be built is skipped and the coins picked again; none left that covers is `Error::UnspendableCoin`, `Unspendable coin: txid.vout: ...`), every stored root read against the tracker (`RecordingTracker`); then one `BEGIN IMMEDIATE` transaction writes the action, allocates only those coins (`ChangePick::Within`) and assembles the BEEF from stored bytes with the answers in hand (`CachedTracker`, `writer` `None`). A coin taken or a root unread meanwhile rolls back and the pre-check runs once more; the second time a coin taken refuses `StoreBusy { MovedUnderUs }`, never `InsufficientFunds`, and a root unread refuses `ServiceError` naming its height (the header service was not read; the store did not move). With `signAndProcess` the door is ONE transaction (`create_action_at_door` / `create_action_at_door_internal`, `CreateActionDoor { offer, must_spend, sign_and_process }`): after the create step's rows, the wallet's signer (`DoorSigner`, local CPU, nothing awaited) and `process_action_writes` (processAction's writes on the same connection) run before the one commit; any refusal (`DoorRefusal::{Create, Sign, Process}`) rolls everything back, so a busy anywhere wrote nothing. `process_action_internal` is begin, `process_action_writes`, commit (its own one transaction, for `signAction`). The broadcast comes after the commit; a status write after a post that fails is never the call's error (`storage::broadcast::post_broadcast_status_write_failed`, marker `post_broadcast_status_write_failed`): the row stays `sending` / `unsent` and `send_waiting_transactions` heals it. A broadcast `abort_action` commits the root, then retires its descendants; an error there is logged (`abort_action::post_commit_retire_failed`, marker `post_commit_retire_failed`) and the call answers its success: `MonitorStorage::review_status` (check 4) retires the unproven descendants of a failed root whose req is `invalid` / `doubleSpend` with no chain evidence. Never return `StoreBusy` after a committed write. A pre-check uncovered only because other calls hold the coins (`InFlightPicks`; planned again with a private `PickReservation`, covered) refuses `StoreBusy { InFlight }` at once, never `InsufficientFunds`. The BEEF the door assembles ends at a stored proof; its raw segment is the wallet's own unmined chain (the host offers unmined own change by default), bounded only by that chain's length: no size gate on purpose (fix the cause, never gate on size); the monitor bounds the chain by fetching proofs. The host's offer (`CreateActionDoor::offer`, `StorageSqlx::create_action_offering`): `ChangePick::DryRun { offer }` picks only offered output ids, `ChangePick::Within` takes the pre-checked coins intersected with the offer; an offer that does not cover, or an empty one, is `InsufficientFunds`. The must-spend list (`CreateActionDoor::must_spend`, the last parameter of `StorageSqlx::create_action_offering`, #353): `must_spend::check_listed` refuses a named id outside the offer or named twice before anything is read; `generate_change`'s funding loop takes each named coin ahead of the pick (`allocate_named_change_input`: the row read, refused by name when not the user's, not wallet-managed change of the default basket under `MANAGED_CHANGE_CLAUSE`, spent or reserved, a received coin whose parent is not `completed` without the action's rule (R1 (d), `ChangePick::allows_received`), or a parent neither `completed` nor `unproven`; the dry run reserves it in `PickReservation`, `StoreBusy { InFlight }` when another call holds it; under the lock it is marked `spent_by` as a picked coin), each counted as a picked coin, then the pick while the action is short, the formula unchanged; with no list the loop is the reference's `while fee_excess < 0`. A named coin whose BEEF cannot be built is `UnspendableCoin`, never picked around. A caller-complete transaction takes the list only when its signatures permit funding, else `mustSpend: a caller input is signed over the whole transaction`. Every `mustSpend` text is the port's own: the reference names a transaction's inputs through `inputs` alone, and the offering call with its list is the port's composition. The action writes nothing on a refusal (the pre-check's root-check records, demotions and the default basket may be written). No undo exists. `create_action` asks no provider (`build_input_beef` takes no storage): an ancestor not stored is a refusal naming it; the broadcast rebuild and `list_outputs` with whole transactions still fetch. A sqlx error with SQLite primary code 5 (517 included) converts to `Error::StoreBusy { reason: SqliteBusy { code } }` ("Store busy: sqlite code N"); code 6 does not. The writer not acquired within `STORE_BUSY_CEILING_MS` (the writer pool's `acquire_timeout`) is `StoreBusy { reason: WriterNotAcquired }` at the acquire site only: `begin_immediate` (every `write_tx`) and `acquire_writer` / `writer_conn()` (every single-statement write: `.execute(&mut *self.writer_conn().await?)`, never `.execute(self.writer())` in production code). A reader-pool `PoolTimedOut` stays the plain `SqlxError`. A begin or commit (`begin_immediate`, `commit()`) that fails for anything but busy is `DatabaseError`.

**Arcade callback status writes (#205):** `mark_transaction_seen_on_network_by`
in `storage_sqlx/monitor.rs` opens one `write_tx()` for both status updates
(`proven_tx_reqs` to `unmined`, `transactions` to `unproven`) and the provider's
broadcast hint through `record_broadcast_status_on`. The wrapper
`mark_transaction_seen_on_network` delegates to it. The SEEN, IN_BLOCK and
MINED arms therefore acquire one writer and take one `busy_timeout`, returning
typed `StoreBusy` without writes when the lock is held (#44). A status error
or cancellation before commit rolls back the group; hint-write errors keep
the logged, never returned policy of `record_broadcast_status_quiet` (#201).
No service or tracker is awaited in this transaction. MINED's later ingest
still checks the tracker outside a writer, then opens its own transaction;
a busy there can follow committed statuses and delivery retry is idempotent.
The timing tests cover known and unknown txids below 1.5 times a 200 ms
timeout; the previous two waits could exceed the webhook sender's 10 s limit
with the default 5 s store timeout. The host deadline remains CLI #249's job.

**ChainTracker methods:**
- `set_chain_tracker(tracker)` - Set ChainTracker for BEEF and proof verification. Without one NO merkle proof is stored: `ingest_merkle_proof` returns `ProofIngestOutcome::TrackerUnavailable`, the BEEF walk fetches none, `internalize_action` holds (`ServiceError`, nothing stored): since #103 the door needs the tracker for the verifier's roots and tip, and a missing tracker is could-not-look, never a verdict
- `clear_chain_tracker()` - Remove the ChainTracker (from then on no proof is stored)
- `get_chain_tracker()` - Internal: get current ChainTracker if set

**Proof root checks (migration 005, `proof_root_checks.rs`):** every proof the store funnel writes is recorded as checked. A `proven_txs` row with no record (stored before 005, or merged by sync) is checked once when a BEEF read needs it (the walk, stored-BEEF compaction) and demoted when the tracker refutes it; compaction attaches it only once a tracker confirmed it (no tracker or a tracker fault: not attached, as the ingest refuses); a checked row is demoted only by the reorg and review tasks.

**Services methods:**
- `set_services(services)` - Set WalletServices for blockchain operations (required before operations needing chain access)
- `get_services()` - Get WalletServices (returns error if not set via `set_services`)

### `DEFAULT_MAX_OUTPUT_SCRIPT`
Constant defining maximum script length stored inline (10,000 bytes). Scripts longer than this are retrieved from raw transactions.

### `entity_names`
Module containing entity name constants for sync operations (exported from `sync.rs`).

### BEEF Verification Functions
Exported from `beef_verification.rs`:
- `verify_beef_merkle_proofs(beef, chain_tracker, mode, known_txids)` - Verify all merkle proofs in a BEEF
- `verify_txid_merkle_proof(beef, txid, chain_tracker)` - Verify a single transaction's merkle proof

## Trait Implementations

`StorageSqlx` implements the full trait hierarchy defined in `src/storage/traits.rs`:

```
WalletStorageReader     - Read operations
        ↑
WalletStorageWriter     - Write operations (+ TrxToken scopes)
        ↑
WalletStorageSync       - Sync operations
        ↑
WalletStorageProvider   - Full provider interface
        +
MonitorStorage          - Background monitoring operations (+ task locking)
```

### WalletStorageReader Methods
| Method | Description |
|--------|-------------|
| `is_available()` | Check if storage is initialized |
| `get_settings()` | Get cached settings (OnceLock fallback to default) |
| `get_services()` | Get WalletServices instance (error if not set) |
| `find_certificates()` | Query certificates by certifier/type |
| `find_output_baskets()` | Query output baskets by name |
| `find_outputs()` | Query outputs with filters |
| `find_proven_tx_reqs()` | Query proof requests by status |
| `list_actions()` | List transactions with labels, inputs, outputs (label ANY/ALL filtering via CTE); inputs by output id, each `sequenceNumber` and `unlockingScript` from the action's parsed raw transaction (#307) |
| `list_certificates()` | List certificates with field values and keyring |
| `list_outputs()` | List spendable outputs with tags and labels (tag ANY/ALL filtering via CTE) |

### WalletStorageWriter Methods
| Method | Description |
|--------|-------------|
| `make_available()` | Initialize and load settings |
| `migrate()` | Run schema migrations |
| `destroy()` | Drop all tables |
| `find_or_insert_user()` | Get or create user by identity key |
| `insert_certificate()` | Insert new certificate |
| `insert_certificate_field()` | Insert certificate field with keyring |
| `relinquish_certificate()` | Soft-delete certificate |
| `relinquish_output()` | Remove output from basket |
| `abort_action()` | Abort pending transaction, release locked outputs (delegates to abort_action.rs) |
| `create_action()` | Create new transaction with inputs/outputs (delegates to create_action/mod.rs) |
| `process_action()` | Process signed transaction (delegates to process_action.rs) |
| `internalize_action()` | Internalize external transaction into wallet (delegates to internalize_action.rs) |
| `update_transaction_status_after_broadcast()` | Update tx status after broadcast success/failure (delegates to process_action.rs) |
| `update_transaction_status_after_post()` | The composition's post: the `processActionPost` note with the per-instance words, then the statuses as `update_transaction_status_after_broadcast()` (#187) |
| `review_status()` | Sync proven_tx_req completion status to transaction table |
| `purge_data()` | Purge old failed/invalid proven_tx_reqs, clean completed raw data |
| `begin_transaction()` | Begin a TrxToken transaction scope; returns opaque u64 token |
| `commit_transaction()` | Commit a TrxToken transaction scope |
| `rollback_transaction()` | Rollback a TrxToken transaction scope |

### WalletStorageSync Methods
| Method | Description |
|--------|-------------|
| `find_or_insert_sync_state()` | Get or create sync state |
| `set_active()` | Set user's active storage |
| `get_sync_chunk()` | Get data chunk for synchronization |
| `process_sync_chunk()` | Apply received sync chunk with upsert logic |

### WalletStorageProvider Methods
| Method | Description |
|--------|-------------|
| `storage_identity_key()` | Get storage identity key |
| `storage_name()` | Get storage name |
| `set_services()` | Set WalletServices for blockchain operations |

### MonitorStorage Methods
| Method | Description |
|--------|-------------|
| `synchronize_transaction_statuses()` | FIRST adopt proof-less transactions that have no req (`adopt_unproven_transactions`: link from an existing `proven_txs` row, else create an `unmined` req, bounded per pass), THEN query unmined/unknown/callback/sending/unconfirmed proven_tx_reqs, call `services.get_merkle_path()`, update proven_txs/proven_tx_reqs/transactions on proof found; every pass that finds no proof is an attempt; past the reference's limit (144 mainnet, 10 otherwise) the count writes no word and the req is asked again every pass (bsv-stack-lean #66; the reference writes it off, zanaadu-v2#368) |
| `send_waiting_transactions()` | Query unsent/sending proven_tx_reqs older than min age, build BEEF from raw_tx + input_beef, broadcast via `services.post_beef()`; every word short of an acceptance is a hint recorded on the request and re-asked on the cadence, a named competitor queued for a proof ask (bsv-stack-lean #65, #66) |
| `abort_abandoned()` | Query unsigned/unprocessed outgoing transactions older than timeout, abort each via `abort_action` |
| `un_fail()` | Query unfail proven_tx_reqs, check chain for merkle path via services, restore to unmined/unproven if found, mark invalid if not |
| `review_status()` | Monitor-level status review across all users (no AuthId required); an invalid request fails its transaction and voids its created outputs atomically, leaving consumed inputs unchanged; every terminal failed creator's outputs still `spendable = 1` are voided each pass, so a store from 0.13.0 heals on the pass after upgrade (#175) |
| `purge_data()` | Monitor-level purge (no AuthId required) |
| `try_acquire_task_lock()` | Acquire in-memory task lock for multi-instance monitor support (lock TTL = 2x task interval) |
| `release_task_lock()` | Release a previously acquired task lock |
| `update_proven_tx_req_status()` | Update proven_tx_req status directly |
| `mark_transaction_seen_on_network()` / `mark_transaction_seen_on_network_by()` | One transaction for both statuses and the provider hint; one busy wait on the SEEN, IN_BLOCK and MINED callback status paths (#205) |

### Utility Methods (on `StorageSqlx` directly)
| Method | Description |
|--------|-------------|
| `find_user()` | Find user by identity key |
| `find_user_by_id()` | Find user by ID |
| `insert_user()` | Insert a new user |
| `update_user_active_storage()` | Update user's active storage |
| `find_outputs_internal()` | User-scoped full output rows, including stored origin and spending metadata (no auth) |
| `find_output_by_outpoint()` | User-scoped full output row by txid + vout |
| `allocate_change_input()` | Allocate a change input for transaction |
| `count_change_inputs()` | Count available change inputs |
| `find_output_baskets_internal()` | Internal basket query |
| `find_or_create_default_basket()` | Find or create the "default" basket |
| `find_certificates_internal()` | Internal certificate query |
| `find_certificate_fields()` | Get fields for a certificate |
| `find_proven_tx_reqs_internal()` | Internal proven_tx_req query |
| `get_proven_or_raw_tx()` | Raw tx bytes from `proven_txs`, else a `proven_tx_reqs` row in the reference's six statuses; never `transactions.raw_tx` (#351) |
| `find_or_insert_sync_state_internal()` | Internal sync state lookup |
| `get_labels_for_transaction_id()` | Get labels for a transaction |
| `get_tags_for_output_id()` | Get tags for an output |
| `admin_stats()` | Get administrative statistics |

`StorageSqlx::allocate_change_input()` in `storage_sqlx/output_selection.rs`
returns the claimed row's `type` column as `TableOutput.output_type`.
The create door uses the separate allocator in `create_action/allocation.rs`,
whose inputs are returned with the `P2PKH` type.

Both allocators and both counts take only wallet-managed change, the
reference's filters (`StorageKnex.allocateChangeInput` 2241-2250 at
ts-stack edf6e03; `MANAGED_CHANGE_CLAUSE`, #295): `change = 1`,
`type = 'P2PKH'`, `provided_by = 'storage'`, `purpose` `change` or
`receive` (the port's spelling of a received wallet payment, #169), a
non-empty `derivation_prefix` and `derivation_suffix`, spendable and
`spent_by` null. The door takes a `completed` or `unproven` parent; the
public allocator takes those and `sending` unless `exclude_sending`
(2230-2231, 2256). A caller-provided coin, a non-P2PKH coin, a coin with
any other purpose (a synced row without one is stored `''`, #313) or no
derivation fields is never picked as change.

The two allocators share a second clause, `RECEIVED_COIN_CLAUSE` (R1 (d)
of the wallet charter, #169, #304): a received coin (`purpose = 'receive'`)
whose parent is not `completed` is taken only under the rule its one `?`
binds. The door binds the action's `underReceivedRule` label, matched after
`normalize_label` as it is recorded (`underreceivedrule`, #437); the public
allocator and its count name no rule and bind it off (`ALLOW_RECEIVED`), so
they never take an unproven or sending receipt. The wallet's own unproven
change is not a receipt and stays eligible in both. The
reference's `action_batch_outputs` exclusion (2252-2255) has no port
equivalent: the port has no action batches.

Bench seam: `benchmark_allocate_change_input()` (create_action/allocation.rs) wraps the test-only allocator for the door bench; it and `benchmark_support::allocate_change_input` compile only under the non-default `bench-support` feature.

## Commission Tracking

The storage provides commission tracking for monitoring redeemable outputs:

| Method | Description |
|--------|-------------|
| `insert_commission(user_id, transaction_id, satoshis, locking_script, key_offset)` | Record a new commission output |
| `get_unredeemed_commissions(user_id)` | Query all unredeemed commissions for a user |
| `redeem_commission(commission_id)` | Mark a commission as redeemed |

## Monitor Events

Event logging for background task monitoring and debugging:

| Method | Description |
|--------|-------------|
| `log_monitor_event(event, details)` | Record monitor event with optional JSON data |
| `get_monitor_events(limit, event_filter)` | Query events by type with limit |
| `cleanup_monitor_events(older_than)` | Remove events older than retention period |

## Abort Action Implementation

The `abort_action.rs` module cancels pending transactions and releases locked UTXOs:

### Core Function
```rust
pub async fn abort_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: AbortActionArgs,
) -> Result<AbortActionResult>
```

### Functionality
| Feature | Description |
|---------|-------------|
| Transaction lookup | Finds by reference or txid (if 64 hex chars) |
| Status validation | Only abortable statuses: unsigned, unprocessed, nosend, nonfinal, unfail |
| Outgoing check | Must be an outgoing transaction |
| Output protection | Fails if transaction outputs have been spent |
| UTXO release | Sets `spendable=true`, `spent_by=NULL` for locked inputs |
| Status update | Sets transaction status to 'failed' |

### Abortable vs Non-Abortable Statuses
| Abortable | Non-Abortable |
|-----------|---------------|
| unsigned, unprocessed, nosend, nonfinal, unfail | completed, failed, sending, unproven |

## Process Action Implementation

The `process_action.rs` module handles signed transactions after `create_action`:

### Core Function
```rust
pub async fn process_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: StorageProcessActionArgs,
) -> Result<StorageProcessActionResults>
```

### Functionality (1:1 Parity with Go/TypeScript)
- Args validation (reference, txid, raw_tx)
- txid validation via double SHA256 hash
- Transaction lookup by reference with isOutgoing validation
- inputBEEF validation (ensures not already processed)
- Status validation (must be 'unsigned' or 'unprocessed')
- Script verification and offset parsing from raw_tx
- DB updates: transaction, outputs, proven_tx_req
- Status determination: nosend/delayed/immediate modes
- Batch support and re-broadcast (is_new_tx=false)

### Status Determination
| Condition | Transaction Status | ProvenTxReq Status |
|-----------|-------------------|-------------------|
| is_no_send && !is_send_with | nosend | nosend |
| is_delayed | sending | unsent |
| immediate (SendWithResult) | sending | unsent |
| already sent | unproven | (existing) |
| immediate post accepted | unproven | unmined |
| immediate post refused, a double-spend word, or a fault (a hint, bsv-stack-lean #66) | sending | unsent (the word on its history, the re-ask on the cadence) |

Note: Immediate broadcast uses `"sending"` status (not `"unproven"`). The wallet layer calls `update_transaction_status_after_broadcast()` AFTER broadcast to finalize status; if that write fails it is logged (`post_broadcast_status_write_failed`) and the call still succeeds, the send-waiting task healing the row.

`process_action_internal` opens its own `write_tx`, runs `process_action_writes(storage, conn, user_id, &args)` and commits; `createAction`'s door calls `process_action_writes` on its own transaction instead.

## Internalize Action Implementation

The `internalize_action.rs` module allows a wallet to take ownership of outputs in external transactions:

### Core Function
```rust
pub async fn internalize_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: InternalizeActionArgs,
) -> Result<StorageInternalizeActionResult>
```

### Protocols
| Protocol | Description |
|----------|-------------|
| `wallet payment` | Adds output value to wallet's change balance in "default" basket |
| `basket insertion` | Custom output in specified basket, no balance effect |

### Functionality
- AtomicBEEF parsing with atomic_txid validation
- BEEF verification against ChainTracker (if set)
- Output extraction (satoshis, locking scripts)
- Merge support for existing transactions (completed/unproven/nosend)
- Balance tracking with net satoshi change calculation
- Label and tag support for transaction/output organization
- Received wallet payments carry `outputs.purpose = "receive"`, including converted custom outputs; `create_action` offers unproven receipts only under its `underReceivedRule` label, matched exact on the normalized bytes (`underreceivedrule`, any case or surrounding whitespace, #437) and recorded on that action, while completed receipts and own change keep their eligibility (#169).
- ProvenTxReq creation for unproven transactions
- Remittance exclusivity (#337, #371): one remittance per output. At step 0, per output in order and ahead of the remittance's fields (its customInstructions, its tags bound) and of any BEEF, a `wallet payment` with no `paymentRemittance` or carrying an `insertionRemittance` refuses `Error::ValidationError("The output remittance parameter must be only paymentRemittance for wallet payment")` and a `basket insertion` with no `insertionRemittance` or carrying a `paymentRemittance` refuses `Error::ValidationError("The output remittance parameter must be only insertionRemittance for basket insertion")`, the reference's `WERR_INVALID_PARAMETER` (ts-stack@edf6e03 `validationHelpers.ts:972, 975`, before `validateBasketInsertion` at 981-982). An earlier output's refusal answers ahead of a later one's. Step 9's missing-remittance check is defensive only, unreachable behind step 0. Witnesses: `internalize_remittance_tests.rs`
- validateWalletPayment (#377): at step 0, per output in order, after remittance exclusivity and ahead of the description and labels, a wallet payment's `derivationPrefix`, then `derivationSuffix`, must be base64 as validateBase64String reads it (trimmed with ECMAScript's whitespace by `label.rs`, as the key below is, so U+FEFF is stripped and U+0085 kept; not empty, the alphabet, `=` only in the last two places, a length that is a multiple of 4), else `Error::ValidationError("The derivationPrefix parameter must be valid base64 string")` (or `derivationSuffix`); then `senderIdentityKey` must be a compressed secp256k1 key in hex, else `Error::ValidationError("The senderIdentityKey parameter must be <mustBe>")` with validateHexString's `even length, not <n>.`, `hexadecimal string.`, `at least 66 length.`, `no more than 66 length.` or `a valid compressed secp256k1 public key` (ts-stack@edf6e03 `validationHelpers.ts:914-923`, 488-498, 185-191, 522-534). `Wallet::internalize_action` runs this shared validator before the BRC-29 derivation check (#376), so an empty derivation or invalid sender now answers this argument text through `WalletRefusal` with class `InvalidParameter`. Witnesses: `internalize_payment_tests.rs`, `src/wallet/nosend_label_tests.rs`
- validateLabel (#365): after the labels bound and ahead of the BRC-177 refusal, every label, trimmed with ECMAScript's whitespace and lowercased (`label.rs`, #379: U+FEFF is stripped and U+0085 kept, as `String.prototype.trim` does and Rust's `str::trim` does not), is 1 to 300 UTF-8 bytes, else `Error::ValidationError("The label parameter must be at least 1 length.")` or `("The label parameter must be no more than 300 length.")`, the reference's texts (ts-stack@edf6e03 `validationHelpers.ts:385-387, 409-417`, mapped at 1054 before `Wallet.ts:1248-1250`). The step 0 order: the outputs bound; per output, remittance exclusivity, the wallet payment's fields, customInstructions, the tags bound; the description; the labels bound; each label; BRC-177. The shared validator lives in `src/wallet/internalize_args.rs`, called here at step 0 and by `Wallet::internalize_action` before BRC-29 derivation checks, including with remote storage (#376, `docs/ARCHITECTURE.md` D13 closed). Witnesses: `internalize_nosend_tests.rs`, `src/wallet/nosend_label_tests.rs`
- The description (#380): after the outputs' step 0 checks and ahead of the labels bound, the description is 5 to 2000 UTF-8 bytes as given, untrimmed, else `Error::ValidationError("The description parameter must be at least 5 length.")` or `("The description parameter must be no more than 2000 length.")`, the reference's validateStringLength (ts-stack@edf6e03 `validationHelpers.ts:1053`, 348-356); bsv-rs's validator reads only the outputs' customInstructions. Witnesses: `internalize_description_tests.rs`, `src/wallet/nosend_label_tests.rs`
- BRC-177 (#357): after the labels bound, ahead of any write, a label starting `p nosend ` (trimmed with ECMAScript's whitespace and lowercased by `label.rs`, as the SDK's validateLabel leaves it at `validationHelpers.ts:1054`, #379; `startsWith`, ts-stack@edf6e03 `brc177NoSendExpiry.ts:5,119`) refuses `Error::ValidationError("The labels parameter must be BRC-177 noSend expiry labels only on outgoing createAction requests")`, the reference's `WERR_INVALID_PARAMETER` on internalizeAction (`Wallet.ts:1248-1250`). The create door's refusal is #312 (Create Action, Features). `p nosendx` and `p nosend` alone are ordinary labels. Witnesses: `internalize_nosend_tests.rs`, `src/wallet/nosend_label_tests.rs`

### Merge Behavior
| Scenario | Balance Change |
|----------|---------------|
| New wallet payment | +satoshis |
| Existing change output -> wallet payment | 0 (ignored) |
| Existing non-change -> wallet payment | +satoshis |
| Change output -> basket insertion | -satoshis |

## Sync Implementation

The `sync.rs` module enables multi-storage synchronization:

### Core Functions
```rust
pub async fn get_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
) -> Result<SyncChunk>

pub async fn process_sync_chunk_internal(
    storage: &StorageSqlx,
    args: RequestSyncChunkArgs,
    chunk: SyncChunk,
    sync_map: &mut crate::storage::SyncMap,
) -> Result<ProcessSyncChunkResult>
```

### Entity Names
Constants for sync offsets (exported as `entity_names` module):
- `OUTPUT_BASKET`, `PROVEN_TX`, `PROVEN_TX_REQ`, `TX_LABEL`, `OUTPUT_TAG`
- `TRANSACTION`, `OUTPUT`, `TX_LABEL_MAP`, `OUTPUT_TAG_MAP`
- `CERTIFICATE`, `CERTIFICATE_FIELD`, `COMMISSION`

### Key Features
- **get_sync_chunk**: Dependency-ordered entity processing, offset-based resumption, size/item limiting, since filtering
- **process_sync_chunk**: Upsert logic (INSERT/UPDATE by updated_at), ID translation for foreign keys, empty detection, change tracking

Output sync preserves all 25 `TableOutput` columns, including
`output_description`, `spent_by`, `sequence_number`, `spending_description`,
`purpose`, and `provided_by` (#276). The exporter selects each column explicitly
and decodes nullable metadata without hiding missing columns. Sequence numbers
retain the full `u32` range. Inserts write the whole row. Both the insert and
the newer-row update write `purpose` as given: a missing one is `''` (Go's
zero value; the TS reference refuses the row, #350 holds the pick; the column
is `TEXT NOT NULL`), never `change`, so a synced coin without purpose is never
offered as change (#313).
Newer-row updates
write the reference `mergeExisting` columns (`output_description`, `spent_by`,
`spending_description` among them) and no others: `transaction_id` (the match
key), `satoshis`, `derivation_prefix`, `derivation_suffix` and `sequence_number`
stay as stored (#276, #298); an incoming null clears a written value. Equal or older rows keep the existing
values. `spent_by`, like the parent transaction,
uses the session's transaction ID map, including transactions from earlier
chunks; an unmapped spender becomes null, matching the reference merge.
The witness module is `sync_output_tests.rs`; the complete wire column list and
the pinned reference's merge details are in `docs/STORAGE-WIRE.md`.

## BEEF Verification Implementation

The `beef_verification.rs` module provides BEEF (Background Evaluation Extended Format) merkle proof verification:

The live `internalize_action` door passes `get_settings().chain` to
`verify_beef_spends`. Each input of each unique unproven raw transaction,
including parents, runs through `Spend::set_flags(ScriptFlags::standard(era))`.
Proven raw entries supply outputs; txid-only entries supply proof identity.
Neither has its own scripts rejudged, and duplicate raw entries are not rejudged.

This is an interim second evaluation until bsv-rs #440 supplies its per-input
hook. The SDK's `verify_stream_two_pass` retains the structural/proof verdict;
only `SpendRefusal::Script` is set aside. A default script refusal can stop
that scan early, so `verify_stream_structure` must then finish cleanly before
the word pass. The word pass preserves the later mining, outpoint, resource
and value checks that such a refusal can mask. It retains only referenced
outputs, shares one sighash cache per transaction, and releases transactions
waiting for parents in wire order.

The `pub(crate)` `input_era` helper uses a mainnet coin's proven BUMP height,
or the tracker tip for an unproven coin, against bsv-rs's Genesis and Chronicle
constants. Pre-Genesis mainnet refuses with
`The mainnet coin height is before Genesis.`. Testnet and regtest use the tip
against those constants until #440 supplies their activation heights; regtest
fixtures name the era they model. An unsupported network or tip also refuses.
The network comes from settings, never from guessing what a Genesis height
means. The `script_pin_tests` witnesses pin the fork-ID refusal, post-Chronicle
version-1 `0x61` acceptance and pre-Chronicle era refusal. D15 is closed by
#422; create/process script verification stays with #426.

### Core Functions
```rust
pub async fn verify_beef_merkle_proofs(
    beef: &mut Beef,
    chain_tracker: &dyn ChainTracker,
    mode: BeefVerificationMode,
    known_txids: &HashSet<String>,
) -> Result<bool>

pub async fn verify_txid_merkle_proof(
    beef: &Beef,
    txid: &str,
    chain_tracker: &dyn ChainTracker,
) -> Result<bool>
```

### Verification Modes
| Mode | Description |
|------|-------------|
| `Strict` | Verify all merkle roots against chain (default) |
| `TrustKnown` | Skip verification for known txids |
| `Disabled` | Skip all verification |

### Usage
- Called by `internalize_action` to verify incoming transaction proofs
- Called by `create_action` to verify user-provided inputBEEF
- Requires `ChainTracker` to be set via `storage.set_chain_tracker()`
- Returns `Ok(true)` if valid, `Ok(false)` if no proofs to verify, `Err` if invalid

## Create Action Implementation

The private `create_action/` module provides full transaction creation functionality:

### Core Function
```rust
pub async fn create_action_internal(
    storage: &StorageSqlx,
    chain_tracker: Option<&dyn ChainTracker>,
    user_id: i64,
    args: CreateActionArgs,
) -> Result<StorageCreateActionResult>
```

`create_action_internal` is the door with no sign step and no offer. The door itself:

```rust
pub async fn create_action_at_door_internal(
    storage: &StorageSqlx,
    chain_tracker: Option<&dyn ChainTracker>,
    user_id: i64,
    args: CreateActionArgs,
    door: CreateActionDoor<'_>,          // { offer, must_spend, sign_and_process }
) -> std::result::Result<CreateActionDone, DoorRefusal>
```

With `sign_and_process` the template is signed and processed inside the create step's `BEGIN IMMEDIATE`, committed once (bsv-wallet-cli #44); the caller broadcasts after.

### Constants
| Constant | Value | Description |
|----------|-------|-------------|
| `MAX_SATOSHIS` | 2,100,000,000,000,000 | Total BTC supply in satoshis |
| `MAX_POSSIBLE_SATOSHIS` | 2,099,999,999,999,999 | Sentinel for "use max" |
| `DEFAULT_FEE_RATE_SAT_PER_KB` | 101 | Default fee rate |
| `P2PKH_LOCKING_SCRIPT_LENGTH` | 25 | Standard P2PKH output size |
| `P2PKH_UNLOCKING_SCRIPT_LENGTH` | 107 | Standard P2PKH input size |
| `MIN_DESCRIPTION_LENGTH` | 5 | Minimum output-description bytes |
| `MAX_DESCRIPTION_LENGTH` | 2000 | The output-description refusal text's figure (the description bound itself is `validate_description`'s 5 and 2000, #383) |

### Features
- Validation in ts-stack@edf6e03 validateCreateActionArgs order (749-808): description, inputs, outputs (each output's tags last), labels, options (knownTxids, noSendChange and its duplicates, sendWith); before #309 it ran labels, outputs, inputs, so a request with several bad arguments now answers the inputs' texts ahead of the outputs' and the labels'
- The description (#383) uses `validate_description` from `internalize_action.rs`,
  shared with the internalize door: 5 to 2000 untrimmed UTF-8 bytes, as
  `validateStringLength(args.description, 'description', 5, 2000)` at
  ts-stack@edf6e03 `validationHelpers.ts:755, 348-356`. A shorter value answers
  `The description parameter must be at least 5 length.`; a longer one answers
  `The description parameter must be no more than 2000 length.`. At
  `WalletInterface` these are `WalletRefusal { class: InvalidParameter, message }`,
  with `Validation error: ` before the text. The check precedes inputs and every
  write. Witnesses: `create_action/tests/validation_description.rs` (exact
  messages, both byte boundaries, multibyte strings, whitespace retained in
  storage, and an invalid output beside a short description).
- The labels (#386) use `validate_label` from `src/wallet/internalize_args.rs`,
  shared with the internalize door: each normalized label must contain 1 to 300
  UTF-8 bytes after ECMAScript trim and lowercase (`label.rs`, #379), as
  `validateLabel` does at ts-stack@edf6e03 `validationHelpers.ts:385-387, 409-417`.
  It runs after the labels array bound at 762 and before options, the BRC-177
  grammar and the capability refusal. Empty normalized labels answer
  `The label parameter must be at least 1 length.`; oversized ones answer
  `The label parameter must be no more than 300 length.`. At `WalletInterface`
  both carry class `InvalidParameter` and the `Validation error: ` prefix.
  Both action doors store the normalized value; `listActions` uses normalized
  filters against those stored values (#392). Witnesses:
  `create_action/tests/validation_labels.rs` (exact refusals, byte boundaries,
  normalization, stored and listed form, and BRC-177 precedence).
- BRC-177 (#312): after every other check, a label starting `p nosend ` (trimmed with ECMAScript's whitespace and lowercased by `label.rs`, as the SDK's validateLabel leaves it, #379: `\u{FEFF}p nosend batch` meets the grammar and `\u{0085}p nosend ...` is an ordinary label; `startsWith`, ts-stack@edf6e03 `brc177NoSendExpiry.ts:5,119`) refuses `Error::InvalidOperation("Active storage does not support BRC-177 noSend expiry")`, the reference's text on a store without the capability (`createNoSendExpiryAction.ts:201-203`; the reference routes the label there at `Wallet.ts:1162-1165`). The port has no capability, so every such label is refused, ahead of any write; the label grammar's checks run ahead of it, as `parseBrc177NoSendExpiryLabels` does at `Wallet.ts:1163` (`brc177NoSendExpiry.ts:81-116`, in its order), each refusing `Error::ValidationError("The labels parameter must be <mustBe>")` with the reference's `WERR_INVALID_PARAMETER` text (`WERR_errors.ts:105`, no full stop): exactly one expiry label and one module label, one space between mode and value, the mode `seconds`/`timestamp`/`blockheight`, a canonical unsigned value within 2^53 - 1, `seconds` above zero (#359). A label the grammar accepts meets the capability text. `p nosendx` and `p nosend` alone are ordinary labels. Witnesses: `create_action/tests/validation_nosend.rs`, `create_action/tests/validation_nosend_grammar.rs`, `src/wallet/nosend_label_tests.rs`; the sync divergences are ARCHITECTURE D7 A `Wallet` over `StorageClient` (`remote`) passes the label to the remote store, where the reference parses in `Wallet` ahead of storage (#359's placement note; the lanes for #357 and #359 would put both refusals in the Wallet, keeping the door checks for direct storage callers).
- Fee calculation with accurate transaction size estimation
- Change generation with automatic UTXO management
- Full input/output database state management
- Labels and tags for transaction/output organization

### Stored labels (#392)

The create and internalize label writers use `normalize_label` before lookup
and insertion, so equivalent spellings share a label row and a transaction map.
This follows ts-stack@edf6e03 `validationHelpers.ts:762,1054`,
`createAction.ts:741-745`, `internalizeAction.ts:742-744` and
`StorageReaderWriter.ts:267-281`. `listActions` validates and normalizes each
filter as `validationHelpers.ts:1691` does, then compares it to the stored string
as `listActionsKnex.ts:127-140` does. Existing rows retain their spelling and
match only if equal to the normalized filter; an unfiltered listing returns them
unchanged. The wire fixtures carry `  LaBeL ` to pin the client request spelling.
The SQL behavior is covered by `create_stored_labels_are_normalized_and_reused`,
`internalize_label_tests.rs` and the storage fixture replays. The two label-error
vectors use the reference's current texts and are executed by `test_vectors`.

### Array bounds (#309)

`validate_array_bound` (`src/wallet/internalize_args.rs`, re-exported by `internalize_action.rs` and shared by the storage doors)
is validateArray's bound (ts-stack@edf6e03 validationHelpers.ts:105-114):
past it the door refuses `Error::ValidationError` with
`The <name> parameter must be an array of at most <maximum> items`
(WERR_INVALID_PARAMETER.ts:14). The maximum is 100000
(`MAXIMUM_WALLET_COLLECTION_ITEMS`, 67) except sendWith's 1000
(`MAXIMUM_SEND_WITH_TRANSACTIONS`, 69). Each bound runs where the reference
calls validateArray, ahead of the array's items: an over-long array of valid
items is refused by the bound, a bad item under it by the item's text.

| Door | Array (reference line) | Place in the port |
|------|------------------------|-------------------|
| create (`create_action/validation.rs`) | `inputs` (758), `outputs` (759), an output's `tags` (639), `labels` (762), `knownTxids` (662), `noSendChange` (667), `sendWith` (137, 1000) | after description, in that order |
| internalize (`internalize_action.rs`) | `outputs` (1052), an insertion's `tags` (948), `labels` (1054) | step 0; tags after the same output's remittance exclusivity and `customInstructions` and every earlier output's |
| listCertificates (`storage_sqlx/certificates.rs`) | `certifiers` (1195), `types` (1198) | after the auth check, ahead of any query |
| listOutputs (`storage_sqlx/list_outputs.rs`) | `tags` (1637) | after the auth check, ahead of the basket lookup |
| listActions (`storage_sqlx/transactions.rs`) | `labels` (1691) | after the auth check, ahead of the label lookup |

`Wallet::list_*` delegates to the finders unchanged, so a default call
through `WalletInterface` and one through the storage trait meet the same
bound. Under the bound the finders bind one SQL placeholder per item: past
SQLite's 32766 they answer `too many SQL variables` (a finding of #309).

### BEEF Building (1:1 Parity with Go/TypeScript)
- User inputBEEF merging, recursive ancestor lookup until proven transactions
- knownTxids trimming to txid-only format, returnTXIDOnly support

## Database Schema

The module creates 16 tables via `migrations/001_initial.sql`:

| Category | Tables |
|----------|--------|
| Core | `settings`, `users`, `transactions`, `outputs`, `output_baskets` |
| Proofs | `proven_txs`, `proven_tx_reqs` |
| Certificates | `certificates`, `certificate_fields` |
| Labels/Tags | `tx_labels`, `tx_labels_map`, `output_tags`, `output_tags_map` |
| Other | `commissions`, `sync_states`, `monitor_events` |

## Usage

```rust
use bsv_wallet_toolbox::storage::sqlx::StorageSqlx;
use bsv_wallet_toolbox::services::{Services, Chain};

// Setup
let storage = StorageSqlx::open("wallet.db").await?;  // or in_memory()
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

// Set services for blockchain operations (required for MonitorStorage methods)
let services = Arc::new(Services::new(Chain::Main)?);
storage.set_services(services);

let identity_key = "03abc..."; // 66-char hex public key
let (user, is_new) = storage.find_or_insert_user(&identity_key).await?;
let auth = AuthId::with_user_id(&identity_key, user.user_id);

// Transaction operations
let result = storage.create_action(&auth, CreateActionArgs { ... }).await?;
storage.abort_action(&auth, AbortActionArgs { reference: "ref".to_string() }).await?;
storage.internalize_action(&auth, InternalizeActionArgs { tx: beef_bytes, ... }).await?;

// TrxToken scopes
let token = storage.begin_transaction().await?;
// ... perform operations within scope ...
storage.commit_transaction(token).await?;  // or rollback_transaction(token)

// Query operations
let actions = storage.list_actions(&auth, ListActionsArgs { ... }).await?;
let outputs = storage.list_outputs(&auth, ListOutputsArgs { basket: "default".to_string(), ... }).await?;
let certs = storage.find_certificates(&auth, FindCertificatesArgs { ... }).await?;

// Commission tracking
let commission_id = storage.insert_commission(user.user_id, tx_id, 500, &script, "offset").await?;
let unredeemed = storage.get_unredeemed_commissions(user.user_id).await?;
storage.redeem_commission(commission_id).await?;

// Monitor events
storage.log_monitor_event("sync_started", Some(r#"{"block": 800000}"#)).await?;
let events = storage.get_monitor_events(100, Some("sync_started")).await?;
storage.cleanup_monitor_events(Duration::from_secs(86400)).await?;

// Multi-instance task locking
let acquired = storage.try_acquire_task_lock("check_proofs", "instance-1", ttl).await?;
if acquired {
    // ... run task ...
    storage.release_task_lock("check_proofs", "instance-1").await?;
}
```

## Feature Flags

Feature `sqlite` (default) enables SQLite support. MySQL is planned but not yet implemented.

## Implementation Notes

### Lock Utilities
All `std::sync::RwLock` access goes through `lock_utils::{lock_read, lock_write}` helpers (from `src/lock_utils.rs`), which convert poisoned lock errors into `crate::Error` instead of panicking.

### Owned Settings
`get_settings`, `storage_identity_key` and `storage_name` return owned copies (`TableSettings`, `String`) taken under their `RwLock`s; there is no `unsafe` in the crate. Every handle of a store shares these caches, so a reference lent out of a lock could be freed by another handle's `make_available` (review 4 G7 of bsv-wallet-cli #44). Before `make_available()`, `get_settings()` returns `TableSettings::default()`.

### TrxToken Scope Tracking
The `active_transactions` field tracks in-flight transaction scopes using opaque `u64` token IDs generated by `AtomicU64`. `begin_transaction()` registers a token, `commit_transaction()` and `rollback_transaction()` remove it. This enables grouping multiple storage operations into logical units.

### Multi-Instance Task Locking
The `task_locks` field provides in-memory task locking for multi-instance monitor support. `try_acquire_task_lock()` checks if a task is already locked by another instance (with TTL = 2x task interval). `release_task_lock()` removes the lock. This prevents duplicate task execution across monitor instances.

### Soft Deletes
Certificates and baskets use `is_deleted` flag for soft deletes rather than actual row removal. This preserves history for sync operations.

### Dynamic Query Building
The `find_*` and `list_*` methods build SQL dynamically based on provided filter arguments. Parameters are bound safely to prevent SQL injection.

`find_outputs_internal` (called by the authenticated `find_outputs`) and
`find_output_row_by_outpoint` (called by `find_output_by_outpoint` and the
create-action stored-input path), together with internalization's
`load_existing_outputs`, select every `TableOutput` column by name. Every
reader using the shared `output_from_row` decoder must select this full row,
including `provided_by`, `purpose`,
`output_description`, `spent_by`, `sequence_number`, and
`spending_description`. The decoder is strict: every column uses
`try_get(...)?`, so a missing or invalid column returns an error instead of
a default. SQL NULL remains `None` for nullable fields. `sequence_number` is read
only through `sequence_number_from_row` (`storage_sqlx/outputs.rs`), shared by
`output_from_row`, the sync export and the allocator: the INTEGER column
decodes as i64 and converts to the full u32 the writers store; a negative
i32 an earlier writer left keeps its bits (-1 is 0xFFFFFFFF), anything outside both ranges is a `ColumnDecode` error (#301,
witnesses in `sequence_number_tests.rs`). A received output reads
back as `providedBy: "storage"`, `purpose: "receive"` when those are stored.
The default finder includes the locking script; `no_script: true` suppresses
only the returned script and retains the origin and spending metadata.
The allocation reader also names every column in its full-output SELECT.

`list_actions` with `includeInputs` does not read `sequence_number` (#307):
as `enrichActionInputs` (ts-stack@edf6e03 `listActionsKnex.ts:66-95`), it
reads the action's raw transaction once when the action has inputs, whatever
`includeInputUnlockingScripts` says, through `get_proven_or_raw_tx`, as the
reference's `getRawTxOfKnownValidTransaction` (`StorageKnex.ts:507-508`)
through `getProvenOrRawTx` (189-211): the `proven_txs` row, else a
`proven_tx_reqs` row `unsent`, `unmined`, `unconfirmed`, `sending`, `nosend`
or `completed` (`RAW_TX_REQ_STATUSES`); `transactions.raw_tx` is not read, so
an `invalid` or `doubleSpend` request serves 0 though the row keeps its copy
(#351). It parses the bytes with bsv-rs (a raw transaction that does not
parse is the call's error, `SdkError(ReaderUnderflow { .. })` for short
bytes, as `fromBinary` throws), matches each
input by source txid and vout and serves its `sequence`, 0 when there is no
raw transaction or no such input (`input?.sequence || 0`, :89), and its
unlocking script under `includeInputUnlockingScripts` (:93). The inputs come
by `output_id`, the unpaged finder's order (:73, no ORDER BY). Witnesses in
`storage_sqlx/list_action_inputs_tests.rs`.

`list_outputs` uses a separate `WalletOutput` projection whose existing wire
fields do not include `providedBy` or `purpose`. The sync export has its own
query and already selects those two origin columns.

### Settings Caching
Settings are loaded once via `make_available()` and cached in an `RwLock`. The `get_settings()` method returns a reference to cached data.

The settings row's `chain` is written once, when `migrate()` creates the row: a later `migrate()` keeps the stored chain and rewrites the other columns, the name, the identity key, `max_output_script` (back to its default) and `updated_at` (#435; the reference inserts `chain` only in the migration that creates the row, `KnexMigrations.ts` at ts-stack edf6e03). A new row is `mainnet`: no constructor takes a `Chain` yet, so a testnet or regtest store's chain is set only by writing the row (the tests use `UPDATE settings SET chain`). Readers match its prefix (`proof_requests.rs`: `starts_with("main")`).

### MonitorStorage Integration
All MonitorStorage methods are fully implemented:
- `synchronize_transaction_statuses` - queries chain via services, updates proof records
- `send_waiting_transactions` - broadcasts via `services.post_beef()`; a refusal or a double-spend word is a hint (bsv-stack-lean #66)
- `abort_abandoned` - queries and aborts stale unsigned/unprocessed transactions
- `un_fail` - checks chain via services, restores or invalidates failed transactions
- `try_acquire_task_lock` / `release_task_lock` - multi-instance coordination
- `update_proven_tx_req_status` - direct status updates

The `review_status` and `purge_data` methods on `WalletStorageWriter` are also fully implemented:
- `review_status` - syncs completed proven_tx_req status to associated transactions
- `purge_data` - deletes old failed/invalid proven_tx_reqs, cleans raw data from completed ones

### Services Dependency
Operations requiring blockchain access (BEEF verification, broadcasting, header lookups, MonitorStorage methods) require `WalletServices` to be set via `set_services()`. Calling `get_services()` before setting returns an error.

## Tests

The create-action tests are grouped by concern under `create_action/tests/`.

| Module | Tests | Key Coverage |
|--------|-------|--------------|
| `create_action/` | 131 | Validation, fee calculation, BEEF building, allocation, and door scenarios |
| `process_action.rs` | 35 | txid computation, VarInt parsing, script offsets, Go test parity |
| `storage_sqlx/` | 73 | CRUD, list methods, certificate filters, monitor ops, TrxToken, task locks |
| `beef_verification.rs` | 25 | Merkle proof verification, verification modes, edge cases |
| `abort_action.rs` | 19 | Status validation, UTXO release, lookup by txid |
| `internalize_action.rs` | 11 | Wallet payment, basket insertion, merge scenarios |
| `sync.rs` | 9 | Chunk retrieval, upsert logic, ID translation, roundtrip |

Run with:
```bash
cargo test --features sqlite storage::sqlx
```

## Related

- `../traits.rs` - Trait definitions (`WalletStorageReader`, `WalletStorageWriter`, `MonitorStorage`)
- `../entities/` - Table entity structs (`TableUser`, `TableOutput`, `TransactionStatus`)
- `../client/` - Remote storage client (alternative implementation)
