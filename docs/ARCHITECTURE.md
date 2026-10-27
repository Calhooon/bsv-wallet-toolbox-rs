# Architecture: the storage machine, the ladders, the proof path, the monitor

One dataflow for the crate, read from the code at beta `901d72c` (2026-10-25).
The charters cited below are the stack's at `bsv-stack-lean` commit `36d2d6b`
(`origin/main`, 2026-10-25), read with `git show 36d2d6b:docs/charters/<file>`:
`wallet.md`, `storage.md`, `broadcaster.md`, `tracker.md`. A citation
`wallet.md section 2` is that file's section at that commit.

Each machine's section ends with an invariants table: `invariant | charter |
test`. The test column names a test in this repository. A lib test is proved
with `cargo test --lib <name> -- --list`; an integration test, written
`` `name` (`--test file`) ``, with `cargo test --test <file> <name> -- --list`;
each shows one test whose path ends in the name (a lib test is listed with its
module path). `tools/arch-doc-tests.sh` runs every row and checks that. A row
whose test column reads `(no test)` is listed again at the end: those rows and
the divergences are the findings of the lane that wrote this (#64).

Monitor tasks are counted from `src/monitor/tasks/`: fifteen task files (the
issue said thirteen and the root `CLAUDE.md` twelve until #64 corrected it; the
directory is the truth).

## 0. The dataflow

```
               application (BRC-100)                           couriers and hints
  createAction   signAction   internalizeAction   abortAction   Arcade SSE, webhooks
       |             |              |                 |                 |
       v             v              v                 v                 |
 +---------------------------------------------------------------+      |
 |  Wallet<S, V>   src/wallet/wallet.rs                           |      |
 |  spend_lock, permissions, the local signer (DoorSigner)        |      |
 +------+-------------+--------------+----------------+-----------+      |
        |             |              |                |                  |
   the doors: one BEGIN IMMEDIATE each; nothing but the store awaited    |
   while it is held; the broadcast comes after the commit                |
        |             |              |                |                  |
 +------v-------------v--------------v----------------v-----------+      |
 |  StorageSqlx   src/storage/sqlx/                               |      |
 |   pool()    readers, PRAGMA query_only            (every read) |      |
 |   writer()  one connection; write_tx() BEGIN IMMEDIATE (writes)|      |
 |   tables: transactions, outputs, proven_tx_reqs, proven_txs,   |      |
 |     proof_root_checks, broadcast_seen, locked_input_checks,    |      |
 |     monitor_state                                              |      |
 |   the proof funnel: ingest_merkle_proof                        |<-----+
 |     (root checked against the tracker, then the gate, then    |
 |      one transaction)                                          |
 +------+---------------------------------------------+-----------+
        | post_beef, after the commit         reads   ^  writes through
        v                                             |  MonitorStorage
 +-------------------------------+   +----------------+------------------------+
 |  Services   src/services/     |   |  Monitor<S, V>   src/monitor/           |
 |  the read ladders over the    |<--|  fifteen tasks on a clock, one task     |
 |   explorers: WhatsOnChain,    |   |   lock each (TTL twice the interval)    |
 |   BananaBlocks, Bitails       |   |  new_header       -> the proof gate     |
 |  RateGate, one per host       |   |  check_for_proofs -> the funnel         |
 |  the header service:          |   |  reorg, review_proven_txs -> demote     |
 |   chaintracks, then BHS,      |   |  send_waiting     -> post_beef (hints)  |
 |   never a courier             |   |  fail_abandoned   -> abort_action       |
 |  the broadcasters: Arcade,    |   +-----------------------------------------+
 |   TAAL ARC, GorillaPool ARC   |
 +---------------+---------------+
                 ^
                 | headers: the one source of a chain word (fail closed)
        the header service (chaintracks; BHS as its fallback)
```

The words: a transaction's word comes only from a merkle path whose root our
header at its height carries (`tracker.md section 2`); every broadcaster word
and every explorer answer is a hint (`broadcaster.md section 1 B1`,
`wallet.md section 2`); the wallet writes one word of its own about a coin, the
lock (`wallet.md section 1`). The sections below follow a transaction through
the code: the doors, the store's writer, the ladders the monitor asks, the
proof funnel, the monitor's tasks, and the status words.

## 1. The doors

The wallet layer (`src/wallet/wallet.rs`) takes the spend lock for building and
permission checks; the stored post door takes no wallet lock. Each write uses
`write_tx()` on the writer (`BEGIN IMMEDIATE`); nothing but the store is awaited
while it is held, and a refusal rolls it back. A broadcast comes after the
commit. The files are `src/storage/sqlx/create_action/mod.rs`,
`process_action.rs`, `internalize_action.rs`, `abort_action.rs`.

| door | before the lock (reader, nothing held) | inside the one transaction | after the commit |
|---|---|---|---|
| `createAction` (`create_action_at_door_internal`) | outputs and inputs validated; the caller's `inputBEEF` roots read against the tracker (`RecordingTracker`); the change coins picked as a dry run (`ChangePick::DryRun`) from `change = 1` outputs of `completed` and `unproven` transactions in the change basket, within the host's offer, never a coin another call of this process has reserved (`InFlightPicks`); the BEEF assembled from stored bytes, every stored proof's root read against the tracker and the answers kept; a stored proof with no `proof_root_checks` record is checked once and demoted when refuted (a short write through the writer) | `transactions` row (`unsigned`), labels, the inputs, the allocation among the pre-checked coins only (`ChangePick::Within`: `outputs.spendable = 0`, `spent_by`), the new outputs and tags, the BEEF rebuilt from the kept answers (`CachedTracker`); with `sign_and_process` the signer (`DoorSigner`, local CPU) and `process_action_writes` on the same connection; one commit | `post_beef`; the outcome classified; `update_transaction_status_after_broadcast`; a busy on that write is logged (`post_broadcast_status_write_failed`) and the call still succeeds, `send_waiting` heals the row |
| `processAction` (`Wallet::process_action`, #66) | user-scoped references or txids resolved; templates refused; signed BEEF rebuilt and roots checked | request `unsent`, transaction `sending`, result `sending`; one short commit | no wallet lock or writer across the post; every named instance's word recorded; acceptance writes `unmined`/`unproven` in short writes; a repeat answers `unproven` without posting |
| `signAction` (`process_action_internal`) | the signature, local CPU (`src/wallet/signer.rs`) | `transactions` row: txid, `raw_tx`, status (`nosend`, `sending`, or `unproven` when already sent); `outputs` script offsets; the `proven_tx_reqs` row (`nosend`, `unsent`); one commit | as `createAction`: post, classify, status write |
| `internalizeAction` (`internalize_action_internal`) | arguments validated (`customInstructions` at most 1,000 bytes); the bytes refused on shape (`refuse_invalid_beef_bytes`); parsed; the structure checked; with a tracker every BUMP root checked (`verify_beef_merkle_proofs`, `Strict`); the subject's own BUMP checked against the tracker and the gate (`validate_bump_claim`); the default basket read | `transactions` row inserted, or merged into an existing `completed`, `unproven` or `nosend` one; `outputs` (a wallet payment goes to the change basket with `change = 1`; a basket insertion to its basket); labels and tags; the inputs it consumes that are ours marked spent; the `proven_txs` link when the BUMP checked, else a `proven_tx_reqs` row (`unmined` when the gate deferred a checked BUMP, `unsent` for a new unproven transaction); one commit | a new `unsent` request is posted once (`broadcast_new_internalized_req`); an acceptance moves it to `unmined`; every other word is a hint recorded on the request |
| `abortAction`, a template (`unsigned`, `unprocessed`, `nosend`, `nonfinal`, `unfail`) | the transaction read on the pool | the outputs checked unspent; the inputs released (`spendable = 1`, `spent_by = NULL`); status `failed`; one commit | nothing |
| `abortAction`, a broadcast transaction (`sending`, `unproven`) | the chain evidence read on the pool: a `chain|seen` or `mined` row in the broadcast memory, or a `completed` descendant, refuses | the inputs released; its own outputs `spendable = 0`; status `failed`; the request `invalid`; one commit | the memory records `rejected`; the unproven descendants are retired (`retire_poisoned_descendants`); a failure there is logged (`post_commit_retire_failed`) and `review_status` check 4 finishes it |

The pre-check and the step can disagree: a coin taken, or a root unread,
between them rolls the step back and the pre-check runs once more; the second
time a coin taken refuses `StoreBusy { MovedUnderUs }` (never
`InsufficientFunds`) and a root unread refuses `ServiceError` naming its
height. A busy answer promises nothing was written (`wallet.md section 6`, R8).
No `createAction` asks a provider: an ancestor not stored is a refusal naming
it (`UnspendableCoin`).

| invariant | charter | test |
|---|---|---|
| the door is one transaction: a busy in the process step writes nothing | `storage.md section 2 S9` | `create_action_busy_in_the_process_step_writes_nothing` |
| a contended store answers busy or done, never `InsufficientFunds` | `storage.md section 2 S9`; `wallet.md section 6` R8 | `nine_concurrent_creates_on_eight_coins_are_done_or_busy_never_insufficient_funds` |
| a coin taken after the pre-check refuses with nothing written | `storage.md section 2 S9` | `a_coin_taken_after_the_precheck_with_none_left_refuses_with_nothing_written` |
| a root unread twice refuses as a service error naming its height | `wallet.md section 2` (the guard re-reads a stored proof now) | `a_root_unread_twice_refuses_as_a_service_error_naming_its_height` |
| a caller's root the tracker refuses is refused before any allocation | `wallet.md section 2` (a body is credited only when its roots check) | `a_caller_root_the_tracker_refuses_is_refused_before_any_allocation` |
| the door spends only the host's offer | `wallet.md section 2` (the rule the host names) | `create_action_offering_spends_only_the_offer` |
| a child of a failed parent is refused at the door | `wallet.md section 12`, the lineage rule | `create_action_refuses_a_child_of_a_failed_parent` |
| a status write that is busy after the broadcast is the call's success; the monitor heals it | `storage.md section 2 S9` (never a busy after a committed write) | `a_busy_status_write_after_the_broadcast_is_logged_and_healed_by_send_waiting` |
| `signAction` busy in the process step keeps the action and its coins | `storage.md section 2 S9` | `sign_action_busy_in_process_action_keeps_the_action_and_its_coins` |
| an empty caller unlocking script or zero unlocking script length is refused at the create door before BEEF parsing or storage, naming the parameter bare as `unlockingScript` or `unlockingScriptLength`, with nothing stored | `ts-stack@edf6e03 sdk/validationHelpers.ts:522-530,595,758` and `sdk/WERR_INVALID_PARAMETER.ts:14`; `BRCs@5119a9b wallet/0100.md:649` | `create_action_refuses_empty_caller_unlocking_script`, `create_action_refuses_zero_caller_unlocking_script_length` |
| `signAction` signs only storage-allocated inputs and preserves the caller's script on a named input, whatever its derivation metadata | `BRCs@5119a9b wallet/0100.md:643-649`; `ts-stack@edf6e03 signer/methods/buildSignableTransaction.ts:176-189` | `sign_action_refuses_empty_caller_script_for_named_input_with_derivation`, `sign_action_requires_caller_script_for_named_input_with_derivation` |
| `signAction` refuses a caller spend on a storage-allocated input before signing or writing; the unsigned action remains available to complete | `ts-stack@edf6e03 signer/methods/completeSignedTransaction.ts:19-24`; `sdk/WERR_errors.ts:105` | `sign_action_refuses_caller_spend_for_allocated_input` |
| `signAction` refuses a caller spend longer than the byte length declared at `createAction` before signing or writing; equal and shorter scripts remain accepted | `ts-stack@edf6e03 signer/methods/completeSignedTransaction.ts:25-30`; `sdk/WERR_errors.ts:105` | `sign_action_refuses_caller_spend_over_declared_length`, `sign_action_accepts_caller_spend_at_or_below_declared_length` |
| a missing named-input script is refused before signing; the unsigned action remains available to its caller | `BRCs@5119a9b wallet/0100.md:724-729`; `ts-stack@edf6e03 signer/methods/verifyUnlockScripts.ts:51-64` | `sign_action_requires_caller_script_for_named_input_with_derivation` |
| a broadcaster's refusal at the door is a hint, never the call's error | `broadcaster.md section 1 B5`; `wallet.md section 5` (the hint row) | `create_action_answered_465_naming_a_competitor_is_a_hint_not_an_error` (`--test immediate_broadcast_hint_tests`) |
| a refusal keeps the inputs locked and the change | `wallet.md section 2` (from a broadcaster: a refusal moves no coin) | `test_broadcast_refusal_is_a_hint_that_keeps_inputs_and_change` |
| an invalid BEEF stores nothing | `wallet.md section 5` (internalize, not valid: unchanged, refused) | `test_internalize_invalid_beef` |
| a courier's stub is refused without storing | `wallet.md section 2` (from a courier) | `l2_23_pin_i_internalize_refuses_stub_without_storing` |
| a checked BUMP the gate defers lands `unproven` with an `unmined` request | `wallet.md section 2` (the host's depth); `tracker.md section 3` (no new `mined` until the tip is fresh) | `a_validated_bump_the_gate_defers_lands_unproven_with_an_unmined_request` |
| `customInstructions` over 1,000 bytes is refused; 1,000 is stored whole | `wallet.md section 3` (the bound, ruled 2026-10-24) | `custom_instructions_over_1000_bytes_is_refused_and_1000_is_stored_whole` (`--test internalize_custom_instructions_tests`) |
| an internalize whose post is refused keeps the consumed coin spent | `wallet.md section 2` (a hint moves no coin) | `test_internalize_broadcast_refusal_keeps_the_consumed_coin_spent` |
| an abort of a template releases its inputs | `wallet.md section 5` (abort `t`, `t` unknown or built: released) | `test_abort_unsigned_transaction_success` |
| an abort of a broadcast transaction with chain evidence is refused | `wallet.md section 5` (abort `t`, `t` announced or later: refused) | `test_abort_unproven_with_chain_seen_row_is_refused` |
| a broadcaster's status is never chain evidence, however often refreshed | `broadcaster.md section 1 B1` | `test_a_broadcaster_status_never_counts_as_chain_evidence_however_often_refreshed` |
| a proven descendant refuses the abort | `wallet.md section 12`, the lineage rule | `test_abort_sending_with_a_proven_descendant_is_refused` |
| a busy retirement after a broadcast abort is its success; `review_status` finishes it | `storage.md section 2 S9` | `a_busy_retirement_after_a_broadcast_abort_is_its_success_and_review_status_finishes_it` |
| exactly one of a concurrent abort and process wins | `storage.md section 2 S9` (one writer) | `test_concurrent_abort_and_process_exactly_one_wins` (`--test concurrent_tests`) |
| certificate issuance encrypts each field with an independent symmetric key and wraps that key in the master keyring; its fields and verifier keyring decrypt by the reference's procedure | `ts-stack@edf6e03 MasterCertificate.ts:277-293,380-420`; `VerifiableCertificate.ts:180-211`; bsv-rs #261 fixture at `2e4b477` | `toolbox_issued_certificate_decrypts_like_reference` |
| certificate intake preserves the signed Base64 field text byte for byte | `BRCs@5119a9b peer-to-peer/0052.md:97`; toolbox #197 and #203 | `reference_issued_certificate_survives_intake` |
| a stored reference-issued or Rust-issued certificate proves to the verifier and decrypts to the fixture's plaintexts | `conformance/upstream/261-certificate-field-value/` (bsv-rs #261); toolbox #203 | `reference_issued_certificate_proves_and_decrypts` |
| a corrupt master keyring entry is refused at prove time before a verifier keyring is returned | `ts-stack@edf6e03 MasterCertificate.ts:382-393,612-622`; toolbox #222 | `prove_certificate_rejects_corrupt_master_keyring_entry` |
| a legacy 31-byte field key is restored to the exact 32-byte key required by the verifier | `ts-stack@edf6e03 MasterCertificate.ts:393,606-619`; `VerifiableCertificate.ts:54-60,203`; toolbox #228 | `test_create_keyring_pads_legacy_field_key_for_verifier` |
| an empty master keyring refuses with the field recovery WalletError and the reference's exact text | `ts-stack@edf6e03 MasterCertificate.ts:573-622`; toolbox #229 | `test_create_keyring_empty_master_keyring_fails` |
| a missing master entry refuses with the field recovery WalletError and the reference's exact text | `ts-stack@edf6e03 MasterCertificate.ts:573-622`; toolbox #229 | `test_create_keyring_field_not_in_master_keyring_fails` |
| a non-Base64 master entry refuses with the field recovery WalletError and the reference's exact text | `ts-stack@edf6e03 MasterCertificate.ts:573-622`; toolbox #229 | `test_create_keyring_invalid_base64_master_key_fails` |
| non-UTF-8 field plaintext is refused before returning a verifier keyring | `ts-stack@edf6e03 MasterCertificate.ts:619-622`; toolbox #229 | `test_create_keyring_rejects_non_utf8_field` |
| a requested field name over 50 UTF-8 bytes refuses with the field recovery WalletError and its exact text (the reference also applies the rule before its subset loop, `MasterCertificate.ts:348/:138` and `:349-350/:49`; this port keeps it inside the recovery by #233's choice) | `ts-stack@edf6e03 MasterCertificate.ts:23-29,573-574,621-622`; toolbox #233 | `test_create_keyring_rejects_overlong_field_names` |
| the requested field names `__proto__`, `constructor`, and `prototype` refuse with the same field recovery error | `ts-stack@edf6e03 MasterCertificate.ts:23-29,573-574,621-622`; `SafeRecord.ts:2-3`; toolbox #233 | `test_create_keyring_rejects_unsafe_field_names` |
| valid names at 50 UTF-8 bytes, Unicode names, and safe lookalikes of reserved names still produce a verifier keyring | `ts-stack@edf6e03 MasterCertificate.ts:23-29`; `SafeRecord.ts:2-3`; toolbox #233 | `test_create_keyring_accepts_valid_field_names` |
| an invalid requested name absent from the certificate keeps the subset refusal ahead of field recovery | `ts-stack@edf6e03 MasterCertificate.ts:371-384`; toolbox #233 | `test_create_keyring_invalid_missing_field_keeps_subset_error` |
| equal and empty plaintext fields receive distinct 32-byte keys across fields and issuances | `ts-stack@edf6e03 MasterCertificate.ts:277-293`; toolbox #203 | `issuance_uses_fresh_keys_for_equal_and_empty_plaintexts` |

## 2. The writer and the pool

`StorageSqlx` (`src/storage/sqlx/storage_sqlx.rs`) holds two pools over one
SQLite file. `pool()` is the reader pool: ten connections, each
`PRAGMA query_only = ON`, so a write sent there fails with
`attempt to write a readonly database`. `writer()` is the writer pool: exactly
one connection, for single-statement writes through `writer_conn()`.
`write_tx()` opens a write transaction on the writer with `BEGIN IMMEDIATE`
(`begin_immediate`): the write lock is taken before any read, so a
contender in another process waits `busy_timeout` instead of failing with
`SQLITE_BUSY_SNAPSHOT` (517), and a contender in this process waits for the one
writer connection. It is the only way this crate opens a transaction:
`clippy.toml` disallows `sqlx::Pool::begin`, `Pool::begin_with`,
`Connection::begin` and `Acquire::begin`, and CI runs clippy with
`-D warnings`.

Both waits share one ceiling, `STORE_BUSY_CEILING_MS` (5,000 ms): the writer
pool's `acquire_timeout` and SQLite's `busy_timeout`. Past it the call answers
`Error::StoreBusy` having written nothing: `WriterNotAcquired` at the acquire
site (`begin_immediate`, `writer_conn`), `SqliteBusy { code }` for SQLite's
primary code 5 (517 included). A busy on a begin or a commit is `StoreBusy`;
any other failure there is `DatabaseError`. A reader-pool timeout stays the
plain SQLx error. Never a `StoreBusy` after a committed write: a write that
fails after the commit (the status write after a post, the retirement after a
broadcast abort) is logged and the call answers its success.

The Arcade SEEN, IN_BLOCK and MINED status paths all use
`mark_transaction_seen_on_network_by` in `storage_sqlx/monitor.rs`. It groups
the request and transaction status updates with the provider hint in one
`write_tx()`: one writer acquisition and one `busy_timeout` (#205), with
rollback on a status error or cancellation before commit. Hint-write errors
remain logged without being returned. Previously the hint write's wait was
followed by the status write's wait, risking the Arcade webhook sender's 10 s
timeout with the default 5 s store timeout. MINED's later proof-ingest
transaction stays separate (#201): it checks the tracker outside the writer,
and a busy there can follow committed statuses; a delivery retry is
idempotent. CLI #249 retains the deadline for the whole callback.

Nothing awaits a service or a chain tracker while holding the writer or a
`write_tx`: ask the network, then write. The BEEF walk reads on a reader
connection and writes its read checks through the writer it is passed, never
with the writer held (one connection would wait on itself). The services and
the tracker hang on the store (`set_services`, `set_chain_tracker`) and are
asked before the lock (`create_action`'s pre-check, `internalize_action`'s
verification, the monitor's fetches).

One store per file per process. `StorageSqlx` is `Clone`: a clone is a second
handle over the same writer pool, reader pool, settings, tracker, services,
broadcast memory, in-flight reservations and task locks. A `Wallet` and a
`Monitor` get clones of one store. Two `StorageSqlx::open` over one file are
two stores whose writers meet in SQLite's busy handler and two reservation sets.

| invariant | charter | test |
|---|---|---|
| a write on `pool()` fails under `query_only` | `storage.md section 2 S9` (one writer per store) | `a_write_on_pool_fails_under_query_only` (`--test store_write_lock_tests`) |
| a deferred `BEGIN` that read before writing answers 517 after another commit | `storage.md section 2 S9` (the lock before the read) | `a_deferred_begin_that_read_first_answers_517_after_another_commit` (`--test store_write_lock_tests`) |
| the same shape through `write_tx` waits and both commit, across two stores | `storage.md section 2 S9` (the lock before the read) | `the_same_shape_through_write_tx_waits_and_both_commit_across_stores` (`--test store_write_lock_tests`) |
| two handles of one store queue on one writer and both commit | `storage.md section 2 S9` | `two_handles_of_one_store_queue_on_one_writer_and_both_commit` (`--test store_write_lock_tests`) |
| a store opened twice has two writers; a shared store one | `storage.md section 2 S9` | `a_store_opened_twice_has_two_writers_and_a_shared_store_one` (`--test store_write_lock_tests`) |
| a wallet and a monitor over one store share the writer | `storage.md section 2 S9` | `a_wallet_and_a_monitor_over_one_store_share_the_writer` |
| the writer not acquired within the ceiling is `StoreBusy` | `storage.md section 2 S9` (a typed busy that promises nothing was written) | `the_writer_not_acquired_within_the_ceiling_is_store_busy` (`--test store_write_lock_tests`) |
| Arcade SEEN status contention takes less than 2 times `busy_timeout`, returns typed busy and writes nothing (#44, #205) | `storage.md section 2 S9`; `wallet.md section 6` R8 | `seen_status_busy_waits_once_without_writes` |
| Arcade IN_BLOCK status contention takes less than 2 times `busy_timeout`, returns typed busy and writes nothing (#44, #205) | `storage.md section 2 S9`; `wallet.md section 6` R8 | `in_block_status_busy_waits_once_without_writes` |
| Arcade MINED status contention takes less than 2 times `busy_timeout`, returns typed busy and writes nothing (#44, #205) | `storage.md section 2 S9`; `wallet.md section 6` R8 | `mined_status_busy_waits_once_without_writes` |
| Arcade status updates and their hint commit once per callback | `storage.md section 2 S9` | `status_delivery_commits_statuses_and_hint_once` |
| cancellation during an Arcade status write leaves both statuses and the hint unchanged | `storage.md section 2 S9`; `wallet.md section 6` R8 | `cancelled_status_delivery_rolls_back_statuses_and_hint` |
| an Arcade transaction-status failure rolls back its request status too | `storage.md section 2 S9` | `seen_status_updates_roll_back_together` |
| an optional Arcade hint-write failure keeps the status updates | `broadcaster.md section 1 B1`; toolbox #201 | `non_busy_broadcast_memory_error_keeps_status_updates` |
| an Arcade MINED or IMMUTABLE proof-ingest busy keeps its type and skips the fetch fallback (#44, #201) | `storage.md section 2 S9`; `wallet.md section 6` R8 | `replayed_mined_inline_proof_keeps_store_busy_without_fetch`, `mined_inline_proof_busy_after_sending_status_commit_retries_idempotently` |
| a unique violation is not `StoreBusy` | `storage.md section 2 S9` | `a_unique_violation_is_not_store_busy` (`--test store_write_lock_tests`) |
| both pools keep their pragmas | `storage.md section 2 S9` | `both_pools_keep_the_pragmas` (`--test store_write_lock_tests`) |
| the door's busy answer is typed at the wallet | `wallet.md section 6` R8 | `create_action_on_a_busy_store_is_wallet_busy` |
| every other `begin` is banned (`clippy.toml`) | `storage.md section 2 S9` | (no test) |
| no service or tracker await while the writer or a `write_tx` is held | `storage.md section 2 S9` | (no test) |
| SQL stays in `src/storage/sqlx/` | (no charter; #56) | `sql_stays_in_storage_sqlx` |

## 3. The services ladders

`Services` (`src/services/services.rs`) asks the explorers (WhatsOnChain,
BananaBlocks, Bitails) as couriers and the header service (chaintracks, then
BHS) for headers. The rules are the owner's word of 2026-10-22 (bsv-wallet-cli
#50) and Rule 28; the fixtures under `tests/fixtures/data/ladders/` replay each
ladder's scenes (`ladder_fixtures_replay`, a lib test: `tests/fixtures/ladders.rs`
is included by `src/services/ladder_fixtures_tests.rs` through `#[path]`, not
declared in `tests/fixtures/main.rs`, so it runs under
`cargo test --lib ladder_fixtures` and not under the `fixtures` integration
binary).

**The verdict rules.** A negative needs two clean answers: `get_utxo_status`
returns "not in the unspent set" only when `UTXO_NEGATIVE_PROVIDERS` (two)
explorers each say so, and "unspent" (a negative too: "no spender") only when
`UTXO_POSITIVE_PROVIDERS` (two) each say so. The start rotates one explorer on
per call; the ladder keeps asking until two agree or no rung is left. A fault
(a 429, a timeout, a 5xx, a malformed body, a BananaBlocks output whose script
is not the asked one, a rung closed by an earlier 429) is "could not look": it
moves the question to the next rung in the same call. One retry round, after a
two second pause, re-asks only a transient fault (`FaultKind::Transient`: a
5xx, a 408, a timeout, a transport failure); a deterministic fault would come
back the same and a rate-limited rung is closed. Anything else (an outage, one
unspent beside one not listed, a lone answer) is `status: "error"` with
`is_utxo: None`: could not look, never "nothing there". BananaBlocks answers an
outpoint only: for a script's question it is neither asked nor counted.
`get_raw_tx` is "not found" only when every explorer says no such
transaction; one fault makes the absence unknown (`could_not_look`).
`get_status_for_txids` is could-not-look when no chain index answered; two
chain indexes saying unknown override a broadcaster's known.

**The spender words (#96).** `is_utxo` binds, then proves (`spend_evidence`):
a provider names the spender (WhatsOnChain's spent route, BananaBlocks' txo
route, each the other's fallback); the named transaction's bytes, fetched by
`get_raw_tx` and bound to the txid, must have an input naming the outpoint;
`get_merkle_path` then gives a path only after its root met the header
service's header. The verdicts: `Spent` (bound and proven), `SpentHint` (bound,
no proof held, or two explorers agreeing not unspent), `UnspentHint` (two
unspent answers and no spender), `Unknown` otherwise. A spender named whose
bytes could not be read or do not spend the outpoint is `Unreadable`: `Unknown`
for the whole question, whatever the unspent sets say, so a host holds the coin
and asks again. A spender its bytes bind stands on one provider, even against
two unspent answers. `is_utxo_with_spender` returns the bound name beside the
verdict (`BoundSpender { txid, confirmed }`).

**The `RateGate` and its clock** (`src/services/providers/pace.rs`). A provider
never sleeps on a rate limit. A 429 is `Error::RateLimited` at once, carrying
the Retry-After seconds (delta-seconds or an HTTP date; the provider's default
when absent: one second for WhatsOnChain and Bitails, sixty for BananaBlocks;
held to `MAX_RETRY_AFTER_SECS`, 600), and the rung is closed until it passes: a
call inside the window is the same fault without a request. BananaBlocks also
paces itself with a token bucket (`BANANABLOCKS_PER_MINUTE`, 60: a burst of 30
refilled at 0.5 a second, so 60 in the opening minute and 30 a minute
sustained); an empty bucket is the fault too. A gate is the process's, not a
`Services`'s: `RateGate::shared` hands every provider built for one host key
the same gate, so several `Services` in one process spend one budget and a 429
one of them met closes the rung for all. The gate reads now from its `Clock`:
the wall clock in production, an instant held still in a test
(`RateGate::with_clock`), so a replay's calls land at one instant of the gate's
time. Every ladder records a rate limit in its call history as `RATE_LIMITED`,
apart from every other fault.

**The header ladder.** `get_height` and `get_chain_tip_header` read chaintracks,
then BHS, never an explorer; with none reachable the answer is an error, never
an explorer's number. `hash_to_header` reads the header service; with the
header service down it answers an error naming the setting and asks no
explorer, unless `ServicesOptions::break_glass_explorer_headers` is set (off by
default): then it asks WhatsOnChain and Bitails, each the other's fallback, and
binds the answer to the hash. The default tracker never takes an explorer's
root.

**The proof ladder.** `get_merkle_path` fetches nothing with no header service
configured: the answer is `merkle_path: None` with a note, never an unchecked
proof and never "not mined". A served path must resolve its header (else it is
dropped and the next rung asked) and its computed root must meet the tracker at
that height: a definite false is `NOTE_REFUTED` (a refutation, retained for the
re-prove) and the next rung is asked; a tracker error is a fault. Every
per-provider verdict rides in the answer's notes.

**The broadcast ladder.** `post_beef` submits to every named broadcaster in
parallel by default (`PostBeefMode::PromiseAll`, B3), retaining named transport
faults alongside answers. Explicit `UntilSuccess` keeps the ordered path and
its broadcast memory (`broadcast_seen`); the public post door records every
instance's result on the request. What comes back is classified
(`classify_broadcast_results`) and handed to the store as a hint: an acceptance
moves the request to `unmined` and the transaction to `unproven`; every other
word (a refusal, a 465, a double-spend word named or not, an orphan-mempool
hold, a transient word, no broadcaster reached) is written on the request's
history, the request stays `unsent` for the re-ask on the cadence
(`send_waiting_reask_minutes`: one minute, doubling, capped at sixty-four), and
a competitor the word names is queued for a proof ask. Only a competitor's
proof checked against our headers writes `doubleSpend`.

| invariant | charter | test |
|---|---|---|
| a negative needs two of the three explorers | `wallet.md section 2` (from an explorer: a hint); Rule 28 | `a_negative_needs_two_of_the_three` |
| a lone unspent is never the answer | `wallet.md section 2` | `a_lone_unspent_is_never_the_answer` |
| one negative and two faults is could not look | `tracker.md section 2` (fail closed) | `one_negative_and_two_faults_is_could_not_look` |
| an outage is could not look, never spent | `tracker.md section 2` | `an_outage_is_could_not_look_never_spent` |
| a raw-tx fault is not erased by the others' not found | `tracker.md section 2` | `a_raw_tx_fault_is_not_erased_by_the_other_explorers_not_found` |
| a raw-tx both explorers lack is not found | `tracker.md section 2` | `a_raw_tx_both_explorers_lack_is_not_found` |
| a fault is transient only for an outage; the retry round asks nothing else | `tracker.md section 2` | `a_fault_is_transient_only_for_an_outage` |
| a 429 is not asked again in the retry round | `tracker.md section 2` | `a_bitails_429_is_not_asked_again_in_the_retry_round` |
| BananaBlocks is not asked a script's question | `tracker.md section 2` | `bananablocks_is_not_asked_a_scripts_question` |
| `is_utxo` is the spender verdict in the scenes of #96 | `wallet.md section 2` (a stranger's spend is a chain fact only by its proof) | `is_utxo_is_the_spender_verdict_in_the_scenes_of_96` |
| a named spender whose bytes fault answers unknown | `wallet.md section 2` | `a_named_spender_whose_bytes_fault_answers_unknown` |
| a named spender whose bytes spend another outpoint answers unknown | `wallet.md section 2` | `a_named_spender_whose_bytes_spend_another_outpoint_answers_unknown` |
| a named spender without a proof is not a spend | `wallet.md section 2`; `storage.md section 2 S3` | `a_named_spender_without_a_proof_is_not_a_spend` |
| a proven spend answers its spender confirmed | `storage.md section 2 S3` | `a_proven_spend_answers_its_spender_confirmed` |
| a header-service fault names the bound spender unconfirmed | `tracker.md section 2` (fail closed) | `a_header_service_fault_names_the_bound_spender_unconfirmed` |
| no site names an explorer's unspent as a verdict | `wallet.md section 2` | `no_site_names_an_explorers_unspent_as_a_verdict` (`--test rule_28_unspent_hint_tests`) |
| a 429 closes the rung for its Retry-After and is the fault returned | `wallet.md section 4` (the re-ask is the tracker's, never a sleep) | `a_429_closes_the_rung_for_its_retry_after` |
| a 429 without Retry-After uses the default; huge values are held | `wallet.md section 4` | `a_429_without_retry_after_uses_the_default_and_huge_values_are_held` |
| a 429 is a fault at once and the rung is skipped until Retry-After | `wallet.md section 4` | `a_429_is_a_fault_at_once_and_the_rung_is_skipped_until_retry_after` |
| the bucket lets at most its budget through in any minute | (no charter; the owner's word of 2026-10-22) | `the_bucket_lets_at_most_its_budget_through_in_any_minute` |
| providers for one base share one gate | (no charter; the owner's word of 2026-10-22) | `providers_for_one_base_share_one_gate` |
| a held clock keeps the default window closed; the default clock is the wall clock | (no charter; #53) | `the_default_clock_is_the_wall_clock` |
| `get_height` is the header service's tip and asks no explorer | `wallet.md section 6` (the header service is the one headers machine) | `get_height_is_the_header_services_tip_and_asks_no_explorer` |
| `hash_to_header` with the header service down and the setting off asks no explorer; the error names the setting | `wallet.md section 6` | `hash_to_header_asks_no_explorer_when_the_header_service_fails` |
| the tracker holds no explorer unless `break_glass_explorer_headers` is set, and the default is off | `wallet.md section 6` | `the_tracker_holds_no_explorer_unless_break_glass_is_set` |
| the default tracker never takes an explorer's root | `tracker.md section 2` | `the_default_tracker_never_takes_an_explorers_root` |
| no proof is fetched from an explorer without a header service | `tracker.md section 2` | `no_proof_is_fetched_from_an_explorer_without_a_header_service` |
| a served path the tracker refuted is a refutation, not a proof | `tracker.md section 4` (`proofFailed`) | `a_served_path_the_tracker_refuted_is_a_refutation_not_a_proof` |
| two chain indexes saying unknown override a broadcaster's known | `broadcaster.md section 1 B1` | `test_two_chain_indexes_unknown_override_a_broadcasters_known` |
| every rejection code is a hint that keeps the word | `broadcaster.md section 1 B5` | `every_rejection_code_is_a_hint_that_keeps_the_word` |
| a 465 naming a competitor without its proof writes no word and queues it | `broadcaster.md section 1 B5`; `tracker.md section 1` (`conflicted`) | `a_465_naming_a_competitor_without_its_proof_writes_no_word_and_queues_it` |
| a 465 naming a competitor whose proof meets our header writes `doubleSpend` | `tracker.md section 1` (`conflicted`) | `a_465_naming_a_competitor_whose_proof_meets_our_header_writes_the_double_spend_word` |
| a double-spend word naming no competitor writes no word and schedules the re-ask | `tracker.md section 2` | `a_double_spend_word_naming_no_competitor_writes_no_word_and_schedules_the_reask` |
| a refusal from every source is still a hint | `broadcaster.md section 1 B3` (instances never vote) | `a_refusal_from_every_source_is_still_a_hint` |
| the cadence is one to sixty-four minutes, doubling | `wallet.md section 4` (the age re-ask on the host's clock) | `the_cadence_is_one_to_sixty_four_minutes_doubling` (`--test rule_28_cadence_tests`) |
| the sticky provider never moves ahead of a configured Arcade | (no charter) | `sticky_never_moves_ahead_of_a_configured_arcade` |
| every ladder scene under `tests/fixtures/data/ladders/` replays from its fixture | (no charter; #53) | `ladder_fixtures_replay` (a lib test, included from `src/services/ladder_fixtures_tests.rs`; not in the `fixtures` integration binary) |
| a service answer is built only by services | (no charter; #56) | `service_answers_are_only_built_by_services` |
| provider requests stay in providers | (no charter; #56) | `provider_requests_stay_in_providers` |

## 4. The proof path

A proof is checked against the header service before anything is stored.
Every delivery path meets one funnel, `StorageSqlx::ingest_merkle_proof`
(`src/storage/sqlx/storage_sqlx.rs`): the polling task
(`synchronize_transaction_statuses` through `get_merkle_path`), an Arcade or
ARC MINED webhook carrying `merklePath`, an SSE MINED frame with its inline
BUMP (`ingest_push_proof`), a relay drain, and the reorg task's re-prove.
`internalize_action`'s own BUMP claim passes the same check
(`validate_bump_claim`). The steps:

1. Parse the BUMP and compute its root for the txid (`InvalidProof` otherwise).
2. Ask the store's tracker `is_valid_root_for_height(root, height)`. No tracker
   wired: `TrackerUnavailable`, nothing stored, no attempt counted. A definite
   false: `InvalidMerkleRoot`, nothing stored. A tracker error:
   `TrackerError`, nothing stored, asked again later.
3. The gate: `max_acceptable_proof_height`, one row of `monitor_state` every
   process on the file reads, set by the header task once a header stayed the
   tip for a full cycle. Zero is closed. A proof above the gate is
   `DeferredAboveProcessedHeight`: not stored, never an attempt, re-presented
   once the header has aged. This is what keeps an orphan's proof out of
   `proven_txs`.
4. One transaction: `INSERT OR IGNORE INTO proven_txs` with the raw bytes from
   the request or the transaction row, the `proof_root_checks` record (the row
   is a checked row), the request and the transaction `completed`.

`synchronize_transaction_statuses` asks a queued competitor's proof before the
wallet's own requests (a competitor's checked proof is the one evidence for
`doubleSpend`), then adopts proof-less transactions that have no request
(`adopt_unproven_transactions`, at most `ADOPT_UNPROVEN_TX_LIMIT` a pass), then
asks the courier for every `unmined`, `unknown`, `callback`, `sending` and
`unconfirmed` request. Every pass that finds no proof is an attempt; past
`PROOF_ATTEMPTS_LIMIT_MAIN` (144 on mainnet, 10 elsewhere) the count writes no
word and the request is asked again every pass.

A `proven_txs` row with no `proof_root_checks` record (written before migration
005, or merged by sync) is checked once when a BEEF read needs it and demoted
when the tracker refutes it; a checked row is demoted only by the reorg and
review tasks, when a local header read finds its root gone.

Demotion (`demote_stale_proof_on`) is the funnel in reverse, in one
transaction: the request back to `unmined` (attempts 0, no proof link;
re-created with the proof row's bytes when the purge had deleted it), every
linked transaction back to `unproven` (still spendable, still listed:
"confirming"), the terminal `mined` broadcast memory forgotten, the
`proven_txs` row deleted last. No input is released, nothing is failed.

| invariant | charter | test |
|---|---|---|
| a proof stored through the funnel is a checked row | `tracker.md section 2` (only headers and proofs are evidence) | `a_proof_stored_through_the_funnel_is_a_checked_row` |
| a trackerless store refuses a BUMP and the row stays `unproven` | `tracker.md section 2` (fail closed) | `a_trackerless_storage_refuses_a_bump_and_the_row_stays_unproven` |
| a closed gate defers every proof | `wallet.md section 2` (the host's depth); `tracker.md section 3` (no new `mined` until the tip is fresh) | `a_closed_gate_defers_every_proof` |
| the gate is closed until the second observation of a tip | `tracker.md section 2` | `the_gate_is_closed_until_the_second_observation` |
| a header is processed only after a full cycle as tip | `tracker.md section 2` | `a_header_is_processed_only_after_a_full_cycle_as_tip` |
| a tracker outage keeps the stored BUMP and mutates nothing | `tracker.md section 4` (`recheck`: a header the service cannot answer is re-read later) | `a_tracker_outage_keeps_the_stored_bump_and_mutates_nothing` |
| an unchecked stored proof is demoted on read when its root is not active | `wallet.md section 2` (a stored proof is re-read by the guard, never trusted by age) | `an_unchecked_stored_proof_is_demoted_on_read_when_its_root_is_not_active` |
| an unchecked stored proof is kept when no tracker can decide | `tracker.md section 4` (`recheck`) | `an_unchecked_stored_proof_is_kept_when_no_tracker_can_decide` |
| the BEEF walk skips a refuted stored BUMP and demotes nothing | `wallet.md section 12` 7c | `the_beef_walk_skips_a_refuted_stored_bump_and_demotes_nothing` |
| break-glass asks the explorer only when the header service is down | `wallet.md section 6` | `break_glass_asks_the_explorer_only_when_the_header_service_is_down` |
| the root read decides stale; an unknown never does | `tracker.md section 1` (`stale`); ruling 5 | `ruling_5_the_root_read_decides_stale_and_an_unknown_never_does` |
| a proof whose root the active header still carries moves nothing | `tracker.md section 4` (a word whose root stands keeps its word) | `a_proof_whose_root_the_active_header_still_carries_moves_nothing` (`--test reorg_tests`) |
| demotion is one transaction | `storage.md section 2 S9` | `demotion_is_one_transaction` |
| demotion reverts to the pre-proof state and keeps the coins visible | `wallet.md section 12` 7b | `demoting_a_stale_proof_reverts_to_the_pre_proof_state_and_keeps_the_coins_visible` |
| a checked replacement proof for another block replaces the stored anchor | `tracker.md section 4` (`Reask.reorg`) | `a_validated_proof_for_a_different_block_replaces_the_stored_anchor` |
| a provider error does not hold a proof whose root moved | `tracker.md section 2` | `a_provider_error_does_not_hold_a_proof_whose_root_moved` (`--test reorg_tests`) |
| an unreadable header never demotes | `tracker.md section 4` (no header: the word stands with `recheck`) | `an_unreadable_header_never_demotes` (`--test reorg_tests`) |
| the courier's word announces only on unknown (7d) | `wallet.md section 12` 7d | `ruling_7d_step_4_the_courier_word_announces_only_on_unknown` |
| a never-held, never-mined request is counted past the limit and never written off | `wallet.md section 4` (#66: no count writes a word) | `a_never_held_never_mined_req_is_counted_past_the_limit_and_never_written_off` |
| a request the status sources hold is not failed past the limit | `wallet.md section 4` | `a_req_the_status_sources_hold_is_not_failed_past_the_limit` |
| a checked competitor proof retires the lineage | `wallet.md section 12`, the lineage rule | `a_checked_competitor_proof_retires_the_lineage` |
| a checked competitor proof outranks a known word | `wallet.md section 12`, the lineage rule (#42) | `a_checked_competitor_proof_outranks_a_known_word` |
| a coin a proven competitor spent is lost, whatever an explorer says | `wallet.md section 5` (competitor `c`, proof `p`: `lost(c)`) | `a_coin_a_proven_competitor_spent_is_lost_whatever_an_explorer_says` |
| adoption never touches a transaction that already has a request | (no charter) | `adoption_never_touches_a_tx_that_already_has_a_req` (`--test monitor_tests`) |
| the crate carries no header store of its own | `wallet.md section 6` | `the_crate_carries_no_header_store_of_its_own` (`--test rule_28_header_store_tests`) |

## 5. The monitor

`Monitor<S, V>` (`src/monitor/daemon.rs`) spawns one tokio task per enabled
`TaskConfig` (`src/monitor/config.rs`): `setup()` once, then `run()` every
`interval`, the opening run delayed by one interval unless `start_immediately`.
Before each run the task takes the store's task lock
(`try_acquire_task_lock`, TTL twice the interval, in memory per store) and
skips the run when another instance holds it. A `Monitor` holds a clone of the
wallet's store, so its writes queue on the same writer. The fifteen tasks, from
`src/monitor/tasks/`:

| task | interval | reads | may write |
|---|---|---|---|
| `clock` | 1 s | the wall clock | nothing |
| `new_header` | 60 s | the header service's tip (height and hash), never a courier | `monitor_state`: the ring of recent tips and the proof gate (`set_max_acceptable_proof_height`) once a header stayed the tip a full cycle; a remembered hash the service no longer carries is queued for the reorg task |
| `reorg` | 60 s | queued deactivated headers aged ten minutes (`REORG_PROCESS_DELAY_SECS`), the stored anchors citing them, one local header read per height | a moved root replaced from a checked provider proof, or demoted (`demote_stale_proof`), at most `REPROVES_PER_PASS` (4) a pass; a header unreadable three times (`MAX_RETRY_COUNT`) is dropped |
| `review_proven_txs` | 10 min | the anchors in the last `REVIEW_HEIGHTS` (12) heights below the gate, one canonical header per height | an empty stored block hash filled when its root is canonical; a moved anchor re-proved or demoted, a few a pass |
| `check_for_proofs` | 60 s (runs on the header flag or a two hour fallback) | the requests owed a proof, the courier | the funnel (section 4): `proven_txs`, `proof_root_checks`, request and transaction `completed`; attempts on the request; adoption of proof-less transactions |
| `arcade_events` | 60 s health cadence; the SSE stream is persistent | Arcade's status frames on the wallet's own stream | SEEN words: request `unmined`, transaction `unproven`; MINED with an inline BUMP through the funnel; a busy returns as `StoreBusy`, other missed ingests raise the proof flag; REJECTED and DOUBLE_SPEND_ATTEMPTED recorded as hints, the named competitors queued; `reorg_unmined` raises the flag and changes nothing |
| `send_waiting` | 5 min (minimum age 30 s; a local try-lock beside the task lock) | `unsent` and `sending` requests due by the cadence, their stored bytes | the BEEF rebuilt and posted; an acceptance: request `unmined`, transaction `unproven`; every other word a hint on the history, the request left `unsent`, a competitor queued |
| `fail_abandoned` | 5 min (timeout 5 min) | `unsigned` and `unprocessed` outgoing transactions older than the timeout | `abort_action` on each: inputs released, status `failed` |
| `unfail` | 10 min | `failed` transactions whose request is `invalid` or `doubleSpend` (hourly backoff), and `unfail` requests; the courier | a transaction the network knows: request `unmined`, transaction `unproven`; an `unfail` request with no proof back to `invalid`; a provider error holds it |
| `check_no_sends` | 24 h | `nosend` requests, the courier | `synchronize_transaction_statuses` when a proof is found (its own doc says that pass does not cover `nosend`) |
| `review_status` | 15 min | transactions beside their requests | check 1: a transaction whose request is `invalid` is `failed`; check 2: a `completed` request's transaction is `completed`; the descendants of a failed root whose request is `invalid` or `doubleSpend` with no chain evidence retired (check 4) |
| `purge` | 1 h | aged rows (default 30 days) | old `failed` and `invalid` requests deleted; `raw_tx` and `input_beef` cleared from completed ones |
| `compact_beef` | 15 min | completed requests' `input_beef` blobs | the blobs compacted: a proven ancestor replaced by its checked BUMP, never an unchecked one |
| `sync_when_idle` | 60 s | the wallet's activity clock | nothing in this crate (a hook for the host's sync) |
| `monitor_call_history` | 12 min | the services' call history (reset on read) | nothing; the history is logged |

Every write above goes through `MonitorStorage` on the one writer; every fetch
is made before the write, never under it. The daemon's `run_once` runs each
task once for a one-shot process.

| invariant | charter | test |
|---|---|---|
| an unreadable tip observes nothing | `tracker.md section 2` (fail closed) | `an_unreadable_tip_observes_nothing` |
| a tip decrease is an old header, never a reorg | `tracker.md section 3` (a reorg is the header service's) | `a_tip_decrease_is_an_old_header_and_changes_nothing` |
| a same-height hash change queues the old hash; the replacement is processed later | `tracker.md section 4` (a reorg: a local header read) | `a_same_height_hash_change_queues_the_old_hash_and_the_replacement_is_processed_later` |
| a replaced previous tip under a new tip is deactivated by the ring walk | `tracker.md section 4` | `a_replaced_previous_tip_under_a_new_tip_is_deactivated_by_the_ring_walk` |
| the header task's state survives a new task instance | (no charter) | `the_tracker_state_survives_a_new_task_instance` |
| an unreadable header is processed three times, then dropped | `tracker.md section 4` (`recheck`) | `an_unreadable_header_is_processed_three_times_then_dropped` (`--test reorg_tests`) |
| a closed gate audits nothing; an open gate finds the stale row | `tracker.md section 2` | `a_closed_gate_audits_nothing_and_an_open_gate_finds_the_stale_row` |
| an Arcade double-spend or rejected event is a hint | `broadcaster.md section 1 B1`, B5 | `an_arcade_double_spend_or_rejected_event_is_a_hint` |
| seven transient words keep the transaction announced with a scheduled re-ask | `broadcaster.md section 1 B4` (a) | `seven_transient_words_keep_the_transaction_announced_with_a_scheduled_reask` (`--test monitor_tests`) |
| an orphan-mempool word never releases inputs, whatever the attempt count | `wallet.md section 2` (from a broadcaster; from a count) | `orphan_mempool_never_releases_inputs_whatever_the_attempt_count` (`--test monitor_tests`) |
| `send_waiting` posts nothing over a failed parent | `wallet.md section 12`, the lineage rule | `send_waiting_posts_nothing_over_a_failed_parent` |
| `fail_abandoned` never retires an announced transaction | `broadcaster.md section 1 B4` (d); `tracker.md section 1` (`abandoned`) | `fail_abandoned_never_retires_an_announced_transaction` (`--test monitor_tests`) |
| an explicit `unfail` holds on a provider error | `tracker.md section 2` (fail closed) | `explicit_unfail_holds_on_provider_error` |
| `review_status` leaves the descendants of a failed root that have chain evidence | `wallet.md section 12`, the lineage rule | `review_status_leaves_the_descendants_of_a_failed_root_with_chain_evidence` |
| `review_status` completes a transaction whose request completed | `wallet.md section 5` (a proof `p` of `t`: `mined`) | `test_review_status_syncs_completed_proof` (`--test review_status_tests`) |
| a `sending` transaction's change is excluded from selection under the delayed rule | `wallet.md section 2` (the host's rule for `made` change) | `test_sending_tx_change_outputs_excluded_from_utxo_selection` (`--test review_status_tests`) |
| purge deletes only the terminal request statuses | (no charter) | `test_monitor_purge_failed_delete_targets_correct_statuses` |
| compaction attaches no unchecked proof without a tracker answer | `tracker.md section 2` | `compaction_attaches_no_unchecked_proof_without_a_tracker_answer` |
| adoption is bounded per pass and drains | (no charter) | `adoption_is_bounded_per_pass_and_drains` (`--test monitor_tests`) |
| a kept locked input is re-checked and restored on a later pass | `wallet.md section 4` (a stranger's spend on a cadence) | `a_kept_locked_input_is_rechecked_and_restored_on_a_later_pass` (`--test poisoned_chain_tests`) |
| an outside input the chain cannot vouch for stays locked | `wallet.md section 2` (the lock moves only on evidence) | `an_outside_input_the_chain_cannot_vouch_for_stays_locked` (`--test poisoned_chain_tests`) |
| `sync_when_idle` runs only when idle | (no charter) | `test_sync_when_idle_skips_when_active` |
| a found `nosend` proof is stored through the funnel | `wallet.md section 5` (a proof of the creator: `confirmed`) | (no test) |
| a task runs under its lock; a held lock skips the run | (no charter) | `test_task_lock_acquire_and_release` |

## 6. The status words and their transitions

`TransactionStatus` (`src/storage/entities/mod.rs`): `unsigned`,
`unprocessed`, `nosend`, `nonfinal`, `sending`, `unproven`, `completed`,
`failed`, `unfail`. `ProvenTxReqStatus`: `unprocessed`, `nosend`, `nonfinal`,
`unsent`, `sending`, `unmined`, `unknown`, `callback`, `unconfirmed`,
`completed`, `invalid`, `doubleSpend`, `unfail`, and the generic `pending`,
`inProgress`, `failed`, `notFound`. The stack's words (`tracker.md section 1`)
map onto them: `built` is `unsigned` to `unprocessed`; `announced` and `seen`
are `sending` and `unproven`; `mined(h)` is `completed`; `stale` is a demoted
`unproven` ("confirming"); `conflicted` is `doubleSpend`; `abandoned` and the
host's retire are `failed` with `invalid`. The transitions, with who writes each:

| from | to | written by | on |
|---|---|---|---|
| (none) | `unsigned`, request none | `create_action` | the template |
| `unsigned`, `unprocessed` | `nosend` / `nosend` | `process_action` | `noSend` |
| `unsigned`, `unprocessed` | `sending` / `unsent` | `process_action` | delayed or immediate send |
| `unsigned`, `unprocessed` | `unproven` / (kept) | `process_action` | already sent (`is_new_tx` false) |
| `sending` / `unsent` | `unproven` / `unmined` | the after-broadcast write, `send_waiting`, an Arcade SEEN frame, a status source holding it | an acceptance (a hint; the hint tier only) |
| `sending` / `unsent` | unchanged; the word on the history | the after-broadcast write, `send_waiting` | a refusal, a double-spend word, an orphan hold, a transient word |
| `unproven` / `unmined` | `completed` / `completed` | the funnel | a proof checked against the tracker, inside the gate |
| `completed` / `completed` | `unproven` / `unmined` | `demote_stale_proof` (the reorg and review tasks, the BEEF walk for an unchecked row) | the stored root gone from our header at its height |
| `sending`, `unproven` / `unsent`, `unmined` | `failed` / `doubleSpend` | the competitor path (`retire_poisoned_descendants`) | a competitor's checked proof spending an input |
| `unsigned`, `unprocessed`, `nosend`, `nonfinal`, `unfail` | `failed` | `abort_action`, `fail_abandoned` | the host's abort; inputs released |
| `sending`, `unproven` / any | `failed` / `invalid` | `abort_action` (broadcast kind), `retire_undeliverable_txid` | the host's word with no chain evidence; inputs released |
| any / `invalid` | `failed` | `review_status` check 1 | the request already `invalid` |
| `failed` / `invalid`, `doubleSpend` | `unproven` / `unmined` | `un_fail` | the network knows the transaction |
| (none) | `completed` or `unproven` / `unmined`, `unsent` | `internalize_action` | a received transaction, its BUMP checked or not |
| `completed`, `unproven`, `nosend` | merged | `internalize_action` | a known txid: outputs not yet held added |

| invariant | charter | test |
|---|---|---|
| an immediate send writes `sending` | `wallet.md section 5` (create `t`: `built` if signed) | `test_process_action_immediate_broadcast` |
| immediate and delayed sends write the request `unsent`; `noSend` writes `nosend` | `wallet.md section 5` | `test_process_action_proven_tx_req_status_modes` |
| an acceptance racing an abort never lifts the `failed` transaction, never reopens the `invalid` request | `tracker.md section 1` (a hint moves only the hint tier) | `test_race_broadcast_status_vs_abort` (`--test concurrent_tests`) |
| an acceptance never lifts a `completed` transaction, never reopens a `doubleSpend` request | `tracker.md section 1` | (no test): the `NOT IN` guards of the after-broadcast write (`src/storage/sqlx/process_action.rs`) hold it; no test seeds a `completed` or `doubleSpend` row before an acceptance |
| a double-spend word releases no input | `wallet.md section 2` | `test_double_spend_word_releases_no_input` |
| a service error keeps the inputs locked | `wallet.md section 2` | `test_service_error_keeps_inputs_locked` |
| a transaction the status service knows is promoted, not retired | `broadcaster.md section 1 B1` | `a_tx_the_status_service_knows_is_promoted_not_retired` (`--test retire_rejected_tests`) |
| an absent transaction retired keeps an explorer's unspent input locked | `wallet.md section 2` (from an explorer) | `absent_tx_is_failed_and_an_explorers_unspent_input_stays_locked` (`--test retire_rejected_tests`) |
| an already failed root still retires its descendants | `wallet.md section 12`, the lineage rule | `an_already_failed_root_still_retires_its_descendants` (`--test poisoned_chain_tests`) |
| a `nosend` merge with a checked BUMP links the proof | `wallet.md section 5` (internalize rows) | `test_merge_nosend_validated_bump_links_proven_tx` |
| a `nosend` merge without headers refuses and leaves the action unchanged | `tracker.md section 2` (fail closed) | `test_merge_nosend_without_headers_is_unchanged` |
| a transaction's status is read from the request by the review pass | `wallet.md section 5` | `test_review_status_marks_tx_failed_when_req_invalid` (`--test review_status_tests`) |

## 7. Divergences

Where the code and a charter at `36d2d6b` disagree, both cited; nothing judged.

| # | the code | the charter |
|---|---|---|
| D1 | `abort_action` honours an abort of a broadcast transaction (`sending`, `unproven`) when the store holds no chain evidence: inputs released, outputs dead, request `invalid`, descendants retired (`src/storage/sqlx/abort_action.rs`, module doc; `test_abort_sending_transaction_without_chain_evidence_succeeds`); `retire_undeliverable_txid` is the same host's word | `wallet.md section 5`: abort `t`, `t` announced or later: unchanged, refused; `tracker.md section 1`: `abandoned` refused once the word left `built`; `broadcaster.md section 1 B4` (d): a template already announced is never abandoned; the exit is the replacement spend (B4 (c), #67) |
| D1 (closed) | the code follows the charter since #165: `abort_action` and `retire_undeliverable_txid` refuse once the store holds an accepting word for the transaction, honour a `sending` row no instance accepted, and ask no chain or explorer on the abort path | the row above, kept as the record of the reading |
| D2 | `review_proven_txs` audits a fresh window of the last 12 heights below the gate every ten minutes (`REVIEW_HEIGHTS`, `src/monitor/tasks/review_proven_txs.rs`; `docs/REORG-DIVERGENCES.md` R4 against the reference) | `tracker.md section 4`, "Never a schedule over every transaction": the 12-height review every ten minutes is named as what the rule replaces; `wallet.md section 4`, "Never a sweep" |
| D3, closed (#66, #167) | `post_beef` defaults to `PromiseAll`, one pass over every named instance with every result retained; the post door records those words per instance | `broadcaster.md` B3, ruled in section 6; `UntilSuccess` is an explicit opt-in on the 0.14 line pending the owner's removal word |
| D4 | `process_sync_chunk_internal` takes the incoming row when `chunk.updated_at > local.updated_at` (`src/storage/sqlx/sync.rs`; `test_process_sync_chunk_upsert_existing`) | `storage.md section 2 S4`: no timestamp is a word; a merge never compares `updated_at` (a draft row, not yet ruled, as `storage.md section 2` says after its table) |
| D5, closed (#169) | `internalize_action` marks received origin as `outputs.purpose = "receive"`; the change count and allocator offer unproven receipts only when the current action supplies the exact `underReceivedRule` label, recorded with that action; completed receipts and own change retain their eligibility | `wallet.md section 2` and `section 5` (R1 (d)): received pending coins require the action's rule at the door, off by default and recorded on the action |
| D6 | the `WalletStorageManager` and `StorageClient` keep the reference's active-and-backups shape; no push of an action to a second store before the broadcast | `storage.md section 2 S1`: two instances either share one store or the spending instance pushes its action to the other's store before it broadcasts (ruled); `storage.md section 6` records that S1's push is not implemented by the CLI |

## 8. Rows with no test

| row | section |
|---|---|
| every other `begin` is banned (`clippy.toml`); the gate is clippy with `-D warnings`, not a test | 2 |
| no service or tracker await while the writer or a `write_tx` is held | 2 |
| a found `nosend` proof is stored through the funnel (`check_no_sends.rs` says the pass it calls does not cover `nosend`) | 5 |
| an acceptance never lifts a `completed` transaction, never reopens a `doubleSpend` request (the `NOT IN` guards of the after-broadcast write) | 6 |
