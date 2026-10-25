# The door split: allocate-and-sign, then broadcast (#66)

A design, no code. Labels: `[X]` read in the code at the tip of `origin/beta`
(c444c14; the cited files were unchanged since 6317222 at that commit; by beta 901d72c `create_action.rs` moved below `:2492` and line 28 carries the one citation that shifted) or run; `[D]` told (the issue, its comments, a charter's ruling).
Pins: the stack's charters at bsv-stack-lean `origin/main` 360c941
(`docs/charters/wallet.md`, `storage.md`, `broadcaster.md`); the CLI at
bsv-wallet-cli `origin/main` 5e0dbb0; #53's door fixtures at PR #87 (ab53007);
#54's benches at PR #86 (2cda035). The consumer is the CLI's #86 `[D]`.

## 1. Today's sequence inside `create_action_offering`

`Wallet::create_action_offering` (`src/wallet/wallet.rs:1018`) is
`create_action_at(args, originator, Some(offer))` (`wallet.rs:1027`); the
`WalletInterface` `create_action` is the same with no offer (`wallet.rs:1842`).
Every step below is `[X]`.

| # | step | where | rows and statuses written | lock, transaction, network |
|---|---|---|---|---|
| 1 | originator check, then the toolbox's own `spend_lock` taken | `wallet.rs:1037` | none | the mutex is held until the call returns, through the post |
| 2 | BRC-98/99 spending and basket permission checks | `wallet.rs:1040-1065` | none (the permissions manager's own) | may await the permissions manager |
| 3 | flags read: `sign_and_process`, `is_sign_action`, `no_send`, `accept_delayed_broadcast`, `send_with` | `wallet.rs:1082-1115`, `is_sign_action` at `wallet.rs:3322` | none | |
| 4 | the signer closure built (local CPU: `build_unsigned_transaction`, `WalletSigner::sign_transaction`) and the `CreateActionDoor { offer, sign_and_process }` | `wallet.rs:1124-1190`; `src/storage/traits.rs:299-320` | none | |
| 5 | `storage.create_action_at_door` | `wallet.rs:1192`; `src/storage/sqlx/create_action.rs:429` | the default basket may be created (`create_action.rs:441`) | before the lock, its own connection |
| 6 | the caller's `inputBEEF` roots read against the tracker | `create_action.rs:447-449` | none | the tracker (a header service), before the lock |
| 7 | `precheck` on a reader: inputs validated, change planned as a dry run, coins reserved in-process (`PickReservation`), the input BEEF assembled, every stored root read against the tracker | `create_action.rs:590-734` | the walk's read checks, one short write each through the writer (`:695,715`) | the tracker; no write lock |
| 8 | `step`: `BEGIN IMMEDIATE` on the one writer | `create_action.rs:760`, `storage_sqlx.rs:507` | | the write lock opens |
| 9 | the transaction row | `create_action.rs:1640` | `transactions` status `unsigned`, `is_outgoing` 1 | under the lock |
| 10 | labels, then the allocation from the pre-checked coins within the offer | `create_action.rs:777-806`, `allocate_change_input_by` `:2700` (`:2667` at c444c14; the bench-support accessor and the #105 doc comment landed above it) | none yet; a shortfall is `Step::Again`, rolled back | |
| 11 | the inputs locked | `create_action.rs:1744` (`update_output_spent`) | `outputs` of each input: `spendable` 0, `spent_by` = the row's id | |
| 12 | outputs inserted | `create_action.rs:866-934` | user outputs `spendable` 1; change outputs `spendable` 0 (`:923`), empty script | |
| 13 | the BEEF from stored bytes, the tracker answered from the pre-check | `create_action.rs:1026-1056` | `transactions.input_beef` | a root unread: `Again`, rolled back |
| 14 | the signer, inside the transaction | `create_action.rs:1073` | none | local CPU, no await |
| 15 | `process_action_writes` on the same connection | `create_action.rs:1086`; `process_action.rs:576` | `transactions`: `txid`, `raw_tx`, status from `determine_statuses` (`:529`): `unprocessed`, or `nosend`; `input_beef` NULL (`:370`). `outputs`: `txid`, offsets, change scripts, `spendable` 1 unless noSend (`:676`). `proven_tx_reqs` inserted: `unprocessed`, `unsent` (delayed) or `nosend` (`:715`). Then, unless noSend: req `unsent` with `attempts + 1` (`:768,796`, `:499`), transaction `sending` (`:775,803`); `sendWithResults` `sending` | |
| 16 | commit | `create_action.rs:1103` | all of the above, once | the write lock closes |
| 17 | the full BEEF: the input BEEF plus the signed tx, pruned to the ancestor closure, V1 | `wallet.rs:1257-1329` | none (a hex dump to `temp_dir`, `:1338`) | |
| 18 | `services.post_beef`, the ladder with its retries | `wallet.rs:1377` | none | the network; the `spend_lock` still held |
| 19 | `update_transaction_status_after_broadcast` | `wallet.rs:1451`; `process_action.rs:918` | success: `transactions` `unproven`, `proven_tx_reqs` `unmined` (`:951,961`). Any other word: `record_immediate_broadcast_hint` (`storage_sqlx.rs:3782`): req stays `unsent`, `attempts`, `history`, a named competitor queued; the transaction stays `sending` | its own `write_tx` (`:940`); a refusal reads the status sources before it (`:929`) |
| 20 | `sendWithResults` marked `unproven` on success | `wallet.rs:1456`, `:2862` | none | |
| 21 | `CreateActionResult { txid, tx, no_send_change, send_with_results, beef }` | `wallet.rs:1474-1493` | none | the `spend_lock` released |

The deferred path (`is_sign_action`): steps 1 to 13 and 16 (no signer, no
process writes; the row stays `unsigned`), the template cached in
`pending_transactions` (`wallet.rs:1548-1571`), the answer
`signable_transaction` with the reference (`wallet.rs:1627`). `sign_action`
(`wallet.rs:1850`) later runs `process_action` (`:1987`) and the same
broadcast half (`:2041-2124`) under the same `spend_lock` (`:1858`).

A BEEF that cannot be built (step 17 answers `None`) aborts the action and
answers an error (`wallet.rs:1427-1443`): the one committed write the
broadcast half makes besides step 19.

## 2. The split

Two public methods on `Wallet<S, V>`, beside the two that exist. The storage
trait needs nothing: the door already stops at the commit
(`create_action.rs:1103`), and the two writes of the broadcast half are trait
methods already (`update_transaction_status_after_broadcast`, `abort_action`).

```rust
impl<S, V> Wallet<S, V> {
    /// Steps 1 to 17: the lock, the checks, the door (one write transaction:
    /// the allocation, the signing, processAction's writes, one commit), the
    /// BEEF. Nothing is posted. The rows are as after the door today.
    pub async fn allocate_and_sign(
        &self, args: CreateActionArgs, originator: &str, offer: &[i64],
    ) -> bsv_rs::Result<Signed>;

    /// Steps 18 to 21: the post through the ladder, the status writes after
    /// the broadcaster's word, the answer. No lock of the wallet's is held;
    /// many may run at once. A `Signed` that posts nothing (noSend, delayed,
    /// a template) answers its result unchanged.
    pub async fn broadcast(&self, signed: Signed) -> bsv_rs::Result<CreateActionResult>;

    /// The composition, byte for byte today's answer and today's rows.
    pub async fn create_action_offering(
        &self, args: CreateActionArgs, originator: &str, offer: &[i64],
    ) -> bsv_rs::Result<CreateActionResult> {
        let signed = self.allocate_and_sign(args, originator, offer).await?;
        self.broadcast(signed).await
    }
}

/// What `CreateActionResult` carries before the post, plus what the post needs.
pub struct Signed {
    pub reference: String,                 // the row's reference (`transactions.reference`)
    pub txid: Option<[u8; 32]>,            // None for a template
    pub tx: Option<Vec<u8>>,               // the signed raw tx; None for a template
    pub beef: Option<Vec<u8>>,             // step 17's bytes: today's `CreateActionResult.beef`
    pub no_send_change: Option<Vec<Outpoint>>,
    pub send_with_results: Option<Vec<SendWithResult>>, // the process step's words (`sending`)
    pub signable_transaction: Option<SignableTransaction>, // the deferred shape, template only
    post: Post,                            // Now | Delayed | NoSend | Template
}
impl Signed {
    pub fn reference(&self) -> &str;
    pub fn txid(&self) -> Option<&[u8; 32]>;
    pub fn posts(&self) -> bool;           // Post::Now
    pub fn into_result(self) -> CreateActionResult;  // the answer with no post
}
```

**What `broadcast` takes: the `Signed` value, by value.** `[X]` The post needs
the full BEEF of step 17, which lives only in memory in that shape: the
transaction row's `input_beef` is set NULL at the process step
(`process_action.rs:370`), and rebuilding from the request row's `raw_tx` and
`input_beef` is the monitor's path (`storage_sqlx.rs:5192`), another assembly
and a store read under no lock that could race the monitor's own re-post. The
answer also needs the process step's `send_with_results` and `no_send_change`,
which only the allocating call has. A reference or a txid alone would need a
user-scoped lookup and a second BEEF builder, and the two results could then
differ. By value: a `Signed` posted once is consumed, so a double post is a
compile error, not a race. `Signed` is owned data, `Send + 'static`: a host
may move it into a task. A `broadcast_by_reference` for a host that lost the
value between the calls is section 7's question 5; the monitor's send-waiting
task already is that path.

**`noSend`**: `allocate_and_sign` alone is today's `noSend` result: the door
writes `nosend` rows (`process_action.rs:529-534`), change stays
`spendable` 0 (`:676`), `send_with_results` is `Some([])`, `beef` is step
17's bytes; `Post::NoSend`, and `broadcast` answers `into_result()` with no
post, as the composition needs. **`acceptDelayedBroadcast`**: the door writes
`sending` and `unsent` (`:538`, `:768-781`); `Post::Delayed`; `broadcast`
posts nothing; the monitor's send-waiting task posts on the cadence, as today
(`wallet.rs:1331` skips the post for both). **A template** (`is_sign_action`):
`allocate_and_sign` writes the `unsigned` row, caches the pending
transaction, answers `Post::Template` with `signable_transaction`; `broadcast`
answers `into_result()`. The composition answers exactly `wallet.rs:1614-1634`.

## 3. The rows at each step against the charter

The charter's words (`wallet.md:13`) are the tracker's; the toolbox's stored
statuses are the reference's views of them (`wallet.md:13`, last sentence).
Rows `[X]`; the word each row is, `[D]` from `wallet.md:89,94` and the
broadcaster charter B1 (`broadcaster.md:15`).

| call | table | before | after (immediate) | after (delayed) | after (noSend) | the charter's transition |
|---|---|---|---|---|---|---|
| `allocate_and_sign` | `transactions` | no row | `sending` (`unsigned`, `unprocessed`, `sending` in one transaction) | `sending` | `nosend` | `absent` to `pending`, the action `built` (signed at the door; `wallet.md:89`: "built if signed") |
| | `outputs` (inputs) | `spendable` 1, `spent_by` NULL | `spendable` 0, `spent_by` = row | same | same | each coin `locked(t)` (`wallet.md:90-92`; `storage.md` S2: the lock is the store's own act) |
| | `outputs` (change) | none | `spendable` 1, script, `txid` | same | `spendable` 0 | the change `pending`, offered under the host's `made` rule |
| | `proven_tx_reqs` | none | `unsent`, `attempts` 1 | `unsent`, `attempts` 1 | `nosend` | no word: the request to announce |
| `broadcast`, accepted | `transactions` | `sending` | `unproven` | not called | not called | `built` to `announced` (B1: the hint tier) |
| | `proven_tx_reqs` | `unsent` | `unmined` | | | |
| `broadcast`, any other word | `transactions` | `sending` | `sending` | | | no word moves (B1; `wallet.md:97`: a hint moves nothing but the hint tier; the re-ask) |
| | `proven_tx_reqs` | `unsent` | `unsent`, `attempts`, `history`, a competitor queued | | | |
| `broadcast`, no BEEF | both | `sending`, `unsent` | the abort path as today (`abort_action.rs:154-158,203-215`) | | | `built` to `abandoned` (`wallet.md:95`) |

`built` is written at the commit of `allocate_and_sign`; `announced` at the
success write of `broadcast`; the order cannot invert because the second write
is keyed on the row the allocating call committed (`process_action.rs:947-960` update
`WHERE txid = ?`). A broadcast that fails leaves the row as today: `sending`
and `unsent` for the monitor, the hint on the request's history.

**The fixture.** PR #87's `tests/fixtures/data/storage/create_signed_change.json`
(`[X]` its calls: `create_action` with `sign: true`, then `send_waiting`
twice around `age_requests`) records exactly the committed rows of the door
plus the monitor's post: its expected rows after call 1 are the rows after
`allocate_and_sign`, and its rows after call 2 are the rows after an accepted
`broadcast` (both write `unproven` and `unmined`, `storage_sqlx.rs:5382-5388`
and `process_action.rs:951,961`). The fixtures run at the storage layer
(`tests/fixtures/storage/operations.rs:120-165` calls
`create_action_at_door`), so the equality the issue asks for needs the harness
to grow two wallet-layer methods, `allocate_and_sign` and `broadcast` against
`MockWalletServices`, and one new fixture, `door_split_signed_then_broadcast.json`,
whose expected section the test asserts equal to a `create_action_offering`
fixture with the same seed and options (`create_offering_100.json`'s shape
with `signAndProcess: true`). The row equality is the proof; the result
equality (`txid`, `tx`, `beef`, `sendWithResults`) is in the same expected
section.

## 4. The lock

**What the host holds a lock around:** `allocate_and_sign` only. Inside it
the toolbox takes its own `spend_lock` (`wallet.rs:1037`) and one write
transaction (`create_action.rs:760` to `:1103`): the pre-check's picks are
reserved in-process (`PickReservation`, `:470`), the allocation, the lock of
the inputs, the signing and the process writes are one commit. **What runs
without it:** `broadcast`, concurrently for many `Signed` values: the post
awaits the network with no lock of the wallet's; the status write after the
word opens its own short `write_tx` (`process_action.rs:940`) after the
status-source read (`:929`), as S9 requires (no network await under the
write lock, `storage.md:36`).

**A host without a lock is still correct** `[X]`: two calls in one process
never pick the same coin (the reservation; a shortfall caused only by calls in
flight is `StoreBusy { InFlight }`, `create_action.rs:675`, never a wrong
allocation); two processes on one file meet at `BEGIN IMMEDIATE` (one waits
`busy_timeout`, `storage_sqlx.rs:502-508`), and a coin taken between the
pre-check and the lock rolls the step back (`Again::Uncovered`, `:802`), the
second time `StoreBusy { MovedUnderUs }` (`:492`). The host's lock buys FIFO
order and no busy answers, not correctness. The CLI's spend guard and
assessment (`spend_gate.rs:1371-1420` at 5e0dbb0, `[X]`) are reads before the
call; they stay under the host's lock so the offer is not stale when the door
opens.

**The CLI's question: one call, or signing as its own step?** One call, as
today. `[X]` The signer runs inside the write transaction between
`create_action.rs:760` and `:1103` as a plain closure (`DoorSigner`, local
CPU, nothing awaited, `:1073`), which S9 allows: the rule bans a network or
tracker await under the lock, not CPU. Signing as its own step would commit
`unsigned` rows with the inputs locked, release the writer, then open a second
write transaction for the process writes (the deferred path's shape,
`wallet.rs:1987`): two lock acquisitions per spend, two chances of a busy, and
a crash between them leaves a locked `unsigned` row that only an abort
releases. The deferred path exists for the caller who must sign elsewhere; it
stays. The lock scope the CLI holds is the same either way `[D]`.

**The toolbox's own `spend_lock`** spans the post today (`wallet.rs:1037`
to `:1493`). In the split it spans `allocate_and_sign`. Whether the
composition `create_action_offering` keeps today's span across `broadcast`
is a behaviour choice (two one-call clients could post concurrently):
section 7, question 6. The recommendation: the refactor PR keeps today's
span in the composition; releasing it is its own commit with the bench.

## 5. Errors and retries

`allocate_and_sign` refuses with nothing written, every time: the door rolls
back on every error (`create_action.rs:736-742`), and the refusals before the
door write nothing. The host's lock is released with no row to clean.

| refusal | where | the host |
|---|---|---|
| `Insufficient funds: need N, have M` (`error.rs:136`) | the plan over the offer (`create_action.rs:680-688`), or an empty offer | re-assess and offer again: the CLI already does one full `assess` and a second call `[D]` |
| `Unspendable coin: <outpoint>` (`error.rs:153`) | a picked coin's stored ancestry is incomplete and the rest do not cover (`:685`) | drop the coin from the offer, re-assess |
| `WalletBusy` (`error.rs:328`): `InFlight` (`:675`), `MovedUnderUs` (`:492`), `SqliteBusy` or `WriterNotAcquired` at begin or commit (`storage_sqlx.rs:72-95`) | the writer, within `STORE_BUSY_CEILING_MS` | repeat the call unchanged after the wait; nothing was written (S9, R8) |
| `ServiceError` naming a height | a root the header service did not answer, twice (`:480-485`) | later; a header service fault |
| `Signing failed: ...`, `Process action failed: ...` | `DoorRefusal::Sign`, `::Process` (`wallet.rs:1198-1217`) | the caller's error; the action was never written |
| permission refusals | `wallet.rs:1040-1065` | the caller's |

`broadcast` never answers a broadcaster's word as an error (`wallet.rs:1462-1465`):

| outcome | rows | the answer | the monitor |
|---|---|---|---|
| accepted | `unproven`, `unmined` | `sendWithResults` `unproven` | proofs on the cadence |
| a refusal, a double-spend word, orphan mempool, a transient word, no broadcaster reached (the post's `Err`, `wallet.rs:1416-1422`) | hint: req `unsent`, `attempts`, `history`; competitor queued; transaction `sending` | the txid with `sending` | `send_waiting_transactions` (`storage_sqlx.rs:5192`) takes `unsent` and `sending` requests, writes `sending` (`:5366`), re-posts, on acceptance `unmined` and `unproven` (`:5382-5388`), else the hint again; no count writes a word (`wallet.md:70`, #66). A host's out-of-band verdict: `retire_undeliverable_txid` (`storage_sqlx.rs:3640`) |
| the status write fails after the post | unchanged | the txid, success; marker `post_broadcast_status_write_failed` (`:1454`) | the request is `unsent`: the next pass re-posts, an accepted tx answers success again, `unmined` |
| no BEEF (`beef` None) | the abort path | the error, as today (`:1441`) | none |

Under no host lock the monitor's pass and a host's `broadcast` may post the
same request; today too (the monitor never held the host's lock). Both land
on `WHERE txid = ? AND status NOT IN (...)` writes (`process_action.rs:947-960`).

## 6. Nothing changes a transition

Every status word the sequence writes today, `[X]`:

- `transactions`: `unsigned` (`create_action.rs:1640`); `unprocessed`, `nosend` (`process_action.rs:529-545` via `:666`); `sending` (`:775`, `:803`); `unproven` (`:951`); `failed` only by the abort on the no-BEEF fault (`abort_action.rs:158`).
- `proven_tx_reqs`: `unprocessed`, `unsent`, `nosend` (`:715` via `:529-545`); `unsent` with `attempts + 1` (`:768`, `:796`; the hint `storage_sqlx.rs:3815`); `unmined` (`:961`); `invalid` only by the abort of a broadcast transaction (`abort_action.rs:214`).
- `outputs`: `spendable` 0 and `spent_by` on the inputs (`create_action.rs:1744`); `spendable` 1 on user outputs and, unless noSend, on change (`process_action.rs:676`).
- `sendWithResults`: `sending`, `unproven`, `failed` (`process_action.rs:762-814`; `wallet.rs:2869`).

For the keeper: the split writes every one of these words at the same site,
in the same order, inside the same transactions as today. It adds no status,
moves no word to another call than the one that writes it today (the words of
steps 9 to 16 belong to `allocate_and_sign`, of 19 to `broadcast`), and
changes no order: `unsigned`, `unprocessed`, `sending` commit together, then
`unproven` or the hint. The one thing that moves is where a lock may be
released: between the commit (`create_action.rs:1103`) and the post
(`wallet.rs:1377`), where today no row is written. Anything that would move a
word is a question below, not a proposal.

## 7. Open questions

For the keeper (captain-bsv-stack-lean):

1. The toolbox commits `sending` on the transaction and `unsent` on the request before any post (`process_action.rs:775,796`). Section 3 reads this as the `built` view, since `built` to `announced` is the hint tier's move on a broadcaster's word (B1). Is `sending` before a post a `built` view, or does the charter want a word that says "built, a post intended" separately?
2. A template under the split (`is_sign_action`): `allocate_and_sign` writes `unsigned`, the charter's `unknown` (`wallet.md:89`). The value is typed `Signed` but is not signed. Is the one type with `Post::Template` acceptable to the charter's reading, or does the split want a second return type for the template?
3. The abort on the no-BEEF fault runs after `built` committed (`wallet.rs:1430`): `built` to `abandoned` by the wallet's own fault, `wallet.md:95`. Confirm this is the row the charter means, with no broadcaster asked.
4. `attempts` is incremented at the process step (`process_action.rs:499`), before any post. A count, not a word; confirm it is not a word.

For the owner (a public API shape):

5. `Signed` as a public struct: public fields or accessors; `Clone` or not (a clone invites a double post); and whether a `broadcast_by_reference(reference)` for a host that lost the value is wanted now, or left to the monitor's send-waiting task, which is that path already.
6. Whether the composition `create_action_offering` keeps the toolbox's `spend_lock` across the post (today's span) or releases it after `allocate_and_sign`. The rows are the same either way; the concurrency of one-call clients is not.
7. The names: `allocate_and_sign` and `broadcast` as the CLI asked, or `build` and `announce` as the charter's words.
8. Whether `sign_action`'s second half (`wallet.rs:1987-2124`, the same code) gets the same split in a later PR, and whether `broadcast` then takes a `Signed` built from a `sign_action`.
9. Whether `broadcast` on a `Signed` that posts nothing answers the result unchanged (the composition's need) or a typed refusal.

## 8. The proof plan for the implementing lane

1. **The refactor PR.** `allocate_and_sign` and `broadcast` extracted from `create_action_at` with the composition in place; `cargo test --lib` and `cargo test --test fixtures` unchanged, no fixture rewritten (`UPDATE_FIXTURES` never set): the proof that no row moved.
2. **The fixtures (PR #87's harness).** Two wallet-layer methods in `tests/fixtures/storage/operations.rs`, `allocate_and_sign` and `broadcast`, against `MockWalletServices`; new fixtures: `door_split_signed_then_broadcast.json` (accepted), `door_split_broadcast_refused_keeps_sending.json` (a refusal: `sending`, `unsent`, the hint), `door_split_nosend_broadcast_is_noop.json`, `door_split_delayed_then_send_waiting.json`, `door_split_template.json`. One test asserts the expected rows of `door_split_signed_then_broadcast.json` equal to those of the one-call fixture with the same seed. A planted swap of the flags (`no_send` for `accept_delayed_broadcast`) must redden the fixtures; shown, reverted.
3. **The differential run.** A test helper runs the same `CreateActionArgs` on two fresh in-memory stores with the same seed and the same mock answers, one through `create_action_offering`, one through the two calls, dumps both with #87's normalizer and asserts equality, for every `create_*` fixture's inputs and for the three option shapes; a planted missing `update_transaction_status_after_broadcast` in `broadcast` must fail it.
4. **The bench (PR #86's `benches/door.rs`).** `allocate_and_sign_{coins}_coins_{offered}_offered` beside `create_action_offering_{coins}_coins_{offered}_offered`, and `broadcast_against_mock`; `tools/bench-compare` against `benches/baseline.json`, the baseline rewritten in its own commit. The CLI's spends-per-second bench is the CLI's #86 exit test `[D]`.
5. **The mutation floor (#57's lane).** `cargo mutants` over the two new functions: the mutants that must die are the dropped post, the dropped status write, a `Post` arm swapped, `into_result` answering `unproven` with no post, and the lock released before the commit. Each is caught by 2 or 3.
6. **The gate.** `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --test fixtures`, with `RUSTFLAGS=-Dwarnings`; a CHANGELOG line under `## [Unreleased]` for the two public methods and the `Signed` type.

## 9. The keeper's readings (2026-10-23)

The four questions of section 7 for the keeper, answered by captain-bsv-stack-lean by message and recorded on the stack's rulings page. No new word is needed; the readings stand unless the owner says otherwise.

1. `sending` on the transaction and `unsent` on the request before any post are the machine's `built` (wallet.md:94). The charter has no word for "built, a post intended" and wants none: the intent to post is the host's own state, never a word of the action (T5).
2. One `Signed` type carrying `Post::Template` is acceptable on two conditions: the template's action word is `unknown` (the reference's `unsigned`), and `broadcast` refuses a template with a host error that writes no word; a template is never posted, the move to `built` is the signing. The type's name is the owner's question 5.
3. The abort on the no-BEEF fault after `built` is the row at wallet.md:95: `abandoned`, the outputs `void`, the wallet's own act, no broadcaster asked, refused once the action is announced or later. It runs in its own short write transaction (S9).
4. `attempts` is a count, not a word (wallet.md:39). Incrementing it at the process step before any post is fine; nothing may move on its value.

On the whole: the split meets S9 and B1 and changes no transition, so no charter row moves. Whether the composition keeps today's lock span across the post is the owner's call (question 6).

What this fixes for the implementing lane beyond sections 2 to 6: `broadcast` on a `Post::Template` is a typed refusal that writes nothing (question 9 for the template case; for `NoSend` and `Delayed` the composition still needs the result unchanged, which the owner decides); the no-BEEF abort runs in its own short write transaction.

## 10. The owner's words (2026-10-25)

Recorded on #66 the same day; they close questions 5 to 9 and fix the plan the implementing lane follows.

- **The division is the reference's.** `@bsv/wallet-toolbox` already splits this door: `createAction` with `noSend` builds and signs, `processAction` with `sendWith` posts. The toolbox exposes that division and introduces no new verb: `Wallet::create_action_offering` with `no_send` is the build step a host holds its lock around (the `built` word written at signing), and a public post door on `Wallet`, named for the reference's `processAction` and taking the references or txids to send, is the post (the `announced` word on the broadcaster's acceptance), callable with no lock of the wallet's and concurrently for many. `create_action_offering` without `no_send` stays the one-call composition.
- **No `Signed` type (question 5).** The reference's `reference` (with the txid) is the handle between the two calls, consumed once by the post; a second post of the same reference is the reference's typed refusal. The post rebuilds what it needs for a `nosend` row from storage, as the reference's `processAction` does. If the implementing lane finds the BEEF cannot be rebuilt from storage for such a row, that reason goes on #66 before any by-value type is added; section 2's `Signed` is the fallback, not the plan.
- **The toolbox's own `spend_lock` (question 6).** Released after the build step in the composition too, landed as its own commit after the refactor commits, with #54's bench run before and after it; the refactor commits keep today's span so the bench has one commit to point at (the CLI's #86 exit test).
- **The names (question 7).** The reference's, above. Neither `allocate_and_sign` and `broadcast` nor `build` and `announce` become public names.
- **`sign_action` (question 8).** The reference's `signAction` then `processAction` is already this division; the post door takes a reference from either path. Its own PR after this one.
- **A post that posts nothing (question 9).** The reference's semantics: `noSend` and `acceptDelayedBroadcast` answer the result unchanged; a template is never posted and the post door refuses it with the keeper's typed refusal that writes no word (section 9).

**What the division changes in sections 3, 6 and 8** (found by the cold review of this document at beta 901d72c; recorded on #66 the same day). Sections 2 to 8 describe the by-value design; under the reference's division three of their statements no longer hold, and the implementing lane reads them with this paragraph:

1. *Where `sending` and `unsent` are written.* The build step is `create_action_offering` with `no_send`, whose committed rows are section 3's noSend column: `nosend` on the transaction and on the request. The post door is the process path with sendWith, which writes `unsent` and `sending` before the post and the broadcaster's words after it. So of the words section 6 assigns to the build call, three move to the post call under the division: `unsent` on the request, `sending` on the transaction and `sending` in `sendWithResults` (`process_action.rs:750-814` writes exactly those for a found request not already sent); `unsigned`, the inputs' `spendable` 0 and `spent_by`, the outputs, `txid`, `raw_tx` and `nosend` on both rows stay with the build. No charter word moves: a `nosend` row is signed, so `built` (wallet.md:89), and `sending` before a post is `built` too (section 9, reading 1).
2. *Every output's `spendable`, not the change's alone.* The noSend build writes `spendable = 0` on the user outputs as well as the change: `mark_spendable = !args.is_no_send || args.is_send_with` at `process_action.rs:676`, passed at `:708` into `update_output_with_script_offset` for every non-change output and bound as `spendable = ?` at `:382-405`, after `create_action.rs:866-905` inserted them `spendable` true. The process path with sendWith never flips any of them back (`:676` runs only under `is_new_tx`, opened `:598` and closed `:732`; the sendWith loop at `:750-814` and `update_transaction_status_after_broadcast_internal` at `:918-975` write statuses only). Section 6's sentence "spendable 1 on user outputs and, unless noSend, on change" is therefore wrong on beta: under noSend every output of the row is 0. The issue's exit test, the same rows after the two calls as after the one, fails on every output's `spendable` unless the post door writes them, and that write is on neither #66 nor this document. Sent to #66 beside the BEEF rebuild: the lane reads the reference at its pin (after `createAction({noSend: true})` then `processAction({sendWith})`, which outputs are spendable and where is that written?) and the toolbox does what the reference does; if the reference leaves them unspendable (the `noSendChange` contract: outputs are chained by explicit reference), the exit test is restated as "the same rows as the reference's noSend then sendWith" and a host that wants the one-call rows keeps the composition.
3. *Section 8's proof plan.* Items that name `Post` arms, `into_result` or a CHANGELOG line for the `Signed` type fall away; the rest holds with the post door in place of `broadcast` and `create_action_offering` with `no_send` in place of `allocate_and_sign`. The implementing lane restates the plan in its PR before the proof runs.

The implementing lane runs after #61 (the `create_action.rs` split), since it lands in that code.
