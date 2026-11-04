# The door mutation oracle, before and after its widening (2026-11-01)

Base `83a5b049`. Every value below is written by `record.py` from the lane's run outputs; the machine-readable record, with the commands, times, toolchain, every verdict and the failing targets per mutant, is [mutants-oracle-2026-11-01.json](mutants-oracle-2026-11-01.json). The outputs are on the lane volume under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-535-door-oracle-widen/` (`runs/narrow`, `runs/wide`, `baseline/`, `reach/`, `reach2/`, `catalog-doors.json`, `scripts/`). The word: the owner's of 2026-11-01 14:1xZ (23.5 (c), the 80 percent gate on the doors that judge under the node's word) and 14:4xZ (the oracle widened before a floor is written; the floors from the widened figure), #535 and #534's door rows.

Method: cargo-mutants 27.1.0, rustc 1.95.0 (59807616e 2026-04-14), on Johns-Mac-Studio.local; one mutant per cargo-mutants call, in place in one of five scratch trees (git archive of the base), each worker its own CARGO_TARGET_DIR ($HOME/bsv/targets/p5-535-door-oracle-widen/wK); the mutant is round-robin shard I/N of the file's own list (`--no-config --file FILE`), whose names equal the Mutants workflow catalogue's; scored by tools/mutants-score collect() and score_table() with the skipped baseline. Test and build timeouts 900 s each (the host's load ran from 55 to 130 on ten cores; at 420 s nine early builds timed out before a test ran, six of them a worker's cold build under cargo-mutants' `--cap-lints` flags; those runs were discarded as host-load timeouts, not verdicts, their outputs kept under `warmup-timeouts/` and `load-timeouts/`; one run under an earlier `--tests` shape of the oracle, superseded by the reach set, is kept under `superseded-tests-oracle/`). Score = caught / (caught + missed + timeout), unviable excluded.

Baselines on the base, green before any mutant:

- narrow: `HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 ALL_PROXY=http://127.0.0.1:9 NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 cargo test --lib`: 1727 passed, 0 failed targets, compiled the tree: yes.
- wide: `HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 ALL_PROXY=http://127.0.0.1:9 NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 CARGO_ENCODED_RUSTFLAGS=--cap-lints=warn cargo test --features sqlite,remote,bench-support --lib --test fixtures --test caller_complete_funding_tests --test immediate_broadcast_hint_tests --test known_ancestor_tests --test test_vectors --no-fail-fast`: 1998 passed, 0 failed targets, compiled the tree: yes.

## Where the door witnesses live (the reach table)

A scratch copy of the base (never committed) logs each door function on entry; every test target ran once with features `sqlite,remote,bench-support` (`reach/`). The cells count the door's functions the target entered; a target absent from a row entered none, so it cannot kill a mutant of that file (no integration target reads a door file's text). The kills are the widened run's: caught mutants whose run failed that target (`--no-fail-fast`), and in parentheses those no other target caught. The fixtures' JSON scenes under `tests/fixtures/data/storage/` are the `fixtures` target's.

| File | `lib` | `fixtures` | `caller_complete_funding_tests` | `immediate_broadcast_hint_tests` | `known_ancestor_tests` | `test_vectors` | `internalize_custom_instructions_tests` |
|---|---:|---:|---:|---:|---:|---:|---:|
| `src/storage/sqlx/create_action/must_spend.rs` | 1 fn, 0 kills (0) | 5 fn, 18 kills (18) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | . |
| `src/storage/sqlx/create_action/own_word.rs` | 3 fn, 5 kills (5) | 3 fn, 0 kills (0) | 3 fn, 0 kills (0) | 3 fn, 0 kills (0) | 3 fn, 0 kills (0) | . | . |
| `src/storage/sqlx/create_action/review_beef.rs` | 7 fn, 13 kills (13) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | 1 fn, 0 kills (0) | . | . |
| `src/storage/sqlx/beef_verification/own_word.rs` | 11 fn | 10 fn | 6 fn | 10 fn | 6 fn | . | 6 fn |

`beef_verification/own_word.rs` (the verifier the create door's own word calls, `verify_own`) is outside the Mutants workflow's set (`.cargo/mutants.toml`'s globs), so it has no catalogue entry, no floor and no score; it is named here as a finding, not measured. Since #572 it is in `.cargo/mutants-doors.toml`, with `internalize_custom_instructions_tests` added to that oracle's targets, and measured under it (the last section).

**Finding: the other create-door files.** The second probe (`reach2/`, every function of `create_action/*.rs`, `process_action.rs` and `internalize_action.rs`, integration targets only) shows each of them entered by integration targets as well as the lib, so their `--lib` scores in the Mutants workflow count only the lib's witnesses. Under the owner's 14:1xZ word they stay on the ratchet with the `--lib` oracle; widening them is not this lane's. Targets entering each file:

- `allocation.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `beef.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `beef_graph.rs`: `arc_broadcast_body_tests`, `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `monitor_tests`
- `beef_storage.rs`: `arc_broadcast_body_tests`, `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `monitor_tests`
- `beef_walk.rs`: `arc_broadcast_body_tests`, `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `monitor_tests`
- `broadcast.rs`: `arc_broadcast_body_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `monitor_tests`
- `change.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `fees.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `inputs.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `internalize_action.rs`: `fixtures`, `immediate_broadcast_hint_tests`, `internalize_custom_instructions_tests`
- `mod.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `outputs.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`
- `process_action.rs`: `caller_complete_funding_tests`, `concurrent_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `monitor_tests`, `poisoned_chain_tests`, `retire_rejected_tests`
- `signing.rs`: `caller_complete_funding_tests`, `fixtures`
- `tracker.rs`: `caller_complete_funding_tests`, `fixtures`, `immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`

## The figure before and after

Before: the Mutants workflow's oracle before this lane: `cargo test --lib`, default features. After: the door oracle (`.cargo/mutants-doors.toml`): `cargo test --lib --test fixtures --test caller_complete_funding_tests --test immediate_broadcast_hint_tests --test known_ancestor_tests --test test_vectors`, features `sqlite,remote,bench-support`.

| File | Oracle | Caught | Missed | Timeout | Unviable | Score |
|---|---|---:|---:|---:|---:|---:|
| `src/storage/sqlx/create_action/must_spend.rs` | before | 0 | 19 | 0 | 3 | 0.00% |
| `src/storage/sqlx/create_action/must_spend.rs` | after | 18 | 1 | 0 | 3 | 94.74% |
| `src/storage/sqlx/create_action/must_spend.rs` | after #574 | 19 | 0 | 0 | 3 | 100.00% |
| `src/storage/sqlx/create_action/own_word.rs` | before | 5 | 2 | 0 | 2 | 71.43% |
| `src/storage/sqlx/create_action/own_word.rs` | after | 5 | 2 | 0 | 2 | 71.43% |
| `src/storage/sqlx/create_action/own_word.rs` | after #573 | 6 | 1 | 0 | 2 | 85.71% |
| `src/storage/sqlx/create_action/review_beef.rs` | before | 13 | 0 | 0 | 1 | 100.00% |
| `src/storage/sqlx/create_action/review_beef.rs` | after | 13 | 0 | 0 | 1 | 100.00% |
| `src/storage/sqlx/beef_verification/own_word.rs` | doors, at #572 | 17 | 2 | 0 | 4 | 89.47% |
| `src/storage/sqlx/beef_verification/own_word.rs` | doors, after #572 | 19 | 0 | 0 | 4 | 100.00% |
| `src/services/providers/bhs.rs` | `--lib`, before #602 (the complete record) | 8 | 4 | 0 | 5 | 66.67% |
| `src/services/providers/bhs.rs` | doors, after #602 | 12 | 0 | 0 | 5 | 100.00% |
| `src/storage/sqlx/create_action/mod.rs` | `--lib`, before #604 (the complete record) | 23 | 14 | 0 | 2 | 62.16% |
| `src/storage/sqlx/create_action/mod.rs` | doors, after #604 | 35 | 2 | 0 | 2 | 94.59% |

- before: 45 runs, 2026-11-01T17:45:19.725138Z to 2026-11-01T18:33:11.434179Z, per mutant 72 to 369 s (mean 270 s) on the loaded host; collect problems: none; scoring problems: ['src/storage/sqlx/create_action/must_spend.rs: BELOW FLOOR'].
- after: 45 runs, 2026-11-01T15:58:36.455874Z to 2026-11-01T17:44:53.000086Z, per mutant 114 to 927 s (mean 579 s) on the loaded host; collect problems: none; scoring problems: none.

`tools/mutants-score` over the widened outcomes (the floors as this record writes them):

| File | Caught | Missed | Timeout | Unviable | Tested/expected | Score | Floor | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| src/storage/sqlx/create_action/must_spend.rs | 18 | 1 | 0 | 3 | 22/22 | 94.74% | 80 | PASS |
| src/storage/sqlx/create_action/own_word.rs | 5 | 2 | 0 | 2 | 9/9 | 71.43% | 71 | PASS |
| src/storage/sqlx/create_action/review_beef.rs | 13 | 0 | 0 | 1 | 14/14 | 100.00% | 80 | PASS |

## The survivors under the widened oracle (the triage's input)

No fix here: each survivor is the next lane's, per file, killed by a test written from the rule the door judges or declared an equivalent mutant with that rule as its reason (the owner's 14:4xZ word). The rules, read with `git show` at bsv-stack-lean `f9197467` (`docs/charters/wallet.md`, last changed at `7eb03a69`) and the sibling bsv-script-lean at `v1.1.1`:

**`src/storage/sqlx/create_action/must_spend.rs`** (1 survivors; before the widening 19)

The rule: a coin is offered, and so spent, only under the spend guard: no action of ours holds it, and its creator's word allows it under the host's rule for our own change or the action's received rule (wallet charter section 2, `wallet.md:32`, rows `:93-95`); a coin the caller names is the door's to refuse by name when it is not ours to name (row `:96`); the must-spend list is the offer's own (#353, bsv-wallet-cli #313): a wallet-managed change coin inside the offer, the user's and unspent, under the pick's filters, else refused by name. The node's word behind it: the coin exists unspent on the active chain (`wallet.md:143`).

- `src/storage/sqlx/create_action/must_spend.rs:131:31: replace || with && in NamedCoin::read` (MissedMutant here; killed by #574, the section below)

**`src/storage/sqlx/create_action/own_word.rs`** (2 survivors; before the widening 2)

The rule: the wallet's own newly signed transaction is judged at its signing by the evaluator's verdict, the chain tier's word (`wallet.md:13`; the host's act is build on signing), before any writer (#450); the verdict is the node's script rules, the sibling's S6 cited through the tracker and never restated (`wallet.md:5`; bsv-script-lean `v1.1.1` `CLAUDE.md` rules 1, 2 and 19, its corpus by family under `corpus/`), at the tip read before the writer (the era words of #422 in this repository's `CLAUDE.md`).

- `src/storage/sqlx/create_action/own_word.rs:46:47: replace match guard text == NO_HEADER_SERVICE with true in own_word_tip` (MissedMutant here; killed by #573, the section below)
- `src/storage/sqlx/create_action/own_word.rs:92:44: replace || with && in judge_own_tx` (MissedMutant here; declared equivalent by #573, the section below)

**`src/storage/sqlx/create_action/review_beef.rs`** (0 survivors; before the widening 0)

The rule: a double-spend word from a broadcaster is a hint that moves no coin (`wallet.md:38`); the one evidence for a competitor is its checked proof against our headers (`wallet.md:127`, rows `:111-115`), so the review refusal carries the stored evidence for each competing transaction (#389) for the caller to check, never a word of its own.

- none

Rechecked after the lane's rebase onto beta `6afdfe47` (new lib and fixtures tests landed between the bases; the door files unchanged), the widened survivors under the same command: `src/storage/sqlx/create_action/must_spend.rs:131:31: replace || with && in NamedCoin::read` MissedMutant; `src/storage/sqlx/create_action/own_word.rs:46:47: replace match guard text == NO_HEADER_SERVICE with true in own_word_tip` MissedMutant; `src/storage/sqlx/create_action/own_word.rs:92:44: replace || with && in judge_own_tx` MissedMutant.

Killed by the widening (survivors before, caught after), with the targets that caught them:

- `src/storage/sqlx/create_action/must_spend.rs:21:5: replace check_listed -> Result<()> with Ok(())`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:22:38: delete ! in check_listed`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:58:12: delete ! in allocate_named_change_input`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:73:35: replace == with != in allocate_named_change_input`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:95:9: replace NamedCoin::sql -> String with String::new()`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:95:9: replace NamedCoin::sql -> String with "xyzzy".into()`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:121:71: replace == with != in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:127:12: delete ! in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:131:58: replace && with || in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:131:61: delete ! in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:137:20: replace == with != in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:137:33: replace && with || in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:137:43: replace != with == in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:137:58: replace && with || in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:137:61: delete ! in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:147:19: replace != with == in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:147:34: replace && with || in NamedCoin::read`: `fixtures`
- `src/storage/sqlx/create_action/must_spend.rs:147:44: replace != with == in NamedCoin::read`: `fixtures`

## The survivor at 131:31, killed (#574)

The lane p5-574-must-spend-survivor, on beta `fa8b280c`, the tree at `6cfb715f`; the record's json under `issue_574`, the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-574-must-spend-survivor/run`.

The mutant: `spent_by.is_some() || (self.require_spendable && !spendable)` read as `spent_by.is_some() && (...)`, so a coin another action holds passes the read whenever the action does not ask the `spendable` flag (`acceptDelayedBroadcast`, `require_spendable` false) or the flag reads 1. Not equivalent, but quiet alone: under the write lock the allocation's `UPDATE ... WHERE spent_by IS NULL` marks no row and refuses with the same text, `is spent or reserved`. It shows only where a held coin also fails a later rule of the read, since the mutant moves the refusal from the held rule to that one. The rule (the spend guard, `wallet.md:32`, rows `:93-96`): no action of ours holds it, judged by `spent_by` alone as the pick's own `o.spent_by IS NULL` judges it; a coin held is refused as held, whatever else is said of it.

The witness, `fixtures::storage::must_spend::refusals::a_held_coin_is_refused_as_held_ahead_of_the_received_rule`: an earlier action under `underReceivedRule` holds the unproven receipt; a delayed action without the rule names it. At the tip: `mustSpend[0]: output <txid>.0 is spent or reserved`. Under the mutant (planted by hand, then by cargo-mutants): `... is a received coin whose transaction is unproven, not completed; the action does not name underReceivedRule`. The only failure under the mutant, in `fixtures`.

The file under the door oracle (`.cargo/mutants-doors.toml`'s targets and features, given with `--no-config`: the config's `examine_globs` override `--file`; `--in-place`, serial, which refuses `--jobs`), timeouts 900 s, with the unmutated baseline (26 s build, 18 s test): `22 mutants tested in 15m: 19 caught, 3 unviable`. Score 100.00% (19/19); the floor stays 80.

| Mutant | Verdict |
|---|---|
| `src/storage/sqlx/create_action/must_spend.rs:15:5: replace refusal -> Error with Default::default()` | Unviable |
| `src/storage/sqlx/create_action/must_spend.rs:21:5: replace check_listed -> Result<()> with Ok(())` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:22:38: delete ! in check_listed` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:46:5: replace allocate_named_change_input -> Result<AllocatedChangeInput> with Ok(Default::default())` | Unviable |
| `src/storage/sqlx/create_action/must_spend.rs:58:12: delete ! in allocate_named_change_input` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:73:35: replace == with != in allocate_named_change_input` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:95:9: replace NamedCoin::sql -> String with String::new()` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:95:9: replace NamedCoin::sql -> String with "xyzzy".into()` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:115:9: replace NamedCoin::read -> Result<AllocatedChangeInput> with Ok(Default::default())` | Unviable |
| `src/storage/sqlx/create_action/must_spend.rs:121:71: replace == with != in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:127:12: delete ! in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:131:31: replace || with && in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:131:58: replace && with || in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:131:61: delete ! in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:137:58: replace && with || in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:137:33: replace && with || in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:137:20: replace == with != in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:137:43: replace != with == in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:137:61: delete ! in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:147:34: replace && with || in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:147:19: replace != with == in NamedCoin::read` | CaughtMutant |
| `src/storage/sqlx/create_action/must_spend.rs:147:44: replace != with == in NamedCoin::read` | CaughtMutant |

## The survivors at 46:47 and 92:44, killed and declared equivalent (#573)

The lane p5-573-own-word-survivors, on beta `ed1b904e`, the tree at `355120e1` (the run's; the witness's helper since shared with the route's `sign` as `sign_on`, the ratchet's duplicated blocks, its claim unchanged, and the plant repeated by hand on beta `4d22d3c7` at `09fb4120`: red, `Ok("sending")`); the record's json under `issue_573`, the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-573-own-word-survivors/run`.

**46:47, killed.** The mutant: the guard `text == NO_HEADER_SERVICE` read as `true`, so every `ServiceError` from the services' `get_chain_tracker` gives the own word the latest era, where the rule gives it only for the not-configured answer. The rule (`own_word_tip`'s, #506 and #464): services with no header service (`Services` built with no `chaintracks_url`) judge as a tracker with no tip does, at the latest era; any other `get_chain_tracker` error holds with `header_fault`'s text, a fault being could-not-look and never an admit (the rule `reachable_tracker` keeps for the caller's roots, #515). The witness, `storage::sqlx::process_action::script_word_route_tests::no_header_source::own_word_holds_for_a_header_service_fault_other_than_not_configured` (the lib's door tests, `src/storage/sqlx/tests/no_header_source.rs`): the signAction route, a new signed transaction whose every judged input is valid, no tracker on the storage, the services faulting with another text (`UnreachableHeaders`). At the tip, posted or delayed: `Service error: ChainTracker verification failed: Service error: the header service is unreachable: connection refused`, the services asked once, the action still `unsigned` with no request; the control, `Services::mainnet()` with no `chaintracks_url`, is sent. Under the mutant (planted by hand, then by cargo-mutants): `Ok("sending")`. The only failure under the mutant, in the lib.

**92:44, equivalent.** The mutant: `sources.contains_key(&txid) || stored.contains(&txid)` read as `&&`, the skip of an input whose source transaction an earlier input already placed. Each txid is placed in exactly one of the two: `sources` when `beef_source(beef, txid)` answers, `stored` when it does not, and `beef_source` is a function of the call's parsed input BEEF and the txid alone. No txid is ever in both, so the `&&` is never true and the mutant never skips: a later input of the same parent runs the same placement again. A BEEF source is re-inserted with an equal `SourceTx`; a stored one is pushed twice and `get_tx_with_proof` reads it twice on the same connection (two `SELECT`s, no write in this function or under it). On the create route the call runs inside `write_tx`, so nothing lands between the two reads: the same row, the same `SourceTx`, the same skip for `Ok(None)` and `FailedAncestor`, and any other error already returned at the earlier read. On the signAction route (`process_action.rs:612`, a reader-pool connection in autocommit) another connection's commit can land between them (a proof arriving in `proven_txs`, the parent failed), so under the mutant the second read can carry that parent's newer row, the row a call begun a moment later would have read; the equivalence holds for every call that runs alone, and the cold read of PR #772 named this caveat. `verify_own` receives the same map, so the verdict is the same for every input; the mutant costs only the repeated read. The rule it serves (the own word judges each source transaction once; an input with neither bytes nor a live word is skipped, `verifyUnlockScripts.ts:316-321`) holds under it. cargo-mutants counts an equivalent as missed, and the figure below counts it so.

The file under the door oracle (`.cargo/mutants-doors.toml`'s targets and features, given with `--no-config`; `--in-place` in a `git archive` tree with the worktree's `Cargo.lock`, serial), timeouts 900 s, with the unmutated baseline (73 s build, 25 s test): `9 mutants tested in 10m: 1 missed, 6 caught, 2 unviable`. Score 85.71% (6/7); the floor raised from 71 to 80.

| Mutant | Verdict |
|---|---|
| `src/storage/sqlx/create_action/own_word.rs:42:5: replace own_word_tip -> Result<Option<OwnTip>> with Ok(None)` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:42:5: replace own_word_tip -> Result<Option<OwnTip>> with Ok(Some(Default::default()))` | Unviable |
| `src/storage/sqlx/create_action/own_word.rs:46:47: replace match guard text == NO_HEADER_SERVICE with true in own_word_tip` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:46:47: replace match guard text == NO_HEADER_SERVICE with false in own_word_tip` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:46:52: replace == with != in own_word_tip` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:59:5: replace beef_source -> Option<SourceTx> with None` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:59:5: replace beef_source -> Option<SourceTx> with Some(Default::default())` | Unviable |
| `src/storage/sqlx/create_action/own_word.rs:83:5: replace judge_own_tx -> Result<()> with Ok(())` | CaughtMutant |
| `src/storage/sqlx/create_action/own_word.rs:92:44: replace || with && in judge_own_tx` | MissedMutant |

## The verifier `beef_verification/own_word.rs` in the catalogue, its survivors at 48:27 killed (#572)

The lane p5-572-verify-own-catalogue, on beta `68275a33`; the record's json under `issue_572`, the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-572-verify-own-catalogue/` (`run`, `run/kills`, `tip`, `catalog-doors.json`).

**The catalogue entry.** The file holds `verify_own`, the verifier the create door's own word calls (`create_action/own_word.rs`'s `judge_own_tx` and the signAction route), and `InputJudgements` and `verify_selected`, the per-transaction dispatcher and the host batch the live doors share. It judges under the node's word and its witnesses live in the lib and five integration targets (the reach table's row), so it joins `.cargo/mutants-doors.toml`'s globs, not the lib set; `internalize_custom_instructions_tests`, which enters six of its functions and was in no oracle, joins that config's targets (a target only adds witnesses to the other three door files). Its 23 mutants enter the Mutants workflow's doors shards through the config's globs.

**48:27, killed (both mutants).** The mutants: `output.at -= offset` in `read_tx` read as `+=` and as `/=`. The rule (`read_tx`'s own line: the body read by bsv-rs's streaming reader, its offsets rebased onto `raw`): `verify_own` wraps the signed transaction in a BEEF to stream it, so every offset bsv-rs gives (`InputRef::at`, `OutputRef::at`, stream offsets) is moved back by the transaction's place in that stream; the inputs' offsets name a refusal's place, and the whole body, outputs included, reaches a host `SpendVerifier` as `SpendRequest::body`. No interpreter path reads an output's `at` (the sighash cache reads `satoshis` and the script range), which is why no target caught them. The witness, `storage::sqlx::beef_verification::own_word::tests::host::host_body_offsets_lie_within_the_signed_transaction` (the lib, `own_word_host_tests.rs`): a signed transaction of three inputs and two outputs judged with a recording host; every body the host receives has each input's previous txid at its `at` and each output's eight value bytes at its `at`, within the signed transaction's bytes. At the tip: green (offsets 129 and 139 within 153 bytes). Under each mutant (planted by hand, then by cargo-mutants, `run/kills`: `2 mutants tested in 3m: 2 caught`): red at the output's value, the only failure in the oracle.

The file under the door oracle (`.cargo/mutants-doors.toml`'s targets, `internalize_custom_instructions_tests` among them, and features, given with `--no-config`; `--in-place` in a `git archive` tree with the worktree's `Cargo.lock`, serial, sharded round-robin), timeouts 900 s. At the entry (`d9295bca`, six shards, baseline 4 s build + 19 s test on a warm tree): 23 mutants, 17 caught, 2 missed, 4 unviable, 89.47% (17/19). At the tip (`afb323c8`'s test, four shards, baseline 37 s build + 18 s test): 19 caught, 0 missed, 4 unviable, 100.00% (19/19); `tools/mutants-score` over the four shards: `19 | 0 | 0 | 4 | 23/23 | 100.00% | 100 | PASS`. No timeout, so no Timeout to judge against the baseline. The floor written at 100, the figure rounded down.

| Mutant | At the entry | At the tip |
|---|---|---|
| `src/storage/sqlx/beef_verification/own_word.rs:33:5: replace read_tx -> Result<(Hash32, TxBody)> with Ok((Default::default(), Default::default()))` | Unviable | Unviable |
| `src/storage/sqlx/beef_verification/own_word.rs:45:26: replace -= with += in read_tx` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:45:26: replace -= with /= in read_tx` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:48:27: replace -= with += in read_tx` | MissedMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:48:27: replace -= with /= in read_tx` | MissedMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(None)` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((0, Vec::leak(Vec::new()))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((0, Vec::leak(vec![0]))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((0, Vec::leak(vec![1]))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((1, Vec::leak(Vec::new()))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((1, Vec::leak(vec![0]))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:62:5: replace source_output -> Result<Option<(u64, &[u8])>> with Ok(Some((1, Vec::leak(vec![1]))))` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:72:18: replace == with != in source_output` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:95:5: replace verify_own -> Result<()> with Ok(())` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:174:9: replace InputJudgements<'a>::with_transaction_work -> Self with Default::default()` | Unviable | Unviable |
| `src/storage/sqlx/beef_verification/own_word.rs:185:9: replace InputJudgements<'a>::judge -> Result<()> with Ok(())` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:232:9: replace InputJudgements<'a>::finish -> Result<()> with Ok(())` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:236:9: replace InputJudgements<'a>::into_selected -> Vec<(SpendRequest, SpendVerificationContext)> with vec![(Default::default(), Default::default())]` | Unviable | Unviable |
| `src/storage/sqlx/beef_verification/own_word.rs:236:9: replace InputJudgements<'a>::into_selected -> Vec<(SpendRequest, SpendVerificationContext)> with vec![]` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:241:5: replace host_refusal -> Error with Default::default()` | Unviable | Unviable |
| `src/storage/sqlx/beef_verification/own_word.rs:255:5: replace verify_selected -> Result<()> with Ok(())` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:261:23: replace != with == in verify_selected` | CaughtMutant | CaughtMutant |
| `src/storage/sqlx/beef_verification/own_word.rs:272:12: delete ! in verify_selected` | CaughtMutant | CaughtMutant |

## The header service `services/providers/bhs.rs` in the catalogue, its four survivors killed (#602)

The lane p5-602-bhs-survivors, on beta `88a93492`, the tree at `d17e8ca9` (the two tests) with the catalogue of `151d2d77`; the json under `issue_602` (the complete record's, the widened record's and the door oracle record's json carry the same key), the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-602-bhs-survivors` (`run`, `run.log`, `catalog-lib.json`, `catalog-doors.json`). The oracle is the one whose targets enter the file: the file joins `.cargo/mutants-doors.toml` (the header service's `is_valid_root_for_height`, the chain's evidence the tracker's verdict reads, docs/ARCHITECTURE.md section 4 step 2; `fixtures` the one integration target that enters it) and leaves `.cargo/mutants.toml`'s lib set. `bhs.rs`'s rules are unchanged; each kill is a lib test in the file's own test module from the rule, red under the mutant planted by hand and green at the tip, then caught again by cargo-mutants.

**123:12 (`delete !` in `current_height`), killed.** The rule (the line's guard): a non-success status is the service's error, never its body read as a height. The witness, `services::providers::bhs::tests::current_height_is_a_successful_answer_and_a_failed_status_is_an_error`: a 200 with the body `900002` answers 900002, and a 503 with the same body is `BHS current_height failed: HTTP 503`. Under the mutant the 200 is the error, red at the success's unwrap.

**165:9 (the function replaced by `Ok(true)` and by `Ok(false)`) and 166:31 (`==` to `!=`), killed.** The rule: a root is valid for a height when it is the merkle root of the service's header at that height, and invalid when it is any other. The witness, `services::providers::bhs::tests::is_valid_root_for_height_is_the_root_of_the_header_at_the_height`: against a header at 900002 whose root is `c` repeated, that root answers true and the root `e` repeated answers false. `Ok(false)` and `!=` fail the matching root, `Ok(true)` the mismatching one.

The file under the door oracle (given with `--no-config`, the doors config's flags by hand, in place, serial, timeouts 900 s): `17 mutants tested in 11m: 12 caught, 5 unviable`, 100.00% (12/12), no timeout to judge against the baseline (0 s build + 18 s test). Before (the complete record's `--lib` and the widened re-measure): 66.67% (8/12). The floor stays 80. The catalogues listed on the tree: the lib set 2218 (2235 before), the doors 85 (68 before, `bhs.rs` 17).

## The create door `create_action/mod.rs` in the catalogue, five survivors killed and two equivalent (#604)

The lane p5-604-create-door-survivors, on beta `b1879b8d` with #602's lane commits (base `b044407f`), the tree at `2fb239d5` (the five tests, `create_action/tests/door_steps.rs`) with the catalogue of `7313d361`; the json under `issue_604` (the complete record's, the widened record's and the door oracle record's json carry the same key), the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-604-create-door-survivors` (`survivors`, `file/s0` to `file/s3`, `verdicts.json`). The oracle is the door oracle (`.cargo/mutants-doors.toml`, the file in its examine_globs since #604): the lib and the six targets, features `sqlite,remote,bench-support`; the five targets that enter the file (caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, test_vectors) are all among them. The witnesses were reworked after this measurement (867fcf42, the ratchet's duplicated-window rule) and the lane rebased, so neither `2fb239d5` nor `7313d361` is on the PR's branch; PR #799's standard read re-ran the door oracle at the PR's tip rebased onto beta `651a6c46`, in place, serial, thirteen shards: the same 35 caught, 2 missed (311:9, 713:27), 2 unviable (152:5, 369:9).

**339:9 (`is_delayed` replaced by `false` and by `true`), killed.** The rule: the delayed route may take a change coin not yet marked spendable, the immediate route only a coin marked spendable (the allocation's `require_spendable` is `!is_delayed`). The witnesses, `the_delayed_route_may_spend_a_change_coin_not_marked_spendable` (red under `false`: the delayed action is refused `InsufficientFunds`) and `the_immediate_route_spends_only_a_change_coin_marked_spendable` (red under `true`: the immediate action spends the unmarked coin).

**399:13 (`delete !` in `Door::step`), killed.** The rule: the write-locked step counts the change coins its route may spend, as the pre-check does, so an unmarked coin never stands in for a change output the basket wants. The witness, `the_step_counts_only_the_change_coins_its_route_may_spend`: with the basket wanting two coins, an immediate action makes as many change outputs beside an unmarked coin as without it; under the mutant it makes fewer.

**203:47 (`||` to `&&` in `create_action_at_door_internal`), killed.** The rule: with no caller tracker the step's assembly reads every stored proof against the storage's own tracker's answers (`reads_proofs`), so a checked stored bump whose root that tracker refutes never enters the BEEF (ruling 7c). The witness, `without_a_caller_tracker_the_step_reads_stored_proofs_against_the_storages_own`: the coin's transaction rides without its refuted bump, above its proven parent; under the mutant the step merges the refuted bump.

**738:21 (the field `is_new_tx` deleted), killed.** The rule: `SignAndProcess`'s contract, the door sets `is_new_tx` itself (with the reference, the txid and the raw bytes) whatever the host's flags say. The witness, `the_door_processes_its_signed_action_as_a_new_transaction`: a host passing `is_new_tx: false` still has its signed bytes stored by processAction's new-transaction writes; under the mutant they are not.

**311:9 (`Again`'s Display replaced by an empty answer), equivalent.** The text is read only by the `reason` field of the `create_action_precheck_again` log; every refusal built from an `Again` is the match's own error (`ServiceError` naming the height, `StoreBusy { MovedUnderUs }`), never the Display. No caller, response or stored row reads it.

**713:27 (the guard `!created.inputs.is_empty()` replaced by `true`), equivalent.** The step reaches the guard only after `plan_change` returned a plan, and every plan carries an input: the fee is at least 2 sats (a transaction is at least 10 bytes at 101 sat/kB, rounded up), so an action with no input is short and `generate_change` refuses `InsufficientFunds`; a caller-complete plan has a caller input. The two forms agree on every reachable call.

The file under the door oracle (given with `--no-config`, the doors config's flags by hand, in place, serial, four shards, timeouts 900 s): `39 mutants tested` (10, 10, 10, 9) in 33 minutes, 35 caught, 2 missed (the two equivalents, counted as missed as cargo-mutants counts them), 0 timeout, 2 unviable: 94.59% (35/37), no timeout to judge against the baseline (30 s build + 19 s test). Before: 62.16% (23/37) under `--lib`, 81.08% (30/37) under the widened re-measure. The floor stays 80. The catalogues listed on the tree: the lib set 2179 (2218 before), the doors 124 (85 before).
