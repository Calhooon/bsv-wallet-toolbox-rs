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

`beef_verification/own_word.rs` (the verifier the create door's own word calls, `verify_own`) is outside the Mutants workflow's set (`.cargo/mutants.toml`'s globs), so it has no catalogue entry, no floor and no score; it is named here as a finding, not measured.

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
| `src/storage/sqlx/create_action/own_word.rs` | before | 5 | 2 | 0 | 2 | 71.43% |
| `src/storage/sqlx/create_action/own_word.rs` | after | 5 | 2 | 0 | 2 | 71.43% |
| `src/storage/sqlx/create_action/review_beef.rs` | before | 13 | 0 | 0 | 1 | 100.00% |
| `src/storage/sqlx/create_action/review_beef.rs` | after | 13 | 0 | 0 | 1 | 100.00% |

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

- `src/storage/sqlx/create_action/must_spend.rs:131:31: replace || with && in NamedCoin::read` (MissedMutant)

**`src/storage/sqlx/create_action/own_word.rs`** (2 survivors; before the widening 2)

The rule: the wallet's own newly signed transaction is judged at its signing by the evaluator's verdict, the chain tier's word (`wallet.md:13`; the host's act is build on signing), before any writer (#450); the verdict is the node's script rules, the sibling's S6 cited through the tracker and never restated (`wallet.md:5`; bsv-script-lean `v1.1.1` `CLAUDE.md` rules 1, 2 and 19, its corpus by family under `corpus/`), at the tip read before the writer (the era words of #422 in this repository's `CLAUDE.md`).

- `src/storage/sqlx/create_action/own_word.rs:46:47: replace match guard text == NO_HEADER_SERVICE with true in own_word_tip` (MissedMutant)
- `src/storage/sqlx/create_action/own_word.rs:92:44: replace || with && in judge_own_tx` (MissedMutant)

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
