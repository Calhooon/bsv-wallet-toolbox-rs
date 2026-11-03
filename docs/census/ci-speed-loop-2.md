Loop two of the CI speed line (#632, #634, #644), split from the record [ci-speed-2026-11-02.md](ci-speed-2026-11-02.md) (#670).

## Loop two, cut (1): a docs-only change exits every required job green (#632)

Base `c3b0b32e` (origin/beta, PR #639 landed). The keeper's round-two reading of 2026-11-03 02:1xZ: #620, #622, #628, #629 and #640 tonight were HANDOFF and fold PRs, each a full run for a change to Markdown, and under the merge queue a PR costs two runs. The loop's number: two, and this is its cut (1).

### The step

`tools/docs-only` (with `tools/docs-only-test`, run as a `checks` gate) is the step after the checkout in each of the five jobs, `id: scope`; every step after it carries `steps.scope.outputs.docs_only != 'true'` in its `if:`, joined to the step's existing condition (`!cancelled()` on the lint and checks gates, the affected-tests outputs on the run steps). When the step says `true`, every later step is skipped and the job concludes `success`: a job whose steps all succeeded or were skipped is a success, and the job's check-run is the required context, so all five contexts report and the protection holds. Skipped steps, never a skipped job: a job a job-level `if:` skips concludes `skipped`, and the merge queue holds a group whose required job reports no success (cut (3)'s triggers). No early `exit 0`: a step cannot end its job, so the steps after it would still run.

| event | the change read | short-circuits |
|---|---|---|
| `pull_request` | the merge commit against `HEAD^1`, its base (lint and checks now check out two deep, as the Test Suite jobs already did) | when every path is a doc |
| `merge_group` | the group's head against `merge_group.base_sha`, its parent, fetched when the checkout lacks it; a group without it runs the full set (`HEAD^1` is the group's last commit alone when its PR has several) | when every path is a doc: the queue runs nothing a doc changes |
| `push`, a tag's call (`workflow_call` reads the caller's `push`) | none | never |

An empty diff, or one `git diff` cannot read, runs the full set with a warning. Renames are read as a delete and an add (`--no-renames`), so a file moved out of `src/` into `docs/` runs everything.

### The docs set, exactly

| path | the workflow reads it as |
|---|---|
| under `docs/` (`docs/census/*.json` and `docs/lanes/*.md` among them) | a doc |
| any `*.md` (`HANDOFF.md`, `CHANGELOG.md`, `README.md`, a `CLAUDE.md` under `src/`, `benches/README.md`) | a doc |
| `docs/ARCHITECTURE.md`, `docs/architecture/` | not a doc: `tools/arch-doc-tests.sh` reads its rows (below) |
| `docs/NORTH-STAR.md` | not a doc: `tools/scoreboard-write --check` reads its table against `benches/baseline.json` and `tools/ratchet.json` |
| a `*.md` under `tests/`, `fuzz/`, `test_vectors/` | not a doc: tests walk those directories. `tests/storage_wire.rs`'s `every_rpc_method_has_a_witness` parses every file in `tests/fixtures/data/storage-wire/` as JSON, and `src/storage/sqlx/a8_beef_bytes_tests.rs` reads every file under `fuzz/corpus/beef_door/` as a corpus input. Planted [X]: a `README.md` in `tests/fixtures/data/storage-wire/` fails the test (`panicked at tests/storage_wire.rs:247:22`, `test result: FAILED. 0 passed; 1 failed`), removed it passes (`1 passed`) |
| a `*.md` under `tools/` or `.github/` | not a doc: the gates and the workflow themselves, whose directories `tools/affected-tests` already sends to everything |
| anything else | not a doc |

No cargo test reads a `*.md` by name (`include_str!`, `read_to_string` and `CARGO_MANIFEST_DIR` reads across `src/` and `tests/` name none), no gate but those two reads a document under `docs/`, and `cargo semver-checks` and `cargo doc` read the crate's sources alone (no `#![doc = include_str!(...)]`). One finding outside this lane's bounds: `tools/affected-tests` classes every `*.md` as `none` before it reads `tests/`, so a `*.md` in a fixture data directory runs no cargo test on a PR's own run (the queue's run, every test, still catches it); filed as #644 and closed in loop two's hygiene row (below).

### The architecture doc gate: excluded from the docs set

The gate lists every invariants row's test with `cargo test --lib|--test <t> <name> -- --list`, from the Test Suite's two builds. On a docs-only run it would need those builds: the job's setup, both builds and the listing, about 3 minutes warm on the runner (#626's run: build steps 1:07, the listing 0:46, about a minute of setup) and 3:52 plus setup cold (#621's: 2:47 and 1:05), which is cold for every queue group while main holds no cache (cut (3)'s cache finding). Locally the gate ran 169 s from a dependency-warm target dir, building the lib and the tests [X]. The list cannot come from the cache: `Swatinem/rust-cache` prunes the workspace crate before its save, so no test binary of this crate is ever restored, and the names are the compiled tests', which a source grep would only approximate. So `docs/ARCHITECTURE.md` and `docs/architecture/` leave the docs set and a change to them runs every job. The cost of that choice is small: none of tonight's five fold PRs touched them (#620, #628, #629 and #640 changed `HANDOFF.md` alone, #622 `HANDOFF.md` and this record), and a change that edits a row is the change the gate exists for.

### The witness

`tools/docs-only-test` [X]: 56 cases, `56 passed, 0 failed`: nine doc paths, fifteen paths that run the full set (the three gate-read documents, a `*.md` under each excluded directory, code, `HANDOFF.txt`), a docs-only diff, a mixed diff, an `.md` under `tests/` beside a doc, `docs/ARCHITECTURE.md` beside a doc, an empty diff, `push`, `workflow_call` and `workflow_dispatch`, a docs-only group, then scratch commits in a throwaway repository (a docs commit, a source commit, a group read against its parent across both, a group without its parent, an unreadable base, `docs/ARCHITECTURE.md` alone), and ci.yml's shape: the five jobs, each with the checkout, then the step, then every later step gated. Planted [X]: the exclusions removed from `is_doc`, `28 passed, 12 failed`; one lint step's gate removed from ci.yml, `FAIL lint: every later step is gated on it ['- name: Clippy (remote only)']`; each reverted, 56 passed.

The dry runs, each a scratch commit in a throwaway worktree under `$TMPDIR` on this branch's tree, the step run as CI runs it [X]:

| scratch diff | the step's output |
|---|---|
| docs only: `HANDOFF.md`, `docs/lanes/README.md`, `CHANGELOG.md` | `docs_only=true` (and `docs_only=true` written to `GITHUB_OUTPUT`) |
| mixed: `HANDOFF.md`, `src/wallet/wallet.rs` | `docs_only=false` |
| `docs/ARCHITECTURE.md` alone | `docs_only=false` |
| `tests/CLAUDE.md` alone | `docs_only=false` |
| a group of two commits, `src/lib.rs` then `HANDOFF.md` | `HEAD^1` alone `true`; against the group's parent `false`; a group without `--base` `false` |
| `--event push` | `docs_only=false` |

### Before: tonight's doc PRs

Each run's wall clock is its creation to its last job's end; the landing latency is the PR's open to its merge.

| PR | run | jobs | wall clock | worst queue wait (created to a job's start) | landing latency |
|---|---|---|---|---|---|
| #620 | 41546970109 | ten | 18:25 | 10:28 | 18:45 |
| #622 | 41552028831 | eleven | 23:24 | 18:26 | 24:01 |
| #628 | 41559537101 | eleven | 13:02 | 9:09 | 13:25 |
| #629 | 41563270106 | five | 7:56 (the queue quiet; 7:57 to the run's last update) | 0:43 | 8:10 |
| #640 | 41567210695 | five | 7:40 | 0:03 | 8:04 |

In #629's and #640's five-job runs the Test Suite set the wall clock (7:36 and 7:37: every test, #618 not yet landed), then the remote-only job (4:15, 4:07), lint (4:12, 2:07), checks (1:50, 1:48) and semver (1:39, 1:14). After this cut a docs-only job is its runner's start, the checkout and the step: seconds per job, so a docs-only PR's wall clock is the queue wait plus under a minute, on its own run and on its group's.

### After (the captain fills at the landing)

- this PR's own run 41573724108 on the rebased tip 6183eb72 (the step said `false`; every job its full set): wall clock 8:04; checks 1:40 (waited 0:56), semver 1:30 (waited 0:56), Test Suite (ubuntu-latest, stable) 7:07 (waited 0:56), lint 1:38 (waited 0:56), Test Suite (remote only) 4:12 (waited 0:58); the worst queue wait 0:58; the five contexts reported success. Its queue group's run 41574586600 (9d611cea): wall clock 7:33; lint 4:25 (waited 0:03), Test Suite (ubuntu-latest, stable) 7:30 (waited 0:03), semver 2:41 (waited 0:03), Test Suite (remote only) 4:15 (waited 0:03), checks 3:29 (waited 0:03); the worst wait 0:03; success. The group run's cache step, the #642 seed's after witness: Test Suite (ubuntu-latest, stable): `Restored from cache key "v0-rust-test-sqlite-remote-Linux-x64-e7cfd85a-24ba8ce8" full match: true.`; Test Suite (remote only): `Restored from cache key "v0-rust-test-remote-Linux-x64-e7cfd85a-24ba8ce8" full match: true.`. Merged 03:58:43Z at 9d611cea; the landing latency from the lane's DONE (03:20:10Z) to the merge is in the figures line below.
- the next docs-only PR is the fold that carries this section, PR #646: its own run 41575436501 (4520fb7c): wall clock 0:25; Test Suite (ubuntu-latest, stable) 0:07 (waited 0:03; 12 steps after `Docs only` skipped, 2 ran); checks 0:06 (waited 0:10; 13 steps after `Docs only` skipped, 2 ran); Test Suite (remote only) 0:08 (waited 0:03; 7 steps after `Docs only` skipped, 2 ran); lint 0:12 (waited 0:12; 11 steps after `Docs only` skipped, 2 ran); semver 0:07 (waited 0:03; 5 steps after `Docs only` skipped, 2 ran); the five contexts all green. Its queue group's run is read in the captain's batch line and the next census touch (#634).

### The five figures of loop two, as they stand

| figure | value | source |
|---|---|---|
| a PR's CI wall clock | a docs-only PR 7:40 to 7:56 with the queue quiet, 13:02 to 23:24 with it busy; a code PR 6:04 (#639's own run 41569305840, everything reached) | the runs above |
| the queue wait | 0:03 to 0:43 quiet, 9:09 to 18:26 busy | the runs above |
| the nightly's length | none | this repository has no scheduled workflow |
| a cold read's length | 10 to 13 min | as recorded at round one [D] |
| the landing latency | a docs-only PR 8:04 to 24:01; #639 43:15 (open 02:20:57Z, merged 03:04:12Z) | the PRs' open and merge times |

At #645's landing (03:58:43Z): a code PR's own run 8:04 (its group run 7:33); the queue wait 0:58 (0:03 in the group); the nightly's length none; a cold read 3 minutes (this PR's lens read, 03:36Z to 03:39Z); the landing latency 0:38 (the lane DONE 03:20:10Z to the merge; the read, the wording fix, three doc commits, a rebase for the row conflict and 7:33 of group run inside it).

What the cut leaves: a push to beta runs the full set for every change, a doc's too (tonight's fold pushes: 41564155475 4:43, 41560974504 10:31, 41568004709 10:03), by the brief and by the push's role (it seeds the caches and checks the line's tip); the HANDOFF row at a PR's open (#633) removes most fold PRs and with them their push runs.

## Loop two, cut (2): the ten slowest tests (#634)

Base `110942af` (origin/beta). The measure is the merge queue's run 41578469987 (2026-11-03 04:30Z, beta 110942af's group, every test run): the Test Suite (ubuntu-latest, stable) job took 7:33 warm, its unit run under nextest 3:57 for 1972 tests. nextest's `PASS [ <s>s ]` lines of that run, the colour codes stripped (`gh run view 41578469987 --log`, read only), give 1433.8 test-seconds over the run's 3541 passes: 809.9 in the Test Suite's unit run, 72.8 in its integration run, 551.1 in the remote-only job's 1100. The ten slowest by name hold 583 of them [X]:

| runner seconds (unit; remote-only) | test |
|---|---|
| 164.5; 130.4 | `services::services::tests::test_the_status_verdict_is_the_rule_over_all_81_rows_from_both_starts` (#627) |
| 55.4 | `wallet::wallet::busy_door_tests::a_wallet_and_a_monitor_over_one_store_share_the_writer` |
| 50.1; 49.5 | `services::services::rule_28_tests::the_utxo_verdict_is_the_rule_from_every_start` |
| 28.8; 29.2 | `services::services::ladder_fixtures_tests::ladders::lock::ladder_fixtures_server_locks_and_disposals_stay_off_runtime_workers` |
| 27.4; 25.7 | `services::services::ladder_fixtures_tests::ladders::ladder_fixtures_replay` |
| 21.4 | `storage::sqlx::create_action::beef::beef_read_once::the_judge_reads_the_corpus_the_same_on_both_paths` |
| 19.9 | `storage::sqlx::create_action::beef::beef_read_once::the_census_creates_the_same_rows_on_both_paths` |
| 15.9; 15.2 | `services::services::ladder_fixtures_tests::ladders::cut::ladder_fixtures_a_cut_answer_always_reaches_the_client_as_a_body_cut_short` |
| 15.3 | `storage::sqlx::internalize_action::a8_beef_bytes_tests::the_census_beefs_read_the_same_on_both_paths` |
| 14.8 | `storage::sqlx::internalize_action::a8_beef_bytes_tests::the_census_internalizes_the_same_rows_on_both_paths` |

Five of the ten run in both Test Suite jobs (the remote-only job runs the lib tests again under `--no-default-features --features remote`); the busy door and the four BEEF tests are `sqlite`'s alone.

### What made a row dear on the runner

The 81-row table took 0.31 s on the box and 164.5 s on the runner: the ratio is not the machine's. A test that builds one `Services` took 0.28 s on the runner in the same run (`test_one_chain_index_unknown_and_a_fault_is_could_not_look`, 0.284 s), and the 27-row status table, which builds 54, 11.2 s: the cost is the build, a TLS context a client (native-tls is OpenSSL there, cheap on macOS), about 50 ms a build (164.5 s over the table's 3240 builds). No row sleeps; no row builds a store. The rule-28 table builds one `Services` with three explorer providers a row and start (768 client builds), and sleeps the utxo ladder's 2 s retry round once a start for its faulted rows. The ladder replay and the lock test build one `Services` a scene (541 and 549 builds) and wait out the scenes' scripted timeouts and retry rounds. The busy door's monitor pass reads every `nosend` action the rounds before it left, so its 300 rounds cost as their square (a pass alone 19 ms at round 100, 54 ms at round 250, the joined round 46 ms and 116 ms, on the box). The four BEEF tests read the census's 1, 4 and 10 MB BEEFs on both paths, about 0.13 s a MB a path in a debug build on the box; generating them is a fifth of that (the 10 MB BEEF built in 0.30 s, read in 1.33 s on both paths). The cut test sends a thousand cuts, each 544 KiB of padding.

### The cuts

Client builds counted by a probe in the test transport's `build` on the base copy and on the tip (each test alone under nextest, `--no-capture`; the probe reverted on both) [X]: the 81-row table 3240 to 1, the rule-28 table 768 to 1, the lock test 549 to 10, the replay 541 to 7 (a few workers race the opening build of a timeout); the other six build no client through the transport.

| test | cut | reason (in the test's doc comment) |
|---|---|---|
| 81-row status verdict | made fast: `share_clients_on_this_thread` | every client the isolated builder makes differs only in its two timeouts, so one per setting serves the table; each of the 648 asks still builds its own `Services`, gates and collections |
| rule-28 utxo verdict | made fast: the same share, and the three starts joined | the builds as above; the starts ran one after another, each sleeping the 2 s round, and share one round joined; the 3 x 64 asks are counted |
| ladder lock, ladder replay | made fast: `SharedClients::building` in `services_at` | the replay pool's workers move between runtime threads, so the host builds each scene's `Services` under one process set of clients; each scene keeps its own `Services`, gates and held clock |
| busy door shared writer | made fast: 60 rounds, not 300 | each round joins `createAction` with the monitor's pass and meets the one writer; the planted refusal bites by round 5 (below) |
| the four census BEEF tests | made fast: the 1 MB census alone (`CENSUS_MEGABYTES`) | both paths read through bsv-rs's decoder in 16 KiB chunks (`beef_stream.rs` `CHUNK`), 1 MB crosses it 61 times, and no bound of the door's lies between 1 and 10 MB (the next is `DOOR_STACK_MEMORY_BYTES`, 100 MB); a recorded input would save only the fifth that generation costs; the census example keeps its three sizes |
| ladder cut | one job alone: ignored where `sqlite` is off | it reaches mockito, hyper and reqwest and no feature of the crate; the thousand stay, since the race it guards is 1 to 10 in 10,000 bare cuts and the padding removed passed three runs of three on the box: a smaller count would weaken a kill that is already statistical |

The seams, both `cfg(test)` in the test transport `src/services/providers/http_client.rs` (the `shared` module of `isolated`; production and `bench-support` builds unchanged): `share_clients_on_this_thread()`, a guard under which the thread's builds share one client per `(timeout, connect_timeout)`, and `SharedClients::building(f)`, the same for a synchronous build on whichever thread. The builder records its two timeouts under `cfg(test)` as the key.

Nightly-only is not available: the toolbox has no scheduled workflow, and the Mutants workflow (dispatch only) runs no ignored test.

### The after on the box

Each of the ten alone under `cargo nextest run --lib --test-threads 1` (nextest 0.9 from `~/bsv/targets/nextest-tools`), host seconds, the before at 110942af in this worktree before any edit, the after at the lane's tip [X]:

| test | before | after | ratio |
|---|---|---|---|
| 81-row status verdict | 0.313 | 0.163 | 1.9x |
| busy door shared writer | 21.519 | 0.635 | 33.9x |
| rule-28 utxo verdict | 6.118 | 2.108 | 2.9x |
| ladder lock | 12.180 | 12.183 | 1.0x (the scenes' waits; builds 549 to 10) |
| ladder replay | 10.059 | 10.060 | 1.0x (the scenes' waits; builds 541 to 7) |
| judge reads the corpus | 3.607 | 0.501 | 7.2x |
| census creates the same rows | 3.824 | 0.269 | 14.2x |
| ladder cut | 0.772 | 0.502 | (the same work; not cut on this job) |
| census BEEFs read the same | 2.607 | 0.165 | 15.8x |
| census internalizes the same rows | 4.742 | 0.275 | 17.2x |
| the ten | 65.743 | 26.863 | 2.4x |

The box cannot show the runner's build cost; the PR's own run shows it.

### The kill kept

Each plant applied to a scratch copy of the base (`git archive 110942af`) and to the tip, the ten run on each, every plant reverted on both and the files compared to the base [X]:

| plant | test | red on the base | red on the tip |
|---|---|---|---|
| `absent >= UTXO_NEGATIVE_PROVIDERS` to `>` in `get_status_for_txids` | 81-row | `Arcade Known, WhatsOnChain Unknown, Bitails Unknown, BananaBlocks Known, start 0, use_next false`: left `Some("known")`, right `Some("unknown")` | the same row, the same values |
| `answers.len() >= needed` to `>` in `utxo_ladder` | rule-28 | `WoC Unspent, BananaBlocks Unspent, Bitails Unspent, start 0`: left `("error", None)`, right `("success", Some(true))` | the same |
| `begin_immediate` refuses `StoreBusy` when the writer is held | busy door | `round 0: createAction beside the monitor: WalletBusy { detail: "Store busy: the writer was not acquired within the store's ceiling" ... }`, five runs of five at round 0 | the same text at round 7, then rounds 4, 5, 5, 5, 2 in five more |
| `script` takes the servers' locks on the worker (no `spawn_blocking`) | ladder lock | `a runtime worker took a server's lock, which can wait behind a sleeping answer (#292)`, left 93, right 0 | the same |
| the leads ladder's retry round re-asks the faults that are not transient (with the two above) | ladder replay | `20 of 92 ladder fixtures differ` (5 `get_utxo_leads`, 8 `get_utxo_status`, 3 `is_utxo`, 3 `is_utxo_with_spender`, 1 `get_merkle_path`) | the same 20 |
| `cut_short` returns `Ok(())` | ladder cut | `cut 0: the body read Ok(...), not a body cut short` | the same |
| `read_once` drops the subject; `raw_unproven` inverted | the four BEEF tests | the census tests at `census 1 MB` (internalize: left `Ok(InternalizeActionResult { accepted: true })`, right `Err(ValidationError("BEEF is not AtomicBEEF (missing atomic_txid)"))`; create: left `Err(SpendRefused { ... OP_EQUALVERIFY ... })`, right `Ok(...)`), the judge at corpus input `02665cb9...` (tip, main) | the same scenes and values |

An earlier round with the writer's ceiling planted at 1 ms failed the busy door and the judge at their stores' setup (`migrate`, `in_memory`), not at their assertions; it was reverted and replaced by the plant in the table. No mutants record names the mutants these tests kill: the complete record of 2026-11-02 names the busy door only among the tests that ran past 60 s under the timeout mutant n 1125 (`RateGate::admit_at` to `Ok(())`).

### Findings (outside this lane's bounds)

- The remote-only job also runs the 81-row and rule-28 tables, the replay and the lock test; the services code they reach has no feature `cfg`, so a `ci.yml` filter could leave `services::` lib tests to the Test Suite job alone (24.5 s on the box for the four now, 22.2 of it the replay's and the lock test's scene waits). Not made: the reason is the services code's today, not a property of the tests.
- The BEEF tests' cost is reading in a debug build; a dependency `opt-level` in the test profile is `Cargo.toml`, the owner's word, and unmeasured here.

### The five figures

| figure | value | source |
|---|---|---|
| the Test Suite job's warm time | before 7:33 (run 41578469987); after 4:57 on the PR's own run 41585313745 (the unit run 1:41 against 3:57; the remote-only job 2:11 against 4:15) and 3:59 on its group run 41588171820 (the unit run 1:16; the remote-only job 2:06; the run's wall clock 4:02) | the runs |
| the ten's runner seconds | before 583 (the table above); after 32 in the group run 41588171820 (ladder_fixtures_server_locks_and 12.4, ladder_fixtures_replay 10.4, the_utxo_verdict_is_the_rule_from_every_ 2.2, ladder_fixtures_a_cut_answer_alwa 1.9, the_judge_reads_the_corpus_the_same_on 1.5, a_wallet_and_a_monitor_over_one_store_sh 1.1, the_census_creates_the_same_rows_on_bo 0.9, the_census_beefs_read_the_same_on 0.6, the_census_internalizes 0.5, test_the_status_verdict_is_the_rule_over 0.5; the cut test runs in the sqlite job alone) | runs 41578469987 and 41588171820 |
| the ten's box seconds | 65.7 before, 26.9 after | above [X] |
| the nightly's length | none | no scheduled workflow |
| a cold read's length, the landing latency | the read 27 minutes on the Studio (05:41Z to 06:08Z, a cold build inside it); the landing 34:26 from the lane's DONE (05:40Z) to the merge at 06:14:36Z (004f77b3) | the read lane's record; the PR |


## Loop two, cut (3): the remote-only job's repeated services tests (#651)

Base `90ac9c7d` (origin/beta). The `Test Suite (remote only)` job runs the lib tests again under `--no-default-features --features remote`; four of #634's ten ran there as in the sqlite job, the same code: `src/services/**` carries no `cfg(feature = "sqlite")` and no `cfg(feature = "remote")` on their paths [X] (`grep -rn 'feature *= *"' src/services`). The one `any(feature = "sqlite", feature = "remote")` there, `providers/http_client.rs:81`, is the `brc104_door_cases` test module, true in both jobs and off these paths. The `break-glass-script-history` cfgs sit on the script hash history scan (the `Services` field `get_script_hash_history_services`, its getter and wiring in `services.rs`, and the T13/T14 tests at `rule_28_tests.rs:141` and `:154`), off in both jobs; the `bench-support` cfgs sit on the test transport's `isolated` module in `providers/http_client.rs`, which `cfg(test)` turns on in both jobs alike. None changes what the four tests reach between the two jobs.

Seconds from nextest's `PASS [` lines, the colour codes stripped (`gh api .../actions/jobs/<id>/logs`, read only) [X]:

| test | remote-only, run 41578469987 | remote-only, run 41588171820 (after #634) | sqlite unit, 41578469987 | sqlite unit, 41588171820 |
|---|---|---|---|---|
| `services::services::tests::test_the_status_verdict_is_the_rule_over_all_81_rows_from_both_starts` | 130.4 | 0.7 | 164.5 | 0.5 |
| `services::services::rule_28_tests::the_utxo_verdict_is_the_rule_from_every_start` | 49.5 | 2.4 | 50.2 | 2.2 |
| `services::services::ladder_fixtures_tests::ladders::lock::ladder_fixtures_server_locks_and_disposals_stay_off_runtime_workers` | 29.2 | 12.7 | 28.8 | 12.4 |
| `services::services::ladder_fixtures_tests::ladders::ladder_fixtures_replay` | 25.7 | 10.7 | 27.4 | 10.4 |
| the four | 234.8 | 26.5 | 270.8 | 25.5 |

The remote-only job's length: 4:15 in run 41578469987 (1100 tests run, 13 skipped, nextest's summary 200.3 s), 2:06 in run 41588171820 (1099 run, 14 skipped, 77.8 s).

### The cut

Each of the four carries `#[cfg_attr(not(feature = "sqlite"), ignore = "runs in the sqlite Test Suite job; the services module compiles the same under both feature sets (#651)")]`, the pattern #634 gave the ladder cut test. The attribute and not a `ci.yml` filter: a filter by module (`services::`) would also leave out of the remote-only job any later services test that does depend on the feature set, silently; the attribute is per test, opt in, with its reason at the test, and a new services test runs in both jobs until someone confines it. `cargo test --lib --no-default-features --features remote` on the box [X]: before (the base's four files) `851 passed; 0 failed; 3 ignored`, after `847 passed; 0 failed; 7 ignored`, the cut test of #634 among the three and the seven, each of the four `ignored, runs in the sqlite Test Suite job; ...`. The remote-only job loses 26.5 test-seconds of its after (two thirds of them the replay's and the lock test's scene waits); the job runs its tests in parallel, so the job's length falls by less. The remote-only job is not the wall clock (the sqlite Test Suite is): the cut reaches the account's slot-minutes and the group's second job, not the figures' wall clock.

### The guard

`tools/ignore-sites` reads every `cfg_attr(not(feature = "sqlite"), ignore` site under `src/` and `tests/` and matches its function against the `sqlite,remote` build's lists (`cargo test --lib --tests --features sqlite,remote -- --list`, and `-- --list --ignored`): a site the build does not list, or lists as ignored, is red, so no test confined to the sqlite builds is ignored in every job. Its cases are `tools/ignore-sites-test` (six, over scratch trees: green, one line under `tests/`, ignored under both, unlisted, a plain `#[ignore]` not a site, an empty list a usage fault), 6 passed [X]. On the tree: `ignore-sites: 5 sites, 0 red` [X]. The plant, a second `#[ignore = "plant (#651)"]` on `ladder_fixtures_replay`, rebuilt and listed: `FAIL tests/fixtures/ladders.rs:800 ladder_fixtures_replay: ignored in the sqlite build too: no job runs it`, `5 sites, 1 red`, exit 1; reverted, the file's diff empty [X]. The guard is not in a workflow: `ci.yml` is outside this lane's bounds, so the captain decides whether the Test Suite job runs it after its build (the two `--list` runs reuse that build).

### After (the captain fills at the landing)

The PR's own run and its group run fill the after (the captain): the remote-only job's length against 2:06, its skipped count against 14 (18 expected), and the four absent from its `PASS [` lines and present in the sqlite job's.

### The five figures

| figure | value | source |
|---|---|---|
| a PR's CI wall clock | unchanged by this cut (the sqlite Test Suite job is the wall clock; 4:02 in run 41588171820) | run 41588171820 |
| the queue wait | unchanged by this cut | as above |
| the nightly's length | none | no scheduled workflow |
| a cold read's length | the captain's at the landing | |
| the landing latency | the captain's at the landing | |

## Loop two, the hygiene row: the map's walked set (#644)

The finding of cut (1) above: `tools/affected-tests` classed every `*.md` as `none` before it read the path's directory, so a PR changing only a `README.md` under `tests/fixtures/data/storage-wire/` ran no cargo test on its own run, and `every_rpc_method_has_a_witness` went red only on the queue's group run. A late red, not a bad landing. The figures are unchanged by it.

The cut: a `*.md` under `tests/`, `fuzz/`, `test_vectors/`, `tools/` or `.github/` reaches everything; `docs/` and a `*.md` elsewhere stay `none` (the architecture doc gate runs on every PR outside the map). The set has one source: `tools/affected-tests` loads `tools/docs-only` with `SourceFileLoader` and reads its `NOT_DOC_PREFIXES`, no file added; `tools/affected-tests-test` loads `tools/docs-only` apart and checks the two sets agree. Everything rather than named targets: no one target per directory walks it alone [X, by grep for `read_dir`, `CARGO_MANIFEST_DIR` and the directory names]. `tests/fixtures/data/` is walked by `storage_wire` and `fixtures` (`ladders.rs`, `ladder_arcade.rs`, `storage.rs`); `fuzz/` by a `storage` unit test (`a8_beef_bytes_tests.rs`); `test_vectors/` is read by `test_vectors`, `fixtures` (`storage/seed.rs`) and `managers` unit tests (`include_str!`); `tools/` and `.github/` went to everything already.

Cases: one per directory in `tools/affected-tests-test` (the planted README of #632 among them) and the two-sets case, 37 in all. Planted [X]: the old rule back for `tests/` alone turns the README case red (`scope: none`), and a literal set without `fuzz/` turns the `fuzz/README.md` case and the agreement case red; reverted, 37 pass.
