# Mutation evidence (#57)

The independent `Mutants` workflow (`mutants.yml`, named `Nightly mutants` until 2026-10-27) runs on manual dispatch only. Its
04:41 UTC schedule is off since 2026-10-25 (#153). Two dispatches against beta
(runs 40126339129 and 40136138380) showed sixteen round-robin shards over 1994
mutants at about 2.8 minutes a mutant, nearly all of it the per-mutant
`cargo test --no-run` rebuild, so every shard ran past the 330 minute job limit,
no scores were produced, and the sixteen runner slots they held for five and a
half hours queued every other repository's CI behind them. The schedule returns
when the run is shaped to finish: the three door files (861 mutants) nightly in
fewer shards and the full inventory weekly, a cheaper per-mutant build, or both.
It is informational and must not become a required branch protection context.
Beside the sixteen shards, the `doors` job runs the three doors that judge under
the node's word in two shards of their own, under a wider oracle (below, #535).
Each shard uploads its `mutants.out`, inventory and exit code for 14 days, even
on failure. `Mutation scores` combines disjoint outcomes of both shard groups against the
two catalogues joined (they are disjoint: `.cargo/mutants.toml` excludes the
doors) and prints the per-file table in the job summary. Missing shards, failed baselines, unfinished runs,
duplicate mutants, unsupported outcomes and missing floor entries fail the gate.

`cargo-mutants` 27.1.0 is installed under a separate tool root. The workflow uses
the existing CI's Rust stable toolchain and action commits; artifact actions are
also pinned by commit. No credentials beyond read access are requested. The
workflow writes artifacts and job summaries, and opens no issues or pull requests.
Dependencies are fetched before mutation testing. Mutation builds are offline;
an unreachable loopback proxy blocks accidental external HTTP while allowing
the local mock servers.

The workflow file lives on `main`, because GitHub fires a `schedule` only from
the default branch and registers no workflow that exists only on `beta` (#153);
with the schedule off, the `main` copy is what makes the dispatch available.
The floors in `tools/mutants-floor.json` were measured on `beta`; `main` is the
stable line. A dispatch measures the commit it was given (the sha, so every
shard and the scores job read one tree however long they run), so
`gh workflow run Mutants --ref beta` measures beta by hand. Dispatch
only when nothing is landing: the shards take every runner slot the account
has. A dispatch
against `main` compares main's tree against the floors beta measured, since the
tool reads `tools/mutants-floor.json` from the tree it checked out. The same
six files on `beta` (this document, the workflow, `.cargo/mutants.toml`,
`.cargo/mutants-doors.toml`, `tools/mutants-score` and `tools/mutants-floor.json`)
are the copy that moves with the programme; `main` carries them so the dispatch
is registered. A change to the workflow lands on `beta` and the captain carries
it to `main` with the files it reads: a `main` workflow that names
`.cargo/mutants-doors.toml` needs that file on `main` too.

## Measurement

`.cargo/mutants.toml` selects the three storage doors, the services orchestrator,
all providers and all monitor tasks, less the three files of the door oracle
below. Both mutation build and test phases use
`--lib`, so the test command is `cargo test --lib`. The library exercises the
storage doors with `MockWalletServices` and the providers with `mockito`.
Integration witnesses in `tests/` are not part of this score. In particular,
monitor persistence/rollback, broadcaster HTTP bodies, and orchestrator failover
have additional integration coverage. A miss here is a gap in this measured
suite, not a claim that every integration witness misses it. Adding integration
targets changes the measurement and needs a fresh baseline. The door oracle below
is the one place the measurement includes them. Ignored tests that
require private wallet data or external services remain ignored.

The test timeout is three times the baseline test duration with a 20 second
minimum. The build timeout is three times the baseline build duration. Mutations
that introduce unused code are allowed to compile (`cap_lints = true`), while
the ordinary CI and lane checks retain `RUSTFLAGS=-Dwarnings`.

Score is `100 * caught / (caught + missed + timeout)`. A timeout is reported
separately and does not count as caught. Unviable mutants are excluded from the
denominator. No generated mutants means `N/A`; an all-unviable file fails rather
than appearing fully tested. Comparisons use integer arithmetic, without rounding
a score up to a floor. The floors come from the complete figure by the rule
below (the floors from the complete figure); JSON `_comment` fields give each
entry's record, figure, date and oracle.
A null floor still fails enforcement when that file has viable mutants, so a new
file cannot pass unmeasured.

`tools/mutants-score --self-test` exercises aggregation and failure handling.
For a complete run:

```sh
export PATH="$HOME/bsv/targets/mutants-tools/bin:$PATH"
evidence=/Volumes/Lanes/out/bsv-wallet-toolbox-rs/oracle-57-mutants-d
cargo mutants --list --json > "$evidence/catalog.json"
cargo mutants --in-place --output "$evidence/run"
python3 tools/mutants-score --catalog "$evidence/catalog.json" \
  "$evidence/run/mutants.out/outcomes.json"
```

`--report-only` displays partial evidence but never enforces floors. A sampled
floor is provisional until a complete nightly measures that file.

## The door oracle (#535, 2026-11-01)

The owner's word of 2026-11-01 (14:1xZ, 23.5 (c), and 14:4xZ) puts the doors
that judge under the node's word, `create_action/must_spend.rs`,
`create_action/own_word.rs` and `create_action/review_beef.rs`, at an 80 percent
gate, measured with their real witnesses: `must_spend.rs`'s live in the
`fixtures` suite, so `--lib` alone read it at 0 of 19 (#535). Those three files
run from `.cargo/mutants-doors.toml`: the lib and every integration target that
enters a door function (`fixtures`, `caller_complete_funding_tests`,
`immediate_broadcast_hint_tests`, `known_ancestor_tests`, `test_vectors`), with
the CI integration step's features `sqlite,remote,bench-support`. A target that
never enters a file's functions cannot kill its mutants, and no integration
target reads a door file's text, so the oracle is the reach set; a new target
that enters a door joins the list in both the config and the workflow's compile
step. The set came from a probe that logs each door function on entry, run over
every target once; the table and the figures before and after the widening are
`docs/census/mutants-oracle-2026-11-01.md`, and the door floors in
`tools/mutants-floor.json` come from the widened figure only.

The 45 door mutants run as their own shard group, `doors`, two round-robin
shards. The five targets add five test binaries to each mutant's build and about
13 s to its test run beside the lib's (the `fixtures` suite 11.7 s on the lane
host), so a door mutant costs about what a lib mutant costs (about 2.8 minutes on
the runners, mostly the build): 23 mutants a shard is about 64 minutes, inside
the 330 minute limit with room to spare. The rest of the inventory stays on
`--lib` as ruled (the ratchet). Other create-door files are entered by
integration targets as well (the record's second probe); widening them is not
in the ruling.

## The floors from the complete figure (#534, 2026-11-02)

`tools/mutants-floor.json` holds an entry for every file the complete record
catalogues (`docs/census/mutants-complete-2026-11-02.{md,json}`: the Mutants
workflow's set at base `42559923` under the `--lib` oracle, 59.22 percent of
1,974 viable), written by the owner's 23.5 (c) word and the keeper's reading of
2026-11-02 10:4xZ. The rule per entry:

- the floor is the measured figure rounded down to a whole percent: a floor
No cap applies: a file measured above 80 carries its measured figure as its floor (fees 100, beef_graph 92), and a single new survivor there turns the scores job red, which is the ratchet's meaning.
  moves when a floors lane writes it from a record, never by a measurement alone,
  and never falls;
- a file on the paths that judge the node's word and the chain's evidence that
  measures below 80 is raised to 80: red by design, with one triage issue per
  file for its survivors (a test, or an equivalent mutant with its reason). The
  files placed there, each by its `docs/ARCHITECTURE.md` row (named in its
  entry's comment): the create door's BEEF judging (`create_action/beef.rs`,
  `beef_graph.rs`, `beef_storage.rs`, `beef_walk.rs`, `tracker.rs` and the door's
  pre-check in `mod.rs`), the internalize door (`internalize_action.rs`), the
  process door's root check (`process_action.rs`), the reorg and review passes
  (`monitor/tasks/reorg.rs`, `review_proven_txs.rs`), the proof gate
  (`new_header.rs`) and the header services' verdicts (`bhs.rs`,
  `chaintracks_client.rs`, `fallback_chain_tracker.rs`);
- a file whose figure fell below its existing floor keeps the floor, and is a
  finding;
- the three doors keep the floors #571 wrote from the widened figure (80, 71, 80);
- each comment names the record, the figure, the date and the oracle, and a file
  whose figure carries a timeout under the restored environment names it.

The oracle before a finding. The complete record's oracle is `--lib`, and a file
whose witnesses live in an integration target reads low under it (must_spend:
0 of 19 under `--lib`, 18 of 19 widened). A node's-word or evidence file below
80 whose functions an integration target enters is measured again under the
widened oracle before its finding is filed, and the finding stands on that
figure; its entry's comment names both figures and calls the `--lib` one a lower
bound. The floor stays the `--lib` rule's (80 for these files), since the
workflow scores them under `--lib`; a low score never blocks a landing.

The widened re-measure of 2026-11-02 (lane p5-mutants-floors) runs each `--lib`
survivor of the eight path files below 80 once more (217 mutants, none a timeout
under `--lib`), one mutant per `cargo mutants --no-config --file F --re '^NAME$'
--in-diff LINE --in-place --baseline skip --timeout 270 --build-timeout 900` call
(`LINE` a one-line diff naming the mutant's line, which keeps the struct-field
deletions of other lines out, below) in scratch
copies of the base with the record's lockfile, features
`sqlite,remote,bench-support`, building the lib and the file's integration
targets (the reach of #571's second probe for the create, process and
internalize files; `git grep` of the task types for the monitor files;
the `fixtures` ladders for `bhs.rs`). A mutant the lib caught stays caught under
a wider oracle, and the lib's verdict on each survivor under the default features
is the record's, so each run selects by exact name every test of those targets
and the 167 lib tests the widened features add (the remote client's and two
wallet tests); the rest of the lib already missed it. The widened figure is the
record's caught plus the survivors a selected test caught. Leaving the lib's
other tests out of the run also leaves out
`a_wallet_and_a_monitor_over_one_store_share_the_writer`, which under the host's
load (one-minute 37) took 122 s alone and held the lib pass to 309 s, past the
270 s timeout. The ledger, every run's `mutants.out` and log, the queue and the
driver are under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-mutants-floors/`.
The host's load held the floors lane's run at 13 of the 217 survivors; the
re-measure lane finished the rest on the box (`docs/census/mutants-paths-widened-2026-11-02`),
so every path entry's comment carries its widened figure and its finding rests on it.

## Filters in cargo-mutants 27.1.0

Two behaviours shape any partial run. `--file` adds to `examine_globs` from
`.cargo/mutants.toml` rather than replacing them; pass `--no-config` to narrow
by file. `--re` and `--exclude-re` do not filter struct-field deletions ("delete
field ... from struct ... expression"): every filtered list carries all of them
from the selected files (32 over this inventory), and `--shard` with `--re`
carries them into every shard. An unfiltered `--shard k/16` is disjoint and
complete (1981 mutants over eight shards checked with `--list`), so the nightly
is unaffected. `--in-diff` with a diff naming one line does confine them.

## The lane measurement (2026-10-24)

The lane machine was shared (load 12 to 50 on 10 cores), so the lane ran one
mutant per `cargo mutants` call: `--no-config --file F --in-diff <that line>
--re '^<name>$' --in-place --baseline skip --timeout 120 --build-timeout 300
--cargo-arg=--lib --cap-lints true`, five workers on warmed snapshots under
`$TMPDIR`, driven by `drive.py` in the evidence directory and scored with
`score.sh` there (`tools/mutants-score --baseline-skipped`). The baseline was
green before the runs (`cargo test --lib`: 957 passed). Outside the three doors
the runs skip one test, `wallet::wallet::busy_door_tests::a_wallet_and_a_monitor_over_one_store_share_the_writer`
(52 s under load, a wallet-level store test): skipping a test can only lose
kills, so those scores are lower bounds on the nightly's. The doors ran the
whole suite. The door sample is every eighth door mutant of the catalog from
index 1 (107 of 851); its floors are provisional until the nightly has run once. The sample was taken at beta fc343d6; since then create_action.rs (#105, PR #107) and internalize_action.rs (#138, PR #139) changed on beta, so the door floors are lower bounds that predate those two commits.

Monitor tasks score low on `--lib` alone (seven files at 0%): their tests live
in `tests/` (`monitor_tests.rs`, `review_status_tests.rs`, `reorg_tests.rs`),
which this score does not count; whether those tests catch the mutants is
unmeasured. Adding an
integration target would change the measurement and needs a fresh floor set.

The lane's retained evidence, exact commands, tables, command tails and surviving
door mutants live under the external evidence directory above. A surviving door
mutant needs an issue containing its file, line, mutation, source commit and log;
the captain files it and adds a witness in a separate behavior/test lane.
Do not silently lower a floor to make a nightly green.

T3: bounded shards and timeouts cap measurement cost. T5: the gate checks recorded
outcomes instead of trusting a claimed score. T6: the inventory, executable score
checks and mutation diffs let another machine repeat the result. This tooling
does not establish T1, T2 or T4 for wallet behavior.
