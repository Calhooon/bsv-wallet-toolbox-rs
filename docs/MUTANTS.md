# Mutation evidence (#57)

The independent `Nightly mutants` workflow runs at 04:41 UTC and on manual
dispatch. It is informational and must not become a required branch protection
context. Sixteen zero-based round-robin shards cover the same complete inventory
(about 124 mutants each at about 90 s a mutant, inside the 330 minute job).
Each uploads its `mutants.out`, inventory and exit code for 14 days, even on
failure. `Mutation scores` combines disjoint outcomes and prints the per-file
table in the job summary. Missing shards, failed baselines, unfinished runs,
duplicate mutants, unsupported outcomes and missing floor entries fail the gate.

`cargo-mutants` 27.1.0 is installed under a separate tool root. The workflow uses
the existing CI's Rust stable toolchain and action commits; artifact actions are
also pinned by commit. No credentials beyond read access are requested. The
workflow writes artifacts and job summaries, and opens no issues or pull requests.
Dependencies are fetched before mutation testing. Mutation builds are offline;
an unreachable loopback proxy blocks accidental external HTTP while allowing
the local mock servers.

## Measurement

`.cargo/mutants.toml` selects the three storage doors, the services orchestrator,
all providers and all monitor tasks. Both mutation build and test phases use
`--lib`, so the test command is `cargo test --lib`. The library exercises the
storage doors with `MockWalletServices` and the providers with `mockito`.
Integration witnesses in `tests/` are not part of this score. In particular,
monitor persistence/rollback, broadcaster HTTP bodies, and orchestrator failover
have additional integration coverage. A miss here is a gap in this measured
suite, not a claim that every integration witness misses it. Adding integration
targets changes the measurement and needs a fresh baseline. Ignored tests that
require private wallet data or external services remain ignored.

The test timeout is three times the baseline test duration with a 20 second
minimum. The build timeout is three times the baseline build duration. Mutations
that introduce unused code are allowed to compile (`cap_lints = true`), while
the ordinary CI and lane checks retain `RUSTFLAGS=-Dwarnings`.

Score is `100 * caught / (caught + missed + timeout)`. A timeout is reported
separately and does not count as caught. Unviable mutants are excluded from the
denominator. No generated mutants means `N/A`; an all-unviable file fails rather
than appearing fully tested. Comparisons use integer arithmetic, without rounding
a score up to a floor. Each measured floor is the smaller of 80 and the observed
score rounded down; every floor in `tools/mutants-floor.json` is a measured one
(the door floors from the 1/8 sample, provisional). JSON `_comment` fields give
the measured score and name the follow-up issue to file for each file below 80.
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

## Filters in cargo-mutants 27.1.0

Two behaviours shape any partial run. `--file` adds to `examine_globs` from
`.cargo/mutants.toml` rather than replacing them; pass `--no-config` to narrow
by file. `--re` and `--exclude-re` do not filter struct-field deletions ("delete
field ... from struct ... expression"): every filtered list carries all of them
from the selected files (32 over this inventory), and `--shard` with `--re`
carries them into every shard. An unfiltered `--shard k/16` is disjoint and
complete (1981 mutants over eight shards checked with `--list`), so the nightly
is unaffected. `--in-diff` with a diff naming one line does confine them.

## The lane measurement (2026-10-23)

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
index 1 (107 of 851); its floors are provisional until the first nightly.

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
