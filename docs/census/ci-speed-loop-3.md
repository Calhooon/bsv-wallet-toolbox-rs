Loop three of the CI speed line (#635), split from the record [ci-speed-2026-11-02.md](ci-speed-2026-11-02.md) (#670).

## Loop three, cut (1): compile time by evidence (#635)

Base `81f5c750` (origin/beta). The box is the Studio at `CARGO_BUILD_JOBS=4` throughout, with the workflow's `CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_INCREMENTAL=0`; each build under `cargo test --no-run ... --timings`, its figures read from the report's unit table (the HTML stays out of the tree). Loop two's runner figures, the before: in run 41588171820 the Test Suite job took 3:59, the unit test build 1:03, the integration build 0:31, the architecture doc gate 0:49 [D].

### The builds on the box

| build | cold | warm (the crate cleaned) | units | what the warm build rebuilds |
|---|---|---|---|---|
| unit tests, `--lib --features sqlite,remote` | 53.9 s (alone) | 33.3 s | 360 | the lib test binary alone |
| integration, `--test '*' --features sqlite,remote,bench-support`, after the unit build | 22.7 s (alone) | 21.6 s | 397 | the lib (13.9 s, 12.9 of it the frontend) and the 37 test binaries |
| remote only, `--lib --tests --no-default-features --features remote` | 48.8 s (beside the unit build) | 17.4 s | 385 | the lib test binary (17.2 s), the lib and the 37 test binaries |

No dependency rebuilds in a warm build: every rebuilt unit is the crate's own [X].

The critical path of the cold unit build: the dependency frontier to 19.1 s (some 120 units at four jobs, `bs58` the last before it), `bsv-rs` 8.4 s, the lib test binary 26.4 s; 53.9 s. The ten longest units: the lib test binary 26.4 s, `bsv-rs` 8.4, `tokio` 2.0 and 1.6 (two builds), `syn` 3 1.6, `h2` 1.6, `syn` 2 1.4, `zerocopy` 1.3, `sqlx-sqlite` 1.2 and 1.2 (two builds) [X]. 36 dependency units build twice in one build (the target and the host copies `sqlx-macros` pulls, its proc-macro side resolved apart), 11.9 s of the build's 138.3 CPU seconds [X].

### The integration binaries

37 targets: 36 `tests/*.rs` and `tests/fixtures/main.rs`. In the warm integration build the binaries take 28.8 CPU seconds, 7.5 s of wall after the lib at four jobs; the longest `fixtures` 3.0 s, `storage_wire` 2.5, `monitor_tests` 1.2, `concurrent_tests` 1.1, `broadcast_memory_tests` 1.1; the three `rule_28_*` scanners and `certificate_map_order` 0.1 to 0.3 s each (they link no store) [X]. The executables run 1 to 46 MB (775 MB for the 38 with the lib test binary). A scratch copy with 34 of the 36 entry points as `mod`s of one `tests/all/main.rs` (`autotests = false`; `storage_wire` and `network_isolation_tests` kept apart for their `#[path]` mounts, `fixtures` for its directory) built warm in 17.6 s against 21.6: the binaries 4.3 s of wall after the lib, 12.2 CPU seconds [X].

### The architecture doc gate

`tools/arch-doc-tests.sh` ran one `cargo test ... <name> -- --list` per row: 345 calls for 349 rows (4 `(no test)`), naming 14 binaries (the lib for 309 rows, 13 integration targets for 36). On the box the gate took 63 s; a `--list` through cargo 0.17 s for the lib and 0.46 s for a target, the lib binary's own `--list` 0.03 s; the rows' parsing alone (`--dry-run`) 4 s [X]. The runner's 0:49 to 1:07 is those 345 cargo starts, each a freshness check of the whole graph.

### The cuts

| cut | before | after (the box, four jobs) | commit |
|---|---|---|---|
| the gate lists each binary once, every row matched against its binary's list with the same suffix pattern and the same one-match count | 63 s, 345 cargo calls | 11 s, 14 cargo calls; the same 345 `ok` lines and matches (`diff` of the two logs' `->` columns empty) | `Gate: tools/arch-doc-tests.sh lists each binary once` |
| `tokio-test` removed (no test, bench, example or doc names it) | 360 units | 359 | `Deps: drop the tokio-test dev-dependency` |
| `anyhow` to dev-dependencies (the storage fixtures' error type; `src/` names it nowhere): a dependent's build no longer compiles it | `anyhow` in the library's normal graph | in the dev graph alone (`cargo tree -e normal -i anyhow` empty) | `Deps: anyhow moves to dev-dependencies` |

The two dependency cuts are off the critical path: the cold unit build at the tip took 56.8 s against the base's 53.9 s alone, within the box's noise (the same base build took 65.9 s beside another) [X]. The gate's plant: two rows' names planted wrong in `docs/ARCHITECTURE.md` (one lib row, one `--test store_write_lock_tests` row), the gate exit 1 with `failed: 2` naming both, reverted [X].

Feature unification: the unit build's and the integration build's dependency features resolve the same (`cargo tree -e normal,build,dev -f '{p}|{f}'`, the diff empty), so one dependency build already serves both of the Test Suite's builds. The remote-only set differs from them in `sqlite` alone (`sqlx`, `sqlx-macros`, `sqlx-macros-core` without it, `sqlx-sqlite`, `libsqlite3-sys`, `flume`, `atoi`, the futures crates' `sink`), the feature that job exists to leave out: no unifying feature, and the two `shared-key`s stay apart [X].

### The cuts the evidence asked for and this lane did not make

- Integration binaries merged: the whole merge saves 4.0 s warm on the box (16.6 CPU seconds), but one binary costs `tools/affected-tests` its per-target selection (#618: a change to one target would run all 34), and `cargo test` without nextest would run 34 files' tests in one process. The pairwise merges the brief names gain under 0.3 s each (the smallest binaries' whole cost). For the captain to file: a merge with the map taught `binary(=all) & test(/^<file>::/)`, measured on the runner.
- `sqlx` without its default features (`any`, `macros`, `migrate`, `json`; the crate uses none: no `query!`, `FromRow` derive, migrator or `Any` pool in `src/`, `tests/` or `benches/`): drops `sqlx-macros` and the 36 units built twice, 11.9 CPU seconds of a cold build; zero on a warm cache. A feature removal on a dependency a dependent may lean on through unification: the owner's word.
  Ruled yes by the owner on 2026-11-03 through the keeper (#658): `default-features = false` on `sqlx`, with `runtime-tokio`, `chrono` and `uuid` named; the crate's `sqlite` and `mysql` features map to sqlx's as before. The feature view (`cargo tree -e normal,build -f '{p}|{f}'`, the sqlx rows) loses `any`, `macros`, `migrate`, `json`, `derive` and `default` on `sqlx`, `any` and `json` on `sqlx-core` and `sqlx-sqlite`; the package set loses `sqlx-macros`, `sqlx-macros-core`, `heck` and `dotenvy` (234 to 230). The figures, on this host (Apple M5 Max) at `CARGO_BUILD_JOBS=4` with `CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_INCREMENTAL=0`, each a cold `cargo test --no-run ... --features sqlite,remote --timings` after `cargo clean` of the lane's own target directory, the units twice counted from the report's `UNIT_DATA` (a unit is its name, version and target; more than one entry is built twice): the unit build (`--lib`) 359 units to 319, 36 twice to 0, 110.1 CPU seconds to 91.5, 50.6 s of wall to 45.1; the lib and the test targets (`--lib --tests`) 397 units to 357, 36 twice to 0, the dependency units' CPU 78.7 s to 69.1 (the crate's own units 58.8 and 57.8), 49.6 s of wall to 46.9 (a second after sample took 52.2 s and 140.6 CPU seconds in all against the base's 137.6: the crate's own type checking is the noise, the dependency side is the cut) [X]. The 36 duplicates' copies sum to 18.6 CPU seconds here, the second copies alone 8.8; the #635 row's 11.9 is the Studio's figure for the same 36 [D]. `chrono` is needed (without it the lib fails with 588 errors, `DateTime<Utc>: sqlx::Encode` among them); without `uuid` or `runtime-tokio` the lib still type checks, and they stay as the brief left them [X]. The lockfile note: `Cargo.lock` is not committed, rust-cache keys on the resolved lock, and the resolved set moves, so the PR's own run and the next push to beta each carry one cold-ish dependency build before the cache settles.
- `futures-util` beside `futures` (`src/services/providers/arcade.rs` alone names `futures_util`): one direct dependency fewer with the import moved to `futures::`; a `src/` change, outside this lane.
- The lib test binary (26.4 s) and the lib's frontend (12.9 s) are the build: what remains is the crate's own type checking, which a split of the crate or a `[profile]` change would move, both the owner's.
- The example `a8_census` fails `cargo check --all-targets --no-default-features --features remote` (it imports `StorageSqlx` without `required-features = ["sqlite"]`); CI's remote-only job builds `--lib --tests` and does not see it. A finding to file.
  Filed as #660 and closed by its lane: the example declares `required-features = ["sqlite"]`, and the lint job's remote-only clippy now runs `--all-targets`, so it reaches every target the `remote` set builds.

### The five figures

| figure | value | source |
|---|---|---|
| the architecture doc gate | before 0:49 (run 41588171820); after 0:05 on the PR's own run 41599282197 and 0:07 on its group run 41601013639 | the runs |
| the Test Suite's two builds | before 1:03 and 0:31; after 1:39 and 0:34 on the PR's own run, 1:37 and 0:45 on its group run (the Test Suite job 4:20 and 4:53) | the runs |
| the gate on the box | 63 s before, 11 s after | above [X] |
| the cold unit build on the box | 53.9 s at the base, 56.8 s at the tip (noise) | above [X] |
| the remote-only build | before 2:06 for the job (run 41588171820); after 2:30 on the PR's own run and 2:51 on its group run | the runs |
