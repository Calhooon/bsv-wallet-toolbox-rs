# oracle-57-mutants (#57)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-57-mutants` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: `CLAUDE.md`, `.github/workflows/ci.yml` (the action pins by sha, the toolchain), issue #57, the CLI's shape `git -C ~/bsv/bsv-wallet-cli show origin/beta:docs/lanes/oracle-nightly.md`.

The work:
- `cargo-mutants` installed under `~/bsv/targets/mutants-tools` (`cargo install --root`, never `~/.cargo/bin`); `.cargo/mutants.toml` listing the files: `src/storage/sqlx/create_action.rs`, `process_action.rs`, `internalize_action.rs`, `src/services/services.rs`, `src/services/providers/*.rs`, `src/monitor/tasks/*.rs`; a timeout multiplier that fits the suite; the test command `cargo test --lib` (say why, if the integration tests are needed for a file).
- `.github/workflows/nightly-mutants.yml`: a cron at a quiet hour UTC and `workflow_dispatch`; runs `cargo mutants --in-place` sharded if the run is over the job limit; publishes per-file caught, missed, timeout and unviable counts as a job summary table and uploads `mutants.out` as an artifact; `tools/mutants-score` (one Python 3 script) computes the score per file from `mutants.out/outcomes.json` and fails when a listed file is under its floor.
- The floors in `tools/mutants-floor.json`: 80 for a file at or above 80 today; else today's score rounded down to the integer, with a comment line per file naming the issue to raise it (the captain files the issues from your report).
- Measurement on your machine, within the budget: run the full set for `src/services/providers/*.rs` and `src/monitor/tasks/*.rs` (the smaller files), and for the three door files run with `--shard 1/8` or a `--jobs` and a wall-clock budget of two hours; record the scores you measured as `[X]` and the rest as `[D]` to be read from the first nightly. List every surviving mutant in the door files in the report (file, line, the mutation), since each is an issue.

The proof: the commands and tails; the summary table as your tool prints it; a planted floor above a measured score makes `tools/mutants-score` red; revert.

Bounds: no change to `src/`; no `Cargo.toml` edit; the nightly is not a required context. Eight hours wall clock including the runs.
