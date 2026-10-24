
# oracle-54-fix (#54, the workflow file is invalid)

Repository: bsv-wallet-toolbox-rs. Your worktree is on branch `lane/oracle-54-fix`, cut from `lane/oracle-54-benches` (two commits past `origin/beta`: the criterion benches, `tools/bench-compare`, the `bench` CI job). Read `.github/workflows/ci.yml` whole and the brief's intent in issue #54 (`gh issue view 54`).

The fault: GitHub refused the workflow on this branch ("This run likely failed because of a workflow file issue"), so no check runs on PR #86. `actionlint` names it: the `bench` job's job-level `env` uses `${{ runner.temp }}` (lines 38 to 40: `CARGO_TARGET_DIR`, `CRITERION_HOME`, a third), and the `runner` context is not available in a job-level `env`. Fix: move those `env` lines onto the steps that need them (step-level `env` may read `runner`), or set them in a first step with `echo "NAME=$RUNNER_TEMP/..." >> "$GITHUB_ENV"`. Then run `actionlint` on `.github/workflows/ci.yml` and `.github/workflows/release.yml` (install under `~/bsv/targets/actionlint-tools` if not on the PATH; `brew` is not yours) and show zero findings. One commit.

While there: read the whole `bench` job against the brief (the Linux baseline only is compared; a laptop's numbers are never compared to it; the job is not required) and fix anything that would make the job red on its first run (a missing `python3`, a path that does not exist on the runner, a baseline keyed to the wrong machine name). Say what you changed and why, or that nothing else needed to.

Gate: `actionlint` clean; `cargo fmt --all -- --check`; `cargo clippy --all-targets -- -D warnings`; `cargo test --lib`; `python3 tests/bench_compare_test.py`.

Bounds: `.github/workflows/ci.yml`, and `tools/bench-compare` or `benches/baseline.json` only if the job's first run would otherwise be red. One hour.
