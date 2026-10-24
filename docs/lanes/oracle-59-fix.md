
# oracle-59-fix (#59, the cold review's fixes)

Repository: bsv-wallet-toolbox-rs. Your worktree is on branch `lane/oracle-59-fix`, cut from `lane/oracle-59-dependents` (two commits past `origin/beta`: `docs/DEPENDENTS.md`, `tools/dependents-check.sh`, the `semver` and `dependents` CI jobs). Read those three files whole and issue #59 (`gh issue view 59`).

The cold review's findings and the captain's decisions. One commit per item, each with its proof (plant, run, show, revert):
1. (must) The semver job lets a breaking change through when `Cargo.toml` is bumped to the next minor on 0.x (the reviewer removed `StoreBusyReason`, set 0.14.0, and `cargo semver-checks --baseline-rev v0.13.0` passed with "0 checks"). The policy: the public API's shape is the owner's word, so the job fails on any breaking change against the baseline whatever the version says (`--release-type minor` or the flag that pins the comparison to the baseline's minor; read `cargo semver-checks --help` and say which). The one way past it is a PR label `api-break` the owner sets: when `github.event.pull_request.labels.*.name` contains it, the job prints a notice naming the breaks and passes. Proof: the reviewer's plant at 0.14.0 now fails; the same plant with the label condition forced true passes with the notice.
2. (declined by the captain, record it) dolphin-milk stays out of the CI rows: it does not build against this tree on its own `bsv-rs` 0.3 pin, so its row would be red for a reason this repo cannot fix. Add to `docs/DEPENDENTS.md` a "re-pin plan" column or line per excluded row: the blocker, the issue that tracks it here (the captain files it; write `tracked in #TBD` and the captain fills the number), and that the row joins the job when its captain re-pins.
3. (must) The script runs `cargo test --lib` only; a planted `compile_error!` in a CLI integration test passed. Run `cargo test --locked` (every target) by default; a row may name a narrower test command in the table with a reason beside it. Proof: the reviewer's plant now fails.
4. (must) A missing or empty table passes silently (removing the table file printed "No such file or directory" and exited 0). Fail on a missing file, a missing `dependents-check:begin` marker, or an empty selection, with the reason. Proof: each case exits non-zero.
5. (must) The comment at `tools/dependents-check.sh:20` carries the word the rules forbid ("first"); reword it. Grep the three files for any other.

Gate: `bash -n`, `shellcheck tools/dependents-check.sh`, `actionlint .github/workflows/ci.yml` (install under `~/bsv/targets/actionlint-tools` if absent; never `brew`), `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`; and `tools/dependents-check.sh --only bsv-wallet-cli` against a copy of the CLI clone in `$TMPDIR` (read-only clone of the public repository is a read).

Bounds: the three files only. Two hours.
