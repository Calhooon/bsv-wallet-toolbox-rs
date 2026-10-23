# oracle-59-dependents (#59)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-59-dependents` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: `CLAUDE.md`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, issue #59, the epic #80's dependents map, the CLI's shape `git -C ~/bsv/bsv-wallet-cli show origin/beta:docs/lanes/oracle-dependents.md`. The dependents' manifests, read-only at a ref: `git -C ~/bsv/bsv-wallet-cli show origin/main:Cargo.toml` (the pin on this crate), and the agents' `manage` crate under `~/bsv/agents` (find its `Cargo.toml` by grep for `bsv-wallet-toolbox`; read, never write). dolphinmilk if it is in `~/bsv` or the org.

The work:
- `tools/dependents-check.sh`: for each dependent named in `docs/DEPENDENTS.md`, clone it at its default branch into `$TMPDIR` (in CI, the runner's temp), rewrite its dependency on `bsv-wallet-toolbox` to a path dependency on the PR's tree (a `[patch.crates-io]` entry does not apply across a version the pin excludes, so edit the dependency line; say how you keep `Cargo.lock` honest: `cargo update -p bsv-wallet-toolbox` only), then `cargo build --locked` falling back to `cargo build` with a note, and `cargo test --lib`. Prove it against the local CLI checkout copied to `$TMPDIR` (never the checkout itself) and against `manage`.
- `cargo-semver-checks` (under `~/bsv/targets/semver-tools`) against the last release tag: `cargo semver-checks --baseline-rev v0.13.0`; a job `semver` in `ci.yml` that fails on a breaking change when `Cargo.toml`'s version is not a major bump from the baseline. Run it on the tree; its tail.
- A job `dependents` in `ci.yml` running the script (the clone is a read; the job needs no token for a public repo; if a dependent is private, say so and leave it to the captain).
- `docs/DEPENDENTS.md`: a table (crate, repository, pinned version of this crate, how it consumes: which features, which API surface by module, its captain) for bsv-wallet-cli, the agents' `manage`, dolphinmilk if found; a line saying the pin-move PRs keep it current.

The proof: the script's run against the CLI copy and `manage` with tails; `cargo semver-checks` tail; a planted break (rename a public function in `src/lib.rs` re-exports) makes semver-checks red; revert.

Bounds: no change to `src/`; no `Cargo.toml` edit; neither job a required context yet. Six hours.
