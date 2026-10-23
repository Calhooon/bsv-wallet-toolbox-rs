# oracle-55-ratchet (#55)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-55-ratchet` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: `CLAUDE.md`, `clippy.toml`, `.github/workflows/ci.yml`, issue #55 (the numbers measured 2026-10-23), the CLI's shape `git -C ~/bsv/bsv-wallet-cli show origin/beta:docs/lanes/oracle-ratchets.md`.

The work: `tools/ratchet`, one Python 3 script (standard library only), measuring on the tree it is run in and writing one JSON object with stable key order:
- `clippy_pedantic`: the count of distinct warnings from `cargo clippy --all-targets --message-format=json -- -W clippy::pedantic -A clippy::all` (count by lint name, file and line, deduplicated; the sum, and the per-lint counts);
- `unwraps_outside_tests`: `.unwrap()` and `.expect(` in `src/` outside `#[cfg(test)]` modules and `tests/` (a simple module-boundary scan is fine; say its limits), the total and a per-file table;
- `files_over_800`: the files and their line counts; `functions_over_60`: count and the list; `cognitive_complexity`: the count of `clippy::cognitive_complexity` warnings at threshold 25 (`-W clippy::cognitive_complexity`, the threshold in `clippy.toml` if it is not there already, without changing any existing key);
- `duplicated_blocks`: jscpd's count when `jscpd` is on the PATH, else `null` with a `skipped` note (never install it);
- `undocumented_public_items`: the count from `cargo doc --no-deps` with `RUSTDOCFLAGS="-W missing_docs"` or `cargo clippy -- -W missing_docs`, whichever gives a stable count; say which;
- `env_reads`: `env::var` and `std::env::var` calls outside tests.
`tools/ratchet.json` is the baseline in the repo, written by `tools/ratchet --write`; `tools/ratchet --check` fails when any number rises above the baseline and prints each rise; falls are reported and accepted (the baseline is rewritten by hand in the PR that earns the fall). A CI job `ratchet` running `--check` on ubuntu; not a required context until the captain adds it.

The proof: two runs on one tree give byte-identical JSON (`diff` of the two); `--check` green on the baseline; a planted `unwrap()` in `src/lib.rs` makes `--check` red naming the file; revert; the tails in the report. The baseline's numbers against the issue's: 92,371 lines in 85 files, create_action.rs 11,441, 79 unwraps, 5 env reads; explain any gap.

Bounds: no change to `src/` other than none; no `Cargo.toml` edit. Six hours.
