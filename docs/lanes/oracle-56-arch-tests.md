# oracle-56-arch-tests (#56)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-56-arch-tests` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: `CLAUDE.md`, `src/services/rule_28_tests.rs` (the shape to copy), `src/services/CLAUDE.md`, `src/storage/CLAUDE.md`, `src/error.rs`, issue #56, the CLI's shape `git -C ~/bsv/bsv-wallet-cli show origin/beta:docs/lanes/oracle-lints.md`.

The work: `src/architecture_tests.rs` (`#[cfg(test)]`, declared from `lib.rs`), one test per rule, each scanning `src/` from `CARGO_MANIFEST_DIR` and failing with the file and line of every violation:
1. Provider requests only under `services/providers/` (and the chaintracks client if it lives elsewhere; name where the HTTP client is built today and set the rule to that boundary): no `reqwest::` outside it.
2. SQL only under `storage/sqlx/`: no `sqlx::query`, `query_as`, `query_scalar` or a string beginning `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE` outside it.
3. `env::var` only in the config modules (name them from today's five reads).
4. No `unwrap()` or `expect(` outside `#[cfg(test)]`, with an allowlist by file carrying today's count as the ceiling; a file over its ceiling fails; a file under it is reported so the ceiling can fall.
5. No error classified by a string prefix: no `starts_with(`, `contains(` or `==` against a literal applied to an error's `to_string()` or message outside tests; an allowlist by file with today's counts.
6. Every public verdict type (`UtxoVerdict`, `BoundSpender`, the ladder result types in `services/types.rs` or wherever they live) constructed only in its own module: no `UtxoVerdict::Spent` style constructor outside `services/`, tests excepted.
The allowlists live in the test file as tables with a comment naming the structure-phase issue that shrinks them (#61, #62, #63).

The proof: `cargo test --lib architecture_tests` green on the tree; then a planted violation per rule (a `reqwest::Client::new()` in `wallet/wallet.rs`; a `sqlx::query` in `services/services.rs`; an `env::var` in `monitor/`; an `unwrap()` in a file at its ceiling; a `.to_string().starts_with("429")` in a provider; a `UtxoVerdict::Spent` in `wallet/`) each makes exactly its test red naming the line; revert each; the tails in the report.

Bounds: no change to `src/` beyond the test module and its `mod` line; no `Cargo.toml` edit. Six hours.
