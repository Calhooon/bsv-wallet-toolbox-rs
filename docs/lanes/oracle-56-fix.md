
# oracle-56-fix (#56, the cold review's fixes)

Repository: bsv-wallet-toolbox-rs. Your worktree is on branch `lane/oracle-56-fix`, cut from `lane/oracle-56-arch-tests` (one commit past `origin/beta`: `src/architecture_tests.rs`, 783 lines, and its `mod` line in `lib.rs`). Read `src/architecture_tests.rs` whole, `src/services/rule_28_tests.rs`, and issue #56 (`gh issue view 56`).

The cold review's findings, each shown by a plant that the test did not catch. Fix them in `src/architecture_tests.rs`, one commit per finding, each with its own planted-violation proof (plant, run `cargo test --lib architecture_tests`, show the failure naming the line, revert):
1. (must) Slack in a ceiling never fails: `enforce` prints "lower this ceiling" when count < ceiling and asserts nothing. Assert `count == ceiling` for every listed file, so a removed site forces the table down and a ceiling cannot carry slack. Proof: raise one ceiling by one and show the test red.
2. (must) The HTTP rule matches only the token pair `reqwest ::`, so `use reqwest as x;` plus `x::Client::new()` evades it. Count the bare `reqwest` identifier (any `use reqwest`, `reqwest::`, `reqwest as`) outside the boundary. Proof: the aliased plant in `src/wallet/wallet.rs`.
3. (must) `std::env::var_os` passes the env rule; treat `var`, `var_os`, `vars`, `vars_os` alike. Proof: a `var_os` plant.
4. (must) SQL is matched by an uppercase leading keyword only; match case-insensitively (`select`, `insert`, `update`, `delete`, `create`, `pragma`, `with`). Proof: a lowercase `"select 1"` plant.
5. (must) `matches!(e.to_string().as_str(), "busy" | "locked")` passes the error-text rule. Add `matches!` and `match` on a `.to_string()` or message expression to the rule (a textual check: `to_string()` or `.message` within a `match`/`matches!` scrutinee). Proof: that plant.
6. (should) Lowercase constructors (`BroadcastStatus::parse`, `from_status`, `from_arc_status`) are not seen by the verdict rule while `UtxoVerdict::Spent` is. Decide and document in the test: the rule is about constructing a verdict *value* outside its module; an associated function call that returns one is a construction too. Either count them (and set the ceilings to today's counts) or write the rule's words in a comment saying why not. Proof or a comment, your call; say which.
7. (should) A path-qualified return type `-> crate::services::traits::UtxoVerdict` is counted as a construction; exclude a type position after `->` through path segments.
Keep the file under 800 lines; if a fix pushes it over, split the helpers into `src/architecture_tests/scan.rs` (a `#[cfg(test)]` submodule), a refactor commit by itself.

Gate: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`. Every ceiling in the table must equal the count the fixed scanner gives (update the table in the same commit as the scanner change that moves it, and say which numbers moved and why).

Bounds: `src/architecture_tests.rs` (and a `scan.rs` submodule if split), nothing else. Three hours.
