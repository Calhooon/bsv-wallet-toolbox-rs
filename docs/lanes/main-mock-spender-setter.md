# main-mock-spender-setter (a quick fix on main, for bsv-wallet-cli)

Repository: bsv-wallet-toolbox-rs. Branch `lane/main-mock-spender-setter` from `origin/main` (this is a quick fix on the stable line, not programme work). Rules: `docs/lanes/README.md` on branch `beta` (`git show origin/beta:docs/lanes/README.md` once the captain lands it; until then the same text is this brief's parent folder), with `main` in place of `beta`. Read: `CLAUDE.md`, `src/services/CLAUDE.md`, `src/services/mock.rs` (the `is_utxo_response` builder, the `set_` variants, `call_history`), `src/services/traits.rs` (`is_utxo_with_spender` and its default body), the CHANGELOG's 0.13.0 entry.

The ask, from the CLI's captain: `MockWalletServices` has no setter for `is_utxo_with_spender`; the trait's default answers `is_utxo`'s verdict and `None`, so a host cannot test a bound spender against the mock.

The work, one commit of behaviour and one of CHANGELOG:
- `pub fn is_utxo_with_spender_response(mut self, response: MockResponse<(UtxoVerdict, Option<BoundSpender>)>) -> Self` in the builder style of `is_utxo_response`, and the `set_is_utxo_with_spender_response` variant in the style of the existing `set_` methods; the mock's `is_utxo_with_spender` records its call in `call_history` under its own name and answers the set response when one is set; when none is set, it answers as today (`is_utxo`'s response and `None`) so no existing test moves.
- Tests beside the mock's: set and answered with a bound spender; unset falls back to `is_utxo`'s response and `None`; the call recorded in `call_history`.
- CHANGELOG: `## [Unreleased]` above 0.13.0, an `### Added` line in the file's voice. Do not change `Cargo.toml`'s version (the 0.13.1 tag and version are the owner's).

The proof: the new tests red before the change (write them against the trait's default and show the failure), green after; `cargo test --lib`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`; tails in the report.

Bounds: `src/services/mock.rs`, its tests, `CHANGELOG.md`; nothing else. Two hours.
