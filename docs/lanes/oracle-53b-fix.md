
# oracle-53b-fix (#53, the ladders half, the cold review's fix)

Repository: bsv-wallet-toolbox-rs. Your worktree is on branch `lane/oracle-53b-fix`, cut from `lane/oracle-53b-ladder-fixtures` (seven commits past `origin/beta`: the ladder fixtures harness `tests/fixtures/ladders.rs`, its driver `src/services/ladder_fixtures_tests.rs`, 90 fixtures under `tests/fixtures/data/ladders/`). Read the harness whole and one fixture per ladder.

The cold review's one finding (P2): `tests/fixtures/ladders.rs:310` matches a provider request by method and path only and ignores the POST body, so a broken status request passes. The reviewer renamed the request key `txids` to `wrong_txids` in the code path under test and all 90 fixtures still replayed green.

Fix, one commit: record the POST body (as parsed JSON, normalized: keys sorted, txids replaced by the same tokens the fixture uses elsewhere) in each fixture that has one (`get_status_for_txids` and any other POST route; ARC's broadcast is not in these ladders) and match it in the replay; a request whose body differs from the recorded one is a mismatch that fails the fixture naming the route and the differing key. Re-bless the fixtures (`UPDATE` mode as the harness names it) so the recorded bodies land, in the same commit; show that the only diff in the fixture files is the added body fields. Proof: the reviewer's plant (`txids` -> `wrong_txids`) now fails the replay; revert; green. Two bless runs byte-identical.

Gate: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib` (the replay runs inside it as `ladder_fixtures`).

Bounds: `tests/fixtures/ladders.rs` and the fixture data files; nothing under `src/` beyond the driver if a signature must change. One hour.
