# HANDOFF: captain-bsv-wallet-toolbox-rs

The captain's record of the north-star programme (epic #80). Re-orient from this file and `cca runs --project bsv-wallet-toolbox-rs --open` after any restart. The lane briefs are under `docs/lanes/`; the rules every lane assumes are `docs/lanes/README.md`.

## The lines (2026-10-23)

- `main`: the stable line for quick fixes. v0.13.0 at e9857b8 is the newest tag (the owner's, 2026-10-23).
- `beta`: the programme's line, cut from `main` at e9857b8 on 2026-10-23 and protected like `main` (strict checks: Test Suite (ubuntu-latest, stable), Clippy, Rustfmt, vectors, locks; admins enforced; linear history; no force push; no deletion). Every programme PR targets `beta`. CI runs on `beta` once #52 lands.
- A tag of any kind, a public API shape, the schema, a pin, a spend: the owner's word. The stack keeper (captain-bsv-stack-lean) holds the charters and the line.
- The CLI (captain-bsv-wallet-cli) consumes #66, #67, #68 and #70; the shapes are agreed by issue comment before coding. The CLI's beta is the canary for every beta prerelease.

## Wave 1 (launched 2026-10-23, Phase 0, the oracle)

| lane | issue | PR | state (2026-10-23 late) |
|---|---|---|---|
| oracle-52-beta-line | #52 | #81 | landed on beta (rebase merge, tip 6317222); review on Codex: merge, no findings; #52 closed |
| main-mock-spender-setter | the CLI's ask | #84 | landed on main (ff0be45); review on Codex: merge; synced into beta by #88; the 0.13.1 tag is the owner's |
| oracle-53a-door-fixtures | #53 | #87 | draft; review on Claude in flight; finding to reproduce: list_outputs offset 99 reports total 99 |
| oracle-53b-ladder-fixtures | #53 | #91 | draft; review on Codex in flight; replay is under `cargo test --lib ladder_fixtures` because a mock-base `Services` constructor is a public shape (owner's word); findings filed #92, #93 |
| oracle-54-benches (owns Cargo.toml) | #54 | #86 | draft; its ci.yml is invalid (`runner.temp` in a job-level env), fix lane `oracle-54-fix` running; review relaunch after the fix |
| oracle-55-ratchet | #55 | #90 | draft; review on Codex in flight; finding filed #89 (src/utils.rs unreachable) |
| oracle-56-arch-tests | #56 | #83 | draft; review on Claude: fix (ceilings never fail on slack; aliased reqwest, var_os, lowercase SQL, matches! evade); fix lane `oracle-56-fix` running |
| oracle-57-mutants | #57 | none | the lane ended with no commits and no report; relaunched as `oracle-57-mutants-b` |
| oracle-58-fuzz | #58 | none | stopped by the Codex provider's classifier at its first turn; per cca rule 18 the owner names its next model |
| oracle-59-dependents | #59 | #85 | draft; review on Codex: fix (semver job let a break through at 0.14.0; integration tests not run; silent table read; a forbidden word); fix lane `oracle-59-fix` running; dolphinmilk stays out of CI by the captain's decision, tracked in #94 |
| oracle-60-north-star | #60 | #82 | draft; its review lane ended with no report; relaunched as `oracle-60-review-b` |

Lessons of the wave: a lane that arms a background job and ends its turn leaves no report (three did); every brief now says foreground only, REPORT last. A review brief built with a base ref containing a slash broke the substitution; `|` is the delimiter now. The fix briefs for 54 and 56 went out without the shared rules (the checkout was on main, where docs/lanes does not exist); briefs are built from `git show captain/wave-1-records:docs/lanes/README.md`. Reviews run on the other provider than the code lane (Claude code, Codex review, and the reverse), per the epic's "a different model reads the diff cold".

Landing order: #52 lands first (it gives `beta` its CI); then the rest one at a time after a cold review lane (the diff and the issue only, never the lane's report); `tests/fixtures/main.rs` from 53a and 53b is merged by hand (two `mod` lines). Four stale `cli44-toolbox-review` records sit blocked with no account from an earlier wave; not this programme's.

## Open with the CLI's captain

- The mock setter (`is_utxo_with_spender_response` on `MockWalletServices`): in a lane; tell the CLI when it lands.
- #66's shape: the CLI comments with its handler's call sequence; the toolbox answers with the draft API on the issue; the stack's word before landing (`stack-charter`).
- #68's shape: reader methods or versioned views, by issue comment.
- #67 waits for the stack's #184 vectors.

## The stack's bindings (the keeper's message of 2026-10-23)

- The wallet machine's definition is bsv-stack-lean `docs/charters/wallet.md` with `lean/Wallet.lean`; a transition, a word, the evidence rule or a pin changes only through the keeper (captain-bsv-stack-lean), by message or an issue on bsv-stack-lean; wait for the word.
- The reorg rule is root-based (ruling 5 of 10-20; `src/monitor/reorg_ops.rs`, `docs/REORG-DIVERGENCES.md`) and does not move. Rule 28 (`docs/p0/rule-28-explorer-calls.md`) bounds every explorer call; T21 names any new one a finding.
- The line of record is `~/bsv/epoch/LINE.md`; the captains' channel is `~/bsv/epoch/IDENTITY-PROGRAM-2026-10.md` (one line per captain per round, by pathspec).
- The branches `lane/toolbox-44-spender` and `lane/toolbox-47-header-ladder` are the stack's; they stay until the keeper removes them.
- #67 replays the wallet gate's vectors that stack #184 exports into the conformance directory; the keeper sends the row shape when that lane reports.
- The wallet column of the stack's `docs/MATRIX.md` names the toolbox's open gaps at v0.12.1: two instances of one key, the storage split, the pending prompt, the signature window, the race rule's explorer read.
- 0.13.0 is on crates.io (2026-10-23 21:54Z); the Release run was green.
