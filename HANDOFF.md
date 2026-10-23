# HANDOFF: captain-bsv-wallet-toolbox-rs

The captain's record of the north-star programme (epic #80). Re-orient from this file and `cca runs --project bsv-wallet-toolbox-rs --open` after any restart. The lane briefs are under `docs/lanes/`; the rules every lane assumes are `docs/lanes/README.md`.

## The lines (2026-10-23)

- `main`: the stable line for quick fixes. v0.13.0 at e9857b8 is the newest tag (the owner's, 2026-10-23).
- `beta`: the programme's line, cut from `main` at e9857b8 on 2026-10-23 and protected like `main` (strict checks: Test Suite (ubuntu-latest, stable), Clippy, Rustfmt, vectors, locks; admins enforced; linear history; no force push; no deletion). Every programme PR targets `beta`. CI runs on `beta` once #52 lands.
- A tag of any kind, a public API shape, the schema, a pin, a spend: the owner's word. The stack keeper (captain-bsv-stack-lean) holds the charters and the line.
- The CLI (captain-bsv-wallet-cli) consumes #66, #67, #68 and #70; the shapes are agreed by issue comment before coding. The CLI's beta is the canary for every beta prerelease.

## Wave 1 (launched 2026-10-23, Phase 0, the oracle)

| lane | issue | base | state |
|---|---|---|---|
| oracle-52-beta-line | #52 | origin/beta | running |
| oracle-53a-door-fixtures | #53 | origin/beta | running |
| oracle-53b-ladder-fixtures | #53 | origin/beta | running |
| oracle-54-benches (owns Cargo.toml) | #54 | origin/beta | running |
| oracle-55-ratchet | #55 | origin/beta | running |
| oracle-56-arch-tests | #56 | origin/beta | running |
| oracle-57-mutants | #57 | origin/beta | running |
| oracle-58-fuzz | #58 | origin/beta | stopped by the Codex provider's classifier at its first turn (openai-2, box); per cca rule 18 it is recorded, not retried; the owner names its next model |
| oracle-59-dependents | #59 | origin/beta | running |
| oracle-60-north-star | #60 | origin/beta | running |
| main-mock-spender-setter | the CLI's ask | origin/main | running; a quick fix on main; the 0.13.1 version and tag are the owner's |

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
