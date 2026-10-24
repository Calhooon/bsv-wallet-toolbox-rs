# HANDOFF: captain-bsv-wallet-toolbox-rs

The captain's record of the north-star programme (epic #80). Re-orient from this file and `cca runs --project bsv-wallet-toolbox-rs --open` after any restart. The lane briefs are under `docs/lanes/`; the rules every lane assumes are `docs/lanes/README.md`.

## The lines (2026-10-23)

- `main`: the stable line for quick fixes. v0.13.0 at e9857b8 is the newest tag (the owner's, 2026-10-23).
- `beta`: the programme's line, cut from `main` at e9857b8 on 2026-10-23 and protected like `main` (strict checks: Test Suite (ubuntu-latest, stable), Clippy, Rustfmt, vectors, locks; admins enforced; linear history; no force push; no deletion). Every programme PR targets `beta`. CI runs on `beta` once #52 lands.
- A tag of any kind, a public API shape, the schema, a pin, a spend: the owner's word. The stack keeper (captain-bsv-stack-lean) holds the charters and the line.
- The CLI (captain-bsv-wallet-cli) consumes #66, #67, #68 and #70; the shapes are agreed by issue comment before coding. The CLI's beta is the canary for every beta prerelease.

## Wave 1 (launched 2026-10-23, Phase 0, the oracle)

| lane | issue | PR | state (2026-10-24 early) |
|---|---|---|---|
| oracle-52-beta-line | #52 | #81 | landed on beta (6317222); #52 closed |
| main-mock-spender-setter | the CLI's ask | #84 | landed on main (ff0be45) and synced into beta by #88; the 0.13.1 tag is the owner's |
| oracle-53a-door-fixtures | #53 | #87 | landed on beta; the finding is a door bug, filed #99 |
| oracle-53b-ladder-fixtures | #53 | #91 | fixed (POST bodies matched; replay under `cargo test --lib ladder_fixtures`); second cold review queued (`reviews/oracle-53b-ladder-fixtures-b.md`); #92, #93 filed |
| oracle-54-benches (owns Cargo.toml) | #54 | #86 | fixed twice (both seams behind the non-default `bench-support` feature; workflow env in `$GITHUB_ENV`); third cold review queued (`reviews/oracle-54-benches-c.md`) |
| oracle-55-ratchet | #55 | #90 | fixed (built-in duplicate detector, baseline 6796; the toolchain pin kept as the instrument); second cold review queued (`reviews/oracle-55-ratchet-b.md`); #89 filed |
| oracle-56-arch-tests | #56 | #83 | fixed twice (`scan.rs` split out, exact-count ceilings); third cold review queued (`reviews/oracle-56-arch-tests-c.md`) |
| oracle-57-mutants | #57 | #109 (draft) | measured by the fourth lane (`oracle-57-mutants-d`, 1,237 runs: providers, tasks and services.rs complete; doors on a 1/8 sample); floors in `tools/mutants-floor.json`, 16-shard nightly, `tools/mutants-score` gate; issues #110 to #133 (one per file below 80) and #134 to #136 (surviving door mutants per door file); cold review queued (`reviews/oracle-57-mutants-d.md`). The door floors are provisional until the nightly's full run |
| oracle-58-fuzz | #58 | none | stopped by the Codex provider's classifier at its opening turn; per cca rule 18 the owner names its next model |
| oracle-59-dependents | #59 | #85 | fixed (`--release-type minor`, `api-break` label as the owner's word, re-pin plans in #94); second cold review queued (`reviews/oracle-59-dependents-b.md`) |
| oracle-60-north-star | #60 | #82 | fixed (scoreboard job calls the writer; self-test); second cold review queued (`reviews/oracle-60-north-star-c.md`) |
| main-96-spender-verdicts | #96 (the CLI's) | #101 into main | fixed per the keeper's word (unreadable named spender is `(Unknown, None)`; bytes without a header service is `(SpentHint, Some{confirmed:false})`); cold review queued (`reviews/main-96-spender-verdicts.md`); after it lands, a cherry-pick sync into beta with ladder fixtures re-blessed; tell the CLI |
| hot-103-internalize-verify | #103 | #104 (draft, red half) | the keeper's words are on the issue (verify every unproven input's script with bsv-rs `verify_stream_async`; hand the verifier the tip from the header service so `Unverifiable` never reaches the door; a header fault is a 503 hold, nothing stored). Blocked on the owner's word for `Error::SpendRefused { offset, txid, input, why }`; brief at `$TMPDIR/toolbox-lanes/hot-103-internalize-verify.md`, relaunch from tip ec0e83b |
| hot-105-change-tiebreak | #105 (the CLI's) | #107 (draft) | done: the change pick drops the `LENGTH(input_beef)` order and ends on `o.output_id ASC`; red test in `src/storage/sqlx/change_tiebreak_tests.rs`; cold review queued (`reviews/hot-105-change-tiebreak.md`); #108 filed (below-target tie is `desc` in the reference) |

Also this wave: the records (#97), HANDOFF (#100) and briefs (#102) landed, so `docs/lanes/` is on beta; `docs/design/66-door-split.md` (PR #95) carries the #66 shape with the keeper's four readings as section 9, the owner's questions 5 to 9 open on the issue; the keeper's gate vectors for #67 are recorded on that issue; #106 (a seedable RNG behind a constructor option, the CLI's) is inventoried on the issue and waits on the owner's word for the shape, placed right after #66.

Blockers at 2026-10-24 01:00: every pool account's Fable points are at zero, so no review lane launches (reviews run on Fable by cca's picker; the owner may say `--model opus` on a launch line, or the points refill in about twelve hours); eight PRs wait on those reviews (#82, #83, #85, #86, #90, #91, #107, #109 into beta; #101 into main). The box refuses every lane with "no claude account on box has room" although `cca status` lists those accounts signed in there (a cca fault shown to the owner); lanes run on the Studio with `--host studio`. The Studio's internal disk was cleared to about 64 GB free (caches, a local snapshot, four toolchains); `~/.cargo` and `~/.rustup` can move to the lane volume with symlinks when no cargo runs. Both Codex accounts are at 0 percent of the week.

Hazards found by lanes: (1) lanes that share `CARGO_TARGET_DIR=~/bsv/targets/<project>-lanes` write the same test binary name, and one lane ran a sibling's binary with zero of its own tests and exited 0; every brief now gives the lane its own target subfolder and asks for the printed `test result:` lines (a cca rule-23 matter for the owner); (2) the lane machines' default macOS SDK fails to link sqlx-macros under Xcode 26.3's linker; every lane sets `SDKROOT=$(xcrun --sdk macosx --show-sdk-path)`; (3) `${{ runner.temp }}` in a job-level `env` fails a workflow at parse time; use `$GITHUB_ENV` in a step; (4) a lane whose base moved under it shows the base's new files as deletions; rebase onto `origin/beta` before pushing (57-d did).

Lessons of the wave: a lane that arms a background job and ends its turn leaves no report (three did); every brief says foreground only, REPORT last. A review brief built with a base ref containing a slash broke the substitution; `|` is the delimiter now. Briefs are built from `git show origin/beta:docs/lanes/README.md` plus the lane's own text. Reviews run on a different model than the code lane (code on Opus, review on Fable or Codex), per the epic's "a different model reads the diff cold". `gh issue create --label X` creates nothing when the label does not exist; create the label before a batch.

Landing order: one PR at a time after its cold review says merge, rebased onto `origin/beta` (or `main` for #101) and `gh pr merge N --rebase --delete-branch` on `mergeStateStatus CLEAN`; `Nightly mutants` is never a required context; `fixtures`, `ratchet`, `scoreboard`, `semver`, `dependents` and `bench` become required contexts after a week green. Four stale `cli44-toolbox-review` records sit blocked with no account from an earlier wave; not this programme's.

## Open with the CLI's captain

- The mock setter landed on main (#84) and beta (#88); the CLI has it. #96's fix (#101) lands on main after review; tell the CLI the tag is the owner's.
- #105 landed on beta tells the CLI to re-run its differential floor on the beta tip; #106's shape is the owner's and goes right after #66.
- #66's shape: the CLI comments with its handler's call sequence; the toolbox answers with the draft API on the issue; the stack's word before landing (`stack-charter`).
- #68's shape: reader methods or versioned views, by issue comment.
- #67: the keeper's `conformance/wallet-gate-vectors.json` (schema wallet-gate-vectors/1, 490 rows) is recorded on the issue; the replay lane follows the oracle.

## The stack's bindings (the keeper's message of 2026-10-23)

- The wallet machine's definition is bsv-stack-lean `docs/charters/wallet.md` with `lean/Wallet.lean`; a transition, a word, the evidence rule or a pin changes only through the keeper (captain-bsv-stack-lean), by message or an issue on bsv-stack-lean; wait for the word.
- The reorg rule is root-based (ruling 5 of 10-20; `src/monitor/reorg_ops.rs`, `docs/REORG-DIVERGENCES.md`) and does not move. Rule 28 (`docs/p0/rule-28-explorer-calls.md`) bounds every explorer call; T21 names any new one a finding.
- The line of record is `~/bsv/epoch/LINE.md`; the captains' channel is `~/bsv/epoch/IDENTITY-PROGRAM-2026-10.md` (one line per captain per round, by pathspec).
- The branches `lane/toolbox-44-spender` and `lane/toolbox-47-header-ladder` are the stack's; they stay until the keeper removes them.
- #67 replays the wallet gate's vectors that stack #184 exports into the conformance directory; the keeper sends the row shape when that lane reports.
- The wallet column of the stack's `docs/MATRIX.md` names the toolbox's open gaps at v0.12.1: two instances of one key, the storage split, the pending prompt, the signature window, the race rule's explorer read.
- 0.13.0 is on crates.io (2026-10-23 21:54Z); the Release run was green.
