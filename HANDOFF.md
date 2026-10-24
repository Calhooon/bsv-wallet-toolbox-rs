# HANDOFF: captain-bsv-wallet-toolbox-rs

The captain's record of the north-star programme (epic #80). Re-orient from this file and `cca runs --project bsv-wallet-toolbox-rs --open` after any restart. The lane briefs are under `docs/lanes/`; the rules every lane assumes are `docs/lanes/README.md`.

## The lines (2026-10-23)

- `main`: the stable line for quick fixes. v0.13.0 at e9857b8 is the newest tag (the owner's, 2026-10-23). Since then on main: the mock spender setter (#84), the #96 spender verdicts (#101, a34c3c8) and the mutation nightly's file (#154, c5ad88d); the 0.13.1 tag is the owner's.
- `beta`: the programme's line, cut from `main` at e9857b8 on 2026-10-23 and protected like `main` (strict checks: Test Suite (ubuntu-latest, stable), Clippy, Rustfmt, vectors, locks; admins enforced; linear history; no force push; no deletion). Every programme PR targets `beta`. CI runs on both lines (#52).
- A tag of any kind, a public API shape, the schema, a pin, a spend: the owner's word. The stack keeper (captain-bsv-stack-lean) holds the charters and the line.
- The CLI (captain-bsv-wallet-cli) consumes #66, #67, #68 and #70; the shapes are agreed by issue comment before coding. The CLI's beta is the canary for every beta prerelease.
- A `Closes #N` fires only on a merge into `main`; an issue landed on `beta` is closed by hand with a comment naming the PR and the beta commit.

## Wave 1 (launched 2026-10-23, Phase 0, the oracle): landed

Every oracle PR is on beta as of 2026-10-24 afternoon, each after a cold review said merge (or fix, applied and read again):

| issue | PR | what landed | beta |
|---|---|---|---|
| #52 | #81 | the two lines, CI on both, RELEASING | 6317222 |
| the CLI's | #84, #88 | mock spender setter, on main and synced | ef082b4 |
| #53 door | #87 | door fixtures, `cargo test --test fixtures`; door bug #99 | |
| #137 | #137 | HANDOFF at the reviews-queued milestone | 36e34b8 |
| #138 (the CLI's) | #139 | `customInstructions` at most 1,000 bytes at the internalize door, `Error::ValidationError` before anything is stored; wallet-door and `StorageClient` half is #143 | df0d7fb |
| #105 (the CLI's) | #107 | change pick ends on `o.output_id ASC`, no `LENGTH(input_beef)`; below-target tie is `desc` in the reference, #108 | 40cabb9 |
| #57 | #109 | mutants: `.cargo/mutants.toml`, 16-shard nightly, `tools/mutants-score`, `tools/mutants-floor.json`, `docs/MUTANTS.md`; floors below 80 are #110 to #133, surviving door mutants #134 to #136 | 6f34907 |
| #96 (the CLI's) | #101 into main, #145 sync | unreadable named spender is `(Unknown, None)`; bytes without a header service is `(SpentHint, Some{confirmed:false})` | 33870d5 |
| #59 | #85 | dependents check reads every target; semver job fails any break unless the owner's `api-break` label; `pull_request` runs on labeled and unlabeled; judgement items #150 | 68f0002 |
| #60 | #82 | `docs/NORTH-STAR.md` scoreboard and its CI job | 39e1566 |
| #54 | #86 | criterion benches of the door behind `bench-support`; `benches/README.md`; bench CI job; the Linux profile is still to record | e10b35e |
| #56 | #83 | architecture tests (`src/architecture_tests.rs`, `scan.rs`): boundary rules, exact-count ceilings, `impl_headers`; aliases and renames are #151 | 934a668 |
| #53 ladders | #91 | 90 ladder fixtures under `cargo test --lib ladder_fixtures`; POST bodies matched; Retry-After remainders of later calls masked; the rate gates read an injectable `Clock` the replay holds still (test-only), with a test naming the wall clock as production's; #92, #93 | a958b41 |
| #153 | #154 into main, #155 sync | the nightly's file lives on main (GitHub schedules only the default branch and registers no workflow that lives only on beta); a schedule checks out beta, a dispatch its sha | |
| #55 | #90 | the ratchet (`tools/ratchet`, `tools/ratchet.json`, toolchain pin 1.98.1 as the instrument); baseline re-recorded at the landing tree; the file carries `measured_at` and a flat `metrics` object the scoreboard reads (four cells of `docs/NORTH-STAR.md`); #146 follow-ups | 00c2e94 |

Not landed: `oracle-58-fuzz` (#58) was stopped by the Codex provider's classifier at its opening turn; per cca rule 18 the owner names its next model. `hot-103-internalize-verify` (#103, PR #104 red half at ec0e83b) waits on the owner's word for `Error::SpendRefused { offset, txid, input, why }` (the keeper asked him by name); the brief is `$TMPDIR/toolbox-lanes/hot-103-internalize-verify.md`.

What landing taught (2026-10-24):

- A ceiling or baseline recorded by a lane drifts while its PR waits: beta gained unwrap sites (#96's setter in `mock.rs`; #86's bench seam opened `with_base_url` to the feature in `bitails.rs` and `whatsonchain.rs`), and the architecture test's exact-count ceilings and the ratchet's baseline both had to be re-recorded at the landing tree in a captain commit that says why. Land the measuring PRs last, and re-record once.
- A behaviour change on one line moves fixtures on the other: #96's sync changed two spender scenes (`404_spender_bytes_not_held` is `Unknown`; with no header service the probe still asks `/spent`, so the scene scripts that route as 404). Re-bless in its own commit with the issue's words.
- Fixtures that read the wall clock flake on slow runners (the instrumented coverage job took 216 s for the lib tests): a remainder of seconds, and a one-second default window. Mask the remainder of later calls; hold the gate's clock in the replay. Both fixes are in `tests/fixtures/ladders.rs` and `src/services/providers/pace.rs`.
- Two lanes that ran side by side can disagree on a file's shape: #60's scoreboard writer read `measured_at` and a flat `metrics` object from `tools/ratchet.json`, which #55's ratchet never wrote; the ratchet now writes both (the gate ignores them) and the writer's test edits whichever measured cell the document has, not one literal day and number. When one lane consumes another's file, the brief names the shape or the consumer tolerates absence.
- rustdoc under `-Dwarnings` refuses an intra-doc link to a private item from a public module's doc; name it in plain code.
- A `cfg(test)` constructor a `cfg(any(test, feature))` seam calls does not build under the feature; both gates must match.
- `mergeStateStatus UNSTABLE` while checks run means pending, not failed; the landing script (`scratchpad/land.sh`) waits until nothing is in progress and skips only on a `FAILURE` conclusion. The script rebases onto `origin/beta` in a scratch worktree, keeps both sides of a CHANGELOG conflict, pushes with `--force-with-lease`, and merges with `gh pr merge N --rebase` on `CLEAN`; any other conflict stops it for the captain.
- `gh pr merge --delete-branch` fails on a detached HEAD; delete the remote branch by hand. `--base` on a `cca run` must name a ref that exists locally (`origin/lane/...`).

Hazards found by lanes: (1) lanes that share `CARGO_TARGET_DIR=~/bsv/targets/<project>-lanes` write the same test binary name, and one lane ran a sibling's binary with zero of its own tests and exited 0; every brief now gives the lane its own target subfolder and asks for the printed `test result:` lines (a cca rule-23 matter for the owner); (2) the lane machines' default macOS SDK fails to link sqlx-macros under Xcode 26.3's linker; every lane sets `SDKROOT=$(xcrun --sdk macosx --show-sdk-path)`; (3) `${{ runner.temp }}` in a job-level `env` fails a workflow at parse time; use `$GITHUB_ENV` in a step; (4) a lane whose base moved under it shows the base's new files as deletions; rebase onto `origin/beta` before pushing.

Lessons of the wave: a lane that arms a background job and ends its turn leaves no report (three did); every brief says foreground only, REPORT last. A review brief built with a base ref containing a slash broke the substitution; `|` is the delimiter now. Briefs are built from `git show origin/beta:docs/lanes/README.md` plus the lane's own text. Reviews run on a different model than the code lane (code on Opus, review on Fable or Codex), per the epic's "a different model reads the diff cold"; reviews need Fable points, which refilled during 2026-10-24. `gh issue create --label X` creates nothing when the label does not exist; create the label before a batch. The box refuses every lane with "no claude account on box has room" although `cca status` lists those accounts there (a cca fault shown to the owner); lanes run on the Studio.

Required contexts: `fixtures`, `ratchet`, `scoreboard`, `semver`, `dependents` and `bench` become required after a week green; `Nightly mutants` never. The nightly's 16 shards take every runner slot the repository has, so a dispatch during a landing run queues every CI run behind it for hours (the one-off against 934a668 was cancelled for that); dispatch it only when nothing is landing, and read the door floors from that run (#110 to #136 depend on the reading). Four stale `cli44-toolbox-review` records sit blocked with no account from an earlier wave; not this programme's.

## Next (Phase 1 and the owner's window)

- The owner's: #58's model (cca rule 18); `Error::SpendRefused` for #103; #66 questions 5 to 9; #106's constructor shape (after #66); the 0.13.1 tag; the bsv-rs line.
- The bsv-rs line: bsv-rs beta is 0.9.0 (#87 typed transport fault at 4dc505f; #117 `validate_internalize_action_args` in `src/wallet/validation/internalize.rs`). The toolbox can pin only a published version, so the one word is 0.9.0-beta.1 on the index as `bsv-rs = "=0.9.0-beta.1"`; until then lanes read bsv-rs beta for design and fixtures only, nothing in `Cargo.toml`. Then: #63/#140 (typed faults, wildcard arms under `#[non_exhaustive]`, the one text site at `storage_client.rs:519-527`, #144), #143 (the door calls bsv-rs's validator and wraps it as `Error::ValidationError`), #149 (BRC-56 key rule half), #152 (BRCs pin citation moves with the next release's docs).
- Phase 1 lanes from the epic (#61 to #70) in order once the owner's words are in; #66's design is `docs/design/66-door-split.md`.
- Record the Linux bench profile; confirm the door floors from the nightly; the ratchet's follow-ups (#146).

## Open with the CLI's captain

- The CLI's differential floor read zero against beta 00c2e94 (2026-10-24 21:36Z: two pairs, 335 calls and 35 commands each, 0 differences; evidence `/Volumes/Lanes/out/bsv-wallet-cli/diff-00c2e94/`), as it did at 40cabb9. The CLI's `refactor` hard-fail (CLI #196) and its side of the customInstructions bound (CLI #157) wait for the 0.13.1 pin that carries #105 and #138; CLI #198 and #199 assumed a bsv-rs 0.8.3 tag and are reread against 0.9.0-beta.1.
- #138 landed (#139): the door's refusal is `Validation error: customInstructions length must be at most 1000 bytes, got N`; the CLI maps it to 400 `VALIDATION_ERROR`. The wallet-door half is #143.
- #66's shape: the CLI comments with its handler's call sequence; the toolbox answers with the draft API on the issue; the stack's word before landing (`stack-charter`).
- #68's shape: reader methods or versioned views, by issue comment.
- #67: the keeper's `conformance/wallet-gate-vectors.json` (schema wallet-gate-vectors/1, 490 rows) is recorded on the issue; the replay lane follows the oracle.

## The stack's bindings (the keeper's messages of 2026-10-23 and 2026-10-24)

- The wallet machine's definition is bsv-stack-lean `docs/charters/wallet.md` with `lean/Wallet.lean`; a transition, a word, the evidence rule or a pin changes only through the keeper (captain-bsv-stack-lean), by message or an issue on bsv-stack-lean; wait for the word.
- The reorg rule is root-based (ruling 5 of 10-20; `src/monitor/reorg_ops.rs`, `docs/REORG-DIVERGENCES.md`) and does not move. Rule 28 (`docs/p0/rule-28-explorer-calls.md`) bounds every explorer call; T21 names any new one a finding.
- The line of record is `~/bsv/epoch/LINE.md`; the captains' channel is `~/bsv/epoch/IDENTITY-PROGRAM-2026-10.md` (one line per captain per round, by pathspec).
- The branches `lane/toolbox-44-spender` and `lane/toolbox-47-header-ladder` are the stack's; they stay until the keeper removes them.
- #67 replays the wallet gate's vectors that stack #184 exports into the conformance directory; the keeper sends the row shape when that lane reports.
- The stack's scenario for the customInstructions bound names toolbox #143 as the door's word, with #138 as the reading issue it cites.
- The BRCs pin of record moved ed1b015 to 5119a9b (#152): the toolbox cites the old pin once, in a historical CHANGELOG line; it moves with the next release, never ahead of the line.
- The wallet column of the stack's `docs/MATRIX.md` names the toolbox's open gaps at v0.12.1: two instances of one key, the storage split, the pending prompt, the signature window, the race rule's explorer read.
- 0.13.0 is on crates.io (2026-10-23 21:54Z); the Release run was green.
