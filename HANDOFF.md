# HANDOFF: captain-bsv-wallet-toolbox-rs

The captain's record of the north-star programme (epic #80). Re-orient from this file and `cca runs --project bsv-wallet-toolbox-rs --open` after any restart. The lane briefs are under `docs/lanes/`; the rules every lane assumes are `docs/lanes/README.md`.

## The lines (2026-10-23)

- `main`: the stable line for quick fixes. v0.13.0 at e9857b8 is the newest tag (the owner's, 2026-10-23). Main is at a01c550 with version 0.13.1 and its CHANGELOG heading (PR #160): the mock spender setter (#84), the #96 spender verdicts (#101), the #161 review-pass fix (#164) and the mutation nightly's file (#154, #157). The `v0.13.1` tag and the crates.io publish are the owner's; bsv-wallet-cli #196 and #157 wait on them. Every fix on main reaches beta by a sync PR (#155, #158, #178, #179), never the other way until a release.
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

## Wave 2 (2026-10-25, the owner's green light): landed and in flight

The owner's words of 2026-10-25, by message to the captain ("green light to make decisions based on north star, keeping it stable, the ecosystem's names"): the captain takes the shape decisions and records each as the owner's word on its issue; tags, publishes and pins stay his explicit word.

| issue | PR | what landed | beta |
|---|---|---|---|
| #153 | #157 into main, #158 sync | the mutation nightly's schedule off (sixteen shards at 2.8 minutes a mutant ran past the 330-minute limit every night, no scores, every repository's CI queued behind them); dispatch only; #159 reshapes it | 7e95a26 |
| #66 design | #95 | `docs/design/66-door-split.md` sections 9 and 10: the keeper's readings and the owner's words (the reference's division, no new verb; no `Signed` type; the lock released in its own benched commit; D3's default); two gaps for the implementing lane (every output's `spendable` under noSend; the three words that move to the post call) | 4a62914 |
| #64 | #162 | `docs/ARCHITECTURE.md` (134 invariant rows, 130 with a test proved by `tools/arch-doc-tests.sh`), six code-vs-charter divergences filed as #165 to #170 with the keeper's readings, three missing tests #171 to #173 | d3a33ac |
| #61 | #174 | `create_action.rs` (11,474 lines) split into `src/storage/sqlx/create_action/` (fourteen modules, largest 674); a pure move proven by token bag, literals, SQL and test names; mutants glob and fourteen provisional floors follow; ratchet re-recorded | 2b291bb |
| #62 | #176 | `storage_sqlx.rs` (9,955 lines) split into `src/storage/sqlx/storage_sqlx/` (37 files, largest 686), every public path unchanged (698 rustdoc paths identical); twelve pedantic entries newly visible because `#[async_trait]` had hidden seven bodies from clippy, re-recorded and explained, no allow added | c9f3afa |

In flight at the time of writing (each with its PR; the cold reviews wait on the pool's Fable allowance, spent for this window):

- #103 landed at beta 603cecb (PR #163) after the CLI's captain moved its test trackers onto a mining tip on its main (CLI PRs 246 and 251, the second making three tests version-agnostic), so the dependents gate read green and no label was needed. The internalize door runs bsv-rs's spend verifier before anything is stored; `Error::SpendRefused { offset, txid, input, why }` (the owner's shape; `why` bsv-rs's `SpendRefusal`, rendered by a private function until the 0.9.0-beta.1 pin brings bsv-rs's `Display`, #213); the tip handed through `Headers::mining_tip`; a header fault or a missing tip holds. The CHANGELOG tells hosts their tracker must answer `mining_tip`; bsv-rs #218 gives its mock one at 0.9.0-beta.1 (no 0.8.4: a private field on a constructible struct is a major on 0.8.x).
- #165 D1 (PR #177, lane `p1-165-abort-refusal-b`): abort and `retire_undeliverable_txid` refuse once the store holds an accepting word (the keeper's amended rule), honour a `sending` row no instance accepted, ask no chain or explorer; the retire half applied by the captain at `storage_sqlx/retirement.rs` after #62. Ratchet green. Awaiting its cold review.
- #161 port (PR #178, lane `port-161-beta`): main's review-pass fix onto the split tree. Awaiting its cold review.
- 0.13.1 heading sync (PR #179): version and release heading on beta by hand, beta's own entries kept under Unreleased.
- #66 implementation (lane `p2-66-door-split`, launched 2026-10-25 afternoon): the post door named for the reference's `processAction`, D3's default flip (#167), the lock released in its own benched commit, section 11 of the design with the reference reads before any code.

Closed today: #53, #54, #55, #56, #57, #59, #60 (Wave 1), #61, #62, #64, #103, #153. Filed today: #159 (reshape the nightly), #161 (review_status voids created outputs), #165 to #170 (divergences), #171 to #173 (missing tests), #175 (sweep of pre-existing failed-beside-spendable rows).

Not landed: `oracle-58-fuzz` (#58) stopped twice: the Codex classifier at its opening turn on 2026-10-24, and on 2026-10-25 a named Claude model that armed a background job and ended its turn without a REPORT, twice; per cca rule 18 the lane is recorded (worktree `oracle-58-fuzz-b`, nine uncommitted files) and the owner names the next model or the brief is rewritten to a fixed foreground budget per target.

What landing taught (2026-10-25):

- A measuring PR that re-records `tools/ratchet.json` must run `python3 tools/scoreboard-write` in the same commit, or the `scoreboard` job fails on table drift (#174).
- The landing script force-pushes a rebased branch, so a lane worktree is behind it afterwards; a follow-up commit pushed plainly is refused and the next landing attempt reads the stale FAILED. Rebase onto `origin/lane/<name>` before every follow-up (#163, #174).
- A captain command chain that pipes a gate through `tail` or `grep` loses its exit status, and a `;` runs the push after a failed gate: once a clippy-failing state and once a non-compiling test module were pushed. Join every step with `&&` and read the gate's own status; never two chains in one worktree.
- The CI runner's stable is Rust 1.99 while the Studio has 1.95: `clippy::chunks_exact_to_as_chunks` and `items_after_test_module` bite only in CI. Run the exact CI command and expect new lints after a runner update.
- The `#[async_trait]` expansion hides function bodies from clippy's `too_many_lines` and `cognitive_complexity`; splitting a trait impl into inherent functions makes them visible and the ratchet rises by that much. Explain and re-record; never add an allow to hide a body.
- The dependents gate (#59) builds the CLI's beta against each PR: a door that newly requires something of a host's `ChainTracker` (here `mining_tip`) fails the CLI's tests before the CLI knows. Tell the CLI's captain the exact requirement with the PR, and name it in the CHANGELOG under a hosts paragraph.
- A fix lane told "not that file" because another lane is splitting it leaves a prepared patch; the captain applies it at the new path after the split lands (#165's retire half, #103's ancestry tests).
- Reviews need Fable points on a pool account; when the pool's allowance is spent, PRs queue reviewed-but-unlanded. The code lanes ran on Codex; a review on a different model would still satisfy the epic's rule, but naming a model is the owner's (cca rule 7).

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

## Next (Phase 2 and the owner's window)

- The owner's explicit words still needed: the `v0.13.1` tag and publish on main a01c550; bsv-rs's `0.9.0-beta.1` (the one pin word, #144; #218's mock tip and #213's `Display` ride with it); the `api-break` label on #181; #58's model or a rewritten brief (cca rule 18).
- Landings waiting on reviews: #177 (D1, the delta re-review), #178 (#161 port), #179 (0.13.1 sync), #180 (this file), #181 (#66's implementation; its dependents gate will read red on one CLI noSend test by design, the owner's `api-break` label or the CLI's pin). Review capacity: two pool accounts hold Fable points but have no Studio login (`cca login calhounjohn.dev@proton.me`, `cca login ceo@calhounjohn.com`).
- After #163 and #66: #169 (D5, the received-coin rule at the door, with the mark on an existing column and the action's labels as the carrier; a new column is the schema word), #106 (the seed field), #167 closed by #66's D3 commit.
- The bsv-rs line: #63/#140 (typed faults, wildcard arms under the `#[non_exhaustive]` `Reason` and `Kind`, `..Default::default()` on the seven key args types, the three removed writer helpers), #143, #149, #152 launch when 0.9.0-beta.1 is on the index; then the renderer in `error.rs` goes for bsv-rs's `Display` (#213).
- The nightly reshaped (#159) so the door floors stop being provisional; the three missing tests #171 to #173; #175's sweep as a 0.13.2 candidate; the Linux bench profile; the ratchet follow-ups (#146).

## Open with the CLI's captain

- The CLI's differential floor read zero against beta 00c2e94 (2026-10-24; evidence `/Volumes/Lanes/out/bsv-wallet-cli/diff-00c2e94/`). CLI #196 and #157 wait on the `v0.13.1` tag; the CLI pins `bsv-wallet-toolbox-rs = "=0.13.0"` and `bsv-rs = "=0.8.2"` exactly, so every pin move here is a coordinated step of three crates.
- #103: the CLI's fixture 103 flips when a published toolbox version carries `Error::SpendRefused` (CLI #140); the CLI classifies it 400 `INVALID_BEEF` with the text `spend refused at offset N, input I of <txid>: <why>`. Its test mocks need `mining_tip` before #163 can land (the dependents gate).
- #66: the division is the reference's; the CLI reads the create and post door shapes when the implementing lane's PR opens, before briefing its #78; its #86 exit test points at the lock-release commit.
- #161 fixed on main (in 0.13.1); CLI #219's gate holds coins by status meanwhile and #175 heals 0.13.0 stores.
- #165: the keeper told the CLI's captain about the retire path; the CLI's `retire_guard.rs` keys on the same accepting word.
- #68's shape: reader methods or versioned views, by issue comment, after #66.

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
- 2026-10-25: the keeper recorded the owner's words on #103, #66 and #106 in the stack's RULINGS by pointer; read the six divergences (#165 to #170): D1, D3, D5 move the code, D2 goes with #67/#70, D4 waits on S4's ruling, D6 is no divergence for the shapes shipped; the wallet charter's hosting row (wallet.md:126 at stack main 85e4f21) cites `docs/ARCHITECTURE.md` at beta d3a33ac. The keeper owes scenario rows beside wallet.md:98 (the abort refusal) and :94 (the received coin) and asks for the beta shas of D1, D3 and D5 at landing, and of #103 for the internalize scenario replay.
