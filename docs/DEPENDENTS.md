# Dependents

The crates that build on `bsv-wallet-toolbox-rs`, read from their manifests at
the branch matching each of this crate's lines (2026-10-26; the CLI's main and beta). Every pin-move PR in a dependent (or here,
when a release moves what a dependent should take) updates its row in the same
PR, so this page stays current.

| crate | repository | pins this crate | how it consumes | captain |
|---|---|---|---|---|
| `bsv-wallet-cli` 0.18.2 | [Calhooon/bsv-wallet-cli](https://github.com/Calhooon/bsv-wallet-cli) (public) | main `=0.13.1` with `bsv-rs` `=0.8.3` (CLI PR 264, 2026-10-26); beta `=0.14.0-beta.3` with `bsv-rs` `=0.9.0-beta.3` and `bsv-tracker` `=0.9.0-beta.3` (CLI PR 345, 6e1a7bc, 2026-10-28 21:11Z; before it `=0.14.0-beta.2` with the beta.2 pair, CLI PR 332, b101d67, 2026-10-28; before it `=0.14.0-beta.1` with the beta.1 pair, CLI PR 274, bb0a149, 2026-10-26); its main never pins a prerelease; renamed `bsv-wallet-toolbox` | features `sqlite`. The crate root re-exports (`Wallet`, `StorageSqlx`, `Services`, `ServicesOptions`, `Chain`, `Monitor`, `Error` and `StoreBusyReason`, the storage traits `WalletStorageProvider`, `WalletStorageWriter`, `MonitorStorage`, the broadcast and status words `BroadcastStatus`, `BROADCAST_*`, `PROVIDER_*`, `classify_broadcast_results`, `is_fatal_status`, `unproven_ancestors_in_beef`); `services` (`mock::MockWalletServices` in its tests, `traits`, `UtxoVerdict`, `providers::{arcade, whatsonchain, BananaBlocks}`); `storage` (`sqlx::refuse_invalid_beef_bytes`, `ProvenTxAnchor`, `StorageProcessActionArgs`); `monitor` (`ArcadeEventsTask`, `reorg_ops`, `tasks::check_for_proofs`) | the stack keeper (the CLI's programme, bsv-wallet-cli #106) |
| `agent-manage` 0.1.0 (`manage/` in the agents repository) | Calgooon/agents (private) | `0.7.7` (locked 0.7.7), renamed `bsv-wallet-toolbox`, with `bsv-rs` `0.5.0` | features `remote`. The crate root (`Wallet`, `Services`, `ServicesOptions`, `Chain`, `StorageClient`, the storage traits `WalletStorageReader`, `WalletStorageWriter`, `WalletStorageSync`, `WalletStorageProvider`, `WalletServices`); `storage` (`StorageClient`, `entities`, the sync arguments `RequestSyncChunkArgs`, `SyncChunk`, `SyncOffsets`, `FindProvenTxReqsArgs`); `services::providers::arc::ArcConfig` | the agents captain |
| `dolphin-milk` 0.1.4 | [Calhooon/dolphinmilk](https://github.com/Calhooon/dolphinmilk) (public) | `0.3.38`, optional, with `bsv-rs` `0.3.4` and `bsv-wallet-cli` `0.1.23`; no committed `Cargo.lock` | behind its default feature `embedded-wallet`, default features of this crate (`sqlite`). The crate root (`Wallet`, `WalletOptions`, `Monitor`, `Services`, `ServicesOptions`, `StorageSqlx`, `Chain`, `WalletStorageWriter`); `services::providers::ArcConfig` | none named |

## The check

`tools/dependents-check.sh` reads the table below, clones each dependent at the
branch named after the line under test when it has one (`--line`; in CI the pull
request's base or the pushed branch: the toolbox's beta builds the CLI's beta, which
is where the CLI pins this crate's prereleases, and main builds the CLI's main; a
dependent without such a branch is cloned at its default with a note only for an
implicit line; an explicit `--line` (including a non-empty `cli_ref`) must name an
existing branch or the row fails without cloning. A probe
that faults rather than answers marks the row failed, #208) into a
scratch directory, rewrites its dependency on this crate
to a path dependency on the tree under test, and runs `cargo build --locked`
(falling back to `cargo build` with a note) and `cargo test --locked --no-fail-fast`
on every target (`cargo test --no-fail-fast` when the build fell back). A row whose test column is not
`default` runs that narrower command instead, and its reason column says why;
a narrowed row is noted in the summary. The dispatched workflow `dependents.yml`
(`gh workflow run Dependents --ref beta|main`) runs the rows marked `yes`, once
before a tag and when the pin lane asks, never on a push run (the owner's word of
2026-10-27); a private repository needs a token the job does not hold, so its
row runs on a laptop with `--from`.
Select a CLI branch with the optional `cli_ref` input: `gh workflow run Dependents --ref beta -f cli_ref=<branch>`; leaving it empty keeps the branch named like this line, including the CLI's main on main.
Every probe or clone fault appears as a summary row, and the job fails for the named reason.

The rows a test run carries (#252). A `default` row's tests run with
`WIRE_TEXT_DIFF=report` in their environment (a dependent that does not read
it ignores it) and `--no-fail-fast`, so every target runs (a narrowed row runs
its own command as written); the output is
printed and captured. Under the knob the CLI's wire replay (bsv-wallet-cli
#290) prints one line `TEXT-DIFF: <fixture file> <expected message> <actual
message>` for a fixture whose replayed refusal differs from it in the message
text alone (status, class, offset, reason and stored state equal), and counts
the fixture as replayed. The gate lifts each such line into a row `text
differs: <fixture file> (pending re-record at the pin)`, which never fails the
job: a refusal's words are this crate's and move with the reference's, so a
text-only difference closes when the dependent re-records at its next pin of
this crate. A difference in anything else fails the dependent's test, and its
row reads `FAIL (<command>): <test>, <test>, ...`, the libtest `test NAME ...
FAILED` lines, eight named and then `and N more`, so a reader tells a
row-level red from a text one without the log. Every libtest shape is named
(#257): a unit or integration test by its path, a doctest by its file, item and
line (`src/lib.rs - module::item (line 3)`), and a `should_panic`,
`compile_fail` or `no_run` test without the mode libtest appends, as its `failures:` list
names it. The marker reaches the gate
only where libtest shows it (a passing test's `println!` and `eprintln!` are
captured). The rows go to the terminal and, in CI, to the job summary;
`tools/dependents-check.sh --parse FILE` reads a captured output alone, and
`tools/dependents-check-test.sh` proves the parsing on fixed output.

What the check found against 0.13.0 on 2026-10-23: `bsv-wallet-cli` at
`main` builds `--locked` and `cargo test --locked` passes on every target
(rerun with every target the same day, at its `5e0dbb0`); `agent-manage` and
`dolphin-milk` do not build, because each pins an older `bsv-rs` (0.5.0 and
0.3.x) whose types this crate's `bsv-rs` 0.8 does not accept. Each joins the CI
job when its captain re-pins `bsv-rs` and this crate (the pin spread is a
scoreboard line of epic #80).

From #144 this crate's beta line takes `bsv-rs` `=0.9.0-beta.1` (the owner's
word): a dependent that builds against it re-pins `bsv-rs` to the same
version, since the two share bsv-rs's types. `bsv-wallet-cli` did so on its
beta at bb0a149 (CLI PR 274, 2026-10-26) together with this crate's
`=0.14.0-beta.1`; its main stays at `=0.13.1` with `bsv-rs` `=0.8.3` and never
pins a prerelease, so the gate builds the CLI's beta on this crate's beta and
its main on main.

From #199 the beta line takes `bsv-rs` `=0.9.0-beta.2` (the owner's standing
word of 2026-10-27); the CLI's beta re-pins it in its pin PR (CLI #277) with
this crate's next beta, and the CHANGELOG's `[Unreleased]` host note lists
every host-visible change that pin carries.

On 2026-10-28 the CLI's beta took that pin at b101d67 (CLI PR 332, 29 commits,
two Opus reviews): `=0.14.0-beta.2` with `bsv-rs` and `bsv-tracker`
`=0.9.0-beta.2`, both workspaces and locks, its CI green on the ten required
checks against the published crates. Two Dependents runs against this tree are
red by construction and record it: run 40679043409 (11:19Z, the CLI's
dispatch with `cli_ref=lane/p1-277-pin-beta2` at bb9266b, the clone line naming
the branch, #405) and run 40680949785 (11:34Z, the plain dispatch on beta 6957487
against the CLI's beta at b101d67). Both fail the CLI's build at
`create_action_offering` (`E0061`: four arguments on this tree, three in the
published crate): #353's must-spend list (PR #399, `api-break`) landed on beta
behind the tag v0.14.0-beta.2, and the published crate the CLI pins does not
carry it. The red closes when the CLI takes the shape (CLI #313) at this
crate's next prerelease; until then a beta dispatch reads the same and proves
the input and the clone line, not the pin.

On 2026-10-28 the CLI's beta took the beta.3 pin at 6e1a7bc (CLI PR 345,
21:11:41Z): `=0.14.0-beta.3` with `bsv-rs` and `bsv-tracker` `=0.9.0-beta.3`,
both workspaces, with #353's fourth argument (CLI #313) and the typed refusal
map (CLI #330) inside. Two Dependents runs record it: run 40765080419 (the
CLI's dispatch with `cli_ref` against the pin branch at 809d782: `build: ok
(--locked) test: ok`, the clone line naming the branch) and run 40770352588 (the
plain dispatch on beta a101a10c against the CLI's beta at 6e1a7bc): green.
The beta.2 row's red above closed with this pin, as #313 said it would. The
CLI's mainnet gate passed at 21:17Z on 6e1a7bc (24 of 24).

The toolbox's beta.4 re-pin (#488, the tag at `56d12075`) moves this tree to
`bsv-rs =0.9.0-beta.4`. Against it the CLI's beta (c1f7cd3, still
`=0.14.0-beta.3` with `bsv-rs` and `bsv-tracker` `=0.9.0-beta.3`) is red by
construction, as the beta.2 row was behind its tag: `tools/dependents-check.sh
--only bsv-wallet-cli` on the lane (2026-10-30) answers `build: FAIL, test:
not run`, cargo's resolver refusing the two exact pins (`failed to select a
version for bsv-rs ... previously selected package bsv-rs v0.9.0-beta.3`).
Nothing compiled, so no CLI source is implicated. The row turns green when
the CLI takes the toolbox's next prerelease with `bsv-rs` and `bsv-tracker`
`=0.9.0-beta.4` (its pin lane, after the toolbox's), with the `[Unreleased]`
host lines (CLI #347).

### Re-pin plan

The toolbox's #419 pin moves this tree to `bsv-rs =0.9.0-beta.3` in both
workspaces; the lock files stay untracked (the pin does not force tracking them). CLI #338 takes that SDK version with the next toolbox
prerelease; the CLI row above records its existing published beta.2 pin,
not a completed beta.3 migration. The `[Unreleased]` host notes list the
ordered maps, validator and transport words, linkage keys, and the default
script behavior the CLI must account for. Run the dependents check after
landing and after the CLI takes the new shapes.

A row whose ci column is not `yes` stays out of the CI job, and the job never
carries a row that is red for a reason this repository cannot fix. Each
excluded row joins the job when its captain re-pins it onto this crate's
`bsv-rs`.

| row | blocker | tracked | joins the job |
|---|---|---|---|
| `dolphin-milk` | pins `bsv-rs` 0.3, which does not build against this tree; kept out by the captain's decision on the #59 cold review | tracked in #94 | when its captain re-pins `bsv-rs` and this crate |
| `agent-manage` | a private repository (the job holds no token), and it pins `bsv-rs` 0.5.0 | tracked in #94 | when its captain re-pins `bsv-rs` and this crate; until then it runs on a laptop with `--from` |

The lock is moved by `cargo update -p bsv-wallet-toolbox-rs@<locked version>`
alone, so only this crate and what it newly needs change; the script prints
that diff. A build that still needs `--locked` relaxed says so in its summary.

<!-- dependents-check:begin -->
| crate | clone | dir | ci | test | reason |
|---|---|---|---|---|---|
| bsv-wallet-cli | https://github.com/Calhooon/bsv-wallet-cli.git | . | yes | default | |
| agent-manage | https://github.com/Calgooon/agents.git | manage | no: private | default | |
| dolphin-milk | https://github.com/Calhooon/dolphinmilk.git | . | no: bsv-rs 0.3 | default | |
<!-- dependents-check:end -->
