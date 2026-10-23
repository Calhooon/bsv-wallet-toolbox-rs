# Dependents

The crates that build on `bsv-wallet-toolbox-rs`, read from their manifests at
their default branches on 2026-10-23. Every pin-move PR in a dependent (or here,
when a release moves what a dependent should take) updates its row in the same
PR, so this page stays current.

| crate | repository | pins this crate | how it consumes | captain |
|---|---|---|---|---|
| `bsv-wallet-cli` 0.18.0 | [Calhooon/bsv-wallet-cli](https://github.com/Calhooon/bsv-wallet-cli) (public) | `=0.12.1` (main and beta), renamed `bsv-wallet-toolbox` | features `sqlite`. The crate root re-exports (`Wallet`, `StorageSqlx`, `Services`, `ServicesOptions`, `Chain`, `Monitor`, `Error` and `StoreBusyReason`, the storage traits `WalletStorageProvider`, `WalletStorageWriter`, `MonitorStorage`, the broadcast and status words `BroadcastStatus`, `BROADCAST_*`, `PROVIDER_*`, `classify_broadcast_results`, `is_fatal_status`, `unproven_ancestors_in_beef`); `services` (`mock::MockWalletServices` in its tests, `traits`, `UtxoVerdict`, `providers::{arcade, whatsonchain, BananaBlocks}`); `storage` (`sqlx::refuse_invalid_beef_bytes`, `ProvenTxAnchor`, `StorageProcessActionArgs`); `monitor` (`ArcadeEventsTask`, `reorg_ops`, `tasks::check_for_proofs`) | the stack keeper (the CLI's programme, bsv-wallet-cli #106) |
| `agent-manage` 0.1.0 (`manage/` in the agents repository) | Calgooon/agents (private) | `0.7.7` (locked 0.7.7), renamed `bsv-wallet-toolbox`, with `bsv-rs` `0.5.0` | features `remote`. The crate root (`Wallet`, `Services`, `ServicesOptions`, `Chain`, `StorageClient`, the storage traits `WalletStorageReader`, `WalletStorageWriter`, `WalletStorageSync`, `WalletStorageProvider`, `WalletServices`); `storage` (`StorageClient`, `entities`, the sync arguments `RequestSyncChunkArgs`, `SyncChunk`, `SyncOffsets`, `FindProvenTxReqsArgs`); `services::providers::arc::ArcConfig` | the agents captain |
| `dolphin-milk` 0.1.4 | [Calhooon/dolphinmilk](https://github.com/Calhooon/dolphinmilk) (public) | `0.3.38`, optional, with `bsv-rs` `0.3.4` and `bsv-wallet-cli` `0.1.23`; no committed `Cargo.lock` | behind its default feature `embedded-wallet`, default features of this crate (`sqlite`). The crate root (`Wallet`, `WalletOptions`, `Monitor`, `Services`, `ServicesOptions`, `StorageSqlx`, `Chain`, `WalletStorageWriter`); `services::providers::ArcConfig` | none named |

## The check

`tools/dependents-check.sh` reads the table below, clones each dependent at its
default branch into a scratch directory, rewrites its dependency on this crate
to a path dependency on the tree under test, and runs `cargo build --locked`
(falling back to `cargo build` with a note) and `cargo test --locked` on every
target (`cargo test` when the build fell back). A row whose test column is not
`default` runs that narrower command instead, and its reason column says why;
a narrowed row is noted in the summary. The CI job `dependents` runs the rows
marked `yes`; a private repository needs a token the job does not hold, so its
row runs on a laptop with `--from`.

What the check found against 0.13.0 on 2026-10-23: `bsv-wallet-cli` at
`main` builds `--locked` and its library tests pass; `agent-manage` and
`dolphin-milk` do not build, because each pins an older `bsv-rs` (0.5.0 and
0.3.x) whose types this crate's `bsv-rs` 0.8 does not accept. Each joins the CI
job when its captain re-pins `bsv-rs` and this crate (the pin spread is a
scoreboard line of epic #80).

### Re-pin plan

A row whose ci column is not `yes` stays out of the CI job, and the job never
carries a row that is red for a reason this repository cannot fix. Each
excluded row joins the job when its captain re-pins it onto this crate's
`bsv-rs`.

| row | blocker | tracked | joins the job |
|---|---|---|---|
| `dolphin-milk` | pins `bsv-rs` 0.3, which does not build against this tree; kept out by the captain's decision on the #59 cold review | tracked in #TBD | when its captain re-pins `bsv-rs` and this crate |
| `agent-manage` | a private repository (the job holds no token), and it pins `bsv-rs` 0.5.0 | tracked in #TBD | when its captain re-pins `bsv-rs` and this crate; until then it runs on a laptop with `--from` |

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
