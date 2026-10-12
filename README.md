# Rust Wallet Toolbox

Storage, signing, services and monitoring for BSV wallets. **0.7.5** uses
**bsv-rs =0.4.5**, the Rust SDK published by this project. The toolbox implements
wallet operations over local SQLite or a remote JSON-RPC storage service.

## Install and architecture

```toml
[dependencies]
bsv-wallet-toolbox-rs = { version = "=0.7.5", features = ["sqlite"] }
```

[Cargo.toml](Cargo.toml) declares features and dependencies. The Rust library
name is `bsv_wallet_toolbox_rs`. [src](src) contains the wallet, signer, storage,
services and monitor; [tests](tests) contains integration witnesses.
`StorageSqlx` supplies SQLite storage and `StorageClient` connects to a remote
wallet storage service. The wallet uses the SDK's keys, transaction and BEEF
types; it does not maintain an independent header store.

## The local tracker rule

A broadcaster's refusal, a mempool conflict and a retry count are hints, not
proof that a transaction is invalid. On the immediate post and in the monitor,
a refusal records the hint and schedules another check without releasing the
transaction's inputs or writing off change. Named competitors are queued for
proof checks. A caller can still receive errors for its own bad arguments,
signing, storage or funding failures. Explicit host retirement is a separate
act, not a conclusion derived from a broadcaster's word.

The witnesses include [tests/immediate_broadcast_hint_tests.rs](tests/immediate_broadcast_hint_tests.rs)
and [tests/monitor_tests.rs](tests/monitor_tests.rs). Header and merkle-proof
checks determine chain evidence; stored broadcaster acceptance is not mining.

## Known ancestors

When a caller declares a known ancestor, the result BEEF represents that
ancestor by txid only. This applies to createAction and signAction and is the
known-ancestor rule exercised for ts-stack issue 660 parity. It does not turn
an unknown, unproven input into trusted ancestry: input validation and the
wallet's own known-transaction resolution remain separate obligations.
[tests/known_ancestor_tests.rs](tests/known_ancestor_tests.rs) records both paths.

## Build and gate

```sh
cargo build --features sqlite
cargo test --features sqlite --test known_ancestor_tests --test immediate_broadcast_hint_tests
cargo fmt --all -- --check
```

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs SQLite unit, integration
and doc tests, formatting, clippy and named vectors on PR heads and main pushes;
it also builds documentation and collects coverage. The vector job checks
broadcast-body, status-word and cross-SDK fixtures. Static test-count badges
are not a gate result.

## Records and releases

[RELEASING.md](RELEASING.md) defines the machine gate and trusted publishing
from release tags. [CHANGELOG.md](CHANGELOG.md) holds changes and migration
notes; [docs/REORG-DIVERGENCES.md](docs/REORG-DIVERGENCES.md) records the reorg
comparison. Licensed under MIT, as declared in [Cargo.toml](Cargo.toml).
