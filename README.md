# Rust Wallet Toolbox

Storage, signing, services and monitoring for BSV wallets. The release of record
is the tag **v0.9.0**, published on crates.io as **0.9.0** (the same version);
the repository's Releases page carries a Release per tag. It declares
**bsv-rs 0.7.0** by caret, the Rust SDK published by this project. The toolbox
implements wallet operations over local SQLite storage or a remote
JSON-RPC storage service.

## Install and architecture

```toml
[dependencies]
bsv-wallet-toolbox-rs = { version = "0.9.0", features = ["sqlite"] }
```

[Cargo.toml](Cargo.toml) declares features and dependencies: `sqlite`
(default), `mysql` (it enables sqlx's MySQL driver; `StorageSqlx` is SQLite
only, no MySQL storage exists), `remote` (`StorageClient`), `full` (the
three), and `break-glass-script-history`, off by default and not part of
`full`. The Rust library name is `bsv_wallet_toolbox_rs`. [src](src) contains
the wallet, signer, storage, services, monitor and managers; [tests](tests)
contains integration witnesses. `StorageSqlx` supplies SQLite storage and
`StorageClient` (feature `remote`) connects to a remote wallet storage service.
The wallet uses the SDK's keys, hashes, transaction and BEEF types. It keeps no
header store of its own: every header question is asked of the header service
(`ServicesOptions::chaintracks_url`) and fails closed without one, and
[tests/rule_28_header_store_tests.rs](tests/rule_28_header_store_tests.rs)
fails if an embedded store comes back.

## The local tracker rule

A broadcaster's refusal, a mempool conflict and a retry count are hints, not
proof that a transaction is invalid. On the immediate post and in the monitor,
a refusal records the hint and schedules another check without releasing the
transaction's inputs or writing off change. Named competitors are queued for
proof checks. A caller can still receive errors for its own bad arguments,
signing, storage or funding failures. The host's explicit retire,
`StorageSqlx::retire_undeliverable_txid`, is a separate act, not a conclusion
derived from a broadcaster's word.

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
broadcast-body, status-word and cross-SDK fixtures. [deny.toml](deny.toml) is
the stack's dependency policy for `cargo deny check`, which the workflow does
not run. Dependabot moves the stack's crates daily at 18:00Z under
[.github/dependabot.yml](.github/dependabot.yml).

## Reorgs

A reorg is absorbed below the user and the app. A stored proof is stale when,
and only when, its root is not the active header's root at its height in the
wallet's own headers; the monitor rechecks the proofs a reorg touches with a
local header read and re-proves only those whose root moved, a few a pass. A
transaction whose proof was lost reads `unproven` ("confirming"): its inputs
stay allocated to it, its outputs stay spendable as an unproven creator's are,
and nothing is failed until a competitor's checked proof arrives. The courier
is asked once for it and told once only when it does not know it. A BEEF never
carries a stored path whose root the wallet's headers do not hold: that
ancestor goes in as raw bytes and the walk continues to proven ancestors.

## Records and releases

[RELEASING.md](RELEASING.md) defines the machine gate and trusted publishing
from release tags. [CHANGELOG.md](CHANGELOG.md) holds changes and migration
notes; [docs/REORG-DIVERGENCES.md](docs/REORG-DIVERGENCES.md) records where the
reorg rule above differs from the reference. Licensed under MIT, as declared in [Cargo.toml](Cargo.toml).
