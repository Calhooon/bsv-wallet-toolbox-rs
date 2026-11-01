# Storage fixtures (#53)

`cargo test --test fixtures` discovers every JSON file in `data/storage/`.
Each file records `name`, `issue`, `inputs`, `calls`, and `expected`. An unknown
fixture field is an error. The storage tests require the default `sqlite` feature.
The services lane can add `mod ladders;` beside `mod storage;` in `main.rs`.

The services ladder scenes under `data/ladders/` replay as a lib test
(`cargo test --lib ladder_fixtures`, `ladders.rs`). A scene that differs because
a rung faulted at the 2000 ms client deadline (from 100 ms before it on, `LATE_SLACK_MS`,
with no ceiling: a 2106 ms fault is a stall too)
where the scene scripts no late answer is a runner stall, replayed and named in
the run's log. `LADDER_FIXTURES_STALL_REPLAYS` is how many replays it gets: one
when unset, so the local run is unchanged; CI sets two on the steps that run the
ladders (#513). A stall past the last replay fails with `RUNNER STALL, not a
ladder verdict` at the head of the message; a difference that is the ladder's is
never replayed. The deadline itself does not move: the scripted timeouts
(`delay_ms` 3000) are written against it.

`UPDATE_FIXTURES=1 cargo test --test fixtures` rewrites only `expected` and
canonical formatting. Review that diff as a behavior change. Replay without the
variable never writes fixtures. A mismatch prints a unified diff and fails.

Inputs select a public test key by name from `test_vectors/keys/test_users.json`.
The harness migrates a fresh `StorageSqlx::in_memory()`, creates that user, and
seeds the recorded baskets, transactions, output metadata, labels and commissions.
An output's `repeat` expands it into consecutive outputs of the same transaction.
`store: false` builds an external transaction without putting it in the wallet.
Synthetic anchors use non-null outpoints; custom outputs default to `OP_DROP OP_1`.
`spends: "name.vout"` links to an earlier seed. A seed's input pushes a minimal zero
unless `unlocking` gives its unlocking script as hex. A `proven` seed has a synthetic
single-leaf BUMP; the test tracker accepts only the recorded roots and heights.
The proof-height gate is explicitly opened to 880000. These proofs are synthetic
test evidence. They make no claim about a real chain.

Calls run in array order. `as` names a create result for later processing or
abort. `sign: true` exercises the storage door's sign-and-process callback with
the public `WalletSigner` and the test key. `process_action` signs the saved
template before calling storage. `send_waiting` exercises the public monitor
method with `MockWalletServices`; it performs no network request. An offering
call names its entire offered set as `name.vout` entries. `$outpoint:name.vout`
and `$txid:name` in arguments resolve to the recorded seed's transaction.
`age_requests` is a harness clock step: it backdates request creation and update
timestamps by the recorded seconds, so the real retry cadence can be exercised
without sleeping. It records the number of aged requests.

Mock answers use `{ "kind": "success", "value": ... }`,
`{ "kind": "error", "message": ... }`, or
`{ "kind": "sequence", "values": [...] }` under the method name.
Unspecified answers use `MockWalletServices` defaults. Service call order,
arguments and success are part of every snapshot. Lead calls are recorded
directly through the mock service boundary used by sync: discovery must leave
the wallet's rows unchanged. This does not implement the CLI's sync loop.

Each call captures its result and every column of users, transactions, outputs,
proven transactions and requests, baskets, labels, tags, their maps, commissions,
sync state, broadcast memory and preferences, locked-input checks, proof-root
checks and monitor state. Rows are ordered by their creation id; key-only tables
have an explicit key order. No rows are discarded.

Normalization preserves nulls, flags, counts, order, values and relationships:

- Row ids become consecutive creation-order numbers per entity, including
  foreign keys. Transaction hashes and references become shared tokens in
  encounter order, including embedded outpoints and errors.
- Wall-clock fields are omitted. Version, lock time, heights, attempts and
  sequence numbers remain visible.
- BEEF, raw transactions and BUMPs are decoded, preserving transaction order,
  scripts, amounts, proof links and format versions.
- Random derivations become shared tokens. A generated change script is
  replaced only when it equals the BRC-29 script independently derived from
  its recorded derivation fields. Other scripts remain exact hex.
- P2PKH signature bytes become tokens; the sighash and public key remain.
  Variable signature sizes also move byte offsets, so an output's script offset
  becomes its vout token only when it exactly matches the parsed raw bytes.
  A wrong offset stays a number and causes a diff. Other script lengths remain.
  Mock broadcast byte counts are canonicalized to 107-byte P2PKH unlocks only
  when the count matches a BEEF reconstructed from the persisted request.

Database rows occupy one JSON line each to keep the 100-coin offer in one file
below 800 lines. The formatter and offset/script preservation have focused tests.

These are witnesses of the Rust implementation at its recorded baseline, not
cross-implementation parity claims. A policy change needs its own reviewed
fixture diff. No production source, schema or public API is changed here.
