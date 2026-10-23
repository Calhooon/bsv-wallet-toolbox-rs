# Door and explorer benchmarks

Run from the lane worktree with build and measurement output outside it:

```sh
export CARGO_TARGET_DIR="$HOME/bsv/targets/oracle-54-benches"
export CRITERION_HOME="${TMPDIR:?}/oracle-54-criterion"
export RUSTFLAGS=-Dwarnings
DOOR_BENCH_SAMPLES=10 cargo bench --bench door
tools/bench-compare "$CRITERION_HOME" --profile laptop --write
tools/bench-compare "$CRITERION_HOME" --profile laptop
python3 tests/bench_compare_test.py
```

When `/Volumes/Lanes` is mounted, keep measurement output under
`/Volumes/Lanes/out/bsv-wallet-toolbox-rs/oracle-54-benches/` instead of temporary
storage. `--write` updates only the selected profile in `benches/baseline.json`.
Use `--baseline PATH` for a candidate that should not replace the stored one.
`--tolerance 8` permits an eight percent p99 increase; the default is five.
`--filter NAME` compares one benchmark during a targeted regression witness.

Criterion's `html_reports` and plotting dependencies are disabled. The default
sample size is 50; `DOOR_BENCH_SAMPLES` selects at least 10 samples and also
reduces warmup, measurement duration and bootstrap resampling for CI. The
Criterion `--quick` option also works, but collects fewer tail observations.

| Benchmark | Work inside the timer |
| --- | --- |
| `seed_10000` | Open and migrate an in-memory store, derive a synthetic funding key, seed 10,000 distinct proved parents and coins, store an incoming proof |
| `create_action_offering_1000_coins_100_offered` | Public wallet offer through P2PKH signing and process writes |
| `create_action_offering_10000_coins_100_offered` | Same offer size against a larger wallet |
| `create_action_offering_10000_coins_10000_offered` | Offer every coin through the signed door |
| `create_action_process_action_10000` | Public wallet create action, including the signer and process writes within its door |
| `internalize_action_stored_proof_10000` | Import a new owned transaction whose proof is already stored; assert completed status |
| `list_outputs_10000` | Return all 10,000 outputs, without BEEF or labels |
| `allocate_change_input_10000` | One allocator SELECT and UPDATE on an already acquired write transaction |
| `is_utxo_unspent` | Real service ladder returning `UnspentHint` |
| `is_utxo_spent` | Bound spender bytes and a merkle proof checked against the mock header service |
| `is_utxo_spent_hint` | Named spender with bound bytes and no proof |
| `is_utxo_could_not_look` | All explorers return a deterministic HTTP authorization fault |
| `get_raw_tx_200` | Real raw transaction ladder returns bytes bound to their txid |
| `get_raw_tx_404` | All three explorers return 404 |

The shared seed is `common/mod.rs`. Every coin has its own small transaction and
stored proof, avoiding a large shared parent that would change the BEEF cost.
The keys, transactions and headers are synthetic. Outgoing actions use
`no_send`; `MockWalletServices` records zero calls for them. Intake reads one
local mock header, with the proof gate set to the fixture height. No real funds or remote
explorers are involved. SQL cleanup restores the same coin set after each
action, outside its timer. Allocation rolls back outside the timer. The
internalize fixture contains its incoming proof before measurement, and each
import creates a fresh transaction row rather than timing the merge path.

The hidden `benchmark_support` module delegates allocation to the existing
allocator: its six-argument wrapper was otherwise test-only, and no public
operation isolates it. The same hidden module redirects every provider used
by the real ladders to mockito; the existing provider constructors and service
builder remain crate-private. It gives BananaBlocks a large local token
budget, retaining the token-bucket code without exhausting a remote-service
budget during a local benchmark. Ladders reuse connections, retain their
rotating provider start, and clear call history between iterations. Each call
checks its verdict and request count. Counts are 3 or 4 for unspent, 6 or 7 for
proved spent, 7 for spent hint, 5 for could not look, and 1 or 3 for raw 200 or
404 respectively. Rotation and the header cache account for the alternatives.
Unmatched GETs and every POST fail fixture assertions. Fixture construction
is outside the timer.

## Quantiles and comparisons

Criterion supplies estimates of batch averages, including their median, but
does not supply p99. The harness uses `iter_custom` and records each call's
wall time. It matches the trailing batches against `new/sample.json`, discards
warmup, and adds explicitly labelled `p50` and `p99` point estimates to
`new/estimates.json`. Both use linear interpolation at `(N - 1) * q` over the
individual call durations in nanoseconds. Sorted durations are retained in
`new/latencies.json`. These are empirical quantiles, without a confidence
interval; reduced CI runs have noisier tails. They are not a normal-distribution
approximation or a p99 of batch averages.

`door-run.json` lists completed benchmarks and records whether the run finished.
The comparator reads only those estimates and fails on incomplete runs,
missing or extra benchmarks, nonfinite numbers, incompatible platforms, or
unknown quantile methods. A p99 increase strictly greater than the tolerance
returns 1. Invalid or incomplete input returns 2. P50 is displayed but does
not gate. To demonstrate the gate, add a one millisecond sleep just inside
`create_action_at_door_internal`, run the offered-door benchmark into a separate
directory, compare with `--filter create_action_offering_1000_coins_100_offered`,
then remove the sleep and rerun.

## Linux baseline handoff

The `bench` job is an advisory context on `ubuntu-latest`, with the reduced
sample size and output under the runner's temporary directory. It prints a
candidate Linux profile before comparing with the committed Linux profile.
It never automatically accepts that candidate. Laptop measurements are stored
separately and cannot pass a Linux comparison.

The Linux profile is pending until an actual runner supplies it. Its numbers
are `[D]`, not local measurements. The initial comparison intentionally fails
with `baseline pending`; after the captain's CI run, copy the logged Linux
profile into `baseline.json`, preserve the laptop profile, and review and
commit it. Keep `bench` out of required branch-protection contexts. The epic's
3 ms signed-door, 2 ms allocation-and-sign and 1 ms ladder targets are targets,
not asserted results. Publish actual Linux numbers to the scoreboard after CI.

These choices measure T3 and make the check executable (T6). Local, bounded
fixtures exercise T1; stored proofs against a header oracle exercise T2 and
T4; byte-bound answers and separate unknown/hint/proved cases support T5.
