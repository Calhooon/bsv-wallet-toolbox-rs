# Fuzzing

The `fuzz/` crate holds twelve cargo-fuzz targets, one per parser that reads
bytes from outside the wallet (#58). Each is a thin wrapper that feeds the
fuzzer's bytes to the production entry point and asserts nothing but "no
panic": a refusal is a fine answer.

The crate is its own workspace (`fuzz/Cargo.toml`); the root crate's
`Cargo.toml` does not know it. It depends on the root crate with the
`bench-support` feature, for the one hidden seam it uses
(`benchmark_support::local_explorer_services`, the explorers at a local base).

## The targets

| target | what it reads | entry points |
|---|---|---|
| `beef_door` | a stranger's BEEF, as the internalize and create doors take it | `refuse_invalid_beef_bytes` (bsv-rs's stream verifier), then on bytes it lets through `Beef::from_binary`, `verify_valid`, `validate_beef_for_broadcast`, `beef_to_ef_batch`, `unproven_ancestors_in_beef`, `to_binary_atomic` |
| `tsc_proof` | a TSC proof (WhatsOnChain, Bitails) | `tsc_json_to_bump_hex`, `tsc_json_to_bump_binary` (the private `tsc_proof_to_merkle_path`), the BUMP read back; the input's leading four bytes are the block height |
| `merkle_path` | a BUMP, as the reorg pass and internalize read it | `MerklePath::from_binary`, `from_hex`, `compute_root` for each leaf, the round trip |
| `broadcast_memory` | `broadcast_seen` rows as text, `txid,provider,status,seconds` per line | `BroadcastStatus::parse`, the ladder (`InMemoryBroadcastMemory::record_broadcast_status_at`), `seen_set_from_records`, `oldest_stale_ancestor`, `unproven_ancestors_in_beef` |
| `arcade_sse` | the Arcade V2 status stream | `SseFrameParser` (two chunks split at the leading byte's offset), the `ArcadeStatusEvent` JSON, `arcade_reorg_marker`, and `ArcadeSseClient::stream_once` reading the body as its `/events` stream |
| `whatsonchain` | WhatsOnChain's answers | every read ladder of `Services` (raw tx, merkle path, status, utxo status, utxo with spender, leads) with WhatsOnChain answering |
| `bitails` | Bitails's answers | the same ladders with Bitails answering |
| `bananablocks` | BananaBlocks's answers | the same ladders with BananaBlocks answering |
| `arc` | ARC's answers | `Arc::get_tx_data`, `get_merkle_path`, `post_raw_tx`, `post_beef` |
| `arcade` | Arcade V2's answers | `Arcade::get_tx_status`, `get_merkle_path`, `get_status_for_txids`, `health`, `post_beef` |
| `bhs` | the Block Header Service's answers | `BlockHeaderService::current_height`, `chain_header_by_height`, `is_valid_root_for_height`, `find_chain_tip_header` |
| `chaintracks` | the Chaintracks client's answers | `ChaintracksServiceClient::get_present_height`, `find_header_for_height`, `find_header_for_block_hash`, `find_chain_tip_header`, `is_valid_root_for_height`, `ChainTracker::mining_tip` |

### How a provider target answers

The provider targets (`arcade_sse` and the seven providers) run the real
clients against a loopback HTTP server in the fuzz process (`fuzz/src/lib.rs`).
A target arms a path prefix; every request under it is answered with the
fuzzer's bytes: the leading byte picks the status from a fixed list, the rest
is the body. A `/headers` request gets a fixed Chaintracks header, so a ladder
that checks a header before it asks an explorer still reaches the explorer.
Any other path gets a 404.

- No request leaves the machine. The server listens on 127.0.0.1, ARC and the
  header service are pointed at it, and the harness sets `HTTPS_PROXY` and
  `HTTP_PROXY` to a refused local port (with `NO_PROXY` for the loopback), so
  a request to any other host fails at once.
- The status list has no 429 (it closes the rung for its Retry-After, which
  would starve every later input) and no 5xx (its body is not parsed, and the
  explorers wait two seconds before their retry).
- A provider target runs at tens to hundreds of inputs a second (about thirty
  loopback requests an input for an explorer); the pure targets run at
  thousands.

## Seeds and corpus

`fuzz/corpus/<target>/` holds each target's corpus. The seeds are copied from
the fixtures and vectors already in the repository by `fuzz/seed_corpus.py`
(run it from the repository root) and named by their source:

- `tests/fixtures/data/ladders/**`: each provider's recorded answers, the
  status as the target's status byte (429 and 5xx answers are skipped), for
  the explorers, BHS and Chaintracks; the TSC proofs among them for
  `tsc_proof`; the BUMPs among them for `merkle_path`.
- `test_vectors/transactions/merkle_path.json`: the TSC proof vectors.
- `tests/vectors/broadcast_body/*.bin`: the six BEEF rows, for `beef_door`
  and `broadcast_memory`.
- `tests/vectors/arcade_status_verdicts.json`: the submit and status bodies
  for `arc` and `arcade`, the same bodies as SSE frames for `arcade_sse`, and
  the memory row each word is recorded as for `broadcast_memory`.

The hash-named files beside the seeds are what the targets found in their
runs, minimized with `cargo fuzz cmin`. To keep a later run's growth, run
`cargo +nightly fuzz cmin <target>`, run `python3 fuzz/seed_corpus.py` to put
the named seeds back, drop the hash-named copies of a seed, and commit.

## Running one locally

Fuzzing needs the nightly toolchain and cargo-fuzz. Keep the build out of the
checkout and give every run a fixed budget in the foreground:

```bash
export CARGO_TARGET_DIR=$HOME/bsv/targets/fuzz
export SDKROOT=$(xcrun --sdk macosx --show-sdk-path)   # macOS only
cargo install --root ~/bsv/targets/fuzz-tools cargo-fuzz
export PATH=$HOME/bsv/targets/fuzz-tools/bin:$PATH

cd fuzz
cargo +nightly fuzz build                       # every target
cargo +nightly fuzz run beef_door -- -max_total_time=60 -timeout=10
```

`-max_total_time` is the budget in seconds and `-timeout` the limit on one
input. Never start a run with `&` or a loop that outlives its budget. The run
ends with a summary line such as

```
#311069	DONE   cov: 3239 ft: 10085 corp: 398/139Kb lim: 1748 exec/s: 5099 rss: 461Mb
```

`#` is the inputs executed, `cov` the edges covered, `corp` the corpus size
(inputs and bytes) and `exec/s` the rate.

## Reading a crash

A panic, an address-sanitizer report, an input over `-timeout` or a run over
the memory limit stops the run with a nonzero exit, a report and a line such as

```
Test unit written to fuzz/artifacts/<target>/crash-<sha1>
```

1. Read the report: the panic message and its location, or the sanitizer's
   kind and stack. A `timeout-` or `oom-` artifact is a hang or a blow-up, not
   a panic.
2. Reproduce it: `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/crash-<sha1>`.
3. Minimize it with a budget:
   `cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/crash-<sha1> -- -max_total_time=60`.
4. Commit the minimized artifact under `fuzz/artifacts/<target>/` and file an
   issue with the target, the panic text and the artifact path. The fix is
   the parser's own change, with a regression test that feeds the artifact's
   bytes to the entry point.

## In CI

`.github/workflows/fuzz.yml` runs on manual dispatch only, never on a
schedule (#153, #159): one job per target, five minutes each
(`-max_total_time=300`), fifteen minutes a job at most. Each job uploads the
target's artifacts and grown corpus. It is informational and never a required
check.
