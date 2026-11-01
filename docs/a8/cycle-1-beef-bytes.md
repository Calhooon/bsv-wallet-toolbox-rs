# A8 cycle 1: the BEEF path's bytes (#556)

The opening A8 cycle of bsv-wallet-toolbox-rs, on the owner's word of
2026-11-01 14:1xZ (the keeper's reading 22.9 (a); the stack's charter
`docs/charters/auto-research.md`, A8-1 to A8-7). The flag is the cargo
feature `a8-beef-bytes`, off by default and not part of `full`; the oracle
path is the tree's path while it is off, and stays in the tree beside it
(A8-2). It turns on for a line only on the owner's word (A8-6). The figures
are `cycle-1-beef-bytes.json`.

The counts, the profiles and the bench ran on bsv-rs 0.9.0-beta.5 (the
base, f59fce2b). The lane was then rebased onto `origin/beta` at 8c0d2bf6,
which pins 0.9.0-beta.6 (#533); the files this cycle reads and mirrors
(`transaction/beef_stream.rs`, `beef.rs`, `beef_tx.rs`, `beef/`,
`transaction.rs`, `primitives/hash.rs`, `primitives/bsv/sighash.rs`) are
byte-identical between the two releases, and the gates and the vectors
below ran on the rebased tree.

## The row (A8-1)

The census of 2026-10-31 (the stack's `docs/a8/census-2026-10-31.json`,
the toolbox's rows at 1a12236a): through `beef-internalize`
`sha2::sha256::aarch64_sha2::compress` takes 51.82, 49.27 and 46.56 percent
of the door's on-CPU samples at 1, 4 and 10 MB and `_platform_memmove`
17.71, 24.98 and 31.03; through `beef-create` 68.87, 59.73 and 59.65, and
19.97, 35.53 and 36.87. The scenes are `examples/a8_census.rs`: an atomic
BEEF of a proven parent and an unproven child of about N MB (one signed
P2PKH input, an `OP_FALSE OP_RETURN` of N MB), through internalize and
through createAction's caller BEEF pre-check.

The base (f59fce2b, this lane's cut of `origin/beta`), three samply runs a
scene at the census's iteration counts, the median, reproduces the row:

{sec["BASE"]}
Shares are percent of the on-CPU samples inside the door calls'
windows (`benches/a8_census_profile.py`); wall and on-CPU milliseconds are
per call.

## The counts (A8-1)

How many times the child's body (the 1 MB transaction) is hashed in one door
call: a scratch copy of the tree (never this checkout) built against a copy
of bsv-rs 0.9.0-beta.5 with a counter in `sha256`, `sha256d` and the stream
decoder's per-transaction hash, logging every hash over 100 kB with its
backtrace; one call of each scene, the fixture's own hashes left out.

| site | internalize, off | internalize, on | create, off | create, on |
|---|---|---|---|---|
| `Wallet`: `Beef::from_binary`, and `find_atomic_transaction` (a `Transaction::hash`) | 2 | 2 | 1 | 1 |
| `refuse_invalid_beef_bytes` (the wallet's and the door's validator) | | | 2 | 2 |
| `tracker::caller_beef_roots` | | | 1 | 1 |
| `inputs::validate_and_extend_inputs` (pre-check and step) | | | 2 | 2 |
| `assemble_input_beef`: `Beef::from_binary` (pre-check and step) | | | 2 | 2 |
| `merge_beef_linked`: bsv-rs `merge_raw_tx` hashes a re-serialization (pre-check and step) | | | 2 | 2 |
| internalize: the subject scan (`read_incoming_payment`) | 1 | | | |
| internalize: the structure pass (`verify_stream_structure`) | 1 | | | |
| the word pass's two readings (`live_word::verify_judging`) | 2 | | 2 | |
| create's judge: `Beef::from_binary` of the caller's BEEF and of the assembly | | | 2 | |
| the read-once path: `read_once` (internalize one reading; the judge one of each BEEF) | | 1 | | 2 |
| the sighash's `hashOutputs` over the outputs (the input's script, not a txid) | 1 | 1 | 1 | 1 |
| **total** | **7** | **4** | **15** | **13** |

Every reading through bsv-rs's stream decoder hashes the body and copies it
(`beef_stream.rs:878-879`, `934-935`, the hash closed at `1153`);
`Beef::from_binary` copies it twice (the decoder's bytes and
`Transaction::from_binary`, `beef/collect.rs:170`). The copies the
flag removes, read from the code and the memmove call tree: internalize three
decoder copies of the body; the judge two decoder copies and the two parses'
script copies; the assembly one clone of the caller's `Beef` (its bytes and
its parse) per assembly when it has no txid-only entry.

## The design (A8-2)

`a8_beef_bytes()` chooses; a test chooses either path on its thread. Under
the flag:

- internalize reads the AtomicBEEF once (`read_incoming_once`, `read_once`)
  and hands the elements by reference to the subject scan, the structure
  pass and the word pass. The structure pass folds them into bsv-rs's
  structure-only `BeefIndex` as `StreamVerifier::structure_only` folds them
  (held to the prefix's subject before any element, the freed
  transactions released after each, the atomic rule at offset 4 at the end,
  `Judge::conclude`, `beef_stream.rs:3044`); a BEEF with no prefix takes the
  oracle's pass. The word pass is the oracle's own `WordPass`, fed borrowed
  elements (`live_word::verify_elements`, the refactor commit). The subject's
  body moves out of the reading after the verdict.
- create's judge reads the caller's BEEF and the assembly once each
  (`judge_read_caller_beef`): the same judged set (each entry by its last
  occurrence in the assembly, as a parsed `Beef`'s index finds it), the same
  store read, the word pass fed the assembly's elements. Bytes either
  stream refuses take the oracle, so its texts stand.
- the assembly keeps the caller's parsed `Beef` when it has no txid-only
  entry, where the split cloned it.

The reading is held until the verdict: about the BEEF's size again, where
the oracle keeps the subject and the referenced outputs alone. No public
shape, schema, pin or text moves.

## The differential target (A8-3)

Both paths in one binary, the same bytes, any difference in a value, an
error's form and text or a stored row a failure:

- `a8_differential::differential`: the internalize door's reading (the
  subject's txid, roots, body and BUMP claim, or the error) under four header
  contexts (the census tip with every root carried, with none, no tip,
  testnet's word), over the 378 inputs of `fuzz/corpus/beef_door`
  (`the_corpus_reads_the_same_on_both_paths`) and the 1, 4 and 10 MB census
  BEEFs (`the_census_beefs_read_the_same_on_both_paths`);
- the internalize door on the census scenes, its answer and every row of
  every table but the clocks and the door's random reference
  (`the_census_internalizes_the_same_rows_on_both_paths`), and four
  refusals: a wrong key's signature, a moved data byte, a cut stream, an
  uncarried root (`the_census_refusals_are_the_same_on_both_paths`);
- create's judge on every corpus input that parses (206 of 382 with the
  census scenes; the judge runs only on a caller's BEEF the assembly parsed)
  on two tips and two networks
  (`the_judge_reads_the_corpus_the_same_on_both_paths`), and the create door
  on the census scenes and a wrong key's spend, its answer and rows
  (`the_census_creates_the_same_rows_on_both_paths`);
- the stored fixtures' replay (`--test fixtures`, every recorded answer and
  row) with the flag off and on (A8-4 below);
- the fuzz target `a8_beef_bytes` (the seam a hidden re-export under the
  feature), seeded from a copy of `fuzz/corpus/beef_door`: 756,374 runs in
  501 seconds, no difference, the copy grown from 378 to 649 inputs.

The target found three differences in its opening run, each read: the
internalize door's reference is drawn at random outside the seeded stream
(different between any two calls on either path; masked); the judge on bytes
`Beef::from_binary` refuses answers `Ok` unjudged on the oracle path, a
state no door reaches (the corpus held to the judge's precondition); the
census BEEF is refused under testnet's word on both paths
(`PreGenesisSpend`: testnet's Genesis is above the census tip).

Planted reds (each reverted): one byte of the subject body the new path hands
back (three internalize tests red); the judged set's unproven test inverted
(both create tests red).

## The vectors on and off (A8-4)

Every suite with the flag off (CI's default) and again with
`a8-beef-bytes` added, each a fresh compile of this tree, under rule 10's
proxy variables (the logs in the lane folder's `gates/`):

| command | off | on |
|---|---|---|
| `cargo test --lib` | 1,730 passed, 3 ignored | 1,730 passed, 3 ignored |
| `cargo test --lib --features full` | 1,897 passed, 3 ignored | 1,897 passed, 3 ignored |
| `cargo test --tests --features sqlite,remote,bench-support` | 38 binaries, 2,365 passed, 14 ignored | 38 binaries, 2,365 passed, 14 ignored |
| `cargo test --test test_vectors` | 58 passed | 58 passed |
| `cargo test --lib --tests --no-default-features --features remote` | 38 binaries green, 797 lib | 38 binaries green, 797 lib |

The stored fixtures' replay (`replay_storage_fixtures` in `--test
fixtures`, every recorded answer and every persisted column, recorded on
the oracle path) is green with the flag on: no moved verdict, text or row.

## The bench (A8-5)

The tip's census binary built with the flag off and on (ad7b1712; the docs
commit adds no code), three samply runs a scene and binary, off and on
alternated run by run (r1 off then on, r2 on then off, r3 off then on), the
median. The host is shared with other lanes: round 1 ran at load averages of
about 106 to 126, round 2 at about 27 to 62, so the shares and times move
between rounds, and the seven scenes without a caller BEEF (which run the
same code on both builds but the small internalize) show the noise floor.
A gain here is a figure, not a claim.

Round 1 (17:37Z to 17:46Z):

{sec["R1"]}
Round 2 (17:53Z to 17:57Z):

{sec["R2"]}
The harness's other scenes, round 1 (inputs for create and process, one
output for internalize; the on-CPU and wall milliseconds per call):

{sec["DOORS"]}
Read: on the internalize door the flag takes the body's hashes from 7 to 4
and its sha2 share falls (round 2: 56.45 to 43.43, 51.95 to 38.75, 46.13 to
40.01); the door's on-CPU time per call falls by about a quarter (round 2:
8.84 to 6.70, 36.99 to 27.38, 97.42 to 67.90 ms; round 1 the same
direction). The memmove share rises a little as the total shrinks: the
copies left are the wallet's parse, the stored rows and SQLite's. On create
the hashes go from 15 to 13 and the on-CPU time falls 18 to 26 percent in
round 2, within reach of the noise floor (process at 100 inputs moved 21
percent with no code difference), so create's figure waits for a quiet host.
No loss is measured.

## What is left (the next rows)

- bsv-rs: `Beef::merge_beef` (`beef.rs:500-516`) re-reads every raw
  transaction through `merge_raw_tx` (`:396-405`), whose `BeefTx::from_raw_tx`
  (`beef_tx.rs:89-101`) holds no txid, so `BeefTx::txid` (`:166-190`,
  `tx.id()` at `:178`) serializes the parse and hashes it
  (`Transaction::hash`, `transaction.rs:1373-1381`) although the source entry
  carries the txid its decoder hashed. A merge that keeps it (a public
  `merge_beef_tx` taking the source `BeefTx`, the TypeScript SDK's
  `mergeBeefTx`) removes two hashes and two copies of every caller body per
  createAction. To file on bsv-rs.
- bsv-rs: `StreamVerifier` takes only a `Read` (`from_parts`,
  `beef_stream.rs:3159`; `Judge` crate-private at `:2968`), so a caller with
  the elements in hand mirrors `Judge::conclude`'s atomic rule (offset 4)
  through `BeefIndex::fold` and `finish`. A public structure verdict over
  elements would retire the mirror. To file on bsv-rs.
- this crate, outside this lane's files: the create door still reads the
  caller's BEEF eleven times (the two validators, `caller_beef_roots`, the
  two `validate_and_extend_inputs`, the two assemblies and their merges, the
  wallet's parse) and internalize's wallet layer twice; one parse carried
  from the validator through the door would take each to one or two. The
  next cycle's candidate under A8-7, after this record lands.
