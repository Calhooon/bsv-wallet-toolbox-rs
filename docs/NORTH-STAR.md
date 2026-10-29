# The toolbox north star

This crate builds and stores wallet actions and supplies the service ladders its
hosts use. Its programme is [#80][epic]: one row beside the CLI and bsv-rs on a
shared scoreboard, with consumers checked through the worker layer to the apps.
Decisions cite the stack's [six tests][six-tests] and the charter sections below.
Work is taken from the ranked queue under [the lane rules](lanes/README.md).

## The scoreboard row

The observed column is written by `tools/scoreboard-write`. Targets are gates,
not observations. Door latency means `create_action_offering` through a signed
transaction, excluding network time; allocation and signing has its own row.
The initial dated cells are **[D]**, reported by [#55][i55]
and [#59][i59] on 2026-10-23; this lane has not rerun those measurements. The
11,441 seed is the largest file's line count. The count of files above 800 is
still unmeasured. Pin endpoints describe direct dependents' toolbox requirements
(dolphinmilk and the CLI); the wider stack's bsv-rs spread is in [#80][epic].

<!-- scoreboard:begin -->
| Measure | Observed | Target / issue | JSON metric |
|---|---|---|---|
| Door, 1,000 coins, p99 (ms) | not yet measured | Track alongside 10,000 coins; #54 | `door_1000_p99_ms` |
| Door, 10,000 coins, p99 (ms) | not yet measured | < 3 ms, no network; #54 | `door_10000_p99_ms` |
| Allocate and sign, p99 (ms) | not yet measured | < 2 ms; #66 | `allocate_and_sign_p99_ms` |
| Ladder verdict, p99 (ms), maximum across rungs | not yet measured | Each rung < 1 ms; #54 | `ladder_p99_ms` |
| Ladder requests per call, maximum across cases | not yet measured | Pinned per case; #53, #54 | `ladder_request_count` |
| Verify 10 MB BEEF (ms) | not yet measured | Track time; #54, #58 | `beef_10mb_verify_ms` |
| Verify 10 MB BEEF, peak memory (bytes) | not yet measured | Flat memory as input grows; #58 | `beef_10mb_peak_bytes` |
| Door with monitor beside it, p99 (ms) | not yet measured | Unchanged from door baseline; #69 | `writer_door_p99_ms` |
| Explorer calls per block | not yet measured | Bounded by re-ask schedule; #72 | `explorer_calls_per_block` |
| Rust files over 800 lines (count) | measured 2026-10-28: 33 | 0; #55, #61, #62 | `files_over_800` |
| Largest Rust file (lines) | measured 2026-10-28: 7193 | <= 800; #55, #61, #62 | `max_file_lines` |
| Unwrap/expect outside tests (count) | measured 2026-10-28: 61 | 0; #55, #56 | `unwraps_outside_tests` |
| Door and ladders mutation score (%) | not yet measured | >= 80; #57 | `mutation_score_pct` |
| Undocumented public items (count) | measured 2026-10-28: 308 | 0; #55 | `undocumented_public_items` |
| Direct dependents, oldest toolbox pin | measured 2026-10-23: 0.3.38 | Current minor or dated plan; #59 | `dependent_pin_min` |
| Direct dependents, newest toolbox pin | measured 2026-10-23: 0.12.1 | Current minor or dated plan; #59 | `dependent_pin_max` |

<!-- scoreboard:sha256 0ffa033e3436bf06ed92d83be333b19e3b246d7050c72fd47731a4454edcade2 -->
<!-- scoreboard:end -->

### Feeding the table

Python 3 and its standard library suffice. From any directory, run
`tools/scoreboard-write` using its path in this checkout; the producing CI job
must then run `tools/scoreboard-write --check`. The latter is read-only and fails on stale
source values or an edited table. With both JSON sources absent it checks the
table against its checksum alone and passes when they match.
The generated checksum covers the table. Only text between the markers changes.
An absent source, omitted metric or null metric preserves its existing cell.
`--root PATH` selects an isolated tree for fixture checks.

The producers [#54][i54] and [#55][i55] write this envelope, with `measured_at`
set to the run's date, and keys from the table's JSON column:

```json
{
  "measured_at": "2026-10-23",
  "metrics": {
    "door_1000_p99_ms": 1.25,
    "ladder_p99_ms": {"get_raw_tx/bitails/404": 0.75},
    "ladder_request_count": {"get_raw_tx/bitails/404": 2}
  }
}
```

These example numbers are synthetic. `benches/baseline.json` supplies latency,
memory, request and explorer metrics. `tools/ratchet.json` supplies code-health,
mutation and pin metrics, incorporating the exports of #57 and #59; a metric in
the other file fails with its key and the file it belongs to. A flat
object with `measured_at` and metric keys is also accepted; extra keys are ignored.
Numbers are nonnegative, counts are integers, mutation is a percentage, and pin
endpoints are version strings such as `0.12.1`. Invalid known values fail before
any write. The producer chooses comparable runner, workload and sample settings.

The two ladder maps name every measured operation/provider/answer-pattern rung.
The writer adds one row per case, plus its maximum summary; an omitted case is
preserved. Include raw-tx, merkle-path, UTXO-status, status-batch, is-UTXO,
UTXO-leads and both header ladders from #53. Record request counts for the same
cases. A 10 MB memory point needs companion input sizes in the bench evidence
to establish flat memory. This tool transfers observations; #54 and #55 own
regression decisions. CI integration for those lanes must run the writer after
producing the JSON, retain the document diff, and check the generated document.
The `scoreboard` CI job only checks; the write step stays with the producers,
#54's bench job and #55's ratchet job.

## The phases and their exits

Each line is that issue's exit test. A phase exits only when **all** its listed
tests pass with recorded evidence; a later phase also requires earlier exits.

### Phase 0: the oracle

- [#52][i52]: a PR into beta runs checks, a beta prerelease tag publishes a prerelease, and a plain tag on beta is refused.
- [#53][i53]: `cargo test --test fixtures` replays every door and ladder fixture, with CI diffs; wave 1 splits delivery into #53a and #53b.
- [#54][i54]: measured numbers reach the scoreboard and a planted performance regression fails a PR.
- [#55][i55]: two runs on one tree produce identical JSON and an increased count fails.
- [#56][i56]: a planted architecture violation fails and structure work shrinks the allowlists.
- [#57][i57]: the dispatched `Mutants` workflow (once before a tag; no schedule, the owner's word of 2026-10-25 and 2026-10-27) publishes mutation scores per file, with the issue's floor enforced.
- [#58][i58]: fuzz targets run on dispatch (once before a tag) without a crash and the corpus grows.
- [#59][i59]: a breaking minor change or a dependent build failure fails CI.
- [#60][i60]: CI writes the measured numbers into this table.

Exit: #52 AND #53a AND #53b AND #54 AND #55 AND #56 AND #57 AND #58 AND #59 AND #60.

### Phase 1: structure

- [#61][i61]: every file in the door directory is at most 800 lines, fixtures have zero diff, and mutation score does not fall.
- [#62][i62]: no resulting storage file exceeds 800 lines and fixtures have zero diff.
- [#63][i63]: the no-error-by-prefix lint passes and the CLI maps errors by type.
- [#64][i64]: each architecture invariant names the test that holds it.
- [#65][i65]: CLI stats read the counters and every door-call span carries the request id.

Exit: #61 AND #62 AND #63 AND #64 AND #65.

### Phase 2: the hot path

- [#66][i66]: the CLI bench gains an order of magnitude in spends/second with the allocation lock scope, and the split calls leave the same fixture rows as the combined call.
- [#67][i67]: the door query plan uses one index scan and the cached verdict equals the Lean function on every witness vector.
- [#68][i68]: the CLI's no-SQL-outside-storage lint passes with an empty allowlist.
- [#69][i69]: the door's benchmark p99 is unchanged with the monitor beside it.
- [#70][i70]: a filled header window removes door HTTP calls and proven-coin doors stay open in the header-service-down scenario.

Exit: #66 AND #67 AND #68 AND #69 AND #70.

### Phase 3: throughput

- [#71][i71]: the CLI throughput target holds while the outbox drains and every attempt is recorded per source.
- [#72][i72]: explorer calls per block are bounded by the re-ask schedule and proof lag stays within one block on the hermetic chain.
- [#73][i73]: the CLI policy reaches its target coin shape from one coin using these primitives alone.
- [#74][i74]: chained-spend BEEF p99 is below 10 KB and ancestor depth is at most 3.

Exit: #71 AND #72 AND #73 AND #74.

### Phase 4: fleet-facing

- [#75][i75]: the CLI's 1,000-tenant load test keeps door p99 within target and monitor lag bounded.
- [#76][i76]: each feature builds alone in CI, with the consumer requirements recorded in `docs/DEPENDENTS.md`.

Exit: #75 AND #76.

### Phase 5: hardening

- [#77][i77]: the chaos suite runs on every PR, each scenario names its charter row, and results are cited in the stack's `docs/matrix/wallet.md`.
- [#78][i78]: two builds of a tag are identical and a breaking change cannot publish as a minor.
- [#79][i79]: `RELEASING.md` records the canary and re-pin policy and every release record cites it.

Exit: #77 AND #78 AND #79.

## Never without the owner's word

Verbatim from [#80][epic]:

The public API's shape, the schema, the pins, any release, any spend from a wallet.

## The ranked research queue

Take the highest ready item, cite its line in the lane brief, and satisfy its
dependencies. Phase 0 follows wave order, including the two #53 lanes. Structure
precedes the dependent hot path: #61 unblocks #66; #63 lands with #56's lint.
The titles below are shortened. Each line names its downstream use, its stack
alignment and the issue's routing labels; `stack-charter` needs the stack's word
before landing. Later phases remain queued until their predecessor exits.

1. [#52][i52] Beta line: unblocks all beta PR gates; alignment: stack line rules; labels: `oracle`.
2. [#53a][i53] Door fixtures: unblocks #61, #62 and #66 differential proof; alignment: [storage rules][storage], [wallet evidence][wallet-evidence]; labels: `oracle`.
3. [#53b][i53] Ladder fixtures: unblocks #56, #63 and #77 verdict/request proof; alignment: [wallet evidence][wallet-evidence]; labels: `oracle`.
4. [#54][i54] Bench baseline: unblocks #66, #69 and scoreboard deltas; alignment: [T3][six-tests]; labels: `oracle`, `scoreboard`.
5. [#55][i55] Code-health ratchet: unblocks measurable #61/#62 reductions; alignment: stack gate legs; labels: `oracle`.
6. [#56][i56] Architecture tests: unblocks #63 with no-error-by-prefix enforcement; alignment: [charter boundaries][storage]; labels: `oracle`.
7. [#57][i57] Mutation floor: unblocks refactors and hot-path proof; alignment: [T6][six-tests]; labels: `oracle`.
8. [#58][i58] External-byte fuzzing: unblocks #77 hostile-input proof; alignment: [BEEF charter][beef]; labels: `oracle`, `hardening`.
9. [#59][i59] Dependents and semver: unblocks consumer-safe public changes; alignment: stack line and re-pin rules; labels: `oracle`, `dependents`.
10. [#60][i60] North-star document: unblocks measured queue selection; alignment: charter pointers; labels: `oracle`, `scoreboard`.
11. [#61][i61] Split the door: after #53a/#54/#57, unblocks #66 allocation boundary; alignment: [wallet section 5][wallet-transitions]; labels: `structure`.
12. [#62][i62] Split storage by entity: after #53a/#55, unblocks #67/#68/#69 storage work; alignment: [storage S rules][storage]; labels: `structure`.
13. [#63][i63] Typed errors: with #56 and the bsv-rs transport fault, unblocks the CLI's typed handler and #77; alignment: [broadcaster words per source][broadcaster]; labels: `structure`.
14. [#64][i64] Architecture dataflow: with #61/#62, unblocks invariant review across the hot path; alignment: wallet, storage, broadcaster and tracker sections below; labels: `structure`.
15. [#65][i65] Observability hooks: unblocks CLI metrics and measured fleet behaviour; alignment: scoreboard; labels: `structure`.
16. [#66][i66] Split allocate/sign from broadcast: after #61, unblocks the CLI's lock-scope work and #71; alignment: [wallet section 5][wallet-transitions]; labels: `hot-path`, `stack-charter`, `scoreboard`, `dependents`.
17. [#67][i67] Maintained offerable verdict: after #62, unblocks the CLI's indexed door; alignment: [Wallet.lean][wallet-lean]; labels: `hot-path`, `stack-charter`, `scoreboard`, `dependents`.
18. [#68][i68] Versioned host readers: after #62, unblocks the CLI's SQL removal; alignment: [storage rules][storage]; labels: `hot-path`, `dependents`.
19. [#69][i69] Writer discipline: after #54/#62, unblocks uncontended door p99 under monitor load; alignment: [storage S9][storage]; labels: `hot-path`, `scoreboard`.
20. [#70][i70] Local header window: unblocks door availability and shared fleet headers; alignment: [tracker transitions][tracker-transitions]; labels: `hot-path`, `stack-charter`.
21. [#71][i71] Durable broadcast outbox: after #66/#69, unblocks CLI throughput and backpressure; alignment: [broadcaster B1 to B5][broadcaster]; labels: `throughput`, `stack-charter`, `scoreboard`, `dependents`.
22. [#72][i72] Pushed proof intake: after #70, unblocks bounded explorer cost and proof lag; alignment: [tracker section 4][tracker-reask], `age_reasks`; labels: `throughput`, `stack-charter`, `scoreboard`.
23. [#73][i73] Coin-shape primitives: after #66/#68, unblocks the CLI's coin policy; alignment: [wallet R1(b)][wallet-rulings]; labels: `throughput`.
24. [#74][i74] BEEF budget at the door: after #72/#73, unblocks bounded chained spends; alignment: [BEEF charter][beef]; labels: `throughput`, `scoreboard`.
25. [#75][i75] One monitor over tenants: after #65/#69/#70/#72, unblocks CLI fleet scale; alignment: [wallet hosting][wallet-hosting]; labels: `fleet`, `scoreboard`.
26. [#76][i76] Consumer feature hygiene: after #59, unblocks independent native/wasm consumer builds; alignment: [T5][six-tests]; labels: `fleet`, `dependents`.
27. [#77][i77] Chaos suite: after ladder fixtures/#63/#70, unblocks witnessed fault handling across hosts; alignment: [wallet witnesses][wallet-witnesses]; labels: `hardening`, `stack-charter`.
28. [#78][i78] Reproducible signed releases: after #59, unblocks checked release artifacts; alignment: stack rule 2, a checkable pin; labels: `hardening`.
29. [#79][i79] Canary and re-pin policy: after #52/#59/#78, unblocks staged adoption and rollback; alignment: stack line and admission rules; labels: `hardening`, `dependents`.

## The loop

1. Select the highest ready queue line and record its issue, dependencies and exit test.
2. Read the pinned charter sections and apply [T1 to T6][six-tests] to the proposed change.
3. Work in the assigned lane from `origin/beta` under [the lane rules](lanes/README.md).
4. Record the base commit and run the relevant fixtures, ratchets and bench baseline.
5. Change one concern; a behaviour change carries its fixture change in its own commit.
6. Return the change, command/output proof, fixture diff and bench delta, marking unrun claims [D].
7. Run fmt, all-target clippy, library tests and the issue's exit checks with warnings denied.
8. Have a different-model review lane read the diff cold; hot paths also need differential proof, mutation floor and adversarial review.
9. The captain lands the PR into beta on its gates, obtaining every reserved owner's word.
10. Run nightly mutation, fuzz and benches; update the table, use the CLI beta canary and the LOW/witness-wallet soak, then re-rank the queue.

## Pointers at pins

The stack references below are at `13e1fa5d748b44da29e8dbf575bf9894825dfebc`.
Use [wallet section 2][wallet-evidence], [section 5][wallet-transitions],
[section 6][wallet-hosting], [section 7][wallet-spec] and [section 9][wallet-witnesses];
[storage section 2 (S rules, including S9)][storage];
[broadcaster section 1 (B1 to B5)][broadcaster];
[tracker section 4][tracker-reask] and [section 5][tracker-transitions];
and [Wallet.lean][wallet-lean] for the definition. The [six tests][six-tests]
and [L2 lane addendum][l2] govern the alignment review.

Programme sources: [toolbox #60][i60] and [#80][epic], the CLI's [#77][cli77]
document brief and [#106][cli106] programme. At the local refs read on 2026-10-23,
the CLI document was absent from both `origin/beta` (`206d6d3cdec165e043caf9395a310980021f444f`)
and `origin/main`; #77 is the specified fallback. The CLI's
[lane shape at its beta pin][cli-lanes] was read.

[epic]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/80
[i52]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/52
[i53]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/53
[i54]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/54
[i55]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/55
[i56]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/56
[i57]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/57
[i58]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/58
[i59]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/59
[i60]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/60
[i61]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/61
[i62]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/62
[i63]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/63
[i64]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/64
[i65]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/65
[i66]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/66
[i67]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/67
[i68]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/68
[i69]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/69
[i70]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/70
[i71]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/71
[i72]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/72
[i73]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/73
[i74]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/74
[i75]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/75
[i76]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/76
[i77]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/77
[i78]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/78
[i79]: https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/79
[cli77]: https://github.com/Calhooon/bsv-wallet-cli/issues/77
[cli106]: https://github.com/Calhooon/bsv-wallet-cli/issues/106
[cli-lanes]: https://github.com/Calhooon/bsv-wallet-cli/blob/206d6d3cdec165e043caf9395a310980021f444f/docs/lanes/README.md
[six-tests]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/BSV-NORTH-STAR.md#4-the-six-tests
[l2]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/lanes/l2-README.md
[wallet-evidence]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#2-the-evidence-rule
[wallet-transitions]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#5-the-transitions
[wallet-hosting]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#6-hosting
[wallet-spec]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#7-the-lean-specification
[wallet-witnesses]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#9-the-witnesses
[wallet-rulings]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/wallet.md#10-the-rulings-asked-of-the-owner
[wallet-lean]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/lean/Wallet.lean
[storage]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/storage.md#2-the-rules
[broadcaster]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/broadcaster.md#1-the-rules
[tracker-reask]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/tracker.md#4-the-re-ask-policy
[tracker-transitions]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/tracker.md#5-the-transitions
[beef]: https://github.com/Calhooon/bsv-stack-lean/blob/13e1fa5d748b44da29e8dbf575bf9894825dfebc/docs/charters/beef-of-any-size.md#3-the-streaming-reader-in-bsv-rs-nl-3-through-the-gate
