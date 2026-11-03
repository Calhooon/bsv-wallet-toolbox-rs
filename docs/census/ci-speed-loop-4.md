Loop four of the CI speed line (#638, #637), split from the record [ci-speed-2026-11-02.md](ci-speed-2026-11-02.md) (#670).

## Loop four, the loopback transport (#683, the CLI's #526)

The CLI's gate-vector tests (9.2 s), its four served-BEEF door tests (10.4 to 12.0 s) and wire's six corpus replays (10 to 15.6 s) on the runner, against 1.2 to 1.4 s each locally (the CLI's #526, read from its PR 521 log). The toolbox's row: a `bench-support` loopback transport that answers the explorer rungs from the test's own table with no socket, the texts a host sees the socket transport's or each difference named here.

### The seam

- **Where.** `src/services/providers/loopback_table.rs`, a child of the transport's `http_client.rs` under `cfg(any(test, feature = "bench-support"))`. Production (`not(any(test, feature = "bench-support"))`) re-exports reqwest's `Client`, `RequestBuilder`, `Error` and `Response` as before.
- **The table.** `LoopbackTable::new()`, then `table.on(method, host, path, answer)`: the host is the URL's host or `host:port`; the path is the URL's path exactly, its path and query when the row's has a `?`, or a prefix when it ends in `*`. Rows are tried in the order added. `LoopbackAnswer::status(code, body)`, `LoopbackAnswer::json(code, &value)`, `.with_header(name, value)`, and `LoopbackAnswer::SendFault`. An unmatched request is the send fault. `table.requests()` is every request sent, `METHOD url`, in order.
- **How a test installs it.** `table.building(|| Services::with_options(...))`: every client built inside the closure (on that thread, as `SharedClients::building` shapes it, #634) answers from the table wherever it is used later. A host reaches it as `bsv_wallet_toolbox_rs::benchmark_support::{LoopbackTable, LoopbackAnswer}` with the `bench-support` feature.
- **No socket for any host.** A table client never calls `check_url`, since nothing it sends leaves the process; a client built outside `building` keeps the socket transport and the #182 refusal (`a_client_built_outside_the_table_keeps_the_check`).
- **The response.** No `http` crate is a dependency and `reqwest::Response` has no constructor without it (`reqwest::Response::from(Default::default())` is E0283), so the seam's `send` returns `http_client::Response`, an enum of reqwest's response and the table's, with reqwest's own methods (`status`, `headers`, `text`, `json`, `bytes`, `bytes_stream`, `error_for_status`). Four production lines that named `reqwest::Response` now name `http_client::Response`, the same type in production (`arc.rs`, `whatsonchain.rs`, `chaintracks_client.rs`, `services.rs`); chaintracks' `Fault` takes the seam's `RequestError` under the cfg. `services.rs` named `reqwest` once (that annotation), so its ceiling in `src/architecture_tests.rs`'s HTTP scan comes down to none. `collection.rs` is unchanged: the ladders' failover, pacing and call history run over the table as over a socket.

### The texts table

Witnessed by `cargo test --lib --features sqlite,remote,bench-support loopback_table -- --nocapture`, each row one mock server read through the socket and then through the table at the same `host:port` (txids elided).

| case | the socket's text | the table's | witness |
|---|---|---|---|
| a raw tx, every rung 500 | `WhatsOnChain: Service error: WoC getRawTx failed with status 500 Internal Server Error; BananaBlocks: Service error: BananaBlocks getRawTx failed with status 500 Internal Server Error; Bitails: Service error: Bitails getRawTx failed with status 500 Internal Server Error` (`could_not_look: true`) | identical | `a_raw_tx_every_rung_500_reads_the_same` |
| the tip, chaintracks 500 (wire 085's shape) | `get_height: no header service gave the tip height (Service error: Chaintracks findChainTipHeaderHex failed: HTTP 500 Internal Server Error); no explorer is asked (set chaintracks_url or bhs_url)` | identical | `a_header_service_500_reads_the_same` |
| a height, chaintracks 500 (wire 084's shape) | `get_header_for_height: no header service answered (chaintracks: Service error: Chaintracks findHeaderHexForHeight failed: HTTP 500 Internal Server Error)` | identical | `a_header_for_height_500_reads_the_same` |
| reqwest's `error_for_status` (`/getChain` 503) | `HTTP error: HTTP status server error (503 Service Unavailable) for url (http://127.0.0.1:<port>/getChain)` | identical | `reqwest_status_and_decode_texts_read_the_same` |
| reqwest's decode failure (`/getChain` 200 `{not json`) | `HTTP error: error decoding response body` | identical | the same |
| an ARC 465, every result's status, data and service flag | `arcTaal 465 [("465", Some("ARC rejected transaction: fee too low (HTTP 465) - {...}"), false)]`, then Bitails' and WhatsOnChain's send faults | identical (the notes' `when` is the clock's, left out) | `an_arc_refusal_reads_the_same` |
| a send fault: a refused connection (port 9) | `error sending request for url (http://127.0.0.1:9/refused)` | identical (the unmatched request) | `a_send_fault_reads_as_a_refused_connection`, `the_socket_faults_are_one_text_and_the_table_gives_it` |
| a timeout (a listener that never answers, 200 ms) | `error sending request for url (http://127.0.0.1:<port>/silent)` | identical | the same |
| a TLS fault (https at a plain listener) | `error sending request for url (https://127.0.0.1:<port>/tls)` | identical | the same |
| a DNS fault (a resolver that answers nothing) | `error sending request for url (http://dns-fault.test/x)` | identical | the same |

reqwest 0.12's Display is the kind and the URL, never its source, so a refused connection, a timeout, a TLS and a DNS fault read one text, and the table's unmatched request reads it. The differences, each named:

| difference | the socket | the table | a fixture it moves |
|---|---|---|---|
| no table installed, `bench-support` on | a non-loopback host: production's `error sending request for url (<url>)` | `#182 rule 28: test/bench transport refuses non-loopback host <host>; use a loopback fixture URL` (no table: the existing #182 seam, unchanged) | any CLI test whose build turns `bench-support` on and does not build its `Services` inside `building`, the CLI binary its subprocess tests run included (cargo unifies a dev-dependency's features into that build) |
| a client built at call time, outside `building` | as above | the #182 text: the fiat-rate client (`fetch_fiat_exchange_rates`), `ArcadeSseClient::new`, the BRC-104 peer transport (`sdk_transport_client`), and any lookup resolver or storage client built outside the closure | a fixture reading fiat rates, Arcade's event stream, or an authenticated storage or issuance peer |
| the error's source chain and `is_connect`/`is_timeout` | reqwest's error with its hyper source | `RequestError::Loopback(text)`, no source | none: the crate reads neither, and a host sees the text in `Error::HttpError(String)` or a provider's own wrap of it |
| a redirect | followed under the #182 policy | a recorded 3xx is the answer | none in the named sets |
| a body cut short, a slow body | `error decoding response body` or `request or response body error` | no form: a body is whole | the toolbox's ladder replays (`ladder_cut.rs`, `delay_ms` scenes) stay on mockito |
| a delayed answer, a client timeout | the client's timer | no timer; a timeout's text is `SendFault` | the ladder stall reading stays on mockito |
| a charset other than UTF-8 | reqwest decodes by `Content-Type` | UTF-8 with replacement, a leading BOM dropped | none |
| the per-test seconds | the socket's | below | |

Per fixture class the CLI names (the CLI's tree read at `origin/beta` 9e99a33, not built here):

| fixture class | how it builds its services | through the table | texts that move |
|---|---|---|---|
| gate vectors (`tests/gate_vectors.rs`) | `MockWalletServices`; no transport | nothing to answer; its 9.2 s is the 2 s `header_retry_pause` of `GateOptions::default()` (the CLI's `changelog.d/506.md`), not an explorer | none |
| served-BEEF doors (`tests/served_beef_doors.rs`) | `Services::with_options(Chain::Main, ServicesOptions::mainnet())` in process; its header says no header service and no broadcaster is asked | build it inside `building`; any rung that is asked reads the table's answer, or the send fault, the socket's text for a refused or timed-out host | none for a rung the table answers as recorded; a rung left unmatched reads `error sending request for url (<url>)`, the runner's own text for its timeouts |
| wire's six corpus replays (`tests/wire.rs`: `wire`, `a_recording_is_not_a_replay`, the four knob tests) | `wire_support::services()` in process; chaintracks at a loopback stub, ARC at `arc_stub` or a closed port, WhatsOnChain and Bitails at the closed proxy | build it inside `building`, the stubs' answers as rows | none where the stubs' answers become rows: 084's and 085's chaintracks 500 texts are the two header rows above; 169's `transport fault MAY_HAVE_BEEN_RECEIVED: header fixture unavailable` is the tracker fixture's, not the transport's; WhatsOnChain and Bitails unmatched read the closed proxy's text |
| `tests/json_refusal.rs` | the `bsv-wallet` binary, a subprocess | the in-process table cannot reach it | none: the gate refuses before any service is asked; if `bench-support` is on in the binary's build, a request that did leave would read the #182 text (the no-table row of the differences) |
| the toolbox's services tests | mockito, unchanged in this row | not rewritten | none |

### The seconds

The texts tests time each half on one mock server (three runs, `--test-threads=1`, on the lane's Mac):

| case | the socket | the table |
|---|---|---|
| a raw tx, every rung 500 (three requests) | 857 to 875 µs | 55 to 63 µs |
| the tip, chaintracks 500 | 316 to 321 µs | 38 to 39 µs |
| a height, chaintracks 500 | 645 to 666 µs | 60 to 63 µs |
| `/getChain` 503 | 266 to 307 µs | 32 to 38 µs |
| `/getChain` decode failure | 260 to 298 µs | 31 to 33 µs |
| an ARC 465 (three rungs) | 675 to 719 µs | 192 to 213 µs |

Each rung test (`the_whatsonchain_rung_answers_from_the_table`, `the_bitails_rung_answers_after_two_send_faults`, `the_arc_rung_accepts_from_the_table`, `the_bhs_rung_answers_the_height_after_chaintracks_faults`, `the_chaintracks_rung_answers_the_height`) reads 0.00 s alone (`cargo test --exact`, three runs each), as do the mockito equivalents `a_raw_tx_429_moves_to_bananablocks`, `a_raw_tx_all_three_lack_is_not_found` and `get_height_is_the_header_services_tip_and_asks_no_explorer` (`mining_context_uses_one_tip_and_one_range_with_cached_network` 0.01 s). On loopback the socket already costs under a millisecond a request here; the CLI's ten seconds a test are the runner's TLS and real timeouts to hosts no fixture serves, which the table removes. The runner's after figure is the CLI's row (#526, step 3), not measured here.

The CLI is told this table at the landing.

## Loop four, cut (2): the slot occupancy by hour (#638)

The account's twenty hosted runners, read from the Actions API across its repositories over a week, by hour of the day (UTC). The tool is `tools/slot-occupancy` (python, `gh api` reads only), its cases `tools/slot-occupancy-test` on the recorded fixture `tools/fixtures/slot-occupancy/` (fourteen jobs across two repositories and three hours, each cell worked by hand and the whole output against `expected.md`). The `checks` job's cases step is not changed by this row; a later row wires `tools/slot-occupancy-test` in if the keeper wants it.

### The method and the call count

- **The repositories.** `gh repo list Calhooon` lists 21; ten had runs in the window: bsv-wallet-toolbox-rs, bsv-rs, bsv-wallet-cli, bsv-overlay-cloudflare, bsv-tracker, bsv-wallet-infra-cloudflare, bsv-middleware-rs, bsv-script-lean, bsv-wallet-store, bsv-courier. Four of those (infra-cloudflare, script-lean, wallet-store, courier) ran before the jobs window only. bsv-low is `Calgooon/bsv-low`, under the user account, not the `Calhooon` organisation, so it draws on a separate pool; it ran nothing since 2026-10-27, so it is left out.
- **The window.** Runs created from 2026-10-27T09:00Z to 2026-11-03T09:00Z (seven days, 3,376 runs), listed in one-day slices (a listing query returns 1,000 runs at most; a slice above that is halved). Jobs for the runs created in the last three days (from 2026-10-31T09:00Z, 12,251 jobs), since a call per run for the whole week is about 3,400 calls. The four days before count only by their run-level times (the `run-min` column).
- **The calls.** 1,539 pages, each one `gh api` call, cached as JSON under `$TMPDIR/slot-occupancy/` (not in the tree): 874 in the run that finished, the rest in the run before it, which was stopped (the cache carried the pages over). About 40 more probe reads went to the repository list and the rate limit. The lane stayed under 2,000 calls.
- **A slot.** A job holds a slot from `started_at` to `completed_at`. Left out: jobs no runner took (2,925, `runner_name` empty: 2,207 skipped, 718 cancelled while still queued, whose `started_at` is their `created_at` and whose `completed_at` is the cancel; counting them read peaks of 36 against a pool of 20) and self-hosted jobs (none). The **queue wait** is a job's `started_at` minus its `created_at` (when the job was queued), bucketed by the hour it was queued. The brief's run `created_at` would count a `needs` chain as queue. A run is **heavy** if its event is `schedule` or its name matches mutants, matrix, fuzz, coverage, bench, dependents or nightly.
- **Re-runs.** The jobs endpoint is read with its default `filter=latest`, so a re-run run lists its latest attempt's jobs only and an earlier attempt's jobs hold no slot here. The cache holds the latest attempts alone (112 of the 12,251 jobs are attempt 2 of 9 runs, and no run's jobs come from two attempts), so `filter=all` cannot be measured from it. The tool counts the runs it misses: 9 runs in the jobs window are at `run_attempt` 2 (13 in the week), so the figures below undercount by those runs' earlier attempts.

```
python3 tools/slot-occupancy --now 2026-11-03T09:00:00Z --days 7 --jobs-days 3 \
  --repos bsv-wallet-toolbox-rs,bsv-rs,bsv-wallet-cli,bsv-overlay-cloudflare,bsv-tracker,bsv-wallet-infra-cloudflare,bsv-middleware-rs,bsv-script-lean,bsv-wallet-store,bsv-courier
python3 tools/slot-occupancy --from-cache $TMPDIR/slot-occupancy    # the same tables, no call
```

Every figure below is that `--from-cache` run's (1.6 s, `api calls this run 0`, 1,539 pages): the hour table, the concurrency table and its three lines, the per-repository table, the heavy table and the two closing lines, pasted.

### The hour table (total)

| hour (UTC) | jobs | runner-min | mean slots | peak | non-heavy peak | heavy peak | wait median | wait worst | pr/queue wait median | pr/queue wait worst | run-min |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 00 | 567 | 3405 | 18.9 | 20 | 17 | 15 | 7:10 | 36:22 | 7:25 | 36:22 | 2547 |
| 01 | 511 | 2599 | 14.4 | 20 | 15 | 12 | 1:06 | 10:24 | 1:32 | 10:24 | 1838 |
| 02 | 401 | 2218 | 12.3 | 20 | 16 | 12 | 0:12 | 5:36 | 0:17 | 4:19 | 1764 |
| 03 | 319 | 2008 | 11.2 | 20 | 17 | 12 | 0:02 | 186:19 | 0:02 | 2:50 | 1463 |
| 04 | 326 | 1671 | 9.3 | 20 | 19 | 11 | 0:02 | 53:21 | 0:02 | 53:21 | 1608 |
| 05 | 298 | 2086 | 11.6 | 20 | 17 | 19 | 0:02 | 49:41 | 0:02 | 47:14 | 1896 |
| 06 | 410 | 1654 | 9.2 | 20 | 16 | 9 | 0:02 | 183:20 | 0:02 | 2:37 | 1877 |
| 07 | 271 | 997 | 5.5 | 20 | 15 | 6 | 0:02 | 0:30 | 0:02 | 0:30 | 1698 |
| 08 | 221 | 1515 | 8.4 | 20 | 11 | 13 | 0:03 | 412:05 | 0:02 | 4:44 | 1780 |
| 09 | 455 | 2236 | 12.4 | 30 | 24 | 12 | 0:02 | 147:42 | 0:02 | 8:48 | 1886 |
| 10 | 232 | 1485 | 8.3 | 20 | 17 | 6 | 0:02 | 0:22 | 0:02 | 0:22 | 1668 |
| 11 | 377 | 1530 | 8.5 | 20 | 18 | 6 | 0:02 | 2:04 | 0:02 | 2:04 | 1490 |
| 12 | 336 | 1440 | 8.0 | 20 | 16 | 7 | 0:02 | 131:47 | 0:02 | 0:21 | 1819 |
| 13 | 358 | 1612 | 9.0 | 19 | 15 | 6 | 0:02 | 0:56 | 0:02 | 0:56 | 1947 |
| 14 | 390 | 2273 | 12.6 | 20 | 20 | 20 | 0:04 | 88:53 | 0:05 | 88:53 | 2200 |
| 15 | 286 | 2159 | 12.0 | 20 | 20 | 18 | 0:03 | 44:13 | 0:18 | 44:13 | 1895 |
| 16 | 338 | 1268 | 7.0 | 20 | 20 | 6 | 0:02 | 131:40 | 0:02 | 6:53 | 1297 |
| 17 | 288 | 1345 | 7.5 | 20 | 17 | 15 | 0:03 | 125:15 | 0:03 | 4:22 | 1232 |
| 18 | 288 | 1366 | 7.6 | 20 | 17 | 12 | 0:02 | 6:56 | 0:02 | 6:56 | 1427 |
| 19 | 450 | 1693 | 9.4 | 20 | 20 | 12 | 0:03 | 494:38 | 0:03 | 16:14 | 1468 |
| 20 | 525 | 2594 | 14.4 | 20 | 20 | 12 | 4:44 | 457:30 | 4:56 | 38:52 | 2211 |
| 21 | 507 | 2474 | 13.7 | 20 | 15 | 12 | 9:16 | 31:15 | 9:44 | 31:15 | 2578 |
| 22 | 531 | 2466 | 13.7 | 20 | 16 | 13 | 1:41 | 13:28 | 1:40 | 13:28 | 2158 |
| 23 | 621 | 2720 | 15.1 | 20 | 18 | 15 | 1:06 | 147:55 | 1:48 | 27:08 | 2123 |

pr/queue wait, queued while a heavy job held a slot: median 0:38, worst 88:53 (4326 jobs); while none did: median 0:02, worst 3:16 (1621 jobs)

The pr/queue columns hold pull request and merge group jobs only. The worst waits of 400 minutes and more (hours 08, 19, 20) are the CLI nightly's own Mutants shards waiting behind their own matrix, not on the pool.

### The concurrency by hour (total)

Per minute, sampled at each minute's start. The non-heavy percentiles are nearest rank over the hour's minutes with a non-heavy job running; `minutes at 20+` are the minutes with twenty or more jobs running; `heavy-held` the minutes with a heavy run's job running (of 180 an hour); the peaks by OS read the job's runner label.

| hour (UTC) | runner-min | peak | non-heavy p50 | non-heavy p75 | non-heavy p90 | non-heavy p95 | minutes at 20+ | heavy-held minutes | ubuntu peak | windows peak | macOS peak |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 00 | 3405 | 20 | 8 | 13 | 14 | 14 | 119 | 180 | 20 | 4 | 3 |
| 01 | 2599 | 20 | 8 | 13 | 14 | 14 | 54 | 180 | 20 | 0 | 0 |
| 02 | 2218 | 20 | 6 | 12 | 14 | 14 | 48 | 127 | 20 | 0 | 2 |
| 03 | 2008 | 20 | 2 | 9 | 14 | 14 | 22 | 120 | 20 | 0 | 0 |
| 04 | 1671 | 20 | 5 | 9 | 14 | 15 | 15 | 120 | 20 | 4 | 4 |
| 05 | 2086 | 20 | 4 | 6 | 9 | 11 | 55 | 120 | 20 | 3 | 2 |
| 06 | 1654 | 20 | 4 | 8 | 13 | 14 | 8 | 164 | 20 | 0 | 0 |
| 07 | 997 | 20 | 2 | 5 | 10 | 11 | 3 | 83 | 20 | 0 | 0 |
| 08 | 1515 | 20 | 6 | 6 | 8 | 9 | 12 | 86 | 20 | 0 | 0 |
| 09 | 2236 | 30 | 6 | 9 | 13 | 17 | 31 | 120 | 29 | 4 | 4 |
| 10 | 1485 | 20 | 4 | 6 | 10 | 12 | 1 | 120 | 19 | 3 | 2 |
| 11 | 1530 | 20 | 3 | 6 | 9 | 12 | 5 | 120 | 20 | 0 | 0 |
| 12 | 1440 | 20 | 4 | 7 | 10 | 14 | 2 | 120 | 20 | 0 | 0 |
| 13 | 1612 | 19 | 4 | 6 | 10 | 12 | 0 | 120 | 19 | 0 | 0 |
| 14 | 2273 | 20 | 4 | 11 | 14 | 15 | 59 | 120 | 20 | 0 | 2 |
| 15 | 2159 | 20 | 4 | 7 | 11 | 14 | 66 | 119 | 20 | 2 | 2 |
| 16 | 1268 | 20 | 8 | 14 | 16 | 18 | 18 | 63 | 17 | 6 | 4 |
| 17 | 1345 | 20 | 5 | 8 | 10 | 14 | 22 | 60 | 20 | 2 | 2 |
| 18 | 1366 | 20 | 4 | 8 | 8 | 8 | 28 | 60 | 20 | 0 | 0 |
| 19 | 1693 | 20 | 8 | 10 | 13 | 17 | 30 | 60 | 20 | 0 | 0 |
| 20 | 2594 | 20 | 8 | 9 | 10 | 12 | 97 | 114 | 20 | 0 | 0 |
| 21 | 2474 | 20 | 8 | 14 | 14 | 14 | 105 | 120 | 20 | 0 | 0 |
| 22 | 2466 | 20 | 8 | 14 | 14 | 15 | 80 | 132 | 20 | 0 | 0 |
| 23 | 2720 | 20 | 11 | 14 | 14 | 15 | 46 | 147 | 20 | 4 | 4 |

```
over the 4320 minutes of the jobs window: 20 or more jobs running 926 minutes (21%); mean slots held 10.8; a heavy job running 2775 minutes; non-heavy per minute over the 3939 minutes with a non-heavy job: p50 5, p75 9, p90 14, p95 14; above 6 (20 less a heavy run at 14) in 1692 minutes, 43% of those minutes
peak by OS: ubuntu 29 at 2026-11-01T09:40Z, windows 6 at 2026-11-01T16:44Z, macOS 4 at 2026-10-31T23:34Z
minutes above 20: 14, 2026-11-01T09:40Z; 2026-11-01T09:42Z to 09:54Z; the worst 2026-11-01T09:51Z, 30 jobs: 22 ubuntu, 4 windows, 4 macOS
```

The twenty slots were all in use for 926 of the 4,320 minutes (21%); the mean was 10.8 slots held; a heavy run's job ran in 2,775 minutes (64%). The non-heavy concurrency per minute, over the 3,939 minutes with a non-heavy job: p50 5, p75 9, p90 14, p95 14. It was above six (twenty less a heavy run at fourteen) in 1,692 of those minutes (43%; 39% of all 4,320). The only minutes above twenty are 14 on 2026-11-01 (09:40Z, and 09:42Z to 09:54Z), the worst 09:51Z at 30 jobs: 22 ubuntu, 4 windows, 4 macOS. That is not explained here (the pool section below).

### The pool's size by OS

- **The plan.** `gh api orgs/Calhooon --jq .plan.name` reads `free` [X].
- **GitHub's documented limits** (docs.github.com, Actions limits, the standard GitHub-hosted runner table) [D]: Free 20 total concurrent jobs, at most 5 of them macOS; Pro 40 and 5; Team 60 and 5; Enterprise 500 and 50. The page says the macOS maximum is shared between standard and larger runners; it names no separate windows limit, and it does not say in words that macOS and windows jobs count inside the total. The table's shape (a total, and a macOS maximum beside it) reads as one pool of 20 for every OS with macOS capped at 5 inside it.
- **The measured peaks by OS** [X] (the concurrency table's last three columns): ubuntu 29 (2026-11-01T09:40Z; 20 or less in every hour but 09), windows 6 (2026-11-01T16:44Z), macOS 4 (2026-10-31T23:34Z, under the plan's 5). Windows and macOS jobs ran in hours 00, 02, 04, 05, 09, 10, 14 to 17 and 23; in each of those hours but 09 the total peak is 20, never above. In hour 16 ubuntu peaked at 17 beside windows 6 and macOS 4 and the total at 20. That fits one shared pool; it is not a proof of one, since an hour's peaks by OS need not fall in the same minute.
- **The 14 minutes above 20** are not windows or macOS jobs outside the pool: ubuntu alone reached 29 (09:40Z) and was 22 at the worst minute (09:51Z, 30 jobs). They stay unexplained. In them bsv-rs's CI ran its macOS and windows cells beside its nightly of 10-31 23:31Z (cancelled at 09:50Z), and from 09:42Z the next nightly, dispatched at 09:42Z, ran beside both.
- **What the cap formula assumes.** One pool of 20 that every OS draws on, the documented reading and the one the measured peaks fit. The number does not hang on it: the non-heavy p95 of the ubuntu jobs alone is also 14 (the cap line below), so a pool of 20 for ubuntu alone gives the same 4.

### Per repository (runner-min / peak / wait median; the jobs window's empty columns dropped)

| hour (UTC) | bsv-middleware-rs | bsv-overlay-cloudflare | bsv-rs | bsv-tracker | bsv-wallet-cli | bsv-wallet-toolbox-rs |
| --- | --- | --- | --- | --- | --- | --- |
| 00 | 0 / 0 / - | 346 / 9 / 3:59 | 1267 / 14 / 7:06 | 0 / 0 / - | 1633 / 14 / 7:27 | 159 / 6 / 7:58 |
| 01 | 0 / 0 / - | 208 / 9 / 0:30 | 976 / 14 / 1:05 | 0 / 0 / - | 1209 / 15 / 0:38 | 206 / 7 / 3:19 |
| 02 | 0 / 0 / - | 160 / 9 / 0:22 | 764 / 14 / 0:29 | 0 / 0 / - | 1130 / 18 / 0:14 | 164 / 8 / 0:02 |
| 03 | 0 / 0 / - | 115 / 7 / 0:02 | 745 / 14 / 0:02 | 0 / 0 / - | 970 / 16 / 0:02 | 178 / 8 / 0:05 |
| 04 | 0 / 0 / - | 126 / 7 / 0:02 | 1028 / 19 / 0:02 | 0 / 0 / - | 433 / 12 / 0:02 | 84 / 5 / 0:02 |
| 05 | 0 / 0 / - | 152 / 8 / 0:02 | 1540 / 19 / 0:02 | 0 / 0 / - | 358 / 13 / 0:02 | 37 / 5 / 12:16 |
| 06 | 0 / 0 / - | 47 / 4 / - | 1014 / 15 / 0:02 | 0 / 0 / - | 498 / 12 / 0:02 | 95 / 5 / 0:02 |
| 07 | 0 / 0 / - | 0 / 0 / - | 774 / 20 / 0:02 | 0 / 0 / - | 205 / 15 / 0:02 | 18 / 5 / 0:01 |
| 08 | 0 / 0 / - | 0 / 0 / - | 975 / 12 / 0:02 | 0 / 0 / - | 415 / 18 / 1:33 | 125 / 9 / 0:02 |
| 09 | 0 / 0 / - | 298 / 8 / 0:02 | 1263 / 29 / 0:02 | 0 / 0 / - | 652 / 18 / 0:03 | 23 / 7 / 0:02 |
| 10 | 0 / 0 / - | 402 / 16 / 0:02 | 651 / 18 / 0:02 | 0 / 0 / - | 373 / 12 / - | 60 / 7 / 0:02 |
| 11 | 0 / 0 / - | 295 / 17 / 0:02 | 517 / 11 / 0:02 | 0 / 0 / - | 383 / 12 / 0:52 | 335 / 9 / 0:02 |
| 12 | 0 / 0 / - | 309 / 13 / 0:02 | 527 / 12 / 0:02 | 0 / 0 / - | 384 / 11 / 1:27 | 219 / 15 / 0:02 |
| 13 | 0 / 0 / - | 258 / 10 / 0:02 | 763 / 14 / 0:02 | 0 / 0 / - | 359 / 6 / - | 231 / 9 / 0:02 |
| 14 | 0 / 0 / - | 137 / 6 / 1:47 | 786 / 15 / 0:02 | 0 / 0 / - | 548 / 14 / 0:02 | 801 / 15 / 1:09 |
| 15 | 0 / 0 / - | 190 / 6 / 0:14 | 501 / 19 / 0:02 | 16 / 10 / 0:02 | 382 / 17 / 0:03 | 1071 / 19 / 0:40 |
| 16 | 0 / 0 / - | 43 / 7 / 0:02 | 865 / 20 / 0:02 | 4 / 1 / 6:09 | 197 / 7 / 0:02 | 159 / 10 / 0:02 |
| 17 | 0 / 0 / - | 53 / 8 / 0:02 | 747 / 14 / 0:02 | 10 / 5 / 3:16 | 304 / 9 / 0:02 | 230 / 8 / 0:03 |
| 18 | 2 / 1 / 0:02 | 132 / 8 / 0:02 | 630 / 15 / 0:02 | 2 / 1 / 0:02 | 395 / 10 / 0:07 | 204 / 7 / 0:38 |
| 19 | 0 / 0 / - | 179 / 9 / 0:02 | 521 / 19 / 0:02 | 0 / 0 / - | 692 / 16 / 0:26 | 302 / 10 / 0:11 |
| 20 | 0 / 0 / - | 175 / 10 / 9:34 | 579 / 11 / 1:42 | 0 / 0 / - | 1487 / 18 / 6:55 | 353 / 11 / 0:09 |
| 21 | 0 / 0 / - | 240 / 7 / 14:39 | 840 / 12 / 9:00 | 0 / 0 / - | 1139 / 15 / 11:16 | 254 / 10 / 3:38 |
| 22 | 0 / 0 / - | 257 / 8 / 3:58 | 813 / 10 / 1:28 | 0 / 0 / - | 1228 / 15 / 1:28 | 168 / 10 / 0:40 |
| 23 | 0 / 0 / - | 325 / 10 / 1:21 | 914 / 16 / 0:23 | 0 / 0 / - | 1210 / 15 / 3:29 | 271 / 8 / 0:12 |

### The heavy runs

The jobs window's heavy runs by workflow (a heavy run is a `schedule` run or one whose name matches mutants, matrix, fuzz, coverage, bench, dependents or nightly; `hours` are the hours the jobs held a slot; `run starts` the runs' `created_at`):

| repository | workflow | event | runs | jobs | hours (UTC) | runner-min | run starts (UTC) |
|---|---|---|---|---|---|---|---|
| bsv-wallet-cli | Nightly | workflow_dispatch | 5 | 226 | 00,01,02,03,04,05,06,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23 | 10850 | 10-31 23:36, 11-01 17:11, 11-01 19:09, 11-02 08:33, 11-02 20:05 |
| bsv-rs | Nightly oracles | workflow_dispatch | 6 | 218 | 00,01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23 | 9039 | 10-31 23:31, 11-01 09:17, 11-01 09:42, 11-01 16:41, 11-01 20:28, 11-03 04:42 |
| bsv-wallet-toolbox-rs | Mutants | workflow_dispatch | 1 | 17 | 14,15 | 1494 | 11-01 14:18 |
| bsv-wallet-toolbox-rs | Dependents | workflow_dispatch | 3 | 3 | 00,22 | 37 | 10-31 22:24, 11-01 21:53, 11-02 00:05 |

The runs behind it (created in the week, `created_at` to `updated_at`):

- **bsv-rs Nightly oracles** (dispatched): 23:31Z on 10-31 (to 09:50Z, cancelled), 09:17Z and 09:42Z on 11-01 (cancelled), 16:41Z (to 23:52Z), 20:28Z (to 07:11Z next day), 04:42Z on 11-03 (to 07:10Z). Six to eleven hours each.
- **bsv-wallet-cli Nightly** (dispatched, its fifty Mutants shards inside): 23:36Z on 10-31 (to 00:55Z), 17:11Z on 11-01 (cancelled 19:26Z), 19:09Z (to 05:00Z), 08:33Z on 11-02 (to 16:46Z), 20:05Z (to 06:21Z on 11-03). Eight to ten hours each.
- **This repository:** Mutants at 14:18Z on 11-01 (to 15:59Z, cancelled; 17 jobs, 1,494 runner-minutes, heavy peak 20 in hour 14), Mutants at 04:45Z on 10-28 (before the jobs window), Dependents eight times (two to twenty minutes each), Bench once.
- **Never in the jobs window:** fuzz, matrix, coverage, any `schedule` run.

### The reading

- **No quiet hour.** The peak reaches twenty in every hour but 13 (19). By the brief's measure (a peak under six) none is quiet. By mean slots held, the lightest hours are 07 (5.5), 16 (7.0), 17 (7.5), 18 (7.6), 12 (8.0) and 10 (8.3). The hottest are 00 (18.9; PR median wait 7:25), 23 (15.1), 01 and 20 (14.4), 21 and 22 (13.7; 21's PR median wait 9:44). This matches the keeper's waits of the night: 18:03 at 21:58Z, 16:19 at 23:57Z, 10:49 at 00:30Z.
- **The heavy runs are the queue.** The two nightlies were dispatched at seven different hours and ran six to eleven hours each, so a heavy run held slots in 2,775 of the 4,320 minutes (the `heavy-held` column: hours 00 and 01 all 180 minutes; 02 to 15 between 119 and 164, except 07 and 08 at 83 and 86; 16 to 19 between 60 and 63; 20 to 23 between 114 and 147). A PR or queue job queued while a heavy job held a slot waited a median of 0:38, worst 88:53 (4,326 jobs). With none running: median 0:02, worst 3:16 (1,621 jobs). This repository's Mutants at 14:18Z took all twenty slots (heavy peak 20, before `max-parallel: 14` landed), and hour 14's PR worst wait is 88:53.
- **Fourteen is too many while PR and queue work goes on.** Fourteen heavy jobs plus a non-heavy p95 of 14 is 28 against 20. With a heavy run at fourteen, PR and queue jobs queue whenever their own load passes six, which was 1,692 of the 3,939 minutes with a non-heavy job (43%). The cap the evidence gives is twenty minus (the non-heavy p95 of 14 plus a margin of 2), which is **4**. The non-heavy p95 per hour stays between 8 (hour 18) and 18 (hour 16), so no hour allows more than ten (20 minus 8 minus 2), and that is hour 18 alone; the next is hour 08 (p95 9) at nine.
- **The stagger slots are in hot hours.** 00:00Z is the hottest hour of the day. 03:00Z (mean 11.2, non-heavy p95 14) and 06:00Z (9.2, p95 14) are middling. And a slot cannot hold a heavy run that lasts eight hours: a run dispatched at 00:00Z is still running at 08:00Z.

### The recommendation

Every dispatched or scheduled heavy run (the bsv-rs and CLI nightlies, this repository's Mutants, fuzz, matrix) carries `max-parallel: 4`, not 14: the measured non-heavy p95 of 14 plus a margin of 2, of 20 slots. A heavy dispatch starts in the lightest hours, not at 00:00Z: from 07:00Z (mean 5.5 slots, non-heavy p95 11) for the longest run, with the evening 17:00 to 18:00Z (mean 7.5 and 7.6, p95 14 and 8) for a second. Hour 16 has the evening's fewest runner-minutes (mean 7.0) but the day's highest non-heavy p95 (18), so a heavy run at four there meets twenty-two. Two heavy runs never overlap. At four shards, a heavy run takes about 3.5 times as long as at fourteen (the CLI's fifty-shard Mutants grows from eight hours toward a day). That cost is the keeper's to weigh against the 0:38 median and 88:53 worst PR wait measured under heavy load. The other way to buy the same headroom is to cut the nightlies' shard count or frequency (two dispatches a day each in this window).

`recommendation: max-parallel 4 (non-heavy p95 14 + margin 2 of 20 slots; non-heavy peak 24); quiet hours none; lightest hours by runner-min 07,16,17,18; stagger 00:00Z hot, 03:00Z hot, 06:00Z hot` (the tool's line, verbatim), and the cap's inputs on one line:

`cap: max-parallel = slots 20 - (non-heavy p95 14 over 3939 minutes with a non-heavy job + margin 2) = 4; ubuntu jobs alone: non-heavy p95 14; per-hour non-heavy p95 from 8 (18) to 18 (16)`

### The five figures

The queue wait by hour is this row's figure (the table above: the PR and queue median from 0:02 in the day to 9:44 at 21Z, worst 88:53; 0:38 under a heavy run against 0:02 without one). The others are unchanged.

## Loop four, cut (1): the landing latency (#637)

The fifth figure, from the lane's DONE to the PR's merge, per landing and per stage, for every pull request merged into beta from 2026-11-02 21:00Z to 10:29:55Z on 2026-11-03: 27, of which 17 are code landings and 10 are folds. Measured by `tools/landing-latency --since 2026-11-02T21:00:00Z` (reads only: one search page and one timeline page per PR, 28 `gh api` calls at 10:33Z, every page cached under `$TMPDIR/landing-latency/`; the cca run records on the Studio), its cases in `tools/landing-latency-test` on a recorded fixture of seven landings and three folds (48 cases). Base `20357f00`.

### The method

The marks of a landing, in order: the lane's DONE (its run record's `ended` on the Studio, `rec`; else the end time in the header of the captain's "The lane's REPORT (<lane>, ..., HH:MMZ to HH:MMZ; ...)" comment, `hdr`, [D] from the captain; an `x` minute digit reads as 0, `hdr~`; else the PR's open, a lower bound, `open`, the total then marked `>=`); the open (`created_at`); the earliest read's start and end (its review lane's record, else its header; a read with no times ends at its comment's time); ready (the latest end of any read or fix round); the enqueue (`auto_merge_enabled` before the queue's add, else the add); the queue's add (the earliest `added_to_merge_queue`); the merge (`merged_at`). The stages are named by the mark that ends them: open (DONE to open: the captain's turn, the push and the PR), wait (open to the read), read, fix (the read's end to ready: fix lanes, re-reads, the captain's hand), post (ready to the enqueue: the REPORTs posted, the rebase, the row), ci (the enqueue to the add: the PR's own run), queue (the add to the merge: the group run and any removal inside it). A stage whose start mark is absent spans from the last present mark and is starred; the seven landings before the queue (#641 is the queue's opening landing) have no enqueue or add, so their queue* runs from ready to the merge and holds the landing script's polls and the PR's own run. A mark earlier than the one before it (minute-resolution headers) is held at it. A removal is a `removed_from_merge_queue` that is not the merge's own. A fold is the next PR titled "HANDOFF: the rows landed" or "Census:" that names the landing; its figure runs from the landing's merge to the fold's, outside the total. The HANDOFF row's commit (its author date) sits 0.1 minutes after the open in every landing that has one (2.6 for #639): the round-two rule holds.

The lanes before 03:20Z ran on the box and their REPORT headers carry no times, so their DONE is the open's lower bound; the keeper's samples in #637 [D] (#611 about 30, #621 36, #623 59, #626 about 95, #630 25) sit beside these bounds (#630's 25 is under its open-to-merge 33.3, so that sample measured a shorter span).

### The landings

Minutes; `-` absent, `*` spans the absent stages before it.

| PR | DONE (source) | open | wait | read | fix | post | ci | queue | total | fold |
|---|---|---|---|---|---|---|---|---|---|---|
| #592 | 14:21 (open) | - | - | 6.9* | 419.3 | - | - | 26.6* | >=452.8 | #610 +69.1 |
| #611 | 23:04 (open) | - | - | 11.3* | 0.0 | - | - | 20.1* | >=31.4 | #620 +20.8 |
| #621 | 23:43 (open) | - | - | 4.4* | 0.0 | - | - | 28.6* | >=33.1 | #622 +27.3 |
| #623 | 00:23 (open) | - | - | 18.8* | 0.1 | - | - | 37.8* | >=56.6 | #628 +17.7 |
| #626 | 00:30 (open) | - | - | 5.0* | 0.0 | - | - | 80.6* | >=85.6 | #629 +11.6 |
| #630 | 02:01 (open) | - | - | 12.4* | 0.0 | - | - | 20.9* | >=33.3 | #640 +10.2 |
| #639 | 02:20 (open) | - | - | 6.6* | 0.0 | - | - | 36.6* | >=43.2 | - |
| #641 | 02:56 (open) | - | - | 13.5* | 0.0 | 0.6 | 12.6 | 7.2 | >=34.0 | - |
| #645 | 03:20 (hdr) | 16.3 | 0.0 | 2.7 | 0.0 | 2.7 | 8.6 | 8.4 | 38.7 | #646 +3.0 |
| #648 | 03:51 (hdr) | 26.3 | 0.0 | 11.7 | 0.0 | 0.8 | 0.0 | 8.4 | 47.2 | - |
| #650 | 05:40 (hdr) | 0.6 | 0.9 | 27.4 | 0.0 | 0.8 | 0.0 | 4.9 | 34.6 | #653 +2.5 |
| #654 | 06:26 (open) | - | 0.2 | 1.0 | 2.2 | 1.6 | 6.2 | 8.8 | >=20.1 | - |
| #661 | 07:40 (hdr~) | 12.9 | 1.1 | 13.0 | 0.0 | 0.6 | 0.0 | 5.6 | 33.2 | #662 +2.8 |
| #663 | 08:26 (rec) | 0.6 | 0.1 | 1.9 | 0.0 | 0.9 | 3.9 | 4.9 | 12.5 | - |
| #664 | 08:36 (rec) | 0.5 | 0.2 | 13.5 | 0.0 | 1.1 | 4.9 | 5.6 | 25.8 | - |
| #667 | 10:02 (open) | - | 0.2 | 1.5 | 2.3 | 0.9 | 4.7 | 5.5 | >=15.1 | - |
| #665 | 09:30 (rec) | 26.9 | 0.2 | 2.2 | 12.0 | 0.7 | 2.6 | 15.1 | 59.7 | - |

#592 had five reads and four fix rounds; #623 three reads; #648 one fix lane (16.0 of its open stage, 03:54Z to 04:1xZ, before the PR opened); #654 and #667 two reads each (a lens and a narrow re-read, the captain's hand between); #665 a fix lane (7.2) and a narrow re-read, and one removal: out of the queue at 10:17:45Z as #667 landed, re-added 6.8 minutes later. The folds' own open to merge: #608 53.0, #610 65.7, #620 18.8, #622 24.0, #628 13.4, #629 8.2, #640 8.1, #646 2.1, #653 1.5, #662 1.5.

### The stages across the night

| stage | n | median | worst |
|---|---|---|---|
| open (DONE to open) | 7 | 12.9 | 26.9 (#665) |
| wait | 9 | 0.2 | 1.1 (#661) |
| read | 9 | 2.7 | 27.4 (#650) |
| fix | 17 | 0.0 | 419.3 (#592) |
| post | 10 | 0.8 | 2.7 (#645) |
| ci (the PR's own run) | 10 | 4.3 | 12.6 (#641) |
| queue (the group run) | 10 | 6.4 | 15.1 (#665) |
| total (DONE to merge) | 7 | 34.6 | 59.7 (#665) |
| fold (merge to the fold's merge) | 9 | 11.6 | 69.1 (#592) |

The share of each stage in the seven landings timed from a DONE (251.7 minutes): open 33%, read 29%, queue 21%, ci 8%, fix 5%, post 3%, wait 1%. The reads by kind: a no-cargo lens 1.0 to 2.7 (#645, #654, #663, #665, #667); a standard read with the suites 11.7 to 27.4 (#648, #650, #661, #664). The folds since the round-two rule (#645 on) 2.5 to 3.0 from the landing's merge, a docs-only run each; before it 10.2 to 69.1.

### The three largest single costs

1. #592, fix, 419.3 (from comment times): the lane's hazard put to the owner at 14:23Z (#594, a v1 key file with no password check), then three fix rounds, each with a narrow re-read (posted 20:00Z, 20:56Z, 21:28Z). A hold for the owner's word, not a cost the landing practice cuts.
2. #626, queue*, 80.6: the landing's rebase conflicted in this record with #622's fold, which had filled #612's after row while the branch wrote the same rows (the captain's comment at 01:42:40Z); resolved by hand; held last by design ([D], #637).
3. #623, queue*, 37.8: one red row of the architecture doc gate at CI, a row naming a test the PR removed; no read ran that gate (the comment at 01:03:29Z); fixed by the captain's hand and landed again.

Among the landings timed from a DONE: #650's read 27.4 (the standard read with every suite, built cold on the Studio since the lane's target sat on the box), #665's open 26.9 (the record's DONE 09:30:14Z to the open 09:57:07Z; the cause is the captain's own lag: the run record's DONE of 09:30:14Z is the right one, and the 09:5xZ in the lane's REPORT line was the captain's reading time after the profile relaunch of 09:31Z, so the 26.9-minute open stage stands), #648's open 26.3 (the fix lane's 16.0 inside it).

### The cuts

- **The row at open.** One removal in the 13 landings through the queue, folds included (#641 on): #665, at the second #667 landed, 6.8 minutes to the re-add and a queue stage of 15.1 against the median 6.4. The row is the cause [X]: #665's row alone, written on its base `b2fb3f99`, against #667's tip `20357f00` gives `git merge-tree` a conflict in `HANDOFF.md` and nowhere else (both insert under the same heading at the same blank line). A heading per PR does not stop it [X]: in a scratch repository two PRs each inserting `### PR #<n>` and a row at one place conflict; one file per row merges clean. No README sentence: the sentence the brief offered (a heading per PR) would not hold. A row for the captain: a row file per PR (`docs/handoff/<n>.md`, HANDOFF.md's section naming the directory), or keep the enqueue script's re-check [D]; at one removal in 13 the re-check's 6.8 minutes is the cost either way.
- **The read.** A lens read costs 1 to 3 minutes, a standard read 12 to 27; the standard reads in the queue era were #648 (the KDF's concurrency in `src/`), #650 (the slowest tests), #661 (dependencies and the gate) and #664 (an example's `required-features` in `Cargo.toml` and one CI flag, 13.5 minutes). A row for the keeper: the classes that need the suites are a `src/` change and a dependency change; build metadata and a workflow flag are proven by the PR's own run, as loop one's item 6 already says for a workflow change. The ensemble rule is the owner's; this is the measure for it.
- **The DONE to open.** The largest share (33%, median 12.9; it rests on #665's run-record DONE of 09:30:14Z, the right one, as the landings' note says): 0.5 to 0.6 minutes where the captain opened the PR and launched the read at the DONE's wake (#650, #663, #664), 12.9 to 26.9 where it waited (#645, #648, #661, #665). A row for the captain: the push, the PR with its row and the read's launch in one step at the wake, before the REPORT is read through.
- **The fix rounds.** #648's fix lane (16.0) moved a knob onto an additive method since a public API shape is the owner's word; rule 6 already says it, and the brief could have named the method's shape. #665's fix lane (7.2) and re-read cost 12.0 on the lens read's four findings. #654 and #667 took 2.2 and 2.3 by the captain's hand with a narrow re-read. No row beyond the brief's sentence.
- **The fold.** Since the round-two rule a fold costs 2.5 to 3.0 minutes from the landing's merge (a docs-only run and the queue), against 10.2 to 69.1 before it. Folding the after figures into the next code PR saves those minutes and one push run on beta; the rule as it stands (a fold only where a gate or a record needs the landing's runs) already keeps folds rare. No change.
- **The queue's own wait by hour.** The ci stage (median 4.3, worst 12.6 for #641, the queue's opening landing) and the queue stage (6.4, 15.1) here; loop four's cut (2) above gives a job's wait beside a heavy run 0:38 median and 88:53 worst against 0:02 and 3:16 beside none. Whether a heavy run overlapped tonight's queue-era landings is not measured here; a row: the next heavy dispatch's landings re-measured with this tool beside #638's table.

### The five figures

| figure | value | source |
|---|---|---|
| a PR's CI wall clock, the queue wait | the ci stage median 4.3 (worst 12.6) over ten queued landings, three of them 0.0 (#648, #650, #661: their own run overlapped the read and the timeline holds no auto-merge event), so 4.9 over the seven nonzero; the queue stage median 6.4 (worst 15.1) | the timelines' enqueue, add and merge |
| the nightly's length | none | no scheduled workflow |
| a cold read's length | a lens 1.0 to 2.7, a standard read 11.7 to 27.4 | the review records and the REPORT headers |
| the landing latency | median 34.6 minutes over the seven landings timed from a DONE (12.5 #663 to 59.7 #665); the open 33% of it, the read 29%, the queue 21% | `tools/landing-latency`, above |
