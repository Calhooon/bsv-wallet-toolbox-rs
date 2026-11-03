window: runs 2026-10-27T00:00:00Z to 2026-11-03T00:00:00Z (7 days), jobs from 2026-10-31T00:00:00Z (3 days)
repositories: Calhooon/alpha, Calhooon/beta
runs 6, jobs 14, left out: self-hosted 1, no runner (skipped, or cancelled queued) 2; api calls this run 0, pages in the cache 8 (one call each)
re-run runs in the jobs window (run_attempt above 1) 1: the jobs endpoint's default filter=latest lists their latest attempt's jobs only

## Total

| hour (UTC) | jobs | runner-min | mean slots | peak | non-heavy peak | heavy peak | wait median | wait worst | pr/queue wait median | pr/queue wait worst | run-min |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 00 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 01 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 02 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 03 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 04 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 05 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 06 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 07 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 08 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 09 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 10 | 3 | 85 | 0.5 | 2 | 2 | 0 | 2:00 | 4:00 | 2:00 | 4:00 | 90 |
| 11 | 6 | 190 | 1.1 | 6 | 2 | 4 | 1:30 | 15:00 | 12:30 | 15:00 | 95 |
| 12 | 2 | 56 | 0.3 | 2 | 1 | 2 | 1:30 | 3:00 | - | - | 56 |
| 13 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 14 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 15 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 16 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 17 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 18 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 19 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 20 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 21 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 22 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |
| 23 | 0 | 0 | 0.0 | 0 | 0 | 0 | - | - | - | - | 0 |

pr/queue wait, queued while a heavy job held a slot: median 12:30, worst 15:00 (2 jobs); while none did: median 2:00, worst 4:00 (3 jobs)

## Concurrency by hour

| hour (UTC) | runner-min | peak | non-heavy p50 | non-heavy p75 | non-heavy p90 | non-heavy p95 | minutes at 20+ | heavy-held minutes | ubuntu peak | windows peak | macOS peak |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 00 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 01 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 02 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 03 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 04 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 05 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 06 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 07 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 08 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 09 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | 85 | 2 | 1 | 2 | 2 | 2 | 0 | 0 | 2 | 0 | 0 |
| 11 | 190 | 6 | 1 | 2 | 2 | 2 | 0 | 59 | 5 | 1 | 1 |
| 12 | 56 | 2 | 1 | 1 | 1 | 1 | 0 | 45 | 2 | 0 | 0 |
| 13 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 14 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 15 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 16 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 17 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 18 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 19 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 20 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 22 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 23 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

over the 4320 minutes of the jobs window: 20 or more jobs running 0 minutes (0%); mean slots held 0.1; a heavy job running 104 minutes; non-heavy per minute over the 103 minutes with a non-heavy job: p50 1, p75 2, p90 2, p95 2; above 6 (20 less a heavy run at 14) in 0 minutes, 0% of those minutes
peak by OS: ubuntu 5 at 2026-11-02T11:02Z, windows 1 at 2026-11-02T11:25Z, macOS 1 at 2026-11-02T11:20Z
minutes above 20: none

## Per repository (runner-min / peak / wait median)

| hour (UTC) | alpha | beta |
|---|---|---|
| 00 | 0 / 0 / - | 0 / 0 / - |
| 01 | 0 / 0 / - | 0 / 0 / - |
| 02 | 0 / 0 / - | 0 / 0 / - |
| 03 | 0 / 0 / - | 0 / 0 / - |
| 04 | 0 / 0 / - | 0 / 0 / - |
| 05 | 0 / 0 / - | 0 / 0 / - |
| 06 | 0 / 0 / - | 0 / 0 / - |
| 07 | 0 / 0 / - | 0 / 0 / - |
| 08 | 0 / 0 / - | 0 / 0 / - |
| 09 | 0 / 0 / - | 0 / 0 / - |
| 10 | 85 / 2 / 2:00 | 0 / 0 / - |
| 11 | 160 / 5 / 1:00 | 30 / 2 / 12:30 |
| 12 | 1 / 1 / - | 55 / 2 / 1:30 |
| 13 | 0 / 0 / - | 0 / 0 / - |
| 14 | 0 / 0 / - | 0 / 0 / - |
| 15 | 0 / 0 / - | 0 / 0 / - |
| 16 | 0 / 0 / - | 0 / 0 / - |
| 17 | 0 / 0 / - | 0 / 0 / - |
| 18 | 0 / 0 / - | 0 / 0 / - |
| 19 | 0 / 0 / - | 0 / 0 / - |
| 20 | 0 / 0 / - | 0 / 0 / - |
| 21 | 0 / 0 / - | 0 / 0 / - |
| 22 | 0 / 0 / - | 0 / 0 / - |
| 23 | 0 / 0 / - | 0 / 0 / - |

## Heavy runs

| repository | workflow | event | runs | jobs | hours (UTC) | runner-min | run starts (UTC) |
|---|---|---|---|---|---|---|---|
| alpha | Mutants | workflow_dispatch | 1 | 4 | 11,12 | 146 | 11-02 11:00 |
| beta | Nightly | schedule | 1 | 1 | 12 | 45 | 11-02 12:00 |

recommendation: max-parallel 16 (non-heavy p95 2 + margin 2 of 20 slots; non-heavy peak 2); quiet hours 00,01,02,03,04,05,06,07,08,09,10,12,13,14,15,16,17,18,19,20,21,22,23; lightest hours by runner-min 00,01,02,03; stagger 00:00Z quiet, 03:00Z quiet, 06:00Z quiet
cap: max-parallel = slots 20 - (non-heavy p95 2 over 103 minutes with a non-heavy job + margin 2) = 16; ubuntu jobs alone: non-heavy p95 2; per-hour non-heavy p95 from 1 (12) to 2 (10,11)
