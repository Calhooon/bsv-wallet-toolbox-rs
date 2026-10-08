# Lens of record: zanaadu-368/proof-attempt-backstop (e64e863), zanaadu-v2 #368

Verdict: ACCEPT WITH CORRECTIONS (two patches, both against the three-way merged tree 34288e4;
`corrections-1+2-combined.patch` applies with `git apply`, gates green with it: fmt, clippy -D warnings, 884 lib).

## Gates
- Lane merged on main (e29f013): fmt ok; clippy ok; cargo test 21 result lines, 1170 passed, 0 failed, 38 ignored (lib 867).
- Three-way (34288e4 = lane + p0-1d/beef-walk-sibling-bump + zanaadu-357/rejected-final): one CHANGELOG conflict, resolved into
  one `[0.4.1] - 2026-10-08` heading (357's bullets, then 368's, Tests, "(Calgooon/zanaadu-v2#357, #368.)").
  fmt ok; clippy ok; cargo test 21 result lines (lib + every integration binary + doctests), 1186 passed, 0 failed, 39 ignored (lib 883).

## Rate (item 2)
TS: `countsAsAttempt = TaskCheckForProofs.checkNow`, set only by `Monitor.processNewBlockHeader`; the time fallback is commented out.
So 144 attempts is about 144 blocks, about 24 h on mainnet.
Rust: check_for_proofs polls every 60 s and runs on (a) the shared flag raised by new_header when a header is PROCESSED (about once per block),
(b) the same flag raised by ArcadeEventsTask on MINED (when the inline proof is not ingested, normally the case since the block is
above the processed header), IN_BLOCK and reorg_unmined, (c) the 2 h fallback, plus check_no_sends (daily, on nosend proof) and
`run_once`/`bsv-wallet tick`. Every one of those passes counts on this branch.
- No Arcade (classic ARC): about 1 pass per block, so about 24 h. Matches the reference.
- Daemon with Arcade (the soak default): an active wallet gets a MINED word per block it has a tx in, landing a minute or more before the
  header is processed, so about 2 passes per block. 144 is then about 12 h. With MINED words every minute the floor is 144 x 60 s = 2.4 h.
  A daily `tick` cron adds more.
Matching rule: count at most one attempt per processed header height. See correction-2: a persisted `proof_attempt_height` in
monitor_state; a pass counts only when the gate rose above it. The write-off check still runs every pass, as in the TS where the
`attempts > limit` test is not gated on countsAsAttempt.

## Write-off (item 3)
retire_undeliverable_tx: one live status read (known/mined means Alive, nothing touched); else req invalid with attempts 0,
tx failed, own unspent outputs unspendable, each input released only on is_utxo true; broadcast memory marked rejected-quiet; and
poisoned descendants retired.
For a tx held only by a mempool none of our sources see: is_utxo reads the same blind sources, so the inputs ARE released, and the next
create_action can re-spend them. If the hidden copy mines first, our new spend is a double spend (a payment to someone fails; no funds
lost). The hidden tx's change sits unspendable until the canary sees it. The canary (un_fail Phase A) only RECOVERS: when a source says
known/mined it restores the req (unmined, attempts 0), re-enables outputs on is_utxo and re-marks inputs spent, skipping an input
already recorded against a competing spend. With no word it re-stamps (attempts+1). It never re-writes-off, and the sync query does not
read `invalid` reqs, so a req is written off ONCE and inputs are released once, not hourly. Residual: a recovered req that then goes
unheld again gets a fresh 144-attempt budget. That is bounded and correct.

## Composition (item 4)
correction-1: drop BACKSTOP_MEMORY_EVIDENCE_MAX_AGE_SECS and fresh_memory_evidence. Use #357's `memory_holds(txid, None, false)`
(same semantics: fresh seen/mined, a bare accepted does not count), then `network_holds(services, txid, None, false, false)`, which
adds each broadcaster's own status read (Arcade GET) that the status sources do not cover. ask_status is false because this pass
already asked and retire asks again; ask_proof is false because the fetch loop owns proofs.
`sending`: keep it uncounted. send_waiting_transactions uses the same `attempts` column as its re-broadcast budget and ends `sending`
through the same release rule, so counting both would halve one budget or the other. The TS counts sending; this is a stated divergence,
not a hole.

## Removed arm (item 5)
The old arm (mined at depth >= 1, no proof, attempts >= 144 meant req `invalid`, tx left `unproven`) only bounded getMerklePath calls.
It left an incoherent row: the canary reads invalid only when the tx is `failed`, so the req was never asked again and the tx stayed
`unproven` forever anyway. After removal a mined tx with a permanently missing or refused proof stays `unproven`, outputs spendable,
and is re-asked every pass forever. Under "no tracker, no proof" that is the right outcome: the tx IS mined, so failing it would be
false, and a proof is never fabricated. The cost is unbounded provider calls per such req (one per header with correction-2). The TS
would write it off at > 144 (and EntityProvenTx.fromReq gives up at > 8 attempts after 60 min). The held guard is a deliberate departure.

## Unverified
- The Arcade MINED-before-header ordering on the live soak (the 12 h figure is reasoned from code; no soak db or daemon was read).
- TS release-on-invalid behavior was not traced (TaskReviewStatus); only TaskCheckForProofs, EntityProvenTx.fromReq and Monitor.
- Correction-2 counts across processes through monitor_state (the CLI and the daemon share the row). Not run against a live daemon.
