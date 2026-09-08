# Reorg handling: the divergence register

The reference implementation is the TypeScript `@bsv/wallet-toolbox`
(`~/bsv/ts-stack/packages/wallet/wallet-toolbox/src`): `monitor/tasks/TaskNewHeader.ts`,
`monitor/tasks/TaskReorg.ts`, `monitor/tasks/TaskReviewProvenTxs.ts`,
`monitor/tasks/TaskCheckForProofs.ts`, `storage/WalletStorageManager.ts`
(`reproveHeader`, `reproveProven`) and `storage/StorageProvider.ts`
(`handleProvenTxBranch`). This crate conforms to it wherever the reference
decides something; every place it decides differently is a row here, with the
why. A row is deleted only by re-converging on the reference, never by deleting
the code.

Conformed (no row needed): the first header is queued and processed only after
it stayed the tip for a full cycle; a lower height is an "old header", reverted,
never a reorg; a same-height hash change queues the new header and deactivates
the old one; proofs are accepted only up to the processed height (the LAG gate:
nothing at all while no header has been processed, as the reference's undefined
`maxAcceptableHeight`); a validated provider proof for a different block UPDATES
the stored row in place; "unchanged" and "unavailable" are retained and retried,
aged 10 minutes per try, 3 tries, then "maximum retries exceeded" and the original
retained; the BEEF walk skips a stored bump the tracker definitely refutes and
recurses through the raw transaction, touching no storage, and a tracker error
keeps the bump for the final verification.

| # | Divergence | Reference behavior | Ours, and why | Where |
|---|---|---|---|---|
| R1 | **Demotion on positive evidence** | Never demotes: a stale proof whose replacement is unavailable is retained after 3 tries, and the row stays `completed` | A stored proof is DEMOTED (reverted to the pre-proof state in ONE transaction: request `unmined` with the bytes, transaction `unproven` and still spendable, the `mined` broadcast memory forgotten, the proof row deleted last) when the chain POSITIVELY refutes it: the tracker answers a definite false for the stored root AND at least two providers answered cleanly "not mined" AND no provider served a path. Faults (429, timeout, 5xx, tracker errors) and a served path the tracker refutes are retained and retried exactly as the reference does. Why: a retained stale proof refuses every spend that touches it (2026-09-07: 28 fleet seats, `createAction` 400 for hours); the reference's callers tolerate `WERR_INVALID_MERKLE_ROOT` with `skipInvalidProofs`, ours could not build a valid BEEF around the row at all | `monitor/reorg_ops.rs` `reprove_anchor` (`DEMOTION_WITNESSES`); `storage/sqlx/storage_sqlx.rs` `demote_stale_proof_on`; `broadcast_seen.rs` `forget_mined_on` (the one sanctioned ladder downgrade) |
| R2 | **The CLI's `tip - 1` gate** (`bsv-wallet reproof --execute`) | The gate is only ever a header that survived a full monitor cycle | The heal verb reads the persisted gate and, when it is 0 or below `tip - 1`, sets it to `tip - 1` after one header read: "one block on top" instead of "survived a cycle", so a wallet whose daemon is not running can re-anchor at buried heights right away. The dry run never writes it. Why: the verb exists for a wallet that is not running its monitor; a cycle never comes | `bsv-wallet-cli/src/commands/reproof.rs` |
| R3 | **Ring-based deactivation** | `Monitor.processReorg(depth, oldTip, newTip, deactivatedHeaders)` is pushed by a chaintracks that emits reorg events; without one, only the same-height hash change the header task itself observes is named | The header task keeps a ring of the last 12 observed `(height, hash)` tips; on every tip move it re-reads the header service at each remembered height from the newest down and deactivates every remembered hash that no longer matches, stopping at the first match or the first unreadable height (an unknown is never a reorg). Why: our chaintracks pushes no reorg event, and a fork deeper than one block (A@H replaced by A'@H under B'@H+1) is invisible to the same-height check alone | `monitor/reorg_ops.rs` `HeaderTracker::deactivate_mismatched`, `HEADER_RING_LEN`; `monitor/tasks/new_header.rs` |
| R4 | **The review window** | `TaskReviewProvenTxs`: at least 100 blocks below the tip (`minBlockAge`), up to 100 heights per run from a persisted checkpoint, so every height is eventually audited exactly once | A fresh window of the last 12 heights below the proof gate every 10 minutes, no checkpoint; also the net for a one-shot process whose reorg queue does not outlive it, and the backfill of a stored proof's empty block hash. Why: the proofs a reorg can touch are the recent ones; a hundred-deep sweep on every wallet of a fleet is header-service load for rows a reorg cannot reach. A proof older than 12 blocks that turns stale is caught by the walk's raw-leg recursion at spend time | `monitor/tasks/review_proven_txs.rs` `REVIEW_HEIGHTS` |
| R5 | **The persisted gate and tracker state** | The gate and the queued header live on the monitor object; one process, one monitor | `monitor_state` (migration 004): the gate is ONE row every `StorageSqlx` instance and every process on the same file reads on the same connection as the proof write, and the header task's tip/queue/ring is persisted beside it. Why: the daemon holds two storage instances (the monitor's and the wallet's, which the webhook and the relay poller ingest on), and `bsv-wallet tick` is a new process per cycle; an in-memory gate was 0 = open forever on the second instance and never opened for `tick`. Consequence: `tick` opens the gate on its second run; the deactivated-header queue stays per process (R4 is its net) | `storage/sqlx/monitor_state.rs`, `migrations/004_monitor_state.sql` |
| R6 | **Provider verdicts in the notes** | `getMerklePath` returns the first proof or nothing; the per-provider history lives in `ReqHistoryNote`s | The ladder writes one note per provider outcome (`getMerklePathInvalidRoot`, `getMerklePathTrackerError`, `getMerklePathHeaderUnresolved`, `getMerklePathBadProof`, `getMerklePathError`, beside the providers' own `NotFound`/`NoData`/`NotMined`/`BadStatus`/`ServiceError` notes) and `GetMerklePathResult::provider_verdicts()` reads them back so R1 can tell a clean negative from a fault. Same shape, one more use | `services/traits.rs` `ProviderVerdict`; `services/services.rs` |
| R7 | **The fallback tracker's outage answer** | The reference's `ChainTracker` has no fallback; `isValidRootForHeight` throws on a service error | `FallbackChainTracker`: chaintracks `Ok(true)` is true; chaintracks' definite `Ok(false)` is confirmed with WoC and stands alone when WoC fails; chaintracks `Err` defers to WoC and is `Err` when WoC fails too. Before 0.3.66 a double outage answered `Ok(false)`, which every caller read as "the chain refutes this root". Now an outage is an error, which every caller already fails closed on | `services/providers/fallback_chain_tracker.rs` |

## What each caller does with a tracker `Err` (the F2b audit, 0.3.66)

- `storage/sqlx/internalize_action.rs` `validate_bump_claim`: `Err` is "not proven": the BUMP is not stored, the transaction is `unproven` with an `unmined` request. Fails closed, never credits a proof.
- `storage/sqlx/beef_verification.rs` `verify_beef_merkle_proofs` / `verify_txid_merkle_proof`: `Err` becomes `Error::ValidationError`: the incoming BEEF is refused. Fails closed (the same status as before, a clearer message).
- `storage/sqlx/create_action.rs` `validate_stored_beef`: `Err` discards the stored `input_beef` and falls through to the per-transaction lookup and the network fallback. No storage mutation.
- `storage/sqlx/create_action.rs` `fetch_and_store_merkle_path`: `Err` returns `None`: the fetched proof is not stored, the walk continues raw. No storage mutation.
- `storage/sqlx/create_action.rs` `stored_bump_refuted` (the walk): `Err` is not a refutation: the stored bump stays attached and the final BEEF verification decides. No storage mutation.
- `storage/sqlx/create_action.rs` final BEEF verification in `build_input_beef`: `Err` becomes `Error::ValidationError`: the spend is refused rather than sent with an unverifiable BEEF. Fails closed, as before.
- `storage/sqlx/storage_sqlx.rs` `ingest_merkle_proof`: `Err` is `ProofIngestOutcome::TrackerError`: not stored; the polling path counts an attempt and retries next cycle, the push paths fall back to the fetch.
- `services/services.rs` `get_merkle_path` (Layer 3): `Err` is a FAULT note for that provider and the next provider is tried; with no provider left the result carries no path and the faults, which `reprove_anchor` reads as "retain, retry", never as evidence.
