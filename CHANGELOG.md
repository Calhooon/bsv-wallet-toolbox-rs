# Changelog

## [0.4.1] - unreleased

### Fixed

- The proof attempt backstop reaches every req the proof task checks. `synchronize_transaction_statuses` counted an attempt only for a req the status sources called mined, so a broadcast transaction that no source holds and no broadcaster refused stayed `unproven` with attempts 0, its change spendable and its inputs locked, forever. Now a status pass that finds no proof is an attempt, as in the TypeScript `TaskCheckForProofs`, and once `attempts` is greater than the reference's limit (`unprovenAttemptsLimitMain` 144, `unprovenAttemptsLimitTest` 10) the req goes `invalid` and the transaction `failed` through the release rule (`retire_undeliverable_tx`: one more live status read, its own outputs unspendable, each input released only on its own `is_utxo`). A req a source still holds is never written off by the count alone: the status sources say known or mined, or the broadcast memory has a seen or mined row no older than 2 hours. A status source that cannot answer, a closed proof gate and a `sending` req count nothing.
- A req the status sources call mined but that no merkle path arrives for is no longer set `invalid` at 144 attempts with its transaction left `unproven`; it keeps counting and waits for its proof.
- A written-off req restarts at attempts 0, so the auto-unfail canary asks about it hourly (it reads `attempts` as its own counter and would have asked daily).

(Calgooon/zanaadu-v2#368.)

## [0.4.0] - 2026-10-08

### Changed, breaking

- No tracker, no proof: with no chain tracker attached, every path that stores a merkle proof refuses it (`ProofIngestOutcome::TrackerUnavailable`); proofs already stored unchecked are checked once on the next read and demoted if the tracker refutes them (a new table records the checks, created on open). The BEEF builder's own proof fetch and compaction follow the same rule.
- No explorer in the proof path: WhatsOnChain is no longer asked for headers unless `break_glass_explorer_headers` is set (off by default, every call logged at warn), and never overrules the configured header service.
- One verdict per Arcade client: `txStatus` decides on a 2xx (REJECTED is a failure, an unknown word an invalid response, IMMUTABLE serves the proof, 476 is retryable, 466 is a conflict), and the reorg words `reorg_reanchor` and `reorg_unmined` schedule a re-check instead of changing a status.

(bsv-stack-lean #35, #48, #49.)

