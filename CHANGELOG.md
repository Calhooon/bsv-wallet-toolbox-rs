# Changelog

## [0.4.0] - 2026-10-08

### Changed, breaking

- No tracker, no proof: with no chain tracker attached, every path that stores a merkle proof refuses it (`ProofIngestOutcome::TrackerUnavailable`); proofs already stored unchecked are checked once on the next read and demoted if the tracker refutes them (a new table records the checks, created on open). The BEEF builder's own proof fetch and compaction follow the same rule.
- No explorer in the proof path: WhatsOnChain is no longer asked for headers unless `break_glass_explorer_headers` is set (off by default, every call logged at warn), and never overrules the configured header service.
- One verdict per Arcade client: `txStatus` decides on a 2xx (REJECTED is a failure, an unknown word an invalid response, IMMUTABLE serves the proof, 476 is retryable, 466 is a conflict), and the reorg words `reorg_reanchor` and `reorg_unmined` schedule a re-check instead of changing a status.

(bsv-stack-lean #35, #48, #49.)

