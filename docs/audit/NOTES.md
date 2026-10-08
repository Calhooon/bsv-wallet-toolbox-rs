# Lens: lane/toolbox-rejected-final (zanaadu-v2 #357)

- Gates on the merge: fmt exit 0; clippy --all-targets -D warnings exit 0; cargo test exit 0 (lib 863 passed, 2 ignored; every integration binary ok). Logs: fmt.log, clippy.log, test.log.
- Reference (~/bsv/wallet-toolbox TaskArcSSE.ts:152-178): SSE REJECTED -> req 'invalid' + tx 'failed', DOUBLE_SPEND_ATTEMPTED -> 'doubleSpend', unconditionally; no cross-source weighing. Backstop: TaskCheckForProofs attempts > unprovenAttemptsLimitMain (144) -> 'invalid' for ANY unmined req; TaskUnFail recovers only via getMerklePath.
- Rust backstop gap: synchronize_transaction_statuses counts attempts only for reqs the triage calls mined, so an 'unmined' req nobody holds never reaches 144 and is never failed.
- Lens replay (pre-correction, passed = the defect is real): GorillaPool 'seen' row aged 6 h, every live source unknown, Arcade REJECTED x3 -> applied false each time; 200 synchronize passes -> req unmined attempts 0, tx unproven, change spendable; un_fail does not touch it. Same for a 466 conflict. => locked forever.
- Correction: correction-memory-evidence-window.diff (2 h window on memory evidence; live reads decide past it) plus 4 tests; 867 lib tests pass, clippy clean with it.
- Soak DB (read-only): incident tx 374 (1.9 MB) has both inputs spent_by. The head row without spent_by is tx 370 (2 inputs, 1 spent_by): input 0's parent tx has no row in storage (known only from inputBEEF; input_beef now NULL); input 1 (storage change) is marked. No release path cleared anything.
- CLI: ~/bsv/bsv-wallet-cli/Cargo.toml pins version "0.4.0" (caret, ^0.4.0) -> 0.4.1 is in range with no edit; Cargo.lock pins 0.4.0, so `cargo update -p bsv-wallet-toolbox-rs` is needed.
