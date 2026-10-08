-- 005: proof root checks (P0-1)
--
-- proof_root_checks: one row per transaction whose stored merkle proof had
-- its root confirmed by a ChainTracker at its height, keyed by txid and
-- carrying the (height, root) that was confirmed. A proven_txs row is a
-- CHECKED row when a record exists for its txid with its height and the root
-- its stored merkle_path computes; any other proven_txs row is UNCHECKED.
--
-- Every proof the store funnel writes is checked (the funnel is reached only
-- after the tracker confirmed the root). Unchecked rows are the ones written
-- before this file (when a storage with no tracker stored proofs as proven)
-- and the ones merged by sync from another storage. An unchecked row is
-- checked once, on the next read that puts its proof into a BEEF, and
-- demoted when the tracker refutes it. Bounded by the wallet's own rows and
-- by its reads: there is no sweep.
--
-- A separate table, not a column: every statement here is IF NOT EXISTS and
-- applied on every open, so a database created before this file gets it the
-- next time it is opened. No ALTER TABLE.

CREATE TABLE IF NOT EXISTS proof_root_checks (
    txid TEXT PRIMARY KEY,
    height INTEGER NOT NULL,
    merkle_root TEXT NOT NULL,
    checked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
