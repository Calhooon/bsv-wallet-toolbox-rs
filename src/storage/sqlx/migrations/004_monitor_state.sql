-- 004: monitor state (0.3.66)
--
-- monitor_state: the small persisted facts the monitor's tasks share across
-- every StorageSqlx instance and every process opened on the same file.
--
--   max_acceptable_proof_height (value): the proof LAG gate, the highest
--     block height whose merkle proofs may be stored. Absent or 0 = CLOSED:
--     every proof is deferred (never stored, never an attempt) until the
--     header task has watched a header stay the chain tip for a full cycle,
--     exactly as the reference processes nothing while its
--     maxAcceptableHeight is undefined. One row, one point read on the same
--     connection as the proof write, so the daemon's webhook and relay
--     instances and the CLI are bounded by the same gate as the monitor.
--
--   header_tracker (text_value, JSON): the header task's last tip, queued
--     header and the ring of recent tips, so a one-shot process
--     (`bsv-wallet tick`) conforms across runs: the gate opens on the run
--     AFTER the one that first saw the tip.
--
-- Every statement is IF NOT EXISTS: applied on migrate() AND on
-- make_available(), so a database created before this file gets the table
-- the next time it is opened. No ALTER TABLE.

CREATE TABLE IF NOT EXISTS monitor_state (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL,
    text_value TEXT,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
