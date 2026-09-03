-- 003: locked input re-checks (0.3.59)
--
-- locked_input_checks: inputs THE RELEASE RULE kept locked because the
-- chain could not vouch for them when their spender was retired (a
-- rate-limited or inconclusive UTXO lookup). Re-examined by the reconcile
-- passes with exponential backoff (1, 2, 4 ... 64 minutes) until the chain
-- says unspent (restored to coin selection) or spent (left, terminal). One
-- row per output; dropped once decided. Live lesson (2026-09-02, w0):
-- 224,575 sats sat on the output a phantom root spent, kept locked by one
-- inconclusive lookup, and nothing retried.
--
-- Every statement is IF NOT EXISTS: applied on migrate() AND on
-- make_available(), so a database created before this file gets the table
-- the next time it is opened.

CREATE TABLE IF NOT EXISTS locked_input_checks (
    output_id INTEGER PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_verdict TEXT,
    last_checked_at DATETIME,
    next_check_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_locked_input_checks_next ON locked_input_checks(next_check_at);
