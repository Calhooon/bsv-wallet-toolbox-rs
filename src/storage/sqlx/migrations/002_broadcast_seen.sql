-- 002: broadcast acceptance memory (0.3.56)
--
-- broadcast_seen: which broadcast provider has already accepted or seen a
-- txid. status is 'accepted' (the provider took it on a submit), 'seen' (a
-- push channel reported it seen on the network) or 'mined' (proven, never
-- downgraded). provider 'network' holds facts about the network as a whole
-- and counts toward every provider's seen set. Drives the reduced sends in
-- Services::post_beef.
--
-- broadcast_prefs: small broadcast preferences, e.g. the provider that
-- accepted the previous broadcast (key 'last_accepted_provider').
--
-- Every statement is IF NOT EXISTS: applied on migrate() AND on
-- make_available(), so a database created before this file gets the tables
-- the next time it is opened.

CREATE TABLE IF NOT EXISTS broadcast_seen (
    txid TEXT NOT NULL,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (txid, provider)
);

CREATE INDEX IF NOT EXISTS idx_broadcast_seen_txid ON broadcast_seen(txid);

CREATE TABLE IF NOT EXISTS broadcast_prefs (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
