-- 006: an index on transactions(updated_at)
--
-- The release search's mark (stored_spenders.rs, mark_of) reads the highest
-- transaction id and the newest updated_at of the store's transactions, one
-- row left out. With no index on updated_at that read walked every row of
-- transactions, whose rows hold whole transactions and their input BEEFs, so
-- its cost followed the table's bytes. With this index each half of the mark
-- is a seek: the read does not grow with the store.
--
-- Every statement is IF NOT EXISTS: applied on migrate() AND on
-- make_available(), so a database created before this file gets the index
-- the next time it is opened. A release without this file opens a database
-- that has it and ignores it (SQLite keeps an index current on every write).

CREATE INDEX IF NOT EXISTS idx_transactions_updated_at ON transactions(updated_at);
