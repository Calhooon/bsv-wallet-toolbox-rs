-- 006: held receipts (the owner's ruling of 2026-11-17, DECISIONS 29.1)
--
-- A received transaction with no checked proof that no broadcaster took is
-- HELD: the internalize door writes it transactions.status = 'sending', so
-- the balance and listOutputs leave its received outputs out and no
-- allocator offers them. A store written by 0.14.0-beta.8 or
-- 0.14.0-beta.8.1 holds such a receipt as 'unproven'. This statement moves
-- it to 'sending': a received transaction (is_outgoing = 0), 'unproven',
-- with no proof row linked, whose request is still 'unsent' or 'sending'
-- and that no broadcaster is recorded as having taken or seen, and no block
-- as having mined (broadcast_seen 'accepted', 'seen' or 'mined').
--
-- A receipt an instance took (request 'unmined') is not touched; neither is
-- a checked proof the proof-lag gate deferred (request 'unmined'). Only the
-- transaction's status moves: its outputs, its request and a coin the
-- wallet's own action already spent stay as they are.
--
-- A data statement, no schema change. Applied on every migrate() and every
-- make_available(), as 002 to 005 are; a second run finds nothing to move.
-- updated_at is written in the RFC 3339 form the store's own writes use.

UPDATE transactions
SET status = 'sending',
    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
WHERE is_outgoing = 0
  AND status = 'unproven'
  AND proven_tx_id IS NULL
  AND txid IN (SELECT txid FROM proven_tx_reqs WHERE status IN ('unsent', 'sending'))
  AND txid NOT IN (
    SELECT txid FROM broadcast_seen WHERE status IN ('accepted', 'seen', 'mined')
  );
