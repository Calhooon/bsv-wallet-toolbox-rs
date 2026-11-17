-- 006: held receipts (the owner's ruling of 2026-11-17, DECISIONS 29.1)
--
-- A received transaction with no checked proof that no broadcaster took is
-- HELD: the internalize door writes it transactions.status = 'sending', so
-- the balance and listOutputs leave every output it gives the wallet out
-- and no allocator offers them. A store written by 0.14.0-beta.8 or
-- 0.14.0-beta.8.1 holds such a receipt as 'unproven'. This statement moves
-- it to 'sending': a received transaction (is_outgoing = 0), 'unproven',
-- with no proof row linked, that has a request, under the rule below.
--
-- The migration's own rule, apart from the door's. The door reads a
-- recorded take (broadcast_seen 'accepted', 'seen' or 'mined') or a stored
-- proof, never a request's word (held_receipts.rs). Rows this statement
-- reads were written when no record was kept of every take, so it keeps
-- the request's word for them: a row whose request is 'unmined',
-- 'callback', 'unconfirmed' or 'completed' is left as it is, and so is a
-- row with a record of a take (the same broadcast_seen words the door
-- reads; a unit test holds them equal); a row whose request carries any
-- other word ('unsent', 'sending', another user's 'nosend', 'invalid', ...)
-- and no record moves. A row with no request at all is the proof path's
-- adoption to take up, not this statement's.
--
-- Only the transaction's status moves: its outputs, its request and a coin
-- the wallet's own action already spent stay as they are.
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
  AND txid IN (SELECT txid FROM proven_tx_reqs)
  AND txid NOT IN (SELECT txid FROM proven_tx_reqs WHERE status IN ('unmined', 'callback', 'unconfirmed', 'completed'))
  AND txid NOT IN (SELECT txid FROM broadcast_seen WHERE status IN ('accepted', 'seen', 'mined'));
