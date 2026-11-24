-- 006: the change pick's index
--
-- idx_outputs_change_pick: the free change coins of each basket, in value
-- order. The change pick (create_action.rs, allocate_change_input_by) asks
-- it once per tier: covering coins upward from the target, the others
-- downward from it, each read ending at the first coin whose transaction
-- is in the tier's status. Without it every pick read every spendable
-- output of the store (idx_outputs_spendable) and sorted them all.
--
-- Partial: a coin leaves the index when it is spent or set unspendable, so
-- the index holds the wallet's free change, not its history. The pick's
-- WHERE names the same three terms, which is what lets SQLite use it.
--
-- IF NOT EXISTS and applied on every open, so a database created before
-- this file gets it the next time it is opened.

CREATE INDEX IF NOT EXISTS idx_outputs_change_pick
    ON outputs(user_id, basket_id, satoshis)
    WHERE change = 1 AND spendable = 1 AND spent_by IS NULL;
