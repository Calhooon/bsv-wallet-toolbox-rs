//! Which transactions this store holds, not failed, spend an outpoint, read
//! from their bytes.
//!
//! A store a sync has written holds a coin the other store spent as not
//! spendable with no spender: the sync carries a transaction's bytes and
//! not an output's spender. The bytes still name what they spend: each
//! input carries its outpoint as the source txid's 32 bytes in internal
//! order, then the output index as four bytes little-endian. This module
//! finds the transactions whose bytes carry those 36 bytes and PARSES each
//! one, so a match anywhere else in a transaction (an output script, an
//! unlocking script) is never read as a spend.
//!
//! Where a transaction's bytes live on this line: `transactions.raw_tx`
//! (NULL for an action `createAction` wrote and `processAction` has not yet
//! signed, for a completed row a purge cleared, and for such a row a sync
//! brought from a store that held it so), and `proven_txs.raw_tx` (NOT NULL)
//! for a transaction linked to its proof.
//!
//! A row with no bytes in either table is unread. Only some of them can
//! hide a spender of the outpoint, and only those are counted:
//! - a `failed` or `completed` row is not: the one spends nothing, the
//!   other's spend is mined, where a provider's unspent word reads it;
//! - a spend of a coin of the store by a row this store created is
//!   recorded as `spent_by` on the coin's row, which this module reads on
//!   every row of the outpoint;
//! - what an unread row can still hide is a spend of the outpoint as an
//!   input from outside the store, whose source then travels in the row's
//!   `input_beef` (`createAction` refuses an outside input whose source is
//!   not in the caller's BEEF). So a row is counted when its `input_beef`
//!   carries the outpoint's own transaction whole, or its txid's 32 bytes
//!   (an entry made txid-only by `knownTxids`), or when there is no
//!   `input_beef` to read (NULL or empty: `returnTXIDOnly`, a row a sync
//!   brought with none), or when this store does not hold the outpoint's
//!   transaction's bytes to look for. A row whose `input_beef` carries
//!   none of those (an unsigned action whose inputs all come from
//!   elsewhere) cannot spend this outpoint and is not counted, whether or
//!   not an output row names it as a spender.
//! - a row of the output's own user that an output row names as its
//!   spender is not counted when the caller names that output
//!   (`stored_spenders_of_output`) and its order shows it could not have
//!   taken the output unrecorded. `createAction` takes a coin of the store
//!   only by allocating change or by a named input it looks up by the
//!   caller's user and refuses unless spendable (a change coin is never
//!   named), and either way records itself as the coin's spender; an
//!   outside input is one with no row of the caller's. The store keeps
//!   nothing else of an unsigned action's inputs, so the rule rests on
//!   order: the row is left out when the output's row has a lower
//!   `output_id` than every output row of the action's own (the lookup of
//!   the caller's inputs and the insert of the action's outputs are one
//!   write, so the output was held when the action was made), or when the
//!   output's transaction is an outgoing action with a higher
//!   `transaction_id` and a `created_at` not older than the action's (its
//!   txid did not exist when the action was made). Its two limits: that
//!   second arm trusts the stamp, which a sync carries from its reader;
//!   and the other arm reads a row written before the action as held
//!   then, which an output row of the user's own action not yet signed
//!   (no txid to be looked up by) was not (#864).
//!   An action made before the user's row of the output was written (it
//!   took the output as an input from outside the store), an action with
//!   no output row of its own, another user's row, and a row a sync
//!   brought (the spender does not travel, so no output names it) stay
//!   counted.
//!
//! A caller that would free a coin frees none while a counted row is held.

use sqlx::{Row, SqliteConnection};

use crate::error::{Error, Result};

/// A transaction this store holds that spends the outpoint, read from its
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSpender {
    pub transaction_id: i64,
    pub txid: Option<String>,
    pub status: String,
}

/// The answer for one outpoint: the transactions not failed whose bytes
/// spend it, and how many transactions not failed could not be read (no
/// bytes in either table, or bytes carrying the outpoint that do not parse).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StoredSpenders {
    pub spenders: Vec<StoredSpender>,
    pub unreadable: u64,
}

impl StoredSpenders {
    /// Nothing this store holds spends the outpoint, and nothing it holds
    /// is unread: the only answer on which a coin may be freed.
    pub fn none_held(&self) -> bool {
        self.spenders.is_empty() && self.unreadable == 0
    }
}

/// The outpoint as an input carries it: the txid's 32 bytes in internal
/// (reversed display) order, then the output index little-endian.
pub(crate) fn outpoint_bytes(txid: &str, vout: u32) -> Result<[u8; 36]> {
    let mut id = hex::decode(txid)
        .map_err(|e| Error::ValidationError(format!("txid {txid} is not hex: {e}")))?;
    if id.len() != 32 {
        return Err(Error::ValidationError(format!(
            "txid {txid} is not 32 bytes"
        )));
    }
    id.reverse();
    let mut out = [0u8; 36];
    out[..32].copy_from_slice(&id);
    out[32..].copy_from_slice(&vout.to_le_bytes());
    Ok(out)
}

/// Whether `raw` parses and one of its inputs spends `txid`.`vout`: `None`
/// when it does not parse.
fn bytes_spend(raw: &[u8], txid: &str, vout: u32) -> Option<bool> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    Some(tx.inputs.iter().any(|i| {
        i.source_output_index == vout
            && i.source_txid
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case(txid))
    }))
}

/// Which transactions of this store that are not failed spend `txid`.`vout`,
/// by their bytes (`transactions.raw_tx`, else the linked `proven_txs.raw_tx`)
/// or recorded as `spent_by` on a row of the outpoint, and how many
/// transactions that could hide a spender cannot be read. Four reads on
/// `conn`: a byte search (`instr`, a scan of the bytes of every transaction
/// not failed, no index) whose candidates are each parsed, the recorded
/// spenders, the bytes of the outpoint's own transaction, and a count of
/// the rows that could hide a spender (module docs).
#[cfg(test)]
pub(crate) async fn stored_spenders_of(
    conn: &mut SqliteConnection,
    txid: &str,
    vout: u32,
) -> Result<StoredSpenders> {
    stored_spenders_except(conn, txid, vout, None, None).await
}

/// `stored_spenders_of` for the output row `output_id` of the outpoint:
/// the unread count leaves out a row of that output's own user that an
/// output row names as its spender, made after that output's row was
/// written or before its outgoing transaction was (module docs).
pub(crate) async fn stored_spenders_of_output(
    conn: &mut SqliteConnection,
    output_id: i64,
    txid: &str,
    vout: u32,
) -> Result<StoredSpenders> {
    stored_spenders_except(conn, txid, vout, None, Some(output_id)).await
}

/// `stored_spenders_of`, the unread count leaving out the row `except`
/// (an action being signed in the caller's write, whose own bytes the
/// caller reads) and, with `output`, a row of that output row's user that
/// an output row names as its spender, made after that output's row was
/// written or before its outgoing transaction was.
async fn stored_spenders_except(
    conn: &mut SqliteConnection,
    txid: &str,
    vout: u32,
    except: Option<i64>,
    output: Option<i64>,
) -> Result<StoredSpenders> {
    let needle = outpoint_bytes(txid, vout)?.to_vec();
    let candidates = sqlx::query(
        "SELECT t.transaction_id, t.txid, t.status, t.raw_tx AS t_raw, p.raw_tx AS p_raw \
         FROM transactions t LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.status != 'failed' \
           AND (instr(t.raw_tx, ?) > 0 OR instr(p.raw_tx, ?) > 0) \
         ORDER BY t.transaction_id",
    )
    .bind(&needle)
    .bind(&needle)
    .fetch_all(&mut *conn)
    .await?;

    let mut answer = StoredSpenders::default();
    for row in &candidates {
        let readings: Vec<Option<bool>> = ["t_raw", "p_raw"]
            .iter()
            .filter_map(|c| row.get::<Option<Vec<u8>>, _>(*c))
            .filter(|raw| raw.windows(36).any(|w| w == needle.as_slice()))
            .map(|raw| bytes_spend(&raw, txid, vout))
            .collect();
        if readings.contains(&Some(true)) {
            answer.spenders.push(StoredSpender {
                transaction_id: row.get("transaction_id"),
                txid: row.get("txid"),
                status: row.get("status"),
            });
        } else if readings.contains(&None) {
            // Carries the outpoint's bytes and does not parse: it cannot be
            // ruled out.
            answer.unreadable += 1;
        }
    }

    // A spender recorded on any row of the outpoint (another user's row of
    // it included), not failed and not already read from its bytes.
    let recorded = sqlx::query(
        "SELECT DISTINCT t.transaction_id, t.txid, t.status FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.spent_by \
         WHERE o.txid = ? AND o.vout = ? AND t.status != 'failed' \
         ORDER BY t.transaction_id",
    )
    .bind(txid)
    .bind(i64::from(vout))
    .fetch_all(&mut *conn)
    .await?;
    for row in &recorded {
        let id: i64 = row.get("transaction_id");
        if !answer.spenders.iter().any(|s| s.transaction_id == id) {
            answer.spenders.push(StoredSpender {
                transaction_id: id,
                txid: row.get("txid"),
                status: row.get("status"),
            });
        }
    }

    // The bytes of the outpoint's own transaction, which an action's
    // `input_beef` carries when it may spend the outpoint from outside the
    // store. Unknown: every unread row is counted.
    let source: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT COALESCE(NULLIF(t.raw_tx, X''), p.raw_tx) FROM transactions t \
         LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.txid = ? AND COALESCE(NULLIF(t.raw_tx, X''), p.raw_tx) IS NOT NULL \
         LIMIT 1",
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?
    .flatten();
    // The txid's 32 bytes, both orders: a BEEF entry that carries the
    // transaction by txid only (`knownTxids`, a txid-only entry) holds them.
    let internal = needle[..32].to_vec();
    let mut display = internal.clone();
    display.reverse();
    // With `output`, a row of that output's own user that an output row
    // names as its spender is left out only by order: the output's row has
    // a lower id than every output row of the action's own (`createAction`
    // looks up the caller's inputs and inserts the action's outputs in one
    // write, so a row of the output's user written before the action is
    // one it looked up and recorded itself on if it took it), or the
    // output's transaction is an outgoing action written after it and not
    // stamped older (its txid did not exist when the action was made).
    let bare: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM transactions t \
         LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.status NOT IN ('failed', 'completed') \
           AND (t.raw_tx IS NULL OR length(t.raw_tx) = 0) \
           AND (p.raw_tx IS NULL OR length(p.raw_tx) = 0) \
           AND t.transaction_id != ? \
           AND NOT (EXISTS (SELECT 1 FROM outputs me \
                            JOIN transactions ot ON ot.transaction_id = me.transaction_id \
                            WHERE me.output_id = ? AND me.user_id IS t.user_id \
                              AND (me.output_id < (SELECT MIN(a.output_id) FROM outputs a \
                                                   WHERE a.transaction_id = t.transaction_id) \
                                   OR (ot.is_outgoing = 1 \
                                       AND ot.transaction_id > t.transaction_id \
                                       AND julianday(ot.created_at) >= julianday(t.created_at)))) \
                    AND EXISTS (SELECT 1 FROM outputs n WHERE n.spent_by = t.transaction_id)) \
           AND (? IS NULL \
                OR t.input_beef IS NULL OR length(t.input_beef) = 0 \
                OR instr(t.input_beef, ?) > 0 \
                OR instr(t.input_beef, ?) > 0 \
                OR instr(t.input_beef, ?) > 0)",
    )
    .bind(except.unwrap_or(-1))
    .bind(output.unwrap_or(-1))
    .bind(source.as_deref())
    .bind(source.as_deref())
    .bind(&internal)
    .bind(&display)
    .fetch_one(&mut *conn)
    .await?
    .get("n");
    answer.unreadable += bare as u64;
    Ok(answer)
}

/// The `monitor_events` name of an output withheld only for an unread row,
/// which the unfail task tries again at a later pass. Its details:
/// `outputId=<id> <txid>.<vout>`.
pub(crate) const WITHHELD_EVENT: &str = "outputWithheld";

/// The most withheld outputs one pass of the unfail task tries again.
pub(crate) const WITHHELD_PER_PASS: i64 = 20;

/// Records `output_id` as withheld, unless a record of it is held.
pub(crate) async fn record_withheld(
    conn: &mut SqliteConnection,
    output_id: i64,
    txid: &str,
    vout: u32,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let prefix = format!("outputId={output_id} ");
    sqlx::query(
        "INSERT INTO monitor_events (event, details, created_at, updated_at) \
         SELECT ?, ?, ?, ? WHERE NOT EXISTS \
         (SELECT 1 FROM monitor_events WHERE event = ? AND substr(details, 1, ?) = ?)",
    )
    .bind(WITHHELD_EVENT)
    .bind(format!("{prefix}{txid}.{vout}"))
    .bind(now)
    .bind(now)
    .bind(WITHHELD_EVENT)
    .bind(prefix.len() as i64)
    .bind(&prefix)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The output a withheld record names.
pub(crate) fn parse_withheld(details: &str) -> Option<i64> {
    details
        .strip_prefix("outputId=")?
        .split(' ')
        .next()?
        .parse()
        .ok()
}

/// A withheld record tried and still withheld: both stamps now.
pub(crate) async fn renew_withheld(
    conn: &mut SqliteConnection,
    event_id: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    sqlx::query("UPDATE monitor_events SET created_at = ?, updated_at = ? WHERE event_id = ?")
        .bind(now)
        .bind(now)
        .bind(event_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// A withheld record resolved: dropped.
pub(crate) async fn drop_withheld(conn: &mut SqliteConnection, event_id: i64) -> Result<()> {
    sqlx::query("DELETE FROM monitor_events WHERE event_id = ? AND event = ?")
        .bind(event_id)
        .bind(WITHHELD_EVENT)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The withheld records one pass tries: at most `WITHHELD_PER_PASS`, the
/// longest untried leading.
pub(crate) async fn withheld_to_try(pool: &sqlx::SqlitePool) -> Result<Vec<(i64, String)>> {
    Ok(sqlx::query_as::<_, (i64, String)>(
        "SELECT event_id, COALESCE(details, '') FROM monitor_events WHERE event = ? \
         ORDER BY julianday(updated_at) ASC, event_id ASC LIMIT ?",
    )
    .bind(WITHHELD_EVENT)
    .bind(WITHHELD_PER_PASS)
    .fetch_all(pool)
    .await?)
}

/// The output rows a send of the caller's `nosend` transaction `txid`
/// releases, the transaction's own row reading `nosend` and the caller's:
/// each output the `noSend` state held back, the change and every output
/// the action put in a basket, that records no spender. While the
/// transaction reads `nosend` processAction writes every output of it not
/// spendable (and every output of any other transaction spendable), so a
/// row of it not spendable with no spender is one the state held back. An
/// output in no basket stays as it is: one paid to another key, or one the
/// host relinquished (`relinquishOutput` clears its basket), is not the
/// wallet's to list or spend.
const SENT_ROWS: &str = "SELECT o.output_id, o.vout FROM outputs o \
     JOIN transactions t ON t.transaction_id = o.transaction_id \
     WHERE t.txid = ? AND t.user_id = ? AND t.status = 'nosend' \
       AND o.user_id = t.user_id AND o.basket_id IS NOT NULL \
       AND o.spendable = 0 AND o.spent_by IS NULL \
     ORDER BY o.output_id";

/// The store's transactions as a search read them: the highest id and the
/// newest stamp, the row `except` left out.
async fn mark_of(conn: &mut SqliteConnection, except: Option<i64>) -> Result<(i64, String)> {
    Ok(sqlx::query_as(
        "SELECT COALESCE(MAX(transaction_id), 0), \
         COALESCE(CAST(MAX(updated_at) AS TEXT), '') FROM transactions \
         WHERE transaction_id != ?",
    )
    .bind(except.unwrap_or(-1))
    .fetch_one(&mut *conn)
    .await?)
}

/// What a send's release found on a reader before the write: the store's
/// mark, and for each output it would release, the stored spenders.
#[derive(Debug, Default)]
pub(crate) struct SentChangeSearch {
    /// Whether any txid was searched: a search of none read no mark.
    searched: bool,
    mark: (i64, String),
    except: Option<i64>,
    found: std::collections::HashMap<i64, StoredSpenders>,
}

impl SentChangeSearch {
    /// A search that searched txids and found nothing (its mark unread).
    #[cfg(test)]
    pub(crate) fn searched_nothing() -> Self {
        Self {
            searched: true,
            ..Self::default()
        }
    }

    /// Inside the caller's write, before any of its sends' status writes:
    /// `self` when the store's transactions are as the search read them
    /// (`current`, the action the write signs, left out), else `None`, and
    /// each release searches on the writer.
    ///
    /// A search of no txid (an action that sends nothing with it, every
    /// immediate `createAction`) has no release to stand for and is `None`
    /// without reading the mark: the mark's `MAX(updated_at)` reads every
    /// transaction of the store, inside the write lock.
    pub(crate) async fn still_read(
        &self,
        conn: &mut SqliteConnection,
        current: Option<i64>,
    ) -> Result<Option<&Self>> {
        if !self.searched || self.except.is_some_and(|e| Some(e) != current) {
            return Ok(None);
        }
        Ok((mark_of(conn, current).await? == self.mark).then_some(self))
    }
}

/// The search of `release_sent_change`, run on a reader before the caller's
/// write: for each of `txids` the caller's `nosend` transaction's change
/// rows the send would release (`SENT_ROWS`), and what the store holds that
/// spends each.
/// `current`: the reference of an action the write signs (its own row is
/// left out of the unread count; the write reads its bytes).
pub(crate) async fn search_sent_change(
    storage: &super::StorageSqlx,
    user_id: i64,
    txids: &[String],
    current: Option<&str>,
) -> Result<SentChangeSearch> {
    let mut search = SentChangeSearch::default();
    if txids.is_empty() {
        return Ok(search);
    }
    search.searched = true;
    let mut conn = storage.pool().acquire().await?;
    if let Some(reference) = current {
        search.except =
            sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE reference = ?")
                .bind(reference)
                .fetch_optional(&mut *conn)
                .await?;
    }
    search.mark = mark_of(&mut conn, search.except).await?;
    for txid in txids {
        let rows: Vec<(i64, i64)> = sqlx::query_as(SENT_ROWS)
            .bind(txid)
            .bind(user_id)
            .fetch_all(&mut *conn)
            .await?;
        for (output_id, vout) in rows {
            let held = stored_spenders_except(
                &mut conn,
                txid,
                vout as u32,
                search.except,
                Some(output_id),
            )
            .await?;
            search.found.insert(output_id, held);
        }
    }
    Ok(search)
}

/// The caller's `nosend` transaction `txid` being sent (its own row still
/// reads `nosend` and is the caller's, `user_id`): each output of it the
/// `noSend` state held back (`SENT_ROWS`: the change and each output in a
/// basket) that records no spender becomes spendable in the caller's write, unless
/// a transaction this store holds spends it (by its bytes or as a recorded
/// spender on a row of it) or an unread row could hide one; one withheld
/// only for an unread row is recorded for the unfail task to try again.
///
/// The byte search runs before the write (`search_sent_change`; `pre`, the
/// search `SentChangeSearch::still_read` kept): inside it the rows are read
/// again, a spender recorded since is read, and the bytes of the action the
/// write signs (`current`: its id and bytes) are parsed. A row `pre` did not
/// search, or no `pre`, is searched here, on the writer. Answers how many were set
/// spendable.
pub(crate) async fn release_sent_change(
    conn: &mut SqliteConnection,
    user_id: i64,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
    pre: Option<&SentChangeSearch>,
    current: Option<(i64, &[u8])>,
) -> Result<u64> {
    let rows: Vec<(i64, i64)> = sqlx::query_as(SENT_ROWS)
        .bind(txid)
        .bind(user_id)
        .fetch_all(&mut *conn)
        .await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let except = current.map(|(id, _)| id);
    let searched = pre;
    let mut released = 0;
    for (output_id, vout) in rows {
        let vout = vout as u32;
        let mut held = match searched.and_then(|p| p.found.get(&output_id)) {
            Some(found) => {
                let mut held = found.clone();
                // A spender recorded on a row of the outpoint since.
                let recorded: Vec<i64> = sqlx::query_scalar(
                    "SELECT DISTINCT t.transaction_id FROM outputs o \
                     JOIN transactions t ON t.transaction_id = o.spent_by \
                     WHERE o.txid = ? AND o.vout = ? AND t.status != 'failed'",
                )
                .bind(txid)
                .bind(i64::from(vout))
                .fetch_all(&mut *conn)
                .await?;
                for id in recorded {
                    if !held.spenders.iter().any(|s| s.transaction_id == id) {
                        held.spenders.push(StoredSpender {
                            transaction_id: id,
                            txid: None,
                            status: String::new(),
                        });
                    }
                }
                held
            }
            None => stored_spenders_except(&mut *conn, txid, vout, except, Some(output_id)).await?,
        };
        if let Some((id, raw)) = current {
            match bytes_spend(raw, txid, vout) {
                Some(false) => {}
                Some(true) => held.spenders.push(StoredSpender {
                    transaction_id: id,
                    txid: None,
                    status: String::new(),
                }),
                None => held.unreadable += 1,
            }
        }
        if held.none_held() {
            released += sqlx::query(
                "UPDATE outputs SET spendable = 1, updated_at = ? \
                 WHERE output_id = ? AND spendable = 0 AND spent_by IS NULL",
            )
            .bind(now)
            .bind(output_id)
            .execute(&mut *conn)
            .await?
            .rows_affected();
        } else if held.spenders.is_empty() {
            record_withheld(&mut *conn, output_id, txid, vout, now).await?;
        }
    }
    Ok(released)
}
