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
//! - a `completed` row is not: its spend is mined, where a provider's
//!   unspent word reads it;
//! - a row this store created names itself as `spent_by` on each coin of
//!   the store it spends, which a caller reads on the coin's own row (and
//!   this module reads on every row of the outpoint): it is not counted
//!   unless its `input_beef` carries the bytes of the outpoint's own
//!   transaction, where it may also spend the outpoint as an input from
//!   outside the store;
//! - what remains is counted: a row neither `failed` nor `completed`, with
//!   no bytes, that no output row names as `spent_by` (one a sync brought,
//!   or an action whose every input is from outside this store).
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
pub(crate) async fn stored_spenders_of(
    conn: &mut SqliteConnection,
    txid: &str,
    vout: u32,
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
    let bare: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM transactions t \
         LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
         WHERE t.status NOT IN ('failed', 'completed') \
           AND (t.raw_tx IS NULL OR length(t.raw_tx) = 0) \
           AND (p.raw_tx IS NULL OR length(p.raw_tx) = 0) \
           AND (NOT EXISTS (SELECT 1 FROM outputs o WHERE o.spent_by = t.transaction_id) \
                OR ? IS NULL \
                OR instr(t.input_beef, ?) > 0)",
    )
    .bind(source.as_deref())
    .bind(source.as_deref())
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
