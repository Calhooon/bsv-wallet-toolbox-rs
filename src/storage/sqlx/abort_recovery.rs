//! A TAKE HEARD AFTER A RELEASE MADE ON NO EVIDENCE, and the one recovery
//! of a `failed` transaction's inputs.
//!
//! THE RULE IS BY STATE (the keeper's readings 18 and 20 of section 30,
//! the broadcaster charter's B4 (d)): a `failed` outgoing row whose inputs
//! are released, with no competitor's checked proof on record against it,
//! was released on no evidence, whichever act released it (the host's
//! abort, an end by count, a fail on a broadcaster's word). In the one
//! write that applies a take (`competitor_proof::apply_take_on`), behind
//! the competitor guard, each `failed` outgoing row of the txid reads one
//! of three states ([`failed_outgoing_rows`]):
//!
//! - RELEASED (no output row marked spent by it, no evidence): recovered
//!   ([`recover_released_on`]): the transaction `unproven`, each input still
//!   free marked spent by it again, its own outputs that nothing spent
//!   spendable again, the request `unmined`, the cause on the request's
//!   history naming the instances (`takenAfterAbort` where the abort's mark
//!   names the outputs it closed, `takenAfterRelease` where no mark is
//!   there and the outputs are read from the row).
//! - LOCKED (an input still marked spent by it: the retire path's
//!   `failed`): nothing is recovered; the take is recorded as a hint
//!   (`takeHeldAsHint`) and the request keeps its word, raised only for a
//!   live row of another user of the store.
//! - EVIDENCE (a `locked_input_checks` row `spent-proven` at one of its
//!   input outpoints, on the wallet's row of the coin spent by it or by
//!   nothing): nothing is recovered and the request keeps its word.
//!
//! A refusal of the wallet's own run leaves no record on an outgoing row:
//! the signing check refuses before anything is written. So a request
//! `unmined` beside a `failed` row whose inputs are released never stands
//! after a take: a coin offered while the network holds a spend of it.
//!
//! THE ABORT'S MARK: the abort writes, in its own write, the request's
//! history key `abort` naming the transaction row it failed and the
//! outputs it closed; the recovery reads it for those outputs and marks it
//! recovered (once).
//!
//! An input another action of the wallet spent meanwhile stays with that
//! action: the two are competitors, and the proof path decides between
//! them. The recovered transaction reads `unproven` meanwhile.
//!
//! THE ONE RECOVERY OF INPUTS ([`remark_inputs_spent_on`]) has three
//! callers: the take after a release, the unfail task's recovery of a
//! transaction failed on a false word (`TakeRows::LiftFailed`), both inside
//! the take's write, and a checked proof's completion of a `failed` row
//! (`proof_completion`).

use sqlx::{Row, SqliteConnection};

use crate::Result;

/// The request's history key of the abort's mark.
const ABORT_KEY: &str = "abort";

/// The history with the abort's mark: `transaction_id` failed by the
/// host's abort, `closed` the outputs of it the abort made unspendable.
pub(crate) fn mark_abort(
    history: &str,
    transaction_id: i64,
    closed: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    let mut object = match serde_json::from_str::<serde_json::Value>(history) {
        Ok(serde_json::Value::Object(map)) => map,
        _ if history.trim().is_empty() => serde_json::Map::new(),
        _ => {
            let mut map = serde_json::Map::new();
            map.insert("previous".to_string(), serde_json::Value::from(history));
            map
        }
    };
    object.insert(
        ABORT_KEY.to_string(),
        serde_json::json!({
            "when": now.to_rfc3339(),
            "transactionId": transaction_id,
            "closed": closed,
        }),
    );
    serde_json::Value::Object(object).to_string()
}

/// Write the abort's mark on `txid`'s request, in the abort's write.
pub(crate) async fn write_abort_mark(
    conn: &mut SqliteConnection,
    txid: &str,
    transaction_id: i64,
    closed: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((id, history)) = row else {
        return Ok(());
    };
    sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
        .bind(mark_abort(&history, transaction_id, closed, now))
        .bind(id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// What a `failed` outgoing row of the txid reads by its state, at the take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailedState {
    /// Its inputs released and no evidence on record: a release made on no
    /// evidence (an abort, an end by count, a fail on a broadcaster's
    /// word), recovered by the take.
    Released,
    /// An input still marked spent by it (the retire path's `failed`): no
    /// coin is offered, nothing is recovered.
    Locked,
    /// A competitor's checked proof on record against it (a
    /// `locked_input_checks` row `spent-proven` at one of its input
    /// outpoints, the row spent by it or by nothing): nothing is
    /// recovered.
    Evidence,
}

/// A `failed` outgoing row of the txid and its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FailedRow {
    pub(crate) transaction_id: i64,
    pub(crate) state: FailedState,
}

/// Every `failed` outgoing row of `txid` with its state, read on the
/// take's write. `outpoints` are the outpoints the txid's bytes spend.
pub(crate) async fn failed_outgoing_rows(
    conn: &mut SqliteConnection,
    txid: &str,
    outpoints: &[(String, u32)],
) -> Result<Vec<FailedRow>> {
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions \
         WHERE txid = ? AND status = 'failed' AND is_outgoing = 1 ORDER BY transaction_id",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    let mut rows = Vec::with_capacity(ids.len());
    for transaction_id in ids {
        let mut evidence = false;
        for (source, vout) in outpoints {
            let proven: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM outputs o \
                 JOIN locked_input_checks c ON c.output_id = o.output_id \
                 WHERE lower(o.txid) = lower(?) AND o.vout = ? AND c.last_verdict = ? \
                   AND (o.spent_by IS NULL OR o.spent_by = ?)",
            )
            .bind(source)
            .bind(i64::from(*vout))
            .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
            .bind(transaction_id)
            .fetch_one(&mut *conn)
            .await?;
            if proven > 0 {
                evidence = true;
                break;
            }
        }
        let locked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs WHERE spent_by = ?")
            .bind(transaction_id)
            .fetch_one(&mut *conn)
            .await?;
        let state = if evidence {
            FailedState::Evidence
        } else if locked > 0 {
            FailedState::Locked
        } else {
            FailedState::Released
        };
        rows.push(FailedRow {
            transaction_id,
            state,
        });
    }
    Ok(rows)
}

/// The outputs the abort's mark on `txid`'s request names as closed, when
/// the mark names `transaction_id` and no take has recovered it.
async fn abort_mark_for(
    conn: &mut SqliteConnection,
    txid: &str,
    transaction_id: i64,
) -> Result<Option<Vec<i64>>> {
    let history: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let history = history
        .and_then(|h| serde_json::from_str::<serde_json::Value>(&h).ok())
        .unwrap_or_default();
    let Some(mark) = history.get(ABORT_KEY) else {
        return Ok(None);
    };
    let names = mark
        .get("transactionId")
        .and_then(serde_json::Value::as_i64)
        == Some(transaction_id);
    if !names || mark.get("recovered").is_some() {
        return Ok(None);
    }
    Ok(Some(
        mark.get("closed")
            .and_then(|v| v.as_array())
            .map(|ids| ids.iter().filter_map(serde_json::Value::as_i64).collect())
            .unwrap_or_default(),
    ))
}

/// Append `note` to `txid`'s request's history, on the caller's write (no
/// request, nothing written).
pub(crate) async fn note_request_on(
    conn: &mut SqliteConnection,
    txid: &str,
    note: serde_json::Value,
) -> Result<()> {
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((id, history)) = row else {
        return Ok(());
    };
    let history = super::storage_sqlx::append_req_history_note(&history, note);
    sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
        .bind(history)
        .bind(id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Open again `transaction_id`'s own outputs that nothing spent: those in
/// `closed`, or every one closed when `closed` is `None`. Returns the ids.
pub(crate) async fn open_own_outputs(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    closed: Option<Vec<i64>>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let closed: Vec<i64> = match closed {
        Some(closed) => closed,
        None => {
            sqlx::query_scalar(
                "SELECT output_id FROM outputs \
                 WHERE transaction_id = ? AND spendable = 0 AND spent_by IS NULL \
                 ORDER BY output_id",
            )
            .bind(transaction_id)
            .fetch_all(&mut *conn)
            .await?
        }
    };
    sqlx::query(
        "UPDATE outputs SET spendable = 1, updated_at = ? \
         WHERE transaction_id = ? AND spent_by IS NULL \
           AND output_id IN (SELECT value FROM json_each(?))",
    )
    .bind(now)
    .bind(transaction_id)
    .bind(serde_json::Value::from(closed.clone()).to_string())
    .execute(&mut *conn)
    .await?;
    Ok(closed)
}

/// THE TAKE AFTER A RELEASE MADE ON NO EVIDENCE, on the take's write (see
/// the module): every `failed` outgoing row of `txid` in
/// [`FailedState::Released`] is recovered. Returns the rows recovered.
pub(crate) async fn recover_released_on(
    conn: &mut SqliteConnection,
    txid: &str,
    rows: &[FailedRow],
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<usize> {
    let skip: Vec<i64> = rows
        .iter()
        .filter(|r| r.state == FailedState::Evidence)
        .map(|r| r.transaction_id)
        .collect();
    let mut recovered = 0;
    for row in rows.iter().filter(|r| r.state == FailedState::Released) {
        let transaction_id = row.transaction_id;
        // The outputs the release closed: the abort's mark names them;
        // without it they are read from the row (every signed output is
        // spendable).
        let marked = abort_mark_for(conn, txid, transaction_id).await?;
        let was_marked = marked.is_some();
        sqlx::query(
            "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE transaction_id = ?",
        )
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;
        let closed = open_own_outputs(conn, transaction_id, marked, now).await?;
        remark_inputs_spent_on(conn, txid, &skip, now).await?;
        if was_marked {
            sqlx::query(
                "UPDATE proven_tx_reqs SET history = json_set(history, '$.abort.recovered', ?) \
                 WHERE txid = ?",
            )
            .bind(now.to_rfc3339())
            .bind(txid)
            .execute(&mut *conn)
            .await?;
        }
        let what = if was_marked {
            "takenAfterAbort"
        } else {
            "takenAfterRelease"
        };
        note_request_on(
            conn,
            txid,
            serde_json::json!({
                "what": what,
                "when": now.to_rfc3339(),
                "transactionId": transaction_id,
                "outputs": closed,
                "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            transaction_id,
            marked = was_marked,
            marker = "taken_after_release",
            "a take heard after a release made on no evidence: the release is undone, the transaction unproven, its free inputs marked again"
        );
        recovered += 1;
    }
    Ok(recovered)
}

/// THE ONE RECOVERY OF A `failed` TRANSACTION'S INPUTS, on the caller's
/// write: every wallet-tracked output row at each of the transaction's
/// input outpoints that is still free (or consumed with no spender
/// recorded) becomes unspendable (the outpoint is consumed), `spent_by` the
/// row owner's own transaction row for `txid` where one exists, except a
/// row named in `skip` (a `failed` row left so, with evidence on record). A row
/// another transaction spends is never taken from it. The bytes are the request's copy, else a row's; without them the
/// outpoints are unknowable and nothing is marked.
pub(crate) async fn remark_inputs_spent_on(
    conn: &mut SqliteConnection,
    txid: &str,
    skip: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let raw: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         UNION ALL SELECT raw_tx FROM transactions WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         LIMIT 1",
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(raw) = raw else {
        tracing::warn!(txid = %txid, "no bytes held: the inputs cannot be marked spent again");
        return Ok(());
    };
    let tx = match bsv_rs::transaction::Transaction::from_binary(&raw) {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(txid = %txid, error = ?e, "the held bytes do not parse: the inputs are not marked again");
            return Ok(());
        }
    };
    for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&tx) {
        remark_outpoint(conn, txid, (&source_txid, vout), skip, now).await?;
    }
    Ok(())
}

/// Every wallet row at one input outpoint of `txid`, marked spent by its
/// owner's own row for `txid` where the rule of [`remark_inputs_spent_on`]
/// lets it.
async fn remark_outpoint(
    conn: &mut SqliteConnection,
    txid: &str,
    (source_txid, vout): (&str, u32),
    skip: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT output_id, user_id, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
    )
    .bind(source_txid)
    .bind(i64::from(vout))
    .fetch_all(&mut *conn)
    .await?;
    for row in rows {
        let spendable: bool = row.get("spendable");
        let spent_by: Option<i64> = row.get("spent_by");
        // The owner's own row for the spending txid (none for a user
        // without one: `spent_by` names the owner's own scope).
        let own: Option<i64> = sqlx::query_scalar(
            "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
        )
        .bind(txid)
        .bind(row.get::<i64, _>("user_id"))
        .fetch_optional(&mut *conn)
        .await?;
        // A row another transaction spends is never taken from it; a row
        // already marked consumed with no spender recorded (a door marks a
        // foreign owner's coin so) is stamped with the owner's own row; a
        // row left `failed` with evidence on record keeps its inputs.
        let taken = spent_by.is_some_and(|existing| own != Some(existing));
        let skipped = own.is_some_and(|r| skip.contains(&r));
        let settled = !spendable && (spent_by.is_some() || own.is_none());
        if taken || skipped || settled {
            continue;
        }
        sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = COALESCE(?, spent_by), updated_at = ? \
             WHERE output_id = ?",
        )
        .bind(own)
        .bind(now)
        .bind(row.get::<i64, _>("output_id"))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
