//! A TAKE HEARD AFTER THE WALLET'S OWN ABORT, and the one recovery of a
//! `failed` transaction's inputs.
//!
//! The abort of an action whose bytes an instance may already hold (the
//! post in flight when `abortAction` landed) is undone by the take, in the
//! write that applies it (`competitor_proof::apply_take_on`): the
//! transaction back to `unproven`, each input still free marked spent by it
//! again, the outputs the abort closed opened again where nothing spent
//! them, and the cause on the request's history (`takenAfterAbort`, with
//! the instances). The request reads `unmined` in the same write. A
//! request `unmined` beside a `failed` row with its inputs released is the
//! state this makes impossible: a coin offered while the network holds a
//! spend of it (broadcaster charter B4 (d), the keeper's reading 18).
//!
//! HOW THE ROWS TELL THE ABORT'S `failed`: the abort writes its mark in its
//! own write, the request's history key `abort` naming the transaction row
//! it failed and the outputs it closed. No other writer of `failed` writes
//! it: the retire path's verdicts (a competitor's checked proof: request
//! `doubleSpend`; a refusal the wallet's own check made: request
//! `invalid`) keep the inputs locked and write no mark, and a received
//! transaction is never aborted. A take recovers a `failed` row only under
//! that mark, and once (`abort.recovered`). The competitor guard stands in
//! front of it: while a competitor's checked proof stands nothing moves.
//!
//! An input another action of the wallet spent meanwhile stays with that
//! action: the two are competitors, and the proof path decides between
//! them. The recovered transaction reads `unproven` meanwhile.
//!
//! THE ONE RECOVERY OF INPUTS ([`remark_inputs_spent_on`]) has two callers:
//! the take after an abort, and the unfail task's recovery of a
//! transaction failed on a false word (`TakeRows::LiftFailed`), both inside
//! the take's write.

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

/// A row the host's abort failed and no take has recovered yet.
struct Aborted {
    req_id: i64,
    history: serde_json::Value,
    transaction_id: i64,
    closed: Vec<i64>,
}

async fn aborted_row(conn: &mut SqliteConnection, txid: &str) -> Result<Option<Aborted>> {
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((req_id, history)) = row else {
        return Ok(None);
    };
    let Ok(history) = serde_json::from_str::<serde_json::Value>(&history) else {
        return Ok(None);
    };
    let Some(mark) = history.get(ABORT_KEY) else {
        return Ok(None);
    };
    if mark.get("recovered").is_some() {
        return Ok(None);
    }
    let Some(transaction_id) = mark
        .get("transactionId")
        .and_then(serde_json::Value::as_i64)
    else {
        return Ok(None);
    };
    let failed: Option<i64> = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions \
         WHERE transaction_id = ? AND txid = ? AND status = 'failed' AND is_outgoing = 1",
    )
    .bind(transaction_id)
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;
    if failed.is_none() {
        return Ok(None);
    }
    let closed = mark
        .get("closed")
        .and_then(|v| v.as_array())
        .map(|ids| ids.iter().filter_map(serde_json::Value::as_i64).collect())
        .unwrap_or_default();
    Ok(Some(Aborted {
        req_id,
        history,
        transaction_id,
        closed,
    }))
}

/// THE TAKE AFTER AN ABORT, on the take's write (see the module): returns
/// whether a row the host's abort failed was recovered.
pub(crate) async fn recover_aborted_on(
    conn: &mut SqliteConnection,
    txid: &str,
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    let Some(mut aborted) = aborted_row(conn, txid).await? else {
        return Ok(false);
    };
    sqlx::query(
        "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE transaction_id = ?",
    )
    .bind(now)
    .bind(aborted.transaction_id)
    .execute(&mut *conn)
    .await?;
    for output_id in &aborted.closed {
        sqlx::query(
            "UPDATE outputs SET spendable = 1, updated_at = ? \
             WHERE output_id = ? AND transaction_id = ? AND spent_by IS NULL",
        )
        .bind(now)
        .bind(output_id)
        .bind(aborted.transaction_id)
        .execute(&mut *conn)
        .await?;
    }
    remark_inputs_spent_on(conn, txid, now).await?;
    if let Some(mark) = aborted.history.get_mut(ABORT_KEY) {
        mark["recovered"] = serde_json::Value::from(now.to_rfc3339());
    }
    let history = super::storage_sqlx::append_req_history_note(
        &aborted.history.to_string(),
        serde_json::json!({
            "what": "takenAfterAbort",
            "when": now.to_rfc3339(),
            "transactionId": aborted.transaction_id,
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    );
    sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
        .bind(history)
        .bind(aborted.req_id)
        .execute(&mut *conn)
        .await?;
    tracing::warn!(
        txid = %txid,
        transaction_id = aborted.transaction_id,
        marker = "taken_after_abort",
        "a take heard after the host's abort: the abort is undone, the transaction unproven, its free inputs marked again"
    );
    Ok(true)
}

/// THE ONE RECOVERY OF A `failed` TRANSACTION'S INPUTS, on the caller's
/// write: every wallet-tracked output row at each of the transaction's
/// input outpoints that is still free (or consumed with no spender
/// recorded) becomes unspendable (the outpoint is consumed), `spent_by` the
/// row owner's own transaction row for `txid` where one exists. A row
/// another transaction spends is never taken from it. The bytes are the request's copy, else a row's; without them the
/// outpoints are unknowable and nothing is marked.
pub(crate) async fn remark_inputs_spent_on(
    conn: &mut SqliteConnection,
    txid: &str,
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
        let rows = sqlx::query(
            "SELECT output_id, user_id, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
        )
        .bind(&source_txid)
        .bind(i64::from(vout))
        .fetch_all(&mut *conn)
        .await?;
        for row in rows {
            let output_id: i64 = row.get("output_id");
            let owner: i64 = row.get("user_id");
            let spendable: bool = row.get("spendable");
            let spent_by: Option<i64> = row.get("spent_by");
            // The owner's own row for the spending txid (none for a user
            // without one: `spent_by` names the owner's own scope).
            let own: Option<i64> = sqlx::query_scalar(
                "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
            )
            .bind(txid)
            .bind(owner)
            .fetch_optional(&mut *conn)
            .await?;
            // A row another transaction spends is never taken from it; a
            // row already marked consumed with no spender recorded (a door
            // marks a foreign owner's coin so) is stamped with the owner's
            // own row.
            if spent_by.is_some_and(|existing| own != Some(existing)) {
                continue;
            }
            if !spendable && (spent_by.is_some() || own.is_none()) {
                continue;
            }
            sqlx::query(
                "UPDATE outputs SET spendable = 0, spent_by = COALESCE(?, spent_by), updated_at = ? \
                 WHERE output_id = ?",
            )
            .bind(own)
            .bind(now)
            .bind(output_id)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}
