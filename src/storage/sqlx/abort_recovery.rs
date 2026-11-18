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
//!   `failed`, whichever of its inputs the locked-input re-check gave
//!   back): it stays ended; every input of it still free is marked spent
//!   by it again in the take's write; the take is recorded as a hint
//!   (`takeHeldAsHint`) and the request keeps its word, raised only for a
//!   live row of another user of the store.
//! - EVIDENCE (a `locked_input_checks` row `spent-proven` at one of its
//!   input outpoints, on the wallet's row of the coin spent by it or by
//!   nothing): the take is refused as a standing competitor's checked
//!   proof refuses it (`competitor_proof::apply_take_on`): nothing is
//!   recovered, no row lifted, no take recorded, the request keeps its
//!   word and a `takeNotCounted` note names the rows.
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
//! action: the two are competitors. The recovery queues the recovered txid
//! on that action's request as its competitor (`competitorsQueued`), so the
//! competitor proof ask settles the two by a proof: one ask of the
//! recovered transaction's bytes and path per pass while that action's
//! request is live. The recovered transaction reads `unproven` meanwhile.
//!
//! THE ONE RECOVERY OF INPUTS ([`remark_inputs_spent_on`]) has four
//! callers: the take after a release, the take beside a locked row, the
//! unfail task's recovery of a transaction failed on a false word
//! (`TakeRows::LiftFailed`), each inside the take's write, and a checked
//! proof's completion of a `failed` row (`proof_completion`).

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

/// The notes on a request's history that record bytes handed to an
/// instance: the door's post, the wallet's own post (its stored post's
/// words and its immediate post's hint), send-waiting's post, a refusal
/// heard from a broadcaster, a lost proof's re-announce.
const POST_NOTES: [&str; 6] = [
    "doorPost",
    "processActionPost",
    "immediateBroadcastHint",
    "sendWaitingHint",
    "broadcasterRefusalHint",
    "lostProofAnnounce",
];

/// May `txid`'s bytes have left for an instance, by the store's own
/// records? A `broadcast_seen` row for the txid naming an instance (any
/// word: a release turns an instance's row `rejected` and keeps it), or the
/// network's row with any word but the release's own `rejected`; a post
/// note on its request's history ([`POST_NOTES`]); or a history the store
/// cannot read whole (not a JSON object, or as many notes as it keeps, the
/// older ones evicted): yes. No request, or a request whose history shows
/// no post (a `noSend` action's, an unsigned one's): no. `attempts` is not
/// read: the abort, the release and the proof asks count on it too.
async fn may_have_left_on(conn: &mut SqliteConnection, txid: &str) -> Result<bool> {
    let seen: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM broadcast_seen WHERE txid = ? \
         AND (provider <> 'network' OR status <> 'rejected')",
    )
    .bind(txid)
    .fetch_one(&mut *conn)
    .await?;
    if seen > 0 {
        return Ok(true);
    }
    let history: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some(history) = history else {
        return Ok(false);
    };
    let history = match serde_json::from_str::<serde_json::Value>(&history) {
        Ok(serde_json::Value::Object(map)) => map,
        _ if history.trim().is_empty() => return Ok(false),
        _ => return Ok(true),
    };
    let notes = match history.get("notes") {
        None => return Ok(false),
        Some(serde_json::Value::Array(notes)) => notes,
        Some(_) => return Ok(true),
    };
    if notes.len() >= super::storage_sqlx::REQ_HISTORY_NOTES_KEPT {
        return Ok(true);
    }
    Ok(notes.iter().any(|n| {
        n.get("what")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|w| POST_NOTES.contains(&w))
    }))
}

/// A COIN WHOSE SPENDER WAS GIVEN UP (the keeper's reading 24): in the
/// write that recovers `transaction_id`, each of its outputs spent by a
/// `failed` descendant whose bytes never left for an instance
/// ([`may_have_left_on`]) is released: no spender, spendable as its other
/// outputs are. An output a descendant spent whose bytes may have left
/// stays as it is (it returns by that descendant's own take, its proof, or
/// a competitor's proof against it). The descendant stays ended either
/// way; only the recovered row's own outputs are read. Returns the ids.
async fn release_given_up_spends_on(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let spent: Vec<(i64, Option<String>)> = sqlx::query_as(
        "SELECT o.output_id, d.txid FROM outputs o JOIN transactions d ON d.transaction_id = o.spent_by \
         WHERE o.transaction_id = ? AND d.status = 'failed' ORDER BY o.output_id",
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut released = Vec::new();
    for (output_id, spender) in spent {
        if let Some(spender) = spender.as_deref() {
            if may_have_left_on(conn, spender).await? {
                continue;
            }
        }
        sqlx::query(
            "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE output_id = ?",
        )
        .bind(now)
        .bind(output_id)
        .execute(&mut *conn)
        .await?;
        released.push(output_id);
    }
    Ok(released)
}

/// After a recovery of `txid`'s row, on the take's write: an input another
/// live action holds stays with it, and the recovered txid is queued as
/// that action's competitor, so the competitor proof ask settles the two by
/// a proof; the abort's mark, where it named the row, is marked recovered.
/// Returns the holding actions' txids.
async fn queue_holders_on(
    conn: &mut SqliteConnection,
    txid: &str,
    was_marked: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<String>> {
    let holders = super::proof_completion::losers_of(conn, txid).await?;
    for holder in &holders {
        super::proof_completion::queue_competitor_on(conn, holder, txid).await?;
    }
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
    Ok(holders)
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
        let given_up = release_given_up_spends_on(conn, transaction_id, now).await?;
        remark_inputs_spent_on(conn, txid, &skip, now).await?;
        let holders = queue_holders_on(conn, txid, was_marked, now).await?;
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
                "givenUpSpends": given_up,
                "competitorOf": holders,
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

/// `raw` as `txid`'s transaction: parsed, and answered only when it hashes
/// to `txid` (bytes are a transaction's only when they hash to its txid).
pub(crate) fn transaction_of(txid: &str, raw: &[u8]) -> Option<bsv_rs::transaction::Transaction> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    tx.id().eq_ignore_ascii_case(txid).then_some(tx)
}

/// The store's own copy of `txid`'s transaction (the request's, a row's or
/// the proof row's), the first that hashes to `txid` ([`transaction_of`]);
/// `None` when no copy is held or none hashes to it. One read.
pub(crate) async fn held_transaction_on(
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Option<bsv_rs::transaction::Transaction>> {
    let copies: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         UNION ALL SELECT raw_tx FROM transactions WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         UNION ALL SELECT raw_tx FROM proven_txs WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    let held = copies.iter().find_map(|raw| transaction_of(txid, raw));
    if held.is_none() {
        tracing::warn!(
            txid = %txid,
            copies = copies.len(),
            marker = "held_bytes_do_not_hash",
            "no held copy of the transaction hashes to its txid: its outpoints are not read"
        );
    }
    Ok(held)
}

/// THE ONE RECOVERY OF A `failed` TRANSACTION'S INPUTS, on the caller's
/// write: every wallet-tracked output row at each of the transaction's
/// input outpoints that is still free (or consumed with no spender
/// recorded) becomes unspendable (the outpoint is consumed), `spent_by` the
/// row owner's own transaction row for `txid` where one exists, except a
/// row named in `skip`. A row another transaction spends is never taken
/// from it. The outpoints are read from the store's own copy of the
/// transaction that hashes to `txid` ([`held_transaction_on`]); without
/// one nothing is marked. Returns whether such a copy was held.
pub(crate) async fn remark_inputs_spent_on(
    conn: &mut SqliteConnection,
    txid: &str,
    skip: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    let Some(tx) = held_transaction_on(conn, txid).await? else {
        return Ok(false);
    };
    for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&tx) {
        remark_outpoint(conn, txid, (&source_txid, vout), skip, now).await?;
    }
    Ok(true)
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
