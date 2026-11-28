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
//!   nothing, whose recorded spender is another txid: a competitor queued
//!   on its request with a checked proof whose bytes spend it,
//!   `word_scope::spent_proven_against_on`): the take is refused as a standing competitor's checked
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
    /// outpoints, the row spent by it or by nothing, its recorded spender
    /// another txid): nothing is recovered.
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
        for outpoint in outpoints {
            if super::word_scope::spent_proven_against_on(
                &mut *conn,
                txid,
                outpoint,
                Some(transaction_id),
            )
            .await?
            {
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

/// What the store's own records say of a `failed` descendant's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostRecord {
    /// A positive ground that the bytes never left: a request whose whole
    /// history reads and shows no post, or no request beside a row that was
    /// never signed.
    NeverPosted,
    /// A record of a post, or records the store cannot read whole: the
    /// bytes may have left, or the store cannot tell.
    MayHaveLeft,
}

/// What a request's `history` says of a post ([`post_record_on`]).
fn history_record(history: &str) -> PostRecord {
    let history = match serde_json::from_str::<serde_json::Value>(history) {
        Ok(serde_json::Value::Object(map)) => map,
        _ if history.trim().is_empty() => return PostRecord::NeverPosted,
        _ => return PostRecord::MayHaveLeft,
    };
    if history.contains_key("previous") {
        return PostRecord::MayHaveLeft;
    }
    let notes = match history.get("notes") {
        None => return PostRecord::NeverPosted,
        Some(serde_json::Value::Array(notes)) => notes,
        Some(_) => return PostRecord::MayHaveLeft,
    };
    if notes.len() >= super::storage_sqlx::REQ_HISTORY_NOTES_KEPT {
        return PostRecord::MayHaveLeft;
    }
    let posted = notes.iter().any(|n| {
        n.get("what")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|w| POST_NOTES.contains(&w))
    });
    if posted {
        PostRecord::MayHaveLeft
    } else {
        PostRecord::NeverPosted
    }
}

/// Did the bytes of the descendant `transaction_id` (`txid` where its row
/// has one) leave for an instance, by the store's own records? "Never
/// posted" needs a positive ground; a missing record is not one.
///
/// - A `broadcast_seen` row for the txid naming an instance (any word: a
///   release turns an instance's row `rejected` and keeps it), or the
///   network's row with any word but the release's own `rejected`: may
///   have left.
/// - A request whose whole history reads (a JSON object with no `previous`
///   key, fewer notes than it keeps): never posted where no note of
///   [`POST_NOTES`] is on it (a `noSend` action's), else may have left.
/// - A request whose history the store cannot read whole (not a JSON
///   object, a `previous` key holding an older history kept unparsed, as
///   many notes as it keeps, the older ones evicted): cannot tell.
/// - NO REQUEST (the purge deletes an ended request with its history):
///   never posted only where the descendant's own row was never signed,
///   `transactions.txid` and `transactions.raw_tx` both empty (the signed
///   bytes and their txid reach the row in `process_action`'s write, the
///   one that makes the request; an action the wallet signed and never
///   handed to that write reads so, and the row's word before its end is
///   not kept). A signed row with no request: cannot tell.
///
/// `attempts` is not read: the abort, the release and the proof asks count
/// on it too.
async fn post_record_on(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    txid: Option<&str>,
) -> Result<PostRecord> {
    let signed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE transaction_id = ? \
         AND (COALESCE(txid, '') <> '' OR COALESCE(length(raw_tx), 0) > 0)",
    )
    .bind(transaction_id)
    .fetch_one(&mut *conn)
    .await?;
    let Some(txid) = txid.filter(|t| !t.is_empty()) else {
        return Ok(if signed == 0 {
            PostRecord::NeverPosted
        } else {
            PostRecord::MayHaveLeft
        });
    };
    let seen: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM broadcast_seen WHERE txid = ? \
         AND (provider <> 'network' OR status <> 'rejected')",
    )
    .bind(txid)
    .fetch_one(&mut *conn)
    .await?;
    if seen > 0 {
        return Ok(PostRecord::MayHaveLeft);
    }
    let history: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some(history) = history else {
        // No request: a missing record is not a no.
        return Ok(if signed == 0 {
            PostRecord::NeverPosted
        } else {
            PostRecord::MayHaveLeft
        });
    };
    Ok(history_record(&history))
}

/// A COIN WHOSE SPENDER WAS GIVEN UP (the keeper's reading 24): in the
/// write that recovers `transaction_id`, each of its outputs spent by a
/// `failed` descendant the store's records show was never posted
/// ([`post_record_on`]) is released: no spender, spendable as its other
/// outputs are. An output a descendant spent whose bytes may have left, or
/// of which the store cannot tell, stays as it is (it returns by that
/// descendant's own take, its proof, or a competitor's proof against it).
/// An output with a `spent-proven` check recorded at its own outpoint is
/// never released. The descendant stays ended either way; only the
/// recovered row's own outputs are read. Returns the ids.
async fn release_given_up_spends_on(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let spent: Vec<(i64, i64, Option<String>)> = sqlx::query_as(
        "SELECT o.output_id, d.transaction_id, d.txid FROM outputs o \
         JOIN transactions d ON d.transaction_id = o.spent_by \
         WHERE o.transaction_id = ? AND d.status = 'failed' ORDER BY o.output_id",
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut released = Vec::new();
    for (output_id, descendant, spender) in spent {
        // The output's own verdict: a `spent-proven` check recorded at its
        // outpoint says the chain shows it spent, and it is not released.
        let spent_proven: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM locked_input_checks WHERE output_id = ? AND last_verdict = ?",
        )
        .bind(output_id)
        .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
        .fetch_one(&mut *conn)
        .await?;
        if spent_proven > 0
            || post_record_on(conn, descendant, spender.as_deref()).await?
                != PostRecord::NeverPosted
        {
            continue;
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

/// What a recovery of a released row answers to.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RecoveryCause<'a> {
    /// A take heard: each `(provider, word)`.
    Take(&'a [(&'a str, &'a str)]),
    /// A request that read `completed` beside a proof the tracker refutes
    /// (`refuted_recovery`).
    RefutedProof,
}

impl<'a> RecoveryCause<'a> {
    /// The cause's name on the request's history and the takers it names.
    fn named(self, was_marked: bool) -> (&'static str, Vec<&'a str>) {
        match self {
            Self::Take(takers) => (
                if was_marked {
                    "takenAfterAbort"
                } else {
                    "takenAfterRelease"
                },
                takers.iter().map(|(p, _)| *p).collect(),
            ),
            Self::RefutedProof => ("recoveredOnRefutedProof", Vec::new()),
        }
    }
}

/// THE RECOVERY OF A ROW RELEASED ON NO EVIDENCE, on the caller's write
/// (see the module): every `failed` outgoing row of `txid` in
/// [`FailedState::Released`] is recovered. Two callers: the take's write
/// and the demotion of a refuted proof. Returns the rows recovered.
pub(crate) async fn recover_released_on(
    conn: &mut SqliteConnection,
    txid: &str,
    rows: &[FailedRow],
    cause: RecoveryCause<'_>,
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
        let (what, takers) = cause.named(was_marked);
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
                "takers": takers,
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            transaction_id,
            marked = was_marked,
            cause = what,
            marker = "taken_after_release",
            "a release made on no evidence is undone: the transaction unproven, its free inputs marked again"
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
/// the proof row's), one that hashes to `txid` ([`transaction_of`]);
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
