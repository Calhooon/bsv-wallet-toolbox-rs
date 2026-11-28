//! A word ends only the rows of the user whose word it is (the keeper's
//! reading 27, section 30's reading 11).
//!
//! A request is one per txid, and several users of one store may hold a row
//! of that txid: the sender's own action (`is_outgoing = 1`, written by its
//! create door) and a receipt of another user (`is_outgoing = 0`, written by
//! the internalize door). A request's word (`invalid` from the sender's
//! abort, a host's retire, a broadcaster's refusal of the sender's post) is
//! the act of the user who built the transaction, the one user whose row of
//! it is outgoing: it ends the outgoing rows and no other. A received row
//! beside it stays as it is (held `sending`, or `unproven` where an instance
//! took it) and ends on evidence about its transaction, as any held
//! receipt does.
//!
//! Evidence about the transaction is the same for every user and ends every
//! row of the txid. It is a proof this store checked, read from the store's
//! own records on the caller's write ([`evidence_on_record_on`]): a stored
//! competitor (an output row at one of the txid's input outpoints whose
//! `spent_by` names another transaction with a `proven_txs` row this store
//! checked, its root-check record, whose bytes spend that outpoint), or a
//! `spent-proven` check (`locked_input_checks`) on an output row at one of
//! its input outpoints whose recorded spender is another txid (a proof row
//! this store checked whose bytes spend that outpoint, found by the
//! outpoint, `competitor_proof::checked_spenders_on`); the records the take guard
//! reads (`competitor_proof.rs`, `abort_recovery::failed_outgoing_rows`).
//! A `spent-proven` check with no spender on record is evidence too, unless
//! the txid holds a proof this store checked. A proof row this store has
//! not checked, and a `spent-proven` check whose recorded spender is the
//! txid itself, are not evidence.

use sqlx::SqliteConnection;

use crate::Result;

/// Does the store hold evidence against `txid` on record: a stored
/// competitor this store checked, or a `spent-proven` check at one of its
/// input outpoints that is evidence against it ([`spent_proven_against_on`])? The outpoints
/// are read from a held copy that hashes to the txid; with none, the
/// outpoints of the outputs a row of the txid spends.
pub(crate) async fn evidence_on_record_on(conn: &mut SqliteConnection, txid: &str) -> Result<bool> {
    let mut outpoints: Vec<(String, u32)> =
        super::abort_recovery::held_transaction_on(&mut *conn, txid)
            .await?
            .map(|tx| super::competitor_proof::inputs_of(&tx))
            .unwrap_or_default();
    if outpoints.is_empty() {
        let spent: Vec<(String, i64)> = sqlx::query_as(
            "SELECT lower(o.txid), o.vout FROM outputs o \
             JOIN transactions t ON t.transaction_id = o.spent_by \
             WHERE t.txid = ? AND o.txid IS NOT NULL",
        )
        .bind(txid)
        .fetch_all(&mut *conn)
        .await?;
        outpoints = spent
            .into_iter()
            .filter_map(|(t, v)| Some((t, u32::try_from(v).ok()?)))
            .collect();
    }
    if super::competitor_proof::stored_competitor_on(&mut *conn, txid, &outpoints)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    for outpoint in &outpoints {
        if spent_proven_against_on(&mut *conn, txid, outpoint, None).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Is there a `spent-proven` check at `outpoint` (on an output row there,
/// `spent_by` narrowed to `spent_by_row` or nothing when given) that is
/// evidence against `txid`? It is, unless the recorded spender is `txid`
/// itself, or no spender is on record and `txid` holds a proof this store
/// checked (then the mark is its own spend).
pub(crate) async fn spent_proven_against_on(
    conn: &mut SqliteConnection,
    txid: &str,
    outpoint: &(String, u32),
    spent_by_row: Option<i64>,
) -> Result<bool> {
    let (source, vout) = outpoint;
    let proven: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs o \
         JOIN locked_input_checks c ON c.output_id = o.output_id \
         WHERE lower(o.txid) = lower(?1) AND o.vout = ?2 AND c.last_verdict = ?3 \
           AND (?4 IS NULL OR o.spent_by IS NULL OR o.spent_by = ?4)",
    )
    .bind(source)
    .bind(i64::from(*vout))
    .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
    .bind(spent_by_row)
    .fetch_one(&mut *conn)
    .await?;
    if proven == 0 {
        return Ok(false);
    }
    let spenders = super::competitor_proof::checked_spenders_on(conn, outpoint).await?;
    if spenders.iter().any(|s| !s.eq_ignore_ascii_case(txid)) {
        return Ok(true);
    }
    if !spenders.is_empty() {
        // The recorded spender is the txid itself: the mark is its own spend.
        return Ok(false);
    }
    // No spender on record (a mark the locked-input re-check wrote): it is
    // evidence unless the txid holds a proof this store checked.
    let sql = format!(
        "SELECT {}",
        super::held_receipts::checked_proof_condition("?1")
    );
    let checked: bool = sqlx::query_scalar(&sql)
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
    Ok(!checked)
}

/// The rows a request's word ends, of `candidates` (`transaction_id`,
/// `txid`, `is_outgoing`): every outgoing row, and a received row only
/// where the store holds evidence against its txid. Read on the caller's
/// write; one evidence read per txid that has a received candidate.
pub(crate) async fn rows_a_word_ends_on(
    conn: &mut SqliteConnection,
    candidates: &[(i64, String, bool)],
) -> Result<Vec<i64>> {
    let mut evidence: Vec<(String, bool)> = Vec::new();
    let mut ended = Vec::with_capacity(candidates.len());
    for (transaction_id, txid, is_outgoing) in candidates {
        if *is_outgoing {
            ended.push(*transaction_id);
            continue;
        }
        let known = evidence.iter().find(|(t, _)| t == txid).map(|(_, e)| *e);
        let against = if let Some(known) = known {
            known
        } else {
            let read = evidence_on_record_on(&mut *conn, txid).await?;
            evidence.push((txid.clone(), read));
            read
        };
        if against {
            ended.push(*transaction_id);
        } else {
            tracing::info!(
                txid = %txid,
                transaction_id = *transaction_id,
                marker = "received_row_kept_beside_word",
                "a request's word beside a received row with no evidence against its transaction: the row keeps its word"
            );
        }
    }
    Ok(ended)
}
