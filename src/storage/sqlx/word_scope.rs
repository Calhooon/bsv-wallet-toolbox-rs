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
//! row of the txid. The store's own records of it, read on the caller's
//! write ([`evidence_on_record_on`]): a stored competitor (an output row at
//! one of the txid's input outpoints whose `spent_by` names another
//! transaction with a `proven_txs` row whose bytes spend that outpoint), or
//! a `spent-proven` check (`locked_input_checks`) on an output row at one of
//! its input outpoints, the two records the take guard reads
//! (`competitor_proof.rs`, `abort_recovery::failed_outgoing_rows`).

use sqlx::SqliteConnection;

use crate::Result;

/// Does the store hold evidence against `txid` on record: a stored
/// competitor, or a `spent-proven` check at one of its input outpoints?
/// The outpoints are read from a held copy that hashes to the txid; with
/// none, the `spent-proven` checks on the outputs a row of the txid spends.
pub(crate) async fn evidence_on_record_on(conn: &mut SqliteConnection, txid: &str) -> Result<bool> {
    let outpoints: Vec<(String, u32)> =
        super::abort_recovery::held_transaction_on(&mut *conn, txid)
            .await?
            .map(|tx| super::competitor_proof::inputs_of(&tx))
            .unwrap_or_default();
    if super::competitor_proof::stored_competitor_on(&mut *conn, txid, &outpoints)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    for (source, vout) in &outpoints {
        let proven: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outputs o \
             JOIN locked_input_checks c ON c.output_id = o.output_id \
             WHERE lower(o.txid) = lower(?) AND o.vout = ? AND c.last_verdict = ?",
        )
        .bind(source)
        .bind(i64::from(*vout))
        .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
        .fetch_one(&mut *conn)
        .await?;
        if proven > 0 {
            return Ok(true);
        }
    }
    let spent: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs o \
         JOIN locked_input_checks c ON c.output_id = o.output_id \
         JOIN transactions t ON t.transaction_id = o.spent_by \
         WHERE t.txid = ? AND c.last_verdict = ?",
    )
    .bind(txid)
    .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
    .fetch_one(&mut *conn)
    .await?;
    Ok(spent > 0)
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
