//! The recovery of a `failed` row beside a proof row this store's tracker
//! refutes, called from the refuted arms of the status review's pass
//! (`invalid_beside_proof.rs`), inside the pass's one write.

use super::storage_sqlx::append_req_history_note;
use crate::Result;

/// A refuted arm beside a `failed` row of the txid: a proof row of the
/// txid says the transaction was mined, and a root this store's headers do
/// not carry does not unsay that (its headers may lag, or the block may
/// have left the chain), so the wallet's own released row comes back
/// toward caution in the caller's write, no service asked.
///
/// A `failed` row of `txid` is recovered only when all four hold:
/// 1. it is an outgoing row (the wallet's own action);
/// 2. the request of the txid does not read `doubleSpend`;
/// 3. the row's own bytes parse and hash to `txid`;
/// 4. at least one output row of the row's own user at one of its input
///    outpoints is free (spendable, and spent by none or by this row).
///
/// It then reads `unproven`, and each such free output row of that user is
/// marked spent by it again. A coin another action has taken, an output
/// row of any other user at the same outpoint, and the row's own outputs
/// are left exactly as they are. The caller writes the request asking.
///
/// Any other `failed` row of the txid (a received row, a row beside a
/// `doubleSpend` request, bytes that do not parse or hash to another txid,
/// a row whose inputs are all still its own or not this user's coins)
/// stays `failed` and nothing is re-marked. The link of every `failed` row
/// of the txid is cleared (the caller removes the proof row). Returns how
/// many rows it recovered.
pub(super) async fn recover_failed_rows_on(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<usize> {
    let failed: Vec<(i64, i64, bool, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT transaction_id, user_id, is_outgoing, raw_tx FROM transactions \
         WHERE txid = ? AND status = 'failed'",
    )
    .bind(txid)
    .fetch_all(&mut *tx)
    .await?;
    if failed.is_empty() {
        return Ok(0);
    }
    let word: Option<String> =
        sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *tx)
            .await?;
    let mut recovered = 0usize;
    for (transaction_id, user_id, is_outgoing, raw_tx) in failed {
        if !is_outgoing || word.as_deref() == Some("doubleSpend") {
            continue;
        }
        let parsed = raw_tx
            .as_deref()
            .and_then(|r| bsv_rs::transaction::Transaction::from_binary(r).ok())
            .filter(|t| t.id() == txid);
        let Some(parsed) = parsed else {
            tracing::warn!(
                txid = %txid,
                transaction_id,
                "review_status: a failed row beside a refuted proof holds no bytes that hash to its txid; left failed"
            );
            continue;
        };
        let mut free: Vec<i64> = Vec::new();
        for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&parsed) {
            let rows: Vec<(i64, bool, Option<i64>)> = sqlx::query_as(
                "SELECT output_id, spendable, spent_by FROM outputs \
                 WHERE txid = ? AND vout = ? AND user_id = ?",
            )
            .bind(&source_txid)
            .bind(vout as i64)
            .bind(user_id)
            .fetch_all(&mut *tx)
            .await?;
            for (output_id, spendable, spent_by) in rows {
                if spendable && (spent_by.is_none() || spent_by == Some(transaction_id)) {
                    free.push(output_id);
                }
            }
        }
        if free.is_empty() {
            continue;
        }
        for output_id in free {
            sqlx::query(
                "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
            )
            .bind(transaction_id)
            .bind(now)
            .bind(output_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE transactions SET status = 'unproven', proven_tx_id = NULL, updated_at = ? \
             WHERE transaction_id = ? AND status = 'failed'",
        )
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *tx)
        .await?;
        recovered += 1;
    }
    sqlx::query(
        "UPDATE transactions SET proven_tx_id = NULL, updated_at = ? \
         WHERE txid = ? AND status = 'failed' AND proven_tx_id IS NOT NULL",
    )
    .bind(now)
    .bind(txid)
    .execute(&mut *tx)
    .await?;
    Ok(recovered)
}

/// After a refuted arm beside a `completed` request: where every
/// transaction row of `txid` reads `failed`, the request is written
/// `invalid`, this line's word for an ended transaction (the word the
/// abort and the host's retire write beside a `failed` row, which no
/// proof or send task asks and the unfail task's sweep reads), with the
/// history note `refutedProofBesideFailedRow`. Returns whether it wrote.
pub(super) async fn end_request_beside_failed_rows_on(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    if live_rows_on(&mut *tx, txid).await? > 0 {
        return Ok(false);
    }
    let history: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(history) = history else {
        return Ok(false);
    };
    let history = append_req_history_note(
        &history,
        serde_json::json!({
            "when": now.to_rfc3339(),
            "what": "refutedProofBesideFailedRow",
            "why": "the proof row is refuted by this store's headers and every transaction row of the txid reads failed: the request does not ask",
        }),
    );
    sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'invalid', history = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(history)
    .bind(now)
    .bind(txid)
    .execute(&mut *tx)
    .await?;
    Ok(true)
}

/// How many transaction rows of `txid` do not read `failed`.
pub(super) async fn live_rows_on(tx: &mut sqlx::SqliteConnection, txid: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE txid = ? AND status != 'failed'",
    )
    .bind(txid)
    .fetch_one(&mut *tx)
    .await?)
}

/// The request's history says why its `failed` rows came back: the note
/// `refutedProofRecoversFailedRow`, logged with marker
/// `refuted_proof_recovers_failed_row`.
pub(super) async fn note_recovery_on(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    recovered: usize,
    height: u32,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let history: Option<String> =
        sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *tx)
            .await?;
    if let Some(history) = history {
        let history = append_req_history_note(
            &history,
            serde_json::json!({
                "when": now.to_rfc3339(),
                "what": "refutedProofRecoversFailedRow",
                "why": "a proof row says the transaction was mined and this store's headers do not carry its root: the failed row is unproven again, its free inputs spent by it, its proof asked for",
                "height": height,
            }),
        );
        sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE txid = ?")
            .bind(history)
            .bind(txid)
            .execute(&mut *tx)
            .await?;
    }
    tracing::warn!(
        txid = %txid,
        height,
        recovered,
        marker = "refuted_proof_recovers_failed_row",
        "review_status: a refuted proof row beside a failed row; the row is unproven again, its free inputs spent by it, its proof asked for"
    );
    Ok(())
}
