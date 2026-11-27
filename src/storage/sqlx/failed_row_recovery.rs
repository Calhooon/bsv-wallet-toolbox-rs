//! The recovery of a `failed` row beside a proof row this store's tracker
//! refutes, called from the refuted arms of the status review's pass
//! (`invalid_beside_proof.rs`), inside the pass's one write.

use super::storage_sqlx::append_req_history_note;
use crate::Result;

/// A refuted arm beside a `failed` row of the txid: a proof row of the
/// txid says the transaction was mined, and a root this store's headers do
/// not carry does not unsay that (its headers may lag, or the block may
/// have left the chain), so the row comes back toward caution in the
/// caller's write, no service asked: each `failed` row of `txid` reads `unproven` with its
/// link cleared, its own outputs left exactly as the failure left them,
/// and each of its inputs still free (spendable, and spent by none or by
/// this row) marked spent by it again, as the unfail task re-marks
/// (`remark_unfailed_inputs_spent`): a coin another action has taken is
/// skipped and stays with that action. The caller leaves the request
/// asking for the proof. Returns how many rows it recovered.
pub(super) async fn recover_failed_rows_on(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<usize> {
    let failed: Vec<(i64, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT transaction_id, raw_tx FROM transactions WHERE txid = ? AND status = 'failed'",
    )
    .bind(txid)
    .fetch_all(&mut *tx)
    .await?;
    if failed.is_empty() {
        return Ok(0);
    }
    let raw: Option<Vec<u8>> = match failed.iter().find_map(|(_, r)| r.clone()) {
        Some(r) => Some(r),
        None => {
            sqlx::query_scalar("SELECT raw_tx FROM proven_txs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&mut *tx)
                .await?
        }
    };
    let outpoints = raw
        .as_deref()
        .and_then(|r| bsv_rs::transaction::Transaction::from_binary(r).ok())
        .map(|t| super::internalize_action::extract_input_outpoints(&t))
        .unwrap_or_default();
    if raw.is_some() && outpoints.is_empty() {
        tracing::debug!(txid = %txid, "review_status: a failed row's bytes name no input to re-mark");
    }
    for (source_txid, vout) in outpoints {
        let rows: Vec<(i64, i64, bool, Option<i64>)> = sqlx::query_as(
            "SELECT output_id, user_id, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
        )
        .bind(&source_txid)
        .bind(vout as i64)
        .fetch_all(&mut *tx)
        .await?;
        for (output_id, user_id, spendable, spent_by) in rows {
            let own: Option<i64> = sqlx::query_scalar(
                "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
            )
            .bind(txid)
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?;
            if !spendable {
                continue;
            }
            if spent_by.is_some() && spent_by != own {
                continue;
            }
            sqlx::query(
                "UPDATE outputs SET spendable = 0, spent_by = COALESCE(?, spent_by), updated_at = ? WHERE output_id = ?",
            )
            .bind(own)
            .bind(now)
            .bind(output_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    let recovered = sqlx::query(
        "UPDATE transactions SET status = 'unproven', proven_tx_id = NULL, updated_at = ? \
         WHERE txid = ? AND status = 'failed'",
    )
    .bind(now)
    .bind(txid)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    Ok(recovered as usize)
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
