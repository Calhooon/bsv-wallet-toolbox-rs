//! A CHECKED PROOF COMPLETES EVERY ROW OF ITS TXID IN THE STORE, A
//! `failed` ONE INCLUDED (the keeper's readings 19 and 20 of section 30).
//!
//! The proof is evidence; no word stands against it. Every writer of a
//! proof's completion (the proof path's `ingest_merkle_proof`, the create
//! door's broadcast with a proof in hand, the status review's check 3)
//! writes it through [`complete_on_proof`], on its own write transaction:
//! every row of the txid `completed` with the proof linked, and for each row
//! that read `failed` before the proof:
//!
//! - its inputs marked spent by it (they are, on chain), through the one
//!   recovery of a failed transaction's inputs
//!   (`abort_recovery::remark_inputs_spent_on`): each row at an input
//!   outpoint still free, or consumed with no spender recorded, becomes
//!   spent by its owner's own row of the txid;
//! - an input another action of the wallet spent meanwhile stays with that
//!   action, which is the loser of a proven conflict: the proven txid is
//!   queued on that action's request as a competitor (`competitorsQueued`),
//!   so the competitor proof ask (`ask_queued_competitor_proofs`) retires it
//!   on this proof through the release rule, the existing conflicted path;
//! - its own outputs that nothing spent spendable again (counted);
//! - the cause on the request's history (`provenOverFailed`, the rows).

use sqlx::{Row, SqliteConnection};

use crate::Result;

/// THE COMPLETION OF A CHECKED PROOF, on the caller's write (see the
/// module): every row of `txid` `completed` with `proven_tx_id` linked.
/// Returns the rows that read `failed` before it.
pub(crate) async fn complete_on_proof(
    conn: &mut SqliteConnection,
    txid: &str,
    proven_tx_id: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let failed: Vec<i64> = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions WHERE txid = ? AND status = 'failed' \
         ORDER BY transaction_id",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    sqlx::query(
        "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(proven_tx_id)
    .bind(now)
    .bind(txid)
    .execute(&mut *conn)
    .await?;
    if failed.is_empty() {
        return Ok(failed);
    }
    super::abort_recovery::remark_inputs_spent_on(conn, txid, &[], now).await?;
    let losers = queue_losers(conn, txid).await?;
    for transaction_id in &failed {
        sqlx::query(
            "UPDATE outputs SET spendable = 1, updated_at = ? \
             WHERE transaction_id = ? AND spendable = 0 AND spent_by IS NULL",
        )
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;
    }
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    if let Some((id, history)) = row {
        let history = super::storage_sqlx::append_req_history_note(
            &history,
            serde_json::json!({
                "what": "provenOverFailed",
                "when": now.to_rfc3339(),
                "transactionIds": failed,
                "losers": losers,
            }),
        );
        sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
            .bind(history)
            .bind(id)
            .execute(&mut *conn)
            .await?;
    }
    tracing::warn!(
        txid = %txid,
        rows = ?failed,
        losers = ?losers,
        marker = "proven_over_failed",
        "a checked proof completes a failed row: its inputs marked spent by it, its outputs counted"
    );
    Ok(failed)
}

/// Queue `txid` as a competitor on the request of every other transaction
/// the store records as spending one of `txid`'s input outpoints, not
/// itself ended: the losers of the proven conflict. Returns their txids.
async fn queue_losers(conn: &mut SqliteConnection, txid: &str) -> Result<Vec<String>> {
    let raw: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT raw_tx FROM proven_txs WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         UNION ALL SELECT raw_tx FROM transactions WHERE txid = ?1 AND raw_tx IS NOT NULL AND length(raw_tx) > 0 \
         LIMIT 1",
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(tx) = raw.and_then(|raw| bsv_rs::transaction::Transaction::from_binary(&raw).ok())
    else {
        return Ok(Vec::new());
    };
    let mut losers: Vec<String> = Vec::new();
    for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&tx) {
        let rows = sqlx::query(
            "SELECT t.txid FROM outputs o JOIN transactions t ON t.transaction_id = o.spent_by \
             WHERE lower(o.txid) = lower(?) AND o.vout = ? AND t.txid <> ? \
               AND t.status NOT IN ('failed', 'completed')",
        )
        .bind(&source_txid)
        .bind(i64::from(vout))
        .bind(txid)
        .fetch_all(&mut *conn)
        .await?;
        for row in rows {
            let loser: String = row.get("txid");
            if !losers.contains(&loser) {
                losers.push(loser);
            }
        }
    }
    for loser in &losers {
        let row: Option<(i64, String)> =
            sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
                .bind(loser)
                .fetch_optional(&mut *conn)
                .await?;
        let Some((id, history)) = row else {
            continue;
        };
        let mut object = match serde_json::from_str::<serde_json::Value>(&history) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
        let mut queued: Vec<serde_json::Value> = object
            .get("competitorsQueued")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default();
        let proven = serde_json::Value::from(txid.to_ascii_lowercase());
        if !queued.contains(&proven) {
            queued.push(proven);
        }
        object.insert(
            "competitorsQueued".to_string(),
            serde_json::Value::from(queued),
        );
        sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
            .bind(serde_json::Value::Object(object).to_string())
            .bind(id)
            .execute(&mut *conn)
            .await?;
    }
    Ok(losers)
}
