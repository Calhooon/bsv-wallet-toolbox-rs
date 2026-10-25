use super::{
    commit, parse_input_txids, rebuild_beef_for_broadcast, Error, Result, StorageSqlx,
    WalletStorageReader,
};

/// Demote one stored proof on an open connection: REVERT TO THE PRE-PROOF
/// STATE with the bytes preserved, inside the caller's transaction, in this
/// order (steps 1 and 2 clear the foreign keys the delete in step 4 needs
/// cleared):
///
/// 1. `proven_tx_reqs`: the row goes back to `unmined`, attempts 0, no proof
///    link; when the completed row had been purged, a new `unmined` row is
///    inserted with the raw bytes taken from the proof row, so the bytes
///    survive even after the 30-day purge cleared `transactions.raw_tx`.
/// 2. `transactions`: every row linked to the proof (and a `completed` row
///    for the txid left without a link) goes back to `unproven`, its
///    `raw_tx` restored from the proof row when the purge had cleared it.
///    `unproven` is in every spendable set (coin selection, `list_outputs`,
///    `list_actions`, the adoption pass), so the coins stay visible.
/// 3. `broadcast_seen`: the terminal `mined` rows are forgotten
///    ([`super::broadcast_seen::forget_mined_on`], the one sanctioned
///    downgrade), so reduced sends carry the transaction again.
/// 4. the `proven_txs` row is deleted.
///
/// Returns whether a proof row existed. The BEEF walk calls this only for a
/// row that was never checked (no `proof_root_checks` record) and that the
/// tracker refutes (`check_unchecked_stored_proof_on`); a checked row's
/// refuted bump is skipped there and demoted by the reorg or review task
/// when the local header read finds its root gone (ruling 5 of 2026-10-20).
/// No input is released and no status is failed: the action is `unproven`
/// ("confirming") until its transaction is proven again.
pub(in crate::storage::sqlx) async fn demote_stale_proof_on(
    conn: &mut sqlx::SqliteConnection,
    txid: &str,
) -> Result<bool> {
    let now = chrono::Utc::now();
    let existing: Option<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT proven_tx_id, raw_tx FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((proven_tx_id, raw_tx)) = existing else {
        return Ok(false);
    };
    // 1. the request: back to unmined with its bytes.
    let updated = sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'unmined', attempts = 0, proven_tx_id = NULL, raw_tx = COALESCE(raw_tx, ?), updated_at = ? WHERE txid = ?",
    )
    .bind(&raw_tx)
    .bind(now)
    .bind(txid)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if updated == 0 {
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, notified, history, notify, raw_tx, input_beef, created_at, updated_at) \
             VALUES (?, 'unmined', 0, 0, '{}', '{}', ?, NULL, ?, ?)",
        )
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }
    // 2. the transactions: back to unproven, bytes restored.
    sqlx::query(
        "UPDATE transactions SET status = 'unproven', proven_tx_id = NULL, raw_tx = COALESCE(raw_tx, ?), updated_at = ? \
         WHERE proven_tx_id = ? OR (txid = ? AND status = 'completed' AND proven_tx_id IS NULL)",
    )
    .bind(&raw_tx)
    .bind(now)
    .bind(proven_tx_id)
    .bind(txid)
    .execute(&mut *conn)
    .await?;
    // 3. the mined memory: the one sanctioned downgrade.
    let forgotten = super::broadcast_seen::forget_mined_on(&mut *conn, txid).await?;
    // 4. the proof row, and its root-check record.
    sqlx::query("DELETE FROM proven_txs WHERE proven_tx_id = ?")
        .bind(proven_tx_id)
        .execute(&mut *conn)
        .await?;
    super::proof_root_checks::forget_root_check_on(&mut *conn, txid).await?;
    tracing::warn!(
        txid = %txid,
        proven_tx_id,
        request_recreated = updated == 0,
        mined_rows_forgotten = forgotten,
        marker = "stale_proof_demoted",
        "demote_stale_proof: the stored proof is gone; the transaction is unproven again with its bytes in the request and will be re-proved"
    );
    Ok(true)
}

impl StorageSqlx {
    pub(super) async fn demote_stale_proof_impl(&self, txid: &str) -> Result<bool> {
        // One transaction: either every step of the revert lands or none.
        let mut tx = self.write_tx().await?;
        let existed = demote_stale_proof_on(&mut tx, txid).await?;
        commit(tx).await?;
        Ok(existed)
    }

    pub(super) async fn announce_lost_proof_impl(
        &self,
        txid: &str,
    ) -> Result<crate::storage::BroadcastOutcome> {
        use crate::storage::{classify_broadcast_results, BroadcastOutcome};
        let services = self.get_services()?;
        let raw: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT raw_tx FROM proven_tx_reqs WHERE txid = ? AND raw_tx IS NOT NULL \
             UNION ALL SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1",
        )
        .bind(txid)
        .bind(txid)
        .fetch_optional(self.pool())
        .await?;
        let Some((raw_tx,)) = raw else {
            return Err(Error::NotFound {
                entity: "transaction bytes".to_string(),
                id: txid.to_string(),
            });
        };
        let input_txids = parse_input_txids(&raw_tx)?;
        let mut beef = {
            let mut conn = self.pool().acquire().await?;
            rebuild_beef_for_broadcast(&mut conn, self.writer(), &input_txids, Some(self)).await?
        };
        beef.merge_raw_tx(raw_tx, None);
        let results = services
            .post_beef(&beef.to_binary(), &[txid.to_string()])
            .await?;
        let outcome = classify_broadcast_results(&results);
        let word = match &outcome {
            BroadcastOutcome::Success => "accepted",
            BroadcastOutcome::OrphanMempool { .. } => "orphanMempool",
            BroadcastOutcome::ServiceError { .. } => "serviceError",
            BroadcastOutcome::DoubleSpend { .. } => "doubleSpend",
            BroadcastOutcome::InvalidTx { .. } => "rejected",
        };
        if let BroadcastOutcome::DoubleSpend { competing_txs, .. } = &outcome {
            self.queue_competitors(txid, competing_txs).await?;
        }
        self.note_on_req_history(
            txid,
            serde_json::json!({
                "when": chrono::Utc::now().to_rfc3339(),
                "what": "lostProofAnnounce",
                "word": word,
                "detail": outcome.error_message(txid),
            }),
        )
        .await?;
        tracing::info!(
            txid = %txid,
            word,
            marker = "lost_proof_announced",
            "lost proof: no broadcaster knew the transaction; announced once"
        );
        Ok(outcome)
    }

    pub(super) async fn note_lost_proof_refusal_impl(
        &self,
        txid: &str,
        verdict: &str,
    ) -> Result<()> {
        self.note_on_req_history(
            txid,
            serde_json::json!({
                "when": chrono::Utc::now().to_rfc3339(),
                "what": "lostProofRefusal",
                "verdict": verdict,
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            verdict,
            marker = "lost_proof_refused",
            "lost proof: a broadcaster refuses the transaction; written, no status moves, a competitor's checked proof decides"
        );
        Ok(())
    }
}
