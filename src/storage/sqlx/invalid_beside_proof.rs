//! The status review's check 1 beside a proof: a request that reads
//! `invalid` for a transaction this store holds a proof row of.
//!
//! The word loses to the proof, wherever it came from (a broadcaster's
//! refusal, the unfail task's finding, the wallet's own end of a hold): a
//! proof whose root this store has checked against its own headers stands,
//! the transaction is passed over and its request brought to `completed`.
//! A proof row this store has not checked (one a sync merged from a local
//! reader, or one stored before migration 005) is checked before either word moves, as every
//! read of an unchecked stored proof is on this line
//! (`check_unchecked_stored_proof_on`): confirmed, it is recorded and
//! stands; refuted, it is demoted (`demote_stale_proof_on`: the transaction
//! `unproven` with its outputs counted, the request `unmined`, the proof task
//! asks for its proof again); with no tracker, a tracker fault or a path that
//! does not compute, nothing is written and the same row is a candidate next
//! pass.
//!
//! The order: a pass takes the candidates it has waited longest to look at,
//! by the request's `updated_at` (then the proof row's number), and stamps
//! `updated_at` on every candidate it looks at and leaves `invalid` (a path
//! that does not parse, no tracker, a tracker fault). A candidate that
//! never leaves the set goes to the back at each look, so it holds no slot
//! for good: with `INVALID_BESIDE_PROOF_PER_PASS` = 100, a candidate behind
//! N others is looked at within ceil((N + 1) / 100) passes, whatever the
//! others do, and a pass reads at most 100 headers. No status, link or
//! history moves on that stamp.
//!
//! A txid with a `failed` transaction row (one user's row of it failed,
//! another's live) is not a candidate: nothing of it is written here, no
//! status, link, history or stamp, and no header is read for it. The
//! unfail task takes its request once that has stood an hour, and recovers
//! the `failed` row when the network holds the transaction (the request
//! `unmined`, the row `unproven`, its outputs checked as unspent counted);
//! the proof task then completes every row of the txid. Check 1 fails no
//! row of it meanwhile, since a proof row of the txid exists.
//!
//! The tracker is asked with neither the writer nor a write transaction
//! held: the candidates are read on a reader, checked, and each is read
//! again inside the write before its request moves: the request still
//! `invalid`, the same proof row, its root still recorded as checked, and
//! no transaction of its txid `failed`. A transaction another path failed
//! meanwhile is left to the next pass, as it stands then (no longer a
//! candidate): its request is not completed, so check 3 does not write a
//! failed transaction `completed` beside outputs left unspendable.

use bsv_rs::transaction::MerklePath;

use super::proof_root_checks::{check_unchecked_stored_proof_on, stored_proof_checked_on};
use super::storage_sqlx::{append_req_history_note, commit, StorageSqlx};
use crate::Result;

/// At most this many candidates are checked per review pass, so at most
/// this many header reads per pass (a candidate already recorded as checked
/// costs none).
pub(super) const INVALID_BESIDE_PROOF_PER_PASS: i64 = 100;

impl StorageSqlx {
    /// Check 1's pass over the transactions that hold a proof row beside an
    /// `invalid` request. Returns how many requests it brought to
    /// `completed`.
    pub(super) async fn pass_over_proven_beside_invalid(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize> {
        let candidates: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
            r#"
            SELECT DISTINCT r.txid, p.proven_tx_id, p.merkle_path
            FROM proven_tx_reqs r
            JOIN proven_txs p ON p.txid = r.txid
            WHERE r.status = 'invalid'
              AND EXISTS (SELECT 1 FROM transactions t WHERE t.txid = r.txid)
              AND NOT EXISTS (SELECT 1 FROM transactions f
                              WHERE f.txid = r.txid AND f.status = 'failed')
            ORDER BY julianday(r.updated_at), p.proven_tx_id
            LIMIT ?
            "#,
        )
        .bind(INVALID_BESIDE_PROOF_PER_PASS)
        .fetch_all(self.pool())
        .await?;
        if candidates.is_empty() {
            return Ok(0);
        }

        let tracker = self.get_chain_tracker().await;
        let mut stands = Vec::new();
        {
            let mut conn = self.pool().acquire().await?;
            for (txid, proven_tx_id, merkle_path) in &candidates {
                let Ok(path) = MerklePath::from_binary(merkle_path) else {
                    tracing::debug!(
                        txid = %txid,
                        "review_status: a proof row beside an invalid request does not parse; left as it is"
                    );
                    continue;
                };
                let check = check_unchecked_stored_proof_on(
                    &mut conn,
                    Some(self.writer()),
                    tracker.as_deref(),
                    txid,
                    &path,
                )
                .await?;
                if check.may_store() {
                    stands.push((txid.clone(), *proven_tx_id));
                } else {
                    tracing::debug!(
                        txid = %txid,
                        check = ?check,
                        "review_status: a proof row beside an invalid request is not checked here; neither failed nor completed"
                    );
                }
            }
        }
        let mut tx = self.write_tx().await?;
        let mut completed = 0usize;
        let mut completed_txids = std::collections::HashSet::new();
        for (txid, proven_tx_id) in &stands {
            let row: Option<(String, Vec<u8>)> = sqlx::query_as(
                "SELECT r.history, p.merkle_path FROM proven_tx_reqs r \
                 JOIN proven_txs p ON p.txid = r.txid \
                 WHERE r.txid = ? AND r.status = 'invalid' AND p.proven_tx_id = ? \
                   AND NOT EXISTS (SELECT 1 FROM transactions t \
                                   WHERE t.txid = r.txid AND t.status = 'failed')",
            )
            .bind(txid)
            .bind(proven_tx_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((history, merkle_path)) = row else {
                continue;
            };
            let Ok(path) = MerklePath::from_binary(&merkle_path) else {
                continue;
            };
            if !stored_proof_checked_on(&mut tx, txid, &path).await? {
                continue;
            }
            let history = append_req_history_note(
                &history,
                serde_json::json!({
                    "when": now.to_rfc3339(),
                    "what": "invalidRequestProofStands",
                    "why": "a proof this store checked against its own headers stands against the invalid word",
                    "height": path.block_height,
                }),
            );
            sqlx::query(
                "UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ?, history = ?, updated_at = ? \
                 WHERE txid = ? AND status = 'invalid'",
            )
            .bind(proven_tx_id)
            .bind(history)
            .bind(now)
            .bind(txid)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE transactions SET proven_tx_id = ?, updated_at = ? \
                 WHERE txid = ? AND status != 'failed' \
                   AND (proven_tx_id IS NULL OR proven_tx_id != ?)",
            )
            .bind(proven_tx_id)
            .bind(now)
            .bind(txid)
            .bind(proven_tx_id)
            .execute(&mut *tx)
            .await?;
            tracing::warn!(
                txid = %txid,
                height = path.block_height,
                marker = "invalid_request_proof_stands",
                "review_status: a transaction holds a proof this store checked; its invalid request is completed, the transaction is not failed"
            );
            completed += 1;
            completed_txids.insert(txid.as_str());
        }
        // Every candidate looked at and left `invalid` goes to the back of
        // the order (a demoted one is `unmined` and not touched).
        for (txid, _, _) in &candidates {
            if completed_txids.contains(txid.as_str()) {
                continue;
            }
            sqlx::query(
                "UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ? AND status = 'invalid'",
            )
            .bind(now)
            .bind(txid)
            .execute(&mut *tx)
            .await?;
        }
        commit(tx).await?;
        Ok(completed)
    }
}
