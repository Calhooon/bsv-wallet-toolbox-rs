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
//! (`check_unchecked_stored_proof_on`, here asked for its verdict alone):
//! confirmed, it is recorded and stands; refuted, it is demoted (`demote_stale_proof_on`: the transaction
//! `unproven` with its outputs counted, the request `unmined`, the proof task
//! asks for its proof again); with no tracker, a tracker fault or a path that
//! does not compute, nothing is written and the same row is a candidate next
//! pass.
//!
//! The order: a pass takes the candidates by the proof row's number,
//! starting after the one the last pass looked at last and coming round,
//! and records where it stopped in `monitor_state`
//! (`invalid_beside_proof_cursor`). A candidate that never leaves the set
//! (a path that does not parse, no tracker, a tracker fault) holds no slot
//! for good: with `INVALID_BESIDE_PROOF_PER_PASS` = 100, a candidate behind
//! N others in that order is looked at within ceil((N + 1) / 100) passes,
//! whatever the others do, counting those put ahead of it meanwhile (a
//! request that turns `invalid` beside a proof row this store holds, or a
//! proof row stored beside an `invalid` request once the cursor has come
//! round past its number); no clock is read for the order. A pass reads at
//! most 100 headers. Nothing
//! of a request it leaves `invalid` is written (no status, link, history or
//! `updated_at`), so the purge's age, the sync's since-mark and the unfail
//! task's hour read the request as they would without this pass.
//!
//! A txid with a `failed` transaction row (one user's row of it failed,
//! another's live) is not a candidate: nothing of it is written here, no
//! status, link, history or `updated_at`, and no header is read for it. The
//! unfail task takes its request once that has stood an hour, and recovers
//! the `failed` row when the network holds the transaction (the request
//! `unmined`, the row `unproven`, its outputs checked as unspent counted);
//! the proof task then completes every row of the txid. Check 1 fails no
//! row of it meanwhile, since a proof row of the txid exists.
//!
//! The tracker is asked with neither the writer nor a write transaction
//! held, and nothing is written while it answers: the candidates are read on
//! a reader and checked, then ONE write per pass reads each again and acts
//! only on a transaction still as the pass read it: no transaction of its
//! txid `failed`, its request still `invalid`, the same proof row with the
//! same path. That holds for every arm: the record that a root was checked
//! and the completion (the root then recorded as checked), and the
//! demotion, which writes the request (`unmined`, attempts 0, its link
//! cleared, its bytes kept, or a new `unmined` request with the bytes if
//! there is none), every transaction row linked to the proof or `completed`
//! with no link (`unproven`, the link cleared, its bytes kept; its outputs
//! are not written, and count as an `unproven` row's do), the mined memory of the
//! txid, the proof row and its root record (both deleted), and only while
//! the row is still unchecked. A row that changed meanwhile is left to the
//! next pass, as it stands then: a transaction another path failed is no
//! longer a candidate, so neither its status, its request nor its proof row
//! is written, and check 3 does not write a failed transaction `completed`
//! beside outputs left unspendable.
//!
//! The same pass takes check 3's candidates: a request that reads
//! `completed` linking a proof row of its txid that this store has not
//! checked, beside a transaction of that txid that is not `completed`. Both
//! kinds share the one order by proof row number, the one cursor and the one
//! bound of 100 a pass (a candidate of either kind behind N of both is
//! looked at within ceil((N + 1) / 100) passes, neither kind ahead of the
//! other). The write re-reads such a row (the request still `completed`
//! linking the same proof row, the same path bytes, a transaction of the
//! txid still not `completed`): confirmed, the root is recorded and every
//! transaction row of the txid written `completed` and linked, as check 3
//! writes it; refuted while still unchecked, a `failed` row's link to the
//! refuted row is cleared (it stays `failed`) and the proof is demoted as
//! above; no tracker or no answer writes nothing.

use bsv_rs::transaction::MerklePath;

use super::monitor_state::{
    read_invalid_beside_proof_cursor_on, write_invalid_beside_proof_cursor_on,
};
use super::proof_root_checks::{
    check_unchecked_stored_proof_on, record_root_checked_on, stored_proof_checked_on, ReadCheck,
};
use super::storage_sqlx::{append_req_history_note, commit, demote_stale_proof_on, StorageSqlx};
use crate::Result;

/// At most this many candidates are checked per review pass, so at most
/// this many header reads per pass (a candidate already recorded as checked
/// costs none).
pub(super) const INVALID_BESIDE_PROOF_PER_PASS: i64 = 100;

impl StorageSqlx {
    /// The pass over the proof rows beside an `invalid` request (check 1)
    /// and beside a `completed` request with a transaction of its txid not
    /// `completed` (check 3's unchecked proofs): one cursor, one bound, one
    /// write. Returns what it wrote.
    pub(super) async fn pass_over_proven_beside_invalid(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<PassCounts> {
        let cursor = {
            let mut conn = self.pool().acquire().await?;
            read_invalid_beside_proof_cursor_on(&mut conn).await?
        };
        let candidates: Vec<(String, i64, Vec<u8>, String)> = sqlx::query_as(
            r#"
            SELECT DISTINCT r.txid, p.proven_tx_id, p.merkle_path, r.status
            FROM proven_tx_reqs r
            JOIN proven_txs p ON p.txid = r.txid
            WHERE (r.status = 'invalid'
                   AND EXISTS (SELECT 1 FROM transactions t WHERE t.txid = r.txid)
                   AND NOT EXISTS (SELECT 1 FROM transactions f
                                   WHERE f.txid = r.txid AND f.status = 'failed'))
               OR (r.status = 'completed'
                   AND r.proven_tx_id = p.proven_tx_id
                   AND EXISTS (SELECT 1 FROM transactions t
                               WHERE t.txid = r.txid AND t.status != 'completed'))
            ORDER BY p.proven_tx_id <= ?, p.proven_tx_id
            LIMIT ?
            "#,
        )
        .bind(cursor)
        .bind(INVALID_BESIDE_PROOF_PER_PASS)
        .fetch_all(self.pool())
        .await?;
        if candidates.is_empty() {
            return Ok(PassCounts::default());
        }

        // The tracker is asked with no lock held and nothing is written
        // here: each verdict is acted on inside the one write below, after
        // the row is read again.
        let tracker = self.get_chain_tracker().await;
        let mut verdicts = Vec::new();
        {
            let mut conn = self.pool().acquire().await?;
            for (txid, proven_tx_id, merkle_path, kind) in &candidates {
                let Ok(path) = MerklePath::from_binary(merkle_path) else {
                    tracing::debug!(
                        txid = %txid,
                        "review_status: a proof row beside an invalid or completed request does not parse; left as it is"
                    );
                    continue;
                };
                let check = check_unchecked_stored_proof_on(
                    &mut conn,
                    None,
                    tracker.as_deref(),
                    txid,
                    &path,
                )
                .await?;
                if check == ReadCheck::Undecided {
                    tracing::debug!(
                        txid = %txid,
                        "review_status: a proof row beside an invalid or completed request is not checked here; nothing written"
                    );
                    continue;
                }
                verdicts.push((
                    txid.clone(),
                    *proven_tx_id,
                    merkle_path.clone(),
                    kind == "completed",
                    check,
                ));
            }
        }
        let mut tx = self.write_tx().await?;
        let mut counts = PassCounts::default();
        for (txid, proven_tx_id, read_path, beside_completed, check) in &verdicts {
            if *beside_completed {
                counts.transactions_completed += complete_beside_checked_proof(
                    &mut tx,
                    txid,
                    *proven_tx_id,
                    read_path,
                    *check,
                    now,
                )
                .await?;
                continue;
            }
            // The re-read: the request still `invalid`, the same proof row
            // with the same path, and no transaction of the txid `failed`.
            // A row that changed meanwhile is left for the next pass.
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
            if &merkle_path != read_path {
                continue;
            }
            let Ok(path) = MerklePath::from_binary(&merkle_path) else {
                continue;
            };
            let checked = stored_proof_checked_on(&mut tx, txid, &path).await?;
            match check {
                ReadCheck::Demoted => {
                    // Refuted: demoted only while still unchecked (a
                    // record written meanwhile is the reorg task's).
                    if !checked {
                        demote_stale_proof_on(&mut tx, txid).await?;
                        tracing::warn!(
                            txid = %txid,
                            height = path.block_height,
                            marker = "unchecked_proof_demoted",
                            "review_status: a proof row beside an invalid request was never checked and the chain tracker refutes it; demoted, the transaction is unproven again"
                        );
                    }
                    continue;
                }
                ReadCheck::Confirmed if !checked => {
                    let Ok(root) = path.compute_root(Some(txid)) else {
                        continue;
                    };
                    record_root_checked_on(&mut tx, txid, path.block_height, &root).await?;
                    tracing::info!(
                        txid = %txid,
                        height = path.block_height,
                        marker = "unchecked_proof_confirmed",
                        "review_status: a proof row beside an invalid request that was never checked is confirmed by the chain tracker"
                    );
                }
                ReadCheck::AlreadyChecked if !checked => continue,
                _ => {}
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
            counts.invalid_stood += 1;
        }
        // The next pass starts after the last proof row this one looked at
        // (and comes round): no request's own `updated_at` keeps the order.
        if let Some((_, last, _, _)) = candidates.last() {
            write_invalid_beside_proof_cursor_on(&mut tx, *last).await?;
        }
        commit(tx).await?;
        Ok(counts)
    }
}

/// What one pass wrote.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PassCounts {
    /// `invalid` requests brought to `completed` beside a checked proof.
    pub invalid_stood: usize,
    /// Transaction rows brought to `completed` beside a `completed` request
    /// whose proof this pass checked.
    pub transactions_completed: usize,
}

/// Check 3's arm of the pass, inside the pass's one write: a `completed`
/// request linking a proof row this store had not checked, beside a
/// transaction of its txid that is not `completed`. The row is read again
/// (the request still `completed` linking the same proof row, the same path
/// bytes, a transaction of the txid still not `completed`) before anything
/// is written. Confirmed (or recorded meanwhile): the record, then the
/// status and the link check 3 writes. Refuted while still unchecked: a
/// `failed` row's link to the refuted row is cleared (it stays `failed`),
/// then the proof is demoted as every refuted unchecked proof is
/// (`demote_stale_proof_on`). Returns how many transaction rows it brought
/// to `completed`.
async fn complete_beside_checked_proof(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    proven_tx_id: i64,
    read_path: &[u8],
    check: ReadCheck,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<usize> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT p.merkle_path FROM proven_tx_reqs r \
         JOIN proven_txs p ON p.proven_tx_id = r.proven_tx_id AND p.txid = r.txid \
         WHERE r.txid = ? AND r.status = 'completed' AND p.proven_tx_id = ? \
           AND EXISTS (SELECT 1 FROM transactions t \
                       WHERE t.txid = r.txid AND t.status != 'completed')",
    )
    .bind(txid)
    .bind(proven_tx_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((merkle_path,)) = row else {
        return Ok(0);
    };
    if merkle_path != read_path {
        return Ok(0);
    }
    let Ok(path) = MerklePath::from_binary(&merkle_path) else {
        return Ok(0);
    };
    let checked = stored_proof_checked_on(&mut *tx, txid, &path).await?;
    match check {
        ReadCheck::Demoted => {
            if !checked {
                sqlx::query(
                    "UPDATE transactions SET proven_tx_id = NULL \
                     WHERE txid = ? AND status = 'failed' AND proven_tx_id = ?",
                )
                .bind(txid)
                .bind(proven_tx_id)
                .execute(&mut *tx)
                .await?;
                demote_stale_proof_on(&mut *tx, txid).await?;
                tracing::warn!(
                    txid = %txid,
                    height = path.block_height,
                    marker = "unchecked_proof_demoted",
                    "review_status: a proof row beside a completed request was never checked and the chain tracker refutes it; demoted, asked again"
                );
            }
            return Ok(0);
        }
        ReadCheck::Confirmed if !checked => {
            let Ok(root) = path.compute_root(Some(txid)) else {
                return Ok(0);
            };
            record_root_checked_on(&mut *tx, txid, path.block_height, &root).await?;
            tracing::info!(
                txid = %txid,
                height = path.block_height,
                marker = "unchecked_proof_confirmed",
                "review_status: a proof row beside a completed request that was never checked is confirmed by the chain tracker"
            );
        }
        ReadCheck::AlreadyChecked if !checked => return Ok(0),
        ReadCheck::Undecided => return Ok(0),
        _ => {}
    }
    complete_transactions_of_on(&mut *tx, txid, proven_tx_id, now).await
}

/// Check 3's write, as this line has always made it: every transaction row
/// of `txid` `completed` and linked to `proven_tx_id`. Returns how many of
/// them were not `completed` before.
pub(super) async fn complete_transactions_of_on(
    tx: &mut sqlx::SqliteConnection,
    txid: &str,
    proven_tx_id: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<usize> {
    let open: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE txid = ? AND status != 'completed'",
    )
    .bind(txid)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(proven_tx_id)
    .bind(now)
    .bind(txid)
    .execute(&mut *tx)
    .await?;
    tracing::debug!(
        txid = %txid,
        "review_status: transaction rows of a checked proof synced to completed"
    );
    Ok(open as usize)
}
