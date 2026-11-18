//! THE STATUS REVIEW'S CHECK 3: a request `completed` with a proof linked,
//! beside a row of its txid that is not `completed`, completes that row
//! only on a proof this store has checked (the keeper's readings 19 and 23
//! of section 30: a wallet credits, and offers, only on a proof it checked
//! against the headers it holds).
//!
//! Per txid, the linked `proven_txs` row (of that txid) is read with its
//! path on a reader, and checked the way this line checks an unchecked
//! stored proof everywhere else (`proof_root_checks::
//! check_unchecked_stored_proof_on`, asked with no writer: the chain
//! tracker is asked with no lock and no write transaction held). Then one
//! write, in which every row is read again (the request still `completed`
//! with the same link, the proof row with the same height and path, every
//! transaction row of the txid at the word it was read at); anything moved,
//! nothing is written and the next pass reads it afresh:
//!
//! - CHECKED (a root-check record for the root its path computes, already
//!   or confirmed now): the record (when new) and the completion
//!   (`proof_completion::complete_on_proof`) in that one write;
//! - REFUTED (the tracker says the root is not at that height): the line's
//!   demotion of an unchecked stored proof (`demote_stale_proof_on`): the
//!   request `unmined` with no link and its bytes, the proof row and its
//!   record gone, each row linked to it (and a `completed` row left without
//!   a link) `unproven`; a `failed` row stays `failed`, its inputs and
//!   outputs untouched;
//! - UNDECIDED (no tracker, a tracker fault, a path that does not parse or
//!   compute): nothing written; the same at the next pass.
//!
//! A request whose link names no proof row of its txid is left as it is.
//!
//! THE LOAD: one reader query per pass; per txid in the set, one header
//! read of the tracker for a proof not yet checked (none for a checked
//! one) and one short write. The set is the txids whose request reads
//! `completed` with a link beside a row that is not `completed`; a checked
//! or refuted txid leaves it in its pass, so only an undecided one is asked
//! again at the next pass.

use std::fmt::Write as _;

use bsv_rs::transaction::MerklePath;

use super::super::proof_completion::{complete_on_proof, CheckedProof};
use super::super::proof_root_checks::{
    check_unchecked_stored_proof_on, record_root_checked_on, ReadCheck,
};
use super::{commit, demote_stale_proof_on, Result, StorageSqlx};

/// One txid of check 3, as read before the tracker is asked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    txid: String,
    proven_tx_id: i64,
    height: i64,
    path: Vec<u8>,
    /// Every transaction row of the txid with its word.
    rows: Vec<(i64, String)>,
}

/// What check 3 did over one pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Check3 {
    /// Transaction rows completed on a checked proof.
    pub(super) completed: usize,
    /// Proof rows demoted on the tracker's refutation.
    pub(super) demoted: usize,
    /// Txids left for the next pass (no tracker, a fault, a row moved).
    pub(super) undecided: usize,
}

/// The candidate's rows as they read now, on `conn`.
async fn read_candidate(
    conn: &mut sqlx::SqliteConnection,
    txid: &str,
    proven_tx_id: i64,
) -> Result<Option<Candidate>> {
    let proof: Option<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT p.height, p.merkle_path FROM proven_tx_reqs r \
         JOIN proven_txs p ON p.proven_tx_id = r.proven_tx_id AND p.txid = r.txid \
         WHERE r.txid = ? AND r.status = 'completed' AND r.proven_tx_id = ?",
    )
    .bind(txid)
    .bind(proven_tx_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((height, path)) = proof else {
        return Ok(None);
    };
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT transaction_id, status FROM transactions WHERE txid = ? ORDER BY transaction_id",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    Ok(Some(Candidate {
        txid: txid.to_string(),
        proven_tx_id,
        height,
        path,
        rows,
    }))
}

impl StorageSqlx {
    /// Check 3 of `review_status_impl` (see the module).
    pub(super) async fn complete_checked_proofs(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        log: &mut String,
    ) -> Result<Check3> {
        let set: Vec<(String, i64)> = sqlx::query_as(
            "SELECT r.txid, r.proven_tx_id FROM proven_tx_reqs r \
             WHERE r.status = 'completed' AND r.proven_tx_id IS NOT NULL \
               AND EXISTS (SELECT 1 FROM transactions t \
                           WHERE t.txid = r.txid AND t.status <> 'completed') \
             ORDER BY r.proven_tx_req_id",
        )
        .fetch_all(self.pool())
        .await?;
        let mut done = Check3::default();
        if set.is_empty() {
            return Ok(done);
        }
        let tracker = self.get_chain_tracker().await;
        for (txid, proven_tx_id) in set {
            let mut conn = self.pool().acquire().await?;
            let Some(read) = read_candidate(&mut conn, &txid, proven_tx_id).await? else {
                continue;
            };
            let (verdict, root) = match MerklePath::from_binary(&read.path) {
                Ok(path) => {
                    let verdict = check_unchecked_stored_proof_on(
                        &mut conn,
                        None,
                        tracker.as_deref(),
                        &txid,
                        &path,
                    )
                    .await?;
                    (verdict, path.compute_root(Some(&txid)).ok())
                }
                Err(_) => (ReadCheck::Undecided, None),
            };
            drop(conn);
            let mut tx = self.write_tx().await?;
            if read_candidate(&mut tx, &txid, proven_tx_id).await?.as_ref() != Some(&read) {
                tx.rollback().await?;
                done.undecided += 1;
                continue;
            }
            match (verdict, root) {
                (ReadCheck::AlreadyChecked | ReadCheck::Confirmed, Some(root)) => {
                    let height = u32::try_from(read.height).unwrap_or_default();
                    if verdict == ReadCheck::Confirmed {
                        record_root_checked_on(&mut tx, &txid, height, &root).await?;
                    }
                    let Some(proof) = CheckedProof::read_on(&mut tx, &txid, proven_tx_id).await?
                    else {
                        tx.rollback().await?;
                        done.undecided += 1;
                        continue;
                    };
                    complete_on_proof(&mut tx, &proof, now).await?;
                    commit(tx).await?;
                    done.completed += read.rows.iter().filter(|(_, s)| s != "completed").count();
                }
                (ReadCheck::Demoted, _) => {
                    demote_stale_proof_on(&mut tx, &txid).await?;
                    commit(tx).await?;
                    done.demoted += 1;
                    let _ = writeln!(
                        log,
                        "Demoted the unchecked proof of {txid}: the chain tracker refutes its root"
                    );
                }
                _ => {
                    tx.rollback().await?;
                    done.undecided += 1;
                }
            }
        }
        Ok(done)
    }
}
