//! A CHECKED PROOF COMPLETES EVERY ROW OF ITS TXID IN THE STORE, A
//! `failed` ONE INCLUDED (the keeper's readings 19 and 20 of section 30).
//!
//! The proof is evidence; no word stands against it. Every writer of a
//! proof's completion (the proof path's `ingest_merkle_proof`, the create
//! door's broadcast with a proof in hand, the status review's check 3)
//! writes it through [`complete_on_proof`], on its own write transaction,
//! and only with a [`CheckedProof`]: the one value that names a stored
//! proof whose root this store checked, made by reading the root-check
//! record in that write ([`CheckedProof::read_on`]), so a completion on a
//! proof this store has not checked cannot be written. It writes every row
//! of the txid `completed` with the proof linked, and for each row that
//! read `failed` before the proof:
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
//!
//! The inputs and the losers are read from the store's own copy of the
//! transaction that hashes to the txid (`abort_recovery::
//! held_transaction_on`); with no such copy the completion marks no input
//! and queues no loser, and its note says so (`inputsMarked: false`).

use sqlx::SqliteConnection;

use crate::Result;

/// A stored proof whose root this store checked: the `proven_txs` row
/// `proven_tx_id` of `txid`, whose stored path computes a root that
/// `proof_root_checks` records at its height (the definition of a checked
/// row, `proof_root_checks::stored_proof_checked_on`). Made only by
/// [`CheckedProof::read_on`], on the write that completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckedProof {
    txid: String,
    proven_tx_id: i64,
}

impl CheckedProof {
    /// Read, on the caller's write, whether the proof row `proven_tx_id` of
    /// `txid` is one this store checked. `None`: no such row, a path that
    /// does not parse, or no root-check record for the root its path
    /// computes.
    pub(crate) async fn read_on(
        conn: &mut SqliteConnection,
        txid: &str,
        proven_tx_id: i64,
    ) -> Result<Option<Self>> {
        let path: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT merkle_path FROM proven_txs WHERE proven_tx_id = ? AND txid = ?",
        )
        .bind(proven_tx_id)
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(path) = path.and_then(|p| bsv_rs::transaction::MerklePath::from_binary(&p).ok())
        else {
            return Ok(None);
        };
        let checked =
            super::proof_root_checks::stored_proof_checked_on(&mut *conn, txid, &path).await?;
        Ok(checked.then(|| Self {
            txid: txid.to_string(),
            proven_tx_id,
        }))
    }
}

/// THE COMPLETION OF A CHECKED PROOF, on the caller's write (see the
/// module): every row of the proof's txid `completed` with the proof
/// linked. Returns the rows that read `failed` before it.
pub(crate) async fn complete_on_proof(
    conn: &mut SqliteConnection,
    proof: &CheckedProof,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let txid = proof.txid.as_str();
    let proven_tx_id = proof.proven_tx_id;
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
    // The inputs and the losers are read from the store's own copy of the
    // transaction that hashes to the txid; without one neither is read.
    let marked = super::abort_recovery::remark_inputs_spent_on(conn, txid, &[], now).await?;
    let losers = if marked {
        losers_of(conn, txid).await?
    } else {
        Vec::new()
    };
    for loser in &losers {
        queue_competitor_on(conn, loser, txid).await?;
    }
    for transaction_id in &failed {
        super::abort_recovery::open_own_outputs(conn, *transaction_id, None, now).await?;
    }
    super::abort_recovery::note_request_on(
        conn,
        txid,
        serde_json::json!({
            "what": "provenOverFailed",
            "when": now.to_rfc3339(),
            "transactionIds": failed,
            "inputsMarked": marked,
            "losers": losers,
        }),
    )
    .await?;
    tracing::warn!(
        txid = %txid,
        rows = ?failed,
        losers = ?losers,
        marker = "proven_over_failed",
        "a checked proof completes a failed row: its inputs marked spent by it, its outputs counted"
    );
    Ok(failed)
}

/// Every other transaction the store records as spending one of `txid`'s
/// input outpoints (read from the store's copy that hashes to `txid`), not
/// itself ended: the losers of the proven conflict.
async fn losers_of(conn: &mut SqliteConnection, txid: &str) -> Result<Vec<String>> {
    let Some(tx) = super::abort_recovery::held_transaction_on(conn, txid).await? else {
        return Ok(Vec::new());
    };
    let mut losers: Vec<String> = Vec::new();
    for (source_txid, vout) in super::internalize_action::extract_input_outpoints(&tx) {
        let found: Vec<String> = sqlx::query_scalar(
            "SELECT t.txid FROM outputs o JOIN transactions t ON t.transaction_id = o.spent_by \
             WHERE lower(o.txid) = lower(?) AND o.vout = ? AND t.txid <> ? \
               AND t.status NOT IN ('failed', 'completed')",
        )
        .bind(&source_txid)
        .bind(i64::from(vout))
        .bind(txid)
        .fetch_all(&mut *conn)
        .await?;
        for loser in found {
            if !losers.contains(&loser) {
                losers.push(loser);
            }
        }
    }
    Ok(losers)
}

/// Queue `proven` as a competitor on `loser`'s request (`competitorsQueued`,
/// which the competitor proof ask reads).
async fn queue_competitor_on(conn: &mut SqliteConnection, loser: &str, proven: &str) -> Result<()> {
    sqlx::query(
        "UPDATE proven_tx_reqs SET history = json_set( \
           CASE WHEN json_valid(history) AND json_type(history) = 'object' THEN history ELSE '{}' END, \
           '$.competitorsQueued', \
           json((SELECT json_group_array(value) FROM ( \
             SELECT value FROM json_each(COALESCE(json_extract( \
               CASE WHEN json_valid(history) THEN history ELSE '{}' END, '$.competitorsQueued'), '[]')) \
             UNION SELECT lower(?2))))) \
         WHERE txid = ?1",
    )
    .bind(loser)
    .bind(proven)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
