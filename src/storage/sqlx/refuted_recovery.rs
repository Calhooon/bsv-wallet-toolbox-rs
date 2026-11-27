//! A REFUTED PROOF BESIDE A `failed` ROW OF THE WALLET'S OWN (the keeper's
//! reading 26 of section 30).
//!
//! The demotion of a stored proof the tracker refutes
//! (`storage_sqlx::demote_stale_proof_on`) leaves the request `unmined`: the
//! store's word for "the network may hold it". A request reading `completed`
//! beside a proof is a claim that the transaction was mined, and a root the
//! headers refute does not unsay it (a reorg, or headers not held yet,
//! refute a true proof too). So beside the wallet's own `failed` outgoing
//! row the demotion recovers toward caution, in its own write, by the
//! row's state as a take reads it (`abort_recovery::failed_outgoing_rows`):
//!
//! - RELEASED (no output spent by it, no evidence): recovered by the one
//!   recovery (`abort_recovery::recover_released_on`): the row `unproven`,
//!   each free input marked spent by it again, an input another action
//!   spent meanwhile left with that action and the txid queued as that
//!   action's competitor, its own outputs as that recovery leaves them, the
//!   cause `recoveredOnRefutedProof` on the request's history, the request
//!   `unmined`, asking for its proof.
//! - LOCKED (an input still spent by it, all of them or some): it stays
//!   ended and keeps them; every input of it still free is marked spent by
//!   it again; the request reads `invalid`, the ended row's word, unless a
//!   live row of the txid needs it asking (`unmined`).
//! - EVIDENCE (a competitor's checked proof on record against it): not
//!   recovered; that evidence is this store's own and outranks the word.
//!   The request reads `invalid` and a `refutedProofNotRecovered` note
//!   names the rows.
//!
//! With no held copy of the transaction that hashes to its txid the inputs
//! cannot be read: nothing is recovered, the request reads `invalid` and
//! the note says why.
//!
//! So no demotion leaves a request asking for a proof beside a `failed`
//! row with a free input. A row recovered here ends as any asked
//! transaction of this line does: by its proof, by a competitor's checked
//! proof, or by the host's own act (`retire_undeliverable_txid`). The proof
//! task's attempt count writes no word for any request, so it ends none.

use sqlx::SqliteConnection;

use super::abort_recovery::{
    failed_outgoing_rows, held_transaction_on, note_request_on, recover_released_on,
    remark_inputs_spent_on, FailedState, RecoveryCause,
};
use crate::Result;

/// Not recovered: the cause on the request's history, the request back at
/// the ended row's word.
async fn not_recovered_on(
    conn: &mut SqliteConnection,
    txid: &str,
    why: &str,
    evidence: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    note_request_on(
        conn,
        txid,
        serde_json::json!({
            "what": "refutedProofNotRecovered",
            "when": now.to_rfc3339(),
            "why": why,
            "evidenceOnRecord": evidence,
        }),
    )
    .await?;
    end_request_on(conn, txid, now).await
}

/// The request back at the ended row's word, on the demotion's write.
async fn end_request_on(
    conn: &mut SqliteConnection,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? \
         WHERE txid = ? AND status = 'unmined'",
    )
    .bind(now)
    .bind(txid)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The rule of the module, on the demotion's write, after its own steps
/// (the request `unmined` with no link, the proof row gone).
pub(crate) async fn recover_beside_refuted_proof_on(
    conn: &mut SqliteConnection,
    txid: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let stands: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE txid = ? AND status = 'failed' AND is_outgoing = 1",
    )
    .bind(txid)
    .fetch_one(&mut *conn)
    .await?;
    if stands == 0 {
        return Ok(());
    }
    let Some(tx) = held_transaction_on(conn, txid).await? else {
        return not_recovered_on(conn, txid, "noHeldBytesHashToTxid", &[], now).await;
    };
    let outpoints = super::internalize_action::extract_input_outpoints(&tx);
    let failed = failed_outgoing_rows(conn, txid, &outpoints).await?;
    let evidence: Vec<i64> = failed
        .iter()
        .filter(|r| r.state == FailedState::Evidence)
        .map(|r| r.transaction_id)
        .collect();
    if !evidence.is_empty() {
        return not_recovered_on(conn, txid, "evidenceOnRecord", &evidence, now).await;
    }
    recover_released_on(conn, txid, &failed, RecoveryCause::RefutedProof, now).await?;
    if failed.iter().any(|r| r.state == FailedState::Locked) {
        remark_inputs_spent_on(conn, txid, &[], now).await?;
        let live: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM transactions WHERE txid = ? AND status NOT IN ('failed', 'completed')",
        )
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
        if live == 0 {
            end_request_on(conn, txid, now).await?;
        }
    }
    Ok(())
}
