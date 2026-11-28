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
//! row of the txid. It is a proof this store checked, read on the caller's
//! write ([`word_evidence_on`]): a stored competitor (an output row at one
//! of the txid's input outpoints whose `spent_by` names another
//! transaction with a `proven_txs` row whose bytes spend that outpoint)
//! whose proof has its root-check record, or a `spent-proven` check
//! (`locked_input_checks`) on an output row at one of its input outpoints
//! whose recorded spender is another txid (a proof row this store checked
//! whose bytes spend that outpoint, found by the outpoint,
//! `competitor_proof::checked_spenders_on`); the records the take guard
//! reads (`competitor_proof.rs`, `abort_recovery::failed_outgoing_rows`).
//! A `spent-proven` check with no spender on record is evidence too, unless
//! the txid holds a proof this store checked; one whose recorded spender is
//! the txid itself is not.
//!
//! A stored competitor whose proof row has no root-check record is asked of
//! the tracker by every writer of a word BEFORE its write, with no lock
//! held (`StorageSqlx::word_ask`, the lookup the take guard shares): the
//! retire (`retire_undeliverable_tx`'s `retire_rows`), the broadcaster's
//! fatal word (`mark_transaction_rejected`) and the status review's check 1
//! (`review_status_impl`). The root confirmed, the word's write records it
//! and the proof is evidence; refuted, it is none and the row keeps its
//! word. No tracker, or one that errs: it is evidence (a proof nobody could
//! check takes a coin out of the count, never puts one in), and the request
//! records it as a proof this store had NOT checked (the note `what`
//! `endedBesideUncheckedProof`, `why` `trackerNotAnswered`, the
//! `competitor`, the `transactionIds`): no root-check record and no
//! `spent-proven` mark is written on its account. A proof row stored after
//! the ask counts the same way (`why` `storedAfterLookup`). A row ended so
//! completes on its own checked proof, as any `failed` row does
//! (`proof_completion`).

use sqlx::SqliteConnection;

use crate::Result;

/// The outpoints a word's evidence read judges `txid` by: read from a held
/// copy that hashes to the txid; with none, the outpoints of the outputs a
/// row of the txid spends.
pub(crate) async fn word_outpoints_on(
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Vec<(String, u32)>> {
    let outpoints: Vec<(String, u32)> =
        super::abort_recovery::held_transaction_on(&mut *conn, txid)
            .await?
            .map(|tx| super::competitor_proof::inputs_of(&tx))
            .unwrap_or_default();
    if !outpoints.is_empty() {
        return Ok(outpoints);
    }
    let spent: Vec<(String, i64)> = sqlx::query_as(
        "SELECT lower(o.txid), o.vout FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.spent_by \
         WHERE t.txid = ? AND o.txid IS NOT NULL",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    Ok(spent
        .into_iter()
        .filter_map(|(t, v)| Some((t, u32::try_from(v).ok()?)))
        .collect())
}

/// The evidence a word's write finds against a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WordEvidence {
    /// A proof this store checked: a stored competitor with its root-check
    /// record (one the ask confirmed, recorded in this write), or a
    /// `spent-proven` check that is evidence against it.
    Checked,
    /// A stored competitor proof this store had NOT checked: no tracker
    /// answered the ask (`why` `trackerNotAnswered`), or it was stored after
    /// the ask (`storedAfterLookup`). Evidence still (a proof nobody could
    /// check takes a coin out of the count, never puts one in), recorded
    /// as such on the request, never as a checked proof.
    Unchecked {
        competitor: String,
        why: &'static str,
    },
}

/// The evidence against `txid` on the caller's write (see the module),
/// `ask` the word's ask made before the write: a stored competitor whose
/// bytes spend one of its input outpoints, judged by its record or, with
/// none, by the ask ([`super::competitor_proof::WordAsk`]: confirmed, its
/// root recorded here; refuted, none; unanswered or not asked, unchecked
/// evidence); or a `spent-proven` check at one of its input outpoints that
/// is evidence against it ([`spent_proven_against_on`]).
pub(crate) async fn word_evidence_on(
    conn: &mut SqliteConnection,
    txid: &str,
    ask: &super::competitor_proof::WordAsk,
) -> Result<Option<WordEvidence>> {
    use super::competitor_proof::WordRoot;
    let outpoints = word_outpoints_on(&mut *conn, txid).await?;
    let competitors =
        super::competitor_proof::spending_competitors_on(&mut *conn, txid, &outpoints).await?;
    let mut unchecked = None;
    for (competitor, _, _, recorded) in &competitors {
        if *recorded {
            return Ok(Some(WordEvidence::Checked));
        }
        match ask.answer(txid, competitor) {
            Some(WordRoot::Confirmed { height, root }) => {
                // The root the ask confirmed, recorded in the word's write.
                super::proof_root_checks::record_root_checked_on(conn, competitor, *height, root)
                    .await?;
                return Ok(Some(WordEvidence::Checked));
            }
            Some(WordRoot::Refuted) => {}
            Some(WordRoot::Unanswered) => {
                unchecked.get_or_insert(WordEvidence::Unchecked {
                    competitor: competitor.clone(),
                    why: "trackerNotAnswered",
                });
            }
            None => {
                unchecked.get_or_insert(WordEvidence::Unchecked {
                    competitor: competitor.clone(),
                    why: "storedAfterLookup",
                });
            }
        }
    }
    for outpoint in &outpoints {
        if spent_proven_against_on(&mut *conn, txid, outpoint, None).await? {
            return Ok(Some(WordEvidence::Checked));
        }
    }
    Ok(unchecked)
}

/// The txids of `candidates` (`transaction_id`, `txid`, `is_outgoing`)
/// with a received row: the txids a word's ask reads before its write.
pub(crate) fn received_txids(candidates: &[(i64, String, bool)]) -> Vec<String> {
    let mut txids: Vec<String> = Vec::new();
    for (_, txid, is_outgoing) in candidates {
        if !*is_outgoing && !txids.contains(txid) {
            txids.push(txid.clone());
        }
    }
    txids
}

/// Is there a `spent-proven` check at `outpoint` (on an output row there,
/// `spent_by` narrowed to `spent_by_row` or nothing when given) that is
/// evidence against `txid`? It is, unless the recorded spender is `txid`
/// itself, or no spender is on record and `txid` holds a proof this store
/// checked (then the mark is its own spend).
pub(crate) async fn spent_proven_against_on(
    conn: &mut SqliteConnection,
    txid: &str,
    outpoint: &(String, u32),
    spent_by_row: Option<i64>,
) -> Result<bool> {
    let (source, vout) = outpoint;
    let proven: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs o \
         JOIN locked_input_checks c ON c.output_id = o.output_id \
         WHERE lower(o.txid) = lower(?1) AND o.vout = ?2 AND c.last_verdict = ?3 \
           AND (?4 IS NULL OR o.spent_by IS NULL OR o.spent_by = ?4)",
    )
    .bind(source)
    .bind(i64::from(*vout))
    .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
    .bind(spent_by_row)
    .fetch_one(&mut *conn)
    .await?;
    if proven == 0 {
        return Ok(false);
    }
    let spenders = super::competitor_proof::checked_spenders_on(conn, outpoint).await?;
    if spenders.iter().any(|s| !s.eq_ignore_ascii_case(txid)) {
        return Ok(true);
    }
    if !spenders.is_empty() {
        // The recorded spender is the txid itself: the mark is its own spend.
        return Ok(false);
    }
    // No spender on record (a mark the locked-input re-check wrote): it is
    // evidence unless the txid holds a proof this store checked.
    let sql = format!(
        "SELECT {}",
        super::held_receipts::checked_proof_condition("?1")
    );
    let checked: bool = sqlx::query_scalar(&sql)
        .bind(txid)
        .fetch_one(&mut *conn)
        .await?;
    Ok(!checked)
}

/// The rows a request's word ends, of `candidates` (`transaction_id`,
/// `txid`, `is_outgoing`): every outgoing row, and a received row only
/// where the store holds evidence against its txid ([`word_evidence_on`],
/// `ask` the word's ask made before the write). Read on the caller's
/// write; one evidence read per txid that has a received candidate. A
/// received row ended beside a proof this store had not checked is noted
/// on the request (`what` `endedBesideUncheckedProof`, `why`, the
/// `competitor`'s txid, the `transactionIds` ended).
pub(crate) async fn rows_a_word_ends_on(
    conn: &mut SqliteConnection,
    candidates: &[(i64, String, bool)],
    ask: &super::competitor_proof::WordAsk,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<i64>> {
    let mut evidence: Vec<(String, Option<WordEvidence>, Vec<i64>)> = Vec::new();
    let mut ended = Vec::with_capacity(candidates.len());
    for (transaction_id, txid, is_outgoing) in candidates {
        if *is_outgoing {
            ended.push(*transaction_id);
            continue;
        }
        let at = if let Some(at) = evidence.iter().position(|(t, ..)| t == txid) {
            at
        } else {
            let read = word_evidence_on(&mut *conn, txid, ask).await?;
            evidence.push((txid.clone(), read, Vec::new()));
            evidence.len() - 1
        };
        if evidence[at].1.is_some() {
            ended.push(*transaction_id);
            evidence[at].2.push(*transaction_id);
        } else {
            tracing::info!(
                txid = %txid,
                transaction_id = *transaction_id,
                marker = "received_row_kept_beside_word",
                "a request's word beside a received row with no evidence against its transaction: the row keeps its word"
            );
        }
    }
    for (txid, read, rows) in &evidence {
        let Some(WordEvidence::Unchecked { competitor, why }) = read else {
            continue;
        };
        super::abort_recovery::note_request_on(
            conn,
            txid,
            serde_json::json!({
                "what": ENDED_BESIDE_UNCHECKED_PROOF,
                "why": why,
                "competitor": competitor,
                "transactionIds": rows,
                "when": now.to_rfc3339(),
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            competitor = %competitor,
            why,
            rows = ?rows,
            marker = "received_row_ended_beside_unchecked_proof",
            "a request's word beside a stored competitor proof this store had not checked: the received rows end, recorded as unchecked"
        );
    }
    Ok(ended)
}

/// The note's `what` for a received row a word ended beside a stored
/// competitor proof this store had not checked.
pub(crate) const ENDED_BESIDE_UNCHECKED_PROOF: &str = "endedBesideUncheckedProof";
