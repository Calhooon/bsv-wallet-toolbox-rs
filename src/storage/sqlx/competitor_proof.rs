//! A competitor's checked proof stands against any take (the keeper's
//! ruling of 2026-11-18 on the tracker charter's section 12 item 3 and
//! reading 11's second case, DECISIONS section 30 reading 15).
//!
//! THE LOOKUP ([`StorageSqlx::competitor_check`]). A competitor's checked
//! proof for `txid` is a transaction other than `txid` that the store
//! records as spending an outpoint `txid` spends (an `outputs` row at that
//! outpoint whose `spent_by` names it), whose own stored bytes
//! (`proven_txs.raw_tx`) spend that outpoint, and whose stored merkle path
//! (`proven_txs.merkle_path`) computes a root that our headers carry at its
//! height NOW: the chain tracker is asked at each call. Only a proof this
//! store checked is read at all: a `proven_txs` row with its root-check
//! record (`held_receipts::CHECKED_PROOF_JOIN`); a row merely stored (a
//! sync's merge) is no competitor's proof. A tracker that cannot answer
//! (none, or an error) leaves the check undecided: the competitor neither
//! stands nor is gone, and a take beside it waits (`takeNotCounted`, why
//! `trackerNotAnswered`) until a later take finds the tracker answering.
//! The proof path's competitor ask (`competitors.rs`) keeps the
//! competitor's bytes and checked proof in the write that retires ours, so
//! a `spent-proven` mark it writes has its spender on record: a proof row
//! this store checked whose bytes spend the outpoint, found by the outpoint
//! ([`checked_spenders_on`]).
//!
//! Its cost: one indexed read per input outpoint, and one header read per
//! competitor found (none when none is held). It is asked before any
//! writer is taken: the door asks before its post and before its rows, and
//! each writer of a take asks before it opens the write.
//!
//! THE ONE WRITE THAT APPLIES A TAKE ([`apply_take_on`]). Every writer of a
//! take reaches it: the internalize door, the send-waiting task, the
//! retire path's status source (`promote_if_alive`), the unfail task and
//! the wallet's own post (`process_action`). The rule: the request to
//! `unmined` from every word but `completed`; every row of the txid that is
//! neither `failed` nor `completed` to `unproven` (the unfail task alone
//! lifts `failed`, its recovery); each take recorded; never while a
//! competitor's checked proof stands, or is on record as a verdict against
//! a `failed` row of the txid (`abort_recovery::FailedState::Evidence`). A
//! take heard while one stands raises nothing: no word moves, no row is
//! lifted and no take is recorded, the note alone (`takeNotCounted`) goes
//! on the request's history. Inside the write the lookup's indexed read is run
//! again, so a competitor stored after the header read refuses the take
//! too; a root a reorg has taken away stands no more, and a take heard
//! after that counts.

use sqlx::SqliteConnection;

use super::StorageSqlx;
use crate::storage::traits::WalletStorageReader;
use crate::Result;

/// A competitor's checked proof standing against a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StandingCompetitor {
    /// The competitor's txid.
    pub(crate) txid: String,
    /// The height its stored proof names.
    pub(crate) height: u32,
}

/// The lookup's answer: the competitor that stands, if any, and every
/// stored competitor it read, which the write reads again.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CompetitorCheck {
    txid: String,
    outpoints: Vec<(String, u32)>,
    read: Vec<String>,
    /// The competitor whose checked proof stands, if any.
    pub(crate) standing: Option<StandingCompetitor>,
    /// A stored checked competitor whose root no tracker answered for.
    pub(crate) undecided: Option<String>,
}

/// What the tracker answers of a stored proof's root now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootAnswer {
    /// Our headers carry it.
    Stands,
    /// The path names nothing our headers carry (or does not parse).
    Gone,
    /// No tracker, or the tracker erred.
    Unanswered,
}

/// A stored competitor: its txid, proof height, merkle path and bytes.
type StoredCompetitor = (String, i64, Vec<u8>, Vec<u8>);

/// The outpoints `raw_tx` spends (source txid lowercase, vout), when the
/// bytes hash to `txid` (`abort_recovery::transaction_of`); else none.
fn outpoints_of(txid: &str, raw_tx: &[u8]) -> Vec<(String, u32)> {
    super::abort_recovery::transaction_of(txid, raw_tx)
        .map(|tx| inputs_of(&tx))
        .unwrap_or_default()
}

/// The outpoints `tx` spends (source txid lowercase, vout).
pub(crate) fn inputs_of(tx: &bsv_rs::transaction::Transaction) -> Vec<(String, u32)> {
    tx.inputs()
        .iter()
        .filter_map(|i| {
            Some((
                i.get_source_txid().ok()?.to_ascii_lowercase(),
                i.source_output_index,
            ))
        })
        .collect()
}

/// The stored competitors of `txid`: a transaction the store records as
/// spending one of `outpoints` (an output row there whose `spent_by` names
/// it), with a stored proof this store checked (its root-check record).
/// Indexed by the transactions' txid.
async fn stored_competitors(
    conn: &mut SqliteConnection,
    txid: &str,
    outpoints: &[(String, u32)],
) -> Result<Vec<(StoredCompetitor, (String, u32))>> {
    let mut found = Vec::new();
    for (source, vout) in outpoints {
        let rows: Vec<StoredCompetitor> = sqlx::query_as(
            "SELECT DISTINCT s.txid, p.height, p.merkle_path, p.raw_tx \
             FROM transactions t \
             JOIN outputs o ON o.transaction_id = t.transaction_id \
             JOIN transactions s ON s.transaction_id = o.spent_by \
             JOIN proven_txs p ON p.txid = s.txid \
             JOIN proof_root_checks c \
               ON c.txid = p.txid AND c.height = p.height AND c.merkle_root = lower(p.merkle_root) \
             WHERE t.txid = ? AND o.vout = ? AND s.txid <> ?",
        )
        .bind(source)
        .bind(i64::from(*vout))
        .bind(txid)
        .fetch_all(&mut *conn)
        .await?;
        for row in rows {
            if !found
                .iter()
                .any(|((t, ..), _): &(StoredCompetitor, _)| *t == row.0)
            {
                found.push((row, (source.clone(), *vout)));
            }
        }
    }
    Ok(found)
}

/// A stored competitor of `txid` on record, read on the caller's write: the
/// indexed read of [`stored_competitors`], kept only where the competitor's
/// own stored bytes spend the outpoint (`word_scope`'s evidence read).
pub(crate) async fn stored_competitor_on(
    conn: &mut SqliteConnection,
    txid: &str,
    outpoints: &[(String, u32)],
) -> Result<Option<String>> {
    Ok(stored_competitors(conn, txid, outpoints)
        .await?
        .into_iter()
        .find(|((competitor, _, _, raw), outpoint)| spends(competitor, raw, outpoint))
        .map(|((competitor, ..), _)| competitor))
}

/// The 36 bytes an input spending `outpoint` carries: the source txid in
/// its wire order (reversed hex) and the vout, little-endian.
fn outpoint_bytes((source, vout): &(String, u32)) -> Option<Vec<u8>> {
    let mut bytes = hex::decode(source).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    bytes.reverse();
    bytes.extend_from_slice(&vout.to_le_bytes());
    Some(bytes)
}

/// The recorded spenders of `outpoint`: every proof row this store checked
/// (`held_receipts::CHECKED_PROOF_JOIN`) whose own bytes, hashing to its
/// txid, spend the outpoint, found by the outpoint (a byte search of the
/// checked rows' transactions for its 36 bytes, then a parse that confirms
/// an input). Read only where a `spent-proven` check stands at an input of
/// the judged transaction (`word_scope::spent_proven_against_on`).
pub(crate) async fn checked_spenders_on(
    conn: &mut SqliteConnection,
    outpoint: &(String, u32),
) -> Result<Vec<String>> {
    let Some(needle) = outpoint_bytes(outpoint) else {
        return Ok(Vec::new());
    };
    let sql = format!(
        "SELECT p.txid, p.raw_tx FROM {} WHERE instr(p.raw_tx, ?) > 0",
        super::held_receipts::CHECKED_PROOF_JOIN
    );
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(&sql)
        .bind(needle)
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows
        .into_iter()
        .filter(|(spender, raw)| spends(spender, raw, outpoint))
        .map(|(spender, _)| spender)
        .collect())
}

/// Do `competitor`'s stored bytes, hashing to its txid, spend `outpoint`?
/// Bytes that do not hash to it are no competitor's.
fn spends(competitor: &str, competitor_raw: &[u8], outpoint: &(String, u32)) -> bool {
    outpoints_of(competitor, competitor_raw).contains(outpoint)
}

impl CompetitorCheck {
    /// The txid the check was asked for.
    pub(crate) fn txid(&self) -> &str {
        &self.txid
    }
}

impl StorageSqlx {
    /// Does a competitor's checked proof stand against `txid`? `raw_tx` is
    /// the transaction's bytes when the caller holds them (the door, before
    /// its rows); `None` reads the store's own copy (the request's or a
    /// row's), so a caller with only a txid can ask. See the module.
    pub(crate) async fn competitor_check(
        &self,
        txid: &str,
        raw_tx: Option<&[u8]>,
    ) -> Result<CompetitorCheck> {
        let mut conn = self.pool().acquire().await?;
        let outpoints = match raw_tx {
            Some(raw) => outpoints_of(txid, raw),
            None => super::abort_recovery::held_transaction_on(&mut conn, txid)
                .await?
                .map(|tx| inputs_of(&tx))
                .unwrap_or_default(),
        };
        let found = stored_competitors(&mut conn, txid, &outpoints).await?;
        drop(conn);
        let mut check = CompetitorCheck {
            txid: txid.to_string(),
            outpoints,
            read: found.iter().map(|((t, ..), _)| t.clone()).collect(),
            standing: None,
            undecided: None,
        };
        for ((competitor, height, path, raw), outpoint) in found {
            let Ok(height) = u32::try_from(height) else {
                continue;
            };
            if !spends(&competitor, &raw, &outpoint) {
                continue;
            }
            match self.root_answer(&competitor, height, &path).await {
                RootAnswer::Stands => {
                    check.standing = Some(StandingCompetitor {
                        txid: competitor,
                        height,
                    });
                    break;
                }
                RootAnswer::Unanswered => {
                    check.undecided.get_or_insert(competitor);
                }
                RootAnswer::Gone => {}
            }
        }
        Ok(check)
    }

    /// Does `path` prove `competitor` at `height` under our headers now? A
    /// path that does not parse or names another height proves nothing; a
    /// tracker that cannot answer (none, or an error) leaves it
    /// unanswered: a stored proof stands only on the tracker's word.
    async fn root_answer(&self, competitor: &str, height: u32, path: &[u8]) -> RootAnswer {
        let Ok(bump) = bsv_rs::transaction::MerklePath::from_binary(path) else {
            return RootAnswer::Gone;
        };
        let Ok(root) = bump.compute_root(Some(competitor)) else {
            return RootAnswer::Gone;
        };
        if bump.block_height != height {
            return RootAnswer::Gone;
        }
        let answer = match self.get_chain_tracker().await {
            Some(tracker) => tracker.is_valid_root_for_height(&root, height).await,
            None => match self.get_services() {
                Ok(services) => match services.get_chain_tracker().await {
                    Ok(tracker) => tracker.is_valid_root_for_height(&root, height).await,
                    Err(_) => return RootAnswer::Unanswered,
                },
                Err(_) => return RootAnswer::Unanswered,
            },
        };
        match answer {
            Ok(true) => RootAnswer::Stands,
            Ok(false) => RootAnswer::Gone,
            Err(_) => RootAnswer::Unanswered,
        }
    }
}

/// Which rows of the txid a take lifts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TakeRows {
    /// Every row neither `failed` nor `completed` (every writer but
    /// unfail), and a `failed` outgoing row released on no evidence
    /// (`abort_recovery`).
    Held,
    /// Every row but `completed` and but a `failed` outgoing row with
    /// evidence on record against it: the unfail task's recovery of a
    /// transaction failed on a false word, its free inputs marked spent
    /// again in the same write (`abort_recovery::remark_inputs_spent_on`).
    LiftFailed,
}

/// What [`apply_take_on`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TakeApplied {
    /// The take moved the words and was recorded.
    Applied,
    /// A competitor's checked proof stands, or one is on record as a
    /// verdict against a `failed` row of the txid: nothing raised.
    /// `competitor` names it where its bytes are stored.
    Refused { competitor: Option<String> },
    /// A `failed` outgoing row of the txid is to be read by its inputs and
    /// no held copy of the transaction hashes to its txid, or a stored
    /// competitor's root is unanswered: nothing raised, the cause on the
    /// request's history, asked again by a later take.
    Withheld,
}

impl TakeApplied {
    /// Whether the take was applied.
    pub(crate) fn applied(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// THE ONE WRITE THAT APPLIES A TAKE, on the writer's own transaction (see
/// the module): `check` is the lookup's answer obtained before the write,
/// `takers` each `(provider, word)` recorded with the take.
pub(crate) async fn apply_take_on(
    conn: &mut SqliteConnection,
    check: &CompetitorCheck,
    takers: &[(&str, &str)],
    rows: TakeRows,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<TakeApplied> {
    let txid = check.txid.as_str();
    if let Some(refused) = standing_refusal_on(conn, check, takers, now).await? {
        return Ok(refused);
    }
    // THE RULE BY STATE (`abort_recovery`): each `failed` outgoing row of
    // the txid is read in this write. Released on no evidence, a row is
    // recovered here; its inputs still locked, it stays ended with its free
    // inputs marked spent by it again.
    let failed = super::abort_recovery::failed_outgoing_rows(conn, txid, &check.outpoints).await?;
    if let Some(refused) = row_refusal_on(conn, txid, &failed, takers, now).await? {
        return Ok(refused);
    }
    super::abort_recovery::recover_released_on(
        conn,
        txid,
        &failed,
        super::abort_recovery::RecoveryCause::Take(takers),
        now,
    )
    .await?;
    // A row that stays ended with an input still locked (the retire path's
    // `failed`, some of whose inputs the locked-input re-check gave back):
    // every free input is marked spent by it again, so no coin it spends
    // is offered while the network holds the spend.
    if failed
        .iter()
        .any(|r| r.state == super::abort_recovery::FailedState::Locked)
    {
        super::abort_recovery::remark_inputs_spent_on(conn, txid, &[], now).await?;
    }
    lift_rows_on(conn, txid, rows, now).await?;
    raise_or_hold_on(conn, txid, takers, now).await?;
    for (provider, word) in takers {
        super::held_receipts::record_take_on(conn, txid, provider, word).await?;
    }
    Ok(TakeApplied::Applied)
}

/// A competitor's checked proof standing (the lookup's): the take refused.
/// One the lookup could not decide (no tracker answered), or one stored
/// after it (the indexed read run again in this write, whose root no
/// tracker was asked for): the take waits, written nothing but a note, and
/// a later take reads its root.
async fn standing_refusal_on(
    conn: &mut SqliteConnection,
    check: &CompetitorCheck,
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<TakeApplied>> {
    let txid = check.txid.as_str();
    let late = stored_competitors(&mut *conn, txid, &check.outpoints)
        .await?
        .into_iter()
        .map(|((t, ..), _)| t)
        .find(|t| !check.read.contains(t));
    let waiting = match (&check.undecided, late) {
        (Some(undecided), _) => Some((undecided.clone(), "trackerNotAnswered")),
        (None, Some(late)) => Some((late, "storedAfterLookup")),
        (None, None) => None,
    };
    let Some(competitor) = check.standing.as_ref().map(|s| s.txid.clone()) else {
        let Some((competitor, why)) = waiting else {
            return Ok(None);
        };
        note_take_waits(conn, txid, &competitor, why, takers, now).await?;
        tracing::warn!(
            txid = %txid,
            competitor = %competitor,
            why,
            marker = "take_waits_competitor_root",
            "a take beside a stored competitor proof whose root no tracker answered for: nothing raised, asked again"
        );
        return Ok(Some(TakeApplied::Withheld));
    };
    note_take_refused(conn, txid, Some(&competitor), &[], takers, now).await?;
    tracing::warn!(
        txid = %txid,
        competitor = %competitor,
        marker = "take_refused_competitor_proof",
        "a take heard while a competitor's checked proof stands: nothing raised"
    );
    Ok(Some(TakeApplied::Refused {
        competitor: Some(competitor),
    }))
}

/// The take refused by the txid's `failed` outgoing rows: a competitor's
/// checked proof on record against one of them as a verdict refuses it as
/// a standing one does; a row to read by its inputs with no held copy that
/// hashes to the txid withholds it (nothing of the take is written).
async fn row_refusal_on(
    conn: &mut SqliteConnection,
    txid: &str,
    failed: &[super::abort_recovery::FailedRow],
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<TakeApplied>> {
    let evidence: Vec<i64> = failed
        .iter()
        .filter(|r| r.state == super::abort_recovery::FailedState::Evidence)
        .map(|r| r.transaction_id)
        .collect();
    if !evidence.is_empty() {
        note_take_refused(conn, txid, None, &evidence, takers, now).await?;
        tracing::warn!(
            txid = %txid,
            rows = ?evidence,
            marker = "take_refused_evidence_on_record",
            "a take heard while a competitor's checked proof is on record: nothing raised"
        );
        return Ok(Some(TakeApplied::Refused { competitor: None }));
    }
    if failed.is_empty()
        || super::abort_recovery::held_transaction_on(conn, txid)
            .await?
            .is_some()
    {
        return Ok(None);
    }
    super::abort_recovery::note_request_on(
        conn,
        txid,
        serde_json::json!({
            "what": "takeNotRecovered",
            "when": now.to_rfc3339(),
            "why": "noHeldBytesHashToTxid",
            "transactionIds": failed.iter().map(|r| r.transaction_id).collect::<Vec<_>>(),
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    )
    .await?;
    Ok(Some(TakeApplied::Withheld))
}

/// The rows of `txid` a take lifts (`rows`; a take with evidence on record
/// was refused before); unfail's lift marks the inputs in the same write.
async fn lift_rows_on(
    conn: &mut SqliteConnection,
    txid: &str,
    rows: TakeRows,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    // Unfail's lift of a `failed` row marks its inputs from bytes that hash
    // to the txid; without them no `failed` row is lifted.
    let rows = if rows == TakeRows::LiftFailed
        && super::abort_recovery::held_transaction_on(conn, txid)
            .await?
            .is_none()
    {
        TakeRows::Held
    } else {
        rows
    };
    let rows_sql = match rows {
        TakeRows::Held => {
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status NOT IN ('failed', 'completed')"
        }
        TakeRows::LiftFailed => {
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status <> 'completed'"
        }
    };
    sqlx::query(rows_sql)
        .bind(now)
        .bind(txid)
        .execute(&mut *conn)
        .await?;
    if rows == TakeRows::LiftFailed {
        // The one recovery of a failed transaction's inputs, in this write.
        super::abort_recovery::remark_inputs_spent_on(conn, txid, &[], now).await?;
    }
    Ok(())
}

/// The request is raised unless a `failed` outgoing row of the txid stands
/// unrecovered (its inputs locked): then it keeps its word, raised only for
/// a live row of another user, and the take is a hint on its history.
async fn raise_or_hold_on(
    conn: &mut SqliteConnection,
    txid: &str,
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let (stands, live): (i64, i64) = sqlx::query_as(
        "SELECT \
           (SELECT COUNT(*) FROM transactions WHERE txid = ?1 AND status = 'failed' AND is_outgoing = 1), \
           (SELECT COUNT(*) FROM transactions WHERE txid = ?1 AND status NOT IN ('failed', 'completed'))",
    )
    .bind(txid)
    .fetch_one(&mut *conn)
    .await?;
    if stands == 0 || live > 0 {
        sqlx::query(
            "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ? AND status <> 'completed'",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *conn)
        .await?;
        return Ok(());
    }
    super::abort_recovery::note_request_on(
        conn,
        txid,
        serde_json::json!({
            "what": "takeHeldAsHint",
            "when": now.to_rfc3339(),
            "why": "inputsLocked",
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    )
    .await
}

/// The note of a take that waits on a stored competitor whose root no
/// tracker answered for (`why`), on the request's history.
async fn note_take_waits(
    conn: &mut SqliteConnection,
    txid: &str,
    competitor: &str,
    why: &str,
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let note = serde_json::json!({
        "what": "takeNotCounted", "why": why, "competitor": competitor, "when": now.to_rfc3339(),
        "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
    });
    super::abort_recovery::note_request_on(conn, txid, note).await
}

/// The note of a take that raised nothing, on the request's history: the
/// competitor where its bytes are stored, else the `failed` rows with its
/// verdict on record against them.
async fn note_take_refused(
    conn: &mut SqliteConnection,
    txid: &str,
    competitor: Option<&str>,
    evidence: &[i64],
    takers: &[(&str, &str)],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    super::abort_recovery::note_request_on(
        conn,
        txid,
        serde_json::json!({
            "what": "takeNotCounted",
            "when": now.to_rfc3339(),
            "competitor": competitor,
            "evidenceOnRecord": evidence,
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    )
    .await
}
