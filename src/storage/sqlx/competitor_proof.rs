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
//! height NOW: the chain tracker is asked at each call, never the record
//! of an earlier check. A tracker that cannot answer finds no root gone, so
//! the proof stands. The store holds no other competitor proof: the proof
//! path's competitor ask (`competitors.rs`) checks a fetched proof and
//! records its verdict (the request `doubleSpend`, the lost inputs
//! `spent-proven`), never the competitor's bytes or path, and an outpoint
//! that is no output row of this store has no record of who spent it.
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
//! competitor's checked proof stands. A take heard while one stands raises
//! nothing: no word moves and no take is recorded, the note alone goes on
//! the request's history. Inside the write the lookup's indexed read is run
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
}

/// A stored competitor: its txid, proof height, merkle path and bytes.
type StoredCompetitor = (String, i64, Vec<u8>, Vec<u8>);

/// The outpoints `raw_tx` spends (source txid lowercase, vout).
fn outpoints_of(raw_tx: &[u8]) -> Vec<(String, u32)> {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(raw_tx) else {
        return Vec::new();
    };
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
/// it), with a stored proof. Indexed by the transactions' txid.
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

/// Does `competitor`'s stored bytes spend `outpoint`?
fn spends(competitor_raw: &[u8], outpoint: &(String, u32)) -> bool {
    outpoints_of(competitor_raw).contains(outpoint)
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
        let stored: Option<Vec<u8>> = match raw_tx {
            Some(_) => None,
            None => {
                sqlx::query_scalar(
                    "SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?1 AND raw_tx IS NOT NULL \
                     UNION ALL SELECT raw_tx FROM transactions WHERE txid = ?1 AND raw_tx IS NOT NULL \
                     LIMIT 1",
                )
                .bind(txid)
                .fetch_optional(&mut *conn)
                .await?
            }
        };
        let outpoints = raw_tx.or(stored.as_deref()).map(outpoints_of);
        let outpoints = outpoints.unwrap_or_default();
        let found = stored_competitors(&mut conn, txid, &outpoints).await?;
        drop(conn);
        let mut check = CompetitorCheck {
            txid: txid.to_string(),
            outpoints,
            read: found.iter().map(|((t, ..), _)| t.clone()).collect(),
            standing: None,
        };
        for ((competitor, height, path, raw), outpoint) in found {
            let Ok(height) = u32::try_from(height) else {
                continue;
            };
            if spends(&raw, &outpoint) && self.root_stands(&competitor, height, &path).await {
                check.standing = Some(StandingCompetitor {
                    txid: competitor,
                    height,
                });
                break;
            }
        }
        Ok(check)
    }

    /// Does `path` prove `competitor` at `height` under our headers now? A
    /// path that does not parse or names another height proves nothing; a
    /// tracker that cannot answer (none, or an error) finds no root gone.
    async fn root_stands(&self, competitor: &str, height: u32, path: &[u8]) -> bool {
        let Ok(bump) = bsv_rs::transaction::MerklePath::from_binary(path) else {
            return false;
        };
        let Ok(root) = bump.compute_root(Some(competitor)) else {
            return false;
        };
        if bump.block_height != height {
            return false;
        }
        let answer = match self.get_chain_tracker().await {
            Some(tracker) => tracker.is_valid_root_for_height(&root, height).await,
            None => match self.get_services() {
                Ok(services) => match services.get_chain_tracker().await {
                    Ok(tracker) => tracker.is_valid_root_for_height(&root, height).await,
                    Err(_) => return true,
                },
                Err(_) => return true,
            },
        };
        !matches!(answer, Ok(false))
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
    /// A competitor's checked proof stands: nothing raised.
    Refused { competitor: String },
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
    let late = stored_competitors(&mut *conn, txid, &check.outpoints)
        .await?
        .into_iter()
        .map(|((t, ..), _)| t)
        .find(|t| !check.read.contains(t));
    if let Some(competitor) = check.standing.as_ref().map(|s| s.txid.clone()).or(late) {
        note_take_refused(conn, txid, &competitor, takers, now).await?;
        tracing::warn!(
            txid = %txid,
            competitor = %competitor,
            marker = "take_refused_competitor_proof",
            "a take heard while a competitor's checked proof stands: nothing raised"
        );
        return Ok(TakeApplied::Refused { competitor });
    }
    // THE RULE BY STATE (`abort_recovery`): each `failed` outgoing row of
    // the txid is read in this write. Released on no evidence, it is
    // recovered here; its inputs still locked, or evidence on record
    // against it, nothing of it moves.
    let failed = super::abort_recovery::failed_outgoing_rows(conn, txid, &check.outpoints).await?;
    super::abort_recovery::recover_released_on(conn, txid, &failed, takers, now).await?;
    let evidence: Vec<i64> = failed
        .iter()
        .filter(|r| r.state == super::abort_recovery::FailedState::Evidence)
        .map(|r| r.transaction_id)
        .collect();
    lift_rows_on(conn, txid, rows, &evidence, now).await?;
    raise_or_hold_on(conn, txid, !evidence.is_empty(), takers, now).await?;
    for (provider, word) in takers {
        super::held_receipts::record_take_on(conn, txid, provider, word).await?;
    }
    Ok(TakeApplied::Applied)
}

/// The rows of `txid` a take lifts (`rows`), a `failed` outgoing row with
/// evidence on record never among them; unfail's lift marks the inputs in
/// the same write.
async fn lift_rows_on(
    conn: &mut SqliteConnection,
    txid: &str,
    rows: TakeRows,
    evidence: &[i64],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let rows_sql = match rows {
        TakeRows::Held => {
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status NOT IN ('failed', 'completed') \
               AND transaction_id NOT IN (SELECT value FROM json_each(?))"
        }
        TakeRows::LiftFailed => {
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status <> 'completed' \
               AND NOT (status = 'failed' AND is_outgoing = 1 AND transaction_id IN \
                 (SELECT value FROM json_each(?)))"
        }
    };
    sqlx::query(rows_sql)
        .bind(now)
        .bind(txid)
        .bind(serde_json::Value::from(evidence.to_vec()).to_string())
        .execute(&mut *conn)
        .await?;
    if rows == TakeRows::LiftFailed {
        // The one recovery of a failed transaction's inputs, in this write.
        super::abort_recovery::remark_inputs_spent_on(conn, txid, evidence, now).await?;
    }
    Ok(())
}

/// The request is raised unless a `failed` outgoing row of the txid stands
/// unrecovered: then it keeps its word, raised only for a live row of
/// another user (its inputs locked), never beside a row with evidence on
/// record, and the take is a hint on its history.
async fn raise_or_hold_on(
    conn: &mut SqliteConnection,
    txid: &str,
    evidence: bool,
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
    let still_evidence = evidence && stands > 0;
    if stands == 0 || (live > 0 && !still_evidence) {
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
            "why": if still_evidence { "evidenceOnRecord" } else { "inputsLocked" },
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    )
    .await
}

/// The note of a take that raised nothing, on the request's history.
async fn note_take_refused(
    conn: &mut SqliteConnection,
    txid: &str,
    competitor: &str,
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
            "takers": takers.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        }),
    )
    .await
}
