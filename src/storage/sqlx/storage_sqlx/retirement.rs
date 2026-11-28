use super::{
    Result, Row, StorageSqlx, WalletServices, LOCKED_VERDICT_SPENT_PROVEN, LOCKED_VERDICT_UNKNOWN,
};

/// Outcome of `StorageSqlx::retire_undeliverable_tx`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireOutcome {
    /// The status service says the tx is known/mined after all — promoted to
    /// `unmined`/`unproven`, nothing released.
    Alive,
    /// Retired: req → the given terminal status, tx → `failed`, its own
    /// outputs unspendable; `kept` inputs stay locked. `restored` is 0
    /// since 0.11.1: no explorer's unspent set releases an input on this
    /// path (bsv-wallet-toolbox-rs #42 point 5).
    Retired { restored: u32, kept: u32 },
}

/// What a `StorageSqlx::retire_undeliverable_tx` call stands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetireBasis<'a> {
    /// No chain evidence in hand: a host's or a broadcaster's verdict that
    /// the transaction never reached the network. A status source holding
    /// it (known/mined) outranks the verdict: promoted, nothing retired.
    Undelivered,
    /// A named competitor's bytes spend one of our inputs and its merkle
    /// path met our header (`competitor_is_proven`): the proof is the word,
    /// and no status source's `known` outranks it (bsv-wallet-toolbox-rs
    /// #42 point 1).
    CompetitorProven {
        /// The competitor's txid.
        competitor: &'a str,
        /// The outpoints of ours the competitor's bytes spend (`txid`,
        /// `vout`): lost to it, locked for good (`spent-proven`).
        lost: &'a [(String, u32)],
        /// Its bytes and checked proof, kept in the write that retires
        /// ours (a `proven_txs` row and its root-check record).
        proof: &'a super::competitors::CheckedCompetitorProof,
    },
}

/// The operation `retire_undeliverable_txid`'s refusal names.
const RETIRE_OPERATION: &str = "retire transaction";

/// What `StorageSqlx::retire_rows` writes.
struct RetireRows<'a> {
    txid: &'a str,
    proven_tx_req_id: i64,
    attempts: i64,
    req_status: &'a str,
    /// The operation the accepting-word guard names, when the caller retires
    /// only a transaction no instance accepted.
    guard: Option<&'a str>,
    /// The retire stands on evidence about the transaction (a competitor's
    /// checked proof): every row of the txid ends. Otherwise it is a word
    /// and ends the outgoing rows alone, a received row only where the
    /// store holds evidence on record (`word_scope`).
    evidence: bool,
    /// The competitor whose checked proof the retire stands on, with it.
    competitor: Option<(&'a str, &'a super::competitors::CheckedCompetitorProof)>,
    now: chrono::DateTime<chrono::Utc>,
}

/// A retired transaction's input: `output_id`, source txid, `vout`.
type RetireInput = (i64, String, i32);

impl StorageSqlx {
    /// THE RELEASE RULE (2026-08-29) — the ONE path that may give a
    /// broadcast-failed transaction's inputs back to coin selection.
    ///
    /// Background: `send_waiting_transactions` had three arms that, after a
    /// retry budget or a single broadcaster's "invalid", marked the tx
    /// `failed` and BLINDLY ran `UPDATE outputs SET spendable = 1,
    /// spent_by = NULL WHERE spent_by = ?`. A tx one broadcaster rejects can
    /// be alive in another's orphan pool (a parent broadcast through a
    /// different channel, not yet seen here) and mines the moment that
    /// parent lands — the LOW fleet's 2026-08-29 run A: restored inputs
    /// were re-spent, the earlier copy mined, every later spend became
    /// `UTXO_SPENT` and every child an orphan (that incident's live releaser
    /// was a harness sweep with the same single-source verdict; these arms
    /// are the toolbox's own copies of the defect).
    ///
    /// Rule: an input is released ONLY when
    /// 1. the tx is NOT alive per the status service
    ///    (`reconcile_tx_status_via_services`: known/mined ⇒
    ///    [`RetireOutcome::Alive`], promoted, nothing touched), asked only
    ///    for [`RetireBasis::Undelivered`]: on
    ///    [`RetireBasis::CompetitorProven`] the competitor's checked proof
    ///    is the chain's word and no `known` outranks it, AND
    /// 2. a node's verdict releases it. No explorer's word does (an unspent
    ///    set is a hint, and a coin a mined competitor spent reads unspent
    ///    at a lagging explorer: bsv-wallet-toolbox-rs #42 point 5), so this
    ///    path releases nothing: every input stays locked (`spent_by`
    ///    intact, `spendable = 0`). An input the competitor's checked proof
    ///    spends is lost, scheduled `spent-proven` (terminal); every other
    ///    input is scheduled for the locked-input re-check. The descendants'
    ///    outside inputs are kept and scheduled the same way.
    ///
    /// The tx's own outputs (change) are marked unspendable and the tx
    /// `failed` so its phantom change never funds — and orphans — a new tx.
    ///
    /// Order (the writer discipline): the alive check asks the status
    /// service before any lock; the req, the own outputs and the `failed`
    /// status are written in one write transaction (`retire_rows`, with the
    /// accepting-word guard when `guard` names the operation); the inputs'
    /// re-check schedule, the broadcast memory and the descendants follow
    /// its commit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn retire_undeliverable_tx(
        &self,
        services: &dyn WalletServices,
        txid: &str,
        proven_tx_req_id: i64,
        attempts: i64,
        req_status: &str,
        basis: RetireBasis<'_>,
        guard: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<RetireOutcome> {
        match basis {
            RetireBasis::Undelivered => {
                if self
                    .promote_if_alive(services, txid, proven_tx_req_id, now)
                    .await?
                {
                    return Ok(RetireOutcome::Alive);
                }
            }
            RetireBasis::CompetitorProven { competitor, .. } => {
                tracing::info!(
                    txid = %txid,
                    competitor = %competitor,
                    "retire on a competitor's checked proof: no status source is asked (a known never outranks the proof)"
                );
            }
        }

        let lost: &[(String, u32)] = match basis {
            RetireBasis::CompetitorProven { lost, .. } => lost,
            RetireBasis::Undelivered => &[],
        };
        let rows = RetireRows {
            txid,
            proven_tx_req_id,
            attempts,
            req_status,
            guard,
            evidence: matches!(basis, RetireBasis::CompetitorProven { .. }),
            competitor: match basis {
                RetireBasis::CompetitorProven {
                    competitor, proof, ..
                } => Some((competitor, proof)),
                RetireBasis::Undelivered => None,
            },
            now,
        };
        let Some((transaction_id, inputs)) = self.retire_rows(&rows).await? else {
            return Ok(RetireOutcome::Retired {
                restored: 0,
                kept: 0,
            });
        };

        // Committed. Its inputs: none released (a node's verdict releases a
        // coin; an explorer's unspent set does not). Lost to a proven
        // competitor, or kept for the re-check.
        let mut kept = 0u32;
        for (output_id, source_txid, vout) in &inputs {
            kept += 1;
            let is_lost = lost
                .iter()
                .any(|(t, v)| *v as i64 == *vout as i64 && t.eq_ignore_ascii_case(source_txid));
            if is_lost {
                self.schedule_locked_input_check(*output_id, LOCKED_VERDICT_SPENT_PROVEN)
                    .await;
                tracing::warn!(
                    txid = %txid,
                    outpoint = %format!("{}:{}", source_txid, vout),
                    "retire: input spent by the proven competitor: lost, locked for good"
                );
            } else {
                self.schedule_locked_input_check(*output_id, LOCKED_VERDICT_UNKNOWN)
                    .await;
                tracing::info!(
                    txid = %txid,
                    outpoint = %format!("{}:{}", source_txid, vout),
                    "retire: input stays LOCKED (no explorer's word releases it), re-checked later"
                );
            }
        }

        // No provider skips it again, and everything this wallet built on
        // it is a phantom too (the poisoned chain, 2026-09-02).
        self.mark_broadcast_rejected_quiet(txid).await;
        // No services: the descendants' outside inputs are kept and
        // scheduled, never released on an explorer's word.
        let (descendants_restored, descendants_kept) = self
            .retire_poisoned_descendants(None, txid, transaction_id, now)
            .await?;

        Ok(RetireOutcome::Retired {
            restored: descendants_restored,
            kept: kept + descendants_kept,
        })
    }

    /// The retire's own rows in one write transaction (`BEGIN IMMEDIATE`):
    /// the accepting-word guard when `guard` names the operation, the req
    /// at its terminal status, the transaction's own outputs unspendable,
    /// the transaction `failed`. An acceptance committed before this lock is
    /// read under it and refuses; one committed after finds the row
    /// `failed`. Nothing awaits a service in here. Returns the transaction
    /// id and its inputs (`output_id`, source txid, `vout`), or `None` when
    /// no `sending` / `unproven` transaction holds `txid` (the req is still
    /// written then).
    async fn retire_rows(&self, rows: &RetireRows<'_>) -> Result<Option<(i64, Vec<RetireInput>)>> {
        let mut db_tx = self.write_tx().await?;
        if let Some(operation) = rows.guard {
            super::super::abort_action::refuse_if_accepted(&mut db_tx, rows.txid, operation)
                .await?;
        }
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(rows.req_status)
            .bind(rows.attempts)
            .bind(rows.now)
            .bind(rows.proven_tx_req_id)
            .execute(&mut *db_tx)
            .await?;
        // The competitor's checked proof, kept in this write: its proof row
        // and this store's root-check record for it, the records a checked
        // proof is read from, so every mark this retire writes has its
        // spender on record.
        if let Some((competitor, proof)) = rows.competitor {
            super::proven_txs::store_validated_proof_on(
                &mut db_tx,
                &super::proven_txs::ValidatedProofRow {
                    txid: competitor,
                    block_height: proof.height,
                    checked_root: &proof.root,
                    block_hash: "",
                    merkle_root: &proof.root,
                    merkle_path: &proof.merkle_path,
                    idx: proof.idx,
                    raw_tx: Some(&proof.raw_tx),
                },
            )
            .await?;
        }
        let ended = retired_rows_on(&mut db_tx, rows.txid, rows.evidence).await?;
        let [transaction_id, ..] = ended[..] else {
            super::commit(db_tx).await?;
            return Ok(None);
        };

        let inputs = retire_inputs(&mut db_tx, transaction_id).await?;
        // Their own outputs never fund anything again.
        fail_rows_on(&mut db_tx, &ended, "AND spent_by IS NULL", rows.now).await?;
        super::commit(db_tx).await?;
        Ok(Some((transaction_id, inputs)))
    }

    /// If a status source holds `txid` (known or mined), promote it: the
    /// request `unmined`, the transaction `unproven`. Returns whether it did.
    pub(super) async fn promote_if_alive(
        &self,
        services: &dyn WalletServices,
        txid: &str,
        proven_tx_req_id: i64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        if !super::process_action::reconcile_tx_status_via_services(services, txid).await {
            return Ok(false);
        }
        let _ = proven_tx_req_id;
        // The status source's word, through the one write that applies a
        // take: nothing raised while a competitor's checked proof stands.
        self.apply_take(
            txid,
            &[(
                crate::services::broadcast_memory::BROADCAST_PROVIDER_CHAIN,
                crate::services::broadcast_memory::BROADCAST_STATUS_SEEN,
            )],
            now,
        )
        .await
    }

    /// A take observed by the send-waiting task, through the one write that
    /// applies a take (`competitor_proof::apply_take_on`): the request
    /// `unmined` from every word but `completed`, every row of the txid
    /// neither `failed` nor `completed` `unproven`, each `provider` recorded
    /// with `word`. Returns whether it was applied (a competitor's checked
    /// proof standing raises nothing).
    pub(super) async fn promote_taken(
        &self,
        proven_tx_req_id: i64,
        txid: &str,
        providers: &[&str],
        word: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        let _ = proven_tx_req_id;
        let takers: Vec<(&str, &str)> = providers.iter().map(|p| (*p, word)).collect();
        self.apply_take(txid, &takers, now).await
    }

    /// The competitor lookup, then the one write that applies a take.
    async fn apply_take(
        &self,
        txid: &str,
        takers: &[(&str, &str)],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        let check = self.competitor_check(txid, None).await?;
        let mut tx = self.write_tx().await?;
        let applied = super::super::competitor_proof::apply_take_on(
            &mut tx,
            &check,
            takers,
            super::super::competitor_proof::TakeRows::Held,
            now,
        )
        .await?;
        super::commit(tx).await?;
        Ok(applied.applied())
    }

    /// THE RELEASE RULE, addressed by txid — for a caller that learned AFTER the
    /// broadcast that the transaction never reached the network (a serving
    /// wallet's asynchronous presence verification coming back definitively
    /// absent; a broadcaster's out-of-band fatal verdict).
    ///
    /// Same path, same guarantees as `Self::retire_undeliverable_tx`: the tx
    /// is alive-checked first (known/mined ⇒ promoted, nothing released), then
    /// every input stays locked and is scheduled for the locked-input
    /// re-check (no explorer's word releases it at retire),
    /// the tx's own outputs go unspendable and the tx is `failed` with its req
    /// at `req_status` (`"invalid"` for a dropped broadcast, `"doubleSpend"`
    /// for a named competitor). The unfail canary keeps re-verifying `invalid`
    /// reqs against the chain, so a false verdict is recoverable.
    ///
    /// Returns `Ok(None)` when no proven_tx_req exists for `txid` (nothing was
    /// ever queued for the network under that id); nothing is touched then.
    ///
    /// Retire only a transaction no instance accepted (#165): the stored
    /// accepting-word guard is shared with `abort_action`, and an acceptance
    /// refuses with `InvalidTransactionStatus` before any service is asked.
    /// The guard is read again inside the write transaction that writes the
    /// retire's rows, after the alive check (`retire_rows`), so an acceptance
    /// committed while the status service was asked still refuses.
    pub async fn retire_undeliverable_txid(
        &self,
        services: &dyn WalletServices,
        txid: &str,
        req_status: &str,
    ) -> Result<Option<RetireOutcome>> {
        // Read here so an accepted transaction asks no service; read again
        // under the retire's write lock, where it decides.
        {
            let mut conn = self.pool().acquire().await?;
            super::super::abort_action::refuse_if_accepted(&mut conn, txid, RETIRE_OPERATION)
                .await?;
        }
        let req: Option<(i64, i64)> =
            sqlx::query_as("SELECT proven_tx_req_id, attempts FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;
        let Some((proven_tx_req_id, attempts)) = req else {
            return Ok(None);
        };
        self.retire_undeliverable_tx(
            services,
            txid,
            proven_tx_req_id,
            attempts.saturating_add(1),
            req_status,
            RetireBasis::Undelivered,
            Some(RETIRE_OPERATION),
            chrono::Utc::now(),
        )
        .await
        .map(Some)
    }
}

/// The rows of `txid` a retire ends, read on its write transaction: every
/// `sending` or `unproven` row on evidence (`evidence`, or evidence on
/// record), else the outgoing ones alone (a word ends the rows of the user
/// whose word it is, `word_scope`). Ordered outgoing before received: the
/// head of the list is the row whose inputs and descendants the retire
/// reads.
async fn retired_rows_on(
    conn: &mut sqlx::SqliteConnection,
    txid: &str,
    evidence: bool,
) -> Result<Vec<i64>> {
    let candidates: Vec<(i64, String, bool)> = sqlx::query_as(
        "SELECT transaction_id, txid, is_outgoing FROM transactions \
         WHERE txid = ? AND status IN ('sending', 'unproven') \
         ORDER BY is_outgoing DESC, transaction_id",
    )
    .bind(txid)
    .fetch_all(&mut *conn)
    .await?;
    if evidence {
        return Ok(candidates.into_iter().map(|(id, ..)| id).collect());
    }
    super::super::word_scope::rows_a_word_ends_on(conn, &candidates).await
}

/// `ids` `failed`, each one's own outputs unspendable (`outputs_clause`
/// narrows which: `AND spent_by IS NULL` for the retire), on the caller's
/// write.
async fn fail_rows_on(
    conn: &mut sqlx::SqliteConnection,
    ids: &[i64],
    outputs_clause: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let outputs = format!(
        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? {outputs_clause}"
    );
    for id in ids {
        sqlx::query(&outputs)
            .bind(now)
            .bind(id)
            .execute(&mut *conn)
            .await?;
        sqlx::query(
            "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
        )
        .bind(now)
        .bind(id)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The inputs a retired transaction spends, read on its write transaction.
async fn retire_inputs(
    conn: &mut sqlx::SqliteConnection,
    transaction_id: i64,
) -> Result<Vec<RetireInput>> {
    Ok(sqlx::query(
        r#"
        SELECT o.output_id, t.txid AS source_txid, o.vout
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        WHERE o.spent_by = ?
        "#,
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .map(|row| {
        (
            row.get("output_id"),
            row.get("source_txid"),
            row.get("vout"),
        )
    })
    .collect())
}

impl StorageSqlx {
    pub(super) async fn mark_transaction_rejected_impl(
        &self,
        txid: &str,
        double_spend: bool,
    ) -> Result<bool> {
        let now = chrono::Utc::now();
        let req_status = if double_spend {
            "doubleSpend"
        } else {
            "invalid"
        };

        // Terminal-fatal broadcaster verdict: stop re-broadcasting. Deeper
        // reconciliation (input restoration with UTXO verification, change
        // invalidation) remains with the existing review/cleanup machinery,
        // which keys off these req statuses.
        let req_result = sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, updated_at = ? \
             WHERE txid = ? AND status NOT IN ('completed', 'doubleSpend', 'invalid', 'failed')",
        )
        .bind(req_status)
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        // A broadcaster's word ends the rows of the user whose post it
        // answers (the outgoing rows), a received row only on evidence on
        // record (`word_scope`).
        let ended = {
            let mut tx = self.write_tx().await?;
            let ended = retired_rows_on(&mut tx, txid, false).await?;
            // Their outputs never fund anything again: change chained on a
            // rejected transaction is how a wallet poisons itself
            // (2026-09-02). The inputs and the descendants are released /
            // retired by the reconcile sweep, which has the services for
            // the per-input chain verification.
            fail_rows_on(&mut tx, &ended, "", now).await?;
            super::commit(tx).await?;
            ended.len()
        };

        let updated = req_result.rows_affected() > 0 || ended > 0;
        if updated {
            // No provider skips it again.
            self.mark_broadcast_rejected_quiet(txid).await;
            tracing::warn!(
                txid = %txid,
                double_spend = double_spend,
                "marked rejected by broadcaster: proven_tx_req → {}, transaction → failed",
                req_status
            );
        }
        Ok(updated)
    }
}
