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
    },
}

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn retire_undeliverable_tx(
        &self,
        services: &dyn WalletServices,
        txid: &str,
        proven_tx_req_id: i64,
        attempts: i64,
        req_status: &str,
        basis: RetireBasis<'_>,
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

        sqlx::query("UPDATE proven_tx_reqs SET status = ?, attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(req_status)
            .bind(attempts)
            .bind(now)
            .bind(proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;

        let tx_row: Option<(i64,)> = sqlx::query_as(
            "SELECT transaction_id FROM transactions WHERE txid = ? AND status IN ('sending', 'unproven')",
        )
        .bind(txid)
        .fetch_optional(self.pool())
        .await?;
        let Some((transaction_id,)) = tx_row else {
            return Ok(RetireOutcome::Retired {
                restored: 0,
                kept: 0,
            });
        };

        // Its own outputs never fund anything again.
        sqlx::query(
            "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
        )
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        // Its inputs: none released (a node's verdict releases a coin; an
        // explorer's unspent set does not). Lost to a proven competitor, or
        // kept for the re-check.
        let lost: &[(String, u32)] = match basis {
            RetireBasis::CompetitorProven { lost, .. } => lost,
            RetireBasis::Undelivered => &[],
        };
        let input_rows = sqlx::query(
            r#"
            SELECT o.output_id, t.txid AS source_txid, o.vout, o.locking_script
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.spent_by = ?
            "#,
        )
        .bind(transaction_id)
        .fetch_all(self.pool())
        .await?;
        let mut kept = 0u32;
        for input_row in &input_rows {
            let output_id: i64 = input_row.get("output_id");
            let source_txid: String = input_row.get("source_txid");
            let vout: i32 = input_row.get("vout");
            kept += 1;
            let is_lost = lost
                .iter()
                .any(|(t, v)| *v as i64 == vout as i64 && t.eq_ignore_ascii_case(&source_txid));
            if is_lost {
                self.schedule_locked_input_check(output_id, LOCKED_VERDICT_SPENT_PROVEN)
                    .await;
                tracing::warn!(
                    txid = %txid,
                    outpoint = %format!("{}:{}", source_txid, vout),
                    "retire: input spent by the proven competitor: lost, locked for good"
                );
            } else {
                self.schedule_locked_input_check(output_id, LOCKED_VERDICT_UNKNOWN)
                    .await;
                tracing::info!(
                    txid = %txid,
                    outpoint = %format!("{}:{}", source_txid, vout),
                    "retire: input stays LOCKED (no explorer's word releases it), re-checked later"
                );
            }
        }

        sqlx::query(
            "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
        )
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

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
        sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(now)
            .bind(proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ? AND status IN ('sending', 'unproven')")
            .bind(now)
            .bind(txid)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        Ok(true)
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
    pub async fn retire_undeliverable_txid(
        &self,
        services: &dyn WalletServices,
        txid: &str,
        req_status: &str,
    ) -> Result<Option<RetireOutcome>> {
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
            chrono::Utc::now(),
        )
        .await
        .map(Some)
    }
}

impl StorageSqlx {
    pub(super) async fn mark_rejected(&self, txid: &str, double_spend: bool) -> Result<bool> {
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

        let tx_result = sqlx::query(
            "UPDATE transactions SET status = 'failed', updated_at = ? \
             WHERE txid = ? AND status IN ('sending', 'unproven')",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        if tx_result.rows_affected() > 0 {
            // Its outputs never fund anything again: change chained on a
            // rejected transaction is how a wallet poisons itself
            // (2026-09-02). The inputs and the descendants are released /
            // retired by the reconcile sweep, which has the services for
            // the per-input chain verification.
            sqlx::query(
                "UPDATE outputs SET spendable = 0, updated_at = ? \
                 WHERE transaction_id IN (SELECT transaction_id FROM transactions WHERE txid = ?)",
            )
            .bind(now)
            .bind(txid)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        }

        let updated = req_result.rows_affected() > 0 || tx_result.rows_affected() > 0;
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
