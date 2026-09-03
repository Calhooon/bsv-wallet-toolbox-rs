//! Locked inputs of failed transactions, re-checked with backoff (migration
//! `003_locked_input_checks`).
//!
//! THE RELEASE RULE keeps an input locked when the chain cannot vouch for it
//! at retire time (`is_utxo` unknown, rate-limited, or false). That is the
//! safe direction, but it must not be the final word: on 2026-09-02 (beta,
//! w0) 224,575 sats sat on the output a phantom root had spent, kept locked
//! by one inconclusive lookup, and nothing ever retried.
//!
//! `locked_input_checks` remembers every such input (the retire paths
//! schedule it; [`StorageSqlx::adopt_locked_inputs`] picks up any locked
//! input of a failed transaction that predates the table).
//! [`StorageSqlx::recheck_locked_inputs`] re-examines the due rows with
//! exponential backoff (1, 2, 4 ... 64 minutes) until the chain answers:
//! unspent, and the coin goes back to coin selection; spent on chain by
//! another transaction, and it is left locked, terminal; a source this
//! wallet retired as a phantom, and the row is dropped (the coin never
//! existed). Every verdict is logged.

use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::error::{Error, Result};
use crate::services::WalletServices;

use super::poisoned_chain::{chain_knowledge, utxo_verdict, ChainKnowledge, UtxoVerdict};
use super::storage_sqlx::StorageSqlx;

/// Migration `003_locked_input_checks` (additive, idempotent).
pub const MIGRATION_003_LOCKED_INPUT_CHECKS_SQL: &str =
    include_str!("migrations/003_locked_input_checks.sql");

/// Name of migration 003, as `StorageSqlx::migrate` reports it.
pub const MIGRATION_003_LOCKED_INPUT_CHECKS_NAME: &str = "003_locked_input_checks";

/// The longest pause between two re-checks of one input (minutes).
pub const LOCKED_INPUT_BACKOFF_CAP_MINUTES: i64 = 64;

/// Pause between two chain lookups of one pass (WhatsOnChain's public rate).
const CHECK_PACE: std::time::Duration = std::time::Duration::from_millis(350);

/// Minutes until the next re-check after `attempts` inconclusive ones:
/// 1, 2, 4, 8, 16, 32, then [`LOCKED_INPUT_BACKOFF_CAP_MINUTES`].
pub fn locked_input_backoff_minutes(attempts: u32) -> i64 {
    (1i64 << attempts.saturating_sub(1).min(6)).min(LOCKED_INPUT_BACKOFF_CAP_MINUTES)
}

/// What one re-check decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockedInputVerdict {
    /// Verifiably unspent: restored to coin selection (or, on a dry run,
    /// would be).
    Restored,
    /// Spent on chain by another transaction: left locked, never re-checked.
    Spent,
    /// The chain could not say: re-checked later with backoff.
    Unknown,
    /// The coin's source transaction is a phantom this wallet retired: the
    /// coin never existed, the row is dropped.
    Phantom,
    /// The output is no longer locked by a failed transaction (already
    /// released, or its spender is live): the row is dropped.
    Released,
}

/// One re-checked input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedInputCheck {
    /// `outputs.output_id`.
    pub output_id: i64,
    /// The coin's source transaction.
    pub source_txid: String,
    /// Output index.
    pub vout: u32,
    /// Its value.
    pub satoshis: i64,
    /// The failed transaction that locked it.
    pub locked_by: String,
    /// The decision.
    pub verdict: LockedInputVerdict,
    /// Re-checks so far (including this one).
    pub attempts: u32,
    /// Minutes until the next re-check (`Unknown` only).
    pub next_check_minutes: Option<i64>,
}

/// The result of one re-check pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockedInputReport {
    /// Locked inputs of failed transactions adopted into the table.
    pub adopted: u32,
    /// Rows due at the start of the pass (before the limit).
    pub due: usize,
    /// Every input examined, in order.
    pub checks: Vec<LockedInputCheck>,
    /// Inputs restored (or, on a dry run, restorable).
    pub restored: u32,
    /// Their value.
    pub restored_sats: i64,
    /// Inputs found spent (terminal).
    pub spent: u32,
    /// Inputs still undecided (backoff).
    pub unknown: u32,
    /// Rows dropped (released or phantom).
    pub dropped: u32,
    /// Whether changes were applied.
    pub executed: bool,
}

/// One due row joined with its output and transactions.
struct DueRow {
    output_id: i64,
    attempts: u32,
    vout: i64,
    satoshis: i64,
    locking_script: Option<Vec<u8>>,
    spendable: bool,
    spent_by: Option<i64>,
    source_txid: Option<String>,
    source_status: String,
    locked_by: Option<String>,
    spender_status: Option<String>,
}

impl StorageSqlx {
    /// Make sure the `locked_input_checks` table exists (migration 003;
    /// idempotent). Called on `make_available` and by every entry point.
    pub(crate) async fn ensure_locked_inputs_schema(&self) -> Result<()> {
        super::broadcast_seen::apply_migration_sql(
            self.pool(),
            MIGRATION_003_LOCKED_INPUT_CHECKS_NAME,
            MIGRATION_003_LOCKED_INPUT_CHECKS_SQL,
        )
        .await
    }

    /// Remember that `output_id` stayed locked with `verdict` (`"unknown"`
    /// or `"spent"`) so the reconcile passes re-check it: a new row starts
    /// at one attempt and one minute; an existing row backs off. Logged,
    /// never failing (bookkeeping must not fail a retire).
    pub(crate) async fn schedule_locked_input_check(&self, output_id: i64, verdict: &str) {
        if let Err(e) = self
            .schedule_locked_input_check_inner(output_id, verdict)
            .await
        {
            tracing::warn!(output_id, error = %e, "locked input: could not schedule the re-check");
        }
    }

    async fn schedule_locked_input_check_inner(&self, output_id: i64, verdict: &str) -> Result<()> {
        self.ensure_locked_inputs_schema().await?;
        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT attempts FROM locked_input_checks WHERE output_id = ?")
                .bind(output_id)
                .fetch_optional(self.pool())
                .await?;
        let attempts = existing.map(|(a,)| a as u32 + 1).unwrap_or(1);
        let minutes = locked_input_backoff_minutes(attempts);
        sqlx::query(
            "INSERT INTO locked_input_checks (output_id, attempts, last_verdict, last_checked_at, next_check_at) \
             VALUES (?, ?, ?, CURRENT_TIMESTAMP, datetime('now', ?)) \
             ON CONFLICT(output_id) DO UPDATE SET \
             attempts = excluded.attempts, last_verdict = excluded.last_verdict, \
             last_checked_at = CURRENT_TIMESTAMP, next_check_at = excluded.next_check_at",
        )
        .bind(output_id)
        .bind(attempts as i64)
        .bind(verdict)
        .bind(format!("+{} minutes", minutes))
        .execute(self.pool())
        .await?;
        tracing::info!(
            output_id,
            verdict,
            attempts,
            next_check_minutes = minutes,
            "locked input: re-check scheduled"
        );
        Ok(())
    }

    /// Give every locked input of a failed transaction (`spendable = 0`,
    /// `spent_by` a `failed` transaction, source not itself a retired
    /// phantom) that has no row a row due now.
    /// Covers inputs kept before this table existed and any path that
    /// failed a transaction without scheduling.
    pub async fn adopt_locked_inputs(&self) -> Result<u32> {
        self.ensure_locked_inputs_schema().await?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO locked_input_checks (output_id, attempts, next_check_at) \
             SELECT o.output_id, 0, CURRENT_TIMESTAMP FROM outputs o \
             JOIN transactions t ON t.transaction_id = o.spent_by \
             JOIN transactions src ON src.transaction_id = o.transaction_id \
             WHERE o.spendable = 0 AND t.status = 'failed' AND src.status <> 'failed'",
        )
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() as u32)
    }

    /// The rows due for a re-check (not terminal, `next_check_at` passed),
    /// oldest due first.
    async fn locked_inputs_due(&self, limit: usize) -> Result<Vec<DueRow>> {
        let rows = sqlx::query(
            "SELECT c.output_id, c.attempts, o.vout, o.satoshis, o.locking_script, o.spendable, o.spent_by, \
                    src.txid AS source_txid, src.status AS source_status, \
                    spender.txid AS locked_by, spender.status AS spender_status \
             FROM locked_input_checks c \
             JOIN outputs o ON o.output_id = c.output_id \
             JOIN transactions src ON src.transaction_id = o.transaction_id \
             LEFT JOIN transactions spender ON spender.transaction_id = o.spent_by \
             WHERE (c.last_verdict IS NULL OR c.last_verdict <> 'spent') \
               AND datetime(c.next_check_at) <= datetime('now') \
             ORDER BY datetime(c.next_check_at) ASC, c.output_id ASC LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .iter()
            .map(|r| DueRow {
                output_id: r.get("output_id"),
                attempts: r.get::<i64, _>("attempts").max(0) as u32,
                vout: r.get("vout"),
                satoshis: r.get("satoshis"),
                locking_script: r.get("locking_script"),
                spendable: r.get("spendable"),
                spent_by: r.get("spent_by"),
                source_txid: r.get("source_txid"),
                source_status: r.get("source_status"),
                locked_by: r.get("locked_by"),
                spender_status: r.get("spender_status"),
            })
            .collect())
    }

    /// Number of rows still waiting for a decision (not terminal).
    pub async fn locked_inputs_pending(&self) -> Result<u32> {
        self.ensure_locked_inputs_schema().await?;
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM locked_input_checks \
             WHERE last_verdict IS NULL OR last_verdict <> 'spent'",
        )
        .fetch_one(self.pool())
        .await?;
        Ok(count.max(0) as u32)
    }

    /// Re-check up to `limit` due locked inputs (see the module docs). With
    /// `execute == false` nothing is written except the adoption
    /// bookkeeping; verdicts are reported as they would be applied.
    pub async fn recheck_locked_inputs(
        &self,
        services: &dyn WalletServices,
        limit: usize,
        execute: bool,
    ) -> Result<LockedInputReport> {
        let mut report = LockedInputReport {
            adopted: self.adopt_locked_inputs().await?,
            ..Default::default()
        };
        let due = self.locked_inputs_due(limit.max(1)).await?;
        report.due = due.len();
        let now = Utc::now();

        for (index, row) in due.iter().enumerate() {
            let source_txid = row.source_txid.clone().unwrap_or_default();
            let locked_by = row.locked_by.clone().unwrap_or_default();
            let mut check = LockedInputCheck {
                output_id: row.output_id,
                source_txid: source_txid.clone(),
                vout: row.vout.max(0) as u32,
                satoshis: row.satoshis,
                locked_by: locked_by.clone(),
                verdict: LockedInputVerdict::Unknown,
                attempts: row.attempts + 1,
                next_check_minutes: None,
            };

            let live_lock = row.spender_status.as_deref().is_some_and(|s| s != "failed");
            if row.spendable || row.spent_by.is_none() || live_lock {
                check.verdict = LockedInputVerdict::Released;
                report.dropped += 1;
                if execute {
                    self.drop_locked_input_check(row.output_id).await?;
                }
                tracing::info!(
                    output_id = row.output_id,
                    outpoint = %format!("{}:{}", source_txid, row.vout),
                    "locked input: no longer locked by a failed transaction, re-check dropped"
                );
                report.checks.push(check);
                continue;
            }
            if row.source_status == "failed" {
                check.verdict = LockedInputVerdict::Phantom;
                report.dropped += 1;
                if execute {
                    self.drop_locked_input_check(row.output_id).await?;
                }
                tracing::warn!(
                    output_id = row.output_id,
                    outpoint = %format!("{}:{}", source_txid, row.vout),
                    satoshis = row.satoshis,
                    "locked input: its source is a retired phantom, the coin never existed; re-check dropped"
                );
                report.checks.push(check);
                continue;
            }

            if index > 0 {
                tokio::time::sleep(CHECK_PACE).await;
            }
            let script = row.locking_script.as_deref().unwrap_or(&[]);
            let verdict = utxo_verdict(services, &source_txid, check.vout, script).await;
            // "Spent" from the UTXO lookup means "not in the unspent set":
            // only a chain-known source makes that a real spend.
            let verdict = match verdict {
                UtxoVerdict::Spent => {
                    tokio::time::sleep(CHECK_PACE).await;
                    match chain_knowledge(services, &source_txid).await {
                        ChainKnowledge::Mined | ChainKnowledge::Known => UtxoVerdict::Spent,
                        ChainKnowledge::Unknown | ChainKnowledge::Unavailable => {
                            UtxoVerdict::Unknown
                        }
                    }
                }
                other => other,
            };
            match verdict {
                UtxoVerdict::Unspent => {
                    check.verdict = LockedInputVerdict::Restored;
                    report.restored += 1;
                    report.restored_sats += row.satoshis.max(0);
                    if execute {
                        sqlx::query(
                            "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? \
                             WHERE output_id = ? AND spendable = 0",
                        )
                        .bind(now)
                        .bind(row.output_id)
                        .execute(self.pool())
                        .await?;
                        self.drop_locked_input_check(row.output_id).await?;
                    }
                    tracing::warn!(
                        output_id = row.output_id,
                        outpoint = %format!("{}:{}", source_txid, row.vout),
                        satoshis = row.satoshis,
                        locked_by = %locked_by,
                        attempts = check.attempts,
                        executed = execute,
                        "locked input: verifiably UNSPENT on chain, restored to coin selection"
                    );
                }
                UtxoVerdict::Spent => {
                    check.verdict = LockedInputVerdict::Spent;
                    report.spent += 1;
                    if execute {
                        sqlx::query(
                            "UPDATE locked_input_checks SET attempts = ?, last_verdict = 'spent', \
                             last_checked_at = CURRENT_TIMESTAMP WHERE output_id = ?",
                        )
                        .bind(check.attempts as i64)
                        .bind(row.output_id)
                        .execute(self.pool())
                        .await?;
                    }
                    tracing::warn!(
                        output_id = row.output_id,
                        outpoint = %format!("{}:{}", source_txid, row.vout),
                        satoshis = row.satoshis,
                        locked_by = %locked_by,
                        "locked input: SPENT on chain by another transaction, left locked (terminal)"
                    );
                }
                UtxoVerdict::Unknown => {
                    let minutes = locked_input_backoff_minutes(check.attempts);
                    check.verdict = LockedInputVerdict::Unknown;
                    check.next_check_minutes = Some(minutes);
                    report.unknown += 1;
                    if execute {
                        sqlx::query(
                            "UPDATE locked_input_checks SET attempts = ?, last_verdict = 'unknown', \
                             last_checked_at = CURRENT_TIMESTAMP, next_check_at = datetime('now', ?) \
                             WHERE output_id = ?",
                        )
                        .bind(check.attempts as i64)
                        .bind(format!("+{} minutes", minutes))
                        .bind(row.output_id)
                        .execute(self.pool())
                        .await?;
                    }
                    tracing::info!(
                        output_id = row.output_id,
                        outpoint = %format!("{}:{}", source_txid, row.vout),
                        satoshis = row.satoshis,
                        attempts = check.attempts,
                        next_check_minutes = minutes,
                        "locked input: the chain could not say, re-check scheduled with backoff"
                    );
                }
            }
            report.checks.push(check);
        }
        report.executed = execute;
        Ok(report)
    }

    async fn drop_locked_input_check(&self, output_id: i64) -> Result<()> {
        sqlx::query("DELETE FROM locked_input_checks WHERE output_id = ?")
            .bind(output_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// `next_check_at` of one row (tests and diagnostics).
    pub async fn locked_input_next_check(&self, output_id: i64) -> Result<Option<DateTime<Utc>>> {
        self.ensure_locked_inputs_schema().await?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT CAST(next_check_at AS TEXT) FROM locked_input_checks WHERE output_id = ?",
        )
        .bind(output_id)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| Error::SqlxError(e.to_string()))?;
        Ok(row.map(|(text,)| super::broadcast_seen::parse_db_timestamp(&text)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_one_minute_and_caps() {
        assert_eq!(locked_input_backoff_minutes(0), 1);
        assert_eq!(locked_input_backoff_minutes(1), 1);
        assert_eq!(locked_input_backoff_minutes(2), 2);
        assert_eq!(locked_input_backoff_minutes(3), 4);
        assert_eq!(locked_input_backoff_minutes(6), 32);
        assert_eq!(locked_input_backoff_minutes(7), 64);
        assert_eq!(locked_input_backoff_minutes(40), 64);
    }
}
