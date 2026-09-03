//! Poisoned chains: a phantom transaction and everything built on it.
//!
//! When a transaction never reached the network (its broadcaster 202'd an EF
//! child whose parent had not propagated, a definitive `REJECTED`, a
//! presence probe absent everywhere for long enough), every later
//! transaction of this wallet that spends one of its outputs is a phantom
//! too: change chained on phantom change, an internalized payment whose
//! source never existed (the 10,000 sats of the 2026-09-02 beta incident),
//! and the transactions that spent that payment on. None of them can ever
//! mine, and every input they took from OUTSIDE the phantom set is a coin
//! frozen behind a transaction that will never settle.
//!
//! [`StorageSqlx::retire_poisoned_chain`] retires the root and every unproven
//! descendant under THE RELEASE RULE (`retire_undeliverable_tx`): the root is
//! alive-checked first, an input from outside the poisoned set is released
//! only on its own `is_utxo` verification, the set's own outputs go
//! unspendable, the transactions turn `failed` with their reqs `invalid`,
//! and the broadcast memory forgets them (`rejected`). A chain that reaches
//! a proven transaction is refused: a proven descendant means the root is
//! on chain and the verdict was wrong.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::error::Result;
use crate::services::WalletServices;

use super::storage_sqlx::StorageSqlx;

/// Transaction statuses a poisoned descendant may be in to be retired.
/// `completed` refuses the whole retire; creation-time statuses
/// (`unsigned`, `unprocessed`, `nonfinal`) are left to `abort_action`.
const RETIRABLE_STATUSES: &[&str] = &["unproven", "sending", "nosend"];

/// One transaction of a poisoned chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoisonedTx {
    /// Transaction id.
    pub txid: String,
    /// `transactions.transaction_id`.
    pub transaction_id: i64,
    /// Status before retirement.
    pub status: String,
    /// `false` for a received (internalized) transaction: its spendable
    /// outputs are payments that trace to a phantom source.
    pub is_outgoing: bool,
    /// 0 for the root, 1 for a direct spender of its outputs, and so on.
    pub depth: u32,
}

impl PoisonedTx {
    /// Whether this transaction's status lets the retire touch it.
    pub fn is_retirable(&self) -> bool {
        RETIRABLE_STATUSES.contains(&self.status.as_str())
    }
}

/// An internalized payment invalidated by a poison retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalizedPhantom {
    /// The received transaction.
    pub txid: String,
    /// Output index of the payment.
    pub vout: u32,
    /// Its value.
    pub satoshis: i64,
}

/// What [`StorageSqlx::retire_poisoned_chain`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoisonOutcome {
    /// The status service knows the root (mempool or chain): nothing is
    /// poisoned. The root is promoted exactly as `retire_undeliverable_tx`
    /// does; nothing else is touched.
    Alive,
    /// Neither the root nor a spender of its outputs exists in this wallet.
    NotFound,
    /// A transaction of the chain is `completed` (proven): the root is on
    /// chain, the verdict was wrong. Nothing is touched.
    Refused {
        /// The proven transaction that stopped the retire.
        proven_txid: String,
    },
    /// The chain was retired (or, on a dry run, would be).
    Retired,
}

/// The result of one poison retirement (or dry run).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoisonReport {
    /// The txid the retire started from.
    pub root: String,
    /// The decision.
    pub outcome: PoisonOutcome,
    /// Whether changes were applied (`false` on a dry run or a non-retire
    /// outcome).
    pub executed: bool,
    /// The root (when this wallet holds it) and every descendant, in walk
    /// order (root first, then by depth).
    pub chain: Vec<PoisonedTx>,
    /// Transactions turned `failed`.
    pub failed: u32,
    /// Outside inputs verified unspent and released.
    pub restored: u32,
    /// Satoshis of the released inputs.
    pub restored_sats: i64,
    /// Outside inputs the chain could not vouch for: kept locked.
    pub kept: u32,
    /// Spendable outputs of the set invalidated.
    pub invalidated: u32,
    /// Satoshis of the invalidated outputs.
    pub invalidated_sats: i64,
    /// Internalized payments among the invalidated outputs.
    pub internalized: Vec<InternalizedPhantom>,
}

impl PoisonReport {
    fn new(root: &str, outcome: PoisonOutcome) -> Self {
        Self {
            root: root.to_string(),
            outcome,
            executed: false,
            chain: Vec::new(),
            failed: 0,
            restored: 0,
            restored_sats: 0,
            kept: 0,
            invalidated: 0,
            invalidated_sats: 0,
            internalized: Vec::new(),
        }
    }

    /// The txids of the chain that the retire touches (retirable statuses).
    pub fn retirable_txids(&self) -> Vec<String> {
        self.chain
            .iter()
            .filter(|t| t.is_retirable())
            .map(|t| t.txid.clone())
            .collect()
    }
}

/// A spendable-or-not answer for one outside input.
async fn verify_unspent(
    services: &dyn WalletServices,
    source_txid: &str,
    vout: u32,
    script: &[u8],
) -> bool {
    match services.is_utxo(source_txid, vout, script).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                source_txid = %source_txid,
                vout,
                error = %e,
                "poisoned chain: is_utxo failed, input stays LOCKED (an unknown never releases)"
            );
            false
        }
    }
}

impl StorageSqlx {
    /// The transactions of this wallet built on `root_txid`: every spender of
    /// one of its outputs, recursively, in walk order (depth 1 first). The
    /// root itself is not included. Read-only.
    pub async fn poisoned_descendants(&self, root_txid: &str) -> Result<Vec<PoisonedTx>> {
        let root: Option<(i64,)> =
            sqlx::query_as("SELECT transaction_id FROM transactions WHERE txid = ?")
                .bind(root_txid)
                .fetch_optional(self.pool())
                .await?;

        // Frontier: (transaction_id, depth). The root's spenders are found
        // through its transactions row when this wallet holds one, and
        // through the outputs' own txid column otherwise (a coin received
        // from a transaction this wallet never recorded).
        let mut frontier: Vec<(i64, u32)> = Vec::new();
        let mut visited: HashSet<i64> = HashSet::new();
        match root {
            Some((id,)) => {
                visited.insert(id);
                frontier.extend(
                    self.spenders_of_transaction(id)
                        .await?
                        .into_iter()
                        .map(|c| (c, 1)),
                );
            }
            None => {
                let rows = sqlx::query(
                    "SELECT DISTINCT spent_by FROM outputs WHERE txid = ? AND spent_by IS NOT NULL",
                )
                .bind(root_txid)
                .fetch_all(self.pool())
                .await?;
                frontier.extend(rows.iter().map(|r| (r.get::<i64, _>("spent_by"), 1)));
            }
        }

        let mut chain: Vec<PoisonedTx> = Vec::new();
        let mut index = 0usize;
        while index < frontier.len() {
            let (id, depth) = frontier[index];
            index += 1;
            if !visited.insert(id) {
                continue;
            }
            let row = sqlx::query(
                "SELECT txid, status, is_outgoing FROM transactions WHERE transaction_id = ?",
            )
            .bind(id)
            .fetch_optional(self.pool())
            .await?;
            let Some(row) = row else {
                continue;
            };
            let is_outgoing: i64 = row.get("is_outgoing");
            chain.push(PoisonedTx {
                txid: row.get::<Option<String>, _>("txid").unwrap_or_default(),
                transaction_id: id,
                status: row.get("status"),
                is_outgoing: is_outgoing != 0,
                depth,
            });
            for child in self.spenders_of_transaction(id).await? {
                frontier.push((child, depth + 1));
            }
        }
        Ok(chain)
    }

    /// `transaction_id`s of the transactions spending an output of
    /// `transaction_id`.
    async fn spenders_of_transaction(&self, transaction_id: i64) -> Result<Vec<i64>> {
        let rows = sqlx::query(
            "SELECT DISTINCT spent_by FROM outputs \
             WHERE transaction_id = ? AND spent_by IS NOT NULL ORDER BY spent_by",
        )
        .bind(transaction_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.iter().map(|r| r.get::<i64, _>("spent_by")).collect())
    }

    /// Retire `root_txid` and every unproven descendant (see the module
    /// docs). `req_status` is the root's proven_tx_req status (`"invalid"`
    /// for a phantom, `"doubleSpend"` for a named competitor); descendants
    /// always get `"invalid"`. With `execute == false` nothing is written:
    /// the report says what would happen (the root's alive check still runs,
    /// so a dry run never proposes retiring a live transaction).
    pub async fn retire_poisoned_chain(
        &self,
        services: &dyn WalletServices,
        root_txid: &str,
        req_status: &str,
        execute: bool,
    ) -> Result<PoisonReport> {
        let now = Utc::now();
        let root_row = sqlx::query(
            "SELECT transaction_id, status, is_outgoing FROM transactions WHERE txid = ?",
        )
        .bind(root_txid)
        .fetch_optional(self.pool())
        .await?;
        let root = root_row.map(|row| {
            let is_outgoing: i64 = row.get("is_outgoing");
            PoisonedTx {
                txid: root_txid.to_string(),
                transaction_id: row.get("transaction_id"),
                status: row.get("status"),
                is_outgoing: is_outgoing != 0,
                depth: 0,
            }
        });
        let descendants = self.poisoned_descendants(root_txid).await?;
        if root.is_none() && descendants.is_empty() {
            return Ok(PoisonReport::new(root_txid, PoisonOutcome::NotFound));
        }

        // A proven transaction anywhere in the chain means the root is on
        // chain: refuse, loudly.
        if let Some(proven) = root
            .iter()
            .chain(descendants.iter())
            .find(|t| t.status == "completed")
        {
            tracing::error!(
                root = %root_txid,
                proven = %proven.txid,
                "poisoned chain: a transaction of the chain is PROVEN; the verdict on the root is wrong, nothing retired"
            );
            let mut report = PoisonReport::new(
                root_txid,
                PoisonOutcome::Refused {
                    proven_txid: proven.txid.clone(),
                },
            );
            report.chain = root.into_iter().chain(descendants).collect();
            return Ok(report);
        }

        // The alive check: the status service knowing the root promotes it
        // (mempool or chain) and ends the retire.
        if super::process_action::reconcile_tx_status_via_services(services, root_txid).await {
            if execute {
                sqlx::query(
                    "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? \
                     WHERE txid = ? AND status IN ('unsent', 'sending', 'unknown', 'callback', 'unconfirmed', 'invalid')",
                )
                .bind(now)
                .bind(root_txid)
                .execute(self.pool())
                .await?;
                sqlx::query(
                    "UPDATE transactions SET status = 'unproven', updated_at = ? \
                     WHERE txid = ? AND status IN ('sending', 'unproven')",
                )
                .bind(now)
                .bind(root_txid)
                .execute(self.pool())
                .await?;
            }
            let mut report = PoisonReport::new(root_txid, PoisonOutcome::Alive);
            report.chain = root.into_iter().chain(descendants).collect();
            return Ok(report);
        }

        let mut report = PoisonReport::new(root_txid, PoisonOutcome::Retired);
        report.chain = root.into_iter().chain(descendants).collect();
        if !execute {
            return Ok(report);
        }

        let poisoned_ids: HashSet<i64> = report.chain.iter().map(|t| t.transaction_id).collect();
        let chain = report.chain.clone();
        for tx in &chain {
            let status = if tx.depth == 0 { req_status } else { "invalid" };
            self.retire_one_poisoned(
                services,
                tx,
                &poisoned_ids,
                root_txid,
                status,
                now,
                &mut report,
            )
            .await?;
        }
        report.executed = true;
        tracing::warn!(
            root = %root_txid,
            txs = report.chain.len(),
            failed = report.failed,
            restored = report.restored,
            restored_sats = report.restored_sats,
            kept_locked = report.kept,
            invalidated = report.invalidated,
            invalidated_sats = report.invalidated_sats,
            internalized = report.internalized.len(),
            "poisoned chain retired"
        );
        Ok(report)
    }

    /// The descendant half of [`StorageSqlx::retire_poisoned_chain`], for a
    /// caller that has just retired the root itself
    /// (`retire_undeliverable_tx`). Returns `(restored, kept)` over the
    /// descendants' outside inputs; nothing happens when a descendant is
    /// proven.
    pub(crate) async fn retire_poisoned_descendants(
        &self,
        services: &dyn WalletServices,
        root_txid: &str,
        root_transaction_id: i64,
        now: DateTime<Utc>,
    ) -> Result<(u32, u32)> {
        let descendants = self.poisoned_descendants(root_txid).await?;
        if descendants.is_empty() {
            return Ok((0, 0));
        }
        if let Some(proven) = descendants.iter().find(|t| t.status == "completed") {
            tracing::error!(
                root = %root_txid,
                proven = %proven.txid,
                "poisoned chain: a descendant is PROVEN; descendants left untouched"
            );
            return Ok((0, 0));
        }
        let mut poisoned_ids: HashSet<i64> = descendants.iter().map(|t| t.transaction_id).collect();
        poisoned_ids.insert(root_transaction_id);
        let mut report = PoisonReport::new(root_txid, PoisonOutcome::Retired);
        for tx in &descendants {
            self.retire_one_poisoned(
                services,
                tx,
                &poisoned_ids,
                root_txid,
                "invalid",
                now,
                &mut report,
            )
            .await?;
        }
        tracing::warn!(
            root = %root_txid,
            descendants = descendants.len(),
            failed = report.failed,
            restored = report.restored,
            kept_locked = report.kept,
            invalidated = report.invalidated,
            "poisoned chain: descendants of the retired transaction retired too"
        );
        Ok((report.restored, report.kept))
    }

    /// Retire one transaction of a poisoned set: outside inputs released on
    /// verification, own outputs invalidated, `failed`, req at `req_status`,
    /// memory `rejected`. A non-retirable status (already `failed`) still
    /// gets its outputs invalidated and its locked inputs re-examined.
    #[allow(clippy::too_many_arguments)]
    async fn retire_one_poisoned(
        &self,
        services: &dyn WalletServices,
        tx: &PoisonedTx,
        poisoned_ids: &HashSet<i64>,
        root_txid: &str,
        req_status: &str,
        now: DateTime<Utc>,
        report: &mut PoisonReport,
    ) -> Result<()> {
        // Inputs: released one by one, each on a chain verification, and
        // only when the coin came from OUTSIDE the poisoned set.
        let inputs = sqlx::query(
            "SELECT o.output_id, o.transaction_id AS parent_id, t.txid AS source_txid, \
                    o.vout, o.locking_script, o.satoshis \
             FROM outputs o JOIN transactions t ON o.transaction_id = t.transaction_id \
             WHERE o.spent_by = ?",
        )
        .bind(tx.transaction_id)
        .fetch_all(self.pool())
        .await?;
        for input in &inputs {
            let output_id: i64 = input.get("output_id");
            let parent_id: i64 = input.get("parent_id");
            let source_txid: Option<String> = input.get("source_txid");
            let source_txid = source_txid.unwrap_or_default();
            let vout: i64 = input.get("vout");
            let satoshis: i64 = input.get("satoshis");
            if poisoned_ids.contains(&parent_id) || source_txid == root_txid {
                tracing::debug!(
                    txid = %tx.txid,
                    source = %source_txid,
                    vout,
                    "poisoned chain: input comes from the poisoned set, stays dead"
                );
                continue;
            }
            let locking_script: Option<Vec<u8>> = input.get("locking_script");
            let script = locking_script.as_deref().unwrap_or(&[]);
            if verify_unspent(services, &source_txid, vout as u32, script).await {
                sqlx::query(
                    "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? \
                     WHERE output_id = ? AND spent_by = ?",
                )
                .bind(now)
                .bind(output_id)
                .bind(tx.transaction_id)
                .execute(self.pool())
                .await?;
                report.restored += 1;
                report.restored_sats += satoshis.max(0);
            } else {
                report.kept += 1;
                tracing::info!(
                    txid = %tx.txid,
                    source = %source_txid,
                    vout,
                    "poisoned chain: input not verifiably unspent, stays LOCKED"
                );
            }
        }

        // Own outputs never fund anything again. A received transaction's
        // outputs are internalized payments: every one of them traces to a
        // phantom source, whether still spendable (the balance overstated
        // by it) or already spent by a poisoned child.
        let outputs = sqlx::query(
            "SELECT output_id, vout, satoshis, spendable, spent_by FROM outputs \
             WHERE transaction_id = ? ORDER BY vout",
        )
        .bind(tx.transaction_id)
        .fetch_all(self.pool())
        .await?;
        for output in &outputs {
            let vout: i64 = output.get("vout");
            let satoshis: i64 = output.get("satoshis");
            let spendable: bool = output.get("spendable");
            let spent_by: Option<i64> = output.get("spent_by");
            if spendable {
                report.invalidated += 1;
                report.invalidated_sats += satoshis.max(0);
            }
            if !tx.is_outgoing {
                tracing::warn!(
                    txid = %tx.txid,
                    vout,
                    satoshis,
                    root = %root_txid,
                    was_spendable = spendable,
                    spent_by = ?spent_by,
                    "internalized payment traces to a phantom source transaction; unspendable (the balance overstated by this amount)"
                );
                report.internalized.push(InternalizedPhantom {
                    txid: tx.txid.clone(),
                    vout: vout as u32,
                    satoshis,
                });
            }
        }
        sqlx::query("UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?")
            .bind(now)
            .bind(tx.transaction_id)
            .execute(self.pool())
            .await?;

        // The transaction and its req.
        if tx.is_retirable() {
            let failed = sqlx::query(
                "UPDATE transactions SET status = 'failed', updated_at = ? \
                 WHERE transaction_id = ? AND status IN ('unproven', 'sending', 'nosend')",
            )
            .bind(now)
            .bind(tx.transaction_id)
            .execute(self.pool())
            .await?;
            report.failed += failed.rows_affected() as u32;
        }
        sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, attempts = attempts + 1, updated_at = ? \
             WHERE txid = ? AND status NOT IN ('completed')",
        )
        .bind(req_status)
        .bind(now)
        .bind(&tx.txid)
        .execute(self.pool())
        .await?;

        // No provider ever skips it again.
        self.mark_broadcast_rejected_quiet(&tx.txid).await;

        tracing::warn!(
            txid = %tx.txid,
            depth = tx.depth,
            root = %root_txid,
            was = %tx.status,
            "poisoned chain: transaction retired"
        );
        Ok(())
    }
}
