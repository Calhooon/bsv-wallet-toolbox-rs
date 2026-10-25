use super::{FindProvenTxReqsArgs, ProvenTxReqStatus, Result, Row, StorageSqlx, TableProvenTxReq};

/// Upper bound on the proof-less `transactions` rows ONE
/// [`StorageSqlx::adopt_unproven_transactions`] pass turns into
/// `proven_tx_reqs`. A backlog drains over successive passes (every
/// new-header trigger / fallback tick of `CheckForProofs`); the bound keeps
/// a single pass' write burst small and its runtime predictable.
pub const ADOPT_UNPROVEN_TX_LIMIT: i64 = 200;

/// Summary of one [`StorageSqlx::adopt_unproven_transactions`] pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UnprovenAdoption {
    /// `transactions.proven_tx_id` links written from an EXISTING
    /// `proven_txs` row — the proof was already stored (by any path) and
    /// only the link was missing; no fetch is needed for these.
    pub linked_from_proven_txs: u64,
    /// `proven_tx_reqs` rows created (`'unmined'`) for proof-less
    /// transactions that had NO req at all — the rows every proof path
    /// was blind to.
    pub reqs_created: u64,
}

impl StorageSqlx {
    // =========================================================================
    // ProvenTxReq Repository
    // =========================================================================

    /// Find proven tx requests.
    pub async fn find_proven_tx_reqs_internal(
        &self,
        args: &FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        let mut sql = String::from(
            r#"
            SELECT proven_tx_req_id, proven_tx_id, txid, status, attempts, history,
                   notified, raw_tx, input_beef, batch, notify, created_at, updated_at
            FROM proven_tx_reqs
            WHERE 1=1
            "#,
        );

        if let Some(ref statuses) = args.status {
            if !statuses.is_empty() {
                let placeholders: Vec<&str> = statuses.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND status IN ({})", placeholders.join(",")));
            }
        }

        if let Some(ref txids) = args.txids {
            if !txids.is_empty() {
                let placeholders: Vec<&str> = txids.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND txid IN ({})", placeholders.join(",")));
            }
        }

        if args.base.since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }

        // Order
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY updated_at DESC");
        } else {
            sql.push_str(" ORDER BY updated_at ASC");
        }

        // Pagination
        if let Some(ref paged) = args.base.paged {
            if let Some(limit) = paged.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = paged.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        let mut query = sqlx::query(&sql);

        if let Some(ref statuses) = args.status {
            for status in statuses {
                query = query.bind(format!("{:?}", status).to_lowercase());
            }
        }

        if let Some(ref txids) = args.txids {
            for txid in txids {
                query = query.bind(txid);
            }
        }

        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.reader).await?;

        let mut reqs = Vec::new();
        for row in rows {
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "pending" => ProvenTxReqStatus::Pending,
                "inprogress" | "in_progress" => ProvenTxReqStatus::InProgress,
                "completed" => ProvenTxReqStatus::Completed,
                "failed" => ProvenTxReqStatus::Failed,
                "notfound" | "not_found" => ProvenTxReqStatus::NotFound,
                _ => ProvenTxReqStatus::Pending,
            };

            let notified_val: i32 = row.get("notified");

            reqs.push(TableProvenTxReq {
                proven_tx_req_id: row.get("proven_tx_req_id"),
                txid: row.get("txid"),
                status,
                attempts: row.get("attempts"),
                history: row.get("history"),
                notified: notified_val != 0,
                notify: row.try_get::<String, _>("notify").unwrap_or_default(),
                raw_tx: row.try_get("raw_tx").ok().flatten(),
                input_beef: row.try_get("input_beef").ok().flatten(),
                proven_tx_id: row.get("proven_tx_id"),
                batch: row.try_get("batch").ok(),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(reqs)
    }
    /// Bring every proof-less spendable transaction back into the
    /// `CheckForProofs` eligible set (the bsv-wallet-cli receive-proof
    /// gap, 2026-08-29).
    ///
    /// ## The gap
    ///
    /// `synchronize_transaction_statuses` walks `proven_tx_reqs` ONLY. A
    /// `transactions` row that is `'completed'` (or `'unproven'`) with
    /// `proven_tx_id IS NULL` and NO req is invisible to it — and to every
    /// other proof path — forever. Before 0.3.52 `internalize_action` wrote
    /// exactly that shape for every internalized coin (status `'completed'`
    /// from `has_proof` alone, no `proven_tx_id`, no req; issue #8), so a
    /// wallet that received coins on those releases carries spendable
    /// outputs whose transactions can never be proven. Measured on the
    /// LOW e2e fleet wallets: dozens of such rows per wallet, still
    /// holding live outputs. The symptom downstream: every BEEF built on
    /// one of those coins drags the whole raw ancestry (no BUMP truncates
    /// it), so BEEF handoffs grow without bound and ARC fee-rejects the
    /// package (the 2026-07 "465 fee too low" incident shape).
    ///
    /// 0.3.52 restored `completed ⇒ proven` for NEW internalizes; this
    /// pass repairs the rows that predate it and any future producer that
    /// slips — a self-healing task rather than a one-shot migration, so a
    /// regression re-heals on the next tick instead of accumulating.
    ///
    /// ## What one pass does (bounded, idempotent, additive)
    ///
    /// 1. **Link.** A `transactions` row whose txid ALREADY has a
    ///    `proven_txs` row gets `proven_tx_id` linked (and `'completed'`,
    ///    the status a proven tx has by definition); any proof-eligible
    ///    req for it (`unmined`/`unknown`/`callback`/`sending`/
    ///    `unconfirmed`) is completed with the same link so it leaves the
    ///    eligible set. `proven_txs` rows are only ever written through a
    ///    validated proof (`ingest_merkle_proof`, the BUMP-bearing
    ///    internalize), so the link asserts nothing new.
    /// 2. **Adopt.** For up to [`ADOPT_UNPROVEN_TX_LIMIT`] remaining
    ///    proof-less rows (status `'completed'`/`'unproven'`, a txid, raw
    ///    bytes, NO req), create the req as `'unmined'` — "broadcast
    ///    externally, awaiting a proof", the same status the nosend-merge
    ///    path uses for an externally-broadcast tx. From here the ordinary
    ///    triage → fetch → [`StorageSqlx::ingest_merkle_proof`] flow owns
    ///    it: a mined tx gets its BUMP and the link on a later pass; a tx
    ///    the chain never saw is triaged `missing` every pass, exactly as
    ///    an ordinary `'unmined'` req of a never-mined tx is today.
    ///
    /// Statuses are otherwise untouched: a `'completed'` row stays
    /// spendable while its proof is fetched (coin selection accepts both
    /// `completed` and `unproven`), and a row that has ANY req — even a
    /// `'nosend'` one (the release-pin flow) or a terminal `'invalid'` —
    /// is left to that req's owner. `review_status` remains the
    /// tx↔req status-mismatch repair; this pass only covers the case it
    /// cannot see (no req to mismatch against).
    pub async fn adopt_unproven_transactions(&self) -> Result<UnprovenAdoption> {
        let now = chrono::Utc::now();
        let mut out = UnprovenAdoption::default();

        // 1. Link from an existing proof. Rows in the eligible statuses
        //    only — a 'failed'/'nosend' row is not made 'completed' by the
        //    existence of a proof row somebody else wrote for the same txid.
        let linked = sqlx::query(
            r#"
            UPDATE transactions
            SET proven_tx_id = (SELECT p.proven_tx_id FROM proven_txs p
                                WHERE p.txid = transactions.txid LIMIT 1),
                status = 'completed',
                updated_at = ?
            WHERE proven_tx_id IS NULL
              AND txid IS NOT NULL
              AND status IN ('completed', 'unproven')
              AND EXISTS (SELECT 1 FROM proven_txs p WHERE p.txid = transactions.txid)
            "#,
        )
        .bind(now)
        .execute(&mut *self.writer_conn().await?)
        .await?
        .rows_affected();
        out.linked_from_proven_txs = linked;
        if linked > 0 {
            // Retire the proof-eligible req (if any) with the same link so
            // the next triage does not re-fetch a proof we already hold.
            sqlx::query(
                r#"
                UPDATE proven_tx_reqs
                SET status = 'completed',
                    proven_tx_id = (SELECT p.proven_tx_id FROM proven_txs p
                                    WHERE p.txid = proven_tx_reqs.txid LIMIT 1),
                    updated_at = ?
                WHERE proven_tx_id IS NULL
                  AND status IN ('unmined', 'unknown', 'callback', 'sending', 'unconfirmed')
                  AND EXISTS (SELECT 1 FROM proven_txs p WHERE p.txid = proven_tx_reqs.txid)
                "#,
            )
            .bind(now)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        }

        // 2. Adopt the req-less remainder, bounded per pass.
        let candidates: Vec<(String, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            r#"
            SELECT t.txid, t.raw_tx, t.input_beef
            FROM transactions t
            WHERE t.proven_tx_id IS NULL
              AND t.txid IS NOT NULL
              AND t.raw_tx IS NOT NULL
              AND t.status IN ('completed', 'unproven')
              AND NOT EXISTS (SELECT 1 FROM proven_tx_reqs r WHERE r.txid = t.txid)
            ORDER BY t.transaction_id ASC
            LIMIT ?
            "#,
        )
        .bind(ADOPT_UNPROVEN_TX_LIMIT)
        .fetch_all(self.pool())
        .await?;

        for (txid, raw_tx, input_beef) in candidates {
            // `txid` is UNIQUE on proven_tx_reqs, so OR IGNORE makes the
            // adoption atomic against any concurrent creator (a req that
            // appeared since the SELECT keeps its own owner/status).
            let created = sqlx::query(
                r#"
                INSERT OR IGNORE INTO proven_tx_reqs
                    (txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at)
                VALUES (?, 'unmined', 0, '{}', '{}', 0, ?, ?, ?, ?)
                "#,
            )
            .bind(&txid)
            .bind(&raw_tx)
            .bind(&input_beef)
            .bind(now)
            .bind(now)
            .execute(&mut *self.writer_conn().await?)
            .await?
            .rows_affected();
            if created > 0 {
                out.reqs_created += 1;
                tracing::debug!(
                    txid = %txid,
                    "adopt_unproven_transactions: created 'unmined' proven_tx_req for a proof-less transaction"
                );
            }
        }

        Ok(out)
    }
}

impl StorageSqlx {
    pub(super) async fn update_proven_tx_req_status_impl(
        &self,
        proven_tx_req_id: i64,
        new_status: ProvenTxReqStatus,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        let status_str = match new_status {
            ProvenTxReqStatus::Unmined => "unmined",
            ProvenTxReqStatus::Completed => "completed",
            ProvenTxReqStatus::Failed => "failed",
            ProvenTxReqStatus::Invalid => "invalid",
            ProvenTxReqStatus::Pending => "pending",
            ProvenTxReqStatus::InProgress => "inProgress",
            ProvenTxReqStatus::NotFound => "notFound",
            ProvenTxReqStatus::Unsent => "unsent",
            ProvenTxReqStatus::Sending => "sending",
            ProvenTxReqStatus::Unknown => "unknown",
            ProvenTxReqStatus::Callback => "callback",
            ProvenTxReqStatus::Unconfirmed => "unconfirmed",
            ProvenTxReqStatus::Unfail => "unfail",
            ProvenTxReqStatus::NoSend => "nosend",
            ProvenTxReqStatus::DoubleSpend => "doubleSpend",
            ProvenTxReqStatus::NonFinal => "nonfinal",
            ProvenTxReqStatus::Unprocessed => "unprocessed",
        };

        sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(status_str)
        .bind(now)
        .bind(proven_tx_req_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        tracing::debug!(
            proven_tx_req_id = proven_tx_req_id,
            new_status = status_str,
            "Updated proven_tx_req status"
        );

        Ok(())
    }
}
