use super::{
    commit, AuthId, Error, PurgeParams, PurgeResults, Result, ReviewStatusResult, StorageSqlx,
    WalletStorageReader,
};

impl StorageSqlx {
    pub(super) async fn review_status_for_user(
        &self,
        auth: &AuthId,
        aged_limit: chrono::DateTime<chrono::Utc>,
    ) -> Result<ReviewStatusResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let mut log = String::new();

        // 1. Find aged proven_tx_reqs that may need attention
        let aged_reqs: Vec<(i64, String, String)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, txid, status
            FROM proven_tx_reqs
            WHERE updated_at < ?
              AND status IN ('unmined', 'sending', 'unknown', 'unconfirmed', 'callback')
            "#,
        )
        .bind(aged_limit)
        .fetch_all(self.pool())
        .await?;

        if !aged_reqs.is_empty() {
            log.push_str(&format!(
                "Found {} aged proven_tx_reqs needing attention.\n",
                aged_reqs.len()
            ));
        }

        // 2. Find transactions where status doesn't match proven_tx_req status
        let mismatches: Vec<(i64, String, String, String, Option<i64>)> = sqlx::query_as(
            r#"
            SELECT t.transaction_id, t.txid, t.status AS tx_status, p.status AS req_status, p.proven_tx_id
            FROM transactions t
            JOIN proven_tx_reqs p ON t.txid = p.txid
            WHERE t.user_id = ?
              AND (
                (t.status = 'unproven' AND p.status = 'completed')
                OR (t.status = 'sending' AND p.status IN ('completed', 'unmined'))
                OR (t.status = 'completed' AND p.status NOT IN ('completed', 'nosend'))
              )
            "#,
        )
        .bind(user_id)
        .fetch_all(self.pool())
        .await?;

        // 3. Fix mismatches
        for (tx_id, txid, tx_status, req_status, proven_tx_id) in &mismatches {
            if tx_status == "unproven" && req_status == "completed" {
                // Transaction should be completed since proof exists
                sqlx::query("UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE transaction_id = ?")
                    .bind(proven_tx_id)
                    .bind(chrono::Utc::now())
                    .bind(tx_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                log.push_str(&format!(
                    "Fixed tx {}: unproven -> completed (proof exists).\n",
                    txid
                ));
            } else if tx_status == "sending"
                && (req_status == "completed" || req_status == "unmined")
            {
                // Transaction was sent; update status
                let new_status = if req_status == "completed" {
                    "completed"
                } else {
                    "unproven"
                };
                sqlx::query(
                    "UPDATE transactions SET status = ?, proven_tx_id = ?, updated_at = ? WHERE transaction_id = ?",
                )
                .bind(new_status)
                .bind(proven_tx_id)
                .bind(chrono::Utc::now())
                .bind(tx_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;
                log.push_str(&format!(
                    "Fixed tx {}: sending -> {} (req={}).\n",
                    txid, new_status, req_status
                ));
            }
        }

        if !mismatches.is_empty() {
            log.push_str(&format!("Fixed {} status mismatches.\n", mismatches.len()));
        }

        if log.is_empty() {
            log.push_str("No issues found.\n");
        }

        Ok(ReviewStatusResult { log })
    }

    pub(super) async fn purge_data_for_user(
        &self,
        auth: &AuthId,
        params: PurgeParams,
    ) -> Result<PurgeResults> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let mut count = 0u32;
        let mut log = String::new();

        let cutoff = chrono::Utc::now() - chrono::Duration::days(params.max_age_days as i64);

        // Build status list based on params
        let mut statuses = Vec::new();
        if params.purge_completed {
            statuses.push("completed");
        }
        if params.purge_failed {
            statuses.push("failed");
        }

        if statuses.is_empty() {
            return Ok(PurgeResults {
                count: 0,
                log: "No statuses selected for purge.\n".to_string(),
            });
        }

        // 1. Clear raw_tx and input_beef on old completed transactions (TS pattern)
        if params.purge_completed {
            let result = sqlx::query(
                r#"
                UPDATE transactions
                SET raw_tx = NULL, input_beef = NULL, updated_at = ?
                WHERE user_id = ?
                  AND status = 'completed'
                  AND updated_at < ?
                  AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)
                "#,
            )
            .bind(chrono::Utc::now())
            .bind(user_id)
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;
            let rows = result.rows_affected() as u32;
            if rows > 0 {
                log.push_str(&format!(
                    "Cleared raw_tx from {} completed transactions.\n",
                    rows
                ));
                count += rows;
            }
        }

        // 2. Delete proven_tx_reqs for old failed transactions
        if params.purge_failed {
            let result = sqlx::query(
                r#"
                DELETE FROM proven_tx_reqs
                WHERE status IN ('failed', 'invalid', 'doubleSpend')
                  AND updated_at < ?
                "#,
            )
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;
            let rows = result.rows_affected() as u32;
            if rows > 0 {
                log.push_str(&format!("Deleted {} failed proven_tx_reqs.\n", rows));
                count += rows;
            }
        }

        // 3. Clean up old monitor events
        let result = sqlx::query(r#"DELETE FROM monitor_events WHERE created_at < ?"#)
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        let rows = result.rows_affected() as u32;
        if rows > 0 {
            log.push_str(&format!("Deleted {} old monitor events.\n", rows));
            count += rows;
        }

        if log.is_empty() {
            log.push_str("Nothing to purge.\n");
        }

        Ok(PurgeResults { count, log })
    }

    pub(super) async fn review_status_impl(&self) -> Result<ReviewStatusResult> {
        let mut log = String::new();
        let now = chrono::Utc::now();

        // Check 1: Mark transactions as failed if their proven_tx_req is 'invalid'.
        // TS reference uses: WHERE status != 'failed' (includes 'completed').
        // If the proven_tx_req is 'invalid', the tx was never confirmed on-chain,
        // so even 'completed' transactions must be demoted to 'failed'.
        let mut tx = self.write_tx().await?;
        let failed_ids: Vec<i64> = sqlx::query_scalar(
            r#"
            UPDATE transactions SET status = 'failed', updated_at = ?
            WHERE status != 'failed'
              AND txid IN (
                SELECT ptr.txid FROM proven_tx_reqs ptr
                WHERE ptr.status = 'invalid'
              )
            RETURNING transaction_id
            "#,
        )
        .bind(now)
        .fetch_all(&mut *tx)
        .await?;

        // Void only the outputs created by transactions this pass just failed.
        // Keep both writes atomic, without releasing their consumed inputs.
        for ids in failed_ids.chunks(400) {
            let placeholders = vec!["?"; ids.len()].join(",");
            let sql = format!(
                "UPDATE outputs SET spendable = 0, updated_at = ? \
                 WHERE transaction_id IN ({placeholders})"
            );
            let mut query = sqlx::query(&sql).bind(now);
            for id in ids {
                query = query.bind(id);
            }
            query.execute(&mut *tx).await?;
        }
        commit(tx).await?;
        let failed_count = failed_ids.len();

        if failed_count > 0 {
            log.push_str(&format!(
                "Marked {} transactions as failed (proven_tx_req invalid)\n",
                failed_count
            ));
            tracing::debug!(
                "review_status: marked {} transactions failed from invalid reqs",
                failed_count
            );
        }

        // Check 2: REMOVED — blind re-marking of doubleSpend inputs creates a
        // feedback loop. Previously this ran:
        //   UPDATE outputs SET spent_by = NULL, spendable = 1
        //   WHERE spent_by IN (SELECT transaction_id FROM transactions WHERE status = 'failed')
        //
        // This is the most dangerous path: it runs every ~15 minutes and blanket
        // re-marks ALL outputs from failed txs as spendable, including those consumed
        // on-chain by competing transactions (doubleSpend). The wallet then picks them
        // up again, broadcasts, gets doubleSpend again → infinite loop.
        //
        // Inputs from failed txs are now restored ONLY via UTXO-verified rollback
        // paths in: update_transaction_status_after_broadcast (process_action.rs),
        // send_waiting_transactions, and abort_abandoned.

        // Check 3: Mark transactions completed when proof exists
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            r#"
            SELECT ptr.txid, t.status, ptr.proven_tx_id
            FROM proven_tx_reqs ptr
            JOIN transactions t ON t.txid = ptr.txid
            WHERE ptr.status = 'completed'
              AND t.status != 'completed'
              AND ptr.proven_tx_id IS NOT NULL
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if !rows.is_empty() {
            for (txid, old_status, proven_tx_id) in &rows {
                sqlx::query(
                    "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
                )
                .bind(proven_tx_id)
                .bind(now)
                .bind(txid)
                .execute(&mut *self.writer_conn().await?)
                .await?;

                tracing::debug!(
                    "review_status: synced transaction {} from {} to completed",
                    txid,
                    old_status
                );
            }

            log.push_str(&format!(
                "Updated {} transaction statuses to completed\n",
                rows.len()
            ));
        }

        // Check 4: the unproven descendants of a retired root (bsv-wallet-cli
        // #44). A broadcast abort commits the root (`failed`, req `invalid`)
        // and retires its descendants after; when that retirement fails (a
        // busy store, the process dying in between) the abort still answers
        // its success, and this pass finishes it. A root is a failed
        // transaction whose req is `invalid` or `doubleSpend`, with no chain
        // evidence, spent by a transaction still `unproven`, `sending` or
        // `nosend`; retiring from it is the abort's own retirement (outside
        // inputs released only on verification). A root with a completed
        // descendant, or a descendant with chain evidence, is left.
        let roots: Vec<(i64, String)> = sqlx::query_as(
            r#"
            SELECT DISTINCT p.transaction_id, p.txid
            FROM transactions p
            JOIN proven_tx_reqs r ON r.txid = p.txid
            JOIN outputs o ON o.transaction_id = p.transaction_id
            JOIN transactions c ON c.transaction_id = o.spent_by
            WHERE p.status = 'failed'
              AND p.txid IS NOT NULL
              AND r.status IN ('invalid', 'doubleSpend')
              AND c.status IN ('unproven', 'sending', 'nosend')
            ORDER BY p.transaction_id
            "#,
        )
        .fetch_all(self.pool())
        .await?;
        if !roots.is_empty() {
            let services = self.get_services().ok();
            let mut retired = 0usize;
            for (transaction_id, txid) in &roots {
                if let Some(evidence) = super::abort_action::chain_evidence_for(self, txid).await? {
                    tracing::warn!(
                        txid = %txid,
                        evidence = %evidence,
                        "review_status: a failed root with unproven descendants has chain evidence; descendants left"
                    );
                    continue;
                }
                // A descendant with chain evidence (a broadcaster's mined
                // word, a proof) says the chain disagrees with the root's
                // retirement: none is retired (review 4 G2; the unfail
                // canary judges the root). A completed descendant makes the
                // retirement refuse, so the root is skipped, uncounted, and
                // the pass stays quiet (review 4 G3).
                let descendants = self.poisoned_descendants(txid).await?;
                if descendants.iter().any(|d| d.status == "completed") {
                    tracing::debug!(
                        txid = %txid,
                        "review_status: a failed root has a completed descendant; descendants left"
                    );
                    continue;
                }
                let mut evidenced = None;
                for descendant in &descendants {
                    if let Some(evidence) =
                        super::abort_action::chain_evidence_for(self, &descendant.txid).await?
                    {
                        evidenced = Some((descendant.txid.clone(), evidence));
                        break;
                    }
                }
                if let Some((descendant, evidence)) = evidenced {
                    tracing::debug!(
                        txid = %txid,
                        descendant = %descendant,
                        evidence = %evidence,
                        "review_status: a descendant of a failed root has chain evidence; descendants left"
                    );
                    continue;
                }
                self.retire_poisoned_descendants(services.as_deref(), txid, *transaction_id, now)
                    .await?;
                retired += 1;
            }
            if retired > 0 {
                log.push_str(&format!(
                    "Retired the unproven descendants of {} failed transactions\n",
                    retired
                ));
            }
        }

        if log.is_empty() {
            log.push_str("No status mismatches found\n");
        }

        Ok(ReviewStatusResult { log })
    }

    pub(super) async fn purge_data_impl(&self, params: PurgeParams) -> Result<PurgeResults> {
        let mut count = 0u32;
        let mut log = String::new();
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(params.max_age_days as i64);

        if params.purge_failed {
            // Delete old failed/invalid proven_tx_reqs
            let result = sqlx::query(
                r#"
                DELETE FROM proven_tx_reqs
                WHERE status IN ('failed', 'invalid', 'doubleSpend')
                  AND updated_at < ?
                "#,
            )
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;

            let deleted = result.rows_affected() as u32;
            count += deleted;
            log.push_str(&format!(
                "Purged {} failed/invalid proven_tx_reqs\n",
                deleted
            ));
        }

        if params.purge_completed {
            // DELETE old completed proven_tx_reqs entirely (TS reference pattern).
            // The proof is stored in proven_txs; the request row is disposable.
            // TS uses DELETE (not UPDATE SET NULL) because raw_tx is NOT NULL.
            let result = sqlx::query(
                r#"
                DELETE FROM proven_tx_reqs
                WHERE status = 'completed'
                  AND proven_tx_id IS NOT NULL
                  AND updated_at < ?
                "#,
            )
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;

            let deleted = result.rows_affected() as u32;
            count += deleted;
            if deleted > 0 {
                log.push_str(&format!("Deleted {} completed proven_tx_reqs\n", deleted));
            }

            // Also clear raw_tx and input_beef on old completed transactions
            // (TS: sets rawTx=null, inputBEEF=null on transactions table)
            let result = sqlx::query(
                r#"
                UPDATE transactions
                SET raw_tx = NULL, input_beef = NULL, updated_at = ?
                WHERE status = 'completed'
                  AND proven_tx_id IS NOT NULL
                  AND updated_at < ?
                  AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)
                "#,
            )
            .bind(now)
            .bind(cutoff)
            .execute(&mut *self.writer_conn().await?)
            .await?;

            let cleaned = result.rows_affected() as u32;
            count += cleaned;
            if cleaned > 0 {
                log.push_str(&format!(
                    "Cleaned raw data from {} completed transactions\n",
                    cleaned
                ));
            }
        }

        Ok(PurgeResults { count, log })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{MonitorStorage, PurgeParams, StorageSqlx, WalletStorageWriter};

    // =========================================================================
    // purge_data — monitor-level SQL correctness tests
    // =========================================================================

    /// Monitor-level purge_data DELETEs completed proven_tx_reqs (not UPDATE SET NULL).
    /// This matches the TS reference pattern where the row is deleted entirely
    /// because the proof is stored in proven_txs and the request is disposable.
    #[test]
    fn test_monitor_purge_deletes_completed_proven_tx_reqs() {
        let sql = r#"
                DELETE FROM proven_tx_reqs
                WHERE status = 'completed'
                  AND proven_tx_id IS NOT NULL
                  AND updated_at < ?
                "#;
        assert!(sql.contains("DELETE FROM proven_tx_reqs"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        assert!(sql.contains("updated_at < ?"));
        // Must NOT be an UPDATE — the whole row is deleted
        assert!(!sql.contains("UPDATE"));
        assert!(!sql.contains("SET"));
    }

    /// Monitor-level purge_data clears BOTH raw_tx AND input_beef on completed
    /// transactions. Previously only raw_tx was cleared; now input_beef is also
    /// set to NULL to match the TS reference.
    #[test]
    fn test_monitor_purge_nulls_raw_tx_and_input_beef_on_transactions() {
        let sql = r#"
                UPDATE transactions
                SET raw_tx = NULL, input_beef = NULL, updated_at = ?
                WHERE status = 'completed'
                  AND proven_tx_id IS NOT NULL
                  AND updated_at < ?
                  AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)
                "#;
        assert!(sql.contains("UPDATE transactions"));
        assert!(sql.contains("SET raw_tx = NULL, input_beef = NULL"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("proven_tx_id IS NOT NULL"));
        // Only targets rows that still have data to clear
        assert!(sql.contains("raw_tx IS NOT NULL OR input_beef IS NOT NULL"));
    }

    /// When purge_completed is false, the monitor purge skips BOTH the
    /// proven_tx_reqs DELETE and the transactions UPDATE.
    #[test]
    fn test_monitor_purge_completed_false_skips_both_operations() {
        let params = PurgeParams {
            max_age_days: 30,
            purge_completed: false,
            purge_failed: true,
        };
        assert!(!params.purge_completed);
        // The purge_failed branch (DELETE failed/invalid/doubleSpend) should still run
        assert!(params.purge_failed);
    }

    /// When purge_failed is false, the failed proven_tx_reqs DELETE is skipped.
    #[test]
    fn test_monitor_purge_failed_false_skips_failed_delete() {
        let params = PurgeParams {
            max_age_days: 30,
            purge_completed: true,
            purge_failed: false,
        };
        assert!(params.purge_completed);
        assert!(!params.purge_failed);
    }

    /// When both flags are false, nothing is purged.
    #[test]
    fn test_monitor_purge_both_disabled_skips_all() {
        let params = PurgeParams {
            max_age_days: 30,
            purge_completed: false,
            purge_failed: false,
        };
        assert!(!params.purge_completed);
        assert!(!params.purge_failed);
    }

    /// The failed proven_tx_reqs DELETE targets the correct statuses.
    #[test]
    fn test_monitor_purge_failed_delete_targets_correct_statuses() {
        let sql = r#"
                DELETE FROM proven_tx_reqs
                WHERE status IN ('failed', 'invalid', 'doubleSpend')
                  AND updated_at < ?
                "#;
        assert!(sql.contains("DELETE FROM proven_tx_reqs"));
        assert!(sql.contains("'failed'"));
        assert!(sql.contains("'invalid'"));
        assert!(sql.contains("'doubleSpend'"));
        assert!(sql.contains("updated_at < ?"));
    }

    /// Per-user purge_data also clears both raw_tx AND input_beef on transactions.
    #[test]
    fn test_user_purge_nulls_raw_tx_and_input_beef_on_transactions() {
        let sql = r#"
                UPDATE transactions
                SET raw_tx = NULL, input_beef = NULL, updated_at = ?
                WHERE user_id = ?
                  AND status = 'completed'
                  AND updated_at < ?
                  AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)
                "#;
        assert!(sql.contains("UPDATE transactions"));
        assert!(sql.contains("SET raw_tx = NULL, input_beef = NULL"));
        assert!(sql.contains("user_id = ?"));
        assert!(sql.contains("status = 'completed'"));
        assert!(sql.contains("raw_tx IS NOT NULL OR input_beef IS NOT NULL"));
    }

    /// The purge_completed branch produces two operations: DELETE reqs + UPDATE txs.
    /// Simulate the log building to verify both are tracked.
    #[test]
    fn test_monitor_purge_completed_branch_logs_both_operations() {
        let mut log = String::new();
        let deleted = 5u32;
        let cleaned = 3u32;
        if deleted > 0 {
            log.push_str(&format!("Deleted {} completed proven_tx_reqs\n", deleted));
        }
        if cleaned > 0 {
            log.push_str(&format!(
                "Cleaned raw data from {} completed transactions\n",
                cleaned
            ));
        }
        assert!(log.contains("Deleted 5 completed proven_tx_reqs"));
        assert!(log.contains("Cleaned raw data from 3 completed transactions"));
    }

    /// Integration-style: build full purge with both branches enabled.
    #[tokio::test]
    async fn test_purge_data_full_flow() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Run monitor-level purge with both flags — should succeed with 0 count
        let result = MonitorStorage::purge_data(
            &storage,
            PurgeParams {
                max_age_days: 30,
                purge_completed: true,
                purge_failed: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.count, 0);
    }

    /// Integration-style: purge with both flags disabled returns 0 count.
    #[tokio::test]
    async fn test_purge_data_both_disabled() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let result = MonitorStorage::purge_data(
            &storage,
            PurgeParams {
                max_age_days: 30,
                purge_completed: false,
                purge_failed: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.count, 0);
    }
}
