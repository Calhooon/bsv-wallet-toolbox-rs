use super::{
    parse_input_txids, rebuild_beef_for_broadcast, validate_beef_for_broadcast, Beef, Duration,
    Error, FindProvenTxReqsArgs, ProvenTxReqStatus, Result, SendWithResult,
    StorageProcessActionResults, StorageSqlx, TableProvenTxReq, Utc, WalletStorageReader,
};

/// How many notes a request's history keeps, the newest.
const REQ_HISTORY_NOTES_KEPT: usize = 32;

/// Is `req` due for a re-ask by `send_waiting_transactions` at `now`?
/// (`services::cadence::send_waiting_reask_minutes`; bsv-stack-lean #65.)
fn send_waiting_reask_due(req: &TableProvenTxReq, now: chrono::DateTime<chrono::Utc>) -> bool {
    let attempts = u32::try_from(req.attempts).unwrap_or(0);
    let pause = crate::services::cadence::send_waiting_reask_minutes(attempts);
    req.updated_at + chrono::Duration::minutes(pause) <= now
}

/// Each provider's word on one post, as a history note records it.
fn post_beef_words(results: &[crate::services::PostBeefResult]) -> serde_json::Value {
    serde_json::Value::Array(
        results
            .iter()
            .map(|r| {
                serde_json::json!({
                    "provider": r.name,
                    "status": r.status,
                    "error": r.error,
                    "txids": r.txid_results.iter().map(|tr| serde_json::json!({
                        "status": tr.status,
                        "data": tr.data,
                        "serviceError": tr.service_error,
                        "orphanMempool": tr.orphan_mempool,
                        "doubleSpend": tr.double_spend,
                        "competingTxs": tr.competing_txs,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

/// `history` (a request's `{"notes": [...]}`) with `note` appended and only
/// the newest [`REQ_HISTORY_NOTES_KEPT`] notes kept. A history that is not
/// a JSON object is kept whole under `previous`.
pub(super) fn append_req_history_note(history: &str, note: serde_json::Value) -> String {
    let mut object = match serde_json::from_str::<serde_json::Value>(history) {
        Ok(serde_json::Value::Object(map)) => map,
        _ if history.trim().is_empty() => serde_json::Map::new(),
        _ => {
            let mut map = serde_json::Map::new();
            map.insert("previous".to_string(), serde_json::Value::from(history));
            map
        }
    };
    let mut notes = match object.remove("notes") {
        Some(serde_json::Value::Array(notes)) => notes,
        _ => Vec::new(),
    };
    notes.push(note);
    let excess = notes.len().saturating_sub(REQ_HISTORY_NOTES_KEPT);
    notes.drain(..excess);
    object.insert("notes".to_string(), serde_json::Value::Array(notes));
    serde_json::Value::Object(object).to_string()
}

impl StorageSqlx {
    /// A post drew no accepting word, or never reached a broadcaster: the
    /// request stays `unsent`, `attempts` paces the next re-ask on the
    /// cadence and nothing else, and the words are recorded on its history.
    /// The transaction's word, its inputs and its outputs do not move
    /// (bsv-stack-lean #65; `docs/charters/tracker.md` section 2 there).
    async fn record_send_waiting_hint(
        &self,
        req: &TableProvenTxReq,
        attempts: i32,
        words: serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let next = crate::services::cadence::send_waiting_reask_minutes(
            u32::try_from(attempts).unwrap_or(0),
        );
        let history = append_req_history_note(
            &req.history,
            serde_json::json!({
                "when": now.to_rfc3339(),
                "what": "sendWaitingHint",
                "attempts": attempts,
                "words": words,
                "nextReaskMinutes": next,
            }),
        );
        sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent', attempts = ?, history = ?, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(i64::from(attempts))
            .bind(history)
            .bind(now)
            .bind(req.proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        Ok(())
    }

    /// The immediate post of `createAction`, `signAction` or
    /// `internalizeAction` drew no accepting word (a refusal, a double-spend
    /// word, a transient word, or no broadcaster reached): the same hint as
    /// [`Self::record_send_waiting_hint`]. The request is left `unsent` with
    /// `attempts` counted, the word (`outcome`, `words`) recorded on its
    /// history, the next re-ask on the cadence, and a competitor the word
    /// names queued for a proof ask. The transaction's word, its inputs and
    /// its outputs do not move (bsv-stack-lean #66). A request already past
    /// the post (`unmined`, `completed`, a host's retire) is not touched.
    pub(in crate::storage::sqlx) async fn record_immediate_broadcast_hint(
        &self,
        txid: &str,
        outcome: &str,
        words: serde_json::Value,
        competitors: &[String],
    ) -> Result<()> {
        let row: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT proven_tx_req_id, attempts, history FROM proven_tx_reqs \
             WHERE txid = ? AND status IN ('unsent', 'sending', 'unprocessed')",
        )
        .bind(txid)
        .fetch_optional(self.pool())
        .await?;
        let Some((proven_tx_req_id, attempts, history)) = row else {
            return Ok(());
        };
        let attempts = attempts.saturating_add(1);
        let now = chrono::Utc::now();
        let next = crate::services::cadence::send_waiting_reask_minutes(
            u32::try_from(attempts).unwrap_or(u32::MAX),
        );
        let history = append_req_history_note(
            &history,
            serde_json::json!({
                "when": now.to_rfc3339(),
                "what": "immediateBroadcastHint",
                "outcome": outcome,
                "attempts": attempts,
                "words": words,
                "nextReaskMinutes": next,
            }),
        );
        sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent', attempts = ?, history = ?, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(attempts)
            .bind(history)
            .bind(now)
            .bind(proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        self.queue_competitors(txid, competitors).await?;
        Ok(())
    }

    pub(super) async fn send_waiting_transactions_impl(
        &self,
        min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        // Query proven_tx_reqs with unsent/sending status
        let statuses = vec![ProvenTxReqStatus::Unsent, ProvenTxReqStatus::Sending];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = self.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(None);
        }

        // Filter by min_transaction_age
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(min_transaction_age).unwrap_or_default();
        let reqs: Vec<_> = reqs.into_iter().filter(|r| r.created_at < cutoff).collect();

        // The re-ask cadence (`services::cadence::send_waiting_reask_minutes`,
        // bsv-stack-lean #65): a request is posted again only once the pause
        // since its last attempt has passed. A transient word or a fault
        // schedules the next re-ask; no count of them retires anything.
        let now = chrono::Utc::now();
        let reqs: Vec<_> = reqs
            .into_iter()
            .filter(|r| send_waiting_reask_due(r, now))
            .collect();

        if reqs.is_empty() {
            return Ok(None);
        }

        let services = self.get_services()?;

        tracing::debug!(
            "send_waiting_transactions: broadcasting {} transactions",
            reqs.len()
        );

        let mut send_with_results = Vec::new();

        for req in &reqs {
            // Get raw_tx for this proven_tx_req
            let raw_tx = match &req.raw_tx {
                Some(tx) if !tx.is_empty() => tx.clone(),
                _ => {
                    // Try to get from transaction table
                    let row: Option<(Vec<u8>,)> = sqlx::query_as(
                        "SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL",
                    )
                    .bind(&req.txid)
                    .fetch_optional(self.pool())
                    .await?;
                    match row {
                        Some((tx,)) => tx,
                        None => {
                            tracing::warn!("No raw_tx found for txid {}", req.txid);
                            continue;
                        }
                    }
                }
            };

            // Rebuild BEEF from current DB state at broadcast time.
            // This matches Go's GetBEEFForTxIDs approach: instead of just upgrading
            // proofs on txs already in the stored BEEF (compact_stored_beef), we walk
            // the full ancestor chain from scratch. This picks up:
            //   - Ancestors that were missing when input_beef was first built
            //   - Merkle proofs that arrived after create_action
            //   - Transactions internalized from other actions
            let beef_bytes = {
                // Extract input txids from the raw transaction
                let input_txids = match parse_input_txids(&raw_tx) {
                    Ok(txids) => txids,
                    Err(e) => {
                        tracing::warn!(
                            "send_waiting: failed to parse inputs from raw_tx for {}: {e}",
                            req.txid
                        );
                        continue;
                    }
                };

                if input_txids.is_empty() {
                    // Coinbase or no inputs — cannot build BEEF
                    tracing::warn!(
                        "No input txids parsed for txid {} — skipping broadcast",
                        req.txid
                    );
                    continue;
                }

                // Rebuild BEEF from DB state
                let mut conn = self.pool().acquire().await?;
                let mut beef = match rebuild_beef_for_broadcast(
                    &mut conn,
                    self.writer(),
                    &input_txids,
                    Some(self),
                )
                .await
                {
                    Ok(b) => b,
                    // A failed ancestor: the stored inputBEEF carries the
                    // same dead lineage, so there is no fallback and no
                    // post (bsv-wallet-toolbox-rs #42 point 4).
                    Err(e @ Error::FailedAncestor { .. }) => {
                        tracing::warn!(
                            txid = %req.txid,
                            marker = "failed_ancestor_not_posted",
                            "send_waiting: {e}; not posted"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "send_waiting: rebuild_beef_for_broadcast failed for {}: {e}",
                            req.txid
                        );
                        // Fall back to stored input_beef + compact if rebuild fails
                        if let Some(ref input_beef) = req.input_beef {
                            match Beef::from_binary(input_beef) {
                                Ok(mut fallback_beef) => {
                                    let tracker = self.get_chain_tracker().await;
                                    if let Err(e2) = super::create_action::compact_stored_beef(
                                        &mut conn,
                                        Some(self.writer()),
                                        &mut fallback_beef,
                                        tracker.as_deref(),
                                    )
                                    .await
                                    {
                                        tracing::debug!(
                                                "send_waiting: compact fallback also failed for {}: {e2}",
                                                req.txid
                                            );
                                    }
                                    fallback_beef
                                }
                                Err(e2) => {
                                    tracing::warn!(
                                        "send_waiting: fallback parse also failed for {}: {e2}",
                                        req.txid
                                    );
                                    continue;
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                };

                // Merge the transaction being broadcast into the BEEF
                beef.merge_raw_tx(raw_tx.clone(), None);

                // Validate BEEF structure before broadcast (diagnostic)
                if let Err(e) = validate_beef_for_broadcast(&beef, &req.txid) {
                    tracing::warn!(
                        "send_waiting: BEEF validation warning for {}: {}",
                        req.txid,
                        e
                    );
                }

                beef.to_binary()
            };

            // Update status to sending
            sqlx::query("UPDATE proven_tx_reqs SET status = 'sending', updated_at = ? WHERE proven_tx_req_id = ?")
                .bind(now)
                .bind(req.proven_tx_req_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;

            // Broadcast via services - returns Vec<PostBeefResult> (one per provider)
            match services
                .post_beef(&beef_bytes, std::slice::from_ref(&req.txid))
                .await
            {
                Ok(results_vec) => {
                    // Check if any provider returned success
                    let success = results_vec.iter().any(|r| r.is_success());
                    if success {
                        // Update to unmined
                        sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(&mut *self.writer_conn().await?)
                            .await?;

                        sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?")
                            .bind(now)
                            .bind(&req.txid)
                            .execute(&mut *self.writer_conn().await?)
                            .await?;

                        send_with_results.push(SendWithResult {
                            txid: req.txid.clone(),
                            status: "unproven".to_string(),
                        });
                    } else {
                        // Check for orphan mempool (parent not propagated — NOT double-spend)
                        let is_orphan_mempool = results_vec
                            .iter()
                            .any(|r| r.txid_results.iter().any(|tr| tr.orphan_mempool));

                        // Check for double spend in any provider's results
                        // (exclude orphan mempool — it's not a double-spend)
                        let is_double_spend = results_vec.iter().any(|r| {
                            r.txid_results
                                .iter()
                                .any(|tr| tr.double_spend && !tr.orphan_mempool)
                        });

                        // Check for definitive rejection (invalid tx, not just service error):
                        // ARC/Arcade 4xx rejection codes and Arcade's fatal statuses
                        // (`is_definitive_rejection`), plus the historical "46x"/"invalid"
                        // status sniff. A hint since 0.7.4 (bsv-stack-lean #66).
                        let is_invalid = results_vec.iter().any(|r| {
                            r.txid_results.iter().any(|tr| {
                                crate::storage::broadcast::is_definitive_rejection(tr)
                                    || (!tr.orphan_mempool
                                        && !tr.service_error
                                        && (tr.status.contains("46")
                                            || tr.status.contains("invalid")))
                            })
                        });

                        let attempts = req.attempts + 1;

                        if is_orphan_mempool && !is_double_spend {
                            // Orphan mempool: parent tx not yet propagated.
                            // Check if the tx is actually known on-chain.
                            let reconciled =
                                super::process_action::reconcile_tx_status_via_services(
                                    &*services, &req.txid,
                                )
                                .await;

                            if reconciled {
                                // Tx is actually alive on chain — treat as success
                                sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
                                    .bind(now)
                                    .bind(req.proven_tx_req_id)
                                    .execute(&mut *self.writer_conn().await?)
                                    .await?;
                                sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?")
                                    .bind(now)
                                    .bind(&req.txid)
                                    .execute(&mut *self.writer_conn().await?)
                                    .await?;
                                tracing::info!(
                                    "send_waiting: tx {} reported as orphan mempool but found alive on chain — treating as success",
                                    req.txid
                                );
                                send_with_results.push(SendWithResult {
                                    txid: req.txid.clone(),
                                    status: "unproven".to_string(),
                                });
                                continue;
                            }

                            // THE RELEASE RULE (2026-08-29, the LOW run-A double-spend
                            // chain): an orphan-mempool verdict means a broadcaster HOLDS
                            // the bytes — they can mine the moment the parent lands. This
                            // arm used to fail the tx after 6 attempts and BLINDLY restore
                            // its inputs (`spent_by = NULL`), so the next spend of those
                            // inputs double-spent a tx still alive in a peer's orphan
                            // pool. A held tx is never failed here; it stays retryable
                            // (the parent may land any time) and abandonment belongs to a
                            // corroborated, multi-source reconcile — never to a retry
                            // counter.
                            self.record_send_waiting_hint(
                                req,
                                attempts,
                                post_beef_words(&results_vec),
                                now,
                            )
                            .await?;
                            tracing::warn!(
                                "send_waiting: tx {} orphan mempool (attempt {}): stays announced, inputs locked, re-asked on the cadence",
                                req.txid,
                                attempts
                            );
                            send_with_results.push(SendWithResult {
                                txid: req.txid.clone(),
                                status: "sending".to_string(),
                            });
                            continue;
                        }

                        if is_double_spend || is_invalid {
                            // A definitive word (a 465 and every other
                            // `is_rejection` code, Arcade's REJECTED, a
                            // double spend named or not): a hint
                            // (bsv-stack-lean #66). The status sources are
                            // read once, as before; a transaction they hold
                            // is promoted. Otherwise the word is recorded,
                            // the re-ask scheduled on the cadence and a
                            // named competitor queued for a proof ask; the
                            // transaction, its inputs and its change keep
                            // their word. Until 0.7.4 this took the release
                            // rule to `failed`.
                            if self
                                .promote_if_alive(&*services, &req.txid, req.proven_tx_req_id, now)
                                .await?
                            {
                                tracing::info!(
                                    "send_waiting: tx {} drew a definitive word but a status source holds it: treating as success",
                                    req.txid
                                );
                                send_with_results.push(SendWithResult {
                                    txid: req.txid.clone(),
                                    status: "unproven".to_string(),
                                });
                                continue;
                            }
                            self.record_send_waiting_hint(
                                req,
                                attempts,
                                post_beef_words(&results_vec),
                                now,
                            )
                            .await?;
                            let competitors: Vec<String> = results_vec
                                .iter()
                                .flat_map(|r| r.txid_results.iter())
                                .filter_map(|tr| tr.competing_txs.clone())
                                .flatten()
                                .collect();
                            self.queue_competitors(&req.txid, &competitors).await?;
                            tracing::warn!(
                                "send_waiting: tx {} drew a definitive word (double_spend={}, attempt {}): a hint; stays announced, re-asked on the cadence",
                                req.txid,
                                is_double_spend,
                                attempts
                            );
                            send_with_results.push(SendWithResult {
                                txid: req.txid.clone(),
                                status: "sending".to_string(),
                            });
                            continue;
                        } else {
                            // A transient word (a 5xx, a 429, ARC's 400 for a
                            // request it could not read): a hint. Recorded, the
                            // re-ask scheduled, and nothing else moves however
                            // many there have been (bsv-stack-lean #65).
                            self.record_send_waiting_hint(
                                req,
                                attempts,
                                post_beef_words(&results_vec),
                                now,
                            )
                            .await?;
                            tracing::warn!(
                                "send_waiting: tx {} drew no accepting word (attempt {}): stays announced, re-asked on the cadence",
                                req.txid,
                                attempts
                            );
                            send_with_results.push(SendWithResult {
                                txid: req.txid.clone(),
                                status: "sending".to_string(),
                            });
                            continue;
                        }
                    }
                }
                Err(e) => {
                    let attempts = req.attempts + 1;
                    tracing::warn!(
                        "Failed to broadcast tx {} (attempt {}): {}",
                        req.txid,
                        attempts,
                        e
                    );

                    // A fault reaching the broadcasters is no word about the
                    // transaction: recorded, the re-ask scheduled, never a
                    // retire however many there have been (bsv-stack-lean #65).
                    self.record_send_waiting_hint(
                        req,
                        attempts,
                        serde_json::json!([{ "error": e.to_string() }]),
                        now,
                    )
                    .await?;

                    send_with_results.push(SendWithResult {
                        txid: req.txid.clone(),
                        status: "sending".to_string(),
                    });
                }
            }
        }

        Ok(Some(StorageProcessActionResults {
            send_with_results: Some(send_with_results),
            not_delayed_results: None,
            log: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[tokio::test]
    async fn test_send_waiting_transactions_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no unsent transactions, should return None
        let result = storage
            .send_waiting_transactions(Duration::from_secs(0))
            .await
            .unwrap();
        assert!(result.is_none());
    }
}
