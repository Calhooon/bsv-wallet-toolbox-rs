// =============================================================================
// MonitorStorage Implementation
// =============================================================================

use super::{
    async_trait, parse_input_txids, prune_beef_to_roots, Beef, Duration, HeaderTrackerState,
    MerklePath, MonitorStorage, ProofIngestOutcome, ProvenTxAnchor, ProvenTxReqStatus, PurgeParams,
    PurgeResults, Result, ReviewStatusResult, Row, StorageProcessActionResults, StorageSqlx,
    TxSynchronizedStatus, Utc,
};

#[async_trait]
impl MonitorStorage for StorageSqlx {
    async fn max_acceptable_proof_height(&self) -> Result<u32> {
        let mut conn = self.pool().acquire().await?;
        super::monitor_state::read_proof_gate_on(&mut conn).await
    }

    async fn set_max_acceptable_proof_height(&self, height: u32) -> Result<()> {
        let mut conn = self.writer_conn().await?;
        super::monitor_state::write_proof_gate_on(&mut conn, height).await
    }

    async fn load_header_tracker_state(&self) -> Result<Option<HeaderTrackerState>> {
        let mut conn = self.pool().acquire().await?;
        super::monitor_state::read_header_tracker_on(&mut conn).await
    }

    async fn save_header_tracker_state(&self, state: &HeaderTrackerState) -> Result<()> {
        let mut conn = self.writer_conn().await?;
        super::monitor_state::write_header_tracker_on(&mut conn, state).await
    }

    async fn set_proven_tx_block_hash_if_empty(
        &self,
        txid: &str,
        block_hash: &str,
    ) -> Result<bool> {
        self.set_proven_tx_block_hash_if_empty_impl(txid, block_hash)
            .await
    }

    async fn find_proven_txs_by_block_hash(&self, block_hash: &str) -> Result<Vec<ProvenTxAnchor>> {
        self.find_proven_txs_by_block_hash_impl(block_hash).await
    }

    async fn find_proven_txs_in_heights(
        &self,
        min_height: u32,
        max_height: u32,
    ) -> Result<Vec<ProvenTxAnchor>> {
        self.find_proven_txs_in_heights_impl(min_height, max_height)
            .await
    }

    async fn demote_stale_proof(&self, txid: &str) -> Result<bool> {
        self.demote_stale_proof_impl(txid).await
    }

    async fn announce_lost_proof(&self, txid: &str) -> Result<crate::storage::BroadcastOutcome> {
        self.announce_lost_proof_impl(txid).await
    }

    async fn note_lost_proof_refusal(&self, txid: &str, verdict: &str) -> Result<()> {
        self.note_lost_proof_refusal_impl(txid, verdict).await
    }

    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        // A competitor a broadcaster's word named is asked for first: its
        // checked proof is the one evidence for the double-spend word
        // (bsv-stack-lean #66). Then this wallet's own requests.
        let mut results = self.ask_queued_competitor_proofs().await?;
        results.extend(self.synchronize_own_reqs().await?);
        Ok(results)
    }

    async fn send_waiting_transactions(
        &self,
        min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        self.send_waiting_transactions_impl(min_transaction_age)
            .await
    }

    async fn abort_abandoned(&self, timeout: Duration) -> Result<()> {
        self.abort_abandoned_impl(timeout).await
    }

    async fn un_fail(&self) -> Result<()> {
        self.un_fail_impl().await
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        self.review_status_impl().await
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        self.purge_data_impl(params).await
    }

    async fn compact_input_beefs(&self) -> Result<u32> {
        use bsv_rs::transaction::{Beef, MerklePath};

        // Find completed proven_tx_reqs with non-null input_beef.
        // Process in batches of 50 to avoid holding the connection too long.
        let rows: Vec<(i64, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, input_beef, raw_tx
            FROM proven_tx_reqs
            WHERE status = 'completed'
              AND input_beef IS NOT NULL
              AND LENGTH(input_beef) > 1000
            ORDER BY LENGTH(input_beef) DESC
            LIMIT 50
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut compacted = 0u32;

        for (req_id, beef_bytes, raw_tx) in &rows {
            let mut beef = match Beef::from_binary(beef_bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let original_size = beef_bytes.len();

            // Find unproven txids in this BEEF
            let unproven_txids: Vec<String> = beef
                .txs
                .iter()
                .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
                .map(|tx| tx.txid())
                .collect();

            if unproven_txids.is_empty() {
                continue;
            }

            // Check proven_txs for available proofs (batch query)
            let mut upgraded = 0u32;

            for chunk in unproven_txids.chunks(400) {
                let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query_str = format!(
                    "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
                    placeholders
                );

                let mut query = sqlx::query(&query_str);
                for txid in chunk {
                    query = query.bind(txid);
                }

                let proof_rows: Vec<(String, Vec<u8>)> = query
                    .fetch_all(self.pool())
                    .await?
                    .into_iter()
                    .map(|row| {
                        let txid: String = sqlx::Row::get(&row, "txid");
                        let mp: Vec<u8> = sqlx::Row::get(&row, "merkle_path");
                        (txid, mp)
                    })
                    .collect();

                let mut conn = self.pool().acquire().await?;
                let tracker = self.get_chain_tracker().await;
                for (txid, merkle_path_bytes) in &proof_rows {
                    if let Ok(merkle_path) = MerklePath::from_binary(merkle_path_bytes) {
                        // P0-1: a stored proof that was never checked is
                        // checked here, once; a refuted one is demoted. The
                        // rewritten BEEF is stored, so it takes only what
                        // the ingest would store (P0-1c): with no tracker or
                        // a tracker fault the proof is not merged and the
                        // row stays unchecked for a later pass.
                        if !super::proof_root_checks::check_unchecked_stored_proof_on(
                            &mut conn,
                            Some(self.writer()),
                            tracker.as_deref(),
                            txid,
                            &merkle_path,
                        )
                        .await?
                        .may_store()
                        {
                            continue;
                        }
                        // P0-1d: linked with its own leaf flagged, so the
                        // stored link survives the next merge.
                        let Some(bump_index) =
                            super::create_action::merge_proof_for(&mut beef, merkle_path, txid)
                        else {
                            continue;
                        };
                        if let Some(tx) = beef.find_txid_mut(txid) {
                            tx.set_bump_index(Some(bump_index));
                            upgraded += 1;
                        }
                    }
                }
            }

            if upgraded == 0 {
                continue;
            }

            // Now that ancestors carry proofs, drop everything the transaction's
            // own inputs no longer reach: a BUMP terminates its branch. Rooted at
            // the inputs, so an input that is still unproven keeps its history.
            // (`Beef::trim_known_proven` is root-blind: it walks from whichever
            // transactions nothing else spends, which can drop an unproven input
            // that a proven sibling happens to descend from.)
            if let Some(raw) = raw_tx {
                if let Ok(roots) = parse_input_txids(raw) {
                    prune_beef_to_roots(&mut beef, &roots);
                }
            }

            let new_bytes = beef.to_binary();
            let new_size = new_bytes.len();

            // Only update if we actually saved space
            if new_size < original_size {
                let now = chrono::Utc::now();
                sqlx::query(
                    "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                )
                .bind(&new_bytes)
                .bind(now)
                .bind(req_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;

                compacted += 1;
                tracing::debug!(
                    req_id = req_id,
                    original_kb = original_size / 1024,
                    new_kb = new_size / 1024,
                    savings_pct = ((original_size - new_size) * 100) / original_size,
                    "Compacted input_beef"
                );
            }
        }

        Ok(compacted)
    }

    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let now = std::time::Instant::now();
        let mut locks = self.task_locks.write().await;

        // Check if there's an existing lock
        if let Some((holder, expiry)) = locks.get(task_name) {
            if *expiry > now {
                // Lock is still valid
                if holder == instance_id {
                    // We already hold it - extend the TTL
                    locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
                    return Ok(true);
                }
                // Another instance holds it
                return Ok(false);
            }
            // Lock has expired - fall through to acquire
        }

        // Acquire the lock
        locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
        Ok(true)
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        let mut locks = self.task_locks.write().await;
        if let Some((holder, _)) = locks.get(task_name) {
            if holder == instance_id {
                locks.remove(task_name);
            }
        }
        Ok(())
    }

    async fn update_proven_tx_req_status(
        &self,
        proven_tx_req_id: i64,
        new_status: ProvenTxReqStatus,
    ) -> Result<()> {
        self.update_proven_tx_req_status_impl(proven_tx_req_id, new_status)
            .await
    }

    async fn mark_transaction_seen_on_network(&self, txid: &str) -> Result<bool> {
        self.mark_transaction_seen_on_network_by(
            txid,
            crate::services::broadcast_memory::BROADCAST_PROVIDER_NETWORK,
        )
        .await
    }

    async fn mark_transaction_seen_on_network_by(
        &self,
        txid: &str,
        provider: &str,
    ) -> Result<bool> {
        // The reporting plane has the tx: remember it for reduced sends.
        self.record_broadcast_status_quiet(
            txid,
            provider,
            crate::services::broadcast_memory::BROADCAST_STATUS_SEEN,
        )
        .await;

        let now = chrono::Utc::now();

        // Same transition a successful post_beef broadcast performs in
        // send_waiting_transactions: req → 'unmined' (awaiting proof),
        // tx → 'unproven' (spendable). Only lift PRE-broadcast statuses —
        // never demote 'completed'.
        let req_result = sqlx::query(
            "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? \
             WHERE txid = ? AND status IN ('unsent', 'sending', 'unknown', 'callback', 'unconfirmed')",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        let tx_result = sqlx::query(
            "UPDATE transactions SET status = 'unproven', updated_at = ? \
             WHERE txid = ? AND status = 'sending'",
        )
        .bind(now)
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        let updated = req_result.rows_affected() > 0 || tx_result.rows_affected() > 0;
        if updated {
            tracing::info!(txid = %txid, "marked seen-on-network: proven_tx_req → unmined, transaction → unproven");
        }
        Ok(updated)
    }

    async fn mark_transaction_rejected(&self, txid: &str, double_spend: bool) -> Result<bool> {
        self.mark_transaction_rejected_impl(txid, double_spend)
            .await
    }

    async fn mark_transaction_rejected_by(
        &self,
        txid: &str,
        provider: &str,
        double_spend: bool,
    ) -> Result<bool> {
        // A broadcaster's word is a hint (bsv-stack-lean #66; its tracker
        // charter, section 2): `rejected` is the node's rules run locally
        // and `conflicted` a mined competitor's checked proof, neither of
        // which this word is. The refusing broadcaster is remembered (it
        // never skips this txid as an ancestor again), the word goes on the
        // request's history, and the transaction keeps its word: the re-ask
        // and the proof task decide. A competitor the word names is queued
        // by `queue_competitor_proof_asks`. Until 0.7.4 the refusal was
        // final when no other source held the transaction (#357).
        self.record_broadcast_status_quiet(
            txid,
            provider,
            crate::services::broadcast_memory::BROADCAST_STATUS_REJECTED,
        )
        .await;
        self.note_on_req_history(
            txid,
            serde_json::json!({
                "when": chrono::Utc::now().to_rfc3339(),
                "what": "broadcasterRefusalHint",
                "provider": provider,
                "doubleSpend": double_spend,
            }),
        )
        .await?;
        tracing::warn!(
            txid = %txid,
            provider = %provider,
            double_spend = double_spend,
            marker = "broadcaster_refusal_hint",
            "a broadcaster refused the transaction: a hint, no word written; the re-ask and the proof task decide"
        );
        Ok(false)
    }

    async fn queue_competitor_proof_asks(&self, txid: &str, competitors: &[String]) -> Result<()> {
        self.queue_competitors(txid, competitors).await.map(|_| ())
    }

    async fn ingest_push_proof(
        &self,
        txid: &str,
        merkle_path: &[u8],
        block_height: u32,
        block_hash: &str,
    ) -> Result<Option<ProofIngestOutcome>> {
        self.ingest_push_proof_impl(txid, merkle_path, block_height, block_hash)
            .await
    }
}
#[cfg(test)]
mod tests {
    use super::super::*;

    // =========================================================================
    // Task Lock Tests
    // =========================================================================

    #[tokio::test]
    async fn test_task_lock_acquire_and_release() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Acquire should succeed
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Release should succeed
        storage
            .release_task_lock("test_task", "instance_a")
            .await
            .unwrap();

        // After release, another instance can acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_contention() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Instance A acquires the lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Instance B should fail to acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!acquired);

        // Instance A can re-acquire (extend TTL)
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_expiry() {
        let storage = StorageSqlx::in_memory().await.unwrap();

        // Acquire with a very short TTL
        let ttl = Duration::from_millis(1);
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Instance B should now be able to acquire the expired lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_different_tasks() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Different tasks should have independent locks
        let acquired_a = storage
            .try_acquire_task_lock("task_one", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_a);

        let acquired_b = storage
            .try_acquire_task_lock("task_two", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_b);

        // Another instance should be blocked on task_one but not task_three
        let blocked = storage
            .try_acquire_task_lock("task_one", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!blocked);

        let ok = storage
            .try_acquire_task_lock("task_three", "instance_b", ttl)
            .await
            .unwrap();
        assert!(ok);
    }
}
