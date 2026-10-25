// =============================================================================
// MonitorStorage Implementation
// =============================================================================

use super::{
    async_trait, Duration, HeaderTrackerState, MonitorStorage, ProofIngestOutcome, ProvenTxAnchor,
    ProvenTxReqStatus, PurgeParams, PurgeResults, Result, ReviewStatusResult,
    StorageProcessActionResults, StorageSqlx, TxSynchronizedStatus,
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
        self.compact_input_beefs_impl().await
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
    use super::super::{Duration, MonitorStorage, StorageSqlx};

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
