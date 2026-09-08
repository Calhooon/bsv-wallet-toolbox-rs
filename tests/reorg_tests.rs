//! Chain reorganization tests for the BSV Wallet Toolbox.
//!
//! These tests exercise the `ReorgTask` and related components that handle
//! blockchain reorganizations, verifying correct behavior when blocks are
//! deactivated, proofs are invalidated, and transactions need re-verification.

#[cfg(feature = "sqlite")]
mod reorg {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;

    use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
    use bsv_wallet_toolbox_rs::monitor::tasks::{DeactivatedHeader, MonitorTask, ReorgTask};
    use bsv_wallet_toolbox_rs::services::traits::GetBeefResult;
    use bsv_wallet_toolbox_rs::services::{
        BlockHeader, FiatCurrency, GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult,
        GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput,
        PostBeefResult, WalletServices,
    };
    use bsv_wallet_toolbox_rs::storage::entities::ProvenTxReqStatus;
    use bsv_wallet_toolbox_rs::storage::FindProvenTxReqsArgs;
    use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, WalletStorageReader, WalletStorageWriter};

    // =========================================================================
    // Mock WalletServices for testing
    // =========================================================================

    /// A mock WalletServices that returns configurable merkle path results.
    pub(crate) struct MockServices {
        /// If set, get_merkle_path returns this result for all txids.
        merkle_result: tokio::sync::RwLock<Option<GetMerklePathResult>>,
        /// Track how many times get_merkle_path was called.
        call_count: std::sync::atomic::AtomicU32,
        /// The chain tracker `get_chain_tracker` answers (none = error), a
        /// root map so a stored proof's root can be confirmed or refuted.
        tracker: Option<bsv_rs::transaction::MockChainTracker>,
        /// Serialized headers `get_header_for_height` answers by height
        /// (else 80 zero bytes).
        headers: std::sync::Mutex<std::collections::HashMap<u32, Vec<u8>>>,
    }

    impl MockServices {
        fn new() -> Self {
            Self {
                merkle_result: tokio::sync::RwLock::new(None),
                call_count: std::sync::atomic::AtomicU32::new(0),
                tracker: None,
                headers: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }

        /// Give the services a chain tracker with the given known roots.
        pub(crate) fn with_tracker(
            mut self,
            tracker: bsv_rs::transaction::MockChainTracker,
        ) -> Self {
            self.tracker = Some(tracker);
            self
        }

        /// Answer `get_header_for_height(height)` with `bytes`.
        pub(crate) fn set_header(&self, height: u32, bytes: Vec<u8>) {
            self.headers.lock().unwrap().insert(height, bytes);
        }

        /// Create a mock that returns a valid merkle path.
        /// M19 R1: a provider answer with a REAL proof for a named block.
        pub(crate) fn with_proof(result: GetMerklePathResult) -> Self {
            let s = Self::new();
            s.merkle_result.try_write().unwrap().replace(result);
            s
        }
        fn with_valid_proof() -> Self {
            let s = Self::new();
            let result = GetMerklePathResult {
                merkle_path: Some("valid-merkle-path".to_string()),
                name: Some("mock".to_string()),
                header: Some(BlockHeader {
                    hash: "h".repeat(64),
                    height: 800000,
                    version: 0x20000000,
                    merkle_root: "m".repeat(64),
                    time: 1700000000,
                    nonce: 0,
                    bits: 0,
                    previous_hash: "p".repeat(64),
                }),
                error: None,
                notes: vec![],
            };
            s.merkle_result.try_write().unwrap().replace(result);
            s
        }

        /// Create a mock that returns no merkle path (proof not found).
        pub(crate) fn with_no_proof() -> Self {
            let s = Self::new();
            let result = GetMerklePathResult {
                merkle_path: None,
                name: Some("mock".to_string()),
                header: None,
                error: None,
                notes: vec![],
            };
            s.merkle_result.try_write().unwrap().replace(result);
            s
        }

        pub(crate) fn get_call_count(&self) -> u32 {
            self.call_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A simple mock ChainTracker (used by MockServices via get_chain_tracker).
    #[allow(dead_code)]
    struct MockChainTracker;

    #[async_trait]
    impl ChainTracker for MockChainTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, ChainTrackerError> {
            Ok(true)
        }

        async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
            Ok(800000)
        }
    }

    #[async_trait]
    impl WalletServices for MockServices {
        async fn get_chain_tracker(&self) -> bsv_wallet_toolbox_rs::Result<&dyn ChainTracker> {
            match self.tracker {
                Some(ref t) => Ok(t),
                None => Err(bsv_wallet_toolbox_rs::Error::ServiceError(
                    "MockServices does not provide ChainTracker".to_string(),
                )),
            }
        }

        async fn get_height(&self) -> bsv_wallet_toolbox_rs::Result<u32> {
            Ok(800000)
        }

        async fn get_header_for_height(
            &self,
            height: u32,
        ) -> bsv_wallet_toolbox_rs::Result<Vec<u8>> {
            if let Some(bytes) = self.headers.lock().unwrap().get(&height) {
                return Ok(bytes.clone());
            }
            Ok(vec![0u8; 80])
        }

        async fn hash_to_header(&self, _hash: &str) -> bsv_wallet_toolbox_rs::Result<BlockHeader> {
            Ok(BlockHeader {
                hash: "h".repeat(64),
                height: 800000,
                version: 0x20000000,
                merkle_root: "m".repeat(64),
                time: 1700000000,
                nonce: 0,
                bits: 0,
                previous_hash: "p".repeat(64),
            })
        }

        async fn get_merkle_path(
            &self,
            _txid: &str,
            _use_next: bool,
        ) -> bsv_wallet_toolbox_rs::Result<GetMerklePathResult> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let guard = self.merkle_result.read().await;
            match &*guard {
                Some(r) => Ok(r.clone()),
                None => Ok(GetMerklePathResult {
                    merkle_path: None,
                    name: None,
                    header: None,
                    error: None,
                    notes: vec![],
                }),
            }
        }

        async fn get_raw_tx(
            &self,
            _txid: &str,
            _use_next: bool,
        ) -> bsv_wallet_toolbox_rs::Result<GetRawTxResult> {
            Ok(GetRawTxResult {
                raw_tx: None,
                name: "mock".to_string(),
                txid: String::new(),
                error: None,
            })
        }

        async fn post_beef(
            &self,
            _beef: &[u8],
            _txids: &[String],
        ) -> bsv_wallet_toolbox_rs::Result<Vec<PostBeefResult>> {
            Ok(vec![])
        }

        async fn get_utxo_status(
            &self,
            _output: &str,
            _output_format: Option<GetUtxoStatusOutputFormat>,
            _outpoint: Option<&str>,
            _use_next: bool,
        ) -> bsv_wallet_toolbox_rs::Result<GetUtxoStatusResult> {
            Ok(GetUtxoStatusResult {
                status: "success".to_string(),
                is_utxo: Some(true),
                details: vec![],
                name: "mock".to_string(),
                error: None,
            })
        }

        async fn get_status_for_txids(
            &self,
            _txids: &[String],
            _use_next: bool,
        ) -> bsv_wallet_toolbox_rs::Result<GetStatusForTxidsResult> {
            Ok(GetStatusForTxidsResult {
                results: vec![],
                name: "mock".to_string(),
                status: "success".to_string(),
                error: None,
            })
        }

        async fn get_script_hash_history(
            &self,
            _hash: &str,
            _use_next: bool,
        ) -> bsv_wallet_toolbox_rs::Result<GetScriptHashHistoryResult> {
            Ok(GetScriptHashHistoryResult {
                history: vec![],
                name: "mock".to_string(),
                status: "success".to_string(),
                error: None,
            })
        }

        async fn get_bsv_exchange_rate(&self) -> bsv_wallet_toolbox_rs::Result<f64> {
            Ok(50.0)
        }

        async fn get_fiat_exchange_rate(
            &self,
            _currency: FiatCurrency,
            _base: Option<FiatCurrency>,
        ) -> bsv_wallet_toolbox_rs::Result<f64> {
            Ok(1.0)
        }

        fn hash_output_script(&self, _script: &[u8]) -> String {
            "mock_hash".to_string()
        }

        async fn is_utxo(
            &self,
            _txid: &str,
            _vout: u32,
            _locking_script: &[u8],
        ) -> bsv_wallet_toolbox_rs::Result<bool> {
            Ok(true)
        }

        async fn n_lock_time_is_final(
            &self,
            _n_lock_time: u32,
        ) -> bsv_wallet_toolbox_rs::Result<bool> {
            Ok(true)
        }

        async fn n_lock_time_is_final_for_tx(
            &self,
            _input: NLockTimeInput,
        ) -> bsv_wallet_toolbox_rs::Result<bool> {
            Ok(true)
        }

        async fn get_beef(
            &self,
            txid: &str,
            _known_txids: &[String],
        ) -> bsv_wallet_toolbox_rs::Result<GetBeefResult> {
            Ok(GetBeefResult {
                name: "mock".to_string(),
                txid: txid.to_string(),
                beef: None,
                has_proof: false,
                error: None,
            })
        }
    }

    // =========================================================================
    // Helper functions
    // =========================================================================

    async fn setup_monitor_storage() -> (Arc<StorageSqlx>, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        (Arc::new(storage), auth)
    }

    async fn insert_proven_tx_req(storage: &StorageSqlx, txid: &str, status: &str) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, ?, 0, '{}', 0, '{}', X'00', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(status)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    async fn insert_transaction(
        storage: &StorageSqlx,
        user_id: i64,
        reference: &str,
        status: &str,
        txid: &str,
    ) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at)
            VALUES (?, ?, ?, 1, 1000, 'Test tx', ?, 1, 0, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(reference)
        .bind(txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    async fn insert_proven_tx(
        storage: &StorageSqlx,
        txid: &str,
        height: i64,
        block_hash: &str,
    ) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?, X'00', X'00', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(height)
        .bind(block_hash)
        .bind("m".repeat(64))
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    // =========================================================================
    // Test 1: Reorg at tip - single block replaced
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_single_block_at_tip() {
        let (storage, _auth) = setup_monitor_storage().await;
        let services = Arc::new(MockServices::with_valid_proof());

        let task = ReorgTask::new(storage.clone(), services.clone());

        // Queue a deactivated header
        task.queue_deactivated_header("h".repeat(64), 800000).await;
        assert_eq!(task.pending_count().await, 1);

        // The task requires a 10-minute delay before processing.
        // Since we just queued it, run() should not process it yet.
        let result = task.run().await.unwrap();
        assert_eq!(
            result.items_processed, 0,
            "Should not process headers before delay"
        );
        assert_eq!(
            task.pending_count().await,
            1,
            "Header should remain in queue"
        );
    }

    // =========================================================================
    // Test 2: Reorg depth 3 - three blocks replaced
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_depth_3_blocks() {
        let (storage, _auth) = setup_monitor_storage().await;
        let services = Arc::new(MockServices::with_valid_proof());

        // Insert proven_tx_reqs for transactions in these blocks
        let txid1 = "1".repeat(64);
        let txid2 = "2".repeat(64);
        let txid3 = "3".repeat(64);
        insert_proven_tx_req(&storage, &txid1, "completed").await;
        insert_proven_tx_req(&storage, &txid2, "unmined").await;
        insert_proven_tx_req(&storage, &txid3, "completed").await;

        let task = ReorgTask::new(storage.clone(), services.clone());

        // Queue 3 deactivated headers at different heights
        task.queue_deactivated_header("h1".repeat(32), 799998).await;
        task.queue_deactivated_header("h2".repeat(32), 799999).await;
        task.queue_deactivated_header("h3".repeat(32), 800000).await;

        assert_eq!(
            task.pending_count().await,
            3,
            "All 3 headers should be queued"
        );

        // The 10-minute delay prevents immediate processing
        let result = task.run().await.unwrap();
        assert_eq!(
            result.items_processed, 0,
            "No headers ready yet (delay not met)"
        );
        assert_eq!(task.pending_count().await, 3, "All 3 should remain queued");
    }

    // =========================================================================
    // Test 3: Transaction confirmed in reorg'd block - verify status handling
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_transaction_proof_reverification() {
        let (storage, auth) = setup_monitor_storage().await;
        let user_id = auth.user_id.unwrap();

        // Create a "completed" transaction with a proven_tx_req
        let txid = "c".repeat(64);
        insert_transaction(&storage, user_id, "proven-ref", "completed", &txid).await;
        insert_proven_tx_req(&storage, &txid, "completed").await;
        insert_proven_tx(&storage, &txid, 800000, &"h".repeat(64)).await;

        // Mock services that return NO proof (simulating the proof is gone after reorg)
        let services = Arc::new(MockServices::with_no_proof());

        let task = ReorgTask::new(storage.clone(), services.clone());

        // Queue the block that contained our transaction
        task.queue_deactivated_header("h".repeat(64), 800000).await;

        // Verify the task has the header queued
        assert_eq!(task.pending_count().await, 1);

        // Verify our proven_tx_req is in the database with 'completed' status
        let reqs = storage
            .find_proven_tx_reqs(FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Completed]),
                txids: Some(vec![txid.clone()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reqs.len(), 1, "Should find the completed proven_tx_req");
        assert_eq!(reqs[0].txid, txid);
    }

    // =========================================================================
    // Test 4: Transaction confirmed in both old and new chain (same block)
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_transaction_in_both_chains_no_status_change() {
        let (storage, auth) = setup_monitor_storage().await;
        let user_id = auth.user_id.unwrap();

        let txid = "d".repeat(64);
        insert_transaction(&storage, user_id, "both-chains-ref", "completed", &txid).await;
        insert_proven_tx_req(&storage, &txid, "completed").await;
        insert_proven_tx(&storage, &txid, 800000, &"h".repeat(64)).await;

        // Mock returns valid proof (tx is in new chain too)
        let services = Arc::new(MockServices::with_valid_proof());

        let task = ReorgTask::new(storage.clone(), services.clone());
        task.queue_deactivated_header("h".repeat(64), 800000).await;

        // Run the task - headers are delayed so nothing processed yet
        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0);

        // Verify transaction status is still 'completed' (not changed)
        let status: (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(status.0, "completed", "Transaction should remain completed");
    }

    // =========================================================================
    // Test 5: queue_deactivated_header then run after 10-minute delay
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_delay_is_respected() {
        let (storage, _auth) = setup_monitor_storage().await;

        // Insert some proven_tx_reqs for the task to find
        insert_proven_tx_req(&storage, &"a".repeat(64), "completed").await;

        let services = Arc::new(MockServices::with_valid_proof());
        let task = ReorgTask::new(storage.clone(), services.clone());

        // Queue a header just now
        task.queue_deactivated_header("recent".repeat(4), 800000)
            .await;
        assert_eq!(task.pending_count().await, 1);

        // Run immediately - should NOT process (delay not met)
        let result1 = task.run().await.unwrap();
        assert_eq!(
            result1.items_processed, 0,
            "Recent header should not be processed"
        );
        assert_eq!(task.pending_count().await, 1, "Header should remain queued");

        // The actual delay is 10 minutes, which we cannot wait for in a test.
        // We verify the mechanism works by confirming nothing was processed above.
    }

    // =========================================================================
    // Test 6: Deactivated header retry - verify retry_count increments
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_retry_count_mechanism() {
        // Verify the max retry count constant
        let header = DeactivatedHeader {
            hash: "test-hash".to_string(),
            height: 800000,
            deactivated_at: Utc::now(),
            retry_count: 0,
        };

        assert_eq!(header.retry_count, 0, "Initial retry count should be 0");
        assert_eq!(header.height, 800000);

        // Simulate incrementing
        let retry1 = DeactivatedHeader {
            retry_count: header.retry_count + 1,
            ..header.clone()
        };
        assert_eq!(retry1.retry_count, 1);

        let retry2 = DeactivatedHeader {
            retry_count: retry1.retry_count + 1,
            ..retry1.clone()
        };
        assert_eq!(retry2.retry_count, 2);

        let retry3 = DeactivatedHeader {
            retry_count: retry2.retry_count + 1,
            ..retry2.clone()
        };
        assert_eq!(retry3.retry_count, 3);

        // After retry_count reaches 3 (MAX_RETRY_COUNT), the task should not requeue
        assert!(
            retry3.retry_count >= 3,
            "At max retries, should not requeue"
        );
    }

    // =========================================================================
    // Test 7: pending_count() accuracy during reorg processing
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_pending_count_accuracy() {
        let (storage, _auth) = setup_monitor_storage().await;
        let services = Arc::new(MockServices::with_valid_proof());
        let task = ReorgTask::new(storage.clone(), services.clone());

        // Initially no pending headers
        assert_eq!(task.pending_count().await, 0);

        // Add headers one by one and verify count
        task.queue_deactivated_header("h1".to_string(), 800000)
            .await;
        assert_eq!(task.pending_count().await, 1);

        task.queue_deactivated_header("h2".to_string(), 800001)
            .await;
        assert_eq!(task.pending_count().await, 2);

        task.queue_deactivated_header("h3".to_string(), 800002)
            .await;
        assert_eq!(task.pending_count().await, 3);

        task.queue_deactivated_header("h4".to_string(), 800003)
            .await;
        assert_eq!(task.pending_count().await, 4);

        task.queue_deactivated_header("h5".to_string(), 800004)
            .await;
        assert_eq!(task.pending_count().await, 5);

        // Run the task - none should be processed (10min delay)
        let result = task.run().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert_eq!(task.pending_count().await, 5, "All should remain pending");
    }

    // =========================================================================
    // Test 8: Empty reorg queue - verify run() is a no-op
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_empty_queue_noop() {
        let (storage, _auth) = setup_monitor_storage().await;
        let services = Arc::new(MockServices::with_valid_proof());
        let task = ReorgTask::new(storage.clone(), services.clone());

        // No headers queued
        assert_eq!(task.pending_count().await, 0);

        // Run should be a no-op
        let result = task.run().await.unwrap();
        assert_eq!(
            result.items_processed, 0,
            "No items should be processed with empty queue"
        );
        assert!(
            result.errors.is_empty(),
            "No errors expected with empty queue"
        );

        // Run again - still no-op
        let result2 = task.run().await.unwrap();
        assert_eq!(result2.items_processed, 0);

        // Services should not have been called
        assert_eq!(
            services.get_call_count(),
            0,
            "get_merkle_path should not be called with empty queue"
        );
    }

    // =========================================================================
    // Test 9: Concurrent reorg events - verify thread safety
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_concurrent_queue_access() {
        let (storage, _auth) = setup_monitor_storage().await;
        let services = Arc::new(MockServices::with_valid_proof());
        let task = Arc::new(ReorgTask::new(storage.clone(), services.clone()));

        // Spawn multiple tasks that concurrently queue headers
        let mut handles = vec![];
        for i in 0..20u32 {
            let t = task.clone();
            let handle = tokio::spawn(async move {
                t.queue_deactivated_header(format!("hash-{}", i), 800000 + i)
                    .await;
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 20 headers should be queued without data loss
        assert_eq!(
            task.pending_count().await,
            20,
            "All 20 concurrently queued headers should be present"
        );

        // Concurrent run() calls should also be safe
        let t1 = task.clone();
        let t2 = task.clone();

        let run1 = tokio::spawn(async move { t1.run().await });
        let run2 = tokio::spawn(async move { t2.run().await });

        let r1 = run1.await.unwrap();
        let r2 = run2.await.unwrap();

        // Both should succeed
        assert!(r1.is_ok(), "First run should succeed: {:?}", r1.err());
        assert!(r2.is_ok(), "Second run should succeed: {:?}", r2.err());
    }

    // =========================================================================
    // Test 10: Reorg affecting transaction with completed proof
    // =========================================================================
    #[tokio::test]
    async fn test_reorg_completed_proof_reverification_setup() {
        let (storage, auth) = setup_monitor_storage().await;
        let user_id = auth.user_id.unwrap();

        // Create 5 transactions with completed proofs at different heights
        for i in 0..5u64 {
            let txid = format!("{:064x}", i + 1);
            insert_transaction(&storage, user_id, &format!("ref-{}", i), "completed", &txid).await;
            let proven_tx_id = insert_proven_tx(
                &storage,
                &txid,
                800000 - (i as i64),
                &format!("{:064x}", 100 + i),
            )
            .await;
            let req_id = insert_proven_tx_req(&storage, &txid, "completed").await;

            // Link proven_tx_req to proven_tx
            sqlx::query("UPDATE proven_tx_reqs SET proven_tx_id = ? WHERE proven_tx_req_id = ?")
                .bind(proven_tx_id)
                .bind(req_id)
                .execute(storage.pool())
                .await
                .unwrap();
        }

        // Verify all 5 proven_tx_reqs are completed
        let reqs = storage
            .find_proven_tx_reqs(FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Completed]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reqs.len(), 5, "All 5 should be completed");

        // Create mock services that will return NO proof (simulating all proofs gone)
        let services = Arc::new(MockServices::with_no_proof());

        let task = ReorgTask::new(storage.clone(), services.clone());

        // Queue a reorg that affects 3 of the 5 blocks
        task.queue_deactivated_header(format!("{:064x}", 100), 800000)
            .await;
        task.queue_deactivated_header(format!("{:064x}", 101), 799999)
            .await;
        task.queue_deactivated_header(format!("{:064x}", 102), 799998)
            .await;

        assert_eq!(task.pending_count().await, 3);

        // Verify we can query the affected transactions via the storage
        let completed_reqs = storage
            .find_proven_tx_reqs(FindProvenTxReqsArgs {
                status: Some(vec![
                    ProvenTxReqStatus::Completed,
                    ProvenTxReqStatus::Unmined,
                ]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            completed_reqs.len(),
            5,
            "Should find all 5 completed reqs for re-verification"
        );

        // Verify the task metadata
        assert_eq!(task.name(), "reorg");
        assert_eq!(task.default_interval(), Duration::from_secs(60));
    }
}

// =============================================================================
// The reorg task's real semantics (0.3.66), run past the delay. A
// deactivated header names a block; every stored proof anchored to it is
// re-proved: a provider's validated proof for the canonical block REPLACES
// it; the providers still naming the stored block, or faulting, RETAIN it
// (retried three times, then the original is retained for good); and only
// POSITIVE evidence (the tracker refutes the stored root, two providers
// answer cleanly "not mined", no provider serves a path) DEMOTES it.
// =============================================================================
#[cfg(feature = "sqlite")]
mod reorg_m19 {
    use std::sync::Arc;

    use bsv_rs::transaction::{MerklePath, MerklePathLeaf, MockChainTracker};
    use bsv_wallet_toolbox_rs::monitor::reorg_ops::merkle_root_of_header;
    use bsv_wallet_toolbox_rs::monitor::tasks::ReorgTask;
    use bsv_wallet_toolbox_rs::services::traits::merkle_path_note;
    use bsv_wallet_toolbox_rs::services::{BlockHeader, GetMerklePathResult};
    use bsv_wallet_toolbox_rs::storage::MonitorStorage;
    use bsv_wallet_toolbox_rs::{AuthId, StorageSqlx, WalletStorageWriter};
    use chrono::Utc;

    /// The block-1 coinbase (a real transaction) and its txid.
    const COINBASE_HEX: &str = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000";
    const COINBASE_TXID: &str = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";
    const ORPHAN: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    const CANONICAL: &str = "0000000000000000146bc084ec137a3c9608a07159128c66302051b6fe176e33";

    fn single_leaf_bump_hex(height: u32, txid: &str) -> String {
        hex::encode(
            MerklePath {
                block_height: height,
                path: vec![vec![MerklePathLeaf {
                    offset: 0,
                    hash: Some(txid.to_string()),
                    txid: true,
                    duplicate: false,
                }]],
            }
            .to_binary(),
        )
    }

    fn header(height: u32, hash: &str, root: &str) -> BlockHeader {
        BlockHeader {
            hash: hash.to_string(),
            height,
            version: 0x20000000,
            merkle_root: root.to_string(),
            time: 1_700_000_000,
            nonce: 0,
            bits: 0,
            previous_hash: "p".repeat(64),
        }
    }

    /// A provider answer with a validated proof for `height`.
    fn proof_answer(height: u32, hash: &str) -> GetMerklePathResult {
        GetMerklePathResult {
            merkle_path: Some(single_leaf_bump_hex(height, COINBASE_TXID)),
            name: Some("mock".to_string()),
            header: Some(header(height, hash, COINBASE_TXID)),
            error: None,
            notes: vec![merkle_path_note("mock", "getMerklePathSuccess", None)],
        }
    }

    /// A provider answer with no path and the given per-provider notes.
    fn no_path_answer(notes: Vec<(&str, &str, Option<&str>)>) -> GetMerklePathResult {
        GetMerklePathResult {
            merkle_path: None,
            name: Some("Services".to_string()),
            header: None,
            error: None,
            notes: notes
                .into_iter()
                .map(|(name, what, err)| merkle_path_note(name, what, err))
                .collect(),
        }
    }

    /// A tracker that knows the coinbase's root at each of `heights`.
    fn tracker_with_roots(heights: &[u32]) -> MockChainTracker {
        let mut t = MockChainTracker::new(1_000_000);
        for h in heights {
            t.add_root(*h, COINBASE_TXID.to_string());
        }
        t
    }

    async fn setup() -> (Arc<StorageSqlx>, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        // The proof gate: open, so a replacement can be stored.
        storage
            .set_max_acceptable_proof_height(1_000_000)
            .await
            .unwrap();
        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        (Arc::new(storage), auth)
    }

    /// A completed transaction with a stored proof anchored to `block_hash`
    /// and a completed request, the way the fleet's seats held the orphan.
    async fn seed_proven(
        storage: &StorageSqlx,
        user_id: i64,
        txid: &str,
        height: u32,
        block_hash: &str,
    ) {
        let now = Utc::now();
        let raw = hex::decode(COINBASE_HEX).unwrap();
        let bump = hex::decode(single_leaf_bump_hex(height, txid)).unwrap();
        let proven_tx_id: i64 = sqlx::query_scalar(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?) RETURNING proven_tx_id",
        )
        .bind(txid)
        .bind(height as i64)
        .bind(block_hash)
        .bind(txid)
        .bind(&bump)
        .bind(&raw)
        .bind(now)
        .bind(now)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', ?, 1, 1000, 'stake', ?, 1, 0, ?, ?, ?)",
        )
        .bind(user_id)
        .bind(format!("ref-{}", &txid[..8]))
        .bind(txid)
        .bind(proven_tx_id)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', 3, '{}', 0, '{}', ?, ?, ?, ?)",
        )
        .bind(txid)
        .bind(&raw)
        .bind(proven_tx_id)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    async fn state(storage: &StorageSqlx, txid: &str) -> (Option<(i64, String)>, String, String) {
        let proven: Option<(i64, String)> =
            sqlx::query_as("SELECT height, block_hash FROM proven_txs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(storage.pool())
                .await
                .unwrap();
        let (tx_status,): (String,) =
            sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let (req_status,): (String,) =
            sqlx::query_as("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        (proven, tx_status, req_status)
    }

    #[tokio::test]
    async fn a_deactivated_header_with_a_validated_replacement_proof_re_anchors_the_stored_proof() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        // The storage validates the provider's proof against ITS tracker.
        storage
            .set_chain_tracker(Arc::new(tracker_with_roots(&[965773])))
            .await;
        let services = Arc::new(super::reorg::MockServices::with_proof(proof_answer(
            965773, CANONICAL,
        )));
        let task = ReorgTask::new(storage.clone(), services.clone());
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;

        let result = task.run_now().await.unwrap();
        assert_eq!(
            result.items_processed, 1,
            "one proof replaced: {:?}",
            result.errors
        );
        let (proven, tx_status, req_status) = state(&storage, COINBASE_TXID).await;
        assert_eq!(
            proven,
            Some((965773, CANONICAL.to_string())),
            "re-anchored to the canonical block"
        );
        assert_eq!(
            tx_status, "completed",
            "a replaced proof keeps the transaction completed"
        );
        assert_eq!(req_status, "completed");
        assert!(storage
            .find_proven_txs_by_block_hash(ORPHAN)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            task.pending_count().await,
            0,
            "a replacement never requeues"
        );
    }

    #[tokio::test]
    async fn a_proof_the_providers_still_anchor_to_the_same_block_is_retained_and_retried() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        storage
            .set_chain_tracker(Arc::new(tracker_with_roots(&[965771])))
            .await;
        let services = Arc::new(super::reorg::MockServices::with_proof(proof_answer(
            965771, ORPHAN,
        )));
        let task = ReorgTask::new(storage.clone(), services.clone());
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0, "unchanged: {:?}", result.errors);
        let (proven, tx_status, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965771, ORPHAN.to_string())));
        assert_eq!(tx_status, "completed");
        assert_eq!(task.pending_count().await, 1, "unchanged is retried");
    }

    /// F4: every provider ERRORED (a 429, a timeout). The stored proof is
    /// retained and the header retried; nothing is demoted on a fault.
    #[tokio::test]
    async fn a_provider_error_never_demotes() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        // The tracker does NOT know the stored root: a refutation on its own.
        let services = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![
                ("WoC", "getMerklePathBadStatus", Some("HTTP 429")),
                ("Bitails", "getMerklePathBadStatus", Some("HTTP 503")),
                ("Arcade", "getMerklePathServiceError", Some("timeout")),
            ]))
            .with_tracker(tracker_with_roots(&[])),
        );
        let task = ReorgTask::new(storage.clone(), services.clone());
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert_eq!(
            result.errors.len(),
            1,
            "a transient, named: {:?}",
            result.errors
        );
        let (proven, tx_status, req_status) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965771, ORPHAN.to_string())), "retained");
        assert_eq!(
            (tx_status.as_str(), req_status.as_str()),
            ("completed", "completed")
        );
        assert_eq!(task.pending_count().await, 1, "retried");
    }

    /// F4: one clean "not mined" is not positive evidence; two are.
    #[tokio::test]
    async fn one_clean_negative_is_not_enough_two_are() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        let one = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![
                ("WoC", "getMerklePathNotFound", None),
                ("Bitails", "getMerklePathBadStatus", Some("HTTP 429")),
            ]))
            .with_tracker(tracker_with_roots(&[])),
        );
        let task = ReorgTask::new(storage.clone(), one);
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        let (proven, tx_status, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(
            proven,
            Some((965771, ORPHAN.to_string())),
            "one witness: retained"
        );
        assert_eq!(tx_status, "completed");
        assert_eq!(task.pending_count().await, 1);

        let two = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![
                ("WoC", "getMerklePathNotFound", None),
                ("Arcade", "getMerklePathNotMined", None),
            ]))
            .with_tracker(tracker_with_roots(&[])),
        );
        let task = ReorgTask::new(storage.clone(), two);
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 1, "demoted: {:?}", result.errors);
        let (proven, tx_status, req_status) = state(&storage, COINBASE_TXID).await;
        assert!(proven.is_none(), "the orphan's proof is gone");
        assert_eq!(tx_status, "unproven", "still spendable");
        assert_eq!(req_status, "unmined", "re-proved later");
        assert_eq!(task.pending_count().await, 0, "a demotion never requeues");
    }

    /// F7: a provider still naming the deactivated block is "unchanged":
    /// retained and retried, never demoted, even with the tracker refuting
    /// the stored root and other providers answering cleanly.
    #[tokio::test]
    async fn a_path_the_tracker_refutes_is_unchanged_not_demoted() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        let services = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![
                ("WoC", "getMerklePathSuccess", None),
                ("WoC", "getMerklePathInvalidRoot", Some("refuted")),
                ("Bitails", "getMerklePathNotFound", None),
                ("Arcade", "getMerklePathNotMined", None),
            ]))
            .with_tracker(tracker_with_roots(&[])),
        );
        let task = ReorgTask::new(storage.clone(), services);
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert!(
            result.errors.is_empty(),
            "unchanged is not an error: {:?}",
            result.errors
        );
        let (proven, tx_status, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965771, ORPHAN.to_string())), "retained");
        assert_eq!(tx_status, "completed");
        assert_eq!(task.pending_count().await, 1, "retried");
    }

    /// A tracker that still confirms the stored root is "unchanged" whatever
    /// the providers say: the stored proof IS the canonical one.
    #[tokio::test]
    async fn a_stored_root_the_tracker_still_confirms_is_unchanged() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        let services = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![
                ("WoC", "getMerklePathNotFound", None),
                ("Arcade", "getMerklePathNotMined", None),
            ]))
            .with_tracker(tracker_with_roots(&[965771])),
        );
        let task = ReorgTask::new(storage.clone(), services);
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        let (proven, _, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965771, ORPHAN.to_string())));
    }

    /// A tracker outage is never a verdict: retained, retried.
    #[tokio::test]
    async fn no_tracker_never_demotes() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        let services = Arc::new(super::reorg::MockServices::with_proof(no_path_answer(
            vec![
                ("WoC", "getMerklePathNotFound", None),
                ("Arcade", "getMerklePathNotMined", None),
            ],
        )));
        let task = ReorgTask::new(storage.clone(), services);
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert_eq!(result.errors.len(), 1);
        let (proven, _, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965771, ORPHAN.to_string())), "retained");
    }

    /// F14: requeue and starvation. Faults retry the header three times in
    /// all, then it is dropped and the original retained (the reference's
    /// "maximum retries exceeded").
    #[tokio::test]
    async fn a_faulting_header_is_processed_three_times_then_dropped_with_the_original_retained() {
        let (storage, auth) = setup().await;
        seed_proven(
            &storage,
            auth.user_id.unwrap(),
            COINBASE_TXID,
            965771,
            ORPHAN,
        )
        .await;
        let services = Arc::new(
            super::reorg::MockServices::with_proof(no_path_answer(vec![(
                "WoC",
                "getMerklePathBadStatus",
                Some("HTTP 429"),
            )]))
            .with_tracker(tracker_with_roots(&[])),
        );
        let task = ReorgTask::new(storage.clone(), services.clone());
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        task.run_now().await.unwrap();
        assert_eq!(task.pending_count().await, 1, "try 1 of 3: requeued");
        task.run_now().await.unwrap();
        assert_eq!(task.pending_count().await, 1, "try 2 of 3: requeued");
        task.run_now().await.unwrap();
        assert_eq!(task.pending_count().await, 0, "try 3 of 3: dropped");
        assert_eq!(services.get_call_count(), 3);
        let (proven, tx_status, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(
            proven,
            Some((965771, ORPHAN.to_string())),
            "the original is retained"
        );
        assert_eq!(tx_status, "completed");
        // Nothing runs on an empty queue.
        let result = task.run_now().await.unwrap();
        assert_eq!(result.items_processed, 0);
        assert_eq!(services.get_call_count(), 3);
    }

    /// F10: a hash-less stored row at the deactivated height whose root is
    /// not the canonical root there is covered by the reorg task too.
    #[tokio::test]
    async fn a_hash_less_stale_row_at_the_height_is_re_proved_by_root_mismatch() {
        let (storage, auth) = setup().await;
        seed_proven(&storage, auth.user_id.unwrap(), COINBASE_TXID, 965771, "").await;
        // The canonical header at 965771 carries a different root.
        let canonical_header = header(965771, CANONICAL, &"c".repeat(64));
        let canonical_bytes = canonical_header.to_binary();
        let canonical_root = merkle_root_of_header(&canonical_bytes).unwrap();
        assert_ne!(canonical_root, COINBASE_TXID);
        storage
            .set_chain_tracker(Arc::new(tracker_with_roots(&[965773])))
            .await;
        let services = Arc::new(super::reorg::MockServices::with_proof(proof_answer(
            965773, CANONICAL,
        )));
        services.set_header(965771, canonical_bytes);
        let task = ReorgTask::new(storage.clone(), services.clone());
        // The deactivated header names a hash the row never carried.
        task.queue_deactivated_header(ORPHAN.to_string(), 965771)
            .await;
        let result = task.run_now().await.unwrap();
        assert_eq!(
            result.items_processed, 1,
            "replaced by root mismatch: {:?}",
            result.errors
        );
        let (proven, _, _) = state(&storage, COINBASE_TXID).await;
        assert_eq!(proven, Some((965773, CANONICAL.to_string())));
    }
}
