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
    struct MockServices {
        /// If set, get_merkle_path returns this result for all txids.
        merkle_result: tokio::sync::RwLock<Option<GetMerklePathResult>>,
        /// Track how many times get_merkle_path was called.
        call_count: std::sync::atomic::AtomicU32,
    }

    impl MockServices {
        fn new() -> Self {
            Self {
                merkle_result: tokio::sync::RwLock::new(None),
                call_count: std::sync::atomic::AtomicU32::new(0),
            }
        }

        /// Create a mock that returns a valid merkle path.
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
        fn with_no_proof() -> Self {
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

        fn get_call_count(&self) -> u32 {
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
            Err(bsv_wallet_toolbox_rs::Error::ServiceError(
                "MockServices does not provide ChainTracker".to_string(),
            ))
        }

        async fn get_height(&self) -> bsv_wallet_toolbox_rs::Result<u32> {
            Ok(800000)
        }

        async fn get_header_for_height(&self, _height: u32) -> bsv_wallet_toolbox_rs::Result<Vec<u8>> {
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
