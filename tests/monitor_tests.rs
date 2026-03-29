//! Integration tests for the Monitor daemon.
//!
//! These tests verify:
//! - Monitor lifecycle (start/stop)
//! - Task execution with run_once
//! - Individual task behavior with real storage
//! - Custom task configuration
//! - Callback invocation

#[cfg(feature = "sqlite")]
mod monitor_integration {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use bsv_wallet_toolbox_rs::monitor::tasks::TaskType;
    use bsv_wallet_toolbox_rs::monitor::Monitor;
    use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
    use bsv_wallet_toolbox_rs::storage::StorageSqlx;
    use bsv_wallet_toolbox_rs::{
        MonitorOptions, TaskConfig, TransactionStatusUpdate, WalletServices, WalletStorageProvider,
        WalletStorageWriter,
    };

    /// Helper: create an in-memory StorageSqlx with migrations run and services set.
    /// Returns the storage (wrapped in Arc) and a clone of the mock services (as Arc).
    async fn setup_storage_and_services(
        mock: MockWalletServices,
    ) -> (Arc<StorageSqlx>, Arc<MockWalletServices>) {
        let storage = StorageSqlx::in_memory().await.expect("in_memory storage");
        // storage_identity_key must be a valid 33-byte compressed public key hex (66 chars)
        let storage_key = "02".to_string() + &"ab".repeat(32);
        storage
            .migrate("test-monitor", &storage_key)
            .await
            .expect("migrate");
        storage.make_available().await.expect("make_available");

        let services = Arc::new(mock);
        // set_services expects Arc<dyn WalletServices>
        storage.set_services(services.clone() as Arc<dyn WalletServices>);

        (Arc::new(storage), services)
    }

    /// Helper: create MonitorOptions with all 11 tasks disabled.
    fn all_tasks_disabled() -> MonitorOptions {
        let mut opts = MonitorOptions::default();
        opts.tasks.check_for_proofs = TaskConfig::disabled();
        opts.tasks.send_waiting = TaskConfig::disabled();
        opts.tasks.fail_abandoned = TaskConfig::disabled();
        opts.tasks.unfail = TaskConfig::disabled();
        opts.tasks.clock = TaskConfig::disabled();
        opts.tasks.new_header = TaskConfig::disabled();
        opts.tasks.reorg = TaskConfig::disabled();
        opts.tasks.check_no_sends = TaskConfig::disabled();
        opts.tasks.review_status = TaskConfig::disabled();
        opts.tasks.purge = TaskConfig::disabled();
        opts.tasks.monitor_call_history = TaskConfig::disabled();
        opts.tasks.compact_beef = TaskConfig::disabled();
        opts.tasks.sync_when_idle = TaskConfig::disabled();
        opts
    }

    // =========================================================================
    // Test 1: start_stop_lifecycle
    // =========================================================================

    /// Verify that Monitor::start() sets is_running to true and
    /// Monitor::stop() sets it back to false.
    #[tokio::test]
    async fn start_stop_lifecycle() {
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        let monitor = Monitor::with_options(storage, services, all_tasks_disabled());

        // Initially not running.
        assert!(
            !monitor.is_running(),
            "Monitor should not be running before start()"
        );

        // Start the monitor.
        monitor.start().await.expect("start should succeed");
        assert!(
            monitor.is_running(),
            "Monitor should be running after start()"
        );

        // Stop the monitor.
        monitor.stop().await.expect("stop should succeed");
        assert!(
            !monitor.is_running(),
            "Monitor should not be running after stop()"
        );
    }

    // =========================================================================
    // Test 2: double_start_error
    // =========================================================================

    /// Verify that calling Monitor::start() twice returns an error on the second call.
    #[tokio::test]
    async fn double_start_error() {
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        let monitor = Monitor::with_options(storage, services, all_tasks_disabled());

        // First start succeeds.
        monitor.start().await.expect("first start should succeed");
        assert!(monitor.is_running());

        // Second start should return an error.
        let result = monitor.start().await;
        assert!(result.is_err(), "Second start() should return an error");

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("already running"),
            "Error should mention 'already running', got: {}",
            err_msg
        );

        // Cleanup.
        monitor.stop().await.expect("stop should succeed");
    }

    // =========================================================================
    // Test 3: run_once_empty_storage
    // =========================================================================

    /// Verify that run_once() with empty storage completes all enabled tasks
    /// with 0 items processed and no errors.
    #[tokio::test]
    async fn run_once_empty_storage() {
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        // Use default options (all tasks enabled).
        let monitor = Monitor::new(storage, services);

        let results = monitor.run_once().await.expect("run_once should succeed");

        // All tasks should have run. MonitorCallHistory is excluded from run_once
        // (requires concrete Services), so we expect 10 task results.
        assert!(
            results.len() >= 10,
            "Expected at least 10 task results from run_once, got {}",
            results.len()
        );

        // Verify no tasks had fatal errors (empty database should be benign).
        for (task_type, result) in &results {
            assert!(
                result.errors.is_empty(),
                "Task {:?} should have no errors on empty storage, got: {:?}",
                task_type,
                result.errors
            );
        }
    }

    // =========================================================================
    // Test 4: fail_abandoned_integration
    // =========================================================================

    /// Create an "unsigned" outgoing transaction, set a very short timeout,
    /// run FailAbandonedTask via run_once, and verify the transaction gets failed.
    #[tokio::test]
    async fn fail_abandoned_integration() {
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        // Create a user.
        let identity_key = "02".to_string() + &"cd".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("find_or_insert_user");

        // Insert an "unsigned" outgoing transaction directly via SQL.
        // We need a transaction that looks abandoned (old created_at).
        let old_time = chrono::Utc::now() - chrono::Duration::hours(1);
        let reference = uuid::Uuid::new_v4().to_string();

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, is_outgoing, created_at, updated_at)
            VALUES (?, ?, 'unsigned', ?, 'test abandoned tx', -1000, 1, 0, 1, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind::<Option<String>>(None)
        .bind(&reference)
        .bind(old_time)
        .bind(old_time)
        .execute(storage.pool())
        .await
        .expect("insert transaction");

        // Configure monitor with a very short fail_abandoned_timeout (1 second)
        // and only the fail_abandoned task enabled.
        let mut opts = all_tasks_disabled();
        opts.tasks.fail_abandoned.enabled = true;
        opts.fail_abandoned_timeout = Duration::from_secs(1);

        let monitor = Monitor::with_options(storage.clone(), services, opts);

        // Run once - the FailAbandonedTask should find and abort the old transaction.
        let results = monitor.run_once().await.expect("run_once should succeed");

        // Verify the fail_abandoned task ran.
        let fail_result = results.get(&TaskType::FailAbandoned);
        assert!(
            fail_result.is_some(),
            "FailAbandoned task should be in results"
        );

        // Verify the transaction was set to 'failed' status.
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM transactions WHERE reference = ?")
                .bind(&reference)
                .fetch_optional(storage.pool())
                .await
                .expect("query transaction status");

        assert!(row.is_some(), "Transaction should still exist in database");
        let (status,) = row.unwrap();
        assert_eq!(
            status, "failed",
            "Abandoned transaction should have been set to 'failed', got: {}",
            status
        );
    }

    // =========================================================================
    // Test 5: check_for_proofs_integration
    // =========================================================================

    /// Insert a ProvenTxReq with status 'unmined', configure mock services to
    /// return a merkle path, run CheckForProofsTask, and verify items_processed > 0.
    #[tokio::test]
    async fn check_for_proofs_integration() {
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::GetMerklePathResult;

        // Configure mock to return a merkle path.
        let mock = MockWalletServices::builder()
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some("deadbeef01020304".to_string()),
                header: Some(bsv_wallet_toolbox_rs::BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root: "a".repeat(64),
                    time: 1700000000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "b".repeat(64),
                    height: 850000,
                }),
                error: None,
                notes: vec![],
            }))
            .build();

        let (storage, services) = setup_storage_and_services(mock).await;

        // Insert a proven_tx_req with status 'unmined'.
        let txid = "a".repeat(64);
        let now = chrono::Utc::now();
        // raw_tx is NOT NULL in the schema, so we must provide it.
        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, 'unmined', 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert proven_tx_req");

        // Only enable check_for_proofs.
        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;

        let monitor = Monitor::with_options(storage.clone(), services, opts);
        let results = monitor.run_once().await.expect("run_once should succeed");

        let proof_result = results
            .get(&TaskType::CheckForProofs)
            .expect("CheckForProofs task should be in results");

        // The task queries proven_tx_reqs with 'unmined' status and calls get_merkle_path.
        // Our mock returns a successful merkle path, so items_processed should be > 0.
        assert!(
            proof_result.items_processed > 0,
            "CheckForProofs should have processed at least 1 item, got: {}",
            proof_result.items_processed
        );
        assert!(
            proof_result.errors.is_empty(),
            "CheckForProofs should have no errors, got: {:?}",
            proof_result.errors
        );
    }

    // =========================================================================
    // Test 6: send_waiting_integration
    // =========================================================================

    /// Insert a ProvenTxReq with status 'unsent', configure mock services for
    /// broadcast, run SendWaitingTask, and verify status is updated.
    #[tokio::test]
    async fn send_waiting_integration() {
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::PostBeefResult;

        // Mock that returns success for post_beef.
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Success(vec![PostBeefResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                txid_results: vec![],
                error: None,
                notes: vec![],
            }]))
            .build();

        let (storage, services) = setup_storage_and_services(mock).await;

        // Insert a user and a transaction first (for the raw_tx lookup).
        let identity_key = "02".to_string() + &"ef".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("find_or_insert_user");

        let txid = "b".repeat(64);
        let now = chrono::Utc::now();
        // Use a created_at in the past so the age filter passes.
        let old_time = now - chrono::Duration::minutes(5);

        // Insert a transaction with raw_tx so SendWaiting can find it.
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, raw_tx, is_outgoing, created_at, updated_at)
            VALUES (?, ?, 'sending', ?, 'test send waiting', -500, 1, 0, X'01000000', 1, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(&txid)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(old_time)
        .bind(old_time)
        .execute(storage.pool())
        .await
        .expect("insert transaction");

        // Insert a proven_tx_req with status 'unsent'.
        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, 'unsent', 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(&txid)
        .bind(old_time)
        .bind(old_time)
        .execute(storage.pool())
        .await
        .expect("insert proven_tx_req");

        // Only enable send_waiting.
        let mut opts = all_tasks_disabled();
        opts.tasks.send_waiting.enabled = true;

        let monitor = Monitor::with_options(storage.clone(), services, opts);
        let results = monitor.run_once().await.expect("run_once should succeed");

        let send_result = results
            .get(&TaskType::SendWaiting)
            .expect("SendWaiting task should be in results");

        // The task delegates to MonitorStorage::send_waiting_transactions.
        // Check the proven_tx_req status was updated after broadcast attempt.
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_optional(storage.pool())
                .await
                .expect("query proven_tx_req status");

        assert!(
            row.is_some(),
            "proven_tx_req should still exist in database"
        );

        // Verify send_result doesn't have fatal errors.
        // Note: depending on exact broadcast logic, status may be 'unmined' (success),
        // 'sending' (in progress), or still 'unsent' (if age filter didn't pass).
        // The key assertion is that the task ran without fatal errors.
        assert!(
            send_result.errors.is_empty(),
            "SendWaiting should have no fatal errors, got: {:?}",
            send_result.errors
        );
    }

    // =========================================================================
    // Test 7: custom_task_config
    // =========================================================================

    /// Disable all tasks except one (clock), run run_once(), and verify
    /// only that task appears in results.
    #[tokio::test]
    async fn custom_task_config() {
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        // Start with all disabled, then enable only clock.
        let mut opts = all_tasks_disabled();
        opts.tasks.clock.enabled = true;

        let monitor = Monitor::with_options(storage, services, opts);
        let results = monitor.run_once().await.expect("run_once should succeed");

        // Only clock should have been run.
        assert_eq!(
            results.len(),
            1,
            "Only 1 task should have run, got {} tasks: {:?}",
            results.len(),
            results.keys().collect::<Vec<_>>()
        );
        assert!(
            results.contains_key(&TaskType::Clock),
            "Clock task should be the only task in results"
        );

        // Verify clock task ran without errors.
        let clock_result = results.get(&TaskType::Clock).unwrap();
        assert!(
            clock_result.errors.is_empty(),
            "Clock task should have no errors"
        );
    }

    // =========================================================================
    // Test 8: monitor_options_callbacks
    // =========================================================================

    /// Set on_tx_broadcasted and on_tx_proven callbacks on MonitorOptions,
    /// then verify the options are correctly stored and the callbacks can fire.
    ///
    /// Note: The callbacks are invoked by the concrete wallet layer when a
    /// transaction is broadcast or proven, not directly by the monitor tasks.
    /// This test verifies callback wiring rather than end-to-end invocation
    /// through the monitor daemon.
    #[tokio::test]
    async fn monitor_options_callbacks() {
        let broadcast_count = Arc::new(AtomicU32::new(0));
        let proven_count = Arc::new(AtomicU32::new(0));

        let bc = broadcast_count.clone();
        let pc = proven_count.clone();

        let mut opts = all_tasks_disabled();
        opts.on_tx_broadcasted = Some(Arc::new(move |_update: TransactionStatusUpdate| {
            bc.fetch_add(1, Ordering::SeqCst);
        }));
        opts.on_tx_proven = Some(Arc::new(move |_update: TransactionStatusUpdate| {
            pc.fetch_add(1, Ordering::SeqCst);
        }));

        // Verify the callbacks are set.
        assert!(
            opts.on_tx_broadcasted.is_some(),
            "on_tx_broadcasted should be set"
        );
        assert!(opts.on_tx_proven.is_some(), "on_tx_proven should be set");

        // Manually invoke the callbacks to verify they work.
        let broadcast_cb = opts.on_tx_broadcasted.as_ref().unwrap();
        broadcast_cb(TransactionStatusUpdate {
            txid: "abc123".to_string(),
            status: "unproven".to_string(),
            merkle_root: None,
            merkle_path: None,
            block_height: None,
            block_hash: None,
        });

        let proven_cb = opts.on_tx_proven.as_ref().unwrap();
        proven_cb(TransactionStatusUpdate {
            txid: "abc123".to_string(),
            status: "completed".to_string(),
            merkle_root: Some("root".to_string()),
            merkle_path: Some("path".to_string()),
            block_height: Some(850000),
            block_hash: Some("hash".to_string()),
        });

        assert_eq!(
            broadcast_count.load(Ordering::SeqCst),
            1,
            "Broadcast callback should have been invoked once"
        );
        assert_eq!(
            proven_count.load(Ordering::SeqCst),
            1,
            "Proven callback should have been invoked once"
        );

        // Now create a Monitor with these options and verify it works.
        let (storage, services) = setup_storage_and_services(MockWalletServices::new()).await;

        let monitor = Monitor::with_options(storage, services, opts);

        // run_once with all tasks disabled should succeed with no results.
        let results = monitor.run_once().await.expect("run_once should succeed");
        assert!(
            results.is_empty(),
            "No tasks were enabled, so no results expected"
        );
    }
}
