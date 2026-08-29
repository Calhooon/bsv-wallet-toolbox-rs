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
        use bsv_rs::transaction::MerklePath;
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::services::TxStatusDetail;
        use bsv_wallet_toolbox_rs::{GetMerklePathResult, GetStatusForTxidsResult};

        let txid = "a".repeat(64);
        let height = 850000u32;

        // Build a valid BUMP (coinbase-style single-tx merkle path) and hex-encode it.
        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let bump_hex = hex::encode(bump.to_binary());
        // For a single-tx block the merkle root equals the txid.
        let merkle_root = bump
            .compute_root(Some(&txid))
            .expect("compute_root for coinbase bump");

        // Configure mock to return the valid merkle path. The triage step
        // (get_status_for_txids) must report the tx as mined with depth >= 1,
        // otherwise synchronize_transaction_statuses skips the proof fetch.
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "mined".to_string(),
                    depth: Some(2),
                }],
            }))
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some(bump_hex),
                header: Some(bsv_wallet_toolbox_rs::BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root,
                    time: 1700000000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "b".repeat(64),
                    height,
                }),
                error: None,
                notes: vec![],
            }))
            .build();

        let (storage, services) = setup_storage_and_services(mock).await;

        // Insert a proven_tx_req with status 'unmined'.
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
    // Test 5b: one-shot proof back-fill (issue #8)
    // =========================================================================

    /// A single `run_once()` must be able to back-fill `transactions.proven_tx_id`
    /// for a tx that mined while nothing was running — no long-lived daemon and
    /// no new-block trigger required. This is the path `bsv-wallet tick` uses.
    ///
    /// Deliberately does NOT pre-set services on storage: `run_once` establishes
    /// that itself (the same prerequisite `start()` sets up), so an ephemeral
    /// process cannot silently report "0 processed" for every storage-driven
    /// task. Consumers whose confirmed-coin oracle is `proven_tx_id IS NOT NULL`
    /// depend on this (Calgooon/bsv-wallet-toolbox-rs#8).
    #[tokio::test]
    async fn run_once_backfills_proven_tx_id_without_preset_services() {
        use bsv_rs::transaction::MerklePath;
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::services::TxStatusDetail;
        use bsv_wallet_toolbox_rs::{GetMerklePathResult, GetStatusForTxidsResult};

        let txid = "c".repeat(64);
        let height = 851_234u32;

        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let bump_hex = hex::encode(bump.to_binary());
        let merkle_root = bump.compute_root(Some(&txid)).expect("compute_root");

        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "mined".to_string(),
                    depth: Some(3),
                }],
            }))
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some(bump_hex),
                header: Some(bsv_wallet_toolbox_rs::BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root: merkle_root.clone(),
                    time: 1_700_000_000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "d".repeat(64),
                    height,
                }),
                error: None,
                notes: vec![],
            }))
            .build();

        // Build storage WITHOUT calling storage.set_services().
        let storage = StorageSqlx::in_memory().await.expect("in_memory storage");
        let storage_key = "02".to_string() + &"ab".repeat(32);
        storage
            .migrate("test-monitor", &storage_key)
            .await
            .expect("migrate");
        storage.make_available().await.expect("make_available");
        let storage = Arc::new(storage);
        let services = Arc::new(mock);

        let identity_key = "02".to_string() + &"cd".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("find_or_insert_user");

        let now = chrono::Utc::now();

        // The shape issue #8 reports: an incoming coin internalized BEFORE it
        // mined — tx 'unproven' with proven_tx_id NULL, req 'unmined'.
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, raw_tx, is_outgoing, created_at, updated_at)
            VALUES (?, ?, 'unproven', ?, 'incoming coin', 150000, 1, 0, X'01000000', 0, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(&txid)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert transaction");

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

        // Precondition: the consumer oracle reads "not confirmed".
        let before: Option<i64> =
            sqlx::query_scalar("SELECT proven_tx_id FROM transactions WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("read proven_tx_id");
        assert!(before.is_none(), "precondition: proven_tx_id starts NULL");

        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;

        let monitor = Monitor::with_options(storage.clone(), services, opts);
        let results = monitor.run_once().await.expect("run_once should succeed");

        let proof_result = results
            .get(&TaskType::CheckForProofs)
            .expect("CheckForProofs must run under run_once");
        assert!(
            proof_result.errors.is_empty(),
            "run_once must supply services to storage; got errors: {:?}",
            proof_result.errors
        );
        assert!(
            proof_result.items_processed > 0,
            "one run_once must process the mined req, got {}",
            proof_result.items_processed
        );

        // The whole point: the consumer oracle now reads "confirmed".
        let after: Option<i64> =
            sqlx::query_scalar("SELECT proven_tx_id FROM transactions WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("read proven_tx_id");
        assert!(
            after.is_some(),
            "a single run_once must back-fill transactions.proven_tx_id"
        );

        let tx_status: String =
            sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("read tx status");
        assert_eq!(tx_status, "completed");

        let stored_root: String =
            sqlx::query_scalar("SELECT merkle_root FROM proven_txs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("proven_txs row must exist");
        assert_eq!(stored_root, merkle_root);
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

    // =========================================================================
    // The receive-proof gap (bsv-wallet-cli, 2026-08-29): proof-less
    // transactions with NO proven_tx_req are invisible to CheckForProofs.
    // =========================================================================

    /// Shared fixture: mock services that report `txid` MINED (depth 2) and
    /// serve a valid single-tx BUMP for it. Returns (mock, merkle_root).
    fn mined_with_bump(txid: &str, height: u32) -> (MockWalletServices, String) {
        use bsv_rs::transaction::MerklePath;
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::services::TxStatusDetail;
        use bsv_wallet_toolbox_rs::{GetMerklePathResult, GetStatusForTxidsResult};

        let bump = MerklePath::from_coinbase_txid(txid, height);
        let bump_hex = hex::encode(bump.to_binary());
        let merkle_root = bump.compute_root(Some(txid)).expect("compute_root");
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.to_string(),
                    status: "mined".to_string(),
                    depth: Some(2),
                }],
            }))
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some(bump_hex),
                header: Some(bsv_wallet_toolbox_rs::BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root: merkle_root.clone(),
                    time: 1_700_000_000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "e".repeat(64),
                    height,
                }),
                error: None,
                notes: vec![],
            }))
            .build();
        (mock, merkle_root)
    }

    /// Insert a `transactions` row in the given status with NULL proven_tx_id
    /// and NO proven_tx_req — the pre-0.3.52 internalize shape.
    async fn insert_proofless_tx(storage: &StorageSqlx, user_id: i64, txid: &str, status: &str) {
        let now = chrono::Utc::now();
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, raw_tx, is_outgoing, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'Internalize external funding', 12345, 1, 0, X'01000000', 0, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(txid)
        .bind(status)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert transaction");
    }

    async fn req_rows(storage: &StorageSqlx, txid: &str) -> Vec<(String, Option<i64>)> {
        sqlx::query_as("SELECT status, proven_tx_id FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_all(storage.pool())
            .await
            .expect("read reqs")
    }

    async fn tx_link(storage: &StorageSqlx, txid: &str) -> (String, Option<i64>) {
        sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .expect("read tx")
    }

    /// THE GAP, red→green: a `'completed'` transaction with NULL
    /// `proven_tx_id` and NO req (what every pre-0.3.52 internalize wrote)
    /// must be adopted by ONE `run_once` and come out proven — req created,
    /// BUMP ingested, `transactions.proven_tx_id` linked. Before this change
    /// the walk below never saw the row and it stayed proof-less forever.
    #[tokio::test]
    async fn run_once_adopts_a_proofless_completed_tx_with_no_req() {
        let txid = "f".repeat(64);
        let (mock, merkle_root) = mined_with_bump(&txid, 852_001);
        let (storage, services) = setup_storage_and_services(mock).await;
        let identity_key = "02".to_string() + &"ef".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("user");
        insert_proofless_tx(&storage, user.user_id, &txid, "completed").await;
        assert!(
            req_rows(&storage, &txid).await.is_empty(),
            "precondition: the legacy shape has NO req"
        );

        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;
        let monitor = Monitor::with_options(storage.clone(), services, opts);
        let results = monitor.run_once().await.expect("run_once");
        let proof_result = results
            .get(&TaskType::CheckForProofs)
            .expect("CheckForProofs ran");
        assert!(proof_result.errors.is_empty(), "{:?}", proof_result.errors);
        assert!(
            proof_result.items_processed > 0,
            "the adopted tx must be proven in the SAME pass that adopted it"
        );

        let reqs = req_rows(&storage, &txid).await;
        assert_eq!(reqs.len(), 1, "exactly one req adopted");
        assert_eq!(reqs[0].0, "completed", "the req completed on the BUMP");
        assert!(reqs[0].1.is_some(), "the req carries the proven_tx_id");

        let (status, link) = tx_link(&storage, &txid).await;
        assert_eq!(status, "completed");
        assert!(link.is_some(), "transactions.proven_tx_id must be linked");
        let stored_root: String =
            sqlx::query_scalar("SELECT merkle_root FROM proven_txs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("proven_txs row");
        assert_eq!(stored_root, merkle_root);
    }

    /// An `'unproven'` row with no req (issue #8's unreachable shape) is
    /// covered by the same adoption — the net is status-agnostic on purpose.
    #[tokio::test]
    async fn run_once_adopts_an_orphan_unproven_tx_too() {
        let txid = "e".repeat(64);
        let (mock, _) = mined_with_bump(&txid, 852_002);
        let (storage, services) = setup_storage_and_services(mock).await;
        let identity_key = "02".to_string() + &"ee".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("user");
        insert_proofless_tx(&storage, user.user_id, &txid, "unproven").await;

        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;
        let monitor = Monitor::with_options(storage.clone(), services, opts);
        monitor.run_once().await.expect("run_once");

        let (status, link) = tx_link(&storage, &txid).await;
        assert_eq!(status, "completed", "proven ⇒ completed");
        assert!(link.is_some());
    }

    /// A proof that is ALREADY stored (a `proven_txs` row exists) only needs
    /// the link: no req is created and nothing is fetched — the services
    /// here report the tx UNKNOWN, so a fetch could not have succeeded.
    #[tokio::test]
    async fn adoption_links_from_an_existing_proven_txs_row_without_fetching() {
        use bsv_wallet_toolbox_rs::services::mock::MockResponse;
        use bsv_wallet_toolbox_rs::services::TxStatusDetail;
        use bsv_wallet_toolbox_rs::GetStatusForTxidsResult;

        let txid = "d".repeat(64);
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "unknown".to_string(),
                    depth: None,
                }],
            }))
            .build();
        let (storage, services) = setup_storage_and_services(mock).await;
        let identity_key = "02".to_string() + &"dd".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("user");
        insert_proofless_tx(&storage, user.user_id, &txid, "completed").await;
        let now = chrono::Utc::now();
        sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, 852003, 0, ?, ?, X'00', X'01000000', ?, ?)
            "#,
        )
        .bind(&txid)
        .bind("b".repeat(64))
        .bind("c".repeat(64))
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert proven_txs");
        let proven_id: i64 =
            sqlx::query_scalar("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .expect("proven id");

        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;
        let monitor = Monitor::with_options(storage.clone(), services, opts);
        monitor.run_once().await.expect("run_once");

        let (status, link) = tx_link(&storage, &txid).await;
        assert_eq!(status, "completed");
        assert_eq!(link, Some(proven_id), "linked to the EXISTING proof row");
        assert!(
            req_rows(&storage, &txid).await.is_empty(),
            "a linked row is no longer a candidate — no req is minted for it"
        );
    }

    /// A row that already has a req — here `'nosend'`, the release-pin
    /// flow's shape — belongs to that req's owner. Adoption must neither add
    /// a second req (UNIQUE txid would refuse anyway) nor touch the status.
    #[tokio::test]
    async fn adoption_never_touches_a_tx_that_already_has_a_req() {
        let txid = "c".repeat(64);
        let (mock, _) = mined_with_bump(&txid, 852_004);
        let (storage, services) = setup_storage_and_services(mock).await;
        let identity_key = "02".to_string() + &"cc".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("user");
        insert_proofless_tx(&storage, user.user_id, &txid, "completed").await;
        let now = chrono::Utc::now();
        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, 'nosend', 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert nosend req");

        let mut opts = all_tasks_disabled();
        opts.tasks.check_for_proofs.enabled = true;
        let monitor = Monitor::with_options(storage.clone(), services, opts);
        monitor.run_once().await.expect("run_once");

        let reqs = req_rows(&storage, &txid).await;
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].0, "nosend", "the nosend owner keeps its req");
        let (status, link) = tx_link(&storage, &txid).await;
        assert_eq!(status, "completed");
        assert!(link.is_none(), "nothing proven behind the owner's back");
    }

    /// One pass adopts at most `ADOPT_UNPROVEN_TX_LIMIT` rows; the rest
    /// drain on later passes. Calls the pass directly so the bound is
    /// pinned as a number, not as a side effect of a mock's behaviour.
    #[tokio::test]
    async fn adoption_is_bounded_per_pass_and_drains() {
        use bsv_wallet_toolbox_rs::storage::sqlx::ADOPT_UNPROVEN_TX_LIMIT;

        let (storage, _services) = setup_storage_and_services(MockWalletServices::new()).await;
        let identity_key = "02".to_string() + &"bb".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("user");
        let extra = 5usize;
        let total = ADOPT_UNPROVEN_TX_LIMIT as usize + extra;
        for i in 0..total {
            let txid = format!("{:064x}", 0x1000 + i);
            insert_proofless_tx(&storage, user.user_id, &txid, "completed").await;
        }

        let first = storage
            .adopt_unproven_transactions()
            .await
            .expect("first pass");
        assert_eq!(
            first.reqs_created as usize,
            ADOPT_UNPROVEN_TX_LIMIT as usize
        );
        assert_eq!(first.linked_from_proven_txs, 0);

        let second = storage
            .adopt_unproven_transactions()
            .await
            .expect("second pass");
        assert_eq!(second.reqs_created as usize, extra, "the remainder drains");

        let third = storage
            .adopt_unproven_transactions()
            .await
            .expect("third pass");
        assert_eq!(third.reqs_created, 0, "idempotent once every row has a req");

        let unmined: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM proven_tx_reqs WHERE status = 'unmined'")
                .fetch_one(storage.pool())
                .await
                .expect("count");
        assert_eq!(unmined as usize, total);
    }
}
