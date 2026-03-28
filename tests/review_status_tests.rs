//! Tests for the `review_status()` method on StorageSqlx (MonitorStorage trait).
//!
//! These tests verify the three checks performed by review_status:
//! 1. Mark transactions as 'failed' when their proven_tx_req is 'invalid'
//! 2. Release outputs spent by failed transactions (spendable=1, spent_by=NULL)
//! 3. Mark transactions completed when proof exists (proven_tx_req completed with proven_tx_id)

#[cfg(feature = "sqlite")]
mod review_status {
    use std::sync::Arc;

    use bsv_wallet_toolbox_rs::{AuthId, MonitorStorage, StorageSqlx, WalletStorageWriter};
    use chrono::Utc;

    /// Helper: set up an in-memory storage with migrations and a test user.
    async fn setup_storage() -> (Arc<StorageSqlx>, AuthId) {
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

    /// Helper: insert a transaction directly via SQL and return its transaction_id.
    async fn insert_transaction(
        storage: &StorageSqlx,
        user_id: i64,
        txid: &str,
        status: &str,
    ) -> i64 {
        let now = Utc::now();
        let reference = uuid::Uuid::new_v4().to_string();
        let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, is_outgoing, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'test tx', -1000, 1, 0, 1, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(txid)
        .bind(status)
        .bind(reference)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    /// Helper: insert a proven_tx_req directly via SQL.
    async fn insert_proven_tx_req(
        storage: &StorageSqlx,
        txid: &str,
        status: &str,
        proven_tx_id: Option<i64>,
    ) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, proven_tx_id, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, ?, ?, 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(status)
        .bind(proven_tx_id)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    /// Helper: insert an output with an optional spent_by reference.
    async fn insert_output(
        storage: &StorageSqlx,
        user_id: i64,
        transaction_id: i64,
        txid: &str,
        vout: i32,
        satoshis: i64,
        spendable: bool,
        spent_by: Option<i64>,
    ) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO outputs (user_id, transaction_id, txid, vout, satoshis, script_length, script_offset,
                                 type, provided_by, purpose, spendable, change, locking_script,
                                 created_at, updated_at, spent_by)
            VALUES (?, ?, ?, ?, ?, 25, 0, 'P2PKH', 'you', 'change', ?, 0,
                    X'76a914000000000000000000000000000000000000000088ac', ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(txid)
        .bind(vout)
        .bind(satoshis)
        .bind(spendable)
        .bind(now)
        .bind(now)
        .bind(spent_by)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    // =========================================================================
    // Test 1: Check 1 — Mark transactions as failed when proven_tx_req is invalid
    // =========================================================================

    #[tokio::test]
    async fn test_review_status_marks_tx_failed_when_req_invalid() {
        let (storage, auth) = setup_storage().await;
        let user_id = auth.user_id.unwrap();
        let txid = "aa".repeat(32);

        // Insert a transaction with status 'unproven'
        insert_transaction(&storage, user_id, &txid, "unproven").await;

        // Insert a proven_tx_req with status 'invalid' for the same txid
        insert_proven_tx_req(&storage, &txid, "invalid", None).await;

        // Verify initial state: transaction is 'unproven'
        let row: (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.0, "unproven", "precondition: tx should be unproven");

        // Run review_status (MonitorStorage trait method — no auth needed)
        let result = MonitorStorage::review_status(storage.as_ref())
            .await
            .unwrap();

        // Verify the log mentions marking transactions as failed
        assert!(
            result.log.contains("failed"),
            "Log should mention failed transactions, got: {}",
            result.log
        );

        // Verify: transaction status is now 'failed'
        let row: (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            row.0, "failed",
            "Transaction should be marked 'failed' when proven_tx_req is 'invalid'"
        );
    }

    // =========================================================================
    // Test 2: Check 2 — Release outputs spent by failed transactions
    // =========================================================================

    #[tokio::test]
    async fn test_review_status_releases_outputs_from_failed_tx() {
        let (storage, auth) = setup_storage().await;
        let user_id = auth.user_id.unwrap();

        // Create a "source" transaction that produced the output
        let source_txid = "bb".repeat(32);
        let source_tx_id = insert_transaction(&storage, user_id, &source_txid, "completed").await;

        // Create a "spending" transaction that is already failed
        let spending_txid = "cc".repeat(32);
        let spending_tx_id = insert_transaction(&storage, user_id, &spending_txid, "failed").await;

        // Insert an output from source_tx that was locked by the failed spending_tx
        // (spendable=0, spent_by=spending_tx_id)
        insert_output(
            &storage,
            user_id,
            source_tx_id,
            &source_txid,
            0,
            5000,
            false, // not spendable — locked by spending tx
            Some(spending_tx_id),
        )
        .await;

        // Verify precondition: output is locked
        let row: (bool, Option<i64>) =
            sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0")
                .bind(&source_txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(!row.0, "precondition: output should not be spendable");
        assert_eq!(
            row.1,
            Some(spending_tx_id),
            "precondition: output should be locked by spending tx"
        );

        // Run review_status
        let result = MonitorStorage::review_status(storage.as_ref())
            .await
            .unwrap();

        // Verify the log mentions releasing outputs
        assert!(
            result.log.contains("Released"),
            "Log should mention releasing outputs, got: {}",
            result.log
        );

        // Verify: output is now spendable=1 and spent_by=NULL
        let row: (bool, Option<i64>) =
            sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0")
                .bind(&source_txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(row.0, "Output should be spendable after failed tx cleanup");
        assert!(
            row.1.is_none(),
            "Output spent_by should be NULL after failed tx cleanup"
        );
    }

    // =========================================================================
    // Test 3: Check 3 — Mark transactions completed when proof exists
    // =========================================================================

    /// Helper: insert a proven_txs row (the proof record) and return its proven_tx_id.
    async fn insert_proven_tx(storage: &StorageSqlx, txid: &str) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, 850000, 0, ?, ?, X'deadbeef', X'01000000', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(&"b".repeat(64))
        .bind(&"a".repeat(64))
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    #[tokio::test]
    async fn test_review_status_syncs_completed_proof() {
        let (storage, auth) = setup_storage().await;
        let user_id = auth.user_id.unwrap();
        let txid = "dd".repeat(32);

        // Insert a transaction with status 'unproven'
        insert_transaction(&storage, user_id, &txid, "unproven").await;

        // Insert a proven_txs record (the actual proof) to satisfy the FK constraint
        let proven_tx_id = insert_proven_tx(&storage, &txid).await;

        // Insert a proven_tx_req with status 'completed' and proven_tx_id set
        // (simulating that a proof was found and stored)
        insert_proven_tx_req(&storage, &txid, "completed", Some(proven_tx_id)).await;

        // Verify initial state
        let row: (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.0, "unproven", "precondition: tx should be unproven");

        // Run review_status
        let result = MonitorStorage::review_status(storage.as_ref())
            .await
            .unwrap();

        // Verify the log mentions updating statuses
        assert!(
            result.log.contains("completed"),
            "Log should mention completed transactions, got: {}",
            result.log
        );

        // Verify: transaction status is now 'completed'
        let row: (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            row.0, "completed",
            "Transaction should be marked 'completed' when proven_tx_req has proof"
        );
    }
}
