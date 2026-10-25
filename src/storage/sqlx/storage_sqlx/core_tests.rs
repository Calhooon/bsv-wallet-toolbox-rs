#[cfg(test)]
mod tests {
    use super::super::*;

    /// A tracker that answers every root valid.
    struct AnyRoot;

    #[async_trait]
    impl ChainTracker for AnyRoot {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
            Ok(true)
        }

        async fn current_height(
            &self,
        ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
            Ok(1)
        }
    }

    /// One store, two handles (bsv-wallet-cli #44: one store per file per
    /// process): the chain tracker, the services, the settings and the
    /// identity set through one handle are seen through the other, cleared
    /// as well; the two hold one broadcast memory and one writer.
    #[tokio::test]
    async fn the_tracker_services_and_settings_set_on_one_handle_are_seen_by_the_other() {
        let a = StorageSqlx::in_memory().await.unwrap();
        let b = a.clone();
        assert!(b.get_chain_tracker().await.is_none());
        assert!(b.get_services().is_err());

        let tracker: Arc<dyn ChainTracker> = Arc::new(AnyRoot);
        a.set_chain_tracker(tracker.clone()).await;
        let seen = b.get_chain_tracker().await.expect("set through a");
        assert!(Arc::ptr_eq(&seen, &tracker));
        b.clear_chain_tracker().await;
        assert!(a.get_chain_tracker().await.is_none(), "cleared through b");

        let services: Arc<dyn WalletServices> =
            Arc::new(crate::services::mock::MockWalletServices::new());
        b.set_services(services.clone());
        assert!(Arc::ptr_eq(&a.get_services().unwrap(), &services));

        a.migrate("shared", &"02".repeat(33)).await.unwrap();
        assert_eq!(b.storage_identity_key(), "02".repeat(33));
        assert_eq!(b.storage_name(), "shared");
        assert_eq!(b.get_settings().storage_name, "shared");
        let copy = b.get_settings();
        a.make_available().await.unwrap();
        b.make_available().await.unwrap();
        assert_eq!(copy.storage_identity_key, "02".repeat(33));
        assert_eq!(a.get_settings(), b.get_settings());

        assert!(Arc::ptr_eq(
            &a.sqlx_broadcast_memory(),
            &b.sqlx_broadcast_memory()
        ));
        let held = a.writer().acquire().await.unwrap();
        assert_eq!(b.writer().num_idle(), 0, "one writer connection");
        drop(held);
    }

    /// Review 4 G7 of #44: `get_settings`, `storage_identity_key` and
    /// `storage_name` lent references out of their locks through `unsafe`,
    /// so with shared handles a second handle's `make_available` (or
    /// `migrate`) could free a value another was reading. They return owned
    /// copies now, taken under the lock. Two handles: one replaces the
    /// settings, the identity and the name with changing values, over and
    /// over, while four threads read them through the other; every value
    /// read is one that was written, whole.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_through_one_handle_race_make_available_through_another() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g7.db").to_string_lossy().to_string();
        let a = StorageSqlx::open(&path).await.unwrap();
        let b = a.clone();
        let name = |i: usize| format!("store-{}", i % 7);
        let key = |i: usize| format!("{:02x}", i % 5).repeat(33);
        a.migrate(&name(0), &key(0)).await.unwrap();
        a.make_available().await.unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let b = b.clone();
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let names: HashSet<String> = (0..7).map(name).collect();
                    let keys: HashSet<String> = (0..5).map(key).collect();
                    let mut reads = 0u64;
                    while !done.load(std::sync::atomic::Ordering::SeqCst) {
                        let settings = b.get_settings();
                        assert!(names.contains(&settings.storage_name), "{settings:?}");
                        assert!(keys.contains(&settings.storage_identity_key));
                        assert!(names.contains(&b.storage_name()));
                        assert!(keys.contains(&b.storage_identity_key()));
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        for i in 1..300 {
            if i % 10 == 0 {
                a.migrate(&name(i), &key(i)).await.unwrap();
            } else {
                sqlx::query("UPDATE settings SET storage_name = ?, updated_at = ?")
                    .bind(name(i))
                    .bind(Utc::now())
                    .execute(a.writer())
                    .await
                    .unwrap();
            }
            a.make_available().await.unwrap();
            b.make_available().await.unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        for reader in readers {
            assert!(reader.join().expect("a reader saw a whole value") > 0);
        }
    }

    /// Review 2 F4 of #44: a begin that fails for anything but busy keeps
    /// the `Database error:` prefix these sites answered before #44. The
    /// pool's one connection is already inside a transaction, so SQLite
    /// refuses `BEGIN IMMEDIATE` (code 1, not busy).
    #[tokio::test]
    async fn a_non_busy_begin_failure_is_a_database_error() {
        let in_a_transaction = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("BEGIN").execute(&mut *conn).await?;
                    Ok(())
                })
            })
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let err = match begin_immediate(&in_a_transaction).await {
            Ok(_) => panic!("a transaction within a transaction"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::DatabaseError(_)), "{err:?}");
        assert!(err.to_string().starts_with("Database error: "), "{err}");
        assert!(
            err.to_string()
                .contains("cannot start a transaction within a transaction"),
            "{err}"
        );
    }

    /// The begin/commit mapping: busy is `StoreBusy`, anything else
    /// `DatabaseError` with the error's text.
    #[test]
    fn a_begin_or_commit_error_is_store_busy_only_when_busy() {
        let err = write_tx_error(sqlx::Error::PoolTimedOut);
        assert!(
            matches!(
                err,
                Error::StoreBusy {
                    reason: StoreBusyReason::WriterNotAcquired
                }
            ),
            "{err:?}"
        );
        let err = write_tx_error(sqlx::Error::Protocol("a broken pipe".to_string()));
        assert!(matches!(err, Error::DatabaseError(_)), "{err:?}");
        assert!(err.to_string().starts_with("Database error: "), "{err}");
        assert!(err.to_string().contains("a broken pipe"), "{err}");
    }

    /// Only a timeout acquiring the WRITER is busy: a reader pool of one,
    /// held, and a second read times out on the reader pool. That is the
    /// plain SQLx error, not `StoreBusy`.
    #[tokio::test]
    async fn a_reader_pool_timeout_is_not_store_busy() {
        let store = StorageSqlx::connect(
            "sqlite::memory:",
            1,
            Some(std::time::Duration::from_millis(200)),
        )
        .await
        .unwrap();
        let _held = store.pool().acquire().await.unwrap();

        let err = store.find_user("02ab").await.unwrap_err();
        assert!(matches!(err, Error::SqlxError(_)), "{err:?}");
        assert!(!err.to_string().starts_with("Store busy"), "{err}");
        assert!(err.to_string().contains("pool timed out"), "{err}");
    }

    /// The writer's single-statement path: with the writer held, a
    /// single-statement write waits the ceiling and answers
    /// `StoreBusy` (`WriterNotAcquired`) at its acquire.
    #[tokio::test]
    async fn the_writer_not_acquired_for_one_statement_is_store_busy() {
        let store = StorageSqlx::in_memory().await.unwrap();
        let held = store.write_tx().await.unwrap();

        let started = std::time::Instant::now();
        let err = store.log_monitor_event("busy", None).await.unwrap_err();
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(4500),
            "it waited the ceiling"
        );
        assert!(
            matches!(
                err,
                Error::StoreBusy {
                    reason: StoreBusyReason::WriterNotAcquired
                }
            ),
            "{err:?}"
        );
        held.rollback().await.unwrap();
    }

    // =============================================================================
    // Services Integration Tests
    // =============================================================================

    #[tokio::test]
    async fn test_get_services_without_set_returns_error() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // get_services without set_services should return error
        let result = storage.get_services();
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("setServices"),
                    "Error message should mention setServices: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_set_and_get_services() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Create a services instance
        let services = Arc::new(Services::new(Chain::Main).unwrap());

        // Set services
        storage.set_services(services.clone());

        // Get services should now work
        let retrieved = storage.get_services();
        assert!(retrieved.is_ok());

        // Verify we can call a method on the retrieved services
        let retrieved_services = retrieved.unwrap();
        let script_hash = retrieved_services.hash_output_script(&[0x76, 0xa9]);
        assert!(!script_hash.is_empty());
    }

    #[tokio::test]
    async fn test_set_services_replaces_previous() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Set first services instance
        let services1 = Arc::new(Services::new(Chain::Main).unwrap());
        storage.set_services(services1);

        // Verify first services
        let retrieved1 = storage.get_services().unwrap();
        let hash1 = retrieved1.hash_output_script(&[0x76]);

        // Set second services instance (replaces first)
        let services2 = Arc::new(Services::new(Chain::Test).unwrap());
        storage.set_services(services2);

        // Get services again - should still work (services were replaced)
        let retrieved2 = storage.get_services().unwrap();
        let hash2 = retrieved2.hash_output_script(&[0x76]);

        // Both should produce the same hash (hash_output_script is deterministic)
        assert_eq!(hash1, hash2);
    }

    // =============================================================================
    // TrxToken / Transaction Scope Tests
    // =============================================================================

    #[tokio::test]
    async fn test_begin_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        assert!(token.id() > 0);
    }

    #[tokio::test]
    async fn test_commit_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.commit_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rollback_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.rollback_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trx_token_uniqueness() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token1 = storage.begin_transaction().await.unwrap();
        let token2 = storage.begin_transaction().await.unwrap();
        assert_ne!(token1.id(), token2.id());
        storage.commit_transaction(token1).await.unwrap();
        storage.commit_transaction(token2).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.commit_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    #[tokio::test]
    async fn test_rollback_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.rollback_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    #[tokio::test]
    async fn review_status_voids_created_outputs_of_newly_failed_transactions() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("review-status", &"0".repeat(64))
            .await
            .unwrap();
        let (user, _) = storage.find_or_insert_user(&"a".repeat(66)).await.unwrap();
        let seeded_at = Utc::now() - chrono::Duration::days(1);
        let mut transaction_ids = Vec::new();

        for (index, (status, req_status)) in [
            ("completed", None),
            ("unproven", Some("invalid")),
            ("sending", Some("invalid")),
            ("unproven", Some("unmined")),
            ("failed", Some("invalid")),
        ]
        .into_iter()
        .enumerate()
        {
            let txid = format!("{:064x}", index + 1);
            let transaction_id = sqlx::query(
                "INSERT INTO transactions (user_id, txid, status, reference, description, \
                 is_outgoing, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, 'review status regression', 1, ?, ?)",
            )
            .bind(user.user_id)
            .bind(&txid)
            .bind(status)
            .bind(format!("review-status-{index}"))
            .bind(seeded_at)
            .bind(seeded_at)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid();
            transaction_ids.push(transaction_id);

            if let Some(req_status) = req_status {
                sqlx::query(
                    "INSERT INTO proven_tx_reqs (txid, status, raw_tx, created_at, updated_at) \
                     VALUES (?, ?, X'01000000', ?, ?)",
                )
                .bind(&txid)
                .bind(req_status)
                .bind(seeded_at)
                .bind(seeded_at)
                .execute(storage.writer())
                .await
                .unwrap();
            }
        }

        let source = transaction_ids[0];
        let newly_failed = &transaction_ids[1..3];
        for (creator, vout, spendable, spent_by) in [
            // Consumed inputs must keep their locks and timestamps.
            (source, 0, false, Some(newly_failed[0])),
            (source, 1, false, Some(newly_failed[1])),
            // Clear every created output, including an existing spent_by.
            (newly_failed[0], 0, true, None),
            (newly_failed[0], 1, false, Some(source)),
            (newly_failed[1], 0, true, Some(source)),
            // A live request and an older failure are outside this transition.
            (transaction_ids[3], 0, true, None),
            (transaction_ids[4], 0, true, None),
        ] {
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, provided_by, \
                 purpose, type, spendable, spent_by, created_at, updated_at) \
                 VALUES (?, ?, ?, 1000, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)",
            )
            .bind(user.user_id)
            .bind(creator)
            .bind(vout)
            .bind(spendable)
            .bind(spent_by)
            .bind(seeded_at)
            .bind(seeded_at)
            .execute(storage.writer())
            .await
            .unwrap();
        }

        const OUTPUT_STATE: &str = "SELECT transaction_id, spendable, spent_by, \
            CAST(updated_at AS TEXT) FROM outputs ORDER BY output_id";
        const TRANSACTION_STATE: &str = "SELECT status, CAST(updated_at AS TEXT) \
            FROM transactions ORDER BY transaction_id";
        type OutputState = (i64, bool, Option<i64>, String);
        let before: Vec<OutputState> = sqlx::query_as(OUTPUT_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();

        let review = MonitorStorage::review_status(&storage).await.unwrap();
        assert!(review.log.contains("Marked 2 transactions as failed"));
        let transactions: Vec<(String, String)> = sqlx::query_as(TRANSACTION_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            transactions
                .iter()
                .map(|row| row.0.as_str())
                .collect::<Vec<_>>(),
            ["completed", "failed", "failed", "unproven", "failed"]
        );

        let after: Vec<OutputState> = sqlx::query_as(OUTPUT_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(after.len(), before.len());
        for (original, current) in before.iter().zip(&after) {
            if let Some(index) = transaction_ids
                .iter()
                .position(|id| *id == original.0)
                .filter(|index| (1..3).contains(index))
            {
                assert!(
                    !current.1,
                    "a newly failed transaction's created output must be void"
                );
                assert_eq!(
                    current.2, original.2,
                    "a void output must keep spent_by unchanged"
                );
                assert_eq!(current.3, transactions[index].1);
                assert_ne!(current.3, original.3, "the output timestamp must advance");
            } else {
                assert_eq!(
                    current, original,
                    "consumed inputs and unrelated outputs stay unchanged"
                );
            }
        }

        let again = MonitorStorage::review_status(&storage).await.unwrap();
        assert_eq!(again.log, "No status mismatches found\n");
        let unchanged: Vec<OutputState> = sqlx::query_as(OUTPUT_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(unchanged, after, "a repeat pass must not rewrite outputs");

        // An output-write failure must roll back the status write too.
        sqlx::query("UPDATE transactions SET status = 'unproven' WHERE transaction_id = ?")
            .bind(newly_failed[0])
            .execute(storage.writer())
            .await
            .unwrap();
        let before_failure: Vec<(String, String)> = sqlx::query_as(TRANSACTION_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_review_output_update BEFORE UPDATE OF spendable ON outputs \
             BEGIN SELECT RAISE(ABORT, 'injected output failure'); END",
        )
        .execute(storage.writer())
        .await
        .unwrap();
        let error = MonitorStorage::review_status(&storage).await.unwrap_err();
        assert!(error.to_string().contains("injected output failure"));
        let after_failure: Vec<(String, String)> = sqlx::query_as(TRANSACTION_STATE)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            after_failure, before_failure,
            "the failed pass must roll back"
        );
    }

    #[tokio::test]
    async fn review_status_newly_failed_root_retires_live_descendant() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("review-status", &"0".repeat(64))
            .await
            .unwrap();
        let (user, _) = storage.find_or_insert_user(&"a".repeat(66)).await.unwrap();
        let seeded_at = Utc::now() - chrono::Duration::days(1);
        let mut transaction_ids = Vec::new();

        for (index, (status, req_status)) in [
            ("completed", None),
            ("unproven", Some("invalid")),
            ("unproven", Some("unmined")),
        ]
        .into_iter()
        .enumerate()
        {
            let txid = format!("{:064x}", index + 1);
            let transaction_id = sqlx::query(
                "INSERT INTO transactions (user_id, txid, status, reference, description, \
                 is_outgoing, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, 'review status descendant regression', 1, ?, ?)",
            )
            .bind(user.user_id)
            .bind(&txid)
            .bind(status)
            .bind(format!("review-status-{index}"))
            .bind(seeded_at)
            .bind(seeded_at)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid();
            transaction_ids.push(transaction_id);

            if let Some(req_status) = req_status {
                sqlx::query(
                    "INSERT INTO proven_tx_reqs (txid, status, raw_tx, created_at, updated_at) \
                     VALUES (?, ?, X'01000000', ?, ?)",
                )
                .bind(&txid)
                .bind(req_status)
                .bind(seeded_at)
                .bind(seeded_at)
                .execute(storage.writer())
                .await
                .unwrap();
            }
        }

        let source = transaction_ids[0];
        let root = transaction_ids[1];
        let descendant_id = transaction_ids[2];
        for (creator, spendable, spent_by) in [
            (source, false, Some(root)),
            (root, false, Some(descendant_id)),
            (descendant_id, true, None),
        ] {
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, provided_by, \
                 purpose, type, spendable, spent_by, created_at, updated_at) \
                 VALUES (?, ?, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)",
            )
            .bind(user.user_id)
            .bind(creator)
            .bind(spendable)
            .bind(spent_by)
            .bind(seeded_at)
            .bind(seeded_at)
            .execute(storage.writer())
            .await
            .unwrap();
        }

        let review = MonitorStorage::review_status(&storage).await.unwrap();
        let descendant: (String, bool) = sqlx::query_as(
            "SELECT t.status, o.spendable FROM transactions t \
             JOIN outputs o ON o.transaction_id = t.transaction_id \
             WHERE t.transaction_id = ?",
        )
        .bind(descendant_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        println!("descendant={descendant:?}; log={}", review.log);
        assert_eq!(
            descendant,
            ("failed".to_string(), false),
            "check 4 must retire a live descendant of a root newly failed by check 1"
        );
        assert!(review.log.contains("Marked 1 transactions as failed"));
        assert!(review
            .log
            .contains("Retired the unproven descendants of 1 failed transactions"));
    }
}
