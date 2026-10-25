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
}
