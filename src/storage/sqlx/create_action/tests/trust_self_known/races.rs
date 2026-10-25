use super::*;

/// Takes `coin` for another action the first time the root `when` is
/// asked (the pre-check, no lock held): a concurrent allocation
/// between the pre-check and the write lock. Records every root asked.
struct TakingTracker {
    writer: sqlx::SqlitePool,
    when: String,
    coin: i64,
    by: i64,
    taken: std::sync::atomic::AtomicBool,
    asked: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ChainTracker for TakingTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        _height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        self.asked.lock().unwrap().push(root.to_string());
        if root == self.when && !self.taken.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let taken = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
                    .bind(self.by)
                    .bind(self.coin)
                    .execute(&self.writer),
            )
            .await;
            assert!(
                matches!(taken, Ok(Ok(_))),
                "the writer is free during the pre-check"
            );
        }
        Ok(true)
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(100_000)
    }
}

/// The pre-checked coin is taken before the write lock: the step
/// rolls back (nothing written), the pre-check runs once more and
/// picks the other coin, and the action is created from it.
#[tokio::test]
async fn a_coin_taken_after_the_precheck_is_skipped_and_the_precheck_runs_again() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let near = tx_spending(None, 0x31, 50_000);
    let far = tx_spending(None, 0x32, 900_000);
    let near_coin = seed_change_coin(&storage, user_id, &near, true).await;
    let far_coin = seed_change_coin(&storage, user_id, &far, true).await;
    let outsider: i64 =
        sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE txid = ?")
            .bind(near.id())
            .fetch_one(storage.pool())
            .await
            .unwrap();

    let tracker = TakingTracker {
        writer: storage.writer().clone(),
        when: near.id(),
        coin: near_coin,
        by: outsider,
        taken: Default::default(),
        asked: Default::default(),
    };
    let result = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect("the action is created from the other coin");

    let asked = tracker.asked.lock().unwrap().clone();
    assert_eq!(
        asked.first(),
        Some(&near.id()),
        "the first pre-check read the near coin"
    );
    assert!(
        asked.contains(&far.id()),
        "the second pre-check read the far coin: {asked:?}"
    );
    let sources: Vec<&str> = result
        .inputs
        .iter()
        .map(|i| i.source_txid.as_str())
        .collect();
    assert_eq!(sources, vec![far.id().as_str()]);
    let spent_by = |coin: i64| {
        let pool = storage.pool().clone();
        async move {
            sqlx::query_scalar::<_, Option<i64>>("SELECT spent_by FROM outputs WHERE output_id = ?")
                .bind(coin)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(
        spent_by(near_coin).await,
        Some(outsider),
        "the taker keeps it"
    );
    let action: i64 = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions WHERE description = 'the door in one step'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(spent_by(far_coin).await, Some(action));
    assert_eq!(
        count(
            &storage,
            "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
        )
        .await,
        1,
        "one action row: the rolled-back step left none"
    );
}

/// Stores `proof` for `txid` the first time the root `when` is asked
/// (the pre-check, no lock held): the monitor storing a proof between
/// the pre-check and the write lock. Records every root asked.
struct ProvingTracker {
    writer: sqlx::SqlitePool,
    when: String,
    txid: String,
    raw: Vec<u8>,
    proof: Vec<u8>,
    stored: std::sync::atomic::AtomicBool,
    asked: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ChainTracker for ProvingTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        _height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        self.asked.lock().unwrap().push(root.to_string());
        if root == self.when && !self.stored.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let now = Utc::now();
            sqlx::query(
                        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
                         VALUES (?, 901, 0, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind(&self.txid)
                    .bind("ab".repeat(32))
                    .bind(&self.txid)
                    .bind(&self.proof)
                    .bind(&self.raw)
                    .bind(now)
                    .bind(now)
                    .execute(&self.writer)
                    .await
                    .expect("the writer is free during the pre-check");
        }
        Ok(true)
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(100_000)
    }
}

/// A proof stored between the pre-check and the write lock is a root
/// the pre-check did not read: the step rolls back (nothing written),
/// the pre-check runs once more and reads it, and the action's BEEF
/// carries the new proof, its parent no longer needed.
#[tokio::test]
async fn a_proof_stored_after_the_precheck_is_read_before_the_lock_on_the_second_try() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let parent = tx_spending(None, 0x51, 60_000);
    seed_known(&storage, user_id, &parent, true).await;
    let funding = tx_spending(Some(&parent.id()), 0x52, 50_000);
    seed_change_coin(&storage, user_id, &funding, false).await;

    let tracker = ProvingTracker {
        writer: storage.writer().clone(),
        when: parent.id(),
        txid: funding.id(),
        raw: funding.to_binary(),
        proof: bump_for(&funding.id(), 901).to_binary(),
        stored: Default::default(),
        asked: Default::default(),
    };
    let result = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect("the action is created on the second try");

    let asked = tracker.asked.lock().unwrap().clone();
    assert_eq!(
        asked.first(),
        Some(&parent.id()),
        "the first pre-check read the parent"
    );
    assert!(
        asked.contains(&funding.id()),
        "the second read the new proof: {asked:?}"
    );
    let beef = Beef::from_binary(result.input_beef.as_ref().expect("a BEEF")).unwrap();
    assert!(
        beef.find_txid(&funding.id())
            .and_then(|t| t.bump_index())
            .is_some(),
        "the funding transaction rides with its new proof"
    );
    assert!(
        beef.find_txid(&parent.id()).is_none(),
        "its parent is not needed"
    );
    assert_eq!(
        count(
            &storage,
            "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
        )
        .await,
        1
    );
}

/// One proof a `StagedProvingTracker` stores: (trigger root, txid, raw
/// tx, proof, height).
type ProofStage = (String, String, Vec<u8>, Vec<u8>, u32);

/// Stores the next proof of `stages` each time its trigger root is
/// asked (the pre-check, no lock held): the monitor storing a proof
/// between each pre-check and its write lock.
struct StagedProvingTracker {
    writer: sqlx::SqlitePool,
    stages: Vec<ProofStage>,
    stored: std::sync::Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl ChainTracker for StagedProvingTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        _height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        for (when, txid, raw, proof, height) in &self.stages {
            if when == root && self.stored.lock().unwrap().insert(txid.clone()) {
                let now = Utc::now();
                sqlx::query(
                            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
                             VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
                        )
                        .bind(txid)
                        .bind(*height as i64)
                        .bind("ab".repeat(32))
                        .bind(txid)
                        .bind(proof)
                        .bind(raw)
                        .bind(now)
                        .bind(now)
                        .execute(&self.writer)
                        .await
                        .expect("the writer is free during the pre-check");
            }
        }
        Ok(true)
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(100_000)
    }
}

/// A root left unread by the pre-check twice (a proof stored after
/// each pre-check): the store did not move under the lock, the header
/// service was not read for that root. The call refuses with the
/// service-class error naming the height (a host's 502), never
/// `StoreBusy`, and the action wrote nothing.
#[tokio::test]
async fn a_root_unread_twice_refuses_as_a_service_error_naming_its_height() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let grandparent = tx_spending(None, 0x61, 70_000);
    seed_known(&storage, user_id, &grandparent, true).await;
    let parent = tx_spending(Some(&grandparent.id()), 0x62, 60_000);
    seed_known(&storage, user_id, &parent, false).await;
    let funding = tx_spending(Some(&parent.id()), 0x63, 50_000);
    seed_change_coin(&storage, user_id, &funding, false).await;

    let tracker = StagedProvingTracker {
        writer: storage.writer().clone(),
        stages: vec![
            (
                grandparent.id(),
                parent.id(),
                parent.to_binary(),
                bump_for(&parent.id(), 901).to_binary(),
                901,
            ),
            (
                parent.id(),
                funding.id(),
                funding.to_binary(),
                bump_for(&funding.id(), 902).to_binary(),
                902,
            ),
        ],
        stored: Default::default(),
    };
    let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect_err("the second pre-check left a root unread");
    match &err {
        Error::ServiceError(text) => assert!(
            text.contains("at height 902") && text.contains(&funding.id()),
            "{text}"
        ),
        other => panic!("expected the service-class refusal, got {other:?}"),
    }
    assert!(err.to_string().starts_with("Service error: "), "{err}");
    assert_eq!(
        count(
            &storage,
            "SELECT COUNT(*) FROM transactions WHERE description = 'the door in one step'"
        )
        .await,
        0,
        "the action wrote nothing"
    );
}

/// The only coin is taken before the write lock: the step rolls
/// back, the pre-check finds nothing to fund the action, and the call
/// refuses with insufficient funds, nothing written by it.
#[tokio::test]
async fn a_coin_taken_after_the_precheck_with_none_left_refuses_with_nothing_written() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let only = tx_spending(None, 0x41, 50_000);
    let coin = seed_change_coin(&storage, user_id, &only, true).await;
    let outsider: i64 =
        sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE txid = ?")
            .bind(only.id())
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let tracker = TakingTracker {
        writer: storage.writer().clone(),
        when: only.id(),
        coin,
        by: outsider,
        taken: Default::default(),
        asked: Default::default(),
    };
    let mut before = the_store(&storage).await;

    let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect_err("no coin is left");
    assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");

    // Only the taker's write moved anything: the coin it took.
    before[0] = Some(format!("{coin}:0:{outsider}"));
    assert_eq!(the_store(&storage).await, before);
}
