use super::*;

// =====================================================================
// No tracker await under the write lock (bsv-wallet-cli #44)
// =====================================================================

/// Answers every root as `refuse` says and records, for each ask,
/// whether the storage's writer connection was held at that moment.
struct ProbeTracker {
    writer: sqlx::SqlitePool,
    refuse_height: Option<u32>,
    asked: std::sync::atomic::AtomicUsize,
    asked_under_lock: std::sync::atomic::AtomicUsize,
}

impl ProbeTracker {
    fn new(storage: &StorageSqlx, refuse_height: Option<u32>) -> Self {
        Self {
            writer: storage.writer().clone(),
            refuse_height,
            asked: Default::default(),
            asked_under_lock: Default::default(),
        }
    }
    fn asked(&self) -> usize {
        self.asked.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn asked_under_lock(&self) -> usize {
        self.asked_under_lock
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ChainTracker for ProbeTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        use std::sync::atomic::Ordering::SeqCst;
        self.asked.fetch_add(1, SeqCst);
        // The writer is one connection. Held by the caller that is
        // awaiting this answer, it can never be acquired here; a
        // connection on its way back to the pool is, at once.
        let probe =
            tokio::time::timeout(std::time::Duration::from_secs(2), self.writer.acquire()).await;
        if !matches!(probe, Ok(Ok(_))) {
            self.asked_under_lock.fetch_add(1, SeqCst);
        }
        Ok(self.refuse_height != Some(height))
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(100_000)
    }
}

/// Control: the probe sees a writer held by an open write transaction.
#[tokio::test]
async fn control_the_probe_sees_a_held_writer() {
    let (storage, _) = setup().await;
    let tracker = ProbeTracker::new(&storage, None);
    let tx = storage.write_tx().await.unwrap();
    tracker.is_valid_root_for_height("00", 1).await.unwrap();
    assert_eq!(tracker.asked_under_lock(), 1);
    tx.commit().await.unwrap();
    tracker.is_valid_root_for_height("00", 1).await.unwrap();
    assert_eq!(tracker.asked_under_lock(), 1, "free again after the commit");
}

/// The caller's root (height 800) and the stored proof's (900) are
/// both read against the tracker, and never while the writer is held.
#[tokio::test]
async fn the_tracker_is_never_asked_while_the_writer_is_held() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let known = tx_spending(None, 0xcc, 5_000);
    seed_known(&storage, user_id, &known, true).await;

    let tracker = ProbeTracker::new(&storage, None);
    create_action_internal(
        &storage,
        Some(&tracker),
        user_id,
        args(
            vec![input(&external.id()), input(&known.id())],
            caller_beef(&external, &[known.id()]),
        ),
    )
    .await
    .expect("the action is created");
    assert!(tracker.asked() >= 2, "asked {}", tracker.asked());
    assert_eq!(tracker.asked_under_lock(), 0);
}

/// A caller's root the tracker refuses is refused before the write
/// transaction opens: nothing is allocated, nothing is written.
#[tokio::test]
async fn a_caller_root_the_tracker_refuses_is_refused_before_any_allocation() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let known = tx_spending(None, 0xcc, 5_000);
    seed_known(&storage, user_id, &known, true).await;

    let tracker = ProbeTracker::new(&storage, Some(800));
    let err = create_action_internal(
        &storage,
        Some(&tracker),
        user_id,
        args(
            vec![input(&external.id()), input(&known.id())],
            caller_beef(&external, &[known.id()]),
        ),
    )
    .await
    .expect_err("the caller's root is refused");
    assert!(err.to_string().contains("invalid merkle root"), "{err}");
    assert_eq!(tracker.asked(), 1, "only the caller's root was asked");
    assert_eq!(tracker.asked_under_lock(), 0);
    assert_eq!(
        count(&storage, "SELECT COUNT(*) FROM transactions").await,
        1
    );
    assert_eq!(
        count(
            &storage,
            "SELECT COUNT(*) FROM outputs WHERE spent_by IS NOT NULL"
        )
        .await,
        0
    );
}

/// Option B (the captain's ruling of 2026-10-21): a BEEF failure is a
/// refusal, and the action writes nothing. The only change coin's
/// funding transaction is unproven and its parent is not stored;
/// createAction asks no provider (the services here would answer),
/// skips the coin, and refuses `Unspendable coin:`, naming the coin
/// and the parent. The coins, the action's row, its label, tag and
/// basket, their links and the sequence counter are as they were.
#[tokio::test]
async fn a_beef_failure_refuses_with_nothing_written() {
    use crate::services::mock::{MockResponse, MockWalletServices};
    use crate::services::traits::GetRawTxResult;
    use crate::storage::traits::WalletStorageProvider;

    let (storage, user_id) = setup().await;
    let parent = tx_spending(None, 0x11, 60_000);
    let funding = tx_spending(Some(&parent.id()), 0x22, 50_000);
    seed_change_coin(&storage, user_id, &funding, false).await;
    let services = std::sync::Arc::new(
        MockWalletServices::builder()
            .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
                name: "mock".to_string(),
                txid: parent.id(),
                raw_tx: Some(parent.to_binary()),
                error: None,
                could_not_look: false,
            }))
            .build(),
    );
    storage.set_services(services.clone());
    let before = the_store(&storage).await;

    let tracker = ProbeTracker::new(&storage, None);
    let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect_err("the parent is not stored");
    let msg = err.to_string();
    assert!(
        msg.starts_with(&format!("Unspendable coin: {}.0: ", funding.id())),
        "the refusal names the wallet's coin: {msg}"
    );
    assert!(
        msg.contains(&format!("ancestor(s) not stored: [{}]", parent.id())),
        "the refusal names the missing ancestor: {msg}"
    );
    assert_eq!(services.call_count("get_raw_tx"), 0, "no provider asked");
    assert_eq!(
        services.call_count("get_merkle_path"),
        0,
        "no provider asked"
    );
    assert_eq!(tracker.asked_under_lock(), 0);
    assert_eq!(the_store(&storage).await, before, "nothing written");
}
