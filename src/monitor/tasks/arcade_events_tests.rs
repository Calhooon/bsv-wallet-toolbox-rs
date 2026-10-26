//! Real-store witnesses for the event handler's busy and fallback boundaries.

#![cfg(test)]

use super::*;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MockChainTracker};
use sqlx::{Connection, SqliteConnection};

use crate::services::mock::MockWalletServices;
use crate::storage::StorageSqlx;
use crate::{Error, StoreBusyReason, WalletStorageProvider, WalletStorageWriter};

const HEIGHT: u32 = 850_000;

struct Scene {
    storage: StorageSqlx,
    other: Arc<tokio::sync::Mutex<SqliteConnection>>,
    txid: String,
    _dir: tempfile::TempDir,
}

impl Scene {
    async fn new() -> Self {
        // The two-connection WAL pattern from tests/store_write_lock_tests.rs.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.db");
        let storage = StorageSqlx::open(path.to_str().unwrap()).await.unwrap();
        storage.migrate("events", &"02".repeat(33)).await.unwrap();
        storage.make_available().await.unwrap();
        storage
            .set_max_acceptable_proof_height(HEIGHT)
            .await
            .unwrap();
        // Only shorten the test's SQLite wait; the error still comes from a
        // competing connection's real write lock, with no injected busy error.
        sqlx::query("PRAGMA busy_timeout = 50")
            .execute(storage.writer())
            .await
            .unwrap();
        let other = SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(&path),
        )
        .await
        .unwrap();

        let txid = "ab".repeat(32);
        let (user, _) = storage.find_or_insert_user(&"03".repeat(33)).await.unwrap();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
             VALUES (?, 'unmined', 0, '{}', 0, '{}', X'01000000', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
             VALUES (?, ?, 'unproven', 'event-test', 'event test', 0, 1, 0, X'01000000', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(user.user_id)
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();
        // A replay with an already remembered MINED hint needs no status writes.
        // The proof is still pending, so it must reach the ingest write boundary.
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, "mined")
            .await
            .unwrap();
        Self {
            storage,
            other: Arc::new(tokio::sync::Mutex::new(other)),
            txid,
            _dir: dir,
        }
    }

    fn event(&self, status: &str) -> ArcadeStatusEvent {
        let txid = self.txid.as_str();
        ArcadeStatusEvent {
            txid: txid.to_string(),
            tx_status: status.to_string(),
            timestamp: None,
            block_hash: Some("cd".repeat(32)),
            block_height: Some(HEIGHT),
            merkle_path: Some(MerklePath::from_coinbase_txid(txid, HEIGHT).to_hex()),
            extra_info: None,
            status_code: None,
            competing_txs: None,
            event_id: None,
        }
    }

    async fn set_sending(&self) {
        for table in ["proven_tx_reqs", "transactions"] {
            sqlx::query(&format!("UPDATE {table} SET status = 'sending'"))
                .execute(self.storage.writer())
                .await
                .unwrap();
        }
    }

    async fn lock_at_proof_check(&self) -> Arc<LockAtProofCheck> {
        let tracker = Arc::new(LockAtProofCheck {
            other: self.other.clone(),
            root: self.txid.clone(),
            checked: AtomicBool::new(false),
        });
        self.storage.set_chain_tracker(tracker.clone()).await;
        tracker
    }

    async fn accept_proof(&self) {
        let mut tracker = MockChainTracker::new(HEIGHT);
        tracker.add_root(HEIGHT, self.txid.clone());
        self.storage.set_chain_tracker(Arc::new(tracker)).await;
    }
}

async fn writer_changes(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(storage.writer())
        .await
        .unwrap()
}

async fn assert_pending(storage: &StorageSqlx, writes_before: i64) {
    assert_eq!(
        writer_changes(storage).await,
        writes_before,
        "no rows written"
    );
    let req: (String, Option<i64>) =
        sqlx::query_as("SELECT status, proven_tx_id FROM proven_tx_reqs")
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let tx: (String, Option<i64>) = sqlx::query_as("SELECT status, proven_tx_id FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let proofs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proven_txs")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(req, ("unmined".to_string(), None));
    assert_eq!(tx, ("unproven".to_string(), None));
    assert_eq!(proofs, 0);
}

async fn unchanged_event_fields(storage: &StorageSqlx) -> Vec<String> {
    // Only status, the proof link and updated_at may change on delivery.
    let mut rows = Vec::new();
    for query in [
        "SELECT json_array(proven_tx_req_id, txid, attempts, history, notified, notify, \
         quote(raw_tx), quote(input_beef), batch, created_at) FROM proven_tx_reqs",
        "SELECT json_array(transaction_id, user_id, txid, reference, description, satoshis, \
         version, lock_time, quote(raw_tx), quote(input_beef), is_outgoing, created_at) FROM transactions",
    ] {
        rows.push(
            sqlx::query_scalar(query)
                .fetch_one(storage.pool())
                .await
                .unwrap(),
        );
    }
    rows
}

async fn proof_rows(storage: &StorageSqlx) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(proven_tx_id, txid, height, idx, block_hash, merkle_root, \
         hex(merkle_path), hex(raw_tx), created_at, updated_at) FROM proven_txs ORDER BY proven_tx_id",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap()
}

async fn assert_completed(storage: &StorageSqlx) {
    let proof_id: i64 = sqlx::query_scalar("SELECT proven_tx_id FROM proven_txs")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    for table in ["proven_tx_reqs", "transactions"] {
        let row: (String, Option<i64>) =
            sqlx::query_as(&format!("SELECT status, proven_tx_id FROM {table}"))
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(row, ("completed".to_string(), Some(proof_id)));
    }
}

struct LockAtProofCheck {
    other: Arc<tokio::sync::Mutex<SqliteConnection>>,
    root: String,
    checked: AtomicBool,
}

#[async_trait]
impl ChainTracker for LockAtProofCheck {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        assert_eq!((root, height), (self.root.as_str(), HEIGHT));
        // Taking the lock before apply_event would stop at its status write
        // and never exercise the MINED arm's ingest error handler (#201).
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *self.other.lock().await)
            .await
            .unwrap();
        self.checked.store(true, Ordering::SeqCst);
        Ok(true)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(HEIGHT)
    }
}

fn is_inline_proof_busy(result: &crate::Result<bool>) -> bool {
    matches!(
        result,
        Err(Error::StoreBusy {
            reason: StoreBusyReason::SqliteBusy { code: 5 }
        })
    )
}

async fn assert_fetch_fallback(storage: &StorageSqlx, ev: &ArcadeStatusEvent) {
    let trigger = AtomicBool::new(false);
    let writes_before = writer_changes(storage).await;
    assert!(!ArcadeEventsTask::apply_event(storage, ev, &trigger)
        .await
        .unwrap());
    assert!(trigger.load(Ordering::SeqCst));
    assert_pending(storage, writes_before).await;
}

#[tokio::test]
async fn replayed_mined_inline_proof_keeps_store_busy_without_fetch() {
    for status in ["MINED", "IMMUTABLE"] {
        let scene = Scene::new().await;
        let storage = &scene.storage;
        let services = Arc::new(MockWalletServices::new());
        storage.set_services(services.clone());
        let tracker = scene.lock_at_proof_check().await;
        let trigger = AtomicBool::new(false);
        let writes_before = writer_changes(storage).await;

        let result = ArcadeEventsTask::apply_event(storage, &scene.event(status), &trigger).await;

        sqlx::query("ROLLBACK")
            .execute(&mut *tracker.other.lock().await)
            .await
            .unwrap();
        assert!(tracker.checked.load(Ordering::SeqCst), "ingest was reached");
        assert_pending(storage, writes_before).await;
        assert!(
            is_inline_proof_busy(&result),
            "{status}: result={result:?}, fetch_fallback={}",
            trigger.load(Ordering::SeqCst)
        );
        assert!(!trigger.load(Ordering::SeqCst), "no fetch fallback");
        assert_eq!(services.total_calls(), 0);
    }
}

#[tokio::test]
async fn mined_inline_proof_busy_after_sending_status_commit_retries_idempotently() {
    for status in ["MINED", "IMMUTABLE"] {
        let scene = Scene::new().await;
        let storage = &scene.storage;
        scene.set_sending().await;
        let services = Arc::new(MockWalletServices::new());
        storage.set_services(services.clone());
        let tracker = scene.lock_at_proof_check().await;
        let trigger = AtomicBool::new(false);
        let ev = scene.event(status);
        let fields_before = unchanged_event_fields(storage).await;
        let writes_before = writer_changes(storage).await;

        let result = ArcadeEventsTask::apply_event(storage, &ev, &trigger).await;

        assert!(is_inline_proof_busy(&result), "{status}: {result:?}");
        assert!(tracker.checked.load(Ordering::SeqCst), "ingest was reached");
        // Read while the other connection still holds the writer: both
        // status updates committed before the proof ingest met the busy.
        assert_pending(storage, writes_before + 2).await;
        assert_eq!(unchanged_event_fields(storage).await, fields_before);
        assert!(!trigger.load(Ordering::SeqCst), "no fetch fallback");
        assert_eq!(services.total_calls(), 0);

        sqlx::query("ROLLBACK")
            .execute(&mut *tracker.other.lock().await)
            .await
            .unwrap();
        scene.accept_proof().await;

        assert!(ArcadeEventsTask::apply_event(storage, &ev, &trigger)
            .await
            .unwrap());
        assert_completed(storage).await;
        assert_eq!(unchanged_event_fields(storage).await, fields_before);
        let stored_proofs = proof_rows(storage).await;
        assert_eq!(stored_proofs.len(), 1, "the retry stores one proof");

        // A duplicate delivery preserves the proof, its links and every
        // unrelated field; completion timestamps may be refreshed.
        assert!(ArcadeEventsTask::apply_event(storage, &ev, &trigger)
            .await
            .unwrap());
        assert_completed(storage).await;
        assert_eq!(proof_rows(storage).await, stored_proofs);
        assert_eq!(unchanged_event_fields(storage).await, fields_before);
        assert!(!trigger.load(Ordering::SeqCst), "no fetch fallback");
        assert_eq!(services.total_calls(), 0);
    }
}

#[tokio::test]
async fn status_writes_keep_store_busy_without_fetch() {
    for status in [
        "SEEN_ON_NETWORK",
        "STUMP_PROCESSING",
        "MINED",
        "REJECTED",
        "DOUBLE_SPEND_ATTEMPTED",
    ] {
        let scene = Scene::new().await;
        let storage = &scene.storage;
        let mut other = scene.other.lock().await;
        let mut ev = scene.event(status);
        if status == "DOUBLE_SPEND_ATTEMPTED" {
            ev.competing_txs = Some(vec!["ef".repeat(32)]);
        }
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *other)
            .await
            .unwrap();
        let trigger = AtomicBool::new(false);
        let writes_before = writer_changes(storage).await;
        let result = ArcadeEventsTask::apply_event(storage, &ev, &trigger).await;
        sqlx::query("ROLLBACK").execute(&mut *other).await.unwrap();
        assert!(
            matches!(result, Err(Error::StoreBusy { .. })),
            "{status}: {result:?}"
        );
        assert_pending(storage, writes_before).await;
        assert!(
            !trigger.load(Ordering::SeqCst),
            "{status}: no fetch fallback"
        );
    }
}

#[tokio::test]
async fn non_busy_inline_ingest_error_keeps_fetch_fallback() {
    let scene = Scene::new().await;
    let storage = &scene.storage;
    scene.accept_proof().await;
    sqlx::query(
        "CREATE TRIGGER refuse_proof BEFORE INSERT ON proven_txs \
         BEGIN SELECT RAISE(ABORT, 'test ingest fault'); END",
    )
    .execute(storage.writer())
    .await
    .unwrap();
    let ev = scene.event("MINED");
    // Prove this is the Err branch, rather than a rejected-proof outcome.
    let error = storage
        .ingest_push_proof(
            &scene.txid,
            &hex::decode(ev.merkle_path.as_ref().unwrap()).unwrap(),
            HEIGHT,
            ev.block_hash.as_ref().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::SqlxError(_)), "{error:?}");

    assert_fetch_fallback(storage, &ev).await;
}

#[tokio::test]
async fn malformed_inline_proof_keeps_fetch_fallback() {
    let scene = Scene::new().await;
    let storage = &scene.storage;
    let mut ev = scene.event("MINED");
    ev.merkle_path = Some("dead".to_string());
    assert_fetch_fallback(storage, &ev).await;
}
