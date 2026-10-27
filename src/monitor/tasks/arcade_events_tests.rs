//! Real-store witnesses for the event handler's busy and fallback boundaries.

#![cfg(test)]

use super::*;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MockChainTracker};
use sqlx::{Connection, SqliteConnection};
use std::time::Instant;

use crate::services::mock::MockWalletServices;
use crate::storage::StorageSqlx;
use crate::{Error, StoreBusyReason, WalletStorageProvider, WalletStorageWriter};

const HEIGHT: u32 = 850_000;
const STATUS_EVENTS: [&str; 5] = [
    "SEEN_ON_NETWORK",
    "SEEN_MULTIPLE_NODES",
    "STUMP_PROCESSING",
    "MINED",
    "IMMUTABLE",
];

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

        let started = Instant::now();
        let result = ArcadeEventsTask::apply_event(storage, &scene.event(status), &trigger).await;
        eprintln!(
            "{status} replay inline ingest: elapsed={:?}, timeout=50ms",
            started.elapsed()
        );

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

        let started = Instant::now();
        let result = ArcadeEventsTask::apply_event(storage, &ev, &trigger).await;
        eprintln!(
            "{status} after status commit inline ingest: elapsed={:?}, timeout=50ms",
            started.elapsed()
        );

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

async fn status_words(storage: &StorageSqlx) -> Vec<String> {
    let mut words = Vec::new();
    for table in ["proven_tx_reqs", "transactions"] {
        words.push(
            sqlx::query_scalar(&format!("SELECT status FROM {table}"))
                .fetch_one(storage.pool())
                .await
                .unwrap(),
        );
    }
    words
}

#[derive(Debug, PartialEq, Eq)]
struct StoredEventState {
    writes: i64,
    words: Vec<String>,
    fields: Vec<String>,
    proofs: Vec<String>,
    memory: Vec<crate::services::broadcast_memory::BroadcastSeenRecord>,
}

async fn stored_event_state(storage: &StorageSqlx, txid: &str) -> StoredEventState {
    StoredEventState {
        writes: writer_changes(storage).await,
        words: status_words(storage).await,
        fields: unchanged_event_fields(storage).await,
        proofs: proof_rows(storage).await,
        memory: storage
            .broadcast_records(None, &[txid.to_string()])
            .await
            .unwrap(),
    }
}

/// The writer handouts `apply_event` makes on a busy store: each is one
/// arm that waits `busy_timeout` at its first write, so the count is the
/// number of busy waits without reading a clock (#244).
async fn count_status_busy_arms(status: &str, known: bool) -> usize {
    let timeout = Duration::from_millis(200);
    let scene = Scene::new().await;
    let storage = &scene.storage;
    scene.set_sending().await;
    let services = Arc::new(MockWalletServices::new());
    storage.set_services(services.clone());
    let mut ev = scene.event(status);
    if !known {
        ev.txid = "ef".repeat(32);
        ev.merkle_path = Some(MerklePath::from_coinbase_txid(&ev.txid, HEIGHT).to_hex());
    }
    sqlx::query("PRAGMA busy_timeout = 200")
        .execute(storage.writer())
        .await
        .unwrap();
    let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let query_only: i64 = sqlx::query_scalar("PRAGMA query_only")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(journal, "wal");
    assert_eq!(query_only, 1);
    let before = stored_event_state(storage, &ev.txid).await;
    let mut other = scene.other.lock().await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *other)
        .await
        .unwrap();
    let trigger = AtomicBool::new(false);

    let acquires = storage.writer_acquires();
    let started = Instant::now();
    let result = ArcadeEventsTask::apply_event(storage, &ev, &trigger).await;
    let elapsed = started.elapsed();
    let arms = storage.writer_acquires() - acquires;

    sqlx::query("ROLLBACK").execute(&mut *other).await.unwrap();
    eprintln!(
        "{status} known={known}: arms={arms}, elapsed={elapsed:?}, timeout={timeout:?}, result={result:?}"
    );
    assert!(is_inline_proof_busy(&result), "{status}: {result:?}");
    assert!(!trigger.load(Ordering::SeqCst), "no fetch fallback");
    assert_eq!(services.total_calls(), 0);
    assert_eq!(
        stored_event_state(storage, &ev.txid).await,
        before,
        "no writes or status changes"
    );
    arms
}

async fn assert_status_busy_waits_once(status: &str) {
    for known in [false, true] {
        let arms = count_status_busy_arms(status, known).await;
        assert_eq!(
            arms, 1,
            "{status} known={known}: one write arm, one busy wait"
        );
    }
}

#[tokio::test]
async fn seen_status_busy_waits_once_without_writes() {
    assert_status_busy_waits_once("SEEN_ON_NETWORK").await;
}

#[tokio::test]
async fn in_block_status_busy_waits_once_without_writes() {
    assert_status_busy_waits_once("STUMP_PROCESSING").await;
}

#[tokio::test]
async fn mined_status_busy_waits_once_without_writes() {
    assert_status_busy_waits_once("MINED").await;
}

async fn unremembered_sending_scene() -> Scene {
    let scene = Scene::new().await;
    scene.set_sending().await;
    sqlx::query("DELETE FROM broadcast_seen")
        .execute(scene.storage.writer())
        .await
        .unwrap();
    scene
}

async fn status_fault_scene(trigger_sql: &str) -> Scene {
    let scene = unremembered_sending_scene().await;
    sqlx::query(trigger_sql)
        .execute(scene.storage.writer())
        .await
        .unwrap();
    scene
}

#[tokio::test]
async fn status_delivery_commits_statuses_and_hint_once() {
    for status in STATUS_EVENTS {
        let scene = unremembered_sending_scene().await;
        let commits = Arc::new(AtomicU64::new(0));
        let counted = commits.clone();
        {
            let mut conn = scene.storage.writer().acquire().await.unwrap();
            conn.lock_handle().await.unwrap().set_commit_hook(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                true
            });
        }
        // Count the status transaction only; inline proof ingestion has its
        // own transaction after the tracker check, as the #201 tests cover.
        let mut ev = scene.event(status);
        ev.merkle_path = None;
        let trigger = AtomicBool::new(false);
        assert!(ArcadeEventsTask::apply_event(&scene.storage, &ev, &trigger)
            .await
            .unwrap());
        assert_eq!(commits.load(Ordering::SeqCst), 1, "{status}");
        assert_eq!(status_words(&scene.storage).await, ["unmined", "unproven"]);
        let memory = scene
            .storage
            .broadcast_records(None, std::slice::from_ref(&scene.txid))
            .await
            .unwrap();
        assert_eq!(memory.len(), 1, "{status}");
        assert_eq!(memory[0].status, "seen", "{status}");
    }
}

async fn pause_next_req_update(
    storage: &StorageSqlx,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered, arrival) = tokio::sync::oneshot::channel();
    let (resume, release) = std::sync::mpsc::channel();
    let mut entered = Some(entered);
    let mut conn = storage.writer().acquire().await.unwrap();
    conn.lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |row| {
            if row.table == "proven_tx_reqs" {
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                    // This runs on SQLx's SQLite worker, leaving Tokio free to
                    // cancel the callback while its request update is in flight.
                    let _ = release.recv_timeout(Duration::from_secs(5));
                }
            }
        });
    (arrival, resume)
}

#[tokio::test]
async fn cancelled_status_delivery_rolls_back_statuses_and_hint() {
    for status in STATUS_EVENTS {
        let scene = unremembered_sending_scene().await;
        let fields = unchanged_event_fields(&scene.storage).await;
        let (entered, resume) = pause_next_req_update(&scene.storage).await;
        let trigger = Arc::new(AtomicBool::new(false));
        let callback_trigger = trigger.clone();
        let store = scene.storage.clone();
        let ev = scene.event(status);
        let delivery = tokio::spawn(async move {
            ArcadeEventsTask::apply_event(&store, &ev, &callback_trigger).await
        });
        let arrived = tokio::time::timeout(Duration::from_secs(5), entered).await;
        delivery.abort();
        let cancelled = delivery.await;
        let resumed = resume.send(());
        arrived.unwrap().unwrap();
        resumed.unwrap();
        assert!(cancelled.unwrap_err().is_cancelled(), "{status}");
        {
            // Wait for the SQLite worker to finish the cancelled statement
            // and the transaction's queued rollback before inspecting it.
            let mut conn = scene.storage.writer().acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_update_hook();
        }
        assert_eq!(status_words(&scene.storage).await, ["sending", "sending"]);
        assert_eq!(unchanged_event_fields(&scene.storage).await, fields);
        assert_no_broadcast_memory(&scene).await;
        assert!(proof_rows(&scene.storage).await.is_empty());
        assert!(!trigger.load(Ordering::SeqCst), "{status}");
    }
}

async fn assert_no_broadcast_memory(scene: &Scene) {
    assert!(scene
        .storage
        .broadcast_records(None, std::slice::from_ref(&scene.txid))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn seen_status_updates_roll_back_together() {
    let scene = status_fault_scene(
        "CREATE TRIGGER refuse_seen_status BEFORE UPDATE OF status ON transactions \
         BEGIN SELECT RAISE(ABORT, 'test status fault'); END",
    )
    .await;
    let storage = &scene.storage;
    let trigger = AtomicBool::new(false);

    let result =
        ArcadeEventsTask::apply_event(storage, &scene.event("SEEN_ON_NETWORK"), &trigger).await;

    assert!(matches!(result, Err(Error::SqlxError(_))), "{result:?}");
    assert_eq!(status_words(storage).await, ["sending", "sending"]);
    assert_no_broadcast_memory(&scene).await;
    assert!(!trigger.load(Ordering::SeqCst));
}

#[tokio::test]
async fn non_busy_broadcast_memory_error_keeps_status_updates() {
    let scene = status_fault_scene(
        "CREATE TRIGGER refuse_seen_memory BEFORE INSERT ON broadcast_seen \
         BEGIN SELECT RAISE(ABORT, 'test memory fault'); END",
    )
    .await;
    let storage = &scene.storage;
    let trigger = AtomicBool::new(false);

    assert!(
        ArcadeEventsTask::apply_event(storage, &scene.event("SEEN_ON_NETWORK"), &trigger)
            .await
            .unwrap()
    );

    assert_eq!(status_words(storage).await, ["unmined", "unproven"]);
    assert_no_broadcast_memory(&scene).await;
    assert!(!trigger.load(Ordering::SeqCst));
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
