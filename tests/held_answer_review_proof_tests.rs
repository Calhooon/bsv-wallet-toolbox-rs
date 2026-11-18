//! THE STATUS REVIEW COMPLETES ONLY ON A PROOF THIS STORE CHECKED (the
//! keeper's readings 19 and 23 of section 30).
//!
//! The monitor's status review (its check 3) reads a request `completed`
//! with a proof linked beside a row of its txid that is not `completed`.
//! It completes that row only where this store holds the root-check record
//! of the linked proof; otherwise it asks the chain tracker (no lock held)
//! and then, in one write that reads every row again: confirmed, the record
//! and the completion; refuted, the demotion of an unchecked stored proof,
//! the `failed` row staying `failed` with its coins as they were; no
//! tracker or a tracker fault, nothing written.
//!
//! The state: the sender's action, aborted (`failed`, its inputs released,
//! its change closed), beside a request `completed` linked to a proof row
//! with no root-check record, written directly or by the store's own sync.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use abort_take::*;
use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath};
use bsv_wallet_toolbox_rs::storage::entities::{
    ProvenTxReqStatus, TableProvenTx, TableProvenTxReq,
};
use bsv_wallet_toolbox_rs::storage::{RequestSyncChunkArgs, SyncChunk};
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, WalletStorageSync, WalletStorageWriter};

const REFUTE: u8 = 0;
const CONFIRM: u8 = 1;
const FAULT: u8 = 2;
/// Confirms, after moving the sender's row while it answers.
const MOVE: u8 = 3;

/// A chain tracker whose answer the cell sets; it counts its asks.
struct Switch {
    mode: AtomicU8,
    asks: AtomicU8,
    store: tokio::sync::OnceCell<(StorageSqlx, String)>,
}

#[async_trait]
impl ChainTracker for Switch {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        self.asks.fetch_add(1, Ordering::SeqCst);
        match self.mode.load(Ordering::SeqCst) {
            REFUTE => Ok(false),
            CONFIRM => Ok(true),
            MOVE => {
                // Asked with no lock held: a write lands meanwhile.
                let (store, txid) = self.store.get().unwrap();
                sqlx::query(
                    "UPDATE transactions SET status = 'unproven' \
                     WHERE txid = ? AND is_outgoing = 1",
                )
                .bind(txid)
                .execute(store.writer())
                .await
                .unwrap();
                self.mode.store(CONFIRM, Ordering::SeqCst);
                Ok(true)
            }
            _ => Err(ChainTrackerError::NetworkError("the cell's fault".into())),
        }
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(PROVEN_HEIGHT + 100)
    }
}

/// What a pass leaves: the sender's reading, the proof row, its record.
#[derive(Debug, Clone, PartialEq, Eq)]
struct After {
    reading: Reading,
    proof_rows: i64,
    root_checks: i64,
    linked: Option<i64>,
    proven_over_failed: bool,
}

async fn after(f: &Fixture) -> After {
    let store = &f.store;
    let count = |sql: &'static str| async move {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(store.pool())
            .await
            .unwrap()
    };
    let (linked, history): (Option<i64>, String) =
        sqlx::query_as("SELECT proven_tx_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(&f.txid)
            .fetch_one(store.pool())
            .await
            .unwrap();
    After {
        reading: read(f, false).await,
        proof_rows: count("SELECT COUNT(*) FROM proven_txs").await,
        root_checks: count("SELECT COUNT(*) FROM proof_root_checks").await,
        linked,
        proven_over_failed: history.contains("provenOverFailed"),
    }
}

/// The aborted action and its tracker, before the proof row arrives.
async fn aborted(mode: u8) -> (Fixture, Arc<Switch>, common::SpendParts) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("review-proof", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    let tracker = Arc::new(Switch {
        mode: AtomicU8::new(mode),
        asks: AtomicU8::new(0),
        store: tokio::sync::OnceCell::new(),
    });
    tracker
        .store
        .set((store.clone(), parts.txid.clone()))
        .ok()
        .unwrap();
    store.set_chain_tracker(tracker.clone()).await;
    let f = seed(&store, &parts, Cause::AbortFree).await;
    abort_now(&f, Cause::AbortFree).await;
    (f, tracker, parts)
}

/// The proof row and the request's link as written with no root check.
async fn unchecked_proof(f: &Fixture, raw: &[u8]) {
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    let ptx = sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, ?, 0, ?, ?, ?, ?)",
    )
    .bind(&f.txid)
    .bind(PROVEN_HEIGHT)
    .bind("bb".repeat(32))
    .bind(path.compute_root(Some(&f.txid)).unwrap())
    .bind(path.to_binary().unwrap())
    .bind(raw)
    .execute(f.store.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ? WHERE txid = ?")
        .bind(ptx)
        .bind(&f.txid)
        .execute(f.store.writer())
        .await
        .unwrap();
}

fn released() -> Vec<(bool, String)> {
    vec![(true, "none".to_string()), (true, "none".to_string())]
}

fn own() -> Vec<(bool, String)> {
    vec![(false, "own".to_string()), (false, "own".to_string())]
}

/// Refuted: the demotion. The `failed` row stays `failed`, its inputs
/// released and its change closed as the abort left them (balance 2000:
/// the released coins, no change); the request `unmined` with no link;
/// the proof row and any record gone.
fn assert_refuted(got: &After) {
    let r = &got.reading;
    assert_eq!(r.row.as_deref(), Some("failed"), "{got:?}");
    assert_eq!(r.request.as_deref(), Some("unmined"), "{got:?}");
    assert_eq!(r.inputs, released(), "{got:?}");
    assert_eq!(r.own_output_spendable, Some(false), "{got:?}");
    assert_eq!(r.balance, 2000, "{got:?}");
    assert_eq!((got.proof_rows, got.root_checks, got.linked), (0, 0, None));
    assert!(!got.proven_over_failed);
}

/// Confirmed: the record and the completion in one write.
fn assert_completed(got: &After) {
    let r = &got.reading;
    assert_eq!(r.row.as_deref(), Some("completed"), "{got:?}");
    assert_eq!(r.request.as_deref(), Some("completed"), "{got:?}");
    assert_eq!(r.inputs, own(), "{got:?}");
    assert_eq!(r.own_output_spendable, Some(true), "{got:?}");
    assert_eq!(r.balance, 500, "{got:?}");
    assert_eq!((got.proof_rows, got.root_checks), (1, 1), "{got:?}");
    assert!(got.proven_over_failed);
}

/// The read's cell: a proof row this store never checked, the tracker
/// refuting its root. The review moves no coin of the `failed` row.
#[tokio::test]
async fn the_review_moves_no_coin_on_a_proof_the_tracker_refutes() {
    let (f, tracker, parts) = aborted(REFUTE).await;
    unchecked_proof(&f, &parts.raw).await;
    let review = MonitorStorage::review_status(&f.store).await.unwrap();
    let got = after(&f).await;
    println!("refuted: {got:?}\nlog: {}", review.log.trim());
    assert_refuted(&got);
    assert_eq!(tracker.asks.load(Ordering::SeqCst), 1);
    // The next pass reads nothing to do.
    MonitorStorage::review_status(&f.store).await.unwrap();
    assert_eq!(after(&f).await, got);
    assert_eq!(tracker.asks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_review_completes_on_a_proof_the_tracker_confirms() {
    let (f, tracker, parts) = aborted(CONFIRM).await;
    unchecked_proof(&f, &parts.raw).await;
    MonitorStorage::review_status(&f.store).await.unwrap();
    let got = after(&f).await;
    println!("confirmed: {got:?}");
    assert_completed(&got);
    assert_eq!(tracker.asks.load(Ordering::SeqCst), 1);
}

/// With no tracker, and with a tracker that faults, nothing is written,
/// and the same at the next pass.
#[tokio::test]
async fn the_review_writes_nothing_with_no_tracker_or_a_fault() {
    for no_tracker in [true, false] {
        let (f, _tracker, parts) = aborted(FAULT).await;
        if no_tracker {
            f.store.clear_chain_tracker().await;
        }
        unchecked_proof(&f, &parts.raw).await;
        let before = after(&f).await;
        for pass in 0..2 {
            MonitorStorage::review_status(&f.store).await.unwrap();
            let got = after(&f).await;
            println!("no_tracker={no_tracker} pass {pass}: {got:?}");
            assert_eq!(got, before, "no_tracker={no_tracker} pass {pass}");
            assert_eq!(got.reading.row.as_deref(), Some("failed"));
            assert_eq!(got.reading.inputs, released());
        }
    }
}

/// A row that moves while the tracker answers: nothing is written in that
/// pass; the next pass reads the rows afresh.
#[tokio::test]
async fn the_review_writes_nothing_when_a_row_moved_while_the_tracker_answered() {
    let (f, tracker, parts) = aborted(MOVE).await;
    unchecked_proof(&f, &parts.raw).await;
    MonitorStorage::review_status(&f.store).await.unwrap();
    let got = after(&f).await;
    println!("moved meanwhile: {got:?}");
    // Only the cell's own write: the row `unproven`, no record, no completion.
    assert_eq!(got.reading.row.as_deref(), Some("unproven"));
    assert_eq!(got.reading.request.as_deref(), Some("completed"));
    assert_eq!(got.reading.inputs, released());
    assert_eq!((got.proof_rows, got.root_checks), (1, 0));
    assert!(!got.proven_over_failed);
    MonitorStorage::review_status(&f.store).await.unwrap();
    let next = after(&f).await;
    println!("next pass: {next:?}");
    assert_eq!(next.reading.row.as_deref(), Some("completed"));
    assert_eq!(next.root_checks, 1);
    assert_eq!(tracker.asks.load(Ordering::SeqCst), 2);
}

/// The same state reached through the store's own sync: a chunk carrying
/// the proof row (no root check recorded), then one carrying the request
/// `completed` with its link (the request's link must name a row already
/// on the store: a chunk processes its requests ahead of its proof rows).
/// Refuted: the demotion; confirmed: the completion.
#[tokio::test]
async fn a_synced_request_and_proof_row_complete_only_on_a_checked_root() {
    for mode in [REFUTE, CONFIRM] {
        let (f, _tracker, parts) = aborted(mode).await;
        let identity = PrivateKey::random().public_key().to_hex();
        let args = RequestSyncChunkArgs {
            from_storage_identity_key: "elsewhere".into(),
            to_storage_identity_key: "here".into(),
            identity_key: identity.clone(),
            since: None,
            max_rough_size: 100_000,
            max_items: 100,
            offsets: vec![],
        };
        let now = chrono::Utc::now();
        let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
        let chunk = SyncChunk {
            from_storage_identity_key: "elsewhere".into(),
            to_storage_identity_key: "here".into(),
            user_identity_key: identity,
            proven_txs: Some(vec![TableProvenTx {
                proven_tx_id: 1,
                txid: f.txid.clone(),
                height: i64::from(PROVEN_HEIGHT),
                index: 0,
                block_hash: "bb".repeat(32),
                merkle_root: path.compute_root(Some(&f.txid)).unwrap(),
                merkle_path: path.to_binary().unwrap(),
                raw_tx: parts.raw.clone(),
                created_at: now,
                updated_at: now,
            }]),
            ..Default::default()
        };
        f.store
            .process_sync_chunk(args.clone(), chunk.clone())
            .await
            .unwrap();
        let chunk = SyncChunk {
            proven_txs: None,
            proven_tx_reqs: Some(vec![TableProvenTxReq {
                proven_tx_req_id: 9,
                txid: f.txid.clone(),
                status: ProvenTxReqStatus::Completed,
                attempts: 0,
                history: "{}".into(),
                notified: false,
                notify: "{}".into(),
                raw_tx: Some(parts.raw.clone()),
                input_beef: None,
                // The proof row the chunk before wrote (id 1 on the fresh store).
                proven_tx_id: Some(1),
                batch: None,
                created_at: now,
                updated_at: now,
            }]),
            ..chunk
        };
        f.store.process_sync_chunk(args, chunk).await.unwrap();
        let synced = after(&f).await;
        println!("mode {mode} synced: {synced:?}");
        assert_eq!(synced.reading.request.as_deref(), Some("completed"));
        assert_eq!(synced.linked, Some(1));
        assert_eq!((synced.proof_rows, synced.root_checks), (1, 0));
        MonitorStorage::review_status(&f.store).await.unwrap();
        let got = after(&f).await;
        println!("mode {mode} reviewed: {got:?}");
        if mode == REFUTE {
            assert_refuted(&got);
        } else {
            assert_completed(&got);
        }
    }
}

/// A proof this store checked completes with no ask of the tracker.
#[tokio::test]
async fn a_checked_proof_completes_with_no_ask() {
    let (f, tracker, parts) = aborted(FAULT).await;
    unchecked_proof(&f, &parts.raw).await;
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    sqlx::query(
        "INSERT INTO proof_root_checks (txid, height, merkle_root, checked_at) VALUES (?, ?, ?, ?)",
    )
    .bind(&f.txid)
    .bind(PROVEN_HEIGHT)
    .bind(path.compute_root(Some(&f.txid)).unwrap())
    .bind(chrono::Utc::now())
    .execute(f.store.writer())
    .await
    .unwrap();
    MonitorStorage::review_status(&f.store).await.unwrap();
    let got = after(&f).await;
    assert_completed(&got);
    assert_eq!(tracker.asks.load(Ordering::SeqCst), 0);
}
