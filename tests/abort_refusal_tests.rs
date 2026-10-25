//! The abort door follows stored acceptance, including a post whose status write lagged (#165).

#![cfg(feature = "sqlite")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use bsv_rs::wallet::AbortActionArgs;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::AuthId;
use bsv_wallet_toolbox_rs::{Error, StorageSqlx, WalletStorageProvider, WalletStorageWriter};
use serde_json::json;
use sqlx::{Column, Row};

const TXID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Default)]
struct CountingTracker(AtomicUsize);

#[async_trait]
impl ChainTracker for CountingTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> Result<bool, ChainTrackerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(0)
    }
}

struct Seeded {
    storage: StorageSqlx,
    auth: AuthId,
    services: Arc<MockWalletServices>,
    tracker: Arc<CountingTracker>,
    tx_id: i64,
}

impl Seeded {
    async fn new(status: &str) -> Self {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("abort-refusal", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let identity = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
        let now = chrono::Utc::now();
        let mut ids = Vec::new();
        for (reference, txid, status) in [
            ("funding", "a".repeat(64), "completed"),
            ("abort-ref", TXID.to_string(), status),
        ] {
            let id = sqlx::query(
                "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
                 description, txid, version, lock_time, created_at, updated_at) \
                 VALUES (?, ?, ?, 1, 1000, 'abort test', ?, 1, 0, ?, ?)",
            )
            .bind(user.user_id)
            .bind(status)
            .bind(reference)
            .bind(txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid();
            ids.push(id);
        }
        for (id, txid, spendable, spent_by) in [
            (ids[0], "a".repeat(64), 0, Some(ids[1])),
            (ids[1], TXID.to_string(), 1, None),
        ] {
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, spendable, \
                 change, provided_by, purpose, type, txid, spent_by, created_at, updated_at) \
                 VALUES (?, ?, 0, 1000, ?, 1, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)",
            )
            .bind(user.user_id)
            .bind(id)
            .bind(spendable)
            .bind(txid)
            .bind(spent_by)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
             raw_tx, created_at, updated_at) VALUES (?, 'unsent', 0, '{}', 0, '{}', X'00', ?, ?)",
        )
        .bind(TXID)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
        let services = Arc::new(MockWalletServices::new());
        let tracker = Arc::new(CountingTracker::default());
        storage.set_services(services.clone());
        storage.set_chain_tracker(tracker.clone()).await;
        Self {
            storage,
            auth: AuthId::with_user_id(&identity, user.user_id),
            services,
            tracker,
            tx_id: ids[1],
        }
    }

    async fn request(&self, status: &str, history: serde_json::Value) {
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, history = ? WHERE txid = ?")
            .bind(status)
            .bind(history.to_string())
            .bind(TXID)
            .execute(self.storage.writer())
            .await
            .unwrap();
    }

    async fn abort(&self) -> bsv_wallet_toolbox_rs::Result<bsv_rs::wallet::AbortActionResult> {
        self.storage
            .abort_action(
                &self.auth,
                AbortActionArgs {
                    reference: "abort-ref".to_string(),
                },
            )
            .await
    }

    fn assert_no_chain_calls(&self) {
        assert_eq!(
            self.services.total_calls(),
            0,
            "no explorer or service call"
        );
        assert_eq!(self.tracker.0.load(Ordering::SeqCst), 0, "no header call");
    }

    // Quote every column so a refusal must preserve timestamps, blobs and nulls too.
    async fn snapshot(&self) -> Vec<Vec<Vec<String>>> {
        let mut snapshot = Vec::new();
        for table in [
            "transactions",
            "outputs",
            "proven_tx_reqs",
            "proven_txs",
            "broadcast_seen",
            "locked_input_checks",
        ] {
            let columns: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                    .bind(table)
                    .fetch_all(self.storage.pool())
                    .await
                    .unwrap();
            let projection = columns
                .iter()
                .map(|column| format!("quote(\"{column}\")"))
                .collect::<Vec<_>>()
                .join(", ");
            let rows = sqlx::query(&format!("SELECT {projection} FROM {table} ORDER BY rowid"))
                .fetch_all(self.storage.pool())
                .await
                .unwrap();
            snapshot.push(
                rows.iter()
                    .map(|row| row.columns().iter().map(|c| row.get(c.ordinal())).collect())
                    .collect(),
            );
        }
        snapshot
    }
}

fn assert_acceptance_refusal<T: std::fmt::Debug>(result: bsv_wallet_toolbox_rs::Result<T>) {
    match result.expect_err("an accepting word must refuse retirement") {
        Error::InvalidTransactionStatus(message) => {
            assert!(message.contains("accept"), "{message}");
        }
        other => panic!("expected InvalidTransactionStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn abort_unproven_without_chain_evidence_is_refused_without_writes_or_calls() {
    let s = Seeded::new("unproven").await;
    let before = s.snapshot().await;
    assert_acceptance_refusal(s.abort().await);
    assert_eq!(s.snapshot().await, before);
    s.assert_no_chain_calls();
}

#[tokio::test]
async fn abort_sending_with_a_request_acceptance_is_refused_without_writes_or_calls() {
    let s = Seeded::new("sending").await;
    s.request("unmined", json!({})).await;
    let before = s.snapshot().await;
    assert_acceptance_refusal(s.abort().await);
    assert_eq!(s.snapshot().await, before);
    s.assert_no_chain_calls();
}

#[tokio::test]
async fn abort_sending_when_every_named_instance_refused_uses_no_chain_calls() {
    let s = Seeded::new("sending").await;
    s.request(
        "unsent",
        json!({"notes": [{"what": "sendWaitingHint", "words": [
            {"provider": "instance-a", "status": "error", "txids": [{"status": "error"}]},
            {"provider": "instance-b", "status": "error", "txids": [{"status": "error"}]}
        ]}]}),
    )
    .await;
    for provider in ["instance-a", "instance-b"] {
        s.storage
            .record_broadcast_status(TXID, provider, "rejected")
            .await
            .unwrap();
    }
    assert!(s.abort().await.unwrap().aborted);
    let status: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(s.tx_id)
            .fetch_one(s.storage.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    let input: (i64, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE transaction_id != ?")
            .bind(s.tx_id)
            .fetch_one(s.storage.pool())
            .await
            .unwrap();
    assert_eq!(input, (1, None));
    s.assert_no_chain_calls();
}

#[tokio::test]
async fn retire_undeliverable_with_an_accepting_word_is_refused_without_writes_or_calls() {
    let s = Seeded::new("sending").await;
    s.request("unmined", json!({})).await;
    let before = s.snapshot().await;
    assert_acceptance_refusal(
        s.storage
            .retire_undeliverable_txid(s.services.as_ref(), TXID, "invalid")
            .await,
    );
    assert_eq!(s.snapshot().await, before);
    s.assert_no_chain_calls();
}
