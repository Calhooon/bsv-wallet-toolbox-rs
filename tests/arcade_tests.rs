//! Arcade V2 provider tests (all synthetic — no network, no real keys).
//!
//! Covers:
//! - BEEF → EF batch conversion (source-linking, dependency ordering,
//!   already-mined skip, missing-source errors)
//! - Arcade submit against mockito (single `/tx` binary EF, batch `/txs`
//!   octet-stream concat body, headers, fatal status mapping)
//! - SSE stream: replay-on-connect, Last-Event-ID resume
//! - `StorageSqlx::ingest_merkle_proof` (validate → store → complete) and the
//!   push-status storage transitions (`mark_transaction_seen_on_network`,
//!   `mark_transaction_rejected`)

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, Transaction, TransactionInput, TransactionOutput, BEEF_V1,
};
use bsv_wallet_toolbox_rs::services::{beef_to_ef_batch, Arcade, ArcadeConfig, ArcadeSseClient};

/// EF marker bytes (BRC-30): after the 4-byte version.
const EF_MARKER: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0xEF];

/// P2PKH-style locking script used for synthetic outputs.
const LOCK_HEX: &str = "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac";

fn synthetic_tx(source_txid: Option<String>, source_vout: u32, satoshis: u64) -> Transaction {
    Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(source_txid.unwrap_or_else(|| "00".repeat(32))),
            source_output_index: source_vout,
            unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
            unlocking_script_template: None,
            sequence: 0xFFFFFFFF,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: LockingScript::from_hex(LOCK_HEX).unwrap(),
            change: false,
        }],
        0,
    )
}

/// Build a BEEF: proven root (coinbase-style BUMP) → unproven parent →
/// unproven child. Returns (beef_binary, root_txid, parent_txid, child_txid).
fn build_three_level_beef() -> (Vec<u8>, String, String, String) {
    let height = 800_000u32;

    let root_tx = synthetic_tx(None, 0xFFFFFFFF, 3_000_000);
    let root_txid = root_tx.id();

    let parent_tx = synthetic_tx(Some(root_txid.clone()), 0, 2_000_000);
    let parent_txid = parent_tx.id();

    let child_tx = synthetic_tx(Some(parent_txid.clone()), 0, 1_000_000);
    let child_txid = child_tx.id();

    let bump = MerklePath::from_coinbase_txid(&root_txid, height);

    let mut beef = Beef::with_version(BEEF_V1);
    let bump_idx = beef.merge_bump(bump);
    // Merge deliberately OUT of dependency order (child first) to prove
    // beef_to_ef_batch re-sorts parents before children.
    beef.merge_transaction(child_tx);
    beef.merge_transaction(parent_tx);
    beef.merge_raw_tx(root_tx.to_binary(), Some(bump_idx));

    (beef.to_binary(), root_txid, parent_txid, child_txid)
}

/// Build a BEEF with a proven parent and one unproven child.
/// Returns (beef_binary, parent_txid, child_txid).
fn build_proven_parent_beef() -> (Vec<u8>, String, String) {
    let height = 800_000u32;

    let parent_tx = synthetic_tx(None, 0xFFFFFFFF, 2_000_000);
    let parent_txid = parent_tx.id();
    let child_tx = synthetic_tx(Some(parent_txid.clone()), 0, 1_000_000);
    let child_txid = child_tx.id();

    let bump = MerklePath::from_coinbase_txid(&parent_txid, height);

    let mut beef = Beef::with_version(BEEF_V1);
    let bump_idx = beef.merge_bump(bump);
    beef.merge_raw_tx(parent_tx.to_binary(), Some(bump_idx));
    beef.merge_transaction(child_tx);

    (beef.to_binary(), parent_txid, child_txid)
}

// =============================================================================
// beef_to_ef_batch
// =============================================================================

mod ef_conversion {
    use super::*;

    #[test]
    fn proven_parent_skipped_child_converted() {
        let (beef, parent_txid, child_txid) = build_proven_parent_beef();

        let (efs, subject) = beef_to_ef_batch(&beef).expect("conversion succeeds");

        // Only the unproven child is submitted; the proven parent just
        // provides source data.
        assert_eq!(efs.len(), 1, "only the unproven child should be converted");
        assert_eq!(subject, child_txid);

        // EF marker present after the 4-byte version.
        assert_eq!(&efs[0][4..10], EF_MARKER, "EF marker missing");

        // The child's EF must embed the parent's output satoshis (2_000_000 LE)
        // — proof that source-linking pulled from the BEEF's own tx map.
        let sats_le = 2_000_000u64.to_le_bytes();
        assert!(
            efs[0].windows(8).any(|w| w == sats_le),
            "EF should embed parent output satoshis"
        );
        // And the parent's locking script.
        let lock = hex::decode(LOCK_HEX).unwrap();
        assert!(
            efs[0].windows(lock.len()).any(|w| w == lock.as_slice()),
            "EF should embed parent locking script"
        );
        let _ = parent_txid;
    }

    #[test]
    fn dependency_ordering_parent_before_child() {
        let (beef, _root_txid, parent_txid, child_txid) = build_three_level_beef();

        let (efs, subject) = beef_to_ef_batch(&beef).expect("conversion succeeds");

        assert_eq!(efs.len(), 2, "two unproven txs expected");
        assert_eq!(subject, child_txid, "subject is the last-sorted (leaf) tx");

        // First EF must be the parent: its input references the (proven) root,
        // second is the child referencing the parent. Verify by computing the
        // txid embedded in the input of each EF? Simpler: the child's EF embeds
        // the parent's txid bytes (reversed) in its input outpoint.
        let parent_txid_reversed: Vec<u8> = {
            let mut b = hex::decode(&parent_txid).unwrap();
            b.reverse();
            b
        };
        assert!(
            !efs[0]
                .windows(32)
                .any(|w| w == parent_txid_reversed.as_slice()),
            "first EF must be the parent (must not reference the parent txid)"
        );
        assert!(
            efs[1]
                .windows(32)
                .any(|w| w == parent_txid_reversed.as_slice()),
            "second EF must be the child spending the parent"
        );
    }

    #[test]
    fn all_proven_yields_empty_batch() {
        let height = 800_000u32;
        let tx = synthetic_tx(None, 0xFFFFFFFF, 500_000);
        let txid = tx.id();
        let bump = MerklePath::from_coinbase_txid(&txid, height);

        let mut beef = Beef::with_version(BEEF_V1);
        let bump_idx = beef.merge_bump(bump);
        beef.merge_raw_tx(tx.to_binary(), Some(bump_idx));

        let (efs, subject) = beef_to_ef_batch(&beef.to_binary()).expect("conversion succeeds");
        assert!(efs.is_empty(), "nothing unproven to submit");
        assert_eq!(subject, txid);
    }

    #[test]
    fn missing_source_tx_errors() {
        // Unproven child whose parent is NOT in the BEEF and has no proof.
        let child = synthetic_tx(Some("11".repeat(32)), 0, 1_000);
        let mut beef = Beef::with_version(BEEF_V1);
        beef.merge_transaction(child);

        let err = beef_to_ef_batch(&beef.to_binary());
        assert!(err.is_err(), "missing source must be an error");
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("not present in BEEF"),
            "error should name the missing source, got: {}",
            msg
        );
    }

    #[test]
    fn garbage_beef_errors() {
        assert!(beef_to_ef_batch(&[0xde, 0xad, 0xbe, 0xef]).is_err());
    }
}

// =============================================================================
// Arcade submit (mockito)
// =============================================================================

mod arcade_submit {
    use super::*;

    #[tokio::test]
    async fn single_unproven_tx_posts_binary_ef_to_tx() {
        let (beef, _parent_txid, child_txid) = build_proven_parent_beef();
        let (efs, _) = beef_to_ef_batch(&beef).unwrap();
        assert_eq!(efs.len(), 1);
        let expected_body = efs[0].clone();

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/tx")
            .match_header("content-type", "application/octet-stream")
            .match_header("x-callbacktoken", "tok-abc")
            .match_header("x-fullstatusupdates", "true")
            .match_header("x-callbackurl", "https://cb.example/arc-callback")
            .match_body(expected_body)
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"txid":"{}","status":202,"txStatus":"RECEIVED"}}"#,
                child_txid
            ))
            .create_async()
            .await;

        let config = ArcadeConfig::with_callback_token("tok-abc")
            .with_callback_url("https://cb.example/arc-callback");
        let arcade = Arcade::new(server.url(), Some(config), None).unwrap();

        let result = arcade
            .post_beef(&beef, std::slice::from_ref(&child_txid))
            .await
            .unwrap();

        mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(result
            .txid_results
            .iter()
            .any(|r| r.txid == child_txid && r.status == "success"));
    }

    #[tokio::test]
    async fn multiple_unproven_txs_post_concat_ef_to_txs() {
        let (beef, _root, _parent, child_txid) = build_three_level_beef();
        let (efs, _) = beef_to_ef_batch(&beef).unwrap();
        assert_eq!(efs.len(), 2);
        let mut expected_body = Vec::new();
        for ef in &efs {
            expected_body.extend_from_slice(ef);
        }

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/txs")
            .match_header("content-type", "application/octet-stream")
            .match_header("x-fullstatusupdates", "true")
            .match_body(expected_body)
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(r#"{"duplicates":0,"submitted":2,"total":2}"#)
            .create_async()
            .await;

        let arcade = Arcade::new(
            server.url(),
            Some(ArcadeConfig::with_callback_token("tok")),
            None,
        )
        .unwrap();

        let result = arcade
            .post_beef(&beef, std::slice::from_ref(&child_txid))
            .await
            .unwrap();

        mock.assert_async().await;
        assert_eq!(result.status, "success");
        assert!(result
            .txid_results
            .iter()
            .any(|r| r.txid == child_txid && r.status == "success"));
        // The batch summary is surfaced in the result data.
        assert!(result.txid_results[0]
            .data
            .as_deref()
            .unwrap_or_default()
            .contains("submitted=2"));
    }

    #[tokio::test]
    async fn fatal_status_maps_to_double_spend() {
        let (beef, _parent_txid, child_txid) = build_proven_parent_beef();

        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/tx")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"txid":"{}","status":202,"txStatus":"DOUBLE_SPEND_ATTEMPTED"}}"#,
                child_txid
            ))
            .create_async()
            .await;

        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let result = arcade
            .post_beef(&beef, std::slice::from_ref(&child_txid))
            .await
            .unwrap();

        assert_eq!(result.status, "error");
        let r = &result.txid_results[0];
        assert!(
            r.double_spend,
            "DOUBLE_SPEND_ATTEMPTED must set double_spend"
        );
        assert!(!r.service_error, "fatal verdict is not a service error");
    }

    #[tokio::test]
    async fn http_error_is_service_error() {
        let (beef, _parent_txid, child_txid) = build_proven_parent_beef();

        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/tx")
            .with_status(500)
            .with_body("boom")
            .create_async()
            .await;

        let arcade = Arcade::new(server.url(), None, None).unwrap();
        let result = arcade.post_beef(&beef, &[child_txid]).await.unwrap();

        assert_eq!(result.status, "error");
        assert!(
            result.txid_results[0].service_error,
            "5xx must be retryable"
        );
    }

    #[tokio::test]
    async fn all_proven_beef_submits_nothing() {
        // BEEF whose only tx is proven — no HTTP call should happen (no mock
        // registered; any request would hit mockito's implicit 501).
        let height = 800_000u32;
        let tx = synthetic_tx(None, 0xFFFFFFFF, 500_000);
        let txid = tx.id();
        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let mut beef = Beef::with_version(BEEF_V1);
        let bump_idx = beef.merge_bump(bump);
        beef.merge_raw_tx(tx.to_binary(), Some(bump_idx));

        let server = mockito::Server::new_async().await;
        let arcade = Arcade::new(server.url(), None, None).unwrap();

        let result = arcade
            .post_beef(&beef.to_binary(), std::slice::from_ref(&txid))
            .await
            .unwrap();
        assert_eq!(result.status, "success");
        assert!(result.txid_results.iter().any(|r| r.txid == txid));
    }
}

// =============================================================================
// SSE stream (mockito)
// =============================================================================

mod sse_stream {
    use super::*;

    #[tokio::test]
    async fn connect_replays_all_pending_statuses() {
        let mut server = mockito::Server::new_async().await;
        let body = concat!(
            "id: 100\nevent: status\ndata: {\"txid\":\"aa11\",\"txStatus\":\"SEEN_ON_NETWORK\",\"timestamp\":\"t1\"}\n\n",
            "id: 101\nevent: status\ndata: {\"txid\":\"aa11\",\"txStatus\":\"MINED\",\"timestamp\":\"t2\"}\n\n",
        );
        let mock = server
            .mock("GET", "/events")
            .match_query(mockito::Matcher::UrlEncoded(
                "callbackToken".into(),
                "tok-1".into(),
            ))
            .with_status(200)
            .with_header("content-type", "text/event-stream")
            .with_body(body)
            .create_async()
            .await;

        let mut client = ArcadeSseClient::new(server.url(), "tok-1").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let delivered = client.stream_once(tx).await.unwrap();

        mock.assert_async().await;
        assert_eq!(delivered, 2);

        let ev1 = rx.recv().await.unwrap();
        assert_eq!(ev1.txid, "aa11");
        assert_eq!(ev1.tx_status, "SEEN_ON_NETWORK");
        assert_eq!(ev1.event_id.as_deref(), Some("100"));

        let ev2 = rx.recv().await.unwrap();
        assert_eq!(ev2.tx_status, "MINED");

        // Channel closed after the stream ended.
        assert!(rx.recv().await.is_none());

        // Last-Event-ID tracked for resume.
        assert_eq!(client.last_event_id.as_deref(), Some("101"));
    }

    #[tokio::test]
    async fn reconnect_sends_last_event_id() {
        let mut server = mockito::Server::new_async().await;

        // First connection: one event with id 7.
        let first = server
            .mock("GET", "/events")
            .match_query(mockito::Matcher::UrlEncoded(
                "callbackToken".into(),
                "tok-2".into(),
            ))
            .with_status(200)
            .with_body(
                "id: 7\nevent: status\ndata: {\"txid\":\"bb\",\"txStatus\":\"RECEIVED\"}\n\n",
            )
            .create_async()
            .await;

        let mut client = ArcadeSseClient::new(server.url(), "tok-2").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        client.stream_once(tx).await.unwrap();
        first.assert_async().await;
        let _ = rx.recv().await.unwrap();

        // Second connection MUST carry Last-Event-ID: 7.
        let second = server
            .mock("GET", "/events")
            .match_query(mockito::Matcher::UrlEncoded(
                "callbackToken".into(),
                "tok-2".into(),
            ))
            .match_header("last-event-id", "7")
            .with_status(200)
            .with_body("id: 8\nevent: status\ndata: {\"txid\":\"bb\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n")
            .create_async()
            .await;

        let (tx2, mut rx2) = tokio::sync::mpsc::channel(16);
        let delivered = client.stream_once(tx2).await.unwrap();
        second.assert_async().await;
        assert_eq!(delivered, 1);
        let ev = rx2.recv().await.unwrap();
        assert_eq!(ev.tx_status, "SEEN_ON_NETWORK");
        assert_eq!(client.last_event_id.as_deref(), Some("8"));
    }

    #[tokio::test]
    async fn keepalive_comments_and_unknown_frames_ignored() {
        let mut server = mockito::Server::new_async().await;
        let body = concat!(
            ": keep-alive\n\n",
            "id: 1\nevent: status\ndata: not-json-at-all\n\n",
            "id: 2\nevent: status\ndata: {\"txid\":\"cc\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n",
        );
        let _mock = server
            .mock("GET", "/events")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let mut client = ArcadeSseClient::new(server.url(), "tok-3").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let delivered = client.stream_once(tx).await.unwrap();

        assert_eq!(delivered, 1, "only the valid JSON frame is delivered");
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.txid, "cc");
        // Unparseable frame's id still advances Last-Event-ID resume point.
        assert_eq!(client.last_event_id.as_deref(), Some("2"));
    }
}

// =============================================================================
// ingest_merkle_proof + push-status transitions (sqlite)
// =============================================================================

#[cfg(feature = "sqlite")]
mod proof_ingestion {
    use bsv_rs::transaction::{MerklePath, MockChainTracker};
    use bsv_wallet_toolbox_rs::storage::StorageSqlx;
    use bsv_wallet_toolbox_rs::{MonitorStorage, ProofIngestOutcome, WalletStorageWriter};
    use std::sync::Arc;

    /// In-memory storage with migrations run.
    async fn setup_storage() -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.expect("in_memory storage");
        let storage_key = "02".to_string() + &"ab".repeat(32);
        storage
            .migrate("test-arcade", &storage_key)
            .await
            .expect("migrate");
        storage.make_available().await.expect("make_available");
        storage
    }

    /// Insert a user + proven_tx_req + transaction for `txid` in the given statuses.
    async fn seed_tx(storage: &StorageSqlx, txid: &str, req_status: &str, tx_status: &str) {
        let identity_key = "02".to_string() + &"cd".repeat(32);
        let (user, _) = storage
            .find_or_insert_user(&identity_key)
            .await
            .expect("find_or_insert_user");
        let now = chrono::Utc::now();

        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at)
            VALUES (?, ?, 0, '{}', 0, '{}', X'01000000', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(req_status)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert proven_tx_req");

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                      version, lock_time, raw_tx, is_outgoing, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'arcade test tx', -500, 1, 0, X'01000000', 1, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(tx_status)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .expect("insert transaction");
    }

    async fn req_status(storage: &StorageSqlx, txid: &str) -> String {
        let (s,): (String,) = sqlx::query_as("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .expect("req status");
        s
    }

    async fn tx_status(storage: &StorageSqlx, txid: &str) -> String {
        let (s,): (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .expect("tx status");
        s
    }

    #[tokio::test]
    async fn ingest_happy_path_validates_and_completes() {
        let storage = setup_storage().await;
        let txid = "a".repeat(64);
        let height = 850_000u32;
        seed_tx(&storage, &txid, "unmined", "unproven").await;

        // Synthetic BUMP that actually validates: coinbase-style single-tx
        // block, so computed root == txid.
        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let bump_bytes = bump.to_binary();
        let root = bump.compute_root(Some(&txid)).unwrap();

        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, root.clone());
        storage.set_chain_tracker(Arc::new(tracker)).await;

        let outcome = storage
            .ingest_merkle_proof(&txid, &bump_bytes, height, &"b".repeat(64), None)
            .await
            .expect("ingest ok");

        match outcome {
            ProofIngestOutcome::Ingested(status) => {
                assert_eq!(status.txid, txid);
                assert_eq!(status.block_height, Some(height));
                // No header root supplied → the validated computed root is stored.
                assert_eq!(status.merkle_root.as_deref(), Some(root.as_str()));
            }
            other => panic!("expected Ingested, got {:?}", other),
        }

        // proven_txs row exists with the BUMP binary.
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_txs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(count, 1);

        assert_eq!(req_status(&storage, &txid).await, "completed");
        assert_eq!(tx_status(&storage, &txid).await, "completed");
    }

    #[tokio::test]
    async fn ingest_rejects_invalid_root() {
        let storage = setup_storage().await;
        let txid = "b".repeat(64);
        let height = 850_000u32;
        seed_tx(&storage, &txid, "unmined", "unproven").await;

        let bump = MerklePath::from_coinbase_txid(&txid, height);

        // Tracker knows a DIFFERENT root for this height.
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, "ff".repeat(32));
        storage.set_chain_tracker(Arc::new(tracker)).await;

        let outcome = storage
            .ingest_merkle_proof(&txid, &bump.to_binary(), height, &"b".repeat(64), None)
            .await
            .expect("ingest call ok");

        assert!(
            matches!(outcome, ProofIngestOutcome::InvalidMerkleRoot { .. }),
            "expected InvalidMerkleRoot, got {:?}",
            outcome
        );

        // NOTHING stored, statuses untouched.
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_txs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(req_status(&storage, &txid).await, "unmined");
        assert_eq!(tx_status(&storage, &txid).await, "unproven");
    }

    #[tokio::test]
    async fn ingest_rejects_garbage_bump() {
        let storage = setup_storage().await;
        let txid = "c".repeat(64);
        seed_tx(&storage, &txid, "unmined", "unproven").await;

        let outcome = storage
            .ingest_merkle_proof(&txid, &[0xde, 0xad], 850_000, &"b".repeat(64), None)
            .await
            .expect("ingest call ok");

        assert!(
            matches!(outcome, ProofIngestOutcome::InvalidProof(_)),
            "expected InvalidProof, got {:?}",
            outcome
        );
        assert_eq!(req_status(&storage, &txid).await, "unmined");
    }

    #[tokio::test]
    async fn ingest_stores_header_root_when_provided() {
        let storage = setup_storage().await;
        let txid = "d".repeat(64);
        let height = 850_000u32;
        seed_tx(&storage, &txid, "unmined", "unproven").await;

        let bump = MerklePath::from_coinbase_txid(&txid, height);
        let root = bump.compute_root(Some(&txid)).unwrap();
        let mut tracker = MockChainTracker::new(height + 1);
        tracker.add_root(height, root.clone());
        storage.set_chain_tracker(Arc::new(tracker)).await;

        let outcome = storage
            .ingest_merkle_proof(
                &txid,
                &bump.to_binary(),
                height,
                &"e".repeat(64),
                Some(&root),
            )
            .await
            .expect("ingest ok");
        assert!(matches!(outcome, ProofIngestOutcome::Ingested(_)));

        let (stored_root,): (String,) =
            sqlx::query_as("SELECT merkle_root FROM proven_txs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(stored_root, root);
    }

    #[tokio::test]
    async fn seen_on_network_lifts_sending_to_spendable() {
        let storage = setup_storage().await;
        let txid = "e".repeat(64);
        seed_tx(&storage, &txid, "sending", "sending").await;

        let updated = storage
            .mark_transaction_seen_on_network(&txid)
            .await
            .expect("mark ok");
        assert!(updated);
        assert_eq!(req_status(&storage, &txid).await, "unmined");
        assert_eq!(tx_status(&storage, &txid).await, "unproven");

        // Idempotent: second call changes nothing.
        let updated_again = storage
            .mark_transaction_seen_on_network(&txid)
            .await
            .expect("mark ok");
        assert!(!updated_again);
    }

    #[tokio::test]
    async fn seen_on_network_never_demotes_completed() {
        let storage = setup_storage().await;
        let txid = "f".repeat(64);
        seed_tx(&storage, &txid, "completed", "completed").await;

        let updated = storage
            .mark_transaction_seen_on_network(&txid)
            .await
            .expect("mark ok");
        assert!(!updated);
        assert_eq!(req_status(&storage, &txid).await, "completed");
        assert_eq!(tx_status(&storage, &txid).await, "completed");
    }

    #[tokio::test]
    async fn rejected_marks_double_spend() {
        let storage = setup_storage().await;
        let txid = "1".repeat(64);
        seed_tx(&storage, &txid, "unmined", "unproven").await;

        let updated = storage
            .mark_transaction_rejected(&txid, true)
            .await
            .expect("mark ok");
        assert!(updated);
        assert_eq!(req_status(&storage, &txid).await, "doubleSpend");
        assert_eq!(tx_status(&storage, &txid).await, "failed");
    }

    #[tokio::test]
    async fn rejected_marks_invalid_without_double_spend() {
        let storage = setup_storage().await;
        let txid = "2".repeat(64);
        seed_tx(&storage, &txid, "sending", "sending").await;

        let updated = storage
            .mark_transaction_rejected(&txid, false)
            .await
            .expect("mark ok");
        assert!(updated);
        assert_eq!(req_status(&storage, &txid).await, "invalid");
        // 'sending' is not lifted to failed (only sending/unproven are); it was 'sending'
        assert_eq!(tx_status(&storage, &txid).await, "failed");
    }
}
