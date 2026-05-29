//! Regression for paginated sync pagination and cross-chunk FK resolution via WalletStorageManager.
//!
//! Tests that WalletStorageManager::sync_to_writer correctly advances per-entity offsets
//! (fixing the user-only sync bug) and threads SyncMap across chunks (fixing cross-chunk
//! foreign-key linking). These are native (non-wasm) tests using in-memory sqlx stores.

#![cfg(not(target_arch = "wasm32"))]

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;
use chrono::Utc;
use std::sync::Arc;

async fn insert_transaction(store: &StorageSqlx, user_id: i64, txid: &str) -> i64 {
    let now = Utc::now();
    let reference = uuid::Uuid::new_v4().to_string();
    ::sqlx::query(
        r#"INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
                                     version, lock_time, is_outgoing, created_at, updated_at)
           VALUES (?, ?, 'completed', ?, 'seed tx', 1000, 1, 0, 0, ?, ?)"#,
    )
    .bind(user_id)
    .bind(txid)
    .bind(reference)
    .bind(now)
    .bind(now)
    .execute(store.pool())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn insert_output(
    store: &StorageSqlx,
    user_id: i64,
    transaction_id: i64,
    txid: &str,
    basket_id: Option<i64>,
) {
    let now = Utc::now();
    ::sqlx::query(
        r#"INSERT INTO outputs (user_id, transaction_id, basket_id, txid, vout, satoshis,
                                script_length, script_offset, type, provided_by, purpose,
                                spendable, change, locking_script, created_at, updated_at)
           VALUES (?, ?, ?, ?, 0, 1000, 25, 0, 'P2PKH', 'you', 'change', 1, 0,
                   X'76a914000000000000000000000000000000000000000088ac', ?, ?)"#,
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(basket_id)
    .bind(txid)
    .bind(now)
    .bind(now)
    .execute(store.pool())
    .await
    .unwrap();
}

async fn default_basket_id(store: &StorageSqlx, user_id: i64) -> i64 {
    ::sqlx::query_scalar(
        r#"SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = 'default'"#,
    )
    .bind(user_id)
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn insert_proven_tx_req(
    store: &StorageSqlx,
    txid: &str,
    raw_tx: &[u8],
    notify: &str,
    batch: Option<&str>,
) -> i64 {
    let now = Utc::now();
    ::sqlx::query(
        r#"INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify,
                                       raw_tx, input_beef, batch, created_at, updated_at)
           VALUES (?, 'pending', 0, '[]', 0, ?, ?, NULL, ?, ?, ?)"#,
    )
    .bind(txid)
    .bind(notify)
    .bind(raw_tx)
    .bind(batch)
    .bind(now)
    .bind(now)
    .execute(store.pool())
    .await
    .unwrap()
    .last_insert_rowid()
}

#[tokio::test]
async fn sync_to_writer_links_outputs_across_chunk_boundaries() {
    let identity = "a".repeat(66);

    let reader_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    reader_store
        .migrate("reader", &"1".repeat(64))
        .await
        .unwrap();
    reader_store.make_available().await.unwrap();

    let reader_mgr = WalletStorageManager::new(identity.clone(), Some(reader_store.clone()), None);

    let (ruser, _) = reader_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    // Seed >1000 entities (700 tx + 700 outputs) to force multi-chunk sync.
    const N: usize = 700;
    for i in 0..N {
        let txid = format!("{:064x}", i);
        let tx_id = insert_transaction(&reader_store, ruser.user_id, &txid).await;
        insert_output(&reader_store, ruser.user_id, tx_id, &txid, None).await;
    }

    // Writer storage with offset id-space so a misused foreign id would point wrong.
    let writer_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    writer_store
        .migrate("writer", &"2".repeat(64))
        .await
        .unwrap();
    writer_store.make_available().await.unwrap();

    let (wuser, _) = writer_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    for i in 0..7 {
        insert_transaction(&writer_store, wuser.user_id, &format!("offset{:060x}", i))
            .await;
    }

    // Sync from reader manager to writer storage.
    reader_mgr
        .sync_to_writer(&identity, writer_store.clone())
        .await
        .unwrap();

    // Verify every output links to a transaction with matching txid.
    // Pre-fix, cross-chunk outputs were mis-linked.
    let mismatched: i64 = ::sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM outputs o
           LEFT JOIN transactions t
             ON t.transaction_id = o.transaction_id AND t.user_id = o.user_id
           WHERE o.user_id = ? AND (t.txid IS NULL OR t.txid != o.txid)"#,
    )
    .bind(wuser.user_id)
    .fetch_one(writer_store.pool())
    .await
    .unwrap();

    let synced: i64 = ::sqlx::query_scalar(r#"SELECT COUNT(*) FROM outputs WHERE user_id = ?"#)
        .bind(wuser.user_id)
        .fetch_one(writer_store.pool())
        .await
        .unwrap();

    assert_eq!(synced, N as i64, "all {N} outputs should sync");
    assert_eq!(
        mismatched, 0,
        "no outputs should be mis-linked across chunk boundaries; \
         {mismatched} were mis-linked (transaction_id mismatch)"
    );
}

#[tokio::test]
async fn sync_to_writer_links_baskets_across_chunk_boundaries() {
    let identity = "b".repeat(66);

    let reader_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    reader_store
        .migrate("reader-basket", &"3".repeat(64))
        .await
        .unwrap();
    reader_store.make_available().await.unwrap();

    let reader_mgr = WalletStorageManager::new(identity.clone(), Some(reader_store.clone()), None);

    let (ruser, _) = reader_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();
    let rbasket = default_basket_id(&reader_store, ruser.user_id).await;

    const N: usize = 700;
    for i in 0..N {
        let txid = format!("{:064x}", i);
        let tx_id = insert_transaction(&reader_store, ruser.user_id, &txid).await;
        insert_output(&reader_store, ruser.user_id, tx_id, &txid, Some(rbasket)).await;
    }

    let writer_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    writer_store
        .migrate("writer-basket", &"4".repeat(64))
        .await
        .unwrap();
    writer_store.make_available().await.unwrap();

    let (wuser, _) = writer_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    reader_mgr
        .sync_to_writer(&identity, writer_store.clone())
        .await
        .unwrap();

    // Every output must link to an existing basket. Pre-fix, cross-chunk outputs
    // lost the basket link since it has no natural key fallback.
    let orphaned: i64 = ::sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM outputs o
           WHERE o.user_id = ?
             AND (o.basket_id IS NULL
                  OR NOT EXISTS (SELECT 1 FROM output_baskets b
                                 WHERE b.basket_id = o.basket_id AND b.user_id = o.user_id))"#,
    )
    .bind(wuser.user_id)
    .fetch_one(writer_store.pool())
    .await
    .unwrap();

    let synced: i64 = ::sqlx::query_scalar(r#"SELECT COUNT(*) FROM outputs WHERE user_id = ?"#)
        .bind(wuser.user_id)
        .fetch_one(writer_store.pool())
        .await
        .unwrap();

    assert_eq!(synced, N as i64, "all {N} outputs should sync");
    assert_eq!(
        orphaned, 0,
        "every output must link to an existing basket across chunks; \
         {orphaned} were orphaned (basket_id link dropped)"
    );
}

/// Pre-fix:
///   1) upsert_proven_tx_req's INSERT omitted raw_tx, which is BLOB NOT NULL —
///      applying any pulled chunk carrying a proven_tx_req failed
///      "NOT NULL constraint failed: proven_tx_reqs.raw_tx" (sqlite rc=19),
///      aborting the whole sync. The headless sync test never hit it because
///      the seeded reader had no proven_tx_req rows.
///   2) fetch_proven_tx_reqs_for_sync's SELECT omitted notify/batch — even after
///      the INSERT was fixed, those columns landed as defaults (notify="" /
///      batch=None) on the writer. notify drives the monitor's notification
///      state; batch groups multi-tx broadcasts in send_waiting. A pending
///      multi-tx broadcast restored through L2 would therefore lose its
///      grouping metadata.
///
/// This test seeds a proven_tx_req with a real raw_tx, a non-empty notify, and
/// a non-NULL batch, syncs reader→writer, and asserts all three round-trip.
/// Against either bug the assert fails (rc=19 panics the sync, or notify/batch
/// come back wrong).
#[tokio::test]
async fn sync_to_writer_transfers_proven_tx_req_with_raw_tx_notify_and_batch() {
    let identity = "c".repeat(66);

    let reader_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    reader_store
        .migrate("reader-proven-req", &"5".repeat(64))
        .await
        .unwrap();
    reader_store.make_available().await.unwrap();

    let reader_mgr = WalletStorageManager::new(identity.clone(), Some(reader_store.clone()), None);

    let (_ruser, _) = reader_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    let txid = "abcd".repeat(16);
    let raw_tx: Vec<u8> = vec![0x01, 0x00, 0x00, 0x00, 0x01];
    let notify = "{\"subscribers\":[\"app://wallet\"]}";
    let batch = "batch-multi-tx-1";

    insert_proven_tx_req(&reader_store, &txid, &raw_tx, notify, Some(batch)).await;

    let writer_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    writer_store
        .migrate("writer-proven-req", &"6".repeat(64))
        .await
        .unwrap();
    writer_store.make_available().await.unwrap();

    let (_wuser, _) = writer_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    // Pre-fix this panics with rc=19 on the proven_tx_req INSERT.
    reader_mgr
        .sync_to_writer(&identity, writer_store.clone())
        .await
        .unwrap();

    let (got_raw_tx, got_notify, got_batch): (Vec<u8>, String, Option<String>) = ::sqlx::query_as(
        r#"SELECT raw_tx, notify, batch FROM proven_tx_reqs WHERE txid = ?"#,
    )
    .bind(&txid)
    .fetch_one(writer_store.pool())
    .await
    .unwrap();

    assert_eq!(got_raw_tx, raw_tx, "raw_tx should round-trip reader→writer");
    assert_eq!(got_notify, notify, "notify should round-trip reader→writer");
    assert_eq!(
        got_batch.as_deref(),
        Some(batch),
        "batch should round-trip reader→writer"
    );
}

async fn insert_proven_tx_req_with_timestamps(
    store: &StorageSqlx,
    txid: &str,
    status: &str,
    attempts: i32,
    history: &str,
    notify: &str,
    notified: bool,
    raw_tx: &[u8],
    input_beef: Option<&[u8]>,
    batch: Option<&str>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
) {
    ::sqlx::query(
        r#"INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify,
                                       raw_tx, input_beef, batch, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(txid)
    .bind(status)
    .bind(attempts)
    .bind(history)
    .bind(notified as i32)
    .bind(notify)
    .bind(raw_tx)
    .bind(input_beef)
    .bind(batch)
    .bind(created_at)
    .bind(updated_at)
    .execute(store.pool())
    .await
    .unwrap();
}

/// Regression (2026-05-29, Codex review c815706c): the proven_tx_req
/// UPDATE branch must refresh every mutable field on a newer-wins remote
/// replacement, not just `status/attempts/history/notified/proven_tx_id`.
/// Canonical `process_action` mutates `notify` / `raw_tx` / `input_beef` /
/// `batch` post-creation (history append, post-broadcast metadata, multi-
/// tx batch regrouping), so an existing local row that the chunk
/// supersedes must absorb every newer mutable value. Pre-fix the UPDATE
/// column list omitted `notify` / `raw_tx` / `input_beef` / `batch` and
/// silently kept the older values.
///
/// Setup: seed BOTH reader + writer with the same txid, but writer has the
/// OLDER row (smaller updated_at) carrying old mutable values. After
/// sync_to_writer, writer's row must take reader's newer mutable fields.
#[tokio::test]
async fn sync_to_writer_updates_existing_proven_tx_req_with_all_mutable_fields() {
    let identity = "d".repeat(66);

    let reader_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    reader_store
        .migrate("reader-ptr-upd", &"7".repeat(64))
        .await
        .unwrap();
    reader_store.make_available().await.unwrap();

    let reader_mgr = WalletStorageManager::new(identity.clone(), Some(reader_store.clone()), None);

    let (_ruser, _) = reader_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    let writer_store = Arc::new(StorageSqlx::in_memory().await.unwrap());
    writer_store
        .migrate("writer-ptr-upd", &"8".repeat(64))
        .await
        .unwrap();
    writer_store.make_available().await.unwrap();

    let (_wuser, _) = writer_store
        .find_or_insert_user(&identity)
        .await
        .unwrap();

    let txid = "abcd".repeat(16);
    let writer_old_ts = Utc::now();
    let reader_new_ts = writer_old_ts + chrono::Duration::seconds(1);

    // Writer seeded with the OLDER row carrying old mutable values.
    insert_proven_tx_req_with_timestamps(
        &writer_store,
        &txid,
        "unmined",
        0,
        r#"{"old":true}"#,
        "", // empty notify
        false,
        &[0x01], // placeholder raw_tx
        None,    // no input_beef
        None,    // no batch
        writer_old_ts,
        writer_old_ts,
    )
    .await;

    // Reader seeded with the NEWER row — advances every mutable field.
    // 'completed' status maps cleanly to ProvenTxReqStatus::Completed and
    // re-serializes to "completed" via the canonical Debug-lowercase path
    // (both "unmined" and unrecognized strings normalize to Pending).
    let new_raw_tx: Vec<u8> = vec![0xFE, 0xED, 0xFA, 0xCE];
    let new_input_beef: Vec<u8> = vec![0xBE, 0xEF];
    let new_notify = r#"{"subscribers":["app://wallet"]}"#;
    let new_history = r#"[{"attempt":1,"err":"timeout"}]"#;
    let new_batch = "batch-after-regroup";
    insert_proven_tx_req_with_timestamps(
        &reader_store,
        &txid,
        "completed",
        1,
        new_history,
        new_notify,
        true,
        &new_raw_tx,
        Some(&new_input_beef),
        Some(new_batch),
        writer_old_ts, // created_at preserved
        reader_new_ts,
    )
    .await;

    reader_mgr
        .sync_to_writer(&identity, writer_store.clone())
        .await
        .unwrap();

    let (status, attempts, history, notified, notify, raw_tx, input_beef, batch): (
        String,
        i64,
        String,
        i64,
        String,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<String>,
    ) = ::sqlx::query_as(
        r#"SELECT status, attempts, history, notified, notify, raw_tx, input_beef, batch
           FROM proven_tx_reqs WHERE txid = ?"#,
    )
    .bind(&txid)
    .fetch_one(writer_store.pool())
    .await
    .unwrap();

    assert_eq!(status, "completed", "status must take reader's newer value");
    assert_eq!(attempts, 1, "attempts must take reader's newer value");
    assert_eq!(history, new_history, "history must take reader's newer value");
    assert_eq!(notified, 1, "notified must take reader's newer value");
    assert_eq!(
        notify, new_notify,
        "notify must take reader's newer value (was dropped pre-fix on UPDATE)"
    );
    assert_eq!(
        raw_tx, new_raw_tx,
        "raw_tx must take reader's newer value (was dropped pre-fix on UPDATE)"
    );
    assert_eq!(
        input_beef.as_deref(),
        Some(new_input_beef.as_slice()),
        "input_beef must take reader's newer value (was dropped pre-fix on UPDATE)"
    );
    assert_eq!(
        batch.as_deref(),
        Some(new_batch),
        "batch must take reader's newer value (was dropped pre-fix on UPDATE)"
    );
}
