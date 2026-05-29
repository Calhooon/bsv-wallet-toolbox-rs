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
