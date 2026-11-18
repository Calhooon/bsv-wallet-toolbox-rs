//! A store's `find_or_insert_user` answers every caller at once for one
//! identity the store does not hold: one row, one default basket, and
//! exactly one caller told the row is new.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use ::sqlx::Row;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

const SIK: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const IDENTITY: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CALLERS: usize = 16;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn callers_at_once_for_one_new_identity_are_all_answered_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let store = StorageSqlx::open(path.to_str().unwrap()).await.unwrap();
    store.migrate("store", SIK).await.unwrap();
    store.make_available().await.unwrap();
    let store = Arc::new(store);

    let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
    let tasks: Vec<_> = (0..CALLERS)
        .map(|_| {
            let (store, barrier) = (store.clone(), barrier.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                store.find_or_insert_user(IDENTITY).await
            })
        })
        .collect();
    let mut answers = Vec::new();
    for t in tasks {
        answers.push(t.await.unwrap());
    }
    let errors: Vec<String> = answers
        .iter()
        .filter_map(|a| a.as_ref().err().map(|e| e.to_string()))
        .collect();
    let new_count = answers
        .iter()
        .filter(|a| matches!(a, Ok((_, true))))
        .count();
    let mut ids: Vec<i64> = answers
        .iter()
        .filter_map(|a| a.as_ref().ok().map(|(u, _)| u.user_id))
        .collect();
    ids.sort_unstable();
    ids.dedup();

    let rows: i64 = ::sqlx::query("SELECT COUNT(*) FROM users WHERE identity_key = ?")
        .bind(IDENTITY)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get(0);
    let baskets: i64 = ::sqlx::query(
        "SELECT COUNT(*) FROM output_baskets b JOIN users u ON u.user_id = b.user_id WHERE u.identity_key = ? AND b.name = 'default'",
    )
    .bind(IDENTITY)
    .fetch_one(store.pool())
    .await
    .unwrap()
    .get(0);
    println!(
        "WITNESS {CALLERS} callers at once: ok {}; errors {errors:?}; told new {new_count}; user ids {ids:?}; rows {rows}; default baskets {baskets}",
        CALLERS - errors.len()
    );
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(new_count, 1);
    assert_eq!(ids.len(), 1);
    assert_eq!(rows, 1);
    assert_eq!(baskets, 1);

    // The row and basket the new caller was answered stay the user's: a
    // later call finds them and writes nothing.
    let (again, is_new) = store.find_or_insert_user(IDENTITY).await.unwrap();
    assert!(!is_new);
    assert_eq!(again.user_id, ids[0]);
    assert_eq!(again.active_storage.as_deref(), Some(SIK));
}
