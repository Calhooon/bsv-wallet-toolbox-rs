//! Sync through the manager: what it checks of its stores and of a writer's
//! answers before and while it drains chunks.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

/// A store named `name` whose settings declare `chain`.
async fn chain_store(sik: &str, name: &str, chain: &str) -> StorageSqlx {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate(name, sik).await.unwrap();
    ::sqlx::query("UPDATE settings SET chain = ?")
        .bind(chain)
        .execute(s.writer())
        .await
        .unwrap();
    s.make_available().await.unwrap();
    s
}

async fn users(s: &StorageSqlx) -> i64 {
    count(s, "SELECT COUNT(*) FROM users").await
}

/// Each store's row of the wallet's user names `active`.
async fn name_active(stores: &[&StorageSqlx], active: &str) {
    for s in stores {
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, active)
            .await
            .unwrap();
    }
}

/// Every manager sync between a store declaring one chain and a store
/// declaring another is refused before the writer's user or any chunk.
#[tokio::test]
async fn a_sync_between_stores_of_two_chains_is_refused() {
    let text = "Sync error: Live sync requires matching declared network chains on both storage providers.";
    let main = chain_store(SRC, "main-store", "mainnet").await;
    main.find_or_insert_user(ALICE).await.unwrap();
    seed_all(&main, 1, 'a', true, true).await;
    let test = chain_store(DST, "test-store", "testnet").await;
    let main = Arc::new(main);
    let test = Arc::new(test);

    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(main.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    let to_writer = mgr.sync_to_writer(ALICE, test.clone()).await;
    let test_users_after_push = users(&test).await;
    let from_reader = mgr.sync_from_reader(ALICE, test.clone()).await;

    name_active(&[&*main, &*test], SRC).await;
    let both = WalletStorageManager::new(
        ALICE.to_string(),
        Some(main.clone() as Arc<dyn MonitorStorage>),
        Some(vec![test.clone() as Arc<dyn MonitorStorage>]),
    );
    let backups = both.update_backups().await;
    let set_active = both.set_active(DST).await;
    let test_txs = count(&test, "SELECT COUNT(*) FROM transactions").await;
    println!(
        "WITNESS M2 two chains: sync_to_writer {}; test users after it {test_users_after_push}; sync_from_reader {}; update_backups {}; set_active {}; test-chain transactions {test_txs}",
        refusal(&to_writer),
        refusal(&from_reader),
        refusal(&backups),
        refusal(&set_active)
    );
    assert_eq!(refusal(&to_writer), text);
    assert_eq!(test_users_after_push, 0);
    assert_eq!(refusal(&from_reader), text);
    assert_eq!(refusal(&backups), text);
    assert_eq!(refusal(&set_active), text);
    assert_eq!(test_txs, 0);

    // The same two stores declaring one chain sync as before.
    let other = Arc::new(chain_store(DST, "main-two", "mainnet").await);
    let r = mgr.sync_to_writer(ALICE, other.clone()).await;
    assert_eq!(refusal(&r), "ok");
}

/// A writer that answers a chunk that carried no row with `done = false`
/// is refused at once, not drained for ever under the manager's sync lock.
#[tokio::test]
async fn an_unfinished_answer_to_an_empty_chunk_is_refused() {
    let active = store(SRC).await;
    active.find_or_insert_user(ALICE).await.unwrap();
    let writer = Double::new((*store(DST).await).clone());
    *writer.answer.lock().unwrap() = Some(ProcessSyncChunkResult {
        done: false,
        max_updated_at: None,
        updates: 0,
        inserts: 0,
        error: None,
    });
    let writer = Arc::new(writer);
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(active as Arc<dyn MonitorStorage>),
        None,
    );
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        mgr.sync_to_writer(ALICE, writer.clone()),
    )
    .await;
    let answer = match &r {
        Ok(inner) => refusal(inner),
        Err(_) => "still draining after 10 s".to_string(),
    };
    println!(
        "WITNESS M4 done=false to an empty chunk: {answer}; chunks handed {}",
        writer.chunks.load(Ordering::SeqCst)
    );
    assert_eq!(
        answer,
        "Invalid operation: Sync provider reported an unfinished page without advancing its durable checkpoint."
    );
    // The store's default basket, then the empty chunk that is refused.
    assert_eq!(writer.chunks.load(Ordering::SeqCst), 2);
}

/// A writer's answer that carries an error is a failed sync, whatever its
/// `done` and its counts say.
#[tokio::test]
async fn an_answer_that_carries_an_error_is_a_failed_sync() {
    let active = store(SRC).await;
    active.find_or_insert_user(ALICE).await.unwrap();
    let writer = Double::new((*store(DST).await).clone());
    *writer.answer.lock().unwrap() = Some(ProcessSyncChunkResult {
        done: true,
        max_updated_at: None,
        updates: 0,
        inserts: 3,
        error: Some("the writer refused the page".to_string()),
    });
    name_active(&[&*active, &writer.inner], SRC).await;
    let writer = Arc::new(writer);
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(active as Arc<dyn MonitorStorage>),
        Some(vec![writer.clone() as Arc<dyn MonitorStorage>]),
    );
    let to_writer = mgr.sync_to_writer(ALICE, writer.clone()).await;
    let backups = mgr.update_backups().await;
    println!(
        "WITNESS M3 done=true with an error: sync_to_writer {}; update_backups {}",
        refusal(&to_writer),
        refusal(&backups)
    );
    assert_eq!(
        refusal(&to_writer),
        "Sync error: the writer refused the page"
    );
    assert_eq!(refusal(&backups), "Sync error: the writer refused the page");
}
