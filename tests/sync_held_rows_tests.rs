//! What the manager answers about its active from the rows it holds, apart
//! from the `set_active` matrix (`tests/sync_set_active_tests.rs`): a
//! no-op call dropped while it reads the rows, what `get_stores` answers
//! after a call dropped part way, and the user row a sync hands the active
//! after a row the manager could not read.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

/// Stores A and B, both rows naming A, under a manager made available on A.
async fn two() -> (Vec<Arc<Double>>, WalletStorageManager) {
    let mut doubles = Vec::new();
    for sik in [SRC, DST] {
        let s = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
            .await
            .unwrap();
        s.migrate(if sik == SRC { "A" } else { "B" }, sik)
            .await
            .unwrap();
        s.make_available().await.unwrap();
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, SRC).await.unwrap();
        doubles.push(Arc::new(Double::new(s)));
    }
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(doubles[0].clone() as Arc<dyn MonitorStorage>),
        Some(vec![doubles[1].clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    (doubles, mgr)
}

async fn rows(doubles: &[Arc<Double>]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for d in doubles {
        out.push(active_storage_of(&d.inner, ALICE).await);
    }
    out
}

/// `set_active(A)` to the manager's own active, every row naming A, B's
/// read never answering: the call is dropped while it reads, before any
/// write. It wrote nothing, so the manager still answers A as enabled;
/// once B answers, the call answers unchanged.
#[tokio::test]
async fn a_no_op_dropped_while_it_reads_leaves_the_manager_enabled() {
    let (doubles, mgr) = two().await;
    doubles[1].hang_user_reads.store(true, Ordering::SeqCst);
    let dropped = tokio::time::timeout(Duration::from_millis(300), mgr.set_active(SRC))
        .await
        .is_err();
    let after = (
        rows(&doubles).await,
        mgr.get_active_store().await.unwrap() == SRC,
        mgr.is_active_enabled().await,
    );
    doubles[1].hang_user_reads.store(false, Ordering::SeqCst);
    let again = bounded("the no-op once B answers", mgr.set_active(SRC)).await;
    println!(
        "WITNESS a no-op dropped while B's read hangs: dropped {dropped}; rows, on A, enabled {after:?}; once B answers: {}",
        refusal(&again)
    );
    assert!(dropped);
    assert_eq!(
        after,
        (
            vec![Some(SRC.to_string()), Some(SRC.to_string())],
            true,
            true
        )
    );
    assert_eq!(again.unwrap(), format!("setActive to {SRC} unchanged\n"));
}

/// `set_active(B)` whose target writes its row and never answers, the
/// call dropped: every row names B, and `get_stores` answers the active
/// (still A) as not enabled, as `is_active_enabled` does; a retry once B
/// answers completes and both answer B as enabled.
#[tokio::test]
async fn get_stores_answers_not_enabled_after_a_dropped_switch() {
    let (doubles, mgr) = two().await;
    doubles[1].hang_after_write.store(true, Ordering::SeqCst);
    let dropped = tokio::time::timeout(Duration::from_millis(300), mgr.set_active(DST))
        .await
        .is_err();
    let enabled = |stores: Vec<WalletStorageInfo>| {
        stores
            .iter()
            .find(|s| s.is_active)
            .map(|s| (s.storage_identity_key == SRC, s.is_enabled))
    };
    let after = (
        rows(&doubles).await,
        enabled(mgr.get_stores().await),
        mgr.is_active_enabled().await,
    );
    doubles[1].hang_after_write.store(false, Ordering::SeqCst);
    let retry = refusal(&bounded("the retry", mgr.set_active(DST)).await);
    let after_retry = (
        enabled(mgr.get_stores().await),
        mgr.is_active_enabled().await,
    );
    println!(
        "WITNESS set_active(B) dropped after B wrote: dropped {dropped}; rows, (active is A, enabled) by get_stores, is_active_enabled {after:?}; retry {retry}: {after_retry:?}"
    );
    assert!(dropped);
    assert_eq!(
        after,
        (
            vec![Some(DST.to_string()), Some(DST.to_string())],
            Some((true, false)),
            false
        )
    );
    assert_eq!(retry, "ok");
    assert_eq!(after_retry, (Some((false, true)), true));
}

/// The manager holds A's row as naming no active (A wrote its row, lost
/// its answer and refused the read): A's row as A reads it, after a failed
/// `set_active(B)`.
async fn unreadable_active() -> (Vec<Arc<Double>>, WalletStorageManager, String) {
    let (doubles, mgr) = two().await;
    let a = &doubles[0];
    a.lose_set_active_answer.store(true, Ordering::SeqCst);
    a.refuse_reads_after_lost_answer
        .store(true, Ordering::SeqCst);
    let failed = refusal(&mgr.set_active(DST).await);
    a.lose_set_active_answer.store(false, Ordering::SeqCst);
    a.refuse_reads_after_lost_answer
        .store(false, Ordering::SeqCst);
    a.refuse_user_reads.store(false, Ordering::SeqCst);
    assert_eq!(failed, "Storage error: A answer lost");
    (doubles, mgr, failed)
}

/// A reader holding Alice's row, written after every row of A and B, naming
/// `names`, and one output basket of hers, `rbk`.
async fn reader_naming(names: &str) -> Arc<bsv_wallet_toolbox_rs::StorageSqlx> {
    let reader = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
        .await
        .unwrap();
    reader
        .migrate("R", sync_support::cells::THIRD)
        .await
        .unwrap();
    reader.make_available().await.unwrap();
    let (u, _) = reader.find_or_insert_user(ALICE).await.unwrap();
    exec(&reader, &format!("INSERT INTO output_baskets (user_id,name,number_of_desired_utxos,minimum_desired_utxo_value,created_at,updated_at) VALUES ({},'rbk',6,1000,'2026-01-01 00:00:00','2026-01-01 00:00:00')", u.user_id)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    reader
        .update_user_active_storage(u.user_id, names)
        .await
        .unwrap();
    Arc::new(reader)
}

/// After an active whose row the manager could not read, a
/// `sync_from_reader` from a store whose row names A hands A no user row,
/// never an empty one nor the reader's.
#[tokio::test]
async fn a_sync_after_an_unreadable_active_hands_the_writer_no_user_row() {
    let (doubles, mgr, failed) = unreadable_active().await;
    let reader = reader_naming(SRC).await;
    let synced = refusal(
        &mgr.sync_from_reader(ALICE, reader as Arc<dyn MonitorStorage>)
            .await,
    );
    let a = &doubles[0];
    let recorded = a.user_rows.lock().unwrap().clone();
    let chunks = a.chunks.load(Ordering::SeqCst);
    println!(
        "WITNESS A loses its answer and is unreadable: {failed}; sync_from_reader {synced}; chunks to A {chunks}; user rows handed to A: {recorded:?}"
    );
    assert_eq!(synced, "ok");
    assert!(chunks > 0);
    assert!(recorded.is_empty());
}

/// After an active whose row the manager could not read, a
/// `sync_from_reader` from a reader whose row is newer and names another
/// store: A is handed no user row, A's stored row of Alice names what it
/// named before the sync, the chunk's basket is on A, and once a retried
/// `set_active(B)` completes a sync hands the manager's active, B, again.
#[tokio::test]
async fn a_newer_reader_row_naming_another_store_leaves_the_active_row_as_it_was() {
    let (doubles, mgr, _) = unreadable_active().await;
    let a = &doubles[0];
    let before = active_storage_of(&a.inner, ALICE).await;
    let reader = reader_naming(sync_support::cells::THIRD).await;
    let synced = refusal(
        &mgr.sync_from_reader(ALICE, reader.clone() as Arc<dyn MonitorStorage>)
            .await,
    );
    let recorded = a.user_rows.lock().unwrap().clone();
    let after = active_storage_of(&a.inner, ALICE).await;
    let baskets = count(
        &a.inner,
        "SELECT COUNT(*) FROM output_baskets WHERE name = 'rbk'",
    )
    .await;
    println!(
        "WITNESS sync_from_reader {synced}; user rows handed to A: {recorded:?}; A's row before {before:?} after {after:?}; baskets rbk on A {baskets}"
    );
    assert_eq!(synced, "ok");
    assert!(recorded.is_empty());
    assert!(before.is_some());
    assert_eq!(after, before);
    assert_eq!(baskets, 1);

    let retry = refusal(&mgr.set_active(DST).await);
    let b = &doubles[1];
    b.user_rows.lock().unwrap().clear();
    let again = refusal(
        &mgr.sync_from_reader(ALICE, reader as Arc<dyn MonitorStorage>)
            .await,
    );
    let handed = b.user_rows.lock().unwrap().clone();
    println!("WITNESS retry {retry}; second sync {again}; user rows handed to B: {handed:?}");
    assert_eq!(retry, "ok");
    assert_eq!(again, "ok");
    assert!(!handed.is_empty());
    assert!(handed.iter().all(|r| r.as_deref() == Some(DST)));
}
