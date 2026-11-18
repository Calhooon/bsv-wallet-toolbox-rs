//! The manager's state locks are held only to copy out or to write its own
//! fields, never across a store's call nor across a call that takes one of
//! its locks again: a backup that waits inside its sync, or a store that
//! never answers `make_available`, keeps no other task of the manager
//! waiting on a lock. Every wait is bounded at ten seconds.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bsv_wallet_toolbox_rs::storage::*;

mod sync_support;
use sync_support::cells::*;
use sync_support::double::Double;
use sync_support::*;

const FOURTH: &str = "4444444444444444444444444444444444444444444444444444444444444444";

/// A store whose row of the wallet's user names `active`.
async fn double_naming(sik: &str, active: &str) -> Arc<Double> {
    let s = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
        .await
        .unwrap();
    s.migrate(name(sik), sik).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    s.update_user_active_storage(u.user_id, active)
        .await
        .unwrap();
    Arc::new(Double::new(s))
}

/// `update_backups` over two backups, the one it syncs ahead waiting inside its sync:
/// meanwhile a store is added (a writer on the stores lock) and
/// `set_active` to the active runs from another task. Both complete while
/// the backup waits; once it is released `update_backups` completes too,
/// having synced both backups it began with.
#[tokio::test]
async fn update_backups_holds_no_lock_while_a_backup_syncs() {
    let cell = Cell {
        n: 3,
        switched: false,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    doubles[1].hold_sync.store(true, Ordering::SeqCst);
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let backups = {
        let (mgr, order) = (mgr.clone(), order.clone());
        tokio::spawn(async move {
            let r = mgr.update_backups().await;
            order.lock().unwrap().push("update_backups");
            r
        })
    };
    bounded(
        "the backup's sync begins",
        doubles[1].sync_entered.notified(),
    )
    .await;

    let fourth = double_naming(FOURTH, SRC).await;
    let add = {
        let (mgr, order) = (mgr.clone(), order.clone());
        tokio::spawn(async move {
            let r = mgr
                .add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await;
            order.lock().unwrap().push("add_wallet_storage_provider");
            r
        })
    };
    let switch = {
        let (mgr, order) = (mgr.clone(), order.clone());
        tokio::spawn(async move {
            let r = mgr.set_active(SRC).await;
            order.lock().unwrap().push("set_active");
            r
        })
    };
    let added = bounded("add_wallet_storage_provider while a backup syncs", add)
        .await
        .unwrap();
    let switched = bounded("set_active while a backup syncs", switch)
        .await
        .unwrap();
    let while_held = order.lock().unwrap().clone();

    doubles[1].sync_release.notify_one();
    let backed = bounded("update_backups once the backup is released", backups)
        .await
        .unwrap();
    let after = order.lock().unwrap().clone();
    let stores = mgr.get_stores().await.len();
    println!(
        "WITNESS a backup held inside its sync: while held {while_held:?}; added {}; set_active {}; then {after:?}; update_backups {}; stores {stores}; {:?}",
        refusal(&added),
        refusal(&switched),
        refusal(&backed),
        state(&mgr).await
    );
    assert_eq!(refusal(&added), "ok");
    assert!(switched.unwrap().contains("unchanged"));
    assert_eq!(while_held.len(), 2, "{while_held:?}");
    assert_eq!(after.last(), Some(&"update_backups"));
    let log = backed.unwrap();
    assert!(log.contains("BACKUP CURRENT ACTIVE TO 2 STORES"), "{log}");
    assert_eq!(log.matches("syncToWriter complete").count(), 2, "{log}");
    assert_eq!(stores, 4);
    assert_eq!(state(&mgr).await, ("A".into(), true));
}

/// A store that never answers `make_available`: while the manager's
/// `make_available` waits on it, `is_active_enabled` (false), a plain read
/// of the manager (`get_stores`: two stores, none active; and
/// `get_active_store`, refused: no active yet) and `is_available` (false)
/// answer from other tasks, without waiting on that store.
#[tokio::test]
async fn a_store_that_never_answers_make_available_keeps_no_reader_waiting() {
    let doubles = vec![double_naming(SRC, SRC).await, double_naming(DST, SRC).await];
    doubles[1].hang_make_available.store(true, Ordering::SeqCst);
    let mgr = Arc::new(manager_over(&doubles));
    let opening = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.make_available().await })
    };
    bounded(
        "the store's make_available begins",
        doubles[1].make_available_entered.notified(),
    )
    .await;

    let enabled = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.is_active_enabled().await })
    };
    let read = {
        let mgr = mgr.clone();
        tokio::spawn(async move { (mgr.get_stores().await, mgr.get_active_store().await) })
    };
    let enabled = bounded("is_active_enabled while a store hangs", enabled)
        .await
        .unwrap();
    let (stores, active) = bounded("a plain read while a store hangs", read)
        .await
        .unwrap();
    let available = bounded("is_available while a store hangs", mgr.is_available()).await;
    let actives: Vec<bool> = stores.iter().map(|s| s.is_active).collect();
    println!(
        "WITNESS a store hung in make_available: is_active_enabled {enabled}; get_stores {} stores, active {actives:?}; get_active_store {}; is_available {available}",
        stores.len(),
        refusal(&active)
    );
    assert!(!enabled);
    assert_eq!(actives, vec![false, false]);
    assert_eq!(refusal(&active), "Storage not available");
    assert!(!available);
    assert!(!opening.is_finished());
    opening.abort();
}

/// A store with no row of the wallet's user yet.
async fn double_without_user(sik: &str) -> Arc<Double> {
    let s = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
        .await
        .unwrap();
    s.migrate(name(sik), sik).await.unwrap();
    s.make_available().await.unwrap();
    Arc::new(Double::new(s))
}

/// Each store's `(make_available, find_or_insert_user)` calls.
fn calls(doubles: &[Arc<Double>]) -> Vec<(usize, usize)> {
    doubles
        .iter()
        .map(|d| {
            (
                d.make_available_calls.load(Ordering::SeqCst),
                d.user_reads.load(Ordering::SeqCst),
            )
        })
        .collect()
}

/// Two opens at once over three stores with no row of the wallet's user:
/// the one ahead waits inside the store at index 0; the second waits for
/// it and then finds every store open. Both answer Ok, and each store's
/// `make_available` and `find_or_insert_user` are called once in all. A
/// third task reads `is_active_enabled` meanwhile: it answers, false.
#[tokio::test]
async fn two_opens_at_once_open_each_store_once() {
    let doubles = vec![
        double_without_user(SRC).await,
        double_without_user(DST).await,
        double_without_user(THIRD).await,
    ];
    doubles[0].hold_make_available.store(true, Ordering::SeqCst);
    let mgr = Arc::new(manager_over(&doubles));
    let one = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.make_available().await })
    };
    bounded(
        "the open's store call begins",
        doubles[0].make_available_entered.notified(),
    )
    .await;
    let mut two = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.make_available().await })
    };
    let enabled = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.is_active_enabled().await })
    };
    let enabled = bounded("is_active_enabled while an open waits", enabled)
        .await
        .unwrap();
    // Room for the second open to run, were nothing holding it.
    let early = tokio::time::timeout(std::time::Duration::from_millis(300), &mut two)
        .await
        .ok();
    let second_ended_early = early.is_some();
    doubles[0].make_available_release.notify_one();
    let one = bounded("the open ahead", one).await.unwrap();
    let two = match early {
        Some(two) => two.unwrap(),
        None => bounded("the second open", two).await.unwrap(),
    };
    let counted = calls(&doubles);
    println!(
        "WITNESS two opens at once: one {}; two {}; second ended while the store waited {second_ended_early}; is_active_enabled meanwhile {enabled}; (make_available, find_or_insert_user) per store {counted:?}",
        refusal(&one),
        refusal(&two)
    );
    assert_eq!(refusal(&one), "ok");
    assert_eq!(refusal(&two), "ok");
    assert!(!enabled);
    assert!(!second_ended_early);
    assert_eq!(counted, vec![(1, 1); 3]);
}

/// A `set_active` and an open at once: the open waits inside a store, the
/// `set_active` waits for it; once the store answers both complete.
#[tokio::test]
async fn a_set_active_beside_an_open_waits_for_it_and_completes() {
    let doubles = vec![double_naming(SRC, SRC).await, double_naming(DST, SRC).await];
    doubles[0].hold_make_available.store(true, Ordering::SeqCst);
    let mgr = Arc::new(manager_over(&doubles));
    let open = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.make_available().await })
    };
    bounded(
        "the open's store call begins",
        doubles[0].make_available_entered.notified(),
    )
    .await;
    let switch = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.set_active(DST).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    doubles[0].make_available_release.notify_one();
    let opened = bounded("the open", open).await.unwrap();
    let switched = bounded("set_active beside the open", switch).await.unwrap();
    println!(
        "WITNESS set_active beside an open: open {}; set_active {}; {:?}",
        refusal(&opened),
        refusal(&switched),
        state(&mgr).await
    );
    assert_eq!(refusal(&opened), "ok");
    assert_eq!(refusal(&switched), "ok");
    assert_eq!(state(&mgr).await, ("B".into(), true));
}

/// `update_backups` over two backups (B, then C): once B is synced, the
/// loop waits in C's `make_available` while a `set_active` to C runs to
/// its end from another task. Then C is released: the loop passes over
/// C, now the active, and no store is synced to itself (the doubles
/// record the reader and writer of each chunk). Both calls answer.
#[tokio::test]
async fn update_backups_never_syncs_a_store_to_itself() {
    let cell = Cell {
        n: 3,
        switched: false,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    for d in &doubles {
        d.syncs.lock().unwrap().clear();
    }
    doubles[2].hold_make_available.store(true, Ordering::SeqCst);
    let backups = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.update_backups().await })
    };
    bounded(
        "the loop reaches the next backup",
        doubles[2].make_available_entered.notified(),
    )
    .await;
    let switch = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.set_active(THIRD).await })
    };
    let switched = bounded("set_active to the next backup", switch)
        .await
        .unwrap();
    doubles[2].make_available_release.notify_one();
    let backed = bounded("update_backups once the backup is released", backups)
        .await
        .unwrap();
    let syncs: Vec<(String, String)> = doubles
        .iter()
        .flat_map(|d| d.syncs.lock().unwrap().clone())
        .map(|(from, to)| (name(&from).to_string(), name(&to).to_string()))
        .collect();
    let to_itself: Vec<_> = syncs.iter().filter(|(from, to)| from == to).collect();
    println!(
        "WITNESS a switch to the next backup while update_backups runs: set_active {}; update_backups {}; syncs (reader, writer) {syncs:?}; {:?}",
        refusal(&switched),
        refusal(&backed),
        state(&mgr).await
    );
    assert_eq!(refusal(&switched), "ok");
    assert!(to_itself.is_empty(), "a store synced to itself: {syncs:?}");
    let log = backed.unwrap();
    assert_eq!(log.matches("syncToWriter complete").count(), 1, "{log}");
    assert!(log.contains("passes over C"), "{log}");
    assert_eq!(state(&mgr).await, ("C".into(), true));
}
