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
