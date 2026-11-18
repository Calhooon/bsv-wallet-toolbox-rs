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

/// A store's join answer, or the text of its panic.
fn joined<T>(r: std::result::Result<T, tokio::task::JoinError>) -> std::result::Result<T, String> {
    r.map_err(|e| match e.try_into_panic() {
        Ok(payload) => payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .map_or_else(|| "a panic".to_string(), |text| format!("panic: {text}")),
        Err(e) => e.to_string(),
    })
}

/// An open over three stores waits inside the store at index 0 while a
/// fourth store is added from another task, whose own open is held in the
/// fourth store's user read. The open over three answers (no panic) with
/// the active of the stores that are open; meanwhile `get_stores` lists
/// the fourth, pushed and not yet opened, with no key, no user and no role
/// (and, while the fourth's open runs, the three with no role: an open
/// resets the partition until it ends). Once the fourth's open ends, the
/// partition that follows takes it in, a backup.
#[tokio::test]
async fn a_partition_reads_only_the_stores_that_are_open() {
    let doubles = vec![
        double_naming(SRC, SRC).await,
        double_naming(DST, SRC).await,
        double_naming(THIRD, SRC).await,
    ];
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

    let fourth = double_naming(FOURTH, SRC).await;
    fourth.hold_user_read.store(true, Ordering::SeqCst);
    let add = {
        let (mgr, fourth) = (mgr.clone(), fourth.clone());
        tokio::spawn(async move {
            mgr.add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await
        })
    };
    bounded("the fourth store is pushed", async {
        while mgr.get_stores().await.len() < 4 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    // Room for the fourth's open to reach its held user read, were nothing
    // holding it.
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        fourth.user_read_entered.notified(),
    )
    .await;
    doubles[0].make_available_release.notify_one();
    let opened = joined(bounded("the open over three stores", open).await);
    let opened_word = match &opened {
        Ok(Ok(settings)) => format!("ok, active {}", name(&settings.storage_identity_key)),
        Ok(r) => refusal(r),
        Err(panic) => panic.clone(),
    };
    let held = mgr.get_stores().await;
    let held_names: Vec<(String, bool, bool, bool)> = held
        .iter()
        .map(|s| {
            (
                name(&s.storage_identity_key).to_string(),
                s.is_active,
                s.is_backup,
                s.is_conflicting,
            )
        })
        .collect();
    fourth.user_read_release.notify_one();
    let added = joined(bounded("the fourth store's open", add).await);
    let added_word = match &added {
        Ok(r) => refusal(r),
        Err(panic) => panic.clone(),
    };
    let backups_after = mgr.get_backup_stores().await;
    println!(
        "WITNESS a partition beside a store not yet opened: open {opened_word}; get_stores (name, active, backup, conflicting) {held_names:?}, the fourth's user id {}; add {added_word}; backups after {}",
        held[3].user_id,
        backups_after.len()
    );
    assert_eq!(opened_word, "ok, active A");
    assert_eq!(held.len(), 4);
    let held_keys: Vec<&str> = held_names.iter().map(|n| n.0.as_str()).collect();
    assert_eq!(held_keys, vec!["A", "B", "C", "?"]);
    assert_eq!(held_names[3], ("?".to_string(), false, false, false));
    assert!(held[3].storage_identity_key.is_empty());
    assert_eq!(held[3].user_id, 0);
    assert_eq!(added_word, "ok");
    assert_eq!(backups_after.len(), 3);
    assert!(backups_after.iter().any(|k| k == FOURTH));
    assert_eq!(state(&mgr).await, ("A".into(), true));
}

/// A `set_active` from A to B waits inside its backup's sync while a third
/// store is added from another task, whose open is held in the new store's
/// user read. Once the sync is released the transition calls nothing on
/// the new store (no user read, no `set_active`) and completes over the
/// two open stores while the new store's open is still held (the active is
/// B; the manager answers not enabled until the open ends). Once that
/// open ends the new store is a backup, and the next `set_active` to B
/// reads its row too and answers unchanged.
#[tokio::test]
async fn a_set_active_beside_an_add_calls_only_the_open_stores() {
    let doubles = vec![double_naming(SRC, SRC).await, double_naming(DST, SRC).await];
    let mgr = Arc::new(manager_over(&doubles));
    mgr.make_available().await.unwrap();
    doubles[1].hold_sync.store(true, Ordering::SeqCst);
    let mut switch = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.set_active(DST).await })
    };
    bounded(
        "the transition's backup sync begins",
        doubles[1].sync_entered.notified(),
    )
    .await;

    let fourth = double_naming(FOURTH, DST).await;
    fourth.hold_user_read.store(true, Ordering::SeqCst);
    let add = {
        let (mgr, fourth) = (mgr.clone(), fourth.clone());
        tokio::spawn(async move {
            mgr.add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await
        })
    };
    bounded(
        "the new store's open reaches its user read",
        fourth.user_read_entered.notified(),
    )
    .await;
    let reads_at_hold = fourth.user_reads.load(Ordering::SeqCst);
    doubles[1].sync_release.notify_one();
    let early = tokio::time::timeout(std::time::Duration::from_secs(3), &mut switch)
        .await
        .ok();
    let ended_while_held = early.is_some();
    let reads_by_switch = fourth.user_reads.load(Ordering::SeqCst) - reads_at_hold;
    let set_actives_on_new: Vec<String> = fourth
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.contains("enter"))
        .cloned()
        .collect();
    let held_active = match mgr.get_active_store().await {
        Ok(key) => name(&key).to_string(),
        Err(e) => e.to_string(),
    };

    fourth.user_read_release.notify_one();
    let added = bounded("the new store's open", add).await.unwrap();
    let switched = match early {
        Some(switched) => switched.unwrap(),
        None => bounded("set_active once the open ends", switch)
            .await
            .unwrap(),
    };
    let reads_before_next = fourth.user_reads.load(Ordering::SeqCst);
    let next = mgr.set_active(DST).await;
    let reads_by_next = fourth.user_reads.load(Ordering::SeqCst) - reads_before_next;
    let backups = mgr.get_backup_stores().await;
    println!(
        "WITNESS set_active beside an add: ended while the open was held {ended_while_held}; the new store's user reads by the transition {reads_by_switch}, its set_active calls {set_actives_on_new:?}; active while held {held_active}; set_active {}; add {}; next {:?}, its reads of the new store {reads_by_next}; backups {}; {:?}",
        refusal(&switched),
        refusal(&added),
        next.as_ref().map(|s| s.trim().to_string()).map_err(|e| e.to_string()),
        backups.len(),
        state(&mgr).await
    );
    assert_eq!(
        reads_by_switch, 0,
        "the transition read the store being added"
    );
    assert!(set_actives_on_new.is_empty(), "{set_actives_on_new:?}");
    assert!(ended_while_held);
    // While the new store's open runs the manager is not enabled (its
    // row is not yet read); the active is the target.
    assert_eq!(held_active, "B");
    assert_eq!(refusal(&switched), "ok");
    assert_eq!(refusal(&added), "ok");
    assert_eq!(next.unwrap(), format!("setActive to {DST} unchanged\n"));
    assert_eq!(reads_by_next, 1);
    assert!(backups.iter().any(|k| k == FOURTH));
    assert_eq!(state(&mgr).await, ("B".into(), true));
}

/// A `set_active` to the active waits inside its re-read of A's row while
/// a third store is added from another task, whose open waits in the new
/// store's user read. The no-op does not wait for that open, and the open
/// changes no role until its partition: the call reads the roles whole
/// and answers unchanged while the add's open is held; then the add
/// completes.
#[tokio::test]
async fn a_set_active_no_op_beside_an_add_reads_the_roles_whole() {
    let doubles = vec![double_naming(SRC, SRC).await, double_naming(DST, SRC).await];
    let mgr = Arc::new(manager_over(&doubles));
    mgr.make_available().await.unwrap();
    doubles[0].hold_user_read.store(true, Ordering::SeqCst);
    let switch = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.set_active(SRC).await })
    };
    bounded(
        "set_active's re-read of A begins",
        doubles[0].user_read_entered.notified(),
    )
    .await;

    let fourth = double_naming(FOURTH, SRC).await;
    fourth.hold_user_read.store(true, Ordering::SeqCst);
    let add = {
        let (mgr, fourth) = (mgr.clone(), fourth.clone());
        tokio::spawn(async move {
            mgr.add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await
        })
    };
    bounded("the new store is pushed", async {
        while mgr.get_stores().await.len() < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    // The add's open reaches the new store's user read while the no-op
    // waits on A.
    let reached = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        fourth.user_read_entered.notified(),
    )
    .await
    .is_ok();
    doubles[0].user_read_release.notify_one();
    let switched = bounded("set_active once A answers", switch).await.unwrap();
    fourth.user_read_release.notify_one();
    let added = bounded("the add", add).await.unwrap();
    println!(
        "WITNESS a no-op set_active beside an add: the add's open reached its store while the call decided {reached}; set_active {:?}; add {}; {:?}",
        switched.as_ref().map(|s| s.trim().to_string()).map_err(|e| e.to_string()),
        refusal(&added),
        state(&mgr).await
    );
    assert!(reached, "the no-op kept the add's open waiting");
    assert_eq!(switched.unwrap(), format!("setActive to {SRC} unchanged\n"));
    assert_eq!(refusal(&added), "ok");
    assert_eq!(mgr.get_backup_stores().await.len(), 2);
    assert_eq!(state(&mgr).await, ("A".into(), true));
}
