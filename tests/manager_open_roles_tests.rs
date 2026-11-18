//! An open of the stores changes the manager's roles only through the
//! partition at its end: while a store being added is opened, and after
//! its open fails, the manager answers on the roles it had. Every wait is
//! bounded.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::WalletStorageManager;

mod sync_support;
use sync_support::cells::*;
use sync_support::*;

const FOURTH: &str = "4444444444444444444444444444444444444444444444444444444444444444";

/// Awaits `f` for at most three seconds: its answer, or the words
/// `no answer` (a call left waiting is part of what a witness reports).
async fn within<F: std::future::Future>(f: F) -> std::result::Result<F::Output, String> {
    tokio::time::timeout(Duration::from_secs(3), f)
        .await
        .map_err(|_| "no answer within 3 s".to_string())
}

/// What the manager answers: a plain write's store, `update_backups`'
/// count of backups and its error, `is_active_enabled`.
async fn answers(mgr: &Arc<WalletStorageManager>) -> (String, String, bool) {
    let write = within(mgr.run_as_writer(|active| async move {
        Ok(name(&active.storage_identity_key()).to_string())
    }))
    .await
    .map(|r| r.unwrap_or_else(|e| e.to_string()))
    .unwrap_or_else(|e| e);
    let backups = within(mgr.update_backups())
        .await
        .map(|r| match r {
            Ok(log) => log.lines().next().unwrap_or_default().to_string(),
            Err(e) => e.to_string(),
        })
        .unwrap_or_else(|e| e);
    let enabled = within(mgr.is_active_enabled()).await.unwrap_or(false);
    (write, backups, enabled)
}

fn synced(doubles: &[Arc<sync_support::double::Double>]) -> usize {
    doubles.iter().map(|d| d.syncs.lock().unwrap().len()).sum()
}

/// A store added whose open is held in its user read: meanwhile a plain
/// write lands on the active, `update_backups` syncs the two backups the
/// manager had and `is_active_enabled` answers true, as `get_stores` does
/// for the active. The held open then fails: the add answers the store's
/// error, the same three answers hold,
/// and `get_stores` lists the store with no key (not open). The next open
/// tries it again and takes it in.
#[tokio::test]
async fn an_add_whose_open_is_held_or_fails_leaves_every_role() {
    let cell = Cell {
        n: 3,
        switched: false,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    let fourth = Arc::new(sync_support::double::Double::new(
        (*store(FOURTH).await).clone(),
    ));
    {
        let (u, _) = fourth.inner.find_or_insert_user(ALICE).await.unwrap();
        fourth
            .inner
            .update_user_active_storage(u.user_id, SRC)
            .await
            .unwrap();
    }
    fourth.hold_user_read.store(true, Ordering::SeqCst);
    let add = {
        let (mgr, fourth) = (mgr.clone(), fourth.clone());
        tokio::spawn(async move {
            mgr.add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await
        })
    };
    bounded(
        "the added store's open reaches its user read",
        fourth.user_read_entered.notified(),
    )
    .await;

    let before = synced(&doubles);
    let held = answers(&mgr).await;
    let held_syncs = synced(&doubles) - before;
    // `get_stores` answers the active's `is_enabled` as `is_active_enabled`.
    let listed_enabled = mgr
        .get_stores()
        .await
        .iter()
        .any(|s| s.is_active && s.is_enabled);

    fourth.refuse_user_reads.store(true, Ordering::SeqCst);
    fourth.user_read_release.notify_one();
    let added = bounded("the add once its store fails", add).await.unwrap();
    let before = synced(&doubles);
    let failed = answers(&mgr).await;
    let failed_syncs = synced(&doubles) - before;
    let stores = mgr.get_stores().await;
    let listed: Vec<String> = stores
        .iter()
        .map(|s| name(&s.storage_identity_key).to_string())
        .collect();
    let available = mgr.is_available().await;

    fourth.refuse_user_reads.store(false, Ordering::SeqCst);
    let again = within(mgr.make_available()).await.map(|r| refusal(&r));
    let again_ok = again == Ok("ok".to_string());
    let backups_after = mgr.get_backup_stores().await.len();
    println!(
        "WITNESS an add held, then failing: while held (write, update_backups, enabled) {held:?}, chunks synced {held_syncs}, get_stores' active enabled {listed_enabled}; add {}; after (write, update_backups, enabled) {failed:?}, chunks synced {failed_syncs}; get_stores {listed:?}; is_available {available}; the next open {again:?}, backups {backups_after}",
        refusal(&added)
    );
    let expected = (
        "A".to_string(),
        "BACKUP CURRENT ACTIVE TO 2 STORES".to_string(),
        true,
    );
    assert_eq!(held, expected);
    assert!(held_syncs >= 2, "{held_syncs}");
    assert_eq!(listed_enabled, held.2);
    assert_eq!(refusal(&added), "Storage error: rows refused the user read");
    assert_eq!(failed, expected);
    assert!(failed_syncs >= 2, "{failed_syncs}");
    assert_eq!(listed, vec!["A", "B", "C", "?"]);
    assert!(stores[3].storage_identity_key.is_empty());
    assert!(available);
    assert!(again_ok, "{again:?}");
    assert_eq!(backups_after, 3);
}

/// A `set_active` from A to C whose transition has begun (held in B's
/// backup sync), then a store added whose open is held: the transition's
/// backup syncs every backup it had (B and C), completes while the add's
/// open is held, and a row written to A before the switch is read back
/// from C after it. The added store's row names A, so once its open ends
/// it is conflicting, as it would be were it added after the switch.
#[tokio::test]
async fn a_switch_beside_an_add_backs_up_every_backup_it_had() {
    let cell = Cell {
        n: 3,
        switched: false,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    let (uid, _) = doubles[0].inner.find_or_insert_user(ALICE).await.unwrap();
    exec(
        &doubles[0].inner,
        &format!(
            "INSERT INTO output_baskets (user_id,name,number_of_desired_utxos,minimum_desired_utxo_value,created_at,updated_at) VALUES ({},'held-row',6,1000,'2026-01-01 00:00:00','2026-01-01 00:00:00')",
            uid.user_id
        ),
    )
    .await;
    for d in &doubles {
        d.syncs.lock().unwrap().clear();
    }
    doubles[1].hold_sync.store(true, Ordering::SeqCst);
    let mut switch = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.set_active(THIRD).await })
    };
    bounded(
        "the transition's backup sync of B begins",
        doubles[1].sync_entered.notified(),
    )
    .await;

    let fourth = Arc::new(sync_support::double::Double::new(
        (*store(FOURTH).await).clone(),
    ));
    {
        let (u, _) = fourth.inner.find_or_insert_user(ALICE).await.unwrap();
        fourth
            .inner
            .update_user_active_storage(u.user_id, SRC)
            .await
            .unwrap();
    }
    fourth.hold_user_read.store(true, Ordering::SeqCst);
    let add = {
        let (mgr, fourth) = (mgr.clone(), fourth.clone());
        tokio::spawn(async move {
            mgr.add_wallet_storage_provider(fourth as Arc<dyn MonitorStorage>)
                .await
        })
    };
    bounded(
        "the added store's open reaches its user read",
        fourth.user_read_entered.notified(),
    )
    .await;
    doubles[1].sync_release.notify_one();
    let early = tokio::time::timeout(Duration::from_secs(3), &mut switch)
        .await
        .ok();
    let ended_while_held = early.is_some();
    let mut writers: Vec<String> = doubles
        .iter()
        .flat_map(|d| d.syncs.lock().unwrap().clone())
        .filter(|(from, _)| from == SRC)
        .map(|(_, to)| name(&to).to_string())
        .collect();
    writers.dedup();
    let on_target = count(
        &doubles[2].inner,
        "SELECT COUNT(*) FROM output_baskets WHERE name = 'held-row'",
    )
    .await;

    fourth.user_read_release.notify_one();
    let switched = match early {
        Some(r) => r.unwrap(),
        None => bounded("set_active once the open ends", switch)
            .await
            .unwrap(),
    };
    let added = bounded("the add", add).await.unwrap();
    let active = mgr
        .get_active_store()
        .await
        .map(|k| name(&k).to_string())
        .map_err(|e| e.to_string());
    let conflicting = mgr.get_conflicting_stores().await;
    let retry = mgr.set_active(THIRD).await;
    println!(
        "WITNESS a switch beside an add: ended while the open was held {ended_while_held}; A synced to {writers:?}; the row on C {on_target}; set_active {}; add {}; active {active:?}; conflicting {}; a retry {}",
        refusal(&switched),
        refusal(&added),
        conflicting.len(),
        refusal(&retry)
    );
    assert!(ended_while_held, "the switch waited for the add's open");
    assert_eq!(writers, vec!["B", "C"]);
    assert_eq!(on_target, 1);
    assert_eq!(refusal(&switched), "ok");
    assert_eq!(refusal(&added), "ok");
    assert_eq!(active, Ok("C".to_string()));
    assert_eq!(conflicting, vec![FOURTH.to_string()]);
    assert_eq!(
        refusal(&retry),
        "Access denied: Operation requires active storage"
    );
}
