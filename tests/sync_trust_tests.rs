//! Sync through the manager: what it checks of its stores and of a writer's
//! answers before and while it drains chunks.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::entities::TableCertificate;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

const THIRD: &str = "3333333333333333333333333333333333333333333333333333333333333333";

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

/// A store declaring a chain by this crate's word and a store declaring it
/// by the reference's word are one chain; a test chain is not a main chain
/// under either word.
#[tokio::test]
async fn one_chain_under_either_word_syncs() {
    let text = "Sync error: Live sync requires matching declared network chains on both storage providers.";
    let local = chain_store(SRC, "local", "mainnet").await;
    local.find_or_insert_user(ALICE).await.unwrap();
    seed_all(&local, 1, 'a', true, true).await;
    let local = Arc::new(local);
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(local.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    let main = Arc::new(chain_store(DST, "ref-main", "main").await);
    let to_main = mgr.sync_to_writer(ALICE, main.clone()).await;
    let main_txs = count(&main, "SELECT COUNT(*) FROM transactions").await;
    let test = Arc::new(chain_store(DST, "ref-test", "test").await);
    let to_test = mgr.sync_to_writer(ALICE, test.clone()).await;
    let test_txs = count(&test, "SELECT COUNT(*) FROM transactions").await;

    let testnet = chain_store(SRC, "local-test", "testnet").await;
    testnet.find_or_insert_user(ALICE).await.unwrap();
    seed_all(&testnet, 1, 'a', true, true).await;
    let tmgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(Arc::new(testnet) as Arc<dyn MonitorStorage>),
        None,
    );
    let ref_test = Arc::new(chain_store(DST, "ref-test-two", "test").await);
    let testnet_to_test = tmgr.sync_to_writer(ALICE, ref_test.clone()).await;
    let ref_test_txs = count(&ref_test, "SELECT COUNT(*) FROM transactions").await;
    println!(
        "WITNESS chain words: mainnet->main {} ({main_txs} txs); mainnet->test {} ({test_txs} txs); testnet->test {} ({ref_test_txs} txs)",
        refusal(&to_main),
        refusal(&to_test),
        refusal(&testnet_to_test)
    );
    assert_eq!(refusal(&to_main), "ok");
    assert!(main_txs > 0);
    assert_eq!(refusal(&to_test), text);
    assert_eq!(test_txs, 0);
    assert_eq!(refusal(&testnet_to_test), "ok");
    assert!(ref_test_txs > 0);
}

/// Two stores that declare one word outside the reference's list of sync
/// chains (an empty word, a private chain's) are one chain with each other
/// and with no other word.
#[tokio::test]
async fn two_stores_declaring_one_other_word_sync() {
    let text = "Sync error: Live sync requires matching declared network chains on both storage providers.";
    let mut answers = Vec::new();
    for (word, other) in [
        ("", ""),
        ("private", "private"),
        ("", "mainnet"),
        ("private", "main"),
    ] {
        let reader = chain_store(SRC, "reader", word).await;
        reader.find_or_insert_user(ALICE).await.unwrap();
        seed_all(&reader, 1, 'a', true, true).await;
        let mgr = WalletStorageManager::new(
            ALICE.to_string(),
            Some(Arc::new(reader) as Arc<dyn MonitorStorage>),
            None,
        );
        let writer = Arc::new(chain_store(DST, "writer", other).await);
        let r = mgr.sync_to_writer(ALICE, writer.clone()).await;
        let txs = count(&writer, "SELECT COUNT(*) FROM transactions").await;
        println!(
            "WITNESS chain words {word:?} -> {other:?}: {} ({txs} txs)",
            refusal(&r)
        );
        answers.push((refusal(&r), txs > 0));
    }
    assert_eq!(answers[0], ("ok".to_string(), true));
    assert_eq!(answers[1], ("ok".to_string(), true));
    assert_eq!(answers[2], (text.to_string(), false));
    assert_eq!(answers[3], (text.to_string(), false));
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

/// The users rows of the stores and what the manager answers about them.
async fn active_rows(stores: &[&StorageSqlx]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for s in stores {
        out.push(active_storage_of(s, ALICE).await);
    }
    out
}

fn cert(uid: i64, sn: &str) -> TableCertificate {
    TableCertificate {
        certificate_id: 0,
        user_id: uid,
        cert_type: "t".into(),
        serial_number: sn.into(),
        certifier: "c".into(),
        subject: "s".into(),
        verifier: None,
        revocation_outpoint: "o.0".into(),
        signature: "sig".into(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

async fn certs(s: &StorageSqlx) -> i64 {
    count(s, "SELECT COUNT(*) FROM certificates").await
}

/// Stores A (active) and B (its backup, whose `set_active` fails), both
/// rows naming A, under a made-available manager.
async fn part_way() -> (StorageSqlx, Arc<Double>, WalletStorageManager) {
    let a = store(SRC).await;
    let b = Double::new((*store(DST).await).clone());
    b.fail_set_active.store(true, Ordering::SeqCst);
    let a_inner = (*a).clone();
    name_active(&[&a_inner, &b.inner], SRC).await;
    let b = Arc::new(b);
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(a.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    (a_inner, b, mgr)
}

/// A `set_active` whose call fails at a store part way leaves the manager
/// available on the active it had, not enabled; backups and plain writes go
/// on to the old active. The backup carries the active's row, which names
/// the new active, to the failing store, so a retry writes no store's row
/// and completes the switch while that store still refuses `set_active`.
#[tokio::test]
async fn a_set_active_that_fails_part_way_is_finished_by_a_retry() {
    let (a, b, mgr) = part_way().await;
    let failed = mgr.set_active(DST).await;
    let rows = active_rows(&[&a, &b.inner]).await;
    let available = mgr.is_available().await;
    let enabled = mgr.is_active_enabled().await;
    let active = mgr.get_active_store().await.unwrap();
    let backups = mgr.update_backups().await;
    let auth = mgr.get_auth(false).await.unwrap();
    let write =
        WalletStorageWriter::insert_certificate(&mgr, &auth, cert(auth.user_id.unwrap(), "w1"))
            .await;
    let written = (certs(&a).await, certs(&b.inner).await);
    let still = mgr.set_active(DST).await;
    b.fail_set_active.store(false, Ordering::SeqCst);
    let retry = mgr.set_active(DST).await;
    let carried = certs(&b.inner).await;
    let after = active_rows(&[&a, &b.inner]).await;
    let now = (
        mgr.get_active_store().await.unwrap(),
        mgr.is_active_enabled().await,
    );
    println!(
        "WITNESS set_active(B) failing at B: {}; rows (A, B) {rows:?}; available {available}; enabled {enabled}; active A {}; update_backups {}; plain write {} certificates (A, B) {written:?}; set_active(B) while B fails {}; again once B answers {}; certificates on B {carried}; rows {after:?}; active B {}, enabled {}",
        refusal(&failed),
        active == SRC,
        refusal(&backups),
        refusal(&write),
        refusal(&still),
        refusal(&retry),
        now.0 == DST,
        now.1
    );
    assert_eq!(refusal(&failed), "Storage error: rows refused set_active");
    assert_eq!(rows, vec![Some(DST.to_string()), Some(SRC.to_string())]);
    assert!(available);
    assert!(!enabled);
    assert_eq!(active, SRC);
    assert_eq!(refusal(&backups), "ok");
    assert_eq!(refusal(&write), "ok");
    assert_eq!(written, (1, 0));
    assert_eq!(refusal(&still), "ok");
    assert_eq!(refusal(&retry), "ok");
    assert_eq!(carried, 1);
    assert_eq!(after, vec![Some(DST.to_string()), Some(DST.to_string())]);
    assert_eq!(now, (DST.to_string(), true));
}

/// After a `set_active` that fails part way, a call back to the old active
/// completes once the store answers, and every row names the old active
/// again. A manager made over the stores while they still disagree answers
/// its active as not enabled and refuses its backups.
#[tokio::test]
async fn a_set_active_that_fails_part_way_is_undone_by_a_call_back() {
    let (a, b, mgr) = part_way().await;
    let failed = mgr.set_active(DST).await;
    let fresh = WalletStorageManager::new(
        ALICE.to_string(),
        Some(Arc::new(a.clone()) as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    let fresh_settings = fresh.make_available().await.map(|s| s.storage_identity_key);
    let fresh_enabled = fresh.is_active_enabled().await;
    let fresh_backups = fresh.update_backups().await;
    let fresh_set_active = fresh.set_active(SRC).await;
    b.fail_set_active.store(false, Ordering::SeqCst);
    let undo = mgr.set_active(SRC).await;
    let after = active_rows(&[&a, &b.inner]).await;
    let now = (
        mgr.get_active_store().await.unwrap(),
        mgr.is_active_enabled().await,
    );
    println!(
        "WITNESS set_active(B) failing at B, then set_active(A): {} then {}; rows {after:?}; active A {}, enabled {}; a fresh manager meanwhile: active A {}, enabled {fresh_enabled}, update_backups {}, set_active(A) {}",
        refusal(&failed),
        refusal(&undo),
        now.0 == SRC,
        now.1,
        fresh_settings.as_deref().ok() == Some(SRC),
        refusal(&fresh_backups),
        refusal(&fresh_set_active)
    );
    assert_eq!(refusal(&failed), "Storage error: rows refused set_active");
    assert_eq!(refusal(&undo), "ok");
    assert_eq!(after, vec![Some(SRC.to_string()), Some(SRC.to_string())]);
    assert_eq!(now, (SRC.to_string(), true));
    assert_eq!(fresh_settings.as_deref().ok(), Some(SRC));
    assert!(!fresh_enabled);
    assert_eq!(
        refusal(&fresh_backups),
        "Access denied: Operation requires active storage"
    );
    assert_eq!(
        refusal(&fresh_set_active),
        "Access denied: Operation requires active storage"
    );
}

/// A manager whose stores are in conflict (a backup's row names a store
/// other than the active) refuses `set_active`.
#[tokio::test]
async fn set_active_refuses_while_the_stores_are_in_conflict() {
    let a = store(SRC).await;
    let b = store(DST).await;
    name_active(&[&a], SRC).await;
    name_active(&[&b], THIRD).await;
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(a.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    let conflicting = mgr.get_conflicting_stores().await;
    let r = mgr.set_active(DST).await;
    println!(
        "WITNESS set_active with a store in conflict: conflicting {}; {}",
        conflicting.len(),
        refusal(&r)
    );
    assert_eq!(conflicting, vec![DST.to_string()]);
    assert_eq!(
        refusal(&r),
        "Access denied: Operation requires active storage"
    );
}

/// Two `set_active` calls at once run one after the other: no store's
/// `set_active` call of one runs between two of the other's.
#[tokio::test]
async fn two_set_active_calls_do_not_interleave() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut doubles = Vec::new();
    for sik in [SRC, DST, THIRD] {
        let s = StorageSqlx::in_memory().await.unwrap();
        s.migrate(&sik[..1], sik).await.unwrap();
        s.make_available().await.unwrap();
        name_active(&[&s], SRC).await;
        let mut d = Double::new(s);
        d.set_active_delay_ms = 50;
        d.events = events.clone();
        doubles.push(Arc::new(d));
    }
    let mgr = Arc::new(WalletStorageManager::new(
        ALICE.to_string(),
        Some(doubles[0].clone()),
        Some(vec![doubles[1].clone(), doubles[2].clone()]),
    ));
    mgr.make_available().await.unwrap();

    let (m1, m2) = (mgr.clone(), mgr.clone());
    let one = tokio::spawn(async move { m1.set_active(DST).await });
    let two = tokio::spawn(async move { m2.set_active(THIRD).await });
    let (one, two) = (
        bounded("set_active(B)", one).await.unwrap(),
        bounded("set_active(C)", two).await.unwrap(),
    );
    let seen = events.lock().unwrap().clone();
    // Each call's calls name its key; a run of one key never breaks.
    let mut runs = Vec::new();
    for e in &seen {
        let key = e.rsplit(' ').next().unwrap().to_string();
        if runs.last() != Some(&key) {
            runs.push(key);
        }
    }
    println!(
        "WITNESS M7b two set_active at once: {} / {}; key runs {}; calls {seen:?}",
        refusal(&one),
        refusal(&two),
        runs.len()
    );
    assert_eq!(refusal(&one), "ok");
    assert_eq!(refusal(&two), "ok");
    assert_eq!(runs.len(), 2);
    assert_eq!(seen.len(), 12);
}

/// Every sync entry of the manager that takes an identity refuses one that
/// is not the manager's, before it reads or writes a store.
#[tokio::test]
async fn every_manager_sync_entry_refuses_another_identity() {
    let text = "Access denied: Identity key mismatch";
    let act = store(SRC).await;
    act.find_or_insert_user(ALICE).await.unwrap();
    let (bob, _) = act.find_or_insert_user(BOB).await.unwrap();
    seed_all(&act, bob.user_id, 'b', true, true).await;
    let other = store(DST).await;
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();

    let to_writer = mgr.sync_to_writer(BOB, other.clone()).await;
    let other_rows = (
        users(&other).await,
        count(&other, "SELECT COUNT(*) FROM transactions").await,
    );
    let from_reader = mgr.sync_from_reader(BOB, other.clone()).await;
    let bob_auth = AuthId::with_user_id(BOB, bob.user_id);
    let state = mgr.find_or_insert_sync_state(&bob_auth, DST, "other").await;
    let states = count(&act, "SELECT COUNT(*) FROM sync_states").await;
    let set_active = WalletStorageSync::set_active(&mgr, &bob_auth, DST).await;
    let bob_active = active_storage_of(&act, BOB).await;
    println!(
        "WITNESS M1 manager for alice, bob named: sync_to_writer {} (other store users, transactions {other_rows:?}); sync_from_reader {}; find_or_insert_sync_state {} (sync states {states}); set_active {} (bob's active {bob_active:?})",
        refusal(&to_writer),
        refusal(&from_reader),
        refusal(&state),
        refusal(&set_active)
    );
    assert_eq!(refusal(&to_writer), text);
    assert_eq!(other_rows, (0, 0));
    assert_eq!(refusal(&from_reader), text);
    assert_eq!(refusal(&state), text);
    assert_eq!(states, 0);
    assert_eq!(refusal(&set_active), text);
    assert_ne!(bob_active.as_deref(), Some(DST));
}
