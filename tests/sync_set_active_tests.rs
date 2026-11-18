//! `set_active` over two stores and three, whichever store fails part way
//! and wherever it stands in the stores' order: what the manager answers
//! after the failure, a retry to the same target, a call back to the old
//! active, and every store's row read back. A store fails by refusing,
//! by refusing the backup's sync, or by writing its row and then losing its
//! answer (as a storage client whose reply the network drops), with or
//! without refusing the read of its row after.
#![cfg(feature = "sqlite")]

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

const THIRD: &str = "3333333333333333333333333333333333333333333333333333333333333333";
const KEYS: [&str; 3] = [SRC, DST, THIRD];

/// Where the failing store stands in the transition.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Role {
    /// The manager's active when the call is made.
    Active,
    /// The store the call makes active.
    Target,
    /// Neither (three stores only).
    Third,
}

/// What fails at that store.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Fails {
    /// Its own `set_active` call.
    SetActive,
    /// The backup's sync inside the transition (a backup only).
    Sync,
    /// Its own `set_active` writes the row, then answers an error.
    AnswerLost,
    /// As `AnswerLost`, and the store then refuses to read its row until
    /// it answers again.
    AnswerLostUnread,
}

/// One cell: the number of stores, whether the manager has already
/// switched from the store at index 0 to the one at index 1 (so the active
/// stands behind a store in the order), the failing store and its failure.
#[derive(Clone, Copy, Debug)]
struct Cell {
    n: usize,
    switched: bool,
    role: Role,
    fails: Fails,
}

impl Cell {
    /// The active when the call is made, and the call's target.
    fn old_and_new(&self) -> (&'static str, &'static str) {
        if self.switched {
            (DST, SRC)
        } else {
            (SRC, DST)
        }
    }
    fn failing(&self) -> usize {
        let (from, to) = self.old_and_new();
        let key = match self.role {
            Role::Active => from,
            Role::Target => to,
            Role::Third => THIRD,
        };
        KEYS.iter().position(|k| *k == key).unwrap()
    }
}

fn arm(d: &Double, fails: Fails, on: bool) {
    match fails {
        Fails::SetActive => d.fail_set_active.store(on, Ordering::SeqCst),
        Fails::AnswerLost => d.lose_set_active_answer.store(on, Ordering::SeqCst),
        Fails::AnswerLostUnread => {
            d.lose_set_active_answer.store(on, Ordering::SeqCst);
            d.refuse_reads_after_lost_answer.store(on, Ordering::SeqCst);
            if !on {
                d.refuse_user_reads.store(false, Ordering::SeqCst);
            }
        }
        Fails::Sync => {
            *d.answer.lock().unwrap() = on.then(|| ProcessSyncChunkResult {
                done: true,
                max_updated_at: None,
                updates: 0,
                inserts: 0,
                error: Some("the backup refused the page".to_string()),
            })
        }
    }
}

/// The stores (every row naming the store at index 0), the manager over
/// them made available and, for a switched cell, switched to index 1.
async fn setup(cell: Cell) -> (Vec<Arc<Double>>, WalletStorageManager) {
    let mut doubles = Vec::new();
    for sik in &KEYS[..cell.n] {
        let s = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
            .await
            .unwrap();
        s.migrate(name(sik), sik).await.unwrap();
        s.make_available().await.unwrap();
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, SRC).await.unwrap();
        doubles.push(Arc::new(Double::new(s)));
    }
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(doubles[0].clone() as Arc<dyn MonitorStorage>),
        Some(
            doubles[1..]
                .iter()
                .map(|d| d.clone() as Arc<dyn MonitorStorage>)
                .collect(),
        ),
    );
    mgr.make_available().await.unwrap();
    if cell.switched {
        mgr.set_active(DST).await.unwrap();
    }
    (doubles, mgr)
}

async fn rows(doubles: &[Arc<Double>]) -> Vec<String> {
    let mut out = Vec::new();
    for d in doubles {
        let key = active_storage_of(&d.inner, ALICE).await.unwrap_or_default();
        out.push(name(&key).to_string());
    }
    out
}

fn name(key: &str) -> &'static str {
    match key {
        SRC => "A",
        DST => "B",
        THIRD => "C",
        _ => "?",
    }
}

/// The manager's active and whether it answers it as enabled.
async fn state(mgr: &WalletStorageManager) -> (String, bool) {
    (
        name(&mgr.get_active_store().await.unwrap()).to_string(),
        mgr.is_active_enabled().await,
    )
}

/// What one cell answers: after the failure; the retry; the call back.
#[derive(Debug)]
struct Answers {
    failed: String,
    after_failure: (Vec<String>, (String, bool)),
    retry: String,
    after_retry: (Vec<String>, (String, bool)),
    call_back: String,
    after_call_back: (Vec<String>, (String, bool)),
}

async fn run(cell: Cell) -> Answers {
    let (from, to) = cell.old_and_new();
    let i = cell.failing();

    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[i], cell.fails, true);
    let failed = refusal(&mgr.set_active(to).await);
    let after_failure = (rows(&doubles).await, state(&mgr).await);
    arm(&doubles[i], cell.fails, false);
    let retry = refusal(&mgr.set_active(to).await);
    let after_retry = (rows(&doubles).await, state(&mgr).await);

    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[i], cell.fails, true);
    let _ = mgr.set_active(to).await;
    arm(&doubles[i], cell.fails, false);
    let call_back = refusal(&mgr.set_active(from).await);
    let after_call_back = (rows(&doubles).await, state(&mgr).await);

    Answers {
        failed,
        after_failure,
        retry,
        after_retry,
        call_back,
        after_call_back,
    }
}

fn cells() -> Vec<Cell> {
    let mut out = Vec::new();
    for n in [2, 3] {
        for switched in [true, false] {
            for role in [Role::Active, Role::Target, Role::Third] {
                for fails in [
                    Fails::SetActive,
                    Fails::Sync,
                    Fails::AnswerLost,
                    Fails::AnswerLostUnread,
                ] {
                    // The active is no backup: no sync writes to it.
                    let reachable = (n == 3 || role != Role::Third)
                        && !(role == Role::Active && fails == Fails::Sync);
                    if reachable {
                        out.push(Cell {
                            n,
                            switched,
                            role,
                            fails,
                        });
                    }
                }
            }
        }
    }
    out
}

/// Every cell: after the failure the manager answers its active as enabled
/// only when every store's row names it (never after a lost answer); a retry to the same target, once
/// the store answers, makes every row name the target; a call back to the
/// old active makes every row name it; after either the manager is enabled
/// on that store.
#[tokio::test]
async fn set_active_finishes_or_undoes_a_switch_whichever_store_failed() {
    let mut bad = Vec::new();
    for cell in cells() {
        let (from, to) = cell.old_and_new();
        let (from, to) = (name(from).to_string(), name(to).to_string());
        let a = run(cell).await;
        println!(
            "WITNESS {} stores, {}, failing {:?} ({}) at {:?}: {}; rows {:?}, active {:?}; retry {}: rows {:?}, active {:?}; call back {}: rows {:?}, active {:?}",
            cell.n,
            if cell.switched { "after A to B, B to A" } else { "A to B" },
            cell.role,
            name(KEYS[cell.failing()]),
            cell.fails,
            a.failed,
            a.after_failure.0,
            a.after_failure.1,
            a.retry,
            a.after_retry.0,
            a.after_retry.1,
            a.call_back,
            a.after_call_back.0,
            a.after_call_back.1,
        );
        let (rows, (active, enabled)) = &a.after_failure;
        let agree = rows.iter().all(|r| r == active);
        let expected_failure = match cell.fails {
            Fails::SetActive => format!(
                "Storage error: {} refused set_active",
                name(KEYS[cell.failing()])
            ),
            Fails::Sync => "Sync error: the backup refused the page".to_string(),
            Fails::AnswerLost | Fails::AnswerLostUnread => {
                format!("Storage error: {} answer lost", name(KEYS[cell.failing()]))
            }
        };
        // A store that wrote its row and lost its answer moved its row: the
        // rows no longer name the manager's active.
        let lost = matches!(cell.fails, Fails::AnswerLost | Fails::AnswerLostUnread);
        let every = |r: &[String], k: &str| r.len() == cell.n && r.iter().all(|x| x == k);
        let ok = a.failed == expected_failure
            && *enabled == agree
            && !(lost && *enabled)
            && *active == from
            && a.retry == "ok"
            && every(&a.after_retry.0, &to)
            && a.after_retry.1 == (to.clone(), true)
            && a.call_back == "ok"
            && every(&a.after_call_back.0, &from)
            && a.after_call_back.1 == (from.clone(), true);
        if !ok {
            bad.push(format!("{cell:?}"));
        }
    }
    assert!(bad.is_empty(), "cells that do not hold: {bad:?}");
}

/// The order the stores' rows alone decide: after a switch from A to B,
/// `set_active(A)` failing at B leaves A's row naming A and B's naming B;
/// the manager answers B as not enabled and a call back to B makes both
/// rows name B.
#[tokio::test]
async fn a_call_back_after_a_failure_at_the_active_behind_writes_every_row() {
    let cell = Cell {
        n: 2,
        switched: true,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let a = run(cell).await;
    println!("WITNESS after A to B, set_active(A) failing at B: {a:?}");
    assert_eq!(a.failed, "Storage error: B refused set_active");
    assert_eq!(
        a.after_failure,
        (vec!["A".into(), "B".into()], ("B".into(), false))
    );
    assert_eq!(a.call_back, "ok");
    assert_eq!(
        a.after_call_back,
        (vec!["B".into(), "B".into()], ("B".into(), true))
    );
}

/// Two stores whose rows disagree (A's names B, B's names A: a switch
/// from A to B that failed at B), each a fresh manager's.
async fn disagreeing() -> Vec<Arc<Double>> {
    let cell = Cell {
        n: 2,
        switched: false,
        role: Role::Target,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[1], Fails::SetActive, true);
    let _ = mgr.set_active(DST).await;
    arm(&doubles[1], Fails::SetActive, false);
    doubles
}

fn manager_over(doubles: &[Arc<Double>]) -> WalletStorageManager {
    WalletStorageManager::new(
        ALICE.to_string(),
        Some(doubles[0].clone() as Arc<dyn MonitorStorage>),
        Some(
            doubles[1..]
                .iter()
                .map(|d| d.clone() as Arc<dyn MonitorStorage>)
                .collect(),
        ),
    )
}

/// Each store's own `set_active`, by that store's row of the wallet's user.
async fn name_on_every_store(doubles: &[Arc<Double>], key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for d in doubles {
        let (u, _) = d.inner.find_or_insert_user(ALICE).await.unwrap();
        let auth = AuthId::with_user_id(ALICE, u.user_id);
        out.push(refusal(
            &WalletStorageSync::set_active(&d.inner, &auth, key).await,
        ));
    }
    out
}

/// A manager made over stores whose rows disagree refuses `update_backups`
/// and `set_active` to either store. Each store's own `set_active` to one
/// store makes every row name it; a manager made over the stores again is
/// then enabled on that store and backs up. The refusing manager's own
/// `set_active` answers unchanged and is then enabled when that store is
/// its active, and still refuses when it is not.
#[tokio::test]
async fn a_new_manager_over_stores_that_disagree_has_a_way_through() {
    let denied = "Access denied: Operation requires active storage";
    let doubles = disagreeing().await;
    let fresh = manager_over(&doubles);
    fresh.make_available().await.unwrap();
    let before = (
        rows(&doubles).await,
        state(&fresh).await,
        refusal(&fresh.update_backups().await),
        refusal(&fresh.set_active(SRC).await),
        refusal(&fresh.set_active(DST).await),
    );
    println!("WITNESS a new manager over rows that disagree: {before:?}");
    assert_eq!(before.0, vec!["B".to_string(), "A".to_string()]);
    assert_eq!(before.1, ("A".to_string(), false));
    assert_eq!((before.2.as_str(), before.3.as_str()), (denied, denied));
    assert_eq!(before.4, denied);

    for (key, letter) in [(SRC, "A"), (DST, "B")] {
        let doubles = disagreeing().await;
        let fresh = manager_over(&doubles);
        fresh.make_available().await.unwrap();
        let named = name_on_every_store(&doubles, key).await;
        let again = manager_over(&doubles);
        again.make_available().await.unwrap();
        let made_again = (
            rows(&doubles).await,
            state(&again).await,
            refusal(&again.update_backups().await),
        );
        let own = fresh.set_active(key).await;
        let own = (
            own.map(|log| log == format!("setActive to {key} unchanged\n")),
            state(&fresh).await,
            refusal(&fresh.update_backups().await),
        );
        println!(
            "WITNESS each store's own set_active to {letter}: {named:?}; a manager made again: {made_again:?}; the refusing manager (active A) set_active({letter}): {own:?}"
        );
        assert_eq!(named, vec!["ok".to_string(), "ok".to_string()]);
        assert_eq!(made_again.0, vec![letter.to_string(), letter.to_string()]);
        assert_eq!(made_again.1, (letter.to_string(), true));
        assert_eq!(made_again.2, "ok");
        if key == SRC {
            assert_eq!(own.0.ok(), Some(true));
            assert_eq!(own.1, ("A".to_string(), true));
            assert_eq!(own.2, "ok");
        } else {
            assert_eq!(refusal(&own.0), denied);
            assert_eq!(own.1, ("A".to_string(), false));
            assert_eq!(own.2, denied);
        }
    }
}

/// The cell the stores' rows alone tell: A active, `set_active(B)`, A
/// (ahead in the order) writes its row and loses its answer. The rows read
/// B, A; the manager answers A as not enabled; a retry and, apart, a call
/// back each make every row name one active.
#[tokio::test]
async fn a_store_that_wrote_its_row_and_lost_its_answer_is_read_again() {
    let cell = Cell {
        n: 2,
        switched: false,
        role: Role::Active,
        fails: Fails::AnswerLost,
    };
    let a = run(cell).await;
    println!("WITNESS A to B, A writes its row and loses its answer: {a:?}");
    assert_eq!(a.failed, "Storage error: A answer lost");
    assert_eq!(
        a.after_failure,
        (vec!["B".into(), "A".into()], ("A".into(), false))
    );
    assert_eq!(a.retry, "ok");
    assert_eq!(
        a.after_retry,
        (vec!["B".into(), "B".into()], ("B".into(), true))
    );
    assert_eq!(a.call_back, "ok");
    assert_eq!(
        a.after_call_back,
        (vec!["A".into(), "A".into()], ("A".into(), true))
    );
}

/// The same store also refuses the read of its row after the lost answer:
/// the manager answers A as not enabled; while the store still refuses, a
/// retry answers its refusal and the manager stays not enabled; once it
/// answers, the retry completes.
#[tokio::test]
async fn a_store_that_lost_its_answer_and_cannot_be_read_is_held_unknown() {
    let cell = Cell {
        n: 2,
        switched: false,
        role: Role::Active,
        fails: Fails::AnswerLostUnread,
    };
    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[0], cell.fails, true);
    let failed = refusal(&mgr.set_active(DST).await);
    let after_failure = (rows(&doubles).await, state(&mgr).await);
    doubles[0]
        .lose_set_active_answer
        .store(false, Ordering::SeqCst);
    let unread_retry = refusal(&mgr.set_active(DST).await);
    let after_unread_retry = (rows(&doubles).await, state(&mgr).await);
    arm(&doubles[0], cell.fails, false);
    let retry = refusal(&mgr.set_active(DST).await);
    let after_retry = (rows(&doubles).await, state(&mgr).await);
    println!(
        "WITNESS A loses its answer and refuses the read: {failed}; {after_failure:?}; retry while unread {unread_retry}: {after_unread_retry:?}; retry {retry}: {after_retry:?}"
    );
    assert_eq!(failed, "Storage error: A answer lost");
    assert_eq!(
        after_failure,
        (vec!["B".into(), "A".into()], ("A".into(), false))
    );
    assert_eq!(unread_retry, "Storage error: A refused the user read");
    assert_eq!(
        after_unread_retry,
        (vec!["B".into(), "A".into()], ("A".into(), false))
    );
    assert_eq!(retry, "ok");
    assert_eq!(
        after_retry,
        (vec!["B".into(), "B".into()], ("B".into(), true))
    );
}

/// Two stores and three, the manager enabled on A: a backup's row is moved
/// behind the manager's back by that store's own `set_active` (to itself);
/// the manager's `set_active(A)` then writes A to that store and does not
/// answer unchanged; every row names A and the manager is enabled on A.
#[tokio::test]
async fn a_row_moved_behind_the_manager_is_written_by_set_active_to_its_own_active() {
    for n in [2, 3] {
        for moved in 1..n {
            let cell = Cell {
                n,
                switched: false,
                role: Role::Target,
                fails: Fails::SetActive,
            };
            let (doubles, mgr) = setup(cell).await;
            let d = &doubles[moved];
            let (u, _) = d.inner.find_or_insert_user(ALICE).await.unwrap();
            let auth = AuthId::with_user_id(ALICE, u.user_id);
            WalletStorageSync::set_active(&d.inner, &auth, KEYS[moved])
                .await
                .unwrap();
            let before = rows(&doubles).await;
            let log = mgr.set_active(SRC).await;
            let unchanged = log
                .as_ref()
                .map(|l| *l == format!("setActive to {SRC} unchanged\n"));
            let after = (rows(&doubles).await, state(&mgr).await);
            println!(
                "WITNESS {n} stores, {} moved to itself: rows {before:?}; set_active(A) {} unchanged {unchanged:?}; {after:?}",
                name(KEYS[moved]),
                refusal(&log),
            );
            assert_eq!(unchanged.ok(), Some(false));
            assert_eq!(after.0, vec!["A".to_string(); n]);
            assert_eq!(after.1, ("A".to_string(), true));
        }
    }
}

/// Each store's `find_or_insert_user` calls since `reset`.
fn reads(doubles: &[Arc<Double>]) -> Vec<usize> {
    doubles
        .iter()
        .map(|d| d.user_reads.swap(0, Ordering::SeqCst))
        .collect()
}

/// What `set_active` reads: each store's row of the wallet's user once for
/// a no-op, twice (the active) or three times (each other store) for a
/// switch, and once more each after an error inside the transition.
#[tokio::test]
async fn set_active_reads_each_stores_row_once_for_a_no_op_and_more_for_a_switch() {
    for n in [2, 3] {
        let cell = Cell {
            n,
            switched: false,
            role: Role::Target,
            fails: Fails::SetActive,
        };
        let (doubles, mgr) = setup(cell).await;
        reads(&doubles);
        let noop = refusal(&mgr.set_active(SRC).await);
        let noop_reads = reads(&doubles);
        let switch = refusal(&mgr.set_active(DST).await);
        let switch_reads = reads(&doubles);

        let (doubles, mgr) = setup(cell).await;
        arm(&doubles[1], Fails::SetActive, true);
        reads(&doubles);
        let failed = refusal(&mgr.set_active(DST).await);
        let failed_reads = reads(&doubles);
        println!(
            "WITNESS {n} stores, user reads per store: no-op {noop} {noop_reads:?}; switch A to B {switch} {switch_reads:?}; switch failing at B {failed} {failed_reads:?}"
        );
        assert_eq!(noop, "ok");
        assert_eq!(noop_reads, vec![1; n]);
        assert_eq!(switch, "ok");
        let mut expected = vec![3; n];
        expected[0] = 2;
        assert_eq!(switch_reads, expected);
        assert_eq!(failed, "Storage error: B refused set_active");
        let once_more: Vec<usize> = expected.iter().map(|r| r + 1).collect();
        assert_eq!(failed_reads, once_more);
    }
}

/// `set_active` to the manager's own active, every row naming it, answers
/// a store's error when that store cannot be read.
#[tokio::test]
async fn set_active_to_its_own_active_answers_a_store_that_cannot_be_read() {
    let cell = Cell {
        n: 2,
        switched: false,
        role: Role::Target,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    doubles[1].refuse_user_reads.store(true, Ordering::SeqCst);
    let r = refusal(&mgr.set_active(SRC).await);
    let after = (rows(&doubles).await, state(&mgr).await);
    doubles[1].refuse_user_reads.store(false, Ordering::SeqCst);
    let again = mgr.set_active(SRC).await;
    println!(
        "WITNESS set_active(A) to its own active, B unreadable: {r}; {after:?}; once B answers: {}",
        refusal(&again)
    );
    assert_eq!(r, "Storage error: B refused the user read");
    assert_eq!(after, (vec!["A".into(), "A".into()], ("A".into(), true)));
    assert_eq!(again.unwrap(), format!("setActive to {SRC} unchanged\n"));
}

/// After A to B, `set_active(A)` failing at B leaves the rows A, B; a
/// store added to the manager then (its row naming B) partitions the
/// stores again, and the manager answers as one made new over stores
/// that disagree: A active, not enabled, `update_backups` and `set_active`
/// to either refused.
#[tokio::test]
async fn a_store_added_after_a_part_way_failure_partitions_again() {
    let denied = "Access denied: Operation requires active storage";
    let cell = Cell {
        n: 2,
        switched: true,
        role: Role::Active,
        fails: Fails::SetActive,
    };
    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[1], Fails::SetActive, true);
    let failed = refusal(&mgr.set_active(SRC).await);
    arm(&doubles[1], Fails::SetActive, false);
    let before = state(&mgr).await;
    let c = bsv_wallet_toolbox_rs::StorageSqlx::in_memory()
        .await
        .unwrap();
    c.migrate(name(THIRD), THIRD).await.unwrap();
    c.make_available().await.unwrap();
    let (u, _) = c.find_or_insert_user(ALICE).await.unwrap();
    c.update_user_active_storage(u.user_id, DST).await.unwrap();
    let added = refusal(
        &mgr.add_wallet_storage_provider(Arc::new(c) as Arc<dyn MonitorStorage>)
            .await,
    );
    let after = state(&mgr).await;
    let backups = refusal(&mgr.update_backups().await);
    let to_a = refusal(&mgr.set_active(SRC).await);
    let to_b = refusal(&mgr.set_active(DST).await);
    println!(
        "WITNESS after A to B, set_active(A) failing at B: {failed}, {before:?}; a store added: {added}, {after:?}; update_backups {backups}; set_active(A) {to_a}; set_active(B) {to_b}"
    );
    assert_eq!(failed, "Storage error: B refused set_active");
    assert_eq!(before, ("B".into(), false));
    assert_eq!(added, "ok");
    assert_eq!(after, ("A".into(), false));
    assert_eq!((backups.as_str(), to_a.as_str()), (denied, denied));
    assert_eq!(to_b, denied);
}
