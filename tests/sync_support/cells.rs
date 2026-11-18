//! The `set_active` matrix's cells, the doubles' arming and one cell's run
//! (`tests/sync_set_active_tests.rs`): the stores, where the failing store
//! stands and what fails at it, and what the manager answers after.
#![allow(dead_code)]

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::entities::TableCertificate;
use bsv_wallet_toolbox_rs::storage::*;

use super::double::Double;
use super::*;

pub const THIRD: &str = "3333333333333333333333333333333333333333333333333333333333333333";
pub const KEYS: [&str; 3] = [SRC, DST, THIRD];

/// Where the failing store stands in the transition.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    /// The manager's active when the call is made.
    Active,
    /// The store the call makes active.
    Target,
    /// Neither (three stores only).
    Third,
}

/// What fails at that store.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fails {
    /// Its own `set_active` call.
    SetActive,
    /// The backup's sync inside the transition (a backup only).
    Sync,
    /// Its own `set_active` writes the row, then answers an error.
    AnswerLost,
    /// As `AnswerLost`, and the store then refuses to read its row until
    /// it answers again.
    AnswerLostUnread,
    /// Its own `set_active` writes the row and never answers: the call is
    /// dropped by a bound around it.
    Dropped,
    /// Its own `set_active` writes the row and panics: the call runs on a
    /// spawned task, whose join error is the answer.
    Panics,
    /// As `Sync`; then the retry's call at that store writes its row and
    /// loses its answer, and a second retry completes.
    SyncThenRetryLost,
    /// The backup merges the chunk of its sync, then answers an error.
    SyncLost,
}

/// One cell: the number of stores, whether the manager has already
/// switched from the store at index 0 to the one at index 1 (so the active
/// stands behind a store in the order), the failing store and its failure.
#[derive(Clone, Copy, Debug)]
pub struct Cell {
    pub n: usize,
    pub switched: bool,
    pub role: Role,
    pub fails: Fails,
}

impl Cell {
    /// The active when the call is made, and the call's target.
    pub fn old_and_new(&self) -> (&'static str, &'static str) {
        if self.switched {
            (DST, SRC)
        } else {
            (SRC, DST)
        }
    }
    pub fn failing(&self) -> usize {
        let (from, to) = self.old_and_new();
        let key = match self.role {
            Role::Active => from,
            Role::Target => to,
            Role::Third => THIRD,
        };
        KEYS.iter().position(|k| *k == key).unwrap()
    }
}

pub fn arm(d: &Double, fails: Fails, on: bool) {
    match fails {
        Fails::Dropped => d.hang_after_write.store(on, Ordering::SeqCst),
        Fails::Panics => d.panic_after_write.store(on, Ordering::SeqCst),
        Fails::SetActive => d.fail_set_active.store(on, Ordering::SeqCst),
        Fails::AnswerLost => d.lose_set_active_answer.store(on, Ordering::SeqCst),
        Fails::AnswerLostUnread => {
            d.lose_set_active_answer.store(on, Ordering::SeqCst);
            d.refuse_reads_after_lost_answer.store(on, Ordering::SeqCst);
            if !on {
                d.refuse_user_reads.store(false, Ordering::SeqCst);
            }
        }
        Fails::SyncLost => d.lose_sync_answer.store(on, Ordering::SeqCst),
        Fails::SyncThenRetryLost => {
            arm(d, Fails::Sync, on);
            if !on {
                arm(d, Fails::AnswerLost, false);
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
pub async fn setup(cell: Cell) -> (Vec<Arc<Double>>, Arc<WalletStorageManager>) {
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
    let mgr = Arc::new(manager_over(&doubles));
    mgr.make_available().await.unwrap();
    if cell.switched {
        mgr.set_active(DST).await.unwrap();
    }
    (doubles, mgr)
}

pub async fn rows(doubles: &[Arc<Double>]) -> Vec<String> {
    let mut out = Vec::new();
    for d in doubles {
        let key = active_storage_of(&d.inner, ALICE).await.unwrap_or_default();
        out.push(name(&key).to_string());
    }
    out
}

pub fn name(key: &str) -> &'static str {
    match key {
        SRC => "A",
        DST => "B",
        THIRD => "C",
        _ => "?",
    }
}

/// The manager's active and whether it answers it as enabled.
pub async fn state(mgr: &WalletStorageManager) -> (String, bool) {
    (
        name(&mgr.get_active_store().await.unwrap()).to_string(),
        mgr.is_active_enabled().await,
    )
}

/// What the manager answers after the stores' rows are read again: on the
/// store every row names, enabled, when every store was read and the rows
/// agree; otherwise on the active it had, not enabled.
pub fn read_again(rows: &[String], readable: bool, from: &str) -> (String, bool) {
    if readable && rows.iter().all(|r| *r == rows[0]) {
        (rows[0].clone(), true)
    } else {
        (from.to_string(), false)
    }
}

/// The stores' rows and the manager's active with whether it is enabled.
pub type Seen = (Vec<String>, (String, bool));

/// What one cell answers: after the failure; the retry; the call back.
#[derive(Debug)]
pub struct Answers {
    pub failed: String,
    pub after_failure: (Vec<String>, (String, bool)),
    /// The stores a plain write through the manager lands on, read back.
    pub written: Vec<String>,
    /// A retry that fails at the store's lost answer: its answer, then
    /// the rows and the manager's state.
    pub retry_lost: Option<(String, Seen)>,
    pub retry: String,
    pub after_retry: (Vec<String>, (String, bool)),
    pub call_back: String,
    pub after_call_back: (Vec<String>, (String, bool)),
}

/// One `set_active` call: a call whose store never answers is dropped by
/// a bound around it (`dropped`); a call whose store may panic runs on a
/// spawned task (`panicked` when it does).
pub async fn attempt(mgr: &Arc<WalletStorageManager>, key: &str, fails: Fails) -> String {
    match fails {
        Fails::Dropped => {
            match tokio::time::timeout(Duration::from_millis(300), mgr.set_active(key)).await {
                Ok(r) => refusal(&r),
                Err(_) => "dropped".to_string(),
            }
        }
        Fails::Panics => {
            let (m, k) = (mgr.clone(), key.to_string());
            let task = tokio::spawn(async move { m.set_active(&k).await });
            match bounded("a set_active whose store panics", task).await {
                Ok(r) => refusal(&r),
                Err(e) if e.is_panic() => "panicked".to_string(),
                Err(e) => format!("join error: {e}"),
            }
        }
        _ => refusal(&bounded("set_active", mgr.set_active(key)).await),
    }
}

/// The rows each store holds right after the failing call: a store ahead
/// of the failing one in the order was written; the failing one was
/// written when its failure comes after its write.
pub fn rows_after_failure(cell: Cell) -> Vec<String> {
    let (from, to) = cell.old_and_new();
    let i = cell.failing();
    let through = match cell.fails {
        Fails::Sync | Fails::SyncThenRetryLost | Fails::SyncLost => 0,
        Fails::SetActive => i,
        _ => i + 1,
    };
    (0..cell.n)
        .map(|j| name(if j < through { to } else { from }).to_string())
        .collect()
}

/// A call's answer: `unchanged` for the no-op's log, else its refusal or `ok`.
pub fn word(r: &bsv_wallet_toolbox_rs::Result<String>) -> String {
    match r {
        Ok(log) if log.ends_with(" unchanged\n") => "unchanged".to_string(),
        _ => refusal(r),
    }
}

/// A certificate written through the manager by its own auth; the stores
/// that then hold it, read back from each store.
pub async fn plain_write(doubles: &[Arc<Double>], mgr: &WalletStorageManager) -> Vec<String> {
    let auth = mgr.get_auth(false).await.unwrap();
    let cert = TableCertificate {
        certificate_id: 0,
        user_id: auth.user_id.unwrap(),
        cert_type: "t".into(),
        serial_number: "plain".into(),
        certifier: "c".into(),
        subject: "s".into(),
        verifier: None,
        revocation_outpoint: "o.0".into(),
        signature: "sig".into(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    WalletStorageWriter::insert_certificate(mgr, &auth, cert)
        .await
        .unwrap();
    let mut out = Vec::new();
    for d in doubles {
        if count(&d.inner, "SELECT COUNT(*) FROM certificates").await > 0 {
            out.push(name(&d.inner.storage_identity_key()).to_string());
        }
    }
    out
}

pub async fn run(cell: Cell) -> Answers {
    let (from, to) = cell.old_and_new();
    let i = cell.failing();

    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[i], cell.fails, true);
    let failed = attempt(&mgr, to, cell.fails).await;
    let after_failure = (rows(&doubles).await, state(&mgr).await);
    let written = plain_write(&doubles, &mgr).await;
    let retry_lost = if cell.fails == Fails::SyncThenRetryLost {
        arm(&doubles[i], Fails::Sync, false);
        arm(&doubles[i], Fails::AnswerLost, true);
        let r = refusal(&bounded("the retry that fails", mgr.set_active(to)).await);
        Some((r, (rows(&doubles).await, state(&mgr).await)))
    } else {
        None
    };
    arm(&doubles[i], cell.fails, false);
    let retry = word(&bounded("the retry", mgr.set_active(to)).await);
    let after_retry = (rows(&doubles).await, state(&mgr).await);

    let (doubles, mgr) = setup(cell).await;
    arm(&doubles[i], cell.fails, true);
    let _ = attempt(&mgr, to, cell.fails).await;
    arm(&doubles[i], cell.fails, false);
    let call_back = refusal(&mgr.set_active(from).await);
    let after_call_back = (rows(&doubles).await, state(&mgr).await);

    Answers {
        failed,
        after_failure,
        written,
        retry_lost,
        retry,
        after_retry,
        call_back,
        after_call_back,
    }
}

pub fn cells() -> Vec<Cell> {
    let mut out = Vec::new();
    for n in [2, 3] {
        for switched in [true, false] {
            for role in [Role::Active, Role::Target, Role::Third] {
                for fails in [
                    Fails::SetActive,
                    Fails::Sync,
                    Fails::AnswerLost,
                    Fails::AnswerLostUnread,
                    Fails::Dropped,
                    Fails::Panics,
                    Fails::SyncThenRetryLost,
                    Fails::SyncLost,
                ] {
                    // The active is no backup: no sync writes to it. One
                    // cell of a backup's lost sync answer: no row moves.
                    let sync = matches!(
                        fails,
                        Fails::Sync | Fails::SyncThenRetryLost | Fails::SyncLost
                    );
                    let reachable = (n == 3 || role != Role::Third)
                        && !(role == Role::Active && sync)
                        && (fails != Fails::SyncLost
                            || (n == 2 && !switched && role == Role::Target));
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

pub fn manager_over(doubles: &[Arc<Double>]) -> WalletStorageManager {
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
