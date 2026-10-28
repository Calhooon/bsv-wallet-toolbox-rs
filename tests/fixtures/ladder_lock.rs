//! The replay and mockito's server lock (bsv-wallet-toolbox-rs#292).
//!
//! A mockito 1.7.2 server answers on a thread of its own, holding its
//! state's write lock (`std::sync::RwLock`, `server.rs:536`) while it runs a
//! mock's status closure, and a `delay_ms` answer sleeps inside that closure.
//! `Server::reset`, `Mock::create` and `Server`'s drop take the same
//! lock synchronously. The fixtures' 3000 ms answers outlive the client's
//! 2000 ms deadline by a second, so a worker that moves to its next scene
//! on the same servers waits out that second inside `reset`; on a runtime
//! worker, that blocks the thread, and with every thread blocked the timers
//! and the I/O of every other scene wait too.
//!
//! So every call that takes a server's lock runs on tokio's blocking pool
//! (`spawn_blocking`): the reset and the mocks of each scene ([`script`])
//! and the servers' drop. The worker awaits it; a scene after a sleeping
//! answer still waits out that answer, on a blocking thread, and no
//! runtime thread does. The alternatives: a fresh server per scene costs
//! five server threads a scene and still locks in the old servers' drop
//! (a reset), so it needs this anyway; awaiting the delayed answers before
//! the reset needs a count kept in every delayed closure and misses an
//! answer whose closure starts after the wait (a request the server queued
//! behind a sleeping one and serves once it wakes).

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

tokio::task_local! {
    static REPLAY_WORKER: ();
    static LOCK_WITNESS: Arc<LockWitness>;
    static DISPOSING_SERVERS: ();
}

/// Count lock-taking batches and those called while an async worker is polled.
/// The worker marker follows the future across threads and awaits, but is not
/// inherited by `spawn_blocking`. Scheduling delays cannot change this witness.
#[derive(Default)]
pub(super) struct LockWitness {
    created: AtomicUsize,
    scripts: AtomicUsize,
    batches: AtomicUsize,
    on_worker: AtomicUsize,
    disposals: Mutex<Vec<(std::thread::ThreadId, bool)>>,
}

/// Count the instances independently of their disposal path.
pub(super) fn record_servers_created() {
    LockWitness::current()
        .created
        .fetch_add(1, Ordering::Relaxed);
}

impl LockWitness {
    fn current() -> Arc<Self> {
        LOCK_WITNESS.try_with(Arc::clone).unwrap_or_default()
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.batches.load(Ordering::Relaxed),
            self.on_worker.load(Ordering::Relaxed),
        )
    }
}

/// Scope both the witness and the polling marker to one replay worker.
pub(super) async fn watch_worker<T>(
    witness: Arc<LockWitness>,
    future: impl std::future::Future<Output = T>,
) -> T {
    LOCK_WITNESS
        .scope(witness, REPLAY_WORKER.scope((), future))
        .await
}

/// Called before a scene's reset and mock creation, including replay retries.
pub(super) fn record_script_locks() {
    LockWitness::current()
        .scripts
        .fetch_add(1, Ordering::Relaxed);
    record_server_locks();
}

/// Called at the synchronous entry to a batch of server lock operations.
pub(super) fn record_server_locks() {
    let witness = LockWitness::current();
    witness.batches.fetch_add(1, Ordering::Relaxed);
    if REPLAY_WORKER.try_with(|()| ()).is_ok() {
        witness.on_worker.fetch_add(1, Ordering::Relaxed);
    }
}

/// Stand up the fixture's script on the servers off the runtime's workers:
/// `reset` and mock creation wait there for any answer still sleeping
/// under a server's lock, and this worker awaits them without blocking a
/// thread. The mocks returned must live through the calls.
pub(super) async fn script(
    fixture: &Fixture,
    servers: &mut Servers,
    counts: &Counts,
) -> Result<Vec<mockito::Mock>, String> {
    let providers = fixture.providers.clone();
    let counts = Arc::clone(counts);
    let witness = LockWitness::current();
    let mut held = std::mem::take(&mut servers.0);
    let (held, mocks) = tokio::task::spawn_blocking(move || {
        let mocks =
            LOCK_WITNESS.sync_scope(witness, || script_blocking(&providers, &mut held, &counts));
        (held, mocks)
    })
    .await
    .expect("the script's blocking task");
    servers.0 = held;
    mocks
}

/// Dispose the whole instance on a blocking thread and await its lock release.
/// The disposal scope keeps `Drop` from spawning another, unawaited task.
pub(super) async fn dispose(servers: Servers) {
    let witness = LockWitness::current();
    tokio::task::spawn_blocking(move || {
        LOCK_WITNESS.sync_scope(witness, || {
            DISPOSING_SERVERS.sync_scope((), || drop(servers));
        });
    })
    .await
    .expect("the servers' blocking disposal task");
}

/// Record disposal at entry, before moving any lock work to another thread.
/// Callers outside the replay pool retain automatic blocking-pool cleanup.
impl Drop for Servers {
    fn drop(&mut self) {
        let entry = (
            std::thread::current().id(),
            REPLAY_WORKER.try_with(|()| ()).is_ok(),
        );
        let witness = LockWitness::current();
        witness.disposals.lock().unwrap().push(entry);
        let servers = std::mem::take(&mut self.0);
        let release = move || {
            LOCK_WITNESS.sync_scope(witness, || {
                record_server_locks();
                drop(servers);
            });
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) if DISPOSING_SERVERS.try_with(|()| ()).is_err() => {
                runtime.spawn_blocking(release);
            }
            _ => release(),
        }
    }
}

/// Bitails's fault in call 2 of the late-second-answer scene, replayed in
/// a queue of `company` scenes on `workers` workers: how long the call
/// history says the rung took.
async fn late_fault_msecs(company: Vec<pool::Scene>, workers: usize) -> u64 {
    let late = fixture_root().join("get_status_for_txids/late_second_answer.json");
    let mut scenes = company;
    scenes.push((late.clone(), Ok(stall::late_second_answer())));
    scenes.sort_by(|a, b| a.0.cmp(&b.0));
    let scripted = scenes.len();
    let witness = Arc::new(LockWitness::default());
    let replayed = pool::replay_queue_with_witness(scenes, workers, Arc::clone(&witness)).await;
    let (batches, on_worker) = witness.counts();
    let created = witness.created.load(Ordering::Relaxed);
    let disposals = witness.disposals.lock().unwrap();
    let disposed_on_worker = disposals.iter().filter(|(_, on_worker)| *on_worker).count();
    eprintln!("server lock batches: {batches} checked, {on_worker} on runtime workers ({workers} replay workers)");
    eprintln!("server disposals: {} of {created} created, {disposed_on_worker} on runtime workers: {disposals:?}", disposals.len());
    assert_eq!(created, workers, "each replay worker must create servers");
    assert_eq!(
        disposals.len(),
        created,
        "every created Servers instance must enter Drop before the replay returns"
    );
    assert_eq!(
        disposed_on_worker, 0,
        "Servers entered Drop on a runtime worker: {disposals:?}"
    );
    let scripts = witness.scripts.load(Ordering::Relaxed);
    assert!(scripts >= scripted, "each scene's script must be witnessed");
    assert_eq!(
        batches,
        scripts + created,
        "each script and disposal must witness its server lock batch"
    );
    assert_eq!(
        on_worker, 0,
        "a runtime worker took a server's lock, which can wait behind a sleeping answer (#292)"
    );
    let (_, outcome) = replayed
        .into_iter()
        .find(|(path, _)| *path == late)
        .unwrap();
    let (_, (_, history)) = outcome.unwrap();
    let rung = history
        .iter()
        .find(|r| r.call == 2 && r.provider == "Bitails")
        .expect("Bitails asked in call 2");
    assert!(rung.fault, "{rung}");
    rung.msecs
}

/// The late scene's recorded fault, replayed in its sorted place among
/// every fixture on the default workers, and the same scene on one worker.
/// Both replays must take every server lock and dispose every instance off
/// the runtime workers, awaiting disposal before checking the witness. The
/// timings remain diagnostics: a loaded runner can delay either reading
/// without a lock wait (2587 against 2001 ms in run 40671013907, #410).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_fixtures_server_locks_and_disposals_stay_off_runtime_workers() {
    let company: Vec<pool::Scene> = fixture_files().into_iter().map(pool::load).collect();
    let pooled = late_fault_msecs(company, WORKERS).await;
    let alone = late_fault_msecs(Vec::new(), 1).await;
    eprintln!("late fault: {pooled} ms on {WORKERS} workers, {alone} ms on one");
}
