//! The replay and mockito's server lock (bsv-wallet-toolbox-rs#292).
//!
//! A mockito 1.7.2 server answers on a thread of its own, holding its
//! state's write lock (`std::sync::RwLock`, `server.rs:536`) while it runs a
//! mock's status closure, and a `delay_ms` answer sleeps inside that closure.
//! `Server::reset`, `Mock::create_async` and `Server`'s drop take the same
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
    let mut held = std::mem::take(&mut servers.0);
    let (held, mocks) = tokio::task::spawn_blocking(move || {
        let mocks = script_blocking(&providers, &mut held, &counts);
        (held, mocks)
    })
    .await
    .expect("the script's blocking task");
    servers.0 = held;
    mocks
}

/// A server's drop resets it under its lock: off the runtime's workers too.
impl Drop for Servers {
    fn drop(&mut self) {
        let servers = std::mem::take(&mut self.0);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || drop(servers));
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
    let replayed = pool::replay_queue(scenes, workers).await;
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

/// How far the late fault's reading on the default workers may sit from a
/// single worker's. A worker blocked in `reset` behind a fixture's 3000 ms
/// answer holds every timer back by that answer's remainder past the
/// 2000 ms deadline, 1000 ms (measured on base: 3002 against 2003 ms);
/// half of it parts the two readings with room either side for a loaded
/// runner's scheduling, tens of milliseconds where it was measured.
const LOCK_MARGIN_MS: u64 = 500;

/// The late scene's recorded fault, replayed in its sorted place among
/// every fixture on the default workers, against the same scene on one
/// worker: they agree within [`LOCK_MARGIN_MS`] only when no worker waits
/// on a server's lock behind a sleeping answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_fixtures_replay_times_match_a_single_worker() {
    let company: Vec<pool::Scene> = fixture_files().into_iter().map(pool::load).collect();
    let pooled = late_fault_msecs(company, WORKERS).await;
    let alone = late_fault_msecs(Vec::new(), 1).await;
    eprintln!("late fault: {pooled} ms on {WORKERS} workers, {alone} ms on one");
    assert!(
        pooled.abs_diff(alone) <= LOCK_MARGIN_MS,
        "the late fault took {pooled} ms on {WORKERS} workers, {alone} ms on one: \
         a worker waited on a server's lock behind a sleeping answer (#292)"
    );
}
