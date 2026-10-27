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

use super::*;

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
