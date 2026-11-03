//! The bound on password derivations at once, `kdf_in_flight` (#631).

use super::*;
use std::time::Duration;

/// Long enough for a spawned task to reach its permit, many times over.
const SETTLE: Duration = Duration::from_millis(200);
const PATIENCE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_permit_runs_two_unlocks_one_after_the_other_and_both_succeed() {
    let config = CWIStyleWalletManagerConfig {
        kdf_in_flight: 1,
        ..Default::default()
    };
    let slot: Arc<std::sync::OnceLock<Arc<Semaphore>>> = Arc::default();
    let (probe, mut order) = tokio::sync::mpsc::unbounded_channel();
    let read = slot.clone();
    // At each build, the permits left: the other unlock derives on the one permit
    // the build's own derivation gave back, or it has finished and none derive.
    let (m, seen) = manager_with(config, move || {
        probe.send(read.get().unwrap().available_permits()).unwrap();
    });
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    let a = m.create_profile("A", "pw a").await.unwrap();
    let b = m.create_profile("B", "pw b").await.unwrap();
    let permits = m.kdf_permits.clone().unwrap();
    slot.set(permits.clone()).unwrap();
    let held = permits.acquire_owned().await.unwrap();
    let m = Arc::new(m);
    let (ma, mb) = (m.clone(), m.clone());
    let ua = tokio::spawn(async move { ma.switch_profile(&a.id, "pw a").await });
    let ub = tokio::spawn(async move { mb.switch_profile(&b.id, "pw b").await });
    tokio::time::sleep(SETTLE).await;
    assert!(
        !ua.is_finished() && !ub.is_finished(),
        "both wait on the permit"
    );
    drop(held);
    ua.await.unwrap().unwrap();
    ub.await.unwrap().unwrap();
    let probes = [order.recv().await.unwrap(), order.recv().await.unwrap()];
    assert_eq!(
        probes,
        [0, 1],
        "the later derivation began as the other ended"
    );
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_derivations_run_at_once_by_default_and_a_fifth_waits_for_a_permit() {
    let (m, _) = manager();
    let permits = m.kdf_permits.clone().unwrap();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let (mut releases, mut tasks) = (Vec::new(), Vec::new());
    for i in 0..5 {
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (permits, started) = (permits.clone(), started.clone());
        releases.push(release);
        tasks.push(tokio::spawn(async move {
            off_worker(Some(&permits), "pw", move |_| {
                started.send(i).unwrap();
                let _ = wait.recv();
                Ok(i)
            })
            .await
        }));
        if i < 4 {
            let start = tokio::time::timeout(PATIENCE, starts.recv()).await;
            assert_eq!(start.unwrap(), Some(i), "derivation {i} runs at once");
        }
    }
    tokio::time::sleep(SETTLE).await;
    assert!(starts.try_recv().is_err(), "the fifth waits");
    assert_eq!(permits.available_permits(), 0);
    drop(releases.remove(0));
    let fifth = tokio::time::timeout(PATIENCE, starts.recv()).await;
    assert_eq!(
        fifth.unwrap(),
        Some(4),
        "a returned permit goes to the fifth"
    );
    drop(releases);
    for (i, task) in tasks.into_iter().enumerate() {
        assert_eq!(task.await.unwrap().unwrap(), i);
    }
    assert_eq!(permits.available_permits(), 4, "every permit back");
}

#[tokio::test]
async fn a_kdf_in_flight_of_zero_refuses_each_derivation_by_name() {
    let config = CWIStyleWalletManagerConfig {
        kdf_in_flight: 0,
        ..Default::default()
    };
    let (m, _) = manager_with(config, || {});
    assert!(m.kdf_permits.is_none());
    let refused = m.create_profile("Work", "pw").await;
    assert!(
        matches!(&refused, Err(Error::InvalidArgument(e)) if e == "kdf_in_flight is 0: no password derivation can run"),
        "{refused:?}"
    );
    assert!(m.get_profiles().await.is_empty(), "nothing created");
}
