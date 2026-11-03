//! The bound on password derivations at once, `with_kdf_in_flight` (#631).

use super::*;
use std::time::Duration;

/// Long enough for a spawned task to reach its permit, many times over.
const SETTLE: Duration = Duration::from_millis(200);
const PATIENCE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_permit_runs_two_unlocks_one_after_the_other_and_both_succeed() {
    let slot: Arc<std::sync::OnceLock<Arc<Semaphore>>> = Arc::default();
    let (probe, mut order) = tokio::sync::mpsc::unbounded_channel();
    let read = slot.clone();
    // At each build, the permits left: the other unlock derives on the one permit
    // the build's own derivation gave back, or it has finished and none derive.
    let (m, seen) = manager_with(CWIStyleWalletManagerConfig::default(), move || {
        probe.send(read.get().unwrap().available_permits()).unwrap();
    });
    let m = m.with_kdf_in_flight(1).unwrap();
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    let a = m.create_profile("A", "pw a").await.unwrap();
    let b = m.create_profile("B", "pw b").await.unwrap();
    let permits = m.kdf_permits.clone();
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
    assert_eq!(
        DEFAULT_KDF_IN_FLIGHT, 4,
        "512 MiB of argon2id at the default"
    );
    let (m, _) = manager();
    let held = m.kdf_permits.clone().available_permits();
    assert_eq!(held, DEFAULT_KDF_IN_FLIGHT, "new starts at the default");
    let m = m.with_kdf_in_flight(DEFAULT_KDF_IN_FLIGHT).unwrap();
    let permits = m.kdf_permits.clone();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let (mut releases, mut tasks) = (Vec::new(), Vec::new());
    for i in 0..5 {
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (permits, started) = (permits.clone(), started.clone());
        releases.push(release);
        tasks.push(tokio::spawn(async move {
            off_worker(&permits, "pw", move |_| {
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
async fn with_kdf_in_flight_refuses_zero_by_name_before_any_derivation() {
    let builds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = builds.clone();
    let (m, seen) = manager_with(CWIStyleWalletManagerConfig::default(), move || {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });
    let refused = m.with_kdf_in_flight(0).map(|_| ());
    let named = format!(
        "with_kdf_in_flight(0): kdf_in_flight must be in 1..={}",
        Semaphore::MAX_PERMITS
    );
    assert!(
        matches!(&refused, Err(Error::InvalidArgument(e)) if *e == named),
        "{refused:?}"
    );
    let past = manager().0.with_kdf_in_flight(usize::MAX).map(|_| ());
    assert!(
        matches!(past, Err(Error::InvalidArgument(_))),
        "no panic past the max"
    );
    assert_eq!(
        builds.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no derivation ran"
    );
    assert!(seen.lock().unwrap().is_empty());
}
