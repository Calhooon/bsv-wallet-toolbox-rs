//! The replay's worker pool (bsv-wallet-toolbox-rs#292): scenes replayed
//! `workers` at a time, each worker on servers of its own.

use super::*;

/// A scene to replay: its file and the fixture, or why it cannot replay.
pub(super) type Scene = (PathBuf, Result<Fixture, String>);

/// A scene replayed: the fixture with what the replay recorded and the
/// ladder's call history, or why it did not replay.
pub(super) type Replayed = (
    PathBuf,
    Result<(Fixture, (Expected, Vec<stall::Rung>)), String>,
);

/// A fixture file read and its shape checked.
pub(super) fn load(path: PathBuf) -> Scene {
    let fixture = std::fs::read_to_string(&path)
        .map_err(|e| format!("does not read: {e}"))
        .and_then(|text| {
            serde_json::from_str::<Fixture>(&text).map_err(|e| format!("does not parse: {e}"))
        })
        .and_then(|fixture| check_shape(&path, &fixture).map(|()| fixture));
    (path, fixture)
}

/// Replay `scenes` in queue order on `workers` workers; the results sorted
/// by file, so a report reads the same twice.
pub(super) async fn replay_queue(scenes: Vec<Scene>, workers: usize) -> Vec<Replayed> {
    replay_queue_with_witness(scenes, workers, Arc::default()).await
}

/// Replay with a witness shared by each worker and its blocking lock calls.
pub(super) async fn replay_queue_with_witness(
    scenes: Vec<Scene>,
    workers: usize,
    witness: Arc<lock::LockWitness>,
) -> Vec<Replayed> {
    let queue = Arc::new(Mutex::new(scenes.into_iter().collect::<VecDeque<_>>()));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let queue = Arc::clone(&queue);
        handles.push(tokio::spawn(lock::watch_worker(
            Arc::clone(&witness),
            async move {
                let mut servers = Servers::new().await;
                let mut done = Vec::new();
                loop {
                    let Some((path, fixture)) = queue.lock().unwrap().pop_front() else {
                        break;
                    };
                    let outcome = match fixture {
                        Err(e) => Err(e),
                        Ok(fixture) => stall::replay_scene(&fixture, &fixture, &mut servers)
                            .await
                            .map(|(actual, history, retried)| {
                                if let Some(note) = retried {
                                    stall::name_the_retry(&path, &note);
                                }
                                (fixture, (actual, history))
                            }),
                    };
                    done.push((path, outcome));
                }
                done
            },
        )));
    }
    let mut replayed = Vec::new();
    for handle in handles {
        replayed.extend(handle.await.unwrap());
    }
    replayed.sort_by(|a, b| a.0.cmp(&b.0));
    replayed
}
