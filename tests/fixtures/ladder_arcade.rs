//! The read ladders in Arcade mode, replayed from fixtures
//! (bsv-wallet-toolbox-rs#367). Arcade has been the default mode since
//! #342; the scenes under `tests/fixtures/data/ladders/` replay classic
//! mode, which builds no Arcade rung. Each file
//! `tests/fixtures/data/ladders_arcade/<ladder>/<name>.json` is a scene of
//! the same shape and harness (`ladders.rs`): the servers, the worker pool,
//! the stall reading, the comparison, `LADDER_FIXTURES_BLESS=1`. It names
//! an `ArcadeV2` provider ahead of the explorers, whose answers are
//! Arcade's status documents, `GET /tx/{txid}` (`providers/arcade.rs`):
//! `txid`, `txStatus` and, on a `MINED` one, `blockHash`, `blockHeight`
//! and `merklePath` (a BUMP, hex). A scene that names `ArcadeV2` replays
//! with `ServicesOptions::with_arcade` at that provider's local server,
//! its client giving up at the explorers' deadline; every other scene in
//! classic mode.
//!
//! Only the two ladders Arcade is a rung of are here, each in the order
//! the ladder asks its providers (`arcade_mode_keeps_the_read_ladders`
//! pins the same order on the collections). The replay checks every
//! rung each call asked, from the services' call history, against that
//! order, and Arcade asked ahead of every explorer.

use super::super::toolbox::services::ArcadeConfig;
use super::*;

/// The ladders with an Arcade rung, each with its providers in the order
/// the ladder asks them in Arcade mode.
const ARCADE_LADDERS: [(&str, &[&str]); 2] = [
    (
        "get_merkle_path",
        &[
            "ArcadeV2",
            "WhatsOnChain",
            "BananaBlocks",
            "Bitails",
            "Chaintracks",
        ],
    ),
    (
        "get_status_for_txids",
        &["ArcadeV2", "WhatsOnChain", "Bitails", "BananaBlocks"],
    ),
];

/// The classic scenes recorded the explorer read ladders, which carry no
/// broadcaster rung only in classic mode: the deprecated knob, at a
/// loopback port nothing listens on, since no fixture posts (#317).
#[allow(deprecated)]
pub(super) fn classic_options() -> ServicesOptions {
    ServicesOptions::mainnet().with_classic_arc("http://127.0.0.1:9", None)
}

/// A scene's mode: Arcade at its `ArcadeV2` server when it names one, its
/// client's deadline the explorers'; classic otherwise.
pub(super) fn options(fixture: &Fixture, servers: &Servers) -> ServicesOptions {
    if !fixture.providers.iter().any(|p| p.name == "ArcadeV2") {
        return classic_options();
    }
    let config = ArcadeConfig {
        timeout_secs: Some(CLIENT_TIMEOUT_SECS),
        ..Default::default()
    };
    ServicesOptions::mainnet().with_arcade(servers.url("ArcadeV2"), Some(config))
}

fn arcade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/data/ladders_arcade")
}

/// Every Arcade scene file, sorted.
fn arcade_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    for dir in std::fs::read_dir(arcade_root()).expect("the Arcade ladder fixtures") {
        let dir = dir.unwrap().path();
        if !dir.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(&dir).unwrap() {
            let file = file.unwrap().path();
            if file.extension().is_some_and(|e| e == "json") {
                files.push(file);
            }
        }
    }
    files.sort();
    files
}

fn arcade_order(ladder: &str) -> Result<&'static [&'static str], String> {
    ARCADE_LADDERS
        .iter()
        .find(|(name, _)| *name == ladder)
        .map(|(_, order)| *order)
        .ok_or_else(|| format!("ladder {ladder:?} has no Arcade rung"))
}

/// An Arcade scene's shape: a ladder with an Arcade rung, `ArcadeV2`
/// named ahead of the rest, and the rest the classic scene's shape
/// (`check_shape`: its own directory, the ladder's order, WhatsOnChain
/// and Bitails named, a call).
fn check_arcade_shape(path: &Path, fixture: &Fixture) -> Result<(), String> {
    arcade_order(&fixture.ladder)?;
    if !matches!(fixture.providers.as_slice(), [p, ..] if p.name == "ArcadeV2") {
        return Err("an Arcade scene names ArcadeV2 ahead of every provider".to_string());
    }
    let classic = Fixture {
        about: fixture.about.clone(),
        ladder: fixture.ladder.clone(),
        providers: fixture.providers[1..].to_vec(),
        calls: fixture.calls.clone(),
        expected: None,
    };
    check_shape(path, &classic)
}

fn load(path: PathBuf) -> pool::Scene {
    let fixture = std::fs::read_to_string(&path)
        .map_err(|e| format!("does not read: {e}"))
        .and_then(|text| {
            serde_json::from_str::<Fixture>(&text).map_err(|e| format!("does not parse: {e}"))
        })
        .and_then(|fixture| check_arcade_shape(&path, &fixture).map(|()| fixture));
    (path, fixture)
}

/// Each call's rungs, from the call history, in the ladder's order, and
/// the opening call's opening rung Arcade. A rung may stand twice in a
/// row: a proof refuted at the header service records the answer and the
/// refutation.
fn check_order(fixture: &Fixture, history: &[stall::Rung]) -> Result<(), String> {
    let order = arcade_order(&fixture.ladder)?;
    if !matches!(history, [r, ..] if r.provider == "ArcadeV2") {
        return Err(format!(
            "Arcade is not the opening rung asked: {}",
            stall::history_text(history)
        ));
    }
    for call in 1..=fixture.calls.len() {
        let asked: Vec<usize> = history
            .iter()
            .filter(|r| r.call == call)
            .map(|r| {
                order
                    .iter()
                    .position(|n| *n == r.provider)
                    .unwrap_or(usize::MAX)
            })
            .collect();
        if asked.windows(2).any(|w| w[0] > w[1]) {
            return Err(format!(
                "call {call} asked out of the ladder's order {order:?}: {}",
                stall::history_text(history)
            ));
        }
    }
    Ok(())
}

#[test]
fn arcade_ladders_open_with_arcade_and_keep_the_classic_order() {
    for (ladder, order) in ARCADE_LADDERS {
        let classic = LADDERS.iter().find(|(name, _)| *name == ladder).unwrap().1;
        assert_eq!(order[0], "ArcadeV2", "{ladder}");
        assert_eq!(&order[1..], classic, "{ladder}");
    }
}

/// Every Arcade scene replayed: each call's result and each provider's
/// requests by route are the scene's `expected`, and each call asked its
/// rungs in the Arcade ladder's order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_fixtures_replay_in_arcade_mode() {
    let bless = std::env::var_os("LADDER_FIXTURES_BLESS").is_some_and(|v| v == "1");
    let files = arcade_files();
    assert!(
        !files.is_empty(),
        "no Arcade ladder fixtures under {:?}",
        arcade_root()
    );
    let mut ladders: Vec<String> = Vec::new();
    let scenes = files.into_iter().map(load).collect();
    let replayed = pool::replay_queue(scenes, WORKERS).await;

    let mut failures = Vec::new();
    let total = replayed.len();
    for (path, outcome) in replayed {
        let name = path
            .strip_prefix(arcade_root())
            .unwrap_or(&path)
            .display()
            .to_string();
        match outcome {
            Err(e) => failures.push(format!("{name}: {e}")),
            Ok((mut fixture, (actual, history))) => {
                ladders.push(fixture.ladder.clone());
                if let Err(e) = check_order(&fixture, &history) {
                    failures.push(format!("{name}: {e}"));
                }
                if bless {
                    fixture.expected = Some(actual);
                    std::fs::write(&path, fixture_text(&fixture)).unwrap();
                } else if let Some(difference) = stall::difference(&fixture, &actual, &history) {
                    failures.push(format!("{name}: {difference}"));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} Arcade ladder fixtures differ:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    for (ladder, _) in ARCADE_LADDERS {
        assert!(
            ladders.iter().any(|l| l == ladder),
            "no Arcade scene replays {ladder}"
        );
    }
}
