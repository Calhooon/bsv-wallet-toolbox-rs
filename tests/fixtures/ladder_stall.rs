//! The replay's reading of a difference (bsv-wallet-toolbox-rs#194): the
//! ladder's call history and the requests per provider beside every
//! difference, and a runner stall told apart from a ladder verdict.
//!
//! Run 40310320187 differed on `429_without_retry_after_closes_the_rung`:
//! Bitails's second mock answer came past the fixture client's deadline on
//! a loaded runner, the ladder took it as a fault and asked the unscripted
//! `BananaBlocks` rung, and the result's error (`last_error`) named only that
//! rung's 501. Runs 40515505707 (`Code Coverage`) and 40522857063 differed
//! with nothing unscripted after the fault: one rung faulted at 2.2 to
//! 3.0 s beside ordinary answers of 1.2 to 1.5 s, on loaded hosted runners
//! (an ordinary answer costs at most 138 ms where it was measured, 19 ms
//! instrumented, against the 2000 ms deadline). So a fault whose call
//! lasted to the deadline where the fixture scripts no delay is the
//! runner's, whatever the ladder did after: the message opens with that
//! and names any request the fixture does not script, and the history says
//! which rung faulted.
//!
//! A scene that differs and reads a runner stall is replayed as many extra times as `LADDER_FIXTURES_STALL_REPLAYS` allows, one when unset,
//! ([`replay_scene`]) and judged on that replay; the stall and the stalled
//! attempt's history go to the run's log past the test harness's capture
//! ([`name_the_retry`]), so a run that passes on the retry still names it.
//! A stall past the last replay fails the scene with the reading at its
//! head. A scene that scripts a timeout (`delay_ms` to the deadline) reads
//! no stall: its fault is the fixture's, never retried.
//!
//! How many replays a stalled scene gets is `LADDER_FIXTURES_STALL_REPLAYS`
//! ([`stall_replays`]), one when unset, so the local run is as it was. CI
//! sets two on the steps that replay the ladders (#513: push run
//! 41183213380 stalled one scene twice, 2001 ms then 2106 ms, and the next
//! push run was green on the same code). A longer deadline would not do:
//! the scripted timeouts hold their answers 3000 ms against the 2000 ms
//! deadline, and the reading counts a fault late from that deadline.
//!
//! The deadline is reqwest's own timer, set from `timeout_secs` inside the
//! providers' constructors; the held `Clock` (`pace.rs`) reads only the rate
//! gates, so it cannot drive the deadline without production source. A
//! paused tokio clock cannot either: it jumps to the next timer whenever the
//! runtime waits, so it would fire every client timeout while a mock's
//! answer is on its way. The scene below holds the late answer back on the
//! mock's side instead, past the deadline by `LATE_SLACK_MS`, so the client
//! times out on every runner.

use super::super::toolbox::services::ServiceCallHistory;
use super::*;

/// A fault is late when its call lasted to within this of the deadline:
/// the call history's clock (chrono, the system clock) and the client's
/// timer (tokio, the monotonic clock) part by a little.
const LATE_SLACK_MS: u64 = 100;

/// The knob that names how many replays a stalled scene gets (#513).
pub(super) const STALL_REPLAYS_VAR: &str = "LADDER_FIXTURES_STALL_REPLAYS";

/// The replays a stalled scene gets when the knob is unset: the local run.
pub(super) const STALL_REPLAYS_DEFAULT: usize = 1;

/// The knob's value read: unset is [`STALL_REPLAYS_DEFAULT`], anything but
/// a count is an error, never a silent default.
fn stall_replays_from(value: Result<String, std::env::VarError>) -> Result<usize, String> {
    match value {
        Err(std::env::VarError::NotPresent) => Ok(STALL_REPLAYS_DEFAULT),
        Ok(text) => text
            .trim()
            .parse()
            .map_err(|e| format!("{STALL_REPLAYS_VAR}={text:?} is not a count: {e}")),
        Err(e) => Err(format!("{STALL_REPLAYS_VAR} does not read: {e}")),
    }
}

/// How many replays a stalled scene gets in this run, from
/// `LADDER_FIXTURES_STALL_REPLAYS`.
pub(super) fn stall_replays() -> usize {
    stall_replays_from(std::env::var(STALL_REPLAYS_VAR)).unwrap_or_else(|e| panic!("{e}"))
}

/// One rung the ladder asked, as its call history records it.
#[derive(Debug, Clone)]
pub(super) struct Rung {
    /// The fixture's call (from 1) that asked it.
    pub(super) call: usize,
    /// The call history's collection (`getStatusForTxids`, `getRawTx`...).
    pub(super) service: String,
    pub(super) provider: String,
    pub(super) msecs: u64,
    pub(super) fault: bool,
    /// `answered`, `fault <code>: <message>` or `failed: <result>`.
    pub(super) word: String,
}

impl std::fmt::Display for Rung {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "call {}: {} ({}, {} ms) {}",
            self.call, self.provider, self.service, self.msecs, self.word
        )
    }
}

/// Every call the services' history holds, oldest to newest.
fn all_calls(services: &Services) -> Vec<Rung> {
    let Ok(all) = services.get_services_call_history(false) else {
        return Vec::new();
    };
    let collections: Vec<ServiceCallHistory> = [
        all.get_merkle_path,
        all.get_raw_tx,
        all.post_beef,
        all.get_utxo_status,
        all.get_status_for_txids,
        all.get_script_hash_history,
    ]
    .into_iter()
    .flatten()
    .collect();
    let mut calls = Vec::new();
    for collection in collections {
        for (provider, history) in collection.history_by_provider {
            for call in history.calls {
                let word = match (&call.error, call.success) {
                    (Some(e), _) => format!("fault {}: {}", e.code, e.message),
                    (None, true) => "answered".to_string(),
                    (None, false) => format!("failed: {}", call.result.as_deref().unwrap_or("")),
                };
                let rung = Rung {
                    call: 0,
                    service: collection.service_name.clone(),
                    provider: provider.clone(),
                    msecs: call.msecs,
                    fault: !call.success,
                    word,
                };
                calls.push((call.when, rung));
            }
        }
    }
    calls.sort_by_key(|(when, _)| *when);
    calls.into_iter().map(|(_, rung)| rung).collect()
}

/// After the fixture's call `call`: the rungs it asked, appended to
/// `history`. A ladder whose calls keep no history (the header ladders)
/// adds none.
pub(super) fn record_history(services: &Services, call: usize, history: &mut Vec<Rung>) {
    let seen = history.len();
    for mut rung in all_calls(services).into_iter().skip(seen) {
        rung.call = call;
        history.push(rung);
    }
}

/// Each server's address in the history becomes its provider's name.
pub(super) fn normalize(history: &mut [Rung], servers: &Servers) {
    for rung in history {
        for name in PROVIDERS {
            let url = servers.url(name);
            rung.word = rung.word.replace(&url, &format!("<{name}>"));
            let host = url.trim_start_matches("http://");
            rung.word = rung.word.replace(host, &format!("<{name}>"));
        }
    }
}

/// How many of `provider`'s requests the fixture scripts late: answers
/// held past the deadline, the route's last one serving every request
/// after it.
fn scripted_late(fixture: &Fixture, provider: &str, deadline_ms: u64) -> usize {
    let late = |answer: &Answer| {
        answer
            .delay_ms
            .is_some_and(|delay| delay + LATE_SLACK_MS >= deadline_ms)
    };
    let mut total = 0usize;
    for route in fixture
        .providers
        .iter()
        .filter(|p| p.name == provider)
        .flat_map(|p| &p.routes)
    {
        for (at, answer) in route.answers.iter().enumerate() {
            if late(answer) {
                let serves = if at + 1 == route.answers.len() {
                    usize::MAX
                } else {
                    answer.times.unwrap_or(1)
                };
                total = total.saturating_add(serves);
            }
        }
    }
    total
}

/// A runner stall: a rung's fault came at the fixture client's deadline
/// where the fixture scripts no late answer, whatever the ladder did after
/// (a later rung asked a request the fixture does not script is named
/// too). `None` when the difference is the ladder's to explain.
pub(super) fn reading(
    fixture: &Fixture,
    actual: &Expected,
    history: &[Rung],
    deadline: Duration,
) -> Option<String> {
    let unscripted: Vec<(&str, &str)> = actual
        .requests
        .iter()
        .flat_map(|(provider, routes)| {
            routes
                .keys()
                .filter(|route| route.starts_with("UNSCRIPTED "))
                .map(move |route| (provider.as_str(), route.as_str()))
        })
        .collect();
    let deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX);
    let late = |rung: &Rung| rung.fault && rung.msecs + LATE_SLACK_MS >= deadline_ms;
    for (at, rung) in history.iter().enumerate().filter(|(_, rung)| late(rung)) {
        let late_so_far = history[..=at]
            .iter()
            .filter(|r| r.provider == rung.provider && late(r))
            .count();
        if late_so_far <= scripted_late(fixture, &rung.provider, deadline_ms) {
            continue;
        }
        let asked = history[at + 1..]
            .iter()
            .find_map(|next| {
                unscripted
                    .iter()
                    .find(|(provider, _)| *provider == next.provider)
                    .map(|(provider, route)| (next.call, *provider, *route))
            })
            .map(|(call, provider, route)| {
                format!(
                    "; the ladder moved on and asked {provider} `{route}` in call {call}, \
                     which the fixture does not script"
                )
            })
            .unwrap_or_default();
        return Some(format!(
            "RUNNER STALL, not a ladder verdict: {}'s answer in call {} faulted after {} ms, \
             at the fixture client's {} ms deadline, where the fixture scripts no late \
             answer{asked}. The runner was too slow to serve the mock in time: re-run before \
             reading the ladder.",
            rung.provider, rung.call, rung.msecs, deadline_ms
        ));
    }
    None
}

/// The ladder's call history, one rung a line.
pub(super) fn history_text(history: &[Rung]) -> String {
    if history.is_empty() {
        return "call history: none recorded (the header ladders keep none)".to_string();
    }
    let lines: Vec<String> = history.iter().map(|rung| format!("\n  {rung}")).collect();
    format!(
        "call history (each rung asked, in order):{}",
        lines.concat()
    )
}

/// The replay's requests per provider, with how many no route scripts.
pub(super) fn requests_text(actual: &Expected) -> String {
    let per_provider: Vec<String> = actual
        .requests
        .iter()
        .map(|(provider, routes)| {
            let total: usize = routes.values().sum();
            let unscripted: usize = routes
                .iter()
                .filter(|(route, _)| route.starts_with("UNSCRIPTED "))
                .map(|(_, n)| n)
                .sum();
            if unscripted > 0 {
                format!("{provider} {total} ({unscripted} unscripted)")
            } else {
                format!("{provider} {total}")
            }
        })
        .collect();
    format!("requests per provider: {}", per_provider.join(", "))
}

/// What a replay that differs from its fixture prints: a runner stall when
/// it is one, the request body difference, the call history, the requests
/// per provider, and both sides. `None` when they agree.
pub(super) fn difference(fixture: &Fixture, actual: &Expected, history: &[Rung]) -> Option<String> {
    if fixture.expected.as_ref() == Some(actual) {
        return None;
    }
    let stall = reading(
        fixture,
        actual,
        history,
        Duration::from_secs(CLIENT_TIMEOUT_SECS),
    )
    .map(|stall| format!("\n{stall}"))
    .unwrap_or_default();
    let body_difference = fixture
        .expected
        .as_ref()
        .and_then(|expected| request_body_difference(expected, actual))
        .unwrap_or_default();
    Some(format!(
        "{}{stall}\n{body_difference}\n{}\n{}\n--- expected\n{}\n+++ replayed\n{}",
        fixture.about,
        history_text(history),
        requests_text(actual),
        serde_json::to_string_pretty(&fixture.expected).unwrap(),
        serde_json::to_string_pretty(actual).unwrap()
    ))
}

/// A scene replayed on a worker's servers, attempt `n` (from 0) stood up as
/// `loaded[n]` and every attempt past `loaded` as the scene itself (in a
/// test, `loaded` is the scene as a loaded runner serves it): what the last
/// attempt recorded, the ladder's call history, and a note for each runner
/// stall it was replayed once more for, `replays` at most.
pub(super) async fn replay_scene(
    fixture: &Fixture,
    loaded: &[Fixture],
    replays: usize,
    servers: &mut Servers,
) -> Result<(Expected, Vec<Rung>, Vec<String>), String> {
    let mut notes = Vec::new();
    loop {
        let attempt = loaded.get(notes.len()).unwrap_or(fixture);
        assert_eq!(fixture.calls, attempt.calls, "one scene's calls");
        let (actual, history) = replay(attempt, servers).await?;
        let stall = if notes.len() < replays {
            stalled(fixture, &actual, &history)
        } else {
            None
        };
        let Some(stall) = stall else {
            return Ok((actual, history, notes));
        };
        notes.push(format!(
            "retry {} of {replays}: replayed once more for a {stall}\nthe stalled attempt's {}",
            notes.len() + 1,
            history_text(&history)
        ));
    }
}

/// The runner stall a replay that differs from its fixture reads, if any.
fn stalled(fixture: &Fixture, actual: &Expected, history: &[Rung]) -> Option<String> {
    if fixture.expected.as_ref() == Some(actual) {
        return None;
    }
    let deadline = Duration::from_secs(CLIENT_TIMEOUT_SECS);
    reading(fixture, actual, history, deadline)
}

/// A scene replayed once more for a runner stall, named in the run's log
/// whether it then agrees or not: straight to the process's stderr, past
/// the test harness's capture, so a passing run names it too.
pub(super) fn name_the_retry(path: &Path, note: &str) {
    use std::io::Write;
    let name = path.strip_prefix(fixture_root()).unwrap_or(path).display();
    let _ = writeln!(std::io::stderr(), "{name}: {note}");
}

/// The recorded scene of run 40310320187: `WhatsOnChain`'s 429 with no
/// Retry-After closes its rung, Bitails answers both calls.
fn recorded_scene() -> Fixture {
    let path =
        fixture_root().join("get_status_for_txids/429_without_retry_after_closes_the_rung.json");
    let fixture: Fixture = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    check_shape(&path, &fixture).unwrap();
    fixture
}

/// What the loaded runner of run 40310320187 did: the recorded script,
/// the second Bitails answer held back past the deadline.
pub(super) fn late_second_answer() -> Fixture {
    let mut stalled = recorded_scene();
    let bitails = stalled
        .providers
        .iter_mut()
        .find(|p| p.name == "Bitails")
        .unwrap();
    let answers = &mut bitails.routes[0].answers;
    answers[0].times = Some(1);
    let mut late = answers[0].clone();
    late.delay_ms = Some(CLIENT_TIMEOUT_SECS * 1000 + LATE_SLACK_MS);
    answers.push(late);
    stalled
}

/// A planted mismatch: the message names each rung asked, in order, with
/// its answer or fault, and the requests per provider, and reads no stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_a_mismatch_prints_the_call_history() {
    let mut fixture = recorded_scene();
    let expected = fixture.expected.as_mut().unwrap();
    *expected
        .requests
        .get_mut("Bitails")
        .unwrap()
        .values_mut()
        .next()
        .unwrap() = 3;
    let mut servers = Servers::new().await;
    let (actual, history) = replay(&fixture, &mut servers).await.unwrap();
    let message = difference(&fixture, &actual, &history).expect("the planted count differs");
    eprintln!("{message}");

    let lines: Vec<String> = history.iter().map(ToString::to_string).collect();
    let starts = [
        "call 1: WhatsOnChain (getStatusForTxids, ",
        "call 1: Bitails (getStatusForTxids, ",
        "call 2: WhatsOnChain (getStatusForTxids, 0 ms) fault RATE_LIMITED: ",
        "call 2: Bitails (getStatusForTxids, ",
    ];
    assert_eq!(lines.len(), starts.len(), "{lines:#?}");
    assert!(lines[0].contains(" ms) fault RATE_LIMITED: "), "{lines:#?}");
    assert!(lines[1].ends_with(" ms) answered"), "{lines:#?}");
    assert!(lines[3].ends_with(" ms) answered"), "{lines:#?}");
    for (line, start) in lines.iter().zip(starts) {
        assert!(line.starts_with(start), "{line:?} starts {start:?}");
        assert!(
            message.contains(line.as_str()),
            "{line:?} is in the message"
        );
    }
    assert!(message.contains("call history (each rung asked, in order):"));
    assert!(message.contains("requests per provider: Bitails 2, WhatsOnChain 1"));
    assert!(!message.contains("RUNNER STALL"), "{message}");
}

/// The late-second-answer scene (#194): the recorded fixture, replayed on
/// a runner that serves Bitails's second answer past the client's
/// deadline. The ladder takes it as a fault and falls to the unscripted
/// `BananaBlocks` rung; the result's error names only that rung; the replay
/// reads a runner stall and the history names the Bitails fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_a_late_second_answer_reads_as_a_runner_stall() {
    let recorded = recorded_scene();
    let stalled = late_second_answer();
    let mut servers = Servers::new().await;
    let (actual, history) = replay(&stalled, &mut servers).await.unwrap();
    let expected = recorded.expected.as_ref().unwrap();

    // The ladder: call 1 as recorded; call 2 could not look.
    assert_eq!(actual.results[0], expected.results[0]);
    assert_eq!(actual.results[1]["ok"]["name"], "Services", "{actual:#?}");
    assert_eq!(actual.results[1]["ok"]["status"], "error", "{actual:#?}");
    let error = actual.results[1]["ok"]["error"].to_string();
    assert!(
        !error.contains("Bitails"),
        "last_error names only the last rung: {error}"
    );
    // WhatsOnChain's rung stayed closed; Bitails was asked twice; the
    // unscripted BananaBlocks rung once.
    assert_eq!(
        actual.requests["WhatsOnChain"],
        expected.requests["WhatsOnChain"]
    );
    assert_eq!(actual.requests["Bitails"], expected.requests["Bitails"]);
    let bananablocks: Vec<_> = actual.requests["BananaBlocks"].iter().collect();
    assert_eq!(bananablocks.len(), 1, "{bananablocks:?}");
    assert!(
        bananablocks[0].0.starts_with("UNSCRIPTED "),
        "{bananablocks:?}"
    );

    // The history names the Bitails fault at the deadline.
    let late = history
        .iter()
        .find(|r| r.call == 2 && r.provider == "Bitails")
        .expect("Bitails asked in call 2");
    assert!(late.fault, "{late}");
    assert!(
        late.msecs + LATE_SLACK_MS >= CLIENT_TIMEOUT_SECS * 1000,
        "{late}"
    );
    let after: Vec<&str> = history
        .iter()
        .filter(|r| r.call == 2)
        .map(|r| r.provider.as_str())
        .collect();
    assert_eq!(after, ["WhatsOnChain", "Bitails", "BananaBlocks"]);

    // Against the recorded fixture: a stall, the history, the counts.
    let message = difference(&recorded, &actual, &history).expect("the replay differs");
    eprintln!("{message}");
    assert!(
        message.contains("RUNNER STALL, not a ladder verdict: Bitails's answer in call 2"),
        "{message}"
    );
    assert!(message.contains(&late.to_string()), "{message}");
    assert!(
        message.contains(
            "requests per provider: BananaBlocks 1 (1 unscripted), Bitails 2, WhatsOnChain 1"
        ),
        "{message}"
    );
    // A fixture that scripts the late answer is a verdict, not a stall.
    let deadline = Duration::from_secs(CLIENT_TIMEOUT_SECS);
    assert_eq!(reading(&stalled, &actual, &history, deadline), None);
}

/// `fixture` as a loaded runner serves it: `provider`'s answer to its
/// request `nth` (from 1, on route 0) held back past the client's
/// deadline by less than [`LATE_SLACK_MS`], where the fixture scripts it
/// immediate. The route's opening answer must serve that request.
fn held_past_the_deadline(fixture: &Fixture, provider: &str, nth: usize) -> Fixture {
    let mut loaded = Fixture {
        about: fixture.about.clone(),
        ladder: fixture.ladder.clone(),
        providers: fixture.providers.clone(),
        calls: fixture.calls.clone(),
        expected: None,
    };
    let route = &mut loaded
        .providers
        .iter_mut()
        .find(|p| p.name == provider)
        .unwrap()
        .routes[0];
    let scripted = route.answers.remove(0);
    let serves = scripted.times.unwrap_or(1);
    assert!(scripted.delay_ms.is_none() && nth <= serves, "{scripted:?}");
    let mut late = scripted.clone();
    late.times = Some(1);
    late.delay_ms = Some(CLIENT_TIMEOUT_SECS * 1000 + LATE_SLACK_MS / 2);
    let mut answers = Vec::new();
    for (times, answer) in [(nth - 1, &scripted), (1, &late), (serves - nth, &scripted)] {
        if times > 0 {
            let mut answer = answer.clone();
            answer.times = Some(times);
            answers.push(answer);
        }
    }
    answers.append(&mut route.answers);
    route.answers = answers;
    loaded
}

/// A fault at the client's deadline where the scene scripts an immediate
/// answer is the runner's (#194: runs 40515505707 and 40522857063, each a
/// rung faulted at 2.2 to 3.0 s beside ordinary answers of 1.2 to 1.5 s on
/// a loaded hosted runner). Two recorded scenes, each with one Bitails
/// answer held past the deadline by less than [`LATE_SLACK_MS`] on its
/// opening attempt: the merkle path scene asks nothing unscripted after the
/// fault, the status scene falls to an unscripted rung. Each is replayed
/// once more, agrees with its fixture, and names the stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_a_fault_at_the_deadline_is_replayed_once_more() {
    let deadline_ms = CLIENT_TIMEOUT_SECS * 1000;
    let mut servers = Servers::new().await;
    for (scene, nth) in [
        ("get_merkle_path/429_retry_after_closes_the_rung.json", 1),
        (
            "get_status_for_txids/429_without_retry_after_closes_the_rung.json",
            2,
        ),
    ] {
        let path = fixture_root().join(scene);
        let (_, fixture) = pool::load(path.clone());
        let fixture = fixture.unwrap();
        let loaded = held_past_the_deadline(&fixture, "Bitails", nth);
        eprintln!(
            "{scene}: Bitails's answer {nth} held {} ms against the client's {deadline_ms} ms \
             deadline",
            deadline_ms + LATE_SLACK_MS / 2
        );
        let (actual, history, retries) = replay_scene(
            &fixture,
            std::slice::from_ref(&loaded),
            STALL_REPLAYS_DEFAULT,
            &mut servers,
        )
        .await
        .unwrap();
        if let Some(difference) = difference(&fixture, &actual, &history) {
            panic!("{scene}: the loaded runner's fault failed the scene:\n{difference}");
        }
        let [note] = retries.as_slice() else {
            panic!("{scene}: one stall named: {retries:#?}");
        };
        eprintln!("{scene}: {note}");
        assert!(
            note.contains(&format!(
                "RUNNER STALL, not a ladder verdict: Bitails's answer in call {nth} faulted"
            )),
            "{note}"
        );
        assert!(
            note.contains(&format!(
                "at the fixture client's {deadline_ms} ms deadline"
            )),
            "{note}"
        );
    }
}

/// The merkle path scene a stall is planted in.
const STALLING_SCENE: &str = "get_merkle_path/429_retry_after_closes_the_rung.json";

/// The scene replayed with its opening `stalls` attempts stood up with
/// Bitails's one answer held past the deadline and `replays` replays
/// allowed: the difference left, if any, and the retries named, each
/// written to the run's log as the pool writes it.
async fn stalled_attempts(stalls: usize, replays: usize) -> (Option<String>, Vec<String>) {
    let path = fixture_root().join(STALLING_SCENE);
    let fixture = pool::load(path.clone()).1.unwrap();
    let loaded: Vec<Fixture> = (0..stalls)
        .map(|_| held_past_the_deadline(&fixture, "Bitails", 1))
        .collect();
    let mut servers = Servers::new().await;
    let (actual, history, retries) = replay_scene(&fixture, &loaded, replays, &mut servers)
        .await
        .unwrap();
    for note in &retries {
        name_the_retry(&path, note);
    }
    (difference(&fixture, &actual, &history), retries)
}

/// Each retry names its count and the stall it was replayed for.
fn assert_named(retries: &[String], replays: usize) {
    for (at, note) in retries.iter().enumerate() {
        let opening = format!(
            "retry {} of {replays}: replayed once more for a RUNNER STALL, not a ladder \
             verdict: Bitails's answer in call 1 faulted",
            at + 1
        );
        assert!(note.starts_with(&opening), "{note}");
    }
}

/// The knob (#513): unset is one replay, a count is that count, anything
/// else is refused rather than read as the default.
#[test]
fn ladder_fixtures_the_stall_replays_knob_reads_a_count() {
    use std::env::VarError;
    assert_eq!(stall_replays_from(Err(VarError::NotPresent)), Ok(1));
    assert_eq!(stall_replays_from(Ok("2".into())), Ok(2));
    assert_eq!(stall_replays_from(Ok(" 2\n".into())), Ok(2));
    assert_eq!(stall_replays_from(Ok("0".into())), Ok(0));
    for text in ["", "two", "-1"] {
        let refused = stall_replays_from(Ok(text.into())).unwrap_err();
        assert!(
            refused.starts_with("LADDER_FIXTURES_STALL_REPLAYS="),
            "{refused}"
        );
    }
}

/// Under the knob at two (CI, #513): a scene that stalls twice agrees on
/// its third replay, and the log names both retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_two_stalls_pass_on_the_third_replay_under_two() {
    let (left, retries) = stalled_attempts(2, 2).await;
    if let Some(difference) = left {
        panic!("two stalls failed the scene under two replays:\n{difference}");
    }
    assert_eq!(retries.len(), 2, "{retries:#?}");
    assert_named(&retries, 2);
}

/// Under the knob at two: a third stall fails the scene with the reading
/// at the head of its message, after both retries are named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_a_third_stall_fails_under_two() {
    let (left, retries) = stalled_attempts(3, 2).await;
    let difference = left.expect("a third stall fails the scene");
    let head = difference.lines().nth(1).unwrap_or_default();
    assert!(
        head.starts_with("RUNNER STALL, not a ladder verdict: Bitails's answer in call 1"),
        "{difference}"
    );
    assert_eq!(retries.len(), 2, "{retries:#?}");
    assert_named(&retries, 2);
}

/// Under the default (the local run, unchanged): one stall is retried and
/// agrees; a second stall fails the scene with the reading at its head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_the_default_retries_one_stall() {
    let (left, retries) = stalled_attempts(1, STALL_REPLAYS_DEFAULT).await;
    assert_eq!(left, None);
    assert_eq!(retries.len(), 1, "{retries:#?}");
    assert_named(&retries, 1);

    let (left, retries) = stalled_attempts(2, STALL_REPLAYS_DEFAULT).await;
    let difference = left.expect("a second stall fails the scene");
    let head = difference.lines().nth(1).unwrap_or_default();
    assert!(
        head.starts_with("RUNNER STALL, not a ladder verdict"),
        "{difference}"
    );
    assert_eq!(retries.len(), 1, "{retries:#?}");
}

/// A difference that is the ladder's, not a stall, is never retried, under
/// the knob at two as under the default: the planted request count, and
/// the late answer a fixture scripts (a timeout, the fixture's verdict).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ladder_fixtures_a_ladder_difference_is_never_retried() {
    let mut planted = recorded_scene();
    let expected = planted.expected.as_mut().unwrap();
    *expected
        .requests
        .get_mut("Bitails")
        .unwrap()
        .values_mut()
        .next()
        .unwrap() = 3;
    let mut scripted = late_second_answer();
    scripted.expected = recorded_scene().expected;
    let mut servers = Servers::new().await;
    for (scene, fixture) in [("planted count", planted), ("scripted late", scripted)] {
        for replays in [STALL_REPLAYS_DEFAULT, 2] {
            let (actual, history, retries) = replay_scene(&fixture, &[], replays, &mut servers)
                .await
                .unwrap();
            assert!(retries.is_empty(), "{scene}, {replays}: {retries:#?}");
            let message = difference(&fixture, &actual, &history).expect("the scene differs");
            assert!(!message.contains("RUNNER STALL"), "{scene}: {message}");
        }
    }
}
