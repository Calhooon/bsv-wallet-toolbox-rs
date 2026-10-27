//! The replay's reading of a difference (bsv-wallet-toolbox-rs#194): the
//! ladder's call history and the requests per provider beside every
//! difference, so an archived failure says which rung faulted.

use super::super::toolbox::services::ServiceCallHistory;
use super::*;

/// One rung the ladder asked, as its call history records it.
#[derive(Debug, Clone)]
pub(super) struct Rung {
    /// The fixture's call (from 1) that asked it.
    pub(super) call: usize,
    /// The call history's collection (`getStatusForTxids`, `getRawTx`...).
    pub(super) service: String,
    pub(super) provider: String,
    pub(super) msecs: u64,
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

/// Every call the services' history holds, oldest first.
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

/// The ladder's call history, one rung a line.
pub(super) fn history_text(history: &[Rung]) -> String {
    if history.is_empty() {
        return "call history: none recorded (the header ladders keep none)".to_string();
    }
    let mut text = "call history (each rung asked, in order):".to_string();
    for rung in history {
        text.push_str(&format!("\n  {rung}"));
    }
    text
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

/// What a replay that differs from its fixture prints: the request body
/// difference, the call history, the requests per provider, and both
/// sides. `None` when they agree.
pub(super) fn difference(fixture: &Fixture, actual: &Expected, history: &[Rung]) -> Option<String> {
    if fixture.expected.as_ref() == Some(actual) {
        return None;
    }
    let body_difference = fixture
        .expected
        .as_ref()
        .and_then(|expected| request_body_difference(expected, actual))
        .unwrap_or_default();
    Some(format!(
        "{}\n{body_difference}\n{}\n{}\n--- expected\n{}\n+++ replayed\n{}",
        fixture.about,
        history_text(history),
        requests_text(actual),
        serde_json::to_string_pretty(&fixture.expected).unwrap(),
        serde_json::to_string_pretty(actual).unwrap()
    ))
}

/// A recorded scene: WhatsOnChain's 429 with no Retry-After closes its
/// rung, Bitails answers both calls.
fn recorded_scene() -> Fixture {
    let path =
        fixture_root().join("get_status_for_txids/429_without_retry_after_closes_the_rung.json");
    let fixture: Fixture = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    check_shape(&path, &fixture).unwrap();
    fixture
}

/// A planted mismatch: the message names each rung asked, in order, with
/// its answer or fault, and the requests per provider.
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
}
