//! The services ladders, replayed from fixtures (bsv-wallet-toolbox-rs#53).
//!
//! Each file `tests/fixtures/data/ladders/<ladder>/<name>.json` is one
//! scene: the ladder; the providers configured, in the ladder's order, with
//! each one's answers per route as a mockito script; the calls; and
//! `expected`, each call's result (normalized) and the requests that
//! reached each provider, by route, over all the calls. POST request bodies
//! are parsed JSON with sorted keys and the same txid tokens as the calls;
//! their order and values must match too. The replay stands a
//! local server in each provider's place, builds `Services` over them,
//! makes the calls and compares. Nothing touches the network.
//!
//! WhatsOnChain and Bitails are in every `Services`, so every fixture names
//! them (with no routes where the ladder never asks them); BananaBlocks,
//! Chaintracks and BHS are configured only when named.
//!
//! A route is `"<METHOD> <path and query>"`. An answer is a `status`,
//! `headers`, and a `body` (text) or `json`. `times` (default 1) is how many
//! requests it serves, in order; the route's last answer serves every
//! request after. `delay_ms` holds the answer back past the explorers'
//! client timeout ([`CLIENT_TIMEOUT_SECS`]): a timeout. `cut` sends the body and
//! drops the connection before the end: a body cut short, its head always
//! out before the fault, in the order `ladder_cut.rs` settles without a
//! clock (#479). A request no route scripts is answered 501 and counted as
//! `UNSCRIPTED <method> <path>`, so a request the fixture did not expect is
//! a difference too.
//!
//! The results name each local server `<Provider>`; a fixture never holds
//! a port. The results lose the wall clock: merkle path notes their `when`,
//! and every call after the earliest that says `retry after` its remainder
//! (`retry after <n>s`, the seconds a closed rung has left, which tick on a
//! slow runner); the call that received the 429 stays exact. The host's
//! `services_at` holds the explorers' rate gates on one clock that does not
//! move during a scene, so a rung a 429 closed for its default second is
//! still closed for the scene's next call however slow the runner. The
//! boundary test advances that same clock, without sleeping, to prove the
//! status rung stays closed at 999 ms and opens at 1000 ms (#194).
//! `LADDER_FIXTURES_BLESS=1` writes each fixture's `expected` from
//! the replay instead of comparing: read the diff against the rules
//! `services.rs` documents (a fault is "could not look", a negative needs
//! two clean answers, a closed rung costs no request), never take it as
//! the rule.
//!
//! A difference prints the ladder's call history (each rung asked, per
//! call, in order, with its answer or fault and how long it took) and the
//! requests per provider; a difference that is the runner's, not the
//! ladder's, is a runner stall, replayed as many extra times as `LADDER_FIXTURES_STALL_REPLAYS` allows, one when unset (`ladder_stall.rs`).
//!
//! The host gives two names: `toolbox` (the crate) and `services_at`.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::services_at;
use super::toolbox::services::traits::{GetUtxoStatusOutputFormat, WalletServices};
use super::toolbox::services::{Services, ServicesOptions};

#[path = "ladder_stall.rs"]
mod stall;

#[path = "ladder_pool.rs"]
mod pool;

#[path = "ladder_lock.rs"]
mod lock;

#[path = "ladder_arcade.rs"]
mod arcade;

#[path = "ladder_cut.rs"]
mod cut;

/// The explorers' client timeout in a replay: a `delay_ms` past it is a
/// timeout.
const CLIENT_TIMEOUT_SECS: u64 = 2;

/// How many fixtures replay at once, each worker on servers of its own.
const WORKERS: usize = 8;

/// Every provider a fixture can name, in the order the servers are built
/// (`ArcadeV2` only in an Arcade scene, `ladder_arcade.rs`).
const PROVIDERS: [&str; 6] = [
    "WhatsOnChain",
    "BananaBlocks",
    "Bitails",
    "Chaintracks",
    "BHS",
    "ArcadeV2",
];

/// The ladders, each with its providers in the order the ladder asks them.
const LADDERS: [(&str, &[&str]); 9] = [
    ("get_raw_tx", &["WhatsOnChain", "BananaBlocks", "Bitails"]),
    (
        "get_merkle_path",
        &["WhatsOnChain", "BananaBlocks", "Bitails", "Chaintracks"],
    ),
    (
        "get_utxo_status",
        &["WhatsOnChain", "BananaBlocks", "Bitails"],
    ),
    (
        "get_status_for_txids",
        &["WhatsOnChain", "Bitails", "BananaBlocks"],
    ),
    (
        "is_utxo",
        &["WhatsOnChain", "BananaBlocks", "Bitails", "Chaintracks"],
    ),
    (
        "is_utxo_with_spender",
        &["WhatsOnChain", "BananaBlocks", "Bitails", "Chaintracks"],
    ),
    (
        "get_utxo_leads",
        &["WhatsOnChain", "Bitails", "BananaBlocks"],
    ),
    (
        "get_header_for_height",
        &["Chaintracks", "BHS", "WhatsOnChain", "Bitails"],
    ),
    (
        "get_chain_tip_header",
        &["Chaintracks", "BHS", "WhatsOnChain", "Bitails"],
    ),
];

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    about: String,
    ladder: String,
    providers: Vec<Provider>,
    calls: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected: Option<Expected>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    name: String,
    routes: Vec<Route>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Route {
    route: String,
    answers: Vec<Answer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Answer {
    status: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    json: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    times: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delay_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    cut: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    results: Vec<Value>,
    requests: BTreeMap<String, BTreeMap<String, usize>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    request_bodies: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
}

/// Requests seen, by provider and route, including each POST body in order.
#[derive(Default)]
struct Requests {
    counts: BTreeMap<String, BTreeMap<String, usize>>,
    bodies: BTreeMap<String, BTreeMap<String, Vec<Value>>>,
    errors: Vec<String>,
}

type Counts = Arc<Mutex<Requests>>;

fn record(counts: &Counts, provider: &str, route: String, req: &mockito::Request) {
    let mut requests = counts.lock().unwrap();
    *requests
        .counts
        .entry(provider.to_string())
        .or_default()
        .entry(route.clone())
        .or_default() += 1;
    if req.method() == "POST" {
        let body = req
            .body()
            .map_err(|e| e.to_string())
            .and_then(|bytes| serde_json::from_slice::<Value>(bytes).map_err(|e| e.to_string()));
        match body {
            Ok(mut body) => {
                // Calls already use the fixture's txid tokens: keep their
                // values and array order, sorting only object keys.
                body.sort_all_objects();
                requests
                    .bodies
                    .entry(provider.to_string())
                    .or_default()
                    .entry(route)
                    .or_default()
                    .push(body);
            }
            Err(e) => requests
                .errors
                .push(format!("{provider} {route}: request body is not JSON: {e}")),
        }
    }
}

/// Locate a differing key or array position, including missing/extra keys.
fn json_difference(expected: &Value, actual: &Value) -> Option<String> {
    match (expected, actual) {
        (Value::Object(a), Value::Object(b)) => {
            for key in a.keys().chain(b.keys()) {
                let difference = match (a.get(key), b.get(key)) {
                    (Some(a), Some(b)) => json_difference(a, b),
                    _ => Some(String::new()),
                };
                if let Some(difference) = difference {
                    return Some(format!("/{key}{difference}"));
                }
            }
            None
        }
        (Value::Array(a), Value::Array(b)) => {
            for (index, (a, b)) in a.iter().zip(b).enumerate() {
                if let Some(difference) = json_difference(a, b) {
                    return Some(format!("/{index}{difference}"));
                }
            }
            (a.len() != b.len()).then(|| "/length".to_string())
        }
        _ => (expected != actual).then(String::new),
    }
}

fn request_body_difference(expected: &Expected, actual: &Expected) -> Option<String> {
    for (provider, routes) in expected.request_bodies.iter().chain(&actual.request_bodies) {
        for route in routes.keys() {
            let bodies = |expected: &Expected| {
                serde_json::to_value(
                    expected
                        .request_bodies
                        .get(provider)
                        .and_then(|routes| routes.get(route)),
                )
                .unwrap()
            };
            let (expected, actual) = (bodies(expected), bodies(actual));
            if let Some(mut key) = json_difference(&expected, &actual) {
                if key.is_empty() {
                    key.push('/');
                }
                return Some(format!(
                    "{provider} {route}: request body differs at {key}: expected {expected}, replayed {actual}"
                ));
            }
        }
    }
    None
}

#[tokio::test]
async fn ladder_fixtures_post_bodies_are_normalized() {
    let mut server = mockito::Server::new_async().await;
    let counts = Counts::default();
    let answer = serde_json::from_value(json!({ "status": 200, "times": 2 })).unwrap();
    let mock = mock_answer(
        &mut server,
        "WhatsOnChain",
        "POST",
        "/other",
        "POST /other",
        &answer,
        &counts,
    )
    .unwrap();
    let client = reqwest::Client::new();
    for body in [
        r#"{"z":{"b":2,"a":1},"txids":["b","a"]}"#,
        r#"{ "txids": ["b", "a"], "z": {"a": 1, "b": 2} }"#,
    ] {
        let response = client
            .post(server.url() + "/other")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    mock.assert_async().await;
    let recorded = counts.lock().unwrap();
    assert!(recorded.errors.is_empty());
    assert_eq!(recorded.counts["WhatsOnChain"]["POST /other"], 2);
    let bodies = &recorded.bodies["WhatsOnChain"]["POST /other"];
    assert_eq!(bodies.len(), 2);
    for body in bodies {
        assert_eq!(
            serde_json::to_string(body).unwrap(),
            r#"{"txids":["b","a"],"z":{"a":1,"b":2}}"#
        );
    }
}

/// The seconds a closed rung has left, as an error's text gives them.
const RETRY_AFTER: &str = "retry after ";

/// Every `retry after <digits>s` in `text` with its digits masked.
fn mask_remainder(text: &str) -> String {
    let mut masked = String::new();
    let mut rest = text;
    while let Some(at) = rest.find(RETRY_AFTER) {
        let (head, tail) = rest.split_at(at + RETRY_AFTER.len());
        masked.push_str(head);
        let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0 && tail[digits..].starts_with('s') {
            masked.push_str("<n>s");
            rest = &tail[digits + 1..];
        } else {
            rest = tail;
        }
    }
    masked.push_str(rest);
    masked
}

/// A Retry-After remainder is wall-clock: a call made inside a window an
/// earlier call's 429 opened reports the seconds left, and on a slow runner
/// a second ticks between the calls (30s, then 29s). So every call after
/// the earliest one whose result says `retry after` has its `retry after
/// <digits>s` masked to `retry after <n>s`; the call that received the 429
/// reports the header's own value and stays exact. Nothing else is masked.
fn mask_retry_after_remainders(results: &mut [Value]) {
    let says = |result: &Value| result.to_string().contains(RETRY_AFTER);
    let Some(earliest) = results.iter().position(says) else {
        return;
    };
    for result in &mut results[earliest + 1..] {
        *result = serde_json::from_str(&mask_remainder(&result.to_string())).unwrap();
    }
}

#[test]
fn ladder_fixtures_mask_only_a_later_calls_remainder() {
    let note =
        |secs: &str| json!({ "error": format!("Rate limited: WhatsOnChain: retry after {secs}") });
    let mut expected = vec![note("30s"), note("30s")];
    let mut replayed = vec![note("30s"), note("29s")];
    mask_retry_after_remainders(&mut expected);
    mask_retry_after_remainders(&mut replayed);
    assert_eq!(expected, replayed);
    assert_eq!(replayed, vec![note("30s"), note("<n>s")]);

    // The call that received the 429 stays exact.
    let mut replayed = vec![note("29s"), note("29s")];
    mask_retry_after_remainders(&mut replayed);
    assert_ne!(expected, replayed);

    // Only `retry after <digits>s` is masked.
    assert_eq!(
        mask_remainder("retry after 7s; retry after soon; retry after 12x; retry after 3s"),
        "retry after <n>s; retry after soon; retry after 12x; retry after <n>s"
    );
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/data/ladders")
}

/// Every fixture file, sorted, so a replay's report reads the same twice.
fn fixture_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    for dir in std::fs::read_dir(fixture_root()).expect("the ladder fixtures") {
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

/// The fixture's shape: a known ladder in its own directory, known
/// providers in the ladder's order, WhatsOnChain and Bitails named.
fn check_shape(path: &Path, fixture: &Fixture) -> Result<(), String> {
    let order = LADDERS
        .iter()
        .find(|(name, _)| *name == fixture.ladder)
        .map(|(_, order)| *order)
        .ok_or_else(|| format!("unknown ladder {:?}", fixture.ladder))?;
    let dir = path.parent().and_then(Path::file_name).unwrap_or_default();
    if dir != fixture.ladder.as_str() {
        return Err(format!("ladder {:?} filed under {:?}", fixture.ladder, dir));
    }
    let named: Vec<&str> = fixture.providers.iter().map(|p| p.name.as_str()).collect();
    let in_order: Vec<&str> = order
        .iter()
        .copied()
        .filter(|n| named.contains(n))
        .collect();
    if named != in_order {
        return Err(format!(
            "providers {named:?}: the ladder's order is {order:?}, each once"
        ));
    }
    for required in ["WhatsOnChain", "Bitails"] {
        if !named.contains(&required) {
            return Err(format!("{required} is in every Services: name it"));
        }
    }
    if fixture.calls.is_empty() {
        return Err("no calls".to_string());
    }
    Ok(())
}

/// One worker's servers, one per provider, reset between fixtures: off the
/// runtime's workers (`lock::script`), and dropped off them too.
struct Servers(Vec<(&'static str, mockito::Server)>);

impl Servers {
    async fn new() -> Self {
        let mut servers = Vec::new();
        for name in PROVIDERS {
            // Outside mockito's pool: a worker holds five at once.
            let server = mockito::Server::new_with_opts_async(mockito::ServerOpts::default()).await;
            servers.push((name, server));
        }
        lock::record_servers_created();
        Servers(servers)
    }

    fn url(&self, provider: &str) -> String {
        self.0
            .iter()
            .find(|(name, _)| *name == provider)
            .map(|(_, server)| server.url())
            .unwrap()
    }
}

/// Stand up the providers' script on the servers; the mocks returned must
/// live through the calls. Each server's lock is taken here, so this runs
/// on a blocking thread (`lock::script`), never on a runtime worker.
fn script_blocking(
    providers: &[Provider],
    servers: &mut [(&'static str, mockito::Server)],
    counts: &Counts,
) -> Result<Vec<mockito::Mock>, String> {
    lock::record_script_locks();
    let mut mocks = Vec::new();
    for (name, server) in servers.iter_mut() {
        server.reset();
        // Made before the routes, wanting no request: it answers only what
        // no route scripts (mockito takes the earliest match still owed a
        // request, else the latest match).
        for method in ["GET", "POST"] {
            let seen = Arc::clone(counts);
            let provider = name.to_string();
            let mock = server
                .mock(method, mockito::Matcher::Any)
                .with_status_code_from_request(move |req| {
                    let route = format!("UNSCRIPTED {} {}", req.method(), req.path_and_query());
                    record(&seen, &provider, route, req);
                    501
                })
                .expect(0)
                .create();
            mocks.push(mock);
        }
        let Some(provider) = providers.iter().find(|p| p.name == *name) else {
            continue;
        };
        for route in &provider.routes {
            let (method, path) = route
                .route
                .split_once(' ')
                .ok_or_else(|| format!("route {:?} is not \"METHOD path\"", route.route))?;
            if route.answers.is_empty() {
                return Err(format!("route {:?} has no answers", route.route));
            }
            for answer in &route.answers {
                mocks.push(mock_answer(
                    server,
                    name,
                    method,
                    path,
                    &route.route,
                    answer,
                    counts,
                )?);
            }
        }
    }
    Ok(mocks)
}

fn mock_answer(
    server: &mut mockito::Server,
    provider: &str,
    method: &str,
    path: &str,
    route: &str,
    answer: &Answer,
    counts: &Counts,
) -> Result<mockito::Mock, String> {
    let body: Vec<u8> = match (&answer.body, &answer.json) {
        (Some(_), Some(_)) => return Err(format!("{route}: a body or json, not both")),
        (Some(text), None) => text.clone().into_bytes(),
        (None, Some(value)) => serde_json::to_vec(value).unwrap(),
        (None, None) => Vec::new(),
    };
    let seen = Arc::clone(counts);
    let (provider, key, status) = (provider.to_string(), route.to_string(), answer.status);
    // A delay holds the status line back: the client sees no answer at all.
    let delay = answer.delay_ms.map(Duration::from_millis);
    let mut mock = server
        .mock(method, path)
        .with_status_code_from_request(move |req| {
            record(&seen, &provider, key.clone(), req);
            if let Some(delay) = delay {
                std::thread::sleep(delay);
            }
            status
        })
        .expect(answer.times.unwrap_or(1));
    for (name, value) in &answer.headers {
        mock = mock.with_header(name.as_str(), value);
    }
    mock = if answer.cut {
        mock.with_chunked_body(move |w| cut::cut_short(w, &body))
    } else {
        mock.with_body(body)
    };
    Ok(mock.create())
}

fn arg<'a>(call: &'a Value, key: &str) -> Result<&'a Value, String> {
    call.get(key)
        .ok_or_else(|| format!("call {call} has no {key:?}"))
}

fn str_arg<'a>(call: &'a Value, key: &str) -> Result<&'a str, String> {
    arg(call, key)?
        .as_str()
        .ok_or_else(|| format!("{key:?} is not a string"))
}

fn use_next(call: &Value) -> bool {
    call.get("use_next")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn format_arg(call: &Value) -> Result<Option<GetUtxoStatusOutputFormat>, String> {
    Ok(match call.get("format").and_then(Value::as_str) {
        None => None,
        Some("hashLE") => Some(GetUtxoStatusOutputFormat::HashLE),
        Some("hashBE") => Some(GetUtxoStatusOutputFormat::HashBE),
        Some("script") => Some(GetUtxoStatusOutputFormat::Script),
        Some(other) => return Err(format!("unknown format {other:?}")),
    })
}

/// A call's outcome: `{"ok": <result>}` or `{"err": "<error>"}`.
fn outcome<T: Serialize>(result: super::toolbox::Result<T>) -> Value {
    match result {
        Ok(value) => json!({ "ok": serde_json::to_value(value).unwrap() }),
        Err(e) => json!({ "err": e.to_string() }),
    }
}

/// One call of the ladder, its result normalized: merkle path notes lose
/// their clock (`when`).
async fn call_ladder(services: &Services, ladder: &str, call: &Value) -> Result<Value, String> {
    Ok(match ladder {
        "get_raw_tx" => outcome(
            services
                .get_raw_tx(str_arg(call, "txid")?, use_next(call))
                .await,
        ),
        "get_merkle_path" => {
            let mut result = outcome(
                services
                    .get_merkle_path(str_arg(call, "txid")?, use_next(call))
                    .await,
            );
            if let Some(notes) = result
                .pointer_mut("/ok/notes")
                .and_then(Value::as_array_mut)
            {
                for note in notes {
                    if let Some(note) = note.as_object_mut() {
                        note.remove("when");
                    }
                }
            }
            result
        }
        "get_utxo_status" => outcome(
            services
                .get_utxo_status(
                    str_arg(call, "output")?,
                    format_arg(call)?,
                    call.get("outpoint").and_then(Value::as_str),
                    use_next(call),
                )
                .await,
        ),
        "get_status_for_txids" => {
            let txids: Vec<String> = serde_json::from_value(arg(call, "txids")?.clone())
                .map_err(|e| format!("txids: {e}"))?;
            outcome(services.get_status_for_txids(&txids, use_next(call)).await)
        }
        "is_utxo" | "is_utxo_with_spender" => {
            let txid = str_arg(call, "txid")?;
            let vout = arg(call, "vout")?.as_u64().ok_or("vout is not a number")? as u32;
            let script = hex::decode(str_arg(call, "locking_script")?)
                .map_err(|e| format!("locking_script: {e}"))?;
            if ladder == "is_utxo" {
                json!({ "verdict": format!("{:?}", services.is_utxo(txid, vout, &script).await) })
            } else {
                let (verdict, spender) = services.is_utxo_with_spender(txid, vout, &script).await;
                json!({
                    "verdict": format!("{verdict:?}"),
                    "spender": spender.map(|s| json!({ "txid": s.txid, "confirmed": s.confirmed })),
                })
            }
        }
        "get_utxo_leads" => outcome(
            services
                .get_utxo_leads(str_arg(call, "output")?, format_arg(call)?)
                .await,
        ),
        "get_header_for_height" => {
            let height = arg(call, "height")?
                .as_u64()
                .ok_or("height is not a number")? as u32;
            outcome(
                services
                    .get_header_for_height(height)
                    .await
                    .map(hex::encode),
            )
        }
        "get_chain_tip_header" => outcome(services.get_chain_tip_header().await),
        other => return Err(format!("unknown ladder {other:?}")),
    })
}

/// Replay one fixture on a worker's servers: what it recorded and the
/// ladder's call history.
async fn replay(
    fixture: &Fixture,
    servers: &mut Servers,
) -> Result<(Expected, Vec<stall::Rung>), String> {
    let counts: Counts = Arc::default();
    let _mocks = lock::script(fixture, servers, &counts).await?;
    let named = |name: &str| fixture.providers.iter().any(|p| p.name == name);
    let mut options = arcade::options(fixture, servers);
    if named("Chaintracks") {
        options = options.with_chaintracks_url(servers.url("Chaintracks"));
    }
    if named("BHS") {
        options = options.with_bhs_url(servers.url("BHS"));
    }
    let bananablocks = named("BananaBlocks").then(|| servers.url("BananaBlocks"));
    let (services, _clock) = services_at(
        options,
        &servers.url("WhatsOnChain"),
        &servers.url("Bitails"),
        bananablocks.as_deref(),
        CLIENT_TIMEOUT_SECS,
    );

    let mut results = Vec::new();
    let mut history = Vec::new();
    for (index, call) in fixture.calls.iter().enumerate() {
        results.push(call_ladder(&services, &fixture.ladder, call).await?);
        stall::record_history(&services, index + 1, &mut history);
    }
    mask_retry_after_remainders(&mut results);
    let (requests, request_bodies) = {
        let recorded = counts.lock().unwrap();
        if !recorded.errors.is_empty() {
            return Err(recorded.errors.join("\n"));
        }
        (recorded.counts.clone(), recorded.bodies.clone())
    };

    // Each server's address becomes its provider's name.
    let mut text = serde_json::to_string(&results).unwrap();
    for name in PROVIDERS {
        let url = servers.url(name);
        text = text.replace(&url, &format!("<{name}>"));
        text = text.replace(url.trim_start_matches("http://"), &format!("<{name}>"));
    }
    if text.contains("127.0.0.1") || text.contains("localhost") {
        return Err(format!("a local address survived normalizing: {text}"));
    }
    stall::normalize(&mut history, servers);
    let actual = Expected {
        results: serde_json::from_str(&text).unwrap(),
        requests,
        request_bodies,
    };
    Ok((actual, history))
}

/// Replay the recorded status scene before the default deadline, then ask
/// again at the deadline. Only the returned clock moves the window.
#[tokio::test]
async fn ladder_status_429_without_retry_after_uses_the_controlled_clock() {
    let path =
        fixture_root().join("get_status_for_txids/429_without_retry_after_closes_the_rung.json");
    let fixture: Fixture = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let expected = fixture.expected.as_ref().unwrap();
    assert_eq!(fixture.calls.len(), 2);
    let mut servers = Servers::new().await;
    let counts: Counts = Arc::default();
    let _mocks = lock::script(&fixture, &mut servers, &counts).await.unwrap();
    let (services, clock) = services_at(
        arcade::classic_options(),
        &servers.url("WhatsOnChain"),
        &servers.url("Bitails"),
        Some(&servers.url("BananaBlocks")),
        CLIENT_TIMEOUT_SECS,
    );
    assert!(services.whatsonchain.rate_gate().admit().is_ok());
    for (index, call) in fixture.calls.iter().enumerate() {
        if index > 0 {
            clock.advance(Duration::from_millis(999));
        }
        let actual = call_ladder(&services, &fixture.ladder, call).await.unwrap();
        assert_eq!(actual, expected.results[index]);
    }
    assert!(matches!(
        services.whatsonchain.rate_gate().admit(),
        Err(super::toolbox::Error::RateLimited {
            retry_after_secs: 1,
            ..
        })
    ));
    {
        let recorded = counts.lock().unwrap();
        assert!(recorded.errors.is_empty());
        assert_eq!(recorded.counts, expected.requests);
        assert_eq!(recorded.bodies, expected.request_bodies);
    }

    clock.advance(Duration::from_millis(1));
    assert!(
        services.whatsonchain.rate_gate().admit().is_ok(),
        "the status rung opens at 1000 ms on the controlled clock"
    );
    let actual = call_ladder(&services, &fixture.ladder, &fixture.calls[0])
        .await
        .unwrap();
    assert_eq!(actual, expected.results[0]);
    let recorded = counts.lock().unwrap();
    assert!(recorded.errors.is_empty());
    assert_eq!(recorded.counts["WhatsOnChain"]["POST /txs/status"], 2);
    assert_eq!(recorded.bodies["WhatsOnChain"]["POST /txs/status"].len(), 2);
}

/// A fixture file as written: two-space JSON and a final newline.
fn fixture_text(fixture: &Fixture) -> String {
    let mut text = serde_json::to_string_pretty(fixture).unwrap();
    text.push('\n');
    text
}

/// Every ladder fixture replayed: each call's result and each provider's
/// requests by route are the fixture's `expected`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_fixtures_replay() {
    let bless = std::env::var_os("LADDER_FIXTURES_BLESS").is_some_and(|v| v == "1");
    let files = fixture_files();
    assert!(
        !files.is_empty(),
        "no ladder fixtures under {:?}",
        fixture_root()
    );
    let scenes = files.into_iter().map(pool::load).collect();
    let replayed = pool::replay_queue(scenes, WORKERS).await;

    let mut failures = Vec::new();
    let total = replayed.len();
    for (path, outcome) in replayed {
        let name = path
            .strip_prefix(fixture_root())
            .unwrap_or(&path)
            .display()
            .to_string();
        match outcome {
            Err(e) => failures.push(format!("{name}: {e}")),
            Ok((mut fixture, (actual, history))) => {
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
        "{} of {total} ladder fixtures differ:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
