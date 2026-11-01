//! The proof path under the header service's Retry-After gate (#550): the
//! `ChainTracker` calls the wallet checks a proof with go through
//! Chaintracks' gate (#148), so a 429 closes the rung for its window and a
//! call inside it makes no request and is could-not-look at the tracker (a
//! fault, never `Ok(false)` or any verdict); a call after the window asks
//! again. Each scene builds its client with `at_clock` on a clock held
//! still (never `from_url`'s process gate keyed by the URL), so the window
//! is the scene's alone and moves only when the scene advances it.

use std::time::Duration;

use bsv_rs::transaction::{ChainTracker, ChainTrackerError};

use super::super::chaintracks_client::ChaintracksServiceClient;
use super::FallbackChainTracker;
use crate::services::providers::pace::Clock;
use crate::services::MockServer;

/// The window each scene's 429 asks for.
const RETRY_AFTER_SECS: u64 = 30;

/// The tracker the wallet builds by default (no break-glass) over a
/// Chaintracks client at `url` whose gate reads `clock`.
fn tracker_at(url: &str, clock: &Clock) -> FallbackChainTracker {
    FallbackChainTracker::new(ChaintracksServiceClient::at_clock(url, clock.clone()))
}

/// `route` answers `status` with `body` (a Retry-After of
/// `RETRY_AFTER_SECS` beside it), and is asked exactly `count` times.
async fn serve(
    server: &mut MockServer,
    route: &str,
    status: usize,
    body: String,
    count: usize,
) -> mockito::Mock {
    server
        .mock("GET", route)
        .with_status(status)
        .with_header("retry-after", &RETRY_AFTER_SECS.to_string())
        .with_body(body)
        .expect(count)
        .create_async()
        .await
}

/// `route` answers 429, asked exactly `count` times.
async fn too_many(server: &mut MockServer, route: &str, count: usize) -> mockito::Mock {
    serve(server, route, 429, "Too Many Requests".into(), count).await
}

/// `route` answers Chaintracks' success frame around `value`, asked
/// exactly `count` times.
async fn answer(
    server: &mut MockServer,
    route: &str,
    value: serde_json::Value,
    count: usize,
) -> mockito::Mock {
    let frame = serde_json::json!({"status": "success", "value": value});
    serve(server, route, 200, frame.to_string(), count).await
}

/// A Chaintracks header at `height` carrying `merkle_root`.
fn header_json(height: u32, merkle_root: &str) -> serde_json::Value {
    serde_json::json!({
        "version": 536_870_912, "previousHash": "0".repeat(64),
        "merkleRoot": merkle_root, "time": 1_700_000_000, "bits": 402_917_821,
        "nonce": 12345, "height": height, "hash": "0".repeat(64)
    })
}

/// Could-not-look: the tracker's network fault naming the closed rung.
fn assert_could_not_look<T: std::fmt::Debug>(got: &Result<T, ChainTrackerError>, when: &str) {
    assert!(
        matches!(got, Err(ChainTrackerError::NetworkError(m)) if m.contains("Rate limited: Chaintracks")),
        "{when}: a 429's window is could-not-look, never a verdict: {got:?}"
    );
}

/// The root check: the 429 closes the rung; inside the window the check
/// asks nothing and cannot look (never `Ok(false)`); after it, the header
/// service is asked again and its answer is the answer.
#[tokio::test]
async fn root_check_inside_the_window_asks_nothing_and_cannot_look() {
    let mut server = crate::services::mock_server().await;
    let clock = Clock::held();
    let tracker = tracker_at(&server.url(), &clock);
    let root = "ab".repeat(32);
    let route = "/findHeaderHexForHeight?height=700";

    let limited = too_many(&mut server, route, 1).await;
    let met = tracker.is_valid_root_for_height(&root, 700).await;
    clock.advance(Duration::from_secs(RETRY_AFTER_SECS - 1));
    let inside = tracker.is_valid_root_for_height(&root, 700).await;
    // The request count before the answers: the call inside the window
    // made none.
    limited.assert_async().await;
    assert_could_not_look(&met, "the 429 itself");
    assert_could_not_look(&inside, "inside the window");
    limited.remove_async().await;

    clock.advance(Duration::from_secs(1));
    let asked = answer(&mut server, route, header_json(700, &root), 1).await;
    let after = tracker.is_valid_root_for_height(&root, 700).await;
    assert!(matches!(after, Ok(true)), "after the window: {after:?}");
    asked.assert_async().await;
}

/// The height read (`current_height`, `/getPresentHeight`): the same
/// window, the same could-not-look, asked again after it.
#[tokio::test]
async fn height_read_inside_the_window_asks_nothing_and_cannot_look() {
    let mut server = crate::services::mock_server().await;
    let clock = Clock::held();
    let tracker = tracker_at(&server.url(), &clock);
    let route = "/getPresentHeight";

    let limited = too_many(&mut server, route, 1).await;
    let met = tracker.current_height().await;
    clock.advance(Duration::from_secs(RETRY_AFTER_SECS - 1));
    let inside = tracker.current_height().await;
    limited.assert_async().await;
    assert_could_not_look(&met, "the 429 itself");
    assert_could_not_look(&inside, "inside the window");
    limited.remove_async().await;

    clock.advance(Duration::from_secs(1));
    let asked = answer(&mut server, route, serde_json::json!(900_000), 1).await;
    let after = tracker.current_height().await;
    assert!(matches!(after, Ok(900_000)), "after the window: {after:?}");
    asked.assert_async().await;
}

/// The tip read (`mining_tip`: `/getChain` once per client, then the tip
/// header and the eleven-header range): a 429 on any of its three calls
/// closes the rung, and inside the window no call of the read makes a
/// request; the read is could-not-look (`NetworkError`, the class of the
/// root check and the height read, #554), never a tip and never
/// `Ok(None)`. After the window the read asks again.
#[tokio::test]
async fn tip_read_inside_the_window_asks_nothing_and_is_a_fault() {
    let tip_route = "/findChainTipHeaderHex";
    let range_route = "/getHeaders?height=900000&count=11";
    // The route that answers 429; each route before it answers.
    for limited_route in ["/getChain", tip_route, range_route] {
        let mut server = crate::services::mock_server().await;
        let clock = Clock::held();
        let tracker = tracker_at(&server.url(), &clock);
        let tip = header_json(900_010, &"11".repeat(32));

        // The routes before the limited one answer: the network once (it is
        // kept), the tip header on the read before the window and after it.
        let network = match limited_route {
            "/getChain" => None,
            _ => Some(answer(&mut server, "/getChain", serde_json::json!("main"), 1).await),
        };
        let tip_read = match limited_route {
            r if r == range_route => Some(answer(&mut server, tip_route, tip, 2).await),
            _ => None,
        };
        let limited = too_many(&mut server, limited_route, 1).await;

        let is_a_fault = |got: &Result<_, ChainTrackerError>, when: &str| {
            assert_could_not_look(got, &format!("{limited_route} {when}"));
        };
        let met = tracker.mining_tip().await;
        clock.advance(Duration::from_secs(RETRY_AFTER_SECS - 1));
        let inside = tracker.mining_tip().await;
        limited.assert_async().await;
        is_a_fault(&met, "the 429 itself");
        is_a_fault(&inside, "inside the window");
        limited.remove_async().await;

        // After the window the read asks again: the limited route once
        // more (the network is kept once read).
        clock.advance(Duration::from_secs(1));
        let again = too_many(&mut server, limited_route, 1).await;
        let after = tracker.mining_tip().await;
        again.assert_async().await;
        is_a_fault(&after, "after the window");
        for answered in network.iter().chain(tip_read.iter()) {
            answered.assert_async().await;
        }
    }
}

/// A fault in what the service answered is not could-not-look (#554): a
/// range the client refuses (header bytes that are not hex) keeps the tip
/// read's `Other`, its text naming the refusal.
#[tokio::test]
async fn tip_read_over_a_refused_range_is_other() {
    let mut server = crate::services::mock_server().await;
    let clock = Clock::held();
    let tracker = tracker_at(&server.url(), &clock);
    let network = answer(&mut server, "/getChain", serde_json::json!("main"), 1).await;
    let tip = header_json(900_010, &"11".repeat(32));
    let tip_read = answer(&mut server, "/findChainTipHeaderHex", tip, 1).await;
    let range_route = "/getHeaders?height=900000&count=11";
    let range = answer(&mut server, range_route, serde_json::json!("zz"), 1).await;

    let got = tracker.mining_tip().await;
    assert!(
        matches!(&got, Err(ChainTrackerError::Other(m)) if m.contains("invalid header bytes")),
        "a refused range is the client's Other: {got:?}"
    );
    network.assert_async().await;
    tip_read.assert_async().await;
    range.assert_async().await;
}

/// The client's own root check (its `ChainTracker` impl, beneath the
/// fallback tracker's) maps the window as the tip read does (#554):
/// could-not-look, `NetworkError`, the text unchanged.
#[tokio::test]
async fn client_root_check_inside_the_window_cannot_look() {
    let mut server = crate::services::mock_server().await;
    let clock = Clock::held();
    let client = ChaintracksServiceClient::at_clock(&server.url(), clock.clone());
    let root = "ab".repeat(32);
    let route = "/findHeaderHexForHeight?height=700";

    let limited = too_many(&mut server, route, 1).await;
    let met = ChainTracker::is_valid_root_for_height(&client, &root, 700).await;
    clock.advance(Duration::from_secs(RETRY_AFTER_SECS - 1));
    let inside = ChainTracker::is_valid_root_for_height(&client, &root, 700).await;
    limited.assert_async().await;
    assert_could_not_look(&met, "the 429 itself");
    assert_could_not_look(&inside, "inside the window");
}
