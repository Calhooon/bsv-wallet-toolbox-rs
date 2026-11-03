//! The loopback table (#683). One test per rung class drives a provider
//! through the table-backed client at its production host and asserts the
//! answer and the call history; the texts tests hold the texts a host sees
//! through the table to the socket transport's for the same answer, on one
//! mock server's host and port, and the send fault to the socket's own
//! faults (a refused connection, a timeout, a TLS and a DNS fault).

use std::sync::Arc as StdArc;
use std::time::Duration;

use super::{LoopbackAnswer, LoopbackTable};
use crate::services::providers::http_client::Client;
use crate::services::providers::{BananaBlocks, Bitails, WhatsOnChain};
use crate::services::{
    mock_server, Chain, ServiceCallHistory, Services, ServicesOptions, WalletServices,
};

const WOC: &str = "api.whatsonchain.com";
const BANANABLOCKS: &str = "bananablocks.com";
const BITAILS: &str = "api.bitails.io";

/// A transaction spending `source:vout` to one 900-sat output (`OP_TRUE`),
/// written byte by byte: its txid and its hex.
fn spending(source: &str, vout: u32) -> (String, String) {
    let mut outpoint = hex::decode(source).unwrap();
    outpoint.reverse();
    let hex = [
        "01000000".to_string(),
        format!(
            "01{}{}",
            hex::encode(outpoint),
            hex::encode(vout.to_le_bytes())
        ),
        "0100ffffffff".to_string(),
        format!("01{}0151", hex::encode(900u64.to_le_bytes())),
        "00000000".to_string(),
    ]
    .concat();
    let txid = bsv_rs::transaction::Transaction::from_hex(&hex)
        .unwrap()
        .id();
    (txid, hex)
}

fn a_tx(vout: u32) -> (String, String) {
    spending(&"11".repeat(32), vout)
}

/// A header service's frame around the tip header at `height`.
fn tip_frame(height: u32) -> serde_json::Value {
    serde_json::json!({
        "status": "success",
        "value": {
            "version": 536_870_912u32,
            "previousHash": "0".repeat(64),
            "merkleRoot": "a".repeat(64),
            "time": 1_700_000_000u32,
            "bits": 402_917_821u32,
            "nonce": 7u32,
            "height": height,
            "hash": "b".repeat(64),
        }
    })
}

/// The production ladders (mainnet hosts), every client on the table.
fn production(table: &StdArc<LoopbackTable>, options: ServicesOptions) -> Services {
    table.building(|| Services::with_options(Chain::Main, options).unwrap())
}

/// Each provider's recorded calls in a ladder's history: (provider, success).
fn rungs(history: &ServiceCallHistory) -> Vec<(String, bool)> {
    let mut rungs: Vec<(String, bool)> = history
        .history_by_provider
        .iter()
        .flat_map(|(name, h)| h.calls.iter().map(move |c| (name.clone(), c.success)))
        .collect();
    rungs.sort();
    rungs
}

#[tokio::test]
async fn the_whatsonchain_rung_answers_from_the_table() {
    let (txid, hex) = a_tx(0);
    let table = LoopbackTable::new();
    table.on(
        "GET",
        WOC,
        &format!("/v1/bsv/main/tx/{txid}/hex"),
        LoopbackAnswer::status(200, hex.clone()),
    );
    let services = production(&table, ServicesOptions::mainnet());
    let result = services.get_raw_tx(&txid, false).await.unwrap();
    assert_eq!(result.raw_tx, Some(hex::decode(&hex).unwrap()));
    assert_eq!(result.name, "WoC");
    let history = services.get_services_call_history(false).unwrap();
    assert_eq!(
        rungs(&history.get_raw_tx.unwrap()),
        [("WhatsOnChain".to_string(), true)]
    );
    assert_eq!(
        table.requests(),
        [format!("GET https://{WOC}/v1/bsv/main/tx/{txid}/hex")]
    );
}

/// `WhatsOnChain` and `BananaBlocks` unmatched (the socket's send fault),
/// so the ladder fails over to `Bitails` within the same call.
#[tokio::test]
async fn the_bitails_rung_answers_after_two_send_faults() {
    let (txid, hex) = a_tx(1);
    let table = LoopbackTable::new();
    table.on(
        "GET",
        BITAILS,
        &format!("/tx/{txid}/hex"),
        LoopbackAnswer::status(200, hex.clone()),
    );
    let services = production(&table, ServicesOptions::mainnet());
    let result = services.get_raw_tx(&txid, false).await.unwrap();
    assert_eq!(result.raw_tx, Some(hex::decode(&hex).unwrap()));
    assert_eq!(result.name, "Bitails");
    let history = services.get_services_call_history(false).unwrap();
    assert_eq!(
        rungs(&history.get_raw_tx.unwrap()),
        [
            ("BananaBlocks".to_string(), false),
            ("Bitails".to_string(), true),
            ("WhatsOnChain".to_string(), false),
        ]
    );
    assert_eq!(
        table.requests(),
        [
            format!("GET https://{WOC}/v1/bsv/main/tx/{txid}/hex"),
            format!("GET https://{BANANABLOCKS}/api/v1/tx/{txid}/hex"),
            format!("GET https://{BITAILS}/tx/{txid}/hex"),
        ]
    );
}

/// A BEEF of a proven parent and its child (no keys, no signature).
fn a_beef() -> (Vec<u8>, String) {
    use bsv_rs::transaction::{Beef, MerklePath};
    let (parent_txid, parent) = a_tx(2);
    let (txid, child) = spending(&parent_txid, 0);
    let mut beef = Beef::new();
    let bump = MerklePath::from_coinbase_txid(&parent_txid, 100_000);
    let proof = beef.merge_bump(bump);
    beef.merge_raw_tx(hex::decode(parent).unwrap(), Some(proof));
    beef.merge_raw_tx(hex::decode(child).unwrap(), None);
    (beef.to_binary(), txid)
}

#[tokio::test]
#[allow(deprecated)]
async fn the_arc_rung_accepts_from_the_table() {
    let (beef, txid) = a_beef();
    let table = LoopbackTable::new();
    table.on(
        "POST",
        "arc.test",
        "/v1/tx",
        LoopbackAnswer::json(
            200,
            &serde_json::json!({"txid": txid, "txStatus": "SEEN_ON_NETWORK"}),
        ),
    );
    let options = ServicesOptions::mainnet().with_classic_arc("https://arc.test", None);
    let services = production(&table, options);
    let results = services
        .post_beef(&beef, std::slice::from_ref(&txid))
        .await
        .unwrap();
    assert!(results[0].is_success(), "{results:?}");
    assert_eq!(results[0].txid_results[0].txid, txid);
    // PromiseAll, the default: every rung is asked; Bitails and
    // WhatsOnChain are unmatched, the socket's send fault.
    let history = services.get_services_call_history(false).unwrap();
    assert_eq!(
        rungs(&history.post_beef.unwrap()),
        [
            ("Bitails".to_string(), false),
            ("TaalArcBeef".to_string(), true),
            ("WhatsOnChain".to_string(), false),
        ]
    );
    let mut requests = table.requests();
    requests.sort();
    assert_eq!(
        requests,
        [
            format!("POST https://{BITAILS}/tx/broadcast/multi"),
            format!("POST https://{WOC}/v1/bsv/main/tx/raw"),
            "POST https://arc.test/v1/tx".to_string(),
        ]
    );
}

/// The header ladders keep no call history (services/CLAUDE.md), so the
/// table's request log is the order the rungs were asked.
#[tokio::test]
async fn the_bhs_rung_answers_the_height_after_chaintracks_faults() {
    let table = LoopbackTable::new();
    table.on(
        "GET",
        "bhs.test",
        "/api/v1/chain/tip/height",
        LoopbackAnswer::status(200, "900123"),
    );
    let options = ServicesOptions::mainnet()
        .with_chaintracks_url("https://chaintracks.test")
        .with_bhs_url("https://bhs.test");
    let services = production(&table, options);
    assert_eq!(services.get_height().await.unwrap(), 900_123);
    assert_eq!(
        table.requests(),
        [
            "GET https://chaintracks.test/findChainTipHeaderHex",
            "GET https://bhs.test/api/v1/chain/tip/height",
        ]
    );
}

#[tokio::test]
async fn the_chaintracks_rung_answers_the_height() {
    let table = LoopbackTable::new();
    table.on(
        "GET",
        "chaintracks.test",
        "/findChainTipHeaderHex",
        LoopbackAnswer::json(200, &tip_frame(900_456)),
    );
    let options = ServicesOptions::mainnet()
        .with_chaintracks_url("https://chaintracks.test")
        .with_bhs_url("https://bhs.test");
    let services = production(&table, options);
    assert_eq!(services.get_height().await.unwrap(), 900_456);
    assert_eq!(
        table.requests(),
        ["GET https://chaintracks.test/findChainTipHeaderHex"]
    );
}

/// A client built outside `building` keeps the socket transport and #182.
#[tokio::test]
async fn a_client_built_outside_the_table_keeps_the_check() {
    let table = LoopbackTable::new();
    table.on("GET", WOC, "*", LoopbackAnswer::status(200, "unused"));
    let error = Client::new()
        .get(format!("https://{WOC}/v1/bsv/main/chain/info"))
        .send()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("#182 rule 28"), "{error}");
    assert!(table.requests().is_empty());
}

// =============================================================================
// The texts: the table against the socket, for the same answer
// =============================================================================

/// One recorded route: method, path (and query), status, body.
type Route = (&'static str, String, u16, String);

/// The text `ask` reads off what `build` builds at one mock server,
/// through the socket with `routes` served by the mock, then through the
/// table with the same routes at the server's host and port.
async fn socket_and_table<S, B, A, Fut>(routes: Vec<Route>, build: B, ask: A) -> (String, String)
where
    B: Fn(&str) -> S,
    A: Fn(S) -> Fut,
    Fut: std::future::Future<Output = String>,
{
    let mut server = mock_server().await;
    let mut mocks = Vec::new();
    for (method, path, status, body) in &routes {
        mocks.push(
            server
                .mock(method, path.as_str())
                .with_status(usize::from(*status))
                .with_body(body)
                .create_async()
                .await,
        );
    }
    let url = server.url();
    let started = std::time::Instant::now();
    let socket = ask(build(&url)).await;
    let socket_took = started.elapsed();
    let authority = url.trim_start_matches("http://").to_string();
    let table = LoopbackTable::new();
    for (method, path, status, body) in routes {
        table.on(
            method,
            &authority,
            &path,
            LoopbackAnswer::status(status, body),
        );
    }
    let started = std::time::Instant::now();
    let through_table = ask(table.building(|| build(&url))).await;
    let table_took = started.elapsed();
    // The record quotes these texts and seconds (docs/census/ci-speed-loop-4.md).
    eprintln!("socket ({socket_took:?}): {socket}\ntable ({table_took:?}):  {through_table}");
    (socket, through_table)
}

/// The three explorers at one base, so one server serves every rung.
fn explorers_at(base: &str) -> Services {
    Services::with_explorers(
        Chain::Main,
        ServicesOptions::mainnet().with_arcade("http://127.0.0.1:9", None),
        StdArc::new(WhatsOnChain::with_base_url(Chain::Main, base)),
        StdArc::new(Bitails::with_base_url(Chain::Main, &format!("{base}/"))),
        Some(StdArc::new(BananaBlocks::with_base_unshared(base))),
    )
    .unwrap()
}

fn headers_at(base: &str) -> Services {
    Services::with_options(
        Chain::Main,
        ServicesOptions::mainnet()
            .with_arcade("http://127.0.0.1:9", None)
            .with_chaintracks_url(base),
    )
    .unwrap()
}

#[tokio::test]
async fn a_raw_tx_every_rung_500_reads_the_same() {
    let (txid, _) = a_tx(3);
    let routes = vec![(
        "GET",
        format!("/tx/{txid}/hex"),
        500,
        "upstream down".to_string(),
    )];
    let (socket, table) = socket_and_table(routes, explorers_at, |services| {
        let txid = txid.clone();
        async move { format!("{:?}", services.get_raw_tx(&txid, false).await) }
    })
    .await;
    assert_eq!(table, socket);
}

/// wire 085's shape: the header service's 500 on the tip.
#[tokio::test]
async fn a_header_service_500_reads_the_same() {
    let routes = vec![(
        "GET",
        "/findChainTipHeaderHex".to_string(),
        500,
        String::new(),
    )];
    let (socket, table) = socket_and_table(routes, headers_at, |services| async move {
        format!("{:?}", services.get_height().await)
    })
    .await;
    assert!(
        socket.contains("HTTP 500 Internal Server Error"),
        "{socket}"
    );
    assert_eq!(table, socket);
}

/// wire 084's shape: the header service's 500 on a height.
#[tokio::test]
async fn a_header_for_height_500_reads_the_same() {
    let routes = vec![(
        "GET",
        "/findHeaderHexForHeight?height=900000".to_string(),
        500,
        String::new(),
    )];
    let (socket, table) = socket_and_table(routes, headers_at, |services| async move {
        format!("{:?}", services.get_header_for_height(900_000).await)
    })
    .await;
    assert_eq!(table, socket);
}

/// reqwest's own texts, through the verifier's `/getChain` read: a status
/// `error_for_status` refuses, and a body that does not decode.
#[tokio::test]
async fn reqwest_status_and_decode_texts_read_the_same() {
    use bsv_rs::transaction::ChainTracker;
    for (status, body) in [(503, String::new()), (200, "{not json".to_string())] {
        let routes = vec![("GET", "/getChain".to_string(), status, body)];
        let (socket, table) = socket_and_table(routes, headers_at, |services| async move {
            let tracker = services.chaintracks.as_ref().unwrap().primary();
            format!("{:?}", tracker.mining_tip().await)
        })
        .await;
        assert!(
            socket.contains("HTTP status server error (503 Service Unavailable) for url")
                || socket.contains("error decoding response body"),
            "{socket}"
        );
        assert_eq!(table, socket);
    }
}

/// ARC's refusal and its JSON that does not parse, through the post ladder.
#[tokio::test]
#[allow(deprecated)]
async fn an_arc_refusal_reads_the_same() {
    let (beef, txid) = a_beef();
    let routes = vec![(
        "POST",
        "/v1/tx".to_string(),
        465,
        r#"{"status":465,"title":"Fee too low","detail":"fee"}"#.to_string(),
    )];
    let build = |base: &str| {
        Services::with_explorers(
            Chain::Main,
            ServicesOptions::mainnet().with_classic_arc(base, None),
            StdArc::new(WhatsOnChain::with_base_url(
                Chain::Main,
                "http://127.0.0.1:9",
            )),
            StdArc::new(Bitails::with_base_url(Chain::Main, "http://127.0.0.1:9/")),
            None,
        )
        .unwrap()
    };
    let (socket, table) = socket_and_table(routes, build, |services| {
        let (beef, txid) = (beef.clone(), txid.clone());
        async move {
            let results = services.post_beef(&beef, &[txid]).await.unwrap();
            // Every field but the notes, whose `when` is the clock's.
            results
                .iter()
                .map(|r| {
                    let words: Vec<_> = r
                        .txid_results
                        .iter()
                        .map(|t| (t.status.clone(), t.data.clone(), t.service_error))
                        .collect();
                    format!("{} {} {words:?}", r.name, r.status)
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    })
    .await;
    assert!(socket.contains("465"), "{socket}");
    assert_eq!(table, socket);
}

/// An unmatched request reads as the socket's refused connection at the
/// same URL (nothing listens on port 9).
#[tokio::test]
async fn a_send_fault_reads_as_a_refused_connection() {
    let (txid, _) = a_tx(4);
    let base = "http://127.0.0.1:9";
    let socket = format!("{:?}", explorers_at(base).get_raw_tx(&txid, false).await);
    let table = LoopbackTable::new();
    let through_table = table
        .building(|| explorers_at(base))
        .get_raw_tx(&txid, false)
        .await;
    assert_eq!(format!("{through_table:?}"), socket);
}

/// A resolver that answers nothing: a DNS fault with no network.
struct NoAnswer;

impl reqwest::dns::Resolve for NoAnswer {
    fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async { Err("no such host".into()) })
    }
}

/// The socket's own faults, read off reqwest at the client: a refused
/// connection, a timeout, a TLS fault and a DNS fault are one text, the
/// table's send fault at the same URL.
#[tokio::test]
async fn the_socket_faults_are_one_text_and_the_table_gives_it() {
    use tokio::io::AsyncWriteExt;

    let table = LoopbackTable::new();
    let table_text = |url: String| {
        let table = table.clone();
        async move {
            let client = table.building(Client::new);
            client.get(url).send().await.unwrap_err().to_string()
        }
    };

    // (the socket's text, the URL), read in turn, then each against the table.
    let mut faults = Vec::new();

    // A refused connection.
    let refused = "http://127.0.0.1:9/refused".to_string();
    let error = Client::new().get(&refused).send().await.unwrap_err();
    faults.push((error.to_string(), refused));

    // A timeout: a listener that accepts and never answers.
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/silent", silent.local_addr().unwrap());
    let held = tokio::spawn(async move {
        let (stream, _) = silent.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        drop(stream);
    });
    let impatient = Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let error = impatient.get(&url).send().await.unwrap_err();
    faults.push((error.to_string(), url));
    held.abort();

    // A TLS fault: an https URL at a listener that writes plain bytes.
    let plain = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}/tls", plain.local_addr().unwrap());
    let served = tokio::spawn(async move {
        let (mut stream, _) = plain.accept().await.unwrap();
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await;
    });
    let error = Client::new().get(&url).send().await.unwrap_err();
    faults.push((error.to_string(), url));
    served.abort();

    // A DNS fault, with no network: a resolver that answers nothing.
    let url = "http://dns-fault.test/x".to_string();
    let unresolved = reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(StdArc::new(NoAnswer))
        .build()
        .unwrap();
    let error = unresolved.get(&url).send().await.unwrap_err();
    faults.push((error.to_string(), url));

    for (socket, url) in faults {
        assert_eq!(socket, table_text(url).await);
    }
}
