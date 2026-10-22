//! The three explorers' ladders (0.12.0; the owner's word of 2026-10-22,
//! bsv-wallet-cli#50): WhatsOnChain, BananaBlocks and Bitails are each the
//! others' fallback; a fault (a 429, a timeout, a 5xx, a malformed body) is
//! "could not look" and moves the question to the next rung within the same
//! call, never a negative; a 429 is not slept on, and its rung is skipped
//! without a request until Retry-After passes. Local fixtures only.

use super::rule_28_tests::{
    a_spender_of, bananablocks_txo_body, services_with_three, sorted_names, utxo_services,
    utxo_status, Says, UTXO_SCRIPT, UTXO_TXID,
};
use super::*;
use crate::services::providers::WhatsOnChainConfig;
use crate::services::traits::{WalletServices, LEAD_PAGES_MAX};
use std::time::{Duration, Instant};

/// The recorded calls of `provider` in a ladder's call history.
fn calls(history: &crate::services::ServiceCallHistory, provider: &str) -> Vec<ServiceCall> {
    history
        .history_by_provider
        .get(provider)
        .map(|h| h.calls.clone())
        .unwrap_or_default()
}

fn utxo_history(services: &Services) -> crate::services::ServiceCallHistory {
    services
        .get_services_call_history(false)
        .unwrap()
        .get_utxo_status
        .unwrap()
}

/// A 429 on the first rung reaches the others within the same call, with
/// no sleep (WhatsOnChain slept 2 s twice before 0.12.0), and the call
/// history records the rate limit apart from other faults.
#[tokio::test]
async fn a_429_on_the_first_rung_reaches_the_second_without_a_sleep() {
    let f = utxo_services(Says::RateLimited, Says::Unspent, Says::Unspent).await;
    let started = Instant::now();
    let result = utxo_status(&f.services).await;
    let elapsed = started.elapsed();
    assert_eq!(
        (result.status.as_str(), result.is_utxo, result.name.as_str()),
        ("success", Some(true), "BananaBlocks+Bitails"),
        "{result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1000),
        "a 429 is not slept on: {elapsed:?}"
    );
    f.woc.1.expect(1).assert_async().await;
    f.bitails.1.expect(1).assert_async().await;

    let woc = calls(&utxo_history(&f.services), "WhatsOnChain");
    assert_eq!(woc.len(), 1);
    let error = woc[0]
        .error
        .as_ref()
        .expect("the 429 is recorded as an error");
    assert_eq!(error.code, "RATE_LIMITED");
    assert!(
        error.message.contains("retry after 30s"),
        "{}",
        error.message
    );
}

/// The rung a 429 closed is skipped without a request until Retry-After
/// passes: three calls rotate over all three starts, and WhatsOnChain is
/// asked once. The negative still needs two: BananaBlocks and Bitails.
#[tokio::test]
async fn the_rung_is_skipped_during_retry_after() {
    let f = utxo_services(Says::RateLimited, Says::NotListed, Says::NotListed).await;
    let started = Instant::now();
    for _ in 0..3 {
        let result = utxo_status(&f.services).await;
        assert_eq!(
            (result.status.as_str(), result.is_utxo),
            ("success", Some(false)),
            "{result:?}"
        );
    }
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "{:?}",
        started.elapsed()
    );
    // Call 1 starts at WhatsOnChain (429), call 2 at BananaBlocks (two
    // negatives, WhatsOnChain not reached), call 3 at Bitails (WhatsOnChain
    // skipped inside its Retry-After, then BananaBlocks).
    f.woc.1.expect(1).assert_async().await;
    let woc = calls(&utxo_history(&f.services), "WhatsOnChain");
    assert_eq!(woc.len(), 2, "two calls, one request");
    assert!(woc
        .iter()
        .all(|c| c.error.as_ref().is_some_and(|e| e.code == "RATE_LIMITED")));
}

/// A rung that is rate limited is not asked again by the retry round: with
/// every rung closed by a 429 the answer comes at once, "could not look".
#[tokio::test]
async fn rate_limited_rungs_are_not_retried_after_a_sleep() {
    let f = utxo_services(Says::RateLimited, Says::RateLimited, Says::NotListed).await;
    let started = Instant::now();
    let result = utxo_status(&f.services).await;
    assert_eq!((result.status.as_str(), result.is_utxo), ("error", None));
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "{:?}",
        started.elapsed()
    );
}

// =============================================================================
// getRawTx: WoC → BananaBlocks → Bitails
// =============================================================================

struct RawScene {
    services: Services,
    _servers: Vec<mockito::ServerGuard>,
    mocks: Vec<mockito::Mock>,
}

async fn raw_scene(
    txid: &str,
    woc: (usize, &str),
    bb: (usize, &str),
    bitails: (usize, &str),
) -> RawScene {
    let mut servers = Vec::new();
    let mut mocks = Vec::new();
    for (status, body) in [woc, bb, bitails] {
        let mut server = mockito::Server::new_async().await;
        let mut mock = server
            .mock("GET", format!("/tx/{txid}/hex").as_str())
            .with_status(status)
            .with_body(body);
        if status == 429 {
            mock = mock.with_header("retry-after", "30");
        }
        mocks.push(mock.create_async().await);
        servers.push(server);
    }
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &servers[0].url(),
        &servers[1].url(),
        &servers[2].url(),
    );
    RawScene {
        services,
        _servers: servers,
        mocks,
    }
}

/// WhatsOnChain rate limited: BananaBlocks serves the bytes (bound to the
/// txid) in the same call, at once, and Bitails is not asked.
#[tokio::test]
async fn a_raw_tx_429_moves_to_bananablocks() {
    let (txid, hex) = a_spender_of(UTXO_TXID, 0);
    let scene = raw_scene(&txid, (429, ""), (200, &hex), (200, &hex)).await;
    let started = Instant::now();
    let result = scene.services.get_raw_tx(&txid, false).await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(1000));
    assert_eq!(result.raw_tx, Some(hex::decode(&hex).unwrap()));
    assert_eq!(result.name, "BananaBlocks");
    let mut mocks = scene.mocks;
    mocks.pop().unwrap().expect(0).assert_async().await;
}

/// BananaBlocks' bytes that do not hash to the txid are a fault: with the
/// others' "no such transaction" the answer is "could not look", never
/// "not found", and never the wrong bytes.
#[tokio::test]
async fn bytes_that_do_not_hash_are_a_fault_not_an_answer() {
    let (txid, _) = a_spender_of(UTXO_TXID, 0);
    let (_, other_hex) = a_spender_of(UTXO_TXID, 1);
    let scene = raw_scene(&txid, (404, ""), (200, &other_hex), (404, "")).await;
    let result = scene.services.get_raw_tx(&txid, false).await.unwrap();
    assert!(result.raw_tx.is_none());
    assert!(result.could_not_look, "{result:?}");
    assert!(!result.is_not_found());
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("BananaBlocks")),
        "{result:?}"
    );
}

/// All three answering "no such transaction" is "not found".
#[tokio::test]
async fn a_raw_tx_all_three_lack_is_not_found() {
    let txid = "ab".repeat(32);
    let scene = raw_scene(&txid, (404, ""), (404, ""), (404, "")).await;
    let result = scene.services.get_raw_tx(&txid, false).await.unwrap();
    assert!(result.is_not_found(), "{result:?}");
}

// =============================================================================
// getMerklePath: BananaBlocks' proof, checked against the header service
// =============================================================================

/// WhatsOnChain has no proof; BananaBlocks' `/proof` (target: the merkle
/// root) is handed on with the block hash, the header comes from the header
/// service, the root is checked at that height, and the BUMP is returned.
#[tokio::test]
async fn bananablocks_serves_a_proof_the_header_service_accepts() {
    let a = "aa".repeat(32);
    let b = "bb".repeat(32);
    let block_hash = "cc".repeat(32);
    let height = 900_000u32;
    // The root of the two-leaf block [a, b].
    let mut joined = hex::decode(&a).unwrap();
    joined.reverse();
    let mut right = hex::decode(&b).unwrap();
    right.reverse();
    joined.extend(right);
    let mut root = sha256d(&joined).to_vec();
    root.reverse();
    let root = hex::encode(root);

    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let bitails = mockito::Server::new_async().await;
    let mut ct = mockito::Server::new_async().await;
    let _w = woc
        .mock("GET", format!("/tx/{a}/proof/tsc").as_str())
        .with_status(404)
        .create_async()
        .await;
    let _tx = bb
        .mock("GET", format!("/tx/{a}").as_str())
        .with_status(200)
        .with_body(format!(
            r#"{{"txid":"{a}","block_hash":"{block_hash}","block_height":{height}}}"#
        ))
        .create_async()
        .await;
    let _proof = bb
        .mock("GET", format!("/tx/{a}/proof").as_str())
        .with_status(200)
        .with_body(format!(
            r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["{b}"]}}"#
        ))
        .create_async()
        .await;
    let header = serde_json::json!({
        "status": "success",
        "value": {
            "version": 1, "previousHash": "0".repeat(64), "merkleRoot": root,
            "time": 1700000000u32, "bits": 486604799u32, "nonce": 1u32,
            "height": height, "hash": block_hash,
        }
    })
    .to_string();
    let _by_hash = ct
        .mock(
            "GET",
            format!("/findHeaderHexForBlockHash?hash={block_hash}").as_str(),
        )
        .with_status(200)
        .with_body(&header)
        .create_async()
        .await;
    let _by_height = ct
        .mock(
            "GET",
            format!("/findHeaderHexForHeight?height={height}").as_str(),
        )
        .with_status(200)
        .with_body(&header)
        .create_async()
        .await;

    let services = services_with_three(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services.get_merkle_path(&a, false).await.unwrap();
    let bump = result.merkle_path.as_deref().expect("a proof");
    let path = bsv_rs::transaction::MerklePath::from_hex(bump).unwrap();
    assert_eq!(path.block_height, height);
    assert_eq!(path.compute_root(Some(&a)).unwrap(), root);
    assert_eq!(result.header.as_ref().unwrap().hash, block_hash);
}

// =============================================================================
// getStatusForTxids: BananaBlocks behind the chain indexes
// =============================================================================

/// WhatsOnChain and Bitails could not look; BananaBlocks places the
/// transaction in a block.
#[tokio::test]
async fn bananablocks_answers_the_status_when_the_others_could_not_look() {
    let txid = "ab".repeat(32);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("POST", "/txs/status")
        .with_status(429)
        .create_async()
        .await;
    let _t = bitails
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(500)
        .create_async()
        .await;
    let _b = bb
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(200)
        .with_body(format!(
            r#"{{"txid":"{txid}","block_hash":"{}","block_height":900000}}"#,
            "cc".repeat(32)
        ))
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services
        .get_status_for_txids(std::slice::from_ref(&txid), false)
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].status, "mined");
    assert_eq!(result.results[0].depth, Some(1));
}

/// BananaBlocks' `unknown` (its index lags the mempool) never replaces a
/// chain index's `known`.
#[tokio::test]
async fn bananablocks_unknown_never_replaces_a_chain_indexs_known() {
    let txid = "ab".repeat(32);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("POST", "/txs/status")
        .with_status(200)
        .with_body(format!(r#"[{{"txid":"{txid}"}}]"#))
        .create_async()
        .await;
    let _t = bitails
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(500)
        .create_async()
        .await;
    let _b = bb
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(404)
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services
        .get_status_for_txids(std::slice::from_ref(&txid), false)
        .await
        .unwrap();
    assert_eq!(result.results[0].status, "known", "{result:?}");
}

// =============================================================================
// spend_evidence: BananaBlocks' spent_txid beside WhatsOnChain's spent route
// =============================================================================

/// WhatsOnChain names no spender; BananaBlocks names one whose bytes spend
/// the outpoint: bound, so a hint even while the unspent sets could not be
/// read (one negative, two faults: "could not look" alone).
#[tokio::test]
async fn bananablocks_names_the_spender_when_whatsonchain_does_not() {
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, 0);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let bitails = mockito::Server::new_async().await;
    let ct = mockito::Server::new_async().await;
    let script_hash = {
        let mut h = sha256(UTXO_SCRIPT);
        h.reverse();
        hex::encode(h)
    };
    let _unspent = woc
        .mock("GET", format!("/script/{script_hash}/unspent/all").as_str())
        .with_status(429)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let _spent = woc
        .mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
        .with_status(404)
        .create_async()
        .await;
    let _raw = bb
        .mock("GET", format!("/tx/{spender}/hex").as_str())
        .with_status(200)
        .with_body(&spender_hex)
        .create_async()
        .await;
    let names = bb
        .mock("GET", format!("/txo/{UTXO_TXID}/0").as_str())
        .with_status(200)
        .with_body(bananablocks_txo_body(Some(&spender)))
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    assert_eq!(
        services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
        UtxoVerdict::SpentHint
    );
    // Once, as the unspent question's rung: `spend_evidence` takes the
    // spender from that answer (review 2 L3 of #43).
    names.expect(1).assert_async().await;
}

/// An unspent verdict costs one BananaBlocks request when it answers
/// (review 2 L3 of #43): `/txo` read as a rung of the unspent question is
/// the spender question's answer too, and where the ladder did not reach
/// BananaBlocks `spend_evidence` asks it once. Three calls, one from each start.
#[tokio::test]
async fn an_unspent_verdict_costs_one_bananablocks_request() {
    let f = utxo_services(Says::Unspent, Says::Unspent, Says::Unspent).await;
    let (mut woc, _unspent) = f.woc;
    let ct = mockito::Server::new_async().await;
    let _spent = woc
        .mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
        .with_status(404)
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &f.bananablocks.0.url(),
        &f.bitails.0.url(),
    );
    for start in 0..3 {
        assert_eq!(
            services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
            UtxoVerdict::UnspentHint,
            "call {start}"
        );
    }
    f.bananablocks.1.expect(3).assert_async().await;
}

/// The retry round re-asks a transient fault only (review 2 L2 of #43): a
/// BananaBlocks 404 (the index does not know the output: an answer, not
/// an outage) and a WhatsOnChain body that is not the unspent set's shape
/// would come back the same, so neither is asked again and nothing sleeps.
#[tokio::test]
async fn a_deterministic_fault_is_not_asked_again() {
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let hash = script_hash_be(UTXO_SCRIPT);
    let malformed = woc
        .mock("GET", format!("/script/{hash}/unspent/all").as_str())
        .with_status(200)
        .with_body("not json")
        .create_async()
        .await;
    let not_indexed = bb
        .mock("GET", format!("/txo/{UTXO_TXID}/0").as_str())
        .with_status(404)
        .with_body("TXO not found")
        .create_async()
        .await;
    let _not_listed = bitails
        .mock("GET", format!("/scripthash/{hash}/unspent").as_str())
        .with_status(200)
        .with_body(format!(r#"{{"scripthash":"{hash}","unspent":[]}}"#))
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let started = Instant::now();
    let result = utxo_status(&services).await;
    assert_eq!((result.status.as_str(), result.is_utxo), ("error", None));
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "no retry round, no sleep: {:?}",
        started.elapsed()
    );
    malformed.expect(1).assert_async().await;
    not_indexed.expect(1).assert_async().await;
}

/// The fault classes the retry round tells apart (review 2 L2 of #43).
#[test]
fn a_fault_is_transient_only_for_an_outage() {
    let transient = [
        Error::NetworkError("Request failed: operation timed out".to_string()),
        Error::NetworkError("BananaBlocks request failed: connection refused".to_string()),
        Error::ServiceError("BananaBlocks txo ab.0: HTTP 503 Service Unavailable: ".to_string()),
    ];
    for e in &transient {
        assert_eq!(FaultKind::of_error(e), FaultKind::Transient, "{e}");
    }
    for message in ["HTTP 500 Internal Server Error", "HTTP 408 Request Timeout"] {
        assert_eq!(
            FaultKind::of_message(message),
            FaultKind::Transient,
            "{message}"
        );
    }

    let deterministic = [
        Error::ServiceError(
            "BananaBlocks txo ab.0: its script hashes to 11, not the asked 22".to_string(),
        ),
        Error::ServiceError("BananaBlocks txo ab.0: HTTP 404 Not Found: TXO not found".to_string()),
        Error::ServiceError("BananaBlocks txo ab.0: bad json: expected value: x".to_string()),
        Error::ServiceError("Failed to parse UTXO response: expected value".to_string()),
        Error::InvalidArgument("outpoint ab: not txid.vout".to_string()),
    ];
    for e in &deterministic {
        assert_eq!(FaultKind::of_error(e), FaultKind::Deterministic, "{e}");
    }
    for message in ["HTTP 404 Not Found", "HTTP 400 Bad Request", "no answer"] {
        assert_eq!(
            FaultKind::of_message(message),
            FaultKind::Deterministic,
            "{message}"
        );
    }

    let limited = Error::RateLimited {
        provider: "Bitails".to_string(),
        retry_after_secs: 30,
    };
    assert_eq!(FaultKind::of_error(&limited), FaultKind::RateLimited);
}

/// A server that answers every request with a 200 promising a 1000-byte
/// body, sends a few bytes of it and drops the connection.
async fn a_body_cut_short() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                      content-length: 1000\r\n\r\n{\"result\":[",
                )
                .await;
            let _ = socket.shutdown().await;
        }
    });
    url
}

/// The unspent-set read's two body faults, by provider (review 3 L1 of
/// #43): a body cut short is a failed request, transient (asked again in
/// the retry round); a body that arrived malformed is deterministic.
async fn the_body_faults_are_told_apart<F, Fut>(read: F)
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<GetUtxoStatusResult>>,
{
    let cut = read(a_body_cut_short().await)
        .await
        .expect_err("a body cut short is a fault");
    assert!(cut.to_string().contains("read failed"), "{cut}");
    assert_eq!(FaultKind::of_error(&cut), FaultKind::Transient, "{cut}");

    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", mockito::Matcher::Any)
        .with_status(200)
        .with_body("not json")
        .create_async()
        .await;
    let malformed = read(format!("{}/", server.url()))
        .await
        .expect_err("a malformed body is a fault");
    assert!(
        malformed.to_string().contains("Failed to parse"),
        "{malformed}"
    );
    assert_eq!(
        FaultKind::of_error(&malformed),
        FaultKind::Deterministic,
        "{malformed}"
    );
}

#[tokio::test]
async fn a_whatsonchain_unspent_body_cut_short_is_transient_a_malformed_one_deterministic() {
    the_body_faults_are_told_apart(|url| async move {
        WhatsOnChain::with_base_url(Chain::Main, url.trim_end_matches('/'))
            .get_utxo_status(
                &hex::encode(UTXO_SCRIPT),
                Some(GetUtxoStatusOutputFormat::Script),
                None,
            )
            .await
    })
    .await;
}

#[tokio::test]
async fn a_bitails_unspent_body_cut_short_is_transient_a_malformed_one_deterministic() {
    the_body_faults_are_told_apart(|url| async move {
        crate::services::providers::Bitails::with_base_url(Chain::Main, &url)
            .get_utxo_status(
                &hex::encode(UTXO_SCRIPT),
                Some(GetUtxoStatusOutputFormat::Script),
                None,
            )
            .await
    })
    .await;
}

/// BananaBlocks' named spender whose bytes do not spend the outpoint is
/// dropped: nothing bound, the unspent sets' "could not look" stands.
#[tokio::test]
async fn a_bananablocks_spender_its_bytes_do_not_bear_out_is_dropped() {
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, 1);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let bitails = mockito::Server::new_async().await;
    let ct = mockito::Server::new_async().await;
    let _spent = woc
        .mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
        .with_status(404)
        .create_async()
        .await;
    let _raw = bb
        .mock("GET", format!("/tx/{spender}/hex").as_str())
        .with_status(200)
        .with_body(&spender_hex)
        .create_async()
        .await;
    let _names = bb
        .mock("GET", format!("/txo/{UTXO_TXID}/0").as_str())
        .with_status(200)
        .with_body(bananablocks_txo_body(Some(&spender)))
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    assert_eq!(
        services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
        UtxoVerdict::Unknown
    );
}

// =============================================================================
// WhatsOnChain: a 429 is returned at once; the key travels as woc-api-key
// =============================================================================

/// Before 0.12.0 a WhatsOnChain GET that met a 429 slept 2 s and retried
/// twice; now the 429 is the fault, at once, with its Retry-After, and the
/// next call inside it makes no request.
#[tokio::test]
async fn a_whatsonchain_429_is_a_fault_at_once_not_a_sleeping_retry() {
    let txid = "ab".repeat(32);
    let mut server = mockito::Server::new_async().await;
    let route = server
        .mock("GET", format!("/tx/{txid}/hex").as_str())
        .with_status(429)
        .with_header("retry-after", "9")
        .expect(1)
        .create_async()
        .await;
    let woc = WhatsOnChain::with_base_url(Chain::Main, &server.url());
    let started = Instant::now();
    match woc.get_raw_tx(&txid).await {
        Err(Error::RateLimited {
            provider,
            retry_after_secs,
        }) => {
            assert_eq!(provider, "WhatsOnChain");
            assert_eq!(retry_after_secs, 9);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        woc.get_merkle_path(&txid).await,
        Err(Error::RateLimited { .. })
    ));
    assert!(started.elapsed() < Duration::from_millis(1000));
    route.assert_async().await;
}

/// Before 0.12.0 a WhatsOnChain broadcast that met a 429 slept 2 s, up to
/// five times; now it is a service error at once and the broadcast ladder
/// moves on.
#[tokio::test]
async fn a_whatsonchain_broadcast_429_is_an_error_at_once_not_five_sleeps() {
    let (_, hex) = a_spender_of(UTXO_TXID, 0);
    let mut server = mockito::Server::new_async().await;
    let route = server
        .mock("POST", "/tx/raw")
        .with_status(429)
        .expect(1)
        .create_async()
        .await;
    let woc = WhatsOnChain::with_base_url(Chain::Main, &server.url());
    let started = Instant::now();
    let result = woc.post_raw_tx(&hex).await.unwrap();
    assert_eq!(result.status, "error");
    assert!(result.service_error);
    assert!(!result.double_spend);
    assert!(result
        .data
        .as_deref()
        .is_some_and(|d| d.contains("Rate limited")));
    // Inside the Retry-After (the 1 s default) the rung is closed.
    let again = woc.post_raw_tx(&hex).await.unwrap();
    assert!(again.service_error);
    assert!(started.elapsed() < Duration::from_millis(1000));
    route.assert_async().await;
}

/// A configured key is sent as `woc-api-key` (and `Authorization`, as
/// before).
#[tokio::test]
async fn the_woc_api_key_header_is_sent() {
    let txid = "ab".repeat(32);
    let mut server = mockito::Server::new_async().await;
    let route = server
        .mock("GET", format!("/tx/{txid}/hex").as_str())
        .match_header("woc-api-key", "mainnet_k")
        .match_header("authorization", "Bearer mainnet_k")
        .with_status(404)
        .expect(1)
        .create_async()
        .await;
    let woc = WhatsOnChain::with_base_url_and_config(
        Chain::Main,
        &server.url(),
        WhatsOnChainConfig::with_api_key("mainnet_k"),
    );
    assert!(woc.get_raw_tx(&txid).await.unwrap().is_not_found());
    route.assert_async().await;
}

// =============================================================================
// The owner's rule, applied (review of #43, answer 9)
// =============================================================================

/// The big-endian hash the explorers' script routes carry for `script`.
fn script_hash_be(script: &[u8]) -> String {
    let mut h = sha256(script);
    h.reverse();
    hex::encode(h)
}

async fn empty_unspent_set(
    woc: &mut mockito::ServerGuard,
    bitails: &mut mockito::ServerGuard,
    script: &[u8],
) -> (mockito::Mock, mockito::Mock) {
    let hash = script_hash_be(script);
    let w = woc
        .mock("GET", format!("/script/{hash}/unspent/all").as_str())
        .with_status(200)
        .with_body(format!(r#"{{"script":"{hash}","result":[]}}"#))
        .create_async()
        .await;
    let t = bitails
        .mock("GET", format!("/scripthash/{hash}/unspent").as_str())
        .with_status(200)
        .with_body(format!(r#"{{"scripthash":"{hash}","unspent":[]}}"#))
        .create_async()
        .await;
    (w, t)
}

/// The review's live counterexample (2026-10-22) as a fixture: WhatsOnChain's
/// `/spent` names a mempool spender (`659e6e1d…` of `826718bf…:0` live; the
/// fixture's spender is built, so its bytes are held) while BananaBlocks
/// answers `spent: false`. Whatever Bitails says, even a lagging "unspent"
/// that makes two unspent answers, the verdict is the bound spender,
/// never unspent.
#[tokio::test]
async fn whatsonchain_names_a_spender_bananablocks_says_unspent_the_spender_stands() {
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, 0);
    for bitails_says in [Says::Unspent, Says::NotListed, Says::Fault] {
        let f = utxo_services(Says::NotListed, Says::Unspent, bitails_says).await;
        let (mut woc, _not_listed) = f.woc;
        let ct = mockito::Server::new_async().await;
        let _spent = woc
            .mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
            .with_status(200)
            .with_body(format!(r#"{{"txid":"{spender}","vin":0}}"#))
            .create_async()
            .await;
        let _raw = woc
            .mock("GET", format!("/tx/{spender}/hex").as_str())
            .with_status(200)
            .with_body(&spender_hex)
            .create_async()
            .await;
        let services = services_with_three(
            ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
            &woc.url(),
            &f.bananablocks.0.url(),
            &f.bitails.0.url(),
        );
        assert_eq!(
            services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
            UtxoVerdict::SpentHint,
            "Bitails {bitails_says:?}"
        );
    }
}

/// `is_utxo` with no stored script (`un_fail` passes `&[]`): BananaBlocks'
/// `spent: false` is for an output whose script is not the empty one, so
/// it is a fault, never `UnspentHint`; with the others down it is
/// "could not look", and with their empty sets "not listed".
#[tokio::test]
async fn the_empty_script_never_comes_back_unspent() {
    // The others could not look: before the fix, BananaBlocks alone was
    // `UnspentHint`.
    let f = utxo_services(Says::Fault, Says::Unspent, Says::RateLimited).await;
    assert_eq!(
        f.services.is_utxo(UTXO_TXID, 0, &[]).await,
        UtxoVerdict::Unknown
    );
    let bb = calls(&utxo_history(&f.services), "BananaBlocks");
    assert!(
        bb.iter().all(|c| c
            .error
            .as_ref()
            .is_some_and(|e| e.message.contains("not the asked"))),
        "the mismatch is a fault: {bb:?}"
    );

    // The others answer for the empty script's hash: nothing listed.
    let f = utxo_services(Says::Fault, Says::Unspent, Says::Fault).await;
    let (mut woc, _) = f.woc;
    let (mut bitails, _) = f.bitails;
    let _sets = empty_unspent_set(&mut woc, &mut bitails, &[]).await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &f.bananablocks.0.url(),
        &bitails.url(),
    );
    let verdict = services.is_utxo(UTXO_TXID, 0, &[]).await;
    assert!(!verdict.is_unspent_hint(), "{verdict:?}");
}

/// The status ladder with both chain indexes faulting and BananaBlocks'
/// 404: BananaBlocks' `unknown` is a gap (its index lags the mempool), not
/// "absent": the txid is left out and the answer says it could not look
/// (it was a clean `unknown` before the fix: review H2).
#[tokio::test]
async fn the_status_ladder_with_both_chain_indexes_down_and_a_bananablocks_404_could_not_look() {
    let txid = "ab".repeat(32);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("POST", "/txs/status")
        .with_status(429)
        .create_async()
        .await;
    let _t = bitails
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(500)
        .create_async()
        .await;
    let _b = bb
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(404)
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services
        .get_status_for_txids(std::slice::from_ref(&txid), false)
        .await
        .unwrap();
    assert!(result.results.is_empty(), "{result:?}");
    assert_eq!(result.status, "error", "{result:?}");
    assert!(
        result.error.as_deref().is_some_and(|e| e
            .contains("could not look: no chain index answered")
            && e.contains(&txid)),
        "{result:?}"
    );
}

/// BananaBlocks' `known` against WhatsOnChain's `unknown` (Bitails down):
/// one provider's "absent" never stands against a second that holds the
/// transaction (review M2).
#[tokio::test]
async fn bananablocks_known_beats_a_lone_whatsonchain_unknown() {
    let txid = "ab".repeat(32);
    let mut woc = mockito::Server::new_async().await;
    let mut bb = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("POST", "/txs/status")
        .with_status(200)
        .with_body(format!(r#"[{{"txid":"{txid}","error":"unknown"}}]"#))
        .create_async()
        .await;
    let _t = bitails
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(500)
        .create_async()
        .await;
    let _b = bb
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(200)
        .with_body(format!(
            r#"{{"txid":"{txid}","block_hash":"","block_height":0}}"#
        ))
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services
        .get_status_for_txids(std::slice::from_ref(&txid), false)
        .await
        .unwrap();
    assert_eq!(result.results.len(), 1, "{result:?}");
    assert_eq!(result.results[0].status, "known", "{result:?}");
    assert_eq!(result.error, None, "{result:?}");
}

/// BananaBlocks' token bucket runs dry mid-ladder: its rung is a rate-limit
/// fault without a request, recorded `RATE_LIMITED`, and the ladder takes
/// its answer from the two others, at once.
#[tokio::test]
async fn an_empty_bucket_skips_bananablocks_mid_ladder() {
    let f = utxo_services(Says::NotListed, Says::Unspent, Says::NotListed).await;
    let gate = f.services.bananablocks.as_ref().unwrap().rate_gate();
    for start in 0..3 {
        while gate.admit().is_ok() {}
        {
            let mut collection = lock_write(&f.services.get_utxo_status_services).unwrap();
            while collection.current_name() != Some("WhatsOnChain") {
                collection.next();
            }
            for _ in 0..start {
                collection.next();
            }
        }
        let started = Instant::now();
        let result = utxo_status(&f.services).await;
        assert_eq!(
            (
                result.status.as_str(),
                result.is_utxo,
                sorted_names(&result)
            ),
            ("success", Some(false), "Bitails+WoC".to_string()),
            "start {start}: {result:?}"
        );
        assert!(started.elapsed() < Duration::from_millis(1000));
    }
    f.bananablocks.1.expect(0).assert_async().await;
    let bb = calls(&utxo_history(&f.services), "BananaBlocks");
    assert!(!bb.is_empty());
    assert!(bb
        .iter()
        .all(|c| c.error.as_ref().is_some_and(|e| e.code == "RATE_LIMITED")));
}

/// A broadcast while the rung is closed by an earlier Retry-After (a GET's
/// 429): a service error at once, and no request is made.
#[tokio::test]
async fn a_broadcast_inside_an_earlier_retry_after_makes_no_request() {
    let (_, hex) = a_spender_of(UTXO_TXID, 0);
    let txid = "ab".repeat(32);
    let mut server = mockito::Server::new_async().await;
    let _get = server
        .mock("GET", format!("/tx/{txid}/hex").as_str())
        .with_status(429)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let post = server
        .mock("POST", mockito::Matcher::Any)
        .expect(0)
        .create_async()
        .await;
    let woc = WhatsOnChain::with_base_url(Chain::Main, &server.url());
    assert!(matches!(
        woc.get_raw_tx(&txid).await,
        Err(Error::RateLimited { .. })
    ));
    let result = woc.post_raw_tx(&hex).await.unwrap();
    assert!(result.service_error, "{result:?}");
    assert!(!result.double_spend);

    let bitails = Bitails::with_base_url(Chain::Main, &server.url());
    let _bitails_get = server
        .mock("GET", format!("/tx/{txid}").as_str())
        .with_status(429)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let _ = bitails
        .get_status_for_txids(std::slice::from_ref(&txid))
        .await;
    let result = bitails.broadcast(&hex::decode(&hex).unwrap()).await;
    assert!(
        matches!(result, Err(Error::RateLimited { .. })),
        "{result:?}"
    );
    post.assert_async().await;
}

/// A WhatsOnChain fault on `/spent` (a 500 or a 429, not a 404) leads
/// `spend_evidence` to BananaBlocks, whose named spender is bound by its
/// bytes.
#[tokio::test]
async fn a_whatsonchain_spent_fault_leads_spend_evidence_to_bananablocks() {
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, 0);
    for (status, retry_after) in [(500, None), (429, Some("30"))] {
        let mut woc = mockito::Server::new_async().await;
        let mut bb = mockito::Server::new_async().await;
        let bitails = mockito::Server::new_async().await;
        let ct = mockito::Server::new_async().await;
        let mut spent = woc
            .mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
            .with_status(status);
        if let Some(secs) = retry_after {
            spent = spent.with_header("retry-after", secs);
        }
        let _spent = spent.create_async().await;
        let _raw = bb
            .mock("GET", format!("/tx/{spender}/hex").as_str())
            .with_status(200)
            .with_body(&spender_hex)
            .create_async()
            .await;
        let names = bb
            .mock("GET", format!("/txo/{UTXO_TXID}/0").as_str())
            .with_status(200)
            .with_body(bananablocks_txo_body(Some(&spender)))
            .expect_at_least(1)
            .create_async()
            .await;
        let services = services_with_three(
            ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
            &woc.url(),
            &bb.url(),
            &bitails.url(),
        );
        assert_eq!(
            services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
            UtxoVerdict::SpentHint,
            "WhatsOnChain {status}"
        );
        assert!(names.matched_async().await);
    }
}

/// WhatsOnChain's status meets a 429 on its second chunk of 20: the first
/// chunk's answers are kept with the error set, the rest left out (review
/// L2); the ladder asks the next rung about the txids left out.
#[tokio::test]
async fn a_whatsonchain_status_429_on_a_later_chunk_keeps_the_earlier_answers() {
    let txids: Vec<String> = (0..25u8)
        .map(|i| format!("{:02x}", i + 1).repeat(32))
        .collect();
    let mined: Vec<String> = txids[..20]
        .iter()
        .map(|t| format!(r#"{{"txid":"{t}","confirmations":3}}"#))
        .collect();
    let mut woc = mockito::Server::new_async().await;
    let _first = woc
        .mock("POST", "/txs/status")
        .match_body(mockito::Matcher::Regex(txids[0].clone()))
        .with_status(200)
        .with_body(format!("[{}]", mined.join(",")))
        .create_async()
        .await;
    let _second = woc
        .mock("POST", "/txs/status")
        .match_body(mockito::Matcher::Regex(txids[20].clone()))
        .with_status(429)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let provider = WhatsOnChain::with_base_url(Chain::Main, &woc.url());
    let partial = provider.get_status_for_txids(&txids).await.unwrap();
    assert_eq!(partial.results.len(), 20, "{partial:?}");
    assert!(partial.results.iter().all(|d| d.status == "mined"));
    assert!(
        partial
            .error
            .as_deref()
            .is_some_and(|e| e.contains("stopped early") && e.contains("Rate limited")),
        "{partial:?}"
    );

    // Through the ladder: Bitails is asked about the five left out only.
    let mut woc = mockito::Server::new_async().await;
    let _first = woc
        .mock("POST", "/txs/status")
        .match_body(mockito::Matcher::Regex(txids[0].clone()))
        .with_status(200)
        .with_body(format!("[{}]", mined.join(",")))
        .create_async()
        .await;
    let _second = woc
        .mock("POST", "/txs/status")
        .match_body(mockito::Matcher::Regex(txids[20].clone()))
        .with_status(429)
        .create_async()
        .await;
    let mut bitails = mockito::Server::new_async().await;
    let bb = mockito::Server::new_async().await;
    let mut asked = Vec::new();
    for t in &txids[20..] {
        asked.push(
            bitails
                .mock("GET", format!("/tx/{t}").as_str())
                .with_status(200)
                .with_body(format!(r#"{{"txid":"{t}"}}"#))
                .expect(1)
                .create_async()
                .await,
        );
    }
    let never = bitails
        .mock(
            "GET",
            mockito::Matcher::Regex(format!("/tx/({})", txids[..20].join("|"))),
        )
        .expect(0)
        .create_async()
        .await;
    let services = services_with_three(
        ServicesOptions::mainnet(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let result = services.get_status_for_txids(&txids, false).await.unwrap();
    let statuses: Vec<(&str, &str)> = result
        .results
        .iter()
        .map(|d| (d.txid.as_str(), d.status.as_str()))
        .collect();
    let expected: Vec<(&str, &str)> = txids
        .iter()
        .enumerate()
        .map(|(i, t)| (t.as_str(), if i < 20 { "mined" } else { "known" }))
        .collect();
    assert_eq!(statuses, expected);
    assert_eq!(result.error, None, "{result:?}");
    for m in asked {
        m.assert_async().await;
    }
    never.assert_async().await;
}

/// A Bitails 429 is a rate-limit fault at once (review L1): the utxo
/// ladder's retry round does not ask it again, though it re-asks the
/// rungs that faulted otherwise.
#[tokio::test]
async fn a_bitails_429_is_not_asked_again_in_the_retry_round() {
    let f = utxo_services(Says::Fault, Says::Fault, Says::RateLimited).await;
    let result = utxo_status(&f.services).await;
    assert_eq!((result.status.as_str(), result.is_utxo), ("error", None));
    f.woc.1.expect(2).assert_async().await;
    f.bananablocks.1.expect(2).assert_async().await;
    f.bitails.1.expect(1).assert_async().await;
    let bitails = calls(&utxo_history(&f.services), "Bitails");
    assert_eq!(bitails.len(), 1);
    assert_eq!(bitails[0].error.as_ref().unwrap().code, "RATE_LIMITED");
}

/// Two `Services` in one process (the daemon builds two per tenant) share
/// one gate per explorer host: one budget, and a 429 one met closes the
/// rung for the other without a request (review M3).
#[tokio::test]
async fn two_services_share_one_gate_per_host() {
    // Never reached: the gate stops the call before a request.
    let base = "http://127.0.0.1:9/two-services-share-one-gate";
    let options = || ServicesOptions::mainnet().with_bananablocks_base(base);
    let a = Services::with_options(Chain::Main, options()).unwrap();
    let b = Services::with_options(Chain::Main, options()).unwrap();
    let (a_bb, b_bb) = (
        a.bananablocks.as_ref().unwrap(),
        b.bananablocks.as_ref().unwrap(),
    );
    assert!(StdArc::ptr_eq(a_bb.rate_gate(), b_bb.rate_gate()));
    assert!(StdArc::ptr_eq(
        a.whatsonchain.rate_gate(),
        b.whatsonchain.rate_gate()
    ));
    let keyed =
        Services::with_options(Chain::Main, options().with_woc_api_key("another-key")).unwrap();
    assert!(
        !StdArc::ptr_eq(a.whatsonchain.rate_gate(), keyed.whatsonchain.rate_gate()),
        "a key is its own budget"
    );

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
    let _ = a_bb.rate_gate().close(&headers);
    let started = Instant::now();
    assert!(matches!(
        b_bb.get_spend(UTXO_TXID, 0).await,
        Err(Error::RateLimited { .. })
    ));
    assert!(started.elapsed() < Duration::from_millis(500));
}

// =============================================================================
// getUtxoLeads: discovery, not a verdict (the CLI's sync, its review's M1)
// =============================================================================

/// A deposit address's script: P2PKH to `1MVzmKLnHNJ9Dkv7xb8iQUzy2mKrJNzTcH`
/// (the output BananaBlocks served for the probe of 2026-10-22).
const LEAD_SCRIPT_HEX: &str = "76a914e0de61b233403b703ef20fb6b0f5dc0389b5c48a88ac";
const LEAD_ADDRESS: &str = "1MVzmKLnHNJ9Dkv7xb8iQUzy2mKrJNzTcH";

fn lead_txid(c: char) -> String {
    c.to_string().repeat(64)
}

/// The script hash as the explorers' routes carry it.
fn lead_route_hash() -> String {
    route_hash_of(LEAD_SCRIPT_HEX)
}

fn route_hash_of(script_hex: &str) -> String {
    let mut hash = sha256(&hex::decode(script_hex).unwrap());
    hash.reverse();
    hex::encode(hash)
}

/// The script hash as `get_utxo_leads` is asked by default (`HashLE`).
fn lead_hash_le(services: &Services) -> String {
    services.hash_output_script(&hex::decode(LEAD_SCRIPT_HEX).unwrap())
}

/// What a lead source's fixture answers.
enum Lists {
    /// A 200 listing these `(txid char, vout)`, 1000 sats each.
    These(&'static [(char, u32)]),
    /// A 200 listing these `(txid as given, vout)`.
    Raw(&'static [(&'static str, u32)]),
    /// A 200 whose body does not parse.
    Malformed,
    /// This HTTP status (a 429 carries `Retry-After: 30`).
    Status(usize),
}

/// `(txid, vout)` pairs from a fixture's list.
fn outputs_of(lists: &Lists) -> Vec<(String, u32)> {
    match lists {
        Lists::These(outputs) => outputs.iter().map(|(c, v)| (lead_txid(*c), *v)).collect(),
        Lists::Raw(outputs) => outputs.iter().map(|(t, v)| (t.to_string(), *v)).collect(),
        _ => Vec::new(),
    }
}

/// A WhatsOnChain unspent page: `next` is its `nextPageToken`.
fn woc_page(route_hash: &str, outputs: &[(String, u32)], next: Option<&str>) -> String {
    let result: Vec<String> = outputs
        .iter()
        .map(|(txid, vout)| {
            format!(r#"{{"height":900000,"tx_pos":{vout},"tx_hash":"{txid}","value":1000}}"#)
        })
        .collect();
    let next = next
        .map(|t| format!(r#","nextPageToken":"{t}""#))
        .unwrap_or_default();
    format!(
        r#"{{"script":"{route_hash}","result":[{}]{next},"error":""}}"#,
        result.join(",")
    )
}

/// A Bitails unspent page.
fn bitails_page(route_hash: &str, outputs: &[(String, u32)]) -> String {
    let unspent: Vec<String> = outputs
        .iter()
        .map(|(txid, vout)| {
            format!(r#"{{"txid":"{txid}","vout":{vout},"satoshis":1000,"blockheight":900000}}"#)
        })
        .collect();
    format!(
        r#"{{"scripthash":"{route_hash}","unspent":[{}]}}"#,
        unspent.join(",")
    )
}

/// A BananaBlocks address page: `cursor` set means `hasMore`.
fn bananablocks_page(outputs: &[(String, u32)], cursor: Option<&str>) -> String {
    let items: Vec<String> = outputs
        .iter()
        .map(|(txid, vout)| {
            format!(
                r#"{{"txid":"{txid}","vout":{vout},"value":1000,"script_type":"p2pkh","block_height":900000}}"#
            )
        })
        .collect();
    match cursor {
        Some(c) => format!(
            r#"{{"items":[{}],"cursor":"{c}","hasMore":true}}"#,
            items.join(",")
        ),
        None => format!(r#"{{"items":[{}],"hasMore":false}}"#, items.join(",")),
    }
}

/// The first page's route of each explorer, query pinned.
fn woc_first_page(route_hash: &str) -> mockito::Matcher {
    mockito::Matcher::Regex(format!(r"^/script/{route_hash}/unspent/all$"))
}

fn bitails_page_at(route_hash: &str, from: usize) -> mockito::Matcher {
    mockito::Matcher::Regex(format!(
        r"^/scripthash/{route_hash}/unspent\?limit=1000&from={from}$"
    ))
}

fn bananablocks_first_page() -> mockito::Matcher {
    mockito::Matcher::Regex(format!(r"^/address/{LEAD_ADDRESS}/utxos\?limit=1000$"))
}

async fn woc_lists_for(
    server: &mut mockito::ServerGuard,
    route_hash: &str,
    lists: Lists,
) -> mockito::Mock {
    let mock = server.mock("GET", woc_first_page(route_hash));
    let mock = match lists {
        Lists::These(_) | Lists::Raw(_) => {
            mock.with_status(200)
                .with_body(woc_page(route_hash, &outputs_of(&lists), None))
        }
        Lists::Malformed => mock.with_status(200).with_body("<html>busy</html>"),
        Lists::Status(429) => mock.with_status(429).with_header("retry-after", "30"),
        Lists::Status(status) => mock.with_status(status),
    };
    mock.create_async().await
}

async fn bitails_lists_for(
    server: &mut mockito::ServerGuard,
    route_hash: &str,
    lists: Lists,
) -> mockito::Mock {
    let mock = server.mock("GET", bitails_page_at(route_hash, 0));
    let mock = match lists {
        Lists::These(_) | Lists::Raw(_) => mock
            .with_status(200)
            .with_body(bitails_page(route_hash, &outputs_of(&lists))),
        Lists::Malformed => mock.with_status(200).with_body(r#"{"unspent":"soon"}"#),
        Lists::Status(429) => mock.with_status(429).with_header("retry-after", "30"),
        Lists::Status(status) => mock.with_status(status),
    };
    mock.create_async().await
}

async fn bananablocks_lists(server: &mut mockito::ServerGuard, lists: Lists) -> mockito::Mock {
    let mock = server.mock("GET", bananablocks_first_page());
    let mock = match lists {
        Lists::These(_) | Lists::Raw(_) => mock
            .with_status(200)
            .with_body(bananablocks_page(&outputs_of(&lists), None)),
        Lists::Malformed => mock.with_status(200).with_body(r#"{"utxos":[]}"#),
        Lists::Status(429) => mock.with_status(429).with_header("retry-after", "30"),
        Lists::Status(status) => mock.with_status(status),
    };
    mock.create_async().await
}

/// A scene holds three of mockito's pooled servers (a pool of 20 on
/// macOS): a test drops one before it builds the next, or parallel tests
/// wait on the pool for good.
struct LeadScene {
    services: Services,
    /// WhatsOnChain, Bitails, BananaBlocks: a test adds its later pages.
    servers: [mockito::ServerGuard; 3],
    woc: mockito::Mock,
    bitails: mockito::Mock,
    bananablocks: mockito::Mock,
}

async fn lead_scene(woc: Lists, bitails: Lists, bananablocks: Lists) -> LeadScene {
    lead_scene_for(LEAD_SCRIPT_HEX, woc, bitails, bananablocks).await
}

/// A scene whose WhatsOnChain and Bitails list under `script_hex`'s hash
/// (BananaBlocks' fixture stays at the lead address).
async fn lead_scene_for(
    script_hex: &str,
    woc: Lists,
    bitails: Lists,
    bananablocks: Lists,
) -> LeadScene {
    let route_hash = route_hash_of(script_hex);
    let mut w = mockito::Server::new_async().await;
    let mut t = mockito::Server::new_async().await;
    let mut b = mockito::Server::new_async().await;
    let woc = woc_lists_for(&mut w, &route_hash, woc).await;
    let bitails = bitails_lists_for(&mut t, &route_hash, bitails).await;
    let bananablocks = bananablocks_lists(&mut b, bananablocks).await;
    let services = services_with_three(ServicesOptions::mainnet(), &w.url(), &b.url(), &t.url());
    LeadScene {
        services,
        servers: [w, t, b],
        woc,
        bitails,
        bananablocks,
    }
}

/// `(txid char, vout, listed_by)` of each lead, in order.
fn leads_of(leads: &UtxoLeads) -> Vec<(char, u32, Vec<&str>)> {
    leads
        .leads
        .iter()
        .map(|l| {
            (
                l.txid.chars().next().unwrap(),
                l.vout,
                l.listed_by.iter().map(String::as_str).collect(),
            )
        })
        .collect()
}

/// The union keyed by outpoint: WhatsOnChain lists A and B, Bitails B and
/// C; the leads are A, B, C, each naming every explorer that listed it.
/// Asked by script hash, BananaBlocks (which lists by address) is not
/// asked.
#[tokio::test]
async fn the_leads_are_the_union_of_every_explorers_list() {
    let s = lead_scene(
        Lists::These(&[('a', 0), ('b', 1)]),
        Lists::These(&[('b', 1), ('c', 2)]),
        Lists::These(&[('d', 0)]),
    )
    .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(
        leads_of(&leads),
        vec![
            ('a', 0, vec!["WhatsOnChain"]),
            ('b', 1, vec!["WhatsOnChain", "Bitails"]),
            ('c', 2, vec!["Bitails"]),
        ]
    );
    assert_eq!(leads.answered, vec!["WhatsOnChain", "Bitails"]);
    assert!(leads.faults.is_empty(), "{:?}", leads.faults);
    assert_eq!(leads.leads[0].satoshis, Some(1000));
    s.bananablocks.expect(0).assert_async().await;
}

/// One explorer's list is enough to look: WhatsOnChain's 429 is recorded
/// as its fault, not slept on and not asked again, and Bitails' list is
/// the leads.
#[tokio::test]
async fn a_429_leaves_the_other_explorers_list_without_a_sleep() {
    let s = lead_scene(
        Lists::Status(429),
        Lists::These(&[('b', 1), ('c', 2)]),
        Lists::These(&[]),
    )
    .await;
    let started = Instant::now();
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "a 429 is not slept on: {:?}",
        started.elapsed()
    );
    assert_eq!(
        leads_of(&leads),
        vec![('b', 1, vec!["Bitails"]), ('c', 2, vec!["Bitails"])]
    );
    assert_eq!(leads.answered, vec!["Bitails"]);
    assert_eq!(leads.faults.len(), 1, "{:?}", leads.faults);
    assert_eq!(leads.faults[0].0, "WhatsOnChain");
    assert!(
        leads.faults[0].1.contains("Rate limited"),
        "{:?}",
        leads.faults
    );
    s.woc.expect(1).assert_async().await;
}

/// No explorer answered at all: "could not look", an error and never an
/// empty list. The 5xx is transient (asked once more in the retry round),
/// the 429 is not.
#[tokio::test]
async fn every_explorer_faulting_is_could_not_look() {
    let s = lead_scene(Lists::Status(500), Lists::Status(429), Lists::These(&[])).await;
    let err = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap_err();
    match &err {
        Error::NoExplorerAnswered { faults } => {
            let names: Vec<&str> = faults.iter().map(|(n, _)| n.as_str()).collect();
            assert_eq!(names, vec!["WhatsOnChain", "Bitails"], "{faults:?}");
        }
        other => panic!("expected NoExplorerAnswered, got {other:?}"),
    }
    assert!(
        err.to_string()
            .starts_with("Could not look: no explorer answered: "),
        "{err}"
    );
    s.woc.expect(2).assert_async().await;
    s.bitails.expect(1).assert_async().await;
}

/// A malformed list is that explorer's fault (deterministic: not asked
/// again), never an empty list; the other explorer's good list is the
/// leads. Either way round.
#[tokio::test]
async fn a_malformed_list_leaves_the_good_one() {
    let s = lead_scene(
        Lists::Malformed,
        Lists::These(&[('c', 2)]),
        Lists::These(&[]),
    )
    .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(leads_of(&leads), vec![('c', 2, vec!["Bitails"])]);
    assert_eq!(leads.answered, vec!["Bitails"]);
    assert_eq!(leads.faults[0].0, "WhatsOnChain");
    s.woc.expect(1).assert_async().await;

    drop(s.servers);
    let s = lead_scene(
        Lists::These(&[('a', 0)]),
        Lists::Malformed,
        Lists::These(&[]),
    )
    .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(leads_of(&leads), vec![('a', 0, vec!["WhatsOnChain"])]);
    assert_eq!(leads.answered, vec!["WhatsOnChain"]);
    assert_eq!(leads.faults[0].0, "Bitails");
    s.bitails.expect(1).assert_async().await;
}

/// BananaBlocks lists by address: asked with the P2PKH script itself, it
/// is the third lead source (its address route), and its list joins the
/// union; an empty list from an explorer is an answer.
#[tokio::test]
async fn bananablocks_lists_a_p2pkh_script_by_its_address() {
    let s = lead_scene(
        Lists::These(&[('a', 0)]),
        Lists::These(&[]),
        Lists::These(&[('a', 0), ('d', 3)]),
    )
    .await;
    let leads = s
        .services
        .get_utxo_leads(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script))
        .await
        .unwrap();
    assert_eq!(
        leads_of(&leads),
        vec![
            ('a', 0, vec!["WhatsOnChain", "BananaBlocks"]),
            ('d', 3, vec!["BananaBlocks"]),
        ]
    );
    assert_eq!(
        leads.answered,
        vec!["WhatsOnChain", "Bitails", "BananaBlocks"]
    );
    s.bananablocks.expect(1).assert_async().await;

    // WhatsOnChain and Bitails rate limited, BananaBlocks malformed:
    // nothing answered.
    drop(s.servers);
    let s = lead_scene(Lists::Status(429), Lists::Status(429), Lists::Malformed).await;
    let err = s
        .services
        .get_utxo_leads(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::NoExplorerAnswered { faults } if faults.len() == 3),
        "{err:?}"
    );

    // BananaBlocks alone answering is enough to look.
    drop(s.servers);
    let s = lead_scene(
        Lists::Status(429),
        Lists::Status(404),
        Lists::These(&[('d', 3)]),
    )
    .await;
    let leads = s
        .services
        .get_utxo_leads(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script))
        .await
        .unwrap();
    assert_eq!(leads_of(&leads), vec![('d', 3, vec!["BananaBlocks"])]);
    assert_eq!(leads.answered, vec!["BananaBlocks"]);
}

/// A script that is not the exact P2PKH template has no address:
/// BananaBlocks is not asked (no request at all), and WhatsOnChain's and
/// Bitails' lists by the script's hash still answer. An OP_RETURN has no
/// address; a non-minimal push of the lead's hash (`76a94c14<20>88ac`) is
/// another script, whose outputs the address list is not.
#[tokio::test]
async fn a_script_with_no_address_is_not_asked_of_bananablocks() {
    let non_minimal = LEAD_SCRIPT_HEX.replacen("76a914", "76a94c14", 1);
    for script in ["6a0568656c6c6f", non_minimal.as_str()] {
        let mut s = lead_scene_for(
            script,
            Lists::These(&[('a', 0)]),
            Lists::These(&[]),
            Lists::These(&[('d', 3)]),
        )
        .await;
        let any_request = s.servers[2]
            .mock("GET", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let leads = s
            .services
            .get_utxo_leads(script, Some(GetUtxoStatusOutputFormat::Script))
            .await
            .unwrap();
        assert_eq!(leads_of(&leads), vec![('a', 0, vec!["WhatsOnChain"])]);
        assert_eq!(leads.answered, vec!["WhatsOnChain", "Bitails"], "{script}");
        s.bananablocks.expect(0).assert_async().await;
        any_request.assert_async().await;
    }

    let services = Services::mainnet().unwrap();
    assert_eq!(
        services.p2pkh_address(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script)),
        Some(LEAD_ADDRESS.to_string())
    );
    assert_eq!(
        services.p2pkh_address(&non_minimal, Some(GetUtxoStatusOutputFormat::Script)),
        None
    );
    assert_eq!(
        services.p2pkh_address(
            &format!("{LEAD_SCRIPT_HEX}00"),
            Some(GetUtxoStatusOutputFormat::Script)
        ),
        None
    );
    assert_eq!(services.p2pkh_address(&lead_hash_le(&services), None), None);
    // Testnet's version byte (0x6f): the same hash is an `m`/`n` address.
    let testnet = Services::testnet().unwrap();
    assert_eq!(
        testnet.p2pkh_address(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script)),
        Some("n21x4NRm6PjPzsPjgA76EQDHtkvZE1khzd".to_string())
    );
}

/// A txid that is not 64 hex characters makes that explorer's list a
/// malformed answer (deterministic: asked once), never a lead: `zz` and
/// `../x` would be put in a path later. The other explorer's list stands.
#[tokio::test]
async fn a_bad_txid_is_that_explorers_malformed_answer() {
    static ZZ: [(&str, u32); 2] = [
        (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        ),
        ("zz", 0),
    ];
    static DOTS: [(&str, u32); 1] = [("../x", 1)];
    for bad in [&ZZ[..], &DOTS[..]] {
        let s = lead_scene(
            Lists::Raw(bad),
            Lists::These(&[('b', 1)]),
            Lists::These(&[]),
        )
        .await;
        let leads = s
            .services
            .get_utxo_leads(&lead_hash_le(&s.services), None)
            .await
            .unwrap();
        assert_eq!(leads_of(&leads), vec![('b', 1, vec!["Bitails"])]);
        assert_eq!(leads.answered, vec!["Bitails"]);
        assert_eq!(leads.faults.len(), 1, "{:?}", leads.faults);
        assert_eq!(leads.faults[0].0, "WhatsOnChain");
        assert!(
            leads.faults[0].1.contains("not 64 hex characters"),
            "{:?}",
            leads.faults
        );
        s.woc.expect(1).assert_async().await;
    }
}

/// BananaBlocks' list is read by its cursor while `hasMore`: both pages'
/// leads are in the one union, the cursor carried back percent-encoded,
/// and a list that ended is not partial.
#[tokio::test]
async fn bananablocks_pages_by_its_cursor() {
    let mut s = lead_scene(
        Lists::These(&[('a', 0)]),
        Lists::These(&[]),
        Lists::These(&[]),
    )
    .await;
    // The scene's first page gives way to this test's pages.
    s.bananablocks.remove_async().await;
    let first = s.servers[2]
        .mock("GET", bananablocks_first_page())
        .with_status(200)
        .with_body(bananablocks_page(
            &[(lead_txid('a'), 0), (lead_txid('d'), 3)],
            Some("OTcy+/=1"),
        ))
        .expect(1)
        .create_async()
        .await;
    let second = s.servers[2]
        .mock(
            "GET",
            mockito::Matcher::Regex(format!(
                r"^/address/{LEAD_ADDRESS}/utxos\?limit=1000&cursor=OTcy%2B%2F%3D1$"
            )),
        )
        .with_status(200)
        .with_body(bananablocks_page(&[(lead_txid('e'), 4)], None))
        .expect(1)
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script))
        .await
        .unwrap();
    assert_eq!(
        leads_of(&leads),
        vec![
            ('a', 0, vec!["WhatsOnChain", "BananaBlocks"]),
            ('d', 3, vec!["BananaBlocks"]),
            ('e', 4, vec!["BananaBlocks"]),
        ]
    );
    assert!(leads.partial.is_empty(), "{:?}", leads.partial);
    first.assert_async().await;
    second.assert_async().await;
}

/// At the page cap BananaBlocks is in `partial`: still answered, its pages'
/// leads taken, and exactly the cap's pages asked.
#[tokio::test]
async fn bananablocks_at_the_page_cap_is_partial() {
    let mut s = lead_scene(Lists::These(&[]), Lists::These(&[]), Lists::These(&[])).await;
    s.bananablocks.remove_async().await;
    let every_page = s.servers[2]
        .mock(
            "GET",
            mockito::Matcher::Regex(format!(
                r"^/address/{LEAD_ADDRESS}/utxos\?limit=1000(&cursor=more)?$"
            )),
        )
        .with_status(200)
        .with_body(bananablocks_page(&[(lead_txid('d'), 3)], Some("more")))
        .expect(LEAD_PAGES_MAX)
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(LEAD_SCRIPT_HEX, Some(GetUtxoStatusOutputFormat::Script))
        .await
        .unwrap();
    assert_eq!(leads_of(&leads), vec![('d', 3, vec!["BananaBlocks"])]);
    assert_eq!(
        leads.answered,
        vec!["WhatsOnChain", "Bitails", "BananaBlocks"]
    );
    assert_eq!(leads.partial, vec!["BananaBlocks"]);
    assert!(leads.faults.is_empty(), "{:?}", leads.faults);
    every_page.assert_async().await;
}

/// 1000 distinct outputs, `(txid, vout)` with txid `{n:064x}`.
fn a_full_page(offset: usize) -> Vec<(String, u32)> {
    (offset..offset + 1000)
        .map(|n| (format!("{n:064x}"), 0))
        .collect()
}

/// Bitails says nothing of more: a full page (1000) is followed by the
/// next (`from=1000`); a shorter one ends the list. A full page whose next
/// could not be read (a 429) cuts Bitails' list short: Bitails answered,
/// its page's leads are taken, and it is in `partial`, not in `faults`.
/// At the page cap it is partial too.
#[tokio::test]
async fn a_full_bitails_page_is_read_on_or_partial() {
    let route = lead_route_hash();
    // The next page ends the list.
    let mut s = lead_scene(Lists::These(&[]), Lists::These(&[]), Lists::These(&[])).await;
    s.bitails.remove_async().await;
    s.servers[1]
        .mock("GET", bitails_page_at(&route, 0))
        .with_status(200)
        .with_body(bitails_page(&route, &a_full_page(0)))
        .create_async()
        .await;
    let second = s.servers[1]
        .mock("GET", bitails_page_at(&route, 1000))
        .with_status(200)
        .with_body(bitails_page(&route, &[(lead_txid('e'), 4)]))
        .expect(1)
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(leads.leads.len(), 1001);
    assert!(leads.partial.is_empty(), "{:?}", leads.partial);
    second.assert_async().await;

    // The next page is rate limited: cut short.
    drop(s.servers);
    let mut s = lead_scene(Lists::These(&[]), Lists::These(&[]), Lists::These(&[])).await;
    s.bitails.remove_async().await;
    s.servers[1]
        .mock("GET", bitails_page_at(&route, 0))
        .with_status(200)
        .with_body(bitails_page(&route, &a_full_page(0)))
        .create_async()
        .await;
    s.servers[1]
        .mock("GET", bitails_page_at(&route, 1000))
        .with_status(429)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(leads.leads.len(), 1000);
    assert_eq!(leads.answered, vec!["WhatsOnChain", "Bitails"]);
    assert_eq!(leads.partial, vec!["Bitails"]);
    assert!(leads.faults.is_empty(), "{:?}", leads.faults);

    // Every page full: the cap's pages are read, and Bitails is partial.
    drop(s.servers);
    let mut s = lead_scene(Lists::These(&[]), Lists::These(&[]), Lists::These(&[])).await;
    s.bitails.remove_async().await;
    let every_page = s.servers[1]
        .mock(
            "GET",
            mockito::Matcher::Regex(format!(
                r"^/scripthash/{route}/unspent\?limit=1000&from=\d+$"
            )),
        )
        .with_status(200)
        .with_body(bitails_page(&route, &a_full_page(0)))
        .expect(LEAD_PAGES_MAX)
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(leads.leads.len(), 1000);
    assert_eq!(leads.partial, vec!["Bitails"]);
    every_page.assert_async().await;
}

/// WhatsOnChain's list is read by its `nextPageToken` (`?token=`, carried
/// back percent-encoded): both pages' leads are in the union.
#[tokio::test]
async fn whatsonchain_pages_by_its_token() {
    let route = lead_route_hash();
    let mut s = lead_scene(Lists::These(&[]), Lists::These(&[]), Lists::These(&[])).await;
    s.woc.remove_async().await;
    s.servers[0]
        .mock("GET", woc_first_page(&route))
        .with_status(200)
        .with_body(woc_page(&route, &[(lead_txid('a'), 0)], Some("00ff/+")))
        .create_async()
        .await;
    let second = s.servers[0]
        .mock(
            "GET",
            mockito::Matcher::Regex(format!(r"^/script/{route}/unspent/all\?token=00ff%2F%2B$")),
        )
        .with_status(200)
        .with_body(woc_page(&route, &[(lead_txid('b'), 1)], None))
        .expect(1)
        .create_async()
        .await;
    let leads = s
        .services
        .get_utxo_leads(&lead_hash_le(&s.services), None)
        .await
        .unwrap();
    assert_eq!(
        leads_of(&leads),
        vec![
            ('a', 0, vec!["WhatsOnChain"]),
            ('b', 1, vec!["WhatsOnChain"])
        ]
    );
    assert!(leads.partial.is_empty(), "{:?}", leads.partial);
    second.assert_async().await;
}

/// A gate closed by an earlier 429 is skipped without a request: the
/// second look asks WhatsOnChain nothing (its Retry-After of 30 s has not
/// passed), records it as rate limited, and Bitails' list still answers.
#[tokio::test]
async fn a_closed_gate_is_skipped_without_a_request() {
    let s = lead_scene(
        Lists::Status(429),
        Lists::These(&[('b', 1)]),
        Lists::These(&[]),
    )
    .await;
    for _ in 0..2 {
        let leads = s
            .services
            .get_utxo_leads(&lead_hash_le(&s.services), None)
            .await
            .unwrap();
        assert_eq!(leads_of(&leads), vec![('b', 1, vec!["Bitails"])]);
        assert_eq!(leads.faults[0].0, "WhatsOnChain");
        assert!(
            leads.faults[0].1.contains("Rate limited"),
            "{:?}",
            leads.faults
        );
    }
    s.woc.expect(1).assert_async().await;
    s.bitails.expect(2).assert_async().await;
}
