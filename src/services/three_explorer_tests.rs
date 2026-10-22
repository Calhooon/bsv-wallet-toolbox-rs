//! The three explorers' ladders (0.12.0; the owner's word of 2026-10-22,
//! bsv-wallet-cli#50): WhatsOnChain, BananaBlocks and Bitails are each the
//! others' fallback; a fault (a 429, a timeout, a 5xx, a malformed body) is
//! "could not look" and moves the question to the next rung within the same
//! call, never a negative; a 429 is not slept on, and its rung is skipped
//! without a request until Retry-After passes. Local fixtures only.

use super::rule_28_tests::{
    a_spender_of, services_with_three, utxo_services, utxo_status, Says, UTXO_SCRIPT, UTXO_TXID,
};
use super::*;
use crate::services::providers::WhatsOnChainConfig;
use crate::services::traits::WalletServices;
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

/// A 429 on the first rung reaches the second within the same call, with
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
        ("success", Some(true), "BananaBlocks"),
        "{result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1000),
        "a 429 is not slept on: {elapsed:?}"
    );
    f.woc.1.expect(1).assert_async().await;
    f.bitails.1.expect(0).assert_async().await;

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
// spend_evidence: BananaBlocks' spentTxid beside WhatsOnChain's spent route
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
        .mock("GET", format!("/txo/{UTXO_TXID}/0/spend").as_str())
        .with_status(200)
        .with_body(format!(r#"{{"spent":true,"spentTxid":"{spender}"}}"#))
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
    // Once as the unspent question's rung, once as the spender's name.
    names.expect(2).assert_async().await;
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
        .mock("GET", format!("/txo/{UTXO_TXID}/0/spend").as_str())
        .with_status(200)
        .with_body(format!(r#"{{"spent":true,"spentTxid":"{spender}"}}"#))
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
