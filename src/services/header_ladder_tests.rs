//! The header ladders (`get_chain_tip_header`, `get_header_for_height`):
//! chaintracks, then BHS. When no rung gives a header the answer names each
//! configured rung's error; "no header service configured" only when
//! neither is configured (bsv-wallet-toolbox-rs#47). Each rung is a local
//! fixture; nothing here touches the network.

use super::*;
use crate::services::traits::WalletServices;

const HEIGHT: u32 = 900_002;

/// A chaintracks frame around a header at `height`.
fn chaintracks_frame(height: u32) -> String {
    serde_json::json!({
        "status": "success",
        "value": {
            "version": 536870912u32,
            "previousHash": "0".repeat(64),
            "merkleRoot": "a".repeat(64),
            "time": 1700000000u32,
            "bits": 402917821u32,
            "nonce": 7u32,
            "height": height,
            "hash": "b".repeat(64),
        }
    })
    .to_string()
}

/// A BHS header at `height`, with `nonce` to tell it from another rung's.
fn bhs_header(height: u32, nonce: u32) -> BlockHeader {
    BlockHeader {
        version: 536870912,
        previous_hash: "0".repeat(64),
        merkle_root: "c".repeat(64),
        time: 1700000000,
        bits: 402917821,
        nonce,
        hash: "d".repeat(64),
        height,
    }
}

fn services(chaintracks: Option<&str>, bhs: Option<&str>) -> Services {
    let mut options = ServicesOptions::mainnet();
    if let Some(url) = chaintracks {
        options = options.with_chaintracks_url(url);
    }
    if let Some(url) = bhs {
        options = options.with_bhs_url(url);
    }
    Services::with_options(Chain::Main, options).unwrap()
}

async fn failing(
    server: &mut crate::services::MockServer,
    path: &str,
    status: usize,
) -> mockito::Mock {
    server
        .mock("GET", path)
        .with_status(status)
        .expect(1)
        .create_async()
        .await
}

async fn answering(
    server: &mut crate::services::MockServer,
    path: &str,
    body: String,
) -> mockito::Mock {
    server
        .mock("GET", path)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body)
        .expect(1)
        .create_async()
        .await
}

fn ct_tip() -> String {
    "/findChainTipHeaderHex".to_string()
}

fn ct_height() -> String {
    format!("/findHeaderHexForHeight?height={HEIGHT}")
}

fn bhs_tip() -> String {
    "/api/v1/chain/header/tip".to_string()
}

fn bhs_height() -> String {
    format!("/api/v1/chain/header/byHeight?height={HEIGHT}")
}

// =============================================================================
// get_chain_tip_header
// =============================================================================

#[tokio::test]
async fn a_failing_chaintracks_tip_is_its_error_not_not_configured() {
    let mut ct = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_tip(), 503).await;
    let err = services(Some(&ct.url()), None)
        .get_chain_tip_header()
        .await
        .expect_err("the only rung failed");
    let text = err.to_string();
    assert!(
        text.contains("get_chain_tip_header: no header service answered (chaintracks: "),
        "{text}"
    );
    assert!(text.contains("HTTP 503"), "the rung's error: {text}");
    assert!(!text.contains("service configured"), "{text}");
    m.assert_async().await;
}

#[tokio::test]
async fn no_header_service_tip_is_not_configured() {
    let err = services(None, None)
        .get_chain_tip_header()
        .await
        .expect_err("nothing configured");
    assert!(
        err.to_string()
            .contains("get_chain_tip_header: no header service configured"),
        "{err}"
    );
}

#[tokio::test]
async fn a_failing_chaintracks_tip_then_bhs_answers_the_header() {
    let mut ct = crate::services::mock_server().await;
    let mut bhs = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_tip(), 500).await;
    let b = answering(
        &mut bhs,
        &bhs_tip(),
        serde_json::to_string(&bhs_header(HEIGHT, 42)).unwrap(),
    )
    .await;
    let header = services(Some(&ct.url()), Some(&bhs.url()))
        .get_chain_tip_header()
        .await
        .expect("BHS answers after chaintracks failed");
    assert_eq!(header.height, HEIGHT);
    assert_eq!(header.nonce, 42);
    m.assert_async().await;
    b.assert_async().await;
}

#[tokio::test]
async fn both_tip_rungs_failing_names_both() {
    let mut ct = crate::services::mock_server().await;
    let mut bhs = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_tip(), 502).await;
    let b = failing(&mut bhs, &bhs_tip(), 504).await;
    let err = services(Some(&ct.url()), Some(&bhs.url()))
        .get_chain_tip_header()
        .await
        .expect_err("both rungs failed");
    let text = err.to_string();
    assert!(text.contains("(chaintracks: "), "{text}");
    assert!(text.contains("HTTP 502"), "{text}");
    assert!(text.contains("; bhs: "), "{text}");
    assert!(text.contains("HTTP 504"), "{text}");
    m.assert_async().await;
    b.assert_async().await;
}

// =============================================================================
// get_header_for_height
// =============================================================================

#[tokio::test]
async fn a_failing_chaintracks_header_is_its_error_not_not_configured() {
    let mut ct = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_height(), 503).await;
    let err = services(Some(&ct.url()), None)
        .get_header_for_height(HEIGHT)
        .await
        .expect_err("the only rung failed");
    let text = err.to_string();
    assert!(
        text.contains("get_header_for_height: no header service answered (chaintracks: "),
        "{text}"
    );
    assert!(text.contains("HTTP 503"), "the rung's error: {text}");
    assert!(!text.contains("service configured"), "{text}");
    m.assert_async().await;
}

#[tokio::test]
async fn no_header_service_header_is_not_configured() {
    let err = services(None, None)
        .get_header_for_height(HEIGHT)
        .await
        .expect_err("nothing configured");
    assert!(
        err.to_string()
            .contains("get_header_for_height: no header service configured"),
        "{err}"
    );
}

#[tokio::test]
async fn a_failing_chaintracks_header_then_bhs_answers_the_header() {
    let mut ct = crate::services::mock_server().await;
    let mut bhs = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_height(), 500).await;
    let header = bhs_header(HEIGHT, 42);
    let b = answering(
        &mut bhs,
        &bhs_height(),
        serde_json::to_string(&header).unwrap(),
    )
    .await;
    let bytes = services(Some(&ct.url()), Some(&bhs.url()))
        .get_header_for_height(HEIGHT)
        .await
        .expect("BHS answers after chaintracks failed");
    assert_eq!(bytes, header.to_binary());
    m.assert_async().await;
    b.assert_async().await;
}

#[tokio::test]
async fn both_header_rungs_failing_names_both() {
    let mut ct = crate::services::mock_server().await;
    let mut bhs = crate::services::mock_server().await;
    let m = failing(&mut ct, &ct_height(), 502).await;
    let b = failing(&mut bhs, &bhs_height(), 504).await;
    let err = services(Some(&ct.url()), Some(&bhs.url()))
        .get_header_for_height(HEIGHT)
        .await
        .expect_err("both rungs failed");
    let text = err.to_string();
    assert!(text.contains("(chaintracks: "), "{text}");
    assert!(text.contains("HTTP 502"), "{text}");
    assert!(text.contains("; bhs: "), "{text}");
    assert!(text.contains("HTTP 504"), "{text}");
    m.assert_async().await;
    b.assert_async().await;
}

/// The chaintracks rung answering is the header; BHS is not asked.
#[tokio::test]
async fn an_answering_chaintracks_asks_no_bhs() {
    let mut ct = crate::services::mock_server().await;
    let mut bhs = crate::services::mock_server().await;
    let m = answering(&mut ct, &ct_height(), chaintracks_frame(HEIGHT)).await;
    let b = bhs
        .mock("GET", bhs_height().as_str())
        .expect(0)
        .create_async()
        .await;
    services(Some(&ct.url()), Some(&bhs.url()))
        .get_header_for_height(HEIGHT)
        .await
        .expect("chaintracks answers");
    m.assert_async().await;
    b.assert_async().await;
}
