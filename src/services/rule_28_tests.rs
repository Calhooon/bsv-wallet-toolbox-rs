//! Rule 28 witnesses (bsv-stack-lean `docs/p0/rule-28-explorer-calls.md`,
//! section 1): a third-party explorer is a break-glass read. Each test
//! stands a local fixture in an explorer's place and counts the requests
//! that reach it. Nothing here touches the network.

use super::*;
use crate::services::traits::WalletServices;

/// `Services` with both explorers pointed at local fixtures.
fn services_with_explorers(options: ServicesOptions, woc_url: &str, bitails_url: &str) -> Services {
    let mut services = Services::with_options(Chain::Main, options).unwrap();
    services.whatsonchain = StdArc::new(WhatsOnChain::with_base_url(Chain::Main, woc_url));
    services.bitails = StdArc::new(Bitails::with_base_url(Chain::Main, bitails_url));
    services
}

/// A header service's frame around a header at `height`.
fn header_frame(height: u32) -> String {
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

// =============================================================================
// Item 1 (T1, T2): the tip height is the header service's
// =============================================================================

/// The two explorers' tip routes, each expecting no request.
async fn explorer_tips_never_asked(
    woc: &mut mockito::ServerGuard,
    bitails: &mut mockito::ServerGuard,
) -> (mockito::Mock, mockito::Mock) {
    let w = woc
        .mock("GET", "/chain/info")
        .with_status(200)
        .with_body(r#"{"chain":"main","blocks":111,"headers":111,"bestblockhash":"00"}"#)
        .expect(0)
        .create_async()
        .await;
    let b = bitails
        .mock("GET", "/network/info")
        .with_status(200)
        .with_body(r#"{"blocks":222}"#)
        .expect(0)
        .create_async()
        .await;
    (w, b)
}

/// T1, T2: the tip height is the header service's tip header's height, the
/// same source as the tip hash. No explorer is asked while it answers.
#[tokio::test]
async fn get_height_is_the_header_services_tip_and_asks_no_explorer() {
    let mut ct = mockito::Server::new_async().await;
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let tip = ct
        .mock("GET", "/findChainTipHeaderHex")
        .with_status(200)
        .with_body(header_frame(900_001))
        .expect(1)
        .create_async()
        .await;
    let (w, b) = explorer_tips_never_asked(&mut woc, &mut bitails).await;

    let services = services_with_explorers(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bitails.url(),
    );
    assert_eq!(services.get_height().await.unwrap(), 900_001);
    tip.assert_async().await;
    w.assert_async().await;
    b.assert_async().await;
}

/// T1, T2: with no header service the height is an error ("could not
/// look"), never an explorer's number.
#[tokio::test]
async fn get_height_without_a_header_service_is_an_error_not_an_explorers_word() {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let (w, b) = explorer_tips_never_asked(&mut woc, &mut bitails).await;

    let services = services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
    let err = services
        .get_height()
        .await
        .expect_err("no header service: no height");
    assert!(
        err.to_string().contains("header service"),
        "the error names what is missing: {err}"
    );
    w.assert_async().await;
    b.assert_async().await;
}
