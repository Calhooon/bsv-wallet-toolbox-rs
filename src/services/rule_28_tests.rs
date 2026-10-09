//! Rule 28 witnesses (bsv-stack-lean `docs/p0/rule-28-explorer-calls.md`,
//! section 1): a third-party explorer is a break-glass read. Each test
//! stands a local fixture in an explorer's place and counts the requests
//! that reach it. Nothing here touches the network.

use super::*;
use crate::services::traits::WalletServices;

/// `Services` with both explorers pointed at local fixtures, in every
/// collection and in the break-glass tracker.
fn services_with_explorers(options: ServicesOptions, woc_url: &str, bitails_url: &str) -> Services {
    Services::with_explorers(
        Chain::Main,
        options,
        StdArc::new(WhatsOnChain::with_base_url(Chain::Main, woc_url)),
        StdArc::new(Bitails::with_base_url(Chain::Main, bitails_url)),
    )
    .unwrap()
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

// =============================================================================
// Item 2 (T13, T14): the script hash history is a chain scan
// =============================================================================

/// T13, T14: a default build wires no script hash history provider. A wallet
/// learns of an output by being handed a BEEF; every transaction that ever
/// touched a script is a chain scan, and it exists only under the cargo
/// feature `break-glass-script-history`.
#[cfg(not(feature = "break-glass-script-history"))]
#[test]
fn a_default_build_wires_no_script_hash_history_scan() {
    let services = Services::mainnet().unwrap();
    let history = services.get_services_call_history(false).unwrap();
    assert!(
        history.get_script_hash_history.is_none(),
        "no scan provider is wired without the break-glass feature"
    );
}

/// The break-glass scan still answers when it is built in, from the first
/// explorer that can.
#[cfg(feature = "break-glass-script-history")]
#[tokio::test]
async fn the_break_glass_scan_answers_when_built_in() {
    let mut woc = mockito::Server::new_async().await;
    let bitails = mockito::Server::new_async().await;
    let hash_le = format!("{}{}", "00".repeat(31), "01");
    let hash_be = format!("01{}", "00".repeat(31));
    let _confirmed = woc
        .mock(
            "GET",
            format!("/script/{}/confirmed/history", hash_be).as_str(),
        )
        .with_status(200)
        .with_body(format!(
            r#"{{"result":[{{"tx_hash":"{}","height":900000}}]}}"#,
            "ab".repeat(32)
        ))
        .create_async()
        .await;
    let _unconfirmed = woc
        .mock(
            "GET",
            format!("/script/{}/unconfirmed/history", hash_be).as_str(),
        )
        .with_status(404)
        .create_async()
        .await;

    let services = services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
    let result = services
        .get_script_hash_history(&hash_le, false)
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.history.len(), 1);
    assert!(services
        .get_services_call_history(false)
        .unwrap()
        .get_script_hash_history
        .is_some());
}

// =============================================================================
// Item 3 (T15): no explorer tip, header or root beside the proof path
// =============================================================================

/// T15: Bitails is asked whether it holds a transaction, and nothing about
/// the tip. Its status answer read `network/info` to turn a block height
/// into a confirmation count; the header service holds the tip, and the
/// answer that matters ("in a block") is in the transaction's own record.
#[tokio::test]
async fn the_bitails_status_read_asks_no_explorer_for_the_tip() {
    let mut bitails = mockito::Server::new_async().await;
    let txid = "ab".repeat(32);
    let tip = bitails
        .mock("GET", "/network/info")
        .with_status(200)
        .with_body(r#"{"blocks":900010}"#)
        .expect(0)
        .create_async()
        .await;
    let _tx = bitails
        .mock("GET", format!("/tx/{}", txid).as_str())
        .with_status(200)
        .with_body(format!(
            r#"{{"txid":"{}","blockHash":"{}","blockHeight":900000}}"#,
            txid,
            "cd".repeat(32)
        ))
        .create_async()
        .await;

    let provider = Bitails::with_base_url(Chain::Main, &bitails.url());
    let result = provider
        .get_status_for_txids(std::slice::from_ref(&txid))
        .await
        .unwrap();
    assert_eq!(result.results[0].status, "mined");
    assert_eq!(
        result.results[0].depth,
        Some(1),
        "in a block by its own record: at least one deep, no tip read"
    );
    tip.assert_async().await;
}
