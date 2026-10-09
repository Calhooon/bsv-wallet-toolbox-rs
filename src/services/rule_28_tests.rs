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

// =============================================================================
// Item 5 (T10): an output's spend, the irreducible case
// =============================================================================
//
// Headers and proofs prove inclusion, never that an output is unspent, so
// the unspent set of a script is asked of the explorers. The shape: each is
// the other's fallback, the start rotates, a negative needs both, and
// "could not look" is never "spent".

const UTXO_TXID: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const UTXO_SCRIPT: &[u8] = &[0x76, 0xa9, 0x14, 0x01, 0x02, 0x88, 0xac];

/// The script hash of `UTXO_SCRIPT` as the explorers' routes carry it.
fn utxo_script_hash_be() -> String {
    let mut hash = sha256(UTXO_SCRIPT);
    hash.reverse();
    hex::encode(hash)
}

/// What an explorer fixture says about the script's unspent set.
#[derive(Clone, Copy)]
enum Says {
    /// The outpoint `UTXO_TXID:0` is in the unspent set.
    Unspent,
    /// The unspent set is empty.
    NotListed,
    /// HTTP 500.
    Fault,
}

async fn woc_unspent(server: &mut mockito::ServerGuard, says: Says) -> mockito::Mock {
    let path = format!("/script/{}/unspent/all", utxo_script_hash_be());
    let mock = server.mock("GET", path.as_str());
    let mock = match says {
        Says::Unspent => mock.with_status(200).with_body(format!(
            r#"{{"script":"{}","result":[{{"height":900000,"tx_pos":0,"tx_hash":"{}","value":1000}}]}}"#,
            utxo_script_hash_be(),
            UTXO_TXID
        )),
        Says::NotListed => mock.with_status(200).with_body(format!(
            r#"{{"script":"{}","result":[]}}"#,
            utxo_script_hash_be()
        )),
        Says::Fault => mock.with_status(500),
    };
    mock.create_async().await
}

async fn bitails_unspent(server: &mut mockito::ServerGuard, says: Says) -> mockito::Mock {
    let path = format!("/scripthash/{}/unspent", utxo_script_hash_be());
    let mock = server.mock("GET", path.as_str());
    let mock = match says {
        Says::Unspent => mock.with_status(200).with_body(format!(
            r#"{{"scripthash":"{}","unspent":[{{"txid":"{}","vout":0,"satoshis":1000,"blockheight":900000}}]}}"#,
            utxo_script_hash_be(),
            UTXO_TXID
        )),
        Says::NotListed => mock.with_status(200).with_body(format!(
            r#"{{"scripthash":"{}","unspent":[]}}"#,
            utxo_script_hash_be()
        )),
        Says::Fault => mock.with_status(500),
    };
    mock.create_async().await
}

/// `Services` over two explorer fixtures that answer as given.
async fn utxo_services(
    woc_says: Says,
    bitails_says: Says,
) -> (
    Services,
    (mockito::ServerGuard, mockito::Mock),
    (mockito::ServerGuard, mockito::Mock),
) {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let w = woc_unspent(&mut woc, woc_says).await;
    let b = bitails_unspent(&mut bitails, bitails_says).await;
    let services = services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
    (services, (woc, w), (bitails, b))
}

async fn utxo_status(services: &Services) -> GetUtxoStatusResult {
    let hash = services.hash_output_script(UTXO_SCRIPT);
    let outpoint = format!("{}.0", UTXO_TXID);
    services
        .get_utxo_status(&hash, None, Some(&outpoint), false)
        .await
        .unwrap()
}

/// T10: one explorer's "not in the unspent list" is not "spent" while the
/// other could not look.
#[tokio::test]
async fn one_explorers_negative_is_not_spent_while_the_other_could_not_look() {
    let (services, _woc, _bitails) = utxo_services(Says::NotListed, Says::Fault).await;
    let result = utxo_status(&services).await;
    assert_ne!(
        result.is_utxo,
        Some(false),
        "a negative needs the second provider: {result:?}"
    );
    assert_eq!(result.status, "error", "could not look: {result:?}");
}

/// T10: the second explorer is asked after a negative, and its positive
/// stands.
#[tokio::test]
async fn the_second_explorers_positive_stands_after_a_negative() {
    let (services, _woc, _bitails) = utxo_services(Says::NotListed, Says::Unspent).await;
    let result = utxo_status(&services).await;
    assert_eq!(result.status, "success", "{result:?}");
    assert_eq!(result.is_utxo, Some(true), "{result:?}");
}

/// T10: two explorers that both leave the outpoint out of the unspent set
/// are the negative.
#[tokio::test]
async fn two_explorers_negatives_are_spent() {
    let (services, _woc, _bitails) = utxo_services(Says::NotListed, Says::NotListed).await;
    let result = utxo_status(&services).await;
    assert_eq!(result.status, "success", "{result:?}");
    assert_eq!(result.is_utxo, Some(false), "{result:?}");
}

/// T10: the start rotates. With both explorers listing the outpoint, each
/// call ends at its first answer, and two calls reach one explorer each.
#[tokio::test]
async fn the_utxo_read_rotates_its_starting_explorer() {
    let (services, (_woc, w), (_bitails, b)) = utxo_services(Says::Unspent, Says::Unspent).await;
    assert_eq!(utxo_status(&services).await.is_utxo, Some(true));
    assert_eq!(utxo_status(&services).await.is_utxo, Some(true));
    w.expect(1).assert_async().await;
    b.expect(1).assert_async().await;
}

/// T10: an outage is "could not look", never "spent" (`is_utxo` answered
/// `false` for it, and four callers read that as spent).
#[tokio::test]
async fn an_outage_is_could_not_look_never_spent() {
    let (services, _woc, _bitails) = utxo_services(Says::Fault, Says::Fault).await;
    let answer = services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_eq!(answer, UtxoVerdict::Unknown, "an outage read as {answer:?}");
}

/// T10: the three answers of `is_utxo`, one per row of what the two
/// explorers say.
#[tokio::test]
async fn is_utxo_gives_the_three_answers() {
    for (woc_says, bitails_says, expected) in [
        (Says::Unspent, Says::Fault, UtxoVerdict::Unspent),
        (Says::NotListed, Says::Unspent, UtxoVerdict::Unspent),
        (Says::NotListed, Says::NotListed, UtxoVerdict::Spent),
        (Says::NotListed, Says::Fault, UtxoVerdict::Unknown),
        (Says::Fault, Says::NotListed, UtxoVerdict::Unknown),
    ] {
        let (services, _woc, _bitails) = utxo_services(woc_says, bitails_says).await;
        assert_eq!(services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await, expected);
    }
}

/// T10: a Bitails body that is not an unspent list, and a list long enough
/// to be one page of more, are "could not look", never an empty set.
#[tokio::test]
async fn a_bitails_answer_that_is_not_a_whole_unspent_list_is_could_not_look() {
    let hash = hex::encode(sha256(UTXO_SCRIPT));
    let outpoint = format!("{}.0", UTXO_TXID);
    let path = format!("/scripthash/{}/unspent", utxo_script_hash_be());

    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", path.as_str())
        .with_status(200)
        .with_body(r#"{"scripthash":"00","result":[]}"#)
        .create_async()
        .await;
    let provider = Bitails::with_base_url(Chain::Main, &server.url());
    assert!(
        provider
            .get_utxo_status(&hash, None, Some(&outpoint))
            .await
            .is_err(),
        "a body with no unspent list is a fault"
    );

    let mut server = mockito::Server::new_async().await;
    let page: Vec<String> = (0..crate::services::providers::bitails::BITAILS_UNSPENT_PAGE)
        .map(|i| {
            format!(
                r#"{{"txid":"{}","vout":{},"satoshis":1}}"#,
                "22".repeat(32),
                i
            )
        })
        .collect();
    let _m = server
        .mock("GET", path.as_str())
        .with_status(200)
        .with_body(format!(r#"{{"unspent":[{}]}}"#, page.join(",")))
        .create_async()
        .await;
    let provider = Bitails::with_base_url(Chain::Main, &server.url());
    let result = provider
        .get_utxo_status(&hash, None, Some(&outpoint))
        .await
        .unwrap();
    assert_eq!(result.status, "error");
    assert_eq!(result.is_utxo, None);

    let mut server = mockito::Server::new_async().await;
    let _m = server
        .mock("GET", path.as_str())
        .with_status(404)
        .create_async()
        .await;
    let provider = Bitails::with_base_url(Chain::Main, &server.url());
    let result = provider
        .get_utxo_status(&hash, None, Some(&outpoint))
        .await
        .unwrap();
    assert_eq!(result.status, "error", "a 404 is not an empty set");
}
