//! Rule 28 witnesses (bsv-stack-lean `docs/p0/rule-28-explorer-calls.md`,
//! section 1): a third-party explorer is a break-glass read. Each test
//! stands a local fixture in an explorer's place and counts the requests
//! that reach it. Nothing here touches the network.

use super::*;
use crate::services::traits::{BlockHeader, WalletServices};

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

/// T10: the answers of `is_utxo` from the explorers' unspent sets alone,
/// one per row of what the two say. Their agreement on a negative is the
/// hint tier, never `Spent` (the second pass).
#[tokio::test]
async fn is_utxo_gives_the_three_answers() {
    for (woc_says, bitails_says, expected) in [
        (Says::Unspent, Says::Fault, UtxoVerdict::UnspentHint),
        (Says::NotListed, Says::Unspent, UtxoVerdict::UnspentHint),
        (Says::NotListed, Says::NotListed, UtxoVerdict::SpentHint),
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

// =============================================================================
// Item 7 (T3, T4): a header by hash under break-glass
// =============================================================================
//
// The header service holds every header; with it unreachable nothing else
// we run does, so under `break_glass_explorer_headers` the explorers are
// asked, each the other's fallback.

/// A header (its 80 bytes as hex, its fields and its hash) at `height`.
fn a_header(height: u32, nonce: u32) -> (String, BlockHeader) {
    let mut header = BlockHeader {
        version: 536870912,
        previous_hash: "11".repeat(32),
        merkle_root: "22".repeat(32),
        time: 1700000000,
        bits: 402917821,
        nonce,
        hash: String::new(),
        height,
    };
    let bytes = header.to_binary();
    let mut hash = sha256(&sha256(&bytes));
    hash.reverse();
    header.hash = hex::encode(hash);
    (hex::encode(bytes), header)
}

fn woc_header_json(header: &BlockHeader) -> String {
    serde_json::json!({
        "hash": header.hash,
        "height": header.height,
        "version": header.version,
        "merkleroot": header.merkle_root,
        "time": header.time,
        "nonce": header.nonce,
        "bits": format!("{:08x}", header.bits),
        "previousblockhash": header.previous_hash,
    })
    .to_string()
}

fn bitails_block_json(header: &BlockHeader, bytes_hex: &str) -> String {
    serde_json::json!({
        "hash": header.hash,
        "height": header.height,
        "header": bytes_hex,
    })
    .to_string()
}

/// `Services` under break-glass with an unreachable header service.
fn break_glass_services(woc_url: &str, bitails_url: &str) -> Services {
    services_with_explorers(
        ServicesOptions::mainnet()
            .with_chaintracks_url("http://127.0.0.1:9")
            .with_break_glass_explorer_headers(true),
        woc_url,
        bitails_url,
    )
}

/// T3, T4: a WhatsOnChain fault falls through to Bitails (it returned
/// through `?` and Bitails was never asked).
#[tokio::test]
async fn a_whatsonchain_fault_falls_through_to_bitails_for_a_header() {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let (bytes, header) = a_header(900_000, 1);
    let _w = woc
        .mock("GET", format!("/block/{}/header", header.hash).as_str())
        .with_status(500)
        .create_async()
        .await;
    let _b = bitails
        .mock("GET", format!("/block/{}", header.hash).as_str())
        .with_status(200)
        .with_body(bitails_block_json(&header, &bytes))
        .create_async()
        .await;

    let services = break_glass_services(&woc.url(), &bitails.url());
    let got = services
        .hash_to_header(&header.hash)
        .await
        .expect("Bitails answers when WhatsOnChain faults");
    assert_eq!(got.hash, header.hash);
    assert_eq!(got.height, 900_000);
    assert_eq!(got.merkle_root, header.merkle_root);
}

/// T3, T4: the start rotates. With both explorers holding the header, two
/// reads reach one explorer each.
#[tokio::test]
async fn the_header_read_rotates_its_starting_explorer() {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let (bytes, header) = a_header(900_000, 2);
    let w = woc
        .mock("GET", format!("/block/{}/header", header.hash).as_str())
        .with_status(200)
        .with_body(woc_header_json(&header))
        .expect(1)
        .create_async()
        .await;
    let b = bitails
        .mock("GET", format!("/block/{}", header.hash).as_str())
        .with_status(200)
        .with_body(bitails_block_json(&header, &bytes))
        .expect(1)
        .create_async()
        .await;

    let services = break_glass_services(&woc.url(), &bitails.url());
    for _ in 0..2 {
        let got = services.hash_to_header(&header.hash).await.unwrap();
        assert_eq!(got.height, 900_000);
    }
    w.assert_async().await;
    b.assert_async().await;
}

/// T3, T4: "no such header" needs both explorers; one saying so while the
/// other could not look is an error, and a header that does not hash to
/// the hash asked for is no answer.
#[tokio::test]
async fn a_missing_header_needs_both_explorers_and_the_answer_is_bound_to_the_hash() {
    let (_bytes, header) = a_header(900_000, 3);
    let (other_bytes, other) = a_header(900_000, 4);
    let woc_path = format!("/block/{}/header", header.hash);
    let bitails_path = format!("/block/{}", header.hash);

    // Both say there is no such header: NotFound.
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("GET", woc_path.as_str())
        .with_status(404)
        .create_async()
        .await;
    let _b = bitails
        .mock("GET", bitails_path.as_str())
        .with_status(404)
        .create_async()
        .await;
    let services = break_glass_services(&woc.url(), &bitails.url());
    for _ in 0..2 {
        let err = services.hash_to_header(&header.hash).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err}");
    }

    // One says so, the other could not look: not NotFound, on either start.
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("GET", woc_path.as_str())
        .with_status(404)
        .create_async()
        .await;
    let _b = bitails
        .mock("GET", bitails_path.as_str())
        .with_status(500)
        .create_async()
        .await;
    let services = break_glass_services(&woc.url(), &bitails.url());
    for _ in 0..2 {
        let err = services.hash_to_header(&header.hash).await.unwrap_err();
        assert!(!matches!(err, Error::NotFound { .. }), "{err}");
    }

    // Each explorer answers with another block's header: no answer.
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = woc
        .mock("GET", woc_path.as_str())
        .with_status(200)
        .with_body(woc_header_json(&other))
        .create_async()
        .await;
    let _b = bitails
        .mock("GET", bitails_path.as_str())
        .with_status(200)
        .with_body(bitails_block_json(&other, &other_bytes))
        .create_async()
        .await;
    let services = break_glass_services(&woc.url(), &bitails.url());
    let answer = services.hash_to_header(&header.hash).await;
    assert!(
        answer.is_err(),
        "another block's header was taken: {answer:?}"
    );
}

// =============================================================================
// Item 8 (T6, T7): no explorer's proof without a header service
// =============================================================================

/// T6, T7: a proof nobody pushed to us is fetched from an explorer as a
/// courier and believed only for its root, checked against the header
/// service. With no header service there is nothing to check it against,
/// so the explorers are not asked and no proof is returned.
#[tokio::test]
async fn no_proof_is_fetched_from_an_explorer_without_a_header_service() {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let txid = "ab".repeat(32);
    let w = woc
        .mock("GET", format!("/tx/{}/proof/tsc", txid).as_str())
        .with_status(200)
        .with_body("[]")
        .expect(0)
        .create_async()
        .await;
    let b = bitails
        .mock("GET", format!("/tx/{}/proof/tsc", txid).as_str())
        .with_status(404)
        .expect(0)
        .create_async()
        .await;

    let services = services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
    let result = services.get_merkle_path(&txid, false).await.unwrap();
    assert_eq!(result.merkle_path, None);
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("header service")),
        "{:?}",
        result.error
    );
    w.assert_async().await;
    b.assert_async().await;
}

// =============================================================================
// Item 9 (T8, T9): a transaction's bytes, "not found" apart from "fault"
// =============================================================================

async fn raw_tx_fixture(
    server: &mut mockito::ServerGuard,
    txid: &str,
    status: usize,
) -> mockito::Mock {
    server
        .mock("GET", format!("/tx/{}/hex", txid).as_str())
        .with_status(status)
        .create_async()
        .await
}

/// T8, T9: one explorer could not look and the other has no such
/// transaction. That is not "not found": the fault is kept, whichever of
/// the two it came from (a later 404 used to erase an earlier fault).
#[tokio::test]
async fn a_raw_tx_fault_is_not_erased_by_the_other_explorers_not_found() {
    let txid = "ab".repeat(32);
    for (woc_status, bitails_status) in [(500, 404), (404, 500)] {
        let mut woc = mockito::Server::new_async().await;
        let mut bitails = mockito::Server::new_async().await;
        let _w = raw_tx_fixture(&mut woc, &txid, woc_status).await;
        let _b = raw_tx_fixture(&mut bitails, &txid, bitails_status).await;
        let services =
            services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
        let result = services.get_raw_tx(&txid, false).await.unwrap();
        assert!(result.raw_tx.is_none());
        assert!(
            result.error.is_some(),
            "WoC {woc_status}, Bitails {bitails_status}: the fault is reported: {result:?}"
        );
        assert!(result.could_not_look, "{result:?}");
        assert!(!result.is_not_found(), "{result:?}");
    }
}

/// T8, T9: both explorers answering "no such transaction" is "not found",
/// with no error.
#[tokio::test]
async fn a_raw_tx_both_explorers_lack_is_not_found() {
    let txid = "ab".repeat(32);
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let _w = raw_tx_fixture(&mut woc, &txid, 404).await;
    let _b = raw_tx_fixture(&mut bitails, &txid, 404).await;
    let services = services_with_explorers(ServicesOptions::mainnet(), &woc.url(), &bitails.url());
    let result = services.get_raw_tx(&txid, false).await.unwrap();
    assert!(result.raw_tx.is_none());
    assert!(result.error.is_none(), "{result:?}");
    assert!(result.is_not_found(), "{result:?}");
}

// =============================================================================
// The second pass (0.6.0): a stranger's spend is a chain fact only by proof
// =============================================================================
//
// The owner's ruling of 2026-10-09 (bsv-stack-lean `NORTH-STAR.md`): a
// stranger's spend of our output becomes a chain fact only by the spending
// transaction's merkle proof checked against our headers. A provider's
// "spent by X" is a word; X's own bytes naming the outpoint are a fact; X's
// merkle path against the header service is the chain's word. Two
// explorers agreeing the outpoint is not unspent is a hint.

/// A transaction with one input, spending `source_txid:source_vout`: its
/// txid and its bytes in hex.
fn a_spender_of(source_txid: &str, source_vout: u32) -> (String, String) {
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut tx = Transaction::new();
    tx.version = 1;
    tx.lock_time = 0;
    let mut input = TransactionInput::new(source_txid.to_string(), source_vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(900),
        locking_script: LockingScript::from_hex("51").unwrap(),
        change: false,
    });
    (tx.id(), tx.to_hex())
}

/// A proof courier that serves one fixed answer.
struct FixedProof(GetMerklePathResult);

#[async_trait]
impl MerklePathService for FixedProof {
    async fn get_merkle_path(&self, _txid: &str) -> Result<GetMerklePathResult> {
        Ok(self.0.clone())
    }
}

const SPEND_HEIGHT: u32 = 900_123;

/// What the fixtures of one spend scenario say.
struct SpendScene {
    woc_unspent: Says,
    bitails_unspent: Says,
    /// The outpoint the named spender's bytes spend; `None`: WhatsOnChain
    /// names no spender (404).
    spender_spends: Option<(&'static str, u32)>,
    /// A courier serves the spender's merkle path, and the header service
    /// holds its root at its height.
    proven: bool,
    /// A header service is configured.
    header_service: bool,
}

struct SpendFixtures {
    services: Services,
    spent_route: mockito::Mock,
    _servers: Vec<mockito::ServerGuard>,
    _mocks: Vec<mockito::Mock>,
}

async fn spend_scene(scene: SpendScene) -> SpendFixtures {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let mut ct = mockito::Server::new_async().await;
    let mut mocks = vec![
        woc_unspent(&mut woc, scene.woc_unspent).await,
        bitails_unspent(&mut bitails, scene.bitails_unspent).await,
    ];

    let spent_path = format!("/tx/{}/0/spent", UTXO_TXID);
    let spent_route = woc.mock("GET", spent_path.as_str());
    let mut proof = GetMerklePathResult {
        name: Some("courier".to_string()),
        merkle_path: None,
        header: None,
        error: None,
        notes: vec![],
    };
    let spent_route = match scene.spender_spends {
        Some((source_txid, source_vout)) => {
            let (spender, spender_hex) = a_spender_of(source_txid, source_vout);
            mocks.push(
                woc.mock("GET", format!("/tx/{}/hex", spender).as_str())
                    .with_status(200)
                    .with_body(spender_hex)
                    .create_async()
                    .await,
            );
            if scene.proven {
                let bump =
                    bsv_rs::transaction::MerklePath::from_coinbase_txid(&spender, SPEND_HEIGHT);
                let root = bump.compute_root(Some(&spender)).unwrap();
                let header = BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root: root.clone(),
                    time: 1700000000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "b".repeat(64),
                    height: SPEND_HEIGHT,
                };
                mocks.push(
                    ct.mock(
                        "GET",
                        format!("/findHeaderHexForHeight?height={}", SPEND_HEIGHT).as_str(),
                    )
                    .with_status(200)
                    .with_body(
                        serde_json::json!({
                            "status": "success",
                            "value": {
                                "version": 1,
                                "previousHash": "0".repeat(64),
                                "merkleRoot": root,
                                "time": 1700000000u32,
                                "bits": 486604799u32,
                                "nonce": 12345u32,
                                "height": SPEND_HEIGHT,
                                "hash": "b".repeat(64),
                            }
                        })
                        .to_string(),
                    )
                    .create_async()
                    .await,
                );
                proof.merkle_path = Some(bump.to_hex());
                proof.header = Some(header);
            }
            spent_route
                .with_status(200)
                .with_body(format!(r#"{{"txid":"{}","vin":0}}"#, spender))
        }
        None => spent_route.with_status(404),
    };
    let spent_route = if scene.header_service {
        spent_route
    } else {
        spent_route.expect(0)
    };
    let spent_route = spent_route.create_async().await;

    let options = if scene.header_service {
        ServicesOptions::mainnet().with_chaintracks_url(ct.url())
    } else {
        ServicesOptions::mainnet()
    };
    let mut services = services_with_explorers(options, &woc.url(), &bitails.url());
    let mut couriers = ServiceCollection::new("getMerklePath");
    let courier: MerklePathProvider = StdArc::new(FixedProof(proof));
    couriers.add("courier", courier);
    services.get_merkle_path_services = RwLock::new(couriers);

    SpendFixtures {
        services,
        spent_route,
        _servers: vec![woc, bitails, ct],
        _mocks: mocks,
    }
}

/// Two explorers agreeing the outpoint is not in the unspent set, with no
/// spender named, is not a chain fact (0.5.0 answered `Spent` here).
#[tokio::test]
async fn two_explorers_agreeing_is_not_a_spend() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: None,
        proven: false,
        header_service: true,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_ne!(answer, UtxoVerdict::Spent, "two words read as a fact");
    assert_eq!(answer, UtxoVerdict::SpentHint);
    f.spent_route.assert_async().await;
}

/// A spender is named and its bytes name the outpoint, but no merkle path
/// of it meets the header service: unproven, so not a chain fact.
#[tokio::test]
async fn a_named_spender_without_a_proof_is_not_a_spend() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: false,
        header_service: true,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_ne!(
        answer,
        UtxoVerdict::Spent,
        "an unproven spender read as a fact"
    );
    assert_eq!(answer, UtxoVerdict::SpentHint);
}

/// The named spender's bytes spend another outpoint. Its proof is real,
/// and proves nothing about ours: the name was a word.
#[tokio::test]
async fn a_proven_transaction_that_does_not_name_the_outpoint_is_not_a_spend() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: Some((UTXO_TXID, 1)),
        proven: true,
        header_service: true,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_ne!(
        answer,
        UtxoVerdict::Spent,
        "an unbound spender read as a fact"
    );
    assert_eq!(
        answer,
        UtxoVerdict::SpentHint,
        "the explorers' agreement stands as a hint"
    );
}

/// The spending transaction's bytes name the outpoint and its merkle path
/// meets the header service's header: a chain fact, whatever the explorers'
/// unspent sets say or fail to say (0.5.0 answered `Unknown` here).
#[tokio::test]
async fn the_spenders_bytes_and_its_proof_are_a_spend() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::Fault,
        bitails_unspent: Says::Fault,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: true,
        header_service: true,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_eq!(answer, UtxoVerdict::Spent);
    f.spent_route.assert_async().await;
}

/// With no header service no proof can be checked, so no spender is asked
/// for and nothing is `Spent`.
#[tokio::test]
async fn without_a_header_service_no_spend_is_a_fact_and_no_spender_is_asked() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: true,
        header_service: false,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_ne!(
        answer,
        UtxoVerdict::Spent,
        "a spend with no header to check it"
    );
    assert_eq!(answer, UtxoVerdict::SpentHint);
    f.spent_route.assert_async().await;
}

/// A spender is named and its bytes name the outpoint while both unspent
/// sets could not be read: the bound bytes are a hint on their own, and
/// without a proof still no more than a hint.
#[tokio::test]
async fn a_bound_spender_is_a_hint_when_the_unspent_sets_could_not_be_read() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::Fault,
        bitails_unspent: Says::Fault,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: false,
        header_service: true,
    })
    .await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_eq!(answer, UtxoVerdict::SpentHint);
}

/// An outpoint in an unspent set asks for no spender.
#[tokio::test]
async fn an_outpoint_in_an_unspent_set_asks_for_no_spender() {
    let mut woc = mockito::Server::new_async().await;
    let mut bitails = mockito::Server::new_async().await;
    let ct = mockito::Server::new_async().await;
    let _w = woc_unspent(&mut woc, Says::Unspent).await;
    let _b = bitails_unspent(&mut bitails, Says::Unspent).await;
    let spent_path = format!("/tx/{}/0/spent", UTXO_TXID);
    let spent_route = woc
        .mock("GET", spent_path.as_str())
        .with_status(404)
        .expect(0)
        .create_async()
        .await;
    let services = services_with_explorers(
        ServicesOptions::mainnet().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bitails.url(),
    );
    assert_eq!(
        services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
        UtxoVerdict::UnspentHint
    );
    spent_route.assert_async().await;
}
