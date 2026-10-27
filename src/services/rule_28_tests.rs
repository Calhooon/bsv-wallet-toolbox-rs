//! Rule 28 witnesses (bsv-stack-lean `docs/p0/rule-28-explorer-calls.md`,
//! section 1): a third-party explorer is a break-glass read. Each test
//! stands a local fixture in an explorer's place and counts the requests
//! that reach it. Nothing here touches the network.

use super::*;
use crate::services::traits::{BlockHeader, WalletServices};

/// `Services` with WhatsOnChain and Bitails pointed at local fixtures, in
/// every collection and in the break-glass tracker, and BananaBlocks off.
fn services_with_explorers(options: ServicesOptions, woc_url: &str, bitails_url: &str) -> Services {
    Services::with_explorers(
        Chain::Main,
        options,
        StdArc::new(WhatsOnChain::with_base_url(Chain::Main, woc_url)),
        StdArc::new(Bitails::with_base_url(Chain::Main, bitails_url)),
        None,
    )
    .unwrap()
}

/// `Services` with all three explorers pointed at local fixtures.
pub(super) fn services_with_three(
    options: ServicesOptions,
    woc_url: &str,
    bananablocks_url: &str,
    bitails_url: &str,
) -> Services {
    Services::with_explorers(
        Chain::Main,
        options,
        StdArc::new(WhatsOnChain::with_base_url(Chain::Main, woc_url)),
        StdArc::new(Bitails::with_base_url(Chain::Main, bitails_url)),
        Some(StdArc::new(BananaBlocks::with_base_unshared(
            bananablocks_url,
        ))),
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
    woc: &mut crate::services::MockServer,
    bitails: &mut crate::services::MockServer,
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
    let mut ct = crate::services::mock_server().await;
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let tip = ct
        .mock("GET", "/findChainTipHeaderHex")
        .with_status(200)
        .with_body(header_frame(900_001))
        .expect(1)
        .create_async()
        .await;
    let (w, b) = explorer_tips_never_asked(&mut woc, &mut bitails).await;

    let services = services_with_explorers(
        crate::services::arcade_test_options().with_chaintracks_url(ct.url()),
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let (w, b) = explorer_tips_never_asked(&mut woc, &mut bitails).await;

    let services = services_with_explorers(
        crate::services::arcade_test_options(),
        &woc.url(),
        &bitails.url(),
    );
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
    let services =
        Services::with_options(Chain::Main, crate::services::arcade_test_options()).unwrap();
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
    let mut woc = crate::services::mock_server().await;
    let bitails = crate::services::mock_server().await;
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

    let services = services_with_explorers(
        crate::services::arcade_test_options(),
        &woc.url(),
        &bitails.url(),
    );
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
    let mut bitails = crate::services::mock_server().await;
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
// the unspent set of a script is asked of the explorers. The shape (the
// owner's word of 2026-10-22, bsv-wallet-cli#50): WhatsOnChain, BananaBlocks
// and Bitails are each the others' fallback, the start rotates, a negative
// needs two of the three, a fault moves to the next rung, and "could not
// look" is never "spent".

pub(super) const UTXO_TXID: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";
pub(super) const UTXO_SCRIPT: &[u8] = &[0x76, 0xa9, 0x14, 0x01, 0x02, 0x88, 0xac];
/// The transaction BananaBlocks names as the spender when it says spent.
pub(super) const BB_SPENDER: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";

/// The script hash of `UTXO_SCRIPT` as the explorers' routes carry it.
fn utxo_script_hash_be() -> String {
    let mut hash = sha256(UTXO_SCRIPT);
    hash.reverse();
    hex::encode(hash)
}

/// What an explorer fixture says about the outpoint `UTXO_TXID:0`.
#[derive(Clone, Copy, Debug)]
pub(super) enum Says {
    /// Unspent: in the script's unspent set (WhatsOnChain, Bitails), or
    /// `{"spent": false}` (BananaBlocks).
    Unspent,
    /// Not unspent: the unspent set is empty, or BananaBlocks names a
    /// spender.
    NotListed,
    /// HTTP 500.
    Fault,
    /// HTTP 429 with `Retry-After: 30`.
    RateLimited,
}

fn fault_or_limit(mock: mockito::Mock, says: Says) -> mockito::Mock {
    match says {
        Says::RateLimited => mock.with_status(429).with_header("retry-after", "30"),
        _ => mock.with_status(500),
    }
}

async fn woc_unspent(server: &mut crate::services::MockServer, says: Says) -> mockito::Mock {
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
        Says::Fault | Says::RateLimited => fault_or_limit(mock, says),
    };
    mock.create_async().await
}

async fn bitails_unspent(server: &mut crate::services::MockServer, says: Says) -> mockito::Mock {
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
        Says::Fault | Says::RateLimited => fault_or_limit(mock, says),
    };
    mock.create_async().await
}

/// BananaBlocks' `/txo/{txid}/{vout}` body for `UTXO_TXID:0` (script
/// `UTXO_SCRIPT`), unspent or spent by `spender`.
pub(super) fn bananablocks_txo_body(spender: Option<&str>) -> String {
    let script = hex::encode(UTXO_SCRIPT);
    match spender {
        None => format!(
            r#"{{"value":1000,"index":0,"script_pubkey":"{script}","script_type":"p2pkh","spent":false}}"#
        ),
        Some(spender) => format!(
            r#"{{"value":1000,"index":0,"script_pubkey":"{script}","script_type":"p2pkh","spent_txid":"{spender}","spent":true,"spend_scope":"confirmed"}}"#
        ),
    }
}

pub(super) async fn bananablocks_spend(
    server: &mut crate::services::MockServer,
    says: Says,
) -> mockito::Mock {
    let path = format!("/txo/{}/0", UTXO_TXID);
    let mock = server.mock("GET", path.as_str());
    let mock = match says {
        Says::Unspent => mock.with_status(200).with_body(bananablocks_txo_body(None)),
        Says::NotListed => mock
            .with_status(200)
            .with_body(bananablocks_txo_body(Some(BB_SPENDER))),
        Says::Fault | Says::RateLimited => fault_or_limit(mock, says),
    };
    mock.create_async().await
}

/// The three explorer fixtures of a T10 scene, each with its route's mock.
pub(super) struct ThreeExplorers {
    pub services: Services,
    pub woc: (crate::services::MockServer, mockito::Mock),
    pub bananablocks: (crate::services::MockServer, mockito::Mock),
    pub bitails: (crate::services::MockServer, mockito::Mock),
}

/// `Services` over three explorer fixtures that answer as given, in the
/// ladder's order: WhatsOnChain, BananaBlocks, Bitails.
pub(super) async fn utxo_services(
    woc_says: Says,
    bb_says: Says,
    bitails_says: Says,
) -> ThreeExplorers {
    let mut woc = crate::services::mock_server().await;
    let mut bb = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let w = woc_unspent(&mut woc, woc_says).await;
    let b = bananablocks_spend(&mut bb, bb_says).await;
    let t = bitails_unspent(&mut bitails, bitails_says).await;
    let services = services_with_three(
        crate::services::arcade_test_options(),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    ThreeExplorers {
        services,
        woc: (woc, w),
        bananablocks: (bb, b),
        bitails: (bitails, t),
    }
}

pub(super) async fn utxo_status(services: &Services) -> GetUtxoStatusResult {
    utxo_status_next(services, false).await
}

pub(super) async fn utxo_status_next(services: &Services, use_next: bool) -> GetUtxoStatusResult {
    let hash = services.hash_output_script(UTXO_SCRIPT);
    let outpoint = format!("{}.0", UTXO_TXID);
    services
        .get_utxo_status(&hash, None, Some(&outpoint), use_next)
        .await
        .unwrap()
}

/// T10: one explorer's "not in the unspent list" is not "spent" while the
/// other two could not look.
#[tokio::test]
async fn one_negative_and_two_faults_is_could_not_look() {
    for (woc, bb, bitails) in [
        (Says::NotListed, Says::Fault, Says::Fault),
        (Says::Fault, Says::NotListed, Says::RateLimited),
        (Says::RateLimited, Says::RateLimited, Says::NotListed),
    ] {
        let f = utxo_services(woc, bb, bitails).await;
        let result = utxo_status(&f.services).await;
        assert_ne!(
            result.is_utxo,
            Some(false),
            "WoC {woc:?}, BananaBlocks {bb:?}, Bitails {bitails:?}: a negative needs a second provider: {result:?}"
        );
        assert_eq!(result.status, "error", "could not look: {result:?}");
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("no second explorer confirmed it")),
            "the lone negative is named: {result:?}"
        );
    }
}

/// What the owner's rule gives for three answers (WhatsOnChain,
/// BananaBlocks, Bitails): an unspent from two, a not-listed from two,
/// anything else "could not look". The 2 is written out, not read from
/// the constants under test, so a change to them fails here.
fn the_rule(answers: [Says; 3]) -> (&'static str, Option<bool>) {
    let count = |want: fn(&Says) -> bool| answers.iter().filter(|a| want(a)).count();
    let unspent = count(|a| matches!(a, Says::Unspent));
    let not_listed = count(|a| matches!(a, Says::NotListed));
    if unspent >= 2 {
        ("success", Some(true))
    } else if not_listed >= 2 {
        ("success", Some(false))
    } else {
        ("error", None)
    }
}

/// The `+`-joined names of an agreeing answer, sorted (they come in the
/// order asked, which the start moves).
pub(super) fn sorted_names(result: &GetUtxoStatusResult) -> String {
    let mut names: Vec<&str> = result.name.split('+').collect();
    names.sort_unstable();
    names.join("+")
}

/// The three explorer fixtures with the rotation's start moved `start`
/// rungs on (0 WhatsOnChain, 1 BananaBlocks, 2 Bitails).
pub(super) async fn utxo_services_from(answers: [Says; 3], start: usize) -> ThreeExplorers {
    let f = utxo_services(answers[0], answers[1], answers[2]).await;
    {
        let mut collection = lock_write(&f.services.get_utxo_status_services).unwrap();
        for _ in 0..start {
            collection.next();
        }
    }
    f
}

/// One row of the rule's table on three shared fixture servers: its own
/// outpoint and script, so its routes are its own.
async fn rule_row(
    servers: &mut [crate::services::MockServer; 3],
    row: u8,
    answers: [Says; 3],
) -> (String, Vec<u8>, Vec<mockito::Mock>) {
    let txid = format!("{:02x}", row + 1).repeat(32);
    let script = vec![0x51, 0x01, row];
    let mut be = sha256(&script).to_vec();
    be.reverse();
    let be = hex::encode(be);
    let script_hex = hex::encode(&script);
    let [woc, bb, bitails] = servers;
    let fault = |mock: mockito::Mock, says: Says| match says {
        Says::RateLimited => mock.with_status(429).with_header("retry-after", "30"),
        _ => mock.with_status(500),
    };
    let w = woc.mock("GET", format!("/script/{be}/unspent/all").as_str());
    let w = match answers[0] {
        Says::Unspent => w.with_body(format!(
            r#"{{"script":"{be}","result":[{{"height":1,"tx_pos":0,"tx_hash":"{txid}","value":1}}]}}"#
        )),
        Says::NotListed => w.with_body(format!(r#"{{"script":"{be}","result":[]}}"#)),
        says => fault(w, says),
    };
    let b = bb.mock("GET", format!("/txo/{txid}/0").as_str());
    let b = match answers[1] {
        Says::Unspent => b.with_body(format!(
            r#"{{"script_pubkey":"{script_hex}","spent":false}}"#
        )),
        Says::NotListed => b.with_body(format!(
            r#"{{"script_pubkey":"{script_hex}","spent":true,"spent_txid":"{BB_SPENDER}"}}"#
        )),
        says => fault(b, says),
    };
    let t = bitails.mock("GET", format!("/scripthash/{be}/unspent").as_str());
    let t = match answers[2] {
        Says::Unspent => t.with_body(format!(
            r#"{{"unspent":[{{"txid":"{txid}","vout":0,"satoshis":1}}]}}"#
        )),
        Says::NotListed => t.with_body(r#"{"unspent":[]}"#),
        says => fault(t, says),
    };
    let mocks = vec![
        w.create_async().await,
        b.create_async().await,
        t.create_async().await,
    ];
    (txid, script, mocks)
}

/// T10, the owner's rule (2026-10-22): every combination of unspent, not
/// listed, a fault and a rate limit from the three explorers gives the
/// rule's verdict, from each of the three starts (the verdict does not
/// depend on where the rotation began: review M1 of #43). Among them:
/// P+2N is not listed (the positive is never the answer, wherever it
/// sits), P+N+F and P+2F (the positive BananaBlocks', or any one's) could
/// not look, 2P unspent.
#[tokio::test]
async fn the_utxo_verdict_is_the_rule_from_every_start() {
    let says = [
        Says::Unspent,
        Says::NotListed,
        Says::Fault,
        Says::RateLimited,
    ];
    let mut servers = [
        crate::services::mock_server().await,
        crate::services::mock_server().await,
        crate::services::mock_server().await,
    ];
    let mut rows = Vec::new();
    let mut _mocks = Vec::new();
    for woc in says {
        for bb in says {
            for bitails in says {
                let answers = [woc, bb, bitails];
                let (txid, script, mocks) = rule_row(&mut servers, rows.len() as u8, answers).await;
                _mocks.extend(mocks);
                rows.push((answers, txid, script));
            }
        }
    }
    let urls: Vec<String> = servers.iter().map(|s| s.url()).collect();
    for start in 0..3 {
        // Concurrently: a row with a fault pending sleeps its retry round.
        let verdicts = futures::future::join_all(rows.iter().map(|(answers, txid, script)| {
            let urls = &urls;
            async move {
                let services = services_with_three(
                    crate::services::arcade_test_options(),
                    &urls[0],
                    &urls[1],
                    &urls[2],
                );
                {
                    let mut collection = lock_write(&services.get_utxo_status_services).unwrap();
                    for _ in 0..start {
                        collection.next();
                    }
                }
                let hash = services.hash_output_script(script);
                let result = services
                    .get_utxo_status(&hash, None, Some(&format!("{txid}.0")), false)
                    .await
                    .unwrap();
                (*answers, result)
            }
        }))
        .await;
        for (answers, result) in verdicts {
            assert_eq!(
                (result.status.as_str(), result.is_utxo),
                the_rule(answers),
                "WoC {:?}, BananaBlocks {:?}, Bitails {:?}, start {start}: {result:?}",
                answers[0],
                answers[1],
                answers[2]
            );
        }
    }
}

/// T10: a lone unspent stands nowhere: BananaBlocks' `spent: false` with
/// two explorers that do not list the output is "not listed", and with a
/// fault beside it, or two, "could not look" naming the lone unspent.
#[tokio::test]
async fn a_lone_unspent_is_never_the_answer() {
    for start in 0..3 {
        let f = utxo_services_from([Says::NotListed, Says::Unspent, Says::NotListed], start).await;
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
        // From BananaBlocks' start (1) its unspent comes first and the
        // ladder asks on until the two others agree.
        if start == 1 {
            for rung in [&f.woc.1, &f.bananablocks.1, &f.bitails.1] {
                assert!(rung.matched_async().await, "every rung asked");
            }
        }
    }
    for answers in [
        [Says::Fault, Says::Unspent, Says::NotListed],
        [Says::RateLimited, Says::Unspent, Says::Fault],
    ] {
        let f = utxo_services_from(answers, 0).await;
        let result = utxo_status(&f.services).await;
        assert_eq!((result.status.as_str(), result.is_utxo), ("error", None));
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("BananaBlocks lists the output as unspent")
                    && e.contains("no second explorer confirmed it")),
            "{answers:?}: {result:?}"
        );
    }
}

/// T10: two explorers that leave the outpoint out of the unspent set are
/// the negative, whichever two of the three they are; the result names
/// both.
#[tokio::test]
async fn a_negative_needs_two_of_the_three() {
    for (woc, bb, bitails, names) in [
        (
            Says::NotListed,
            Says::NotListed,
            Says::Fault,
            "WoC+BananaBlocks",
        ),
        (Says::NotListed, Says::Fault, Says::NotListed, "WoC+Bitails"),
        (
            Says::RateLimited,
            Says::NotListed,
            Says::NotListed,
            "BananaBlocks+Bitails",
        ),
    ] {
        let f = utxo_services(woc, bb, bitails).await;
        let result = utxo_status(&f.services).await;
        assert_eq!(
            (result.status.as_str(), result.is_utxo),
            ("success", Some(false)),
            "WoC {woc:?}, BananaBlocks {bb:?}, Bitails {bitails:?}: {result:?}"
        );
        assert_eq!(result.name, names, "{result:?}");
    }
}

/// T10: the start rotates over the three. With every explorer listing the
/// outpoint, each call ends at the second answer, and three calls (starts
/// WhatsOnChain, BananaBlocks, Bitails) reach each explorer twice.
#[tokio::test]
async fn the_utxo_read_rotates_its_starting_explorer_over_three() {
    let f = utxo_services(Says::Unspent, Says::Unspent, Says::Unspent).await;
    for _ in 0..3 {
        assert_eq!(utxo_status(&f.services).await.is_utxo, Some(true));
    }
    f.woc.1.expect(2).assert_async().await;
    f.bananablocks.1.expect(2).assert_async().await;
    f.bitails.1.expect(2).assert_async().await;
}

/// T10: an outage is "could not look", never "spent" (`is_utxo` answered
/// `false` for it, and four callers read that as spent).
#[tokio::test]
async fn an_outage_is_could_not_look_never_spent() {
    let f = utxo_services(Says::Fault, Says::RateLimited, Says::Fault).await;
    let answer = f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await;
    assert_eq!(answer, UtxoVerdict::Unknown, "an outage read as {answer:?}");
}

/// T10: the answers of `is_utxo` from the explorers alone, one row per
/// what WhatsOnChain, BananaBlocks and Bitails say. Agreement on a negative
/// is the hint tier, never `Spent` (the second pass); the three answers
/// mean what they meant with two explorers. BananaBlocks' "not listed"
/// names `BB_SPENDER`, whose bytes no rung holds: a named spender that
/// cannot be read is could not look whatever the sets say (#96).
#[tokio::test]
async fn is_utxo_gives_the_three_answers() {
    for (woc, bb, bitails, expected) in [
        // An unspent from two.
        (
            Says::Unspent,
            Says::Unspent,
            Says::Fault,
            UtxoVerdict::UnspentHint,
        ),
        // Two unspent, and BananaBlocks names a spender whose bytes no
        // rung holds: could not look (#96), never the unspent hint.
        (
            Says::Unspent,
            Says::NotListed,
            Says::Unspent,
            UtxoVerdict::Unknown,
        ),
        // A lone unspent is could not look.
        (
            Says::Unspent,
            Says::Fault,
            Says::Fault,
            UtxoVerdict::Unknown,
        ),
        (
            Says::NotListed,
            Says::Unspent,
            Says::Fault,
            UtxoVerdict::Unknown,
        ),
        (
            Says::Fault,
            Says::RateLimited,
            Says::Unspent,
            UtxoVerdict::Unknown,
        ),
        // A negative from two.
        (
            Says::NotListed,
            Says::RateLimited,
            Says::NotListed,
            UtxoVerdict::SpentHint,
        ),
        // A negative from two, one BananaBlocks' with its unreadable
        // spender: could not look (#96).
        (
            Says::NotListed,
            Says::NotListed,
            Says::Fault,
            UtxoVerdict::Unknown,
        ),
        (
            Says::Fault,
            Says::NotListed,
            Says::NotListed,
            UtxoVerdict::Unknown,
        ),
        // One negative is could not look.
        (
            Says::NotListed,
            Says::Fault,
            Says::Fault,
            UtxoVerdict::Unknown,
        ),
        (
            Says::Fault,
            Says::NotListed,
            Says::Fault,
            UtxoVerdict::Unknown,
        ),
        (
            Says::RateLimited,
            Says::Fault,
            Says::NotListed,
            UtxoVerdict::Unknown,
        ),
    ] {
        let f = utxo_services(woc, bb, bitails).await;
        assert_eq!(
            f.services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
            expected,
            "WoC {woc:?}, BananaBlocks {bb:?}, Bitails {bitails:?}"
        );
    }
}

/// T10: BananaBlocks answers an outpoint only. A script's question (no
/// outpoint) does not ask it and never counts it: WhatsOnChain's lone
/// negative stays "could not look" with Bitails down, BananaBlocks or no.
#[tokio::test]
async fn bananablocks_is_not_asked_a_scripts_question() {
    let f = utxo_services(Says::NotListed, Says::NotListed, Says::Fault).await;
    let hash = f.services.hash_output_script(UTXO_SCRIPT);
    let result = f
        .services
        .get_utxo_status(&hash, None, None, false)
        .await
        .unwrap();
    assert_eq!(result.status, "error", "{result:?}");
    assert_eq!(result.is_utxo, None, "{result:?}");
    f.bananablocks.1.expect(0).assert_async().await;
}

/// T10: a Bitails body that is not an unspent list, and a list long enough
/// to be one page of more, are "could not look", never an empty set.
#[tokio::test]
async fn a_bitails_answer_that_is_not_a_whole_unspent_list_is_could_not_look() {
    let hash = hex::encode(sha256(UTXO_SCRIPT));
    let outpoint = format!("{}.0", UTXO_TXID);
    let path = format!("/scripthash/{}/unspent", utxo_script_hash_be());

    let mut server = crate::services::mock_server().await;
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

    let mut server = crate::services::mock_server().await;
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

    let mut server = crate::services::mock_server().await;
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
    let mut hash = sha256d(&bytes);
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
        crate::services::arcade_test_options()
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
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

    let services = services_with_explorers(
        crate::services::arcade_test_options(),
        &woc.url(),
        &bitails.url(),
    );
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
    server: &mut crate::services::MockServer,
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
        let mut woc = crate::services::mock_server().await;
        let mut bitails = crate::services::mock_server().await;
        let _w = raw_tx_fixture(&mut woc, &txid, woc_status).await;
        let _b = raw_tx_fixture(&mut bitails, &txid, bitails_status).await;
        let services = services_with_explorers(
            crate::services::arcade_test_options(),
            &woc.url(),
            &bitails.url(),
        );
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
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let _w = raw_tx_fixture(&mut woc, &txid, 404).await;
    let _b = raw_tx_fixture(&mut bitails, &txid, 404).await;
    let services = services_with_explorers(
        crate::services::arcade_test_options(),
        &woc.url(),
        &bitails.url(),
    );
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
pub(super) fn a_spender_of(source_txid: &str, source_vout: u32) -> (String, String) {
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
    _servers: Vec<crate::services::MockServer>,
    _mocks: Vec<mockito::Mock>,
}

async fn spend_scene(scene: SpendScene) -> SpendFixtures {
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let mut ct = crate::services::mock_server().await;
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
    let spent_route = spent_route.expect(1).create_async().await;

    let options = if scene.header_service {
        crate::services::arcade_test_options().with_chaintracks_url(ct.url())
    } else {
        crate::services::arcade_test_options()
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
/// and proves nothing about ours: the name was a word its bytes do not
/// bear out, so the question could not be looked at (#96).
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
        UtxoVerdict::Unknown,
        "a name its bytes refuse is could not look"
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

/// With no header service no proof can be checked, so nothing is `Spent`;
/// the spender is still asked for and bound by its bytes, which need no
/// header (#96).
#[tokio::test]
async fn without_a_header_service_no_spend_is_a_fact() {
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

/// Two unspent answers still ask for a spender (a bound spender stands on
/// one provider, even against them: an index can miss a mempool spend);
/// with none named the answer is `UnspentHint`.
#[tokio::test]
async fn two_unspent_answers_still_ask_for_a_spender() {
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let ct = crate::services::mock_server().await;
    let _w = woc_unspent(&mut woc, Says::Unspent).await;
    let _b = bitails_unspent(&mut bitails, Says::Unspent).await;
    let spent_path = format!("/tx/{}/0/spent", UTXO_TXID);
    let spent_route = woc
        .mock("GET", spent_path.as_str())
        .with_status(404)
        .expect(1)
        .create_async()
        .await;
    let services = services_with_explorers(
        crate::services::arcade_test_options().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bitails.url(),
    );
    assert_eq!(
        services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await,
        UtxoVerdict::UnspentHint
    );
    spent_route.assert_async().await;
}

// =============================================================================
// is_utxo_with_spender (#44): the bound spender beside the verdict, one pass
// =============================================================================

/// A proven spend names its spender, confirmed; the verdict is `is_utxo`'s.
#[tokio::test]
async fn a_proven_spend_answers_its_spender_confirmed() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::Fault,
        bitails_unspent: Says::Fault,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: true,
        header_service: true,
    })
    .await;
    let (spender, _) = a_spender_of(UTXO_TXID, 0);
    let (verdict, bound) = f
        .services
        .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
        .await;
    assert_eq!(verdict, UtxoVerdict::Spent);
    assert_eq!(
        bound,
        Some(BoundSpender {
            txid: spender,
            confirmed: true,
        })
    );
    f.spent_route.assert_async().await;
}

/// A bound spender with no proof held names its spender, not confirmed:
/// the verdict is `SpentHint`.
#[tokio::test]
async fn an_unproven_bound_spender_answers_its_spender_unconfirmed() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: Some((UTXO_TXID, 0)),
        proven: false,
        header_service: true,
    })
    .await;
    let (spender, _) = a_spender_of(UTXO_TXID, 0);
    let (verdict, bound) = f
        .services
        .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
        .await;
    assert_eq!(verdict, UtxoVerdict::SpentHint);
    assert_eq!(
        bound,
        Some(BoundSpender {
            txid: spender,
            confirmed: false,
        })
    );
}

/// Two unspent answers and no spender named: `UnspentHint`, no spender.
#[tokio::test]
async fn an_unspent_answers_no_spender() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::Unspent,
        bitails_unspent: Says::Unspent,
        spender_spends: None,
        proven: false,
        header_service: true,
    })
    .await;
    assert_eq!(
        f.services
            .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
            .await,
        (UtxoVerdict::UnspentHint, None)
    );
    f.spent_route.assert_async().await;
}

/// One explorer names a spender whose bytes spend another outpoint: no
/// spender is answered, even with its proof real, and the verdict is
/// `Unknown` (#96: a name its bytes refuse is "could not look").
#[tokio::test]
async fn a_name_its_bytes_refuse_answers_no_spender() {
    let f = spend_scene(SpendScene {
        woc_unspent: Says::NotListed,
        bitails_unspent: Says::NotListed,
        spender_spends: Some((UTXO_TXID, 1)),
        proven: true,
        header_service: true,
    })
    .await;
    assert_eq!(
        f.services
            .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
            .await,
        (UtxoVerdict::Unknown, None)
    );
}

/// One pass of the three explorers' ladder for an outpoint WhatsOnChain
/// and BananaBlocks both name a bound, unproven spender of, each route
/// counted: WhatsOnChain's unspent set, BananaBlocks' `/txo` (the second
/// "not listed", and the carried spender), WhatsOnChain's `/spent`, and
/// the spender's bytes from WhatsOnChain: four requests. Bitails is not
/// reached, BananaBlocks is not asked again for the name it gave, and no
/// second raw read is made.
async fn one_pass(with_spender: bool) -> (UtxoVerdict, Option<BoundSpender>) {
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, 0);
    let mut woc = crate::services::mock_server().await;
    let mut bb = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let ct = crate::services::mock_server().await;
    let routes = vec![
        woc_unspent(&mut woc, Says::NotListed).await.expect(1),
        bb.mock("GET", format!("/txo/{UTXO_TXID}/0").as_str())
            .with_status(200)
            .with_body(bananablocks_txo_body(Some(&spender)))
            .expect(1)
            .create_async()
            .await,
        bitails_unspent(&mut bitails, Says::NotListed)
            .await
            .expect(0),
        woc.mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
            .with_status(200)
            .with_body(format!(r#"{{"txid":"{}","vin":0}}"#, spender))
            .expect(1)
            .create_async()
            .await,
        woc.mock("GET", format!("/tx/{spender}/hex").as_str())
            .with_status(200)
            .with_body(&spender_hex)
            .expect(1)
            .create_async()
            .await,
        bb.mock("GET", format!("/tx/{spender}/hex").as_str())
            .with_status(200)
            .with_body(&spender_hex)
            .expect(0)
            .create_async()
            .await,
        bitails
            .mock("GET", format!("/tx/{spender}/hex").as_str())
            .with_status(200)
            .with_body(&spender_hex)
            .expect(0)
            .create_async()
            .await,
    ];
    let mut services = services_with_three(
        crate::services::arcade_test_options().with_chaintracks_url(ct.url()),
        &woc.url(),
        &bb.url(),
        &bitails.url(),
    );
    let mut couriers = ServiceCollection::new("getMerklePath");
    let courier: MerklePathProvider = StdArc::new(FixedProof(GetMerklePathResult {
        name: Some("courier".to_string()),
        merkle_path: None,
        header: None,
        error: None,
        notes: vec![],
    }));
    couriers.add("courier", courier);
    services.get_merkle_path_services = RwLock::new(couriers);

    let answer = if with_spender {
        services
            .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
            .await
    } else {
        (services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await, None)
    };
    for route in &routes {
        route.assert_async().await;
    }
    answer
}

/// The request count per probe is `is_utxo`'s: the spender comes back
/// from the same four requests, none added (#44).
#[tokio::test]
async fn the_spender_costs_no_request_beyond_is_utxo() {
    let (spender, _) = a_spender_of(UTXO_TXID, 0);
    assert_eq!(one_pass(false).await, (UtxoVerdict::SpentHint, None));
    assert_eq!(
        one_pass(true).await,
        (
            UtxoVerdict::SpentHint,
            Some(BoundSpender {
                txid: spender,
                confirmed: false,
            })
        )
    );
}

// =============================================================================
// #96: a named spender its bytes cannot bear out is "could not look"; the
// bytes binding needs no header service
// =============================================================================

/// A proof courier that serves one fixed answer and counts the asks.
struct CountedProof {
    answer: GetMerklePathResult,
    asked: StdArc<AtomicUsize>,
}

#[async_trait]
impl MerklePathService for CountedProof {
    async fn get_merkle_path(&self, _txid: &str) -> Result<GetMerklePathResult> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(self.answer.clone())
    }
}

/// The scenes of #96. In each, both explorers list the outpoint as unspent
/// (the answer a host releases on) and WhatsOnChain's `/spent` names a
/// spender.
#[derive(Clone, Copy, Debug)]
enum NamedSpender {
    /// The spender's bytes route answers 500 at WhatsOnChain and Bitails.
    BytesFault,
    /// The spender's bytes are a transaction that spends `UTXO_TXID:1`.
    BytesSpendAnother,
    /// The spender's bytes spend the outpoint; no header service is
    /// configured.
    BoundNoHeaderService,
    /// The spender's bytes spend the outpoint and a courier serves its
    /// path; the header service answers 500.
    BoundHeaderServiceFaults,
}

/// One pass of `scene`, through `is_utxo_with_spender` or `is_utxo` (the
/// spender then `None`), with every route's request count pinned: the
/// unspent sets one each, `/spent` one, and the spender's bytes, its proof
/// and its header as the scene asks them.
async fn named_spender_pass(
    scene: NamedSpender,
    with_spender: bool,
) -> (UtxoVerdict, Option<BoundSpender>) {
    let mut woc = crate::services::mock_server().await;
    let mut bitails = crate::services::mock_server().await;
    let mut ct = crate::services::mock_server().await;
    let source_vout = match scene {
        NamedSpender::BytesSpendAnother => 1,
        _ => 0,
    };
    let (spender, spender_hex) = a_spender_of(UTXO_TXID, source_vout);
    let woc_bytes = woc.mock("GET", format!("/tx/{spender}/hex").as_str());
    let bitails_bytes = bitails.mock("GET", format!("/tx/{spender}/hex").as_str());
    let (woc_bytes, bitails_bytes) = match scene {
        NamedSpender::BytesFault => (
            woc_bytes.with_status(500).expect(1),
            bitails_bytes.with_status(500).expect(1),
        ),
        _ => (
            woc_bytes.with_status(200).with_body(&spender_hex).expect(1),
            bitails_bytes
                .with_status(200)
                .with_body(&spender_hex)
                .expect(0),
        ),
    };
    let mut routes = vec![
        woc_unspent(&mut woc, Says::Unspent).await.expect(1),
        bitails_unspent(&mut bitails, Says::Unspent).await.expect(1),
        woc.mock("GET", format!("/tx/{UTXO_TXID}/0/spent").as_str())
            .with_status(200)
            .with_body(format!(r#"{{"txid":"{}","vin":0}}"#, spender))
            .expect(1)
            .create_async()
            .await,
        woc_bytes.create_async().await,
        bitails_bytes.create_async().await,
    ];

    let bump = bsv_rs::transaction::MerklePath::from_coinbase_txid(&spender, SPEND_HEIGHT);
    let root = bump.compute_root(Some(&spender)).unwrap();
    let header_asks = match scene {
        NamedSpender::BoundHeaderServiceFaults => 1,
        _ => 0,
    };
    routes.push(
        ct.mock(
            "GET",
            format!("/findHeaderHexForHeight?height={}", SPEND_HEIGHT).as_str(),
        )
        .with_status(500)
        .expect(header_asks)
        .create_async()
        .await,
    );
    let header_service = !matches!(scene, NamedSpender::BoundNoHeaderService);
    let options = if header_service {
        crate::services::arcade_test_options().with_chaintracks_url(ct.url())
    } else {
        crate::services::arcade_test_options()
    };
    let mut services = services_with_explorers(options, &woc.url(), &bitails.url());
    let asked = StdArc::new(AtomicUsize::new(0));
    let courier: MerklePathProvider = StdArc::new(CountedProof {
        answer: GetMerklePathResult {
            name: Some("courier".to_string()),
            merkle_path: Some(bump.to_hex()),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: root,
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height: SPEND_HEIGHT,
            }),
            error: None,
            notes: vec![],
        },
        asked: asked.clone(),
    });
    let mut couriers = ServiceCollection::new("getMerklePath");
    couriers.add("courier", courier);
    services.get_merkle_path_services = RwLock::new(couriers);

    let answer = if with_spender {
        services
            .is_utxo_with_spender(UTXO_TXID, 0, UTXO_SCRIPT)
            .await
    } else {
        (services.is_utxo(UTXO_TXID, 0, UTXO_SCRIPT).await, None)
    };
    for route in &routes {
        route.assert_async().await;
    }
    let proof_asks = match scene {
        NamedSpender::BoundHeaderServiceFaults => 1,
        _ => 0,
    };
    assert_eq!(
        asked.load(Ordering::SeqCst),
        proof_asks,
        "{scene:?}: the courier's asks"
    );
    answer
}

/// A spender named whose bytes cannot be read (their route faults at both
/// raw-transaction rungs) is "could not look" for the whole question: never
/// the unspent sets' `UnspentHint`, which a host releases the coin on.
#[tokio::test]
async fn a_named_spender_whose_bytes_fault_answers_unknown() {
    assert_eq!(
        named_spender_pass(NamedSpender::BytesFault, true).await,
        (UtxoVerdict::Unknown, None)
    );
}

/// A spender named whose bytes spend another outpoint: the name is not
/// borne out, so the question could not be looked at; `Unknown`, never
/// the unspent sets' `UnspentHint`.
#[tokio::test]
async fn a_named_spender_whose_bytes_spend_another_outpoint_answers_unknown() {
    assert_eq!(
        named_spender_pass(NamedSpender::BytesSpendAnother, true).await,
        (UtxoVerdict::Unknown, None)
    );
}

/// With no header service the bytes binding still runs (it needs no
/// header): the bound spender is named, not confirmed, and no proof is
/// fetched.
#[tokio::test]
async fn without_a_header_service_the_bound_spender_is_named_unconfirmed() {
    let (spender, _) = a_spender_of(UTXO_TXID, 0);
    assert_eq!(
        named_spender_pass(NamedSpender::BoundNoHeaderService, true).await,
        (
            UtxoVerdict::SpentHint,
            Some(BoundSpender {
                txid: spender,
                confirmed: false,
            })
        )
    );
}

/// A header service that faults is "could not confirm", not "could not
/// look" at the binding: the bound spender is named, not confirmed.
#[tokio::test]
async fn a_header_service_fault_names_the_bound_spender_unconfirmed() {
    let (spender, _) = a_spender_of(UTXO_TXID, 0);
    assert_eq!(
        named_spender_pass(NamedSpender::BoundHeaderServiceFaults, true).await,
        (
            UtxoVerdict::SpentHint,
            Some(BoundSpender {
                txid: spender,
                confirmed: false,
            })
        )
    );
}

/// `is_utxo`'s verdict is `is_utxo_with_spender`'s with the spender
/// dropped, from the same requests, in each scene of #96.
#[tokio::test]
async fn is_utxo_is_the_spender_verdict_in_the_scenes_of_96() {
    for scene in [
        NamedSpender::BytesFault,
        NamedSpender::BytesSpendAnother,
        NamedSpender::BoundNoHeaderService,
        NamedSpender::BoundHeaderServiceFaults,
    ] {
        let (verdict, _) = named_spender_pass(scene, true).await;
        assert_eq!(
            named_spender_pass(scene, false).await,
            (verdict, None),
            "{scene:?}"
        );
    }
}
