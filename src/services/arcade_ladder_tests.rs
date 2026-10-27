//! The post ladder in each broadcast mode (bsv-wallet-toolbox-rs#317, the
//! owner's word of 2026-10-27: "we use arcade now"). In Arcade mode no
//! classic ARC provider exists: the post ladder is Arcade, then Bitails and
//! WhatsOnChain, and the broadcaster-status read asks Arcade alone. In
//! classic mode (`arcade_v2` false) TAAL from `arc_url`, GorillaPool when
//! configured, then Bitails and `WhatsOnChain`, as before, now only behind
//! the deprecated `with_classic_arc`. Constructing
//! `Services` makes no request; the status reads go to a loopback mock.

use super::*;

const TXID: &str = "3a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9";

fn post_names(services: &Services) -> Vec<String> {
    lock_read(&services.post_beef_services)
        .unwrap()
        .provider_names()
        .into_iter()
        .map(str::to_string)
        .collect()
}

/// Arcade mode's post ladder: Arcade, Bitails, `WhatsOnChain`; no classic ARC.
fn assert_arcade_ladder(services: &Services) {
    assert_eq!(
        post_names(services),
        [PROVIDER_ARCADE_V2, PROVIDER_BITAILS, PROVIDER_WHATSONCHAIN]
    );
    assert!(services.arcade.is_some());
    assert!(services.arc_taal.is_none());
    assert!(services.arc_gorillapool.is_none());
}

/// Classic mode's post ladder: TAAL, `GorillaPool` when configured, Bitails,
/// `WhatsOnChain`; no Arcade.
fn assert_classic_ladder(services: &Services, gorillapool: bool) {
    let mut expected = vec![PROVIDER_TAAL_ARC];
    if gorillapool {
        expected.push(PROVIDER_GORILLAPOOL_ARC);
    }
    expected.extend([PROVIDER_BITAILS, PROVIDER_WHATSONCHAIN]);
    assert_eq!(post_names(services), expected);
    assert!(services.arcade.is_none());
    assert!(services.arc_taal.is_some());
    assert_eq!(services.arc_gorillapool.is_some(), gorillapool);
}

fn status_names(statuses: &[(String, BroadcastStatus)]) -> Vec<&str> {
    statuses.iter().map(|(name, _)| name.as_str()).collect()
}

/// Every status read on `server` answers 404: the broadcaster does not
/// hold the transaction, so each provider asked gives an `Unknown` row.
async fn holds_nothing(server: &mut crate::services::MockServer) -> (mockito::Mock, mockito::Mock) {
    let arcade = server
        .mock("GET", format!("/tx/{}", TXID).as_str())
        .with_status(404)
        .expect_at_least(0)
        .create_async()
        .await;
    let arc = server
        .mock("GET", format!("/v1/tx/{}", TXID).as_str())
        .with_status(404)
        .expect_at_least(0)
        .create_async()
        .await;
    (arcade, arc)
}

#[test]
fn arcade_mode_post_ladder_is_arcade_bitails_whatsonchain() {
    // A GorillaPool URL set: in Arcade mode it builds nothing.
    let options = ServicesOptions::mainnet()
        .with_arcade(crate::services::ARCADE_V2_MAINNET, None)
        .with_gorillapool(LOOPBACK, None);
    assert!(options.arc_gorillapool_url.is_some());
    assert_arcade_ladder(&Services::with_options(Chain::Main, options).unwrap());
}

#[test]
fn arcade_mode_keeps_the_read_ladders() {
    let options = ServicesOptions::mainnet().with_arcade(crate::services::ARCADE_V2_MAINNET, None);
    let services = Services::with_options(Chain::Main, options).unwrap();

    let status: Vec<String> = lock_read(&services.get_status_for_txids_services)
        .unwrap()
        .provider_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    assert_eq!(
        status,
        ["ArcadeV2", "WhatsOnChain", "Bitails", "BananaBlocks"]
    );
    let merkle: Vec<String> = lock_read(&services.get_merkle_path_services)
        .unwrap()
        .provider_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    assert_eq!(
        merkle,
        ["ArcadeV2", "WhatsOnChain", "BananaBlocks", "Bitails"]
    );
}

#[tokio::test]
async fn arcade_mode_broadcaster_statuses_carry_no_classic_row() {
    let mut server = crate::services::mock_server().await;
    let _mocks = holds_nothing(&mut server).await;
    // GorillaPool pointed at the same loopback server: in Arcade mode it is
    // not built whatever `arc_gorillapool_url` says, so it is never asked.
    let options = ServicesOptions::mainnet()
        .with_arcade(server.url(), None)
        .with_gorillapool(server.url(), None);
    let services = Services::with_options(Chain::Main, options).unwrap();

    let statuses = services.get_broadcaster_statuses(TXID).await;
    assert_eq!(status_names(&statuses), [PROVIDER_ARCADE_V2]);
}

#[test]
#[allow(deprecated)]
fn classic_mode_post_ladder_is_unchanged() {
    let options = ServicesOptions::mainnet()
        .with_classic_arc(LOOPBACK, None)
        .with_gorillapool(LOOPBACK, None);
    assert_classic_ladder(&Services::with_options(Chain::Main, options).unwrap(), true);
}

#[test]
#[allow(deprecated)]
fn classic_mode_without_gorillapool_is_taal_bitails_whatsonchain() {
    let options = ServicesOptions::mainnet().with_classic_arc(LOOPBACK, None);
    assert!(options.arc_gorillapool_url.is_none());
    assert_classic_ladder(
        &Services::with_options(Chain::Main, options).unwrap(),
        false,
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn classic_mode_broadcaster_statuses_ask_both_arcs() {
    let mut server = crate::services::mock_server().await;
    let _mocks = holds_nothing(&mut server).await;
    let options = ServicesOptions::mainnet()
        .with_classic_arc(server.url(), None)
        .with_gorillapool(server.url(), None);
    let services = Services::with_options(Chain::Main, options).unwrap();

    let statuses = services.get_broadcaster_statuses(TXID).await;
    assert_eq!(
        status_names(&statuses),
        [PROVIDER_TAAL_ARC, PROVIDER_GORILLAPOOL_ARC]
    );
}

// The default mode is Arcade (#317, the owner's word of 2026-10-27: "arc is
// just deprecated, arcade is the standard"). `default()` and `mainnet()`
// name the crate's public `ARCADE_V2_MAINNET` (the keeper's reading on PR
// #342); the crate has no Arcade testnet constant, so `testnet()` names
// none, and options whose `arc_url` is empty in Arcade mode refuse at
// construction. Classic ARC is built only behind the deprecated knob.
// Constructing `Services` makes no request.

const LOOPBACK: &str = "http://127.0.0.1:9";

#[test]
fn mainnet_alone_builds_the_arcade_ladder_at_the_public_endpoint() {
    for options in [ServicesOptions::default(), ServicesOptions::mainnet()] {
        assert!(options.arcade_v2, "Arcade is the default mode");
        assert_eq!(options.arc_url, crate::services::ARCADE_V2_MAINNET);
        let services = Services::with_options(Chain::Main, options).unwrap();
        assert_arcade_ladder(&services);
        assert_eq!(
            services.arcade.as_ref().unwrap().url(),
            crate::services::ARCADE_V2_MAINNET
        );
    }
    for services in [Services::new(Chain::Main), Services::mainnet()] {
        assert_arcade_ladder(&services.unwrap());
    }
}

fn assert_refuses(chain: Chain, options: ServicesOptions) {
    assert!(options.arcade_v2, "{chain:?}: Arcade is the default mode");
    let err = Services::with_options(chain, options)
        .err()
        .unwrap_or_else(|| panic!("{chain:?}: built with no Arcade URL"));
    assert_eq!(
        err.to_string(),
        format!("Invalid argument: {NO_ARCADE_URL}")
    );
    assert!(err.to_string().contains("with_arcade"), "{err}");
}

#[test]
fn an_arc_url_emptied_by_the_caller_refuses_construction() {
    let mut options = ServicesOptions::mainnet();
    options.arc_url = String::new();
    assert_refuses(Chain::Main, options);
    let options = ServicesOptions::mainnet().with_arcade("  ", None);
    assert_refuses(Chain::Main, options);
}

#[test]
fn testnet_names_no_arcade_endpoint_and_refuses() {
    assert!(ServicesOptions::testnet().arc_url.is_empty());
    assert_refuses(Chain::Test, ServicesOptions::testnet());
    assert!(Services::new(Chain::Test).is_err());
    assert!(Services::testnet().is_err());
}

#[test]
fn mainnet_with_arcade_builds_the_arcade_ladder() {
    let options = ServicesOptions::mainnet().with_arcade(LOOPBACK, None);
    assert_arcade_ladder(&Services::with_options(Chain::Main, options).unwrap());
}

#[test]
#[allow(deprecated)]
fn the_deprecated_classic_knob_builds_the_classic_ladder() {
    let options = ServicesOptions::mainnet()
        .with_classic_arc(LOOPBACK, None)
        .with_gorillapool(LOOPBACK, None);
    assert!(!options.arcade_v2);
    assert_classic_ladder(&Services::with_options(Chain::Main, options).unwrap(), true);

    let options = ServicesOptions::mainnet().with_classic_arc(LOOPBACK, None);
    assert_classic_ladder(
        &Services::with_options(Chain::Main, options).unwrap(),
        false,
    );
}

#[test]
fn gorillapool_defaults_to_none() {
    assert_eq!(ServicesOptions::default().arc_gorillapool_url, None);
    assert_eq!(ServicesOptions::mainnet().arc_gorillapool_url, None);
    assert_eq!(ServicesOptions::testnet().arc_gorillapool_url, None);
}
