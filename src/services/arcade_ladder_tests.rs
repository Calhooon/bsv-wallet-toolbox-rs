//! The post ladder in each broadcast mode (bsv-wallet-toolbox-rs#317, the
//! owner's word of 2026-10-27: "we use arcade now"). In Arcade mode no
//! classic ARC provider exists: the post ladder is Arcade, then Bitails and
//! WhatsOnChain, and the broadcaster-status read asks Arcade alone. In
//! classic mode (`arcade_v2` false) TAAL from `arc_url`, GorillaPool when
//! configured, then Bitails and WhatsOnChain, as before. Constructing
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
    // The mainnet default `arc_gorillapool_url` is `Some`, left as it is.
    let options = ServicesOptions::mainnet().with_arcade(crate::services::ARCADE_V2_MAINNET, None);
    assert!(options.arc_gorillapool_url.is_some());
    let services = Services::with_options(Chain::Main, options).unwrap();

    assert_eq!(
        post_names(&services),
        [PROVIDER_ARCADE_V2, PROVIDER_BITAILS, PROVIDER_WHATSONCHAIN]
    );
    assert!(services.arcade.is_some());
    assert!(services.arc_gorillapool.is_none());
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
fn classic_mode_post_ladder_is_unchanged() {
    let services = Services::with_options(Chain::Main, ServicesOptions::mainnet()).unwrap();

    assert_eq!(
        post_names(&services),
        [
            PROVIDER_TAAL_ARC,
            PROVIDER_GORILLAPOOL_ARC,
            PROVIDER_BITAILS,
            PROVIDER_WHATSONCHAIN
        ]
    );
    assert!(services.arcade.is_none());
    assert!(services.arc_gorillapool.is_some());
}

#[test]
fn classic_mode_without_gorillapool_is_taal_bitails_whatsonchain() {
    let mut options = ServicesOptions::mainnet();
    options.arc_gorillapool_url = None;
    let services = Services::with_options(Chain::Main, options).unwrap();

    assert_eq!(
        post_names(&services),
        [PROVIDER_TAAL_ARC, PROVIDER_BITAILS, PROVIDER_WHATSONCHAIN]
    );
    assert!(services.arc_gorillapool.is_none());
}

#[tokio::test]
async fn classic_mode_broadcaster_statuses_ask_both_arcs() {
    let mut server = crate::services::mock_server().await;
    let _mocks = holds_nothing(&mut server).await;
    let options = ServicesOptions::mainnet()
        .with_arc(server.url(), None)
        .with_gorillapool(server.url(), None);
    let services = Services::with_options(Chain::Main, options).unwrap();

    let statuses = services.get_broadcaster_statuses(TXID).await;
    assert_eq!(
        status_names(&statuses),
        [PROVIDER_TAAL_ARC, PROVIDER_GORILLAPOOL_ARC]
    );
}
