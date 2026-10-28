//! #248: issuance refuses remote transport while retaining its HTTP contract.

use super::peer_fixture::{Answer, PeerCertifier};
use super::send_issuance_request;
use crate::services::providers::http_client::test_support::assert_isolated;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::ProtoWallet;

fn certifier() -> PeerCertifier {
    PeerCertifier::new(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap())
}

#[tokio::test]
async fn issuance_client_refuses_non_loopback_before_transport() {
    for scheme in ["http", "https"] {
        let error = send_issuance_request(
            ProtoWallet::new(None),
            &format!("{scheme}://provider.invalid/issue"),
            b"{}",
            &certifier().identity(),
        )
        .await
        .unwrap_err();
        assert_isolated(&error);
    }
}

#[tokio::test]
async fn issuance_client_accepts_loopback_fixtures() {
    let certifier = certifier();
    let mut server = mockito::Server::new_async().await;
    certifier.handshake(&mut server).await;
    let request = certifier
        .general(&mut server, "/issue", 200, Answer::Signed, |_| {
            br#"{"serverNonce":"fixture","certificate":null}"#.to_vec()
        })
        .match_header("content-type", "application/json")
        .match_body("{}")
        .expect(1)
        .create_async()
        .await;

    let response = send_issuance_request(
        ProtoWallet::new(None),
        &format!("{}/issue", server.url()),
        b"{}",
        &certifier.identity(),
    )
    .await
    .unwrap();
    assert_eq!(response.server_nonce, "fixture");
    request.assert_async().await;
}

#[tokio::test]
async fn issuance_client_refuses_non_loopback_redirects() {
    let mut server = mockito::Server::new_async().await;
    let redirect = server
        .mock("POST", "/.well-known/auth")
        .with_status(307)
        .with_header("location", "https://provider.invalid/.well-known/auth")
        .expect(1)
        .create_async()
        .await;

    // The SDK refuses the redirect status before any hop (bsv-rs #320).
    let error = send_issuance_request(
        ProtoWallet::new(None),
        &format!("{}/issue", server.url()),
        b"{}",
        &certifier().identity(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("redirect refused: 307"), "{error}");
    redirect.assert_async().await;
}
