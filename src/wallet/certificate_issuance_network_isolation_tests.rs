//! #248: issuance refuses remote transport while retaining its HTTP contract.

use super::send_issuance_request;
use crate::services::providers::http_client::test_support::assert_isolated;

#[tokio::test]
async fn issuance_client_refuses_non_loopback_before_transport() {
    for scheme in ["http", "https"] {
        let error = send_issuance_request(
            &format!("{scheme}://provider.invalid/issue"),
            b"{}",
            "fixture",
            "fixture-certifier",
        )
        .await
        .unwrap_err();
        assert_isolated(&error);
    }
}

#[tokio::test]
async fn issuance_client_accepts_loopback_fixtures() {
    let mut server = mockito::Server::new_async().await;
    let request = server
        .mock("POST", "/issue")
        .match_header("content-type", "application/json")
        .match_header("x-bsv-auth-version", "0.1")
        .match_header("x-bsv-identity-key", "fixture-client")
        .match_body("{}")
        .with_header("content-type", "application/json")
        .with_header("x-bsv-auth-identity-key", "fixture-certifier")
        .with_body(r#"{"serverNonce":"fixture","certificate":null}"#)
        .expect(1)
        .create_async()
        .await;

    let response = send_issuance_request(
        &format!("{}/issue", server.url()),
        b"{}",
        "fixture-client",
        "fixture-certifier",
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
        .mock("POST", "/issue")
        .with_status(307)
        .with_header("location", "https://provider.invalid/issue")
        .expect(1)
        .create_async()
        .await;

    let error = send_issuance_request(
        &format!("{}/issue", server.url()),
        b"{}",
        "fixture",
        "fixture",
    )
    .await
    .unwrap_err();
    assert_isolated(&error);
    redirect.assert_async().await;
}
