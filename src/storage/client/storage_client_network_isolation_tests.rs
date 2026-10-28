//! #248/#263: both storage HTTP paths are isolated in both builds.

use super::{assert_isolated, StorageClient, TableSettings, WalletStorageWriter};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::TransportFault;
use bsv_rs::wallet::ProtoWallet;

fn wallet() -> ProtoWallet {
    ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap()))
}

fn client(endpoint: &str) -> StorageClient<ProtoWallet> {
    StorageClient::new_unauthenticated(wallet(), endpoint)
}

#[tokio::test]
async fn storage_client_refuses_non_loopback_before_transport() {
    for scheme in ["http", "https"] {
        let client = client(&format!("{scheme}://provider.invalid/rpc"));
        assert_isolated(&client.make_available().await.unwrap_err());
    }
}

#[tokio::test]
async fn storage_client_accepts_loopback_fixtures() {
    let mut server = mockito::Server::new_async().await;
    let settings = TableSettings {
        storage_name: "loopback".to_string(),
        ..Default::default()
    };
    let request = server
        .mock("POST", "/rpc")
        .match_header("content-type", "application/json")
        .match_body(mockito::Matcher::Json(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "makeAvailable", "params": []
        })))
        .with_header("content-type", "application/json")
        .with_body(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": settings}).to_string())
        .expect(1)
        .create_async()
        .await;

    let result = client(&format!("{}/rpc", server.url()))
        .make_available()
        .await
        .unwrap();
    assert_eq!(result.storage_name, "loopback");
    request.assert_async().await;
}

#[tokio::test]
async fn storage_client_refuses_non_loopback_redirects() {
    let mut server = mockito::Server::new_async().await;
    let redirect = server
        .mock("POST", "/rpc")
        .with_status(307)
        .with_header("location", "https://provider.invalid/rpc")
        .expect(1)
        .create_async()
        .await;

    let error = client(&format!("{}/rpc", server.url()))
        .make_available()
        .await
        .unwrap_err();
    assert_isolated(&error);
    redirect.assert_async().await;
}

#[tokio::test]
async fn authenticated_storage_client_refuses_non_loopback_before_transport() {
    for scheme in ["http", "https"] {
        let client = StorageClient::new(wallet(), format!("{scheme}://provider.invalid"));
        assert_isolated(&client.make_available().await.unwrap_err());
    }
}

#[tokio::test]
async fn authenticated_storage_client_peer_accessor_refuses_non_loopback() {
    let client = StorageClient::new(wallet(), "http://provider.invalid");
    let error = client
        .peer()
        .to_peer(b"fixture", None, Some(1_000))
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("provider.invalid")
            && message.contains("#182")
            && message.contains("rule 28"),
        "{message}"
    );
}

#[tokio::test]
async fn authenticated_storage_client_accepts_loopback_handshake() {
    let mut server = mockito::Server::new_async().await;
    let handshake = server
        .mock("POST", "/.well-known/auth")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "messageType": "initialRequest"
        })))
        .with_status(429)
        .with_header("retry-after", "7")
        .with_body("loopback handshake refused")
        .expect(1)
        .create_async()
        .await;

    let client =
        StorageClient::new(wallet(), server.url()).with_timeout(std::time::Duration::from_secs(5));
    let error = client.make_available().await.unwrap_err();
    assert!(!error.to_string().contains("#182"), "{error}");
    assert_eq!(
        bsv_rs::Error::from(error).transport_fault(),
        Some(TransportFault::RateLimited {
            retry_after_ms: Some(7_000)
        })
    );
    handshake.assert_async().await;
}

// bsv-rs #320 through the policy's client (#263): a redirect status on the
// handshake is refused before its body and the hop is never asked.
#[tokio::test]
async fn authenticated_storage_client_refuses_a_redirect_hop() {
    let mut server = mockito::Server::new_async().await;
    let redirect = server
        .mock("POST", "/.well-known/auth")
        .with_status(307)
        .with_header(
            "location",
            &format!("{}/hop/.well-known/auth", server.url()),
        )
        .expect(1)
        .create_async()
        .await;
    let hop = server
        .mock("POST", "/hop/.well-known/auth")
        .expect(0)
        .create_async()
        .await;

    let client =
        StorageClient::new(wallet(), server.url()).with_timeout(std::time::Duration::from_secs(5));
    let error = client.make_available().await.unwrap_err().to_string();
    assert!(error.contains("redirect refused: 307"), "{error}");
    redirect.assert_async().await;
    hop.assert_async().await;
}
