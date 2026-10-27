//! #248: the unauthenticated storage HTTP path is isolated in both builds.

use super::{Error, StorageClient, TableSettings, WalletStorageWriter};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::ProtoWallet;

#[path = "../../../tests/common/transport_isolation.rs"]
mod transport_isolation;
use transport_isolation::assert_isolated;

fn client(endpoint: &str) -> StorageClient<ProtoWallet> {
    StorageClient::new_unauthenticated(
        ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap())),
        endpoint,
    )
}

#[tokio::test]
async fn storage_client_refuses_non_loopback_before_transport() {
    for scheme in ["http", "https"] {
        let client = client(&format!("{scheme}://provider.invalid/rpc"));
        assert_isolated(client.make_available().await.unwrap_err());
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
    assert_isolated(error);
    redirect.assert_async().await;
}
