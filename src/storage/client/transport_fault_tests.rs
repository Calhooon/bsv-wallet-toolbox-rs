//! #63/#140: a transport fault of the authenticated peer reaches the host as
//! bsv-rs's typed `Error::TransportFault` (bsv-rs #87), never as the toolbox's
//! text. Each test reads the class by type, so a fault whose words change
//! still classifies.

use super::StorageClient;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::TransportFault;
use bsv_rs::wallet::ProtoWallet;

fn client(url: &str) -> StorageClient<ProtoWallet> {
    let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7u8; 32]).unwrap()));
    StorageClient::new(wallet, url).with_timeout(std::time::Duration::from_secs(5))
}

/// A connection never made is `NeverSent`: no byte of the request left.
#[tokio::test]
async fn a_refused_connection_is_bsv_rs_never_sent() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let err = client(&format!("http://127.0.0.1:{port}"))
        .make_available()
        .await
        .expect_err("nothing listens on the port");
    let sdk = bsv_rs::Error::from(err);
    assert_eq!(
        sdk.transport_fault(),
        Some(TransportFault::NeverSent),
        "{sdk:?}"
    );
}

/// A handshake answered 429 with `Retry-After` is `RateLimited` with its
/// wait, whatever the body says.
#[tokio::test]
async fn a_rate_limited_handshake_carries_its_wait() {
    let mut server = mockito::Server::new_async().await;
    let _limited = server
        .mock("POST", mockito::Matcher::Any)
        .with_status(429)
        .with_header("retry-after", "7")
        .with_body("words that change from server to server")
        .expect_at_least(1)
        .create_async()
        .await;
    let err = client(&server.url())
        .make_available()
        .await
        .expect_err("the handshake is refused");
    let sdk = bsv_rs::Error::from(err);
    assert_eq!(
        sdk.transport_fault(),
        Some(TransportFault::RateLimited {
            retry_after_ms: Some(7_000)
        }),
        "{sdk:?}"
    );
}
