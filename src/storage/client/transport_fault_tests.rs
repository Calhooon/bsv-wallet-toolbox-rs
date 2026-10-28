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

/// A verified answer for a different request leaves our response wait pending.
/// The timeout therefore follows a successful send, without a provider call.
#[tokio::test]
async fn response_wait_timeout_keeps_the_transport_class() {
    use bsv_rs::auth::transports::HttpResponse;
    use bsv_rs::auth::{AuthMessage, MessageType, PeerSession};
    use bsv_rs::primitives::to_base64;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, Protocol, SecurityLevel};

    let mut server = mockito::Server::new_async().await;
    let client = client(&server.url()).with_timeout(std::time::Duration::from_millis(20));
    let server_key = PrivateKey::from_bytes(&[8; 32]).unwrap();
    let server_wallet = ProtoWallet::new(Some(server_key.clone()));
    let local_nonce =
        bsv_rs::auth::utils::create_nonce(client.wallet(), None, Some("bsv-wallet-toolbox"))
            .await
            .unwrap();
    let mut session = PeerSession::with_nonce(local_nonce.clone());
    session.is_authenticated = true;
    session.peer_nonce = Some(to_base64(&[9; 32]));
    session.peer_identity_key = Some(server_key.public_key());
    client
        .peer()
        .session_manager()
        .write()
        .await
        .add_session(session)
        .unwrap();
    client
        .set_server_identity_key(server_key.public_key())
        .await;

    let other_request = [42; 32];
    let response = HttpResponse {
        request_id: other_request,
        status: 200,
        headers: vec![],
        body: b"{}".to_vec(),
    };
    let mut message = AuthMessage::new(MessageType::General, server_key.public_key());
    message.nonce = Some(to_base64(&[10; 32]));
    message.your_nonce = Some(local_nonce.clone());
    message.payload = Some(response.to_payload());
    let signature = server_wallet
        .create_signature(CreateSignatureArgs {
            data: Some(message.signing_data()),
            protocol_id: Protocol::new(
                SecurityLevel::Counterparty,
                bsv_rs::auth::types::AUTH_PROTOCOL_ID,
            ),
            key_id: message.get_key_id(Some(&local_nonce)),
            counterparty: Some(Counterparty::Other(
                PrivateKey::from_bytes(&[7; 32]).unwrap().public_key(),
            )),
            ..Default::default()
        })
        .unwrap()
        .signature;
    let reply = server
        .mock("POST", "/")
        .with_header("x-bsv-auth-version", &message.version)
        .with_header("x-bsv-auth-identity-key", &server_key.public_key().to_hex())
        .with_header("x-bsv-auth-request-id", &to_base64(&other_request))
        .with_header("x-bsv-auth-nonce", message.nonce.as_deref().unwrap())
        .with_header("x-bsv-auth-your-nonce", &local_nonce)
        .with_header("x-bsv-auth-signature", &hex::encode(signature))
        .with_body("{}")
        .expect(1)
        .create_async()
        .await;

    let sdk = bsv_rs::Error::from(client.make_available().await.unwrap_err());
    assert!(
        matches!(&sdk, bsv_rs::Error::TransportFault {
        fault: TransportFault::MayHaveBeenReceived, status: None, detail,
    } if detail == "Request timed out"),
        "{sdk:?}"
    );
    assert_eq!(
        sdk.to_string(),
        "transport fault MAY_HAVE_BEEN_RECEIVED: Request timed out"
    );
    reply.assert_async().await;
}
