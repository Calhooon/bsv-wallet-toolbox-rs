//! #423: bsv-rs beta.3's six BRC-104 response bounds (#419 item 5) through the
//! authenticated storage client. The handshake cases answer a client with no
//! session; the others answer a general message on a seeded session, as
//! `transport_fault_tests.rs` seeds it. Each refusal is bsv-rs's text inside
//! the client's envelope, a `NetworkError` that reaches the wallet boundary as
//! a Transport `WalletFault` (the former `WalletError`), as the client's
//! other non-transport refusals do.

use super::StorageClient;
use crate::services::providers::http_client::brc104_door_cases::{case, header_limit_fault, Case};
use crate::storage::traits::WalletStorageWriter;
use crate::Error;
use bsv_rs::auth::PeerSession;
use bsv_rs::primitives::{to_base64, PrivateKey};
use bsv_rs::wallet::ProtoWallet;

/// The client's envelope around a Peer or transport refusal
/// (`storage_client.rs`'s `rpc_call`).
const ENVELOPE: &str = "Network error: Failed to send request: authentication error: ";

fn server_key() -> PrivateKey {
    PrivateKey::from_bytes(&[8; 32]).unwrap()
}

/// A client with a session to `server_key`, so its next call is a general
/// message; the `transport_fault_tests.rs` seed.
async fn seeded(url: &str) -> StorageClient<ProtoWallet> {
    let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap()));
    let client = StorageClient::new(wallet, url).with_timeout(std::time::Duration::from_secs(5));
    let nonce =
        bsv_rs::auth::utils::create_nonce(client.wallet(), None, Some("bsv-wallet-toolbox"))
            .await
            .unwrap();
    let mut session = PeerSession::with_nonce(nonce);
    session.is_authenticated = true;
    session.peer_nonce = Some(to_base64(&[9; 32]));
    session.peer_identity_key = Some(server_key().public_key());
    let sessions = client.peer().session_manager();
    sessions.write().await.add_session(session).unwrap();
    client
        .set_server_identity_key(server_key().public_key())
        .await;
    client
}

/// The client's refusal of `case`'s answer, on the handshake path or on `/`.
async fn refusal(case: &Case) -> (Error, String) {
    let mut server = mockito::Server::new_async().await;
    let (client, path) = if case.handshake {
        let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32]).unwrap()));
        (
            StorageClient::new(wallet, server.url()),
            "/.well-known/auth",
        )
    } else {
        (seeded(&server.url()).await, "/")
    };
    let mut mock = server
        .mock("POST", path)
        .with_header("x-bsv-auth-version", "0.1")
        .with_header(
            "x-bsv-auth-identity-key",
            &server_key().public_key().to_hex(),
        )
        .with_body(case.body());
    for (name, value) in case.headers() {
        mock = mock.with_header(&name, &value);
    }
    let answer = mock.expect(1).create_async().await;
    let error = client.make_available().await.unwrap_err();
    answer.assert_async().await;
    (error, server.url())
}

/// `text` inside the envelope, a `NetworkError` here and a Transport
/// `WalletFault` through the boundary (bsv-rs #439), displayed as the
/// `WalletError` of the same message.
fn assert_enveloped(name: &str, error: Error, text: &str) {
    let expected = format!("{ENVELOPE}{text}");
    assert!(matches!(error, Error::NetworkError(_)), "{name}: {error:?}");
    assert_eq!(error.to_string(), expected, "{name}");
    assert_eq!(
        bsv_rs::Error::from(error),
        bsv_rs::Error::WalletFault(bsv_rs::WalletFault::new(
            bsv_rs::WalletFaultFamily::Transport,
            expected
        )),
        "{name}"
    );
}

/// bsv-rs's recorded refusal of the vector's case, byte for byte.
async fn refused(family: &str, name: &str) {
    let case = case(family, name);
    let (error, origin) = refusal(&case).await;
    assert_enveloped(name, error, &case.refusal(&origin, "/"));
}

#[tokio::test]
async fn an_oversized_requested_certificates_header_is_refused_by_the_client() {
    refused("certificate_header", "certificate_ascii_262145").await;
}

#[tokio::test]
async fn a_base64_signature_is_refused_by_the_client() {
    refused("signature", "signature_\"MAA=\"").await;
}

#[tokio::test]
async fn a_signature_past_2048_characters_is_refused_by_the_client() {
    refused("signature", "signature_2050_characters").await;
}

#[tokio::test]
async fn a_certificate_request_answer_keeps_its_type_through_the_client() {
    // The type kept: the Peer reads a certificateRequest. Since bsv-rs
    // 0.9.0-beta.4 (#445) its intake checks the nonces before the rest, in the
    // reference's order (`AuthMessageValidation.ts:64-79,355-389`), so the
    // vector's frame, which carries none, is refused on its nonce; before,
    // `requestedCertificates must be an object.`
    let name = "type_certificateRequest";
    let (error, _) = refusal(&case("message_type", name)).await;
    assert_enveloped(
        name,
        error,
        "certificateRequest.nonce must be canonical base64.",
    );
    // The vector's other spelling is a general message: the Peer's intake
    // refuses the answer, which carries no nonce (#445); before,
    // `Unable to verify nonce for general message from: <server key>`.
    let name = "type_CertificateRequest";
    let (error, _) = refusal(&case("message_type", name)).await;
    assert_enveloped(name, error, "general.nonce must be canonical base64.");
}

#[tokio::test]
async fn a_handshake_body_past_1_mib_is_refused_by_the_client() {
    refused("body", "handshake_1048577").await;
}

#[tokio::test]
async fn an_application_body_past_16_mib_is_refused_by_the_client() {
    refused("body", "general_16777217").await;
}

#[tokio::test]
async fn a_request_id_not_of_32_bytes_is_refused_by_the_client() {
    refused("request_id", "request_id_31_bytes").await;
    refused("request_id", "request_id_33_bytes").await;
    refused("request_id", "request_id_\"!\"").await;
}

#[tokio::test]
async fn oversized_signed_headers_are_refused_by_the_client() {
    for name in [
        "signed_name_1025",
        "signed_value_32769",
        "signed_total_262145",
    ] {
        refused("signed_headers", name).await;
    }
}

#[tokio::test]
async fn the_signed_header_count_bound_is_not_reached_over_http1() {
    // As at the issuance door: hyper's HTTP/1 parser takes 100 headers, so
    // the 513th never reaches bsv-rs's count bound.
    let (error, origin) = refusal(&case("signed_headers", "signed_count_513")).await;
    let error = bsv_rs::Error::from(error);
    assert_eq!(error, header_limit_fault(&format!("{origin}/")));
}
