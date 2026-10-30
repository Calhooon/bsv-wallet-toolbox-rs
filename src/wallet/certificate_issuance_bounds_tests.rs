//! #423: bsv-rs beta.3's six BRC-104 response bounds (#419 item 5) through the
//! issuance door. The certifier fixture answers the handshake; the offending
//! answer is then the vector's, and the refusal is bsv-rs's text inside the
//! door's envelope, the class the door's other transport refusals carry.

use super::*;
use crate::services::providers::http_client::brc104_door_cases::{case, header_limit_fault, Case};
use mockito::{Mock, ServerGuard};

/// The issuance door's envelope around a Peer or transport refusal
/// (`certificate_issuance_auth.rs`'s `send_error`, then the wallet boundary).
const ENVELOPE: &str = "Network error: Failed to send request: authentication error: ";

/// The vector's answer on `path`, expected once.
async fn served(server: &mut ServerGuard, path: &str, case: &Case) -> Mock {
    let mut mock = server
        .mock("POST", path)
        .with_header("x-bsv-auth-version", "0.1")
        .with_header("x-bsv-auth-identity-key", &certifier())
        .with_body(case.body());
    for (name, value) in case.headers() {
        mock = mock.with_header(&name, &value);
    }
    mock.expect(1).create_async().await
}

/// The door's refusal of `case`'s answer: on the handshake path with no
/// session, else on `/issue` after the fixture's handshake.
async fn refusal(case: &Case) -> (bsv_rs::Error, String) {
    let mut server = mockito::Server::new_async().await;
    let path = if case.handshake {
        "/.well-known/auth"
    } else {
        signing_certifier(&mut server).await;
        "/issue"
    };
    let answer = served(&mut server, path, case).await;
    let wallet = wallet().await;
    let url = format!("{}/issue", server.url());
    let error = acquire_from_fixture(&wallet, args(certifier(), url))
        .await
        .unwrap_err();
    answer.assert_async().await;
    (error, server.url())
}

/// The door's refusal is `WalletError` with bsv-rs's recorded text inside the
/// envelope, byte for byte.
async fn refused(family: &str, name: &str) {
    let case = case(family, name);
    let (error, origin) = refusal(&case).await;
    match error {
        bsv_rs::Error::WalletError(message) => {
            let text = case.refusal(&origin, "/issue");
            assert_eq!(message, format!("{ENVELOPE}{text}"));
        }
        other => panic!("{name}: expected WalletError, got {other:?}"),
    }
}

/// The text the Peer answers a delivery with, inside the envelope.
async fn peer_refused(family: &str, name: &str, text: &str) {
    let (error, _) = refusal(&case(family, name)).await;
    match error {
        bsv_rs::Error::WalletError(message) => assert_eq!(message, format!("{ENVELOPE}{text}")),
        other => panic!("{name}: expected WalletError, got {other:?}"),
    }
}

#[tokio::test]
async fn an_oversized_requested_certificates_header_is_refused_at_the_door() {
    refused("certificate_header", "certificate_ascii_262145").await;
}

#[tokio::test]
async fn a_base64_signature_is_refused_at_the_door() {
    refused("signature", "signature_\"MAA=\"").await;
}

#[tokio::test]
async fn a_signature_past_2048_characters_is_refused_at_the_door() {
    refused("signature", "signature_2050_characters").await;
}

#[tokio::test]
async fn a_certificate_request_answer_keeps_its_type_through_the_door() {
    // The type kept: the Peer reads a certificateRequest. Since bsv-rs
    // 0.9.0-beta.4 (#445) its intake checks the nonces first, in the
    // reference's order (`AuthMessageValidation.ts:64-79,355-389`), so the
    // vector's frame, which carries none, is refused on its nonce; before,
    // `requestedCertificates must be an object.`
    peer_refused(
        "message_type",
        "type_certificateRequest",
        "certificateRequest.nonce must be canonical base64.",
    )
    .await;
    // The vector's other spelling is a general message: the Peer's intake
    // refuses the fixture's answer, which carries no nonce (#445); before,
    // `Unable to verify nonce for general message from: <sender>`.
    peer_refused(
        "message_type",
        "type_CertificateRequest",
        "general.nonce must be canonical base64.",
    )
    .await;
}

#[tokio::test]
async fn a_handshake_body_past_1_mib_is_refused_at_the_door() {
    refused("body", "handshake_1048577").await;
}

#[tokio::test]
async fn an_application_body_past_16_mib_is_refused_at_the_door() {
    refused("body", "general_16777217").await;
}

#[tokio::test]
async fn a_request_id_not_of_32_bytes_is_refused_at_the_door() {
    refused("request_id", "request_id_31_bytes").await;
    refused("request_id", "request_id_33_bytes").await;
    refused("request_id", "request_id_\"!\"").await;
}

#[tokio::test]
async fn oversized_signed_headers_are_refused_at_the_door() {
    refused("signed_headers", "signed_name_1025").await;
    refused("signed_headers", "signed_value_32769").await;
    refused("signed_headers", "signed_total_262145").await;
}

#[tokio::test]
async fn the_signed_header_count_bound_is_not_reached_over_http1() {
    // The door's client speaks HTTP/1: 512 signed headers already fail in
    // hyper, so the 513th never reaches bsv-rs's count bound.
    let (error, origin) = refusal(&case("signed_headers", "signed_count_513")).await;
    assert_eq!(error, header_limit_fault(&format!("{origin}/issue")));
}
