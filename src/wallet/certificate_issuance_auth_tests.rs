//! #315: the issuance channel is BRC-104 mutual auth, as the reference's
//! `AuthFetch` speaks it (ts-stack@edf6e03 `Wallet.ts:773-795`): the certifier
//! is a peer that signed, not a header a server wrote. The witnesses of the
//! design's section 3.3 (`docs/design/issuance-mutual-auth.md`).

use super::*;
use mockito::{Matcher, Mock, Request, ServerGuard};

/// A body the header check would pass and the envelope check refuses.
const NO_CERTIFICATE: &str = r#"{"serverNonce":"fixture","certificate":null}"#;

/// The prefix of every refusal bsv-rs's Peer or transport answers.
const SEND_REFUSED: &str = "wallet error: Network error: Failed to send request: ";

fn key(n: u8) -> PrivateKey {
    PrivateKey::from_hex(&format!("{n:064x}")).unwrap()
}

fn no_certificate(_: &Request) -> Vec<u8> {
    NO_CERTIFICATE.as_bytes().to_vec()
}

async fn issue(server: &ServerGuard) -> bsv_rs::Result<WalletCertificate> {
    let wallet = wallet().await;
    acquire_from_fixture(
        &wallet,
        args(certifier(), format!("{}/issue", server.url())),
    )
    .await
}

/// The refusal's text, which must be bsv-rs's and carry `words`.
async fn refused_in_send(server: &ServerGuard, words: &str) {
    let error = issue(server).await.unwrap_err().to_string();
    assert!(
        error.starts_with(SEND_REFUSED) && error.contains(words),
        "{error}"
    );
}

/// `certifier`'s answer on `/issue`, expected once.
async fn answered(
    server: &mut ServerGuard,
    certifier: &PeerCertifier,
    status: u16,
    answer: Answer,
    body: impl Fn(&Request) -> Vec<u8> + Send + Sync + 'static,
) -> Mock {
    certifier
        .general(server, "/issue", status, answer, body)
        .expect(1)
        .create_async()
        .await
}

/// A plain answer on `path` (its `location` read on a redirect status),
/// expected `hits` times.
async fn plain(server: &mut ServerGuard, path: &str, status: usize, hits: usize) -> Mock {
    let location = format!("{}/elsewhere", server.url());
    server
        .mock("POST", path)
        .with_status(status)
        .with_header("location", &location)
        .with_header("x-bsv-auth-identity-key", &certifier())
        .with_body(NO_CERTIFICATE)
        .expect(hits)
        .create_async()
        .await
}

#[tokio::test]
async fn a_signing_certifier_is_answered_and_its_certificate_stored() {
    // A server nonce the certifier made for the subject (`verifyNonce.ts:26-57`);
    // `response_run_with` checks the stored row when the call succeeds.
    let data = vec![b'a'; 16];
    let hmac = ProtoWallet::new(Some(key(31)))
        .create_hmac(CreateHmacArgs {
            data: data.clone(),
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "server hmac"),
            key_id: "a".repeat(16),
            counterparty: Some(Counterparty::Other(key(37).public_key())),
            ..Default::default()
        })
        .unwrap()
        .hmac;
    let observer = ObservedWallet::default();
    let nonce = [data, hmac.to_vec()].concat();
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), nonce, |_| {}).await;
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn a_signing_server_with_another_key_is_refused_with_the_reference_text() {
    let mut server = mockito::Server::new_async().await;
    let impostor = PeerCertifier::new(key(41));
    impostor.handshake(&mut server).await;
    let request = answered(&mut server, &impostor, 200, Answer::Signed, no_certificate).await;
    // Wallet.ts:789-791, the received key the session's signed sender.
    assert_wallet_error(
        issue(&server).await,
        &format!(
            "Validation error: Invalid certifier! Expected: {}, Received: {}",
            certifier(),
            impostor.identity()
        ),
    );
    request.assert_async().await;
}

#[tokio::test]
async fn a_plain_server_writing_the_identity_header_is_refused_before_the_body() {
    let mut server = mockito::Server::new_async().await;
    let handshake = plain(&mut server, "/.well-known/auth", 200, 1).await;
    // No keyring is sent past the handshake.
    let request = plain(&mut server, "/issue", 200, 0).await;
    refused_in_send(&server, "Failed to parse auth response").await;
    handshake.assert_async().await;
    request.assert_async().await;
}

#[tokio::test]
async fn an_unsigned_answer_to_the_signed_request_is_refused_before_the_body() {
    let mut server = mockito::Server::new_async().await;
    signing_certifier(&mut server).await;
    let request = plain(&mut server, "/issue", 200, 1).await;
    // SimplifiedFetchTransport.ts:220-232, bsv-rs #354's words.
    refused_in_send(
        &server,
        "without valid BSV authentication (missing headers: x-bsv-auth-version, x-bsv-auth-signature)",
    )
    .await;
    request.assert_async().await;
}

#[tokio::test]
async fn a_bad_response_signature_is_refused_with_the_body_unread() {
    let mut server = mockito::Server::new_async().await;
    let certifier = signing_certifier(&mut server).await;
    let request = answered(
        &mut server,
        &certifier,
        200,
        Answer::BadSignature,
        no_certificate,
    )
    .await;
    // Peer.ts:1303-1305; bsv-rs's verifier errs rather than answer false.
    assert_wallet_error(
        issue(&server).await,
        "Network error: Failed to send request: wallet error: Signature is not valid",
    );
    request.assert_async().await;
}

#[tokio::test]
async fn a_handshake_answered_404_is_refused_with_no_general_request() {
    let mut server = mockito::Server::new_async().await;
    let handshake = plain(&mut server, "/.well-known/auth", 404, 1).await;
    let request = plain(&mut server, "/issue", 200, 0).await;
    refused_in_send(&server, "404").await;
    handshake.assert_async().await;
    request.assert_async().await;
}

#[tokio::test]
async fn the_request_carries_no_caller_bsv_header() {
    let mut server = mockito::Server::new_async().await;
    let certifier = signing_certifier(&mut server).await;
    let request = certifier
        .general(&mut server, "/issue", 200, Answer::Signed, no_certificate)
        .match_header("x-bsv-identity-key", Matcher::Missing)
        .match_header("content-type", "application/json")
        .expect(1)
        .create_async()
        .await;
    assert_wallet_error(
        issue(&server).await,
        "Validation error: No certificate received from certifier!",
    );
    request.assert_async().await;
    // AuthFetch.ts:582-601: the transport's six, nothing of the caller's,
    // and the request signed by the wallet's key.
    let mut auth_headers = vec![
        "x-bsv-auth-identity-key",
        "x-bsv-auth-nonce",
        "x-bsv-auth-request-id",
        "x-bsv-auth-signature",
        "x-bsv-auth-version",
        "x-bsv-auth-your-nonce",
    ];
    auth_headers.sort_unstable();
    assert_eq!(certifier.request_headers(), vec![auth_headers]);
    assert_eq!(certifier.client_signatures(), vec![true]);
}

#[tokio::test]
async fn a_signed_400_with_an_error_body_reaches_the_envelope_check() {
    let mut server = mockito::Server::new_async().await;
    let certifier = signing_certifier(&mut server).await;
    let request = answered(&mut server, &certifier, 400, Answer::Signed, |_| {
        br#"{"status":"error","code":"ERR_BAD_REQUEST","description":"no"}"#.to_vec()
    })
    .await;
    // Wallet.ts:779-795 has no status gate.
    assert_wallet_error(
        issue(&server).await,
        "Validation error: No certificate received from certifier!",
    );
    request.assert_async().await;
}

#[tokio::test]
async fn a_redirect_on_the_handshake_is_refused() {
    let mut server = mockito::Server::new_async().await;
    let handshake = plain(&mut server, "/.well-known/auth", 307, 1).await;
    let hop = plain(&mut server, "/elsewhere", 200, 0).await;
    let request = plain(&mut server, "/issue", 200, 0).await;
    refused_in_send(&server, "redirect refused: 307").await;
    handshake.assert_async().await;
    hop.assert_async().await;
    request.assert_async().await;
}

#[tokio::test]
async fn a_redirect_on_the_signed_request_is_refused() {
    let mut server = mockito::Server::new_async().await;
    signing_certifier(&mut server).await;
    let request = plain(&mut server, "/issue", 307, 1).await;
    let hop = plain(&mut server, "/elsewhere", 200, 0).await;
    refused_in_send(&server, "redirect refused: 307").await;
    request.assert_async().await;
    hop.assert_async().await;
}
