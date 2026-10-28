//! #315: the fixture certifier's self-check: its signature verifies through
//! `ProtoWallet::verify_signature`, and a bsv-rs Peer completes a request
//! through it.

use super::*;
use crate::services::providers::http_client::SimplifiedFetchTransport;
use bsv_rs::auth::{Peer, PeerOptions};
use tokio::sync::oneshot;

fn key(n: u8) -> PrivateKey {
    PrivateKey::from_hex(&format!("{n:064x}")).unwrap()
}

#[test]
fn fixture_signature_verifies_through_proto_wallet() {
    let certifier = PeerCertifier::new(key(31));
    let client = key(37);
    let data = b"request id, status, headers, body".to_vec();
    let signature = certifier.sign(&data, "nonce-a nonce-b", &client.public_key());
    let verified = ProtoWallet::new(Some(client))
        .verify_signature(VerifySignatureArgs {
            data: Some(data),
            signature,
            protocol_id: auth_protocol(),
            key_id: "nonce-a nonce-b".to_string(),
            counterparty: Some(Counterparty::Other(key(31).public_key())),
            ..Default::default()
        })
        .unwrap();
    assert!(verified.valid);
}

/// A bsv-rs Peer, the client's, sends one general request to `/issue`.
async fn peer_run(answer: Answer) -> (PeerCertifier, bsv_rs::Result<(PublicKey, Vec<u8>)>) {
    let certifier = PeerCertifier::new(key(31));
    let mut server = mockito::Server::new_async().await;
    let handshake = certifier.handshake(&mut server).await;
    let general = certifier
        .general(&mut server, "/issue", 200, answer, |_| {
            b"{\"ok\":true}".to_vec()
        })
        .expect(1)
        .create_async()
        .await;
    let peer = Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(key(37))),
        transport: SimplifiedFetchTransport::new(&server.url()),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: true,
        originator: None,
    });
    peer.start();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    peer.listen_for_general_messages(move |sender, payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send((sender, payload));
        }
        Box::pin(async { Ok(()) })
    })
    .await;
    let request = HttpRequest {
        request_id: [9; 32],
        method: "POST".to_string(),
        path: "/issue".to_string(),
        search: String::new(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: b"{}".to_vec(),
    };
    let sent = peer.to_peer(&request.to_payload(), None, Some(5_000)).await;
    handshake.assert_async().await;
    general.assert_async().await;
    let received = match sent {
        Ok(()) => Ok(rx.await.unwrap()),
        Err(error) => Err(error),
    };
    (certifier, received)
}

#[tokio::test]
async fn fixture_answers_a_bsv_rs_peer() {
    let (certifier, received) = peer_run(Answer::Signed).await;
    let (sender, payload) = received.unwrap();
    let response = HttpResponse::from_payload(&payload).unwrap();
    assert_eq!(sender.to_hex(), certifier.identity());
    assert_eq!(response.body, b"{\"ok\":true}");
    assert_eq!(certifier.client_signatures(), vec![true]);
    let mut sent = SIGNED_HEADERS.map(String::from).to_vec();
    sent.sort();
    assert_eq!(certifier.request_headers(), vec![sent]);
}

#[tokio::test]
async fn fixture_bad_signature_is_refused_by_a_bsv_rs_peer() {
    // bsv-rs's verifier errs on a bad signature rather than answer
    // Ok(false), so Peer's "Invalid signature in generalMessage" is unmet.
    let (_, received) = peer_run(Answer::BadSignature).await;
    assert_eq!(
        received.unwrap_err().to_string(),
        "wallet error: Signature is not valid"
    );
}

/// A general request to `/issue` past a handshake the client opened, its
/// signature `signature` and its session nonce the certifier's unless `stale`;
/// the handler is never called, as `index.ts:1380-1395` calls no `next`.
async fn raw_run(signature: &str, stale: bool) -> (PeerCertifier, reqwest::Response) {
    let certifier = PeerCertifier::new(key(31));
    let client = key(37);
    let mut server = mockito::Server::new_async().await;
    certifier.handshake(&mut server).await;
    let handler = certifier
        .general(&mut server, "/issue", 200, Answer::Signed, |_| {
            b"{\"ok\":true}".to_vec()
        })
        .expect(0)
        .create_async()
        .await;
    let http = reqwest::Client::new();
    let mut opening = AuthMessage::new(MessageType::InitialRequest, client.public_key());
    opening.initial_nonce = Some(to_base64(&[5; 32]));
    let answer: AuthMessage = http
        .post(format!("{}/.well-known/auth", server.url()))
        .json(&opening)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session = match stale {
        true => to_base64(&[6; 32]),
        false => answer.initial_nonce.unwrap(),
    };
    let response = http
        .post(format!("{}/issue", server.url()))
        .header("content-type", "application/json")
        .header("x-bsv-auth-version", "0.1")
        .header("x-bsv-auth-identity-key", client.public_key().to_hex())
        .header("x-bsv-auth-nonce", to_base64(&[7; 32]))
        .header("x-bsv-auth-your-nonce", session)
        .header("x-bsv-auth-request-id", to_base64(&[9; 32]))
        .header("x-bsv-auth-signature", signature)
        .body("{}")
        .send()
        .await
        .unwrap();
    handler.assert_async().await;
    (certifier, response)
}

/// The middleware's refusal (`index.ts:1380-1395`): 401, none of the six
/// headers, the error body.
async fn assert_refused(response: reqwest::Response) {
    assert_eq!(response.status(), 401);
    for name in SIGNED_HEADERS {
        assert!(response.headers().get(name).is_none(), "{name}");
    }
    assert_eq!(response.text().await.unwrap(), AUTH_FAILED);
}

#[tokio::test]
async fn fixture_refuses_a_bad_client_signature_unsigned() {
    let (certifier, response) = raw_run("3006020101020101", false).await;
    assert_refused(response).await;
    assert_eq!(certifier.client_signatures(), vec![false]);
}

#[tokio::test]
async fn fixture_refuses_an_unknown_session_unsigned() {
    let (certifier, response) = raw_run("3006020101020101", true).await;
    assert_refused(response).await;
    assert_eq!(certifier.client_signatures(), Vec::<bool>::new());
}
