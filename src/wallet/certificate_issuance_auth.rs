//! #315: the issuance request over BRC-104 mutual auth, as the reference's
//! `AuthFetch` sends it (ts-stack@edf6e03 `Wallet.ts:773-795`): a handshake
//! with the certifier on `/.well-known/auth`, then the request as a signed
//! general message, its answer verified by bsv-rs's Peer before it is read.
//!
//! A Peer per call, as `Wallet.ts:773` builds an `AuthFetch` per call, over
//! the wallet's `ProtoWallet`: the same root key, owned as `Peer` needs it, so
//! `Wallet<S, V>` gains no bound. At bsv-rs 0.9.0-beta.3 `Peer::start`
//! installs the verifying path (bsv-rs #353): the initial response's nonce and
//! signature, and a general message's session, sender and signature.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsv_rs::auth::transports::{HttpRequest, HttpResponse};
use bsv_rs::auth::{Peer, PeerOptions};
use bsv_rs::primitives::PublicKey;
use bsv_rs::wallet::ProtoWallet;
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::services::providers::http_client::{SimplifiedFetchTransport, Url};

/// The answer's deadline (`AuthFetch.ts:90,298-300`).
pub(super) const RESPONSE_DEADLINE: Duration = Duration::from_secs(30);

/// The reference's text when the deadline passes (`AuthFetch.ts:299`).
const TIMED_OUT: &str = "Timed out waiting for authenticated response.";

/// Posts `body` as JSON to `endpoint` over BRC-104 and answers the verified
/// sender with the response. The transport is the shared guarded one: under
/// test and bench-support every send, the handshake included, is checked
/// against #182's policy and goes through its client (`with_client`).
pub(super) async fn post_authenticated(
    peer_wallet: ProtoWallet,
    endpoint: &str,
    body: &[u8],
    deadline: Duration,
) -> Result<(PublicKey, HttpResponse)> {
    let url = Url::parse(endpoint)
        .map_err(|e| Error::NetworkError(format!("Invalid certifier URL: {e}")))?;
    let peer = Peer::new(PeerOptions {
        wallet: peer_wallet,
        transport: SimplifiedFetchTransport::new(&url.origin().ascii_serialization()),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: None,
    });
    peer.start();

    // The request nonce, 32 random bytes (`AuthFetch.ts:644-675`); the only
    // header `content-type`, so no caller `x-bsv-*` header is signed or sent.
    let mut request_id = [0u8; 32];
    crate::rng::os_fill(&mut request_id);
    let request = HttpRequest {
        request_id,
        method: "POST".to_string(),
        path: url.path().to_string(),
        search: url.query().map(|q| format!("?{q}")).unwrap_or_default(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: body.to_vec(),
    };

    // The answer whose leading 32 bytes are the request id (`AuthFetch.ts:437-439`).
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    peer.listen_for_general_messages(move |sender, payload| {
        if payload.get(..32) == Some(&request_id[..]) {
            if let Some(tx) = tx.lock().ok().and_then(|mut tx| tx.take()) {
                let _ = tx.send((sender, payload));
            }
        }
        Box::pin(async { Ok(()) })
    })
    .await;

    // No identity pinned: a fresh `AuthFetch` knows no peer before its answer
    // (`Wallet.ts:773`), and the sender is compared with the certifier after.
    let exchange = async {
        let wait = Some(u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX));
        peer.to_peer(&request.to_payload(), None, wait)
            .await
            .map_err(send_error)?;
        rx.await
            .map_err(|_| Error::NetworkError("Response channel closed".to_string()))
    };
    let (sender, payload) = tokio::time::timeout(deadline, exchange)
        .await
        .map_err(|_| Error::NetworkError(TIMED_OUT.to_string()))??;
    let response = HttpResponse::from_payload(&payload)
        .map_err(|e| Error::NetworkError(format!("Failed to parse response: {e}")))?;
    Ok((sender, response))
}

/// A transport fault keeps bsv-rs's class (bsv-rs #87), as the storage
/// client's does; any other refusal is the named host's.
fn send_error(error: bsv_rs::Error) -> Error {
    if error.transport_fault().is_some() {
        Error::SdkError(error)
    } else {
        Error::NetworkError(format!("Failed to send request: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::peer_fixture::{Answer, PeerCertifier};
    use super::*;
    use bsv_rs::primitives::PrivateKey;

    #[tokio::test]
    async fn an_answer_past_the_deadline_is_the_reference_timeout() {
        let certifier = PeerCertifier::new(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap());
        let mut server = mockito::Server::new_async().await;
        certifier.handshake(&mut server).await;
        let _request = certifier
            .general(&mut server, "/issue", 200, Answer::Signed, |_| {
                std::thread::sleep(Duration::from_millis(600));
                b"{}".to_vec()
            })
            .create_async()
            .await;
        let url = format!("{}/issue", server.url());
        let deadline = Duration::from_millis(200);
        let error = post_authenticated(ProtoWallet::new(None), &url, b"{}", deadline)
            .await
            .err()
            .unwrap();
        assert_eq!(error.to_string(), format!("Network error: {TIMED_OUT}"));
    }
}
