//! #488 surface 5 (bsv-rs #445): the authenticated storage client's Peer
//! carries no certificate policy (`certificates_to_request: None`), so a
//! certificateResponse a server volunteers is refused as the reference's Peer
//! refuses it (`Peer.ts:1229-1241` at ts-stack@edf6e03), in bsv-rs's words
//! (`src/auth/peer_certificates.rs:136` at v0.9.0-beta.4), before any
//! certificate listener. Nothing live volunteers one today.
//!
//! The server's message is signed by a real bsv-rs Peer: a scratch Peer with
//! the client's wallet and originator handshakes with it in process, its
//! session is seeded into the client, and the server's certificateResponse on
//! that session is the mock's answer to the client's handshake POST.

use super::StorageClient;
use crate::storage::traits::WalletStorageWriter;
use crate::Error;
use async_trait::async_trait;
use bsv_rs::auth::transports::{Transport, TransportCallback};
use bsv_rs::auth::{AuthMessage, MasterCertificate, Peer, PeerOptions, VerifiableCertificate};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::ProtoWallet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// The client's originator (`StorageClient::ORIGINATOR`): the scratch Peer's
/// nonces verify on the client's Peer only under the same wallet and originator.
const ORIGINATOR: &str = "bsv-wallet-toolbox";

/// The client's envelope around a Peer refusal (`rpc_call`).
const ENVELOPE: &str = "Network error: Failed to send request: authentication error: ";

type Callback = Arc<Mutex<Option<Box<TransportCallback>>>>;

/// An in-process transport: sends go to a channel, deliveries through the
/// callback `start` installs.
struct Wire {
    sent: mpsc::UnboundedSender<AuthMessage>,
    callback: Callback,
}

#[async_trait]
impl Transport for Wire {
    async fn send(&self, message: &AuthMessage) -> bsv_rs::Result<()> {
        self.sent.send(message.clone()).unwrap();
        Ok(())
    }

    fn set_callback(&self, callback: Box<TransportCallback>) {
        *self.callback.lock().unwrap() = Some(callback);
    }

    fn clear_callback(&self) {
        *self.callback.lock().unwrap() = None;
    }
}

struct End {
    peer: Peer<ProtoWallet, Wire>,
    callback: Callback,
    sent: mpsc::UnboundedReceiver<AuthMessage>,
}

fn key(n: u8) -> PrivateKey {
    PrivateKey::from_bytes(&[n; 32]).unwrap()
}

/// A Peer of `n`'s wallet with no certificate policy.
fn end(n: u8) -> End {
    let (sent, received) = mpsc::unbounded_channel();
    let callback = Callback::default();
    let peer = Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(key(n))),
        transport: Wire {
            sent,
            callback: callback.clone(),
        },
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some(ORIGINATOR.into()),
    });
    peer.start();
    End {
        peer,
        callback,
        sent: received,
    }
}

async fn deliver(callback: &Callback, message: AuthMessage) -> bsv_rs::Result<()> {
    let future = callback.lock().unwrap().as_ref().unwrap()(message);
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .unwrap()
}

/// A certificate of the server (8) by certifier 9, its one field revealed to
/// the client (7).
async fn volunteered() -> VerifiableCertificate {
    let master = MasterCertificate::issue_for_subject(
        &ProtoWallet::new(Some(key(9))),
        &key(9),
        key(8).public_key(),
        [("name".to_string(), "Alice".to_string())].into(),
        [1; 32],
        Some([2; 32]),
        Some(ORIGINATOR),
    )
    .await
    .unwrap();
    let keyring = MasterCertificate::create_keyring_for_verifier(
        &ProtoWallet::new(Some(key(8))),
        &key(9).public_key(),
        &key(7).public_key(),
        &master.certificate.fields,
        &["name".to_string()],
        &master.master_keyring,
        &master.certificate.serial_number,
        None,
        None,
        Some(ORIGINATOR),
    )
    .await
    .unwrap();
    VerifiableCertificate::new(master.certificate, keyring)
}

#[tokio::test]
async fn a_volunteered_certificate_response_is_refused_by_the_policyless_client() {
    // The scratch client (7) and the server (8) authenticate in process.
    let mut scratch = end(7);
    let mut server = end(8);
    let identity = key(8).public_key().to_hex();
    let (session, ()) = tokio::join!(
        scratch
            .peer
            .get_authenticated_session(Some(&identity), Some(5000)),
        async {
            let request = scratch.sent.recv().await.unwrap();
            deliver(&server.callback, request).await.unwrap();
            let response = server.sent.recv().await.unwrap();
            deliver(&scratch.callback, response).await.unwrap();
        }
    );
    let session = session.unwrap();
    server
        .peer
        .send_certificate_response(&key(7).public_key().to_hex(), vec![volunteered().await])
        .await
        .unwrap();
    let message = server.sent.recv().await.unwrap();
    assert_eq!(message.certificates.as_ref().map(Vec::len), Some(1));

    // The storage client holds that session; its call begins a handshake
    // (no server identity is set), and the server's answer is the
    // certificateResponse on the seeded session.
    let mut mock_server = mockito::Server::new_async().await;
    let client = StorageClient::new(ProtoWallet::new(Some(key(7))), mock_server.url())
        .with_timeout(Duration::from_secs(5));
    let sessions = client.peer().session_manager();
    sessions.write().await.add_session(session).unwrap();
    let answer = mock_server
        .mock("POST", "/.well-known/auth")
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_vec(&message).unwrap())
        .expect(1)
        .create_async()
        .await;
    let error = client.make_available().await.unwrap_err();
    answer.assert_async().await;

    // bsv-rs 0.9.0-beta.4's words; at beta.3 the Peer validated the
    // certificate, raised no refusal, and the client waited out its
    // handshake.
    let text = "Certificates do not match a locally requested set for this session.";
    assert!(matches!(error, Error::NetworkError(_)), "{error:?}");
    assert_eq!(error.to_string(), format!("{ENVELOPE}{text}"));
}
