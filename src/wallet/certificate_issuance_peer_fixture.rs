//! #315: a certifier fixture that speaks BRC-104 as the reference middleware
//! answers it (ts-stack@edf6e03 `packages/auth-express-middleware/src/index.ts`):
//! the handshake on `POST /.well-known/auth` (`:1005-1031`), then the issuance
//! path, the client's signature checked and the answer signed (`:931-968`).
//!
//! The mockito closures are synchronous, so the fixture signs with bsv-rs's
//! key deriver and `PrivateKey::sign`; its self-check verifies that signature
//! through `ProtoWallet::verify_signature`, the path the client's Peer takes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bsv_rs::auth::transports::{HttpRequest, HttpResponse};
use bsv_rs::auth::types::{AuthMessage, MessageType, AUTH_PROTOCOL_ID};
use bsv_rs::primitives::{from_base64, from_hex, sha256, to_base64, to_hex, PrivateKey, PublicKey};
use bsv_rs::wallet::{
    Counterparty, KeyDeriver, ProtoWallet, Protocol, SecurityLevel, VerifySignatureArgs,
};
use mockito::{Mock, Request, ServerGuard};

/// The six headers the middleware writes on a general answer (`index.ts:931-968`).
const SIGNED_HEADERS: [&str; 6] = [
    "x-bsv-auth-version",
    "x-bsv-auth-identity-key",
    "x-bsv-auth-nonce",
    "x-bsv-auth-your-nonce",
    "x-bsv-auth-request-id",
    "x-bsv-auth-signature",
];

/// How the fixture answers the issuance path.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Answer {
    /// The six headers, the signature over the answer sent.
    Signed,
    /// The six headers, the signature over another body.
    BadSignature,
}

struct Signed {
    status: u16,
    body: Vec<u8>,
    headers: HashMap<&'static str, String>,
}

#[derive(Default)]
struct State {
    /// The fixture's session nonce to the client's identity and nonce.
    sessions: HashMap<String, (PublicKey, String)>,
    /// The request id to the answer, so the header and body closures agree.
    answers: HashMap<String, Arc<Signed>>,
    /// Whether each general request's signature verified.
    client_signatures: Vec<bool>,
    /// The `x-bsv-*` header names of each general request, sorted.
    request_headers: Vec<Vec<String>>,
}

/// A certifier on a mockito server, holding the private key it signs with.
#[derive(Clone)]
pub(super) struct PeerCertifier {
    key: PrivateKey,
    state: Arc<Mutex<State>>,
}

fn auth_protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, AUTH_PROTOCOL_ID)
}

fn header(request: &Request, name: &str) -> String {
    request
        .header(name)
        .first()
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

impl PeerCertifier {
    pub(super) fn new(key: PrivateKey) -> Self {
        Self {
            key,
            state: Arc::default(),
        }
    }

    /// The certifier's identity key, hex.
    pub(super) fn identity(&self) -> String {
        self.key.public_key().to_hex()
    }

    /// Signs as `Peer.toPeer` does: BKDS, protocol `[2, 'auth message
    /// signature']`, the key ID given, the counterparty the peer.
    pub(super) fn sign(&self, data: &[u8], key_id: &str, peer: &PublicKey) -> Vec<u8> {
        let child = KeyDeriver::new(Some(self.key.clone()))
            .derive_private_key(&auth_protocol(), key_id, &Counterparty::Other(peer.clone()))
            .unwrap();
        child.sign(&sha256(data)).unwrap().to_der()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Whether each general request's signature verified, in order.
    pub(super) fn client_signatures(&self) -> Vec<bool> {
        self.state().client_signatures.clone()
    }

    /// The `x-bsv-*` header names each general request carried, sorted.
    pub(super) fn request_headers(&self) -> Vec<Vec<String>> {
        self.state().request_headers.clone()
    }

    /// `POST /.well-known/auth`, answered with the `initialResponse`.
    pub(super) async fn handshake(&self, server: &mut ServerGuard) -> Mock {
        let certifier = self.clone();
        server
            .mock("POST", "/.well-known/auth")
            .with_header("content-type", "application/json")
            .with_body_from_request(move |request| certifier.initial_response(request))
            .create_async()
            .await
    }

    /// The `initialResponse` as `index.ts:1005-1031` sends it: the certifier's
    /// key, its nonce (derived from the request, so a rerun agrees), the
    /// client's nonce echoed, and the signature over `yourNonce ||
    /// initialNonce` with key ID `"<yourNonce> <initialNonce>"`.
    fn initial_response(&self, request: &Request) -> Vec<u8> {
        let asked: AuthMessage = serde_json::from_slice(request.body().unwrap()).unwrap();
        let client_nonce = asked.initial_nonce.clone().unwrap_or_default();
        let nonce = to_base64(&sha256(format!("certifier {client_nonce}").as_bytes()));
        let mut data = from_base64(&client_nonce).unwrap();
        data.extend(from_base64(&nonce).unwrap());
        let mut answer = AuthMessage::new(MessageType::InitialResponse, self.key.public_key());
        answer.nonce = Some(nonce.clone());
        answer.initial_nonce = Some(nonce.clone());
        answer.your_nonce = Some(client_nonce.clone());
        answer.signature = Some(self.sign(
            &data,
            &format!("{client_nonce} {nonce}"),
            &asked.identity_key,
        ));
        let session = (asked.identity_key, client_nonce);
        self.state().sessions.insert(nonce, session);
        serde_json::to_vec(&answer).unwrap()
    }

    /// `POST <path>` answering `status` and `body(request)`, signed as `answer`
    /// says; not yet created, so the caller adds its matchers and `expect`.
    pub(super) fn general(
        &self,
        server: &mut ServerGuard,
        path: &str,
        status: u16,
        answer: Answer,
        body: impl Fn(&Request) -> Vec<u8> + Send + Sync + 'static,
    ) -> Mock {
        let responder = Arc::new(Responder {
            certifier: self.clone(),
            status,
            answer,
            body: Box::new(body),
        });
        let status_of = Arc::clone(&responder);
        let mut mock = server
            .mock("POST", path)
            .with_status_code_from_request(move |request| status_of.answer(request).status.into());
        for name in SIGNED_HEADERS {
            let responder = Arc::clone(&responder);
            mock = mock.with_header_from_request(name, move |request| {
                let answer = responder.answer(request);
                answer.headers.get(name).cloned().unwrap_or_default()
            });
        }
        mock.with_body_from_request(move |request| responder.answer(request).body.clone())
    }
}

type Body = Box<dyn Fn(&Request) -> Vec<u8> + Send + Sync>;

struct Responder {
    certifier: PeerCertifier,
    status: u16,
    answer: Answer,
    body: Body,
}

impl Responder {
    /// The answer to one request, computed once whichever closure asks.
    fn answer(&self, request: &Request) -> Arc<Signed> {
        let request_id = header(request, "x-bsv-auth-request-id");
        let cached = self.certifier.state().answers.get(&request_id).cloned();
        let signed = cached.unwrap_or_else(|| Arc::new(self.sign(request, &request_id)));
        let answers = &mut self.certifier.state().answers;
        Arc::clone(answers.entry(request_id).or_insert(signed))
    }

    fn sign(&self, request: &Request, request_id: &str) -> Signed {
        let our_nonce = header(request, "x-bsv-auth-your-nonce");
        let session = self.certifier.state().sessions.get(&our_nonce).cloned();
        let Some((client, client_nonce)) = session else {
            // `index.ts:1778-1793`: a request with no session is 401, unsigned.
            let body = r#"{"status":"error","code":"UNAUTHORIZED","message":"Mutual-authentication failed!"}"#;
            let (status, headers) = (401, HashMap::new());
            return Signed {
                status,
                body: body.into(),
                headers,
            };
        };
        let id: [u8; 32] = from_base64(request_id).unwrap().try_into().unwrap();
        let verified = self.verify_request(request, id, &our_nonce, &client);
        let mut names: Vec<String> = request
            .headers()
            .keys()
            .map(|name| name.as_str().to_string())
            .filter(|name| name.starts_with("x-bsv-"))
            .collect();
        names.sort();
        let mut state = self.certifier.state();
        state.client_signatures.push(verified);
        state.request_headers.push(names);
        drop(state);

        let body = (self.body)(request);
        let nonce = to_base64(&sha256(format!("answer {request_id}").as_bytes()));
        let mut signed_body = body.clone();
        if self.answer == Answer::BadSignature {
            signed_body.push(b' ');
        }
        // The response preimage (`index.ts:931-968`): request id, status, the
        // included headers (none here), body.
        let payload = HttpResponse {
            request_id: id,
            status: self.status,
            headers: Vec::new(),
            body: signed_body,
        }
        .to_payload();
        let signature = self
            .certifier
            .sign(&payload, &format!("{nonce} {client_nonce}"), &client);
        let headers = HashMap::from([
            ("x-bsv-auth-version", "0.1".to_string()),
            ("x-bsv-auth-identity-key", self.certifier.identity()),
            ("x-bsv-auth-nonce", nonce),
            ("x-bsv-auth-your-nonce", client_nonce),
            ("x-bsv-auth-request-id", request_id.to_string()),
            ("x-bsv-auth-signature", to_hex(&signature)),
        ]);
        let status = self.status;
        Signed {
            status,
            body,
            headers,
        }
    }

    /// The client's signature over the rebuilt request payload: request id,
    /// method, path, query, the signed headers sorted (`content-type` with its
    /// parameters cut, `authorization`, `x-bsv-*` but not `x-bsv-auth-*`),
    /// body; key ID `"<requestNonce> <ourNonce>"`, counterparty the client.
    fn verify_request(
        &self,
        request: &Request,
        request_id: [u8; 32],
        our_nonce: &str,
        client: &PublicKey,
    ) -> bool {
        let mut headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                let name = name.as_str().to_ascii_lowercase();
                let value = value.to_str().ok()?.to_string();
                if name == "content-type" {
                    let media = value.split(';').next().unwrap_or_default().trim();
                    Some((name, media.to_string()))
                } else if name == "authorization"
                    || (name.starts_with("x-bsv-") && !name.starts_with("x-bsv-auth"))
                {
                    Some((name, value))
                } else {
                    None
                }
            })
            .collect();
        headers.sort();
        let query = request.path_and_query()[request.path().len()..].to_string();
        let payload = HttpRequest {
            request_id,
            method: request.method().to_string(),
            path: request.path().to_string(),
            search: query,
            headers,
            body: request.body().cloned().unwrap_or_default(),
        }
        .to_payload();
        let Ok(signature) = from_hex(&header(request, "x-bsv-auth-signature")) else {
            return false;
        };
        let key_id = format!("{} {our_nonce}", header(request, "x-bsv-auth-nonce"));
        ProtoWallet::new(Some(self.certifier.key.clone()))
            .verify_signature(VerifySignatureArgs {
                data: Some(payload),
                signature,
                protocol_id: auth_protocol(),
                key_id,
                counterparty: Some(Counterparty::Other(client.clone())),
                ..Default::default()
            })
            .is_ok_and(|result| result.valid)
    }
}

#[cfg(test)]
#[path = "certificate_issuance_peer_fixture_tests.rs"]
mod tests;
