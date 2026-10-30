//! #315: a certifier fixture that speaks BRC-104 as the reference middleware
//! answers it (ts-stack@edf6e03 `packages/middleware/auth-express-middleware/src/index.ts`):
//! the handshake on `POST /.well-known/auth` (`:1005-1031`), then the issuance
//! path, the client's signature checked and the answer signed (`:931-968`), or
//! a request whose session or signature fails refused with an unsigned 401
//! (`:1380-1395`), its handler never called.
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
use mockito::{Matcher, Mock, Request, ServerGuard};

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

/// The middleware's answer to a request with no auth headers (`index.ts:1778-1792`).
const UNAUTHENTICATED: &str =
    r#"{"status":"error","code":"UNAUTHORIZED","message":"Mutual-authentication failed!"}"#;

/// The middleware's answer to a general message its Peer refused on a nonce,
/// signature or session (`index.ts:1380-1395`).
pub(super) const AUTH_FAILED: &str =
    r#"{"status":"error","code":"ERR_AUTH_FAILED","description":"Authentication failed."}"#;

/// What the middleware's Peer makes of a request off the handshake path.
#[derive(Clone)]
enum Verdict {
    /// No request id: the unauthenticated path (`index.ts:1097-1104`).
    Unauthenticated,
    /// No session for its nonce (`Peer.ts:1282-1285`) or a signature that
    /// does not verify (`Peer.ts:1303-1305`).
    Refused,
    /// The session's client identity and nonce.
    Accepted(PublicKey, String),
}

#[derive(Default)]
struct State {
    /// The fixture's session nonce to the client's identity and nonce.
    sessions: HashMap<String, (PublicKey, String)>,
    /// The request id to its verdict, so a request is checked once.
    verdicts: HashMap<String, Verdict>,
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

/// A 48-byte session nonce derived from `seed`, the reference's length
/// (`createNonce`: 16 bytes and their 32-byte HMAC); since bsv-rs
/// 0.9.0-beta.4 (#445) a Peer refuses another length at intake. A general
/// message's nonce stays 32 bytes (`Random(32)`).
fn derived_nonce(seed: &str) -> String {
    let mut nonce = sha256(seed.as_bytes()).to_vec();
    nonce.extend_from_slice(&sha256(nonce.as_slice())[..16]);
    to_base64(&nonce)
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

    /// `POST /.well-known/auth`, answered with the `initialResponse`; and on
    /// every other path a request the middleware refuses, answered 401 with
    /// none of the six headers (mockito writes a header it was given even
    /// empty, so the refusal is its own mock).
    pub(super) async fn handshake(&self, server: &mut ServerGuard) -> Mock {
        let (matcher, refusal) = (self.clone(), self.clone());
        server
            .mock("POST", Matcher::Any)
            .match_request(move |request| {
                request.path() != "/.well-known/auth"
                    && !matches!(matcher.verdict(request), Verdict::Accepted(..))
            })
            .with_status(401)
            .with_header("content-type", "application/json")
            .with_body_from_request(move |request| match refusal.verdict(request) {
                Verdict::Unauthenticated => UNAUTHENTICATED.into(),
                _ => AUTH_FAILED.into(),
            })
            .create_async()
            .await;
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
        let nonce = derived_nonce(&format!("certifier {client_nonce}"));
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

    /// The request's verdict, computed once per request id; a request that
    /// reaches a session has its signature and `x-bsv-*` names recorded.
    fn verdict(&self, request: &Request) -> Verdict {
        let request_id = header(request, "x-bsv-auth-request-id");
        // Partial `x-bsv-auth-*` headers with no request id are read as
        // unauthenticated here; the reference throws `AuthProtocolError` for
        // that shape (`index.ts:1099-1102`), which this crate's client never
        // sends.
        if request_id.is_empty() {
            return Verdict::Unauthenticated;
        }
        if let Some(verdict) = self.state().verdicts.get(&request_id).cloned() {
            return verdict;
        }
        let verdict = self.check(request, &request_id);
        let verdicts = &mut self.state().verdicts;
        verdicts.entry(request_id).or_insert(verdict).clone()
    }

    fn check(&self, request: &Request, request_id: &str) -> Verdict {
        let our_nonce = header(request, "x-bsv-auth-your-nonce");
        let session = self.state().sessions.get(&our_nonce).cloned();
        let Some((client, client_nonce)) = session else {
            return Verdict::Refused;
        };
        let Some(id) = from_base64(request_id)
            .ok()
            .and_then(|id| <[u8; 32]>::try_from(id).ok())
        else {
            return Verdict::Refused;
        };
        let verified = self.verify_request(request, id, &our_nonce, &client);
        let mut names: Vec<String> = request
            .headers()
            .keys()
            .map(|name| name.as_str().to_string())
            .filter(|name| name.starts_with("x-bsv-"))
            .collect();
        names.sort();
        let mut state = self.state();
        state.client_signatures.push(verified);
        state.request_headers.push(names);
        match verified {
            true => Verdict::Accepted(client, client_nonce),
            false => Verdict::Refused,
        }
    }

    /// `POST <path>` answering `status` and `body(request)`, signed as `answer`
    /// says, to a request the middleware accepts (`handshake`'s refusal
    /// answers the rest); not yet created, so the caller adds its matchers and
    /// `expect`.
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
        let (status_of, matcher) = (Arc::clone(&responder), self.clone());
        let mut mock = server
            .mock("POST", path)
            .match_request(move |request| matches!(matcher.verdict(request), Verdict::Accepted(..)))
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
        // `general` matches only an accepted request.
        let Verdict::Accepted(client, client_nonce) = self.certifier.verdict(request) else {
            unreachable!("the refusal mock answers a refused request");
        };
        let id: [u8; 32] = from_base64(request_id).unwrap().try_into().unwrap();
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
}

impl PeerCertifier {
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
        ProtoWallet::new(Some(self.key.clone()))
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
