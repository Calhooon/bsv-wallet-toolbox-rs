//! BRC-104 response bounds used by issuance and authenticated storage (#419 item 5).
//! Recipes and expected payloads are extracted unchanged from bsv-rs beta.3's
//! recording of ts-stack@edf6e03. HTTP/2 on a mockito loopback server reaches
//! the SDK's header-count check without an HTTP/1 parser limit masking it.

use std::sync::{Arc, Mutex};

use bsv_rs::auth::transports::{headers, HttpRequest};
use bsv_rs::auth::{AuthMessage, MessageType, SimplifiedFetchTransport, Transport};
use bsv_rs::primitives::{sha256, to_hex, PrivateKey};
use bsv_rs::Error;
use mockito::{Mock, ServerGuard};
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    family: String,
    name: String,
    #[serde(default)]
    handshake: bool,
    status: Option<u16>,
    certificate_bytes: Option<usize>,
    #[serde(default)]
    latin1: bool,
    signature: Option<String>,
    signature_chars: Option<usize>,
    message_type: Option<String>,
    body_bytes: Option<usize>,
    content_length: Option<String>,
    request_id: Option<String>,
    signed_key_bytes: Option<usize>,
    signed_value_bytes: Option<usize>,
    signed_count: Option<usize>,
    signed_total_bytes: Option<usize>,
    #[serde(default)]
    extra_headers: Vec<ExtraHeader>,
    error: Option<RecordedError>,
    delivered: Option<Delivery>,
}

#[derive(Deserialize)]
struct ExtraHeader {
    name: String,
    character: String,
    count: usize,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Delivery {
    message_type: String,
    signature_hex: String,
    payload_bytes: usize,
    payload_sha256: String,
}

#[derive(Deserialize)]
struct RecordedError {
    message: String,
}

fn reference(family: &str) -> Vec<Case> {
    let text = match family {
        "certificate_header" => include_str!("fixtures/data/brc104/certificate_header.json"),
        "signature" => include_str!("fixtures/data/brc104/signature.json"),
        "message_type" => include_str!("fixtures/data/brc104/message_type.json"),
        "body" => include_str!("fixtures/data/brc104/body.json"),
        "request_id" => include_str!("fixtures/data/brc104/request_id.json"),
        "signed_headers" => include_str!("fixtures/data/brc104/signed_headers.json"),
        _ => unreachable!("unknown fixture family"),
    };
    serde_json::from_str(text).unwrap()
}

fn message(handshake: bool) -> AuthMessage {
    let key = PrivateKey::from_hex(&format!("{:064x}", 42)).unwrap();
    let mut message = AuthMessage::new(MessageType::InitialRequest, key.public_key());
    if !handshake {
        message.message_type = MessageType::General;
        message.signature = Some(vec![48, 0]);
        message.payload = Some(
            HttpRequest {
                request_id: [7; 32],
                method: "POST".into(),
                path: "/sendMessage".into(),
                search: String::new(),
                headers: vec![],
                body: vec![],
            }
            .to_payload(),
        );
    }
    message
}

fn body(case: &Case) -> Vec<u8> {
    let mut body = if case.handshake {
        format!(
            "{{\"version\":\"0.1\",\"messageType\":\"initialRequest\",\"identityKey\":\"{}\"}}",
            message(true).identity_key.to_hex()
        )
        .into_bytes()
    } else {
        b"signed".to_vec()
    };
    if let Some(size) = case.body_bytes {
        if case.handshake {
            body.resize(size, b' ');
        } else {
            body = vec![b'a'; size];
        }
    }
    body
}

fn filled(size: usize, latin1: bool) -> String {
    if latin1 {
        "é".repeat(size / 2) + &"x".repeat(size % 2)
    } else {
        "x".repeat(size)
    }
}

fn signed_headers(case: &Case, values: &mut Vec<(String, String)>) {
    if let Some(size) = case.signed_key_bytes {
        values.push((format!("x-bsv-{}", "k".repeat(size - 6)), "v".into()));
    }
    if let Some(size) = case.signed_value_bytes {
        values.push(("x-bsv-value".into(), filled(size, case.latin1)));
    }
    for index in 0..case.signed_count.unwrap_or_default() {
        values.push((format!("x-bsv-{index:03}"), "v".into()));
    }
    if let Some(size) = case.signed_total_bytes {
        let mut remaining = size - 8 * "x-bsv-total-0".len();
        for index in 0..8 {
            let size = remaining.min(32768);
            values.push((format!("x-bsv-total-{index}"), "x".repeat(size)));
            remaining -= size;
        }
        assert_eq!(remaining, 0);
    }
}

fn response_headers(case: &Case) -> Vec<(String, String)> {
    let signature = case.signature.clone().unwrap_or_else(|| {
        case.signature_chars
            .map(|size| "a".repeat(size))
            .unwrap_or_else(|| "3006020101020101".into())
    });
    let mut values = vec![
        (headers::VERSION.into(), "0.1".into()),
        (
            headers::IDENTITY_KEY.into(),
            message(true).identity_key.to_hex(),
        ),
        (headers::SIGNATURE.into(), signature),
    ];
    for (name, value) in [
        (headers::MESSAGE_TYPE, &case.message_type),
        (headers::REQUEST_ID, &case.request_id),
        ("content-length", &case.content_length),
    ] {
        if let Some(value) = value {
            values.push((name.into(), value.clone()));
        }
    }
    if let Some(size) = case.certificate_bytes {
        let prefix = "{\"certifiers\":[],\"types\":{},\"pad\":\"";
        let text = format!(
            "{prefix}{}\"}}",
            filled(size - prefix.len() - 2, case.latin1)
        );
        assert_eq!(text.len(), size);
        values.push((headers::REQUESTED_CERTIFICATES.into(), text));
    }
    signed_headers(case, &mut values);
    values.extend(
        case.extra_headers
            .iter()
            .map(|header| (header.name.clone(), header.character.repeat(header.count))),
    );
    values
}

fn response(case: &Case, server: &mut ServerGuard, at: &str) -> Mock {
    let mut response = server
        .mock("POST", at)
        .with_status(usize::from(case.status.unwrap_or(200)))
        .with_body(body(case));
    for (name, value) in response_headers(case) {
        response = response.with_header(&name, &value);
    }
    response.expect(1)
}

async fn exchange(case: &Case) -> (bsv_rs::Result<()>, Vec<Delivery>, String) {
    let mut server = mockito::Server::new_async().await;
    let at = if case.handshake {
        "/.well-known/auth"
    } else {
        "/sendMessage"
    };
    let served = response(case, &mut server, at).create_async().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .http2_prior_knowledge()
        .http2_max_header_list_size(2 * 1024 * 1024)
        .build()
        .unwrap();
    let transport = SimplifiedFetchTransport::with_client(&server.url(), client);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    transport.set_callback(Box::new(move |message| {
        let payload = message.payload.unwrap_or_default();
        sink.lock().unwrap().push(Delivery {
            message_type: message.message_type.as_str().into(),
            signature_hex: to_hex(&message.signature.unwrap_or_default()),
            payload_bytes: payload.len(),
            payload_sha256: to_hex(&sha256(&payload)),
        });
        Box::pin(async { Ok(()) })
    }));
    let result = transport.send(&message(case.handshake)).await;
    let messages = std::mem::take(&mut *seen.lock().unwrap());
    served.assert_async().await;
    (result, messages, server.url())
}

async fn replay_family(family: &str) {
    let mut failures = Vec::new();
    for case in &reference(family) {
        assert_eq!(case.family, family);
        // Replay fake-fetch Content-Length cases directly in body_tests.rs
        // to isolate the SDK guard from native framing. Actual body lengths
        // are replayed over HTTP here.
        if case.content_length.is_some() {
            continue;
        }
        let (result, messages, origin) = exchange(case).await;
        if let Some(error) = &case.error {
            let expected = Err(Error::AuthError(error.message.replace("{url}", &origin)));
            if result != expected || !messages.is_empty() {
                failures.push(format!(
                    "{}: expected {expected:?}, got {result:?}; callbacks={}",
                    case.name,
                    messages.len()
                ));
            }
        } else if result.is_err()
            || messages.len() != 1
            || case
                .delivered
                .as_ref()
                .is_none_or(|expected| messages.as_slice() != std::slice::from_ref(expected))
        {
            failures.push(format!(
                "{}: expected {:?}, got {result:?}, {messages:?}",
                case.name, case.delivered
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn issue_416_certificate_header_accepts_262144_utf8_bytes_and_refuses_one_more() {
    replay_family("certificate_header").await;
}

#[tokio::test]
async fn issue_416_signatures_require_nonempty_even_hex_at_most_2048_characters() {
    replay_family("signature").await;
}

#[tokio::test]
async fn issue_416_certificate_request_response_keeps_its_type() {
    replay_family("message_type").await;
}

#[tokio::test]
async fn issue_416_bodies_use_the_1_mib_handshake_and_16_mib_application_defaults() {
    replay_family("body").await;
}

#[tokio::test]
async fn issue_416_optional_request_id_requires_canonical_base64_of_exactly_32_bytes() {
    replay_family("request_id").await;
}

#[tokio::test]
async fn issue_416_signed_headers_bound_names_values_count_and_total_utf8_bytes() {
    replay_family("signed_headers").await;
}
