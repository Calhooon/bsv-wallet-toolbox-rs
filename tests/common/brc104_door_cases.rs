//! #423: bsv-rs beta.3's BRC-104 response bounds as a door's peer answers
//! them. The cases are `tests/fixtures/data/brc104/`'s, the vector
//! `tests/brc104_bounds.rs` replays on the SDK transport; here the issuance
//! client and the authenticated storage client are the callers, so each
//! refusal is read inside the door's envelope.

use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Case {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) handshake: bool,
    certificate_bytes: Option<usize>,
    signature: Option<String>,
    message_type: Option<String>,
    body_bytes: Option<usize>,
    request_id: Option<String>,
    signed_key_bytes: Option<usize>,
    signed_value_bytes: Option<usize>,
    signed_count: Option<usize>,
    signed_total_bytes: Option<usize>,
    error: Option<Recorded>,
}

#[derive(Deserialize)]
struct Recorded {
    message: String,
}

/// The vector's case `name` of `family`, unchanged.
pub(crate) fn case(family: &str, name: &str) -> Case {
    let text = match family {
        "certificate_header" => include_str!("../fixtures/data/brc104/certificate_header.json"),
        "signature" => include_str!("../fixtures/data/brc104/signature.json"),
        "message_type" => include_str!("../fixtures/data/brc104/message_type.json"),
        "body" => include_str!("../fixtures/data/brc104/body.json"),
        "request_id" => include_str!("../fixtures/data/brc104/request_id.json"),
        "signed_headers" => include_str!("../fixtures/data/brc104/signed_headers.json"),
        _ => unreachable!("unknown fixture family"),
    };
    let cases: Vec<Case> = serde_json::from_str(text).unwrap();
    cases.into_iter().find(|case| case.name == name).unwrap()
}

/// What a door's HTTP/1 client answers the 513-header count case: hyper's
/// parser takes 100 headers, so the request fails before bsv-rs's bound
/// (`tests/brc104_bounds.rs` replays the bound over HTTP/2).
pub(crate) fn header_limit_fault(url: &str) -> bsv_rs::Error {
    bsv_rs::Error::TransportFault {
        fault: bsv_rs::TransportFault::MayHaveBeenReceived,
        status: None,
        detail: format!("HTTP request failed: error sending request for url ({url})"),
    }
}

impl Case {
    /// bsv-rs's recorded refusal at the door's `origin`, the vector's
    /// general path `/sendMessage` the door's `path`.
    pub(crate) fn refusal(&self, origin: &str, path: &str) -> String {
        let error = self.error.as_ref().expect("a refused case");
        let message = error
            .message
            .replace("{url}/sendMessage", &format!("{origin}{path}"));
        message.replace("{url}", origin)
    }

    /// The answer's body: the vector's size where it sets one.
    pub(crate) fn body(&self) -> Vec<u8> {
        vec![b' '; self.body_bytes.unwrap_or(2)]
    }

    /// The answer's headers past version and identity, built as
    /// `tests/brc104_bounds.rs` builds them (no case here is Latin-1).
    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        let signature = self.signature.as_deref().unwrap_or("3006020101020101");
        let mut values = vec![("x-bsv-auth-signature".to_string(), signature.to_string())];
        for (name, value) in [
            ("x-bsv-auth-message-type", &self.message_type),
            ("x-bsv-auth-request-id", &self.request_id),
        ] {
            if let Some(value) = value {
                values.push((name.into(), value.clone()));
            }
        }
        if let Some(size) = self.certificate_bytes {
            let prefix = "{\"certifiers\":[],\"types\":{},\"pad\":\"";
            let text = format!("{prefix}{}\"}}", "x".repeat(size - prefix.len() - 2));
            assert_eq!(text.len(), size);
            values.push(("x-bsv-auth-requested-certificates".into(), text));
        }
        if let Some(size) = self.signed_key_bytes {
            values.push((format!("x-bsv-{}", "k".repeat(size - 6)), "v".into()));
        }
        if let Some(size) = self.signed_value_bytes {
            values.push(("x-bsv-value".into(), "x".repeat(size)));
        }
        for index in 0..self.signed_count.unwrap_or_default() {
            values.push((format!("x-bsv-{index:03}"), "v".into()));
        }
        if let Some(size) = self.signed_total_bytes {
            let mut remaining = size - 8 * "x-bsv-total-0".len();
            for index in 0..8 {
                let size = remaining.min(32768);
                values.push((format!("x-bsv-total-{index}"), "x".repeat(size)));
                remaining -= size;
            }
            assert_eq!(remaining, 0);
        }
        values
    }
}
