//! The body `Arc::post_beef` hands ARC, against the six rows of the
//! broadcast-body replay (bsv-stack-lean #63, `docs/readings/
//! broadcast-body-forms.md` finding F2; the rows are
//! `tests/vectors/broadcast_body/`, see its README).
//!
//! The mock ARC reads a request the way ARC's intake does at e7efc5b (tag
//! v1.5.11): the body by its content type (`internal/api/handler/
//! parsers.go:20-61`: `text/plain` and `application/json` `{"rawTx": hex}`
//! are hex-decoded, `application/octet-stream` is taken as it is, any other
//! type is 400), then the form by the bytes alone (`internal/validator/
//! helpers.go:36-58`, `internal/beef/beef.go:24-34`): bytes 2 and 3 `BE EF`
//! is a BEEF, bytes 4 to 9 `00 00 00 00 00 EF` is EF, anything else a raw
//! transaction. A body it cannot read is HTTP 400 "Bad request" with the
//! parse error in `extraInfo` (`default.go:405-414, 631-634`). ARC has no
//! case for the BRC-95 AtomicBEEF prefix: `01 01 01 01` goes to the raw
//! parse and is refused (the replay, `corpus/runners/broadcast-body/
//! results.txt`). The mock answers `SEEN_ON_NETWORK` to any body it reads.
//! It replays the parse, not ARC: no validator.

use std::sync::{Arc as StdArc, Mutex};

use bsv_rs::transaction::{Beef, Transaction};
use bsv_wallet_toolbox_rs::services::{Arc as ArcProvider, ArcConfig};

/// The six rows: (name, bytes, the plain twin's name). An AtomicBEEF row is
/// its twin with the 36-byte prefix (`01010101` and the subject's txid).
const ROWS: [(&str, &[u8], &str); 6] = [
    (
        "example_atomic_payment",
        include_bytes!("vectors/broadcast_body/example_atomic_payment.bin"),
        "example",
    ),
    (
        "example",
        include_bytes!("vectors/broadcast_body/example.bin"),
        "example",
    ),
    (
        "small_chain_atomic_true",
        include_bytes!("vectors/broadcast_body/small_chain_atomic_true.bin"),
        "small_chain_true",
    ),
    (
        "small_chain_true",
        include_bytes!("vectors/broadcast_body/small_chain_true.bin"),
        "small_chain_true",
    ),
    (
        "lone_atomic",
        include_bytes!("vectors/broadcast_body/lone_atomic.bin"),
        "lone",
    ),
    (
        "lone",
        include_bytes!("vectors/broadcast_body/lone.bin"),
        "lone",
    ),
];

/// The sha256 of each row, as `corpus/runners/broadcast-body/results.txt`
/// records it.
const ROW_SHA256: [(&str, &str); 6] = [
    (
        "example_atomic_payment",
        "b6b08e4c5f5252224bd89cc0c0c0147e3db390bd189e1d70d48a5218a2a224c9",
    ),
    (
        "example",
        "530b9a600ac45fa14ceeda12e42a57922fb1d70f94c2b61f30d3ccf92987a4d8",
    ),
    (
        "small_chain_atomic_true",
        "2d7b847053f7cbd7af3c17072130ad5faacba56fc3aac9c7b1fce7f6bd4e8c48",
    ),
    (
        "small_chain_true",
        "6b5eb2237845c56bad1761dc4ab4061142868956b5ff96428ded7285aac5ea4f",
    ),
    (
        "lone_atomic",
        "4fc97f61e5c2598b1971d3724d358cf5216fd64be4e67a770ff30cbd188ce3bc",
    ),
    (
        "lone",
        "38656f4c184e754b42b8a30cfc09c959b971a971744db34456d3da3a741fbb50",
    ),
];

fn row(name: &str) -> &'static [u8] {
    ROWS.iter().find(|r| r.0 == name).expect("a row").1
}

/// The subject: the last transaction of the plain twin.
fn subject_of(plain: &[u8]) -> String {
    let beef = Beef::from_binary(plain).expect("the plain row parses");
    beef.txs.last().expect("a transaction").txid()
}

/// What one request looked like to the mock.
#[derive(Debug, Clone)]
struct Seen {
    path: String,
    content_type: String,
    /// The body after the content type's decoding, or the decode error.
    decoded: Result<Vec<u8>, String>,
}

/// ARC's content-type step at e7efc5b (`parsers.go:20-61`).
fn decode_body(content_type: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match ct.as_str() {
        "application/octet-stream" => Ok(body.to_vec()),
        "text/plain" => {
            hex::decode(String::from_utf8_lossy(body).trim()).map_err(|e| format!("hex: {e}"))
        }
        "application/json" => {
            let v: serde_json::Value =
                serde_json::from_slice(body).map_err(|e| format!("json: {e}"))?;
            let raw = v
                .get("rawTx")
                .and_then(|r| r.as_str())
                .ok_or_else(|| "json: no rawTx".to_string())?;
            hex::decode(raw).map_err(|e| format!("hex: {e}"))
        }
        other => Err(format!(
            "given content-type {other} does not match any of the allowed content-types"
        )),
    }
}

/// ARC's form step at e7efc5b (`helpers.go:36-58`, `beef.go:24-49`): `Ok`
/// with the form when the intake reads the bytes, `Err` with the reason when
/// it answers 400 (or 463 for a BEEF that does not decode).
fn arc_reads(bytes: &[u8]) -> Result<&'static str, String> {
    if bytes.len() >= 4 && bytes[2] == 0xBE && bytes[3] == 0xEF {
        return Beef::from_binary(bytes)
            .map(|_| "BEEF")
            .map_err(|e| format!("463 BEEF does not decode: {e}"));
    }
    if bytes.len() > 10 && bytes[4..9] == [0, 0, 0, 0, 0] && bytes[9] == 0xEF {
        return Transaction::from_ef(bytes)
            .map(|_| "EF")
            .map_err(|e| format!("400 EF: {e}"));
    }
    Transaction::from_binary(bytes)
        .map(|_| "raw")
        .map_err(|e| format!("400 raw: {e}"))
}

fn seen_of(req: &mockito::Request) -> Seen {
    let content_type = req
        .header("content-type")
        .first()
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = req.body().cloned().unwrap_or_default();
    Seen {
        path: req.path().to_string(),
        decoded: decode_body(&content_type, &body),
        content_type,
    }
}

fn reads(seen: &Seen) -> bool {
    matches!(&seen.decoded, Ok(b) if arc_reads(b).is_ok())
}

/// A mock ARC at e7efc5b's intake: 200 `SEEN_ON_NETWORK` naming `subject`
/// for a body it reads, 400 Bad request for one it does not. Every request
/// to `POST /v1/tx` is recorded.
async fn mock_arc(server: &mut mockito::ServerGuard, subject: &str) -> StdArc<Mutex<Vec<Seen>>> {
    let log: StdArc<Mutex<Vec<Seen>>> = StdArc::new(Mutex::new(Vec::new()));
    let ok_log = log.clone();
    server
        .mock("POST", "/v1/tx")
        .match_request(move |req| {
            let seen = seen_of(req);
            let ok = reads(&seen);
            if ok {
                ok_log.lock().unwrap().push(seen);
            }
            ok
        })
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid":"{subject}","txStatus":"SEEN_ON_NETWORK","extraInfo":""}}"#
        ))
        .create_async()
        .await;
    let bad_log = log.clone();
    server
        .mock("POST", "/v1/tx")
        .match_request(move |req| {
            let seen = seen_of(req);
            let ok = reads(&seen);
            if !ok {
                bad_log.lock().unwrap().push(seen);
            }
            !ok
        })
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"type":"https://bitcoin-sv.github.io/arc/#/errors?id=_400","title":"Bad request","status":400,"detail":"The request seems to be malformed and cannot be processed","extraInfo":"script(814435): got 667 bytes: unexpected EOF"}"#,
        )
        .create_async()
        .await;
    log
}

fn leading(seen: &Seen) -> String {
    match &seen.decoded {
        Ok(b) => hex::encode(&b[..b.len().min(4)]),
        Err(e) => format!("undecoded ({e})"),
    }
}

#[test]
fn the_rows_are_the_replays_rows() {
    for (name, sha) in ROW_SHA256 {
        let got = hex::encode(bsv_rs::primitives::hash::sha256(row(name)));
        assert_eq!(got, sha, "row {name}");
    }
    for (name, bytes, twin) in ROWS {
        if name != twin {
            assert_eq!(&bytes[..4], &[1, 1, 1, 1], "{name} is an AtomicBEEF");
            assert_eq!(&bytes[36..], row(twin), "{name} is {twin} with the prefix");
        }
    }
}

/// F2's witness: every row reaches ARC in a form its intake reads. At
/// 0.7.1 the AtomicBEEF rows whose path is the full BEEF went out with the
/// prefix (`{"rawTx": hex}` of the bytes as handed in) and were refused 400.
#[tokio::test]
async fn every_replay_row_reaches_arc_in_a_form_its_intake_reads() {
    let mut failures = Vec::new();
    for (name, bytes, twin) in ROWS {
        let subject = subject_of(row(twin));
        let mut server = mockito::Server::new_async().await;
        let log = mock_arc(&mut server, &subject).await;
        let arc =
            ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();
        let result = arc
            .post_beef(bytes, std::slice::from_ref(&subject))
            .await
            .unwrap();
        let seen = log.lock().unwrap().clone();
        let refused: Vec<String> = seen
            .iter()
            .filter(|s| !reads(s))
            .map(|s| format!("{} {} {}", s.path, s.content_type, leading(s)))
            .collect();
        let atomic_sent = seen
            .iter()
            .any(|s| matches!(&s.decoded, Ok(b) if b.starts_with(&[1, 1, 1, 1])));
        if !result.is_success() || !refused.is_empty() || atomic_sent || seen.is_empty() {
            failures.push(format!(
                "{name}: status {}, refused [{}], sent [{}]",
                result.status,
                refused.join("; "),
                seen.iter().map(leading).collect::<Vec<_>>().join(", ")
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "rows ARC's intake at e7efc5b refused:\n{}",
        failures.join("\n")
    );
}

/// The full-BEEF path (an unproven ancestor this ARC has not seen) posts the
/// plain BEEF, the prefix stripped, as `application/octet-stream` to
/// `/v1/tx`: the same bytes for the AtomicBEEF and its plain twin.
#[tokio::test]
async fn the_full_beef_goes_plain_as_octet_stream() {
    for name in ["small_chain_atomic_true", "small_chain_true"] {
        let plain = row("small_chain_true");
        let subject = subject_of(plain);
        let mut server = mockito::Server::new_async().await;
        let log = mock_arc(&mut server, &subject).await;
        let arc =
            ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();
        let result = arc
            .post_beef(row(name), std::slice::from_ref(&subject))
            .await
            .unwrap();
        assert!(result.is_success(), "{name}: {:?}", result);
        let seen = log.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "{name}: one request, got {:?}", seen);
        assert_eq!(seen[0].path, "/v1/tx", "{name}");
        assert_eq!(
            seen[0].content_type, "application/octet-stream",
            "{name}: the bytes, not hex"
        );
        assert_eq!(
            seen[0].decoded.as_deref().ok(),
            Some(plain),
            "{name}: the plain BEEF, byte for byte"
        );
    }
}
