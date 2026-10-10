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

// =============================================================================
// ARC's 400: a request it could not read, never a verdict on the transaction
// =============================================================================
//
// At e7efc5b ARC answers 400 for a request it could not read (a content type,
// a hex error, a callback option, a body its intake does not parse:
// `default.go:304-309, 393-401, 631-634`), never for a verdict on a parsed
// transaction (those are 460 to 475). So a 400 is a fault of the poster: the
// post is retried in the other form ARC reads, and the answer is a hint that
// schedules a re-ask (bsv-stack-lean `docs/charters/tracker.md` section 2:
// every broadcaster word is a hint), never the transaction's rejection.

/// ARC's 400 body for an AtomicBEEF at e7efc5b (the replay's
/// `example_atomic_payment` row).
const ARC_400_PARSE: &str = r#"{"type":"https://bitcoin-sv.github.io/arc/#/errors?id=_400","title":"Bad request","status":400,"detail":"The request seems to be malformed and cannot be processed","extraInfo":"script(814435): got 667 bytes: unexpected EOF"}"#;

/// A mock ARC answering every `POST /v1/tx` with ARC's 400; every request is
/// recorded.
async fn mock_arc_400(server: &mut mockito::ServerGuard) -> StdArc<Mutex<Vec<Seen>>> {
    let log: StdArc<Mutex<Vec<Seen>>> = StdArc::new(Mutex::new(Vec::new()));
    let l = log.clone();
    server
        .mock("POST", "/v1/tx")
        .match_request(move |req| {
            l.lock().unwrap().push(seen_of(req));
            true
        })
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(ARC_400_PARSE)
        .create_async()
        .await;
    log
}

fn has_note(result: &bsv_wallet_toolbox_rs::services::PostBeefResult, what: &str) -> bool {
    result
        .notes
        .iter()
        .chain(result.txid_results.iter().flat_map(|t| t.notes.iter()))
        .any(|n| n.get("what").and_then(|v| v.as_str()) == Some(what))
}

/// The provider's reading of ARC's 400: transient, no rejection, the post
/// retried once in the reference's form (`{"rawTx": hex}` of the same plain
/// BEEF, ts-stack@edf6e03 `ARC.ts:320-340`).
#[tokio::test]
async fn an_arc_400_is_a_request_fault_and_the_post_is_retried_in_the_other_form() {
    let plain = row("small_chain_true");
    let subject = subject_of(plain);
    let mut server = mockito::Server::new_async().await;
    let log = mock_arc_400(&mut server).await;
    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();

    let result = arc
        .post_beef(
            row("small_chain_atomic_true"),
            std::slice::from_ref(&subject),
        )
        .await
        .unwrap();

    assert!(!result.is_success());
    let tr = &result.txid_results[0];
    assert!(
        tr.service_error,
        "a 400 is a fault of the request: transient"
    );
    assert!(!bsv_wallet_toolbox_rs::storage::is_definitive_rejection(tr));
    assert!(
        !tr.status.contains("46") && !tr.status.contains("invalid"),
        "no door's status sniff reads it as invalid: {:?}",
        tr.status
    );
    assert!(has_note(&result, "postRawTxRequestFault"), "{:?}", result);
    assert!(has_note(&result, "postBeefRetryJson"), "{:?}", result);
    assert!(matches!(
        bsv_wallet_toolbox_rs::classify_broadcast_results(std::slice::from_ref(&result)),
        bsv_wallet_toolbox_rs::BroadcastOutcome::ServiceError { .. }
    ));

    let seen = log.lock().unwrap().clone();
    let forms: Vec<(&str, Option<&[u8]>)> = seen
        .iter()
        .map(|s| (s.content_type.as_str(), s.decoded.as_deref().ok()))
        .collect();
    assert_eq!(
        forms,
        vec![
            ("application/octet-stream", Some(plain)),
            ("application/json", Some(plain)),
        ],
        "the plain BEEF as bytes, then once as the reference's JSON hex"
    );
}

/// The re-post in the other form is the post: a 400 on the bytes and a 200 on
/// the JSON hex is a success.
#[tokio::test]
async fn a_400_on_the_bytes_and_a_200_on_the_json_is_a_success() {
    let plain = row("small_chain_true");
    let subject = subject_of(plain);
    let mut server = mockito::Server::new_async().await;
    let bytes = server
        .mock("POST", "/v1/tx")
        .match_header("content-type", "application/octet-stream")
        .with_status(400)
        .with_body(ARC_400_PARSE)
        .expect(1)
        .create_async()
        .await;
    let json = server
        .mock("POST", "/v1/tx")
        .match_header("content-type", "application/json")
        .match_body(mockito::Matcher::Json(
            serde_json::json!({ "rawTx": hex::encode(plain) }),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"txid":"{subject}","txStatus":"SEEN_ON_NETWORK","extraInfo":""}}"#
        ))
        .expect(1)
        .create_async()
        .await;
    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();

    let result = arc
        .post_beef(plain, std::slice::from_ref(&subject))
        .await
        .unwrap();

    bytes.assert_async().await;
    json.assert_async().await;
    assert!(result.is_success(), "{:?}", result);
}

/// Every other 4xx verdict ARC gives on a parsed transaction stays
/// definitive: the 400 reading moves nothing else.
#[tokio::test]
async fn a_465_stays_the_transactions_rejection() {
    let plain = row("small_chain_true");
    let subject = subject_of(plain);
    let mut server = mockito::Server::new_async().await;
    let m = server
        .mock("POST", "/v1/tx")
        .with_status(465)
        .with_header("content-type", "application/json")
        .with_body(r#"{"status":465,"title":"Fee too low","detail":"The fees are too low"}"#)
        .expect(1)
        .create_async()
        .await;
    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();
    let result = arc
        .post_beef(plain, std::slice::from_ref(&subject))
        .await
        .unwrap();
    m.assert_async().await;
    assert!(bsv_wallet_toolbox_rs::storage::is_definitive_rejection(
        &result.txid_results[0]
    ));
}

/// The witness of the brief: ARC's 400 with its parse text, through the
/// monitor's `send_waiting`, leaves the transaction's word unchanged (still
/// `sending`, its request `unsent` for the re-ask, its input still locked).
/// At 0.7.1 the 400 was a definitive rejection: the transaction was retired.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn an_arc_400_leaves_the_transactions_word_unchanged() {
    use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
    use bsv_wallet_toolbox_rs::services::WalletServices;
    use bsv_wallet_toolbox_rs::{
        MonitorStorage, StorageSqlx, WalletStorageProvider, WalletStorageWriter,
    };

    // The provider's real answer to ARC's 400, handed to the storage through
    // the mock services.
    let plain = row("small_chain_true");
    let subject = subject_of(plain);
    let mut server = mockito::Server::new_async().await;
    let _log = mock_arc_400(&mut server).await;
    let arc = ArcProvider::new(server.url(), Some(ArcConfig::default()), Some("arcPin")).unwrap();
    let answer = arc
        .post_beef(plain, std::slice::from_ref(&subject))
        .await
        .unwrap();
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![answer]))
        .build();

    let storage = StorageSqlx::in_memory().await.expect("storage");
    let storage_key = "02".to_string() + &"ab".repeat(32);
    storage
        .migrate("bb-tests", &storage_key)
        .await
        .expect("migrate");
    storage.make_available().await.expect("available");
    storage.set_services(StdArc::new(services) as StdArc<dyn WalletServices>);
    let identity = "02".to_string() + &"cd".repeat(32);
    let (user, _) = storage.find_or_insert_user(&identity).await.expect("user");
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("basket");

    let beef = Beef::from_binary(plain).unwrap();
    let subject_tx = beef.txs.last().unwrap().tx().unwrap().clone();
    let raw = subject_tx.to_binary();
    let parent_txid = subject_tx.inputs[0].get_source_txid().unwrap();
    let then = chrono::Utc::now() - chrono::Duration::minutes(5);
    let lock = hex::decode("51").unwrap();

    let parent_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
         VALUES (?, 'unproven', 'parent', 0, 1000, 1, 0, 'parent', ?, X'01000000', ?, ?)",
    )
    .bind(user.user_id)
    .bind(&parent_txid)
    .bind(then)
    .bind(then)
    .execute(storage.pool())
    .await
    .unwrap()
    .last_insert_rowid();
    let tx_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
         VALUES (?, 'sending', 'ours', 1, -1, 1, 0, 'ours', ?, ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(&subject)
    .bind(&raw)
    .bind(then)
    .bind(then)
    .execute(storage.pool())
    .await
    .unwrap()
    .last_insert_rowid();
    let input_output_id = sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                              txid, type, spendable, change, spent_by, provided_by, purpose,
                              output_description, created_at, updated_at)
         VALUES (?, ?, ?, 0, 1000, ?, ?, 'custom', 0, 1, ?, 'storage', 'change', 'input', ?, ?)",
    )
    .bind(user.user_id)
    .bind(parent_id)
    .bind(basket.basket_id)
    .bind(&lock)
    .bind(&parent_txid)
    .bind(tx_id)
    .bind(then)
    .bind(then)
    .execute(storage.pool())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, created_at, updated_at)
         VALUES (?, 'unsent', 0, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(&subject)
    .bind(&raw)
    .bind(plain)
    .bind(then)
    .bind(then)
    .execute(storage.pool())
    .await
    .unwrap();

    storage
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .expect("send_waiting");

    let tx_status: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(&subject)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let (req_status, attempts): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
            .bind(&subject)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let (spendable, spent_by): (i64, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(input_output_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(tx_status, "sending", "the transaction's word is unchanged");
    assert_eq!(
        (req_status.as_str(), attempts),
        ("unsent", 1),
        "the request waits for the re-ask"
    );
    assert_eq!(
        (spendable, spent_by),
        (0, Some(tx_id)),
        "the input stays locked"
    );
}
