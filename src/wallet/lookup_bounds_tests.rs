//! Witnesses of the bounds `lookup.rs` holds on what an answering SLAP
//! tracker or overlay host chooses, against local listeners on 127.0.0.1.

use super::*;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, TransactionInput, TransactionOutput};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How a local listener frames its answer.
#[derive(Clone)]
enum Framing {
    /// `Content-Length` equal to the body's length.
    Length,
    /// `Transfer-Encoding: chunked`, no length.
    Chunked,
    /// `Content-Length` of the given figure, then the whole body.
    Declared(usize),
}

/// A local listener's answer: status line, extra headers, body, framing.
#[derive(Clone)]
struct Answer {
    status: &'static str,
    headers: Vec<String>,
    body: Arc<Vec<u8>>,
    framing: Framing,
}

impl Answer {
    fn ok(body: Vec<u8>, framing: Framing) -> Self {
        Self {
            status: "200 OK",
            headers: vec![],
            body: Arc::new(body),
            framing,
        }
    }

    fn failing() -> Self {
        Self {
            status: "500 Internal Server Error",
            headers: vec![],
            body: Arc::new(Vec::new()),
            framing: Framing::Length,
        }
    }

    fn redirect(to: &str) -> Self {
        Self {
            status: "307 Temporary Redirect",
            headers: vec![format!("Location: {to}/lookup")],
            body: Arc::new(Vec::new()),
            framing: Framing::Length,
        }
    }
}

/// A listener on 127.0.0.1 answering every request with `answer`; its base
/// URL and the count of requests it took.
async fn listener(answer: Answer) -> (String, Arc<AtomicUsize>) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = socket.accept().await else {
                return;
            };
            counted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(answer_one(stream, answer.clone()));
        }
    });
    (url, hits)
}

async fn answer_one(mut stream: TcpStream, answer: Answer) {
    // Read the request's head and its declared body.
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    let head_end = loop {
        let Ok(n) = stream.read(&mut buf).await else {
            return;
        };
        if n == 0 {
            return;
        }
        seen.extend_from_slice(&buf[..n]);
        if let Some(i) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&seen[..head_end]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    while seen.len() < head_end + length {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => seen.extend_from_slice(&buf[..n]),
        }
    }

    let mut out = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nConnection: close\r\n",
        answer.status
    );
    for h in &answer.headers {
        out.push_str(h);
        out.push_str("\r\n");
    }
    match answer.framing {
        Framing::Length => out.push_str(&format!("Content-Length: {}\r\n", answer.body.len())),
        Framing::Declared(n) => out.push_str(&format!("Content-Length: {n}\r\n")),
        Framing::Chunked => out.push_str("Transfer-Encoding: chunked\r\n"),
    }
    out.push_str("\r\n");
    if stream.write_all(out.as_bytes()).await.is_err() {
        return;
    }
    match answer.framing {
        Framing::Length | Framing::Declared(_) => {
            let _ = stream.write_all(&answer.body).await;
        }
        Framing::Chunked => {
            for chunk in answer.body.chunks(64 * 1024) {
                let size = format!("{:x}\r\n", chunk.len());
                if stream.write_all(size.as_bytes()).await.is_err()
                    || stream.write_all(chunk).await.is_err()
                    || stream.write_all(b"\r\n").await.is_err()
                {
                    return;
                }
            }
            let _ = stream.write_all(b"0\r\n\r\n").await;
        }
    }
    let _ = stream.shutdown().await;
}

/// An AtomicBEEF whose subject's output 0 is locked by `lock`, spending a
/// parent proven by a BUMP.
fn beef_of(lock: LockingScript) -> Vec<u8> {
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new("22".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    parent.outputs.push(TransactionOutput {
        satoshis: Some(10_000),
        locking_script: lock.clone(),
        change: false,
    });
    let parent_txid = parent.id();
    let mut child = Transaction::new();
    let mut input = TransactionInput::new(parent_txid.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: lock,
        change: false,
    });
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, 800_000));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary_atomic(&txid).unwrap()
}

/// A PushDrop lock carrying `fields`.
fn pushdrop(fields: Vec<Vec<u8>>) -> LockingScript {
    PushDrop::new(PrivateKey::random().public_key(), fields).lock()
}

/// An output-list answer of `outputs`, as JSON bytes.
fn answer_of(outputs: Vec<(Vec<u8>, u32)>) -> Vec<u8> {
    serde_json::to_vec(&LookupAnswer::OutputList {
        outputs: outputs
            .into_iter()
            .map(|(beef, output_index)| OutputListItem {
                beef,
                output_index,
                context: None,
            })
            .collect(),
    })
    .unwrap()
}

/// A SLAP answer naming each of `hosts`, one output each.
fn slap_answer(hosts: &[&str]) -> Vec<u8> {
    answer_of(
        hosts
            .iter()
            .map(|h| {
                let lock = pushdrop(vec![
                    b"SLAP".to_vec(),
                    vec![2; 33],
                    h.as_bytes().to_vec(),
                    b"ls_identity".to_vec(),
                ]);
                (beef_of(lock), 0)
            })
            .collect(),
    )
}

/// The PushDrop lock of a certificate of serial number `serial`.
fn cert_lock(serial: &str) -> LockingScript {
    let cert = serde_json::json!({
        "type": "dGVzdA==",
        "serialNumber": serial,
        "subject": "02aa",
        "certifier": "03bb",
    });
    pushdrop(vec![serde_json::to_vec(&cert).unwrap()])
}

/// An overlay answer of one certificate per serial number.
fn cert_answer(serials: &[&str]) -> Vec<u8> {
    answer_of(serials.iter().map(|s| (beef_of(cert_lock(s)), 0)).collect())
}

/// `body` padded with trailing spaces (still JSON) to `len` bytes.
fn padded(mut body: Vec<u8>, len: usize) -> Vec<u8> {
    assert!(body.len() <= len);
    body.resize(len, b' ');
    body
}

/// The serial numbers a lookup through `trackers` answers.
async fn serials(trackers: Vec<String>) -> Vec<String> {
    HttpLookupResolver::with_endpoints(trackers)
        .lookup_by_identity_key("02aa")
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.serial_number)
        .collect()
}

/// Two overlay hosts answering serial "X" and serial "Y", and the URLs.
async fn hosts_x_and_y() -> (String, String) {
    let (x, _) = listener(Answer::ok(cert_answer(&["X"]), Framing::Length)).await;
    let (y, _) = listener(Answer::ok(cert_answer(&["Y"]), Framing::Length)).await;
    (x, y)
}

// =============================================================================
// The body bound
// =============================================================================

/// A tracker whose answer is one byte past the bound (a true length, or no
/// length) is that tracker's failure: the next tracker's hosts answer.
#[tokio::test]
async fn a_tracker_answer_one_byte_past_the_bound_is_that_trackers_failure() {
    let (x, y) = hosts_x_and_y().await;
    let past = MAX_LOOKUP_RESPONSE_BYTES + 1;
    for framing in [Framing::Length, Framing::Chunked] {
        let big = padded(slap_answer(&[&x]), past);
        let (refused, _) = listener(Answer::ok(big, framing)).await;
        let (second, _) = listener(Answer::ok(slap_answer(&[&y]), Framing::Length)).await;
        assert_eq!(serials(vec![refused, second]).await, vec!["Y".to_string()]);
    }
}

/// An overlay host whose answer is one byte past the bound is that host's
/// failure: the next host named answers.
#[tokio::test]
async fn an_overlay_answer_one_byte_past_the_bound_is_that_hosts_failure() {
    let past = MAX_LOOKUP_RESPONSE_BYTES + 1;
    for framing in [Framing::Length, Framing::Chunked] {
        let (x, _) = listener(Answer::ok(padded(cert_answer(&["X"]), past), framing)).await;
        let (y, _) = listener(Answer::ok(cert_answer(&["Y"]), Framing::Length)).await;
        let (tracker, _) = listener(Answer::ok(slap_answer(&[&x, &y]), Framing::Length)).await;
        assert_eq!(serials(vec![tracker]).await, vec!["Y".to_string()]);
    }
}

/// A length that understates a body one byte past the bound: the declared
/// bytes are the answer (they hold the whole JSON) and the rest is never read.
#[tokio::test]
async fn an_understated_length_reads_the_declared_bytes_only() {
    let past = MAX_LOOKUP_RESPONSE_BYTES + 1;
    let at = Framing::Declared(MAX_LOOKUP_RESPONSE_BYTES);
    let (x, _) = listener(Answer::ok(padded(cert_answer(&["X"]), past), at.clone())).await;
    let (tracker, _) = listener(Answer::ok(padded(slap_answer(&[&x]), past), at)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["X".to_string()]);
}

/// Answers of exactly the bound, from a tracker and from a host, parse.
#[tokio::test]
async fn answers_at_the_bound_parse() {
    let at = MAX_LOOKUP_RESPONSE_BYTES;
    let (x, _) = listener(Answer::ok(
        padded(cert_answer(&["X"]), at),
        Framing::Chunked,
    ))
    .await;
    let (tracker, _) = listener(Answer::ok(padded(slap_answer(&[&x]), at), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["X".to_string()]);
}

/// The read itself: a declared length past the bound reads no byte of the
/// body; a body with no length stops at the chunk that passes the bound; an
/// understated length reads that length; each in well under the timeout.
#[tokio::test]
async fn the_refused_read_takes_no_more_than_the_bound() {
    let max = MAX_LOOKUP_RESPONSE_BYTES;
    let client = reqwest::Client::new();
    let get = |url: String| {
        let client = client.clone();
        async move { client.get(url).send().await.unwrap() }
    };
    let body = Arc::new(vec![b' '; max + 1]);
    let answer = |framing| Answer {
        status: "200 OK",
        headers: vec![],
        body: body.clone(),
        framing,
    };

    let (url, _) = listener(answer(Framing::Length)).await;
    let started = std::time::Instant::now();
    let refused = read_lookup_body(get(url).await, max).await.unwrap_err();
    assert_eq!(refused.read, 0, "{refused}");
    assert!(started.elapsed() < std::time::Duration::from_secs(2));

    let (url, _) = listener(answer(Framing::Chunked)).await;
    let started = std::time::Instant::now();
    let refused = read_lookup_body(get(url).await, max).await.unwrap_err();
    assert!(refused.read > max, "{refused}");
    assert!(refused.read <= max + 64 * 1024, "{refused}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));

    let (url, _) = listener(answer(Framing::Declared(1000))).await;
    let read = read_lookup_body(get(url).await, max).await.unwrap();
    assert_eq!(read.len(), 1000);

    let (url, _) = listener(Answer::ok(vec![b' '; max], Framing::Chunked)).await;
    let read = read_lookup_body(get(url).await, max).await.unwrap();
    assert_eq!(read.len(), max);
}

// =============================================================================
// Outputs an answer carries that hold nothing to read
// =============================================================================

/// An overlay output whose PushDrop decodes with no field, and one whose
/// output index is past its transaction's outputs, are skipped: the call
/// answers with the answer's other output.
#[tokio::test]
async fn an_output_with_no_field_or_no_such_output_is_skipped() {
    let answer = answer_of(vec![
        (beef_of(pushdrop(vec![])), 0),
        (beef_of(cert_lock("Z")), 7),
        (beef_of(cert_lock("Y")), 0),
    ]);
    let (host, _) = listener(Answer::ok(answer, Framing::Length)).await;
    let (tracker, _) = listener(Answer::ok(slap_answer(&[&host]), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["Y".to_string()]);
}

/// A SLAP output whose PushDrop decodes with no field, and one whose output
/// index is past its transaction's outputs, name no host: the host the
/// answer's other output names is called.
#[tokio::test]
async fn a_slap_output_with_no_field_or_no_such_output_names_no_host() {
    let (y, _) = listener(Answer::ok(cert_answer(&["Y"]), Framing::Length)).await;
    let mut outputs = vec![(beef_of(pushdrop(vec![])), 0)];
    let named: LookupAnswer = serde_json::from_slice(&slap_answer(&[&y])).unwrap();
    let LookupAnswer::OutputList { outputs: named } = named else {
        unreachable!()
    };
    outputs.push((named[0].beef.clone(), 3));
    outputs.push((named[0].beef.clone(), 0));
    let (tracker, _) = listener(Answer::ok(answer_of(outputs), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["Y".to_string()]);
}

// =============================================================================
// Redirects
// =============================================================================

/// A tracker's and a host's 307 are their failures: the URL each names is
/// never called, and the next tracker's and host's answers stand.
#[tokio::test]
async fn a_redirect_is_that_hosts_failure_and_is_not_followed() {
    let (elsewhere, elsewhere_hits) =
        listener(Answer::ok(cert_answer(&["R"]), Framing::Length)).await;
    let (y, _) = listener(Answer::ok(cert_answer(&["Y"]), Framing::Length)).await;
    let (redirecting_host, _) = listener(Answer::redirect(&elsewhere)).await;
    let (redirecting_tracker, _) = listener(Answer::redirect(&elsewhere)).await;
    let (tracker, _) = listener(Answer::ok(
        slap_answer(&[&redirecting_host, &y]),
        Framing::Length,
    ))
    .await;
    let serials = serials(vec![redirecting_tracker, tracker]).await;
    assert_eq!(serials, vec!["Y".to_string()]);
    assert_eq!(elsewhere_hits.load(Ordering::SeqCst), 0);
}

// =============================================================================
// Outputs one answer may carry
// =============================================================================

/// A host's answer of `count` outputs, each the certificate of serial `serial`.
fn many_certs(serial: &str, count: usize) -> Vec<u8> {
    let beef = beef_of(cert_lock(serial));
    answer_of((0..count).map(|_| (beef.clone(), 0)).collect())
}

/// A host's answer one output past the bound is that host's failure: the
/// next host answers. An answer at the bound is read whole.
#[tokio::test]
async fn an_overlay_answer_past_the_output_bound_is_that_hosts_failure() {
    let (x, _) = listener(Answer::ok(
        many_certs("X", MAX_LOOKUP_OUTPUTS + 1),
        Framing::Length,
    ))
    .await;
    let (y, _) = listener(Answer::ok(cert_answer(&["Y"]), Framing::Length)).await;
    let (tracker, _) = listener(Answer::ok(slap_answer(&[&x, &y]), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["Y".to_string()]);

    let (x, _) = listener(Answer::ok(
        many_certs("X", MAX_LOOKUP_OUTPUTS),
        Framing::Length,
    ))
    .await;
    let (tracker, _) = listener(Answer::ok(slap_answer(&[&x]), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await.len(), MAX_LOOKUP_OUTPUTS);
}

/// A tracker's answer one output past the bound is that tracker's failure:
/// the next tracker's host answers.
#[tokio::test]
async fn a_tracker_answer_past_the_output_bound_is_that_trackers_failure() {
    let (x, y) = hosts_x_and_y().await;
    let named: LookupAnswer = serde_json::from_slice(&slap_answer(&[&x])).unwrap();
    let LookupAnswer::OutputList { outputs: named } = named else {
        unreachable!()
    };
    let many = answer_of(
        (0..=MAX_LOOKUP_OUTPUTS)
            .map(|_| (named[0].beef.clone(), 0))
            .collect(),
    );
    let (refused, _) = listener(Answer::ok(many, Framing::Length)).await;
    let (second, _) = listener(Answer::ok(slap_answer(&[&y]), Framing::Length)).await;
    assert_eq!(serials(vec![refused, second]).await, vec!["Y".to_string()]);
}

// =============================================================================
// Hosts one tracker's answer may name
// =============================================================================

/// A tracker's answer naming one host past the bound: the bound's hosts are
/// called, each failing, and the last one named is never called.
#[tokio::test]
async fn a_tracker_answer_names_at_most_the_bound_of_hosts() {
    let mut urls = Vec::new();
    let mut hits = Vec::new();
    for _ in 0..=MAX_HOSTS_PER_TRACKER {
        let (url, hit) = listener(Answer::failing()).await;
        urls.push(url);
        hits.push(hit);
    }
    let named: Vec<&str> = urls.iter().map(String::as_str).collect();
    let (tracker, _) = listener(Answer::ok(slap_answer(&named), Framing::Length)).await;
    assert!(serials(vec![tracker]).await.is_empty());
    let called: usize = hits.iter().map(|h| h.load(Ordering::SeqCst)).sum();
    assert_eq!(called, MAX_HOSTS_PER_TRACKER);
    assert_eq!(hits[MAX_HOSTS_PER_TRACKER].load(Ordering::SeqCst), 0);
}
