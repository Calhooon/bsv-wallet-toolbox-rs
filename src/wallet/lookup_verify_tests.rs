//! Witnesses of the checks the resolver holds on a tracker's advertisement
//! and a host's answered certificate, against local listeners on 127.0.0.1.

use super::bounds_tests::*;
use super::*;
use bsv_rs::primitives::PrivateKey;
use std::sync::atomic::Ordering;

/// `lock` with its last PushDrop field's byte at `at` flipped, relocked
/// with the same key.
fn with_last_field_flipped(lock: &LockingScript, at: usize) -> LockingScript {
    let mut token = PushDrop::decode(lock).unwrap();
    let last = token.fields.last_mut().unwrap();
    last[at] ^= 1;
    PushDrop::new(token.locking_public_key, token.fields).lock()
}

use bsv_rs::script::LockingScript;

/// A tracker answers four advertisements: one of protocol SHIP, one for
/// another service, one whose signature does not verify, and a true one.
/// Only the true one's host is called.
#[tokio::test]
async fn only_a_verified_slap_advertisement_for_the_service_names_a_host() {
    let mut named = Vec::new();
    let mut outputs = Vec::new();
    let true_lock = |h: &str| slap_lock(&PrivateKey::random(), "SLAP", h, "ls_identity");
    for (tag, make) in [
        (
            "SHIP",
            Box::new(|h: &str| slap_lock(&PrivateKey::random(), "SHIP", h, "ls_identity"))
                as Box<dyn Fn(&str) -> LockingScript>,
        ),
        (
            "other",
            Box::new(|h: &str| slap_lock(&PrivateKey::random(), "SLAP", h, "ls_other")),
        ),
        (
            "unsigned",
            Box::new(move |h: &str| with_last_field_flipped(&true_lock(h), 12)),
        ),
        ("true", Box::new(true_lock)),
    ] {
        let (host, hits) = listener(Answer::ok(cert_answer(&[tag]), Framing::Length)).await;
        outputs.push((beef_of(make(&host)), 0));
        named.push((tag, hits));
    }
    let (tracker, _) = listener(Answer::ok(answer_of(outputs), Framing::Length)).await;
    assert_eq!(serials(vec![tracker]).await, vec!["true".to_string()]);
    let calls: Vec<(&str, usize)> = named
        .iter()
        .map(|(tag, hits)| (*tag, hits.load(Ordering::SeqCst)))
        .collect();
    assert_eq!(
        calls,
        vec![("SHIP", 0), ("other", 0), ("unsigned", 0), ("true", 1)]
    );
}

/// A listener on 127.0.0.1 that takes each request and holds its response
/// open for a minute.
async fn holding_listener() -> String {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                drop(stream);
            });
        }
    });
    url
}

/// Three trackers that each hold their response open: the call answers no
/// certificate by its deadline of 10 s (a tolerance of 2 s), where the
/// three requests one after another would run 30 s.
#[tokio::test]
async fn a_lookup_call_answers_by_its_deadline() {
    let trackers = vec![
        holding_listener().await,
        holding_listener().await,
        holding_listener().await,
    ];
    let started = std::time::Instant::now();
    let answered = HttpLookupResolver::with_endpoints(trackers)
        .lookup_by_identity_key("02aa")
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(answered.is_empty());
    assert!(
        elapsed < std::time::Duration::from_secs(12),
        "the call ran {elapsed:?}"
    );
}
