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
