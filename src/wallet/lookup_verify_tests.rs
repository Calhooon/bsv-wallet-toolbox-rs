//! Witnesses of the checks the resolver holds on a tracker's advertisement
//! and a host's answered certificate, against local listeners on 127.0.0.1.

use super::bounds_tests::*;
use super::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::templates::PushDrop;
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

use crate::wallet::lookup_identity_fixtures::{
    certificate_record, certifier_key, identity_lock, subject_key,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};

/// The serial number of the fixtures' certificate tagged `tag`.
fn serial_of(tag: &str) -> String {
    BASE64.encode(bsv_rs::primitives::sha256(tag.as_bytes()))
}

/// `record` with its certifier's signature replaced by one over other bytes.
fn with_foreign_signature(mut record: serde_json::Value) -> serde_json::Value {
    let other = certificate_record(&subject_key(), &certifier_key(), "other", &[("tag", "x")]);
    record["signature"] = other["signature"].clone();
    record
}

/// A tracker naming one host that answers `locks`; the serial numbers of
/// the certificates `lookup` returns through it.
async fn returned(
    locks: Vec<LockingScript>,
    lookup: impl Fn(HttpLookupResolver) -> futures::future::BoxFuture<'static, Vec<OverlayCertificate>>,
) -> Vec<String> {
    let answer = answer_of(locks.into_iter().map(|l| (beef_of(l), 0)).collect());
    let (host, _) = listener(Answer::ok(answer, Framing::Length)).await;
    let (tracker, _) = listener(Answer::ok(slap_answer(&[&host]), Framing::Length)).await;
    let mut serials: Vec<String> = lookup(HttpLookupResolver::with_endpoints(vec![tracker]))
        .await
        .into_iter()
        .map(|c| c.serial_number)
        .collect();
    serials.sort();
    serials
}

/// A host answers a lookup by identity key with a certificate whose
/// certifier's signature does not verify, one that verifies and is about
/// another subject, and a true one: only the true one is returned.
#[tokio::test]
async fn a_lookup_by_identity_key_returns_only_verified_certificates_about_the_key() {
    let (subject, certifier) = (subject_key(), certifier_key());
    let other = PrivateKey::from_hex(&"33".repeat(32)).unwrap();
    let fields = [("firstName", "Alice")];
    let locks = vec![
        identity_lock(
            &subject,
            &with_foreign_signature(certificate_record(
                &subject,
                &certifier,
                "unverified",
                &fields,
            )),
        ),
        identity_lock(
            &other,
            &certificate_record(&other, &certifier, "other-subject", &fields),
        ),
        identity_lock(
            &subject,
            &certificate_record(&subject, &certifier, "true", &fields),
        ),
    ];
    let key = subject.public_key().to_hex();
    let serials = returned(locks, move |r| {
        let key = key.clone();
        Box::pin(async move { r.lookup_by_identity_key(&key).await.unwrap() })
    })
    .await;
    assert_eq!(serials, vec![serial_of("true")]);
}

/// A host answers a lookup by attributes with a certificate whose
/// certifier's signature does not verify, one whose revealed fields do not
/// match the attributes asked, and a true one: only the true one is
/// returned.
#[tokio::test]
async fn a_lookup_by_attributes_returns_only_verified_certificates_whose_fields_match() {
    let (subject, certifier) = (subject_key(), certifier_key());
    let locks = vec![
        identity_lock(
            &subject,
            &with_foreign_signature(certificate_record(
                &subject,
                &certifier,
                "unverified",
                &[("firstName", "Alice")],
            )),
        ),
        identity_lock(
            &subject,
            &certificate_record(
                &subject,
                &certifier,
                "other-fields",
                &[("firstName", "Bob")],
            ),
        ),
        identity_lock(
            &subject,
            &certificate_record(&subject, &certifier, "true", &[("firstName", "Alice")]),
        ),
    ];
    let serials = returned(locks, |r| {
        Box::pin(async move {
            let attributes = HashMap::from([("firstName".to_string(), "alice".to_string())]);
            r.lookup_by_attributes(&attributes).await.unwrap()
        })
    })
    .await;
    assert_eq!(serials, vec![serial_of("true")]);
}

/// The attribute rule by itself, as the reference's
/// `filterCertificatesByAttributes` reads decrypted fields.
#[test]
fn the_attribute_rule() {
    use super::super::lookup_checks::answers_attributes;
    let cert = |fields: &[(&str, &str)]| OverlayCertificate {
        type_id: String::new(),
        serial_number: String::new(),
        subject: String::new(),
        certifier: String::new(),
        revocation_outpoint: None,
        fields: HashMap::new(),
        keyring: HashMap::new(),
        signature: None,
        decrypted_fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let ask = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let alice = cert(&[("firstName", "Alice Jane"), ("userName", "alice")]);
    assert!(answers_attributes(
        &alice,
        &ask(&[("firstName", "alice jane")])
    ));
    assert!(answers_attributes(&alice, &ask(&[("firstName", "al ja")])));
    assert!(!answers_attributes(
        &alice,
        &ask(&[("firstName", "jane alice")])
    ));
    assert!(answers_attributes(&alice, &ask(&[("userName", "alice")])));
    assert!(!answers_attributes(&alice, &ask(&[("userName", "Alice")])));
    assert!(!answers_attributes(&alice, &ask(&[("lastName", "x")])));
    assert!(!answers_attributes(&alice, &ask(&[("firstName", "  ")])));
    assert!(answers_attributes(&alice, &ask(&[("any", "jane")])));
    assert!(!answers_attributes(&alice, &ask(&[("any", "jane -alice")])));
    assert!(!answers_attributes(&alice, &ask(&[("any", "the")])));
    assert!(answers_attributes(
        &alice,
        &ask(&[("any", "\"alice jane\"")])
    ));
    assert!(!answers_attributes(&alice, &ask(&[("any", "j")])));
}

/// The reference's certificate of its own tests (`Certificate.test.ts`
/// "should create a Certificate from an object using fromObject()"), a
/// certificate of a 21-byte type, verifies under its certifier; with one
/// field's value changed it does not.
#[test]
fn the_references_certificate_verifies() {
    use super::super::lookup_checks::certificate_signature_verifies;
    let fields = [
        ("displayName", "eqsSpUgTKk891y1EkyCPPg+C4YoVZJvB0EQ4iore7VofkM5TB9Ctj7x2PgBaWI0A9tfATDO9"),
        ("email", "n6HVUvyHkIDMvB4ERxVGxmX6lgRBM+e7kbbC5DiRCKe5a60BJeXr05g4POq6OHYO9Wl/b1Xxe+JKsejl"),
        ("phoneNumber", "5yWyN9kOGaZs5G6yvXUWnWj4rm7kDAug4YIsn4BQLKGYRzDx8s1dytb43ega6BnSp0gUTnskcjiM8ekqul2a"),
        ("lat", "lc3u6SFKQ5Mpxp5vc+6s4aXe7lOyQQLfN+CbOu4XlBYsj7Jlc78gt4sGCwDSxbzvA41eElCjlc2Our5bpLcsg1I6"),
        ("lng", "FmY3iM/2/LDfYNEeXpcj7Epn933tRHz50WoBkBrqYv6jmZ6dXE6RRYId9TcaxIvB0D7Y14aD5vjSV6Bx48hdic5g"),
    ];
    let mut cert = OverlayCertificate {
        type_id: "Q29tbW9uU291cmNlIGlkZW50aXR5".to_string(),
        serial_number: "UegX3uufsqHsbEKeBSxUd9AziLSyru86TnwfhPoZJYE=".to_string(),
        subject: "028e2e075e1e57ba4c62c90125468109f9b4e2a7741da3dd76ccd0c73b2a8a37ad".to_string(),
        certifier: "03c644fe2fd97673a5d86555a58587e7936390be6582ece262bc387014bcff6fe4".to_string(),
        revocation_outpoint: Some(
            "0245242bd144a85053b4c1e4a0ed5467c79a4d172680ca77a970ebabd682d564.0".to_string(),
        ),
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        keyring: HashMap::new(),
        signature: Some("304402202c86ef816c469fe657289ddea12d2c444f006ef5ab5851f00107c7724eb67ea602202786244c077567c8f3ec5da78bd61ce0c35bf1eeac0488e026c03b21c403b0fd".to_string()),
        decrypted_fields: HashMap::new(),
    };
    assert!(certificate_signature_verifies(&cert));
    cert.fields.insert("lat".to_string(), "AAAA".to_string());
    assert!(!certificate_signature_verifies(&cert));
}
