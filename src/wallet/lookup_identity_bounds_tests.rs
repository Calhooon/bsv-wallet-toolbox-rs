//! Witnesses of the bounds on an answered certificate (the reference
//! wallet's `identityUtils.ts`): at each bound and one past it.

use crate::wallet::lookup_checks::{
    certificate_record as record_of, identity_output_certificate, MAX_IDENTITY_CERTIFICATE_BYTES,
    MAX_IDENTITY_FIELDS, MAX_KEYRING_VALUE_UNITS,
};
use crate::wallet::lookup_identity_fixtures::{
    certificate_record, certifier_key, identity_lock, identity_lock_of_bytes, subject_key,
};

/// A certificate of `count` fields, every one revealed, passes at the
/// bound of 100 and is left out at 101.
#[test]
fn a_certificate_of_more_than_100_fields_is_left_out() {
    let subject = subject_key();
    let names: Vec<String> = (0..=MAX_IDENTITY_FIELDS).map(|i| format!("f{i}")).collect();
    let fields: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
    let at = certificate_record(
        &subject,
        &certifier_key(),
        "at",
        &fields[..MAX_IDENTITY_FIELDS],
    );
    let past = certificate_record(&subject, &certifier_key(), "past", &fields);
    assert!(identity_output_certificate(&identity_lock(&subject, &at)).is_ok());
    assert_eq!(
        identity_output_certificate(&identity_lock(&subject, &past)).unwrap_err(),
        "fields or keyring count out of bounds"
    );
}

/// A keyring value of 2048 units is read; one of 2049 is refused.
#[test]
fn a_keyring_value_past_2048_units_is_refused() {
    let mut record = certificate_record(&subject_key(), &certifier_key(), "k", &[("a", "v")]);
    record["keyring"]["a"] = serde_json::json!("A".repeat(MAX_KEYRING_VALUE_UNITS));
    assert!(record_of(&serde_json::to_vec(&record).unwrap()).is_ok());
    record["keyring"]["a"] = serde_json::json!("A".repeat(MAX_KEYRING_VALUE_UNITS + 1));
    assert_eq!(
        record_of(&serde_json::to_vec(&record).unwrap()).unwrap_err(),
        "keyring value past its bound"
    );
}

/// A certificate field of 256 KiB (a true certificate padded with spaces)
/// is read; one byte more is left out.
#[test]
fn a_certificate_field_past_256_kib_is_left_out() {
    let subject = subject_key();
    let record = certificate_record(&subject, &certifier_key(), "big", &[("a", "v")]);
    let mut bytes = serde_json::to_vec(&record).unwrap();
    bytes.resize(MAX_IDENTITY_CERTIFICATE_BYTES, b' ');
    assert!(identity_output_certificate(&identity_lock_of_bytes(&subject, bytes.clone())).is_ok());
    bytes.push(b' ');
    assert_eq!(
        identity_output_certificate(&identity_lock_of_bytes(&subject, bytes)).unwrap_err(),
        "certificate field out of bounds"
    );
}

/// A host answering 257 true certificates: the call returns 256.
#[tokio::test]
async fn a_lookup_returns_at_most_256_certificates() {
    use super::bounds_tests::*;
    use super::*;
    let beef = beef_of(cert_lock("many"));
    for (count, expected) in [
        (MAX_IDENTITY_RESULTS, MAX_IDENTITY_RESULTS),
        (MAX_IDENTITY_RESULTS + 1, MAX_IDENTITY_RESULTS),
    ] {
        let answer = answer_of((0..count).map(|_| (beef.clone(), 0)).collect());
        let (host, _) = listener(Answer::ok(answer, Framing::Length)).await;
        let (tracker, _) = listener(Answer::ok(slap_answer(&[&host]), Framing::Length)).await;
        assert_eq!(serials(vec![tracker]).await.len(), expected);
    }
}
