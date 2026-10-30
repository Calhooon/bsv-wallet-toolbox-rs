//! #388: trust filtering, subject binding and paging at the discovery doors.

use super::discovery_trust::{
    certifier_info, discovery_page, filter_by_identity_key, filter_certificates_by_attributes,
    transform,
};
use crate::managers::{Certifier, TrustSettings, DEFAULT_SETTINGS};
use crate::wallet::lookup::OverlayCertificate;
use indexmap::IndexMap;

const STRANGER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
const REFERENCE_ICON: &str = "https://bsvblockchain.org/favicon.ico";

fn host_certifier() -> Certifier {
    Certifier {
        name: "Host certifier".into(),
        description: "A host supplied certifier".into(),
        identity_key: STRANGER.into(),
        trust: 2,
        icon_url: None,
        base_url: None,
    }
}

/// A label as base64 of 32 bytes, a type or serial the door keeps.
fn sn(label: &str) -> String {
    let mut bytes = [0u8; 32];
    bytes[..label.len()].copy_from_slice(label.as_bytes());
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}
/// The smallest canonical DER signature (r = s = 1); none is verified here.
const SIGNATURE: &str = "3006020101020101";

/// An entry as the door holds it after decryption; the unit tests here read
/// the transform and the matcher, which see only that.
fn certificate(certifier: &str, subject: &str, serial: &str) -> OverlayCertificate {
    OverlayCertificate {
        type_id: sn("type"),
        serial_number: sn(serial),
        subject: subject.into(),
        certifier: certifier.into(),
        revocation_outpoint: Some(format!("{}.0", "0".repeat(64))),
        signature: Some(SIGNATURE.into()),
        fields: IndexMap::from([("name".into(), "encrypted".into())]),
        // A keyring value is canonical base64, as the reference drops another.
        keyring: IndexMap::from([("name".into(), "a2V5".into())]),
        decrypted_fields: IndexMap::from([("name".into(), "Alice".into())]),
    }
}

#[test]
fn certifier_info_fills_a_default_certifier_and_drops_a_stranger() {
    let trust = &DEFAULT_SETTINGS.trust_settings;
    assert_eq!(trust.trusted_certifiers.len(), 2);
    for certifier in &trust.trusted_certifiers {
        let info = certifier_info(trust, &certifier.identity_key).unwrap();
        assert_eq!(info.name, certifier.name);
        assert_eq!(
            info.icon_url,
            certifier
                .icon_url
                .clone()
                .unwrap_or_else(|| REFERENCE_ICON.into())
        );
        assert_eq!(info.description, certifier.description);
        assert_eq!(u32::from(info.trust), certifier.trust);
    }
    assert!(certifier_info(trust, STRANGER).is_none());
}

#[test]
fn discovery_description_cuts_utf8_trims_and_falls_back_with_icon() {
    let mut trust = TrustSettings {
        trust_level: 1,
        trusted_certifiers: vec![host_certifier()],
    };
    for (description, expected) in [
        (format!("{}é", "x".repeat(49)), "x".repeat(49)),
        (format!("{}  remainder", "é".repeat(24)), "é".repeat(24)),
        ("😀".into(), "Trusted certifier".into()),
        ("Valid text\u{feff}".into(), "Valid text".into()),
        ("abc\u{0085}".into(), "abc\u{0085}".into()),
    ] {
        trust.trusted_certifiers[0].description = description;
        let info = certifier_info(&trust, STRANGER).unwrap();
        assert_eq!(info.description, expected);
        assert!((5..=50).contains(&info.description.len()));
        assert_eq!(info.icon_url, REFERENCE_ICON);
    }
}

#[test]
fn discovery_sums_distinct_certifiers_once_and_deduplicates_per_subject() {
    let mut trust = DEFAULT_SETTINGS.trust_settings.clone();
    trust.trust_level = 5;
    trust.trusted_certifiers[0].trust = 2;
    let a = &trust.trusted_certifiers[0].identity_key;
    let b = &trust.trusted_certifiers[1].identity_key;
    let one = certificate(a, "subject", "one");
    let certificates = vec![
        one.clone(),
        one.clone(),
        certificate(a, "subject", "two"),
        certificate(b, "subject", "one"),
        certificate(a, "other", "one"),
        certificate(STRANGER, "subject", "stranger"),
    ];
    let result = transform(&trust, certificates.clone()).unwrap();
    assert_eq!(result.total_certificates, 3);
    assert_eq!(
        result
            .certificates
            .iter()
            .map(|c| (c.certificate.serial_number.clone(), c.certifier_info.trust))
            .collect::<Vec<_>>(),
        vec![(sn("one"), 3), (sn("one"), 2), (sn("two"), 2)]
    );
    assert_eq!(result.certificates[1].certificate.fields, one.fields);
    assert_eq!(
        result.certificates[1].decrypted_fields,
        one.decrypted_fields
    );
    assert_eq!(
        result.certificates[1].publicly_revealed_keyring,
        one.keyring
    );
    assert_eq!(result.certificates[1].certificate.signature, SIGNATURE);
    trust.trust_level = 6;
    assert_eq!(
        transform(&trust, certificates).unwrap().total_certificates,
        0,
        "another certificate from one certifier adds no trust"
    );
}

#[test]
fn discovery_binds_subject_case_insensitively_before_the_input_cap() {
    let key = &DEFAULT_SETTINGS.trust_settings.trusted_certifiers[0].identity_key;
    let mut certificates = vec![certificate(key, "wrong", "bad"); 256];
    certificates.push(certificate(key, &STRANGER.to_uppercase(), "kept"));
    let filtered = filter_by_identity_key(certificates, STRANGER);
    assert_eq!(filtered.len(), 1);
    let result = transform(&DEFAULT_SETTINGS.trust_settings, filtered).unwrap();
    assert_eq!(result.total_certificates, 1);
    assert_eq!(result.certificates[0].certificate.serial_number, sn("kept"));
}

#[test]
fn discovery_caps_inputs_and_pages_without_changing_the_total() {
    let trust = &DEFAULT_SETTINGS.trust_settings;
    let key = &trust.trusted_certifiers[0].identity_key;
    let certs = (0..257)
        .map(|i| certificate(key, "subject", &i.to_string()))
        .collect();
    let all = transform(trust, certs).unwrap();
    assert_eq!(all.total_certificates, 256);
    let default_page = discovery_page(all.clone(), None, None);
    assert_eq!(default_page.certificates.len(), 10);
    let last = discovery_page(all.clone(), Some(255), Some(2));
    assert_eq!(last.total_certificates, 256);
    assert_eq!(last.certificates.len(), 1);
    assert_eq!(last.certificates[0].certificate.serial_number, sn("255"));
    assert!(discovery_page(all, Some(u32::MAX), Some(10))
        .certificates
        .is_empty());
}

#[test]
fn discovery_groups_subjects_separately_and_rejects_empty_names() {
    let trust = &DEFAULT_SETTINGS.trust_settings;
    let key = &trust.trusted_certifiers[0].identity_key;
    let result = transform(
        trust,
        vec![
            certificate(key, "one", "same"),
            certificate(key, "two", "same"),
            certificate(key, "", "empty subject"),
            certificate("", "one", "empty certifier"),
        ],
    )
    .unwrap();
    assert_eq!(result.total_certificates, 2);
    assert_eq!(result.certificates[0].certificate.subject, "one");
    assert_eq!(result.certificates[1].certificate.subject, "two");
}

#[test]
fn discovery_attributes_reject_non_string_any_at_the_typed_boundary() {
    use bsv_rs::wallet::DiscoverByAttributesArgs;
    use serde_json::json;

    for attributes in [
        json!({"any": 42}),
        json!({"any": null}),
        json!([]),
        json!(null),
    ] {
        assert!(serde_json::from_value::<DiscoverByAttributesArgs>(json!({
            "attributes": attributes
        }))
        .is_err());
    }
}

#[test]
fn discovery_attributes_fold_case_split_and_drop_as_the_reference() {
    // Each answer read from identityUtils.ts:140-244 at edf6e03 in node.
    for (field, actual, query, matches) in [
        // Legacy /gi: one-unit uppercase folds; ß's is two units, ı's is
        // ASCII, and a non-BMP letter is two surrogates.
        ("name", "\u{ff}", "\u{178}", true),
        ("name", "\u{3c2}", "\u{3a3}", true),
        ("name", "\u{df}", "\u{1e9e}", false),
        ("name", "\u{131}", "I", false),
        ("name", "\u{10428}", "\u{10400}", false),
        // The fold removes Mn after NFD; a Devanagari vowel sign is Mc.
        ("any", "Zo\u{eb} \u{c5}ngstr\u{f6}m", "zoe angstrom", true),
        (
            "any",
            "Zo\u{eb} \u{c5}ngstr\u{f6}m",
            "\"zoe angstrom\"",
            true,
        ),
        (
            "any",
            "\u{915}\u{93f}\u{924}\u{93e}\u{92c}",
            "\u{915}\u{924}\u{92c}",
            false,
        ),
        (
            "any",
            "\u{915}\u{93f}\u{924}\u{93e}\u{92c}",
            "\u{915}\u{93f}\u{924}\u{93e}\u{92c}",
            true,
        ),
        // Terminal_Punctuation (U+3002) and Dash (U+2010) split tokens.
        ("any", "Alice\u{3002}Wonderland", "Wonderland", true),
        ("any", "Alice\u{2010}Wonderland", "Wonderland", true),
        ("any", "the", "the", false),
        ("any", "Bob", "Bob the", true),
        ("any", "Jos\u{e9} Wonderland", "Wonderland -jose", false),
        ("any", "Alice Wonderland", "Alice -\"Bob Smith\"", true),
    ] {
        let mut cert = certificate(STRANGER, STRANGER, "candidate");
        cert.decrypted_fields = IndexMap::from([("name".into(), actual.into())]);
        let attributes = IndexMap::from([(field.to_string(), query.to_string())]);
        assert_eq!(
            filter_certificates_by_attributes(vec![cert], &attributes)
                .unwrap()
                .len(),
            usize::from(matches),
            "{field}: {actual:?} against {query:?}"
        );
    }
    // Object.entries puts array-index keys ahead of the rest when joining `any`'s text.
    let mut cert = certificate(STRANGER, STRANGER, "candidate");
    cert.decrypted_fields = IndexMap::from([
        ("name".into(), "Wonderland".into()),
        ("1".into(), "Alice".into()),
    ]);
    let attributes = IndexMap::from([("any".into(), "\"alice wonderland\"".into())]);
    assert_eq!(
        filter_certificates_by_attributes(vec![cert], &attributes)
            .unwrap()
            .len(),
        1
    );
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "discovery_trust_doors_tests.rs"]
mod doors;
