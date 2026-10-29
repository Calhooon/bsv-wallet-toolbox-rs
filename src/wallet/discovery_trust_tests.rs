//! #388: trust filtering, subject binding and paging at the discovery doors.

use super::discovery_trust::{certifier_info, discovery_page, filter_by_identity_key, transform};
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

fn certificate(certifier: &str, subject: &str, serial: &str) -> OverlayCertificate {
    OverlayCertificate {
        type_id: "dHlwZQ==".into(),
        serial_number: serial.into(),
        subject: subject.into(),
        certifier: certifier.into(),
        revocation_outpoint: Some(format!("{}.0", "0".repeat(64))),
        signature: Some("3044".into()),
        fields: IndexMap::from([("name".into(), "encrypted".into())]),
        keyring: IndexMap::from([("name".into(), "key".into())]),
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
            .map(|c| (c.certificate.serial_number.as_str(), c.certifier_info.trust))
            .collect::<Vec<_>>(),
        vec![("one", 3), ("one", 2), ("two", 2)]
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
    assert_eq!(result.certificates[1].certificate.signature, "3044");
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
    assert_eq!(result.certificates[0].certificate.serial_number, "kept");
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
    assert_eq!(last.certificates[0].certificate.serial_number, "255");
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

/// The door: a stored certificate from a default certifier is answered with
/// its `certifierInfo`; one from a stranger is dropped. The overlay leg is
/// refused by the test transport before any request (#182, rule 28), so the
/// local leg alone answers.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn discovery_keeps_a_default_certifiers_certificate_and_drops_a_strangers() {
    use crate::services::mock::MockWalletServices;
    use crate::storage::entities::TableCertificate;
    use crate::storage::WalletStorageWriter;
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::{DiscoverByIdentityKeyArgs, WalletInterface};

    let key = PrivateKey::from_hex(&format!("{:064x}", 388)).unwrap();
    let subject = key.public_key().to_hex();
    let storage = crate::StorageSqlx::in_memory().await.unwrap();
    storage.migrate("discovery-trust", &subject).await.unwrap();
    storage.make_available().await.unwrap();
    let wallet = super::Wallet::new(Some(key), storage.clone(), MockWalletServices::new())
        .await
        .unwrap();
    let auth = wallet.auth();
    let trusted = &DEFAULT_SETTINGS.trust_settings.trusted_certifiers[0];
    let now = chrono::Utc::now();
    for (serial, certifier) in [
        ("dHJ1c3RlZA==", trusted.identity_key.as_str()),
        ("c3RyYW5nZXI=", STRANGER),
    ] {
        let certificate = TableCertificate {
            certificate_id: 0,
            user_id: auth.user_id.unwrap(),
            cert_type: "dHlwZQ==".into(),
            serial_number: serial.into(),
            certifier: certifier.into(),
            subject: subject.clone(),
            verifier: None,
            revocation_outpoint: format!("{}.0", "0".repeat(64)),
            signature: "3044".into(),
            created_at: now,
            updated_at: now,
        };
        storage
            .insert_certificate(&auth, certificate)
            .await
            .unwrap();
    }

    let result = WalletInterface::discover_by_identity_key(
        &wallet,
        DiscoverByIdentityKeyArgs {
            identity_key: subject.clone(),
            limit: None,
            offset: None,
            seek_permission: None,
        },
        None,
    )
    .await
    .unwrap();

    let kept: Vec<&str> = result
        .certificates
        .iter()
        .map(|c| c.certificate.certifier.as_str())
        .collect();
    assert_eq!(
        kept,
        vec![trusted.identity_key.as_str()],
        "the stranger's certificate is dropped"
    );
    assert_eq!(result.total_certificates, 1);
    let info = &result.certificates[0].certifier_info;
    assert_eq!(info.name, trusted.name);
    assert_eq!(u32::from(info.trust), trusted.trust);
    assert_eq!(info.description, trusted.description);
}
