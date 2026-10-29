//! #388: discovery under the user's trust settings, witnessed at the port's
//! internal seam. The reference fills `certifierInfo` from the trusted
//! certifiers and drops a stranger's certificate (ts-stack edf6e03,
//! `identityUtils.ts:313-372`); with nothing stored it reads
//! `DEFAULT_SETTINGS` (`WalletSettingsManager.ts:160-182,226-232`). The
//! wallet reads no trust settings yet, so both witnesses are red on base and
//! ignored until the shape lands (`docs/design/discovery-trust-settings.md`).

use super::certifier_info;
use crate::managers::DEFAULT_SETTINGS;

/// A compressed-key string no default certifier carries.
const STRANGER: &str = "021111111111111111111111111111111111111111111111111111111111111111";

/// The reference's icon when a trusted certifier names none
/// (`identityUtils.ts:335`).
const REFERENCE_ICON: &str = "https://bsvblockchain.org/favicon.ico";

/// The reference's `certifierDescription` (`identityUtils.ts:35-46`): cut at
/// 50 UTF-8 bytes on a code point boundary, trailing whitespace trimmed, and
/// `Trusted certifier` when fewer than 5 bytes remain (BRC-100
/// `0100.md:1404`, `DescriptionString5to50Characters`).
fn reference_description(description: &str) -> String {
    let mut kept = String::new();
    for ch in description.chars() {
        if kept.len() + ch.len_utf8() > 50 {
            break;
        }
        kept.push(ch);
    }
    let kept = kept.trim_end();
    if kept.len() >= 5 {
        kept.to_string()
    } else {
        "Trusted certifier".to_string()
    }
}

/// The seam: every default trusted certifier has an entry filled from the
/// defaults through the reference's rules, and a stranger has none.
#[test]
#[ignore = "#388: the wallet reads no trust settings, so certifier_info answers None for every certifier"]
fn certifier_info_fills_a_default_certifier_and_drops_a_stranger() {
    let defaults = &DEFAULT_SETTINGS.trust_settings.trusted_certifiers;
    assert_eq!(defaults.len(), 2, "the reference's two default certifiers");
    for certifier in defaults {
        let info = certifier_info(&certifier.identity_key)
            .unwrap_or_else(|| panic!("{} is a default trusted certifier", certifier.name));
        assert_eq!(info.name, certifier.name);
        assert_eq!(
            info.icon_url,
            certifier
                .icon_url
                .clone()
                .unwrap_or_else(|| REFERENCE_ICON.to_string())
        );
        assert_eq!(
            info.description,
            reference_description(&certifier.description)
        );
        assert!(
            (5..=50).contains(&info.description.len()),
            "BRC-100 bounds the description to 5..=50 bytes"
        );
        assert_eq!(u32::from(info.trust), certifier.trust);
    }
    assert!(
        certifier_info(STRANGER).is_none(),
        "a stranger's certifier has no entry"
    );
}

/// The door: a stored certificate from a default certifier is answered with
/// its `certifierInfo`; one from a stranger is dropped. The overlay leg is
/// refused by the test transport before any request (#182, rule 28), so the
/// local leg alone answers.
#[cfg(feature = "sqlite")]
#[tokio::test]
#[ignore = "#388: the wallet reads no trust settings, so discovery answers no certificates"]
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
    assert_eq!(
        info.description,
        reference_description(&trusted.description)
    );
}
