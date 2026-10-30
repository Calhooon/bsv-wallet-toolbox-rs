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

#[cfg(feature = "sqlite")]
mod doors {
    use super::*;
    use crate::managers::{
        WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig,
        TESTNET_DEFAULT_SETTINGS,
    };
    use crate::services::{mock::MockWalletServices, Chain};
    use crate::storage::{entities::TableCertificate, WalletStorageWriter};
    use crate::wallet::{OverlayLookupResolver, Wallet, WalletOptions};
    use bsv_rs::{
        primitives::PrivateKey,
        wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs, WalletInterface},
    };
    use std::sync::{Arc, Mutex};

    type TestWallet = Wallet<crate::StorageSqlx, MockWalletServices>;

    async fn wallet(chain: Chain, manager: Option<Arc<WalletSettingsManager>>) -> TestWallet {
        let key = PrivateKey::from_hex(&format!("{:064x}", 388)).unwrap();
        let storage = crate::StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("discovery-trust", &key.public_key().to_hex())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        Wallet::with_chain(
            Some(key),
            storage,
            MockWalletServices::new(),
            WalletOptions {
                settings_manager: manager,
                ..Default::default()
            },
            chain,
        )
        .await
        .unwrap()
    }

    async fn insert(wallet: &TestWallet, certifier: &str, serial: &str) {
        let now = chrono::Utc::now();
        let certificate = TableCertificate {
            certificate_id: 0,
            user_id: wallet.auth().user_id.unwrap(),
            cert_type: "dHlwZQ==".into(),
            serial_number: serial.into(),
            certifier: certifier.into(),
            subject: wallet.identity_key().into(),
            verifier: None,
            revocation_outpoint: format!("{}.0", "0".repeat(64)),
            signature: "3044".into(),
            created_at: now,
            updated_at: now,
        };
        wallet
            .storage
            .insert_certificate(&wallet.auth(), certificate)
            .await
            .unwrap();
    }

    fn args(wallet: &TestWallet) -> DiscoverByIdentityKeyArgs {
        DiscoverByIdentityKeyArgs {
            identity_key: wallet.identity_key().into(),
            limit: None,
            offset: None,
            seek_permission: None,
        }
    }

    fn settings(certifiers: Vec<Certifier>, threshold: u32) -> WalletSettings {
        WalletSettings {
            trust_settings: TrustSettings {
                trust_level: threshold,
                trusted_certifiers: certifiers,
            },
            theme: None,
            currency: None,
            permission_mode: None,
        }
    }

    fn manager(settings: WalletSettings) -> Arc<WalletSettingsManager> {
        Arc::new(
            WalletSettingsManager::new(Some(WalletSettingsManagerConfig {
                default_settings: settings,
            }))
            .unwrap(),
        )
    }

    struct RecordingResolver {
        certificates: Vec<OverlayCertificate>,
        keys: Vec<String>,
        identities: Mutex<Vec<String>>,
        attributes: Mutex<Vec<IndexMap<String, String>>>,
    }

    impl RecordingResolver {
        fn new(settings: &TrustSettings, certificates: Vec<OverlayCertificate>) -> Self {
            let keys: std::collections::BTreeSet<_> = settings
                .trusted_certifiers
                .iter()
                .map(|c| c.identity_key.clone())
                .collect();
            let keys = keys.into_iter().collect();
            Self {
                certificates,
                keys,
                identities: Mutex::new(vec![]),
                attributes: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait::async_trait]
    impl OverlayLookupResolver for RecordingResolver {
        async fn lookup_by_identity_key(
            &self,
            key: &str,
            certifiers: &[String],
        ) -> crate::Result<Vec<OverlayCertificate>> {
            assert_eq!(
                certifiers, self.keys,
                "identity lookup receives sorted trusted keys"
            );
            self.identities.lock().unwrap().push(key.into());
            Ok(self.certificates.clone())
        }
        async fn lookup_by_attributes(
            &self,
            attributes: &IndexMap<String, String>,
            certifiers: &[String],
        ) -> crate::Result<Vec<OverlayCertificate>> {
            assert_eq!(
                certifiers, self.keys,
                "attribute lookup receives sorted trusted keys"
            );
            self.attributes.lock().unwrap().push(attributes.clone());
            Ok(self.certificates.clone())
        }
    }

    fn attribute_args(attributes: IndexMap<String, String>) -> DiscoverByAttributesArgs {
        DiscoverByAttributesArgs {
            attributes,
            limit: None,
            offset: None,
            seek_permission: None,
        }
    }

    async fn attribute_witnesses() -> (TestWallet, RecordingResolver) {
        let original = settings(vec![host_certifier()], 1);
        let wallet = wallet(Chain::Main, Some(manager(original.clone()))).await;
        let mut mismatch = certificate(STRANGER, wallet.identity_key(), "mismatch");
        mismatch
            .decrypted_fields
            .insert("name".into(), "Bob".into());
        let resolver = RecordingResolver::new(
            &original.trust_settings,
            vec![
                mismatch,
                certificate(STRANGER, wallet.identity_key(), "match"),
            ],
        );
        (wallet, resolver)
    }

    #[tokio::test]
    async fn discovery_attributes_bind_decrypted_fields_before_paging() {
        let (wallet, mut resolver) = attribute_witnesses().await;
        // An encrypted field carrying the query is not matching evidence.
        resolver.certificates[0]
            .fields
            .insert("name".into(), "Alice".into());
        let mut request = attribute_args(IndexMap::from([("name".into(), "Alice".into())]));
        request.limit = Some(1);
        let result = wallet
            .discover_attributes_with_resolver(request.clone(), &resolver)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 1);
        assert_eq!(result.certificates[0].certificate.serial_number, "match");
        request.offset = Some(1);
        let page = wallet
            .discover_attributes_with_resolver(request, &resolver)
            .await
            .unwrap();
        assert_eq!(page.total_certificates, 1);
        assert!(page.certificates.is_empty());
    }

    #[tokio::test]
    async fn discovery_attributes_any_overrides_named_fields_and_excludes_images() {
        let (wallet, mut resolver) = attribute_witnesses().await;
        resolver.certificates[0].decrypted_fields = IndexMap::from([
            ("name".into(), "Malice".into()),
            ("profilePhoto".into(), "Alice".into()),
            ("icon".into(), "Alice".into()),
        ]);
        let request = attribute_args(IndexMap::from([
            ("any".into(), "ALICE".into()),
            ("name".into(), "ignored named constraint".into()),
        ]));
        let result = wallet
            .discover_attributes_with_resolver(request, &resolver)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 1);
        assert_eq!(result.certificates[0].certificate.serial_number, "match");
    }

    #[tokio::test]
    async fn discovery_attributes_empty_queries_match_nothing() {
        let original = settings(vec![host_certifier()], 1);
        let wallet = wallet(Chain::Main, Some(manager(original.clone()))).await;
        let resolver = RecordingResolver::new(
            &original.trust_settings,
            vec![certificate(STRANGER, wallet.identity_key(), "match")],
        );
        for attributes in [
            IndexMap::new(),
            IndexMap::from([("name".into(), "\u{feff}\t ".into())]),
            IndexMap::from([("any".into(), String::new())]),
            IndexMap::from([("any".into(), "A".into())]),
        ] {
            let result = wallet
                .discover_attributes_with_resolver(attribute_args(attributes.clone()), &resolver)
                .await
                .unwrap();
            assert_eq!(result.total_certificates, 0, "query {attributes:?}");
            assert!(result.certificates.is_empty());
        }
    }

    #[tokio::test]
    async fn discovery_attributes_follow_reference_matching_rules() {
        let original = settings(vec![host_certifier()], 1);
        let wallet = wallet(Chain::Main, Some(manager(original.clone()))).await;
        // identityUtils.ts:142-243 at edf6e03, including legacy /gi casing.
        for (field, actual, query, matches) in [
            ("name", "Alice Wonderland", "\u{feff}aLIce\t wonder ", true),
            ("name", "Wonderland Alice", "Alice wonder", false),
            ("name", "Alice\nWonderland", "Alice wonder", false),
            ("name", "Alice\rWonderland", "Alice wonder", false),
            ("name", "Alice\u{2028}Wonderland", "Alice wonder", false),
            ("name", "Alice.*", ".*", true),
            ("name", "Alice", ".*", false),
            ("name", "ſ", "S", false),
            ("name", "\u{212a}", "K", false),
            ("name", "é", "É", true),
            ("name", "é", "e", false),
            ("missing", "Alice", "Alice", false),
            ("Name", "Alice", "Alice", false),
            ("userName", "Alice Smith", " Alice\tSmith ", true),
            ("userName", "alice", "Alice", false),
            ("userName", "Alice Smith", "Alice", false),
            ("userName", " Alice ", "Alice", false),
            ("any", "Alice Wonderland", "ali", false),
            ("any", "Alice Wonderland", "al", true),
            ("any", "Alice Wonderland", "ALICE", true),
            ("any", "Alice Wonderland", "Alice Bob", true),
            ("any", "Alice Wonderland", "Alice -Wonderland", false),
            ("any", "Alice Wonderland", "Alice -Bob", true),
            ("any", "Alice Wonderland", "-Bob", false),
            ("any", "Alice Wonderland", "the and", false),
            ("any", "Alice Wonderland", "\"Alice Wonderland\"", true),
            ("any", "Alice Wonderland", "\"Wonderland Alice\"", false),
            (
                "any",
                "Alice Wonderland",
                "Alice -\"Alice Wonderland\"",
                false,
            ),
            ("any", "O'Neil", "O'Neil", true),
            ("any", "O'Neil", "Neil", false),
            ("any", "Alice\u{2014}Wonderland", "Wonderland", true),
            ("any", "José", "Jose", true),
            ("any", "Jose\u{301}", "José", true),
            (
                "any",
                "한글",
                "\u{1112}\u{1161}\u{11ab}\u{1100}\u{1173}\u{11af}",
                true,
            ),
            ("any", "Alice\u{85}Wonderland", "Wonderland", true),
            ("name", "Alice", "\u{85}Alice", false),
        ] {
            let mut cert = certificate(STRANGER, wallet.identity_key(), "candidate");
            cert.decrypted_fields = IndexMap::from([("name".into(), actual.into())]);
            if field == "userName" {
                cert.decrypted_fields = IndexMap::from([(field.into(), actual.into())]);
            }
            let resolver = RecordingResolver::new(&original.trust_settings, vec![cert]);
            let result = wallet
                .discover_attributes_with_resolver(
                    attribute_args(IndexMap::from([(field.into(), query.into())])),
                    &resolver,
                )
                .await
                .unwrap();
            assert_eq!(
                result.total_certificates,
                u32::from(matches),
                "{field}: {actual:?} against {query:?}"
            );
        }
    }

    #[tokio::test]
    async fn discovery_identity_keeps_both_attribute_witnesses() {
        let (wallet, resolver) = attribute_witnesses().await;
        let result = wallet
            .discover_identity_with_resolver(args(&wallet), &resolver)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 2);
        assert_eq!(result.certificates.len(), 2);
    }

    #[tokio::test]
    async fn discovery_keeps_a_default_certifiers_certificate_and_drops_a_strangers() {
        let wallet = wallet(Chain::Main, None).await;
        let trusted = &DEFAULT_SETTINGS.trust_settings.trusted_certifiers[0];
        insert(&wallet, &trusted.identity_key, "trusted").await;
        insert(&wallet, STRANGER, "stranger").await;
        let result = WalletInterface::discover_by_identity_key(&wallet, args(&wallet), None)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 1);
        assert_eq!(
            result.certificates[0].certificate.certifier,
            trusted.identity_key
        );
        assert_eq!(result.certificates[0].certifier_info.name, trusted.name);
        assert_eq!(
            result.certificates[0].certifier_info.description,
            trusted.description
        );
    }

    #[tokio::test]
    async fn discovery_uses_chain_defaults_without_a_manager() {
        for (chain, defaults) in [
            (Chain::Main, &*DEFAULT_SETTINGS),
            (Chain::Test, &*TESTNET_DEFAULT_SETTINGS),
            (Chain::Regtest, &*TESTNET_DEFAULT_SETTINGS),
        ] {
            let wallet = wallet(chain, None).await;
            for (i, certifier) in defaults
                .trust_settings
                .trusted_certifiers
                .iter()
                .enumerate()
            {
                insert(&wallet, &certifier.identity_key, &i.to_string()).await;
            }
            let result = WalletInterface::discover_by_identity_key(&wallet, args(&wallet), None)
                .await
                .unwrap();
            assert_eq!(
                result.total_certificates, 2,
                "defaults discover on {chain:?}"
            );
            assert_eq!(result.certificates[0].certifier_info.trust, 4);
        }
    }

    #[tokio::test]
    async fn discovery_keeps_a_stranger_after_the_host_sets_trust() {
        let manager = Arc::new(WalletSettingsManager::new(None).unwrap());
        let wallet = wallet(Chain::Main, Some(manager.clone())).await;
        insert(&wallet, STRANGER, "host").await;
        insert(
            &wallet,
            &DEFAULT_SETTINGS.trust_settings.trusted_certifiers[0].identity_key,
            "default",
        )
        .await;
        manager
            .set(settings(vec![host_certifier()], 2))
            .await
            .unwrap();
        let result = WalletInterface::discover_by_identity_key(&wallet, args(&wallet), None)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 1);
        assert_eq!(result.certificates[0].certificate.certifier, STRANGER);
        assert_eq!(result.certificates[0].certifier_info.name, "Host certifier");
    }

    #[tokio::test]
    async fn discovery_reads_a_raised_threshold_on_the_next_call() {
        let original = settings(vec![host_certifier()], 2);
        let manager = manager(original.clone());
        let wallet = wallet(Chain::Main, Some(manager.clone())).await;
        insert(&wallet, STRANGER, "host").await;
        let resolver = RecordingResolver::new(&original.trust_settings, vec![]);
        let before = wallet
            .discover_identity_with_resolver(args(&wallet), &resolver)
            .await
            .unwrap();
        assert_eq!(before.total_certificates, 1);
        let mut raised = original;
        raised.trust_settings.trust_level = 3;
        manager.set(raised).await.unwrap();
        let after = wallet
            .discover_identity_with_resolver(args(&wallet), &resolver)
            .await
            .unwrap();
        assert_eq!(
            after.total_certificates, 0,
            "the next call reads the raised threshold"
        );
        assert_eq!(resolver.identities.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn discovery_reads_removed_certifiers_on_the_next_call() {
        let original = settings(vec![host_certifier()], 2);
        let manager = manager(original.clone());
        let wallet = wallet(Chain::Main, Some(manager.clone())).await;
        insert(&wallet, STRANGER, "host").await;
        manager.set(settings(vec![], 2)).await.unwrap();
        let resolver = RecordingResolver::new(
            &manager.get().await.unwrap().trust_settings,
            vec![certificate(STRANGER, wallet.identity_key(), "overlay")],
        );
        let after_removal = wallet
            .discover_identity_with_resolver(args(&wallet), &resolver)
            .await
            .unwrap();
        assert_eq!(after_removal.total_certificates, 0);
        assert!(after_removal.certificates.is_empty());
    }

    #[tokio::test]
    async fn discovery_identity_and_attributes_share_sorted_keys_and_trust() {
        let original = DEFAULT_SETTINGS.clone();
        let manager = manager(original.clone());
        let wallet = wallet(Chain::Main, Some(manager.clone())).await;
        let a = &original.trust_settings.trusted_certifiers[0].identity_key;
        let b = &original.trust_settings.trusted_certifiers[1].identity_key;
        insert(&wallet, a, "local").await;
        let resolver = RecordingResolver::new(
            &original.trust_settings,
            vec![
                certificate(a, wallet.identity_key(), "local"),
                certificate(b, wallet.identity_key(), "overlay"),
                certificate(STRANGER, wallet.identity_key(), "stranger"),
                certificate(a, STRANGER, "wrong subject"),
            ],
        );
        let identity = wallet
            .discover_identity_with_resolver(args(&wallet), &resolver)
            .await
            .unwrap();
        assert_eq!(identity.total_certificates, 2);
        let attributes = IndexMap::from([("name".into(), "Alice".into())]);
        let attribute_args = DiscoverByAttributesArgs {
            attributes: attributes.clone(),
            limit: Some(1),
            offset: Some(1),
            seek_permission: None,
        };
        let by_attributes = wallet
            .discover_attributes_with_resolver(attribute_args.clone(), &resolver)
            .await
            .unwrap();
        assert_eq!(by_attributes.total_certificates, 3);
        assert_eq!(by_attributes.certificates.len(), 1);
        assert_eq!(by_attributes.certificates[0].certificate.subject, STRANGER);
        assert_eq!(*resolver.attributes.lock().unwrap(), vec![attributes]);
        let mut raised = original;
        raised.trust_settings.trust_level = 8;
        manager.set(raised).await.unwrap();
        assert_eq!(
            wallet
                .discover_attributes_with_resolver(attribute_args, &resolver)
                .await
                .unwrap()
                .total_certificates,
            0
        );
    }

    #[tokio::test]
    async fn discovery_pages_after_merging_and_deduplicating_both_legs() {
        let mut original = DEFAULT_SETTINGS.clone();
        original.trust_settings.trust_level = 7;
        let manager = manager(original.clone());
        let wallet = wallet(Chain::Main, Some(manager)).await;
        let a = &original.trust_settings.trusted_certifiers[0].identity_key;
        let b = &original.trust_settings.trusted_certifiers[1].identity_key;
        insert(&wallet, a, "same").await;
        let resolver = RecordingResolver::new(
            &original.trust_settings,
            vec![
                certificate(a, wallet.identity_key(), "same"),
                certificate(a, wallet.identity_key(), "two"),
                certificate(b, wallet.identity_key(), "same"),
                certificate(b, wallet.identity_key(), "three"),
            ],
        );
        let mut request = args(&wallet);
        request.limit = Some(2);
        request.offset = Some(1);
        let result = wallet
            .discover_identity_with_resolver(request, &resolver)
            .await
            .unwrap();
        assert_eq!(result.total_certificates, 4);
        assert_eq!(
            result
                .certificates
                .iter()
                .map(|c| (
                    c.certificate.certifier.as_str(),
                    c.certificate.serial_number.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![(a.as_str(), "two"), (b.as_str(), "same")]
        );
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "discovery_rows_tests.rs"]
mod discovery_rows_tests;
