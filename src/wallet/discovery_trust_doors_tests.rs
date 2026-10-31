//! #388: the discovery doors, their trust and paging, against a resolver.

use super::*;
use crate::managers::{
    WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig, TESTNET_DEFAULT_SETTINGS,
};
use crate::services::{mock::MockWalletServices, Chain};
use crate::storage::{entities::TableCertificate, WalletStorageWriter};
use crate::wallet::lookup::fixture::{published, revealed};
use crate::wallet::{OverlayLookupResolver, Wallet, WalletOptions};
use bsv_rs::{
    primitives::PrivateKey,
    wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs, WalletInterface},
};
use std::sync::{Arc, Mutex};

type TestWallet = Wallet<crate::StorageSqlx, MockWalletServices>;

fn scalar(n: u32) -> PrivateKey {
    PrivateKey::from_hex(&format!("{n:064x}")).unwrap()
}

/// An overlay record by `certifier` of `subject` (the wallet's key, 388,
/// or `STRANGER`'s, 1), its fields encrypted for `anyone`: what a resolver
/// answers, which the door verifies and decrypts.
async fn sealed(
    certifier: u32,
    subject: u32,
    serial: &str,
    fields: &[(&str, &str)],
) -> OverlayCertificate {
    let mut label = [0u8; 32];
    label[..serial.len()].copy_from_slice(serial.as_bytes());
    let record = revealed(&scalar(certifier), &scalar(subject), label, fields).await;
    assert_eq!(record.serial_number, sn(serial));
    record
}

/// The default settings with each certifier's key one this test holds
/// (21 and 22), so its records verify; names, trust and threshold kept.
fn held_defaults() -> WalletSettings {
    let mut settings = DEFAULT_SETTINGS.clone();
    for (certifier, n) in settings
        .trust_settings
        .trusted_certifiers
        .iter_mut()
        .zip(21..)
    {
        certifier.identity_key = scalar(n).public_key().to_hex();
    }
    settings
}

async fn wallet(chain: Chain, manager: Option<Arc<WalletSettingsManager>>) -> TestWallet {
    let key = scalar(388);
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
        // The overlay records' type, so a stored row and its record dedup.
        cert_type: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1; 32]),
        serial_number: sn(serial),
        certifier: certifier.into(),
        subject: wallet.identity_key().into(),
        verifier: None,
        revocation_outpoint: format!("{}.0", "0".repeat(64)),
        signature: SIGNATURE.into(),
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
    ) -> crate::Result<Vec<bsv_rs::overlay::OutputListItem>> {
        assert_eq!(
            certifiers, self.keys,
            "identity lookup receives sorted trusted keys"
        );
        self.identities.lock().unwrap().push(key.into());
        Ok(self.certificates.iter().map(published).collect())
    }
    async fn lookup_by_attributes(
        &self,
        attributes: &IndexMap<String, String>,
        certifiers: &[String],
    ) -> crate::Result<Vec<bsv_rs::overlay::OutputListItem>> {
        assert_eq!(
            certifiers, self.keys,
            "attribute lookup receives sorted trusted keys"
        );
        self.attributes.lock().unwrap().push(attributes.clone());
        Ok(self.certificates.iter().map(published).collect())
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
    let resolver = RecordingResolver::new(
        &original.trust_settings,
        vec![
            sealed(1, 388, "mismatch", &[("name", "Bob")]).await,
            sealed(1, 388, "match", &[("name", "Alice")]).await,
        ],
    );
    (wallet, resolver)
}

#[tokio::test]
async fn discovery_attributes_bind_decrypted_fields_before_paging() {
    let (wallet, resolver) = attribute_witnesses().await;
    let mut request = attribute_args(IndexMap::from([("name".into(), "Alice".into())]));
    request.limit = Some(1);
    let result = wallet
        .discover_attributes_with_resolver(request.clone(), &resolver)
        .await
        .unwrap();
    assert_eq!(result.total_certificates, 1);
    assert_eq!(
        result.certificates[0].certificate.serial_number,
        sn("match")
    );
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
    let fields = [
        ("name", "Malice"),
        ("profilePhoto", "Alice"),
        ("icon", "Alice"),
    ];
    resolver.certificates[0] = sealed(1, 388, "mismatch", &fields).await;
    let request = attribute_args(IndexMap::from([
        ("any".into(), "ALICE".into()),
        ("name".into(), "ignored named constraint".into()),
    ]));
    let result = wallet
        .discover_attributes_with_resolver(request, &resolver)
        .await
        .unwrap();
    assert_eq!(result.total_certificates, 1);
    let kept = &result.certificates[0].certificate;
    assert_eq!(kept.serial_number, sn("match"));
}

#[tokio::test]
async fn discovery_attributes_empty_queries_match_nothing() {
    let original = settings(vec![host_certifier()], 1);
    let wallet = wallet(Chain::Main, Some(manager(original.clone()))).await;
    let resolver = RecordingResolver::new(
        &original.trust_settings,
        vec![sealed(1, 388, "match", &[("name", "Alice")]).await],
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
        let name = if field == "userName" { field } else { "name" };
        let cert = sealed(1, 388, "candidate", &[(name, actual)]).await;
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
        vec![sealed(1, 388, "overlay", &[("name", "Alice")]).await],
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
    let original = held_defaults();
    let manager = manager(original.clone());
    let wallet = wallet(Chain::Main, Some(manager.clone())).await;
    let a = &original.trust_settings.trusted_certifiers[0].identity_key;
    insert(&wallet, a, "local").await;
    let resolver = RecordingResolver::new(
        &original.trust_settings,
        vec![
            sealed(21, 388, "local", &[("name", "Alice")]).await,
            sealed(22, 388, "overlay", &[("name", "Alice")]).await,
            sealed(1, 388, "stranger", &[("name", "Alice")]).await,
            sealed(21, 1, "wrong subject", &[("name", "Alice")]).await,
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
    let mut original = held_defaults();
    original.trust_settings.trust_level = 7;
    let manager = manager(original.clone());
    let wallet = wallet(Chain::Main, Some(manager)).await;
    let a = &original.trust_settings.trusted_certifiers[0].identity_key;
    let b = &original.trust_settings.trusted_certifiers[1].identity_key;
    insert(&wallet, a, "same").await;
    let resolver = RecordingResolver::new(
        &original.trust_settings,
        vec![
            sealed(21, 388, "same", &[("name", "Alice")]).await,
            sealed(21, 388, "two", &[("name", "Alice")]).await,
            sealed(22, 388, "same", &[("name", "Alice")]).await,
            sealed(22, 388, "three", &[("name", "Alice")]).await,
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
                c.certificate.serial_number.clone()
            ))
            .collect::<Vec<_>>(),
        vec![(a.as_str(), sn("two")), (b.as_str(), sn("same"))]
    );
}
