//! #495: the door verifies each overlay record's signature and decrypts its
//! fields with the keyring, as the reference's `decodeIdentityOutput` does
//! (ts-stack@edf6e03 `identityUtils.ts:546-557`, `VerifiableCertificate.ts:150-219`),
//! so attribute discovery matches what the record reveals. Each witness was
//! red before: the door matched the `decrypted_fields` a resolver supplied,
//! which no real overlay entry fills, and verified no signature.

use std::sync::Arc;

use bsv_rs::wallet::{DiscoverByAttributesArgs, DiscoverByIdentityKeyArgs};
use indexmap::IndexMap;

use super::{OverlayCertificate, OverlayLookupResolver, Wallet};
use crate::managers::{
    Certifier, TrustSettings, WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig,
};
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::wallet::lookup::fixture::{beef, key, record_json, revealed, token};
use crate::wallet::{HttpLookupResolver, WalletOptions};

type TestWallet = Wallet<crate::StorageSqlx, MockWalletServices>;

const CERTIFIER: u8 = 9;
const SUBJECT: u8 = 8;

/// A wallet of `SUBJECT`'s key trusting `CERTIFIER` alone, at threshold 1.
async fn wallet() -> TestWallet {
    let certifier = Certifier {
        name: "Fixture certifier".into(),
        description: "The fixture's certifier".into(),
        identity_key: key(CERTIFIER).public_key().to_hex(),
        trust: 5,
        icon_url: None,
        base_url: None,
    };
    let settings = WalletSettings {
        trust_settings: TrustSettings {
            trust_level: 1,
            trusted_certifiers: vec![certifier],
        },
        theme: None,
        currency: None,
        permission_mode: None,
    };
    let manager = WalletSettingsManager::new(Some(WalletSettingsManagerConfig {
        default_settings: settings,
    }))
    .unwrap();
    let storage = crate::StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("discovery-decrypt", &key(SUBJECT).public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let options = WalletOptions {
        settings_manager: Some(Arc::new(manager)),
        ..Default::default()
    };
    Wallet::with_options(
        Some(key(SUBJECT)),
        storage,
        MockWalletServices::new(),
        options,
    )
    .await
    .unwrap()
}

async fn record(serial: u8, name: &str) -> OverlayCertificate {
    revealed(
        &key(CERTIFIER),
        &key(SUBJECT),
        [serial; 32],
        &[("name", name)],
    )
    .await
}

struct Overlay(Vec<OverlayCertificate>);

#[async_trait::async_trait]
impl OverlayLookupResolver for Overlay {
    async fn lookup_by_identity_key(
        &self,
        _: &str,
        _: &[String],
    ) -> crate::Result<Vec<OverlayCertificate>> {
        Ok(self.0.clone())
    }

    async fn lookup_by_attributes(
        &self,
        _: &IndexMap<String, String>,
        _: &[String],
    ) -> crate::Result<Vec<OverlayCertificate>> {
        Ok(self.0.clone())
    }
}

fn by_name(name: &str) -> DiscoverByAttributesArgs {
    DiscoverByAttributesArgs {
        attributes: IndexMap::from([("name".to_string(), name.to_string())]),
        limit: None,
        offset: None,
        seek_permission: None,
    }
}

/// The serials the identity door answers for the wallet's own key.
async fn identity_serials(wallet: &TestWallet, overlay: Vec<OverlayCertificate>) -> Vec<String> {
    let args = DiscoverByIdentityKeyArgs {
        identity_key: wallet.identity_key().into(),
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let answer = wallet
        .discover_identity_with_resolver(args, &Overlay(overlay))
        .await
        .unwrap();
    answer
        .certificates
        .into_iter()
        .map(|c| c.certificate.serial_number)
        .collect()
}

/// #494's finding: the reference's record, served by an overlay on the
/// loopback as an output-list of identity tokens, is found by the attribute
/// it reveals, its decrypted field in the answer.
#[tokio::test]
async fn attribute_discovery_finds_a_reference_record_through_the_overlay() {
    let wallet = wallet().await;
    let alice = record(2, "Alice").await;
    let mut server = mockito::Server::new_async().await;
    let host = server.url();
    let slap = format!(
        "21{}ac{:02x}{}75",
        key(7).public_key().to_hex(),
        host.len(),
        hex::encode(host.as_bytes())
    );
    let slap = bsv_rs::script::LockingScript::from_hex(&slap).unwrap();
    let answer = |script| {
        serde_json::json!({"type": "output-list", "outputs": [{"beef": beef(script), "outputIndex": 0}]})
            .to_string()
    };
    let _slap = server
        .mock("POST", "/lookup")
        .match_body(mockito::Matcher::PartialJson(
            serde_json::json!({"service": "ls_slap"}),
        ))
        .with_body(answer(slap))
        .create_async()
        .await;
    let _identity = server
        .mock("POST", "/lookup")
        .match_body(mockito::Matcher::PartialJson(
            serde_json::json!({"service": "ls_identity"}),
        ))
        .with_body(answer(token(&key(SUBJECT), record_json(&alice))))
        .create_async()
        .await;
    let resolver = HttpLookupResolver::new(&host);
    let found = wallet
        .discover_attributes_with_resolver(by_name("Alice"), &resolver)
        .await
        .unwrap();
    assert_eq!(found.total_certificates, 1);
    let kept = &found.certificates[0];
    assert_eq!(kept.certificate.serial_number, alice.serial_number);
    assert_eq!(kept.decrypted_fields["name"], "Alice");
    assert_eq!(kept.certificate.fields, alice.fields);
}

/// `decrypted_fields` a resolver supplies are never matched or answered: the
/// door's are the decryption's.
#[tokio::test]
async fn a_resolvers_plaintext_is_replaced_by_the_decryption() {
    let wallet = wallet().await;
    let mut bob = record(2, "Bob").await;
    bob.decrypted_fields = IndexMap::from([("name".to_string(), "Alice".to_string())]);
    for (query, expected) in [("Alice", 0), ("Bob", 1)] {
        let found = wallet
            .discover_attributes_with_resolver(by_name(query), &Overlay(vec![bob.clone()]))
            .await
            .unwrap();
        assert_eq!(found.total_certificates, expected, "{query}");
    }
}

/// `verifiableCert.verify()` (`identityUtils.ts:550`): another record's
/// canonical signature over this one's values is dropped.
#[tokio::test]
async fn a_bad_certificate_signature_is_dropped() {
    let wallet = wallet().await;
    let good = record(3, "Alice").await;
    let mut forged = record(2, "Alice").await;
    forged.signature = good.signature.clone();
    assert!(forged.is_reference_record());
    assert_eq!(
        identity_serials(&wallet, vec![forged, good.clone()]).await,
        vec![good.serial_number]
    );
}

/// `decryptFields` (`VerifiableCertificate.ts:150-219`, caught at
/// `identityUtils.ts:555-557`): a keyring value that is canonical base64 but
/// not this field's key for `anyone` is dropped; the signature covers no
/// keyring, so it still verifies.
#[tokio::test]
async fn an_undecryptable_keyring_value_is_dropped() {
    let wallet = wallet().await;
    let good = record(3, "Alice").await;
    let mut locked = record(2, "Alice").await;
    locked.keyring = good.keyring.clone();
    assert!(locked.is_reference_record());
    assert_eq!(
        identity_serials(&wallet, vec![locked, good.clone()]).await,
        vec![good.serial_number]
    );
}
