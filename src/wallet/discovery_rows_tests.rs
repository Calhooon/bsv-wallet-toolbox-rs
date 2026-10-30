//! #488 surface 7 (bsv-rs #495): the wallet's two discovery answers read
//! back by bsv-rs's own JSON client, `HttpWalletJson`, which since beta.4
//! applies the reference client's nine refusing discovery rows
//! (`WalletWireTransceiver.ts:1796,1842` at ts-stack@edf6e03; bsv-rs
//! `src/wallet/substrates/result_values/certificates.rs` at v0.9.0-beta.4).
//! The answer is the door's (`discover_*_with_resolver`), served unchanged by
//! a loopback mock as a host's JSON daemon would serve it. A malformed
//! overlay entry is dropped from the answer, as the reference drops it.

use super::*;
use crate::managers::{
    Certifier, TrustSettings, WalletSettings, WalletSettingsManager, WalletSettingsManagerConfig,
};
use crate::services::mock::MockWalletServices;
use crate::storage::entities::TableCertificate;
use crate::storage::WalletStorageWriter;
use crate::wallet::lookup::{OverlayCertificate, OverlayLookupResolver};
use bsv_rs::auth::MasterCertificate;
use bsv_rs::wallet::substrates::HttpWalletJson;

type TestWallet = Wallet<crate::StorageSqlx, MockWalletServices>;

const CERTIFIER: u8 = 9;
const SUBJECT: u8 = 8;

fn key(n: u8) -> PrivateKey {
    PrivateKey::from_bytes(&[n; 32]).unwrap()
}

fn certifier(icon_url: Option<String>) -> Certifier {
    Certifier {
        // 100 bytes, R79's and the settings' upper bound
        name: "n".repeat(100),
        // 500 bytes: the settings take 1-500, the door cuts to 5-50
        description: "d".repeat(500),
        identity_key: key(CERTIFIER).public_key().to_hex(),
        trust: 5,
        icon_url,
        base_url: None,
    }
}

async fn wallet(icon_url: Option<String>) -> TestWallet {
    let settings = WalletSettings {
        trust_settings: TrustSettings {
            trust_level: 1,
            trusted_certifiers: vec![certifier(icon_url)],
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
        .migrate("discovery-rows", &key(SUBJECT).public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    Wallet::with_options(
        Some(key(SUBJECT)),
        storage,
        MockWalletServices::new(),
        WalletOptions {
            settings_manager: Some(Arc::new(manager)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// A certificate of the wallet's identity by the trusted certifier, signed,
/// its `name` field encrypted, as an overlay carries it.
async fn overlay_certificate(serial: u8) -> OverlayCertificate {
    let master = MasterCertificate::issue_for_subject(
        &ProtoWallet::new(Some(key(CERTIFIER))),
        &key(CERTIFIER),
        key(SUBJECT).public_key(),
        [("name".to_string(), "Alice".to_string())].into(),
        [1; 32],
        Some([serial; 32]),
        Some("rows.test"),
    )
    .await
    .unwrap();
    // Signed again over a revocation outpoint, which issuance leaves unset.
    let mut signed = master.certificate;
    signed.set_revocation_outpoint(Some(bsv_rs::wallet::Outpoint::new([0xaa; 32], 0)));
    signed.set_signature(None);
    signed.sign(&key(CERTIFIER)).unwrap();
    assert!(signed.verify().unwrap());
    let certificate = signed.to_wallet_certificate();
    OverlayCertificate {
        type_id: certificate.certificate_type,
        serial_number: certificate.serial_number,
        subject: certificate.subject.to_hex(),
        certifier: certificate.certifier.to_hex(),
        revocation_outpoint: certificate.revocation_outpoint.map(|o| o.to_string()),
        signature: certificate.signature.map(hex::encode),
        fields: certificate.fields,
        keyring: master.master_keyring,
        decrypted_fields: [("name".to_string(), "Alice".to_string())].into(),
    }
}

struct Overlay(Vec<OverlayCertificate>);

#[async_trait]
impl OverlayLookupResolver for Overlay {
    async fn lookup_by_identity_key(
        &self,
        _identity_key: &str,
        _certifiers: &[String],
    ) -> crate::Result<Vec<OverlayCertificate>> {
        Ok(self.0.clone())
    }

    async fn lookup_by_attributes(
        &self,
        _attributes: &IndexMap<String, String>,
        _certifiers: &[String],
    ) -> crate::Result<Vec<OverlayCertificate>> {
        Ok(self.0.clone())
    }
}

/// `answer` served on `call` and read by bsv-rs's JSON client.
async fn served(call: &str, answer: &DiscoverCertificatesResult) -> mockito::ServerGuard {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", format!("/{call}").as_str())
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_vec(answer).unwrap())
        .create_async()
        .await;
    server
}

fn client(server: &mockito::ServerGuard) -> HttpWalletJson {
    HttpWalletJson::new(Some("rows.test".into()), Some(server.url())).unwrap()
}

async fn identity_read(
    wallet: &TestWallet,
    overlay: Vec<OverlayCertificate>,
    requested: String,
) -> (
    DiscoverCertificatesResult,
    bsv_rs::Result<DiscoverCertificatesResult>,
) {
    let args = DiscoverByIdentityKeyArgs {
        identity_key: requested,
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let answer = wallet
        .discover_identity_with_resolver(args.clone(), &Overlay(overlay))
        .await
        .unwrap();
    let server = served("discoverByIdentityKey", &answer).await;
    let read = client(&server).discover_by_identity_key(args, None).await;
    (answer, read)
}

async fn attributes_read(
    wallet: &TestWallet,
    overlay: Vec<OverlayCertificate>,
    attributes: &[(&str, &str)],
) -> (
    DiscoverCertificatesResult,
    bsv_rs::Result<DiscoverCertificatesResult>,
) {
    let args = DiscoverByAttributesArgs {
        attributes: attributes
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let answer = wallet
        .discover_attributes_with_resolver(args.clone(), &Overlay(overlay))
        .await
        .unwrap();
    let server = served("discoverByAttributes", &answer).await;
    let read = client(&server).discover_by_attributes(args, None).await;
    (answer, read)
}

fn json(result: &DiscoverCertificatesResult) -> serde_json::Value {
    serde_json::to_value(result).unwrap()
}

/// R03, R74, R76, R79 (name, icon URL, description) and R80 on an answer
/// with a stored certificate and an overlay one: the settings' 100-byte name
/// passes, the 500-byte description is cut to 50 and the absent icon is the
/// reference's fallback, the subject is bound case-insensitively as the row
/// binds it (`scalars.rs` `same`).
#[tokio::test]
async fn the_identity_answer_passes_the_rows() {
    let wallet = wallet(None).await;
    let stored = overlay_certificate(3).await;
    let now = chrono::Utc::now();
    wallet
        .storage
        .insert_certificate(
            &wallet.auth(),
            TableCertificate {
                certificate_id: 0,
                user_id: wallet.auth().user_id.unwrap(),
                cert_type: stored.type_id.clone(),
                serial_number: stored.serial_number.clone(),
                certifier: stored.certifier.clone(),
                subject: stored.subject.clone(),
                verifier: None,
                revocation_outpoint: stored.revocation_outpoint.clone().unwrap(),
                signature: stored.signature.clone().unwrap(),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();
    let requested = wallet.identity_key().to_uppercase();
    let (answer, read) =
        identity_read(&wallet, vec![overlay_certificate(2).await], requested).await;
    assert_eq!(answer.total_certificates, 2);
    let info = &answer.certificates[0].certifier_info;
    assert_eq!(info.description, "d".repeat(50));
    assert_eq!(info.icon_url, "https://bsvblockchain.org/favicon.ico");
    assert_eq!(json(&read.unwrap()), json(&answer));
}

/// R81 and R82: the door keeps a certificate only when its decrypted fields
/// satisfy the query (`filter_certificates_by_attributes`, the reference's
/// `identityUtils.ts:283-293`), so each kept answer passes bsv-rs's binding.
#[tokio::test]
async fn the_attribute_answers_pass_the_rows() {
    let wallet = wallet(None).await;
    let mut user = overlay_certificate(4).await;
    user.decrypted_fields
        .insert("userName".into(), "alice.smith".into());
    for attributes in [
        &[("any", "Alice")][..],
        &[("any", "Al")][..],
        &[("name", "Alice")][..],
        &[("name", "alice"), ("profilePhoto", "")][..],
        &[("userName", "  alice.smith ")][..],
    ] {
        let overlay = vec![overlay_certificate(2).await, user.clone()];
        let (answer, read) = attributes_read(&wallet, overlay, attributes).await;
        assert!(answer.total_certificates > 0, "{attributes:?}");
        assert_eq!(json(&read.unwrap()), json(&answer), "{attributes:?}");
    }
}

/// `answer` holds the well-formed overlay certificate alone, and bsv-rs's
/// client reads it back unchanged.
async fn only_the_good_entry(wallet: &TestWallet, bad: OverlayCertificate) {
    let good = overlay_certificate(3).await;
    let overlay = vec![bad, good.clone()];
    let (answer, read) = identity_read(wallet, overlay, wallet.identity_key().into()).await;
    assert_eq!(answer.total_certificates, 1);
    assert_eq!(
        answer.certificates[0].certificate.serial_number,
        good.serial_number
    );
    assert_eq!(json(&read.unwrap()), json(&answer));
}

/// R74: an overlay certificate with no signature is dropped, as the
/// reference drops it (`certificateRecord` throws on the absent property,
/// `decodeIdentityOutput` answers `null`, ts-stack@edf6e03
/// `identityUtils.ts:105,549-552`); before the fix the door filled `""` and
/// bsv-rs's client refused the whole answer.
#[tokio::test]
async fn an_unsigned_overlay_certificate_is_dropped() {
    let wallet = wallet(None).await;
    let mut unsigned = overlay_certificate(2).await;
    unsigned.signature = None;
    only_the_good_entry(&wallet, unsigned).await;
}

/// R80: an overlay keyring value that is empty is dropped, as the reference
/// drops it (`decryptFields`' canonical base64 of 1 to 2048 bytes,
/// `VerifiableCertificate.ts:48`, caught at `identityUtils.ts:549-552`);
/// before the fix it reached the answer and the client refused it.
#[tokio::test]
async fn an_empty_overlay_keyring_value_is_dropped() {
    let wallet = wallet(None).await;
    let mut certificate = overlay_certificate(2).await;
    certificate.keyring.insert("name".into(), String::new());
    only_the_good_entry(&wallet, certificate).await;
}

/// Each of the reference's other drops (`certificateRecord`, :87-129, and
/// the keyring value's base64): the door's answer never carries the entry,
/// by identity key or by attributes.
#[tokio::test]
async fn the_reference_drops_are_dropped_from_both_answers() {
    let wallet = wallet(None).await;
    let good = overlay_certificate(2).await;
    let mut cases: Vec<(&str, OverlayCertificate)> = Vec::new();
    let mut c = good.clone();
    c.revocation_outpoint = None;
    cases.push(("no revocation outpoint", c));
    let mut c = good.clone();
    c.fields.clear();
    c.keyring.clear();
    cases.push(("no fields", c));
    let mut c = good.clone();
    c.keyring.clear();
    cases.push(("no keyring", c));
    let mut c = good.clone();
    for n in 0..100 {
        c.fields.insert(format!("f{n}"), "AA==".into());
    }
    cases.push(("101 fields", c));
    let mut c = good.clone();
    c.keyring.insert("absent".into(), "AA==".into());
    cases.push(("a keyring name with no field", c));
    let mut c = good.clone();
    c.keyring.insert("name".into(), "A".repeat(2052));
    cases.push(("a keyring value over 2048", c));
    let mut c = good.clone();
    c.keyring.insert("name".into(), "not base64".into());
    cases.push(("a keyring value not base64", c));
    for (case, bad) in cases {
        assert!(!bad.is_reference_record(), "{case}");
        let (answer, read) =
            identity_read(&wallet, vec![bad.clone()], wallet.identity_key().into()).await;
        assert_eq!(answer.total_certificates, 0, "{case}");
        assert_eq!(json(&read.unwrap()), json(&answer), "{case}");
        let (answer, _) = attributes_read(&wallet, vec![bad], &[("any", "Alice")]).await;
        assert_eq!(answer.total_certificates, 0, "{case}");
    }
    assert!(good.is_reference_record());
}

/// R79 (icon URL): the settings bound the icon URL at 1-500 bytes and an
/// absolute HTTP(S) URL, R79's own bounds, so a 500-byte setting reaches the
/// client and passes. A setting whose WHATWG serialization passes 500 bytes
/// (a percent-encoded path) never reaches it: the manager's `get` re-reads
/// the serialized value and the door refuses before any answer.
#[tokio::test]
async fn the_icon_url_row_is_bounded_by_the_settings() {
    let icon = format!("https://example.com/{}", "i".repeat(480));
    assert_eq!(icon.len(), 500);
    let wallet = wallet(Some(icon.clone())).await;
    let overlay = vec![overlay_certificate(2).await];
    let (answer, read) = identity_read(&wallet, overlay, wallet.identity_key().into()).await;
    assert_eq!(answer.certificates[0].certifier_info.icon_url, icon);
    assert_eq!(json(&read.unwrap()), json(&answer));

    let wallet = wallet_with_icon(format!("https://example.com/{}", "é".repeat(240))).await;
    let args = DiscoverByIdentityKeyArgs {
        identity_key: wallet.identity_key().into(),
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let error = wallet
        .discover_identity_with_resolver(args, &Overlay(vec![]))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Validation error: Invalid wallet settings: trustedCertifiers[0].iconUrl must contain 1-500 UTF-8 bytes"
    );
}

async fn wallet_with_icon(icon: String) -> TestWallet {
    assert_eq!(icon.len(), 500);
    wallet(Some(icon)).await
}

/// A value present but one `Certificate.verify()` refuses before it verifies
/// (`toBinary(false)` and the signature's parse, ts-sdk `Certificate.ts:233,
/// 260-270,289,434` at ts-stack@edf6e03, caught at :436-439), so
/// `decodeIdentityOutput` drops the entry (`identityUtils.ts:550,555-557`);
/// before the fix each reached the answer and the client refused it whole.
#[tokio::test]
async fn a_malformed_overlay_value_is_dropped() {
    let wallet = wallet(None).await;
    let good = overlay_certificate(3).await;
    let base = overlay_certificate(2).await;
    let mut cases: Vec<(&str, OverlayCertificate)> = Vec::new();
    let mut c = base.clone();
    c.signature = Some(String::new());
    cases.push(("an empty signature", c));
    let mut c = base.clone();
    c.revocation_outpoint = Some(String::new());
    cases.push(("an empty revocation outpoint", c));
    let mut c = base.clone();
    c.type_id = "bad".into();
    cases.push(("a type not base64 of 32 bytes", c));
    let mut c = base.clone();
    let value = c.fields.shift_remove("name").unwrap();
    let key = c.keyring.shift_remove("name").unwrap();
    c.fields.insert(String::new(), value);
    c.keyring.insert(String::new(), key);
    cases.push(("an empty field name", c));
    let mut reached = Vec::new();
    for (case, bad) in cases {
        let (answer, read) = identity_read(
            &wallet,
            vec![bad.clone(), good.clone()],
            wallet.identity_key().into(),
        )
        .await;
        if bad.is_reference_record() || answer.total_certificates != 1 || read.is_err() {
            reached.push(case);
        }
    }
    assert!(reached.is_empty(), "reached the answer: {reached:?}");
}

/// The divergence row in `docs/ARCHITECTURE.md` section 7: `Signature.fromDER`
/// (ts-sdk `Signature.ts:44-97` at ts-stack@edf6e03) reads an integer whose
/// high bit is set without its zero pad, so the reference's decode keeps the
/// entry, while its own client row wants canonical DER
/// (`WalletResultValidation.ts:1529-1531`, `isCanonicalDERSignature`,
/// `Secp256k1Validation.ts:33-51`) and refuses the answer. The toolbox drops
/// it: the client's canon is what reaches a host.
#[tokio::test]
async fn a_signature_without_its_zero_pad_is_dropped() {
    let wallet = wallet(None).await;
    let mut bad = None;
    for serial in 2..40 {
        let mut c = overlay_certificate(serial).await;
        let der = hex::decode(c.signature.as_ref().unwrap()).unwrap();
        if let Some(unpadded) = without_a_zero_pad(&der) {
            assert!(bsv_rs::primitives::ec::Signature::from_der(&unpadded).is_ok());
            c.signature = Some(hex::encode(unpadded));
            bad = Some(c);
            break;
        }
    }
    let bad = bad.expect("a signature with a padded integer");
    let (answer, read) = identity_read(
        &wallet,
        vec![bad.clone(), overlay_certificate(1).await],
        wallet.identity_key().into(),
    )
    .await;
    assert!(read.is_ok(), "the client refused the answer");
    assert_eq!(answer.total_certificates, 1);
    assert!(!bad.is_reference_record());
}

/// The DER signature with the zero pad of r (or else of s) removed and the
/// lengths adjusted, or `None` when neither integer is padded.
fn without_a_zero_pad(der: &[u8]) -> Option<Vec<u8>> {
    let r_len = usize::from(der[3]);
    let s_at = 4 + r_len;
    let (at, len) = if der[4] == 0 {
        (3, r_len)
    } else if der[s_at + 2] == 0 {
        (s_at + 1, usize::from(der[s_at + 1]))
    } else {
        return None;
    };
    let mut out = der.to_vec();
    out.remove(at + 1);
    out[at] = u8::try_from(len - 1).ok()?;
    out[1] -= 1;
    Some(out)
}
