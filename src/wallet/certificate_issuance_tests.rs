//! #203: exercise the issuance/intake/store/prove paths against bsv-rs #261.
//! The pinned SDK's certificate decrypt helper uses the old scheme, so the
//! oracle below implements the reference's two-stage decrypt directly.

use super::*;
use bsv_rs::primitives::{PrivateKey, Signature, SymmetricKey};
use bsv_rs::wallet::{
    AcquisitionProtocol, CreateHmacArgs, CreateSignatureArgs, DecryptArgs, KeyDeriver, ProtoWallet,
    ProveCertificateArgs,
};
use std::collections::HashSet;

use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::{StorageSqlx, Wallet};

const ORIGINATOR: &str = "certificate.test";
const FIXTURE: &str = include_str!(
    "../../conformance/upstream/261-certificate-field-value/certificate_field_value_261.json"
);

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

#[derive(Deserialize)]
struct Fixture {
    reference_issued: Issued,
    rust_issued: Issued,
}

#[derive(Deserialize)]
struct Keys {
    certifier: u32,
    subject: u32,
    verifier: u32,
}

#[derive(Deserialize)]
struct Issued {
    keys: Keys,
    plaintexts: IndexMap<String, String>,
    certificate: CertificateResponse,
    master_keyring: IndexMap<String, String>,
    verifier_keyring: IndexMap<String, String>,
    #[serde(default)]
    anyone_keyring: IndexMap<String, String>,
    binary_hex: String,
    #[serde(default)]
    preimage_sha256: Option<String>,
    reference_verify: bool,
    reference_master_plaintexts: IndexMap<String, String>,
    reference_verifier_plaintexts: IndexMap<String, String>,
    #[serde(default)]
    reference_anyone_plaintexts: IndexMap<String, String>,
}

fn key(value: u32) -> PrivateKey {
    PrivateKey::from_hex(&format!("{value:064x}")).unwrap()
}

fn response(certificate: CertificateResponse) -> ProtocolIssuanceResponse {
    ProtocolIssuanceResponse {
        protocol: "certificate issuance".to_string(),
        certificate: Some(certificate),
        server_nonce: BASE64.encode([5; 32]),
        timestamp: String::new(),
        version: "1.0".to_string(),
    }
}

async fn wallet(subject: u32) -> (TestWallet, AuthId) {
    let identity_key = key(subject).public_key().to_hex();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("certificate-scheme", &identity_key)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let wallet = Wallet::new(Some(key(subject)), storage, MockWalletServices::new())
        .await
        .unwrap();
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity_key)
        .await
        .unwrap();
    (wallet, AuthId::with_user_id(identity_key, user.user_id))
}

// Certificate.verify at ts-stack@edf6e03 derives the signature key with the
// anyone key, the reference rule this oracle keeps. bsv-rs uses it too since
// 0.9.0-beta.2 (#251); the door's own check is `issued_certificate_verifies`.
fn verify_like_reference(certificate: &Certificate) -> bool {
    let signer = KeyDeriver::new(None)
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "certificate signature"),
            &format!(
                "{} {}",
                certificate.type_base64(),
                certificate.serial_number_base64()
            ),
            &Counterparty::Other(certificate.certifier.clone()),
            false,
        )
        .unwrap();
    signer.verify(
        &certificate.signing_hash().expect("signing hash"),
        &Signature::from_der(certificate.signature.as_ref().unwrap()).unwrap(),
    )
}

// MasterCertificate.decryptField and VerifiableCertificate.decryptFields at
// ts-stack@edf6e03. A master key ID is the field name; a verifier key ID is
// "<serialNumber> <fieldName>". Both keyrings carry encrypted AES keys.
fn decrypt_like_reference(
    reader: &ProtoWallet,
    counterparty: &PublicKey,
    certificate: &CertificateResponse,
    keyring: &IndexMap<String, String>,
    master: bool,
) -> IndexMap<String, String> {
    let mut plaintexts = IndexMap::new();
    let mut names: Vec<_> = keyring.keys().collect();
    names.sort();
    for name in names {
        let key_id = if master {
            name.clone()
        } else {
            format!("{} {name}", certificate.serial_number)
        };
        let field_key = reader
            .decrypt(DecryptArgs {
                ciphertext: BASE64.decode(&keyring[name]).unwrap(),
                protocol_id: Protocol::new(
                    SecurityLevel::Counterparty,
                    "certificate field encryption",
                ),
                key_id,
                counterparty: Some(Counterparty::Other(counterparty.clone())),
                ..Default::default()
            })
            .unwrap()
            .plaintext;
        assert_eq!(
            field_key.len(),
            32,
            "{name}: the keyring must carry an AES-256 key"
        );
        let ciphertext = BASE64.decode(&certificate.fields[name]).unwrap();
        let plaintext = SymmetricKey::from_bytes(&field_key)
            .unwrap()
            .decrypt(&ciphertext)
            .unwrap();
        plaintexts.insert(name.clone(), String::from_utf8(plaintext).unwrap());
    }
    plaintexts
}

async fn store(
    wallet: &TestWallet,
    auth: &AuthId,
    parsed: &ParseCertificateResponseResult,
    master_keyring: &IndexMap<String, String>,
) -> WalletCertificate {
    store_certificate(
        wallet.storage(),
        auth,
        parsed,
        master_keyring,
        &PublicKey::from_hex(&parsed.certificate.certifier).unwrap(),
        &auth.identity_key,
        &parsed.certificate.cert_type,
    )
    .await
    .unwrap()
}

async fn prove(
    wallet: &TestWallet,
    certificate: WalletCertificate,
    verifier: u32,
    fields_to_reveal: Vec<String>,
) -> IndexMap<String, String> {
    wallet
        .prove_certificate(
            ProveCertificateArgs {
                certificate,
                fields_to_reveal,
                verifier: key(verifier).public_key().to_hex(),
                privileged: None,
                privileged_reason: None,
            },
            Some(ORIGINATOR),
        )
        .await
        .unwrap()
        .keyring_for_verifier
}

fn acquisition_args(issued: &Issued) -> AcquireCertificateArgs {
    AcquireCertificateArgs {
        certificate_type: issued.certificate.cert_type.clone(),
        certifier: issued.certificate.certifier.clone(),
        acquisition_protocol: AcquisitionProtocol::Issuance,
        fields: issued.plaintexts.clone(),
        serial_number: None,
        revocation_outpoint: None,
        signature: None,
        certifier_url: None,
        keyring_revealer: None,
        keyring_for_subject: None,
        privileged: Some(false),
        privileged_reason: None,
    }
}

// Simulate the reference's certifier locally, including the BRC-104 nonce
// HMAC and the BRC-52 signature. No HTTP request or payment is involved.
fn certifier_response(
    issued: &Issued,
    prepared: &PrepareIssuanceActionDataResult,
) -> ProtocolIssuanceResponse {
    let certifier = ProtoWallet::new(Some(key(issued.keys.certifier)));
    let server_nonce = [5; 32];
    let mut nonce_data = BASE64.decode(&prepared.client_nonce).unwrap();
    nonce_data.extend_from_slice(&server_nonce);
    let serial = certifier
        .create_hmac(CreateHmacArgs {
            data: nonce_data,
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "certificate issuance"),
            key_id: format!("{}{}", BASE64.encode(server_nonce), prepared.client_nonce),
            counterparty: Some(Counterparty::Other(key(issued.keys.subject).public_key())),
            ..Default::default()
        })
        .unwrap()
        .hmac;
    let mut certificate = Certificate::new(
        BASE64
            .decode(&prepared.cert_type_b64)
            .unwrap()
            .try_into()
            .unwrap(),
        serial,
        key(issued.keys.subject).public_key(),
        key(issued.keys.certifier).public_key(),
    );
    for (name, value) in &prepared.fields {
        certificate.set_field(name.clone(), value.clone());
    }
    let signature = certifier
        .create_signature(CreateSignatureArgs {
            data: Some(certificate.to_binary(false).expect("certificate bytes")),
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "certificate signature"),
            key_id: format!("{} {}", prepared.cert_type_b64, BASE64.encode(serial)),
            counterparty: Some(Counterparty::Anyone),
            ..Default::default()
        })
        .unwrap()
        .signature;
    let mut cert_response = issued.certificate.clone();
    cert_response.fields = prepared.fields.clone();
    cert_response.serial_number = BASE64.encode(serial);
    cert_response.signature = hex::encode(signature);
    response(cert_response)
}

fn parse_issued(issued: &Issued) -> ParseCertificateResponseResult {
    parse_certificate_response(&response(issued.certificate.clone())).unwrap()
}

#[test]
fn reference_issued_certificate_survives_intake() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    for issued in [fixture.reference_issued, fixture.rust_issued] {
        let parsed = parse_issued(&issued);
        assert_eq!(
            verify_like_reference(&decode_issued_certificate(&parsed.certificate).unwrap()),
            issued.reference_verify,
            "intake must preserve the reference's signed Base64 field text"
        );
        // #188: the door's own check (bsv-rs's verify, BRC-52's derived key)
        // answers the reference's verdict on both halves of the fixture.
        assert_eq!(
            issued_certificate_verifies(&parsed.certificate),
            issued.reference_verify
        );
        assert_eq!(
            hex::encode(
                decode_issued_certificate(&parsed.certificate)
                    .unwrap()
                    .to_binary(true)
                    .expect("certificate bytes")
            ),
            issued.binary_hex
        );
        if let Some(hash) = issued.preimage_sha256 {
            assert_eq!(
                hex::encode(
                    decode_issued_certificate(&parsed.certificate)
                        .unwrap()
                        .signing_hash()
                        .expect("signing hash")
                ),
                hash
            );
        }
    }
}

#[tokio::test]
async fn reference_issued_certificate_proves_and_decrypts() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    for issued in [fixture.reference_issued, fixture.rust_issued] {
        let parsed = parse_issued(&issued);
        assert!(verify_like_reference(
            &decode_issued_certificate(&parsed.certificate).unwrap()
        ));
        let (wallet, auth) = wallet(issued.keys.subject).await;
        let stored = store(&wallet, &auth, &parsed, &issued.master_keyring).await;
        assert_eq!(stored.fields, issued.certificate.fields);
        let keyring = prove(
            &wallet,
            stored.clone(),
            issued.keys.verifier,
            issued.certificate.fields.keys().cloned().collect(),
        )
        .await;
        let verifier = ProtoWallet::new(Some(key(issued.keys.verifier)));
        let subject = key(issued.keys.subject).public_key();
        assert_eq!(
            decrypt_like_reference(&verifier, &subject, &issued.certificate, &keyring, false),
            issued.reference_verifier_plaintexts
        );
        assert_eq!(
            decrypt_like_reference(
                &verifier,
                &subject,
                &issued.certificate,
                &issued.verifier_keyring,
                false
            ),
            issued.reference_verifier_plaintexts
        );
        let subject_wallet = ProtoWallet::new(Some(key(issued.keys.subject)));
        assert_eq!(
            decrypt_like_reference(
                &subject_wallet,
                &key(issued.keys.certifier).public_key(),
                &issued.certificate,
                &issued.master_keyring,
                true
            ),
            issued.reference_master_plaintexts
        );
        assert_eq!(issued.reference_verifier_plaintexts, issued.plaintexts);
        if !issued.anyone_keyring.is_empty() {
            assert_eq!(
                decrypt_like_reference(
                    &ProtoWallet::new(Some(key(1))),
                    &subject,
                    &issued.certificate,
                    &issued.anyone_keyring,
                    false
                ),
                issued.reference_anyone_plaintexts
            );
        }
        let subset = prove(
            &wallet,
            stored,
            issued.keys.verifier,
            vec!["city".to_string()],
        )
        .await;
        assert_eq!(subset.len(), 1);
        assert_eq!(
            decrypt_like_reference(&verifier, &subject, &issued.certificate, &subset, false),
            IndexMap::from([("city".to_string(), issued.plaintexts["city"].clone())])
        );
    }
}

#[tokio::test]
async fn toolbox_issued_certificate_decrypts_like_reference() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    for issued in [fixture.reference_issued, fixture.rust_issued] {
        let (wallet, auth) = wallet(issued.keys.subject).await;
        let certifier = key(issued.keys.certifier);
        let subject = key(issued.keys.subject).public_key();
        let args = acquisition_args(&issued);
        let prepared = prepare_issuance_action_data(
            &wallet,
            &args,
            &certifier.public_key(),
            &subject,
            Some(ORIGINATOR),
        )
        .await
        .unwrap();
        let response = certifier_response(&issued, &prepared);
        let cert_response = response.certificate.as_ref().unwrap();
        for (reader, counterparty) in [
            (
                ProtoWallet::new(Some(key(issued.keys.subject))),
                certifier.public_key(),
            ),
            (ProtoWallet::new(Some(certifier.clone())), subject.clone()),
        ] {
            assert_eq!(
                decrypt_like_reference(
                    &reader,
                    &counterparty,
                    cert_response,
                    &prepared.master_keyring,
                    true,
                ),
                issued.plaintexts
            );
        }

        let parsed = parse_certificate_response(&response).unwrap();
        verify_certificate_issuance(
            &wallet,
            &parsed,
            &prepared.client_nonce,
            &prepared.fields,
            &prepared.cert_type_b64,
            &auth.identity_key,
            &cert_response.certifier,
            &certifier.public_key(),
            Some(ORIGINATOR),
        )
        .await
        .unwrap();
        assert!(verify_like_reference(
            &decode_issued_certificate(&parsed.certificate).unwrap()
        ));
        assert_eq!(parsed.certificate.fields, prepared.fields);
        let stored = store(&wallet, &auth, &parsed, &prepared.master_keyring).await;
        let keyring = prove(
            &wallet,
            stored,
            issued.keys.verifier,
            cert_response.fields.keys().cloned().collect(),
        )
        .await;
        assert_eq!(
            decrypt_like_reference(
                &ProtoWallet::new(Some(key(issued.keys.verifier))),
                &subject,
                cert_response,
                &keyring,
                false
            ),
            issued.plaintexts
        );
    }
}

#[tokio::test]
async fn issuance_uses_fresh_keys_for_equal_and_empty_plaintexts() {
    let subject = key(37);
    let subject_wallet = ProtoWallet::new(Some(subject.clone()));
    let certifier = key(31).public_key();
    let plaintexts = IndexMap::from([
        ("a".to_string(), "same value".to_string()),
        ("b".to_string(), "same value".to_string()),
        ("empty".to_string(), String::new()),
    ]);
    let mut all_keys = HashSet::new();
    for _ in 0..2 {
        let (fields, keyring) = create_certificate_fields_for_issuance(
            &subject_wallet,
            &certifier,
            &subject.public_key(),
            &plaintexts,
            Some(ORIGINATOR),
        )
        .await
        .unwrap();
        for (name, ciphertext) in fields {
            let field_key = subject_wallet
                .decrypt(DecryptArgs {
                    ciphertext: keyring[&name].clone(),
                    protocol_id: Protocol::new(
                        SecurityLevel::Counterparty,
                        "certificate field encryption",
                    ),
                    key_id: name.clone(),
                    counterparty: Some(Counterparty::Other(certifier.clone())),
                    ..Default::default()
                })
                .unwrap()
                .plaintext;
            assert_eq!(field_key.len(), 32);
            assert!(
                all_keys.insert(field_key.clone()),
                "field key reused for {name}"
            );
            assert_eq!(
                SymmetricKey::from_bytes(&field_key)
                    .unwrap()
                    .decrypt(&ciphertext)
                    .unwrap(),
                plaintexts[&name].as_bytes()
            );
        }
    }
}

/// The key's text in the reference's lenient `fromString` forms that
/// toBinary's `^(02|03)[0-9a-fA-F]{64}$` refuses: the odd-length string
/// (the compressed text without its leading `0`) and the hybrid key.
fn lenient_forms(key: &bsv_rs::primitives::PublicKey) -> [String; 2] {
    let uncompressed = key.to_hex_uncompressed();
    let y_odd = u8::from_str_radix(&uncompressed[128..], 16).unwrap() & 1 == 1;
    let hybrid = format!("{}{}", if y_odd { "07" } else { "06" }, &uncompressed[2..]);
    [key.to_hex()[1..].to_string(), hybrid]
}

/// bsv-rs 0.9.0-beta.5's `from_hex` reads the reference's lenient key forms
/// (bsv-rs #498); the reference's `Certificate.verify` answers false for a
/// certificate whose subject or certifier text is one of them, its
/// `toBinary` refusing the text (Certificate.ts:240-254, :425-441), and so
/// does the port's verify at the issuance door and the stored-certificate
/// audit (`issued_certificate_verifies`). Red at the beta.5 pin before the
/// strict read: the certifier's hybrid and odd-length texts verified. A
/// `05` tag (bsv-rs #550) is refused as before.
#[test]
fn a_lenient_key_text_does_not_verify_as_the_reference_refuses_it() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    let issued = fixture.reference_issued;
    let parsed = parse_issued(&issued);
    assert!(issued.reference_verify);
    assert!(issued_certificate_verifies(&parsed.certificate));
    let certifier = key(issued.keys.certifier).public_key();
    let subject = key(issued.keys.subject).public_key();
    for (role, key) in [("certifier", &certifier), ("subject", &subject)] {
        let compressed = key.to_hex();
        let tag_05 = format!("05{}", &compressed[2..]);
        for text in lenient_forms(key).into_iter().chain([tag_05]) {
            let mut raw = parsed.certificate.clone();
            if role == "certifier" {
                raw.certifier = text.clone();
            } else {
                raw.subject = text.clone();
            }
            assert!(
                !issued_certificate_verifies(&raw),
                "{role} {text} verified; the reference's toBinary refuses it"
            );
        }
    }
}

/// An uncompressed `04` subject or certifier text: `from_hex` read it at
/// bsv-rs 0.9.0-beta.4 and at beta.5, so a stored certificate carrying one
/// verified; toBinary's `^(02|03)[0-9a-fA-F]{64}$` (Certificate.ts:241-251)
/// throws and `Certificate.verify` answers false (:425-441), and so does the
/// stored-certificate audit (`issued_certificate_verifies`) since the strict
/// read. Red with `certificate_key` reading `from_hex` alone.
#[test]
fn an_uncompressed_key_text_does_not_verify_as_the_reference_refuses_it() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    let issued = fixture.reference_issued;
    let parsed = parse_issued(&issued);
    assert!(issued_certificate_verifies(&parsed.certificate));
    let subject = key(issued.keys.subject).public_key().to_hex_uncompressed();
    let certifier = key(issued.keys.certifier)
        .public_key()
        .to_hex_uncompressed();
    let mut raw = parsed.certificate.clone();
    raw.subject = subject.clone();
    assert!(
        !issued_certificate_verifies(&raw),
        "subject {subject} verified; the reference's toBinary refuses it"
    );
    let mut raw = parsed.certificate.clone();
    raw.certifier = certifier.clone();
    assert!(
        !issued_certificate_verifies(&raw),
        "certifier {certifier} verified; the reference's toBinary refuses it"
    );
}
