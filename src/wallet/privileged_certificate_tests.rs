//! The certificate keyring under the key the caller named (#142): with
//! `privileged: true`, `prove_certificate` derives the verifier's keyring
//! through the privileged key manager, as ts-stack@edf6e03
//! `MasterCertificate.createKeyringForVerifier` decrypts and encrypts with
//! `privileged` (`MasterCertificate.ts:383-401`, called from
//! `signer/methods/proveCertificate.ts:31-41`), and the direct acquire takes
//! its subject and proves its keyring under that key (`Wallet.ts:698-724`).
//! With no manager the acquire is the reference's refusal
//! (`Wallet.ts:460-461`, uncaught at `:697-703`) and the prove the helper's
//! decrypt catch (`MasterCertificate.ts:564-623`), never a proof under the
//! everyday key.

use super::busy_door_tests::{certificate_rows, wallet};
use super::direct_acquire_tests::issued_to as issued;
use super::privileged_keys_tests::{
    assert_privileged_refusal, Asked, PrivilegedDouble, ORIGINATOR,
};
use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::auth::certificates::MasterCertificate;
use bsv_rs::primitives::SymmetricKey;
use bsv_rs::wallet::{AcquisitionProtocol, KeyringRevealer};

use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;

const REASON: &str = "privileged proof";

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

fn certifier() -> PrivateKey {
    PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap()
}

fn verifier() -> PrivateKey {
    PrivateKey::from_hex(&format!("{:064x}", 9)).unwrap()
}

fn outpoint() -> String {
    format!("{}.0", "ab".repeat(32))
}

fn reason(privileged: Option<bool>) -> Option<String> {
    privileged.map(|_| REASON.to_string())
}

fn acquire_args(master: &MasterCertificate, privileged: Option<bool>) -> AcquireCertificateArgs {
    AcquireCertificateArgs {
        certificate_type: BASE64.encode([33; 32]),
        certifier: certifier().public_key().to_hex(),
        acquisition_protocol: AcquisitionProtocol::Direct,
        fields: master.certificate.fields.clone(),
        serial_number: Some(BASE64.encode([32; 32])),
        revocation_outpoint: Some(outpoint()),
        signature: master.certificate.signature.clone().map(hex::encode),
        certifier_url: None,
        keyring_revealer: Some(KeyringRevealer::Certifier),
        keyring_for_subject: Some(master.master_keyring.clone()),
        privileged,
        privileged_reason: reason(privileged),
    }
}

/// The stored row, written as the direct acquire writes it, so the prove
/// scenes need no acquire.
async fn store(wallet: &TestWallet, master: &MasterCertificate) {
    let now = chrono::Utc::now();
    let certificate = &master.certificate;
    let fields = certificate
        .fields
        .iter()
        .map(|(name, value)| TableCertificateField {
            certificate_field_id: 0,
            certificate_id: 0,
            user_id: wallet.user_id,
            field_name: name.clone(),
            field_value: value.clone(),
            master_key: master.master_keyring[name].clone(),
            created_at: now,
            updated_at: now,
        })
        .collect();
    let row = TableCertificate {
        certificate_id: 0,
        user_id: wallet.user_id,
        cert_type: BASE64.encode([33; 32]),
        serial_number: BASE64.encode([32; 32]),
        certifier: certifier().public_key().to_hex(),
        subject: certificate.subject.to_hex(),
        verifier: Some(certifier().public_key().to_hex()),
        revocation_outpoint: outpoint(),
        signature: hex::encode(certificate.signature.clone().unwrap()),
        created_at: now,
        updated_at: now,
    };
    wallet
        .storage()
        .insert_certificate_with_fields(&wallet.auth(), row, fields)
        .await
        .unwrap();
}

async fn prove(
    wallet: &TestWallet,
    master: &MasterCertificate,
    privileged: Option<bool>,
) -> bsv_rs::Result<ProveCertificateResult> {
    prove_revealing(wallet, master, privileged, &["name"]).await
}

async fn prove_revealing(
    wallet: &TestWallet,
    master: &MasterCertificate,
    privileged: Option<bool>,
    fields_to_reveal: &[&str],
) -> bsv_rs::Result<ProveCertificateResult> {
    let certificate = &master.certificate;
    let args = ProveCertificateArgs {
        certificate: WalletCertificate {
            certificate_type: BASE64.encode([33; 32]),
            subject: certificate.subject.to_hex(),
            serial_number: BASE64.encode([32; 32]),
            certifier: certifier().public_key().to_hex(),
            revocation_outpoint: outpoint(),
            signature: hex::encode(certificate.signature.clone().unwrap()),
            fields: certificate.fields.clone(),
        },
        fields_to_reveal: fields_to_reveal
            .iter()
            .map(|name| name.to_string())
            .collect(),
        verifier: verifier().public_key().to_hex(),
        privileged,
        privileged_reason: reason(privileged),
    };
    wallet.prove_certificate(args, Some(ORIGINATOR)).await
}

/// What the verifier reads with its keyring: the `name` field's plaintext.
fn verifier_reads(master: &MasterCertificate, keyring: &IndexMap<String, String>) -> String {
    let field_key = ProtoWallet::new(Some(verifier()))
        .decrypt(DecryptArgs {
            ciphertext: BASE64.decode(&keyring["name"]).unwrap(),
            protocol_id: Protocol::new(
                SecurityLevel::Counterparty,
                CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL,
            ),
            key_id: format!("{} name", BASE64.encode([32; 32])),
            counterparty: Some(Counterparty::Other(master.certificate.subject.clone())),
            ..Default::default()
        })
        .unwrap()
        .plaintext;
    let field = BASE64.decode(&master.certificate.fields["name"]).unwrap();
    let plaintext = SymmetricKey::from_bytes(&field_key)
        .unwrap()
        .decrypt(&field)
        .unwrap();
    String::from_utf8(plaintext).unwrap()
}

fn asked(call: &'static str) -> Asked {
    Asked::privileged(call, REASON, None)
}

#[tokio::test]
async fn privileged_prove_derives_the_keyring_through_the_manager() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    let master = issued(&double.key.identity_key_hex()).await;
    store(&wallet, &master).await;

    let proof = prove(&wallet, &master, Some(true)).await;
    let keyring = proof.expect("the privileged proof").keyring_for_verifier;
    assert_eq!(verifier_reads(&master, &keyring), "Alice");
    assert_eq!(double.asked(), vec![asked("decrypt"), asked("encrypt")]);
}

#[tokio::test]
async fn privileged_direct_acquire_takes_the_managers_subject_and_proof() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    let master = issued(&double.key.identity_key_hex()).await;

    let acquired = wallet
        .acquire_certificate(acquire_args(&master, Some(true)), Some(ORIGINATOR))
        .await
        .expect("the privileged acquire");
    assert_eq!(acquired.subject, double.key.identity_key_hex());
    assert_eq!(certificate_rows(&wallet).await, (1, 2));
    // The subject, then one decrypt per field (Wallet.ts:698-724).
    assert_eq!(
        double.asked(),
        vec![asked("get_public_key"), asked("decrypt"), asked("decrypt")]
    );
    let keyring = prove(&wallet, &master, Some(true))
        .await
        .expect("the privileged proof")
        .keyring_for_verifier;
    assert_eq!(verifier_reads(&master, &keyring), "Alice");
}

#[tokio::test]
async fn privileged_direct_acquire_without_a_manager_is_the_references_refusal() {
    let (_dir, wallet) = wallet().await;
    let master = issued(wallet.identity_key()).await;
    let acquired = wallet
        .acquire_certificate(acquire_args(&master, Some(true)), Some(ORIGINATOR))
        .await;
    assert_privileged_refusal("acquire", acquired);
    assert_eq!(certificate_rows(&wallet).await, (0, 0));
}

/// The prove's answer is the helper's fault: class `Internal`, `message`
/// byte for byte.
fn assert_internal_fault<T: std::fmt::Debug>(
    scene: &str,
    result: bsv_rs::Result<T>,
    message: &str,
) {
    match result {
        Err(bsv_rs::Error::WalletFault(fault)) => {
            assert_eq!(fault.family, bsv_rs::WalletFaultFamily::Internal, "{scene}");
            assert_eq!(fault.message, message, "{scene}");
        }
        other => panic!("{scene}: expected the Internal fault {message:?}, got {other:?}"),
    }
}

// With no manager the prove has no guard of its own, as
// `signer/methods/proveCertificate.ts:31-41` has none: the helper runs, the
// route's refusal is thrown inside `decryptField` and its catch answers
// (`MasterCertificate.ts:564-623`).
#[tokio::test]
async fn privileged_prove_without_a_manager_is_the_decrypt_catch() {
    let (_dir, wallet) = wallet().await;
    let master = issued(wallet.identity_key()).await;
    store(&wallet, &master).await;
    assert_internal_fault(
        "prove",
        prove(&wallet, &master, Some(true)).await,
        "Failed to decrypt certificate field!",
    );
}

#[tokio::test]
async fn privileged_prove_of_no_field_without_a_manager_is_an_empty_keyring() {
    let (_dir, wallet) = wallet().await;
    let master = issued(wallet.identity_key()).await;
    store(&wallet, &master).await;
    let proof = prove_revealing(&wallet, &master, Some(true), &[]).await;
    let keyring = proof.expect("no field touches no key").keyring_for_verifier;
    assert_eq!(keyring, IndexMap::<String, String>::new());
}

#[tokio::test]
async fn privileged_prove_of_a_missing_field_without_a_manager_is_the_subset_refusal() {
    // The subset check precedes any decrypt (`MasterCertificate.ts:370-380`).
    let (_dir, wallet) = wallet().await;
    let master = issued(wallet.identity_key()).await;
    store(&wallet, &master).await;
    assert_internal_fault(
        "prove",
        prove_revealing(&wallet, &master, Some(true), &["nickname"]).await,
        "Fields to reveal must be a subset of the certificate fields. Missing the \"nickname\" field.",
    );
}

#[tokio::test]
async fn unprivileged_certificate_calls_keep_the_everyday_key() {
    for privileged in [None, Some(false)] {
        let (_dir, mut wallet) = wallet().await;
        let double = PrivilegedDouble::new();
        wallet.set_privileged_key_manager(double.clone());
        let master = issued(wallet.identity_key()).await;

        let acquired = wallet
            .acquire_certificate(acquire_args(&master, privileged), Some(ORIGINATOR))
            .await;
        assert_eq!(acquired.unwrap().subject, wallet.identity_key());
        let keyring = prove(&wallet, &master, privileged)
            .await
            .expect("the everyday proof")
            .keyring_for_verifier;
        assert_eq!(verifier_reads(&master, &keyring), "Alice");
        assert_eq!(double.asked(), Vec::<Asked>::new(), "{privileged:?}");
    }
}
