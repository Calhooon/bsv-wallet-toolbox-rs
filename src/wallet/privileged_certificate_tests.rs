//! The certificate keyring under the key the caller named (#142): with
//! `privileged: true`, `prove_certificate` derives the verifier's keyring
//! through the privileged key manager, as ts-stack@edf6e03
//! `MasterCertificate.createKeyringForVerifier` decrypts and encrypts with
//! `privileged` (`MasterCertificate.ts:383-401`, called from
//! `signer/methods/proveCertificate.ts:32-42`), and the direct acquire takes
//! its subject and proves its keyring under that key (`Wallet.ts:698-724`).
//! With no manager both are the reference's refusal (`Wallet.ts:460-461`),
//! never a proof under the everyday key.

use super::busy_door_tests::{certificate_rows, wallet};
use super::privileged_keys_tests::{assert_privileged_refusal, Asked, PrivilegedDouble};
use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::auth::certificates::MasterCertificate;
use bsv_rs::primitives::SymmetricKey;
use bsv_rs::wallet::{AcquisitionProtocol, KeyringRevealer};

use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;

const ORIGINATOR: &str = "privileged.example.com";
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

/// A certificate issued to `subject`, its keyring encrypted to that key.
async fn issued(subject: &str) -> MasterCertificate {
    let certifier = certifier();
    let mut master = MasterCertificate::issue_for_subject(
        &ProtoWallet::new(Some(certifier.clone())),
        &certifier,
        PublicKey::from_hex(subject).unwrap(),
        IndexMap::from([
            ("name".to_string(), "Alice".to_string()),
            ("role".to_string(), "reader".to_string()),
        ]),
        [33; 32],
        Some([32; 32]),
        Some("certificate.test"),
    )
    .await
    .unwrap();
    master.set_revocation_outpoint(Some(Outpoint::new([0xab; 32], 0)));
    master.set_signature(None);
    master.sign(&certifier).unwrap();
    master
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
        fields_to_reveal: vec!["name".to_string()],
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
    Asked {
        call,
        privileged: Some(true),
        privileged_reason: Some(REASON.to_string()),
        seek_permission: None,
        originator: ORIGINATOR.to_string(),
    }
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

#[tokio::test]
async fn privileged_prove_without_a_manager_is_the_references_refusal() {
    let (_dir, wallet) = wallet().await;
    let master = issued(wallet.identity_key()).await;
    store(&wallet, &master).await;
    assert_privileged_refusal("prove", prove(&wallet, &master, Some(true)).await);
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
