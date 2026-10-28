//! The direct acquire's argument checks and cryptographic proof at the
//! `WalletInterface` boundary (#311, #374). The reference's `acquireCertificate` (ts-stack@edf6e03
//! `wallet-toolbox/src/Wallet.ts:826-833`) routes `direct` to
//! `acquireDirectCertificateProtocol` (696-733), which opens with
//! `validateAcquireDirectCertificateArgs` (`sdk/src/wallet/validationHelpers.ts:1401-1450`)
//! through `validateAuthAndArgs` (697). Argument refusals keep that validator's
//! text and order. The proof witnesses exercise signature verification and
//! field decryption (705-731), with no row written on a refusal.

use super::busy_door_tests::certificate_rows;
use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::auth::certificates::MasterCertificate;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{AcquisitionProtocol, KeyringRevealer};

use crate::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig};
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The validator's presence rule at validationHelpers.ts:1409-1430.
const DIRECT: &str = "valid when acquisitionProtocol is \"direct\"";

/// The compressed public key of the private key `scalar`.
fn key(scalar: u64) -> String {
    let private = PrivateKey::from_hex(&format!("{scalar:064x}")).unwrap();
    private.public_key().to_hex()
}

fn refusal(parameter: &str, must_be: &str) -> String {
    format!("Validation error: The {parameter} parameter must be {must_be}")
}

async fn wallet() -> Wallet<StorageSqlx, MockWalletServices> {
    let root = PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("direct-acquire", &key(31)).await.unwrap();
    storage.make_available().await.unwrap();
    Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap()
}

/// Arguments every check accepts: 32-byte type and serial, a compressed
/// certifier, a canonical DER signature (r = 10, s = 11), one field with its
/// master key.
fn direct_args() -> AcquireCertificateArgs {
    AcquireCertificateArgs {
        certificate_type: BASE64.encode([33; 32]),
        certifier: key(7),
        acquisition_protocol: AcquisitionProtocol::Direct,
        fields: HashMap::from([("name".to_string(), "YQ==".to_string())]),
        serial_number: Some(BASE64.encode([32; 32])),
        revocation_outpoint: Some(format!("{}.0", "ab".repeat(32))),
        signature: Some("300602010a02010b".to_string()),
        certifier_url: None,
        keyring_revealer: Some(KeyringRevealer::Certifier),
        keyring_for_subject: Some(HashMap::from([("name".to_string(), "Yg==".to_string())])),
        privileged: None,
        privileged_reason: None,
    }
}

// The SDK equivalent of issueCertificateForSubject encrypts to this wallet
// and signs with the certifier's BRC-52 child key. Re-sign after setting the
// outpoint, since the issuance helper uses the absent-outpoint sentinel.
async fn signed_master(wallet: &Wallet<StorageSqlx, MockWalletServices>) -> MasterCertificate {
    let certifier = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let mut master = MasterCertificate::issue_for_subject(
        &ProtoWallet::new(Some(certifier.clone())),
        &certifier,
        PublicKey::from_hex(wallet.identity_key()).unwrap(),
        HashMap::from([
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
    assert!(master.verify().unwrap());
    master
}

fn master_args(master: MasterCertificate) -> AcquireCertificateArgs {
    let mut args = direct_args();
    args.fields = master.certificate.fields;
    args.signature = master.certificate.signature.map(hex::encode);
    args.keyring_for_subject = Some(master.master_keyring);
    args
}

pub(super) async fn signed_args(
    wallet: &Wallet<StorageSqlx, MockWalletServices>,
) -> AcquireCertificateArgs {
    master_args(signed_master(wallet).await)
}

fn verification_refusal(tail: &str) -> String {
    refusal(
        "args",
        &format!("valid encrypted and signed certificate and keyring from revealer. {tail}"),
    )
}

#[tokio::test]
async fn direct_acquire_verify_refuses_forged_signature() {
    let wallet = wallet().await;
    let mut master = signed_master(&wallet).await;
    // Valid DER from this certifier, over a different certificate body.
    let mut other = master.certificate.clone();
    other.set_field("name".to_string(), BASE64.encode(b"different ciphertext"));
    other.set_signature(None);
    other
        .sign(&PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap())
        .unwrap();
    master.set_signature(other.signature);
    assert!(!master.verify().unwrap());
    assert_refused(
        "forged signature",
        master_args(master),
        &verification_refusal("WERR_UNKNOWN: Certificate verification failed"),
    )
    .await;
}

#[tokio::test]
async fn direct_acquire_verify_refuses_undecryptable_keyring() {
    let wallet = wallet().await;
    let mut master = signed_master(&wallet).await;
    // The signature still verifies. The other field opens, but this entry
    // cannot decrypt to the role field's revelation key for this wallet.
    master
        .master_keyring
        .insert("role".to_string(), BASE64.encode([0; 64]));
    assert!(master.verify().unwrap());
    assert_refused(
        "undecryptable keyring entry",
        master_args(master),
        &verification_refusal("WERR_UNKNOWN: Failed to decrypt all master certificate fields."),
    )
    .await;
}

#[tokio::test]
async fn direct_acquire_verify_refuses_field_over_one_mib() {
    let wallet = wallet().await;
    for (length, tail) in [
        (
            1_048_577,
            "WERR_UNKNOWN: Certificate field name must use canonical base64 encoding",
        ),
        (1_048_580, "WERR_UNKNOWN: Certificate verification failed"),
    ] {
        let mut master = signed_master(&wallet).await;
        // The exact reported size cannot be canonical base64; the next
        // multiple of four also exercises verify's serialized-text bound.
        master.set_field("name".to_string(), "A".repeat(length));
        assert_eq!(master.fields["name"].len(), length);
        assert_eq!(
            master.to_binary(false).unwrap_err().to_string(),
            "certificate validation failed: Certificate field name exceeds the maximum of 1048576 bytes"
        );
        assert_refused(
            "oversized field",
            master_args(master),
            &verification_refusal(tail),
        )
        .await;
    }
}

#[tokio::test]
async fn direct_acquire_verify_stores_ciphertext_and_keyring_unchanged() {
    for revealer in [
        KeyringRevealer::Certifier,
        KeyringRevealer::PublicKey(PublicKey::from_hex(&key(9)).unwrap()),
    ] {
        let wallet = wallet().await;
        let mut args = signed_args(&wallet).await;
        args.keyring_revealer = Some(revealer.clone());
        args.privileged = Some(true);
        args.privileged_reason = Some("certificate import".to_string());
        let expected_fields = args.fields.clone();
        let expected_keyring = args.keyring_for_subject.clone().unwrap();
        let acquired = wallet
            .acquire_certificate(args.clone(), Some("certificate.test"))
            .await
            .unwrap();
        assert_eq!(acquired.fields, expected_fields);
        assert_eq!(acquired.signature, args.signature.unwrap());
        assert_eq!(acquired.subject, wallet.identity_key());
        let stored: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT field_name, field_value, master_key FROM certificate_fields ORDER BY field_name",
        )
        .fetch_all(wallet.storage().pool())
        .await
        .unwrap();
        assert_eq!(stored.len(), expected_fields.len());
        for (name, ciphertext, keyring_entry) in stored {
            assert_eq!(ciphertext, expected_fields[&name]);
            assert_eq!(keyring_entry, expected_keyring[&name]);
        }
        let verifier: String = sqlx::query_scalar("SELECT verifier FROM certificates")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
        assert_eq!(
            verifier,
            match revealer {
                KeyringRevealer::Certifier => key(7),
                KeyringRevealer::PublicKey(key) => key.to_hex(),
            }
        );
    }
}

#[tokio::test]
async fn direct_acquire_verify_needs_no_protocol_permission() {
    let mut wallet = wallet().await;
    let manager = Arc::new(WalletPermissionsManager::new(
        Arc::new(wallet.proto_wallet.clone()),
        "wallet.admin".to_string(),
        WalletPermissionsManagerConfig::all_enabled(),
    ));
    assert_ne!(manager.admin_originator(), "certificate.test");
    let prompts = Arc::new(AtomicUsize::new(0));
    let prompt_count = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |_| {
            prompt_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(crate::Error::AccessDenied(
                    "Protocol permission refused by the test".to_string(),
                ))
            })
        }))
        .await;
    wallet.set_permissions_manager(manager);
    let args = signed_args(&wallet).await;
    let expected_fields = args.fields.clone();
    let result = wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await;
    assert_eq!(prompts.load(Ordering::SeqCst), 0, "{result:?}");
    let acquired = result.expect("direct acquisition needs no protocol permission");
    assert_eq!(acquired.subject, wallet.identity_key());
    assert_eq!(acquired.fields, expected_fields);
    assert_eq!(certificate_rows(&wallet).await, (1, 2));
}

/// The direct acquire of `args` is refused with exactly `expected` and
/// leaves the store without a certificate or a field.
async fn assert_refused(scenario: &str, args: AcquireCertificateArgs, expected: &str) {
    let wallet = wallet().await;
    let result = wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await;
    match result {
        Err(bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            message,
        }) => assert_eq!(message, expected, "{scenario}"),
        other => panic!("{scenario}: expected InvalidParameter({expected:?}), got {other:?}"),
    }
    assert_eq!(
        certificate_rows(&wallet).await,
        (0, 0),
        "{scenario}: stored"
    );
}

async fn refused_with(scenario: &str, edit: impl FnOnce(&mut AcquireCertificateArgs), text: &str) {
    let mut args = direct_args();
    edit(&mut args);
    assert_refused(scenario, args, text).await;
}

#[tokio::test]
async fn accepts_the_valid_arguments_and_stores_one_row() {
    let wallet = wallet().await;
    let acquired = wallet
        .acquire_certificate(signed_args(&wallet).await, Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(acquired.certifier, key(7));
    assert_eq!(certificate_rows(&wallet).await, (1, 2));
}

// 1432: validateBase64String(args.type, 'type', 32, 32), its texts at 439-441
// and 470-475.
#[tokio::test]
async fn refuses_a_bad_type() {
    for (scenario, value, rule) in [
        ("short", BASE64.encode([1; 4]), "at least 32 bytes"),
        ("long", BASE64.encode([1; 33]), "no more than 32 bytes"),
        (
            "not base64",
            "not base64!".to_string(),
            "valid base64 string",
        ),
        ("empty", String::new(), "valid base64 string"),
    ] {
        refused_with(
            scenario,
            |a| a.certificate_type = value,
            &refusal("type", rule),
        )
        .await;
    }
}

// 1409-1410 refuses an absent or empty serial number, then 1433 its encoding
// and size under the bare name.
#[tokio::test]
async fn refuses_a_bad_serial_number() {
    for (scenario, value, rule) in [
        ("absent", None, DIRECT),
        ("empty", Some(String::new()), DIRECT),
        ("short", Some(BASE64.encode([1; 31])), "at least 32 bytes"),
        ("not base64", Some("a=b".to_string()), "valid base64 string"),
    ] {
        refused_with(
            scenario,
            |a| a.serial_number = value,
            &refusal("serialNumber", rule),
        )
        .await;
    }
}

// 1434: validatePublicKey(args.certifier, 'certifier') at 185-191 over
// validateHexString at 522-534.
#[tokio::test]
async fn refuses_a_bad_certifier() {
    for (value, rule) in [
        ("abc".to_string(), "even length, not 3."),
        ("zz".repeat(33), "hexadecimal string."),
        ("02ab".to_string(), "at least 66 length."),
        (
            format!("04{}", "11".repeat(32)),
            "a valid compressed secp256k1 public key",
        ),
    ] {
        refused_with(
            &value.clone(),
            |a| a.certifier = value,
            &refusal("certifier", rule),
        )
        .await;
    }
}

// 1411-1412 refuses an absent or empty signature, then 1437
// validateSignatureHex(args.signature, 'signature') at 536-542.
#[tokio::test]
async fn refuses_a_bad_signature() {
    for (scenario, value, rule) in [
        ("absent", None, DIRECT),
        ("empty", Some(String::new()), DIRECT),
        ("odd", Some("abc".to_string()), "even length, not 3."),
        ("not hex", Some("zz".to_string()), "hexadecimal string."),
        (
            "not DER",
            Some("deadbeef".to_string()),
            "a canonical DER-encoded ECDSA signature",
        ),
    ] {
        refused_with(
            scenario,
            |a| a.signature = value,
            &refusal("signature", rule),
        )
        .await;
    }
}

// 1419-1424 and 1425-1430: the revealer and the keyring are required; an
// empty keyring is present (`== null` at 1425).
#[tokio::test]
async fn refuses_an_absent_keyring_or_revealer() {
    refused_with(
        "no revealer",
        |a| a.keyring_revealer = None,
        &refusal("keyringRevealer", DIRECT),
    )
    .await;
    refused_with(
        "no keyring",
        |a| a.keyring_for_subject = None,
        &refusal("keyringForSubject", DIRECT),
    )
    .await;
    let mut args = direct_args();
    args.keyring_for_subject = Some(HashMap::new());
    // Presence is a validator rule; the later proof has separate requirements.
    assert!(direct_acquire::validate_acquire_direct_args(&args).is_ok());
    assert_refused(
        "empty keyring",
        args,
        &verification_refusal(
            "WERR_UNKNOWN: Master keyring must contain a value for every field. Missing or empty key for field: \"name\".",
        ),
    )
    .await;
}

// 1439: validateKeyringForSubject at 1248-1259: validateRecord (81-103) on
// the map, then each name 1 to 50 bytes and each value base64 of any size.
#[tokio::test]
async fn refuses_a_bad_keyring() {
    let name = "keyringForSubject field name";
    let value = "keyringForSubject field value";
    for (scenario, entry, text) in [
        (
            "unsafe key",
            ("__proto__".to_string(), "Yg==".to_string()),
            refusal("keyringForSubject", "an object with safe string keys"),
        ),
        (
            "empty name",
            (String::new(), "Yg==".to_string()),
            refusal(name, "at least 1 length."),
        ),
        (
            "long name",
            ("n".repeat(51), "Yg==".to_string()),
            refusal(name, "no more than 50 length."),
        ),
        (
            "not base64",
            ("name".to_string(), "Yg=".to_string()),
            refusal(value, "valid base64 string"),
        ),
        (
            "empty value",
            ("name".to_string(), String::new()),
            refusal(value, "valid base64 string"),
        ),
    ] {
        refused_with(
            scenario,
            |a| a.keyring_for_subject = Some(HashMap::from([entry])),
            &text,
        )
        .await;
    }
    refused_with(
        "bound",
        |a| {
            a.keyring_for_subject = Some(
                (0..100_001)
                    .map(|i| (format!("k{i}"), String::new()))
                    .collect(),
            );
        },
        &refusal(
            "keyringForSubject",
            "an object with at most 100000 properties",
        ),
    )
    .await;
}

// 1405: validatePrivilege(args, false) at 207-222 before every other check.
#[tokio::test]
async fn refuses_a_bad_privilege_before_the_certificate() {
    refused_with(
        "privileged without a reason",
        |a| {
            a.privileged = Some(true);
            a.serial_number = None;
        },
        &refusal("privilegedReason", "a value when 'privileged' is true"),
    )
    .await;
    refused_with(
        "short reason",
        |a| {
            a.privileged_reason = Some("abcd".to_string());
            a.certificate_type = String::new();
        },
        &refusal("privilegedReason", "at least 5 length."),
    )
    .await;
}

/// An edit of the valid arguments.
type Edit = Box<dyn Fn(&mut AcquireCertificateArgs)>;

// The order of 1409-1439: the presence checks before any shape, then type,
// serial number, certifier, outpoint, fields, signature, keyring.
#[tokio::test]
async fn refuses_in_the_reference_order() {
    let bad_type = |a: &mut AcquireCertificateArgs| a.certificate_type = String::new();
    let cases: Vec<(&str, Edit, String)> = vec![
        (
            "absent keyring before a bad type",
            Box::new(move |a| {
                bad_type(a);
                a.keyring_for_subject = None;
            }),
            refusal("keyringForSubject", DIRECT),
        ),
        (
            "absent signature before an absent outpoint",
            Box::new(|a| {
                a.signature = None;
                a.revocation_outpoint = None;
            }),
            refusal("signature", DIRECT),
        ),
        (
            "type before serial number",
            Box::new(move |a| {
                bad_type(a);
                a.serial_number = Some("a".to_string());
            }),
            refusal("type", "valid base64 string"),
        ),
        (
            "serial number before certifier",
            Box::new(|a| {
                a.serial_number = Some("a".to_string());
                a.certifier = "abc".to_string();
            }),
            refusal("serialNumber", "valid base64 string"),
        ),
        (
            "certifier before outpoint",
            Box::new(|a| {
                a.certifier = "abc".to_string();
                a.revocation_outpoint = Some("abc.0".to_string());
            }),
            refusal("certifier", "even length, not 3."),
        ),
        (
            "outpoint before fields",
            Box::new(|a| {
                a.revocation_outpoint = Some("abc.0".to_string());
                a.fields = HashMap::from([(String::new(), "YQ==".to_string())]);
            }),
            refusal(
                "revocationOutpoint",
                "a 32-byte txid and canonical numeric output index joined with '.'",
            ),
        ),
        (
            "fields before signature",
            Box::new(|a| {
                a.fields = HashMap::from([(String::new(), "YQ==".to_string())]);
                a.signature = Some("deadbeef".to_string());
            }),
            refusal("field name", "at least 1 length."),
        ),
        (
            "signature before keyring",
            Box::new(|a| {
                a.signature = Some("deadbeef".to_string());
                a.keyring_for_subject = Some(HashMap::from([(String::new(), String::new())]));
            }),
            refusal("signature", "a canonical DER-encoded ECDSA signature"),
        ),
    ];
    for (scenario, edit, text) in cases {
        refused_with(scenario, |a| edit(a), &text).await;
    }
}

// A Rust map keeps no insertion order (bsv-rs's `HashMap` fields), so where
// the reference walks Object.keys order (1236, 1254) the port walks the names
// in byte order: of two bad names the one that sorts lower decides the text,
// every run. A map with one bad name gets the reference's own text.
#[tokio::test]
async fn several_bad_names_refuse_by_the_lowest_name() {
    refused_with(
        "fields: empty name sorts before a long one",
        |a| {
            a.fields = HashMap::from([
                ("n".repeat(51), "YQ==".to_string()),
                (String::new(), "YQ==".to_string()),
            ]);
        },
        &refusal("field name", "at least 1 length."),
    )
    .await;
    refused_with(
        "keyring: a bad value under 'a' before a long name under 'b...'",
        |a| {
            a.keyring_for_subject = Some(HashMap::from([
                ("b".repeat(51), "Yg==".to_string()),
                ("a".to_string(), "!".to_string()),
            ]));
        },
        &refusal("keyringForSubject field value", "valid base64 string"),
    )
    .await;
    refused_with(
        "keyring: a long name under 'a...' before a bad value under 'b'",
        |a| {
            a.keyring_for_subject = Some(HashMap::from([
                ("b".to_string(), "!".to_string()),
                ("a".repeat(51), "Yg==".to_string()),
            ]));
        },
        &refusal("keyringForSubject field name", "no more than 50 length."),
    )
    .await;
}

// acquireDirectCertificate.ts:13-27 stores vargs: the certifier and the
// signature lowercased (validateHexString at 533), type and serial number
// trimmed (490), and the verifier the normalized certifier (19).
#[tokio::test]
async fn stores_the_normalized_arguments() {
    let wallet = wallet().await;
    let mut args = signed_args(&wallet).await;
    let signature = args.signature.clone().unwrap();
    args.certifier = key(7).to_uppercase();
    args.signature = Some(signature.to_uppercase());
    args.certificate_type = format!(" {} ", BASE64.encode([33; 32]));
    args.serial_number = Some(format!("\t{}\n", BASE64.encode([32; 32])));
    let acquired = wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(acquired.certifier, key(7));
    assert_eq!(acquired.signature, signature);
    assert_eq!(acquired.certificate_type, BASE64.encode([33; 32]));
    assert_eq!(acquired.serial_number, BASE64.encode([32; 32]));
    let row: (String, String, String, String, Option<String>) = sqlx::query_as(
        "SELECT type, serial_number, certifier, signature, verifier FROM certificates",
    )
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    assert_eq!(
        row,
        (
            BASE64.encode([33; 32]),
            BASE64.encode([32; 32]),
            key(7),
            signature,
            Some(key(7)),
        )
    );
}

// #188: legacy rows remain readable after the acquire door closes. Seed
// the old invalid row at storage, then acquire a newly signed certificate.
#[tokio::test]
async fn reverify_counts_stored_certificates_that_do_not_verify() {
    let wallet = wallet().await;
    assert_eq!(
        wallet.reverify_stored_certificates().await.unwrap(),
        StoredCertificateReverification::default()
    );
    let now = chrono::Utc::now();
    let legacy = TableCertificate {
        certificate_id: 0,
        user_id: wallet.user_id,
        cert_type: BASE64.encode([34; 32]),
        serial_number: BASE64.encode([32; 32]),
        certifier: key(7),
        subject: wallet.identity_key().to_string(),
        verifier: Some(key(7)),
        revocation_outpoint: format!("{}.0", "ab".repeat(32)),
        signature: "300602010a02010b".to_string(),
        created_at: now,
        updated_at: now,
    };
    wallet
        .storage()
        .insert_certificate(&wallet.auth(), legacy)
        .await
        .unwrap();
    wallet
        .acquire_certificate(signed_args(&wallet).await, Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(
        wallet.reverify_stored_certificates().await.unwrap(),
        StoredCertificateReverification {
            checked: 2,
            failed: 1
        }
    );
    assert_eq!(certificate_rows(&wallet).await, (2, 2), "nothing deleted");
}
