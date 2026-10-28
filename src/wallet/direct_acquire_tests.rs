//! The direct acquire's argument checks at the `WalletInterface` boundary
//! (#311). The reference's `acquireCertificate` (ts-stack@edf6e03
//! `wallet-toolbox/src/Wallet.ts:826-833`) routes `direct` to
//! `acquireDirectCertificateProtocol` (696-733), which opens with
//! `validateAcquireDirectCertificateArgs` (`sdk/src/wallet/validationHelpers.ts:1401-1450`)
//! through `validateAuthAndArgs` (697). Each refusal here is that validator's
//! text, in its order, before any row is written.

use super::busy_door_tests::certificate_rows;
use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{AcquisitionProtocol, KeyringRevealer};

use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;

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

/// The direct acquire of `args` is refused with exactly `expected` and
/// leaves the store without a certificate or a field.
async fn assert_refused(scenario: &str, args: AcquireCertificateArgs, expected: &str) {
    let wallet = wallet().await;
    let result = wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await;
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(message, expected, "{scenario}"),
        other => panic!("{scenario}: expected WalletError({expected:?}), got {other:?}"),
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
        .acquire_certificate(direct_args(), Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(acquired.certifier, key(7));
    assert_eq!(certificate_rows(&wallet).await, (1, 1));
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
    let wallet = wallet().await;
    let mut args = direct_args();
    args.keyring_for_subject = Some(HashMap::new());
    wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(certificate_rows(&wallet).await, (1, 1), "an empty keyring");
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
    let mut args = direct_args();
    args.certifier = key(7).to_uppercase();
    args.signature = Some("300602010A02010B".to_string());
    args.certificate_type = format!(" {} ", BASE64.encode([33; 32]));
    args.serial_number = Some(format!("\t{}\n", BASE64.encode([32; 32])));
    let acquired = wallet
        .acquire_certificate(args, Some("certificate.test"))
        .await
        .unwrap();
    assert_eq!(acquired.certifier, key(7));
    assert_eq!(acquired.signature, "300602010a02010b");
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
            "300602010a02010b".to_string(),
            Some(key(7)),
        )
    );
}

// #188, the owner's word: one read of every stored certificate under the
// derived key, counted, nothing deleted. A direct acquire stores what it is
// given, so one row signed by the certifier and one with a signature that
// does not verify make the count.
#[tokio::test]
async fn reverify_counts_stored_certificates_that_do_not_verify() {
    use bsv_rs::auth::certificates::Certificate;

    let wallet = wallet().await;
    assert_eq!(
        wallet.reverify_stored_certificates().await.unwrap(),
        StoredCertificateReverification::default()
    );

    // direct_args' signature (r = 10, s = 11) is DER that verifies nowhere.
    wallet
        .acquire_certificate(direct_args(), Some("certificate.test"))
        .await
        .unwrap();

    let certifier = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let mut certificate = Certificate::new(
        [34; 32],
        [32; 32],
        PublicKey::from_hex(wallet.identity_key()).unwrap(),
        certifier.public_key(),
    );
    certificate.revocation_outpoint = Some(bsv_rs::wallet::Outpoint::new([0xab; 32], 0));
    certificate.set_field("name".to_string(), "YQ==".to_string());
    certificate.sign(&certifier).unwrap();
    let mut signed = direct_args();
    signed.certificate_type = BASE64.encode([34; 32]);
    signed.signature = Some(hex::encode(certificate.signature.unwrap()));
    wallet
        .acquire_certificate(signed, Some("certificate.test"))
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
