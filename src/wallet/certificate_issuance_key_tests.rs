//! #261: acquisition validates and normalizes the certifier before issuance.

use super::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::AcquisitionProtocol;

use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::{StorageSqlx, Wallet};

const ORIGINATOR: &str = "certificate.test";

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

fn certifier() -> String {
    PrivateKey::from_hex(&format!("{:064x}", 31))
        .unwrap()
        .public_key()
        .to_hex()
}

async fn wallet() -> TestWallet {
    let subject = PrivateKey::from_hex(&format!("{:064x}", 37)).unwrap();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("issuance-key", &subject.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    Wallet::new(Some(subject), storage, MockWalletServices::new())
        .await
        .unwrap()
}

fn args(certifier: String, url: String) -> AcquireCertificateArgs {
    AcquireCertificateArgs {
        certificate_type: BASE64.encode([7; 32]),
        certifier,
        acquisition_protocol: AcquisitionProtocol::Issuance,
        fields: HashMap::new(),
        serial_number: None,
        revocation_outpoint: None,
        signature: None,
        keyring_revealer: None,
        keyring_for_subject: None,
        privileged: None,
        privileged_reason: None,
        certifier_url: Some(url),
    }
}

fn assert_wallet_error(result: bsv_rs::Result<WalletCertificate>, expected: &str) {
    match result {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(message, expected),
        other => panic!("expected WalletError({expected:?}), got {other:?}"),
    }
}

#[tokio::test]
async fn test_issuance_bad_certifier_refuses_before_protocol() {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let request = server.mock("POST", "/issue").expect(0).create_async().await;
    // ts-stack@edf6e03 validationHelpers.ts:185-191,522-534 and
    // WERR_INVALID_PARAMETER.ts:14 supply the exact parameter and rule text.
    for (value, rule) in [
        (String::new(), "hexadecimal string."),
        ("g".repeat(66), "hexadecimal string."),
        ("0".repeat(65), "even length, not 65."),
        ("aa".repeat(32), "at least 66 length."),
        (format!("04{}", "00".repeat(64)), "no more than 66 length."),
        (
            format!("04{}", "00".repeat(32)),
            "a valid compressed secp256k1 public key",
        ),
        (
            format!("02{}", "00".repeat(32)),
            "a valid compressed secp256k1 public key",
        ),
        (
            format!("\u{0085}{}\u{0085}", certifier()),
            "hexadecimal string.",
        ),
    ] {
        let result = wallet
            .acquire_certificate(args(value, format!("{}/issue", server.url())), ORIGINATOR)
            .await;
        assert_wallet_error(
            result,
            &format!("Validation error: The certifier parameter must be {rule}"),
        );
    }
    request.assert_async().await;
}

#[tokio::test]
async fn test_issuance_bad_certifier_precedes_identity() {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let request = server.mock("POST", "/issue").expect(0).create_async().await;
    let invalid = args("gg".to_string(), format!("{}/issue", server.url()));
    // Poison the later identity parse so running it before the certifier
    // check changes the observed refusal. The type is left valid: the
    // reference validates type (validationHelpers.ts:1338) before certifier
    // (:1339), which #271 ports.
    let result = acquire_certificate_issuance(
        &wallet,
        wallet.storage(),
        &AuthId::with_user_id(wallet.identity_key().to_string(), 1),
        invalid,
        "not a public key",
        ORIGINATOR,
    )
    .await
    .map_err(Into::into);
    assert_wallet_error(
        result,
        "Validation error: The certifier parameter must be hexadecimal string.",
    );
    request.assert_async().await;
}

async fn normalized_certifier_reaches_response_checks(value: String) {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let canonical = certifier();
    let issued_type = BASE64.encode([8; 32]);
    let response = ProtocolIssuanceResponse {
        protocol: "certificate issuance".to_string(),
        certificate: Some(CertificateResponse {
            cert_type: issued_type.clone(),
            serial_number: BASE64.encode([9; 32]),
            subject: wallet.identity_key().to_string(),
            certifier: canonical.clone(),
            revocation_outpoint: String::new(),
            fields: HashMap::new(),
            signature: String::new(),
        }),
        server_nonce: BASE64.encode([5; 32]),
        timestamp: String::new(),
        version: "1.0".to_string(),
    };
    let request = server
        .mock("POST", "/issue")
        .match_header(HEADER_IDENTITY_KEY, wallet.identity_key())
        .with_header(HEADER_IDENTITY_KEY, &canonical)
        .with_body(serde_json::to_string(&response).unwrap())
        .expect(1)
        .create_async()
        .await;
    // A deliberately different type witnesses acceptance through preparation,
    // transport, and both canonical certifier comparisons, before verification.
    let result = wallet
        .acquire_certificate(args(value, format!("{}/issue", server.url())), ORIGINATOR)
        .await;
    assert_wallet_error(
        result,
        &format!(
            "Validation error: Invalid certificate type! Expected: {}, Received: {issued_type}",
            BASE64.encode([7; 32])
        ),
    );
    request.assert_async().await;
}

#[tokio::test]
async fn test_issuance_uppercase_certifier_reaches_response_checks() {
    normalized_certifier_reaches_response_checks(certifier().to_ascii_uppercase()).await;
}

#[tokio::test]
async fn test_issuance_padded_certifier_reaches_response_checks() {
    for padding in [" \t\r\n", "\u{feff}"] {
        normalized_certifier_reaches_response_checks(format!("{padding}{}{padding}", certifier()))
            .await;
    }
}

#[tokio::test]
async fn test_issuance_header_certifier_mismatch_uses_reference_text() {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let unexpected = wallet.identity_key();
    let request = server
        .mock("POST", "/issue")
        .with_header(HEADER_IDENTITY_KEY, unexpected)
        .with_body(r#"{"serverNonce":"fixture","certificate":null}"#)
        .expect(1)
        .create_async()
        .await;
    let result = wallet
        .acquire_certificate(
            args(certifier(), format!("{}/issue", server.url())),
            ORIGINATOR,
        )
        .await;
    // ts-stack@edf6e03 Wallet.ts:789-791 uses this text for a differing header.
    assert_wallet_error(
        result,
        &format!(
            "Validation error: Invalid certifier! Expected: {}, Received: {unexpected}",
            certifier()
        ),
    );
    request.assert_async().await;
}
