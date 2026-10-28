//! #261 and #271: issuance argument order, normalization, and response texts.

use super::peer_fixture::{Answer, PeerCertifier};
use super::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{AcquisitionProtocol, CreateHmacArgs, ProtoWallet};
use std::sync::{Arc, Mutex};

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

// The certifier's key on a fixture server: it answers the BRC-104 handshake.
async fn signing_certifier(server: &mut mockito::ServerGuard) -> PeerCertifier {
    let certifier = PeerCertifier::new(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap());
    certifier.handshake(server).await;
    certifier
}

// The test wallet's ProtoWallet: the subject's key, as Wallet::acquire_certificate passes it.
fn subject_proto() -> ProtoWallet {
    ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{:064x}", 37)).unwrap()))
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

async fn assert_type_refusals(certifier: &str, cases: Vec<(String, &str)>) {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let request = server.mock("POST", "/issue").expect(0).create_async().await;
    for (value, rule) in cases {
        let mut invalid = args(certifier.to_string(), format!("{}/issue", server.url()));
        invalid.certificate_type = value;
        assert_wallet_error(
            acquire_from_fixture(&wallet, invalid).await,
            &format!("Validation error: The type parameter must be {rule}"),
        );
    }
    request.assert_async().await;
}

#[tokio::test]
async fn test_issuance_bad_type_refuses_before_protocol() {
    // validationHelpers.ts:439-497,1338: structure before the 32-byte bounds.
    assert_type_refusals(
        &certifier(),
        vec![
            (String::new(), "valid base64 string"),
            ("!".to_string(), "valid base64 string"),
            ("A".to_string(), "valid base64 string"),
            ("A=AA".to_string(), "valid base64 string"),
            ("AA=A".to_string(), "at least 32 bytes"),
            ("A===".to_string(), "valid base64 string"),
            ("=AAA".to_string(), "valid base64 string"),
            ("A".repeat(43), "valid base64 string"),
            (format!("{}==", "A".repeat(42)), "at least 32 bytes"),
            (BASE64.encode([7; 31]), "at least 32 bytes"),
            (BASE64.encode([7; 33]), "no more than 32 bytes"),
            (
                format!("\u{0085}{}\u{0085}", BASE64.encode([7; 32])),
                "valid base64 string",
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_issuance_bad_type_precedes_certifier() {
    // Both arguments are bad; validationHelpers.ts:1338 must win over :1339.
    assert_type_refusals(
        "gg",
        vec![
            ("!".to_string(), "valid base64 string"),
            (BASE64.encode([7; 31]), "at least 32 bytes"),
            (BASE64.encode([7; 33]), "no more than 32 bytes"),
        ],
    )
    .await;
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
        let result =
            acquire_from_fixture(&wallet, args(value, format!("{}/issue", server.url()))).await;
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
        subject_proto(),
        wallet.storage(),
        &AuthId::with_user_id(wallet.identity_key().to_string(), 1),
        invalid,
        "not a public key",
        Some(ORIGINATOR),
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
    normalized_args_reach_response_checks(args(value, String::new()), &BASE64.encode([7; 32]))
        .await;
}

async fn normalized_args_reach_response_checks(
    mut acquisition: AcquireCertificateArgs,
    expected_type: &str,
) {
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
    let request = signing_certifier(&mut server)
        .await
        .general(&mut server, "/issue", 200, Answer::Signed, move |request| {
            let request: ProtocolIssuanceRequest =
                serde_json::from_slice(request.body().unwrap()).unwrap();
            let signer =
                ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap()));
            let mut response = response.clone();
            let mut data = BASE64.decode(&request.client_nonce).unwrap();
            data.extend(BASE64.decode(&response.server_nonce).unwrap());
            let serial = signer
                .create_hmac(CreateHmacArgs {
                    data,
                    protocol_id: Protocol::new(
                        SecurityLevel::Counterparty,
                        CERTIFICATE_ISSUANCE_PROTOCOL,
                    ),
                    key_id: format!("{}{}", response.server_nonce, request.client_nonce),
                    counterparty: Some(Counterparty::Other(
                        PublicKey::from_hex(&response.certificate.as_ref().unwrap().subject)
                            .unwrap(),
                    )),
                    ..Default::default()
                })
                .unwrap()
                .hmac;
            response.certificate.as_mut().unwrap().serial_number = BASE64.encode(serial);
            serde_json::to_vec(&response).unwrap()
        })
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "type": expected_type,
        })))
        .expect(1)
        .create_async()
        .await;
    // A deliberately different type witnesses acceptance through preparation,
    // transport, and a valid serial HMAC before the metadata comparison.
    acquisition.certifier_url = Some(format!("{}/issue", server.url()));
    let result = acquire_from_fixture(&wallet, acquisition).await;
    assert_wallet_error(
        result,
        &format!(
            "Validation error: Invalid certificate type! Expected: {}, Received: {issued_type}",
            expected_type
        ),
    );
    request.assert_async().await;
}

#[tokio::test]
async fn test_issuance_type_normalizes_before_protocol() {
    let canonical = BASE64.encode([7; 32]);
    // The reference trims ECMAScript whitespace but does not check padding bits.
    let noncanonical = format!("{}B=", "A".repeat(42));
    let embedded_padding = format!("{}=A", "A".repeat(42));
    for (value, expected) in [
        (canonical.clone(), canonical.clone()),
        (format!(" \t\r\n{canonical} \t\r\n"), canonical.clone()),
        (format!("\u{feff}{canonical}\u{feff}"), canonical),
        (noncanonical.clone(), noncanonical),
        (embedded_padding.clone(), embedded_padding),
    ] {
        let mut acquisition = args(certifier(), String::new());
        acquisition.certificate_type = value;
        normalized_args_reach_response_checks(acquisition, &expected).await;
    }
}

async fn mismatched_response(field: Option<&str>) -> (bsv_rs::Result<WalletCertificate>, String) {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let mut acquisition = args(certifier(), format!("{}/issue", server.url()));
    if let Some(field) = field {
        acquisition
            .fields
            .insert(field.to_string(), "value".to_string());
    }
    let field = field.map(str::to_string);
    let expected = Arc::new(Mutex::new(String::new()));
    let response_expected = Arc::clone(&expected);
    let subject = PublicKey::from_hex(wallet.identity_key()).unwrap();
    let certifier_key = PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap();
    let signer = ProtoWallet::new(Some(certifier_key));
    let request = signing_certifier(&mut server)
        .await
        .general(&mut server, "/issue", 200, Answer::Signed, move |request| {
            let issued: ProtocolIssuanceRequest =
                serde_json::from_slice(request.body().unwrap()).unwrap();
            let server_nonce = BASE64.encode([5; 32]);
            let mut data = BASE64.decode(&issued.client_nonce).unwrap();
            data.extend_from_slice(&[5; 32]);
            let serial = signer
                .create_hmac(CreateHmacArgs {
                    data,
                    protocol_id: Protocol::new(
                        SecurityLevel::Counterparty,
                        CERTIFICATE_ISSUANCE_PROTOCOL,
                    ),
                    key_id: format!("{}{}", server_nonce, issued.client_nonce),
                    counterparty: Some(Counterparty::Other(subject.clone())),
                    ..Default::default()
                })
                .unwrap()
                .hmac;
            let mut fields = issued.fields;
            let revocation_outpoint = if let Some(field) = &field {
                let received = "dGFtcGVyZWQ=".to_string();
                let sent = fields.insert(field.clone(), received.clone()).unwrap();
                *response_expected.lock().unwrap() = format!(
                    "Validation error: Invalid field! Expected: {sent}, Received: {received}"
                );
                format!("{}.0", "00".repeat(32))
            } else {
                *response_expected.lock().unwrap() =
                    "Validation error: Invalid revocationOutpoint!".to_string();
                String::new()
            };
            serde_json::to_vec(&ProtocolIssuanceResponse {
                protocol: "certificate issuance".to_string(),
                certificate: Some(CertificateResponse {
                    cert_type: issued.cert_type,
                    serial_number: BASE64.encode(serial),
                    subject: subject.to_hex(),
                    certifier: certifier(),
                    revocation_outpoint,
                    fields,
                    signature: String::new(),
                }),
                server_nonce,
                timestamp: String::new(),
                version: "1.0".to_string(),
            })
            .unwrap()
        })
        .expect(1)
        .create_async()
        .await;
    let result = acquire_from_fixture(&wallet, acquisition).await;
    request.assert_async().await;
    let expected = expected.lock().unwrap().clone();
    (result, expected)
}

#[tokio::test]
async fn test_issuance_revocation_outpoint_uses_reference_text() {
    let (result, expected) = mismatched_response(None).await;
    assert_wallet_error(result, &expected);
}

#[tokio::test]
async fn test_issuance_field_mismatch_uses_reference_text() {
    for field in ["city", "name"] {
        let (result, expected) = mismatched_response(Some(field)).await;
        assert_wallet_error(result, &expected);
    }
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

#[cfg(test)]
#[path = "certificate_issuance_guard_tests.rs"]
mod guard_tests;

#[cfg(test)]
#[path = "certificate_issuance_response_tests.rs"]
mod response_tests;

#[cfg(test)]
#[path = "certificate_issuance_nonce_tests.rs"]
mod nonce_tests;

async fn acquire_from_fixture(
    wallet: &TestWallet,
    mut acquisition: AcquireCertificateArgs,
) -> bsv_rs::Result<WalletCertificate> {
    let fixture_url = acquisition.certifier_url.take().unwrap();
    acquisition.certifier_url = Some("https://issuer.example/issue".to_string());
    ISSUANCE_TEST_URL
        .scope(
            fixture_url,
            wallet.acquire_certificate(acquisition, Some(ORIGINATOR)),
        )
        .await
}
