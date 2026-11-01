//! Certificate Issuance Protocol Implementation
//!
//! This module implements the certificate issuance protocol for acquiring identity
//! certificates from a certifier service. It ports the request guards and response checks from the pinned TypeScript
//! implementation while retaining the SDK and transport boundaries of this crate.
//!
//! ## Protocol Flow
//!
//! 1. **PrepareIssuanceActionData**: Create the client nonce as createNonce, encrypt fields using
//!    MasterCertificate::create_certificate_fields, build JSON request body
//!
//! 2. **BRC-104 request** (`certificate_issuance_auth.rs`, #315): a handshake
//!    with the certifier, then the request as a signed general message; the
//!    answer's signature and session are verified by bsv-rs's Peer, and its
//!    signed sender must equal the certifier before the body is read.
//!
//! 3. **ParseCertificateResponse**: Parse JSON response, extract certificate
//!    components
//!
//! 4. **VerifyCertificateIssuance**: Check the server nonce, verify the serial
//!    HMAC, validate metadata and fields, verify the signature, decrypt fields
//!
//! 5. **StoreCertificate**: Save certificate to storage with encrypted fields
//!    and master keyring

use indexmap::IndexMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::auth::certificates::Certificate;
use bsv_rs::primitives::{PublicKey, SymmetricKey};
use bsv_rs::wallet::{
    AcquireCertificateArgs, Counterparty, CreateHmacArgs, ProtoWallet, Protocol, SecurityLevel,
    VerifyHmacArgs, WalletCertificate, WalletInterface,
};
use serde::{Deserialize, Serialize};

use super::wallet::{
    invalid_certificate_argument, trim_ecmascript_whitespace, validate_certificate_base64_arg,
    validate_public_key_arg,
};
use crate::error::{Error, Result};
use crate::services::providers::http_client::Url;
use crate::storage::entities::{TableCertificate, TableCertificateField};
use crate::storage::{not_busy, AuthId, WalletStorageProvider};

#[cfg(test)]
#[path = "certificate_issuance_network_isolation_tests.rs"]
mod network_isolation_tests;

#[cfg(test)]
#[path = "certificate_issuance_peer_fixture.rs"]
mod peer_fixture;

#[path = "certificate_issuance_auth.rs"]
mod auth;

#[cfg(test)]
tokio::task_local! {
    // Route a validated public URL to a loopback fixture within one test future.
    // Argument validation always runs, and production has no override.
    static ISSUANCE_TEST_URL: String;
}

// =============================================================================
// Constants
// =============================================================================

/// Protocol for certificate issuance HMAC verification
const CERTIFICATE_ISSUANCE_PROTOCOL: &str = "certificate issuance";

/// Expected HMAC size (32 bytes)
const NONCE_HMAC_SIZE: usize = 32;

/// The client nonce's random part in bytes (createNonce.ts:27)
const NONCE_SIZE: usize = 16;

/// verifyHmac's keyID bound in UTF-8 bytes (validationHelpers.ts:227)
const MAX_KEY_ID_BYTES: usize = 800;

// =============================================================================
// Request/Response Types
// =============================================================================

/// Certificate signing request sent to the certifier
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolIssuanceRequest {
    /// Certificate type (base64 encoded)
    #[serde(rename = "type")]
    pub cert_type: String,
    /// Client nonce (base64 encoded)
    pub client_nonce: String,
    /// Encrypted field values
    pub fields: IndexMap<String, String>,
    /// Master keyring for field encryption
    pub master_keyring: IndexMap<String, String>,
}

/// Response from the certifier containing the signed certificate
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolIssuanceResponse {
    /// Protocol identifier
    #[serde(default)]
    pub protocol: String,
    /// The signed certificate
    pub certificate: Option<CertificateResponse>,
    /// Server nonce (base64 encoded)
    #[serde(default)]
    pub server_nonce: String,
    /// Timestamp of issuance
    #[serde(default)]
    pub timestamp: String,
    /// Protocol version
    #[serde(default)]
    pub version: String,
}

/// Certificate as returned by the certifier in the issuance protocol response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateResponse {
    /// Certificate type (base64 encoded)
    #[serde(rename = "type")]
    pub cert_type: String,
    /// Serial number (base64 encoded)
    pub serial_number: String,
    /// Subject public key (hex)
    pub subject: String,
    /// Certifier public key (hex)
    pub certifier: String,
    /// Revocation outpoint (txid.vout format)
    #[serde(default)]
    pub revocation_outpoint: String,
    /// Encrypted field values
    pub fields: IndexMap<String, String>,
    /// Signature (hex encoded)
    pub signature: String,
}

// =============================================================================
// Internal Helper Types
// =============================================================================

/// Result of preparing issuance action data
pub(crate) struct PrepareIssuanceActionDataResult {
    /// JSON request body
    pub body: Vec<u8>,
    /// Encrypted fields
    pub fields: IndexMap<String, String>,
    /// Master keyring
    pub master_keyring: IndexMap<String, String>,
    /// Certificate type (base64 encoded)
    pub cert_type_b64: String,
    /// Client nonce (base64 encoded)
    pub client_nonce: String,
}

/// Result of parsing certificate response
#[derive(Debug)]
pub(crate) struct ParseCertificateResponseResult {
    /// Original certificate text, retained through validation and storage.
    pub certificate: CertificateResponse,
    /// Server nonce (base64 encoded).
    pub server_nonce: String,
}

// =============================================================================
// Main Entry Point
// =============================================================================

/// Executes the certificate issuance protocol.
///
/// This function implements the full issuance flow:
/// 1. Prepare encrypted fields and request body
/// 2. Send HTTP POST to certifier
/// 3. Parse and validate response
/// 4. Verify certificate via HMAC and signature
/// 5. Store certificate in storage
///
/// # Arguments
/// * `wallet` - The wallet implementing WalletInterface for crypto operations
/// * `peer_wallet` - The wallet's `ProtoWallet`, the identity the certifier is sent
/// * `storage` - Storage provider for persisting the certificate
/// * `auth` - Authentication context for storage operations
/// * `args` - Certificate acquisition arguments
/// * `identity_key` - Wallet's identity public key (hex)
/// * `originator` - Application originator string
///
/// # Returns
/// The acquired certificate on success
pub async fn acquire_certificate_issuance<W, S>(
    wallet: &W,
    peer_wallet: ProtoWallet,
    storage: &S,
    auth: &AuthId,
    mut args: AcquireCertificateArgs,
    identity_key: &str,
    originator: Option<&str>,
) -> Result<WalletCertificate>
where
    W: WalletInterface + Send + Sync,
    S: WalletStorageProvider + Send + Sync,
{
    let (certifier_hex, certifier) = validate_issuance_args(&mut args)?;
    let certifier_url = args.certifier_url.as_deref().unwrap_or_default();

    // Parse identity key to PublicKey
    let identity_pub_key = PublicKey::from_hex(identity_key)
        .map_err(|e| Error::ValidationError(format!("Invalid identity key: {}", e)))?;

    // Step 1: Prepare issuance action data
    let prepared =
        prepare_issuance_action_data(wallet, &args, &certifier, &identity_pub_key, originator)
            .await?;

    // Step 2: The BRC-104 request; the signed sender is checked before the body
    let response =
        send_issuance_request(peer_wallet, certifier_url, &prepared.body, &certifier_hex).await?;

    // Step 3: Parse and validate response
    let parsed = parse_certificate_response(&response)?;

    // Step 4: Verify certificate issuance
    verify_certificate_issuance(
        wallet,
        &parsed,
        &prepared.client_nonce,
        &prepared.fields,
        &prepared.cert_type_b64,
        identity_key,
        &certifier_hex,
        &certifier,
        originator,
    )
    .await?;

    decrypt_issued_fields(
        wallet,
        &parsed.certificate.fields,
        &prepared.master_keyring,
        &certifier,
        originator,
    )
    .await?;

    // Step 5: Store certificate
    let wallet_cert = store_certificate(
        storage,
        auth,
        &parsed,
        &prepared.master_keyring,
        &certifier,
        identity_key,
        &prepared.cert_type_b64,
    )
    .await?;

    Ok(wallet_cert)
}

// The Rust argument types provide the record, boolean, and string shape checks.
// The remaining guards follow validationHelpers.ts:1307-1349 in order.
fn validate_issuance_args(args: &mut AcquireCertificateArgs) -> Result<(String, PublicKey)> {
    if let Some(reason) = &args.privileged_reason {
        validate_issuance_string(reason, "privilegedReason", 5, 50)?;
    } else if args.privileged == Some(true) {
        return Err(invalid_certificate_argument(
            "privilegedReason",
            "a value when 'privileged' is true",
        )
        .into());
    }
    if args.acquisition_protocol != bsv_rs::wallet::AcquisitionProtocol::Issuance {
        return Err(Error::ValidationError(
            "Only acquire certificate via issuance requests allowed here.".into(),
        ));
    }
    for (name, present) in [
        (
            "serialNumber",
            args.serial_number.as_deref().is_some_and(|v| !v.is_empty()),
        ),
        (
            "signature",
            args.signature.as_deref().is_some_and(|v| !v.is_empty()),
        ),
        (
            "revocationOutpoint",
            args.revocation_outpoint
                .as_deref()
                .is_some_and(|v| !v.is_empty()),
        ),
        ("keyringRevealer", args.keyring_revealer.is_some()),
        ("keyringForSubject", args.keyring_for_subject.is_some()),
    ] {
        if present {
            return Err(invalid_certificate_argument(
                name,
                "valid when acquisitionProtocol is \"direct\"",
            )
            .into());
        }
    }
    let url = args
        .certifier_url
        .as_deref()
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            invalid_certificate_argument(
                "certifierUrl",
                "valid when acquisitionProtocol is \"issuance\"",
            )
        })?;
    args.certificate_type = validate_certificate_base64_arg(&args.certificate_type, "type")?;
    let certifier = validate_public_key_arg(&args.certifier, "certifier")?;
    args.certifier_url = Some(validate_issuance_url(url)?);
    validate_issuance_fields(&args.fields)?;
    args.privileged = Some(args.privileged.unwrap_or(false));
    // The repeated privilege checks at :1342-1348 cannot refuse after :1308.
    Ok(certifier)
}

fn validate_issuance_string(value: &str, name: &str, min: usize, max: usize) -> Result<()> {
    if value.len() < min {
        return Err(invalid_certificate_argument(name, &format!("at least {min} length.")).into());
    }
    if value.len() > max {
        return Err(
            invalid_certificate_argument(name, &format!("no more than {max} length.")).into(),
        );
    }
    Ok(())
}

fn validate_issuance_url(value: &str) -> Result<String> {
    let invalid = |rule| Error::from(invalid_certificate_argument("certifierUrl", rule));
    validate_issuance_string(value, "certifierUrl", 1, 2048)?;
    if value != trim_ecmascript_whitespace(value) {
        return Err(invalid("an exact HTTPS URL without whitespace"));
    }
    let url = Url::parse(value).map_err(|_| invalid("an absolute HTTPS URL"))?;
    if url.scheme() != "https" {
        return Err(invalid("an HTTPS URL"));
    }
    if !url.username().is_empty() || url.password().is_some_and(|p| !p.is_empty()) {
        return Err(invalid("a URL without credentials"));
    }
    if url.query().is_some_and(|q| !q.is_empty()) || url.fragment().is_some_and(|f| !f.is_empty()) {
        return Err(invalid("a URL without a query or fragment"));
    }
    let hostname = url.host_str().unwrap_or_default();
    let valid_label = |label: &str| {
        !label.is_empty()
            && label.len() <= 63
            && label.as_bytes()[0].is_ascii_alphanumeric()
            && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    };
    if hostname == "localhost"
        || hostname.ends_with(".localhost")
        || hostname.contains(':')
        || hostname.parse::<std::net::IpAddr>().is_ok()
        || !hostname.contains('.')
        || !hostname.split('.').all(valid_label)
    {
        return Err(invalid("a public DNS hostname"));
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn validate_issuance_fields(fields: &IndexMap<String, String>) -> Result<()> {
    if fields.len() > 100_000 {
        return Err(invalid_certificate_argument(
            "fields",
            "an object with at most 100000 properties",
        )
        .into());
    }
    if fields
        .keys()
        .any(|key| matches!(key.as_str(), "__proto__" | "constructor" | "prototype"))
    {
        return Err(
            invalid_certificate_argument("fields", "an object with safe string keys").into(),
        );
    }
    for name in fields.keys() {
        validate_issuance_string(name, "field name", 1, 50)?;
    }
    Ok(())
}

// =============================================================================
// Step 1: Prepare Issuance Action Data
// =============================================================================

/// Prepares the certificate signing request payload.
///
/// Generates a random nonce, encrypts each field with a fresh symmetric key,
/// and wraps those keys for the certifier.
async fn prepare_issuance_action_data<W>(
    wallet: &W,
    args: &AcquireCertificateArgs,
    certifier: &PublicKey,
    subject: &PublicKey,
    originator: Option<&str>,
) -> Result<PrepareIssuanceActionDataResult>
where
    W: WalletInterface + Send + Sync,
{
    let client_nonce = create_client_nonce(wallet, certifier, originator).await?;

    // The acquisition gate has validated and trimmed the type. Preserve its
    // text in the request, as Wallet.ts:784 does, without a stricter decoder.
    let cert_type_b64 = args.certificate_type.clone();

    // Create field ciphertexts and wrap their keys from subject to certifier.
    let (encrypted_fields, master_keyring) = create_certificate_fields_for_issuance(
        wallet,
        certifier,
        subject,
        &args.fields,
        originator,
    )
    .await?;

    // Convert to string maps for JSON serialization
    let fields: IndexMap<String, String> = encrypted_fields
        .into_iter()
        .map(|(k, v)| (k, BASE64.encode(&v)))
        .collect();

    let master_keyring: IndexMap<String, String> = master_keyring
        .into_iter()
        .map(|(k, v)| (k, BASE64.encode(&v)))
        .collect();

    // Build JSON request body
    let request = ProtocolIssuanceRequest {
        cert_type: cert_type_b64.clone(),
        client_nonce: client_nonce.clone(),
        fields: fields.clone(),
        master_keyring: master_keyring.clone(),
    };

    let body = serde_json::to_vec(&request).map_err(|e| {
        Error::ValidationError(format!("Failed to serialize issuance request: {}", e))
    })?;

    Ok(PrepareIssuanceActionDataResult {
        body,
        fields,
        master_keyring,
        cert_type_b64,
        client_nonce,
    })
}

// createNonce.ts:21-44 as Wallet.ts:771 calls it: 16 OS bytes (#558), their
// `[2, 'server hmac']` hmac with the certifier under the TextDecoder text of
// those bytes, base64 of the 48 bytes (64 characters, no padding). The SDK
// helper uses another security level and key ID, so it cannot be substituted.
async fn create_client_nonce<W: WalletInterface + Send + Sync>(
    wallet: &W,
    certifier: &PublicKey,
    originator: Option<&str>,
) -> Result<String> {
    let mut random = [0u8; NONCE_SIZE];
    crate::rng::os_fill(&mut random);
    let hmac = wallet
        .create_hmac(
            CreateHmacArgs {
                data: random.to_vec(),
                protocol_id: Protocol::new(SecurityLevel::Counterparty, "server hmac"),
                key_id: nonce_key_id(&random),
                counterparty: Some(Counterparty::Other(certifier.clone())),
                ..Default::default()
            },
            originator,
        )
        .await?
        .hmac;
    Ok(BASE64.encode([random.as_slice(), &hmac].concat()))
}

// utils.ts:211-213: toUTF8 is TextDecoder().decode, one U+FFFD per maximal
// subpart (as from_utf8_lossy) and one leading BOM removed.
fn nonce_key_id(random: &[u8]) -> String {
    let text = String::from_utf8_lossy(random);
    text.strip_prefix('\u{feff}').unwrap_or(&text).to_string()
}

/// Creates encrypted certificate fields for issuance.
///
/// Each field is encrypted with its own random symmetric key. The subject
/// wallet encrypts that key for the certifier to form the master keyring.
async fn create_certificate_fields_for_issuance<W>(
    wallet: &W,
    certifier: &PublicKey,
    _subject: &PublicKey,
    plain_fields: &IndexMap<String, String>,
    originator: Option<&str>,
) -> Result<(IndexMap<String, Vec<u8>>, IndexMap<String, Vec<u8>>)>
where
    W: WalletInterface + Send + Sync,
{
    use bsv_rs::wallet::EncryptArgs;

    let protocol = Protocol::new(SecurityLevel::Counterparty, "certificate field encryption");

    let mut encrypted_fields = IndexMap::new();
    let mut master_keyring = IndexMap::new();

    for (field_name, plain_value) in plain_fields {
        let field_key = SymmetricKey::random();
        let ciphertext = field_key.encrypt(plain_value.as_bytes()).map_err(|e| {
            Error::ValidationError(format!("Failed to encrypt field '{field_name}': {e}"))
        })?;

        // Key ID for master encryption is just the field name
        let key_id = field_name.clone();

        // Encrypt the field key from subject to certifier, as in
        // ts-stack@edf6e03 MasterCertificate.ts:277-293.
        let encrypt_result = wallet
            .encrypt(
                EncryptArgs {
                    plaintext: field_key.as_bytes().to_vec(),
                    protocol_id: protocol.clone(),
                    key_id: key_id.clone(),
                    counterparty: Some(Counterparty::Other(certifier.clone())),
                    ..Default::default()
                },
                originator,
            )
            .await
            .map_err(|e| {
                Error::ValidationError(format!("Failed to encrypt field key '{field_name}': {e}"))
            })?;

        encrypted_fields.insert(field_name.clone(), ciphertext);
        master_keyring.insert(field_name.clone(), encrypt_result.ciphertext);
    }

    Ok((encrypted_fields, master_keyring))
}

// =============================================================================
// Step 2: Send HTTP Request
// =============================================================================

/// Sends the issuance request to the certifier over BRC-104 mutual auth.
///
/// The answer's sender, which bsv-rs's Peer has bound to a signed session,
/// must equal `certifier_hex` before the body is read, as `AuthFetch.ts:479`
/// sets the identity header from it and ts-stack@edf6e03 `Wallet.ts:789-791`
/// compares it. No status gate: `Wallet.ts:779-795` reads any signed body.
async fn send_issuance_request(
    peer_wallet: ProtoWallet,
    certifier_url: &str,
    body: &[u8],
    certifier_hex: &str,
) -> Result<ProtocolIssuanceResponse> {
    #[cfg(test)]
    let fixture_url = ISSUANCE_TEST_URL.try_with(Clone::clone).ok();
    #[cfg(test)]
    let certifier_url = fixture_url.as_deref().unwrap_or(certifier_url);

    let (sender, response) =
        auth::post_authenticated(peer_wallet, certifier_url, body, auth::RESPONSE_DEADLINE).await?;
    let sender = sender.to_hex();
    if sender != certifier_hex {
        return Err(Error::ValidationError(format!(
            "Invalid certifier! Expected: {certifier_hex}, Received: {sender}"
        )));
    }

    serde_json::from_slice(&response.body).map_err(|e| {
        Error::ValidationError(format!(
            "Failed to parse certifier response: {} - body: {}",
            e,
            String::from_utf8_lossy(&response.body)
        ))
    })
}

// =============================================================================
// Step 3: Parse Certificate Response
// =============================================================================

/// Parses the envelope without imposing checks ahead of Wallet.ts:806-817.
fn parse_certificate_response(
    response: &ProtocolIssuanceResponse,
) -> Result<ParseCertificateResponseResult> {
    let certificate = response
        .certificate
        .as_ref()
        .ok_or_else(|| Error::ValidationError("No certificate received from certifier!".into()))?;
    if response.server_nonce.is_empty() {
        return Err(Error::ValidationError(
            "No serverNonce received from certifier!".into(),
        ));
    }
    Ok(ParseCertificateResponseResult {
        certificate: certificate.clone(),
        server_nonce: response.server_nonce.clone(),
    })
}

// Utils.toArray(..., 'base64') accepts omitted padding, URL-safe characters,
// ASCII whitespace and nonzero padding bits. Keep the original text separately.
fn decode_issuance_base64(value: &str) -> Result<Vec<u8>> {
    let mut encoded: String = trim_ecmascript_whitespace(value)
        .chars()
        .filter(|c| !matches!(c, '\r' | '\n' | '\t' | '\u{000c}' | '\u{000b}' | ' '))
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            c => c,
        })
        .collect();
    if let Some(index) = encoded.find('=') {
        if !matches!(&encoded[index..], "=" | "==") {
            return Err(Error::ValidationError("Invalid base64 padding".into()));
        }
        encoded.truncate(index);
    }
    let mut decoded = Vec::with_capacity(encoded.len() * 3 / 4);
    let (mut buffer, mut bits) = (0u32, 0u32);
    for (index, byte) in encoded.bytes().enumerate() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => {
                return Err(Error::ValidationError(format!(
                    "Invalid base64 character at index {index}"
                )))
            }
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            decoded.push((buffer >> bits).to_le_bytes()[0]);
            buffer &= (1 << bits) - 1;
        }
    }
    Ok(decoded)
}

// WalletByteEncoding.base64ToBytes: canonical padding and zero padding bits only.
fn decode_canonical_base64_32(value: &str) -> Option<[u8; 32]> {
    let bytes = BASE64.decode(value).ok()?;
    if BASE64.encode(&bytes) != value {
        return None;
    }
    bytes.try_into().ok()
}

// Certificate.ts:260-270: `txid` or `txid.vout`, 64 hex digits, a decimal index
// without leading zeros up to u32::MAX; a txid-only text is vout 0.
fn parse_revocation_outpoint(text: &str) -> Option<bsv_rs::wallet::Outpoint> {
    let mut parts = text.split('.');
    let txid = parts.next()?;
    let vout = parts.next().unwrap_or("0");
    if parts.next().is_some()
        || txid.len() != 64
        || vout.is_empty()
        || !vout.bytes().all(|b| b.is_ascii_digit())
        || (vout.len() > 1 && vout.starts_with('0'))
    {
        return None;
    }
    let txid = hex::decode(txid).ok()?.try_into().ok()?;
    Some(bsv_rs::wallet::Outpoint::new(txid, vout.parse().ok()?))
}

// Certificate.verify (Certificate.ts:425-441) answers false on any toBinary
// throw; Wallet.ts:816 turns that into one text.
fn decode_issued_certificate(raw: &CertificateResponse) -> Result<Certificate> {
    let invalid = || Error::ValidationError("Certificate verification failed".into());
    // toBinary's assertCanonicalBase64Length (Certificate.ts:233, :237).
    let cert_type = decode_canonical_base64_32(&raw.cert_type).ok_or_else(invalid)?;
    let serial = decode_canonical_base64_32(&raw.serial_number).ok_or_else(invalid)?;
    let subject = certificate_key(&raw.subject).ok_or_else(invalid)?;
    let certifier = certificate_key(&raw.certifier).ok_or_else(invalid)?;
    let mut certificate = Certificate::new(cert_type, serial, subject, certifier);
    // The wire text is stored as answered; this parse is only for verify.
    certificate.revocation_outpoint =
        Some(parse_revocation_outpoint(&raw.revocation_outpoint).ok_or_else(invalid)?);
    for (name, value) in &raw.fields {
        certificate.set_field(name.clone(), value.clone());
    }
    certificate.signature = Some(hex::decode(&raw.signature).map_err(|_| invalid())?);
    Ok(certificate)
}

/// toBinary's subject and certifier (Certificate.ts:240-254): `02` or `03`
/// and 64 hex digits, then the 33 bytes as a compressed point. Since bsv-rs
/// 0.9.0-beta.5 `PublicKey::from_hex` reads the reference's lenient
/// `fromString` forms (bsv-rs #498: an odd-length string padded, a hybrid
/// key), which toBinary's pattern refuses, so the text is read strictly here.
/// The pattern also refuses an uncompressed `04` text, which `from_hex` read
/// at beta.4 as at beta.5: the reference's verdict, not the pin's.
fn certificate_key(key_hex: &str) -> Option<PublicKey> {
    let shaped = key_hex.len() == 66
        && matches!(key_hex.get(..2), Some("02" | "03"))
        && key_hex.bytes().all(|b| b.is_ascii_hexdigit());
    shaped
        .then(|| super::internalize_args::compressed_key(key_hex))
        .flatten()
}

async fn verify_issuance_serial<W: WalletInterface + Send + Sync>(
    wallet: &W,
    raw: &CertificateResponse,
    client_nonce: &str,
    server_nonce: &str,
    certifier: &PublicKey,
    originator: Option<&str>,
) -> Result<()> {
    let invalid = || Error::ValidationError("Invalid serialNumber".into());
    // Wallet.ts:808-809 decode the serial, then `clientNonce + serverNonce` as
    // one string; verifyHmac checks the keyID (validationHelpers.ts:227 via
    // :1772), then the hmac's 32 bytes (:1774). The keyID holds the non-empty
    // server nonce, so only its upper bound can refuse.
    let serial = decode_issuance_base64(&raw.serial_number)?;
    let data = decode_issuance_base64(&format!("{client_nonce}{server_nonce}"))?;
    let key_id = format!("{server_nonce}{client_nonce}");
    if key_id.len() > MAX_KEY_ID_BYTES {
        return Err(Error::ValidationError(format!(
            "The keyID parameter must be no more than {MAX_KEY_ID_BYTES} length."
        )));
    }
    if serial.len() != NONCE_HMAC_SIZE {
        return Err(Error::ValidationError(
            "The hmac parameter must be exactly 32 bytes".into(),
        ));
    }
    let hmac = serial.try_into().map_err(|_| invalid())?;
    let protocol = CERTIFICATE_ISSUANCE_PROTOCOL;
    let valid =
        verify_counterparty_hmac(wallet, hmac, data, protocol, key_id, certifier, originator)
            .await?;
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

// The level 2 verifyHmac both response checks make (Wallet.ts:806 through
// verifyNonce, and :810), with the certifier as counterparty.
async fn verify_counterparty_hmac<W: WalletInterface + Send + Sync>(
    wallet: &W,
    hmac: [u8; NONCE_HMAC_SIZE],
    data: Vec<u8>,
    protocol: &str,
    key_id: String,
    counterparty: &PublicKey,
    originator: Option<&str>,
) -> Result<bool> {
    let args = VerifyHmacArgs {
        hmac,
        data,
        protocol_id: Protocol::new(SecurityLevel::Counterparty, protocol),
        key_id,
        counterparty: Some(Counterparty::Other(counterparty.clone())),
        ..Default::default()
    };
    Ok(wallet.verify_hmac(args, originator).await?.valid)
}

fn validate_issued_certificate(
    raw: &CertificateResponse,
    sent_fields: &IndexMap<String, String>,
    cert_type: &str,
    identity: &str,
    certifier: &str,
) -> Result<()> {
    for (name, expected, received) in [
        ("certificate type", cert_type, raw.cert_type.as_str()),
        ("certificate subject", identity, raw.subject.as_str()),
        ("certifier", certifier, raw.certifier.as_str()),
    ] {
        if received != expected {
            return Err(Error::ValidationError(format!(
                "Invalid {name}! Expected: {expected}, Received: {received}"
            )));
        }
    }
    if raw.revocation_outpoint.is_empty() {
        return Err(Error::ValidationError("Invalid revocationOutpoint!".into()));
    }
    if raw.fields.len() != sent_fields.len() {
        return Err(Error::ValidationError(
            "Fields mismatch! Objects have different numbers of keys.".into(),
        ));
    }
    for (name, expected) in sent_fields {
        let received = raw.fields.get(name).ok_or_else(|| {
            Error::ValidationError(format!("Missing field: {name} in certificate.fields"))
        })?;
        if received != expected {
            return Err(Error::ValidationError(format!(
                "Invalid field! Expected: {expected}, Received: {received}"
            )));
        }
    }
    Ok(())
}

// verifyNonce.ts:26-57. Wallet.ts:806 ignores false, but propagates a wallet
// error. The SDK helper uses another protocol/key ID and a legacy size.
async fn verify_server_nonce<W: WalletInterface + Send + Sync>(
    wallet: &W,
    nonce: &str,
    certifier: &PublicKey,
    originator: Option<&str>,
) -> Result<bool> {
    let Ok(bytes) = BASE64.decode(nonce) else {
        return Ok(false);
    };
    if bytes.len() != NONCE_SIZE + NONCE_HMAC_SIZE || BASE64.encode(&bytes) != nonce {
        return Ok(false);
    }
    let mut hmac = [0; NONCE_HMAC_SIZE];
    hmac.copy_from_slice(&bytes[NONCE_SIZE..]);
    let key_id = nonce_key_id(&bytes[..NONCE_SIZE]);
    let data = bytes[..NONCE_SIZE].to_vec();
    let protocol = "server hmac";
    verify_counterparty_hmac(wallet, hmac, data, protocol, key_id, certifier, originator).await
}

/// Runs the response guards in Wallet.ts:806-816 order.
#[allow(clippy::too_many_arguments)]
async fn verify_certificate_issuance<W>(
    wallet: &W,
    parsed: &ParseCertificateResponseResult,
    client_nonce: &str,
    sent_fields: &IndexMap<String, String>,
    cert_type_b64: &str,
    identity_key: &str,
    certifier_hex: &str,
    certifier: &PublicKey,
    originator: Option<&str>,
) -> Result<()>
where
    W: WalletInterface + Send + Sync,
{
    // Wallet.ts:806 awaits the helper but does not branch on its boolean.
    verify_server_nonce(wallet, &parsed.server_nonce, certifier, originator).await?;
    verify_issuance_serial(
        wallet,
        &parsed.certificate,
        client_nonce,
        &parsed.server_nonce,
        certifier,
        originator,
    )
    .await?;
    validate_issued_certificate(
        &parsed.certificate,
        sent_fields,
        cert_type_b64,
        identity_key,
        certifier_hex,
    )?;
    // Wallet.ts:816: `if (!(await signedCertificate.verify())) throw`. bsv-rs
    // #299 answers false, never an error, for every failure the reference's
    // verify answers false to (absent, non-DER or wrong-signer signature).
    if !issued_certificate_verifies(&parsed.certificate) {
        return Err(Error::ValidationError(
            "Certificate verification failed".into(),
        ));
    }
    Ok(())
}

/// The reference's `Certificate.verify` (Certificate.ts:425-441) on a
/// certificate in its wire shape: false on any decode failure or a signature
/// that does not verify under BRC-52's derived key (bsv-rs #251, #299).
pub(super) fn issued_certificate_verifies(raw: &CertificateResponse) -> bool {
    decode_issued_certificate(raw)
        .and_then(|certificate| certificate.verify().map_err(Into::into))
        .unwrap_or(false)
}

// MasterCertificate.decryptFields at Wallet.ts:817 verifies that every returned
// field decrypts with the master keyring produced for this request.
async fn decrypt_issued_fields<W: WalletInterface + Send + Sync>(
    wallet: &W,
    fields: &IndexMap<String, String>,
    keyring: &IndexMap<String, String>,
    certifier: &PublicKey,
    originator: Option<&str>,
) -> Result<()> {
    // MasterCertificate.ts:522-524, outside the decrypt try at 525-561.
    if keyring.is_empty() {
        return Err(Error::ValidationError(
            "A MasterCertificate must have a valid masterKeyring!".into(),
        ));
    }
    let decrypt_all = async {
        for (name, field) in fields {
            let key = keyring.get(name).ok_or(())?;
            let result = wallet
                .decrypt(
                    bsv_rs::wallet::DecryptArgs {
                        ciphertext: BASE64.decode(key).map_err(|_| ())?,
                        protocol_id: Protocol::new(
                            SecurityLevel::Counterparty,
                            "certificate field encryption",
                        ),
                        key_id: name.clone(),
                        counterparty: Some(Counterparty::Other(certifier.clone())),
                        ..Default::default()
                    },
                    originator,
                )
                .await
                .map_err(|_| ())?;
            // Restore legacy minimal big-endian keys as MasterCertificate does.
            if result.plaintext.len() > 32 {
                return Err(());
            }
            let mut key_bytes = [0; 32];
            key_bytes[32 - result.plaintext.len()..].copy_from_slice(&result.plaintext);
            let ciphertext = BASE64.decode(field).map_err(|_| ())?;
            let plaintext = SymmetricKey::from_bytes(&key_bytes)
                .map_err(|_| ())?
                .decrypt(&ciphertext)
                .map_err(|_| ())?;
            std::str::from_utf8(&plaintext).map_err(|_| ())?;
        }
        Ok::<(), ()>(())
    };
    decrypt_all.await.map_err(|()| {
        Error::ValidationError("Failed to decrypt all master certificate fields.".into())
    })
}

// =============================================================================
// Step 5: Store Certificate
// =============================================================================

/// Tries of the store after the certifier issued: a busy store is tried
/// again, at most this many times in all.
pub(crate) const CERTIFICATE_STORE_TRIES: u32 = 3;

/// The first sleep between tries, doubled after each (250 then 500 ms: 750 ms
/// of sleeps in all). Each try can itself wait up to the writer's 5 s acquire
/// ceiling, `STORE_BUSY_CEILING_MS`, so a call that stays busy waits about
/// 16 s in one process (3 x 5 s + 0.75 s) and about 31 s across processes
/// (another process's lock adds up to 5 s per try).
pub(crate) const CERTIFICATE_STORE_BACKOFF_MS: u64 = 250;

/// Stores the certificate in the wallet storage.
///
/// The certifier has issued by now. A busy store (`Error::StoreBusy`, which
/// wrote nothing) is tried again, `CERTIFICATE_STORE_TRIES` times in all
/// with a doubling wait; if it is still busy the error is a storage error
/// naming the certificate's serial number, certifier and type, so an
/// operator can recover the certificate from the certifier.
pub(super) async fn store_certificate<S>(
    storage: &S,
    auth: &AuthId,
    parsed: &ParseCertificateResponseResult,
    master_keyring: &IndexMap<String, String>,
    certifier: &PublicKey,
    identity_key: &str,
    cert_type_b64: &str,
) -> Result<WalletCertificate>
where
    S: WalletStorageProvider + Send + Sync,
{
    let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

    let revocation_outpoint = parsed.certificate.revocation_outpoint.clone();
    let signature = parsed.certificate.signature.clone();

    // Build TableCertificate
    let table_cert = TableCertificate {
        certificate_id: 0, // Will be assigned by storage
        user_id,
        cert_type: cert_type_b64.to_string(),
        serial_number: parsed.certificate.serial_number.clone(),
        certifier: certifier.to_hex(),
        subject: identity_key.to_string(),
        verifier: Some(certifier.to_hex()), // Certifier is also the verifier for KeyringRevealer.Certifier
        revocation_outpoint: revocation_outpoint.clone(),
        signature: signature.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // The certificate and its fields in one write. This is after the
    // exchange with the certifier, a post to a third party: the certifier
    // has issued, and a busy here is never `StoreBusy` (a host would repeat
    // the whole exchange); it is tried again a bounded number of times,
    // then a storage error the caller sees naming the issued certificate,
    // nothing stored (bsv-wallet-cli #44 review 4 G1, review 5 H2).
    let now = chrono::Utc::now();
    let fields: Vec<TableCertificateField> = parsed
        .certificate
        .fields
        .iter()
        .map(|(field_name, field_value)| TableCertificateField {
            certificate_field_id: 0, // Will be assigned by storage
            certificate_id: 0,       // The certificate's, in storage
            user_id,
            field_name: field_name.clone(),
            field_value: field_value.clone(),
            master_key: master_keyring.get(field_name).cloned().unwrap_or_default(),
            created_at: now,
            updated_at: now,
        })
        .collect();
    let serial_number = table_cert.serial_number.clone();
    let mut attempt = 1;
    loop {
        match storage
            .insert_certificate_with_fields(auth, table_cert.clone(), fields.clone())
            .await
        {
            Ok(_) => break,
            // A busy wrote nothing: the one transaction rolled back, so the
            // same insert is safe to repeat.
            Err(Error::StoreBusy { .. }) if attempt < CERTIFICATE_STORE_TRIES => {
                tokio::time::sleep(std::time::Duration::from_millis(
                    CERTIFICATE_STORE_BACKOFF_MS << (attempt - 1),
                ))
                .await;
                attempt += 1;
            }
            Err(e @ Error::StoreBusy { .. }) => {
                return Err(Error::StorageError(format!(
                    "{} (the certifier issued a certificate that was not stored: \
                     serial number {}, certifier {}, type {}; tried {} times)",
                    e,
                    serial_number,
                    certifier.to_hex(),
                    cert_type_b64,
                    CERTIFICATE_STORE_TRIES
                )));
            }
            Err(e) => return Err(not_busy(e)),
        }
    }

    // Build and return WalletCertificate
    Ok(WalletCertificate {
        certificate_type: cert_type_b64.to_string(),
        serial_number: parsed.certificate.serial_number.clone(),
        subject: identity_key.to_string(),
        certifier: certifier.to_hex(),
        revocation_outpoint,
        signature,
        fields: parsed.certificate.fields.clone(),
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(all(test, feature = "sqlite"))]
#[path = "certificate_issuance_tests.rs"]
mod field_scheme_tests;

#[cfg(all(test, feature = "sqlite"))]
#[path = "certificate_issuance_key_tests.rs"]
mod key_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "sqlite")]
    fn corrupt_city_keyring_entry(
        subject: bsv_rs::primitives::PrivateKey,
        certifier: &PublicKey,
        field_ciphertext: &str,
        master_entry: &str,
    ) -> String {
        use bsv_rs::wallet::{DecryptArgs, EncryptArgs, ProtoWallet};

        let protocol = Protocol::new(SecurityLevel::Counterparty, "certificate field encryption");
        let subject_wallet = ProtoWallet::new(Some(subject));
        let mut field_key = subject_wallet
            .decrypt(DecryptArgs {
                ciphertext: BASE64.decode(master_entry).unwrap(),
                protocol_id: protocol.clone(),
                key_id: "city".to_string(),
                counterparty: Some(Counterparty::Other(certifier.clone())),
                ..Default::default()
            })
            .unwrap()
            .plaintext;
        let ciphertext = BASE64.decode(field_ciphertext).unwrap();
        let decrypt = |key: &[u8]| SymmetricKey::from_bytes(key).unwrap().decrypt(&ciphertext);
        assert!(decrypt(&field_key).is_ok());
        field_key[0] ^= 1;
        assert!(decrypt(&field_key).is_err());
        // The wallet envelope is authentic, but it carries the wrong AES key.
        let wrong_entry = subject_wallet
            .encrypt(EncryptArgs {
                plaintext: field_key,
                protocol_id: protocol,
                key_id: "city".to_string(),
                counterparty: Some(Counterparty::Other(certifier.clone())),
                ..Default::default()
            })
            .unwrap();
        BASE64.encode(wrong_entry.ciphertext)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn prove_certificate_rejects_corrupt_master_keyring_entry() {
        use bsv_rs::primitives::PrivateKey;
        use bsv_rs::wallet::ProveCertificateArgs;

        use crate::services::mock::MockWalletServices;
        use crate::storage::WalletStorageWriter;
        use crate::{StorageSqlx, Wallet};

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../conformance/upstream/261-certificate-field-value/certificate_field_value_261.json"
        ))
        .unwrap();
        for issued in [&fixture["reference_issued"], &fixture["rust_issued"]] {
            let private_key = |role: &str| {
                PrivateKey::from_hex(&format!("{:064x}", issued["keys"][role].as_u64().unwrap()))
                    .unwrap()
            };
            let subject = private_key("subject");
            let identity_key = subject.public_key().to_hex();
            let certifier = private_key("certifier").public_key();
            let certificate: CertificateResponse =
                serde_json::from_value(issued["certificate"].clone()).unwrap();
            let parsed = parse_certificate_response(&ProtocolIssuanceResponse {
                protocol: "certificate issuance".to_string(),
                certificate: Some(certificate.clone()),
                server_nonce: BASE64.encode([5; 32]),
                timestamp: String::new(),
                version: "1.0".to_string(),
            })
            .unwrap();
            let mut keyring: IndexMap<String, String> =
                serde_json::from_value(issued["master_keyring"].clone()).unwrap();
            let wrong_entry = corrupt_city_keyring_entry(
                subject.clone(),
                &certifier,
                &certificate.fields["city"],
                &keyring["city"],
            );
            keyring.insert("city".to_string(), wrong_entry);

            let storage = StorageSqlx::in_memory().await.unwrap();
            storage
                .migrate("verifier-decrypt", &identity_key)
                .await
                .unwrap();
            storage.make_available().await.unwrap();
            let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
            let auth = AuthId::with_user_id(identity_key.clone(), user.user_id);
            let wallet = Wallet::new(Some(subject), storage, MockWalletServices::new())
                .await
                .unwrap();
            let stored = store_certificate(
                wallet.storage(),
                &auth,
                &parsed,
                &keyring,
                &certifier,
                &identity_key,
                &certificate.cert_type,
            )
            .await
            .unwrap();
            let result = wallet
                .prove_certificate(
                    ProveCertificateArgs {
                        certificate: stored,
                        fields_to_reveal: vec!["city".to_string()],
                        verifier: private_key("verifier").public_key().to_hex(),
                        privileged: None,
                        privileged_reason: None,
                    },
                    Some("certificate.test"),
                )
                .await;
            // The reference throws Error, not WERR_INVALID_PARAMETER, at
            // MasterCertificate.ts:621-622 (ts-stack@edf6e03).
            assert!(
                matches!(&result, Err(bsv_rs::Error::WalletFault(fault))
                    if fault.family == bsv_rs::WalletFaultFamily::Internal
                        && fault.message == "Failed to decrypt certificate field!"),
                "prove_certificate must refuse the corrupt master entry: {result:?}"
            );
        }
    }

    #[test]
    fn test_protocol_issuance_request_serialization() {
        let request = ProtocolIssuanceRequest {
            cert_type: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            client_nonce: "dGVzdC1ub25jZQ==".to_string(),
            fields: IndexMap::from([("name".to_string(), "ZW5jcnlwdGVkLW5hbWU=".to_string())]),
            master_keyring: IndexMap::from([("name".to_string(), "bWFzdGVyLWtleQ==".to_string())]),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"type\""));
        assert!(json.contains("\"clientNonce\""));
        assert!(json.contains("\"fields\""));
        assert!(json.contains("\"masterKeyring\""));
    }

    #[test]
    fn test_protocol_issuance_response_deserialization() {
        let json = r#"{
            "protocol": "certificate issuance",
            "certificate": {
                "type": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "serialNumber": "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
                "subject": "02abc123",
                "certifier": "03def456",
                "revocationOutpoint": "0000000000000000000000000000000000000000000000000000000000000001.0",
                "fields": {"name": "encrypted"},
                "signature": "3045022100..."
            },
            "serverNonce": "c2VydmVyLW5vbmNl",
            "timestamp": "2024-01-01T00:00:00Z",
            "version": "1.0"
        }"#;

        let response: ProtocolIssuanceResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.protocol, "certificate issuance");
        assert!(response.certificate.is_some());
        assert_eq!(response.server_nonce, "c2VydmVyLW5vbmNl");
    }

    #[test]
    fn test_parse_certificate_response_missing_both() {
        let response = ProtocolIssuanceResponse {
            protocol: "test".to_string(),
            certificate: None,
            server_nonce: String::new(),
            timestamp: String::new(),
            version: String::new(),
        };

        let result = parse_certificate_response(&response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No certificate"));
    }

    #[test]
    fn test_parse_certificate_response_missing_certificate() {
        let response = ProtocolIssuanceResponse {
            protocol: "test".to_string(),
            certificate: None,
            server_nonce: "nonce".to_string(),
            timestamp: String::new(),
            version: String::new(),
        };

        let result = parse_certificate_response(&response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No certificate"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(NONCE_HMAC_SIZE, 32);
        assert_eq!(NONCE_SIZE, 16);
        assert_eq!(CERTIFICATE_ISSUANCE_PROTOCOL, "certificate issuance");
    }

    #[test]
    fn test_certificate_response_deserialization() {
        let json = r#"{
            "type": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "serialNumber": "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            "subject": "02abc123def456789012345678901234567890123456789012345678901234567890",
            "certifier": "03def456789012345678901234567890123456789012345678901234567890123456",
            "revocationOutpoint": "0000000000000000000000000000000000000000000000000000000000000001.5",
            "fields": {
                "name": "ZW5jcnlwdGVkLW5hbWU=",
                "email": "ZW5jcnlwdGVkLWVtYWls"
            },
            "signature": "3045022100deadbeef"
        }"#;

        let cert: CertificateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            cert.cert_type,
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        );
        assert_eq!(cert.fields.len(), 2);
        assert!(cert.fields.contains_key("name"));
        assert!(cert.fields.contains_key("email"));
    }

    #[test]
    fn test_parse_certificate_response_defers_metadata_validation() {
        let response = ProtocolIssuanceResponse {
            protocol: "certificate issuance".to_string(),
            certificate: Some(CertificateResponse {
                cert_type: "unparsed type".to_string(),
                serial_number: "unparsed serial".to_string(),
                subject: "unparsed subject".to_string(),
                certifier: "unparsed certifier".to_string(),
                revocation_outpoint: "unparsed outpoint".to_string(),
                fields: IndexMap::new(),
                signature: "unparsed signature".to_string(),
            }),
            server_nonce: "fixture".to_string(),
            timestamp: String::new(),
            version: String::new(),
        };
        let parsed = parse_certificate_response(&response).unwrap();
        let retained = parsed.certificate;
        assert_eq!(retained.cert_type, "unparsed type");
        assert_eq!(retained.serial_number, "unparsed serial");
        assert_eq!(retained.subject, "unparsed subject");
        assert_eq!(retained.certifier, "unparsed certifier");
        assert_eq!(retained.revocation_outpoint, "unparsed outpoint");
        assert_eq!(retained.signature, "unparsed signature");
    }

    #[test]
    fn test_prepare_issuance_action_data_result() {
        // Test that PrepareIssuanceActionDataResult is properly constructed
        let result = PrepareIssuanceActionDataResult {
            body: vec![1, 2, 3],
            fields: IndexMap::from([("name".to_string(), "value".to_string())]),
            master_keyring: IndexMap::from([("name".to_string(), "key".to_string())]),
            cert_type_b64: "test_type".to_string(),
            client_nonce: "test_nonce".to_string(),
        };

        assert_eq!(result.body, vec![1, 2, 3]);
        assert_eq!(result.fields.get("name").unwrap(), "value");
        assert_eq!(result.master_keyring.get("name").unwrap(), "key");
        assert_eq!(result.cert_type_b64, "test_type");
        assert_eq!(result.client_nonce, "test_nonce");
    }

    #[test]
    fn test_serial_number_length_validation_is_deferred() {
        // Test that invalid serial number length is rejected
        use bsv_rs::primitives::PrivateKey;

        // Create a minimal valid-looking response with wrong serial number length
        let certifier_key = PrivateKey::random();
        let subject_key = PrivateKey::random();

        // Use a 16-byte serial number (invalid - should be 32)
        let invalid_serial = BASE64.encode([1u8; 16]);

        let response = ProtocolIssuanceResponse {
            protocol: "certificate issuance".to_string(),
            certificate: Some(CertificateResponse {
                cert_type: BASE64.encode([0u8; 32]),
                serial_number: invalid_serial,
                subject: subject_key.public_key().to_hex(),
                certifier: certifier_key.public_key().to_hex(),
                revocation_outpoint:
                    "0000000000000000000000000000000000000000000000000000000000000001.0".to_string(),
                fields: IndexMap::new(),
                signature: "3045022100".to_string(),
            }),
            server_nonce: BASE64.encode([0u8; 32]),
            timestamp: String::new(),
            version: String::new(),
        };

        let result = parse_certificate_response(&response);
        assert_eq!(
            BASE64
                .decode(result.unwrap().certificate.serial_number)
                .unwrap()
                .len(),
            16
        );
    }

    #[tokio::test]
    async fn test_send_issuance_request_network_error() {
        // Test that network errors are properly handled
        let body = b"{}";

        // Use an invalid URL to trigger network error
        let result = send_issuance_request(
            ProtoWallet::new(None),
            "http://invalid-url-that-does-not-exist.local:12345",
            body,
            "02abc123",
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to send"));
    }
}
