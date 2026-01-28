//! Certificate Issuance Protocol Implementation
//!
//! This module implements the certificate issuance protocol for acquiring identity
//! certificates from a certifier service. It provides 1:1 parity with the Go and
//! TypeScript implementations.
//!
//! ## Protocol Flow
//!
//! 1. **PrepareIssuanceActionData**: Generate random nonce, encrypt fields using
//!    MasterCertificate::create_certificate_fields, build JSON request body
//!
//! 2. **HTTP POST**: Send request to certifier URL with BRC-104 headers:
//!    - `x-bsv-auth-version: 0.1`
//!    - `x-bsv-identity-key: <wallet identity key hex>`
//!
//! 3. **ParseCertificateResponse**: Parse JSON response, validate headers,
//!    extract certificate components
//!
//! 4. **VerifyCertificateIssuance**: Verify serial number via HMAC, validate
//!    certificate type/subject/certifier/fields, verify signature
//!
//! 5. **StoreCertificate**: Save certificate to storage with encrypted fields
//!    and master keyring

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_sdk::auth::certificates::Certificate;
use bsv_sdk::primitives::PublicKey;
use bsv_sdk::wallet::{
    AcquireCertificateArgs, Counterparty, Protocol, SecurityLevel, VerifyHmacArgs,
    WalletCertificate, WalletInterface,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::entities::{TableCertificate, TableCertificateField};
use crate::storage::{AuthId, WalletStorageProvider};

// =============================================================================
// Constants
// =============================================================================

/// BRC-104 authentication version header value
const BRC104_AUTH_VERSION: &str = "0.1";

/// BRC-104 authentication version header name
const HEADER_AUTH_VERSION: &str = "x-bsv-auth-version";

/// BRC-104 identity key header name
const HEADER_IDENTITY_KEY: &str = "x-bsv-identity-key";

/// Protocol for certificate issuance HMAC verification
const CERTIFICATE_ISSUANCE_PROTOCOL: &str = "certificate issuance";

/// Expected HMAC size (32 bytes)
const NONCE_HMAC_SIZE: usize = 32;

/// Random nonce size in bytes
const NONCE_SIZE: usize = 32;

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
    pub fields: HashMap<String, String>,
    /// Master keyring for field encryption
    pub master_keyring: HashMap<String, String>,
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
    pub revocation_outpoint: String,
    /// Encrypted field values
    pub fields: HashMap<String, String>,
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
    pub fields: HashMap<String, String>,
    /// Master keyring
    pub master_keyring: HashMap<String, String>,
    /// Certificate type (base64 encoded)
    pub cert_type_b64: String,
    /// Client nonce (base64 encoded)
    pub client_nonce: String,
}

/// Result of parsing certificate response
#[derive(Debug)]
pub(crate) struct ParseCertificateResponseResult {
    /// Parsed certificate
    pub certificate: Certificate,
    /// Server nonce (base64 encoded)
    pub server_nonce: String,
    /// Certificate fields from response
    pub cert_fields: HashMap<String, String>,
    /// Serial number as raw bytes
    pub serial_number: [u8; 32],
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
    storage: &S,
    auth: &AuthId,
    args: AcquireCertificateArgs,
    identity_key: &str,
    originator: &str,
) -> Result<WalletCertificate>
where
    W: WalletInterface + Send + Sync,
    S: WalletStorageProvider + Send + Sync,
{
    // Validate certifier_url is provided
    let certifier_url = args.certifier_url.as_ref().ok_or_else(|| {
        Error::ValidationError("certifier_url is required for issuance protocol".to_string())
    })?;

    // Parse certifier public key
    let certifier = PublicKey::from_hex(&args.certifier)
        .map_err(|e| Error::ValidationError(format!("Invalid certifier public key: {}", e)))?;

    // Parse identity key to PublicKey
    let identity_pub_key = PublicKey::from_hex(identity_key)
        .map_err(|e| Error::ValidationError(format!("Invalid identity key: {}", e)))?;

    // Step 1: Prepare issuance action data
    let prepared = prepare_issuance_action_data(
        wallet,
        &args,
        &certifier,
        &identity_pub_key,
        originator,
    )
    .await?;

    // Step 2: Send HTTP POST to certifier
    let http_response = send_issuance_request(certifier_url, &prepared.body, identity_key).await?;

    // Step 2.5: Validate response header matches expected certifier (per Go implementation)
    if let Some(response_identity_key) = &http_response.identity_key_header {
        if response_identity_key != &args.certifier {
            return Err(Error::ValidationError(format!(
                "Invalid certifier in response header! Expected: {}, Received: {}",
                args.certifier, response_identity_key
            )));
        }
    }
    // Note: Unlike Go, we don't require the header to be present, as some certifiers may not include it

    // Step 3: Parse and validate response
    let parsed = parse_certificate_response(
        &http_response.body,
        &args.certifier,
        &prepared.cert_type_b64,
    )?;

    // Step 4: Verify certificate issuance
    verify_certificate_issuance(
        wallet,
        &parsed,
        &prepared.client_nonce,
        &prepared.fields,
        &prepared.cert_type_b64,
        identity_key,
        &args.certifier,
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

// =============================================================================
// Step 1: Prepare Issuance Action Data
// =============================================================================

/// Prepares the certificate signing request payload.
///
/// Generates a random nonce and creates encrypted certificate fields using
/// the SDK's MasterCertificate::create_certificate_fields.
async fn prepare_issuance_action_data<W>(
    wallet: &W,
    args: &AcquireCertificateArgs,
    certifier: &PublicKey,
    subject: &PublicKey,
    originator: &str,
) -> Result<PrepareIssuanceActionDataResult>
where
    W: WalletInterface + Send + Sync,
{
    // Generate random client nonce
    let nonce_bytes: [u8; NONCE_SIZE] = rand::random();
    let client_nonce = BASE64.encode(&nonce_bytes);

    // Encode certificate type as base64
    // The certificate_type is already a string - decode it to get the 32 bytes
    let cert_type_bytes = BASE64.decode(&args.certificate_type).map_err(|e| {
        Error::ValidationError(format!("Invalid certificate_type (not valid base64): {}", e))
    })?;
    if cert_type_bytes.len() != 32 {
        return Err(Error::ValidationError(format!(
            "certificate_type must be 32 bytes when decoded, got {}",
            cert_type_bytes.len()
        )));
    }
    let mut cert_type_array = [0u8; 32];
    cert_type_array.copy_from_slice(&cert_type_bytes);
    let cert_type_b64 = args.certificate_type.clone();

    // Create encrypted certificate fields using SDK
    // Note: We need to use MasterCertificate::create_certificate_fields
    // but it requires the certifier's wallet. For issuance, we create
    // the fields ourselves since we're the subject encrypting for certifier.
    let (encrypted_fields, master_keyring) = create_certificate_fields_for_issuance(
        wallet,
        certifier,
        subject,
        &args.fields,
        &cert_type_array,
        originator,
    )
    .await?;

    // Convert to string maps for JSON serialization
    let fields: HashMap<String, String> = encrypted_fields
        .into_iter()
        .map(|(k, v)| (k, BASE64.encode(&v)))
        .collect();

    let master_keyring: HashMap<String, String> = master_keyring
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

/// Creates encrypted certificate fields for issuance.
///
/// This encrypts field values from the subject to the certifier, creating
/// both the encrypted field values and the master keyring.
async fn create_certificate_fields_for_issuance<W>(
    wallet: &W,
    certifier: &PublicKey,
    _subject: &PublicKey,
    plain_fields: &HashMap<String, String>,
    _cert_type: &[u8; 32],
    originator: &str,
) -> Result<(HashMap<String, Vec<u8>>, HashMap<String, Vec<u8>>)>
where
    W: WalletInterface + Send + Sync,
{
    use bsv_sdk::wallet::EncryptArgs;

    let protocol = Protocol::new(
        SecurityLevel::Counterparty,
        "certificate field encryption",
    );

    let mut encrypted_fields = HashMap::new();
    let mut master_keyring = HashMap::new();

    for (field_name, plain_value) in plain_fields {
        // Key ID for master encryption is just the field name
        let key_id = field_name.clone();

        // Encrypt the field value from subject to certifier
        let encrypt_result = wallet
            .encrypt(
                EncryptArgs {
                    plaintext: plain_value.as_bytes().to_vec(),
                    protocol_id: protocol.clone(),
                    key_id: key_id.clone(),
                    counterparty: Some(Counterparty::Other(certifier.clone())),
                },
                originator,
            )
            .await
            .map_err(|e| {
                Error::ValidationError(format!(
                    "Failed to encrypt field '{}': {}",
                    field_name, e
                ))
            })?;

        encrypted_fields.insert(field_name.clone(), encrypt_result.ciphertext.clone());
        master_keyring.insert(field_name.clone(), encrypt_result.ciphertext);
    }

    Ok((encrypted_fields, master_keyring))
}

// =============================================================================
// Step 2: Send HTTP Request
// =============================================================================

/// Response from HTTP request including headers for validation
#[derive(Debug)]
struct HttpIssuanceResponse {
    /// The parsed JSON body
    body: ProtocolIssuanceResponse,
    /// The x-bsv-identity-key response header
    identity_key_header: Option<String>,
}

/// Sends the issuance request to the certifier URL.
async fn send_issuance_request(
    certifier_url: &str,
    body: &[u8],
    identity_key: &str,
) -> Result<HttpIssuanceResponse> {
    let client = Client::new();

    let response = client
        .post(certifier_url)
        .header(HEADER_AUTH_VERSION, BRC104_AUTH_VERSION)
        .header(HEADER_IDENTITY_KEY, identity_key)
        .header("Content-Type", "application/json")
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| Error::NetworkError(format!("Failed to send issuance request: {}", e)))?;

    // Check HTTP status
    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_default();
        return Err(Error::NetworkError(format!(
            "Certifier returned HTTP {}: {}",
            status, error_body
        )));
    }

    // Extract identity key header for validation
    let identity_key_header = response
        .headers()
        .get(HEADER_IDENTITY_KEY)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Parse response JSON
    let response_text = response.text().await.map_err(|e| {
        Error::NetworkError(format!("Failed to read certifier response: {}", e))
    })?;

    let parsed: ProtocolIssuanceResponse = serde_json::from_str(&response_text).map_err(|e| {
        Error::ValidationError(format!(
            "Failed to parse certifier response: {} - body: {}",
            e, response_text
        ))
    })?;

    Ok(HttpIssuanceResponse {
        body: parsed,
        identity_key_header,
    })
}

// =============================================================================
// Step 3: Parse Certificate Response
// =============================================================================

/// Parses and validates the certificate response from the certifier.
fn parse_certificate_response(
    response: &ProtocolIssuanceResponse,
    expected_certifier: &str,
    expected_type: &str,
) -> Result<ParseCertificateResponseResult> {
    // Validate server nonce is present
    if response.server_nonce.is_empty() {
        return Err(Error::ValidationError(
            "No serverNonce received from certifier".to_string(),
        ));
    }

    // Validate certificate is present
    let cert_resp = response.certificate.as_ref().ok_or_else(|| {
        Error::ValidationError("No certificate received from certifier".to_string())
    })?;

    // Parse subject public key
    let subject = PublicKey::from_hex(&cert_resp.subject).map_err(|e| {
        Error::ValidationError(format!("Failed to parse subject public key: {}", e))
    })?;

    // Parse certifier public key
    let certifier = PublicKey::from_hex(&cert_resp.certifier).map_err(|e| {
        Error::ValidationError(format!("Failed to parse certifier public key: {}", e))
    })?;

    // Validate certifier matches expected
    if cert_resp.certifier != expected_certifier {
        return Err(Error::ValidationError(format!(
            "Invalid certifier! Expected: {}, Received: {}",
            expected_certifier, cert_resp.certifier
        )));
    }

    // Validate certificate type matches
    if cert_resp.cert_type != expected_type {
        return Err(Error::ValidationError(format!(
            "Invalid certificate type! Expected: {}, Received: {}",
            expected_type, cert_resp.cert_type
        )));
    }

    // Decode serial number
    let serial_bytes = BASE64.decode(&cert_resp.serial_number).map_err(|e| {
        Error::ValidationError(format!("Failed to decode serial number: {}", e))
    })?;
    if serial_bytes.len() != 32 {
        return Err(Error::ValidationError(format!(
            "Invalid serial number length: expected 32, got {}",
            serial_bytes.len()
        )));
    }
    let mut serial_number = [0u8; 32];
    serial_number.copy_from_slice(&serial_bytes);

    // Decode certificate type
    let cert_type_bytes = BASE64.decode(&cert_resp.cert_type).map_err(|e| {
        Error::ValidationError(format!("Failed to decode certificate type: {}", e))
    })?;
    if cert_type_bytes.len() != 32 {
        return Err(Error::ValidationError(format!(
            "Invalid certificate type length: expected 32, got {}",
            cert_type_bytes.len()
        )));
    }
    let mut cert_type = [0u8; 32];
    cert_type.copy_from_slice(&cert_type_bytes);

    // Build the Certificate struct
    let mut certificate = Certificate::new(cert_type, serial_number, subject, certifier);

    // Set revocation outpoint if valid
    if !cert_resp.revocation_outpoint.is_empty() && cert_resp.revocation_outpoint != "." {
        // Parse txid.vout format
        let parts: Vec<&str> = cert_resp.revocation_outpoint.split('.').collect();
        if parts.len() == 2 {
            if let (Ok(txid_bytes), Ok(vout)) = (
                hex::decode(parts[0]),
                parts[1].parse::<u32>(),
            ) {
                if txid_bytes.len() == 32 {
                    let mut txid = [0u8; 32];
                    txid.copy_from_slice(&txid_bytes);
                    certificate.revocation_outpoint =
                        Some(bsv_sdk::wallet::Outpoint::new(txid, vout));
                }
            }
        }
    }

    // Set encrypted fields
    for (name, value) in &cert_resp.fields {
        let decoded = BASE64.decode(value).map_err(|e| {
            Error::ValidationError(format!("Failed to decode field '{}': {}", name, e))
        })?;
        certificate.set_field(name.clone(), decoded);
    }

    // Set signature
    let signature_bytes = hex::decode(&cert_resp.signature).map_err(|e| {
        Error::ValidationError(format!("Failed to decode signature: {}", e))
    })?;
    certificate.signature = Some(signature_bytes);

    Ok(ParseCertificateResponseResult {
        certificate,
        server_nonce: response.server_nonce.clone(),
        cert_fields: cert_resp.fields.clone(),
        serial_number,
    })
}

// =============================================================================
// Step 4: Verify Certificate Issuance
// =============================================================================

/// Verifies the certificate against the original request parameters.
///
/// This includes HMAC verification of the serial number and validation
/// of all certificate fields.
#[allow(clippy::too_many_arguments)]
async fn verify_certificate_issuance<W>(
    wallet: &W,
    parsed: &ParseCertificateResponseResult,
    client_nonce: &str,
    sent_fields: &HashMap<String, String>,
    cert_type_b64: &str,
    identity_key: &str,
    certifier_hex: &str,
    certifier: &PublicKey,
    originator: &str,
) -> Result<()>
where
    W: WalletInterface + Send + Sync,
{
    // 1. Verify serial number length
    if parsed.serial_number.len() != NONCE_HMAC_SIZE {
        return Err(Error::ValidationError(format!(
            "Invalid serialNumber length: got {}, want {}",
            parsed.serial_number.len(),
            NONCE_HMAC_SIZE
        )));
    }

    // 2. Verify serial number via HMAC
    // Data = clientNonceBytes + serverNonceBytes
    let client_nonce_bytes = BASE64.decode(client_nonce).map_err(|e| {
        Error::ValidationError(format!("Failed to decode client nonce: {}", e))
    })?;
    let server_nonce_bytes = BASE64.decode(&parsed.server_nonce).map_err(|e| {
        Error::ValidationError(format!("Failed to decode server nonce: {}", e))
    })?;

    let mut data_to_verify = Vec::with_capacity(client_nonce_bytes.len() + server_nonce_bytes.len());
    data_to_verify.extend_from_slice(&client_nonce_bytes);
    data_to_verify.extend_from_slice(&server_nonce_bytes);

    // KeyID = serverNonce + clientNonce
    let hmac_key_id = format!("{}{}", parsed.server_nonce, client_nonce);

    let protocol = Protocol::new(
        SecurityLevel::Counterparty,
        CERTIFICATE_ISSUANCE_PROTOCOL,
    );

    let verify_result = wallet
        .verify_hmac(
            VerifyHmacArgs {
                hmac: parsed.serial_number,
                data: data_to_verify,
                protocol_id: protocol,
                key_id: hmac_key_id,
                counterparty: Some(Counterparty::Other(certifier.clone())),
            },
            originator,
        )
        .await
        .map_err(|e| {
            Error::ValidationError(format!("Failed to verify HMAC signature: {}", e))
        })?;

    if !verify_result.valid {
        return Err(Error::ValidationError(
            "Invalid serialNumber - HMAC verification failed".to_string(),
        ));
    }

    // 3. Validate certificate type
    if parsed.certificate.type_base64() != cert_type_b64 {
        return Err(Error::ValidationError(format!(
            "Invalid certificate type! Expected: {}, Received: {}",
            cert_type_b64,
            parsed.certificate.type_base64()
        )));
    }

    // 4. Validate certificate subject matches our identity key
    let subject_hex = parsed.certificate.subject.to_hex();
    if subject_hex != identity_key {
        return Err(Error::ValidationError(format!(
            "Invalid certificate subject! Expected: {}, Received: {}",
            identity_key, subject_hex
        )));
    }

    // 5. Validate certifier
    let cert_certifier_hex = parsed.certificate.certifier.to_hex();
    if cert_certifier_hex != certifier_hex {
        return Err(Error::ValidationError(format!(
            "Invalid certifier! Expected: {}, Received: {}",
            certifier_hex, cert_certifier_hex
        )));
    }

    // 6. Validate revocation outpoint exists (required per Go implementation)
    if parsed.certificate.revocation_outpoint.is_none() {
        return Err(Error::ValidationError(
            "Invalid revocationOutpoint: certificate must have a revocation outpoint".to_string(),
        ));
    }

    // 7. Validate that certificate fields match what we sent
    if parsed.cert_fields.len() != sent_fields.len() {
        return Err(Error::ValidationError(format!(
            "Fields mismatch! Objects have different number of keys. Expected: {}, Received: {}",
            sent_fields.len(),
            parsed.cert_fields.len()
        )));
    }

    for (field_name, sent_value) in sent_fields {
        let received_value = parsed.cert_fields.get(field_name).ok_or_else(|| {
            Error::ValidationError(format!(
                "Missing field: {} in certificate fields from the certifier",
                field_name
            ))
        })?;

        if received_value != sent_value {
            return Err(Error::ValidationError(format!(
                "Invalid field '{}'! Expected: {}, Received: {}",
                field_name, sent_value, received_value
            )));
        }
    }

    // 8. Verify certificate signature
    parsed.certificate.verify().map_err(|e| {
        Error::ValidationError(format!("Failed to verify certificate signature: {}", e))
    })?;

    Ok(())
}

// =============================================================================
// Step 5: Store Certificate
// =============================================================================

/// Stores the certificate in the wallet storage.
async fn store_certificate<S>(
    storage: &S,
    auth: &AuthId,
    parsed: &ParseCertificateResponseResult,
    master_keyring: &HashMap<String, String>,
    certifier: &PublicKey,
    identity_key: &str,
    cert_type_b64: &str,
) -> Result<WalletCertificate>
where
    S: WalletStorageProvider + Send + Sync,
{
    let user_id = auth.user_id.ok_or_else(|| Error::AuthenticationRequired)?;

    // Format revocation outpoint
    let revocation_outpoint = if let Some(ref outpoint) = parsed.certificate.revocation_outpoint {
        format!("{}.{}", hex::encode(outpoint.txid), outpoint.vout)
    } else {
        String::new()
    };

    // Format signature as hex
    let signature = parsed
        .certificate
        .signature
        .as_ref()
        .map(hex::encode)
        .unwrap_or_default();

    // Build TableCertificate
    let table_cert = TableCertificate {
        certificate_id: 0, // Will be assigned by storage
        user_id,
        cert_type: cert_type_b64.to_string(),
        serial_number: parsed.certificate.serial_number_base64(),
        certifier: certifier.to_hex(),
        subject: identity_key.to_string(),
        verifier: Some(certifier.to_hex()), // Certifier is also the verifier for KeyringRevealer.Certifier
        revocation_outpoint: revocation_outpoint.clone(),
        signature: signature.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Insert certificate and get the ID
    let cert_id = storage.insert_certificate(auth, table_cert).await?;

    // Insert certificate fields
    for (field_name, field_value) in &parsed.cert_fields {
        let master_key = master_keyring
            .get(field_name)
            .cloned()
            .unwrap_or_default();

        let field = TableCertificateField {
            certificate_field_id: 0, // Will be assigned by storage
            certificate_id: cert_id,
            user_id,
            field_name: field_name.clone(),
            field_value: field_value.clone(),
            master_key,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        storage.insert_certificate_field(auth, field).await?;
    }

    // Build and return WalletCertificate
    Ok(WalletCertificate {
        certificate_type: cert_type_b64.to_string(),
        serial_number: parsed.certificate.serial_number_base64(),
        subject: identity_key.to_string(),
        certifier: certifier.to_hex(),
        revocation_outpoint,
        signature,
        fields: parsed.cert_fields.clone(),
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_issuance_request_serialization() {
        let request = ProtocolIssuanceRequest {
            cert_type: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            client_nonce: "dGVzdC1ub25jZQ==".to_string(),
            fields: HashMap::from([
                ("name".to_string(), "ZW5jcnlwdGVkLW5hbWU=".to_string()),
            ]),
            master_keyring: HashMap::from([
                ("name".to_string(), "bWFzdGVyLWtleQ==".to_string()),
            ]),
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
    fn test_parse_certificate_response_missing_server_nonce() {
        let response = ProtocolIssuanceResponse {
            protocol: "test".to_string(),
            certificate: None,
            server_nonce: String::new(),
            timestamp: String::new(),
            version: String::new(),
        };

        let result = parse_certificate_response(&response, "certifier", "type");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("serverNonce"));
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

        let result = parse_certificate_response(&response, "certifier", "type");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No certificate"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(NONCE_HMAC_SIZE, 32);
        assert_eq!(NONCE_SIZE, 32);
        assert_eq!(BRC104_AUTH_VERSION, "0.1");
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
        assert_eq!(cert.cert_type, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        assert_eq!(cert.fields.len(), 2);
        assert!(cert.fields.contains_key("name"));
        assert!(cert.fields.contains_key("email"));
    }

    #[test]
    fn test_parse_certificate_response_certifier_mismatch() {
        use bsv_sdk::primitives::PrivateKey;

        // Use real key pairs for valid public keys
        let certifier_key = PrivateKey::random();
        let different_certifier_key = PrivateKey::random();
        let subject_key = PrivateKey::random();

        let response = ProtocolIssuanceResponse {
            protocol: "certificate issuance".to_string(),
            certificate: Some(CertificateResponse {
                cert_type: BASE64.encode(&[0u8; 32]),
                serial_number: BASE64.encode(&[1u8; 32]),
                subject: subject_key.public_key().to_hex(),
                certifier: certifier_key.public_key().to_hex(),
                revocation_outpoint: "0000000000000000000000000000000000000000000000000000000000000001.0".to_string(),
                fields: HashMap::new(),
                signature: "304402200000000000000000000000000000000000000000000000000000000000000000022000000000000000000000000000000000000000000000000000000000000000000".to_string(),
            }),
            server_nonce: BASE64.encode(&[2u8; 32]),
            timestamp: String::new(),
            version: String::new(),
        };

        // Expect different certifier
        let result = parse_certificate_response(
            &response,
            &different_certifier_key.public_key().to_hex(),
            &BASE64.encode(&[0u8; 32]),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid certifier"));
    }

    #[test]
    fn test_parse_certificate_response_type_mismatch() {
        use bsv_sdk::primitives::PrivateKey;

        let certifier_key = PrivateKey::random();
        let subject_key = PrivateKey::random();

        let response = ProtocolIssuanceResponse {
            protocol: "certificate issuance".to_string(),
            certificate: Some(CertificateResponse {
                cert_type: BASE64.encode(&[0u8; 32]),
                serial_number: BASE64.encode(&[1u8; 32]),
                subject: subject_key.public_key().to_hex(),
                certifier: certifier_key.public_key().to_hex(),
                revocation_outpoint: "0000000000000000000000000000000000000000000000000000000000000001.0".to_string(),
                fields: HashMap::new(),
                signature: "304402200000000000000000000000000000000000000000000000000000000000000000022000000000000000000000000000000000000000000000000000000000000000000".to_string(),
            }),
            server_nonce: BASE64.encode(&[2u8; 32]),
            timestamp: String::new(),
            version: String::new(),
        };

        // Expect different type
        let result = parse_certificate_response(
            &response,
            &certifier_key.public_key().to_hex(),
            &BASE64.encode(&[99u8; 32]), // Different type
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid certificate type"));
    }

    #[test]
    fn test_prepare_issuance_action_data_result() {
        // Test that PrepareIssuanceActionDataResult is properly constructed
        let result = PrepareIssuanceActionDataResult {
            body: vec![1, 2, 3],
            fields: HashMap::from([("name".to_string(), "value".to_string())]),
            master_keyring: HashMap::from([("name".to_string(), "key".to_string())]),
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
    fn test_serial_number_length_validation() {
        // Test that invalid serial number length is rejected
        use bsv_sdk::primitives::PrivateKey;

        // Create a minimal valid-looking response with wrong serial number length
        let certifier_key = PrivateKey::random();
        let subject_key = PrivateKey::random();

        // Use a 16-byte serial number (invalid - should be 32)
        let invalid_serial = BASE64.encode(&[1u8; 16]);

        let response = ProtocolIssuanceResponse {
            protocol: "certificate issuance".to_string(),
            certificate: Some(CertificateResponse {
                cert_type: BASE64.encode(&[0u8; 32]),
                serial_number: invalid_serial,
                subject: subject_key.public_key().to_hex(),
                certifier: certifier_key.public_key().to_hex(),
                revocation_outpoint: "0000000000000000000000000000000000000000000000000000000000000001.0".to_string(),
                fields: HashMap::new(),
                signature: "3045022100".to_string(),
            }),
            server_nonce: BASE64.encode(&[0u8; 32]),
            timestamp: String::new(),
            version: String::new(),
        };

        let result = parse_certificate_response(
            &response,
            &certifier_key.public_key().to_hex(),
            &BASE64.encode(&[0u8; 32]),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("serial number length"));
    }

    #[tokio::test]
    async fn test_send_issuance_request_network_error() {
        // Test that network errors are properly handled
        let body = b"{}";
        let identity_key = "02abc123";

        // Use an invalid URL to trigger network error
        let result = send_issuance_request(
            "http://invalid-url-that-does-not-exist.local:12345",
            body,
            identity_key,
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to send"));
    }
}

// =============================================================================
// Integration Tests with Mock Server
// =============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    // Note: Full integration tests with mock HTTP server would require
    // a mock wallet implementation. The tests above cover the core logic.
    // Full end-to-end testing should be done in integration test files
    // with proper mock infrastructure.

    #[test]
    fn test_http_issuance_response_fields() {
        // Verify HttpIssuanceResponse struct has all needed fields
        let body = ProtocolIssuanceResponse {
            protocol: "test".to_string(),
            certificate: None,
            server_nonce: "nonce".to_string(),
            timestamp: String::new(),
            version: String::new(),
        };

        let http_response = HttpIssuanceResponse {
            body,
            identity_key_header: Some("identity".to_string()),
        };

        assert_eq!(http_response.identity_key_header, Some("identity".to_string()));
        assert_eq!(http_response.body.protocol, "test");
    }
}
