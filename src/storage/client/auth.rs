//! BRC-31 (Authrite) authentication for remote storage client.
//!
//! This module provides mutual authentication between the storage client and server
//! using the BRC-31 protocol. It handles:
//!
//! - Request signing with wallet identity key
//! - Response signature verification
//! - Nonce generation for replay protection
//! - BRC-104 HTTP header formatting
//!
//! ## Protocol Overview
//!
//! Each authenticated request includes:
//! - `x-bsv-auth-version`: Protocol version (0.1)
//! - `x-bsv-auth-identity-key`: Client's 33-byte compressed public key (hex)
//! - `x-bsv-auth-nonce`: Random nonce for replay protection (base64)
//! - `x-bsv-auth-timestamp`: Unix timestamp in milliseconds
//! - `x-bsv-auth-signature`: Signature over canonical request data (hex)
//!
//! The signature covers: `method || url || SHA256(body) || timestamp || nonce`
//!
//! ## Response Verification
//!
//! Server responses should include matching headers that the client verifies
//! to ensure the response came from the authentic server.

use crate::error::{Error, Result};
use bsv_sdk::primitives::{from_hex, to_hex, PublicKey};
use bsv_sdk::wallet::{
    Counterparty, CreateHmacArgs, CreateSignatureArgs, GetPublicKeyArgs, Protocol, SecurityLevel,
    VerifySignatureArgs, WalletInterface,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// BRC-31 authentication protocol version.
pub const AUTH_VERSION: &str = "0.1";

/// Protocol ID for BRC-31 request signatures.
/// Uses security level 2 (counterparty) for mutual authentication.
pub const AUTH_PROTOCOL_ID: &str = "storage auth";

/// Protocol ID for nonce HMAC computation.
pub const NONCE_PROTOCOL_ID: &str = "storage nonce";

/// Size of random portion of nonce (bytes).
const NONCE_RANDOM_SIZE: usize = 16;

/// Total size of nonce including HMAC (bytes).
const NONCE_TOTAL_SIZE: usize = 32;

/// Maximum age of a valid timestamp (5 minutes in milliseconds).
const MAX_TIMESTAMP_AGE_MS: u64 = 5 * 60 * 1000;

/// BRC-104 HTTP header names for authenticated requests.
pub mod headers {
    /// Auth protocol version.
    pub const VERSION: &str = "x-bsv-auth-version";
    /// Sender's identity public key (33-byte compressed, hex encoded).
    pub const IDENTITY_KEY: &str = "x-bsv-auth-identity-key";
    /// Cryptographic nonce for replay protection (base64 encoded).
    pub const NONCE: &str = "x-bsv-auth-nonce";
    /// Unix timestamp in milliseconds when request was created.
    pub const TIMESTAMP: &str = "x-bsv-auth-timestamp";
    /// Signature over the canonical request data (hex encoded).
    pub const SIGNATURE: &str = "x-bsv-auth-signature";
    /// Content type for JSON-RPC requests.
    pub const CONTENT_TYPE: &str = "content-type";
}

/// Authentication headers for a BRC-31 request.
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    /// Protocol version (always "0.1").
    pub version: String,
    /// Client's identity key (33-byte compressed public key, hex).
    pub identity_key: String,
    /// Cryptographic nonce (base64 encoded, 32 bytes).
    pub nonce: String,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Signature over the canonical request (hex encoded).
    pub signature: String,
}

impl AuthHeaders {
    /// Converts the auth headers to HTTP header tuples.
    pub fn to_header_tuples(&self) -> Vec<(&'static str, String)> {
        vec![
            (headers::VERSION, self.version.clone()),
            (headers::IDENTITY_KEY, self.identity_key.clone()),
            (headers::NONCE, self.nonce.clone()),
            (headers::TIMESTAMP, self.timestamp.to_string()),
            (headers::SIGNATURE, self.signature.clone()),
        ]
    }
}

/// Parsed auth headers from a server response.
#[derive(Debug, Clone)]
pub struct ResponseAuthHeaders {
    /// Protocol version from server.
    pub version: Option<String>,
    /// Server's identity key (hex).
    pub identity_key: Option<String>,
    /// Server's nonce (base64).
    pub nonce: Option<String>,
    /// Server's timestamp.
    pub timestamp: Option<u64>,
    /// Server's signature (hex).
    pub signature: Option<String>,
}

impl ResponseAuthHeaders {
    /// Parses auth headers from an HTTP response.
    pub fn from_response(response: &reqwest::Response) -> Self {
        let headers = response.headers();

        Self {
            version: headers
                .get(headers::VERSION)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            identity_key: headers
                .get(headers::IDENTITY_KEY)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            nonce: headers
                .get(headers::NONCE)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            timestamp: headers
                .get(headers::TIMESTAMP)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok()),
            signature: headers
                .get(headers::SIGNATURE)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
        }
    }

    /// Returns true if all required headers are present.
    pub fn is_complete(&self) -> bool {
        self.version.is_some()
            && self.identity_key.is_some()
            && self.nonce.is_some()
            && self.timestamp.is_some()
            && self.signature.is_some()
    }

    /// Returns the identity key as a PublicKey if valid.
    pub fn get_identity_key(&self) -> Result<Option<PublicKey>> {
        match &self.identity_key {
            Some(hex) => Ok(Some(PublicKey::from_hex(hex)?)),
            None => Ok(None),
        }
    }
}

/// Creates a BRC-31 authentication nonce.
///
/// The nonce format is: base64(random_16_bytes || hmac_16_bytes)
///
/// The HMAC is computed using BRC-42 key derivation, making the nonce
/// verifiable by the server if they know our identity key.
///
/// # Arguments
/// * `wallet` - Wallet for HMAC computation
/// * `server_identity_key` - Server's identity key for HMAC computation
/// * `originator` - Application originator string
pub async fn create_nonce<W: WalletInterface>(
    wallet: &W,
    server_identity_key: Option<&PublicKey>,
    originator: &str,
) -> Result<String> {
    // Generate 16 random bytes
    let mut random_bytes = [0u8; NONCE_RANDOM_SIZE];
    rand::thread_rng().fill_bytes(&mut random_bytes);

    // Compute HMAC of the random bytes
    let protocol = Protocol::new(SecurityLevel::App, NONCE_PROTOCOL_ID);
    let key_id = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &random_bytes);

    let hmac_result = wallet
        .create_hmac(
            CreateHmacArgs {
                data: random_bytes.to_vec(),
                protocol_id: protocol,
                key_id,
                counterparty: server_identity_key.map(|pk| Counterparty::Other(pk.clone())),
            },
            originator,
        )
        .await
        .map_err(|e| Error::StorageError(format!("Failed to create nonce HMAC: {}", e)))?;

    // Combine random bytes and first 16 bytes of HMAC
    let mut nonce = Vec::with_capacity(NONCE_TOTAL_SIZE);
    nonce.extend_from_slice(&random_bytes);
    nonce.extend_from_slice(&hmac_result.hmac[..NONCE_RANDOM_SIZE]);

    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &nonce,
    ))
}

/// Creates a simple random nonce without HMAC verification.
///
/// This is a simpler alternative when mutual verification isn't needed.
pub fn create_simple_nonce() -> String {
    let mut nonce_bytes = [0u8; NONCE_TOTAL_SIZE];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &nonce_bytes)
}

/// Returns the current Unix timestamp in milliseconds.
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Validates a timestamp is within acceptable range.
///
/// Timestamps must be within 5 minutes of current time to prevent replay attacks.
pub fn validate_timestamp(timestamp: u64) -> Result<()> {
    let now = current_timestamp_ms();

    // Check if timestamp is too old
    if now > timestamp && now - timestamp > MAX_TIMESTAMP_AGE_MS {
        return Err(Error::AccessDenied(format!(
            "Request timestamp too old: {} ms ago",
            now - timestamp
        )));
    }

    // Check if timestamp is too far in the future (allow 1 minute for clock skew)
    if timestamp > now && timestamp - now > 60_000 {
        return Err(Error::AccessDenied(format!(
            "Request timestamp in future: {} ms ahead",
            timestamp - now
        )));
    }

    Ok(())
}

/// Creates the canonical signing data for a request.
///
/// Format: `method || path || SHA256(body) || timestamp || nonce`
///
/// # Arguments
/// * `method` - HTTP method (POST, GET, etc.)
/// * `path` - URL path (e.g., "/" for JSON-RPC endpoint)
/// * `body` - Request body bytes
/// * `timestamp` - Unix timestamp in milliseconds
/// * `nonce` - Nonce string (base64)
pub fn create_signing_data(
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: &str,
) -> Vec<u8> {
    let mut data = Vec::new();

    // Method
    data.extend_from_slice(method.as_bytes());

    // Path
    data.extend_from_slice(path.as_bytes());

    // SHA256 of body
    let body_hash = Sha256::digest(body);
    data.extend_from_slice(&body_hash);

    // Timestamp as string
    data.extend_from_slice(timestamp.to_string().as_bytes());

    // Nonce
    data.extend_from_slice(nonce.as_bytes());

    data
}

/// Signs a request using the wallet's identity key.
///
/// # Arguments
/// * `wallet` - Wallet for signing
/// * `method` - HTTP method
/// * `path` - URL path
/// * `body` - Request body
/// * `timestamp` - Unix timestamp
/// * `nonce` - Nonce string
/// * `server_identity_key` - Server's identity key (optional)
/// * `originator` - Application originator
pub async fn sign_request<W: WalletInterface>(
    wallet: &W,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: &str,
    server_identity_key: Option<&PublicKey>,
    originator: &str,
) -> Result<Vec<u8>> {
    let signing_data = create_signing_data(method, path, body, timestamp, nonce);

    let protocol = Protocol::new(SecurityLevel::Counterparty, AUTH_PROTOCOL_ID);

    // Key ID includes timestamp and nonce for uniqueness
    let key_id = format!("{} {}", timestamp, nonce);

    let result = wallet
        .create_signature(
            CreateSignatureArgs {
                data: Some(signing_data),
                hash_to_directly_sign: None,
                protocol_id: protocol,
                key_id,
                counterparty: server_identity_key.map(|pk| Counterparty::Other(pk.clone())),
            },
            originator,
        )
        .await
        .map_err(|e| Error::StorageError(format!("Failed to sign request: {}", e)))?;

    Ok(result.signature)
}

/// Verifies a response signature from the server.
///
/// # Arguments
/// * `wallet` - Wallet for verification
/// * `method` - HTTP method used in request
/// * `path` - URL path
/// * `response_body` - Response body bytes
/// * `response_headers` - Parsed response auth headers
/// * `originator` - Application originator
pub async fn verify_response<W: WalletInterface>(
    wallet: &W,
    method: &str,
    path: &str,
    response_body: &[u8],
    response_headers: &ResponseAuthHeaders,
    originator: &str,
) -> Result<bool> {
    // Extract required headers
    let server_identity_key = response_headers
        .get_identity_key()?
        .ok_or_else(|| Error::AccessDenied("Response missing identity key".to_string()))?;

    let timestamp = response_headers
        .timestamp
        .ok_or_else(|| Error::AccessDenied("Response missing timestamp".to_string()))?;

    let nonce = response_headers
        .nonce
        .as_ref()
        .ok_or_else(|| Error::AccessDenied("Response missing nonce".to_string()))?;

    let signature_hex = response_headers
        .signature
        .as_ref()
        .ok_or_else(|| Error::AccessDenied("Response missing signature".to_string()))?;

    // Validate timestamp
    validate_timestamp(timestamp)?;

    // Parse signature
    let signature = from_hex(signature_hex)
        .map_err(|e| Error::AccessDenied(format!("Invalid signature format: {}", e)))?;

    // Create signing data
    let signing_data = create_signing_data(method, path, response_body, timestamp, nonce);

    let protocol = Protocol::new(SecurityLevel::Counterparty, AUTH_PROTOCOL_ID);
    let key_id = format!("{} {}", timestamp, nonce);

    // Verify signature
    let result = wallet
        .verify_signature(
            VerifySignatureArgs {
                data: Some(signing_data),
                hash_to_directly_verify: None,
                signature,
                protocol_id: protocol,
                key_id,
                counterparty: Some(Counterparty::Other(server_identity_key)),
                for_self: None,
            },
            originator,
        )
        .await
        .map_err(|e| Error::AccessDenied(format!("Signature verification failed: {}", e)))?;

    Ok(result.valid)
}

/// Creates complete authentication headers for a request.
///
/// This is the main entry point for request signing.
///
/// # Arguments
/// * `wallet` - Wallet for signing and identity key retrieval
/// * `method` - HTTP method (e.g., "POST")
/// * `path` - URL path (e.g., "/")
/// * `body` - Request body bytes
/// * `server_identity_key` - Server's identity key (optional)
/// * `originator` - Application originator string
pub async fn create_auth_headers<W: WalletInterface>(
    wallet: &W,
    method: &str,
    path: &str,
    body: &[u8],
    server_identity_key: Option<&PublicKey>,
    originator: &str,
) -> Result<AuthHeaders> {
    // Get our identity key
    let identity_result = wallet
        .get_public_key(
            GetPublicKeyArgs {
                identity_key: true,
                protocol_id: None,
                key_id: None,
                counterparty: None,
                for_self: None,
            },
            originator,
        )
        .await
        .map_err(|_| Error::AuthenticationRequired)?;

    let identity_key = identity_result.public_key;

    // Create nonce and timestamp
    let nonce = create_simple_nonce();
    let timestamp = current_timestamp_ms();

    // Sign the request
    let signature = sign_request(
        wallet,
        method,
        path,
        body,
        timestamp,
        &nonce,
        server_identity_key,
        originator,
    )
    .await?;

    Ok(AuthHeaders {
        version: AUTH_VERSION.to_string(),
        identity_key,
        nonce,
        timestamp,
        signature: to_hex(&signature),
    })
}

/// Result of verifying server authentication.
#[derive(Debug)]
pub struct AuthVerificationResult {
    /// Whether the signature was valid.
    pub signature_valid: bool,
    /// Server's identity key (if present).
    pub server_identity_key: Option<PublicKey>,
    /// Whether all required headers were present.
    pub headers_complete: bool,
}

/// Verifies server response authentication (optional).
///
/// This performs full signature verification if headers are present,
/// or returns a partial result if headers are missing.
///
/// # Arguments
/// * `wallet` - Wallet for verification
/// * `method` - HTTP method used in request
/// * `path` - URL path
/// * `response_body` - Response body bytes
/// * `response_headers` - Parsed response auth headers
/// * `originator` - Application originator
pub async fn verify_response_auth<W: WalletInterface>(
    wallet: &W,
    method: &str,
    path: &str,
    response_body: &[u8],
    response_headers: &ResponseAuthHeaders,
    originator: &str,
) -> Result<AuthVerificationResult> {
    // Check if headers are complete
    if !response_headers.is_complete() {
        return Ok(AuthVerificationResult {
            signature_valid: false,
            server_identity_key: None,
            headers_complete: false,
        });
    }

    // Get server identity key
    let server_identity_key = response_headers.get_identity_key()?;

    // Verify signature
    let signature_valid = verify_response(
        wallet,
        method,
        path,
        response_body,
        response_headers,
        originator,
    )
    .await?;

    Ok(AuthVerificationResult {
        signature_valid,
        server_identity_key,
        headers_complete: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Nonce Tests
    // =========================================================================

    #[test]
    fn test_create_simple_nonce() {
        let nonce1 = create_simple_nonce();
        let nonce2 = create_simple_nonce();

        // Nonces should be different
        assert_ne!(nonce1, nonce2);

        // Should be valid base64
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &nonce1);
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap().len(), NONCE_TOTAL_SIZE);
    }

    #[test]
    fn test_nonce_uniqueness_bulk() {
        // Generate many nonces and verify uniqueness
        let nonces: Vec<_> = (0..100).map(|_| create_simple_nonce()).collect();
        let unique: std::collections::HashSet<_> = nonces.iter().collect();
        assert_eq!(unique.len(), nonces.len(), "All nonces should be unique");
    }

    #[test]
    fn test_nonce_is_valid_base64() {
        let nonce = create_simple_nonce();

        // Should decode without error
        let decoded =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &nonce).unwrap();

        // Should be exactly 32 bytes
        assert_eq!(decoded.len(), 32);

        // Re-encoding should match original
        let re_encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &decoded);
        assert_eq!(re_encoded, nonce);
    }

    // =========================================================================
    // Timestamp Tests
    // =========================================================================

    #[test]
    fn test_current_timestamp() {
        let ts1 = current_timestamp_ms();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts2 = current_timestamp_ms();

        assert!(ts2 > ts1);
        assert!(ts2 - ts1 >= 10);
        assert!(ts2 - ts1 < 1000); // Shouldn't take more than 1 second
    }

    #[test]
    fn test_validate_timestamp_current() {
        let ts = current_timestamp_ms();
        assert!(validate_timestamp(ts).is_ok());
    }

    #[test]
    fn test_validate_timestamp_old() {
        let ts = current_timestamp_ms() - (MAX_TIMESTAMP_AGE_MS + 1000);
        assert!(validate_timestamp(ts).is_err());
    }

    #[test]
    fn test_validate_timestamp_future() {
        let ts = current_timestamp_ms() + 120_000; // 2 minutes in future
        assert!(validate_timestamp(ts).is_err());
    }

    #[test]
    fn test_validate_timestamp_slight_future_ok() {
        // Slight future (30 seconds) should be OK due to clock skew allowance
        let ts = current_timestamp_ms() + 30_000;
        assert!(validate_timestamp(ts).is_ok());
    }

    #[test]
    fn test_validate_timestamp_edge_of_expiry() {
        // Just within the 5-minute window should be OK
        let ts = current_timestamp_ms() - (MAX_TIMESTAMP_AGE_MS - 1000);
        assert!(validate_timestamp(ts).is_ok());
    }

    // =========================================================================
    // Signing Data Tests
    // =========================================================================

    #[test]
    fn test_create_signing_data() {
        let data = create_signing_data("POST", "/", b"test body", 1234567890000, "nonce123");

        // Should contain all components
        assert!(!data.is_empty());

        // Should be deterministic
        let data2 = create_signing_data("POST", "/", b"test body", 1234567890000, "nonce123");
        assert_eq!(data, data2);

        // Different inputs should produce different data
        let data3 = create_signing_data("GET", "/", b"test body", 1234567890000, "nonce123");
        assert_ne!(data, data3);
    }

    #[test]
    fn test_signing_data_includes_body_hash() {
        let data1 = create_signing_data("POST", "/", b"body1", 1234567890000, "nonce");
        let data2 = create_signing_data("POST", "/", b"body2", 1234567890000, "nonce");

        // Different bodies should produce different signing data
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_signing_data_includes_method() {
        let data_post = create_signing_data("POST", "/api", b"body", 123, "nonce");
        let data_get = create_signing_data("GET", "/api", b"body", 123, "nonce");
        assert_ne!(data_post, data_get);
    }

    #[test]
    fn test_signing_data_includes_path() {
        let data1 = create_signing_data("POST", "/api/v1", b"body", 123, "nonce");
        let data2 = create_signing_data("POST", "/api/v2", b"body", 123, "nonce");
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_signing_data_includes_timestamp() {
        let data1 = create_signing_data("POST", "/", b"body", 1000, "nonce");
        let data2 = create_signing_data("POST", "/", b"body", 2000, "nonce");
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_signing_data_includes_nonce() {
        let data1 = create_signing_data("POST", "/", b"body", 123, "nonce1");
        let data2 = create_signing_data("POST", "/", b"body", 123, "nonce2");
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_signing_data_empty_body() {
        let data = create_signing_data("POST", "/", b"", 123, "nonce");
        // Should still work with empty body
        assert!(!data.is_empty());

        // Empty body has a specific hash (SHA256 of empty bytes)
        let data_with_body = create_signing_data("POST", "/", b"x", 123, "nonce");
        assert_ne!(data, data_with_body);
    }

    #[test]
    fn test_signing_data_large_body() {
        // Large body should work (the body is hashed, so size doesn't matter)
        let large_body = vec![0u8; 1_000_000];
        let data = create_signing_data("POST", "/", &large_body, 123, "nonce");
        assert!(!data.is_empty());
    }

    // =========================================================================
    // AuthHeaders Tests
    // =========================================================================

    #[test]
    fn test_auth_headers_to_tuples() {
        let headers = AuthHeaders {
            version: AUTH_VERSION.to_string(),
            identity_key: "02abcdef".to_string(),
            nonce: "base64nonce".to_string(),
            timestamp: 1234567890000,
            signature: "hexsig".to_string(),
        };

        let tuples = headers.to_header_tuples();
        assert_eq!(tuples.len(), 5);

        // Verify all headers are present
        let headers_map: std::collections::HashMap<_, _> = tuples.into_iter().collect();
        assert_eq!(
            headers_map.get(headers::VERSION),
            Some(&AUTH_VERSION.to_string())
        );
        assert_eq!(
            headers_map.get(headers::IDENTITY_KEY),
            Some(&"02abcdef".to_string())
        );
        assert_eq!(
            headers_map.get(headers::NONCE),
            Some(&"base64nonce".to_string())
        );
        assert_eq!(
            headers_map.get(headers::TIMESTAMP),
            Some(&"1234567890000".to_string())
        );
        assert_eq!(
            headers_map.get(headers::SIGNATURE),
            Some(&"hexsig".to_string())
        );
    }

    #[test]
    fn test_auth_headers_correct_header_names() {
        // Verify header names match BRC-31/BRC-104 specification
        assert_eq!(headers::VERSION, "x-bsv-auth-version");
        assert_eq!(headers::IDENTITY_KEY, "x-bsv-auth-identity-key");
        assert_eq!(headers::NONCE, "x-bsv-auth-nonce");
        assert_eq!(headers::TIMESTAMP, "x-bsv-auth-timestamp");
        assert_eq!(headers::SIGNATURE, "x-bsv-auth-signature");
    }

    // =========================================================================
    // ResponseAuthHeaders Tests
    // =========================================================================

    #[test]
    fn test_response_auth_headers_is_complete() {
        let complete = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(complete.is_complete());

        let incomplete = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: None,
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(!incomplete.is_complete());
    }

    #[test]
    fn test_response_auth_headers_missing_version() {
        let headers = ResponseAuthHeaders {
            version: None,
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(!headers.is_complete());
    }

    #[test]
    fn test_response_auth_headers_missing_nonce() {
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: None,
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };
        assert!(!headers.is_complete());
    }

    #[test]
    fn test_response_auth_headers_missing_timestamp() {
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: None,
            signature: Some("sig".to_string()),
        };
        assert!(!headers.is_complete());
    }

    #[test]
    fn test_response_auth_headers_missing_signature() {
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("02abc".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: None,
        };
        assert!(!headers.is_complete());
    }

    #[test]
    fn test_response_auth_headers_get_identity_key_valid() {
        // Valid compressed public key (33 bytes = 66 hex chars)
        let valid_key = "02".to_string() + &"ab".repeat(32);
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some(valid_key.clone()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };

        let result = headers.get_identity_key();
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_response_auth_headers_get_identity_key_none() {
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: None,
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };

        let result = headers.get_identity_key();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_response_auth_headers_get_identity_key_invalid() {
        let headers = ResponseAuthHeaders {
            version: Some("0.1".to_string()),
            identity_key: Some("not-a-valid-key".to_string()),
            nonce: Some("nonce".to_string()),
            timestamp: Some(123456),
            signature: Some("sig".to_string()),
        };

        let result = headers.get_identity_key();
        assert!(result.is_err());
    }

    // =========================================================================
    // Constants Tests
    // =========================================================================

    #[test]
    fn test_auth_version() {
        assert_eq!(AUTH_VERSION, "0.1");
    }

    #[test]
    fn test_auth_protocol_id() {
        assert_eq!(AUTH_PROTOCOL_ID, "storage auth");
    }

    #[test]
    fn test_nonce_protocol_id() {
        assert_eq!(NONCE_PROTOCOL_ID, "storage nonce");
    }

    #[test]
    fn test_max_timestamp_age() {
        // 5 minutes in milliseconds
        assert_eq!(MAX_TIMESTAMP_AGE_MS, 5 * 60 * 1000);
    }

    // =========================================================================
    // Replay Protection Tests
    // =========================================================================

    #[test]
    fn test_replay_protection_different_nonces() {
        let nonce1 = create_simple_nonce();
        let nonce2 = create_simple_nonce();
        let timestamp = current_timestamp_ms();
        let body = b"same body";

        let data1 = create_signing_data("POST", "/", body, timestamp, &nonce1);
        let data2 = create_signing_data("POST", "/", body, timestamp, &nonce2);

        // Same body and timestamp but different nonces should produce different signing data
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_replay_protection_same_nonce_different_timestamp() {
        let nonce = create_simple_nonce();
        let timestamp1 = current_timestamp_ms();
        let timestamp2 = timestamp1 + 1000;
        let body = b"same body";

        let data1 = create_signing_data("POST", "/", body, timestamp1, &nonce);
        let data2 = create_signing_data("POST", "/", body, timestamp2, &nonce);

        // Same nonce but different timestamps should produce different signing data
        assert_ne!(data1, data2);
    }

    // =========================================================================
    // Integration-Style Tests (without actual wallet)
    // =========================================================================

    #[test]
    fn test_full_auth_header_creation_flow() {
        // Simulate the flow of creating auth headers
        let nonce = create_simple_nonce();
        let timestamp = current_timestamp_ms();
        let body = br#"{"jsonrpc":"2.0","method":"makeAvailable","params":[],"id":1}"#;

        // Create signing data
        let signing_data = create_signing_data("POST", "/", body, timestamp, &nonce);
        assert!(!signing_data.is_empty());

        // Validate timestamp
        assert!(validate_timestamp(timestamp).is_ok());

        // Create mock auth headers (signature would normally come from wallet)
        let headers = AuthHeaders {
            version: AUTH_VERSION.to_string(),
            identity_key: "02".to_string() + &"ab".repeat(32), // Mock 33-byte key
            nonce,
            timestamp,
            signature: "deadbeef".to_string(), // Mock signature
        };

        // Verify headers convert to tuples properly
        let tuples = headers.to_header_tuples();
        assert_eq!(tuples.len(), 5);
    }

    #[test]
    fn test_auth_verification_result_creation() {
        // Test creation of verification result types
        let result = AuthVerificationResult {
            signature_valid: true,
            server_identity_key: None,
            headers_complete: true,
        };

        assert!(result.signature_valid);
        assert!(result.server_identity_key.is_none());
        assert!(result.headers_complete);
    }
}
