//! Overlay Service Discovery
//!
//! Provides trait-based abstraction for overlay service lookups used by
//! `discover_by_identity_key` and `discover_by_attributes`. The default
//! implementation (`HttpLookupResolver`) queries overlay services via HTTP
//! POST to their `/lookup` endpoint.

use async_trait::async_trait;
use std::collections::HashMap;

use bsv_rs::overlay::{LookupAnswer, NetworkPreset};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{IdentityCertificate, WalletCertificate};

use crate::Result;

// =============================================================================
// OverlayCertificate
// =============================================================================

/// Certificate returned from overlay lookup, before conversion to SDK format.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OverlayCertificate {
    /// Certificate type identifier (base64).
    #[serde(rename = "type")]
    pub type_id: String,
    /// Unique serial number for this certificate.
    pub serial_number: String,
    /// Subject identity key (hex).
    pub subject: String,
    /// Certifier identity key (hex).
    pub certifier: String,
    /// Revocation outpoint (txid.vout format).
    #[serde(default)]
    pub revocation_outpoint: Option<String>,
    /// Certificate fields (field_name -> encrypted_value).
    #[serde(default)]
    pub fields: HashMap<String, String>,
    /// Keyring for publicly revealed fields.
    #[serde(default)]
    pub keyring: HashMap<String, String>,
    /// Certificate signature.
    #[serde(default)]
    pub signature: Option<String>,
    /// Decrypted field values (populated after decryption).
    #[serde(default)]
    pub decrypted_fields: HashMap<String, String>,
}

impl OverlayCertificate {
    /// Convert to the SDK's `IdentityCertificate` format.
    pub fn to_identity_certificate(&self) -> IdentityCertificate {
        IdentityCertificate {
            certificate: WalletCertificate {
                certificate_type: self.type_id.clone(),
                serial_number: self.serial_number.clone(),
                subject: self.subject.clone(),
                certifier: self.certifier.clone(),
                revocation_outpoint: self.revocation_outpoint.clone().unwrap_or_default(),
                fields: self.fields.clone(),
                signature: self.signature.clone().unwrap_or_default(),
            },
            certifier_info: None,
            publicly_revealed_keyring: if self.keyring.is_empty() {
                None
            } else {
                Some(self.keyring.clone())
            },
            decrypted_fields: if self.decrypted_fields.is_empty() {
                None
            } else {
                Some(self.decrypted_fields.clone())
            },
        }
    }

    /// Create a deduplication key from (type_id, serial_number).
    pub fn dedup_key(&self) -> (String, String) {
        (self.type_id.clone(), self.serial_number.clone())
    }
}

// =============================================================================
// OverlayLookupResolver trait
// =============================================================================

/// Trait for resolving identity lookups via overlay services (SLAP/SHIP).
///
/// Implementations query overlay services to discover certificates associated
/// with an identity key or matching specific attributes. The default
/// implementation (`HttpLookupResolver`) uses direct HTTP POST requests to
/// the overlay service's `/lookup` endpoint.
#[async_trait]
pub trait OverlayLookupResolver: Send + Sync {
    /// Look up certificates by identity key.
    ///
    /// Returns certificates where the subject matches the given identity key.
    /// Network errors are handled gracefully (returns empty vec).
    async fn lookup_by_identity_key(&self, identity_key: &str) -> Result<Vec<OverlayCertificate>>;

    /// Look up certificates by attributes.
    ///
    /// Returns certificates matching the given attribute key-value pairs.
    /// Network errors are handled gracefully (returns empty vec).
    async fn lookup_by_attributes(
        &self,
        attributes: &HashMap<String, String>,
    ) -> Result<Vec<OverlayCertificate>>;
}

// =============================================================================
// HttpLookupResolver
// =============================================================================

/// Default SLAP tracker hosts for mainnet.
const MAINNET_LOOKUP_HOSTS: &[&str] = &["https://lookup.babbage.systems"];

/// Default SLAP tracker hosts for testnet.
const TESTNET_LOOKUP_HOSTS: &[&str] = &["https://staging-lookup.babbage.systems"];

/// HTTP-based lookup resolver that queries overlay services.
///
/// Sends HTTP POST requests to the overlay service's `/lookup` endpoint
/// with a JSON body containing the service name and query parameters.
/// Results are parsed from BEEF-encoded PushDrop transaction outputs.
pub struct HttpLookupResolver {
    /// Overlay service endpoint URLs to query.
    endpoints: Vec<String>,
    /// HTTP client for making requests.
    client: reqwest::Client,
}

impl HttpLookupResolver {
    /// Create a new resolver with the given endpoint URL.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The overlay service URL (e.g., "https://lookup.babbage.systems")
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoints: vec![endpoint.to_string()],
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver with multiple endpoints.
    pub fn with_endpoints(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver for the given network preset with default hosts.
    pub fn for_network(preset: NetworkPreset) -> Self {
        let endpoints = match preset {
            NetworkPreset::Mainnet => MAINNET_LOOKUP_HOSTS.iter().map(|s| s.to_string()).collect(),
            NetworkPreset::Testnet => TESTNET_LOOKUP_HOSTS.iter().map(|s| s.to_string()).collect(),
            NetworkPreset::Local => vec!["http://localhost:8080".to_string()],
        };
        Self {
            endpoints,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver for mainnet with default hosts.
    pub fn mainnet() -> Self {
        Self::for_network(NetworkPreset::Mainnet)
    }

    /// Create a new resolver for testnet with default hosts.
    pub fn testnet() -> Self {
        Self::for_network(NetworkPreset::Testnet)
    }

    /// Query the overlay endpoint and parse certificate results from the response.
    async fn query_overlay(&self, query: serde_json::Value) -> Result<Vec<OverlayCertificate>> {
        let body = serde_json::json!({
            "service": "ls_identity",
            "query": query,
        });

        // Try each endpoint until one succeeds
        for endpoint in &self.endpoints {
            let lookup_url = format!("{}/lookup", endpoint.trim_end_matches('/'));

            let response = match self
                .client
                .post(&lookup_url)
                .header("Content-Type", "application/json")
                .json(&body)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::debug!("Overlay lookup to {} failed (trying next): {}", endpoint, e);
                    continue;
                }
            };

            if !response.status().is_success() {
                tracing::debug!(
                    "Overlay lookup to {} returned status {} (trying next)",
                    endpoint,
                    response.status()
                );
                continue;
            }

            let json: serde_json::Value = match response.json().await {
                Ok(json) => json,
                Err(e) => {
                    tracing::debug!(
                        "Failed to parse overlay response from {} (trying next): {}",
                        endpoint,
                        e
                    );
                    continue;
                }
            };

            // Parse the JSON response into a LookupAnswer
            let answer: LookupAnswer = match serde_json::from_value(json) {
                Ok(answer) => answer,
                Err(e) => {
                    tracing::debug!(
                        "Failed to deserialize LookupAnswer from {} (trying next): {}",
                        endpoint,
                        e
                    );
                    continue;
                }
            };

            return parse_overlay_answer(&answer);
        }

        // All endpoints failed - return empty gracefully
        tracing::debug!("All overlay endpoints failed, returning empty results");
        Ok(vec![])
    }
}

#[async_trait]
impl OverlayLookupResolver for HttpLookupResolver {
    async fn lookup_by_identity_key(&self, identity_key: &str) -> Result<Vec<OverlayCertificate>> {
        let query = serde_json::json!({
            "identityKey": identity_key,
        });
        self.query_overlay(query).await
    }

    async fn lookup_by_attributes(
        &self,
        attributes: &HashMap<String, String>,
    ) -> Result<Vec<OverlayCertificate>> {
        let query = serde_json::json!({
            "attributes": attributes,
        });
        self.query_overlay(query).await
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Parse a `LookupAnswer` into a list of `OverlayCertificate`s.
///
/// Iterates over output-list results, decodes PushDrop from each transaction
/// output, and deserializes the first field as a JSON certificate.
/// Invalid outputs are silently skipped (matching TypeScript behavior).
fn parse_overlay_answer(answer: &LookupAnswer) -> Result<Vec<OverlayCertificate>> {
    match answer {
        LookupAnswer::OutputList { outputs } => {
            let mut certs = Vec::new();
            for output in outputs {
                match parse_single_output(&output.beef, output.output_index) {
                    Ok(cert) => certs.push(cert),
                    Err(e) => {
                        tracing::debug!("Skipping invalid overlay output: {}", e);
                    }
                }
            }
            Ok(certs)
        }
        _ => Ok(vec![]),
    }
}

/// Parse a single BEEF output into an `OverlayCertificate`.
fn parse_single_output(beef: &[u8], output_index: u32) -> Result<OverlayCertificate> {
    // Parse BEEF to get transaction
    let tx = Transaction::from_beef(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to parse BEEF: {}", e)))?;

    // Get the output's locking script
    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("Output index out of bounds".into()))?;

    // Decode PushDrop from the locking script
    let decoded = PushDrop::decode(&output.locking_script)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to decode PushDrop: {}", e)))?;

    // Parse the first field as JSON certificate
    let cert_json = String::from_utf8(decoded.fields[0].clone())
        .map_err(|e| crate::Error::ServiceError(format!("Invalid UTF-8 in certificate: {}", e)))?;

    let cert: OverlayCertificate = serde_json::from_str(&cert_json)?;
    Ok(cert)
}

/// Deduplicate certificates by (type_id, serial_number), keeping the first occurrence.
pub fn dedup_certificates(certs: Vec<IdentityCertificate>) -> Vec<IdentityCertificate> {
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for cert in certs {
        let key = (
            cert.certificate.certificate_type.clone(),
            cert.certificate.serial_number.clone(),
        );
        if seen.insert(key) {
            result.push(cert);
        }
    }
    result
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlay_certificate_serde() {
        let cert = OverlayCertificate {
            type_id: "dGVzdC10eXBl".to_string(),
            serial_number: "abc123".to_string(),
            subject: "02aabbccdd".to_string(),
            certifier: "03eeff0011".to_string(),
            revocation_outpoint: Some("deadbeef.0".to_string()),
            fields: {
                let mut m = HashMap::new();
                m.insert("name".to_string(), "encrypted_value".to_string());
                m
            },
            keyring: HashMap::new(),
            signature: Some("3045022100...".to_string()),
            decrypted_fields: HashMap::new(),
        };

        let json = serde_json::to_string(&cert).unwrap();
        let deserialized: OverlayCertificate = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.type_id, cert.type_id);
        assert_eq!(deserialized.serial_number, cert.serial_number);
        assert_eq!(deserialized.subject, cert.subject);
        assert_eq!(deserialized.certifier, cert.certifier);
        assert_eq!(deserialized.revocation_outpoint, cert.revocation_outpoint);
        assert_eq!(deserialized.fields.len(), 1);
        assert_eq!(deserialized.signature, cert.signature);
    }

    #[test]
    fn test_http_resolver_creation() {
        let resolver = HttpLookupResolver::new("https://example.com");
        assert_eq!(resolver.endpoints.len(), 1);
        assert_eq!(resolver.endpoints[0], "https://example.com");

        let resolver_multi = HttpLookupResolver::with_endpoints(vec![
            "https://a.com".into(),
            "https://b.com".into(),
        ]);
        assert_eq!(resolver_multi.endpoints.len(), 2);
    }

    #[test]
    fn test_http_resolver_mainnet() {
        let resolver = HttpLookupResolver::mainnet();
        assert!(!resolver.endpoints.is_empty());
    }

    #[test]
    fn test_overlay_certificate_to_identity_certificate() {
        let cert = OverlayCertificate {
            type_id: "dGVzdC10eXBl".to_string(),
            serial_number: "abc123".to_string(),
            subject: "02aabbccdd".to_string(),
            certifier: "03eeff0011".to_string(),
            revocation_outpoint: Some("deadbeef.0".to_string()),
            fields: {
                let mut m = HashMap::new();
                m.insert("name".to_string(), "encrypted_value".to_string());
                m
            },
            keyring: {
                let mut m = HashMap::new();
                m.insert("name".to_string(), "keyring_value".to_string());
                m
            },
            signature: Some("3045022100...".to_string()),
            decrypted_fields: {
                let mut m = HashMap::new();
                m.insert("name".to_string(), "Alice".to_string());
                m
            },
        };

        let identity_cert = cert.to_identity_certificate();
        assert_eq!(identity_cert.certificate.certificate_type, "dGVzdC10eXBl");
        assert_eq!(identity_cert.certificate.serial_number, "abc123");
        assert_eq!(identity_cert.certificate.subject, "02aabbccdd");
        assert_eq!(identity_cert.certificate.certifier, "03eeff0011");
        assert_eq!(identity_cert.certificate.revocation_outpoint, "deadbeef.0");
        assert_eq!(identity_cert.certificate.signature, "3045022100...");
        assert!(identity_cert.publicly_revealed_keyring.is_some());
        assert!(identity_cert.decrypted_fields.is_some());
        assert_eq!(
            identity_cert.decrypted_fields.unwrap().get("name").unwrap(),
            "Alice"
        );
    }

    #[test]
    fn test_overlay_certificate_dedup() {
        // Create two certificates with same (type_id, serial_number)
        let cert1 = IdentityCertificate {
            certificate: WalletCertificate {
                certificate_type: "type1".to_string(),
                serial_number: "sn1".to_string(),
                subject: "subject1".to_string(),
                certifier: "certifier1".to_string(),
                revocation_outpoint: String::new(),
                fields: HashMap::new(),
                signature: String::new(),
            },
            certifier_info: None,
            publicly_revealed_keyring: None,
            decrypted_fields: None,
        };

        let cert2 = IdentityCertificate {
            certificate: WalletCertificate {
                certificate_type: "type1".to_string(),
                serial_number: "sn1".to_string(),
                subject: "subject1".to_string(),
                certifier: "certifier1".to_string(),
                revocation_outpoint: String::new(),
                fields: HashMap::new(),
                signature: "different_sig".to_string(),
            },
            certifier_info: None,
            publicly_revealed_keyring: None,
            decrypted_fields: None,
        };

        // Different (type_id, serial_number) should be kept
        let cert3 = IdentityCertificate {
            certificate: WalletCertificate {
                certificate_type: "type2".to_string(),
                serial_number: "sn2".to_string(),
                subject: "subject2".to_string(),
                certifier: "certifier2".to_string(),
                revocation_outpoint: String::new(),
                fields: HashMap::new(),
                signature: String::new(),
            },
            certifier_info: None,
            publicly_revealed_keyring: None,
            decrypted_fields: None,
        };

        let certs = vec![cert1, cert2, cert3];
        let deduped = dedup_certificates(certs);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].certificate.certificate_type, "type1");
        assert_eq!(deduped[1].certificate.certificate_type, "type2");
    }

    #[test]
    fn test_overlay_certificate_fields() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), "encrypted_name".to_string());
        fields.insert("email".to_string(), "encrypted_email".to_string());
        fields.insert("phone".to_string(), "encrypted_phone".to_string());

        let cert = OverlayCertificate {
            type_id: "dGVzdC10eXBl".to_string(),
            serial_number: "serial-001".to_string(),
            subject: "02aabbccdd".to_string(),
            certifier: "03eeff0011".to_string(),
            revocation_outpoint: Some("txid.0".to_string()),
            fields: fields.clone(),
            keyring: HashMap::new(),
            signature: Some("sig".to_string()),
            decrypted_fields: HashMap::new(),
        };

        assert_eq!(cert.fields.len(), 3);
        assert_eq!(cert.fields.get("name").unwrap(), "encrypted_name");
        assert_eq!(cert.fields.get("email").unwrap(), "encrypted_email");
        assert_eq!(cert.fields.get("phone").unwrap(), "encrypted_phone");
        assert_eq!(
            cert.dedup_key(),
            ("dGVzdC10eXBl".to_string(), "serial-001".to_string())
        );
    }

    #[test]
    fn test_overlay_certificate_empty_optional_fields() {
        let cert = OverlayCertificate {
            type_id: "type".to_string(),
            serial_number: "sn".to_string(),
            subject: "subject".to_string(),
            certifier: "certifier".to_string(),
            revocation_outpoint: None,
            fields: HashMap::new(),
            keyring: HashMap::new(),
            signature: None,
            decrypted_fields: HashMap::new(),
        };

        let identity = cert.to_identity_certificate();
        // revocation_outpoint and signature become empty strings (not Optional in WalletCertificate)
        assert_eq!(identity.certificate.revocation_outpoint, "");
        assert_eq!(identity.certificate.signature, "");
        assert!(identity.publicly_revealed_keyring.is_none());
        assert!(identity.decrypted_fields.is_none());
    }

    #[test]
    fn test_parse_overlay_answer_empty_output_list() {
        let answer = LookupAnswer::OutputList { outputs: vec![] };
        let certs = parse_overlay_answer(&answer).unwrap();
        assert!(certs.is_empty());
    }

    #[test]
    fn test_parse_overlay_answer_freeform() {
        let answer = LookupAnswer::Freeform {
            result: serde_json::json!({"some": "data"}),
        };
        let certs = parse_overlay_answer(&answer).unwrap();
        assert!(certs.is_empty());
    }

    #[test]
    fn test_multiple_certificates_same_identity() {
        // Same subject but different certificate types
        let cert1 = OverlayCertificate {
            type_id: "social_cert".to_string(),
            serial_number: "sn1".to_string(),
            subject: "02aabbccdd".to_string(),
            certifier: "03certifier1".to_string(),
            revocation_outpoint: None,
            fields: {
                let mut m = HashMap::new();
                m.insert("name".to_string(), "Alice".to_string());
                m
            },
            keyring: HashMap::new(),
            signature: Some("sig1".to_string()),
            decrypted_fields: HashMap::new(),
        };

        let cert2 = OverlayCertificate {
            type_id: "email_cert".to_string(),
            serial_number: "sn2".to_string(),
            subject: "02aabbccdd".to_string(),
            certifier: "03certifier2".to_string(),
            revocation_outpoint: None,
            fields: {
                let mut m = HashMap::new();
                m.insert("email".to_string(), "alice@example.com".to_string());
                m
            },
            keyring: HashMap::new(),
            signature: Some("sig2".to_string()),
            decrypted_fields: HashMap::new(),
        };

        let certs = vec![cert1, cert2];
        let identity_certs: Vec<IdentityCertificate> =
            certs.iter().map(|c| c.to_identity_certificate()).collect();

        assert_eq!(identity_certs.len(), 2);
        assert_eq!(
            identity_certs[0].certificate.certificate_type,
            "social_cert"
        );
        assert_eq!(identity_certs[1].certificate.certificate_type, "email_cert");
        // Same subject on both
        assert_eq!(identity_certs[0].certificate.subject, "02aabbccdd");
        assert_eq!(identity_certs[1].certificate.subject, "02aabbccdd");

        // Dedup should keep both since they have different (type_id, serial_number)
        let deduped = dedup_certificates(identity_certs);
        assert_eq!(deduped.len(), 2);
    }
}
