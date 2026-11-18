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

/// Default SLAP tracker hosts for mainnet (from @bsv/sdk DEFAULT_SLAP_TRACKERS).
const MAINNET_SLAP_TRACKERS: &[&str] = &[
    "https://overlay-us-1.bsvb.tech",
    "https://overlay-eu-1.bsvb.tech",
    "https://overlay-ap-1.bsvb.tech",
];

/// Default SLAP tracker hosts for testnet.
const TESTNET_SLAP_TRACKERS: &[&str] = &["https://testnet-users.bapp.dev"];

/// Timeout for SLAP tracker queries (seconds).
const SLAP_TIMEOUT_SECS: u64 = 10;

/// Timeout for overlay service queries (seconds).
const OVERLAY_TIMEOUT_SECS: u64 = 10;

/// The most bytes one SLAP tracker's or overlay host's lookup answer may
/// carry: 32 MiB, the reference's `MAX_LOOKUP_RESPONSE_BYTES`
/// (`LookupResolver.ts`, `DEFAULT_LOOKUP_LIMITS.maxResponseBytes`) and the
/// figure bsv-rs names for its resolver's `max_response_bytes`.
pub(crate) const MAX_LOOKUP_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// A lookup answer's body refused by [`read_lookup_body`]: why, and how many
/// of its bytes were read before the refusal.
#[derive(Debug)]
pub(crate) struct LookupBodyRefused {
    /// Bytes taken from the connection before the read stopped.
    pub(crate) read: usize,
    /// Why the body was refused.
    pub(crate) reason: String,
}

impl std::fmt::Display for LookupBodyRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} bytes read)", self.reason, self.read)
    }
}

/// Read a lookup answer's body of at most `max` bytes.
///
/// A declared `Content-Length` past `max` is refused before any byte of the
/// body is read; a body with no length is read chunk by chunk and refused at
/// the chunk that passes `max`. A body longer than its declared length is
/// read to that length only (the HTTP framing stops there).
pub(crate) async fn read_lookup_body(
    mut response: reqwest::Response,
    max: usize,
) -> std::result::Result<Vec<u8>, LookupBodyRefused> {
    if let Some(declared) = response.content_length() {
        if declared > max as u64 {
            return Err(LookupBodyRefused {
                read: 0,
                reason: format!("declared length {declared} exceeds {max} bytes"),
            });
        }
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if chunk.len() > max - body.len() {
                    return Err(LookupBodyRefused {
                        read: body.len() + chunk.len(),
                        reason: format!("body exceeds {max} bytes"),
                    });
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(body),
            Err(e) => {
                return Err(LookupBodyRefused {
                    read: body.len(),
                    reason: format!("body read failed: {e}"),
                })
            }
        }
    }
}

/// HTTP-based lookup resolver that queries overlay services.
///
/// Uses two-step SLAP resolution matching the TypeScript SDK:
/// 1. Query SLAP trackers to find competent hosts for `ls_identity`
/// 2. Query those hosts for the actual identity/attribute lookup
///
/// Results are parsed from BEEF-encoded PushDrop transaction outputs.
pub struct HttpLookupResolver {
    /// SLAP tracker URLs used to discover competent hosts.
    slap_trackers: Vec<String>,
    /// HTTP client for making requests.
    client: reqwest::Client,
}

impl HttpLookupResolver {
    /// Create a new resolver with the given SLAP tracker URL.
    pub fn new(slap_tracker: &str) -> Self {
        Self {
            slap_trackers: vec![slap_tracker.to_string()],
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver with multiple SLAP tracker endpoints.
    pub fn with_endpoints(slap_trackers: Vec<String>) -> Self {
        Self {
            slap_trackers,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver for the given network preset with default SLAP trackers.
    pub fn for_network(preset: NetworkPreset) -> Self {
        let slap_trackers = match preset {
            NetworkPreset::Mainnet => MAINNET_SLAP_TRACKERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            NetworkPreset::Testnet => TESTNET_SLAP_TRACKERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            NetworkPreset::Local => vec!["http://localhost:8080".to_string()],
        };
        Self {
            slap_trackers,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new resolver for mainnet with default SLAP trackers.
    pub fn mainnet() -> Self {
        Self::for_network(NetworkPreset::Mainnet)
    }

    /// Create a new resolver for testnet with default SLAP trackers.
    pub fn testnet() -> Self {
        Self::for_network(NetworkPreset::Testnet)
    }

    /// Resolve competent hosts for a given lookup service via SLAP.
    ///
    /// Queries SLAP trackers with `{service: "ls_slap", query: {service: target}}`
    /// and extracts host URLs from the BEEF-encoded PushDrop outputs.
    async fn resolve_competent_hosts(&self, service: &str) -> Vec<String> {
        let body = serde_json::json!({
            "service": "ls_slap",
            "query": { "service": service },
        });

        for tracker in &self.slap_trackers {
            let url = format!("{}/lookup", tracker.trim_end_matches('/'));

            let response = match self
                .client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .timeout(std::time::Duration::from_secs(SLAP_TIMEOUT_SECS))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::debug!("SLAP query to {} failed (trying next): {}", tracker, e);
                    continue;
                }
            };

            if !response.status().is_success() {
                tracing::debug!(
                    "SLAP query to {} returned {} (trying next)",
                    tracker,
                    response.status()
                );
                continue;
            }

            let bytes = match read_lookup_body(response, MAX_LOOKUP_RESPONSE_BYTES).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!(
                        "SLAP response from {} refused (trying next): {}",
                        tracker,
                        e
                    );
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_slice(&bytes) {
                Ok(j) => j,
                Err(e) => {
                    tracing::debug!(
                        "SLAP response from {} not JSON (trying next): {}",
                        tracker,
                        e
                    );
                    continue;
                }
            };

            let answer: LookupAnswer = match serde_json::from_value(json) {
                Ok(a) => a,
                Err(e) => {
                    tracing::debug!(
                        "SLAP response from {} not a LookupAnswer (trying next): {}",
                        tracker,
                        e
                    );
                    continue;
                }
            };

            let hosts = parse_slap_hosts(&answer);
            if !hosts.is_empty() {
                tracing::debug!(
                    "SLAP resolved {} competent hosts for {}: {:?}",
                    hosts.len(),
                    service,
                    hosts
                );
                return hosts;
            }
        }

        tracing::debug!("No SLAP trackers returned competent hosts for {}", service);
        vec![]
    }

    /// Query the overlay endpoint and parse certificate results from the response.
    async fn query_overlay(&self, query: serde_json::Value) -> Result<Vec<OverlayCertificate>> {
        // Step 1: Resolve competent hosts for ls_identity via SLAP
        let hosts = self.resolve_competent_hosts("ls_identity").await;
        if hosts.is_empty() {
            tracing::debug!("No competent hosts for ls_identity, returning empty");
            return Ok(vec![]);
        }

        let body = serde_json::json!({
            "service": "ls_identity",
            "query": query,
        });

        // Step 2: Query each competent host until one succeeds
        for host in &hosts {
            let lookup_url = format!("{}/lookup", host.trim_end_matches('/'));

            let response = match self
                .client
                .post(&lookup_url)
                .header("Content-Type", "application/json")
                .json(&body)
                .timeout(std::time::Duration::from_secs(OVERLAY_TIMEOUT_SECS))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::debug!("Overlay lookup to {} failed (trying next): {}", host, e);
                    continue;
                }
            };

            if !response.status().is_success() {
                tracing::debug!(
                    "Overlay lookup to {} returned status {} (trying next)",
                    host,
                    response.status()
                );
                continue;
            }

            let bytes = match read_lookup_body(response, MAX_LOOKUP_RESPONSE_BYTES).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!(
                        "Overlay response from {} refused (trying next): {}",
                        host,
                        e
                    );
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_slice(&bytes) {
                Ok(json) => json,
                Err(e) => {
                    tracing::debug!(
                        "Failed to parse overlay response from {} (trying next): {}",
                        host,
                        e
                    );
                    continue;
                }
            };

            // Check for error responses (e.g. {"status":"error","message":"..."})
            if json.get("status").and_then(|s| s.as_str()) == Some("error") {
                let msg = json
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown");
                tracing::debug!("Overlay {} returned error: {} (trying next)", host, msg);
                continue;
            }

            let answer: LookupAnswer = match serde_json::from_value(json) {
                Ok(answer) => answer,
                Err(e) => {
                    tracing::debug!(
                        "Failed to deserialize LookupAnswer from {} (trying next): {}",
                        host,
                        e
                    );
                    continue;
                }
            };

            return parse_overlay_answer(&answer);
        }

        // All hosts failed - return empty gracefully
        tracing::debug!("All overlay hosts failed, returning empty results");
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

/// Parse a SLAP `LookupAnswer` to extract competent host URLs.
///
/// Each output is a BEEF-encoded PushDrop transaction. The host URL is
/// extracted from the PushDrop fields by finding the first field that
/// looks like an HTTP(S) URL.
fn parse_slap_hosts(answer: &LookupAnswer) -> Vec<String> {
    let mut hosts = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let outputs = match answer {
        LookupAnswer::OutputList { outputs } => outputs,
        _ => return hosts,
    };

    for output in outputs {
        match extract_host_from_beef(&output.beef, output.output_index) {
            Ok(host) => {
                if seen.insert(host.clone()) {
                    hosts.push(host);
                }
            }
            Err(e) => {
                tracing::debug!("Skipping SLAP output: {}", e);
            }
        }
    }

    hosts
}

/// Extract a host URL from a BEEF-encoded SLAP PushDrop output.
fn extract_host_from_beef(beef: &[u8], output_index: u32) -> Result<String> {
    let tx = Transaction::from_beef(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("SLAP BEEF parse failed: {}", e)))?;

    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("SLAP output index out of bounds".into()))?;

    let decoded = PushDrop::decode(&output.locking_script)
        .map_err(|e| crate::Error::ServiceError(format!("SLAP PushDrop decode failed: {}", e)))?;

    // Search PushDrop fields for a URL string
    for field in &decoded.fields {
        if let Ok(s) = String::from_utf8(field.clone()) {
            let trimmed = s.trim();
            if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
                return Ok(trimmed.to_string());
            }
        }
    }

    Err(crate::Error::ServiceError(
        "No URL found in SLAP PushDrop fields".into(),
    ))
}

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

    // Parse field 0 as the JSON certificate; a PushDrop with no field
    // carries none, and the output is skipped as any other malformed one.
    let field = match decoded.fields.as_slice() {
        [field, ..] => field,
        [] => {
            return Err(crate::Error::ServiceError(
                "PushDrop carries no field".into(),
            ))
        }
    };
    let cert_json = String::from_utf8(field.clone())
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
#[path = "lookup_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lookup_bounds_tests.rs"]
mod bounds_tests;
