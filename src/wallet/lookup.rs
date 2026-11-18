//! Overlay Service Discovery
//!
//! Provides trait-based abstraction for overlay service lookups used by
//! `discover_by_identity_key` and `discover_by_attributes`. The default
//! implementation (`HttpLookupResolver`) queries overlay services via HTTP
//! POST to their `/lookup` endpoint.

use async_trait::async_trait;
use std::collections::HashMap;

use bsv_rs::overlay::{LookupAnswer, NetworkPreset};
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{IdentityCertificate, WalletCertificate};

use super::lookup_checks::{
    allowed_public_hostname, answers_attributes, answers_identity_key, identity_output_certificate,
    slap_advertisement_domain,
};
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

/// The most outputs one lookup answer may carry: 4096, the reference's
/// `MAX_LOOKUP_OUTPUTS` (`LookupResolver.ts`, `DEFAULT_LOOKUP_LIMITS.maxOutputs`).
/// An answer with more is that tracker's or host's failure.
pub(crate) const MAX_LOOKUP_OUTPUTS: usize = 4096;

/// The most hosts one SLAP tracker's answer may name: 64, the reference's
/// `DEFAULT_LOOKUP_LIMITS.maxHostsPerTracker`. The 64 distinct hosts the
/// answer names earliest are called in order; the rest are not.
pub(crate) const MAX_HOSTS_PER_TRACKER: usize = 64;

/// The longest one lookup call runs, trackers and hosts together: 10 s, the
/// reference's default `deadlineMs` ("whole attempt budget including
/// discovery and queued hosts", `LookupResolver.ts` `LookupQueryOptions`).
/// A call past it answers as one whose every host failed: no certificate.
pub(crate) const LOOKUP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// The output count of an output-list answer past [`MAX_LOOKUP_OUTPUTS`].
fn outputs_past_bound(answer: &LookupAnswer) -> Option<usize> {
    match answer {
        LookupAnswer::OutputList { outputs } if outputs.len() > MAX_LOOKUP_OUTPUTS => {
            Some(outputs.len())
        }
        _ => None,
    }
}

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

/// The resolver's HTTP client: it follows no redirect. A tracker's or a
/// host's 3xx is not a success, so it is that tracker's or host's failure,
/// as the reference resolver's `redirect: 'error'` makes it; the query is
/// never sent on to a URL the answering host names.
fn lookup_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("the lookup client builds as reqwest::Client::new() does")
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
            client: lookup_client(),
        }
    }

    /// Create a new resolver with multiple SLAP tracker endpoints.
    pub fn with_endpoints(slap_trackers: Vec<String>) -> Self {
        Self {
            slap_trackers,
            client: lookup_client(),
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
            client: lookup_client(),
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

    /// Whether a host a tracker names may be plain `http`: only when every
    /// configured tracker is itself `http` (the local preset's
    /// `http://localhost:8080`, or a caller's own plain-HTTP trackers), as the
    /// reference allows HTTP for its local preset alone.
    fn allows_http_hosts(&self) -> bool {
        !self.slap_trackers.is_empty()
            && self
                .slap_trackers
                .iter()
                .all(|t| t.trim().starts_with("http://"))
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

            if let Some(count) = outputs_past_bound(&answer) {
                tracing::debug!(
                    "SLAP response from {} carries {} outputs, past {} (trying next)",
                    tracker,
                    count,
                    MAX_LOOKUP_OUTPUTS
                );
                continue;
            }

            let hosts = parse_slap_hosts(&answer, service, self.allows_http_hosts());
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

    /// Query the overlay endpoint and parse certificate results from the
    /// response, within [`LOOKUP_DEADLINE`].
    ///
    /// A certificate is returned only when it `answers` the lookup asked.
    async fn query_overlay(
        &self,
        query: serde_json::Value,
        answers: impl Fn(&OverlayCertificate) -> bool + Send + Sync,
    ) -> Result<Vec<OverlayCertificate>> {
        let lookup = self.query_overlay_unbounded(query, answers);
        match tokio::time::timeout(LOOKUP_DEADLINE, lookup).await {
            Ok(answer) => answer,
            Err(_) => {
                tracing::debug!(
                    "Lookup passed its deadline of {:?}, returning empty results",
                    LOOKUP_DEADLINE
                );
                Ok(vec![])
            }
        }
    }

    /// [`Self::query_overlay`] with no deadline of its own.
    async fn query_overlay_unbounded(
        &self,
        query: serde_json::Value,
        answers: impl Fn(&OverlayCertificate) -> bool + Send + Sync,
    ) -> Result<Vec<OverlayCertificate>> {
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

            if let Some(count) = outputs_past_bound(&answer) {
                tracing::debug!(
                    "Overlay response from {} carries {} outputs, past {} (trying next)",
                    host,
                    count,
                    MAX_LOOKUP_OUTPUTS
                );
                continue;
            }

            let certificates = parse_overlay_answer(&answer)?;
            return Ok(certificates
                .into_iter()
                .filter(|c| {
                    let kept = answers(c);
                    if !kept {
                        tracing::debug!("Skipping a certificate that does not answer the lookup");
                    }
                    kept
                })
                .collect());
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
        self.query_overlay(query, |c| answers_identity_key(c, identity_key))
            .await
    }

    async fn lookup_by_attributes(
        &self,
        attributes: &HashMap<String, String>,
    ) -> Result<Vec<OverlayCertificate>> {
        let query = serde_json::json!({
            "attributes": attributes,
        });
        self.query_overlay(query, |c| answers_attributes(c, attributes))
            .await
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Parse a SLAP `LookupAnswer` to extract competent host URLs.
///
/// Each output is a BEEF-encoded SLAP advertisement; it names a host only
/// when it is decoded and verified as the reference does at the resolver
/// (`slap_advertisement_domain`: one satoshi, the protocol `SLAP`, the
/// `service` asked for, signed and locked by its identity key's linked
/// key), and its domain is a host by [`named_host`]'s rule.
fn parse_slap_hosts(answer: &LookupAnswer, service: &str, allow_http: bool) -> Vec<String> {
    let mut hosts = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let outputs = match answer {
        LookupAnswer::OutputList { outputs } => outputs,
        _ => return hosts,
    };

    for output in outputs {
        if hosts.len() == MAX_HOSTS_PER_TRACKER {
            tracing::debug!(
                "SLAP answer names more than {} hosts; the rest are not called",
                MAX_HOSTS_PER_TRACKER
            );
            break;
        }
        match extract_host_from_beef(&output.beef, output.output_index, service, allow_http) {
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

/// Extract a host URL from a BEEF-encoded SLAP advertisement output.
fn extract_host_from_beef(
    beef: &[u8],
    output_index: u32,
    service: &str,
    allow_http: bool,
) -> Result<String> {
    // One output's locking script is all this reads: the subject is read
    // unlinked, its inputs' ancestry never built.
    let tx = Transaction::from_beef_subject(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("SLAP BEEF parse failed: {}", e)))?;

    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("SLAP output index out of bounds".into()))?;

    let domain = slap_advertisement_domain(output, service).ok_or_else(|| {
        crate::Error::ServiceError("SLAP output is not a verified advertisement".into())
    })?;
    named_host(&domain, allow_http)
        .ok_or_else(|| crate::Error::ServiceError("SLAP URL is not a host origin".into()))
}

/// The longest host URL a SLAP answer may name, in bytes: 2048, the
/// reference's bound (`canonicalOverlayEndpoint`, `normalizeLookupHost`).
const MAX_HOST_URL_BYTES: usize = 2048;

/// `text` as a host the resolver may call, by the reference's rule for an
/// advertised host (`LookupResolver.ts` `canonicalOverlayEndpoint`): an
/// `https` origin (`http` too when `allow_http`) of at most 2048 bytes,
/// with no user name or password, no query, no fragment, and no path but
/// `/`; an `https` host's name not `localhost` and no label of it in the
/// `xn--` form (`OverlayAdminTokenTemplate.ts` `isAllowedPublicHostname`).
fn named_host(text: &str, allow_http: bool) -> Option<String> {
    if text.len() > MAX_HOST_URL_BYTES {
        return None;
    }
    let url = reqwest::Url::parse(text).ok()?;
    let scheme = url.scheme() == "https" || (allow_http && url.scheme() == "http");
    let public = url.scheme() == "http" || url.host_str().is_some_and(allowed_public_hostname);
    let origin = url.host().is_some()
        && public
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.path() == "/";
    (scheme && origin).then(|| text.to_string())
}

/// Parse a `LookupAnswer` into a list of `OverlayCertificate`s.
///
/// Iterates over output-list results and reads each output's certificate
/// by [`parse_single_output`]. An output that does not pass is skipped
/// (the reference's `parseResults`).
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

/// Parse a single BEEF output into an `OverlayCertificate`: the output's
/// certificate when it passes `identity_output_certificate` (the
/// reference's `decodeIdentityOutput`: the subject's signed output, the
/// certificate record's shape, the certifier's signature, at least one
/// field revealed to `anyone`, which are its decrypted fields).
fn parse_single_output(beef: &[u8], output_index: u32) -> Result<OverlayCertificate> {
    // Parse BEEF to get the transaction; one output's locking script is all
    // this reads, so the subject is read unlinked.
    let tx = Transaction::from_beef_subject(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to parse BEEF: {}", e)))?;

    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("Output index out of bounds".into()))?;

    identity_output_certificate(&output.locking_script)
        .map_err(|why| crate::Error::ServiceError(format!("Identity output refused: {why}")))
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

#[cfg(test)]
#[path = "lookup_verify_tests.rs"]
mod verify_tests;
