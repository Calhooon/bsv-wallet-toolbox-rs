//! Wallet Permissions Manager - BRC-98/99 Permission Enforcement
//!
//! This module provides BRC-98/99 permission management for BSV wallets.
//! It enforces protocol access (DPACP), basket access (DBAP), certificate
//! access (DCAP), and spending authorization (DSAP) permissions.
//!
//! The manager checks for valid permission tokens before allowing operations,
//! caches permissions with a 5-minute TTL, and supports pluggable permission
//! request handlers for user consent flows.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use async_trait::async_trait;
use bsv_rs::primitives::ec::Signature;
use bsv_rs::primitives::hash::sha256;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{
    Counterparty, DecryptArgs, GetPublicKeyArgs, ListOutputsArgs, OutputInclude, Protocol,
    QueryMode, SecurityLevel, WalletInterface, WalletOutput,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{OnceCell, RwLock};

use crate::error::{Error, Result};

// =============================================================================
// Permission Request Handler
// =============================================================================

/// Callback type for requesting permissions from the user.
///
/// When a permission token is not found or has expired, this handler is invoked
/// to prompt the user for consent. The handler receives a `PermissionRequest`
/// and should return a `PermissionToken` if the user grants the permission.
pub type PermissionRequestHandler = Arc<
    dyn Fn(PermissionRequest) -> Pin<Box<dyn Future<Output = Result<PermissionToken>> + Send>>
        + Send
        + Sync,
>;

// =============================================================================
// Usage Type Enums
// =============================================================================

/// Protocol usage type - determines which config flag to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionUsageType {
    /// Signing operations (createSignature)
    Signing,
    /// Encryption operations (encrypt/decrypt)
    Encrypting,
    /// HMAC operations (createHmac/verifyHmac)
    Hmac,
    /// Public key revelation (getPublicKey)
    PublicKey,
    /// Identity key revelation (getPublicKey with identity)
    IdentityKey,
    /// Key linkage revelation (revealCounterpartyKeyLinkage, revealSpecificKeyLinkage)
    LinkageRevelation,
    /// Generic protocol usage
    Generic,
}

/// Basket usage type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasketUsageType {
    /// Inserting outputs into a basket (createAction, internalizeAction)
    Insertion,
    /// Removing outputs from a basket (relinquishOutput)
    Removal,
    /// Listing outputs in a basket (listOutputs)
    Listing,
}

/// Certificate usage type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateUsageType {
    /// Disclosing certificate fields (proveCertificate)
    Disclosure,
}

// =============================================================================
// PermissionsModule Trait
// =============================================================================

/// A permissions module handles request/response transformation for a specific scheme.
///
/// Modules are registered by scheme ID and handle basket/protocol names of the form:
/// `p <schemeID> <rest...>`
#[async_trait]
pub trait PermissionsModule: Send + Sync {
    /// Transforms the request before it's passed to the underlying wallet.
    async fn on_request(
        &self,
        method: &str,
        args: serde_json::Value,
        originator: &str,
    ) -> std::result::Result<serde_json::Value, String>;

    /// Transforms the response from the underlying wallet.
    async fn on_response(
        &self,
        res: serde_json::Value,
        method: &str,
        originator: &str,
    ) -> std::result::Result<serde_json::Value, String>;
}

// =============================================================================
// Permission Data Types
// =============================================================================

/// Describes a group of permissions that can be requested together (BRC-73).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupedPermissions {
    /// Description of what these permissions are for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Spending authorization (amount limit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spending_authorization: Option<SpendingAuthorization>,
    /// Protocol permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_permissions: Option<Vec<ProtocolPermission>>,
    /// Basket access permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basket_access: Option<Vec<BasketAccess>>,
    /// Certificate access permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_access: Option<Vec<CertificateAccess>>,
}

/// Spending authorization permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendingAuthorization {
    /// Maximum amount in satoshis.
    pub amount: u64,
    /// Description of what the spending is for.
    pub description: String,
}

/// Protocol permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolPermission {
    /// The protocol ID (security level + name).
    pub protocol_id: Protocol,
    /// Optional counterparty restriction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counterparty: Option<String>,
    /// Description of the protocol usage.
    pub description: String,
}

/// Basket access permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasketAccess {
    /// The basket name.
    pub basket: String,
    /// Description of the basket access.
    pub description: String,
}

/// Certificate access permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAccess {
    /// Certificate type.
    #[serde(rename = "type")]
    pub cert_type: String,
    /// Fields to access.
    pub fields: Vec<String>,
    /// Verifier public key.
    pub verifier_public_key: String,
    /// Description of the certificate access.
    pub description: String,
}

/// A permission request from an application.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequest {
    /// Type of permission being requested.
    #[serde(rename = "type")]
    pub request_type: PermissionType,
    /// The requesting application's originator.
    pub originator: String,
    /// Optional display originator for UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_originator: Option<String>,
    /// Whether privileged access is requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privileged: Option<bool>,
    /// Protocol ID for protocol permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_id: Option<Protocol>,
    /// Counterparty for protocol permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counterparty: Option<String>,
    /// Basket name for basket permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basket: Option<String>,
    /// Certificate details for certificate permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<CertificatePermissionDetails>,
    /// Spending details for spending permissions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spending: Option<SpendingPermissionDetails>,
    /// Human-readable reason for the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The wallet operation being requested (e.g., "createAction", "signAction").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// The id the host answers with through
    /// [`WalletPermissionsManager::grant_permission`] or
    /// [`WalletPermissionsManager::deny_permission`]: the reference's event
    /// `requestID` (ts-stack@edf6e03 `WalletPermissionsManager.ts:2671-2690`),
    /// set by the manager on the request the handler receives.
    #[serde(rename = "requestID", default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Whether the request renews an expired token (`:336`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewal: Option<bool>,
    /// The expired token a renewal replaces (`:337`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_token: Option<PermissionToken>,
}

impl PermissionRequest {
    /// A request of `request_type` from `originator` naming nothing else.
    pub(crate) fn bare(request_type: PermissionType, originator: &str) -> Self {
        Self {
            request_type,
            originator: originator.to_string(),
            display_originator: None,
            privileged: None,
            protocol_id: None,
            counterparty: None,
            basket: None,
            certificate: None,
            spending: None,
            reason: None,
            operation: None,
            request_id: None,
            renewal: None,
            previous_token: None,
        }
    }
}

/// Type of permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionType {
    Protocol,
    Basket,
    Certificate,
    Spending,
}

/// Certificate permission details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificatePermissionDetails {
    pub verifier: String,
    pub cert_type: String,
    pub fields: Vec<String>,
}

/// Spending permission details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendingPermissionDetails {
    pub satoshis: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_items: Option<Vec<SpendingLineItem>>,
}

/// A line item in a spending request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendingLineItem {
    #[serde(rename = "type")]
    pub item_type: String,
    pub description: String,
    pub satoshis: u64,
}

/// An on-chain permission token.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionToken {
    /// Transaction ID where token resides.
    pub txid: String,
    /// Raw transaction bytes.
    pub tx: Vec<u8>,
    /// Output index.
    pub output_index: u32,
    /// Locking script hex.
    pub output_script: String,
    /// Satoshis in the output.
    pub satoshis: u64,
    /// Originator allowed to use this permission.
    pub originator: String,
    /// The originator as captured when the token was minted, unnormalized
    /// (ts-stack@edf6e03 `WalletPermissionsManager.ts:376-380`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_originator: Option<String>,
    /// Expiration time (UNIX epoch seconds).
    pub expiry: u64,
    /// Whether privileged access is granted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privileged: Option<bool>,
    /// Protocol name (DPACP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Security level (DPACP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_level: Option<u8>,
    /// Counterparty (DPACP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counterparty: Option<String>,
    /// Basket name (DBAP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basket_name: Option<String>,
    /// Certificate type (DCAP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_type: Option<String>,
    /// Certificate fields (DCAP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_fields: Option<Vec<String>>,
    /// Verifier public key (DCAP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verifier: Option<String>,
    /// Authorized spending amount (DSAP).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorized_amount: Option<u64>,
}

// =============================================================================
// Cached Permission
// =============================================================================

/// Internal cached permission entry with TTL tracking.
struct CachedPermission {
    /// The permission token expiry (UNIX epoch seconds).
    expiry: u64,
    /// When this entry was cached.
    cached_at: Instant,
}

/// Cache TTL: 5 minutes.
const CACHE_TTL_SECS: u64 = 5 * 60;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the permissions manager.
#[derive(Clone, Default)]
pub struct WalletPermissionsManagerConfig {
    /// Permission modules by scheme ID.
    pub permission_modules: HashMap<String, Arc<dyn PermissionsModule>>,
    /// Seek protocol permissions for signing operations.
    pub seek_protocol_permissions_for_signing: bool,
    /// Seek protocol permissions for encryption operations.
    pub seek_protocol_permissions_for_encrypting: bool,
    /// Seek protocol permissions for HMAC operations.
    pub seek_protocol_permissions_for_hmac: bool,
    /// Seek permissions for key linkage revelation.
    pub seek_permissions_for_key_linkage_revelation: bool,
    /// Seek permissions for public key revelation.
    pub seek_permissions_for_public_key_revelation: bool,
    /// Seek permissions for identity key revelation.
    pub seek_permissions_for_identity_key_revelation: bool,
    /// Seek permissions for identity resolution.
    pub seek_permissions_for_identity_resolution: bool,
    /// Seek basket insertion permissions.
    pub seek_basket_insertion_permissions: bool,
    /// Seek basket removal permissions.
    pub seek_basket_removal_permissions: bool,
    /// Seek basket listing permissions.
    pub seek_basket_listing_permissions: bool,
    /// Seek permission when applying action labels.
    pub seek_permission_when_applying_action_labels: bool,
    /// Seek permission when listing actions by label.
    pub seek_permission_when_listing_actions_by_label: bool,
    /// Seek certificate disclosure permissions.
    pub seek_certificate_disclosure_permissions: bool,
    /// Seek certificate acquisition permissions.
    pub seek_certificate_acquisition_permissions: bool,
    /// Seek certificate relinquishment permissions.
    pub seek_certificate_relinquishment_permissions: bool,
    /// Seek certificate listing permissions.
    pub seek_certificate_listing_permissions: bool,
    /// Encrypt wallet metadata.
    pub encrypt_wallet_metadata: bool,
    /// Seek spending permissions.
    pub seek_spending_permissions: bool,
    /// Seek grouped permission based on manifest.
    pub seek_grouped_permission: bool,
    /// Differentiate privileged operations.
    pub differentiate_privileged_operations: bool,
    // --- Operation-level permission flags ---
    /// Whether to enforce operation-level permissions.
    pub enforce_permissions: bool,
    /// Allow createAction operations.
    pub allow_create_action: bool,
    /// Allow signAction operations.
    pub allow_sign_action: bool,
    /// Allow abortAction operations.
    pub allow_abort_action: bool,
    /// Allow listActions operations.
    pub allow_list_actions: bool,
    /// Allow internalizeAction operations.
    pub allow_internalize_action: bool,
    /// Allow listOutputs operations.
    pub allow_list_outputs: bool,
    /// Allow relinquishOutput operations.
    pub allow_relinquish_output: bool,
    /// Allow acquireCertificate operations.
    pub allow_acquire_certificate: bool,
    /// Allow listCertificates operations.
    pub allow_list_certificates: bool,
    /// Allow proveCertificate operations.
    pub allow_prove_certificate: bool,
    /// Allow relinquishCertificate operations.
    pub allow_relinquish_certificate: bool,
    /// Allow discover operations (discoverByIdentityKey, discoverByAttributes).
    pub allow_discover: bool,
    /// Allow crypto operations (getPublicKey, encrypt, decrypt, createHmac, verifyHmac, createSignature, verifySignature).
    pub allow_crypto: bool,
}

impl WalletPermissionsManagerConfig {
    /// Creates a new config with all permission checks enabled (most secure).
    pub fn all_enabled() -> Self {
        Self {
            permission_modules: HashMap::new(),
            seek_protocol_permissions_for_signing: true,
            seek_protocol_permissions_for_encrypting: true,
            seek_protocol_permissions_for_hmac: true,
            seek_permissions_for_key_linkage_revelation: true,
            seek_permissions_for_public_key_revelation: true,
            seek_permissions_for_identity_key_revelation: true,
            seek_permissions_for_identity_resolution: true,
            seek_basket_insertion_permissions: true,
            seek_basket_removal_permissions: true,
            seek_basket_listing_permissions: true,
            seek_permission_when_applying_action_labels: true,
            seek_permission_when_listing_actions_by_label: true,
            seek_certificate_disclosure_permissions: true,
            seek_certificate_acquisition_permissions: true,
            seek_certificate_relinquishment_permissions: true,
            seek_certificate_listing_permissions: true,
            encrypt_wallet_metadata: true,
            seek_spending_permissions: true,
            seek_grouped_permission: true,
            differentiate_privileged_operations: true,
            enforce_permissions: true,
            allow_create_action: true,
            allow_sign_action: true,
            allow_abort_action: true,
            allow_list_actions: true,
            allow_internalize_action: true,
            allow_list_outputs: true,
            allow_relinquish_output: true,
            allow_acquire_certificate: true,
            allow_list_certificates: true,
            allow_prove_certificate: true,
            allow_relinquish_certificate: true,
            allow_discover: true,
            allow_crypto: true,
        }
    }

    /// Creates a new config with all permission checks disabled.
    pub fn all_disabled() -> Self {
        Self::default()
    }
}

// =============================================================================
// WalletPermissionsManager
// =============================================================================

/// Wallet permissions manager implementing BRC-98/99 permission enforcement.
///
/// This manager enforces four categories of permissions:
/// - **Protocol (DPACP)**: Access to specific protocols at security levels
/// - **Basket (DBAP)**: Access to specific output baskets
/// - **Certificate (DCAP)**: Access to certificate field disclosure
/// - **Spending (DSAP)**: Authorization for spending operations
///
/// # Permission Flow
///
/// 1. Admin originator always bypasses all checks
/// 2. Security level 0 (Silent) bypasses protocol checks
/// 3. Admin-reserved names (starting with "admin", basket "default") are blocked
/// 4. Per-usage-type config flags can disable specific checks
/// 5. In-memory cache with 5-minute TTL provides fast repeated access
/// 6. If no cached permission, invokes the permission request handler
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig};
///
/// let manager = WalletPermissionsManager::new(
///     underlying_wallet,
///     "admin.wallet".to_string(),
///     WalletPermissionsManagerConfig::all_enabled(),
/// );
///
/// // Check protocol permission
/// let allowed = manager.ensure_protocol_permission(
///     "app.example.com",
///     false,
///     &protocol,
///     Some("counterparty_key"),
///     None,
///     PermissionUsageType::Signing,
/// ).await?;
/// ```
pub struct WalletPermissionsManager {
    /// The underlying wallet.
    underlying: Arc<dyn WalletInterface + Send + Sync>,
    /// Admin originator (always allowed).
    admin_originator: String,
    /// Configuration.
    config: WalletPermissionsManagerConfig,
    /// Permission cache with TTL.
    permission_cache: RwLock<HashMap<String, CachedPermission>>,
    /// Optional permission request handler for user consent.
    permission_request_handler: RwLock<Option<PermissionRequestHandler>>,
    /// Counterparty identity keys (hex) to the protocol names admitted for
    /// them with no prompt; empty unless set (#725).
    whitelisted_counterparties: HashMap<String, Vec<String>>,
    /// The stored tokens' locking key, read once (#751).
    permission_token_locking_key: OnceCell<String>,
    /// The requests awaiting the host's answer, by request id
    /// (`activeRequests`, ts-stack@edf6e03 `WalletPermissionsManager.ts:2652-2668`).
    active_requests: StdMutex<HashMap<String, permission_grant::ActiveRequest>>,
    /// The spending requests' sequence, one prompt per spend (`:710`, `:5830-5835`).
    spending_request_sequence: AtomicU64,
    /// The mints in flight, by request key (`mintsInFlight`, `:702`): a
    /// receiver whose sender drops when the mint ends.
    mints_in_flight: StdMutex<HashMap<String, tokio::sync::watch::Receiver<()>>>,
    /// The recent grants' cover ends, by request key (`recentGrants`, `:675`).
    recent_grants: StdMutex<HashMap<String, Instant>>,
}

impl WalletPermissionsManager {
    /// Creates a new permissions manager.
    pub fn new(
        underlying: Arc<dyn WalletInterface + Send + Sync>,
        admin_originator: String,
        config: WalletPermissionsManagerConfig,
    ) -> Self {
        Self {
            underlying,
            admin_originator,
            config,
            permission_cache: RwLock::new(HashMap::new()),
            permission_request_handler: RwLock::new(None),
            whitelisted_counterparties: HashMap::new(),
            permission_token_locking_key: OnceCell::new(),
            active_requests: StdMutex::new(HashMap::new()),
            spending_request_sequence: AtomicU64::new(0),
            mints_in_flight: StdMutex::new(HashMap::new()),
            recent_grants: StdMutex::new(HashMap::new()),
        }
    }

    /// The manager admitting, with no prompt, an everyday protocol call
    /// whose counterparty is a key in `whitelist` and whose protocol name is
    /// in that key's list: the reference's `whitelistedCounterparties`
    /// (ts-stack@edf6e03 `WalletPermissionsManager.ts:581-586`), a
    /// counterparty identity key's hex to protocol names (#725). A
    /// privileged call under `differentiate_privileged_operations` and a
    /// level-1 call, which names no counterparty, still ask.
    pub fn with_whitelisted_counterparties(
        mut self,
        whitelist: HashMap<String, Vec<String>>,
    ) -> Self {
        self.whitelisted_counterparties = whitelist;
        self
    }

    /// Gets the underlying wallet.
    pub fn wallet(&self) -> Arc<dyn WalletInterface + Send + Sync> {
        self.underlying.clone()
    }

    /// Gets the underlying wallet, checking originator is not admin.
    pub fn wallet_for_originator(
        &self,
        originator: &str,
    ) -> Result<Arc<dyn WalletInterface + Send + Sync>> {
        if originator == self.admin_originator {
            return Err(Error::AccessDenied(
                "External applications cannot use the admin originator".to_string(),
            ));
        }
        Ok(self.underlying.clone())
    }

    /// Returns the admin originator.
    pub fn admin_originator(&self) -> &str {
        &self.admin_originator
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &WalletPermissionsManagerConfig {
        &self.config
    }

    /// Sets the permission request handler for user consent flows.
    pub async fn set_permission_request_handler(&self, handler: PermissionRequestHandler) {
        let mut h = self.permission_request_handler.write().await;
        *h = Some(handler);
    }

    /// Clears the permission request handler.
    pub async fn clear_permission_request_handler(&self) {
        let mut h = self.permission_request_handler.write().await;
        *h = None;
    }

    /// Checks if the originator is the admin (always allowed).
    fn is_admin(&self, originator: &str) -> bool {
        originator == self.admin_originator
    }

    // =========================================================================
    // Admin-Reserved Checks (static helpers)
    // =========================================================================

    /// Checks if a protocol is admin-reserved (name starts with "admin").
    pub fn is_admin_protocol(protocol: &Protocol) -> bool {
        protocol.protocol_name.starts_with("admin")
    }

    /// Checks if a basket is admin-reserved ("default" or starts with "admin").
    pub fn is_admin_basket(basket: &str) -> bool {
        basket == "default" || basket.starts_with("admin")
    }

    // =========================================================================
    // Cache Operations
    // =========================================================================

    /// Builds a cache key for a permission request.
    /// Matches TypeScript's `buildRequestKey` format.
    #[allow(clippy::too_many_arguments)]
    pub fn build_cache_key(
        perm_type: PermissionType,
        originator: &str,
        privileged: Option<bool>,
        protocol: Option<&Protocol>,
        counterparty: Option<&str>,
        basket: Option<&str>,
        certificate: Option<&CertificatePermissionDetails>,
        satoshis: Option<u64>,
    ) -> String {
        match perm_type {
            PermissionType::Protocol => {
                let proto_str = protocol
                    .map(|p| format!("{},{}", p.security_level.as_u8(), p.protocol_name))
                    .unwrap_or_default();
                let cp = counterparty.unwrap_or("undefined");
                format!(
                    "proto:{}:{}:{}:{}",
                    originator,
                    privileged.unwrap_or(false),
                    proto_str,
                    cp
                )
            }
            PermissionType::Basket => {
                let b = basket.unwrap_or("undefined");
                format!("basket:{}:{}", originator, b)
            }
            PermissionType::Certificate => {
                let (v, ct, f) = certificate
                    .map(|c| {
                        (
                            c.verifier.as_str(),
                            c.cert_type.as_str(),
                            c.fields.join("|"),
                        )
                    })
                    .unwrap_or(("", "", String::new()));
                format!(
                    "cert:{}:{}:{}:{}:{}",
                    originator,
                    privileged.unwrap_or(false),
                    v,
                    ct,
                    f
                )
            }
            PermissionType::Spending => {
                let s = satoshis.unwrap_or(0);
                format!("spend:{}:{}", originator, s)
            }
        }
    }

    /// Checks if a permission is cached and still valid.
    pub async fn is_permission_cached(&self, key: &str) -> bool {
        let mut cache = self.permission_cache.write().await;
        if let Some(entry) = cache.get(key) {
            // Check cache TTL
            if entry.cached_at.elapsed().as_secs() > CACHE_TTL_SECS {
                cache.remove(key);
                return false;
            }
            // Check token expiry
            if entry.expiry > 0 {
                let now = Utc::now().timestamp() as u64;
                if entry.expiry < now {
                    cache.remove(key);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    /// Caches a permission with its expiry.
    pub async fn cache_permission(&self, key: String, expiry: u64) {
        let mut cache = self.permission_cache.write().await;
        cache.insert(
            key,
            CachedPermission {
                expiry,
                cached_at: Instant::now(),
            },
        );
    }

    // =========================================================================
    // Permission Request Flow
    // =========================================================================

    // The request flow, `grant_permission` and `deny_permission` are in
    // `permission_grant.rs`.

    // =========================================================================
    // Enforcement Methods
    // =========================================================================

    /// Ensures the originator has protocol usage permission (DPACP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Security level 0 (Silent) bypass
    /// 3. Admin-reserved protocol blocking
    /// 4. Usage-type config flag bypass
    /// 5. Whitelisted counterparty bypass for an everyday call
    /// 6. Cache check
    /// 7. The stored token in the admin basket (#751)
    /// 8. Permission request flow (if handler registered)
    pub async fn ensure_protocol_permission(
        &self,
        originator: &str,
        privileged: bool,
        protocol: &Protocol,
        counterparty: Option<&str>,
        reason: Option<&str>,
        usage_type: PermissionUsageType,
    ) -> Result<bool> {
        self.ensure_protocol_permission_seeking(
            originator,
            privileged,
            true,
            protocol,
            counterparty,
            reason,
            usage_type,
        )
        .await
    }

    /// [`Self::ensure_protocol_permission`] under the request's
    /// `seekPermission` (the reference's default `true`): with it false, no
    /// grant is refused with the reference's text and no prompt
    /// (ts-stack@edf6e03 `WalletPermissionsManager.ts:1487`, `:1533`; #720).
    /// Ahead of the refusal and the prompt the stored token is looked up
    /// (`:1522-1566`, #751 step 1): a live token admits and is cached, an
    /// expired one refuses a seek-false call with `:1549`'s text and prompts
    /// otherwise. A persistent grant mints that token (#751 step 2a), so
    /// it admits a seek-false call past the cache and across a restart.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn ensure_protocol_permission_seeking(
        &self,
        originator: &str,
        privileged: bool,
        seek_permission: bool,
        protocol: &Protocol,
        counterparty: Option<&str>,
        reason: Option<&str>,
        usage_type: PermissionUsageType,
    ) -> Result<bool> {
        // 1) Admin originator can do anything
        if self.is_admin(originator) {
            return Ok(true);
        }

        // 2) Security level 0 (Silent) = open usage
        if protocol.security_level == SecurityLevel::Silent {
            return Ok(true);
        }
        // A level-1 grant names no counterparty: the reference's
        // `if (level === 1) counterparty = ''` (ts-stack@edf6e03
        // `WalletPermissionsManager.ts:1505`), absent here (#581).
        let counterparty = if protocol.security_level == SecurityLevel::App {
            None
        } else {
            counterparty
        };

        // 3) Admin-reserved protocol blocking
        if Self::is_admin_protocol(protocol) {
            return Err(Error::AccessDenied(format!(
                "Protocol \"{}\" is admin-only.",
                protocol.protocol_name
            )));
        }

        // 4) Usage-type config flag bypass
        match usage_type {
            PermissionUsageType::Signing => {
                if !self.config.seek_protocol_permissions_for_signing {
                    return Ok(true);
                }
            }
            PermissionUsageType::Encrypting => {
                if !self.config.seek_protocol_permissions_for_encrypting {
                    return Ok(true);
                }
            }
            PermissionUsageType::Hmac => {
                if !self.config.seek_protocol_permissions_for_hmac {
                    return Ok(true);
                }
            }
            PermissionUsageType::PublicKey => {
                if !self.config.seek_permissions_for_public_key_revelation {
                    return Ok(true);
                }
            }
            PermissionUsageType::IdentityKey => {
                if !self.config.seek_permissions_for_identity_key_revelation {
                    return Ok(true);
                }
            }
            PermissionUsageType::LinkageRevelation => {
                if !self.config.seek_permissions_for_key_linkage_revelation {
                    return Ok(true);
                }
            }
            PermissionUsageType::Generic => {}
        }

        // Normalize privileged flag
        let priv_flag = if self.config.differentiate_privileged_operations {
            privileged
        } else {
            false
        };
        // 5) A whitelisted counterparty's everyday call needs no grant
        // (ts-stack@edf6e03 `WalletPermissionsManager.ts:1516`, #725).
        if !priv_flag && self.is_whitelisted_counterparty_protocol(counterparty, protocol) {
            return Ok(true);
        }

        // 6) Cache check
        let cache_key = Self::build_cache_key(
            PermissionType::Protocol,
            originator,
            Some(priv_flag),
            Some(protocol),
            counterparty,
            None,
            None,
            None,
        );
        // The cache, the recent-grant cover or a mint in flight (`:1518-1519`).
        if self.has_recent_or_pending_grant(&cache_key).await {
            return Ok(true);
        }

        // 7) The stored token (ts-stack@edf6e03 `:1522-1566`, #751)
        let query = TokenQuery::Protocol {
            privileged: priv_flag,
            protocol,
            counterparty: counterparty.unwrap_or("self"),
        };
        let stored = self.stored_token(originator, &query, &cache_key).await?;
        if stored == StoredToken::Live {
            return Ok(true);
        }
        if !seek_permission {
            return Err(Error::AccessDenied(stored.seek_false_refusal().to_string()));
        }

        // 8) Request permission flow
        self.request_permission_flow(PermissionRequest {
            privileged: Some(priv_flag),
            protocol_id: Some(protocol.clone()),
            counterparty: counterparty.map(|s| s.to_string()),
            reason: reason.map(|s| s.to_string()),
            ..PermissionRequest::bare(PermissionType::Protocol, originator)
        })
        .await
    }

    /// Whether `counterparty` is whitelisted for `protocol`'s name: the
    /// counterparty as passed or in either case, never `self`, `anyone` or
    /// none; the name compared case-blind (ts-stack@edf6e03
    /// `WalletPermissionsManager.ts:5731-5742`).
    fn is_whitelisted_counterparty_protocol(
        &self,
        counterparty: Option<&str>,
        protocol: &Protocol,
    ) -> bool {
        let counterparty = match counterparty {
            None | Some("" | "self" | "anyone") => return false,
            Some(counterparty) => counterparty,
        };
        let whitelist = &self.whitelisted_counterparties;
        let protocols = [
            counterparty.to_string(),
            counterparty.to_lowercase(),
            counterparty.to_uppercase(),
        ]
        .iter()
        .find_map(|key| whitelist.get(key));
        let name = protocol.protocol_name.to_lowercase();
        match protocols {
            Some(names) if !name.is_empty() => names.iter().any(|n| n.to_lowercase() == name),
            _ => false,
        }
    }

    /// Ensures the originator has basket access permission (DBAP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Admin-reserved basket blocking
    /// 3. Usage-type config flag bypass
    /// 4. Cache check, then the stored token (#751)
    /// 5. Permission request flow
    pub async fn ensure_basket_access(
        &self,
        originator: &str,
        basket: &str,
        reason: Option<&str>,
        usage_type: BasketUsageType,
    ) -> Result<bool> {
        // 1) Admin bypass
        if self.is_admin(originator) {
            return Ok(true);
        }

        // 2) Admin-reserved basket blocking
        if Self::is_admin_basket(basket) {
            return Err(Error::AccessDenied(format!(
                "Basket \"{}\" is admin-only.",
                basket
            )));
        }

        // 3) Usage-type config flag bypass
        match usage_type {
            BasketUsageType::Insertion => {
                if !self.config.seek_basket_insertion_permissions {
                    return Ok(true);
                }
            }
            BasketUsageType::Removal => {
                if !self.config.seek_basket_removal_permissions {
                    return Ok(true);
                }
            }
            BasketUsageType::Listing => {
                if !self.config.seek_basket_listing_permissions {
                    return Ok(true);
                }
            }
        }

        // 4) Cache check
        let cache_key = Self::build_cache_key(
            PermissionType::Basket,
            originator,
            None,
            None,
            None,
            Some(basket),
            None,
            None,
        );
        if self.has_recent_or_pending_grant(&cache_key).await {
            return Ok(true);
        }
        // The stored token (`:1595-1615`, #751); an expired one prompts.
        let query = TokenQuery::Basket { basket };
        if self.stored_token(originator, &query, &cache_key).await? == StoredToken::Live {
            return Ok(true);
        }

        // 5) Request permission flow
        self.request_permission_flow(PermissionRequest {
            basket: Some(basket.to_string()),
            reason: reason.map(|s| s.to_string()),
            ..PermissionRequest::bare(PermissionType::Basket, originator)
        })
        .await
    }

    /// Ensures the originator has certificate access permission (DCAP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Usage-type config flag bypass
    /// 3. Privileged normalization
    /// 4. Cache check, then the stored token (#751)
    /// 5. Permission request flow
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_certificate_access(
        &self,
        originator: &str,
        privileged: bool,
        verifier: &str,
        cert_type: &str,
        fields: &[String],
        reason: Option<&str>,
        usage_type: CertificateUsageType,
    ) -> Result<bool> {
        // 1) Admin bypass
        if self.is_admin(originator) {
            return Ok(true);
        }

        // 2) Usage-type config flag bypass
        match usage_type {
            CertificateUsageType::Disclosure => {
                if !self.config.seek_certificate_disclosure_permissions {
                    return Ok(true);
                }
            }
        }

        // 3) Normalize privileged flag
        let priv_flag = if self.config.differentiate_privileged_operations {
            privileged
        } else {
            false
        };

        // 4) Cache check
        let cert_details = CertificatePermissionDetails {
            verifier: verifier.to_string(),
            cert_type: cert_type.to_string(),
            fields: fields.to_vec(),
        };
        let cache_key = Self::build_cache_key(
            PermissionType::Certificate,
            originator,
            Some(priv_flag),
            None,
            None,
            None,
            Some(&cert_details),
            None,
        );
        if self.has_recent_or_pending_grant(&cache_key).await {
            return Ok(true);
        }
        // The stored token (`:1669-1709`, #751); an expired one prompts.
        let query = TokenQuery::Certificate {
            privileged: priv_flag,
            verifier,
            cert_type,
            fields,
        };
        if self.stored_token(originator, &query, &cache_key).await? == StoredToken::Live {
            return Ok(true);
        }

        // 5) Request permission flow
        self.request_permission_flow(PermissionRequest {
            privileged: Some(priv_flag),
            certificate: Some(cert_details),
            reason: reason.map(|s| s.to_string()),
            ..PermissionRequest::bare(PermissionType::Certificate, originator)
        })
        .await
    }

    /// Ensures the originator has spending authorization (DSAP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Config flag bypass
    /// 3. Cache check
    /// 4. Permission request flow
    pub async fn ensure_spending_permission(
        &self,
        originator: &str,
        satoshis: u64,
        reason: Option<&str>,
    ) -> Result<bool> {
        // 1) Admin bypass
        if self.is_admin(originator) {
            return Ok(true);
        }

        // 2) Config flag bypass
        if !self.config.seek_spending_permissions {
            return Ok(true);
        }

        // 3) Cache check
        let cache_key = Self::build_cache_key(
            PermissionType::Spending,
            originator,
            None,
            None,
            None,
            None,
            None,
            Some(satoshis),
        );
        if self.is_permission_cached(&cache_key).await {
            return Ok(true);
        }

        // 4) Request permission flow
        self.request_permission_flow(PermissionRequest {
            spending: Some(SpendingPermissionDetails {
                satoshis,
                line_items: None,
            }),
            reason: reason.map(|s| s.to_string()),
            ..PermissionRequest::bare(PermissionType::Spending, originator)
        })
        .await
    }

    // =========================================================================
    // Operation-Level Permission Checking (legacy stub compatibility)
    // =========================================================================

    /// Check if an operation is permitted based on the permission request.
    ///
    /// If `enforce_permissions` is false in the config, all operations are allowed.
    /// If the originator is the admin, the operation is always allowed.
    /// Otherwise, the operation is checked against the per-operation flags in the config.
    pub fn check_permission(&self, request: &PermissionRequest) -> bool {
        // Admin is always allowed
        if self.is_admin(&request.originator) {
            return true;
        }
        // If no restrictions configured, allow all
        if !self.config.enforce_permissions {
            return true;
        }
        // Check against configured operation permissions
        let operation = match &request.operation {
            Some(op) => op.as_str(),
            None => return true,
        };
        match operation {
            "createAction" => self.config.allow_create_action,
            "signAction" => self.config.allow_sign_action,
            "abortAction" => self.config.allow_abort_action,
            "listActions" => self.config.allow_list_actions,
            "internalizeAction" => self.config.allow_internalize_action,
            "listOutputs" => self.config.allow_list_outputs,
            "relinquishOutput" => self.config.allow_relinquish_output,
            "acquireCertificate" => self.config.allow_acquire_certificate,
            "listCertificates" => self.config.allow_list_certificates,
            "proveCertificate" => self.config.allow_prove_certificate,
            "relinquishCertificate" => self.config.allow_relinquish_certificate,
            "discoverByIdentityKey" | "discoverByAttributes" => self.config.allow_discover,
            "getPublicKey" => self.config.allow_crypto,
            "encrypt" | "decrypt" => self.config.allow_crypto,
            "createHmac" | "verifyHmac" => self.config.allow_crypto,
            "createSignature" | "verifySignature" => self.config.allow_crypto,
            _ => true,
        }
    }

    /// Verifies that a permission token is valid.
    pub fn verify_token(&self, token: &PermissionToken) -> Result<()> {
        if token.txid.is_empty() {
            return Err(Error::AccessDenied(
                "Permission token has empty txid".to_string(),
            ));
        }
        if token.output_script.is_empty() {
            return Err(Error::AccessDenied(
                "Permission token has empty output script".to_string(),
            ));
        }
        if token.expiry > 0 {
            let now = Utc::now().timestamp() as u64;
            if token.expiry < now {
                return Err(Error::AccessDenied(format!(
                    "Permission token expired at {} (current time: {})",
                    token.expiry, now
                )));
            }
        }
        Ok(())
    }

    /// Checks whether a permission token grants the requested permission.
    pub fn check_permission_with_token(
        &self,
        request: &PermissionRequest,
        token: &PermissionToken,
    ) -> bool {
        if self.is_admin(&request.originator) {
            return true;
        }
        if self.verify_token(token).is_err() {
            return false;
        }
        if token.originator != request.originator {
            return false;
        }
        match request.request_type {
            PermissionType::Protocol => {
                if let Some(ref req_protocol) = request.protocol_id {
                    match &token.protocol {
                        Some(token_protocol) => {
                            if token_protocol != &req_protocol.protocol_name {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            PermissionType::Basket => {
                if let Some(ref req_basket) = request.basket {
                    match &token.basket_name {
                        Some(token_basket) => {
                            if token_basket != req_basket {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            PermissionType::Certificate => {
                if let Some(ref req_cert) = request.certificate {
                    match &token.cert_type {
                        Some(token_cert_type) => {
                            if token_cert_type != &req_cert.cert_type {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            PermissionType::Spending => {
                if let Some(ref req_spending) = request.spending {
                    match token.authorized_amount {
                        Some(authorized) => {
                            if authorized < req_spending.satoshis {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
        }
    }

    /// Caches a permission token for later use (legacy API).
    pub async fn cache_token(&self, key: String, token: PermissionToken) {
        self.cache_permission(key, token.expiry).await;
    }

    /// Retrieves a cached permission token by key (legacy API).
    /// Note: the new cache stores only expiry info, not full tokens.
    /// This method always returns None in the new implementation.
    pub async fn get_cached_token(&self, _key: &str) -> Option<PermissionToken> {
        None
    }

    /// Removes expired tokens from the cache.
    pub async fn purge_expired_tokens(&self) {
        let now = Utc::now().timestamp() as u64;
        let mut cache = self.permission_cache.write().await;
        cache.retain(|_, entry| {
            if entry.cached_at.elapsed().as_secs() > CACHE_TTL_SECS {
                return false;
            }
            if entry.expiry > 0 && entry.expiry < now {
                return false;
            }
            true
        });
    }
}

// =============================================================================
// Stored Permission Tokens (#751 step 1)
// =============================================================================
//
// The read-only lookup of the protocol, basket and certificate tokens a
// wallet already holds in the reference's admin baskets (ts-stack@edf6e03
// `WalletPermissionsManager.ts:3080-3239`). Nothing here mints, renews or
// revokes; the spending token stays out (no monthly tally, the #751 note).

/// The tokens' field protocol, keyID `"1"` (`:2711-2714`, `:2726-2737`).
const PERM_TOKEN_ENCRYPTION_PROTOCOL: &str = "admin permission token encryption";
/// The admin baskets of the three kinds read here (`BASKET_MAP`, `:418-423`).
const PROTOCOL_TOKEN_BASKET: &str = "admin protocol-permission";
const BASKET_TOKEN_BASKET: &str = "admin basket-access";
const CERTIFICATE_TOKEN_BASKET: &str = "admin certificate-access";
/// The decoder's bounds (`:52-54`).
const MAX_PERMISSION_TOKEN_FIELD_BYTES: usize = 1024 * 1024;
const MAX_PERMISSION_TOKEN_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MAX_PERMISSION_TOKEN_LIST_FIELDS: usize = 256;
/// JavaScript's `Number.MAX_SAFE_INTEGER`, the integer bound at `:57-64`.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
/// The lookup's page (`limit: 10000`, `:3098`).
const TOKEN_LOOKUP_LIMIT: u32 = 10_000;

fn permission_token_protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, PERM_TOKEN_ENCRYPTION_PROTOCOL)
}

/// The reference's `isTokenExpired` (`:2900-2903`): 0 never expires.
fn is_token_expired(expiry: u64) -> bool {
    expiry > 0 && expiry < Utc::now().timestamp().unsigned_abs()
}

/// What a stored token must say to answer a request.
pub(crate) enum TokenQuery<'a> {
    /// A DPACP token; `counterparty` is read at level 2 alone.
    Protocol {
        privileged: bool,
        protocol: &'a Protocol,
        counterparty: &'a str,
    },
    /// A DBAP token.
    Basket { basket: &'a str },
    /// A DCAP token holding every one of `fields`.
    Certificate {
        privileged: bool,
        verifier: &'a str,
        cert_type: &'a str,
        fields: &'a [String],
    },
}

impl TokenQuery<'_> {
    fn basket(&self) -> &'static str {
        match self {
            Self::Protocol { .. } => PROTOCOL_TOKEN_BASKET,
            Self::Basket { .. } => BASKET_TOKEN_BASKET,
            Self::Certificate { .. } => CERTIFICATE_TOKEN_BASKET,
        }
    }

    /// The data fields ahead of the signature (`buildPushdropFields`,
    /// `:3708-3749`).
    fn field_count(&self) -> usize {
        match self {
            Self::Basket { .. } => 3,
            Self::Protocol { .. } | Self::Certificate { .. } => 6,
        }
    }

    /// The lookup's tags, `originator <o>` omitted for the untagged pass
    /// (`protocolTokenTags` `:2941-2956`; `:3171-3175`; `:3209-3214`), each
    /// trimmed and lowercased as the reference's `listOutputs` and its mint's
    /// `createAction` pass every tag through `validateTag` (sdk
    /// `validationHelpers.ts:395`, `:411`, `:639`, `:1637`); the port's store
    /// compares tags as given.
    fn tags(&self, origin: Option<&str>) -> Vec<String> {
        self.request_tags(origin)
            .iter()
            .map(|tag| tag.trim().to_lowercase())
            .collect()
    }

    /// The tags in the request's case, as `buildTagsForRequest` writes them.
    fn request_tags(&self, origin: Option<&str>) -> Vec<String> {
        let mut tags: Vec<String> = origin
            .map(|o| format!("originator {o}"))
            .into_iter()
            .collect();
        match self {
            Self::Protocol {
                privileged,
                protocol,
                counterparty,
            } => {
                let level = protocol.security_level.as_u8();
                tags.push(format!("privileged {privileged}"));
                tags.push(format!("protocolName {}", protocol.protocol_name));
                tags.push(format!("protocolSecurityLevel {level}"));
                if level == 2 {
                    tags.push(format!("counterparty {counterparty}"));
                }
            }
            Self::Basket { basket } => tags.push(format!("basket {basket}")),
            Self::Certificate {
                privileged,
                verifier,
                cert_type,
                ..
            } => {
                tags.push(format!("privileged {privileged}"));
                tags.push(format!("type {cert_type}"));
                tags.push(format!("verifier {verifier}"));
            }
        }
        tags
    }

    /// The token's expiry when its decrypted fields after the domain answer
    /// the request (`:2970-2975`, `:3008-3009`, `:3043-3047`); `None` for a
    /// token that does not answer or does not parse.
    fn expiry_if_matches(&self, texts: &[String]) -> Option<u64> {
        let expiry = canonical_integer(&texts[1], MAX_SAFE_INTEGER)?;
        let matches = match self {
            Self::Protocol {
                privileged,
                protocol,
                counterparty,
            } => {
                let level = canonical_integer(&texts[3], 2)?;
                permission_boolean(&texts[2])? == *privileged
                    && level == u64::from(protocol.security_level.as_u8())
                    && texts[4] == protocol.protocol_name
                    && (level != 2 || texts[5] == *counterparty)
            }
            Self::Basket { basket } => texts[2] == *basket,
            Self::Certificate {
                privileged,
                verifier,
                cert_type,
                fields,
            } => {
                let held = certificate_fields(&texts[4])?;
                permission_boolean(&texts[2])? == *privileged
                    && texts[3] == *cert_type
                    && texts[5] == *verifier
                    && fields.iter().all(|field| held.contains(field))
            }
        };
        matches.then_some(expiry)
    }
}

/// `parseCanonicalPermissionInteger` (`:57-64`).
fn canonical_integer(text: &str, maximum: u64) -> Option<u64> {
    let canonical = text == "0"
        || (!text.starts_with('0') && !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()));
    let value: u64 = if canonical {
        text.parse().ok()?
    } else {
        return None;
    };
    (value <= maximum).then_some(value)
}

/// `parsePermissionBoolean` (`:66-69`).
fn permission_boolean(text: &str) -> Option<bool> {
    match text {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// `parsePermissionCertificateFields` (`:71-90`): a bounded JSON array of
/// distinct non-empty names under 50 bytes.
fn certificate_fields(text: &str) -> Option<Vec<String>> {
    let fields: Vec<String> = serde_json::from_str(text).ok()?;
    let mut seen = std::collections::HashSet::new();
    let valid = fields.len() <= MAX_PERMISSION_TOKEN_LIST_FIELDS
        && fields
            .iter()
            .all(|field| !field.is_empty() && field.len() < 50 && seen.insert(field.as_str()));
    valid.then_some(fields)
}

/// `normalizeOriginator` (`:5707-5729`): the lowercase hostname, a scheme,
/// credentials, path and port dropped. The URL parser's IDN mapping and
/// percent decoding are not ported.
fn normalize_originator(originator: &str) -> String {
    let trimmed = originator.trim();
    let rest = match trimmed.split_once("://") {
        Some((scheme, rest))
            if scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c)) =>
        {
            rest
        }
        _ => trimmed,
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = match host_port.find(']') {
        Some(end) if host_port.starts_with('[') => &host_port[..=end],
        _ => match host_port.rsplit_once(':') {
            Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) => host,
            _ => host_port,
        },
    };
    if host.is_empty() {
        trimmed.to_lowercase()
    } else {
        host.to_lowercase()
    }
}

/// `prepareOriginator`'s lookup values (`:5788-5794`): the trimmed form,
/// the normalized form and the untagged pass, each once. The reference's
/// refusals of a scheme, a path or an IP originator (`:5759-5785`) are not
/// ported here.
fn originator_lookup_values(originator: &str) -> (String, Vec<Option<String>>) {
    let trimmed = originator.trim();
    let normalized = normalize_originator(trimmed);
    let mut values = vec![Some(trimmed.to_string())];
    if normalized != trimmed {
        values.push(Some(normalized.clone()));
    }
    values.push(None);
    (normalized, values)
}

/// `authenticatedPermissionTokenSource`'s source (`:2798-2816`): the
/// output's transaction from the BEEF under its txid, one satoshi, and a
/// canonical lock-before `PushDrop` of `count` data fields and a signature,
/// within the decoder's bounds.
fn token_source(beef: &Beef, output: &WalletOutput, count: usize) -> Option<PushDrop> {
    let txid = bsv_rs::primitives::to_hex(&output.outpoint.txid);
    let tx = beef.find_atomic_transaction(&txid)?;
    let tx_output = tx
        .outputs
        .get(usize::try_from(output.outpoint.vout).ok()?)?;
    if tx.id() != txid || tx_output.satoshis != Some(1) || output.satoshis != 1 {
        return None;
    }
    let script = tx_output.locking_script.to_binary();
    let decoded = PushDrop::decode(&tx_output.locking_script).ok()?;
    let relocked = PushDrop::new(decoded.locking_public_key.clone(), decoded.fields.clone());
    let payload: usize = decoded.fields.iter().map(Vec::len).sum();
    let valid = relocked.lock().to_binary() == script
        && decoded.fields.len() == count + 1
        && payload <= MAX_PERMISSION_TOKEN_PAYLOAD_BYTES
        && decoded
            .fields
            .iter()
            .all(|field| field.len() <= MAX_PERMISSION_TOKEN_FIELD_BYTES);
    valid.then_some(decoded)
}

/// The tail of `ProtoWallet`'s refusal of a call that needs outputs, after
/// the call's name (bsv-rs 0.9.0-beta.6 `proto_wallet/interface.rs:154-156`,
/// `:210-212`): the underlying keeps no outputs.
const PROTO_WALLET_REFUSAL_TAIL: &str =
    " requires a full wallet implementation with UTXO management";

/// Whether a failure of `call` is `ProtoWallet`'s refusal of it: an
/// underlying that keeps no outputs, so holds no token, rather than a fault.
fn proto_wallet_refuses(error: &bsv_rs::Error, call: &str) -> bool {
    matches!(error, bsv_rs::Error::WalletError(message)
        if message.strip_suffix(PROTO_WALLET_REFUSAL_TAIL) == Some(call))
}

/// Whether a list failure is an underlying that cannot list.
fn cannot_list(error: &bsv_rs::Error) -> bool {
    proto_wallet_refuses(error, "listOutputs")
}

/// A stored token's standing for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoredToken {
    /// No token answers the request.
    Absent,
    /// A token answers it and has expired.
    Expired,
    /// A token answers it; the grant is cached as a prompt's would be.
    Live,
}

impl StoredToken {
    /// The protocol door's seek-false refusal: `:1549` for an expired
    /// token, `:1533` otherwise.
    fn seek_false_refusal(self) -> &'static str {
        match self {
            Self::Expired => "Protocol permission expired and no further user consent allowed (seekPermission=false).",
            Self::Absent | Self::Live => "No protocol permission token found (seekPermission=false).",
        }
    }
}

impl WalletPermissionsManager {
    /// The stored token for `query` read before the seek-false refusal and
    /// the prompt (`:1522-1566`, `:1595-1615`, `:1669-1709`): a live token
    /// is cached under `cache_key` with its expiry (`:1565`); a list fault
    /// is the wallet's own fault, never a verdict.
    pub(crate) async fn stored_token(
        &self,
        originator: &str,
        query: &TokenQuery<'_>,
        cache_key: &str,
    ) -> Result<StoredToken> {
        Ok(match self.find_stored_token(originator, query).await? {
            None => StoredToken::Absent,
            Some(expiry) if is_token_expired(expiry) => StoredToken::Expired,
            Some(expiry) => {
                self.cache_permission(cache_key.to_string(), expiry).await;
                StoredToken::Live
            }
        })
    }

    /// `findProtocolToken`, `findBasketToken` and `findCertificateToken`
    /// with `includeExpired` true (`:3080-3117`, `:3163-3194`,
    /// `:3196-3239`): per lookup value, `listOutputs` on the kind's admin
    /// basket with the tags, every tag required, entire transactions, as
    /// the admin originator; the matched token's expiry. An underlying that
    /// cannot list (a `ProtoWallet`) holds no token, and the request's path
    /// stays as it was before the lookup; every other list fault is returned
    /// as it came (the reference throws, `:3092`, `:3172`, `:3208`): the wallet could not look.
    /// A field that does not decrypt is read raw, as the reference's
    /// `decryptPermissionTokenField` does (`:2847-2850`).
    pub(crate) async fn find_stored_token(
        &self,
        originator: &str,
        query: &TokenQuery<'_>,
    ) -> Result<Option<u64>> {
        let (normalized, lookup_values) = originator_lookup_values(originator);
        for origin in lookup_values {
            let args = ListOutputsArgs {
                basket: query.basket().to_string(),
                tags: Some(query.tags(origin.as_deref())),
                tag_query_mode: Some(QueryMode::All),
                include: Some(OutputInclude::EntireTransactions),
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: Some(TOKEN_LOOKUP_LIMIT),
                offset: None,
                seek_permission: None,
            };
            let listed = self
                .underlying
                .list_outputs(args, Some(&self.admin_originator))
                .await;
            let result = match listed {
                Ok(result) => result,
                Err(error) if cannot_list(&error) => return Ok(None),
                Err(error) => return Err(Error::SdkError(error)),
            };
            // One parse per result, as `transactionFromResultBeef` (`:2747`).
            let count = query.field_count();
            let sources: Vec<PushDrop> = match result.beef.as_deref().map(Beef::from_binary) {
                Some(Ok(beef)) => result
                    .outputs
                    .iter()
                    .filter_map(|output| token_source(&beef, output, count))
                    .collect(),
                _ => continue,
            };
            for source in sources {
                let Some(fields) = self.signed_token_fields(source, count).await else {
                    continue;
                };
                if let Some(expiry) = self.token_expiry(&fields, &normalized, query).await {
                    return Ok(Some(expiry));
                }
            }
        }
        Ok(None)
    }

    /// The decrypted fields' answer (`parseProtocolTokenOutput` `:2951`,
    /// `parseBasketTokenOutput`, `parseCertificateTokenOutput`): the domain
    /// normalized against the request's originator, then the kind's match.
    async fn token_expiry(
        &self,
        fields: &[Vec<u8>],
        normalized: &str,
        query: &TokenQuery<'_>,
    ) -> Option<u64> {
        let domain = self.decrypt_token_text(&fields[0]).await?;
        if normalize_originator(&domain) != normalized {
            return None;
        }
        let mut texts = vec![domain];
        for field in &fields[1..] {
            texts.push(self.decrypt_token_text(field).await?);
        }
        query.expiry_if_matches(&texts)
    }

    /// The rest of `authenticatedPermissionTokenSource` (`:2817-2823`):
    /// the lock under this wallet's token key and the signature under it
    /// over the data fields; the data fields, else `None`.
    async fn signed_token_fields(&self, source: PushDrop, count: usize) -> Option<Vec<Vec<u8>>> {
        let key = source.locking_public_key.to_hex().to_lowercase();
        if self.token_locking_key().await? != key {
            return None;
        }
        let (fields, signature) = source.fields.split_at(count);
        let signature = Signature::from_der(&signature[0]).ok()?;
        let digest = sha256(&fields.concat());
        let signed = source.locking_public_key.verify(&digest, &signature);
        signed.then(|| fields.to_vec())
    }

    /// `permissionTokenLockingKey` (`:2770-2796`): the token protocol's key
    /// for `self`, `forSelf`, read once; a failed read is read again.
    async fn token_locking_key(&self) -> Option<&str> {
        let read = || async {
            let args = GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(permission_token_protocol()),
                key_id: Some("1".to_string()),
                counterparty: Some(Counterparty::Self_),
                for_self: Some(true),
                ..Default::default()
            };
            let key = self
                .underlying
                .get_public_key(args, Some(&self.admin_originator))
                .await?
                .public_key;
            let valid = key.len() == 66
                && (key.starts_with("02") || key.starts_with("03"))
                && key.bytes().all(|b| b.is_ascii_hexdigit());
            if valid {
                Ok(key.to_lowercase())
            } else {
                Err(Error::AccessDenied(
                    "Wallet returned an invalid permission-token locking key.".to_string(),
                ))
            }
        };
        let key = self
            .permission_token_locking_key
            .get_or_try_init(read)
            .await;
        key.ok().map(String::as_str)
    }

    /// `decryptPermissionTokenText` (`:2830-2854`): the field under the
    /// token protocol as the admin originator, the raw bytes where it does
    /// not decrypt; `None` for text that is not UTF-8 (`toUTF8Strict`).
    async fn decrypt_token_text(&self, ciphertext: &[u8]) -> Option<String> {
        let args = DecryptArgs {
            ciphertext: ciphertext.to_vec(),
            protocol_id: permission_token_protocol(),
            key_id: "1".to_string(),
            counterparty: None,
            ..Default::default()
        };
        let plaintext = match self
            .underlying
            .decrypt(args, Some(&self.admin_originator))
            .await
        {
            Ok(result) => result.plaintext,
            Err(_) => ciphertext.to_vec(),
        };
        String::from_utf8(plaintext).ok()
    }
}

// =============================================================================
// Permission-module delegation on the key calls (#726)
// =============================================================================

/// The future a delegated call's underlying wallet call answers with: the
/// `std` spelling, crate-owned, so no outside crate's future type is named.
pub(crate) type ModuleFuture<'a, T> = Pin<Box<dyn Future<Output = bsv_rs::Result<T>> + Send + 'a>>;

/// The built-in BRC-177 module's refusal of every method but `createAction`
/// and `listActions` (ts-stack@edf6e03 `WalletPermissionsManager.ts:799-801`).
pub(crate) const NOSEND_KEY_CALL: &str =
    "BRC-177 noSend expiry labels are only valid for createAction and listActions";

/// A P-module refusal at the boundary: the text alone, the class
/// `InvalidParameter` (the keeper's reading of #726).
fn module_refusal(message: impl Into<String>) -> bsv_rs::Error {
    bsv_rs::Error::WalletRefusal {
        class: bsv_rs::WalletRefusalClass::InvalidParameter,
        message: message.into(),
    }
}

/// The scheme of a `p <scheme> <rest...>` name as passed, or `None` for any
/// other name (`WalletPermissionsManager.ts:851-855`: the case-sensitive
/// `'p '` test, the scheme `split(' ')[1]`).
pub(crate) fn module_scheme(name: &str) -> Option<&str> {
    name.strip_prefix("p ")?.split(' ').next()
}

impl WalletPermissionsManager {
    /// The reference's `delegateToPModuleIfNeeded` (`:843-885`) for a key
    /// call: `None` when `name` is not a `p ` name, so the caller runs its
    /// ordinary check; else the module's answer. `nosend` is the reference's
    /// built-in module, so a host's entry under it is never read (`:792-801`).
    /// The module's `on_request` answers the arguments the underlying call
    /// takes, and its `on_response` the result the caller receives.
    pub(crate) async fn delegate_key_call<'a, A: ModuleCall>(
        &self,
        name: &str,
        args: &A,
        originator: &str,
        underlying: impl FnOnce(A) -> ModuleFuture<'a, A::Output> + Send,
    ) -> Option<bsv_rs::Result<A::Output>> {
        let scheme = module_scheme(name)?;
        if scheme == "nosend" {
            return Some(Err(module_refusal(NOSEND_KEY_CALL)));
        }
        let Some(module) = self.config.permission_modules.get(scheme) else {
            return Some(Err(module_refusal(format!(
                "Unsupported P-module scheme: p {scheme}"
            ))));
        };
        Some(run_module(module.as_ref(), args, originator, underlying).await)
    }
}

/// `onRequest`, the underlying call on its arguments, `onResponse`
/// (`:874-884`); a module's own refusal crosses with its text.
async fn run_module<'a, A: ModuleCall>(
    module: &dyn PermissionsModule,
    args: &A,
    originator: &str,
    underlying: impl FnOnce(A) -> ModuleFuture<'a, A::Output>,
) -> bsv_rs::Result<A::Output> {
    let request = serde_json::Value::Object(args.to_json());
    let transformed = module
        .on_request(A::METHOD, request, originator)
        .await
        .map_err(module_refusal)?;
    let transformed = A::from_json(&transformed).map_err(module_refusal)?;
    let result = underlying(transformed).await?;
    let result = module
        .on_response(A::output_json(&result), A::METHOD, originator)
        .await
        .map_err(module_refusal)?;
    A::output_from_json(&result).map_err(module_refusal)
}

/// The modules a basket and label call chains, one per scheme in the order
/// met (the reference's `Map`, baskets then labels).
pub(crate) type ModuleChain = Vec<(String, Arc<dyn PermissionsModule>)>;

/// `splitLabelsByPermissionModule`'s grammar (`:920-958`): the scheme of a
/// `p <scheme> <payload>` label, `None` for a label not starting `p `, or
/// `Invalid P-label format: <label>`.
fn label_scheme(label: &str) -> std::result::Result<Option<&str>, String> {
    let Some(remainder) = label.strip_prefix("p ") else {
        return Ok(None);
    };
    let invalid = || format!("Invalid P-label format: {label}");
    let separator = remainder
        .find(' ')
        .filter(|&at| at > 0)
        .ok_or_else(invalid)?;
    let payload = &remainder[separator + 1..];
    if payload.is_empty() || payload.starts_with(' ') {
        return Err(invalid());
    }
    Ok(Some(&remainder[..separator]))
}

/// Whether the module chain reads `label` as `p nosend <payload>`, the
/// reserved scheme it refuses.
pub(crate) fn is_nosend_label(label: &str) -> bool {
    matches!(label_scheme(label), Ok(Some("nosend")))
}

impl WalletPermissionsManager {
    /// The modules `baskets` and `labels` name (`collectNonPBaskets`
    /// `:4436-4451`, `collectInternalizeActionBaskets` `:4970-4989`,
    /// `splitLabelsByPermissionModule` `:920-958`, `addPModuleByScheme`
    /// `:890-901`): a basket's scheme its second word, a label's by the
    /// grammar; an unregistered scheme refused with `Unsupported
    /// P-basket scheme: p <scheme>` or `Unsupported P-label scheme: p
    /// <scheme>`. A name not starting `p ` takes the ordinary check.
    ///
    /// The reserved scheme `nosend`, a basket's or a label's, is refused on
    /// every chained call with [`NOSEND_KEY_CALL`] alone, class
    /// `InvalidParameter` (the keeper's 2026-11-04 reading), and a host's
    /// entry under it is never read. The reference's built-in module
    /// (`:792-826`) parses the BRC-177 labels and checks label access on
    /// `listActions` (`:796-821`); the port does not run it (it checks no
    /// labels on `list_actions` at all, #786), so it refuses the name until
    /// the module's own row lands.
    pub(crate) fn module_chain<'n>(
        &self,
        baskets: impl IntoIterator<Item = &'n str>,
        labels: impl IntoIterator<Item = &'n str>,
    ) -> bsv_rs::Result<ModuleChain> {
        let mut chain = ModuleChain::new();
        let mut add = |scheme: &str, kind: &str| -> bsv_rs::Result<()> {
            if chain.iter().any(|(held, _)| held == scheme) {
                return Ok(());
            }
            if scheme == "nosend" {
                return Err(module_refusal(NOSEND_KEY_CALL));
            }
            let Some(module) = self.config.permission_modules.get(scheme) else {
                return Err(module_refusal(format!(
                    "Unsupported P-{kind} scheme: p {scheme}"
                )));
            };
            chain.push((scheme.to_string(), Arc::clone(module)));
            Ok(())
        };
        for basket in baskets {
            if let Some(scheme) = module_scheme(basket) {
                add(scheme, "basket")?;
            }
        }
        for label in labels {
            if let Some(scheme) = label_scheme(label).map_err(module_refusal)? {
                add(scheme, "label")?;
            }
        }
        Ok(chain)
    }
}

/// `onRequest` through every module of the chain in order (`:4507-4514`,
/// `:4932-4941`, `:5038-5045`), each given the previous one's arguments;
/// the arguments unchanged with no module.
pub(crate) async fn chain_request<A: ModuleCall>(
    chain: &ModuleChain,
    args: A,
    originator: &str,
) -> bsv_rs::Result<A> {
    if chain.is_empty() {
        return Ok(args);
    }
    let mut value = serde_json::Value::Object(args.to_json());
    for (_, module) in chain {
        value = module
            .on_request(A::METHOD, value, originator)
            .await
            .map_err(module_refusal)?;
    }
    A::from_json(&value).map_err(module_refusal)
}

/// `onResponse` through the chain in reverse (`:4518-4520`, `:4949-4954`,
/// `:5048-5053`); the result unchanged with no module.
pub(crate) async fn chain_response<A: ModuleCall>(
    chain: &ModuleChain,
    output: A::Output,
    originator: &str,
) -> bsv_rs::Result<A::Output> {
    if chain.is_empty() {
        return Ok(output);
    }
    let mut value = A::output_json(&output);
    for (_, module) in chain.iter().rev() {
        value = module
            .on_response(value, A::METHOD, originator)
            .await
            .map_err(module_refusal)?;
    }
    A::output_from_json(&value).map_err(module_refusal)
}

/// A key call's request and result in the reference's JSON shape
/// (`Wallet.interfaces.ts`: `protocolID` as `[level, name]`, `keyID`,
/// `counterparty` as `self`, `anyone` or hex, byte fields as number
/// arrays, an absent option omitted), the one place the port maps them.
pub(crate) trait ModuleCall: Sized + Send + Sync {
    /// The BRC-100 method name the module is told.
    const METHOD: &'static str;
    /// The call's result.
    type Output: Send;
    /// The request's arguments as the module reads them.
    fn to_json(&self) -> serde_json::Map<String, serde_json::Value>;
    /// The module's arguments back, or why they do not parse.
    fn from_json(value: &serde_json::Value) -> std::result::Result<Self, String>;
    /// The result as the module reads it.
    fn output_json(output: &Self::Output) -> serde_json::Value;
    /// The module's result back, or why it does not parse.
    fn output_from_json(value: &serde_json::Value) -> std::result::Result<Self::Output, String>;
}

/// One field of a key call in the reference's JSON: `None` omits it.
trait ModuleField: Sized {
    fn to_field(&self) -> Option<serde_json::Value>;
    /// `None` is the key absent or `null`.
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()>;
}

impl<T: ModuleField> ModuleField for Option<T> {
    fn to_field(&self) -> Option<serde_json::Value> {
        self.as_ref().and_then(ModuleField::to_field)
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        match value {
            None | Some(serde_json::Value::Null) => Ok(None),
            some => T::from_field(some).map(Some),
        }
    }
}

impl ModuleField for bool {
    fn to_field(&self) -> Option<serde_json::Value> {
        Some(serde_json::Value::Bool(*self))
    }
    /// Absent is false, as the reference's optional flags read.
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        match value {
            None | Some(serde_json::Value::Null) => Ok(false),
            Some(value) => value.as_bool().ok_or(()),
        }
    }
}

impl ModuleField for String {
    fn to_field(&self) -> Option<serde_json::Value> {
        Some(serde_json::Value::String(self.clone()))
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        value.and_then(|v| v.as_str()).map(str::to_string).ok_or(())
    }
}

impl ModuleField for Vec<u8> {
    fn to_field(&self) -> Option<serde_json::Value> {
        Some(serde_json::Value::from(self.as_slice()))
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        let items = value.and_then(|v| v.as_array()).ok_or(())?;
        items
            .iter()
            .map(|item| item.as_u64().and_then(|n| u8::try_from(n).ok()).ok_or(()))
            .collect()
    }
}

impl ModuleField for [u8; 32] {
    fn to_field(&self) -> Option<serde_json::Value> {
        Some(serde_json::Value::from(self.as_slice()))
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        Vec::<u8>::from_field(value)?.try_into().map_err(|_| ())
    }
}

impl ModuleField for Protocol {
    fn to_field(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!([
            self.security_level.as_u8(),
            self.protocol_name
        ]))
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        match value.and_then(|v| v.as_array()).map(Vec::as_slice) {
            Some([level, name]) => {
                let level = level
                    .as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or(())?;
                let level = SecurityLevel::from_u8(level).ok_or(())?;
                Ok(Protocol::new(level, name.as_str().ok_or(())?))
            }
            _ => Err(()),
        }
    }
}

impl ModuleField for Counterparty {
    fn to_field(&self) -> Option<serde_json::Value> {
        let word = match self {
            Counterparty::Self_ => "self".to_string(),
            Counterparty::Anyone => "anyone".to_string(),
            Counterparty::Other(key) => key.to_hex(),
        };
        Some(serde_json::Value::String(word))
    }
    fn from_field(value: Option<&serde_json::Value>) -> std::result::Result<Self, ()> {
        match value.and_then(|v| v.as_str()).ok_or(())? {
            "self" => Ok(Counterparty::Self_),
            "anyone" => Ok(Counterparty::Anyone),
            hex => bsv_rs::primitives::PublicKey::from_hex(hex)
                .map(Counterparty::Other)
                .map_err(|_| ()),
        }
    }
}

/// The module's object as a map, or the refusal naming what it answered.
fn module_object<'v>(
    value: &'v serde_json::Value,
    method: &str,
    what: &str,
) -> std::result::Result<&'v serde_json::Map<String, serde_json::Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("P-module {what} for {method} must be an object"))
}

/// One field read back from the module's object, the refusal naming it.
fn module_field<T: ModuleField>(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    method: &str,
) -> std::result::Result<T, String> {
    T::from_field(object.get(key)).map_err(|()| format!("P-module {key} for {method} is invalid"))
}

// One request field's JSON; `when_true` sends a flag the reference types as
// `true` or absent (getPublicKey's `identityKey`) only when it is set.
macro_rules! module_to_field {
    ($value:expr) => {
        $value.to_field()
    };
    ($value:expr, when_true) => {
        $value.then_some(serde_json::Value::Bool(true))
    };
}

// Each key call's mapping: its method name, its fields (the Rust field and
// the reference's key) and its result's; the three permission fields every
// key call ends with are appended.
macro_rules! module_call {
    ($args:ident => $output:ident, $method:literal,
        { $($field:ident: $key:literal $($only:ident)?),* $(,)? },
        { $($out:ident: $out_key:literal),* $(,)? }) => {
        impl ModuleCall for bsv_rs::wallet::$args {
            const METHOD: &'static str = $method;
            type Output = bsv_rs::wallet::$output;
            fn to_json(&self) -> serde_json::Map<String, serde_json::Value> {
                let mut object = serde_json::Map::new();
                let fields = [
                    $(($key, module_to_field!(self.$field $(, $only)?)),)*
                    ("privileged", self.privileged.to_field()),
                    ("privilegedReason", self.privileged_reason.to_field()),
                    ("seekPermission", self.seek_permission.to_field()),
                ];
                for (key, value) in fields {
                    if let Some(value) = value {
                        object.insert(key.to_string(), value);
                    }
                }
                object
            }
            fn from_json(value: &serde_json::Value) -> std::result::Result<Self, String> {
                let object = module_object(value, $method, "arguments")?;
                Ok(Self {
                    $($field: module_field(object, $key, $method)?,)*
                    privileged: module_field(object, "privileged", $method)?,
                    privileged_reason: module_field(object, "privilegedReason", $method)?,
                    seek_permission: module_field(object, "seekPermission", $method)?,
                })
            }
            fn output_json(output: &Self::Output) -> serde_json::Value {
                let mut object = serde_json::Map::new();
                $(if let Some(value) = output.$out.to_field() {
                    object.insert($out_key.to_string(), value);
                })*
                serde_json::Value::Object(object)
            }
            fn output_from_json(
                value: &serde_json::Value,
            ) -> std::result::Result<Self::Output, String> {
                let object = module_object(value, $method, "result")?;
                Ok(bsv_rs::wallet::$output {
                    $($out: module_field(object, $out_key, $method)?,)*
                })
            }
        }
    };
}

module_call!(GetPublicKeyArgs => GetPublicKeyResult, "getPublicKey", {
    identity_key: "identityKey" when_true, protocol_id: "protocolID", key_id: "keyID",
    counterparty: "counterparty", for_self: "forSelf",
}, { public_key: "publicKey" });
module_call!(EncryptArgs => EncryptResult, "encrypt", {
    plaintext: "plaintext", protocol_id: "protocolID", key_id: "keyID",
    counterparty: "counterparty",
}, { ciphertext: "ciphertext" });
module_call!(DecryptArgs => DecryptResult, "decrypt", {
    ciphertext: "ciphertext", protocol_id: "protocolID", key_id: "keyID",
    counterparty: "counterparty",
}, { plaintext: "plaintext" });
module_call!(CreateHmacArgs => CreateHmacResult, "createHmac", {
    data: "data", protocol_id: "protocolID", key_id: "keyID", counterparty: "counterparty",
}, { hmac: "hmac" });
module_call!(VerifyHmacArgs => VerifyHmacResult, "verifyHmac", {
    data: "data", hmac: "hmac", protocol_id: "protocolID", key_id: "keyID",
    counterparty: "counterparty",
}, { valid: "valid" });
module_call!(CreateSignatureArgs => CreateSignatureResult, "createSignature", {
    data: "data", hash_to_directly_sign: "hashToDirectlySign", protocol_id: "protocolID",
    key_id: "keyID", counterparty: "counterparty",
}, { signature: "signature" });
module_call!(VerifySignatureArgs => VerifySignatureResult, "verifySignature", {
    data: "data", hash_to_directly_verify: "hashToDirectlyVerify", signature: "signature",
    protocol_id: "protocolID", key_id: "keyID", counterparty: "counterparty",
    for_self: "forSelf",
}, { valid: "valid" });

// The basket and label calls' mappings (#757): bsv-rs's own camelCase serde
// is the reference's object, field for field, so each row is the method
// name and the two types.
macro_rules! module_call_serde {
    ($args:ident => $output:ident, $method:literal) => {
        impl ModuleCall for bsv_rs::wallet::$args {
            const METHOD: &'static str = $method;
            type Output = bsv_rs::wallet::$output;
            fn to_json(&self) -> serde_json::Map<String, serde_json::Value> {
                serde_object(self)
            }
            fn from_json(value: &serde_json::Value) -> std::result::Result<Self, String> {
                module_object(value, $method, "arguments")?;
                serde_json::from_value(value.clone())
                    .map_err(|_| format!("P-module arguments for {} are invalid", $method))
            }
            fn output_json(output: &Self::Output) -> serde_json::Value {
                serde_json::Value::Object(serde_object(output))
            }
            fn output_from_json(
                value: &serde_json::Value,
            ) -> std::result::Result<Self::Output, String> {
                module_object(value, $method, "result")?;
                serde_json::from_value(value.clone())
                    .map_err(|_| format!("P-module result for {} is invalid", $method))
            }
        }
    };
}

/// A wallet type's object; its derived serde never fails on these types,
/// and an empty object would fail the parse back.
fn serde_object<T: Serialize>(value: &T) -> serde_json::Map<String, serde_json::Value> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::Object(object)) => object,
        _ => serde_json::Map::new(),
    }
}

module_call_serde!(ListOutputsArgs => ListOutputsResult, "listOutputs");
module_call_serde!(RelinquishOutputArgs => RelinquishOutputResult, "relinquishOutput");
module_call_serde!(CreateActionArgs => CreateActionResult, "createAction");
module_call_serde!(ListActionsArgs => ListActionsResult, "listActions");
module_call_serde!(InternalizeActionArgs => InternalizeActionResult, "internalizeAction");

#[path = "permission_grant.rs"]
mod permission_grant;

// =============================================================================
// Tests
// =============================================================================

#[cfg(all(test, feature = "sqlite"))]
#[path = "permission_token_lookup_tests.rs"]
mod permission_token_lookup_tests;

#[cfg(all(test, feature = "sqlite"))]
#[path = "permission_grant_tests.rs"]
mod permission_grant_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::wallet::*;

    // =========================================================================
    // Mock Wallet for testing
    // =========================================================================

    struct MockWallet;

    #[async_trait]
    impl WalletInterface for MockWallet {
        async fn create_action(
            &self,
            _args: CreateActionArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<CreateActionResult> {
            unimplemented!()
        }
        async fn sign_action(
            &self,
            _args: SignActionArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<SignActionResult> {
            unimplemented!()
        }
        async fn abort_action(
            &self,
            _args: AbortActionArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<AbortActionResult> {
            unimplemented!()
        }
        async fn list_actions(
            &self,
            _args: ListActionsArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<ListActionsResult> {
            unimplemented!()
        }
        async fn internalize_action(
            &self,
            _args: InternalizeActionArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<InternalizeActionResult> {
            unimplemented!()
        }
        async fn list_outputs(
            &self,
            _args: ListOutputsArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<ListOutputsResult> {
            // The stored-token lookup's read (#751): a `ProtoWallet`'s
            // refusal, no token held.
            Err(bsv_rs::Error::WalletError(format!(
                "listOutputs{PROTO_WALLET_REFUSAL_TAIL}"
            )))
        }
        async fn relinquish_output(
            &self,
            _args: RelinquishOutputArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<RelinquishOutputResult> {
            unimplemented!()
        }
        async fn get_public_key(
            &self,
            _args: GetPublicKeyArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<GetPublicKeyResult> {
            unimplemented!()
        }
        async fn reveal_counterparty_key_linkage(
            &self,
            _args: interface::RevealCounterpartyKeyLinkageArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<RevealCounterpartyKeyLinkageResult> {
            unimplemented!()
        }
        async fn reveal_specific_key_linkage(
            &self,
            _args: interface::RevealSpecificKeyLinkageArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<RevealSpecificKeyLinkageResult> {
            unimplemented!()
        }
        async fn encrypt(
            &self,
            _args: EncryptArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<EncryptResult> {
            // A grant's mint begins with encrypt; this wallet keeps no outputs.
            Err(bsv_rs::Error::WalletError("no mint here".to_string()))
        }
        async fn decrypt(
            &self,
            _args: DecryptArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<DecryptResult> {
            unimplemented!()
        }
        async fn create_hmac(
            &self,
            _args: CreateHmacArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<CreateHmacResult> {
            unimplemented!()
        }
        async fn verify_hmac(
            &self,
            _args: VerifyHmacArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<VerifyHmacResult> {
            unimplemented!()
        }
        async fn create_signature(
            &self,
            _args: CreateSignatureArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<CreateSignatureResult> {
            unimplemented!()
        }
        async fn verify_signature(
            &self,
            _args: VerifySignatureArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<VerifySignatureResult> {
            unimplemented!()
        }
        async fn acquire_certificate(
            &self,
            _args: AcquireCertificateArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<WalletCertificate> {
            unimplemented!()
        }
        async fn list_certificates(
            &self,
            _args: ListCertificatesArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<ListCertificatesResult> {
            unimplemented!()
        }
        async fn prove_certificate(
            &self,
            _args: ProveCertificateArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<ProveCertificateResult> {
            unimplemented!()
        }
        async fn relinquish_certificate(
            &self,
            _args: RelinquishCertificateArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<RelinquishCertificateResult> {
            unimplemented!()
        }
        async fn discover_by_identity_key(
            &self,
            _args: DiscoverByIdentityKeyArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<DiscoverCertificatesResult> {
            unimplemented!()
        }
        async fn discover_by_attributes(
            &self,
            _args: DiscoverByAttributesArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<DiscoverCertificatesResult> {
            unimplemented!()
        }
        async fn is_authenticated(
            &self,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<AuthenticatedResult> {
            unimplemented!()
        }
        async fn wait_for_authentication(
            &self,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<AuthenticatedResult> {
            unimplemented!()
        }
        async fn get_height(&self, _originator: Option<&str>) -> bsv_rs::Result<GetHeightResult> {
            unimplemented!()
        }
        async fn get_header_for_height(
            &self,
            _args: GetHeaderArgs,
            _originator: Option<&str>,
        ) -> bsv_rs::Result<GetHeaderResult> {
            unimplemented!()
        }
        async fn get_network(&self, _originator: Option<&str>) -> bsv_rs::Result<GetNetworkResult> {
            unimplemented!()
        }
        async fn get_version(&self, _originator: Option<&str>) -> bsv_rs::Result<GetVersionResult> {
            unimplemented!()
        }
    }

    fn make_manager(config: WalletPermissionsManagerConfig) -> WalletPermissionsManager {
        let wallet: Arc<dyn WalletInterface + Send + Sync> = Arc::new(MockWallet);
        WalletPermissionsManager::new(wallet, "admin.wallet".to_string(), config)
    }

    fn make_token(originator: &str, expiry: u64) -> PermissionToken {
        PermissionToken {
            txid: "abc123".to_string(),
            tx: vec![0, 1, 2],
            output_index: 0,
            output_script: "76a914...88ac".to_string(),
            satoshis: 1000,
            originator: originator.to_string(),
            raw_originator: None,
            expiry,
            privileged: None,
            protocol: None,
            security_level: None,
            counterparty: None,
            basket_name: None,
            cert_type: None,
            cert_fields: None,
            verifier: None,
            authorized_amount: None,
        }
    }

    // =========================================================================
    // Original tests (preserved from stub)
    // =========================================================================

    #[test]
    fn test_config_all_enabled() {
        let config = WalletPermissionsManagerConfig::all_enabled();
        assert!(config.seek_spending_permissions);
        assert!(config.seek_basket_listing_permissions);
        assert!(config.differentiate_privileged_operations);
    }

    #[test]
    fn test_config_all_disabled() {
        let config = WalletPermissionsManagerConfig::all_disabled();
        assert!(!config.seek_spending_permissions);
        assert!(!config.seek_basket_listing_permissions);
    }

    #[test]
    fn test_permission_request_serialization() {
        let request = PermissionRequest {
            privileged: Some(false),
            protocol_id: Some(Protocol::new(
                SecurityLevel::Counterparty,
                "test protocol".to_string(),
            )),
            reason: Some("Testing".to_string()),
            operation: Some("createAction".to_string()),
            ..PermissionRequest::bare(PermissionType::Protocol, "app.example.com")
        };
        let json = serde_json::to_string(&request).unwrap();
        let parsed: PermissionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.request_type, PermissionType::Protocol);
        assert_eq!(parsed.originator, "app.example.com");
        assert_eq!(parsed.operation, Some("createAction".to_string()));
    }

    #[test]
    fn test_permission_request_backward_compat() {
        let json = r#"{"type":"protocol","originator":"app.example.com","reason":"test"}"#;
        let parsed: PermissionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.originator, "app.example.com");
        assert!(parsed.operation.is_none());
    }

    #[test]
    fn test_grouped_permissions_serialization() {
        let perms = GroupedPermissions {
            description: Some("Test permissions".to_string()),
            spending_authorization: Some(SpendingAuthorization {
                amount: 10000,
                description: "Test spending".to_string(),
            }),
            protocol_permissions: None,
            basket_access: None,
            certificate_access: None,
        };
        let json = serde_json::to_string(&perms).unwrap();
        let parsed: GroupedPermissions = serde_json::from_str(&json).unwrap();
        assert!(parsed.spending_authorization.is_some());
        assert_eq!(parsed.spending_authorization.unwrap().amount, 10000);
    }

    #[test]
    fn test_config_operation_permissions_default() {
        let config = WalletPermissionsManagerConfig::default();
        assert!(!config.enforce_permissions);
        assert!(!config.allow_create_action);
        assert!(!config.allow_crypto);
    }

    #[test]
    fn test_config_all_enabled_operation_permissions() {
        let config = WalletPermissionsManagerConfig::all_enabled();
        assert!(config.enforce_permissions);
        assert!(config.allow_create_action);
        assert!(config.allow_sign_action);
        assert!(config.allow_abort_action);
        assert!(config.allow_list_actions);
        assert!(config.allow_internalize_action);
        assert!(config.allow_list_outputs);
        assert!(config.allow_relinquish_output);
        assert!(config.allow_acquire_certificate);
        assert!(config.allow_list_certificates);
        assert!(config.allow_prove_certificate);
        assert!(config.allow_relinquish_certificate);
        assert!(config.allow_discover);
        assert!(config.allow_crypto);
    }

    // =========================================================================
    // New enforcement tests
    // =========================================================================

    #[test]
    fn test_is_admin_protocol() {
        let admin = Protocol::new(SecurityLevel::App, "admin_proto".to_string());
        let normal = Protocol::new(SecurityLevel::App, "my_proto".to_string());
        assert!(WalletPermissionsManager::is_admin_protocol(&admin));
        assert!(!WalletPermissionsManager::is_admin_protocol(&normal));
    }

    #[test]
    fn test_is_admin_basket() {
        assert!(WalletPermissionsManager::is_admin_basket("default"));
        assert!(WalletPermissionsManager::is_admin_basket("admin_stuff"));
        assert!(!WalletPermissionsManager::is_admin_basket("my_basket"));
    }

    #[test]
    fn test_build_cache_key_protocol() {
        let proto = Protocol::new(SecurityLevel::Counterparty, "test".to_string());
        let key = WalletPermissionsManager::build_cache_key(
            PermissionType::Protocol,
            "app.example.com",
            Some(false),
            Some(&proto),
            Some("counterparty_key"),
            None,
            None,
            None,
        );
        assert!(key.starts_with("proto:"));
        assert!(key.contains("app.example.com"));
        assert!(key.contains("test"));
        assert!(key.contains("counterparty_key"));
    }

    #[test]
    fn test_build_cache_key_basket() {
        let key = WalletPermissionsManager::build_cache_key(
            PermissionType::Basket,
            "app.example.com",
            None,
            None,
            None,
            Some("my_basket"),
            None,
            None,
        );
        assert_eq!(key, "basket:app.example.com:my_basket");
    }

    #[test]
    fn test_build_cache_key_certificate() {
        let cert = CertificatePermissionDetails {
            verifier: "verifier_key".to_string(),
            cert_type: "identity".to_string(),
            fields: vec!["name".to_string(), "email".to_string()],
        };
        let key = WalletPermissionsManager::build_cache_key(
            PermissionType::Certificate,
            "app.example.com",
            Some(true),
            None,
            None,
            None,
            Some(&cert),
            None,
        );
        assert!(key.starts_with("cert:"));
        assert!(key.contains("verifier_key"));
        assert!(key.contains("name|email"));
    }

    #[test]
    fn test_build_cache_key_spending() {
        let key = WalletPermissionsManager::build_cache_key(
            PermissionType::Spending,
            "app.example.com",
            None,
            None,
            None,
            None,
            None,
            Some(50000),
        );
        assert_eq!(key, "spend:app.example.com:50000");
    }

    #[tokio::test]
    async fn test_admin_bypasses_protocol_permission() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let proto = Protocol::new(SecurityLevel::App, "test".to_string());
        let result = manager
            .ensure_protocol_permission(
                "admin.wallet",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Signing,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_silent_level_bypasses_protocol_permission() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let proto = Protocol::new(SecurityLevel::Silent, "test".to_string());
        let result = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Signing,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_admin_protocol_blocked() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let proto = Protocol::new(SecurityLevel::App, "admin_secret".to_string());
        let result = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Generic,
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("admin-only"));
    }

    #[tokio::test]
    async fn test_config_bypass_signing_disabled() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.seek_protocol_permissions_for_signing = false;
        let manager = make_manager(config);
        let proto = Protocol::new(SecurityLevel::App, "test".to_string());
        let result = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Signing,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_admin_bypasses_basket_access() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let result = manager
            .ensure_basket_access("admin.wallet", "any_basket", None, BasketUsageType::Listing)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_admin_basket_blocked() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let result = manager
            .ensure_basket_access("app.example.com", "default", None, BasketUsageType::Listing)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("admin-only"));
    }

    #[tokio::test]
    async fn test_basket_config_bypass() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.seek_basket_listing_permissions = false;
        let manager = make_manager(config);
        let result = manager
            .ensure_basket_access(
                "app.example.com",
                "my_basket",
                None,
                BasketUsageType::Listing,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_admin_bypasses_spending() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let result = manager
            .ensure_spending_permission("admin.wallet", 100000, None)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_spending_config_bypass() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.seek_spending_permissions = false;
        let manager = make_manager(config);
        let result = manager
            .ensure_spending_permission("app.example.com", 100000, None)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_cache_permission_and_lookup() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let key = "test_cache_key".to_string();
        // Not cached initially
        assert!(!manager.is_permission_cached(&key).await);
        // Cache it
        let future_expiry = Utc::now().timestamp() as u64 + 3600;
        manager.cache_permission(key.clone(), future_expiry).await;
        // Now cached
        assert!(manager.is_permission_cached(&key).await);
    }

    #[tokio::test]
    async fn test_expired_cache_entry_removed() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let key = "expired_key".to_string();
        // Cache with past expiry
        manager.cache_permission(key.clone(), 1).await;
        // Should not be cached (expired)
        assert!(!manager.is_permission_cached(&key).await);
    }

    #[tokio::test]
    async fn test_permission_request_flow_no_handler() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let proto = Protocol::new(SecurityLevel::App, "test".to_string());
        let result = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Generic,
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No permission request handler"));
    }

    #[tokio::test]
    async fn test_permission_request_flow_with_handler() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let future_expiry = Utc::now().timestamp() as u64 + 3600;
        let handler: PermissionRequestHandler = Arc::new(move |_req| {
            let expiry = future_expiry;
            Box::pin(async move {
                Ok(PermissionToken {
                    txid: "granted123".to_string(),
                    tx: vec![],
                    output_index: 0,
                    output_script: "76a914...88ac".to_string(),
                    satoshis: 0,
                    originator: "app.example.com".to_string(),
                    raw_originator: None,
                    expiry,
                    privileged: None,
                    protocol: Some("test".to_string()),
                    security_level: Some(1),
                    counterparty: None,
                    basket_name: None,
                    cert_type: None,
                    cert_fields: None,
                    verifier: None,
                    authorized_amount: None,
                })
            })
        });
        manager.set_permission_request_handler(handler).await;
        let proto = Protocol::new(SecurityLevel::App, "test".to_string());
        let result = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Generic,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
        // Second call should hit cache
        let result2 = manager
            .ensure_protocol_permission(
                "app.example.com",
                false,
                &proto,
                None,
                None,
                PermissionUsageType::Generic,
            )
            .await;
        assert!(result2.is_ok());
        assert!(result2.unwrap());
    }

    #[tokio::test]
    async fn test_admin_bypasses_certificate_access() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let result = manager
            .ensure_certificate_access(
                "admin.wallet",
                false,
                "verifier_key",
                "identity",
                &["name".to_string()],
                None,
                CertificateUsageType::Disclosure,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_certificate_config_bypass() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.seek_certificate_disclosure_permissions = false;
        let manager = make_manager(config);
        let result = manager
            .ensure_certificate_access(
                "app.example.com",
                false,
                "verifier_key",
                "identity",
                &["name".to_string()],
                None,
                CertificateUsageType::Disclosure,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_verify_token_valid() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let future_ts = Utc::now().timestamp() as u64 + 3600;
        let token = make_token("app.example.com", future_ts);
        assert!(manager.verify_token(&token).is_ok());
    }

    #[test]
    fn test_verify_token_expired() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let token = make_token("app.example.com", 1);
        assert!(manager.verify_token(&token).is_err());
    }

    #[test]
    fn test_verify_token_empty_txid() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let mut token = make_token("app.example.com", 0);
        token.txid = String::new();
        assert!(manager.verify_token(&token).is_err());
    }

    #[test]
    fn test_check_permission_admin_always_allowed() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_enabled());
        let req = PermissionRequest {
            operation: Some("createAction".to_string()),
            ..PermissionRequest::bare(PermissionType::Protocol, "admin.wallet")
        };
        assert!(manager.check_permission(&req));
    }

    #[test]
    fn test_check_permission_enforced_denied() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.allow_create_action = false;
        let manager = make_manager(config);
        let req = PermissionRequest {
            operation: Some("createAction".to_string()),
            ..PermissionRequest::bare(PermissionType::Protocol, "app.example.com")
        };
        assert!(!manager.check_permission(&req));
    }

    #[tokio::test]
    async fn test_purge_expired_tokens() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        // Add expired and valid entries
        manager.cache_permission("expired".to_string(), 1).await;
        let future = Utc::now().timestamp() as u64 + 3600;
        manager.cache_permission("valid".to_string(), future).await;
        manager.purge_expired_tokens().await;
        // Expired should be purged
        assert!(!manager.is_permission_cached("expired").await);
        // Valid should remain
        assert!(manager.is_permission_cached("valid").await);
    }

    #[test]
    fn test_wallet_for_originator_blocks_admin() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let result = manager.wallet_for_originator("admin.wallet");
        assert!(result.is_err());
    }

    #[test]
    fn test_wallet_for_originator_allows_non_admin() {
        let manager = make_manager(WalletPermissionsManagerConfig::all_disabled());
        let result = manager.wallet_for_originator("app.example.com");
        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod module_call_tests {
    use super::*;
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::{
        CreateHmacArgs, CreateSignatureArgs, EncryptArgs, VerifyHmacArgs, VerifySignatureArgs,
    };
    use serde_json::{json, Value};

    // =========================================================================
    // The JSON mappings (`Wallet.interfaces.ts`'s shapes): each call's request
    // and result in the reference's object, read and written back unchanged.
    // =========================================================================

    /// `args` and `output` in the reference's shape survive the round trip.
    fn round_trip<A: ModuleCall>(args: &Value, output: &Value) {
        let read = A::from_json(args).unwrap_or_else(|e| panic!("{}: {e}", A::METHOD));
        assert_eq!(&Value::Object(read.to_json()), args, "{}", A::METHOD);
        let result = A::output_from_json(output).unwrap_or_else(|e| panic!("{}: {e}", A::METHOD));
        assert_eq!(&A::output_json(&result), output, "{}", A::METHOD);
    }

    #[test]
    fn mappings_round_trip_the_reference_shape() {
        let peer = PrivateKey::random().public_key().to_hex();
        let hash: Vec<u8> = (0..32).collect();
        let keyed = |extra: Value| {
            let mut object = json!({
                "protocolID": [2, "p btms witness"], "keyID": "1", "counterparty": peer,
                "privileged": true, "privilegedReason": "privileged witness", "seekPermission": false,
            });
            object
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            object
        };
        // `identityKey` is `true` or absent in the reference's type: a false
        // flag leaves the key out, and an absent key reads back false.
        round_trip::<GetPublicKeyArgs>(
            &keyed(json!({ "identityKey": true, "forSelf": true })),
            &json!({ "publicKey": peer }),
        );
        let without = keyed(json!({ "forSelf": true }));
        round_trip::<GetPublicKeyArgs>(&without, &json!({ "publicKey": peer }));
        let read = GetPublicKeyArgs::from_json(&without).unwrap();
        assert!(!read.identity_key);
        assert_eq!(
            serde_json::to_string(&Value::Object(read.to_json())).unwrap(),
            serde_json::to_string(&without).unwrap(),
        );
        round_trip::<EncryptArgs>(
            &keyed(json!({ "plaintext": [1, 2] })),
            &json!({ "ciphertext": [3] }),
        );
        round_trip::<DecryptArgs>(
            &keyed(json!({ "ciphertext": [3] })),
            &json!({ "plaintext": [1, 2] }),
        );
        round_trip::<CreateHmacArgs>(&keyed(json!({ "data": [1] })), &json!({ "hmac": hash }));
        round_trip::<VerifyHmacArgs>(
            &keyed(json!({ "data": [1], "hmac": hash })),
            &json!({ "valid": true }),
        );
        round_trip::<CreateSignatureArgs>(
            &keyed(json!({ "hashToDirectlySign": hash, "counterparty": "anyone" })),
            &json!({ "signature": [48, 1] }),
        );
        round_trip::<VerifySignatureArgs>(
            &keyed(
                json!({ "hashToDirectlyVerify": hash, "signature": [48, 1], "counterparty": "self", "forSelf": false }),
            ),
            &json!({ "valid": false }),
        );
    }

    #[test]
    fn basket_and_label_mappings_round_trip_the_reference_shape() {
        use bsv_rs::wallet::{
            CreateActionArgs, InternalizeActionArgs, ListActionsArgs, ListOutputsArgs,
            RelinquishOutputArgs,
        };
        round_trip::<ListOutputsArgs>(
            &json!({ "basket": "p btms token", "tags": ["t"], "limit": 10, "seekPermission": false }),
            &json!({ "totalOutputs": 0, "outputs": [] }),
        );
        round_trip::<RelinquishOutputArgs>(
            &json!({ "basket": "p btms token", "output": format!("{}.0", "ab".repeat(32)) }),
            &json!({ "relinquished": true }),
        );
        round_trip::<CreateActionArgs>(
            &json!({ "description": "a witness", "labels": ["p btms token"],
                "outputs": [{ "lockingScript": "51", "satoshis": 1,
                    "outputDescription": "a witness", "basket": "p btms token" }] }),
            &json!({ "txid": "ab".repeat(32) }),
        );
        round_trip::<ListActionsArgs>(
            &json!({ "labels": ["p btms token"], "includeLabels": true }),
            &json!({ "totalActions": 0, "actions": [] }),
        );
        round_trip::<InternalizeActionArgs>(
            &json!({ "tx": [1, 2], "description": "a witness", "labels": ["p btms token"],
                "outputs": [{ "outputIndex": 0, "protocol": "basket insertion",
                    "insertionRemittance": { "basket": "p btms token" } }] }),
            &json!({ "accepted": true }),
        );
    }

    #[test]
    fn label_grammar_is_the_references() {
        // `splitLabelsByPermissionModule`'s examples (`:907-915`).
        for (label, scheme) in [
            ("p btms token123", Some("btms")),
            ("p invoicing invoice 2026-02-02", Some("invoicing")),
            ("btms token", None),
            ("P btms token", None),
        ] {
            assert_eq!(label_scheme(label), Ok(scheme), "{label}");
        }
        for label in ["p btms", "p btms ", "p  data", "p btms  x", "p "] {
            let text = format!("Invalid P-label format: {label}");
            assert_eq!(label_scheme(label), Err(text), "{label}");
        }
    }

    #[test]
    fn mappings_refuse_what_does_not_parse() {
        let refusals = [
            EncryptArgs::from_json(&json!([])).map(drop),
            EncryptArgs::from_json(
                &json!({ "plaintext": [256], "protocolID": [1, "x"], "keyID": "1" }),
            )
            .map(drop),
            EncryptArgs::from_json(
                &json!({ "plaintext": [], "protocolID": [3, "x"], "keyID": "1" }),
            )
            .map(drop),
            CreateHmacArgs::output_from_json(&json!({ "hmac": [1] })).map(drop),
        ];
        let texts: Vec<String> = refusals.into_iter().map(|r| r.unwrap_err()).collect();
        assert_eq!(
            texts,
            [
                "P-module arguments for encrypt must be an object",
                "P-module plaintext for encrypt is invalid",
                "P-module protocolID for encrypt is invalid",
                "P-module hmac for createHmac is invalid",
            ]
        );
    }
}
