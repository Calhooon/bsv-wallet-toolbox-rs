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
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bsv_rs::wallet::{Protocol, SecurityLevel, WalletInterface};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

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
/// use bsv_wallet_toolbox::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig};
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
        }
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

    /// Invokes the permission request handler to get user consent.
    ///
    /// If no handler is registered, returns an error.
    /// On success, caches the granted permission.
    pub async fn request_permission_flow(&self, request: PermissionRequest) -> Result<bool> {
        let handler_guard = self.permission_request_handler.read().await;
        let handler = handler_guard.as_ref().ok_or_else(|| {
            Error::AccessDenied(
                "No permission request handler registered. Cannot request user consent."
                    .to_string(),
            )
        })?;

        let token = handler(request.clone()).await?;

        // Cache the granted permission
        let cache_key = Self::build_cache_key(
            request.request_type,
            &request.originator,
            request.privileged,
            request.protocol_id.as_ref(),
            request.counterparty.as_deref(),
            request.basket.as_deref(),
            request.certificate.as_ref(),
            request.spending.as_ref().map(|s| s.satoshis),
        );
        self.cache_permission(cache_key, token.expiry).await;

        Ok(true)
    }

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
    /// 5. Cache check
    /// 6. Permission request flow (if handler registered)
    pub async fn ensure_protocol_permission(
        &self,
        originator: &str,
        privileged: bool,
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

        // 5) Cache check
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
        if self.is_permission_cached(&cache_key).await {
            return Ok(true);
        }

        // 6) Request permission flow
        self.request_permission_flow(PermissionRequest {
            request_type: PermissionType::Protocol,
            originator: originator.to_string(),
            display_originator: None,
            privileged: Some(priv_flag),
            protocol_id: Some(protocol.clone()),
            counterparty: counterparty.map(|s| s.to_string()),
            basket: None,
            certificate: None,
            spending: None,
            reason: reason.map(|s| s.to_string()),
            operation: None,
        })
        .await
    }

    /// Ensures the originator has basket access permission (DBAP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Admin-reserved basket blocking
    /// 3. Usage-type config flag bypass
    /// 4. Cache check
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
        if self.is_permission_cached(&cache_key).await {
            return Ok(true);
        }

        // 5) Request permission flow
        self.request_permission_flow(PermissionRequest {
            request_type: PermissionType::Basket,
            originator: originator.to_string(),
            display_originator: None,
            privileged: None,
            protocol_id: None,
            counterparty: None,
            basket: Some(basket.to_string()),
            certificate: None,
            spending: None,
            reason: reason.map(|s| s.to_string()),
            operation: None,
        })
        .await
    }

    /// Ensures the originator has certificate access permission (DCAP).
    ///
    /// Checks in order:
    /// 1. Admin originator bypass
    /// 2. Usage-type config flag bypass
    /// 3. Privileged normalization
    /// 4. Cache check
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
        if self.is_permission_cached(&cache_key).await {
            return Ok(true);
        }

        // 5) Request permission flow
        self.request_permission_flow(PermissionRequest {
            request_type: PermissionType::Certificate,
            originator: originator.to_string(),
            display_originator: None,
            privileged: Some(priv_flag),
            protocol_id: None,
            counterparty: None,
            basket: None,
            certificate: Some(cert_details),
            spending: None,
            reason: reason.map(|s| s.to_string()),
            operation: None,
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
            request_type: PermissionType::Spending,
            originator: originator.to_string(),
            display_originator: None,
            privileged: None,
            protocol_id: None,
            counterparty: None,
            basket: None,
            certificate: None,
            spending: Some(SpendingPermissionDetails {
                satoshis,
                line_items: None,
            }),
            reason: reason.map(|s| s.to_string()),
            operation: None,
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
// Tests
// =============================================================================

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
            _originator: &str,
        ) -> bsv_rs::Result<CreateActionResult> {
            unimplemented!()
        }
        async fn sign_action(
            &self,
            _args: SignActionArgs,
            _originator: &str,
        ) -> bsv_rs::Result<SignActionResult> {
            unimplemented!()
        }
        async fn abort_action(
            &self,
            _args: AbortActionArgs,
            _originator: &str,
        ) -> bsv_rs::Result<AbortActionResult> {
            unimplemented!()
        }
        async fn list_actions(
            &self,
            _args: ListActionsArgs,
            _originator: &str,
        ) -> bsv_rs::Result<ListActionsResult> {
            unimplemented!()
        }
        async fn internalize_action(
            &self,
            _args: InternalizeActionArgs,
            _originator: &str,
        ) -> bsv_rs::Result<InternalizeActionResult> {
            unimplemented!()
        }
        async fn list_outputs(
            &self,
            _args: ListOutputsArgs,
            _originator: &str,
        ) -> bsv_rs::Result<ListOutputsResult> {
            unimplemented!()
        }
        async fn relinquish_output(
            &self,
            _args: RelinquishOutputArgs,
            _originator: &str,
        ) -> bsv_rs::Result<RelinquishOutputResult> {
            unimplemented!()
        }
        async fn get_public_key(
            &self,
            _args: GetPublicKeyArgs,
            _originator: &str,
        ) -> bsv_rs::Result<GetPublicKeyResult> {
            unimplemented!()
        }
        async fn reveal_counterparty_key_linkage(
            &self,
            _args: interface::RevealCounterpartyKeyLinkageArgs,
            _originator: &str,
        ) -> bsv_rs::Result<RevealCounterpartyKeyLinkageResult> {
            unimplemented!()
        }
        async fn reveal_specific_key_linkage(
            &self,
            _args: interface::RevealSpecificKeyLinkageArgs,
            _originator: &str,
        ) -> bsv_rs::Result<RevealSpecificKeyLinkageResult> {
            unimplemented!()
        }
        async fn encrypt(
            &self,
            _args: EncryptArgs,
            _originator: &str,
        ) -> bsv_rs::Result<EncryptResult> {
            unimplemented!()
        }
        async fn decrypt(
            &self,
            _args: DecryptArgs,
            _originator: &str,
        ) -> bsv_rs::Result<DecryptResult> {
            unimplemented!()
        }
        async fn create_hmac(
            &self,
            _args: CreateHmacArgs,
            _originator: &str,
        ) -> bsv_rs::Result<CreateHmacResult> {
            unimplemented!()
        }
        async fn verify_hmac(
            &self,
            _args: VerifyHmacArgs,
            _originator: &str,
        ) -> bsv_rs::Result<VerifyHmacResult> {
            unimplemented!()
        }
        async fn create_signature(
            &self,
            _args: CreateSignatureArgs,
            _originator: &str,
        ) -> bsv_rs::Result<CreateSignatureResult> {
            unimplemented!()
        }
        async fn verify_signature(
            &self,
            _args: VerifySignatureArgs,
            _originator: &str,
        ) -> bsv_rs::Result<VerifySignatureResult> {
            unimplemented!()
        }
        async fn acquire_certificate(
            &self,
            _args: AcquireCertificateArgs,
            _originator: &str,
        ) -> bsv_rs::Result<WalletCertificate> {
            unimplemented!()
        }
        async fn list_certificates(
            &self,
            _args: ListCertificatesArgs,
            _originator: &str,
        ) -> bsv_rs::Result<ListCertificatesResult> {
            unimplemented!()
        }
        async fn prove_certificate(
            &self,
            _args: ProveCertificateArgs,
            _originator: &str,
        ) -> bsv_rs::Result<ProveCertificateResult> {
            unimplemented!()
        }
        async fn relinquish_certificate(
            &self,
            _args: RelinquishCertificateArgs,
            _originator: &str,
        ) -> bsv_rs::Result<RelinquishCertificateResult> {
            unimplemented!()
        }
        async fn discover_by_identity_key(
            &self,
            _args: DiscoverByIdentityKeyArgs,
            _originator: &str,
        ) -> bsv_rs::Result<DiscoverCertificatesResult> {
            unimplemented!()
        }
        async fn discover_by_attributes(
            &self,
            _args: DiscoverByAttributesArgs,
            _originator: &str,
        ) -> bsv_rs::Result<DiscoverCertificatesResult> {
            unimplemented!()
        }
        async fn is_authenticated(&self, _originator: &str) -> bsv_rs::Result<AuthenticatedResult> {
            unimplemented!()
        }
        async fn wait_for_authentication(
            &self,
            _originator: &str,
        ) -> bsv_rs::Result<AuthenticatedResult> {
            unimplemented!()
        }
        async fn get_height(&self, _originator: &str) -> bsv_rs::Result<GetHeightResult> {
            unimplemented!()
        }
        async fn get_header_for_height(
            &self,
            _args: GetHeaderArgs,
            _originator: &str,
        ) -> bsv_rs::Result<GetHeaderResult> {
            unimplemented!()
        }
        async fn get_network(&self, _originator: &str) -> bsv_rs::Result<GetNetworkResult> {
            unimplemented!()
        }
        async fn get_version(&self, _originator: &str) -> bsv_rs::Result<GetVersionResult> {
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
            request_type: PermissionType::Protocol,
            originator: "app.example.com".to_string(),
            display_originator: None,
            privileged: Some(false),
            protocol_id: Some(Protocol::new(
                SecurityLevel::Counterparty,
                "test protocol".to_string(),
            )),
            counterparty: None,
            basket: None,
            certificate: None,
            spending: None,
            reason: Some("Testing".to_string()),
            operation: Some("createAction".to_string()),
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
            request_type: PermissionType::Protocol,
            originator: "admin.wallet".to_string(),
            display_originator: None,
            privileged: None,
            protocol_id: None,
            counterparty: None,
            basket: None,
            certificate: None,
            spending: None,
            reason: None,
            operation: Some("createAction".to_string()),
        };
        assert!(manager.check_permission(&req));
    }

    #[test]
    fn test_check_permission_enforced_denied() {
        let mut config = WalletPermissionsManagerConfig::all_enabled();
        config.allow_create_action = false;
        let manager = make_manager(config);
        let req = PermissionRequest {
            request_type: PermissionType::Protocol,
            originator: "app.example.com".to_string(),
            display_originator: None,
            privileged: None,
            protocol_id: None,
            counterparty: None,
            basket: None,
            certificate: None,
            spending: None,
            reason: None,
            operation: Some("createAction".to_string()),
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
