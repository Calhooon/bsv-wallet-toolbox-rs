//! Wallet Permissions Manager (Stub)
//!
//! **SECURITY-CRITICAL**: This is a stub implementation. Do not use for production
//! until full BRC-98/99 permission management is implemented.
//!
//! This module provides types and a basic structure for BRC-98/99 permission
//! management, but the actual permission checking logic is not yet implemented.
//! The wrapper simply provides access to the underlying wallet.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsv_sdk::wallet::{Protocol, WalletInterface};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};

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
        }
    }

    /// Creates a new config with all permission checks disabled.
    pub fn all_disabled() -> Self {
        Self::default()
    }
}

/// Wallet permissions manager (stub implementation).
///
/// **WARNING**: This is a stub implementation that does not perform actual
/// permission checks. All operations are passed through to the underlying
/// wallet without verification.
///
/// When fully implemented, this manager will:
/// - Check for valid permission tokens before operations
/// - Request permissions from the user when needed
/// - Create/renew on-chain permission tokens
/// - Transform requests/responses via permission modules
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig};
///
/// let manager = WalletPermissionsManager::new(
///     underlying_wallet,
///     "admin.wallet".to_string(),
///     WalletPermissionsManagerConfig::all_disabled(), // STUB: no checking
/// );
///
/// // Access the underlying wallet
/// let wallet = manager.wallet();
/// let result = wallet.create_action(args, "app.example.com").await?;
/// ```
pub struct WalletPermissionsManager {
    /// The underlying wallet.
    underlying: Arc<dyn WalletInterface + Send + Sync>,
    /// Admin originator (always allowed).
    admin_originator: String,
    /// Configuration.
    #[allow(dead_code)]
    config: WalletPermissionsManagerConfig,
    /// Permission cache (not yet used).
    #[allow(dead_code)]
    permission_cache: RwLock<HashMap<String, PermissionToken>>,
}

impl WalletPermissionsManager {
    /// Creates a new permissions manager.
    ///
    /// **Note**: This is a stub that passes through all operations.
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
        }
    }

    /// Gets the underlying wallet.
    ///
    /// **Note**: This is a stub - no permission checking is performed.
    pub fn wallet(&self) -> Arc<dyn WalletInterface + Send + Sync> {
        self.underlying.clone()
    }

    /// Gets the underlying wallet, checking originator is not admin.
    ///
    /// **Note**: This is a stub - only admin check is performed.
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

    /// Checks if the originator is the admin (always allowed).
    #[allow(dead_code)]
    fn is_admin(&self, originator: &str) -> bool {
        originator == self.admin_originator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                bsv_sdk::wallet::SecurityLevel::Counterparty,
                "test protocol".to_string(),
            )),
            counterparty: None,
            basket: None,
            certificate: None,
            spending: None,
            reason: Some("Testing".to_string()),
        };

        let json = serde_json::to_string(&request).unwrap();
        let parsed: PermissionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.request_type, PermissionType::Protocol);
        assert_eq!(parsed.originator, "app.example.com");
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
}
