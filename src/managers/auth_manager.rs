//! Wallet Authentication Manager
//!
//! Provides a higher-level abstraction over CWIStyleWalletManager with
//! authentication backend (WAB) support. This manager integrates with
//! Wallet Authentication Backends for seamless user authentication flows.

use super::cwi_style_wallet_manager::{
    CWIStyleWalletManager, CWIStyleWalletManagerConfig, CWIWalletBuilder,
};

/// Wallet Authentication Manager for WAB (Wallet Authentication Backend) integration.
///
/// Provides a higher-level abstraction over CWIStyleWalletManager with
/// authentication backend support. This enables integration with external
/// authentication providers while maintaining the multi-profile wallet
/// management capabilities of the underlying CWI-style manager.
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::managers::{WalletAuthenticationManager, CWIStyleWalletManagerConfig};
///
/// let manager = WalletAuthenticationManager::new(
///     "admin.wallet".to_string(),
///     wallet_builder,
///     CWIStyleWalletManagerConfig::default(),
/// );
///
/// // Access the inner CWI manager for profile operations
/// let profiles = manager.inner().get_profiles().await;
/// ```
pub struct WalletAuthenticationManager {
    /// The underlying wallet manager.
    inner: CWIStyleWalletManager,
}

impl WalletAuthenticationManager {
    /// Create a new authentication manager.
    pub fn new(
        admin_originator: String,
        wallet_builder: CWIWalletBuilder,
        config: CWIStyleWalletManagerConfig,
    ) -> Self {
        Self {
            inner: CWIStyleWalletManager::new(admin_originator, wallet_builder, config),
        }
    }

    /// Get a reference to the inner wallet manager.
    pub fn inner(&self) -> &CWIStyleWalletManager {
        &self.inner
    }

    /// Get a mutable reference to the inner wallet manager.
    pub fn inner_mut(&mut self) -> &mut CWIStyleWalletManager {
        &mut self.inner
    }
}
