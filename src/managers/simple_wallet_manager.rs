//! Simple Wallet Manager
//!
//! A wallet manager that requires two authentication factors:
//! 1. A primary key (32 bytes)
//! 2. A privileged key manager
//!
//! Once both are provided, the manager becomes authenticated and delegates
//! all wallet operations to an underlying wallet instance built from these secrets.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bsv_sdk::primitives::{PrivateKey, SymmetricKey};
use bsv_sdk::wallet::WalletInterface;
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// Generate random bytes using the rand crate.
fn generate_random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Type alias for the wallet builder function.
pub type WalletBuilder = Arc<
    dyn Fn(
            Vec<u8>,
            PrivateKey,
        )
            -> Pin<Box<dyn Future<Output = Result<Arc<dyn WalletInterface + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;

/// A simple wallet manager requiring two-factor authentication.
///
/// # Authentication Flow
///
/// 1. Create manager with admin originator and wallet builder
/// 2. Provide primary key via `provide_primary_key()`
/// 3. Provide privileged key manager via `provide_privileged_key_manager()`
/// 4. Once both provided, underlying wallet is built and manager is authenticated
/// 5. Access the underlying wallet via `wallet()` method
///
/// # Security
///
/// - The admin originator is reserved for internal use
/// - External applications cannot use the admin originator
/// - All wallet method calls check authentication before proceeding
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::managers::SimpleWalletManager;
///
/// let manager = SimpleWalletManager::new(
///     "wallet.admin".to_string(),
///     Arc::new(|pk, pkm| Box::pin(async move {
///         // Build wallet from primary key and privileged key manager
///         Ok(Arc::new(wallet) as Arc<dyn WalletInterface + Send + Sync>)
///     })),
///     None,
/// );
///
/// manager.provide_primary_key(primary_key_bytes).await?;
/// manager.provide_privileged_key_manager(pkm_key).await?;
///
/// // Now authenticated - get the wallet and use it
/// let wallet = manager.wallet().await?;
/// let result = wallet.create_action(args, "app.example.com").await?;
/// ```
pub struct SimpleWalletManager {
    /// Whether the user is authenticated.
    authenticated: RwLock<bool>,

    /// The admin originator domain (reserved for internal use).
    admin_originator: String,

    /// Function to build the underlying wallet.
    wallet_builder: WalletBuilder,

    /// The underlying wallet instance (built after authentication).
    underlying: RwLock<Option<Arc<dyn WalletInterface + Send + Sync>>>,

    /// The privileged key manager key.
    privileged_key_manager: RwLock<Option<PrivateKey>>,

    /// The primary key (32 bytes).
    primary_key: RwLock<Option<Vec<u8>>>,
}

impl SimpleWalletManager {
    /// Creates a new simple wallet manager.
    ///
    /// # Arguments
    ///
    /// * `admin_originator` - The reserved originator for admin operations
    /// * `wallet_builder` - Function to build wallet from primary key and PKM
    /// * `state_snapshot` - Optional snapshot to restore state from
    pub fn new(
        admin_originator: String,
        wallet_builder: WalletBuilder,
        state_snapshot: Option<Vec<u8>>,
    ) -> Self {
        let manager = Self {
            authenticated: RwLock::new(false),
            admin_originator,
            wallet_builder,
            underlying: RwLock::new(None),
            privileged_key_manager: RwLock::new(None),
            primary_key: RwLock::new(None),
        };

        // Load snapshot if provided (sync operation during construction)
        if let Some(snapshot) = state_snapshot {
            // This will load the primary key but won't build the wallet yet
            // (privileged key manager still needed)
            if let Ok(pk) = Self::decode_snapshot(&snapshot) {
                *manager.primary_key.blocking_write() = Some(pk);
            }
        }

        manager
    }

    /// Provides the primary key for authentication.
    ///
    /// If the privileged key manager has already been provided, this will
    /// trigger wallet construction and complete authentication.
    pub async fn provide_primary_key(&self, key: Vec<u8>) -> Result<()> {
        if key.len() != 32 {
            return Err(Error::InvalidArgument(
                "Primary key must be 32 bytes".to_string(),
            ));
        }

        *self.primary_key.write().await = Some(key);
        self.try_build_underlying().await
    }

    /// Provides the privileged key manager for authentication.
    ///
    /// If the primary key has already been provided, this will
    /// trigger wallet construction and complete authentication.
    pub async fn provide_privileged_key_manager(&self, manager: PrivateKey) -> Result<()> {
        *self.privileged_key_manager.write().await = Some(manager);
        self.try_build_underlying().await
    }

    /// Attempts to build the underlying wallet if both auth factors are present.
    async fn try_build_underlying(&self) -> Result<()> {
        if *self.authenticated.read().await {
            return Err(Error::InvalidOperation("Already authenticated".to_string()));
        }

        let primary_key = self.primary_key.read().await.clone();
        let pkm = self.privileged_key_manager.read().await.clone();

        match (primary_key, pkm) {
            (Some(pk), Some(pkm_key)) => {
                let wallet = (self.wallet_builder)(pk, pkm_key).await?;
                *self.underlying.write().await = Some(wallet);
                *self.authenticated.write().await = true;
                Ok(())
            }
            _ => Ok(()), // Not ready yet, wait for both factors
        }
    }

    /// Returns whether the user is authenticated.
    pub async fn is_authenticated(&self) -> bool {
        *self.authenticated.read().await
    }

    /// Gets the underlying wallet if authenticated.
    ///
    /// # Returns
    ///
    /// The underlying wallet implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if not authenticated.
    pub async fn wallet(&self) -> Result<Arc<dyn WalletInterface + Send + Sync>> {
        self.underlying
            .read()
            .await
            .clone()
            .ok_or(Error::AuthenticationRequired)
    }

    /// Gets the underlying wallet, checking originator is not admin.
    pub async fn wallet_for_originator(
        &self,
        originator: &str,
    ) -> Result<Arc<dyn WalletInterface + Send + Sync>> {
        if originator == self.admin_originator {
            return Err(Error::AccessDenied(
                "External applications cannot use the admin originator".to_string(),
            ));
        }
        self.wallet().await
    }

    /// Returns the admin originator.
    pub fn admin_originator(&self) -> &str {
        &self.admin_originator
    }

    /// Destroys the underlying wallet, returning to a default (unauthenticated) state.
    pub async fn destroy(&self) {
        *self.underlying.write().await = None;
        *self.privileged_key_manager.write().await = None;
        *self.authenticated.write().await = false;
        *self.primary_key.write().await = None;
    }

    /// Saves the current state to an encrypted snapshot.
    ///
    /// The snapshot contains only the primary key (encrypted).
    /// The privileged key manager must be provided separately after restore.
    pub async fn save_snapshot(&self) -> Result<Vec<u8>> {
        let primary_key = self.primary_key.read().await.clone();

        let pk = primary_key.ok_or_else(|| {
            Error::InvalidOperation("No primary key set; cannot save snapshot".to_string())
        })?;

        Self::encode_snapshot(&pk)
    }

    /// Loads state from a previously saved snapshot.
    ///
    /// This restores the primary key but does not complete authentication.
    /// The privileged key manager must still be provided.
    pub async fn load_snapshot(&self, snapshot: Vec<u8>) -> Result<()> {
        let pk = Self::decode_snapshot(&snapshot)?;
        *self.primary_key.write().await = Some(pk);
        self.try_build_underlying().await
    }

    /// Encodes a snapshot from the primary key.
    fn encode_snapshot(primary_key: &[u8]) -> Result<Vec<u8>> {
        // Generate random encryption key
        let snapshot_key = generate_random_bytes(32);

        // Build payload: version (1 byte) + length (varint) + primary key
        let mut payload = vec![1u8]; // Version 1
        payload.push(primary_key.len() as u8); // Simple length encoding
        payload.extend_from_slice(primary_key);

        // Encrypt payload
        let sym_key = SymmetricKey::from_bytes(&snapshot_key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let encrypted = sym_key
            .encrypt(&payload)
            .map_err(|e| Error::InvalidOperation(format!("Failed to encrypt snapshot: {:?}", e)))?;

        // Build final snapshot: key (32 bytes) + encrypted payload
        let mut result = snapshot_key;
        result.extend_from_slice(&encrypted);

        Ok(result)
    }

    /// Decodes a snapshot to extract the primary key.
    fn decode_snapshot(snapshot: &[u8]) -> Result<Vec<u8>> {
        if snapshot.len() < 33 {
            return Err(Error::InvalidArgument("Snapshot too short".to_string()));
        }

        let snapshot_key = &snapshot[0..32];
        let encrypted = &snapshot[32..];

        // Decrypt payload
        let sym_key = SymmetricKey::from_bytes(snapshot_key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let decrypted = sym_key
            .decrypt(encrypted)
            .map_err(|e| Error::InvalidOperation(format!("Failed to decrypt snapshot: {:?}", e)))?;

        if decrypted.is_empty() {
            return Err(Error::InvalidArgument(
                "Empty decrypted payload".to_string(),
            ));
        }

        // Parse payload
        let version = decrypted[0];
        if version != 1 {
            return Err(Error::InvalidArgument(format!(
                "Unsupported snapshot version: {}",
                version
            )));
        }

        if decrypted.len() < 2 {
            return Err(Error::InvalidArgument(
                "Snapshot payload too short".to_string(),
            ));
        }

        let pk_len = decrypted[1] as usize;
        if decrypted.len() < 2 + pk_len {
            return Err(Error::InvalidArgument(
                "Primary key data truncated".to_string(),
            ));
        }

        Ok(decrypted[2..2 + pk_len].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_roundtrip() {
        let primary_key = vec![1u8; 32];
        let encoded = SimpleWalletManager::encode_snapshot(&primary_key).unwrap();
        let decoded = SimpleWalletManager::decode_snapshot(&encoded).unwrap();
        assert_eq!(primary_key, decoded);
    }

    #[test]
    fn test_snapshot_too_short() {
        let short = vec![0u8; 10];
        let result = SimpleWalletManager::decode_snapshot(&short);
        assert!(result.is_err());
    }
}
