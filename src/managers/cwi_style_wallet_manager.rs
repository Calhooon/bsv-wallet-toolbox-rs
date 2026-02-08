//! CWI Style Wallet Manager
//!
//! A wallet manager compatible with the Common Wallet Interface (CWI) pattern,
//! providing multi-profile support with PBKDF2 password-based key derivation.
//!
//! Once a profile is switched to with the correct password, the manager becomes
//! authenticated and delegates all wallet operations to an underlying wallet instance.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bsv_sdk::primitives::{PrivateKey, SymmetricKey};
use bsv_sdk::wallet::WalletInterface;
use chrono::{DateTime, Utc};
use ring::pbkdf2;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// Generate random bytes using the rand crate.
fn generate_random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Default number of PBKDF2 rounds (matches TypeScript).
pub const DEFAULT_PASSWORD_ROUNDS: u32 = 7777;

/// Profile ID length in bytes.
const PROFILE_ID_LENGTH: usize = 16;

/// Primary pad length in bytes.
const PRIMARY_PAD_LENGTH: usize = 32;

/// Privileged pad length in bytes.
const PRIVILEGED_PAD_LENGTH: usize = 32;

/// Configuration for the CWI-style wallet manager.
#[derive(Debug, Clone)]
pub struct CWIStyleWalletManagerConfig {
    /// Number of PBKDF2 rounds for password derivation.
    pub password_rounds: u32,
    /// Whether to use PBKDF2 for key derivation.
    pub use_pbkdf2: bool,
}

impl Default for CWIStyleWalletManagerConfig {
    fn default() -> Self {
        Self {
            password_rounds: DEFAULT_PASSWORD_ROUNDS,
            use_pbkdf2: true,
        }
    }
}

/// A wallet profile for multi-profile support.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    /// Display name for the profile.
    pub name: String,
    /// Unique identifier (16 bytes).
    #[serde(with = "base64_bytes")]
    pub id: Vec<u8>,
    /// Primary pad XOR'd with derived key to get root key.
    #[serde(with = "base64_bytes")]
    pub primary_pad: Vec<u8>,
    /// Privileged key pad (32 bytes) for two-factor authentication.
    #[serde(default, with = "base64_bytes_default")]
    pub privileged_pad: Vec<u8>,
    /// When the profile was created.
    pub created_at: DateTime<Utc>,
    /// When the profile was last updated.
    pub updated_at: DateTime<Utc>,
}

impl Profile {
    /// Creates a new profile with random id, primary pad, and privileged pad.
    pub fn new(name: String) -> Self {
        Self {
            name,
            id: generate_random_bytes(PROFILE_ID_LENGTH),
            primary_pad: generate_random_bytes(PRIMARY_PAD_LENGTH),
            privileged_pad: generate_random_bytes(PRIVILEGED_PAD_LENGTH),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Creates a profile from a name and existing primary key.
    pub fn from_primary_key(name: String, primary_key: &[u8], derived_key: &[u8]) -> Self {
        // primary_pad = primary_key XOR derived_key
        let primary_pad: Vec<u8> = primary_key
            .iter()
            .zip(derived_key.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        Self {
            name,
            id: generate_random_bytes(PROFILE_ID_LENGTH),
            primary_pad,
            privileged_pad: generate_random_bytes(PRIVILEGED_PAD_LENGTH),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Derives the primary key from the profile pad and derived password key.
    pub fn derive_primary_key(&self, derived_key: &[u8]) -> Vec<u8> {
        // primary_key = primary_pad XOR derived_key
        self.primary_pad
            .iter()
            .zip(derived_key.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }
}

/// Base64 encoding helper for serde.
mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        STANDARD
            .decode(&s)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

/// Base64 encoding helper for serde with default support (for optional/new fields).
mod base64_bytes_default {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        if bytes.is_empty() {
            serializer.serialize_str("")
        } else {
            serializer.serialize_str(&STANDARD.encode(bytes))
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(Vec::new())
        } else {
            STANDARD
                .decode(&s)
                .map_err(|e| serde::de::Error::custom(e.to_string()))
        }
    }
}

/// Universal Message Protocol token for cross-device wallet transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UmpToken {
    /// Token format version.
    pub version: u32,
    /// Encrypted root key material.
    pub key_encrypted: Vec<u8>,
    /// Encrypted profiles data.
    pub profiles_encrypted: Vec<u8>,
}

impl UmpToken {
    /// Create a new UMP token (version 1).
    pub fn new(key_encrypted: Vec<u8>, profiles_encrypted: Vec<u8>) -> Self {
        Self {
            version: 1,
            key_encrypted,
            profiles_encrypted,
        }
    }
}

/// Wallet snapshot for persistence and recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletSnapshot {
    /// Snapshot format version (currently 2).
    pub version: u8,
    /// Encrypted snapshot key material.
    pub snapshot_key: Vec<u8>,
    /// Active profile identifier.
    pub active_profile_id: String,
    /// Encrypted payload containing wallet state.
    pub encrypted_payload: Vec<u8>,
}

impl WalletSnapshot {
    /// Current snapshot version.
    pub const CURRENT_VERSION: u8 = 2;

    /// Create a new V2 snapshot.
    pub fn new(
        snapshot_key: Vec<u8>,
        active_profile_id: String,
        encrypted_payload: Vec<u8>,
    ) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            snapshot_key,
            active_profile_id,
            encrypted_payload,
        }
    }
}

/// Type alias for the wallet builder function.
pub type CWIWalletBuilder = Arc<
    dyn Fn(
            Vec<u8>,
            PrivateKey,
        )
            -> Pin<Box<dyn Future<Output = Result<Arc<dyn WalletInterface + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;

/// CWI-style wallet manager with multi-profile support.
///
/// # Features
///
/// - Multiple profiles per wallet
/// - PBKDF2 password-based key derivation
/// - Profile import/export with encryption
/// - Password-protected profile switching
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::managers::{CWIStyleWalletManager, CWIStyleWalletManagerConfig};
///
/// let manager = CWIStyleWalletManager::new(
///     "admin.wallet".to_string(),
///     wallet_builder,
///     CWIStyleWalletManagerConfig::default(),
/// );
///
/// // Provide privileged key manager first
/// manager.provide_privileged_key_manager(pkm_key).await;
///
/// // Create a new profile
/// let profile = manager.create_profile("Work", "password123").await?;
///
/// // Switch to the profile
/// manager.switch_profile(&profile.id, "password123").await?;
///
/// // Now authenticated - access the wallet
/// let wallet = manager.wallet().await?;
/// let result = wallet.create_action(args, "app.example.com").await?;
/// ```
pub struct CWIStyleWalletManager {
    /// Configuration.
    config: CWIStyleWalletManagerConfig,
    /// All profiles by ID.
    profiles: RwLock<HashMap<Vec<u8>, Profile>>,
    /// Default profile ID.
    default_profile_id: RwLock<Option<Vec<u8>>>,
    /// Currently active profile ID.
    active_profile_id: RwLock<Option<Vec<u8>>>,
    /// Admin originator.
    admin_originator: String,
    /// Wallet builder function.
    wallet_builder: CWIWalletBuilder,
    /// Underlying wallet (built after authentication).
    underlying: RwLock<Option<Arc<dyn WalletInterface + Send + Sync>>>,
    /// Privileged key manager (provided separately).
    privileged_key_manager: RwLock<Option<PrivateKey>>,
}

impl CWIStyleWalletManager {
    /// Creates a new CWI-style wallet manager.
    pub fn new(
        admin_originator: String,
        wallet_builder: CWIWalletBuilder,
        config: CWIStyleWalletManagerConfig,
    ) -> Self {
        Self {
            config,
            profiles: RwLock::new(HashMap::new()),
            default_profile_id: RwLock::new(None),
            active_profile_id: RwLock::new(None),
            admin_originator,
            wallet_builder,
            underlying: RwLock::new(None),
            privileged_key_manager: RwLock::new(None),
        }
    }

    /// Provides the privileged key manager.
    pub async fn provide_privileged_key_manager(&self, manager: PrivateKey) {
        *self.privileged_key_manager.write().await = Some(manager);
    }

    /// Returns whether there is an active authenticated profile.
    pub async fn is_authenticated(&self) -> bool {
        self.underlying.read().await.is_some()
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

    /// Derives a key from password using PBKDF2.
    fn derive_key_from_password(&self, password: &str, salt: &[u8]) -> Vec<u8> {
        let mut derived = vec![0u8; 32];

        if self.config.use_pbkdf2 {
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA512,
                std::num::NonZeroU32::new(self.config.password_rounds).unwrap(),
                salt,
                password.as_bytes(),
                &mut derived,
            );
        } else {
            // Simple fallback: just hash the password (not secure, for testing only)
            use ring::digest::{digest, SHA256};
            let hash = digest(&SHA256, password.as_bytes());
            derived.copy_from_slice(&hash.as_ref()[..32]);
        }

        derived
    }

    /// Creates a new profile.
    ///
    /// The profile is created with a random ID and primary pad. The pad is
    /// designed so that XORing it with the derived password key produces the
    /// root wallet key.
    pub async fn create_profile(&self, name: &str, password: &str) -> Result<Profile> {
        // Generate a random primary key first
        let primary_key = generate_random_bytes(32);

        // Create salt from profile id (will be generated)
        let profile_id = generate_random_bytes(PROFILE_ID_LENGTH);

        // Derive key from password
        let derived_key = self.derive_key_from_password(password, &profile_id);

        // Create profile with computed pad
        let mut profile = Profile::from_primary_key(name.to_string(), &primary_key, &derived_key);
        profile.id = profile_id;

        // Store profile
        let mut profiles = self.profiles.write().await;
        profiles.insert(profile.id.clone(), profile.clone());

        // Set as default if first profile
        if profiles.len() == 1 {
            *self.default_profile_id.write().await = Some(profile.id.clone());
        }

        Ok(profile)
    }

    /// Switches to a profile by ID.
    ///
    /// Requires the correct password to derive the primary key.
    pub async fn switch_profile(&self, profile_id: &[u8], password: &str) -> Result<()> {
        let profiles = self.profiles.read().await;
        let profile = profiles
            .get(profile_id)
            .ok_or_else(|| Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            })?
            .clone();
        drop(profiles);

        // Derive key from password using profile ID as salt
        let derived_key = self.derive_key_from_password(password, &profile.id);

        // Get primary key
        let primary_key = profile.derive_primary_key(&derived_key);

        // Get PKM
        let pkm = self
            .privileged_key_manager
            .read()
            .await
            .clone()
            .ok_or(Error::AuthenticationRequired)?;

        // Build wallet
        let wallet = (self.wallet_builder)(primary_key, pkm).await?;

        *self.underlying.write().await = Some(wallet);
        *self.active_profile_id.write().await = Some(profile_id.to_vec());

        Ok(())
    }

    /// Deletes a profile by ID.
    pub async fn delete_profile(&self, profile_id: &[u8]) -> Result<()> {
        let mut profiles = self.profiles.write().await;

        if !profiles.contains_key(profile_id) {
            return Err(Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            });
        }

        // Can't delete the active profile
        if self.active_profile_id.read().await.as_deref() == Some(profile_id) {
            return Err(Error::InvalidOperation(
                "Cannot delete the active profile".to_string(),
            ));
        }

        profiles.remove(profile_id);

        // Update default if needed
        if self.default_profile_id.read().await.as_deref() == Some(profile_id) {
            let new_default = profiles.keys().next().cloned();
            *self.default_profile_id.write().await = new_default;
        }

        Ok(())
    }

    /// Gets all profiles.
    pub async fn get_profiles(&self) -> Vec<Profile> {
        self.profiles.read().await.values().cloned().collect()
    }

    /// Gets the active profile ID.
    pub async fn get_active_profile_id(&self) -> Option<Vec<u8>> {
        self.active_profile_id.read().await.clone()
    }

    /// Gets the default profile ID.
    pub async fn get_default_profile_id(&self) -> Option<Vec<u8>> {
        self.default_profile_id.read().await.clone()
    }

    /// Sets the default profile ID.
    pub async fn set_default_profile_id(&self, profile_id: Vec<u8>) -> Result<()> {
        let profiles = self.profiles.read().await;
        if !profiles.contains_key(&profile_id) {
            return Err(Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            });
        }
        drop(profiles);

        *self.default_profile_id.write().await = Some(profile_id);
        Ok(())
    }

    /// Exports a profile as encrypted bytes.
    pub async fn export_profile(&self, profile_id: &[u8]) -> Result<Vec<u8>> {
        let profiles = self.profiles.read().await;
        let profile = profiles.get(profile_id).ok_or_else(|| Error::NotFound {
            entity: "Profile".to_string(),
            id: hex::encode(profile_id),
        })?;

        let json = serde_json::to_vec(profile).map_err(Error::JsonError)?;

        // Encrypt with random key (key is prepended to output)
        let key = generate_random_bytes(32);
        let sym_key = SymmetricKey::from_bytes(&key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let encrypted = sym_key
            .encrypt(&json)
            .map_err(|e| Error::InvalidOperation(format!("Encryption failed: {:?}", e)))?;

        let mut result = key;
        result.extend_from_slice(&encrypted);
        Ok(result)
    }

    /// Imports a profile from encrypted bytes.
    pub async fn import_profile(&self, data: &[u8], _password: &str) -> Result<Profile> {
        if data.len() < 33 {
            return Err(Error::InvalidArgument("Export data too short".to_string()));
        }

        let key = &data[0..32];
        let encrypted = &data[32..];

        let sym_key = SymmetricKey::from_bytes(key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let decrypted = sym_key
            .decrypt(encrypted)
            .map_err(|e| Error::InvalidOperation(format!("Decryption failed: {:?}", e)))?;

        let profile: Profile = serde_json::from_slice(&decrypted).map_err(Error::JsonError)?;

        // Store the imported profile
        let mut profiles = self.profiles.write().await;
        profiles.insert(profile.id.clone(), profile.clone());

        Ok(profile)
    }

    /// Exports a single profile to JSON bytes (unencrypted).
    ///
    /// This serializes the profile data to a JSON byte array that can be
    /// stored or transmitted. Unlike `export_profile()`, this does not encrypt
    /// the data, making it suitable for internal backup operations.
    pub async fn export_profile_json(&self, profile_id: &[u8]) -> Result<Vec<u8>> {
        let profiles = self.profiles.read().await;
        let profile = profiles.get(profile_id).ok_or_else(|| Error::NotFound {
            entity: "Profile".to_string(),
            id: hex::encode(profile_id),
        })?;

        serde_json::to_vec(profile).map_err(Error::JsonError)
    }

    /// Imports a profile from JSON bytes (unencrypted).
    ///
    /// Deserializes a profile from a JSON byte array and stores it in the
    /// manager. If a profile with the same ID already exists, it will be
    /// overwritten.
    pub async fn import_profile_json(&self, data: &[u8]) -> Result<Profile> {
        let profile: Profile = serde_json::from_slice(data).map_err(Error::JsonError)?;

        let mut profiles = self.profiles.write().await;
        profiles.insert(profile.id.clone(), profile.clone());

        Ok(profile)
    }

    /// Backs up all profiles to a JSON byte array.
    ///
    /// Serializes all profiles to a JSON array. This is an unencrypted backup
    /// suitable for internal storage. For encrypted backups, use `save_snapshot()`.
    pub async fn backup_all_profiles(&self) -> Result<Vec<u8>> {
        let profiles = self.profiles.read().await;
        let all_profiles: Vec<&Profile> = profiles.values().collect();
        serde_json::to_vec(&all_profiles).map_err(Error::JsonError)
    }

    /// Restores profiles from a JSON byte array backup.
    ///
    /// Deserializes profiles from a JSON array and adds them to the manager.
    /// Existing profiles with the same IDs will be overwritten.
    pub async fn restore_all_profiles(&self, data: &[u8]) -> Result<Vec<Profile>> {
        let restored: Vec<Profile> = serde_json::from_slice(data).map_err(Error::JsonError)?;

        let mut profiles = self.profiles.write().await;
        for profile in &restored {
            profiles.insert(profile.id.clone(), profile.clone());
        }

        Ok(restored)
    }

    /// Destroys the manager state, returning to unauthenticated.
    pub async fn destroy(&self) {
        *self.underlying.write().await = None;
        *self.active_profile_id.write().await = None;
        *self.privileged_key_manager.write().await = None;
    }

    /// Saves profiles state to an encrypted snapshot.
    pub async fn save_snapshot(&self) -> Result<Vec<u8>> {
        let profiles = self.profiles.read().await;
        let default_id = self.default_profile_id.read().await;

        #[derive(Serialize)]
        struct Snapshot {
            profiles: Vec<Profile>,
            default_profile_id: Option<Vec<u8>>,
        }

        let snapshot = Snapshot {
            profiles: profiles.values().cloned().collect(),
            default_profile_id: default_id.clone(),
        };

        let json = serde_json::to_vec(&snapshot).map_err(Error::JsonError)?;

        // Encrypt with random key
        let key = generate_random_bytes(32);
        let sym_key = SymmetricKey::from_bytes(&key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let encrypted = sym_key
            .encrypt(&json)
            .map_err(|e| Error::InvalidOperation(format!("Encryption failed: {:?}", e)))?;

        let mut result = key;
        result.extend_from_slice(&encrypted);
        Ok(result)
    }

    /// Loads profiles state from an encrypted snapshot.
    pub async fn load_snapshot(&self, data: &[u8]) -> Result<()> {
        if data.len() < 33 {
            return Err(Error::InvalidArgument("Snapshot too short".to_string()));
        }

        let key = &data[0..32];
        let encrypted = &data[32..];

        let sym_key = SymmetricKey::from_bytes(key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let decrypted = sym_key
            .decrypt(encrypted)
            .map_err(|e| Error::InvalidOperation(format!("Decryption failed: {:?}", e)))?;

        #[derive(Deserialize)]
        struct Snapshot {
            profiles: Vec<Profile>,
            default_profile_id: Option<Vec<u8>>,
        }

        let snapshot: Snapshot = serde_json::from_slice(&decrypted).map_err(Error::JsonError)?;

        let mut profiles = self.profiles.write().await;
        profiles.clear();
        for profile in snapshot.profiles {
            profiles.insert(profile.id.clone(), profile);
        }

        *self.default_profile_id.write().await = snapshot.default_profile_id;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_new() {
        let profile = Profile::new("Test".to_string());
        assert_eq!(profile.name, "Test");
        assert_eq!(profile.id.len(), PROFILE_ID_LENGTH);
        assert_eq!(profile.primary_pad.len(), PRIMARY_PAD_LENGTH);
        assert_eq!(profile.privileged_pad.len(), PRIVILEGED_PAD_LENGTH);
    }

    #[test]
    fn test_profile_derive_primary_key() {
        let profile = Profile::new("Test".to_string());
        let derived_key = vec![0x55u8; 32];
        let primary_key = profile.derive_primary_key(&derived_key);

        // primary_key = primary_pad XOR derived_key
        for i in 0..32 {
            assert_eq!(primary_key[i], profile.primary_pad[i] ^ derived_key[i]);
        }
    }

    #[test]
    fn test_profile_roundtrip() {
        let primary_key = vec![0x42u8; 32];
        let derived_key = vec![0x55u8; 32];

        let profile = Profile::from_primary_key("Test".to_string(), &primary_key, &derived_key);
        let recovered = profile.derive_primary_key(&derived_key);

        assert_eq!(primary_key, recovered);
    }

    #[test]
    fn test_profile_serialization() {
        let profile = Profile::new("Test".to_string());
        let json = serde_json::to_string(&profile).unwrap();
        let parsed: Profile = serde_json::from_str(&json).unwrap();

        assert_eq!(profile.name, parsed.name);
        assert_eq!(profile.id, parsed.id);
        assert_eq!(profile.primary_pad, parsed.primary_pad);
        assert_eq!(profile.privileged_pad, parsed.privileged_pad);
    }

    #[test]
    fn test_profile_backward_compat_deserialization() {
        // Simulate old format without privileged_pad
        let json = r#"{"name":"Test","id":"AAAAAAAAAAAAAAAAAAAAAA==","primaryPad":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"}"#;
        let parsed: Profile = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.name, "Test");
        // privileged_pad should default to empty vec
        assert!(parsed.privileged_pad.is_empty());
    }

    #[test]
    fn test_ump_token() {
        let token = UmpToken::new(vec![1, 2, 3], vec![4, 5, 6]);
        assert_eq!(token.version, 1);
        assert_eq!(token.key_encrypted, vec![1, 2, 3]);
        assert_eq!(token.profiles_encrypted, vec![4, 5, 6]);

        let json = serde_json::to_string(&token).unwrap();
        let parsed: UmpToken = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, token.version);
    }

    #[test]
    fn test_wallet_snapshot() {
        let snapshot = WalletSnapshot::new(vec![0xAA; 32], "profile-1".to_string(), vec![0xBB; 64]);
        assert_eq!(snapshot.version, WalletSnapshot::CURRENT_VERSION);
        assert_eq!(snapshot.version, 2);
        assert_eq!(snapshot.active_profile_id, "profile-1");

        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: WalletSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.active_profile_id, "profile-1");
    }

    #[test]
    fn test_config_default() {
        let config = CWIStyleWalletManagerConfig::default();
        assert_eq!(config.password_rounds, DEFAULT_PASSWORD_ROUNDS);
        assert!(config.use_pbkdf2);
    }
}
