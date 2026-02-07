//! Wallet Settings Manager
//!
//! Manages wallet settings including trust settings, theme preferences,
//! currency, and permission mode. Provides in-memory storage with
//! serialization support for persistence.

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// A trusted certifier for identity certificates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Certifier {
    /// Display name of the certifier.
    pub name: String,
    /// Description of what this certifier does.
    pub description: String,
    /// The certifier's identity public key (hex).
    pub identity_key: String,
    /// Trust level (1-4, higher = more trusted).
    pub trust: u32,
    /// Optional icon URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    /// Optional base URL for the certifier service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

/// Trust settings for the wallet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TrustSettings {
    /// Overall trust level for the wallet.
    pub trust_level: u32,
    /// List of trusted certifiers.
    pub trusted_certifiers: Vec<Certifier>,
}

/// Theme settings for the wallet UI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WalletTheme {
    /// Theme mode (e.g., "dark", "light").
    pub mode: String,
}

/// Complete wallet settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WalletSettings {
    /// Trust settings.
    pub trust_settings: TrustSettings,
    /// Optional theme settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme: Option<WalletTheme>,
    /// Optional currency preference (e.g., "USD", "EUR").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    /// Optional permission mode identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
}

/// Default mainnet settings.
pub static DEFAULT_SETTINGS: once_cell::sync::Lazy<WalletSettings> =
    once_cell::sync::Lazy::new(|| WalletSettings {
        trust_settings: TrustSettings {
            trust_level: 2,
            trusted_certifiers: vec![
                Certifier {
                    name: "Metanet Trust Services".to_string(),
                    description: "Registry for protocols, baskets, and certificate types".to_string(),
                    icon_url: Some("https://bsvblockchain.org/favicon.ico".to_string()),
                    identity_key: "03daf815fe38f83da0ad83b5bedc520aa488aef5cbc93a93c67a7fe60406cbffe8"
                        .to_string(),
                    trust: 4,
                    base_url: None,
                },
                Certifier {
                    name: "SocialCert".to_string(),
                    description: "Certifies social media handles, phone numbers and emails"
                        .to_string(),
                    icon_url: Some("https://socialcert.net/favicon.ico".to_string()),
                    trust: 3,
                    identity_key: "02cf6cdf466951d8dfc9e7c9367511d0007ed6fba35ed42d425cc412fd6cfd4a17"
                        .to_string(),
                    base_url: None,
                },
            ],
        },
        theme: Some(WalletTheme {
            mode: "dark".to_string(),
        }),
        currency: None,
        permission_mode: Some("simple".to_string()),
    });

/// Default testnet settings with testnet identity keys.
pub static TESTNET_DEFAULT_SETTINGS: once_cell::sync::Lazy<WalletSettings> =
    once_cell::sync::Lazy::new(|| {
        let mut settings = DEFAULT_SETTINGS.clone();
        // Update certifier identity keys for testnet
        for certifier in &mut settings.trust_settings.trusted_certifiers {
            match certifier.name.as_str() {
                "Babbage Trust Services" => {
                    certifier.identity_key =
                        "03d0b36b5c98b000ec9ffed9a2cf005e279244edf6a19cf90545cdebe873162761"
                            .to_string();
                }
                "IdentiCert" => {
                    certifier.identity_key =
                        "036dc48522aba1705afbb43df3c04dbd1da373b6154341a875bceaa2a3e7f21528"
                            .to_string();
                }
                "SocialCert" => {
                    certifier.identity_key =
                        "02cf6cdf466951d8dfc9e7c9367511d0007ed6fba35ed42d425cc412fd6cfd4a17"
                            .to_string();
                }
                _ => {}
            }
        }
        settings
    });

/// Configuration for the settings manager.
#[derive(Debug, Clone)]
pub struct WalletSettingsManagerConfig {
    /// Default settings to use when none are stored.
    pub default_settings: WalletSettings,
}

impl Default for WalletSettingsManagerConfig {
    fn default() -> Self {
        Self {
            default_settings: DEFAULT_SETTINGS.clone(),
        }
    }
}

/// Manages wallet settings in memory with persistence support.
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox::managers::{WalletSettingsManager, WalletSettings};
///
/// let manager = WalletSettingsManager::new(None);
///
/// // Get current settings (or defaults)
/// let settings = manager.get().await;
///
/// // Update settings
/// let mut new_settings = settings;
/// new_settings.currency = Some("EUR".to_string());
/// manager.set(new_settings).await;
///
/// // Serialize for persistence
/// let bytes = manager.save().await?;
///
/// // Later, restore from persisted data
/// manager.load(&bytes).await?;
/// ```
pub struct WalletSettingsManager {
    /// Current settings.
    settings: RwLock<WalletSettings>,
    /// Configuration including default settings.
    config: WalletSettingsManagerConfig,
}

impl WalletSettingsManager {
    /// Creates a new settings manager.
    ///
    /// # Arguments
    ///
    /// * `config` - Optional configuration (uses defaults if None)
    pub fn new(config: Option<WalletSettingsManagerConfig>) -> Self {
        let config = config.unwrap_or_default();
        Self {
            settings: RwLock::new(config.default_settings.clone()),
            config,
        }
    }

    /// Gets the current wallet settings.
    pub async fn get(&self) -> WalletSettings {
        self.settings.read().await.clone()
    }

    /// Sets the wallet settings.
    pub async fn set(&self, settings: WalletSettings) {
        *self.settings.write().await = settings;
    }

    /// Resets settings to defaults.
    pub async fn reset(&self) {
        *self.settings.write().await = self.config.default_settings.clone();
    }

    /// Serializes settings to JSON bytes for persistence.
    pub async fn save(&self) -> Result<Vec<u8>> {
        let settings = self.settings.read().await;
        serde_json::to_vec(&*settings).map_err(Error::JsonError)
    }

    /// Loads settings from JSON bytes.
    pub async fn load(&self, data: &[u8]) -> Result<()> {
        let settings: WalletSettings =
            serde_json::from_slice(data).map_err(Error::JsonError)?;
        *self.settings.write().await = settings;
        Ok(())
    }

    /// Serializes settings to a JSON string for persistence.
    pub async fn save_to_string(&self) -> Result<String> {
        let settings = self.settings.read().await;
        serde_json::to_string(&*settings).map_err(Error::JsonError)
    }

    /// Loads settings from a JSON string.
    pub async fn load_from_string(&self, json: &str) -> Result<()> {
        let settings: WalletSettings =
            serde_json::from_str(json).map_err(Error::JsonError)?;
        *self.settings.write().await = settings;
        Ok(())
    }

    /// Gets the default settings from configuration.
    pub fn default_settings(&self) -> &WalletSettings {
        &self.config.default_settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = DEFAULT_SETTINGS.clone();
        assert_eq!(settings.trust_settings.trust_level, 2);
        assert_eq!(settings.trust_settings.trusted_certifiers.len(), 2);
        assert_eq!(settings.theme.as_ref().unwrap().mode, "dark");
        assert_eq!(settings.permission_mode, Some("simple".to_string()));
    }

    #[test]
    fn test_testnet_settings() {
        let settings = TESTNET_DEFAULT_SETTINGS.clone();
        assert_eq!(settings.trust_settings.trust_level, 2);
        // SocialCert key should be the same on both networks
        let socialcert = settings
            .trust_settings
            .trusted_certifiers
            .iter()
            .find(|c| c.name == "SocialCert");
        assert!(socialcert.is_some());
    }

    #[test]
    fn test_settings_serialization() {
        let settings = WalletSettings {
            trust_settings: TrustSettings {
                trust_level: 3,
                trusted_certifiers: vec![],
            },
            theme: Some(WalletTheme {
                mode: "light".to_string(),
            }),
            currency: Some("USD".to_string()),
            permission_mode: None,
        };

        let json = serde_json::to_string(&settings).unwrap();
        let parsed: WalletSettings = serde_json::from_str(&json).unwrap();
        assert_eq!(settings, parsed);
    }

    #[test]
    fn test_certifier_serialization() {
        let certifier = Certifier {
            name: "Test".to_string(),
            description: "Test certifier".to_string(),
            identity_key: "02abc123".to_string(),
            trust: 2,
            icon_url: Some("https://example.com/icon.png".to_string()),
            base_url: None,
        };

        let json = serde_json::to_string(&certifier).unwrap();
        assert!(!json.contains("baseUrl")); // None should be skipped
        assert!(json.contains("iconUrl"));

        let parsed: Certifier = serde_json::from_str(&json).unwrap();
        assert_eq!(certifier, parsed);
    }

    #[test]
    fn test_config_default() {
        let config = WalletSettingsManagerConfig::default();
        assert_eq!(config.default_settings.trust_settings.trust_level, 2);
    }

    #[tokio::test]
    async fn test_manager_get_set() {
        let manager = WalletSettingsManager::new(None);

        let settings = manager.get().await;
        assert_eq!(settings.trust_settings.trust_level, 2);

        let mut new_settings = settings;
        new_settings.currency = Some("EUR".to_string());
        manager.set(new_settings.clone()).await;

        let retrieved = manager.get().await;
        assert_eq!(retrieved.currency, Some("EUR".to_string()));
    }

    #[tokio::test]
    async fn test_manager_save_load() {
        let manager = WalletSettingsManager::new(None);

        let mut settings = manager.get().await;
        settings.currency = Some("GBP".to_string());
        manager.set(settings).await;

        let saved = manager.save().await.unwrap();

        // Create new manager and load
        let manager2 = WalletSettingsManager::new(None);
        manager2.load(&saved).await.unwrap();

        let loaded = manager2.get().await;
        assert_eq!(loaded.currency, Some("GBP".to_string()));
    }

    #[tokio::test]
    async fn test_manager_reset() {
        let manager = WalletSettingsManager::new(None);

        let mut settings = manager.get().await;
        settings.currency = Some("JPY".to_string());
        manager.set(settings).await;

        manager.reset().await;

        let reset_settings = manager.get().await;
        assert_eq!(reset_settings.currency, None);
    }
}
