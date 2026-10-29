//! Wallet Settings Manager
//!
//! Manages wallet settings including trust settings, theme preferences,
//! currency, and permission mode. Provides in-memory storage with
//! serialization support for persistence.

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::services::Chain;

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
    /// Trust contribution from 1 to 10, counted once per discovered subject.
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
    /// Minimum combined trust from 1 to 2560 for a discovered subject.
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
                    description: "Registry of protocols, baskets, certificate types".to_string(),
                    icon_url: Some("https://bsvblockchain.org/favicon.ico".to_string()),
                    identity_key:
                        "03daf815fe38f83da0ad83b5bedc520aa488aef5cbc93a93c67a7fe60406cbffe8"
                            .to_string(),
                    trust: 4,
                    base_url: None,
                },
                Certifier {
                    name: "SocialCert".to_string(),
                    description: "Certifies social media handles, phones and emails".to_string(),
                    icon_url: Some("https://socialcert.net/favicon.ico".to_string()),
                    trust: 3,
                    identity_key:
                        "02cf6cdf466951d8dfc9e7c9367511d0007ed6fba35ed42d425cc412fd6cfd4a17"
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
        // TS maps by name: only SocialCert matches, and its key is unchanged.
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

/// The Go chain selection with Regtest using this port's test defaults.
impl WalletSettings {
    pub(crate) fn default_settings_for(chain: Chain) -> &'static Self {
        match chain {
            Chain::Main => &DEFAULT_SETTINGS,
            Chain::Test | Chain::Regtest => &TESTNET_DEFAULT_SETTINGS,
        }
    }
}

fn invalid_settings(message: impl Into<String>) -> Error {
    Error::ValidationError(format!("Invalid wallet settings: {}", message.into()))
}

fn bounded_string(value: &str, field: &str, minimum: usize, maximum: usize) -> Result<()> {
    if !(minimum..=maximum).contains(&value.len()) {
        return Err(invalid_settings(format!(
            "{field} must contain {minimum}-{maximum} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn settings_url(value: &str, field: &str) -> Result<String> {
    bounded_string(value, field, 1, 500)?;
    let url = reqwest::Url::parse(value)
        .map_err(|_| invalid_settings(format!("{field} must be an absolute HTTP(S) URL")))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid_settings(format!(
            "{field} must be a credential-free HTTP(S) URL"
        )));
    }
    Ok(url.to_string())
}

/// TS `WalletSettingsManager.ts:98-136`: validate before any trust is used.
impl TrustSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_trust_settings(self)
    }
}

fn validate_trust_settings(settings: &TrustSettings) -> Result<()> {
    if !(1..=2560).contains(&settings.trust_level) {
        return Err(invalid_settings(
            "trustLevel must be an integer from 1 to 2560",
        ));
    }
    if settings.trusted_certifiers.len() > 256 {
        return Err(invalid_settings(
            "trustedCertifiers must contain at most 256 entries",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (index, certifier) in settings.trusted_certifiers.iter().enumerate() {
        let field = format!("trustedCertifiers[{index}]");
        let key = &certifier.identity_key;
        bounded_string(key, &format!("{field}.identityKey"), 66, 66)?;
        let canonical = bsv_rs::primitives::PublicKey::from_hex(key)
            .is_ok_and(|parsed| parsed.to_hex() == *key);
        if !canonical {
            return Err(invalid_settings(format!(
                "{field}.identityKey must be a compressed public key"
            )));
        }
        if !seen.insert(key) {
            return Err(invalid_settings(
                "trusted certifier identity keys must be unique",
            ));
        }
        if !(1..=10).contains(&certifier.trust) {
            return Err(invalid_settings(format!(
                "{field}.trust must be an integer from 1 to 10"
            )));
        }
        bounded_string(&certifier.name, &format!("{field}.name"), 1, 100)?;
        bounded_string(
            &certifier.description,
            &format!("{field}.description"),
            1,
            500,
        )?;
        for (name, url) in [
            ("iconUrl", &certifier.icon_url),
            ("baseURL", &certifier.base_url),
        ] {
            if let Some(url) = url {
                settings_url(url, &format!("{field}.{name}"))?;
            }
        }
    }
    Ok(())
}

fn validate_settings(mut settings: WalletSettings) -> Result<WalletSettings> {
    settings.trust_settings.validate()?;
    for (index, certifier) in settings
        .trust_settings
        .trusted_certifiers
        .iter_mut()
        .enumerate()
    {
        for (name, url) in [
            ("iconUrl", &mut certifier.icon_url),
            ("baseURL", &mut certifier.base_url),
        ] {
            if let Some(value) = url {
                *value = settings_url(value, &format!("trustedCertifiers[{index}].{name}"))?;
            }
        }
    }
    Ok(settings)
}

/// Configuration for the settings manager.
#[derive(Debug, Clone)]
pub struct WalletSettingsManagerConfig {
    /// Default settings to use when none are stored.
    pub default_settings: WalletSettings,
}

impl Default for WalletSettingsManagerConfig {
    fn default() -> Self {
        Self {
            default_settings: WalletSettings::default_settings_for(Chain::Main).clone(),
        }
    }
}

/// Manages wallet settings in memory with persistence support.
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::managers::{WalletSettingsManager, WalletSettings};
///
/// let manager = WalletSettingsManager::new(None)?;
///
/// // Get current settings (or defaults)
/// let settings = manager.get().await?;
///
/// // Update settings
/// let mut new_settings = settings;
/// new_settings.currency = Some("EUR".to_string());
/// manager.set(new_settings).await?;
///
/// // Serialize for persistence
/// let bytes = manager.save().await?;
///
/// // Later, restore from persisted data
/// manager.load(&bytes).await?;
/// ```
#[derive(Debug)]
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
    ///
    /// # Errors
    /// Returns a validation error when the configured trust settings are invalid.
    pub fn new(config: Option<WalletSettingsManagerConfig>) -> Result<Self> {
        let mut config = config.unwrap_or_default();
        config.default_settings = validate_settings(config.default_settings)?;
        Ok(Self {
            settings: RwLock::new(config.default_settings.clone()),
            config,
        })
    }

    /// Gets the current wallet settings.
    ///
    /// # Errors
    /// This in-memory implementation always returns `Ok`.
    pub async fn get(&self) -> Result<WalletSettings> {
        Ok(self.settings.read().await.clone())
    }

    /// Sets the wallet settings.
    ///
    /// # Errors
    /// Invalid trust settings return a validation error without changing state.
    pub async fn set(&self, settings: WalletSettings) -> Result<()> {
        let settings = validate_settings(settings)?;
        *self.settings.write().await = settings;
        Ok(())
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
        let settings: WalletSettings = serde_json::from_slice(data).map_err(Error::JsonError)?;
        self.set(settings).await
    }

    /// Serializes settings to a JSON string for persistence.
    pub async fn save_to_string(&self) -> Result<String> {
        let settings = self.settings.read().await;
        serde_json::to_string(&*settings).map_err(Error::JsonError)
    }

    /// Loads settings from a JSON string.
    pub async fn load_from_string(&self, json: &str) -> Result<()> {
        let settings: WalletSettings = serde_json::from_str(json).map_err(Error::JsonError)?;
        self.set(settings).await
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
        let manager = WalletSettingsManager::new(None).unwrap();

        let settings = manager.get().await.unwrap();
        assert_eq!(settings.trust_settings.trust_level, 2);

        let mut new_settings = settings;
        new_settings.currency = Some("EUR".to_string());
        manager.set(new_settings.clone()).await.unwrap();

        let retrieved = manager.get().await.unwrap();
        assert_eq!(retrieved.currency, Some("EUR".to_string()));
    }

    #[tokio::test]
    async fn test_manager_save_load() {
        let manager = WalletSettingsManager::new(None).unwrap();

        let mut settings = manager.get().await.unwrap();
        settings.currency = Some("GBP".to_string());
        manager.set(settings).await.unwrap();

        let saved = manager.save().await.unwrap();

        // Create new manager and load
        let manager2 = WalletSettingsManager::new(None).unwrap();
        manager2.load(&saved).await.unwrap();

        let loaded = manager2.get().await.unwrap();
        assert_eq!(loaded.currency, Some("GBP".to_string()));
    }

    #[tokio::test]
    async fn test_manager_reset() {
        let manager = WalletSettingsManager::new(None).unwrap();

        let mut settings = manager.get().await.unwrap();
        settings.currency = Some("JPY".to_string());
        manager.set(settings).await.unwrap();

        manager.reset().await;

        let reset_settings = manager.get().await.unwrap();
        assert_eq!(reset_settings.currency, None);
    }

    #[test]
    fn discovery_defaults_match_typescript_and_select_by_chain() {
        use crate::services::Chain;
        assert!(std::ptr::eq(
            WalletSettings::default_settings_for(Chain::Main),
            &*DEFAULT_SETTINGS
        ));
        for chain in [Chain::Test, Chain::Regtest] {
            assert!(std::ptr::eq(
                WalletSettings::default_settings_for(chain),
                &*TESTNET_DEFAULT_SETTINGS
            ));
        }
        let certifiers = &DEFAULT_SETTINGS.trust_settings.trusted_certifiers;
        assert_eq!(
            certifiers[0].description,
            "Registry of protocols, baskets, certificate types"
        );
        assert_eq!(
            certifiers[1].description,
            "Certifies social media handles, phones and emails"
        );
        assert_eq!((certifiers[0].trust, certifiers[1].trust), (4, 3));
        // Only SocialCert matches the TS mapping, and its mapped key is unchanged.
        assert_eq!(*DEFAULT_SETTINGS, *TESTNET_DEFAULT_SETTINGS);
    }

    fn invalid_trust_settings() -> Vec<(WalletSettings, String)> {
        let mut cases = Vec::new();
        let invalid = |text: &str| format!("Invalid wallet settings: {text}");
        for trust_level in [0, 2561, u32::MAX] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trust_level = trust_level;
            cases.push((
                settings,
                invalid("trustLevel must be an integer from 1 to 2560"),
            ));
        }
        for trust in [0, 11, u32::MAX] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trusted_certifiers[0].trust = trust;
            cases.push((
                settings,
                invalid("trustedCertifiers[0].trust must be an integer from 1 to 10"),
            ));
        }
        for name in [String::new(), "é".repeat(51)] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trusted_certifiers[0].name = name;
            cases.push((
                settings,
                invalid("trustedCertifiers[0].name must contain 1-100 UTF-8 bytes"),
            ));
        }
        for description in [String::new(), "é".repeat(251)] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trusted_certifiers[0].description = description;
            cases.push((
                settings,
                invalid("trustedCertifiers[0].description must contain 1-500 UTF-8 bytes"),
            ));
        }
        for key in [String::new(), "02abcd".into()] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trusted_certifiers[0].identity_key = key;
            cases.push((
                settings,
                invalid("trustedCertifiers[0].identityKey must contain 66-66 UTF-8 bytes"),
            ));
        }
        for key in [
            DEFAULT_SETTINGS.trust_settings.trusted_certifiers[0]
                .identity_key
                .to_uppercase(),
            format!("04{}", "0".repeat(64)),
            format!("02{}", "f".repeat(64)),
        ] {
            let mut settings = DEFAULT_SETTINGS.clone();
            settings.trust_settings.trusted_certifiers[0].identity_key = key;
            cases.push((
                settings,
                invalid("trustedCertifiers[0].identityKey must be a compressed public key"),
            ));
        }
        let mut settings = DEFAULT_SETTINGS.clone();
        settings.trust_settings.trusted_certifiers[1] =
            settings.trust_settings.trusted_certifiers[0].clone();
        cases.push((
            settings,
            invalid("trusted certifier identity keys must be unique"),
        ));
        let mut settings = DEFAULT_SETTINGS.clone();
        settings
            .trust_settings
            .trusted_certifiers
            .resize(257, settings.trust_settings.trusted_certifiers[0].clone());
        cases.push((
            settings,
            invalid("trustedCertifiers must contain at most 256 entries"),
        ));
        for (value, suffix) in [
            ("".to_string(), "must contain 1-500 UTF-8 bytes"),
            ("x".repeat(501), "must contain 1-500 UTF-8 bytes"),
            ("relative/path".into(), "must be an absolute HTTP(S) URL"),
            (
                "ftp://example.invalid/icon".into(),
                "must be a credential-free HTTP(S) URL",
            ),
            (
                "https://user:password@example.invalid/".into(),
                "must be a credential-free HTTP(S) URL",
            ),
        ] {
            for field in ["iconUrl", "baseURL"] {
                let mut settings = DEFAULT_SETTINGS.clone();
                let certifier = &mut settings.trust_settings.trusted_certifiers[0];
                if field == "iconUrl" {
                    certifier.icon_url = Some(value.clone());
                } else {
                    certifier.base_url = Some(value.clone());
                }
                cases.push((
                    settings,
                    invalid(&format!("trustedCertifiers[0].{field} {suffix}")),
                ));
            }
        }
        cases
    }

    #[tokio::test]
    async fn discovery_settings_refuse_invalid_new_set_and_load_without_mutating() {
        let manager: Result<WalletSettingsManager> = WalletSettingsManager::new(None);
        let manager = manager.unwrap();
        let original: Result<WalletSettings> = manager.get().await;
        let original = original.unwrap();
        for (settings, expected) in invalid_trust_settings() {
            let result = WalletSettingsManager::new(Some(WalletSettingsManagerConfig {
                default_settings: settings.clone(),
            }));
            assert!(
                matches!(result, Err(Error::ValidationError(ref text)) if text == &expected),
                "new: {expected}"
            );
            let result: Result<()> = manager.set(settings.clone()).await;
            assert!(
                matches!(result, Err(Error::ValidationError(ref text)) if text == &expected),
                "set: {expected}"
            );
            let bytes = serde_json::to_vec(&settings).unwrap();
            for result in [
                manager.load(&bytes).await,
                manager
                    .load_from_string(std::str::from_utf8(&bytes).unwrap())
                    .await,
            ] {
                assert!(
                    matches!(result, Err(Error::ValidationError(ref text)) if text == &expected),
                    "load: {expected}"
                );
            }
            assert_eq!(
                manager.get().await.unwrap(),
                original,
                "refusal leaves settings intact"
            );
        }
    }

    #[tokio::test]
    async fn discovery_settings_accept_inclusive_bounds_and_normalize_urls() {
        let mut settings = DEFAULT_SETTINGS.clone();
        settings.trust_settings.trust_level = 2560;
        settings.trust_settings.trusted_certifiers.clear();
        for n in 1..=256 {
            let certifier = Certifier {
                identity_key: bsv_rs::primitives::PrivateKey::from_hex(&format!("{n:064x}"))
                    .unwrap()
                    .public_key()
                    .to_hex(),
                name: "é".repeat(50),
                description: "é".repeat(250),
                trust: 10,
                icon_url: Some("https://example.invalid".into()),
                base_url: Some("http://example.invalid".into()),
            };
            settings.trust_settings.trusted_certifiers.push(certifier);
        }
        let manager = WalletSettingsManager::new(Some(WalletSettingsManagerConfig {
            default_settings: settings,
        }))
        .unwrap();
        let mut settings = manager.get().await.unwrap();
        assert_eq!(
            settings.trust_settings.trusted_certifiers[0]
                .icon_url
                .as_deref(),
            Some("https://example.invalid/")
        );
        assert_eq!(
            settings.trust_settings.trusted_certifiers[0]
                .base_url
                .as_deref(),
            Some("http://example.invalid/")
        );
        settings.trust_settings.trust_level = 1;
        settings.trust_settings.trusted_certifiers[0].trust = 1;
        settings.trust_settings.trusted_certifiers[0].name = "a".into();
        settings.trust_settings.trusted_certifiers[0].description = "b".into();
        manager.set(settings).await.unwrap();
        manager.reset().await;
        assert_eq!(
            manager.get().await.unwrap().trust_settings.trust_level,
            2560
        );
    }
}
