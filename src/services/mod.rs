//! Blockchain services layer for BSV wallet toolbox.
//!
//! This module provides service providers for interacting with blockchain APIs:
//! - WhatsOnChain - UTXO status, raw transactions, merkle proofs
//! - ARC (TAAL, GorillaPool) - Transaction broadcasting with BEEF
//! - Bitails - Alternative merkle proof provider
//!
//! # Architecture
//!
//! The services layer uses a collection-based failover pattern:
//! - `ServiceCollection` maintains ordered lists of providers for each method
//! - Providers are tried in order until one succeeds
//! - Call history is tracked for diagnostics
//!
//! # Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::services::{Services, ServicesOptions, Chain};
//!
//! // Create mainnet services with defaults
//! let services = Services::new(Chain::Main, ServicesOptions::default());
//!
//! // Get raw transaction
//! let raw_tx = services.get_raw_tx("txid...").await?;
//!
//! // Post BEEF transaction
//! let result = services.post_beef(&beef_bytes, &["txid..."]).await?;
//! ```

pub mod collection;
pub mod mock;
pub mod providers;
#[allow(clippy::module_inception)]
pub mod services;
pub mod traits;

// Re-export Chain from chaintracks for convenience
pub use crate::chaintracks::Chain;

// Re-export main types
pub use traits::{
    BlockHeader, BsvExchangeRate, FiatCurrency, FiatExchangeRates, GetBeefResult,
    GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult, GetStatusForTxidsResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult,
    PostTxResultForTxid, ScriptHistoryItem, ServicesCallHistory, TxStatusDetail, UtxoDetail,
    WalletServices,
};

pub use collection::{
    AdaptiveTimeoutConfig, ProviderCallHistory, ServiceCall, ServiceCallHistory, ServiceCollection,
    ServiceToCall,
};
pub use providers::{
    Arc, ArcConfig, BhsConfig, Bitails, BitailsConfig, BlockHeaderService, ChaintracksConfig,
    ChaintracksServiceClient, WhatsOnChain, WhatsOnChainConfig,
};
pub use services::Services;

/// Configuration options for wallet services.
#[derive(Debug, Clone)]
pub struct ServicesOptions {
    /// WhatsOnChain API key (optional)
    pub whatsonchain_api_key: Option<String>,

    /// Bitails API key (optional)
    pub bitails_api_key: Option<String>,

    /// ARC URL for TAAL
    pub arc_url: String,

    /// ARC configuration for TAAL
    pub arc_config: Option<ArcConfig>,

    /// ARC URL for GorillaPool (optional)
    pub arc_gorillapool_url: Option<String>,

    /// ARC configuration for GorillaPool
    pub arc_gorillapool_config: Option<ArcConfig>,

    /// Block Header Service URL (optional)
    pub bhs_url: Option<String>,

    /// Block Header Service API key (optional)
    pub bhs_api_key: Option<String>,

    /// Chaintracks URL (optional) — e.g. `https://api.calhouninfra.com`
    pub chaintracks_url: Option<String>,

    /// BSV exchange rate cache duration in milliseconds
    pub bsv_update_msecs: u64,

    /// Fiat exchange rate cache duration in milliseconds
    pub fiat_update_msecs: u64,

    /// Initial fiat exchange rates
    pub fiat_exchange_rates: FiatExchangeRates,

    /// Adaptive timeout configuration for service collections
    pub timeout_config: AdaptiveTimeoutConfig,
}

impl Default for ServicesOptions {
    fn default() -> Self {
        Self {
            whatsonchain_api_key: None,
            bitails_api_key: None,
            arc_url: "https://arc.taal.com".to_string(),
            arc_config: None,
            arc_gorillapool_url: Some("https://arc.gorillapool.io".to_string()),
            arc_gorillapool_config: None,
            bhs_url: None,
            bhs_api_key: None,
            chaintracks_url: None,
            bsv_update_msecs: 15 * 60 * 1000,       // 15 minutes
            fiat_update_msecs: 24 * 60 * 60 * 1000, // 24 hours (fiat rates change less frequently)
            fiat_exchange_rates: FiatExchangeRates::default(),
            timeout_config: AdaptiveTimeoutConfig::default(),
        }
    }
}

impl ServicesOptions {
    /// Create options for mainnet with defaults.
    pub fn mainnet() -> Self {
        Self::default()
    }

    /// Create options for testnet.
    pub fn testnet() -> Self {
        Self {
            arc_url: "https://arc-test.taal.com".to_string(),
            arc_gorillapool_url: None, // GorillaPool testnet not commonly used
            ..Default::default()
        }
    }

    /// Set WhatsOnChain API key.
    pub fn with_woc_api_key(mut self, key: impl Into<String>) -> Self {
        self.whatsonchain_api_key = Some(key.into());
        self
    }

    /// Set Bitails API key.
    pub fn with_bitails_api_key(mut self, key: impl Into<String>) -> Self {
        self.bitails_api_key = Some(key.into());
        self
    }

    /// Set ARC URL and config.
    pub fn with_arc(mut self, url: impl Into<String>, config: Option<ArcConfig>) -> Self {
        self.arc_url = url.into();
        self.arc_config = config;
        self
    }

    /// Set GorillaPool ARC URL and config.
    pub fn with_gorillapool(mut self, url: impl Into<String>, config: Option<ArcConfig>) -> Self {
        self.arc_gorillapool_url = Some(url.into());
        self.arc_gorillapool_config = config;
        self
    }

    /// Set Block Header Service URL.
    pub fn with_bhs_url(mut self, url: impl Into<String>) -> Self {
        self.bhs_url = Some(url.into());
        self
    }

    /// Set Block Header Service API key.
    pub fn with_bhs_api_key(mut self, key: impl Into<String>) -> Self {
        self.bhs_api_key = Some(key.into());
        self
    }

    /// Set Block Header Service URL and API key.
    pub fn with_bhs(mut self, url: impl Into<String>, api_key: Option<String>) -> Self {
        self.bhs_url = Some(url.into());
        self.bhs_api_key = api_key;
        self
    }

    /// Set Chaintracks URL for block header lookups.
    pub fn with_chaintracks_url(mut self, url: impl Into<String>) -> Self {
        self.chaintracks_url = Some(url.into());
        self
    }

    /// Set adaptive timeout configuration.
    pub fn with_timeout_config(mut self, config: AdaptiveTimeoutConfig) -> Self {
        self.timeout_config = config;
        self
    }
}
