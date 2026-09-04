//! Blockchain services layer for BSV wallet toolbox.
//!
//! This module provides service providers for interacting with blockchain APIs:
//! - WhatsOnChain - UTXO status, raw transactions, merkle proofs
//! - ARC (TAAL, GorillaPool) - Transaction broadcasting with BEEF
//! - Arcade V2 - Teranode broadcaster; when configured it is also the FIRST
//!   merkle path and batch status provider, so a wallet reads its own proofs
//!   from its own broadcaster instead of a third-party indexer
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
//! use bsv_wallet_toolbox_rs::services::{Services, ServicesOptions, Chain};
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

pub mod broadcast_memory;
pub mod collection;
pub mod mock;
pub mod providers;
#[allow(clippy::module_inception)]
pub mod services;
pub mod traits;

pub use broadcast_memory::{
    apply_sticky_provider_order, is_global_provider, ladder_step, oldest_stale_ancestor,
    seen_set_from_records, unproven_ancestors_in_beef, BroadcastMemory, BroadcastSeenRecord,
    BroadcastStatus, InMemoryBroadcastMemory, LadderStep, BROADCAST_PROVIDER_CHAIN,
    BROADCAST_PROVIDER_NETWORK, BROADCAST_SEEN_STALE_SECS, BROADCAST_STATUS_ACCEPTED,
    BROADCAST_STATUS_MINED, BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_SEEN,
    BROADCAST_STATUS_UNKNOWN, PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_ARCADE_V2, PROVIDER_BITAILS,
    PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC, PROVIDER_WHATSONCHAIN,
};

// Re-export Chain from chaintracks for convenience
pub use crate::chaintracks::Chain;

// Re-export main types
pub use traits::{
    BlockHeader, BsvExchangeRate, FiatCurrency, FiatExchangeRates, GetBeefResult,
    GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult, GetStatusForTxidsResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefDelivery,
    PostBeefResult, PostTxResultForTxid, ScriptHistoryItem, ServicesCallHistory, TxStatusDetail,
    UtxoDetail, WalletServices,
};

pub use collection::{
    AdaptiveTimeoutConfig, ProviderCallHistory, ServiceCall, ServiceCallHistory, ServiceCollection,
    ServiceToCall,
};
pub use providers::{
    arcade_status_rank, beef_to_ef_batch, beef_to_ef_batch_skipping, is_fatal_status,
    missing_parent_hint, Arc, ArcConfig, Arcade, ArcadeConfig, ArcadeSseClient, ArcadeStatusEvent,
    ArcadeTxInfo, BhsConfig, Bitails, BitailsConfig, BlockHeaderService, ChaintracksConfig,
    ChaintracksServiceClient, EfBatch, EfBatchEntry, FallbackChainTracker, SseEvent,
    SseFrameParser, WhatsOnChain, WhatsOnChainConfig, ARCADE_STATUS_CONCURRENCY, ARCADE_V2_MAINNET,
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

    /// Treat `arc_url` as an **Arcade V2** endpoint (EF-only, always-async).
    ///
    /// This is an EXPLICIT flag — the toolbox never guesses Arcade mode from
    /// URL substrings. When `true`:
    /// - An [`Arcade`] broadcaster is registered as the FIRST postBeef
    ///   provider (name `ArcadeV2`), converting BEEF ancestry to EF batches.
    /// - The classic TAAL ARC provider falls back to its chain default URL
    ///   (since `arc_url` now points at Arcade) and remains as failover.
    /// - [`Services::arcade`] is populated so callers (e.g. the Monitor's
    ///   `ArcadeEventsTask`) can reach the SSE stream / callback token.
    ///
    /// Set via [`ServicesOptions::with_arcade`].
    pub arcade_v2: bool,

    /// Arcade V2 configuration (callback token/url, validation skips).
    pub arcade_config: Option<ArcadeConfig>,

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
            arcade_v2: false,
            arcade_config: None,
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

    /// Use an **Arcade V2** endpoint as the primary broadcaster.
    ///
    /// Sets `arc_url` to the Arcade endpoint and flips `arcade_v2 = true`
    /// (explicit opt-in — never inferred from the URL). See
    /// [`ServicesOptions::arcade_v2`] for the resulting provider wiring.
    ///
    /// ```rust,ignore
    /// let opts = ServicesOptions::mainnet().with_arcade(
    ///     bsv_wallet_toolbox_rs::services::ARCADE_V2_MAINNET,
    ///     Some(ArcadeConfig::with_callback_token("my-32-hex-token")),
    /// );
    /// ```
    pub fn with_arcade(mut self, url: impl Into<String>, config: Option<ArcadeConfig>) -> Self {
        self.arc_url = url.into();
        self.arcade_v2 = true;
        self.arcade_config = config;
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
