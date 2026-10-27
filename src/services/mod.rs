//! Blockchain services layer for BSV wallet toolbox.
//!
//! This module provides service providers for interacting with blockchain APIs:
//! - WhatsOnChain - UTXO status, raw transactions, merkle proofs
//! - ARC (TAAL, GorillaPool) - Transaction broadcasting with BEEF
//! - Arcade V2 - Teranode broadcaster; when configured it is also the FIRST
//!   merkle path and batch status provider, so a wallet reads its own proofs
//!   from its own broadcaster instead of a third-party indexer
//! - Bitails - Alternative merkle proof provider
//! - BananaBlocks - GorillaPool's explorer, the third rung of the read ladders
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
pub mod cadence;
pub mod collection;
mod http_client;
pub mod mock;
#[cfg(test)]
mod network_isolation_tests;
#[cfg(test)]
mod network_post_tests;
pub mod providers;
#[allow(clippy::module_inception)]
pub mod services;
pub mod traits;

/// A mock server for a test, never waited on. mockito's shared pool
/// (20 servers on macOS) makes a test wait for a free server, and a test
/// that takes two or more one at a time holds some while it waits for the
/// rest: at 16 or more test threads those tests wait on each other for
/// good. These servers are built outside that pool
/// (`Server::new_with_opts_async`) and recycled through a free list of our
/// own, so a test takes a free one or builds one at once; a server's
/// listener thread lives until the test binary exits, and the free list
/// keeps their number at the most ever held at one time.
#[cfg(test)]
pub(crate) async fn mock_server() -> MockServer {
    let recycled = FREE_MOCK_SERVERS.lock().unwrap().pop();
    let server = match recycled {
        Some(server) => server,
        None => mockito::Server::new_with_opts_async(mockito::ServerOpts::default()).await,
    };
    MockServer(Some(server))
}

#[cfg(test)]
static FREE_MOCK_SERVERS: std::sync::Mutex<Vec<mockito::Server>> =
    std::sync::Mutex::new(Vec::new());

/// A [`mock_server`]: dereferences to the server, and goes back to the
/// free list, its mocks cleared, when dropped.
#[cfg(test)]
pub(crate) struct MockServer(Option<mockito::Server>);

#[cfg(test)]
impl std::ops::Deref for MockServer {
    type Target = mockito::Server;

    fn deref(&self) -> &mockito::Server {
        self.0.as_ref().unwrap()
    }
}

#[cfg(test)]
impl std::ops::DerefMut for MockServer {
    fn deref_mut(&mut self) -> &mut mockito::Server {
        self.0.as_mut().unwrap()
    }
}

#[cfg(test)]
impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(mut server) = self.0.take() {
            server.reset();
            FREE_MOCK_SERVERS.lock().unwrap().push(server);
        }
    }
}

pub use broadcast_memory::{
    apply_sticky_provider_order, is_global_provider, ladder_step, oldest_stale_ancestor,
    seen_set_from_records, unproven_ancestors_in_beef, BroadcastMemory, BroadcastSeenRecord,
    BroadcastStatus, InMemoryBroadcastMemory, LadderStep, BROADCAST_PROVIDER_CHAIN,
    BROADCAST_PROVIDER_NETWORK, BROADCAST_SEEN_STALE_SECS, BROADCAST_STATUS_ACCEPTED,
    BROADCAST_STATUS_MINED, BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_SEEN,
    BROADCAST_STATUS_UNKNOWN, PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_ARCADE_V2, PROVIDER_BITAILS,
    PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC, PROVIDER_WHATSONCHAIN,
};

/// Network chain identifier.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    #[default]
    Main,
    Test,
}

impl Chain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Chain::Main => "main",
            Chain::Test => "test",
        }
    }
}

// Re-export main types
pub use traits::{
    BlockHeader, BoundSpender, BsvExchangeRate, FiatCurrency, FiatExchangeRates, GetBeefResult,
    GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult, GetStatusForTxidsResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefDelivery,
    PostBeefResult, PostTxResultForTxid, ScriptHistoryItem, ServicesCallHistory, TxStatusDetail,
    UnspentPages, UtxoDetail, UtxoLead, UtxoLeads, UtxoVerdict, WalletServices, LEAD_PAGES_MAX,
};

pub use collection::{
    AdaptiveTimeoutConfig, ProviderCallHistory, ServiceCall, ServiceCallHistory, ServiceCollection,
    ServiceToCall,
};
pub use providers::{
    arcade_status_rank, beef_to_ef_batch, beef_to_ef_batch_skipping, is_fatal_status,
    missing_parent_hint, Arc, ArcConfig, Arcade, ArcadeConfig, ArcadeSseClient, ArcadeStatusEvent,
    ArcadeTxInfo, BananaBlocks, BhsConfig, Bitails, BitailsConfig, BlockHeaderService,
    ChaintracksConfig, ChaintracksServiceClient, EfBatch, EfBatchEntry, FallbackChainTracker,
    SseEvent, SseFrameParser, WhatsOnChain, WhatsOnChainConfig, ARCADE_STATUS_CONCURRENCY,
    ARCADE_V2_MAINNET,
};
pub use services::Services;

/// Configuration options for wallet services.
#[derive(Debug, Clone)]
pub struct ServicesOptions {
    /// WhatsOnChain API key (optional)
    pub whatsonchain_api_key: Option<String>,

    /// Bitails API key (optional)
    pub bitails_api_key: Option<String>,

    /// BananaBlocks API base: `None` is the chain's default
    /// (`https://bananablocks.com/api/v1` on main,
    /// `https://test.bananablocks.com/api/v1` on test), an empty string
    /// leaves BananaBlocks out of every ladder. No key exists for it.
    ///
    /// Set via [`ServicesOptions::with_bananablocks_base`]; turn it off with
    /// [`ServicesOptions::without_bananablocks`].
    pub bananablocks_base: Option<String>,

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

    /// Break-glass: let the proof path ask explorers for block headers when
    /// the header service gives no answer (the chain tracker asks
    /// WhatsOnChain and Bitails for a merkle root and takes it only when
    /// they agree; `hash_to_header` asks the two, each the other's
    /// fallback). Off by default: no explorer in the proof path (P0-1c,
    /// the owner's rule of 2026-09-15). Every such call is logged at warn
    /// level with the marker `break_glass_explorer_header`. An explorer never
    /// overrules the header service's definite answer.
    pub break_glass_explorer_headers: bool,

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
            bananablocks_base: None,
            arc_url: "https://arc.taal.com".to_string(),
            arc_config: None,
            arc_gorillapool_url: Some("https://arc.gorillapool.io".to_string()),
            arc_gorillapool_config: None,
            arcade_v2: false,
            arcade_config: None,
            bhs_url: None,
            bhs_api_key: None,
            chaintracks_url: None,
            break_glass_explorer_headers: false,
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

    /// Point BananaBlocks at another API base (a mirror, a fixture). An
    /// empty base leaves it out of every ladder.
    pub fn with_bananablocks_base(mut self, url: impl Into<String>) -> Self {
        self.bananablocks_base = Some(url.into());
        self
    }

    /// Leave BananaBlocks out of every ladder (the same as an empty base).
    pub fn without_bananablocks(mut self) -> Self {
        self.bananablocks_base = Some(String::new());
        self
    }

    /// The BananaBlocks API base for `chain`, `None` when it is turned off.
    pub fn bananablocks_base_for(&self, chain: Chain) -> Option<String> {
        match self.bananablocks_base.as_deref() {
            Some(base) if base.trim().is_empty() => None,
            Some(base) => Some(base.trim().to_string()),
            None => Some(
                match chain {
                    Chain::Main => providers::BANANABLOCKS_MAINNET_URL,
                    Chain::Test => providers::BANANABLOCKS_TESTNET_URL,
                }
                .to_string(),
            ),
        }
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

    /// Break-glass: turn the explorer header fallback on or off (see
    /// [`ServicesOptions::break_glass_explorer_headers`]). Off by default.
    pub fn with_break_glass_explorer_headers(mut self, on: bool) -> Self {
        self.break_glass_explorer_headers = on;
        self
    }

    /// Set adaptive timeout configuration.
    pub fn with_timeout_config(mut self, config: AdaptiveTimeoutConfig) -> Self {
        self.timeout_config = config;
        self
    }
}
