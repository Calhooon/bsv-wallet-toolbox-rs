//! Main Services orchestrator.
//!
//! The `Services` struct coordinates multiple service providers with failover
//! support for each method type. It implements the `WalletServices` trait.

use async_trait::async_trait;
use std::sync::{Arc as StdArc, RwLock};

use crate::chaintracks::Chain;
use crate::lock_utils::{lock_read, lock_write};
use crate::services::{
    collection::{ServiceCall, ServiceCollection},
    providers::{
        Arc, BhsConfig, Bitails, BitailsConfig, BlockHeaderService, ChaintracksConfig,
        ChaintracksServiceClient, WhatsOnChain, WhatsOnChainConfig,
    },
    traits::{
        sha256, BlockHeader, BsvExchangeRate, FiatCurrency, FiatExchangeRates, GetBeefResult,
        GetMerklePathResult, GetRawTxResult, GetScriptHashHistoryResult, GetStatusForTxidsResult,
        GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult,
        ServicesCallHistory, WalletServices,
    },
    ServicesOptions,
};
use crate::{Error, Result};
use bsv_rs::transaction::ChainTracker;

/// Post BEEF mode for handling multiple broadcast services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostBeefMode {
    /// Try services until one succeeds (default).
    #[default]
    UntilSuccess,
    /// Post to all services in parallel.
    PromiseAll,
}

/// Main services orchestrator for blockchain operations.
///
/// Coordinates multiple blockchain service providers (WhatsOnChain, ARC, Bitails,
/// Block Header Service) with automatic failover. Each operation type maintains
/// an ordered list of providers via [`ServiceCollection`], which are tried
/// sequentially until one succeeds.
///
/// `Services` implements the [`WalletServices`] trait and is the standard services
/// backend for [`Wallet`](crate::Wallet).
///
/// # Factory Methods
///
/// | Method | Description |
/// |--------|-------------|
/// | [`Services::new`] | Create with chain-appropriate defaults |
/// | [`Services::mainnet`] | Shorthand for mainnet defaults |
/// | [`Services::testnet`] | Shorthand for testnet defaults |
/// | [`Services::with_options`] | Create with custom [`ServicesOptions`] |
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::{Services, ServicesOptions, Chain};
///
/// // Quick mainnet setup
/// let services = Services::mainnet()?;
///
/// // Custom configuration with API keys
/// let options = ServicesOptions::mainnet()
///     .with_woc_api_key("my-key")
///     .with_bhs("https://bhs.babbage.systems", None);
/// let services = Services::with_options(Chain::Main, options)?;
/// ```
pub struct Services {
    /// Network chain.
    pub chain: Chain,

    /// Configuration options.
    pub options: ServicesOptions,

    /// WhatsOnChain provider.
    pub whatsonchain: StdArc<WhatsOnChain>,

    /// TAAL ARC provider.
    pub arc_taal: StdArc<Arc>,

    /// GorillaPool ARC provider (optional).
    pub arc_gorillapool: Option<StdArc<Arc>>,

    /// Bitails provider.
    pub bitails: StdArc<Bitails>,

    /// Block Header Service provider (optional).
    pub bhs: Option<StdArc<BlockHeaderService>>,

    /// Service collection for getMerklePath.
    get_merkle_path_services: RwLock<MerklePathServiceCollection>,

    /// Service collection for getRawTx.
    get_raw_tx_services: RwLock<RawTxServiceCollection>,

    /// Service collection for postBeef.
    post_beef_services: RwLock<PostBeefServiceCollection>,

    /// Service collection for getUtxoStatus.
    get_utxo_status_services: RwLock<UtxoStatusServiceCollection>,

    /// Service collection for getStatusForTxids.
    get_status_for_txids_services: RwLock<StatusForTxidsServiceCollection>,

    /// Service collection for getScriptHashHistory.
    get_script_hash_history_services: RwLock<ScriptHashHistoryServiceCollection>,

    /// Cached BSV exchange rate.
    #[allow(dead_code)]
    bsv_exchange_rate: RwLock<Option<BsvExchangeRate>>,

    /// Cached fiat exchange rates.
    fiat_exchange_rates: RwLock<FiatExchangeRates>,

    /// Chaintracks service client (optional).
    pub chaintracks: Option<StdArc<ChaintracksServiceClient>>,

    /// Post BEEF mode.
    pub post_beef_mode: PostBeefMode,
}

// Type aliases for service collections
type MerklePathServiceCollection = ServiceCollection<MerklePathProvider>;
type RawTxServiceCollection = ServiceCollection<RawTxProvider>;
type PostBeefServiceCollection = ServiceCollection<PostBeefProvider>;
type UtxoStatusServiceCollection = ServiceCollection<UtxoStatusProvider>;
type StatusForTxidsServiceCollection = ServiceCollection<StatusForTxidsProvider>;
type ScriptHashHistoryServiceCollection = ServiceCollection<ScriptHashHistoryProvider>;

// Provider type aliases
type MerklePathProvider = StdArc<dyn MerklePathService + Send + Sync>;
type RawTxProvider = StdArc<dyn RawTxService + Send + Sync>;
type PostBeefProvider = StdArc<dyn PostBeefService + Send + Sync>;
type UtxoStatusProvider = StdArc<dyn UtxoStatusService + Send + Sync>;
type StatusForTxidsProvider = StdArc<dyn StatusForTxidsService + Send + Sync>;
type ScriptHashHistoryProvider = StdArc<dyn ScriptHashHistoryService + Send + Sync>;

// Service traits for each method
#[async_trait]
trait MerklePathService {
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult>;
}

#[async_trait]
trait RawTxService {
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult>;
}

#[async_trait]
trait PostBeefService {
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult>;
}

#[async_trait]
trait UtxoStatusService {
    async fn get_utxo_status(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult>;
}

#[async_trait]
trait StatusForTxidsService {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult>;
}

#[async_trait]
trait ScriptHashHistoryService {
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult>;
}

// Implement service traits for providers

#[async_trait]
impl MerklePathService for WhatsOnChain {
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        self.get_merkle_path(txid).await
    }
}

#[async_trait]
impl MerklePathService for Bitails {
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        self.get_merkle_path(txid).await
    }
}

#[async_trait]
impl MerklePathService for Arc {
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        self.get_merkle_path(txid).await
    }
}

#[async_trait]
impl RawTxService for WhatsOnChain {
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        self.get_raw_tx(txid).await
    }
}

#[async_trait]
impl RawTxService for Bitails {
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        self.get_raw_tx(txid).await
    }
}

#[async_trait]
impl PostBeefService for WhatsOnChain {
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef(beef, txids).await
    }
}

#[async_trait]
impl PostBeefService for Bitails {
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef(beef, txids).await
    }
}

#[async_trait]
impl PostBeefService for Arc {
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef(beef, txids).await
    }
}

#[async_trait]
impl UtxoStatusService for WhatsOnChain {
    async fn get_utxo_status(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult> {
        self.get_utxo_status(output, format, outpoint).await
    }
}

#[async_trait]
impl StatusForTxidsService for WhatsOnChain {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        self.get_status_for_txids(txids).await
    }
}

#[async_trait]
impl StatusForTxidsService for Bitails {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        self.get_status_for_txids(txids).await
    }
}

#[async_trait]
impl ScriptHashHistoryService for WhatsOnChain {
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        self.get_script_hash_history(hash).await
    }
}

#[async_trait]
impl ScriptHashHistoryService for Bitails {
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        self.get_script_hash_history(hash).await
    }
}

impl Services {
    /// Create new services for the given chain with default options.
    pub fn new(chain: Chain) -> Result<Self> {
        let options = match chain {
            Chain::Main => ServicesOptions::mainnet(),
            Chain::Test => ServicesOptions::testnet(),
        };
        Self::with_options(chain, options)
    }

    /// Create new services with custom options.
    pub fn with_options(chain: Chain, options: ServicesOptions) -> Result<Self> {
        // Create providers
        let woc_config = WhatsOnChainConfig {
            api_key: options.whatsonchain_api_key.clone(),
            timeout_secs: None,
        };
        let whatsonchain = StdArc::new(WhatsOnChain::new(chain, woc_config)?);

        let arc_taal = StdArc::new(Arc::new(
            options.arc_url.clone(),
            options.arc_config.clone(),
            Some("arcTaal"),
        )?);

        let arc_gorillapool = if let Some(ref url) = options.arc_gorillapool_url {
            Some(StdArc::new(Arc::new(
                url.clone(),
                options.arc_gorillapool_config.clone(),
                Some("arcGorillaPool"),
            )?))
        } else {
            None
        };

        let bitails_config = BitailsConfig {
            api_key: options.bitails_api_key.clone(),
            timeout_secs: None,
        };
        let bitails = StdArc::new(Bitails::new(chain, bitails_config)?);

        // Create BHS provider if URL is configured
        let bhs = if let Some(ref bhs_url) = options.bhs_url {
            let bhs_config = BhsConfig {
                url: bhs_url.clone(),
                api_key: options.bhs_api_key.clone(),
            };
            Some(StdArc::new(BlockHeaderService::new(bhs_config)))
        } else {
            None
        };

        // Create Chaintracks client if URL is configured
        let chaintracks = if let Some(ref ct_url) = options.chaintracks_url {
            let ct_config = ChaintracksConfig {
                url: ct_url.clone(),
                api_key: None,
            };
            Some(StdArc::new(ChaintracksServiceClient::new(ct_config)))
        } else {
            None
        };

        // Build service collections

        // getMerklePath: WoC, Bitails
        let mut merkle_path_services = ServiceCollection::new("getMerklePath");
        merkle_path_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as MerklePathProvider,
        );
        merkle_path_services.add("Bitails", StdArc::clone(&bitails) as MerklePathProvider);

        // getRawTx: WoC, Bitails
        let mut raw_tx_services = ServiceCollection::new("getRawTx");
        raw_tx_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as RawTxProvider,
        );
        raw_tx_services.add("Bitails", StdArc::clone(&bitails) as RawTxProvider);

        // postBeef: GorillaPool (if available), TAAL, Bitails, WoC
        let mut post_beef_services = ServiceCollection::new("postBeef");
        if let Some(ref gp) = arc_gorillapool {
            post_beef_services.add("GorillaPoolArcBeef", StdArc::clone(gp) as PostBeefProvider);
        }
        post_beef_services.add("TaalArcBeef", StdArc::clone(&arc_taal) as PostBeefProvider);
        post_beef_services.add("Bitails", StdArc::clone(&bitails) as PostBeefProvider);
        post_beef_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as PostBeefProvider,
        );

        // getUtxoStatus: WoC
        let mut utxo_status_services = ServiceCollection::new("getUtxoStatus");
        utxo_status_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as UtxoStatusProvider,
        );

        // getStatusForTxids: WoC, Bitails
        let mut status_for_txids_services = ServiceCollection::new("getStatusForTxids");
        status_for_txids_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as StatusForTxidsProvider,
        );
        status_for_txids_services.add("Bitails", StdArc::clone(&bitails) as StatusForTxidsProvider);

        // getScriptHashHistory: WoC, Bitails
        let mut script_hash_history_services = ServiceCollection::new("getScriptHashHistory");
        script_hash_history_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as ScriptHashHistoryProvider,
        );
        script_hash_history_services.add(
            "Bitails",
            StdArc::clone(&bitails) as ScriptHashHistoryProvider,
        );

        let fiat_rates = options.fiat_exchange_rates.clone();

        Ok(Self {
            chain,
            options,
            whatsonchain,
            arc_taal,
            arc_gorillapool,
            bitails,
            bhs,
            chaintracks,
            get_merkle_path_services: RwLock::new(merkle_path_services),
            get_raw_tx_services: RwLock::new(raw_tx_services),
            post_beef_services: RwLock::new(post_beef_services),
            get_utxo_status_services: RwLock::new(utxo_status_services),
            get_status_for_txids_services: RwLock::new(status_for_txids_services),
            get_script_hash_history_services: RwLock::new(script_hash_history_services),
            bsv_exchange_rate: RwLock::new(None),
            fiat_exchange_rates: RwLock::new(fiat_rates),
            post_beef_mode: PostBeefMode::default(),
        })
    }

    /// Create mainnet services.
    pub fn mainnet() -> Result<Self> {
        Self::new(Chain::Main)
    }

    /// Create testnet services.
    pub fn testnet() -> Result<Self> {
        Self::new(Chain::Test)
    }

    /// Get services call history.
    pub fn get_services_call_history(&self, reset: bool) -> Result<ServicesCallHistory> {
        Ok(ServicesCallHistory {
            version: 2,
            get_merkle_path: Some(
                lock_write(&self.get_merkle_path_services)?.get_call_history(reset),
            ),
            get_raw_tx: Some(lock_write(&self.get_raw_tx_services)?.get_call_history(reset)),
            post_beef: Some(lock_write(&self.post_beef_services)?.get_call_history(reset)),
            get_utxo_status: Some(
                lock_write(&self.get_utxo_status_services)?.get_call_history(reset),
            ),
            get_status_for_txids: Some(
                lock_write(&self.get_status_for_txids_services)?.get_call_history(reset),
            ),
            get_script_hash_history: Some(
                lock_write(&self.get_script_hash_history_services)?.get_call_history(reset),
            ),
        })
    }

    /// Get count of merkle path providers.
    pub fn get_merkle_path_count(&self) -> Result<usize> {
        Ok(lock_read(&self.get_merkle_path_services)?.count())
    }

    /// Get count of raw tx providers.
    pub fn get_raw_tx_count(&self) -> Result<usize> {
        Ok(lock_read(&self.get_raw_tx_services)?.count())
    }

    /// Get count of post beef providers.
    pub fn post_beef_count(&self) -> Result<usize> {
        Ok(lock_read(&self.post_beef_services)?.count())
    }

    /// Get count of utxo status providers.
    pub fn get_utxo_status_count(&self) -> Result<usize> {
        Ok(lock_read(&self.get_utxo_status_services)?.count())
    }

    /// Set post beef mode.
    pub fn set_post_beef_mode(&mut self, mode: PostBeefMode) {
        self.post_beef_mode = mode;
    }

    // Helper to run service with failover
    #[allow(dead_code)]
    async fn run_with_failover<T, F, Fut>(
        services: &RwLock<ServiceCollection<StdArc<T>>>,
        operation: F,
    ) -> Result<()>
    where
        T: ?Sized + Send + Sync,
        F: Fn(&StdArc<T>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let count = lock_read(services)?.count();
        if count == 0 {
            return Err(Error::NoServicesAvailable);
        }

        for _ in 0..count {
            let service = {
                let collection = lock_read(services)?;
                collection.current_service().cloned()
            };

            if let Some(svc) = service {
                match operation(&svc).await {
                    Ok(()) => return Ok(()),
                    Err(_) => {
                        lock_write(services)?.next();
                    }
                }
            }
        }

        Err(Error::NoServicesAvailable)
    }

    /// Fetch fiat exchange rates from a public API.
    ///
    /// Tries to fetch rates from an open exchange rate API. Falls back to
    /// cached defaults if the fetch fails.
    async fn fetch_fiat_exchange_rates(&self) -> Result<FiatExchangeRates> {
        use std::collections::HashMap;

        // Use a free/open exchange rate API
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| Error::NetworkError(format!("HTTP client error: {}", e)))?;

        let url = "https://open.er-api.com/v6/latest/USD";
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Fiat rate fetch: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Fiat rate API returned HTTP {}",
                response.status()
            )));
        }

        #[derive(serde::Deserialize)]
        struct ExchangeRateResponse {
            rates: HashMap<String, f64>,
        }

        let data: ExchangeRateResponse = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Fiat rate parse: {}", e)))?;

        let mut rates = HashMap::new();
        rates.insert(FiatCurrency::USD, 1.0);

        if let Some(&eur) = data.rates.get("EUR") {
            rates.insert(FiatCurrency::EUR, eur);
        }
        if let Some(&gbp) = data.rates.get("GBP") {
            rates.insert(FiatCurrency::GBP, gbp);
        }

        Ok(FiatExchangeRates::new(rates))
    }
}

#[async_trait]
impl WalletServices for Services {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        if let Some(ref ct) = self.chaintracks {
            Ok(&**ct)
        } else {
            Err(Error::ServiceError(
                "ChainTracker not configured in Services (no chaintracks_url)".to_string(),
            ))
        }
    }

    async fn get_height(&self) -> Result<u32> {
        // Try BHS first if configured
        if let Some(ref bhs) = self.bhs {
            match bhs.current_height().await {
                Ok(h) => return Ok(h),
                Err(e) => tracing::debug!("BHS height failed, trying WoC: {}", e),
            }
        }
        // Try WhatsOnChain
        match self.whatsonchain.get_chain_info().await {
            Ok(info) => return Ok(info.blocks),
            Err(e) => tracing::debug!("WoC height failed, trying Bitails: {}", e),
        }
        // Try Bitails
        match self.bitails.current_height().await {
            Ok(h) => Ok(h),
            Err(e) => Err(Error::ServiceError(format!(
                "All height services failed. Last error: {}",
                e
            ))),
        }
    }

    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        // Try Chaintracks first
        if let Some(ref ct) = self.chaintracks {
            match ct.find_header_for_height(height).await {
                Ok(header) => return Ok(header.to_binary()),
                Err(e) => tracing::debug!("Chaintracks header failed, trying BHS: {}", e),
            }
        }
        // Fall back to BHS
        if let Some(ref bhs) = self.bhs {
            match bhs.chain_header_by_height(height).await {
                Ok(header) => return Ok(header.to_binary()),
                Err(e) => tracing::debug!("BHS header failed: {}", e),
            }
        }
        Err(Error::ServiceError(
            "get_header_for_height: no header service configured".to_string(),
        ))
    }

    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        // Try Chaintracks first (preferred — no rate limits)
        if let Some(ref ct) = self.chaintracks {
            match ct.find_header_for_block_hash(hash).await {
                Ok(header) => return Ok(header),
                Err(e) => tracing::warn!(
                    "Chaintracks hash_to_header failed for {}, falling back to WoC/Bitails: {}",
                    hash,
                    e
                ),
            }
        }

        // Try WhatsOnChain
        if let Some(header) = self.whatsonchain.get_block_header_by_hash(hash).await? {
            return Ok(header);
        }

        // Try Bitails
        if let Some(header) = self.bitails.get_block_header_by_hash(hash).await? {
            return Ok(header);
        }

        Err(Error::NotFound {
            entity: "BlockHeader".to_string(),
            id: hash.to_string(),
        })
    }

    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, RawTxProvider)> = {
            let mut services = lock_write(&self.get_raw_tx_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            services.all_services_from_current()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;

        for (_service_name, provider_name, service) in all_services {
            let mut call = ServiceCall::new();
            match service.get_raw_tx(txid).await {
                Ok(result) if result.raw_tx.is_some() => {
                    call.mark_success(None);
                    lock_write(&self.get_raw_tx_services)?.add_call_success(&provider_name, call);
                    return Ok(result);
                }
                Ok(result) => {
                    call.mark_failure(Some("not found".to_string()));
                    lock_write(&self.get_raw_tx_services)?.add_call_failure(&provider_name, call);
                    last_error = result.error.clone();
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), "ERROR");
                    lock_write(&self.get_raw_tx_services)?.add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                }
            }
        }

        Ok(GetRawTxResult {
            name: "Services".to_string(),
            txid: txid.to_string(),
            raw_tx: None,
            error: last_error,
        })
    }

    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, MerklePathProvider)> = {
            let mut services = lock_write(&self.get_merkle_path_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            services.all_services_from_current()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;
        let mut notes = Vec::new();

        for (_service_name, provider_name, service) in all_services {
            let mut call = ServiceCall::new();
            match service.get_merkle_path(txid).await {
                Ok(result) => {
                    notes.extend(result.notes.clone());
                    if result.merkle_path.is_some() {
                        call.mark_success(None);
                        lock_write(&self.get_merkle_path_services)?
                            .add_call_success(&provider_name, call);

                        // If the provider didn't resolve the block header,
                        // extract the block hash from the proof's "target"
                        // field and resolve it via hash_to_header.
                        let mut result = result;
                        if result.header.is_none() {
                            if let Some(ref mp) = result.merkle_path {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(mp) {
                                    if let Some(target) =
                                        json.get("target").and_then(|t| t.as_str())
                                    {
                                        match self.hash_to_header(target).await {
                                            Ok(header) => {
                                                tracing::debug!(
                                                    "Resolved block header for target {}: height={}",
                                                    target, header.height
                                                );
                                                result.header = Some(header);
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    "Failed to resolve block header for target {}: {}",
                                                    target, e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Convert TSC proof JSON to BUMP hex if needed.
                        // WoC and Bitails return raw TSC JSON; ARC returns BUMP hex.
                        // BEEF construction requires BUMP binary, so we convert here.
                        if let Some(ref mp) = result.merkle_path {
                            if mp.starts_with('{') || mp.starts_with('[') {
                                // TSC proof JSON — convert to BUMP hex
                                if let Some(header) = &result.header {
                                    match crate::tsc_proof::tsc_json_to_bump_hex(mp, header.height)
                                    {
                                        Some(bump_hex) => {
                                            tracing::debug!(
                                                "Converted TSC proof to BUMP hex ({} chars) for txid {}",
                                                bump_hex.len(), txid
                                            );
                                            result.merkle_path = Some(bump_hex);
                                        }
                                        None => {
                                            tracing::warn!(
                                                "Failed to convert TSC proof to BUMP for txid {}",
                                                txid
                                            );
                                        }
                                    }
                                } else {
                                    tracing::warn!(
                                        "Cannot convert TSC proof to BUMP without block height for txid {}, dropping merkle path",
                                        txid
                                    );
                                    result.merkle_path = None;
                                }
                            }
                        }

                        // Layer 2: Never return a merkle_path without a resolved header.
                        // Without a header, the caller stores garbage zeros for height/hash/merkle_root.
                        // Returning None lets the caller treat this as "no proof found" and retry next cycle.
                        if result.merkle_path.is_some() && result.header.is_none() {
                            tracing::warn!(
                                txid = %txid,
                                "Dropping merkle path: header could not be resolved. Will retry on next sync."
                            );
                            result.merkle_path = None;
                        }

                        // Layer 3 (Service-layer validation): Validate the computed
                        // merkle root against ChainTracker BEFORE returning.
                        // This mirrors Go's whatsonchain/service.go where bad proofs
                        // trigger automatic provider failover via the service loop.
                        if let Some(ref mp_hex) = result.merkle_path {
                            if let Some(ref ct) = self.chaintracks {
                                if let Some(ref header) = result.header {
                                    let validation_failed = match hex::decode(mp_hex) {
                                        Ok(mp_bytes) => {
                                            match bsv_rs::transaction::MerklePath::from_binary(
                                                &mp_bytes,
                                            ) {
                                                Ok(bump) => match bump.compute_root(Some(txid)) {
                                                    Ok(computed_root) => {
                                                        match ct
                                                            .is_valid_root_for_height(
                                                                &computed_root,
                                                                header.height,
                                                            )
                                                            .await
                                                        {
                                                            Ok(true) => false,
                                                            Ok(false) => {
                                                                tracing::warn!(
                                                                    txid = %txid,
                                                                    provider = %provider_name,
                                                                    height = header.height,
                                                                    computed_root = %computed_root,
                                                                    "Service-layer merkle root validation failed: \
                                                                     computed root does not match ChainTracker. \
                                                                     Trying next provider."
                                                                );
                                                                true
                                                            }
                                                            Err(e) => {
                                                                tracing::warn!(
                                                                    txid = %txid,
                                                                    provider = %provider_name,
                                                                    error = %e,
                                                                    "ChainTracker error during service-layer \
                                                                     merkle root validation. Trying next provider."
                                                                );
                                                                true
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            txid = %txid,
                                                            provider = %provider_name,
                                                            error = %e,
                                                            "Failed to compute merkle root from BUMP. \
                                                             Trying next provider."
                                                        );
                                                        true
                                                    }
                                                },
                                                Err(e) => {
                                                    tracing::warn!(
                                                        txid = %txid,
                                                        provider = %provider_name,
                                                        error = %e,
                                                        "Failed to parse BUMP binary for validation. \
                                                         Trying next provider."
                                                    );
                                                    true
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                txid = %txid,
                                                provider = %provider_name,
                                                error = %e,
                                                "Failed to decode merkle path hex for validation. \
                                                 Trying next provider."
                                            );
                                            true
                                        }
                                    };

                                    if validation_failed {
                                        let mut fail_call = ServiceCall::new();
                                        fail_call.mark_failure(Some(format!(
                                            "invalid merkle root for txid {} at height {}",
                                            txid, header.height
                                        )));
                                        lock_write(&self.get_merkle_path_services)?
                                            .add_call_failure(&provider_name, fail_call);
                                        last_error = Some(format!(
                                            "Provider {} returned invalid merkle proof for txid {}",
                                            provider_name, txid
                                        ));
                                        continue;
                                    }
                                }
                            }
                        }

                        return Ok(result);
                    } else {
                        call.mark_failure(Some("no proof".to_string()));
                        lock_write(&self.get_merkle_path_services)?
                            .add_call_failure(&provider_name, call);
                        last_error = result.error.clone();
                    }
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), "ERROR");
                    lock_write(&self.get_merkle_path_services)?
                        .add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                }
            }
        }

        Ok(GetMerklePathResult {
            name: Some("Services".to_string()),
            merkle_path: None,
            header: None,
            error: last_error,
            notes,
        })
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, PostBeefProvider)> = {
            let services = lock_read(&self.post_beef_services)?;
            services.all_services_owned()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut results = Vec::new();

        match self.post_beef_mode {
            PostBeefMode::UntilSuccess => {
                for (_service_name, provider_name, service) in all_services {
                    let mut call = ServiceCall::new();
                    match service.post_beef(beef, txids).await {
                        Ok(result) => {
                            let is_success = result.is_success();
                            if is_success {
                                call.mark_success(None);
                                lock_write(&self.post_beef_services)?
                                    .add_call_success(&provider_name, call);
                            } else {
                                call.mark_failure(Some(result.status.clone()));
                                lock_write(&self.post_beef_services)?
                                    .add_call_failure(&provider_name, call);

                                // Move failing service to last
                                if result.txid_results.iter().all(|r| r.service_error) {
                                    lock_write(&self.post_beef_services)?
                                        .move_to_last(&provider_name);
                                }
                            }
                            results.push(result);
                            if is_success {
                                break;
                            }
                        }
                        Err(e) => {
                            call.mark_error(&e.to_string(), "ERROR");
                            lock_write(&self.post_beef_services)?
                                .add_call_error(&provider_name, call);
                        }
                    }
                }
            }
            PostBeefMode::PromiseAll => {
                // Post to all services in parallel
                let futures: Vec<_> = all_services
                    .iter()
                    .map(|(_service_name, _provider_name, service)| {
                        let svc = service.clone();
                        let beef = beef.to_vec();
                        let txids = txids.to_vec();
                        async move { svc.post_beef(&beef, &txids).await }
                    })
                    .collect();

                let parallel_results = futures::future::join_all(futures).await;

                for ((_service_name, provider_name, _service), result) in
                    all_services.iter().zip(parallel_results)
                {
                    let mut call = ServiceCall::new();
                    match result {
                        Ok(r) => {
                            if r.is_success() {
                                call.mark_success(None);
                                lock_write(&self.post_beef_services)?
                                    .add_call_success(provider_name, call);
                            } else {
                                call.mark_failure(Some(r.status.clone()));
                                lock_write(&self.post_beef_services)?
                                    .add_call_failure(provider_name, call);
                            }
                            results.push(r);
                        }
                        Err(e) => {
                            call.mark_error(&e.to_string(), "ERROR");
                            lock_write(&self.post_beef_services)?
                                .add_call_error(provider_name, call);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, UtxoStatusProvider)> = {
            let mut services = lock_write(&self.get_utxo_status_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            services.all_services_from_current()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;

        // Retry loop for transient failures
        for retry in 0..2 {
            for (_service_name, provider_name, service) in &all_services {
                let mut call = ServiceCall::new();
                match service
                    .get_utxo_status(output, output_format, outpoint)
                    .await
                {
                    Ok(result) if result.status == "success" => {
                        call.mark_success(None);
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_success(provider_name, call);
                        return Ok(result);
                    }
                    Ok(result) => {
                        call.mark_failure(result.error.clone());
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_failure(provider_name, call);
                        last_error = result.error.clone();
                    }
                    Err(e) => {
                        call.mark_error(&e.to_string(), "ERROR");
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_error(provider_name, call);
                        last_error = Some(e.to_string());
                    }
                }
            }

            if retry < 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }

        Ok(GetUtxoStatusResult {
            name: "Services".to_string(),
            status: "error".to_string(),
            is_utxo: None,
            details: Vec::new(),
            error: last_error,
        })
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, StatusForTxidsProvider)> = {
            let mut services = lock_write(&self.get_status_for_txids_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            services.all_services_from_current()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;

        for (_service_name, provider_name, service) in all_services {
            let mut call = ServiceCall::new();
            match service.get_status_for_txids(txids).await {
                Ok(result) if result.status == "success" => {
                    call.mark_success(None);
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_success(&provider_name, call);
                    return Ok(result);
                }
                Ok(result) => {
                    call.mark_failure(result.error.clone());
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_failure(&provider_name, call);
                    last_error = result.error.clone();
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), "ERROR");
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                }
            }
        }

        Ok(GetStatusForTxidsResult {
            name: "Services".to_string(),
            status: "error".to_string(),
            error: last_error,
            results: Vec::new(),
        })
    }

    async fn get_script_hash_history(
        &self,
        hash: &str,
        use_next: bool,
    ) -> Result<GetScriptHashHistoryResult> {
        // Get owned copies of services to avoid holding lock across await
        let all_services: Vec<(String, String, ScriptHashHistoryProvider)> = {
            let mut services = lock_write(&self.get_script_hash_history_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            services.all_services_from_current()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;

        for (_service_name, provider_name, service) in all_services {
            let mut call = ServiceCall::new();
            match service.get_script_hash_history(hash).await {
                Ok(result) if result.status == "success" => {
                    call.mark_success(None);
                    lock_write(&self.get_script_hash_history_services)?
                        .add_call_success(&provider_name, call);
                    return Ok(result);
                }
                Ok(result) => {
                    call.mark_failure(result.error.clone());
                    lock_write(&self.get_script_hash_history_services)?
                        .add_call_failure(&provider_name, call);
                    last_error = result.error.clone();
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), "ERROR");
                    lock_write(&self.get_script_hash_history_services)?
                        .add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                }
            }
        }

        Ok(GetScriptHashHistoryResult {
            name: "Services".to_string(),
            status: "error".to_string(),
            error: last_error,
            history: Vec::new(),
        })
    }

    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.whatsonchain
            .update_bsv_exchange_rate(self.options.bsv_update_msecs)
            .await
    }

    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        // Check if we need to update the rates
        let needs_update = {
            let rates = lock_read(&self.fiat_exchange_rates)?;
            rates.is_stale(self.options.fiat_update_msecs)
        };

        if needs_update {
            // Try to fetch updated rates from a public exchange rate API
            match self.fetch_fiat_exchange_rates().await {
                Ok(new_rates) => {
                    let mut rates = lock_write(&self.fiat_exchange_rates)?;
                    *rates = new_rates;
                    tracing::debug!("Updated fiat exchange rates from API");
                }
                Err(e) => {
                    // Fall back to cached/default rates if fetch fails
                    tracing::debug!("Fiat rates fetch failed, using cached rates: {}", e);
                }
            }
        }

        let rates = lock_read(&self.fiat_exchange_rates)?;
        Ok(rates.get_rate(currency, base).unwrap_or(0.0))
    }

    fn hash_output_script(&self, script: &[u8]) -> String {
        let hash = sha256(script);
        // Return LE hex (default format for getUtxoStatus)
        hex::encode(&hash)
    }

    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool> {
        let hash = self.hash_output_script(locking_script);
        let outpoint = format!("{}.{}", txid, vout);
        let result = self
            .get_utxo_status(&hash, None, Some(&outpoint), false)
            .await?;
        Ok(result.is_utxo.unwrap_or(false))
    }

    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        const BLOCK_LIMIT: u32 = 500_000_000;

        if n_lock_time >= BLOCK_LIMIT {
            // Time-based locktime
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
            return Ok(n_lock_time < now);
        }

        // Block-based locktime
        let height = self.get_height().await?;
        Ok(n_lock_time < height)
    }

    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        // BIP 68: If all inputs have max sequence, transaction is immediately final
        if input.all_sequences_final {
            return Ok(true);
        }

        // Check nLockTime finality using the existing logic
        self.n_lock_time_is_final(input.lock_time).await
    }

    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult> {
        use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf, Transaction};
        use std::collections::HashSet;

        /// TSC proof format returned by providers (WhatsOnChain, Bitails).
        #[derive(Debug, serde::Deserialize)]
        struct TscProof {
            index: u64,
            #[serde(rename = "txOrId")]
            tx_or_id: String,
            target: String,
            nodes: Vec<String>,
        }

        /// Parse a JSON-serialized TSC proof string into a MerklePath.
        fn parse_tsc_proof_to_merkle_path(
            json_str: &str,
            block_height: u32,
        ) -> std::result::Result<MerklePath, String> {
            let proof: TscProof = serde_json::from_str(json_str)
                .map_err(|e| format!("Invalid TSC proof JSON: {}", e))?;

            if proof.nodes.is_empty() {
                return Err("empty nodes list".to_string());
            }

            let txid = &proof.tx_or_id;
            if txid.len() != 64 || hex::decode(txid).is_err() {
                return Err("invalid txid in TSC proof".to_string());
            }

            let mut path: Vec<Vec<MerklePathLeaf>> = Vec::new();
            let mut current_offset = proof.index;

            for (level, node) in proof.nodes.iter().enumerate() {
                let mut leaves = Vec::new();

                if level == 0 {
                    let txid_leaf = MerklePathLeaf::new_txid(current_offset, txid.clone());
                    leaves.push(txid_leaf);
                }

                let sibling_offset = if current_offset.is_multiple_of(2) {
                    current_offset + 1
                } else {
                    current_offset - 1
                };

                if node == "*" {
                    leaves.push(MerklePathLeaf::new_duplicate(sibling_offset));
                } else {
                    if node.len() != 64 || hex::decode(node).is_err() {
                        return Err("invalid node hash in TSC proof".to_string());
                    }
                    leaves.push(MerklePathLeaf::new(sibling_offset, node.clone()));
                }

                leaves.sort_by_key(|l| l.offset);
                path.push(leaves);
                current_offset /= 2;
            }

            MerklePath::new(block_height, path).map_err(|e| format!("{}", e))
        }

        // Build known txids lookup set for O(1) checking
        let known_set: HashSet<&str> = known_txids.iter().map(|s| s.as_str()).collect();

        // Get raw transaction
        let raw_tx_result = self.get_raw_tx(txid, false).await?;
        let raw_tx = match raw_tx_result.raw_tx {
            Some(bytes) => bytes,
            None => {
                return Ok(GetBeefResult {
                    name: "Services".to_string(),
                    txid: txid.to_string(),
                    beef: None,
                    has_proof: false,
                    error: raw_tx_result
                        .error
                        .or_else(|| Some("Transaction not found".to_string())),
                });
            }
        };

        // Parse the transaction
        let _tx = match Transaction::from_binary(&raw_tx) {
            Ok(tx) => tx,
            Err(e) => {
                return Ok(GetBeefResult {
                    name: "Services".to_string(),
                    txid: txid.to_string(),
                    beef: None,
                    has_proof: false,
                    error: Some(format!("Failed to parse transaction: {}", e)),
                });
            }
        };

        // Get merkle path for this transaction
        let merkle_result = self.get_merkle_path(txid, false).await?;
        let has_proof = merkle_result.merkle_path.is_some();

        // Create BEEF
        let mut beef = Beef::new();

        // If we have a merkle path, parse and add it.
        // Providers may return BRC-74 hex or JSON-serialized TSC proof strings.
        // Try hex first (backwards compatible), then fall back to JSON TSC proof parsing.
        let bump_index = if let Some(merkle_path_str) = &merkle_result.merkle_path {
            if let Ok(merkle_path) = MerklePath::from_hex(merkle_path_str) {
                Some(beef.merge_bump(merkle_path))
            } else {
                // Try parsing as JSON TSC proof
                let parsed = serde_json::from_str::<TscProof>(merkle_path_str).ok();
                if let Some(proof) = parsed {
                    // Get block height: prefer header from merkle_result, otherwise look up via target hash
                    let block_height = if let Some(header) = &merkle_result.header {
                        Some(header.height)
                    } else {
                        // Look up block header using the TSC proof's target (block hash)
                        match self.hash_to_header(&proof.target).await {
                            Ok(h) => Some(h.height),
                            Err(e) => {
                                tracing::warn!(
                                    "hash_to_header failed for target {} during BEEF construction: {}",
                                    proof.target, e
                                );
                                None
                            }
                        }
                    };
                    if let Some(height) = block_height {
                        parse_tsc_proof_to_merkle_path(merkle_path_str, height)
                            .ok()
                            .map(|mp| beef.merge_bump(mp))
                    } else {
                        tracing::warn!(
                            "Could not determine block height for TSC proof (target: {})",
                            proof.target
                        );
                        None
                    }
                } else {
                    None
                }
            }
        } else {
            None
        };

        // Add the main transaction to BEEF
        // Use merge_raw_tx with bump_index if we have a proof
        beef.merge_raw_tx(raw_tx.clone(), bump_index);

        // Process inputs - for known txids, add as TxIDOnly
        // For this implementation, we just add known txids as references
        for input_txid in known_txids {
            if known_set.contains(input_txid.as_str()) {
                beef.merge_txid_only(input_txid.clone());
            }
        }

        // Serialize BEEF to bytes
        let beef_bytes = beef.to_binary();

        Ok(GetBeefResult {
            name: "Services".to_string(),
            txid: txid.to_string(),
            beef: Some(beef_bytes),
            has_proof,
            error: None,
        })
    }

    fn get_services_call_history(&self, reset: bool) -> ServicesCallHistory {
        // Delegate to the inherent method, falling back to empty on error
        Services::get_services_call_history(self, reset).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_services_creation() {
        let services = Services::mainnet();
        assert!(services.is_ok());
        let services = services.unwrap();
        assert!(services.get_merkle_path_count().unwrap() >= 1);
        assert!(services.post_beef_count().unwrap() >= 1);
    }

    #[test]
    fn test_services_options() {
        let options = ServicesOptions::mainnet()
            .with_woc_api_key("test-key")
            .with_bitails_api_key("bitails-key");

        assert_eq!(options.whatsonchain_api_key, Some("test-key".to_string()));
        assert_eq!(options.bitails_api_key, Some("bitails-key".to_string()));
    }

    #[tokio::test]
    async fn test_get_fiat_exchange_rate() {
        let services = Services::mainnet().unwrap();

        // Test USD to USD (should be 1.0)
        let rate = services
            .get_fiat_exchange_rate(FiatCurrency::USD, Some(FiatCurrency::USD))
            .await
            .unwrap();
        assert!((rate - 1.0).abs() < 0.001);

        // Test EUR with USD base (using default rates)
        let rate = services
            .get_fiat_exchange_rate(FiatCurrency::EUR, None)
            .await
            .unwrap();
        assert!(rate > 0.0 && rate < 2.0); // Reasonable range for EUR/USD

        // Test GBP with EUR base
        let rate = services
            .get_fiat_exchange_rate(FiatCurrency::GBP, Some(FiatCurrency::EUR))
            .await
            .unwrap();
        assert!(rate > 0.0 && rate < 2.0); // Reasonable range for GBP/EUR
    }

    #[test]
    fn test_fiat_currency_parse() {
        assert_eq!(FiatCurrency::parse("USD"), Some(FiatCurrency::USD));
        assert_eq!(FiatCurrency::parse("usd"), Some(FiatCurrency::USD));
        assert_eq!(FiatCurrency::parse("EUR"), Some(FiatCurrency::EUR));
        assert_eq!(FiatCurrency::parse("GBP"), Some(FiatCurrency::GBP));
        assert_eq!(FiatCurrency::parse("XXX"), None);
    }

    #[test]
    fn test_fiat_exchange_rates() {
        let rates = FiatExchangeRates::default();

        // USD to USD should be 1.0
        assert_eq!(
            rates.get_rate(FiatCurrency::USD, Some(FiatCurrency::USD)),
            Some(1.0)
        );

        // EUR to USD should be the EUR rate
        let eur_rate = rates.get_rate(FiatCurrency::EUR, Some(FiatCurrency::USD));
        assert!(eur_rate.is_some());
        assert!(eur_rate.unwrap() > 0.0);

        // Inverse relationship
        let eur_per_usd = rates
            .get_rate(FiatCurrency::EUR, Some(FiatCurrency::USD))
            .unwrap();
        let usd_per_eur = rates
            .get_rate(FiatCurrency::USD, Some(FiatCurrency::EUR))
            .unwrap();
        assert!((eur_per_usd * usd_per_eur - 1.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_get_chain_tracker_returns_client_when_configured() {
        // Build Services with a chaintracks_url configured. The URL doesn't need
        // to be reachable — we only test that get_chain_tracker() returns Ok
        // (i.e. it finds a ChainTracker) rather than the "not configured" error.
        let options =
            ServicesOptions::mainnet().with_chaintracks_url("https://fake-chaintracks.example.com");
        let services = Services::with_options(Chain::Main, options).unwrap();

        let result = services.get_chain_tracker().await;
        assert!(
            result.is_ok(),
            "get_chain_tracker should return Ok when chaintracks_url is configured, got: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_get_chain_tracker_errors_when_not_configured() {
        // Default mainnet Services has no chaintracks_url, so get_chain_tracker
        // should return an error indicating it is not configured.
        let services = Services::mainnet().unwrap();

        let result = services.get_chain_tracker().await;
        assert!(
            result.is_err(),
            "get_chain_tracker should return Err when no chaintracks is configured"
        );
        // Note: can't use unwrap_err() because dyn ChainTracker doesn't impl Debug.
        // The is_err() assertion above is sufficient to verify the error case.
    }

    // =========================================================================
    // Service-layer merkle proof validation tests (Layer 3)
    // =========================================================================

    /// Mock MerklePathService that returns a configurable response.
    struct MockMerklePathProvider {
        response: GetMerklePathResult,
    }

    #[async_trait]
    impl MerklePathService for MockMerklePathProvider {
        async fn get_merkle_path(&self, _txid: &str) -> Result<GetMerklePathResult> {
            Ok(self.response.clone())
        }
    }

    /// Build a ChaintracksServiceClient pointing at a mockito server.
    fn build_mock_chaintracks(server_url: &str) -> StdArc<ChaintracksServiceClient> {
        StdArc::new(ChaintracksServiceClient::from_url(server_url))
    }

    /// Build a Services instance with custom merkle path providers and
    /// optional Chaintracks (via mockito server URL).
    fn build_test_services(
        providers: Vec<(&str, GetMerklePathResult)>,
        chaintracks_url: Option<&str>,
    ) -> Services {
        let mut services = Services::mainnet().unwrap();

        // Replace merkle path service collection with mocks
        let mut collection = ServiceCollection::new("getMerklePath");
        for (name, response) in providers {
            let mock_provider: MerklePathProvider =
                StdArc::new(MockMerklePathProvider { response });
            collection.add(name, mock_provider);
        }
        services.get_merkle_path_services = RwLock::new(collection);

        // Set up Chaintracks if URL provided
        if let Some(url) = chaintracks_url {
            services.chaintracks = Some(build_mock_chaintracks(url));
        } else {
            services.chaintracks = None;
        }

        services
    }

    /// Helper: build a valid BUMP hex and its merkle root for a coinbase-style tx.
    fn build_valid_bump(txid: &str, height: u32) -> (String, String) {
        use bsv_rs::transaction::MerklePath;
        let bump = MerklePath::from_coinbase_txid(txid, height);
        let bump_hex = bump.to_hex();
        let merkle_root = bump
            .compute_root(Some(txid))
            .expect("compute_root for coinbase bump");
        (bump_hex, merkle_root)
    }

    /// Helper: mock Chaintracks /findHeaderHexForHeight endpoint.
    async fn mock_chaintracks_header(
        server: &mut mockito::ServerGuard,
        height: u32,
        merkle_root: &str,
    ) -> mockito::Mock {
        let body = serde_json::json!({
            "status": "success",
            "value": {
                "version": 1,
                "previousHash": "0".repeat(64),
                "merkleRoot": merkle_root,
                "time": 1700000000u32,
                "bits": 486604799u32,
                "nonce": 12345u32,
                "height": height,
                "hash": "b".repeat(64),
            }
        });

        server
            .mock(
                "GET",
                format!("/findHeaderHexForHeight?height={}", height).as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body.to_string())
            .create_async()
            .await
    }

    #[tokio::test]
    async fn test_get_merkle_path_validates_root_against_chaintracks() {
        // Provider returns a bad proof (valid BUMP format but wrong txid,
        // so the computed root won't match the real block's merkle root).
        let txid = "a".repeat(64);
        let height = 850_000u32;

        // Build a valid BUMP for a DIFFERENT txid — this makes the computed
        // root wrong for our target txid.
        let bad_txid = "c".repeat(64);
        let (bad_bump_hex, _bad_root) = build_valid_bump(&bad_txid, height);

        // The "real" merkle root that ChainTracker knows about.
        let (_, real_root) = build_valid_bump(&txid, height);

        let bad_response = GetMerklePathResult {
            name: Some("BadProvider".to_string()),
            merkle_path: Some(bad_bump_hex),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: "f".repeat(64), // wrong root in header too
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let mut mock_server = mockito::Server::new_async().await;
        let _m = mock_chaintracks_header(&mut mock_server, height, &real_root).await;

        let services = build_test_services(
            vec![("BadProvider", bad_response)],
            Some(&mock_server.url()),
        );

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        // The bad proof should be rejected; no merkle_path returned.
        assert!(
            result.merkle_path.is_none(),
            "Expected merkle_path to be None when ChainTracker rejects the root, got: {:?}",
            result.merkle_path
        );
    }

    #[tokio::test]
    async fn test_get_merkle_path_fallback_on_invalid_root() {
        // First provider returns bad proof, second returns good proof.
        // Service-layer validation should reject the first and return the second.
        let txid = "a".repeat(64);
        let height = 850_000u32;

        // Bad provider: BUMP built for wrong txid
        let bad_txid = "c".repeat(64);
        let (bad_bump_hex, _) = build_valid_bump(&bad_txid, height);

        // Good provider: BUMP built for correct txid
        let (good_bump_hex, real_root) = build_valid_bump(&txid, height);

        let bad_response = GetMerklePathResult {
            name: Some("BadProvider".to_string()),
            merkle_path: Some(bad_bump_hex),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: "f".repeat(64),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let good_response = GetMerklePathResult {
            name: Some("GoodProvider".to_string()),
            merkle_path: Some(good_bump_hex.clone()),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: real_root.clone(),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let mut mock_server = mockito::Server::new_async().await;
        let _m = mock_chaintracks_header(&mut mock_server, height, &real_root).await;

        let services = build_test_services(
            vec![
                ("BadProvider", bad_response),
                ("GoodProvider", good_response),
            ],
            Some(&mock_server.url()),
        );

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        // Should have fallen back to the good provider.
        assert_eq!(
            result.merkle_path,
            Some(good_bump_hex),
            "Expected the good provider's BUMP hex after failover"
        );
        assert_eq!(
            result.name,
            Some("GoodProvider".to_string()),
            "Expected result from GoodProvider after BadProvider was rejected"
        );
    }

    #[tokio::test]
    async fn test_get_merkle_path_no_chaintracks_skips_validation() {
        // Without ChainTracker, any proof should pass through unvalidated
        // (backwards compatibility).
        let txid = "a".repeat(64);
        let height = 850_000u32;

        // Build a BUMP for a different txid — normally invalid, but without
        // ChainTracker it should still be returned.
        let other_txid = "c".repeat(64);
        let (bump_hex, _) = build_valid_bump(&other_txid, height);

        let response = GetMerklePathResult {
            name: Some("Provider".to_string()),
            merkle_path: Some(bump_hex.clone()),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: "f".repeat(64),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        // No chaintracks_url — validation should be skipped.
        let services = build_test_services(vec![("Provider", response)], None);

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        assert_eq!(
            result.merkle_path,
            Some(bump_hex),
            "Without ChainTracker, proof should pass through without validation"
        );
    }

    #[tokio::test]
    async fn test_get_merkle_path_valid_root_passes() {
        // Provider returns a valid proof that matches ChainTracker's root.
        let txid = "a".repeat(64);
        let height = 850_000u32;

        let (bump_hex, merkle_root) = build_valid_bump(&txid, height);

        let response = GetMerklePathResult {
            name: Some("GoodProvider".to_string()),
            merkle_path: Some(bump_hex.clone()),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: merkle_root.clone(),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let mut mock_server = mockito::Server::new_async().await;
        let _m = mock_chaintracks_header(&mut mock_server, height, &merkle_root).await;

        let services =
            build_test_services(vec![("GoodProvider", response)], Some(&mock_server.url()));

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        assert_eq!(
            result.merkle_path,
            Some(bump_hex),
            "Valid proof should pass ChainTracker validation"
        );
    }

    #[tokio::test]
    async fn test_get_merkle_path_all_providers_bad() {
        // All providers return bad proofs — result should have no merkle_path.
        let txid = "a".repeat(64);
        let height = 850_000u32;

        let bad_txid_1 = "c".repeat(64);
        let (bad_bump_1, _) = build_valid_bump(&bad_txid_1, height);

        let bad_txid_2 = "d".repeat(64);
        let (bad_bump_2, _) = build_valid_bump(&bad_txid_2, height);

        let (_, real_root) = build_valid_bump(&txid, height);

        let bad_response_1 = GetMerklePathResult {
            name: Some("BadProvider1".to_string()),
            merkle_path: Some(bad_bump_1),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: "f".repeat(64),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let bad_response_2 = GetMerklePathResult {
            name: Some("BadProvider2".to_string()),
            merkle_path: Some(bad_bump_2),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "0".repeat(64),
                merkle_root: "e".repeat(64),
                time: 1700000000,
                bits: 486604799,
                nonce: 12345,
                hash: "b".repeat(64),
                height,
            }),
            error: None,
            notes: vec![],
        };

        let mut mock_server = mockito::Server::new_async().await;
        let _m = mock_chaintracks_header(&mut mock_server, height, &real_root).await;

        let services = build_test_services(
            vec![
                ("BadProvider1", bad_response_1),
                ("BadProvider2", bad_response_2),
            ],
            Some(&mock_server.url()),
        );

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        assert!(
            result.merkle_path.is_none(),
            "All providers returned bad proofs — merkle_path should be None"
        );
        assert!(
            result.error.is_some(),
            "Should have an error message when all providers fail"
        );
    }
}
