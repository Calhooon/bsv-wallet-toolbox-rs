//! Main Services orchestrator.
//!
//! The `Services` struct coordinates multiple service providers with failover
//! support for each method type. It implements the `WalletServices` trait.

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc as StdArc, RwLock};

use crate::lock_utils::{lock_read, lock_write};
use crate::services::broadcast_memory::{
    apply_sticky_provider_order, unproven_ancestors_in_beef, BroadcastMemory, BroadcastStatus,
    BROADCAST_STATUS_ACCEPTED, PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_ARCADE_V2, PROVIDER_BITAILS,
    PROVIDER_GORILLAPOOL_ARC, PROVIDER_TAAL_ARC, PROVIDER_WHATSONCHAIN,
};
#[cfg(feature = "break-glass-script-history")]
use crate::services::traits::GetScriptHashHistoryResult;
use crate::services::traits::{merkle_path_note, PostBeefDelivery, NOTE_REFUTED};
use crate::services::Chain;
use crate::services::{
    collection::{ServiceCall, ServiceCollection},
    providers::{
        Arc, Arcade, BananaBlocks, BhsConfig, Bitails, BitailsConfig, BlockHeaderService,
        ChaintracksConfig, ChaintracksServiceClient, FallbackChainTracker, OutpointSpend,
        WhatsOnChain, WhatsOnChainConfig,
    },
    traits::{
        BlockHeader, BsvExchangeRate, FiatCurrency, FiatExchangeRates, GetBeefResult,
        GetMerklePathResult, GetRawTxResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat,
        GetUtxoStatusResult, NLockTimeInput, PostBeefResult, ServicesCallHistory, TxStatusDetail,
        UnspentPages, UtxoLead, UtxoLeads, UtxoVerdict, WalletServices,
    },
    ServicesOptions,
};
use crate::{Error, Result};
use bsv_rs::primitives::{sha256, sha256d};
use bsv_rs::transaction::ChainTracker;

/// The explorers `hash_to_header` asks under break-glass (WhatsOnChain and
/// Bitails), each the other's fallback.
const EXPLORER_HEADER_SOURCES: usize = 2;

/// How many explorers must each answer "not in the unspent set" before
/// `get_utxo_status` returns that negative (Rule 28: a negative needs a
/// second provider). With three explorers (WhatsOnChain, BananaBlocks,
/// Bitails) a negative is two of the three.
pub const UTXO_NEGATIVE_PROVIDERS: usize = 2;

/// How many explorers must each answer "unspent" before `get_utxo_status`
/// returns that answer. "Unspent" is a negative too ("no spender"), and
/// the owner's rule (2026-10-22, bsv-wallet-cli#50) gives every negative a
/// second provider's clean answer: one explorer's index can miss a mempool
/// spend (BananaBlocks answered `spent: false` for an outpoint
/// WhatsOnChain's mempool showed spent), and the callers that act on an
/// unspent restore or release coins.
pub const UTXO_POSITIVE_PROVIDERS: usize = 2;

/// The code a ladder's call history records for a provider's error: a rate
/// limit (a 429, or the rung closed by an earlier one's Retry-After) apart
/// from every other fault.
fn call_error_code(e: &Error) -> &'static str {
    match e {
        Error::RateLimited { .. } => "RATE_LIMITED",
        _ => "ERROR",
    }
}

/// A rung's fault, by whether asking the same question again could change
/// it: only a [`FaultKind::Transient`] one is asked again in a ladder's
/// retry round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultKind {
    /// A 5xx, a timeout (a 408, or a request that timed out), a transport
    /// failure: the service may answer a second try.
    Transient,
    /// The same question gets the same fault: a 4xx that is an answer (a
    /// 404), a malformed body, a BananaBlocks output whose script is not
    /// the asked one (the same bytes mismatch again).
    Deterministic,
    /// A 429, or a rung closed by an earlier one's Retry-After: not asked
    /// again inside its Retry-After.
    RateLimited,
}

impl FaultKind {
    /// A provider's error. A failed request ([`Error::NetworkError`]: no
    /// connection, a timeout, a body cut short) is transient; a
    /// [`Error::ServiceError`] is transient only when it carries a 5xx (or
    /// 408) status.
    fn of_error(e: &Error) -> Self {
        match e {
            Error::RateLimited { .. } => Self::RateLimited,
            Error::NetworkError(_) => Self::Transient,
            Error::ServiceError(message) => Self::of_message(message),
            _ => Self::Deterministic,
        }
    }

    /// A fault a provider reported in its answer (`status: "error"`), or
    /// in an error's message: transient only for a 5xx or 408 status,
    /// which the explorers write as `HTTP <code>`.
    fn of_message(message: &str) -> Self {
        let status = message
            .match_indices("HTTP ")
            .find_map(|(at, _)| message.get(at + 5..at + 8)?.parse::<u16>().ok());
        match status {
            Some(500..=599 | 408) => Self::Transient,
            _ => Self::Deterministic,
        }
    }
}

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

    /// Arcade V2 provider (populated when `ServicesOptions::arcade_v2` is set).
    ///
    /// Exposed so the Monitor's `ArcadeEventsTask` (and other callers) can
    /// reach the Arcade base URL and callback token for the SSE status stream.
    pub arcade: Option<StdArc<Arcade>>,

    /// Bitails provider.
    pub bitails: StdArc<Bitails>,

    /// BananaBlocks provider: the third explorer of the read ladders, `None`
    /// when [`ServicesOptions::without_bananablocks`] turned it off.
    pub bananablocks: Option<StdArc<BananaBlocks>>,

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

    /// Service collection for getScriptHashHistory: a chain scan, built only
    /// under the break-glass feature (Rule 28, T13 and T14).
    #[cfg(feature = "break-glass-script-history")]
    get_script_hash_history_services: RwLock<ScriptHashHistoryServiceCollection>,

    /// Cached BSV exchange rate.
    #[allow(dead_code)]
    bsv_exchange_rate: RwLock<Option<BsvExchangeRate>>,

    /// Cached fiat exchange rates.
    fiat_exchange_rates: RwLock<FiatExchangeRates>,

    /// The chain tracker over the Chaintracks header service (optional);
    /// an explorer behind it only under the break-glass setting.
    pub chaintracks: Option<StdArc<FallbackChainTracker>>,

    /// The rotating start of the break-glass header-by-hash read.
    hash_to_header_start: AtomicUsize,

    /// Post BEEF mode.
    pub post_beef_mode: PostBeefMode,

    /// Broadcast acceptance memory (reduced sends + sticky provider order),
    /// attached by the wallet / monitor / storage. `None` = the full package
    /// in the static order, exactly the pre-0.3.56 behavior.
    broadcast_memory: RwLock<Option<StdArc<dyn BroadcastMemory>>>,
}

// Type aliases for service collections
type MerklePathServiceCollection = ServiceCollection<MerklePathProvider>;
type RawTxServiceCollection = ServiceCollection<RawTxProvider>;
type PostBeefServiceCollection = ServiceCollection<PostBeefProvider>;
type UtxoStatusServiceCollection = ServiceCollection<UtxoStatusProvider>;
type StatusForTxidsServiceCollection = ServiceCollection<StatusForTxidsProvider>;
#[cfg(feature = "break-glass-script-history")]
type ScriptHashHistoryServiceCollection = ServiceCollection<ScriptHashHistoryProvider>;

// Provider type aliases
type MerklePathProvider = StdArc<dyn MerklePathService + Send + Sync>;
type RawTxProvider = StdArc<dyn RawTxService + Send + Sync>;
type PostBeefProvider = StdArc<dyn PostBeefService + Send + Sync>;
type UtxoStatusProvider = StdArc<dyn UtxoStatusService + Send + Sync>;
type StatusForTxidsProvider = StdArc<dyn StatusForTxidsService + Send + Sync>;
#[cfg(feature = "break-glass-script-history")]
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

    /// `post_beef` with the provider's seen set (txids it has already
    /// accepted or seen, from the attached `BroadcastMemory`). The default
    /// ignores the set and reports a conventional full-package delivery;
    /// the ARC-family providers override it with their reduced sends.
    async fn post_beef_seen(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
    ) -> Result<(PostBeefResult, PostBeefDelivery)> {
        let _ = seen;
        let result = self.post_beef(beef, txids).await?;
        let delivery = PostBeefDelivery::full_package(beef.len(), &result, txids);
        Ok((result, delivery))
    }
}

#[async_trait]
trait UtxoStatusService {
    async fn get_utxo_status(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult>;

    /// Whether this provider answers only an outpoint (BananaBlocks: a
    /// spend status by outpoint, no script's unspent set). For a question
    /// without an outpoint such a provider is not applicable: not asked,
    /// and never counted, as a negative or as a fault.
    fn needs_outpoint(&self) -> bool {
        false
    }

    /// `get_utxo_status`, with the outpoint's spend when the rung read it
    /// on the way (BananaBlocks' `/txo` names the spender beside its
    /// answer): `is_utxo` hands it to `spend_evidence`, which then does not
    /// ask that rung again. `None` for a rung that names no spender.
    async fn get_utxo_status_and_spend(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> (Result<GetUtxoStatusResult>, Option<OutpointSpend>) {
        (self.get_utxo_status(output, format, outpoint).await, None)
    }
}

#[async_trait]
trait StatusForTxidsService {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult>;

    /// Whether this provider indexes the chain (WhatsOnChain, Bitails) or
    /// answers for its own inbox (Arcade). Only a chain index's `unknown`
    /// is an absence vote (two of them make the absence); a broadcaster's
    /// `known` is its word that it holds the transaction, and stands unless
    /// a chain index places it or two chain indexes say `unknown` (see
    /// [`merge_status_results`]).
    fn is_chain_index(&self) -> bool {
        true
    }
}

#[cfg(feature = "break-glass-script-history")]
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
impl MerklePathService for Arcade {
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
impl MerklePathService for BananaBlocks {
    async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        self.get_merkle_path(txid).await
    }
}

#[async_trait]
impl RawTxService for BananaBlocks {
    async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        self.get_raw_tx(txid).await
    }
}

#[async_trait]
impl UtxoStatusService for BananaBlocks {
    async fn get_utxo_status(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult> {
        self.get_utxo_status(output, format, outpoint).await
    }

    fn needs_outpoint(&self) -> bool {
        true
    }

    async fn get_utxo_status_and_spend(
        &self,
        output: &str,
        format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> (Result<GetUtxoStatusResult>, Option<OutpointSpend>) {
        let (result, txo) = self
            .get_utxo_status_with_txo(output, format, outpoint)
            .await;
        (result, txo.map(|txo| txo.spend))
    }
}

#[async_trait]
impl StatusForTxidsService for BananaBlocks {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        self.get_status_for_txids(txids).await
    }

    /// BananaBlocks' index lags the mempool (on the 2026-10-22 probe a
    /// transaction WhatsOnChain held in its mempool was a 404 here), so its
    /// `unknown` is a gap and its `known` a word, as a broadcaster's; its
    /// `mined` stands as every provider's does.
    fn is_chain_index(&self) -> bool {
        false
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

    async fn post_beef_seen(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
    ) -> Result<(PostBeefResult, PostBeefDelivery)> {
        self.post_beef_seen(beef, txids, seen).await
    }
}

#[async_trait]
impl PostBeefService for Arcade {
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult> {
        self.post_beef(beef, txids).await
    }

    async fn post_beef_seen(
        &self,
        beef: &[u8],
        txids: &[String],
        seen: &HashSet<String>,
    ) -> Result<(PostBeefResult, PostBeefDelivery)> {
        self.post_beef_seen(beef, txids, seen).await
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
impl UtxoStatusService for Bitails {
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
impl StatusForTxidsService for Arcade {
    async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        self.get_status_for_txids(txids).await
    }

    /// Arcade answers for the transactions it was handed, from its own
    /// status store: a broadcaster, not a chain index. On 2026-09-02 (beta)
    /// it kept answering `ACCEPTED_BY_NETWORK` for three transactions the
    /// chain had mined two days earlier, and `SEEN_MULTIPLE_NODES` for
    /// phantoms the chain index never saw.
    fn is_chain_index(&self) -> bool {
        false
    }
}

#[cfg(feature = "break-glass-script-history")]
#[async_trait]
impl ScriptHashHistoryService for WhatsOnChain {
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        self.get_script_hash_history(hash).await
    }
}

#[cfg(feature = "break-glass-script-history")]
#[async_trait]
impl ScriptHashHistoryService for Bitails {
    async fn get_script_hash_history(&self, hash: &str) -> Result<GetScriptHashHistoryResult> {
        self.get_script_hash_history(hash).await
    }
}

/// Merge a later provider's answers into `into`.
///
/// `mined` is the only final answer: a slot at `mined` (with the proof it
/// carried) is never touched, and an incoming `mined` always takes the
/// slot. Below that, the chain index decides. `decided` holds the txids a
/// chain index has answered (any status) so far:
///
/// * a chain index's `known` takes any non-mined slot, and its `unknown`
///   takes only a slot no chain index has decided yet and no provider holds
///   as `known`: one chain index's "absent" never stands against a
///   provider that holds the transaction, in either order;
/// * a broadcaster's (or BananaBlocks') `known` fills a slot no chain index
///   has decided (a gap) and takes a slot at `unknown`, but never overrides
///   a chain index's `known`; its `unknown` never replaces anything (its
///   inbox is not the chain);
/// * a txid the earlier answer left out (a provider that stopped early)
///   takes the later provider's answer as it is.
///
/// So any provider's `known` beats one chain index's `unknown`, and the
/// merged status does not depend on the order the providers were asked.
/// The absence that stands is decided after the merge, in
/// `get_status_for_txids`: [`UTXO_NEGATIVE_PROVIDERS`] chain indexes each
/// answering `unknown` make a non-mined txid `unknown`, even against a
/// broadcaster's `known` (the 2026-09-02 phantoms: Arcade
/// `SEEN_MULTIPLE_NODES` for hours, WhatsOnChain 404); any other `unknown`
/// is a gap ("could not look").
fn merge_status_results(
    into: &mut GetStatusForTxidsResult,
    from: GetStatusForTxidsResult,
    from_is_chain_index: bool,
    decided: &mut HashSet<String>,
) {
    for detail in from.results {
        let already_decided = decided.contains(&detail.txid);
        if from_is_chain_index {
            decided.insert(detail.txid.clone());
        }
        let Some(slot) = into.results.iter_mut().find(|d| d.txid == detail.txid) else {
            into.results.push(detail);
            continue;
        };
        if slot.status == "mined" {
            continue;
        }
        let take = match detail.status.as_str() {
            "mined" => true,
            "known" => from_is_chain_index || !already_decided || slot.status == "unknown",
            _ => from_is_chain_index && !already_decided && slot.status != "known",
        };
        if take {
            *slot = detail;
        }
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
        let bitails_config = BitailsConfig {
            api_key: options.bitails_api_key.clone(),
            timeout_secs: None,
        };
        let bitails = StdArc::new(Bitails::new(chain, bitails_config)?);
        let bananablocks = match options.bananablocks_base_for(chain) {
            Some(base) => Some(StdArc::new(BananaBlocks::with_base(&base, None)?)),
            None => None,
        };
        Self::with_explorers(chain, options, whatsonchain, bitails, bananablocks)
    }

    /// `with_options` over the explorer providers given: every collection
    /// and the break-glass tracker are built from these, so a test can
    /// stand local fixtures in their place.
    fn with_explorers(
        chain: Chain,
        options: ServicesOptions,
        whatsonchain: StdArc<WhatsOnChain>,
        bitails: StdArc<Bitails>,
        bananablocks: Option<StdArc<BananaBlocks>>,
    ) -> Result<Self> {
        // Arcade V2 mode (explicit flag — never inferred from the URL): the
        // configured arc_url IS the Arcade endpoint. Build the Arcade
        // broadcaster from it and point the classic TAAL ARC provider at its
        // chain default so it remains a meaningful BEEF-capable failover.
        let arcade = if options.arcade_v2 {
            Some(StdArc::new(Arcade::new(
                options.arc_url.clone(),
                options.arcade_config.clone(),
                Some("ArcadeV2"),
            )?))
        } else {
            None
        };

        let arc_taal_url = if options.arcade_v2 {
            match chain {
                Chain::Main => crate::services::providers::arc::ARC_TAAL_MAINNET.to_string(),
                Chain::Test => crate::services::providers::arc::ARC_TAAL_TESTNET.to_string(),
            }
        } else {
            options.arc_url.clone()
        };

        let arc_taal = StdArc::new(Arc::new(
            arc_taal_url,
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

        // The chain tracker: the header service alone. No explorer in the
        // proof path unless the break-glass setting is on (P0-1c).
        let chaintracks = if let Some(ref ct_url) = options.chaintracks_url {
            let ct_config = ChaintracksConfig {
                url: ct_url.clone(),
                api_key: None,
            };
            let primary = ChaintracksServiceClient::new(ct_config);
            let tracker = if options.break_glass_explorer_headers {
                tracing::warn!(
                    marker = "break_glass_explorer_header",
                    "break-glass: the explorer header fallback is ON; WhatsOnChain and Bitails are asked for merkle roots and headers whenever the header service gives no answer"
                );
                FallbackChainTracker::with_break_glass_explorers(
                    primary,
                    whatsonchain.base_url(),
                    bitails.base_url(),
                )
            } else {
                FallbackChainTracker::new(primary)
            };
            Some(StdArc::new(tracker))
        } else {
            None
        };

        // Build service collections

        // getMerklePath: Arcade (when configured) → WoC → BananaBlocks →
        // Bitails
        //
        // Rule 28 (T6, T7): the two explorers are break-glass couriers of
        // a proof nobody pushed to us; the proof is believed for its root
        // against the header service, never for who served it.
        //
        // A wallet that broadcasts through Arcade already has a first-party
        // source for its own proofs: Arcade's MINED status document carries
        // the BUMP. Asking it first means the third-party indexers are only
        // touched for a transaction Arcade has no proof for. The order is
        // unchanged when Arcade is not configured.
        let mut merkle_path_services = ServiceCollection::new("getMerklePath");
        if let Some(ref arcade_provider) = arcade {
            merkle_path_services.add(
                PROVIDER_ARCADE_V2,
                StdArc::clone(arcade_provider) as MerklePathProvider,
            );
        }
        merkle_path_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as MerklePathProvider,
        );
        if let Some(ref bb) = bananablocks {
            merkle_path_services.add("BananaBlocks", StdArc::clone(bb) as MerklePathProvider);
        }
        merkle_path_services.add("Bitails", StdArc::clone(&bitails) as MerklePathProvider);

        // getRawTx: WoC, BananaBlocks, Bitails (pruned, last). Rule 28 (T8,
        // T9): break-glass couriers of a foreign ancestor's bytes no store of
        // ours holds; the bytes are bound to the txid.
        let mut raw_tx_services = ServiceCollection::new("getRawTx");
        raw_tx_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as RawTxProvider,
        );
        if let Some(ref bb) = bananablocks {
            raw_tx_services.add("BananaBlocks", StdArc::clone(bb) as RawTxProvider);
        }
        raw_tx_services.add("Bitails", StdArc::clone(&bitails) as RawTxProvider);

        // postBeef: TAAL → GorillaPool → Bitails → WoC
        //
        // TAAL is placed first intentionally. In production we observed
        // GorillaPool ARC accepting POSTs with `txStatus: ANNOUNCED_TO_NETWORK`
        // and returning HTTP 200 while the tx never actually propagated to
        // other ARC nodes / miners. Because PostBeefMode::UntilSuccess stops
        // on the first provider that returns success, putting GorillaPool
        // first caused ~185 stuck txs in one wallet over a few hours. TAAL
        // ARC accepts and actually federates, so we try it first and fall
        // back to GorillaPool (still useful when TAAL is degraded).
        let mut post_beef_services = ServiceCollection::new("postBeef");
        // Arcade V2 (when enabled) goes first: it is the explicitly configured
        // primary broadcaster. Classic ARC providers stay behind it as
        // failover so a transient Arcade outage never blocks a broadcast.
        if let Some(ref arcade_provider) = arcade {
            post_beef_services.add(
                PROVIDER_ARCADE_V2,
                StdArc::clone(arcade_provider) as PostBeefProvider,
            );
        }
        post_beef_services.add(
            PROVIDER_TAAL_ARC,
            StdArc::clone(&arc_taal) as PostBeefProvider,
        );
        if let Some(ref gp) = arc_gorillapool {
            post_beef_services.add(
                PROVIDER_GORILLAPOOL_ARC,
                StdArc::clone(gp) as PostBeefProvider,
            );
        }
        post_beef_services.add(
            PROVIDER_BITAILS,
            StdArc::clone(&bitails) as PostBeefProvider,
        );
        post_beef_services.add(
            PROVIDER_WHATSONCHAIN,
            StdArc::clone(&whatsonchain) as PostBeefProvider,
        );

        // getUtxoStatus: WoC, BananaBlocks, Bitails. Rule 28 (T10), the
        // irreducible case: headers and proofs prove inclusion, never that
        // an output is unspent, so the explorers are asked, each the others'
        // fallback (a rotating start, a negative only from two).
        let mut utxo_status_services = ServiceCollection::new("getUtxoStatus");
        utxo_status_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as UtxoStatusProvider,
        );
        if let Some(ref bb) = bananablocks {
            utxo_status_services.add("BananaBlocks", StdArc::clone(bb) as UtxoStatusProvider);
        }
        utxo_status_services.add("Bitails", StdArc::clone(&bitails) as UtxoStatusProvider);

        // getStatusForTxids: Arcade (when configured) → WoC → Bitails →
        // BananaBlocks (one request per txid against its bucket, a burst of
        // 30 refilled at 0.5/s, so 60 in the first minute and 30 a minute
        // sustained, under BananaBlocks' 80; so last: asked only about what
        // no earlier provider placed mined)
        //
        // Rule 28 (T11, T12): "mined" is a proof; "known to the mempool"
        // and "unknown" have no header or proof answer, so the two
        // explorers stay as the break-glass read for those halves.
        //
        // Arcade's status document answers the triage AND carries the proof
        // for a mined transaction, so one call per txid does the work the
        // batch status call plus a getMerklePath used to do. The order is
        // unchanged when Arcade is not configured.
        let mut status_for_txids_services = ServiceCollection::new("getStatusForTxids");
        if let Some(ref arcade_provider) = arcade {
            status_for_txids_services.add(
                PROVIDER_ARCADE_V2,
                StdArc::clone(arcade_provider) as StatusForTxidsProvider,
            );
        }
        status_for_txids_services.add(
            "WhatsOnChain",
            StdArc::clone(&whatsonchain) as StatusForTxidsProvider,
        );
        status_for_txids_services.add("Bitails", StdArc::clone(&bitails) as StatusForTxidsProvider);
        if let Some(ref bb) = bananablocks {
            status_for_txids_services
                .add("BananaBlocks", StdArc::clone(bb) as StatusForTxidsProvider);
        }

        // getScriptHashHistory: WoC, Bitails. Rule 28 (T13, T14): every
        // transaction that touched a script is a chain scan; no header,
        // proof or index of ours answers it because a wallet never needs
        // it (it is handed a BEEF). Built only under the cargo feature
        // `break-glass-script-history`, a break-glass read.
        #[cfg(feature = "break-glass-script-history")]
        let script_hash_history_services = {
            let mut collection = ServiceCollection::new("getScriptHashHistory");
            collection.add(
                "WhatsOnChain",
                StdArc::clone(&whatsonchain) as ScriptHashHistoryProvider,
            );
            collection.add(
                "Bitails",
                StdArc::clone(&bitails) as ScriptHashHistoryProvider,
            );
            collection
        };

        let fiat_rates = options.fiat_exchange_rates.clone();

        Ok(Self {
            chain,
            options,
            whatsonchain,
            arc_taal,
            arc_gorillapool,
            arcade,
            bitails,
            bananablocks,
            bhs,
            chaintracks,
            get_merkle_path_services: RwLock::new(merkle_path_services),
            get_raw_tx_services: RwLock::new(raw_tx_services),
            post_beef_services: RwLock::new(post_beef_services),
            get_utxo_status_services: RwLock::new(utxo_status_services),
            get_status_for_txids_services: RwLock::new(status_for_txids_services),
            #[cfg(feature = "break-glass-script-history")]
            get_script_hash_history_services: RwLock::new(script_hash_history_services),
            bsv_exchange_rate: RwLock::new(None),
            fiat_exchange_rates: RwLock::new(fiat_rates),
            hash_to_header_start: AtomicUsize::new(0),
            post_beef_mode: PostBeefMode::default(),
            broadcast_memory: RwLock::new(None),
        })
    }

    /// Record what `provider_name` just accepted and make it the sticky
    /// provider for the next broadcast. Memory faults are logged, never
    /// surfaced: the memory is an optimization and the broadcast already
    /// succeeded.
    async fn remember_acceptance(
        &self,
        memory: &dyn BroadcastMemory,
        provider_name: &str,
        txids: &[String],
        delivery: &PostBeefDelivery,
        sticky: Option<&str>,
    ) {
        let accepted: Vec<String> = if delivery.accepted_txids.is_empty() {
            txids.to_vec()
        } else {
            delivery.accepted_txids.clone()
        };
        if let Err(e) = memory
            .record_broadcast_seen_many(provider_name, BROADCAST_STATUS_ACCEPTED, &accepted)
            .await
        {
            tracing::warn!(
                name = %provider_name,
                error = %e,
                "broadcast memory: could not record the acceptance"
            );
        }
        if sticky != Some(provider_name) {
            if let Err(e) = memory
                .set_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER, provider_name)
                .await
            {
                tracing::warn!(
                    name = %provider_name,
                    error = %e,
                    "broadcast memory: could not persist the sticky provider"
                );
            }
        }
    }

    /// The unproven ancestors `provider_name` may leave out of this send:
    /// the memory's network-evidence rows
    /// ([`seen_set_from_records`](crate::services::seen_set_from_records);
    /// an acceptance alone never counts), re-checked against Arcade with
    /// one `GET /tx/{txid}` when the oldest evidence is older than
    /// [`BROADCAST_SEEN_STALE_SECS`](crate::services::BROADCAST_SEEN_STALE_SECS):
    /// `SEEN_*` / `MINED` confirms the chain, a `REJECTED` /
    /// `DOUBLE_SPEND_ATTEMPTED` / 404 / pre-gate answer means the memory
    /// was stale (that ancestor is recorded `rejected` / `unknown` and the
    /// full package goes out). Any fault answers with the full package: the
    /// memory is an optimization, never a reason to send less than the
    /// network might need.
    async fn skippable_ancestors(
        &self,
        memory: &dyn BroadcastMemory,
        provider_name: &str,
        unproven_ancestors: &[String],
    ) -> HashSet<String> {
        use crate::services::broadcast_memory::{
            oldest_stale_ancestor, seen_set_from_records, BroadcastStatus,
            BROADCAST_SEEN_STALE_SECS, BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_UNKNOWN,
        };

        let records = match memory
            .broadcast_records(Some(provider_name), unproven_ancestors)
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(
                    name = %provider_name,
                    error = %e,
                    "broadcast memory: record lookup failed; sending the full package"
                );
                return HashSet::new();
            }
        };
        let mut seen = seen_set_from_records(provider_name, unproven_ancestors, &records);
        if seen.is_empty() || provider_name != PROVIDER_ARCADE_V2 {
            return seen;
        }
        let Some(arcade) = &self.arcade else {
            return seen;
        };
        let skipped: Vec<String> = unproven_ancestors
            .iter()
            .filter(|txid| seen.contains(*txid))
            .cloned()
            .collect();
        let Some(stale) = oldest_stale_ancestor(
            provider_name,
            &skipped,
            &records,
            chrono::Utc::now(),
            BROADCAST_SEEN_STALE_SECS,
        ) else {
            return seen;
        };

        let stale_txid: &str = &stale;
        let record = |status: &'static str| async move {
            if let Err(e) = memory
                .record_broadcast_status(stale_txid, provider_name, status)
                .await
            {
                tracing::warn!(txid = %stale_txid, status, error = %e, "broadcast memory: could not record the probe verdict");
            }
        };
        match arcade.get_tx_status(&stale).await {
            Ok(Some(info)) => {
                let status = BroadcastStatus::from_arcade_status(&info.tx_status);
                if status.is_network_evidence() {
                    tracing::info!(
                        name = %provider_name,
                        txid = %stale,
                        tx_status = %info.tx_status,
                        skipped = skipped.len(),
                        "broadcast memory: stale seen re-confirmed by the broadcaster; reduced send stands"
                    );
                    record(status.as_str()).await;
                } else {
                    let recorded = if status == BroadcastStatus::Rejected {
                        BROADCAST_STATUS_REJECTED
                    } else {
                        BROADCAST_STATUS_UNKNOWN
                    };
                    tracing::warn!(
                        name = %provider_name,
                        txid = %stale,
                        tx_status = %info.tx_status,
                        recorded,
                        "broadcast memory: stale seen NOT confirmed by the broadcaster; sending the full package"
                    );
                    record(recorded).await;
                    seen.clear();
                }
            }
            Ok(None) => {
                tracing::warn!(
                    name = %provider_name,
                    txid = %stale,
                    "broadcast memory: stale seen is unknown to the broadcaster (404); sending the full package"
                );
                record(BROADCAST_STATUS_UNKNOWN).await;
                seen.clear();
            }
            Err(e) => {
                tracing::warn!(
                    name = %provider_name,
                    txid = %stale,
                    error = %e,
                    "broadcast memory: stale seen could not be re-checked; sending the full package"
                );
                seen.clear();
            }
        }
        seen
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
            #[cfg(feature = "break-glass-script-history")]
            get_script_hash_history: Some(
                lock_write(&self.get_script_hash_history_services)?.get_call_history(reset),
            ),
            #[cfg(not(feature = "break-glass-script-history"))]
            get_script_hash_history: None,
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
        // Use a free/open exchange rate API
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| Error::NetworkError(format!("HTTP client error: {}", e)))?;

        // Not a chain question (fiat rates), so Rule 28's test does not apply.
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

/// An explorer `get_utxo_leads` asks for a script's unspent list.
enum LeadSource {
    /// `/script/{hash}/unspent/all`.
    WhatsOnChain,
    /// `/scripthash/{hash}/unspent`.
    Bitails,
    /// `/address/{address}/utxos`: BananaBlocks lists by address only, so
    /// it is a lead source only for a P2PKH script handed as itself.
    BananaBlocks(String),
}

impl LeadSource {
    fn name(&self) -> &'static str {
        match self {
            Self::WhatsOnChain => "WhatsOnChain",
            Self::Bitails => "Bitails",
            Self::BananaBlocks(_) => "BananaBlocks",
        }
    }
}

impl Services {
    /// The P2PKH address of `output` when it is a script handed as itself
    /// (`GetUtxoStatusOutputFormat::Script`): BananaBlocks' unspent lists
    /// are by address. Only the exact 25-byte template `76a914<20>88ac`
    /// has one: a non-minimal push of the same hash (`76a94c14<20>88ac`)
    /// is another script, whose outputs BananaBlocks' address list is not.
    /// `None` for a hash (no address is derived from one) or any other
    /// script.
    fn p2pkh_address(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
    ) -> Option<String> {
        if output_format != Some(GetUtxoStatusOutputFormat::Script) {
            return None;
        }
        let script = hex::decode(output).ok()?;
        let hash: [u8; 20] = match script.as_slice() {
            [0x76, 0xa9, 0x14, hash @ .., 0x88, 0xac] => hash.try_into().ok()?,
            _ => return None,
        };
        bsv_rs::script::Address::new_from_public_key_hash(&hash, self.chain == Chain::Main)
            .ok()
            .map(|address| address.to_string())
    }

    /// One lead source's list, each page through its shared rate gate. A
    /// list holding a txid that is not 64 hex characters is the explorer's
    /// malformed answer (deterministic): a lead's txid is put in a path
    /// later (`/tx/{txid}/hex`), so `zz` or `../x` is never passed on.
    async fn lead_list(
        &self,
        source: &LeadSource,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
    ) -> Result<UnspentPages> {
        let list = match source {
            LeadSource::WhatsOnChain => {
                self.whatsonchain
                    .list_script_unspent(output, output_format)
                    .await?
            }
            LeadSource::Bitails => {
                self.bitails
                    .list_script_unspent(output, output_format)
                    .await?
            }
            LeadSource::BananaBlocks(address) => match &self.bananablocks {
                Some(bb) => bb.list_address_unspent(address).await?,
                None => return Err(Error::NoServicesAvailable),
            },
        };
        if let Some(bad) = list
            .details
            .iter()
            .find(|d| d.txid.len() != 64 || !d.txid.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(Error::ServiceError(format!(
                "{} listed a txid that is not 64 hex characters: {:?}",
                source.name(),
                bad.txid.chars().take(80).collect::<String>()
            )));
        }
        Ok(list)
    }

    /// `get_utxo_leads` (its trait doc is the rule): every lead source
    /// asked, a transient fault once more after the retry round's pause,
    /// the answers' union keyed by outpoint.
    async fn utxo_leads(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
    ) -> Result<UtxoLeads> {
        // A malformed question is the caller's error, not an explorer's
        // fault.
        crate::services::traits::convert_script_hash(output, output_format)?;
        let mut sources = vec![LeadSource::WhatsOnChain, LeadSource::Bitails];
        if self.bananablocks.is_some() {
            if let Some(address) = self.p2pkh_address(output, output_format) {
                sources.push(LeadSource::BananaBlocks(address));
            }
        }

        let mut lists: Vec<Option<UnspentPages>> = sources.iter().map(|_| None).collect();
        let mut faults: Vec<Option<String>> = sources.iter().map(|_| None).collect();
        let mut pending: Vec<usize> = (0..sources.len()).collect();
        // One retry round, for transient faults only (see `FaultKind`).
        for retry in 0..2 {
            let mut unanswered = Vec::new();
            for i in pending {
                match self.lead_list(&sources[i], output, output_format).await {
                    Ok(list) => {
                        lists[i] = Some(list);
                        faults[i] = None;
                    }
                    Err(e) => {
                        tracing::debug!(explorer = sources[i].name(), error = %e, "get_utxo_leads: could not look");
                        if FaultKind::of_error(&e) == FaultKind::Transient {
                            unanswered.push(i);
                        }
                        faults[i] = Some(e.to_string());
                    }
                }
            }
            pending = unanswered;
            if pending.is_empty() {
                break;
            }
            if retry < 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }

        let mut answer = UtxoLeads::default();
        // Each outpoint's place in `answer.leads`: lists run to 10,000 each.
        let mut at: HashMap<(String, u32), usize> = HashMap::new();
        for (i, source) in sources.iter().enumerate() {
            if let Some(fault) = faults[i].take() {
                answer.faults.push((source.name().to_string(), fault));
            }
            let Some(list) = lists[i].take() else {
                continue;
            };
            answer.answered.push(source.name().to_string());
            if let Some(why) = list.cut_short {
                tracing::info!(explorer = source.name(), why = %why, "get_utxo_leads: a list cut short");
                answer.partial.push(source.name().to_string());
            }
            for detail in list.details {
                let txid = detail.txid.to_ascii_lowercase();
                match at.get(&(txid.clone(), detail.index)) {
                    Some(&n) => {
                        let lead = &mut answer.leads[n];
                        if !lead.listed_by.iter().any(|n| n == source.name()) {
                            lead.listed_by.push(source.name().to_string());
                        }
                    }
                    None => {
                        at.insert((txid.clone(), detail.index), answer.leads.len());
                        answer.leads.push(UtxoLead {
                            txid,
                            vout: detail.index,
                            satoshis: Some(detail.satoshis),
                            listed_by: vec![source.name().to_string()],
                        });
                    }
                }
            }
        }
        if answer.answered.is_empty() {
            return Err(Error::NoExplorerAnswered {
                faults: answer.faults,
            });
        }
        Ok(answer)
    }
}

/// What we hold about a stranger's spend of one outpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpendEvidence {
    /// The spender's bytes name the outpoint and its merkle path met the
    /// header service's header: a chain fact.
    Proven,
    /// The spender's bytes name the outpoint; no proof of it is held (not
    /// mined yet, or no courier served a path the header service accepts).
    Bound,
    /// No spender named, or a name its own bytes do not bear out.
    None,
}

impl Services {
    /// Bind, then prove (Rule 28; bsv-stack-lean `NORTH-STAR.md`, the
    /// ruling of 2026-10-09: a stranger's spend becomes a chain fact only
    /// by the spending transaction's merkle proof checked against our
    /// headers).
    ///
    /// 1. A provider names the spender: a word, and the provider a courier.
    ///    WhatsOnChain (`/tx/{txid}/{vout}/spent`) and BananaBlocks
    ///    (`/txo/{txid}/{vout}`'s `spent_txid`) are each the other's fallback: a
    ///    fault, no name, or a name its bytes do not bear out moves to the
    ///    next.
    /// 2. The named transaction's bytes (bound to its txid by `get_raw_tx`)
    ///    must have an input that is this outpoint: a fact about the bytes.
    /// 3. `get_merkle_path` returns a path only after its root met the
    ///    header service's header at that height: the chain's word.
    ///
    /// With no header service step 3 cannot be done, so step 1 is not
    /// asked: nothing is fetched that could not be checked.
    ///
    /// `carried` is BananaBlocks' `/txo` spend the UTXO ladder already read
    /// for this outpoint: when given, BananaBlocks is not asked again.
    async fn spend_evidence(
        &self,
        txid: &str,
        vout: u32,
        carried: Option<OutpointSpend>,
    ) -> SpendEvidence {
        if self.chaintracks.is_none() {
            return SpendEvidence::None;
        }
        let mut named: HashSet<String> = HashSet::new();
        let woc = self.whatsonchain.get_spender(txid, vout).await;
        if let Some(spender) = self.named_spender("WhatsOnChain", txid, vout, woc, &mut named) {
            if let Some(found) = self.bind_spender(txid, vout, &spender).await {
                return found;
            }
        }
        if let Some(bb) = &self.bananablocks {
            let spend = match carried {
                Some(spend) => Ok(spend),
                None => bb.get_spend(txid, vout).await,
            };
            let answer = spend.map(|spend| match spend {
                OutpointSpend::SpentBy(spender) => Some(spender),
                OutpointSpend::Unspent => None,
            });
            if let Some(spender) =
                self.named_spender("BananaBlocks", txid, vout, answer, &mut named)
            {
                if let Some(found) = self.bind_spender(txid, vout, &spender).await {
                    return found;
                }
            }
        }
        SpendEvidence::None
    }

    /// The ladder of `get_utxo_status` (its doc there is the rule), with
    /// the outpoint's spend a rung read on the way (BananaBlocks' `/txo`):
    /// `is_utxo` hands it to `spend_evidence`, so an unspent verdict costs
    /// one BananaBlocks request when it answers; after a fault
    /// `spend_evidence` asks it once more (up to three with a 5xx retried
    /// in the ladder).
    async fn utxo_ladder(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<(GetUtxoStatusResult, Option<OutpointSpend>)> {
        let all_services: Vec<(String, String, UtxoStatusProvider)> = {
            let mut services = lock_write(&self.get_utxo_status_services)?;
            // If use_next, skip to next service before starting
            if use_next {
                services.next();
            }
            let from_current = services.all_services_from_current();
            // The rotating start: the next call begins one explorer on.
            services.next();
            from_current
        };
        // A provider that answers only an outpoint is not applicable to a
        // script's question: not asked, never counted.
        let all_services: Vec<(String, String, UtxoStatusProvider)> = all_services
            .into_iter()
            .filter(|(_, _, service)| outpoint.is_some() || !service.needs_outpoint())
            .collect();

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let mut last_error = None;
        // The explorers that answered "unspent", and "not in the unspent set".
        let mut positives: Vec<GetUtxoStatusResult> = Vec::new();
        let mut negatives: Vec<GetUtxoStatusResult> = Vec::new();
        // The outpoint's spend, when a rung read it on the way.
        let mut spend: Option<OutpointSpend> = None;
        // The explorers still owed an answer (a transient fault is asked
        // once more).
        let mut pending: Vec<&(String, String, UtxoStatusProvider)> = all_services.iter().collect();

        // One retry round, for transient faults only (see `FaultKind`).
        'rounds: for retry in 0..2 {
            let mut unanswered = Vec::new();
            for entry in pending {
                let (_service_name, provider_name, service) = entry;
                let mut call = ServiceCall::new();
                let (answer, read) = service
                    .get_utxo_status_and_spend(output, output_format, outpoint)
                    .await;
                if read.is_some() {
                    spend = read;
                }
                match answer {
                    Ok(result) if result.status == "success" && result.is_utxo.is_some() => {
                        call.mark_success(None);
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_success(provider_name, call);
                        let agreeing = if result.is_utxo == Some(true) {
                            positives.push(result);
                            positives.len() >= UTXO_POSITIVE_PROVIDERS
                        } else {
                            negatives.push(result);
                            negatives.len() >= UTXO_NEGATIVE_PROVIDERS
                        };
                        if agreeing {
                            break 'rounds;
                        }
                    }
                    Ok(result) => {
                        call.mark_failure(result.error.clone());
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_failure(provider_name, call);
                        let error = result.error.as_deref().unwrap_or("no answer");
                        last_error = Some(format!("{}: {}", provider_name, error));
                        if FaultKind::of_message(error) == FaultKind::Transient {
                            unanswered.push(entry);
                        }
                    }
                    Err(e) => {
                        call.mark_error(&e.to_string(), call_error_code(&e));
                        lock_write(&self.get_utxo_status_services)?
                            .add_call_error(provider_name, call);
                        last_error = Some(format!("{}: {}", provider_name, e));
                        // A rate-limited rung stays closed for its
                        // Retry-After, and a deterministic fault would
                        // come back the same: the retry round asks
                        // neither.
                        if FaultKind::of_error(&e) == FaultKind::Transient {
                            unanswered.push(entry);
                        }
                    }
                }
            }

            pending = unanswered;
            if pending.is_empty() {
                break;
            }
            if retry < 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }

        let names = |answers: &[GetUtxoStatusResult]| -> String {
            answers
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join("+")
        };
        if !positives.is_empty() && !negatives.is_empty() {
            tracing::warn!(
                output = %output,
                outpoint = ?outpoint,
                unspent_by = %names(&positives),
                not_listed_by = %names(&negatives),
                "get_utxo_status: the explorers disagree"
            );
        }
        for (answers, needed) in [
            (&mut positives, UTXO_POSITIVE_PROVIDERS),
            (&mut negatives, UTXO_NEGATIVE_PROVIDERS),
        ] {
            if answers.len() >= needed {
                let name = names(answers);
                let mut result = answers.swap_remove(0);
                result.name = name;
                return Ok((result, spend));
            }
        }

        // Could not look. A lone answer is named, never returned.
        let lone = |answers: &[GetUtxoStatusResult], what: &str| -> Option<String> {
            (!answers.is_empty()).then(|| format!("{} {}", names(answers), what))
        };
        let said: Vec<String> = [
            lone(&positives, "lists the output as unspent"),
            lone(&negatives, "does not list the output as unspent"),
        ]
        .into_iter()
        .flatten()
        .collect();
        let error = if said.is_empty() {
            last_error
        } else {
            Some(format!(
                "could not look: {} and no second explorer confirmed it ({})",
                said.join("; "),
                last_error
                    .as_deref()
                    .unwrap_or("no other explorer answered")
            ))
        };
        Ok((
            GetUtxoStatusResult {
                name: "Services".to_string(),
                status: "error".to_string(),
                is_utxo: None,
                details: Vec::new(),
                error,
            },
            spend,
        ))
    }

    /// A provider's answer to "who spent this outpoint" as a name to bind,
    /// once per name (a second source naming the same spender adds no
    /// evidence and costs no second fetch).
    fn named_spender(
        &self,
        provider: &str,
        txid: &str,
        vout: u32,
        answer: Result<Option<String>>,
        named: &mut HashSet<String>,
    ) -> Option<String> {
        match answer {
            Ok(Some(spender)) if named.insert(spender.clone()) => Some(spender),
            Ok(_) => None,
            Err(e) => {
                tracing::debug!(txid = %txid, vout, provider, error = %e, "spend evidence: could not ask for a spender");
                None
            }
        }
    }

    /// Steps 2 and 3 for one named spender: `None` when its bytes are not
    /// held or do not name the outpoint (the name is dropped).
    async fn bind_spender(&self, txid: &str, vout: u32, spender: &str) -> Option<SpendEvidence> {
        let raw_tx = match self.get_raw_tx(spender, false).await {
            Ok(GetRawTxResult {
                raw_tx: Some(raw_tx),
                ..
            }) => raw_tx,
            _ => {
                tracing::debug!(txid = %txid, vout, spender = %spender, "spend evidence: the named spender's bytes are not held");
                return None;
            }
        };
        let names_the_outpoint = bsv_rs::transaction::Transaction::from_binary(&raw_tx)
            .map(|tx| {
                tx.inputs.iter().any(|input| {
                    input.source_output_index == vout
                        && input
                            .get_source_txid()
                            .is_ok_and(|source| source.eq_ignore_ascii_case(txid))
                })
            })
            .unwrap_or(false);
        if !names_the_outpoint {
            tracing::warn!(
                txid = %txid,
                vout,
                spender = %spender,
                "spend evidence: the named spender's bytes do not spend the outpoint; the name is dropped"
            );
            return None;
        }
        Some(match self.get_merkle_path(spender, false).await {
            Ok(found) if found.merkle_path.is_some() => SpendEvidence::Proven,
            _ => SpendEvidence::Bound,
        })
    }
}

/// The answer of a header ladder (`get_chain_tip_header`,
/// `get_header_for_height`) when no rung gave a header: each configured
/// rung's error by its name, so "could not look" is never read as "not
/// configured" (bsv-wallet-toolbox-rs#47). "no header service configured"
/// only when neither chaintracks nor BHS is configured.
fn header_ladder_error(call: &str, rung_errors: &[String]) -> String {
    if rung_errors.is_empty() {
        format!("{}: no header service configured", call)
    } else {
        format!(
            "{}: no header service answered ({})",
            call,
            rung_errors.join("; ")
        )
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
        // Rule 28 (T1, T2): the tip height is the header service's tip
        // header's height, the same source `get_chain_tip_header` reads, so
        // the height and the tip hash never come from two places. No
        // explorer is asked: a header service holds this answer, and with
        // none reachable the answer is an error ("could not look"), never
        // an explorer's number.
        let mut last_error = "no header service configured".to_string();
        if let Some(ref ct) = self.chaintracks {
            match ct.primary().find_chain_tip_header().await {
                Ok(header) => return Ok(header.height),
                Err(e) => {
                    tracing::debug!("Chaintracks tip height failed, trying BHS: {}", e);
                    last_error = e.to_string();
                }
            }
        }
        if let Some(ref bhs) = self.bhs {
            match bhs.current_height().await {
                Ok(h) => return Ok(h),
                Err(e) => {
                    tracing::debug!("BHS height failed: {}", e);
                    last_error = e.to_string();
                }
            }
        }
        Err(Error::ServiceError(format!(
            "get_height: no header service gave the tip height ({}); no explorer is asked (set chaintracks_url or bhs_url)",
            last_error
        )))
    }

    async fn get_chain_tip_header(&self) -> Result<BlockHeader> {
        // The header service only: chaintracks (first-party), then BHS.
        // Never a courier, so the height and the hash the monitor's header
        // task observes come from one source.
        let mut rung_errors: Vec<String> = Vec::new();
        if let Some(ref ct) = self.chaintracks {
            match ct.primary().find_chain_tip_header().await {
                Ok(header) => return Ok(header),
                Err(e) => {
                    tracing::warn!("Chaintracks tip header failed, trying BHS: {}", e);
                    rung_errors.push(format!("chaintracks: {}", e));
                }
            }
        }
        if let Some(ref bhs) = self.bhs {
            match bhs.find_chain_tip_header().await {
                Ok(header) => return Ok(header),
                Err(e) => {
                    tracing::warn!("BHS tip header failed: {}", e);
                    rung_errors.push(format!("bhs: {}", e));
                }
            }
        }
        Err(Error::ServiceError(header_ladder_error(
            "get_chain_tip_header",
            &rung_errors,
        )))
    }

    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        // Try Chaintracks first
        let mut rung_errors: Vec<String> = Vec::new();
        if let Some(ref ct) = self.chaintracks {
            match ct.primary().find_header_for_height(height).await {
                Ok(header) => return Ok(header.to_binary()),
                Err(e) => {
                    tracing::warn!("Chaintracks header failed, trying BHS: {}", e);
                    rung_errors.push(format!("chaintracks: {}", e));
                }
            }
        }
        // Fall back to BHS
        if let Some(ref bhs) = self.bhs {
            match bhs.chain_header_by_height(height).await {
                Ok(header) => return Ok(header.to_binary()),
                Err(e) => {
                    tracing::warn!("BHS header failed: {}", e);
                    rung_errors.push(format!("bhs: {}", e));
                }
            }
        }
        Err(Error::ServiceError(header_ladder_error(
            "get_header_for_height",
            &rung_errors,
        )))
    }

    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        // The header service first (preferred, first party, no rate limits).
        let header_service_error = if let Some(ref ct) = self.chaintracks {
            match ct.primary().find_header_for_block_hash(hash).await {
                Ok(header) => return Ok(header),
                Err(e) => e.to_string(),
            }
        } else {
            "no header service configured".to_string()
        };

        // No explorer in the proof path (P0-1c): this header resolves a TSC
        // proof's block and repairs stored proof rows, so an explorer is
        // asked only under the break-glass setting.
        if !self.options.break_glass_explorer_headers {
            return Err(Error::ServiceError(format!(
                "hash_to_header: the header service gave no header for {} ({}); the explorer header fallback is off (break-glass setting break_glass_explorer_headers)",
                hash, header_service_error
            )));
        }

        // Break-glass (Rule 28, T3 and T4): a header by hash. The header
        // service holds every header and gave none; nothing else we run
        // holds it. The two explorers are each other's fallback: the start
        // rotates, a fault falls through to the other, "no such header"
        // needs both to say so, and an answer counts only when its fields
        // hash to the hash asked for.
        let start = self.hash_to_header_start.fetch_add(1, Ordering::Relaxed);
        let mut absent = 0usize;
        let mut faults: Vec<String> = Vec::new();
        for turn in 0..EXPLORER_HEADER_SOURCES {
            let source = (start + turn) % EXPLORER_HEADER_SOURCES;
            let name = if source == 0 {
                "WhatsOnChain"
            } else {
                "Bitails"
            };
            tracing::warn!(
                hash = %hash,
                marker = "break_glass_explorer_header",
                error = %header_service_error,
                explorer = name,
                "break-glass: the header service gave no header; asking an explorer"
            );
            let answer = if source == 0 {
                self.whatsonchain.get_block_header_by_hash(hash).await
            } else {
                self.bitails.get_block_header_by_hash(hash).await
            };
            match answer {
                Ok(Some(header)) => {
                    let mut computed = sha256d(&header.to_binary());
                    computed.reverse();
                    let computed = hex::encode(computed);
                    if computed.eq_ignore_ascii_case(hash) {
                        return Ok(header);
                    }
                    faults.push(format!(
                        "{}: answered with a header that hashes to {}",
                        name, computed
                    ));
                }
                Ok(None) => absent += 1,
                Err(e) => faults.push(format!("{}: {}", name, e)),
            }
        }

        if absent == EXPLORER_HEADER_SOURCES {
            return Err(Error::NotFound {
                entity: "BlockHeader".to_string(),
                id: hash.to_string(),
            });
        }
        // Could not look: at least one explorer gave no usable answer, so
        // "no such header" is not known.
        Err(Error::ServiceError(format!(
            "hash_to_header: could not look up {}: the header service: {}; {}",
            hash,
            header_service_error,
            faults.join("; ")
        )))
    }

    /// Break-glass (Rule 28, T8 and T9): a transaction's bytes by txid. Our
    /// own storage holds our own transactions; for a foreign ancestor the
    /// sender's BEEF did not carry, nothing we hold has them, so the
    /// explorers are asked as couriers, each the other's fallback. The
    /// answer is self-verifying: the bytes must hash to the txid.
    ///
    /// With no bytes, "not found" (every explorer asked has no such
    /// transaction) is kept apart from "could not look"
    /// ([`GetRawTxResult::could_not_look`]).
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

        // The providers that could not look, by name. A provider that
        // answered "no such transaction" adds nothing here.
        let mut faults: Vec<String> = Vec::new();

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
                    if result.could_not_look || result.error.is_some() {
                        faults.push(format!(
                            "{}: {}",
                            provider_name,
                            result.error.as_deref().unwrap_or("could not look")
                        ));
                    }
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), call_error_code(&e));
                    lock_write(&self.get_raw_tx_services)?.add_call_error(&provider_name, call);
                    faults.push(format!("{}: {}", provider_name, e));
                }
            }
        }

        // "Not found" is every provider answering "no such transaction".
        // One that could not look makes the absence unknown, whichever
        // order they were asked in.
        let could_not_look = !faults.is_empty();
        Ok(GetRawTxResult {
            name: "Services".to_string(),
            txid: txid.to_string(),
            raw_tx: None,
            error: could_not_look.then(|| faults.join("; ")),
            could_not_look,
        })
    }

    /// Break-glass where it reaches an explorer (Rule 28, T6 and T7): a
    /// transaction's inclusion proof. For a transaction we received, the
    /// BEEF's own BUMP is the proof; for one we broadcast through Arcade,
    /// the BUMP in Arcade's MINED document is. An explorer is asked only
    /// as the courier of a proof nobody pushed to us (a transaction
    /// broadcast before Arcade was wired, an ancestor received without its
    /// proof), each explorer the other's fallback, and its proof is
    /// believed only for its root, checked against the header service.
    ///
    /// So with no header service configured no proof is fetched and none
    /// is returned: the answer is `merkle_path: None` with a fault note
    /// ("could not look"), never an unchecked proof and never "not mined".
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        if self.chaintracks.is_none() {
            let error = "no header service configured (chaintracks_url): a merkle proof cannot be checked, so none is fetched or returned";
            return Ok(GetMerklePathResult {
                name: Some("Services".to_string()),
                merkle_path: None,
                header: None,
                error: Some(error.to_string()),
                notes: vec![merkle_path_note(
                    "Services",
                    "getMerklePathTrackerError",
                    Some(error),
                )],
            });
        }

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
                        tracing::debug!(
                            provider = %provider_name,
                            txid = %txid,
                            "get_merkle_path: proof served"
                        );

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
                        // Continue to next provider so Bitails/ARC get a chance with a different block hash.
                        if result.merkle_path.is_some() && result.header.is_none() {
                            tracing::warn!(
                                txid = %txid,
                                provider = %provider_name,
                                "Dropping merkle path: header could not be resolved. Trying next provider."
                            );
                            result.merkle_path = None;
                            let mut fail_call = ServiceCall::new();
                            fail_call.mark_failure(Some("header resolution failed".to_string()));
                            lock_write(&self.get_merkle_path_services)?
                                .add_call_failure(&provider_name, fail_call);
                            last_error = Some(format!(
                                "Provider {} returned proof with unresolvable header for txid {}",
                                provider_name, txid
                            ));
                            // A fault, not evidence (the per-provider verdict).
                            notes.push(merkle_path_note(
                                &provider_name,
                                "getMerklePathHeaderUnresolved",
                                Some("header could not be resolved"),
                            ));
                            continue;
                        }

                        // Layer 3 (Service-layer validation): Validate the computed
                        // merkle root against ChainTracker BEFORE returning.
                        // This mirrors Go's whatsonchain/service.go where bad proofs
                        // trigger automatic provider failover via the service loop.
                        // Every failure leaves a per-provider verdict note: the
                        // tracker's definite false is a REFUTATION (retained and
                        // retried by the re-prove), everything else a FAULT.
                        if let Some(ref mp_hex) = result.merkle_path {
                            if let Some(ref ct) = self.chaintracks {
                                if let Some(ref header) = result.header {
                                    let failure: Option<(&str, String)> = match hex::decode(mp_hex)
                                    {
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
                                                            Ok(true) => None,
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
                                                                Some((
                                                                    NOTE_REFUTED,
                                                                    format!(
                                                                        "root {} refuted at height {}",
                                                                        computed_root, header.height
                                                                    ),
                                                                ))
                                                            }
                                                            Err(e) => {
                                                                tracing::warn!(
                                                                    txid = %txid,
                                                                    provider = %provider_name,
                                                                    error = %e,
                                                                    "ChainTracker error during service-layer \
                                                                     merkle root validation. Trying next provider."
                                                                );
                                                                Some((
                                                                    "getMerklePathTrackerError",
                                                                    e.to_string(),
                                                                ))
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
                                                        Some((
                                                            "getMerklePathBadProof",
                                                            e.to_string(),
                                                        ))
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
                                                    Some(("getMerklePathBadProof", e.to_string()))
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
                                            Some(("getMerklePathBadProof", e.to_string()))
                                        }
                                    };

                                    if let Some((what, error)) = failure {
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
                                        notes.push(merkle_path_note(
                                            &provider_name,
                                            what,
                                            Some(&error),
                                        ));
                                        continue;
                                    }
                                }
                            }
                        }

                        // The served answer carries every provider's verdict so far.
                        result.notes = notes;
                        return Ok(result);
                    } else {
                        call.mark_failure(Some("no proof".to_string()));
                        lock_write(&self.get_merkle_path_services)?
                            .add_call_failure(&provider_name, call);
                        last_error = result.error.clone();
                    }
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), call_error_code(&e));
                    lock_write(&self.get_merkle_path_services)?
                        .add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                    // A fault, not evidence (the per-provider verdict).
                    notes.push(merkle_path_note(
                        &provider_name,
                        "getMerklePathError",
                        Some(&e.to_string()),
                    ));
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

    fn set_broadcast_memory(&self, memory: StdArc<dyn BroadcastMemory>) {
        match lock_write(&self.broadcast_memory) {
            Ok(mut slot) => *slot = Some(memory),
            Err(e) => tracing::warn!(error = %e, "could not attach the broadcast memory"),
        }
    }

    fn broadcast_memory(&self) -> Option<StdArc<dyn BroadcastMemory>> {
        lock_read(&self.broadcast_memory)
            .ok()
            .and_then(|slot| slot.clone())
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        // Get owned copies of services to avoid holding lock across await
        let mut all_services: Vec<(String, String, PostBeefProvider)> = {
            let services = lock_read(&self.post_beef_services)?;
            services.all_services_owned()
        };

        if all_services.is_empty() {
            return Err(Error::NoServicesAvailable);
        }

        let subject = txids.last().cloned().unwrap_or_default();
        let memory = self.broadcast_memory();

        // Sticky provider order: the provider that accepted the previous
        // broadcast is tried first (never ahead of a configured Arcade
        // unless it IS Arcade). The static order and the in-memory demotion
        // of a failing provider (`move_to_last`) stay as they were.
        let mut sticky: Option<String> = None;
        if let Some(memory) = &memory {
            match memory.get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER).await {
                Ok(Some(name)) => {
                    apply_sticky_provider_order(&mut all_services, &name, PROVIDER_ARCADE_V2);
                    sticky = Some(name);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    "broadcast memory: could not read the sticky provider; using the static order"
                ),
            }
        }

        // The subject's unproven ancestors decide what each provider may
        // skip. Parsed once per broadcast; nothing to look up without a
        // memory.
        let unproven_ancestors: Vec<String> = if memory.is_some() {
            unproven_ancestors_in_beef(beef, &subject)
        } else {
            Vec::new()
        };

        let mut results = Vec::new();

        match self.post_beef_mode {
            PostBeefMode::UntilSuccess => {
                for (_service_name, provider_name, service) in all_services {
                    // One record query per provider actually tried (usually
                    // one per broadcast), plus at most one Arcade status
                    // probe when the oldest evidence is stale.
                    let seen: HashSet<String> = match &memory {
                        Some(memory) if !unproven_ancestors.is_empty() => {
                            self.skippable_ancestors(
                                memory.as_ref(),
                                &provider_name,
                                &unproven_ancestors,
                            )
                            .await
                        }
                        _ => HashSet::new(),
                    };

                    let mut call = ServiceCall::new();
                    let started = std::time::Instant::now();
                    match service.post_beef_seen(beef, txids, &seen).await {
                        Ok((result, delivery)) => {
                            let is_success = result.is_success();
                            if is_success {
                                call.mark_success(None);
                                lock_write(&self.post_beef_services)?
                                    .add_call_success(&provider_name, call);
                                tracing::info!(
                                    name = %provider_name,
                                    txid = %subject,
                                    reduced = delivery.reduced,
                                    bytes = delivery.bytes_sent,
                                    fallback_full = delivery.fallback_full,
                                    seen = seen.len(),
                                    unproven_ancestors = unproven_ancestors.len(),
                                    ms = started.elapsed().as_millis(),
                                    "broadcast accepted"
                                );
                                if let Some(memory) = &memory {
                                    self.remember_acceptance(
                                        memory.as_ref(),
                                        &provider_name,
                                        txids,
                                        &delivery,
                                        sticky.as_deref(),
                                    )
                                    .await;
                                }
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
                            call.mark_error(&e.to_string(), call_error_code(&e));
                            lock_write(&self.post_beef_services)?
                                .add_call_error(&provider_name, call);
                        }
                    }
                }
            }
            PostBeefMode::PromiseAll => {
                // Post to all services in parallel (always the full package)
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
                                if let Some(memory) = &memory {
                                    let delivery =
                                        PostBeefDelivery::full_package(beef.len(), &r, txids);
                                    self.remember_acceptance(
                                        memory.as_ref(),
                                        provider_name,
                                        txids,
                                        &delivery,
                                        sticky.as_deref(),
                                    )
                                    .await;
                                }
                            } else {
                                call.mark_failure(Some(r.status.clone()));
                                lock_write(&self.post_beef_services)?
                                    .add_call_failure(provider_name, call);
                            }
                            results.push(r);
                        }
                        Err(e) => {
                            call.mark_error(&e.to_string(), call_error_code(&e));
                            lock_write(&self.post_beef_services)?
                                .add_call_error(provider_name, call);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Break-glass (Rule 28, T10): is an output unspent. Headers and proofs
    /// prove inclusion, never that an output is unspent, and our own
    /// outputs table knows only the spends we made, so the explorers
    /// (WhatsOnChain, BananaBlocks, Bitails) are asked, each the others'
    /// fallback (the owner's word of 2026-10-22, bsv-wallet-cli#50: a
    /// negative, "unspent" or "absent", needs a second provider's clean
    /// answer; "could not look" is never "nothing there"). The shape:
    ///
    /// * the start rotates, one explorer on from the last call's;
    /// * an unspent (the outpoint is in WhatsOnChain's or Bitails' unspent
    ///   set for the script, or BananaBlocks says `spent: false` for an
    ///   output whose script is the asked one) is returned only when
    ///   [`UTXO_POSITIVE_PROVIDERS`] explorers give it (two of the three);
    /// * a "not listed" is returned only when [`UTXO_NEGATIVE_PROVIDERS`]
    ///   explorers give it (two of the three);
    /// * the ladder keeps asking until two agree or no rung is left, so
    ///   the verdict does not depend on where the rotation started;
    /// * a fault (a 429, a timeout, a 5xx, a malformed body, a BananaBlocks
    ///   output whose script is not the asked one, a rung closed by an
    ///   earlier 429's Retry-After) is "could not look" and moves the
    ///   question to the next rung within the same call, before any retry
    ///   round; a rate-limited rung costs no request and no wait;
    /// * the one retry round, after a 2 s pause, re-asks only a transient
    ///   fault (a 5xx, a 408, a timeout, a transport failure; see
    ///   `FaultKind`): a 404 that is an answer, a malformed body or a script
    ///   mismatch would come back the same, and a 429's rung is closed;
    /// * BananaBlocks answers an outpoint only: for a question without one
    ///   it is not applicable, neither asked nor counted;
    /// * anything else (an outage, one unspent and one not listed, a lone
    ///   answer no second explorer gave) is `status: "error"` with
    ///   `is_utxo: None`: "could not look".
    ///
    /// A spender named and bound by its own bytes (`is_utxo`'s
    /// `spend_evidence`) stands on one provider: the bytes are the evidence.
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        self.utxo_ladder(output, output_format, outpoint, use_next)
            .await
            .map(|(result, _spend)| result)
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
        let mut answer: Option<GetStatusForTxidsResult> = None;
        // The txids a chain index has answered so far (see
        // `merge_status_results`).
        let mut decided: HashSet<String> = HashSet::new();
        // Per txid, how many chain indexes answered `unknown`: the absence
        // stands only on [`UTXO_NEGATIVE_PROVIDERS`] of them.
        let mut absent_by: HashMap<String, usize> = HashMap::new();

        for (_service_name, provider_name, service) in all_services {
            // The first provider is asked about everything. Every provider
            // after it is asked ONLY about the txids no earlier provider
            // placed as MINED, and its answers are merged under the rule of
            // `merge_status_results`.
            //
            // `unknown` is a gap, not a verdict: Arcade knows the
            // transactions it was handed and answers "unknown" for anything
            // broadcast before it was wired up; left as-is, that would
            // retire a transaction the chain has long since mined.
            //
            // `known` from a broadcaster is not a verdict either: it is the
            // broadcaster's word that it holds the transaction. Arcade kept
            // answering `ACCEPTED_BY_NETWORK` for three transactions the
            // chain had mined two days earlier (the soak wallet,
            // 2026-09-02..04), and `SEEN_MULTIPLE_NODES` for phantoms the
            // chain index never saw. A `known` that stopped the fan-out here
            // left the mined ones unproven forever (no chain index was ever
            // asked, so no proof was ever fetched) and let the phantoms pass
            // the reconciler's alive check every pass. Only `mined` ends the
            // question; the chain indexes answer the rest, and two of their
            // `unknown`s make the absence (see `merge_status_results`).
            let pending: Vec<String> = match &answer {
                None => txids.to_vec(),
                Some(previous) => txids
                    .iter()
                    .filter(|txid| {
                        !previous
                            .results
                            .iter()
                            .any(|d| &d.txid == *txid && d.status == "mined")
                    })
                    .cloned()
                    .collect(),
            };
            if pending.is_empty() {
                break;
            }

            let mut call = ServiceCall::new();
            match service.get_status_for_txids(&pending).await {
                Ok(result) if result.status == "success" => {
                    call.mark_success(None);
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_success(&provider_name, call);
                    let chain_index = service.is_chain_index();
                    tracing::debug!(
                        provider = %provider_name,
                        chain_index,
                        asked = pending.len(),
                        placed = result
                            .results
                            .iter()
                            .filter(|d| d.status != "unknown")
                            .count(),
                        "get_status_for_txids: provider answered"
                    );
                    if chain_index {
                        for d in result.results.iter().filter(|d| d.status == "unknown") {
                            *absent_by.entry(d.txid.clone()).or_default() += 1;
                        }
                    }
                    match answer {
                        None => {
                            if chain_index {
                                decided.extend(result.results.iter().map(|d| d.txid.clone()));
                            }
                            answer = Some(result);
                        }
                        Some(ref mut previous) => {
                            let before: Vec<(String, String)> = previous
                                .results
                                .iter()
                                .map(|d| (d.txid.clone(), d.status.clone()))
                                .collect();
                            merge_status_results(previous, result, chain_index, &mut decided);
                            for (txid, was) in before {
                                let now = previous
                                    .results
                                    .iter()
                                    .find(|d| d.txid == txid)
                                    .map(|d| d.status.as_str())
                                    .unwrap_or("unknown");
                                if was != now {
                                    tracing::debug!(
                                        txid = %txid,
                                        provider = %provider_name,
                                        was = %was,
                                        now = %now,
                                        "get_status_for_txids: a later provider changed the answer"
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(result) => {
                    call.mark_failure(result.error.clone());
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_failure(&provider_name, call);
                    last_error = result.error.clone();
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), call_error_code(&e));
                    lock_write(&self.get_status_for_txids_services)?
                        .add_call_error(&provider_name, call);
                    last_error = Some(e.to_string());
                }
            }
        }

        if let Some(mut answer) = answer {
            // A negative needs two providers' clean answers: a non-mined
            // txid two chain indexes answered `unknown` for is `unknown`,
            // even against a broadcaster's `known` (a phantom). Any other
            // `unknown` (one chain index's alone, a broadcaster's inbox,
            // BananaBlocks' index, which lags the mempool) is a gap, not
            // "absent": it is left out, and so is a txid no provider
            // answered (one stopped early); the answer says it could not
            // look for those.
            for d in answer.results.iter_mut() {
                let absent = absent_by.get(&d.txid).copied().unwrap_or(0);
                if d.status != "mined" && absent >= UTXO_NEGATIVE_PROVIDERS {
                    *d = TxStatusDetail::new(d.txid.clone(), "unknown", None);
                }
            }
            answer.results.retain(|d| {
                d.status != "unknown"
                    || absent_by.get(&d.txid).copied().unwrap_or(0) >= UTXO_NEGATIVE_PROVIDERS
            });
            let gaps: Vec<&str> = txids
                .iter()
                .filter(|t| !answer.results.iter().any(|d| d.txid == **t))
                .map(String::as_str)
                .collect();
            answer.error = if gaps.is_empty() {
                None
            } else {
                tracing::debug!(txids = ?gaps, error = ?last_error, "get_status_for_txids: no chain index answered");
                Some(format!(
                    "could not look: no chain index answered {}",
                    gaps.join(", ")
                ))
            };
            if answer.results.is_empty() {
                answer.status = "error".to_string();
            }
            // In the order asked.
            answer.results.sort_by_key(|d| {
                txids
                    .iter()
                    .position(|t| *t == d.txid)
                    .unwrap_or(usize::MAX)
            });
            return Ok(answer);
        }

        Ok(GetStatusForTxidsResult {
            name: "Services".to_string(),
            status: "error".to_string(),
            error: last_error,
            results: Vec::new(),
        })
    }

    #[cfg(feature = "break-glass-script-history")]
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
                    call.mark_error(&e.to_string(), call_error_code(&e));
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
        hex::encode(hash)
    }

    async fn get_utxo_leads(
        &self,
        script_hash: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
    ) -> Result<UtxoLeads> {
        self.utxo_leads(script_hash, output_format).await
    }

    /// The stranger's spend (Rule 28, the irreducible case). Two explorers
    /// each answering "unspent" is `UnspentHint`. Otherwise the spend is
    /// asked for as a proof: `Spent` only when the spending transaction's
    /// own bytes name the outpoint and its merkle path meets the header
    /// service's header. Two explorers agreeing "not unspent", or a bound
    /// spender with no proof held (one provider's name is enough: its bytes
    /// are the evidence), is `SpentHint`; anything else `Unknown`. An empty
    /// `locking_script` (none stored) is asked as the empty script's hash:
    /// no explorer lists an output under it, and BananaBlocks' answer for
    /// an output with another script is a fault, so it is never
    /// `UnspentHint`.
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> UtxoVerdict {
        let hash = self.hash_output_script(locking_script);
        let outpoint = format!("{}.{}", txid, vout);
        let (unspent_set, carried) = match self
            .utxo_ladder(&hash, None, Some(&outpoint), false)
            .await
        {
            Ok((result, carried)) => {
                let verdict = UtxoVerdict::from_status(&result);
                if verdict == UtxoVerdict::Unknown {
                    tracing::debug!(outpoint = %outpoint, error = ?result.error, "is_utxo: could not look");
                }
                (verdict, carried)
            }
            Err(e) => {
                tracing::debug!(outpoint = %outpoint, error = %e, "is_utxo: could not look");
                (UtxoVerdict::Unknown, None)
            }
        };
        // A spender bound by its own bytes stands on one provider, even
        // against two unspent answers: an index can miss a mempool spend
        // (2026-10-22: WhatsOnChain's `/spent` named a mempool spender of an
        // outpoint BananaBlocks answered `spent: false` for), and the
        // spender's bytes are the evidence.
        let evidence = self.spend_evidence(txid, vout, carried).await;
        if unspent_set == UtxoVerdict::UnspentHint && evidence != SpendEvidence::None {
            tracing::warn!(outpoint = %outpoint, evidence = ?evidence, "is_utxo: explorers list the output as unspent, and a spender its bytes bind is named; the spender stands");
        }
        match evidence {
            SpendEvidence::Proven => UtxoVerdict::Spent,
            SpendEvidence::Bound => UtxoVerdict::SpentHint,
            SpendEvidence::None => unspent_set,
        }
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

    async fn get_broadcaster_statuses(&self, txid: &str) -> Vec<(String, BroadcastStatus)> {
        // Arcade's status document and each classic ARC's `GET /v1/tx`, all
        // at once. A broadcaster that errors says nothing; a 404 is
        // `Unknown` (it does not hold the transaction).
        let arcade = async {
            match &self.arcade {
                Some(arcade) => match arcade.get_tx_status(txid).await {
                    Ok(Some(info)) => Some((
                        PROVIDER_ARCADE_V2.to_string(),
                        BroadcastStatus::from_arcade_status(&info.tx_status.to_ascii_uppercase()),
                    )),
                    Ok(None) => Some((PROVIDER_ARCADE_V2.to_string(), BroadcastStatus::Unknown)),
                    Err(e) => {
                        tracing::debug!(txid = %txid, error = %e, "broadcaster status: Arcade gave no answer");
                        None
                    }
                },
                None => None,
            }
        };
        let arc_status = |name: &'static str, arc: Option<StdArc<Arc>>| async move {
            let arc = arc?;
            match arc.get_tx_data(txid).await {
                Ok(Some(info)) => Some((
                    name.to_string(),
                    BroadcastStatus::from_arc_status(&info.tx_status.to_ascii_uppercase()),
                )),
                Ok(None) => Some((name.to_string(), BroadcastStatus::Unknown)),
                Err(e) => {
                    tracing::debug!(txid = %txid, provider = name, error = %e, "broadcaster status: ARC gave no answer");
                    None
                }
            }
        };
        let (arcade, taal, gorillapool) = tokio::join!(
            arcade,
            arc_status(PROVIDER_TAAL_ARC, Some(StdArc::clone(&self.arc_taal))),
            arc_status(PROVIDER_GORILLAPOOL_ARC, self.arc_gorillapool.clone()),
        );
        [arcade, taal, gorillapool].into_iter().flatten().collect()
    }

    fn get_services_call_history(&self, reset: bool) -> ServicesCallHistory {
        // Delegate to the inherent method, falling back to empty on error
        Services::get_services_call_history(self, reset).unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "rule_28_tests.rs"]
mod rule_28_tests;

#[cfg(test)]
#[path = "three_explorer_tests.rs"]
mod three_explorer_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::providers::ArcadeConfig;
    use crate::services::traits::TxStatusDetail;

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
    // Provider order: Arcade first when configured
    // =========================================================================

    /// Provider names of a collection, in the order they will be tried.
    fn provider_order<S: Clone>(collection: &RwLock<ServiceCollection<S>>) -> Vec<String> {
        collection
            .read()
            .unwrap()
            .all_services_from_current()
            .into_iter()
            .map(|(_service, provider, _s)| provider)
            .collect()
    }

    fn arcade_services() -> Services {
        Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().with_arcade(
                crate::services::ARCADE_V2_MAINNET,
                Some(ArcadeConfig::with_callback_token("tok")),
            ),
        )
        .unwrap()
    }

    #[test]
    fn test_arcade_leads_the_read_collections_when_configured() {
        let services = arcade_services();

        assert_eq!(
            provider_order(&services.get_merkle_path_services),
            vec!["ArcadeV2", "WhatsOnChain", "BananaBlocks", "Bitails"],
            "a wallet on Arcade must not touch WoC for a proof Arcade has"
        );
        assert_eq!(
            provider_order(&services.get_status_for_txids_services),
            vec!["ArcadeV2", "WhatsOnChain", "Bitails", "BananaBlocks"],
        );
    }

    #[test]
    fn test_read_collection_order_unchanged_without_arcade() {
        let services = Services::mainnet().unwrap();

        assert!(services.arcade.is_none());
        assert_eq!(
            provider_order(&services.get_merkle_path_services),
            vec!["WhatsOnChain", "BananaBlocks", "Bitails"],
        );
        assert_eq!(
            provider_order(&services.get_status_for_txids_services),
            vec!["WhatsOnChain", "Bitails", "BananaBlocks"],
        );
    }

    /// The three explorers in every read ladder, BananaBlocks on by default
    /// on both chains (test.bananablocks.com serves testnet), off with an
    /// empty base, and at the base given.
    #[test]
    fn bananablocks_is_the_third_explorer_and_can_be_turned_off() {
        for chain in [Chain::Main, Chain::Test] {
            let services = Services::new(chain).unwrap();
            assert_eq!(
                provider_order(&services.get_raw_tx_services),
                vec!["WhatsOnChain", "BananaBlocks", "Bitails"],
                "{chain:?}"
            );
            assert_eq!(
                provider_order(&services.get_utxo_status_services),
                vec!["WhatsOnChain", "BananaBlocks", "Bitails"],
                "{chain:?}"
            );
        }
        assert_eq!(
            Services::testnet()
                .unwrap()
                .bananablocks
                .as_ref()
                .map(|bb| bb.base_url().to_string()),
            Some(crate::services::providers::BANANABLOCKS_TESTNET_URL.to_string())
        );

        let off = Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().without_bananablocks(),
        )
        .unwrap();
        assert!(off.bananablocks.is_none());
        for order in [
            provider_order(&off.get_raw_tx_services),
            provider_order(&off.get_utxo_status_services),
            provider_order(&off.get_merkle_path_services),
            provider_order(&off.get_status_for_txids_services),
        ] {
            assert!(!order.iter().any(|p| p == "BananaBlocks"), "{order:?}");
        }
        let empty = Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().with_bananablocks_base("  "),
        )
        .unwrap();
        assert!(empty.bananablocks.is_none(), "an empty base is off");

        let mirror = Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().with_bananablocks_base("https://mirror.example/api/v1/"),
        )
        .unwrap();
        assert_eq!(
            mirror.bananablocks.as_ref().unwrap().base_url(),
            "https://mirror.example/api/v1"
        );
    }

    // =========================================================================
    // Status merge: `unknown` is a gap, not a verdict
    // =========================================================================

    fn detail(txid: &str, status: &str) -> TxStatusDetail {
        TxStatusDetail::new(txid, status, None)
    }

    fn status_result(name: &str, results: Vec<TxStatusDetail>) -> GetStatusForTxidsResult {
        GetStatusForTxidsResult {
            name: name.to_string(),
            status: "success".to_string(),
            error: None,
            results,
        }
    }

    fn statuses(result: &GetStatusForTxidsResult) -> Vec<(&str, &str)> {
        result
            .results
            .iter()
            .map(|d| (d.txid.as_str(), d.status.as_str()))
            .collect()
    }

    #[test]
    fn test_merge_status_results_fills_the_gaps_and_keeps_mined() {
        let mut first = status_result(
            "ArcadeV2",
            vec![
                detail("aa", "mined"),
                detail("bb", "unknown"),
                detail("cc", "unknown"),
            ],
        );
        let mut decided = HashSet::new();
        // The later provider disagrees about "aa" (it must not win), places
        // "bb", and cannot place "cc" either.
        let second = status_result(
            "WhatsOnChain",
            vec![
                detail("aa", "known"),
                detail("bb", "mined"),
                detail("cc", "unknown"),
            ],
        );

        merge_status_results(&mut first, second, true, &mut decided);

        assert_eq!(
            statuses(&first),
            vec![("aa", "mined"), ("bb", "mined"), ("cc", "unknown")],
            "mined is final, the gap is filled, the unplaced stays unplaced"
        );
        assert_eq!(decided.len(), 3, "a chain index decided all three");
    }

    /// The soak wallet, 2026-09-02..04: Arcade answered `ACCEPTED_BY_NETWORK`
    /// (`known`) for three transactions WhatsOnChain had at 290+
    /// confirmations. The broadcaster's `known` is not a verdict: the chain
    /// index's `mined` takes the slot, so the proof pass fetches the proof.
    #[test]
    fn test_a_broadcasters_known_yields_to_the_chain_index_mined() {
        let mut first = status_result("ArcadeV2", vec![detail("f5", "known")]);
        let mut decided = HashSet::new();
        let woc = status_result(
            "WhatsOnChain",
            vec![TxStatusDetail::new("f5", "mined", Some(299))],
        );
        merge_status_results(&mut first, woc, true, &mut decided);
        assert_eq!(statuses(&first), vec![("f5", "mined")]);
        assert_eq!(first.results[0].depth, Some(299));
    }

    /// Among chain indexes, one that knows the transaction beats one that
    /// does not, in either order; and a chain index's `known` is never
    /// downgraded by a later chain index's `unknown`.
    #[test]
    fn test_a_chain_index_that_knows_beats_one_that_does_not() {
        let mut first = status_result("WhatsOnChain", vec![detail("k", "unknown")]);
        let mut decided: HashSet<String> = ["k".to_string()].into_iter().collect();
        let bitails = status_result("Bitails", vec![detail("k", "known")]);
        merge_status_results(&mut first, bitails, true, &mut decided);
        assert_eq!(statuses(&first), vec![("k", "known")]);

        let mut first = status_result("WhatsOnChain", vec![detail("k", "known")]);
        let mut decided: HashSet<String> = ["k".to_string()].into_iter().collect();
        let bitails = status_result("Bitails", vec![detail("k", "unknown")]);
        merge_status_results(&mut first, bitails, true, &mut decided);
        assert_eq!(statuses(&first), vec![("k", "known")]);
    }

    /// A broadcaster asked after a chain index (the round-robin rotation)
    /// fills gaps and carries proofs, but never overrides the chain index's
    /// `known`; its `known` against the chain index's `unknown` is a second
    /// provider contradicting a lone "absent", which never stands alone
    /// (the owner's rule; review M2 of #43).
    #[test]
    fn test_a_broadcaster_never_overrides_a_chain_index() {
        let mut first = status_result(
            "WhatsOnChain",
            vec![
                detail("a", "unknown"),
                detail("b", "known"),
                detail("c", "known"),
            ],
        );
        let mut decided: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let arcade = status_result(
            "ArcadeV2",
            vec![
                detail("a", "known"),
                detail("b", "unknown"),
                TxStatusDetail::new("c", "mined", Some(1)),
            ],
        );
        merge_status_results(&mut first, arcade, false, &mut decided);
        assert_eq!(
            statuses(&first),
            vec![("a", "known"), ("b", "known"), ("c", "mined")],
            "a lone absent yields, no downgrade, mined with its proof taken"
        );

        // With no chain index having decided, the broadcaster's known fills
        // the gap (an outage never turns a held transaction into an absent one).
        let mut first = status_result("Bitails", vec![detail("a", "unknown")]);
        let mut decided = HashSet::new();
        let arcade = status_result("ArcadeV2", vec![detail("a", "known")]);
        merge_status_results(&mut first, arcade, false, &mut decided);
        assert_eq!(statuses(&first), vec![("a", "known")]);
    }

    /// A batch status provider for the fan-out cells: canned answers, and a
    /// record of what it was asked.
    struct MockStatusProvider {
        chain_index: bool,
        answers: std::collections::HashMap<String, TxStatusDetail>,
        fail: bool,
        asked: std::sync::Mutex<Vec<Vec<String>>>,
    }

    impl MockStatusProvider {
        fn new(chain_index: bool, answers: Vec<TxStatusDetail>) -> StdArc<Self> {
            StdArc::new(Self {
                chain_index,
                answers: answers.into_iter().map(|d| (d.txid.clone(), d)).collect(),
                fail: false,
                asked: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn failing(chain_index: bool) -> StdArc<Self> {
            StdArc::new(Self {
                chain_index,
                answers: Default::default(),
                fail: true,
                asked: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn asked(&self) -> Vec<Vec<String>> {
            self.asked.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl StatusForTxidsService for MockStatusProvider {
        async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
            self.asked.lock().unwrap().push(txids.to_vec());
            if self.fail {
                return Err(Error::ServiceError(
                    "HTTP 500 Internal Server Error".to_string(),
                ));
            }
            Ok(GetStatusForTxidsResult {
                name: "mock".to_string(),
                status: "success".to_string(),
                error: None,
                results: txids
                    .iter()
                    .map(|t| {
                        self.answers
                            .get(t)
                            .cloned()
                            .unwrap_or_else(|| detail(t, "unknown"))
                    })
                    .collect(),
            })
        }

        fn is_chain_index(&self) -> bool {
            self.chain_index
        }
    }

    /// 0.11.1's Arcade rule through 0.12.0's status ladder (#42 point 2 on
    /// #43's ladder): a holding word (`RECEIVED`, `SENT_TO_NETWORK`,
    /// `ACCEPTED_BY_NETWORK`) is `unknown` from `status_detail`, and a
    /// broadcaster's `unknown` is a gap: no verdict alone, never an absence
    /// vote, never over a chain index's answer. A SEEN word is a `known`
    /// that two chain indexes' `unknown`s still turn to `unknown`.
    #[tokio::test]
    async fn arcades_holding_word_is_a_gap_in_the_status_ladder() {
        use crate::services::providers::arcade::{status_detail, statuses};
        let txid = "cd".repeat(32);
        let arcade = |word: &str| {
            let info: crate::services::providers::ArcadeTxInfo =
                serde_json::from_value(serde_json::json!({ "txid": txid, "txStatus": word }))
                    .unwrap();
            MockStatusProvider::new(false, vec![status_detail(&txid, &info)])
        };
        let ask = |providers: Vec<(&str, StdArc<MockStatusProvider>)>| {
            let services = services_with_status_providers(providers);
            let txid = txid.clone();
            async move {
                let result = services.get_status_for_txids(&[txid], false).await.unwrap();
                result.results.first().map(|d| d.status.clone())
            }
        };
        for word in [
            statuses::RECEIVED,
            statuses::SENT_TO_NETWORK,
            statuses::ACCEPTED_BY_NETWORK,
        ] {
            // Alone (both chain indexes fault): could not look, not known.
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                ("WhatsOnChain", MockStatusProvider::failing(true)),
                ("Bitails", MockStatusProvider::failing(true)),
            ])
            .await;
            assert_eq!(got, None, "{word} alone promotes nothing");
            // One chain index's unknown and the other's fault: the holding
            // word is never the second absence vote.
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
                ("Bitails", MockStatusProvider::failing(true)),
            ])
            .await;
            assert_eq!(got, None, "{word} is never an absence vote");
            // A chain index's known stands.
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                (
                    "WhatsOnChain",
                    MockStatusProvider::new(true, vec![detail(&txid, "known")]),
                ),
                ("Bitails", MockStatusProvider::failing(true)),
            ])
            .await;
            assert_eq!(got.as_deref(), Some("known"), "{word}");
            // Two chain indexes' unknown is the absence.
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
                ("Bitails", MockStatusProvider::new(true, vec![])),
            ])
            .await;
            assert_eq!(got.as_deref(), Some("unknown"), "{word}");
        }
        for word in [statuses::SEEN_ON_NETWORK, statuses::SEEN_MULTIPLE_NODES] {
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                ("WhatsOnChain", MockStatusProvider::failing(true)),
                ("Bitails", MockStatusProvider::new(true, vec![])),
            ])
            .await;
            assert_eq!(got.as_deref(), Some("known"), "{word} against one unknown");
            let got = ask(vec![
                ("ArcadeV2", arcade(word)),
                ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
                ("Bitails", MockStatusProvider::new(true, vec![])),
            ])
            .await;
            assert_eq!(got.as_deref(), Some("unknown"), "{word} against two");
        }
    }

    fn services_with_status_providers(
        providers: Vec<(&str, StdArc<MockStatusProvider>)>,
    ) -> Services {
        let mut services = Services::mainnet().unwrap();
        let mut collection = ServiceCollection::new("getStatusForTxids");
        for (name, provider) in providers {
            collection.add(name, provider as StatusForTxidsProvider);
        }
        services.get_status_for_txids_services = RwLock::new(collection);
        services
    }

    /// The fan-out over the real provider order (Arcade, then the chain
    /// indexes): Arcade's `mined` (with its proof) ends the question for
    /// that txid; its `known` is put to the chain index, which answers
    /// `mined` for the two-day-old soak transactions and `unknown` for the
    /// phantom; its `unknown` is a gap the chain index fills.
    #[tokio::test]
    async fn test_fan_out_puts_a_broadcasters_known_to_the_chain_index() {
        let mut arcade_mined = TxStatusDetail::new("m", "mined", Some(1));
        arcade_mined.merkle_path = Some("beef".to_string());
        let arcade = MockStatusProvider::new(
            false,
            vec![
                arcade_mined,
                detail("stale", "known"),
                detail("phantom", "known"),
                detail("old", "unknown"),
            ],
        );
        let woc = MockStatusProvider::new(
            true,
            vec![
                detail("m", "known"),
                TxStatusDetail::new("stale", "mined", Some(291)),
                detail("phantom", "unknown"),
                TxStatusDetail::new("old", "mined", Some(9601)),
            ],
        );
        let bitails = MockStatusProvider::new(true, vec![detail("phantom", "unknown")]);
        let services = services_with_status_providers(vec![
            ("ArcadeV2", arcade.clone()),
            ("WhatsOnChain", woc.clone()),
            ("Bitails", bitails.clone()),
        ]);
        let txids: Vec<String> = ["m", "stale", "phantom", "old"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let result = services.get_status_for_txids(&txids, false).await.unwrap();

        assert_eq!(result.status, "success");
        assert_eq!(
            statuses(&result),
            vec![
                ("m", "mined"),
                ("stale", "mined"),
                ("phantom", "unknown"),
                ("old", "mined")
            ]
        );
        assert_eq!(
            result.results[0].merkle_path.as_deref(),
            Some("beef"),
            "Arcade's proof is kept"
        );
        assert_eq!(
            result.results[1].depth,
            Some(291),
            "the chain index's depth"
        );
        assert_eq!(
            arcade.asked(),
            vec![txids.clone()],
            "the broadcaster is asked about everything"
        );
        assert_eq!(
            woc.asked(),
            vec![vec![
                "stale".to_string(),
                "phantom".to_string(),
                "old".to_string()
            ]],
            "the chain index is asked about everything Arcade did not place as mined"
        );
        assert_eq!(
            bitails.asked(),
            vec![vec!["phantom".to_string()]],
            "the second chain index only about what is still not mined"
        );
    }

    /// The 2026-09-02 phantom shape, at the ladder: Arcade
    /// `SEEN_MULTIPLE_NODES` (`known`) for hours, WhatsOnChain 404
    /// (`unknown`). After WhatsOnChain alone the broadcaster's `known`
    /// holds (one chain index's absence never stands alone); only when
    /// WhatsOnChain and Bitails both say `unknown` is it `unknown`, which
    /// the reconciler's alive check acts on; the respend gate's own
    /// broadcaster check refuses while Arcade still answers SEEN.
    #[tokio::test]
    async fn test_a_broadcasters_known_the_chain_index_never_saw_is_unknown() {
        let txids = vec!["x".to_string()];
        let after_woc_alone = services_with_status_providers(vec![
            (
                "ArcadeV2",
                MockStatusProvider::new(false, vec![detail("x", "known")]),
            ),
            ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
        ]);
        let result = after_woc_alone
            .get_status_for_txids(&txids, false)
            .await
            .unwrap();
        assert_eq!(statuses(&result), vec![("x", "known")]);
        assert_eq!(result.error, None);

        let after_both = services_with_status_providers(vec![
            (
                "ArcadeV2",
                MockStatusProvider::new(false, vec![detail("x", "known")]),
            ),
            ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
            ("Bitails", MockStatusProvider::new(true, vec![])),
        ]);
        let result = after_both
            .get_status_for_txids(&txids, false)
            .await
            .unwrap();
        assert_eq!(result.status, "success");
        assert_eq!(statuses(&result), vec![("x", "unknown")]);
        assert_eq!(result.error, None);
    }

    /// One chain index's `unknown` with the other faulted is "could not
    /// look", never a clean `unknown` (review 2 H1 of #43: bsv-wallet-cli's
    /// respend gate takes a clean `unknown` as leave to post a competitor).
    #[tokio::test]
    async fn test_one_chain_index_unknown_and_a_fault_is_could_not_look() {
        let services = services_with_status_providers(vec![
            ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
            ("Bitails", MockStatusProvider::failing(true)),
        ]);
        let result = services
            .get_status_for_txids(&["x".to_string()], false)
            .await
            .unwrap();
        assert_eq!(result.status, "error");
        assert!(result.results.is_empty(), "{:?}", result.results);
        assert_eq!(
            result.error.as_deref(),
            Some("could not look: no chain index answered x")
        );
    }

    /// Arcade `known`, WhatsOnChain `unknown`, Bitails 500: the broadcaster
    /// holds the transaction and only one chain index says it is absent.
    #[tokio::test]
    async fn test_a_broadcasters_known_against_one_unknown_and_a_fault_is_known() {
        let services = services_with_status_providers(vec![
            (
                "ArcadeV2",
                MockStatusProvider::new(false, vec![detail("x", "known")]),
            ),
            ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
            ("Bitails", MockStatusProvider::failing(true)),
        ]);
        let result = services
            .get_status_for_txids(&["x".to_string()], false)
            .await
            .unwrap();
        assert_eq!(result.status, "success");
        assert_eq!(statuses(&result), vec![("x", "known")]);
        assert_eq!(result.error, None);
    }

    /// Arcade `known`, WhatsOnChain and Bitails `unknown`: two chain
    /// indexes' clean absence is a negative from two, and it stands
    /// against the broadcaster's word.
    #[tokio::test]
    async fn test_two_chain_indexes_unknown_override_a_broadcasters_known() {
        let services = services_with_status_providers(vec![
            (
                "ArcadeV2",
                MockStatusProvider::new(false, vec![detail("x", "known")]),
            ),
            ("WhatsOnChain", MockStatusProvider::new(true, vec![])),
            ("Bitails", MockStatusProvider::new(true, vec![])),
        ]);
        let result = services
            .get_status_for_txids(&["x".to_string()], false)
            .await
            .unwrap();
        assert_eq!(statuses(&result), vec![("x", "unknown")]);
        assert_eq!(result.error, None);
    }

    /// The verdict does not depend on which provider is asked first (review
    /// 2 M1 of #43): over Arcade, WhatsOnChain and Bitails each answering
    /// `known`, `unknown` or a fault, the answer from the default order and
    /// from one on (`use_next`, WhatsOnChain first) is the same, and it is
    /// the rule: `known` when any provider holds the transaction and fewer
    /// than two chain indexes say `unknown`; `unknown` on two chain
    /// indexes' `unknown`; otherwise "could not look".
    #[tokio::test]
    async fn test_the_status_verdict_is_the_same_from_either_start() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Says {
            Known,
            Unknown,
            Fault,
        }
        let provider = |chain_index: bool, says: Says| match says {
            Says::Known => MockStatusProvider::new(chain_index, vec![detail("x", "known")]),
            Says::Unknown => MockStatusProvider::new(chain_index, vec![]),
            Says::Fault => MockStatusProvider::failing(chain_index),
        };
        let all = [Says::Known, Says::Unknown, Says::Fault];
        for arcade in all {
            for woc in all {
                for bitails in all {
                    let absent = [woc, bitails]
                        .iter()
                        .filter(|s| **s == Says::Unknown)
                        .count();
                    let held = [arcade, woc, bitails].contains(&Says::Known);
                    let expected = if absent == 2 {
                        Some("unknown")
                    } else if held {
                        Some("known")
                    } else {
                        None
                    };
                    for use_next in [false, true] {
                        let services = services_with_status_providers(vec![
                            ("ArcadeV2", provider(false, arcade)),
                            ("WhatsOnChain", provider(true, woc)),
                            ("Bitails", provider(true, bitails)),
                        ]);
                        let result = services
                            .get_status_for_txids(&["x".to_string()], use_next)
                            .await
                            .unwrap();
                        let got = result.results.first().map(|d| d.status.as_str());
                        assert_eq!(
                            got, expected,
                            "Arcade {arcade:?}, WhatsOnChain {woc:?}, Bitails {bitails:?}, use_next {use_next}"
                        );
                        assert_eq!(
                            result.error.is_some(),
                            expected.is_none(),
                            "could not look exactly when there is no verdict: {:?}",
                            result.error
                        );
                    }
                }
            }
        }
    }

    /// The status ladder over all 81 rows (review 3 of #43): Arcade,
    /// WhatsOnChain and Bitails each answering `known`, `unknown` or a
    /// fault, and BananaBlocks (the real provider, against a fixture in
    /// every row) answering `known`, a 404 or a 500; asked from each of the
    /// four starting rungs, each with and without `use_next`. The reading
    /// is the rule: `mined` wins (no row here holds one); WhatsOnChain and
    /// Bitails both `unknown` is `unknown`; else any provider's `known` is
    /// `known`; else "could not look".
    #[tokio::test]
    async fn test_the_status_verdict_is_the_rule_over_all_81_rows_from_both_starts() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Says {
            Known,
            Unknown,
            Fault,
        }
        let txid = "ab".repeat(32);
        let provider = |chain_index: bool, says: Says| match says {
            Says::Known => MockStatusProvider::new(chain_index, vec![detail(&txid, "known")]),
            Says::Unknown => MockStatusProvider::new(chain_index, vec![]),
            Says::Fault => MockStatusProvider::failing(chain_index),
        };
        let all = [Says::Known, Says::Unknown, Says::Fault];
        let mut bb = mockito::Server::new_async().await;
        let mut rows = 0;
        for bananablocks in all {
            let _b = match bananablocks {
                Says::Known => bb
                    .mock("GET", format!("/tx/{txid}").as_str())
                    .with_status(200)
                    .with_body(format!(
                        r#"{{"txid":"{txid}","block_hash":"","block_height":0}}"#
                    )),
                Says::Unknown => bb
                    .mock("GET", format!("/tx/{txid}").as_str())
                    .with_status(404),
                Says::Fault => bb
                    .mock("GET", format!("/tx/{txid}").as_str())
                    .with_status(500),
            }
            .create_async()
            .await;
            for arcade in all {
                for woc in all {
                    for bitails in all {
                        rows += 1;
                        let expected = if woc == Says::Unknown && bitails == Says::Unknown {
                            Some("unknown")
                        } else if [arcade, woc, bitails, bananablocks].contains(&Says::Known) {
                            Some("known")
                        } else {
                            None
                        };
                        for start in 0..4 {
                            for use_next in [false, true] {
                                let mut services = Services::mainnet().unwrap();
                                let mut collection = ServiceCollection::new("getStatusForTxids");
                                collection.add(
                                    "ArcadeV2",
                                    provider(false, arcade) as StatusForTxidsProvider,
                                );
                                collection.add(
                                    "WhatsOnChain",
                                    provider(true, woc) as StatusForTxidsProvider,
                                );
                                collection.add(
                                    "Bitails",
                                    provider(true, bitails) as StatusForTxidsProvider,
                                );
                                collection.add(
                                    "BananaBlocks",
                                    StdArc::new(BananaBlocks::with_base_unshared(&bb.url()))
                                        as StatusForTxidsProvider,
                                );
                                for _ in 0..start {
                                    collection.next();
                                }
                                services.get_status_for_txids_services = RwLock::new(collection);
                                let result = services
                                    .get_status_for_txids(std::slice::from_ref(&txid), use_next)
                                    .await
                                    .unwrap();
                                let row = format!(
                                    "Arcade {arcade:?}, WhatsOnChain {woc:?}, Bitails {bitails:?}, \
                                     BananaBlocks {bananablocks:?}, start {start}, use_next {use_next}: \
                                     {result:?}"
                                );
                                let got = result.results.first().map(|d| d.status.as_str());
                                assert_eq!(got, expected, "{row}");
                                assert_eq!(result.error.is_some(), expected.is_none(), "{row}");
                                assert_eq!(
                                    result.status,
                                    if expected.is_some() {
                                        "success"
                                    } else {
                                        "error"
                                    },
                                    "{row}"
                                );
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(rows, 81);
    }

    /// Every chain index down: the broadcaster's `known` stands (a held
    /// transaction is not declared absent on silence), its `mined` still
    /// answers, and the batch is a success.
    #[tokio::test]
    async fn test_fan_out_keeps_a_broadcasters_known_when_no_chain_index_answers() {
        let arcade = MockStatusProvider::new(
            false,
            vec![
                TxStatusDetail::new("m", "mined", Some(1)),
                detail("held", "known"),
            ],
        );
        let services = services_with_status_providers(vec![
            ("ArcadeV2", arcade),
            ("WhatsOnChain", MockStatusProvider::failing(true)),
            ("Bitails", MockStatusProvider::failing(true)),
        ]);
        let txids = vec!["m".to_string(), "held".to_string()];

        let result = services.get_status_for_txids(&txids, false).await.unwrap();

        assert_eq!(result.status, "success");
        assert_eq!(statuses(&result), vec![("m", "mined"), ("held", "known")]);
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

    /// P0-1c witness (bsv-stack-lean #48): `hash_to_header` resolves a TSC
    /// proof's block (`get_merkle_path`, the BEEF build, the proven_txs
    /// height repair at `storage_sqlx.rs:4029`) and, when the header service
    /// fails, asks WhatsOnChain and then Bitails for the header. No explorer
    /// in the proof path: with the header service unreachable and no
    /// break-glass setting, the answer is an error naming the setting, and
    /// no explorer is asked. Run with the network denied (rule 5): at the
    /// base the error is the explorer request's own.
    #[tokio::test]
    async fn hash_to_header_asks_no_explorer_when_the_header_service_fails() {
        let services = Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().with_chaintracks_url("http://127.0.0.1:9"),
        )
        .unwrap();
        let err = services
            .hash_to_header(&"00".repeat(32))
            .await
            .expect_err("the header service is unreachable");
        assert!(
            err.to_string().contains("break-glass"),
            "refused before any explorer is asked, naming the setting: {err}"
        );
    }

    /// The setting's wiring: the tracker `Services` builds holds no
    /// explorer unless `break_glass_explorer_headers` is set, and then the
    /// chain's own WhatsOnChain (testnet here, which the base got wrong: it
    /// always named mainnet's).
    #[test]
    fn the_tracker_holds_no_explorer_unless_break_glass_is_set() {
        let s = Services::with_options(
            Chain::Main,
            ServicesOptions::mainnet().with_chaintracks_url("http://127.0.0.1:9"),
        )
        .unwrap();
        assert!(!ServicesOptions::default().break_glass_explorer_headers);
        assert!(!s.chaintracks.as_ref().unwrap().break_glass_explorers());

        let s = Services::with_options(
            Chain::Test,
            ServicesOptions::testnet()
                .with_chaintracks_url("http://127.0.0.1:9")
                .with_break_glass_explorer_headers(true),
        )
        .unwrap();
        assert!(s.chaintracks.as_ref().unwrap().break_glass_explorers());
        assert!(s.whatsonchain.base_url().ends_with("/test"));
    }

    /// Build a FallbackChainTracker pointing at a mockito server.
    fn build_mock_chaintracks(server_url: &str) -> StdArc<FallbackChainTracker> {
        let primary = ChaintracksServiceClient::from_url(server_url);
        StdArc::new(FallbackChainTracker::new(primary))
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

    /// Rule 28 witness (T6, T7): with no header service a served proof
    /// cannot be checked, so none is returned. Until 0.5.0 the root check
    /// was skipped silently here and the provider's proof came back
    /// unchecked (this test asserted that, as backwards compatibility).
    #[tokio::test]
    async fn test_get_merkle_path_no_chaintracks_returns_no_proof() {
        let txid = "a".repeat(64);
        let height = 850_000u32;

        // A BUMP for a different txid: nothing checks it without a tracker.
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

        // No chaintracks_url.
        let services = build_test_services(vec![("Provider", response)], None);

        let result = services.get_merkle_path(&txid, false).await.unwrap();

        assert_eq!(
            result.merkle_path, None,
            "without a header service no proof is returned"
        );
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("header service")),
            "the error names what is missing: {:?}",
            result.error
        );
        // A fault, never "not mined": nothing is demoted on it.
        assert!(matches!(
            result.provider_verdicts().as_slice(),
            [crate::services::traits::ProviderVerdict::Fault { .. }]
        ));
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
