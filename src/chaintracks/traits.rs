//! Chaintracks trait definitions
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Api/`

use super::types::*;
use crate::Result;
use async_trait::async_trait;

/// Read-only client interface for Chaintracks
///
/// Based on TypeScript `ChaintracksClientApi`
#[async_trait]
pub trait ChaintracksClient: Send + Sync {
    /// Get the chain being tracked
    async fn get_chain(&self) -> Result<Chain>;

    /// Get system information
    async fn get_info(&self) -> Result<ChaintracksInfo>;

    /// Get the current network height (from ingestors)
    async fn get_present_height(&self) -> Result<u32>;

    /// Check if system is listening for new headers
    async fn is_listening(&self) -> Result<bool>;

    /// Check if initial synchronization is complete
    async fn is_synchronized(&self) -> Result<bool>;

    /// Get the current chain tip height
    async fn current_height(&self) -> Result<u32>;

    /// Find header by height
    async fn find_header_for_height(&self, height: u32) -> Result<Option<BlockHeader>>;

    /// Find header by block hash
    async fn find_header_for_block_hash(&self, hash: &str) -> Result<Option<BlockHeader>>;

    /// Get the current chain tip header
    async fn find_chain_tip_header(&self) -> Result<BlockHeader>;

    /// Get the current chain tip hash
    async fn find_chain_tip_hash(&self) -> Result<String>;

    /// Verify if a merkle root is valid for a given height
    async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool>;

    /// Get multiple serialized headers starting from height
    ///
    /// Returns hex-encoded concatenated 80-byte headers
    async fn get_headers(&self, height: u32, count: u32) -> Result<String>;

    /// Submit a header for processing
    async fn add_header(&self, header: BaseBlockHeader) -> Result<()>;

    /// Start listening for new headers
    async fn start_listening(&self) -> Result<()>;

    /// Wait until listening has started
    async fn listening(&self) -> Result<()>;

    /// Subscribe to new header events
    ///
    /// Returns subscription ID
    async fn subscribe_headers(&self, callback: HeaderCallback) -> Result<String>;

    /// Subscribe to reorg events
    ///
    /// Returns subscription ID
    async fn subscribe_reorgs(&self, callback: ReorgCallback) -> Result<String>;

    /// Unsubscribe from events
    async fn unsubscribe(&self, subscription_id: &str) -> Result<bool>;
}

/// Callback for new headers
pub type HeaderCallback = Box<dyn Fn(BlockHeader) + Send + Sync>;

/// Callback for reorg events
pub type ReorgCallback = Box<dyn Fn(ReorgEvent) + Send + Sync>;

/// Reorg event data
#[derive(Debug, Clone)]
pub struct ReorgEvent {
    /// Depth of the reorg (number of blocks replaced)
    pub depth: u32,
    /// Previous chain tip before reorg
    pub old_tip: BlockHeader,
    /// New chain tip after reorg
    pub new_tip: BlockHeader,
    /// Headers that were deactivated
    pub deactivated_headers: Vec<BlockHeader>,
}

/// Management interface for Chaintracks (extends Client)
#[async_trait]
pub trait ChaintracksManagement: ChaintracksClient {
    /// Shutdown and cleanup resources
    async fn destroy(&self) -> Result<()>;

    /// Validate the entire chain from genesis
    async fn validate(&self) -> Result<bool>;

    /// Export bulk headers to files
    async fn export_bulk_headers(
        &self,
        folder: &str,
        headers_per_file: Option<u32>,
        max_height: Option<u32>,
    ) -> Result<()>;
}

/// Storage query interface (read-only)
#[async_trait]
pub trait ChaintracksStorageQuery: Send + Sync {
    /// Get the chain this storage is for
    fn chain(&self) -> Chain;

    /// Get live height threshold (headers above this stay in live storage)
    fn live_height_threshold(&self) -> u32;

    /// Get reorg height threshold (max reorg depth to track)
    fn reorg_height_threshold(&self) -> u32;

    /// Find the current chain tip header
    async fn find_chain_tip_header(&self) -> Result<Option<LiveBlockHeader>>;

    /// Find the current chain tip hash
    async fn find_chain_tip_hash(&self) -> Result<Option<String>>;

    /// Find header by height
    async fn find_header_for_height(&self, height: u32) -> Result<Option<BlockHeader>>;

    /// Find live header by block hash
    async fn find_live_header_for_block_hash(&self, hash: &str) -> Result<Option<LiveBlockHeader>>;

    /// Find live header by merkle root
    async fn find_live_header_for_merkle_root(
        &self,
        merkle_root: &str,
    ) -> Result<Option<LiveBlockHeader>>;

    /// Get serialized headers as bytes
    async fn get_headers_bytes(&self, height: u32, count: u32) -> Result<Vec<u8>>;

    /// Get all live headers
    async fn get_live_headers(&self) -> Result<Vec<LiveBlockHeader>>;

    /// Get available height ranges (from bulk storage)
    async fn get_available_height_ranges(&self) -> Result<Vec<HeightRange>>;

    /// Find the range of heights in live storage
    async fn find_live_height_range(&self) -> Result<Option<HeightRange>>;

    /// Find common ancestor between two headers
    async fn find_common_ancestor(
        &self,
        header1: &LiveBlockHeader,
        header2: &LiveBlockHeader,
    ) -> Result<Option<LiveBlockHeader>>;

    /// Find reorg depth between a new header and current tip
    async fn find_reorg_depth(&self, new_header: &LiveBlockHeader) -> Result<u32>;
}

/// Storage ingest interface (write operations)
#[async_trait]
pub trait ChaintracksStorageIngest: ChaintracksStorageQuery {
    /// Insert a header into live storage
    async fn insert_header(&self, header: LiveBlockHeader) -> Result<InsertHeaderResult>;

    /// Prune old live headers
    async fn prune_live_block_headers(&self, active_tip_height: u32) -> Result<u32>;

    /// Migrate headers from live to bulk storage
    async fn migrate_live_to_bulk(&self, count: u32) -> Result<u32>;

    /// Delete live headers older than max_height
    async fn delete_older_live_block_headers(&self, max_height: u32) -> Result<u32>;

    /// Initialize storage
    async fn make_available(&self) -> Result<()>;

    /// Run database migrations
    async fn migrate_latest(&self) -> Result<()>;

    /// Drop all data (for testing)
    async fn drop_all_data(&self) -> Result<()>;

    /// Shutdown storage
    async fn destroy(&self) -> Result<()>;
}

/// Full storage provider interface
#[async_trait]
pub trait ChaintracksStorage: ChaintracksStorageIngest {
    /// Get storage type name
    fn storage_type(&self) -> &str;

    /// Check if storage is available
    async fn is_available(&self) -> bool;
}

/// Bulk ingestor interface (fetches historical headers)
#[async_trait]
pub trait BulkIngestor: Send + Sync {
    /// Get the current network height
    async fn get_present_height(&self) -> Result<Option<u32>>;

    /// Synchronize bulk storage up to present height
    async fn synchronize(
        &self,
        present_height: u32,
        before: u32,
        prior_live_headers: &[LiveBlockHeader],
    ) -> Result<BulkSyncResult>;

    /// Fetch headers for a specific range
    async fn fetch_headers(
        &self,
        before: u32,
        fetch_range: HeightRange,
        bulk_range: Option<HeightRange>,
        prior_live_headers: &[LiveBlockHeader],
    ) -> Result<Vec<BlockHeader>>;

    /// Set storage reference
    async fn set_storage(&mut self, storage: Box<dyn ChaintracksStorage>) -> Result<()>;

    /// Shutdown ingestor
    async fn shutdown(&self) -> Result<()>;
}

/// Result of bulk synchronization
#[derive(Debug, Clone)]
pub struct BulkSyncResult {
    /// Headers to add to live storage
    pub live_headers: Vec<BlockHeader>,
    /// Whether sync is complete
    pub done: bool,
}

/// Live ingestor interface (streams new headers)
#[async_trait]
pub trait LiveIngestor: Send + Sync {
    /// Get a header by hash from the network
    async fn get_header_by_hash(&self, hash: &str) -> Result<Option<BlockHeader>>;

    /// Start listening and push headers to the provided vec
    ///
    /// This is a long-running operation that returns when stop_listening is called
    async fn start_listening(&self, live_headers: &mut Vec<BlockHeader>) -> Result<()>;

    /// Stop listening for new headers
    fn stop_listening(&self);

    /// Set storage reference
    async fn set_storage(&mut self, storage: Box<dyn ChaintracksStorage>) -> Result<()>;

    /// Shutdown ingestor
    async fn shutdown(&self) -> Result<()>;
}

/// Options for creating a Chaintracks instance
#[derive(Debug, Clone)]
pub struct ChaintracksOptions {
    /// Chain to track
    pub chain: Chain,
    /// Live height threshold (default: 2000)
    pub live_height_threshold: u32,
    /// Reorg height threshold (default: 400)
    pub reorg_height_threshold: u32,
    /// Max recursive header lookups (default: 36)
    pub add_live_recursion_limit: u32,
    /// Batch insert limit (default: 400)
    pub batch_insert_limit: u32,
    /// Migration chunk size (default: 500)
    pub bulk_migration_chunk_size: u32,
    /// If true, require ingestors to be configured (validation only, not a hard error)
    pub require_ingestors: bool,
    /// If true, prevent any write operations (header insertion, reorg handling)
    pub readonly: bool,
}

impl Default for ChaintracksOptions {
    fn default() -> Self {
        ChaintracksOptions {
            chain: Chain::Main,
            live_height_threshold: 2000,
            reorg_height_threshold: 400,
            add_live_recursion_limit: 36,
            batch_insert_limit: 400,
            bulk_migration_chunk_size: 500,
            require_ingestors: false,
            readonly: false,
        }
    }
}

impl ChaintracksOptions {
    /// Default options for mainnet
    pub fn default_mainnet() -> Self {
        Self::default()
    }

    /// Default options for testnet
    pub fn default_testnet() -> Self {
        ChaintracksOptions {
            chain: Chain::Test,
            ..Self::default()
        }
    }
}
