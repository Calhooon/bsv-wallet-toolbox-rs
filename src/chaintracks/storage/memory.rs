//! In-memory Chaintracks storage (NoDb equivalent)
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/ChaintracksStorageNoDb.ts`

use std::collections::HashMap;
use std::sync::RwLock;
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::Result;
use crate::chaintracks::{
    Chain, LiveBlockHeader, BlockHeader, HeightRange, InsertHeaderResult,
    ChaintracksStorage, ChaintracksStorageQuery, ChaintracksStorageIngest,
    calculate_work,
};

/// In-memory storage for Chaintracks
///
/// Suitable for testing, development, and mobile clients.
/// Data is lost on process restart.
pub struct MemoryStorage {
    chain: Chain,
    live_height_threshold: u32,
    reorg_height_threshold: u32,

    /// Live headers indexed by header_id
    headers: RwLock<HashMap<i64, LiveBlockHeader>>,
    /// Hash to header_id lookup
    hash_to_id: RwLock<HashMap<String, i64>>,
    /// Height to header_id lookup (active chain only)
    height_to_id: RwLock<HashMap<u32, i64>>,
    /// Merkle root to header_id lookup (active chain only)
    merkle_to_id: RwLock<HashMap<String, i64>>,
    /// Next header ID to assign
    next_id: RwLock<i64>,
    /// Current chain tip header ID
    tip_id: RwLock<Option<i64>>,
}

impl MemoryStorage {
    /// Create new in-memory storage
    pub fn new(chain: Chain) -> Self {
        MemoryStorage {
            chain,
            live_height_threshold: 2000,
            reorg_height_threshold: 400,
            headers: RwLock::new(HashMap::new()),
            hash_to_id: RwLock::new(HashMap::new()),
            height_to_id: RwLock::new(HashMap::new()),
            merkle_to_id: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
            tip_id: RwLock::new(None),
        }
    }

    /// Create with custom thresholds
    pub fn with_thresholds(
        chain: Chain,
        live_height_threshold: u32,
        reorg_height_threshold: u32,
    ) -> Self {
        MemoryStorage {
            chain,
            live_height_threshold,
            reorg_height_threshold,
            headers: RwLock::new(HashMap::new()),
            hash_to_id: RwLock::new(HashMap::new()),
            height_to_id: RwLock::new(HashMap::new()),
            merkle_to_id: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
            tip_id: RwLock::new(None),
        }
    }

    fn allocate_id(&self) -> i64 {
        let mut next = self.next_id.write().unwrap();
        let id = *next;
        *next += 1;
        id
    }

    /// Get header count
    pub fn header_count(&self) -> usize {
        self.headers.read().unwrap().len()
    }

    /// Get all headers at a specific height (including forks)
    pub fn get_headers_at_height(&self, height: u32) -> Vec<LiveBlockHeader> {
        let headers = self.headers.read().unwrap();
        headers.values()
            .filter(|h| h.height == height)
            .cloned()
            .collect()
    }

    /// Get all active headers (on the main chain)
    pub fn get_active_headers(&self) -> Vec<LiveBlockHeader> {
        let headers = self.headers.read().unwrap();
        headers.values()
            .filter(|h| h.is_active)
            .cloned()
            .collect()
    }

    /// Get all inactive headers (on forks)
    pub fn get_fork_headers(&self) -> Vec<LiveBlockHeader> {
        let headers = self.headers.read().unwrap();
        headers.values()
            .filter(|h| !h.is_active)
            .cloned()
            .collect()
    }

    /// Find headers that build on a given hash
    pub fn find_children(&self, parent_hash: &str) -> Vec<LiveBlockHeader> {
        let headers = self.headers.read().unwrap();
        headers.values()
            .filter(|h| h.previous_hash == parent_hash)
            .cloned()
            .collect()
    }

    /// Insert multiple headers in batch
    pub async fn insert_headers_batch(&self, headers: Vec<LiveBlockHeader>) -> Result<Vec<InsertHeaderResult>> {
        let mut results = Vec::with_capacity(headers.len());
        for header in headers {
            let result = self.insert_header(header).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Handle a reorg by deactivating the old chain and activating the new one
    async fn handle_reorg(
        &self,
        new_tip: &LiveBlockHeader,
        old_tip: &LiveBlockHeader,
    ) -> Result<(u32, Vec<LiveBlockHeader>)> {
        // Find common ancestor
        let ancestor = match self.find_common_ancestor(old_tip, new_tip).await? {
            Some(a) => a,
            None => {
                warn!("No common ancestor found for reorg");
                return Ok((0, vec![]));
            }
        };

        let reorg_depth = old_tip.height - ancestor.height;
        info!("Reorg detected: depth={}, old_tip={}, new_tip={}",
            reorg_depth, old_tip.height, new_tip.height);

        // Collect headers to deactivate (walk from old tip to ancestor)
        let mut deactivated = Vec::new();
        let mut current = old_tip.clone();

        {
            let mut headers = self.headers.write().unwrap();
            let mut height_to_id = self.height_to_id.write().unwrap();
            let mut merkle_to_id = self.merkle_to_id.write().unwrap();

            while current.hash != ancestor.hash {
                // Mark as inactive
                if let Some(h) = headers.get_mut(&current.header_id) {
                    h.is_active = false;
                    h.is_chain_tip = false;
                    deactivated.push(h.clone());

                    // Remove from height/merkle indexes
                    height_to_id.remove(&h.height);
                    merkle_to_id.remove(&h.merkle_root);
                }

                // Move to previous
                if let Some(prev_id) = current.previous_header_id {
                    if let Some(prev) = headers.get(&prev_id) {
                        current = prev.clone();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            // Now activate the new chain (walk from new tip to ancestor)
            current = new_tip.clone();
            while current.hash != ancestor.hash {
                if let Some(h) = headers.get_mut(&current.header_id) {
                    h.is_active = true;
                    height_to_id.insert(h.height, h.header_id);
                    merkle_to_id.insert(h.merkle_root.clone(), h.header_id);
                }

                if let Some(prev_id) = current.previous_header_id {
                    if let Some(prev) = headers.get(&prev_id) {
                        current = prev.clone();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        Ok((reorg_depth, deactivated))
    }
}

#[async_trait]
impl ChaintracksStorageQuery for MemoryStorage {
    fn chain(&self) -> Chain {
        self.chain
    }

    fn live_height_threshold(&self) -> u32 {
        self.live_height_threshold
    }

    fn reorg_height_threshold(&self) -> u32 {
        self.reorg_height_threshold
    }

    async fn find_chain_tip_header(&self) -> Result<Option<LiveBlockHeader>> {
        let tip_id = self.tip_id.read().unwrap();
        if let Some(id) = *tip_id {
            let headers = self.headers.read().unwrap();
            Ok(headers.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn find_chain_tip_hash(&self) -> Result<Option<String>> {
        let header = self.find_chain_tip_header().await?;
        Ok(header.map(|h| h.hash))
    }

    async fn find_header_for_height(&self, height: u32) -> Result<Option<BlockHeader>> {
        let height_to_id = self.height_to_id.read().unwrap();
        if let Some(&id) = height_to_id.get(&height) {
            let headers = self.headers.read().unwrap();
            Ok(headers.get(&id).map(|h| h.clone().into()))
        } else {
            Ok(None)
        }
    }

    async fn find_live_header_for_block_hash(&self, hash: &str) -> Result<Option<LiveBlockHeader>> {
        let hash_to_id = self.hash_to_id.read().unwrap();
        if let Some(&id) = hash_to_id.get(hash) {
            let headers = self.headers.read().unwrap();
            Ok(headers.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn find_live_header_for_merkle_root(&self, merkle_root: &str) -> Result<Option<LiveBlockHeader>> {
        // First check the merkle root index (active headers)
        {
            let merkle_to_id = self.merkle_to_id.read().unwrap();
            if let Some(&id) = merkle_to_id.get(merkle_root) {
                let headers = self.headers.read().unwrap();
                if let Some(h) = headers.get(&id) {
                    return Ok(Some(h.clone()));
                }
            }
        }

        // Fall back to full scan for inactive headers
        let headers = self.headers.read().unwrap();
        for header in headers.values() {
            if header.merkle_root == merkle_root && header.is_active {
                return Ok(Some(header.clone()));
            }
        }
        Ok(None)
    }

    async fn get_headers_bytes(&self, height: u32, count: u32) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity((count as usize) * 80);
        let height_to_id = self.height_to_id.read().unwrap();
        let headers = self.headers.read().unwrap();

        for h in height..height + count {
            if let Some(&id) = height_to_id.get(&h) {
                if let Some(header) = headers.get(&id) {
                    // Serialize header to 80 bytes
                    result.extend_from_slice(&header.version.to_le_bytes());
                    if let Ok(prev) = hex::decode(&header.previous_hash) {
                        if prev.len() == 32 {
                            result.extend_from_slice(&prev);
                        } else {
                            result.extend_from_slice(&[0u8; 32]);
                        }
                    } else {
                        result.extend_from_slice(&[0u8; 32]);
                    }
                    if let Ok(merkle) = hex::decode(&header.merkle_root) {
                        if merkle.len() == 32 {
                            result.extend_from_slice(&merkle);
                        } else {
                            result.extend_from_slice(&[0u8; 32]);
                        }
                    } else {
                        result.extend_from_slice(&[0u8; 32]);
                    }
                    result.extend_from_slice(&header.time.to_le_bytes());
                    result.extend_from_slice(&header.bits.to_le_bytes());
                    result.extend_from_slice(&header.nonce.to_le_bytes());
                }
            }
        }

        Ok(result)
    }

    async fn get_live_headers(&self) -> Result<Vec<LiveBlockHeader>> {
        let headers = self.headers.read().unwrap();
        let mut result: Vec<_> = headers.values().cloned().collect();
        // Sort by height descending (newest first)
        result.sort_by(|a, b| b.height.cmp(&a.height));
        Ok(result)
    }

    async fn get_available_height_ranges(&self) -> Result<Vec<HeightRange>> {
        // Memory storage has no bulk storage, return empty
        Ok(vec![])
    }

    async fn find_live_height_range(&self) -> Result<Option<HeightRange>> {
        let headers = self.headers.read().unwrap();
        if headers.is_empty() {
            return Ok(None);
        }

        let mut min_height = u32::MAX;
        let mut max_height = 0;

        for header in headers.values() {
            if header.is_active {
                min_height = min_height.min(header.height);
                max_height = max_height.max(header.height);
            }
        }

        if min_height > max_height {
            return Ok(None);
        }

        Ok(Some(HeightRange::new(min_height, max_height)))
    }

    async fn find_common_ancestor(
        &self,
        header1: &LiveBlockHeader,
        header2: &LiveBlockHeader,
    ) -> Result<Option<LiveBlockHeader>> {
        // Walk back from both headers until we find a common point
        let headers = self.headers.read().unwrap();

        let mut h1 = header1.clone();
        let mut h2 = header2.clone();

        // Bring to same height
        while h1.height > h2.height {
            if let Some(prev_id) = h1.previous_header_id {
                if let Some(prev) = headers.get(&prev_id) {
                    h1 = prev.clone();
                } else {
                    // Try to find by hash
                    let hash_to_id = self.hash_to_id.read().unwrap();
                    if let Some(&id) = hash_to_id.get(&h1.previous_hash) {
                        if let Some(prev) = headers.get(&id) {
                            h1 = prev.clone();
                            continue;
                        }
                    }
                    break;
                }
            } else {
                break;
            }
        }

        while h2.height > h1.height {
            if let Some(prev_id) = h2.previous_header_id {
                if let Some(prev) = headers.get(&prev_id) {
                    h2 = prev.clone();
                } else {
                    let hash_to_id = self.hash_to_id.read().unwrap();
                    if let Some(&id) = hash_to_id.get(&h2.previous_hash) {
                        if let Some(prev) = headers.get(&id) {
                            h2 = prev.clone();
                            continue;
                        }
                    }
                    break;
                }
            } else {
                break;
            }
        }

        // Now walk back together
        let max_iterations = self.reorg_height_threshold;
        let mut iterations = 0;

        while h1.hash != h2.hash && iterations < max_iterations {
            iterations += 1;

            let p1 = h1.previous_header_id.and_then(|id| headers.get(&id).cloned());
            let p2 = h2.previous_header_id.and_then(|id| headers.get(&id).cloned());

            match (p1, p2) {
                (Some(prev1), Some(prev2)) => {
                    h1 = prev1;
                    h2 = prev2;
                }
                _ => return Ok(None),
            }
        }

        if h1.hash == h2.hash {
            Ok(Some(h1))
        } else {
            Ok(None)
        }
    }

    async fn find_reorg_depth(&self, new_header: &LiveBlockHeader) -> Result<u32> {
        if let Some(tip) = self.find_chain_tip_header().await? {
            if let Some(ancestor) = self.find_common_ancestor(&tip, new_header).await? {
                return Ok(tip.height - ancestor.height);
            }
        }
        Ok(0)
    }
}

#[async_trait]
impl ChaintracksStorageIngest for MemoryStorage {
    async fn insert_header(&self, mut header: LiveBlockHeader) -> Result<InsertHeaderResult> {
        let mut result = InsertHeaderResult::default();

        // Check for duplicate
        {
            let hash_to_id = self.hash_to_id.read().unwrap();
            if hash_to_id.contains_key(&header.hash) {
                result.dupe = true;
                return Ok(result);
            }
        }

        // Allocate ID
        let id = self.allocate_id();
        header.header_id = id;

        // Calculate chain work if not set
        if header.chain_work.is_empty() || header.chain_work == "0".repeat(64) {
            header.chain_work = calculate_work(header.bits);
        }

        // Find previous header
        let genesis_hash = "0".repeat(64);
        if !header.previous_hash.is_empty() && header.previous_hash != genesis_hash {
            let hash_to_id = self.hash_to_id.read().unwrap();
            if let Some(&prev_id) = hash_to_id.get(&header.previous_hash) {
                header.previous_header_id = Some(prev_id);
            } else {
                result.no_prev = true;
                debug!("Previous header not found: {}", &header.previous_hash[..16]);
            }
        }

        // Get current tip for comparison
        let prior_tip = self.find_chain_tip_header().await?;
        result.prior_tip = prior_tip.clone();

        // Determine if this becomes the new tip
        let becomes_tip = match &prior_tip {
            None => {
                result.no_tip = true;
                true
            }
            Some(tip) => {
                // New header is tip if it extends the current tip
                // or has more work (simplified: higher height)
                header.height > tip.height ||
                (header.height == tip.height && header.previous_hash == tip.hash)
            }
        };

        if becomes_tip {
            header.is_chain_tip = true;
            header.is_active = true;
            result.is_active_tip = true;

            // Check for reorg
            if let Some(ref old_tip) = prior_tip {
                let is_reorg = old_tip.hash != header.previous_hash;

                if is_reorg {
                    let (reorg_depth, deactivated) = self.handle_reorg(&header, old_tip).await?;
                    result.reorg_depth = reorg_depth;
                    result.deactivated_headers = deactivated;
                } else {
                    // Not a reorg, just deactivate the old tip
                    let mut headers = self.headers.write().unwrap();
                    if let Some(old) = headers.get_mut(&old_tip.header_id) {
                        old.is_chain_tip = false;
                    }
                }
            }

            // Update tip pointer
            *self.tip_id.write().unwrap() = Some(id);

            // Update height index
            self.height_to_id.write().unwrap().insert(header.height, id);

            // Update merkle index
            self.merkle_to_id.write().unwrap().insert(header.merkle_root.clone(), id);
        }

        // Store header
        self.headers.write().unwrap().insert(id, header.clone());
        self.hash_to_id.write().unwrap().insert(header.hash.clone(), id);

        result.added = true;
        debug!("Inserted header: height={}, hash={}, is_tip={}",
            header.height, &header.hash[..16], result.is_active_tip);

        Ok(result)
    }

    async fn prune_live_block_headers(&self, active_tip_height: u32) -> Result<u32> {
        let threshold = active_tip_height.saturating_sub(self.live_height_threshold);
        let mut count = 0;

        // Collect IDs of headers to remove
        // Only remove inactive headers below threshold
        let ids_to_remove: Vec<i64> = {
            let headers = self.headers.read().unwrap();
            headers
                .iter()
                .filter(|(_, h)| h.height < threshold && !h.is_active)
                .map(|(id, _)| *id)
                .collect()
        };

        for id in ids_to_remove {
            let mut headers = self.headers.write().unwrap();
            if let Some(header) = headers.remove(&id) {
                self.hash_to_id.write().unwrap().remove(&header.hash);
                // Don't remove from height_to_id since we only prune inactive headers
                count += 1;
            }
        }

        if count > 0 {
            debug!("Pruned {} inactive headers below height {}", count, threshold);
        }

        Ok(count)
    }

    async fn migrate_live_to_bulk(&self, _count: u32) -> Result<u32> {
        // Memory storage has no bulk storage
        Ok(0)
    }

    async fn delete_older_live_block_headers(&self, max_height: u32) -> Result<u32> {
        let mut count = 0;

        let ids_to_remove: Vec<i64> = {
            let headers = self.headers.read().unwrap();
            headers
                .iter()
                .filter(|(_, h)| h.height <= max_height)
                .map(|(id, _)| *id)
                .collect()
        };

        for id in ids_to_remove {
            let mut headers = self.headers.write().unwrap();
            if let Some(header) = headers.remove(&id) {
                self.hash_to_id.write().unwrap().remove(&header.hash);
                self.height_to_id.write().unwrap().remove(&header.height);
                self.merkle_to_id.write().unwrap().remove(&header.merkle_root);
                count += 1;
            }
        }

        if count > 0 {
            info!("Deleted {} headers at or below height {}", count, max_height);
        }

        Ok(count)
    }

    async fn make_available(&self) -> Result<()> {
        Ok(())
    }

    async fn migrate_latest(&self) -> Result<()> {
        Ok(())
    }

    async fn drop_all_data(&self) -> Result<()> {
        self.headers.write().unwrap().clear();
        self.hash_to_id.write().unwrap().clear();
        self.height_to_id.write().unwrap().clear();
        self.merkle_to_id.write().unwrap().clear();
        *self.next_id.write().unwrap() = 1;
        *self.tip_id.write().unwrap() = None;
        info!("Dropped all data from memory storage");
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        self.drop_all_data().await
    }
}

#[async_trait]
impl ChaintracksStorage for MemoryStorage {
    fn storage_type(&self) -> &str {
        "memory"
    }

    async fn is_available(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_header(height: u32, prev_hash: &str, hash: &str) -> LiveBlockHeader {
        LiveBlockHeader {
            version: 1,
            previous_hash: prev_hash.to_string(),
            merkle_root: format!("merkle_{}", hash),
            time: 1231006505 + height,
            bits: 0x1d00ffff,
            nonce: height,
            height,
            hash: hash.to_string(),
            chain_work: calculate_work(0x1d00ffff),
            is_chain_tip: false,
            is_active: false,
            header_id: 0,
            previous_header_id: None,
        }
    }

    #[tokio::test]
    async fn test_memory_storage_basic() {
        let storage = MemoryStorage::new(Chain::Test);

        // Initially empty
        assert!(storage.find_chain_tip_header().await.unwrap().is_none());
        assert_eq!(storage.header_count(), 0);

        // Insert genesis
        let genesis = create_test_header(0, &"0".repeat(64), "genesis_hash");

        let result = storage.insert_header(genesis).await.unwrap();
        assert!(result.added);
        assert!(result.is_active_tip);
        assert!(!result.dupe);

        // Should be the tip now
        let tip = storage.find_chain_tip_header().await.unwrap();
        assert!(tip.is_some());
        assert_eq!(tip.unwrap().hash, "genesis_hash");
        assert_eq!(storage.header_count(), 1);
    }

    #[tokio::test]
    async fn test_chain_growth() {
        let storage = MemoryStorage::new(Chain::Test);

        // Insert genesis
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        // Insert block 1
        let block1 = create_test_header(1, "hash_0", "hash_1");
        let result = storage.insert_header(block1).await.unwrap();
        assert!(result.added);
        assert!(result.is_active_tip);
        assert_eq!(result.reorg_depth, 0);

        // Insert block 2
        let block2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(block2).await.unwrap();

        // Verify chain
        let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(tip.height, 2);
        assert_eq!(tip.hash, "hash_2");

        // Verify height lookups
        let h0 = storage.find_header_for_height(0).await.unwrap().unwrap();
        assert_eq!(h0.hash, "hash_0");

        let h1 = storage.find_header_for_height(1).await.unwrap().unwrap();
        assert_eq!(h1.hash, "hash_1");
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "genesis_hash");

        // First insert
        let result1 = storage.insert_header(genesis.clone()).await.unwrap();
        assert!(result1.added);
        assert!(!result1.dupe);

        // Second insert (duplicate)
        let result2 = storage.insert_header(genesis).await.unwrap();
        assert!(!result2.added);
        assert!(result2.dupe);

        assert_eq!(storage.header_count(), 1);
    }

    #[tokio::test]
    async fn test_hash_lookup() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "genesis_hash");
        storage.insert_header(genesis).await.unwrap();

        // Find by hash
        let found = storage.find_live_header_for_block_hash("genesis_hash").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().height, 0);

        // Not found
        let not_found = storage.find_live_header_for_block_hash("nonexistent").await.unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_merkle_root_lookup() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "genesis_hash");
        storage.insert_header(genesis).await.unwrap();

        // Find by merkle root
        let found = storage.find_live_header_for_merkle_root("merkle_genesis_hash").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().height, 0);
    }

    #[tokio::test]
    async fn test_headers_bytes_serialization() {
        let storage = MemoryStorage::new(Chain::Test);

        let mut genesis = create_test_header(0, &"0".repeat(64), &"a".repeat(64));
        genesis.merkle_root = "b".repeat(64);
        storage.insert_header(genesis).await.unwrap();

        let bytes = storage.get_headers_bytes(0, 1).await.unwrap();
        assert_eq!(bytes.len(), 80);

        // Verify version is at the start (little-endian)
        assert_eq!(bytes[0], 1); // version = 1
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_eq!(bytes[3], 0);
    }

    #[tokio::test]
    async fn test_live_height_range() {
        let storage = MemoryStorage::new(Chain::Test);

        // Empty storage
        let range = storage.find_live_height_range().await.unwrap();
        assert!(range.is_none());

        // Add some headers
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        let block2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(block2).await.unwrap();

        let range = storage.find_live_height_range().await.unwrap().unwrap();
        assert_eq!(range.low, 0);
        assert_eq!(range.high, 2);
    }

    #[tokio::test]
    async fn test_pruning() {
        let storage = MemoryStorage::with_thresholds(Chain::Test, 2, 1);

        // Build a chain of 5 blocks
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        for i in 1..5 {
            let prev = format!("hash_{}", i - 1);
            let hash = format!("hash_{}", i);
            let block = create_test_header(i, &prev, &hash);
            storage.insert_header(block).await.unwrap();
        }

        assert_eq!(storage.header_count(), 5);

        // Prune with tip at height 4, threshold 2 -> remove below height 2
        // But only inactive headers are pruned, and all are active in this chain
        let pruned = storage.prune_live_block_headers(4).await.unwrap();
        assert_eq!(pruned, 0); // All active, so none pruned
    }

    #[tokio::test]
    async fn test_delete_older() {
        let storage = MemoryStorage::new(Chain::Test);

        // Build a chain
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        let block2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(block2).await.unwrap();

        assert_eq!(storage.header_count(), 3);

        // Delete heights 0 and 1
        let deleted = storage.delete_older_live_block_headers(1).await.unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(storage.header_count(), 1);

        // Verify remaining
        let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(tip.height, 2);
    }

    #[tokio::test]
    async fn test_drop_all_data() {
        let storage = MemoryStorage::new(Chain::Test);

        // Add data
        let genesis = create_test_header(0, &"0".repeat(64), "genesis_hash");
        storage.insert_header(genesis).await.unwrap();

        assert_eq!(storage.header_count(), 1);

        // Drop all
        storage.drop_all_data().await.unwrap();

        assert_eq!(storage.header_count(), 0);
        assert!(storage.find_chain_tip_header().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_common_ancestor_same_chain() {
        let storage = MemoryStorage::new(Chain::Test);

        // Build a chain
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        let block2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(block2).await.unwrap();

        // Get headers
        let h0 = storage.find_live_header_for_block_hash("hash_0").await.unwrap().unwrap();
        let h2 = storage.find_live_header_for_block_hash("hash_2").await.unwrap().unwrap();

        // Find common ancestor
        let ancestor = storage.find_common_ancestor(&h0, &h2).await.unwrap().unwrap();
        assert_eq!(ancestor.hash, "hash_0");
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let storage = MemoryStorage::new(Chain::Test);

        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"),
            create_test_header(1, "hash_0", "hash_1"),
            create_test_header(2, "hash_1", "hash_2"),
        ];

        let results = storage.insert_headers_batch(headers).await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.added));

        let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(tip.height, 2);
    }

    #[tokio::test]
    async fn test_get_live_headers_sorted() {
        let storage = MemoryStorage::new(Chain::Test);

        // Insert in random order
        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"),
            create_test_header(2, "hash_1", "hash_2"),
            create_test_header(1, "hash_0", "hash_1"),
        ];

        storage.insert_headers_batch(headers).await.unwrap();

        let live = storage.get_live_headers().await.unwrap();

        // Should be sorted by height descending
        assert_eq!(live[0].height, 2);
        assert_eq!(live[1].height, 1);
        assert_eq!(live[2].height, 0);
    }

    #[tokio::test]
    async fn test_storage_type() {
        let storage = MemoryStorage::new(Chain::Main);
        assert_eq!(storage.storage_type(), "memory");
    }

    #[tokio::test]
    async fn test_is_available() {
        let storage = MemoryStorage::new(Chain::Main);
        assert!(storage.is_available().await);
    }

    #[tokio::test]
    async fn test_chain_accessor() {
        let main_storage = MemoryStorage::new(Chain::Main);
        assert_eq!(main_storage.chain(), Chain::Main);

        let test_storage = MemoryStorage::new(Chain::Test);
        assert_eq!(test_storage.chain(), Chain::Test);
    }

    #[tokio::test]
    async fn test_thresholds() {
        let storage = MemoryStorage::with_thresholds(Chain::Main, 1000, 200);
        assert_eq!(storage.live_height_threshold(), 1000);
        assert_eq!(storage.reorg_height_threshold(), 200);
    }

    #[test]
    fn test_default_thresholds() {
        let storage = MemoryStorage::new(Chain::Main);
        assert_eq!(storage.live_height_threshold(), 2000);
        assert_eq!(storage.reorg_height_threshold(), 400);
    }

    #[tokio::test]
    async fn test_get_active_headers() {
        let storage = MemoryStorage::new(Chain::Test);

        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"),
            create_test_header(1, "hash_0", "hash_1"),
        ];

        storage.insert_headers_batch(headers).await.unwrap();

        let active = storage.get_active_headers();
        assert_eq!(active.len(), 2);
        assert!(active.iter().all(|h| h.is_active));
    }

    #[tokio::test]
    async fn test_get_fork_headers_empty() {
        let storage = MemoryStorage::new(Chain::Test);

        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"),
            create_test_header(1, "hash_0", "hash_1"),
        ];

        storage.insert_headers_batch(headers).await.unwrap();

        // No forks, so should be empty
        let forks = storage.get_fork_headers();
        assert!(forks.is_empty());
    }

    #[tokio::test]
    async fn test_get_headers_at_height() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let at_height_0 = storage.get_headers_at_height(0);
        assert_eq!(at_height_0.len(), 1);

        let at_height_1 = storage.get_headers_at_height(1);
        assert!(at_height_1.is_empty());
    }

    #[tokio::test]
    async fn test_find_children() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        let children = storage.find_children("hash_0");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].hash, "hash_1");

        let no_children = storage.find_children("hash_1");
        assert!(no_children.is_empty());
    }

    #[tokio::test]
    async fn test_available_height_ranges() {
        let storage = MemoryStorage::new(Chain::Test);

        // Memory storage has no bulk storage
        let ranges = storage.get_available_height_ranges().await.unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn test_migrate_live_to_bulk() {
        let storage = MemoryStorage::new(Chain::Test);

        // Memory storage doesn't support migration
        let count = storage.migrate_live_to_bulk(10).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_make_available() {
        let storage = MemoryStorage::new(Chain::Test);
        // Should succeed without error
        storage.make_available().await.unwrap();
    }

    #[tokio::test]
    async fn test_migrate_latest() {
        let storage = MemoryStorage::new(Chain::Test);
        // Should succeed without error
        storage.migrate_latest().await.unwrap();
    }

    #[tokio::test]
    async fn test_destroy() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        assert_eq!(storage.header_count(), 1);

        storage.destroy().await.unwrap();

        assert_eq!(storage.header_count(), 0);
    }

    #[tokio::test]
    async fn test_find_reorg_depth() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1.clone()).await.unwrap();

        // With the tip being block1, reorg depth for a header extending block1 should be 0
        let new_header = create_test_header(2, "hash_1", "hash_2");
        let depth = storage.find_reorg_depth(&LiveBlockHeader {
            previous_hash: "hash_1".to_string(),
            ..new_header
        }).await.unwrap();

        // Should find common ancestor at block1
        assert_eq!(depth, 0);
    }

    #[tokio::test]
    async fn test_no_prev_header() {
        let storage = MemoryStorage::new(Chain::Test);

        // Insert a header with unknown previous hash
        let orphan = create_test_header(100, "unknown_hash", "orphan_hash");
        let result = storage.insert_header(orphan).await.unwrap();

        assert!(result.added);
        assert!(result.no_prev);
    }

    #[tokio::test]
    async fn test_headers_bytes_multiple() {
        let storage = MemoryStorage::new(Chain::Test);

        let headers = vec![
            create_test_header(0, &"0".repeat(64), &"a".repeat(64)),
            create_test_header(1, &"a".repeat(64), &"b".repeat(64)),
        ];

        // Set merkle roots to valid 64-char hex
        for (_i, mut h) in headers.into_iter().enumerate() {
            h.merkle_root = format!("{}", "c".repeat(64));
            storage.insert_header(h).await.unwrap();
        }

        let bytes = storage.get_headers_bytes(0, 2).await.unwrap();
        assert_eq!(bytes.len(), 160); // 2 headers * 80 bytes
    }

    #[tokio::test]
    async fn test_merkle_root_lookup_inactive() {
        let storage = MemoryStorage::new(Chain::Test);

        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        // Search for merkle root that doesn't exist
        let not_found = storage.find_live_header_for_merkle_root("nonexistent").await.unwrap();
        assert!(not_found.is_none());
    }
}
