//! In-memory Chaintracks storage (NoDb equivalent)
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Storage/ChaintracksStorageNoDb.ts`

use std::collections::HashMap;
use std::sync::RwLock;
use async_trait::async_trait;

use crate::Result;
use crate::chaintracks::{
    Chain, LiveBlockHeader, BlockHeader, HeightRange, InsertHeaderResult,
    ChaintracksStorage, ChaintracksStorageQuery, ChaintracksStorageIngest,
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
                        result.extend_from_slice(&prev);
                    }
                    if let Ok(merkle) = hex::decode(&header.merkle_root) {
                        result.extend_from_slice(&merkle);
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
        Ok(headers.values().cloned().collect())
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
            min_height = min_height.min(header.height);
            max_height = max_height.max(header.height);
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
                    break;
                }
            } else {
                break;
            }
        }

        // Now walk back together
        while h1.hash != h2.hash {
            if let (Some(p1), Some(p2)) = (h1.previous_header_id, h2.previous_header_id) {
                if let (Some(prev1), Some(prev2)) = (headers.get(&p1), headers.get(&p2)) {
                    h1 = prev1.clone();
                    h2 = prev2.clone();
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        Ok(Some(h1))
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

        // Find previous header
        if !header.previous_hash.is_empty() && header.previous_hash != "0".repeat(64) {
            let hash_to_id = self.hash_to_id.read().unwrap();
            if let Some(&prev_id) = hash_to_id.get(&header.previous_hash) {
                header.previous_header_id = Some(prev_id);
            } else {
                result.no_prev = true;
            }
        }

        // Get current tip for comparison
        let prior_tip = self.find_chain_tip_header().await?;
        result.prior_tip = prior_tip.clone();

        // Determine if this becomes the new tip
        let becomes_tip = match &prior_tip {
            None => true,
            Some(tip) => {
                // Compare chain work or height
                header.height > tip.height
            }
        };

        if becomes_tip {
            header.is_chain_tip = true;
            header.is_active = true;
            result.is_active_tip = true;

            // Check for reorg BEFORE taking the write lock
            let is_reorg = if let Some(old_tip) = &prior_tip {
                old_tip.height >= header.height || old_tip.hash != header.previous_hash
            } else {
                false
            };

            // If reorg, calculate depth before taking any write locks
            if is_reorg {
                result.reorg_depth = self.find_reorg_depth(&header).await.unwrap_or(0);
                // TODO: Properly handle deactivated headers
            }

            // Deactivate old tip (now we can take the write lock)
            if let Some(old_tip) = &prior_tip {
                let mut headers = self.headers.write().unwrap();
                if let Some(old) = headers.get_mut(&old_tip.header_id) {
                    old.is_chain_tip = false;
                }
            }

            // Update tip pointer
            *self.tip_id.write().unwrap() = Some(id);

            // Update height index
            self.height_to_id.write().unwrap().insert(header.height, id);
        }

        // Store header
        self.headers.write().unwrap().insert(id, header.clone());
        self.hash_to_id.write().unwrap().insert(header.hash.clone(), id);

        result.added = true;
        Ok(result)
    }

    async fn prune_live_block_headers(&self, active_tip_height: u32) -> Result<u32> {
        let threshold = active_tip_height.saturating_sub(self.live_height_threshold);
        let mut count = 0;

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
                self.height_to_id.write().unwrap().remove(&header.height);
                count += 1;
            }
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
                count += 1;
            }
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
        *self.next_id.write().unwrap() = 1;
        *self.tip_id.write().unwrap() = None;
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

    #[tokio::test]
    async fn test_memory_storage_basic() {
        let storage = MemoryStorage::new(Chain::Test);

        // Initially empty
        assert!(storage.find_chain_tip_header().await.unwrap().is_none());

        // Insert genesis
        let genesis = LiveBlockHeader {
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "abc123".to_string(),
            time: 1231006505,
            bits: 0x1d00ffff,
            nonce: 2083236893,
            height: 0,
            hash: "genesis_hash".to_string(),
            chain_work: "1".to_string(),
            is_chain_tip: false,
            is_active: false,
            header_id: 0,
            previous_header_id: None,
        };

        let result = storage.insert_header(genesis).await.unwrap();
        assert!(result.added);
        assert!(result.is_active_tip);

        // Should be the tip now
        let tip = storage.find_chain_tip_header().await.unwrap();
        assert!(tip.is_some());
        assert_eq!(tip.unwrap().hash, "genesis_hash");
    }
}
