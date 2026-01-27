//! Main Chaintracks orchestrator
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Chaintracks.ts`

use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;

use crate::Result;
use super::{
    Chain, ChaintracksOptions, ChaintracksInfo, BlockHeader, BaseBlockHeader,
    ChaintracksClient, ChaintracksManagement, ChaintracksStorage,
    HeaderCallback, ReorgCallback,
};

/// Main Chaintracks orchestrator
///
/// Coordinates storage, bulk ingestors, and live ingestors to maintain
/// a synchronized view of the blockchain header chain.
pub struct Chaintracks {
    options: ChaintracksOptions,
    storage: Arc<RwLock<Box<dyn ChaintracksStorage>>>,

    // State
    available: Arc<RwLock<bool>>,
    listening: Arc<RwLock<bool>>,
    synchronized: Arc<RwLock<bool>>,

    // Subscriptions
    header_subscribers: Arc<RwLock<Vec<(String, HeaderCallback)>>>,
    reorg_subscribers: Arc<RwLock<Vec<(String, ReorgCallback)>>>,

    // Queues for header processing
    base_headers: Arc<RwLock<Vec<BaseBlockHeader>>>,
    live_headers: Arc<RwLock<Vec<BlockHeader>>>,
}

impl Chaintracks {
    /// Create a new Chaintracks instance with the given storage
    pub fn new(
        options: ChaintracksOptions,
        storage: Box<dyn ChaintracksStorage>,
    ) -> Self {
        Chaintracks {
            options,
            storage: Arc::new(RwLock::new(storage)),
            available: Arc::new(RwLock::new(false)),
            listening: Arc::new(RwLock::new(false)),
            synchronized: Arc::new(RwLock::new(false)),
            header_subscribers: Arc::new(RwLock::new(vec![])),
            reorg_subscribers: Arc::new(RwLock::new(vec![])),
            base_headers: Arc::new(RwLock::new(vec![])),
            live_headers: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Initialize storage and start ingestors
    pub async fn make_available(&self) -> Result<()> {
        {
            let storage = self.storage.read().await;
            storage.make_available().await?;
        }

        *self.available.write().await = true;
        Ok(())
    }

    fn generate_subscription_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

#[async_trait]
impl ChaintracksClient for Chaintracks {
    async fn get_chain(&self) -> Result<Chain> {
        Ok(self.options.chain)
    }

    async fn get_info(&self) -> Result<ChaintracksInfo> {
        let storage = self.storage.read().await;
        let tip = storage.find_chain_tip_header().await?;
        let live_range = storage.find_live_height_range().await?;

        Ok(ChaintracksInfo {
            chain: self.options.chain,
            storage_type: storage.storage_type().to_string(),
            bulk_ingestor_count: 0, // TODO: Track ingestors
            live_ingestor_count: 0,
            chain_tip_height: tip.as_ref().map(|h| h.height),
            live_low_height: live_range.as_ref().map(|r| r.low),
            live_high_height: live_range.as_ref().map(|r| r.high),
            is_listening: *self.listening.read().await,
            is_synchronized: *self.synchronized.read().await,
        })
    }

    async fn get_present_height(&self) -> Result<u32> {
        // TODO: Get from bulk ingestors
        let storage = self.storage.read().await;
        if let Some(tip) = storage.find_chain_tip_header().await? {
            Ok(tip.height)
        } else {
            Ok(0)
        }
    }

    async fn is_listening(&self) -> Result<bool> {
        Ok(*self.listening.read().await)
    }

    async fn is_synchronized(&self) -> Result<bool> {
        Ok(*self.synchronized.read().await)
    }

    async fn current_height(&self) -> Result<u32> {
        let storage = self.storage.read().await;
        if let Some(tip) = storage.find_chain_tip_header().await? {
            Ok(tip.height)
        } else {
            Ok(0)
        }
    }

    async fn find_header_for_height(&self, height: u32) -> Result<Option<BlockHeader>> {
        let storage = self.storage.read().await;
        storage.find_header_for_height(height).await
    }

    async fn find_header_for_block_hash(&self, hash: &str) -> Result<Option<BlockHeader>> {
        let storage = self.storage.read().await;
        storage
            .find_live_header_for_block_hash(hash)
            .await
            .map(|opt| opt.map(|h| h.into()))
    }

    async fn find_chain_tip_header(&self) -> Result<BlockHeader> {
        let storage = self.storage.read().await;
        storage
            .find_chain_tip_header()
            .await?
            .map(|h| h.into())
            .ok_or_else(|| crate::Error::NotFound {
                entity: "chain tip".to_string(),
                id: "current".to_string(),
            })
    }

    async fn find_chain_tip_hash(&self) -> Result<String> {
        let storage = self.storage.read().await;
        storage
            .find_chain_tip_hash()
            .await?
            .ok_or_else(|| crate::Error::NotFound {
                entity: "chain tip".to_string(),
                id: "current".to_string(),
            })
    }

    async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool> {
        let storage = self.storage.read().await;
        if let Some(header) = storage.find_header_for_height(height).await? {
            Ok(header.merkle_root == root)
        } else {
            Ok(false)
        }
    }

    async fn get_headers(&self, height: u32, count: u32) -> Result<String> {
        let storage = self.storage.read().await;
        let bytes = storage.get_headers_bytes(height, count).await?;
        Ok(hex::encode(bytes))
    }

    async fn add_header(&self, header: BaseBlockHeader) -> Result<()> {
        let mut queue = self.base_headers.write().await;
        queue.push(header);
        Ok(())
    }

    async fn start_listening(&self) -> Result<()> {
        *self.listening.write().await = true;
        // TODO: Start live ingestors
        Ok(())
    }

    async fn listening(&self) -> Result<()> {
        // Wait until listening is true
        loop {
            if *self.listening.read().await {
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    async fn subscribe_headers(&self, callback: HeaderCallback) -> Result<String> {
        let id = Self::generate_subscription_id();
        let mut subs = self.header_subscribers.write().await;
        subs.push((id.clone(), callback));
        Ok(id)
    }

    async fn subscribe_reorgs(&self, callback: ReorgCallback) -> Result<String> {
        let id = Self::generate_subscription_id();
        let mut subs = self.reorg_subscribers.write().await;
        subs.push((id.clone(), callback));
        Ok(id)
    }

    async fn unsubscribe(&self, subscription_id: &str) -> Result<bool> {
        let mut header_subs = self.header_subscribers.write().await;
        let original_len = header_subs.len();
        header_subs.retain(|(id, _)| id != subscription_id);

        if header_subs.len() != original_len {
            return Ok(true);
        }

        let mut reorg_subs = self.reorg_subscribers.write().await;
        let original_len = reorg_subs.len();
        reorg_subs.retain(|(id, _)| id != subscription_id);

        Ok(reorg_subs.len() != original_len)
    }
}

#[async_trait]
impl ChaintracksManagement for Chaintracks {
    async fn destroy(&self) -> Result<()> {
        *self.listening.write().await = false;
        *self.available.write().await = false;

        let storage = self.storage.read().await;
        storage.destroy().await?;

        Ok(())
    }

    async fn validate(&self) -> Result<bool> {
        // TODO: Implement full chain validation
        Ok(true)
    }

    async fn export_bulk_headers(
        &self,
        _folder: &str,
        _headers_per_file: Option<u32>,
        _max_height: Option<u32>,
    ) -> Result<()> {
        // TODO: Implement bulk export
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaintracks::storage::MemoryStorage;

    #[tokio::test]
    async fn test_chaintracks_basic() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        let info = ct.get_info().await.unwrap();
        assert_eq!(info.chain, Chain::Test);
        assert_eq!(info.storage_type, "memory");
    }
}
