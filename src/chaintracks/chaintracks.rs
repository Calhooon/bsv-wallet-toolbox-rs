//! Main Chaintracks orchestrator
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Chaintracks.ts`

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use super::{
    calculate_work, BaseBlockHeader, BlockHeader, Chain, ChaintracksClient, ChaintracksInfo,
    ChaintracksManagement, ChaintracksOptions, ChaintracksStorage, HeaderCallback, LiveBlockHeader,
    ReorgCallback, ReorgEvent,
};
use crate::Result;

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
    is_syncing: Arc<AtomicBool>,

    // Ingestor tracking
    bulk_ingestor_count: Arc<RwLock<usize>>,
    live_ingestor_count: Arc<RwLock<usize>>,

    // Subscriptions
    header_subscribers: Arc<RwLock<Vec<(String, HeaderCallback)>>>,
    reorg_subscribers: Arc<RwLock<Vec<(String, ReorgCallback)>>>,

    // Queues for header processing
    base_headers: Arc<RwLock<Vec<BaseBlockHeader>>>,
    #[allow(dead_code)]
    live_headers: Arc<RwLock<Vec<BlockHeader>>>,

    // Background sync task handle
    sync_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl Chaintracks {
    /// Create a new Chaintracks instance with the given storage
    pub fn new(options: ChaintracksOptions, storage: Box<dyn ChaintracksStorage>) -> Self {
        if options.require_ingestors {
            tracing::warn!(
                "Chaintracks created - ingestor validation deferred to start_background_sync"
            );
        }

        Chaintracks {
            options,
            storage: Arc::new(RwLock::new(storage)),
            available: Arc::new(RwLock::new(false)),
            listening: Arc::new(RwLock::new(false)),
            synchronized: Arc::new(RwLock::new(false)),
            is_syncing: Arc::new(AtomicBool::new(false)),
            bulk_ingestor_count: Arc::new(RwLock::new(0)),
            live_ingestor_count: Arc::new(RwLock::new(0)),
            header_subscribers: Arc::new(RwLock::new(vec![])),
            reorg_subscribers: Arc::new(RwLock::new(vec![])),
            base_headers: Arc::new(RwLock::new(vec![])),
            live_headers: Arc::new(RwLock::new(vec![])),
            sync_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the number of bulk ingestors.
    pub async fn set_bulk_ingestor_count(&self, count: usize) {
        *self.bulk_ingestor_count.write().await = count;
    }

    /// Set the number of live ingestors.
    pub async fn set_live_ingestor_count(&self, count: usize) {
        *self.live_ingestor_count.write().await = count;
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

    /// Set the listening state.
    async fn set_listening(&self, value: bool) {
        *self.listening.write().await = value;
    }

    /// Process all pending base headers from the queue.
    ///
    /// Drains the `base_headers` queue, computes the hash and determines
    /// the height for each header, validates parent linkage, inserts valid
    /// headers into storage, and fires reorg callbacks when necessary.
    pub async fn process_pending_headers(&self) -> crate::Result<()> {
        // Drain the queue
        let pending: Vec<BaseBlockHeader> = {
            let mut queue = self.base_headers.write().await;
            std::mem::take(&mut *queue)
        };

        if pending.is_empty() {
            return Ok(());
        }

        tracing::debug!("Processing {} pending headers", pending.len());

        let storage = self.storage.read().await;

        for base_header in pending {
            // Compute the hash and build a BlockHeader (height 0 placeholder)
            let block_header = base_header.to_block_header_at_height(0);

            // Determine the height by looking up the parent
            let genesis_hash = "0".repeat(64);
            let (height, previous_header_id) = if base_header.previous_hash == genesis_hash
                || base_header.previous_hash.is_empty()
            {
                // Genesis block
                (0u32, None)
            } else {
                // Look up parent in storage
                match storage
                    .find_live_header_for_block_hash(&base_header.previous_hash)
                    .await?
                {
                    Some(parent) => (parent.height + 1, Some(parent.header_id)),
                    None => {
                        tracing::warn!(
                            "Parent header {} not found for header {}, inserting anyway",
                            &base_header.previous_hash[..16.min(base_header.previous_hash.len())],
                            &block_header.hash[..16.min(block_header.hash.len())]
                        );
                        // Insert with height 0 and let storage figure it out
                        (0u32, None)
                    }
                }
            };

            // Build a LiveBlockHeader for insertion
            let live_header = LiveBlockHeader {
                version: base_header.version,
                previous_hash: base_header.previous_hash.clone(),
                merkle_root: base_header.merkle_root.clone(),
                time: base_header.time,
                bits: base_header.bits,
                nonce: base_header.nonce,
                height,
                hash: block_header.hash.clone(),
                chain_work: calculate_work(base_header.bits),
                is_chain_tip: false,
                is_active: false,
                header_id: 0,
                previous_header_id,
            };

            // Insert into storage
            let result = storage.insert_header(live_header).await?;

            if result.dupe {
                tracing::debug!("Skipping duplicate header {}", &block_header.hash[..16]);
                continue;
            }

            if result.added {
                tracing::debug!(
                    "Inserted header at height {} hash={}",
                    height,
                    &block_header.hash[..16]
                );
            }

            // Fire reorg callbacks if a reorg was detected
            if result.reorg_depth > 0 {
                let old_tip_header: BlockHeader = result
                    .prior_tip
                    .clone()
                    .map(|h| h.into())
                    .unwrap_or_else(|| block_header.clone());

                let new_tip_header = base_header.to_block_header_at_height(height);

                let deactivated: Vec<BlockHeader> = result
                    .deactivated_headers
                    .iter()
                    .map(|h| h.clone().into())
                    .collect();

                let event = ReorgEvent {
                    depth: result.reorg_depth,
                    old_tip: old_tip_header,
                    new_tip: new_tip_header,
                    deactivated_headers: deactivated,
                };

                tracing::info!(
                    "Reorg detected: depth={}, firing {} callbacks",
                    result.reorg_depth,
                    self.reorg_subscribers.read().await.len()
                );

                self.notify_reorg_subscribers(&event).await;
            }
        }

        Ok(())
    }

    /// Start background synchronization.
    ///
    /// Spawns an async task that coordinates bulk and live ingestors,
    /// processes header queues automatically, and fires callbacks to subscribers.
    pub async fn start_background_sync(&self) -> crate::Result<()> {
        if self.is_syncing.load(Ordering::SeqCst) {
            tracing::warn!("Background sync already running");
            return Ok(());
        }
        self.is_syncing.store(true, Ordering::SeqCst);
        tracing::info!("Starting background header sync");

        // Validate ingestor configuration if required
        if self.options.require_ingestors {
            self.validate_ingestor_config().await?;
        }

        // Mark as listening
        self.set_listening(true).await;

        // Clone the Arcs needed by the background task
        let is_syncing = Arc::clone(&self.is_syncing);
        let synchronized = Arc::clone(&self.synchronized);
        let base_headers = Arc::clone(&self.base_headers);
        let storage = Arc::clone(&self.storage);
        let reorg_subscribers = Arc::clone(&self.reorg_subscribers);

        let handle = tokio::spawn(async move {
            let mut first_pass_done = false;

            while is_syncing.load(Ordering::SeqCst) {
                // Drain the queue
                let pending: Vec<BaseBlockHeader> = {
                    let mut queue = base_headers.write().await;
                    std::mem::take(&mut *queue)
                };

                if !pending.is_empty() {
                    tracing::debug!(
                        "Background sync: processing {} pending headers",
                        pending.len()
                    );

                    let stor = storage.read().await;

                    for base_header in &pending {
                        let block_header = base_header.to_block_header_at_height(0);

                        let genesis_hash = "0".repeat(64);
                        let (height, previous_header_id) = if base_header.previous_hash
                            == genesis_hash
                            || base_header.previous_hash.is_empty()
                        {
                            (0u32, None)
                        } else {
                            match stor
                                .find_live_header_for_block_hash(&base_header.previous_hash)
                                .await
                            {
                                Ok(Some(parent)) => (parent.height + 1, Some(parent.header_id)),
                                _ => (0u32, None),
                            }
                        };

                        let live_header = LiveBlockHeader {
                            version: base_header.version,
                            previous_hash: base_header.previous_hash.clone(),
                            merkle_root: base_header.merkle_root.clone(),
                            time: base_header.time,
                            bits: base_header.bits,
                            nonce: base_header.nonce,
                            height,
                            hash: block_header.hash.clone(),
                            chain_work: calculate_work(base_header.bits),
                            is_chain_tip: false,
                            is_active: false,
                            header_id: 0,
                            previous_header_id,
                        };

                        match stor.insert_header(live_header).await {
                            Ok(result) => {
                                if result.reorg_depth > 0 {
                                    let old_tip_header: BlockHeader = result
                                        .prior_tip
                                        .clone()
                                        .map(|h| h.into())
                                        .unwrap_or_else(|| block_header.clone());
                                    let new_tip_header =
                                        base_header.to_block_header_at_height(height);
                                    let deactivated: Vec<BlockHeader> = result
                                        .deactivated_headers
                                        .iter()
                                        .map(|h| h.clone().into())
                                        .collect();
                                    let event = ReorgEvent {
                                        depth: result.reorg_depth,
                                        old_tip: old_tip_header,
                                        new_tip: new_tip_header,
                                        deactivated_headers: deactivated,
                                    };
                                    let callbacks = reorg_subscribers.read().await;
                                    for (_id, cb) in callbacks.iter() {
                                        cb(event.clone());
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Background sync: failed to insert header: {}", e);
                            }
                        }
                    }
                }

                // Mark synchronized after first successful processing pass
                if !first_pass_done {
                    first_pass_done = true;
                    *synchronized.write().await = true;
                    tracing::info!("Background sync: initial synchronization complete");
                }

                // Sleep before polling the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            tracing::info!("Background sync task exiting");
        });

        // Store the handle so stop_background_sync can abort it
        *self.sync_handle.write().await = Some(handle);

        Ok(())
    }

    /// Stop background synchronization.
    pub async fn stop_background_sync(&self) -> crate::Result<()> {
        if !self.is_syncing.load(Ordering::SeqCst) {
            return Ok(());
        }
        tracing::info!("Stopping background header sync");
        self.is_syncing.store(false, Ordering::SeqCst);
        self.set_listening(false).await;

        // Abort and clean up the background task
        let handle = self.sync_handle.write().await.take();
        if let Some(h) = handle {
            h.abort();
            // Ignore JoinError from abort
            let _ = h.await;
        }

        Ok(())
    }

    /// Check if background sync is currently running.
    pub fn is_background_syncing(&self) -> bool {
        self.is_syncing.load(Ordering::SeqCst)
    }

    /// Validate that required ingestors are configured.
    pub async fn validate_ingestor_config(&self) -> crate::Result<()> {
        let bulk_count = *self.bulk_ingestor_count.read().await;
        let live_count = *self.live_ingestor_count.read().await;

        if bulk_count == 0 {
            tracing::warn!("No bulk ingestors configured - bulk sync will not be available");
        }
        if live_count == 0 {
            tracing::warn!(
                "No live ingestors configured - real-time header updates will not be available"
            );
        }
        Ok(())
    }

    /// Fire header callbacks for all subscribers.
    async fn notify_header_subscribers(&self, header: &BlockHeader) {
        let callbacks = self.header_subscribers.read().await;
        for (_id, callback) in callbacks.iter() {
            callback(header.clone());
        }
    }

    /// Fire reorg callbacks for all subscribers.
    async fn notify_reorg_subscribers(&self, event: &ReorgEvent) {
        let callbacks = self.reorg_subscribers.read().await;
        for (_id, callback) in callbacks.iter() {
            callback(event.clone());
        }
    }

    /// Check if the instance is in readonly mode.
    fn check_readonly(&self) -> crate::Result<()> {
        if self.options.readonly {
            return Err(crate::Error::InvalidOperation(
                "Chaintracks is in readonly mode".to_string(),
            ));
        }
        Ok(())
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
            bulk_ingestor_count: *self.bulk_ingestor_count.read().await,
            live_ingestor_count: *self.live_ingestor_count.read().await,
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
        self.check_readonly()?;
        let block_header: BlockHeader = header.clone().into();
        let mut queue = self.base_headers.write().await;
        queue.push(header);

        // Notify subscribers that a header was queued for processing
        tracing::debug!(
            "Header with hash {} queued, notifying subscribers",
            block_header.hash
        );
        drop(queue); // Release the write lock before notifying
        self.notify_header_subscribers(&block_header).await;

        Ok(())
    }

    async fn start_listening(&self) -> Result<()> {
        self.check_readonly()?;
        // In a full implementation, this would:
        // 1. Start live ingestor(s) to receive new headers
        // 2. Set up a processing loop for incoming headers
        // 3. Notify subscribers of new headers and reorgs
        //
        // For now, we set the listening flag and let external code
        // push headers via add_header().
        *self.listening.write().await = true;
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
        self.check_readonly()?;
        *self.listening.write().await = false;
        *self.available.write().await = false;

        // Stop background sync if running
        self.is_syncing.store(false, Ordering::SeqCst);
        let handle = self.sync_handle.write().await.take();
        if let Some(h) = handle {
            h.abort();
            let _ = h.await;
        }

        let storage = self.storage.read().await;
        storage.destroy().await?;

        Ok(())
    }

    async fn validate(&self) -> Result<bool> {
        // Validate that the hash chain is consistent
        // Each header's previous_hash should match the hash of the previous header
        let storage = self.storage.read().await;
        let live_range = storage.find_live_height_range().await?;

        if live_range.is_none() {
            // No headers to validate
            return Ok(true);
        }

        let range = live_range.unwrap();
        let mut prev_hash: Option<String> = None;

        for height in range.low..=range.high {
            if let Some(header) = storage.find_header_for_height(height).await? {
                if let Some(expected_prev) = &prev_hash {
                    if header.previous_hash != *expected_prev {
                        tracing::warn!(
                            height = height,
                            expected = %expected_prev,
                            actual = %header.previous_hash,
                            "Chain validation failed: previous hash mismatch"
                        );
                        return Ok(false);
                    }
                }
                prev_hash = Some(header.hash.clone());
            }
        }

        Ok(true)
    }

    async fn export_bulk_headers(
        &self,
        folder: &str,
        headers_per_file: Option<u32>,
        max_height: Option<u32>,
    ) -> Result<()> {
        use std::fs::{self, File};
        use std::io::Write;

        let storage = self.storage.read().await;
        let tip = storage.find_chain_tip_header().await?;

        if tip.is_none() {
            return Ok(()); // No headers to export
        }

        let tip = tip.unwrap();
        let max_h = max_height.unwrap_or(tip.height);
        let per_file = headers_per_file.unwrap_or(500);

        // Create output folder
        fs::create_dir_all(folder).map_err(|e| {
            crate::Error::StorageError(format!("Failed to create export folder: {}", e))
        })?;

        let mut file_num = 0;
        let mut height = 0u32;

        while height <= max_h {
            let end_height = std::cmp::min(height + per_file - 1, max_h);

            // Collect headers for this file
            let mut header_bytes = Vec::new();
            for h in height..=end_height {
                if let Some(header) = storage.find_header_for_height(h).await? {
                    // Serialize header to 80 bytes
                    let base = BaseBlockHeader {
                        version: header.version,
                        previous_hash: header.previous_hash,
                        merkle_root: header.merkle_root,
                        time: header.time,
                        bits: header.bits,
                        nonce: header.nonce,
                    };
                    header_bytes.extend_from_slice(&base.to_bytes());
                }
            }

            if header_bytes.is_empty() {
                height = end_height + 1;
                continue;
            }

            let filename = format!("{}/headers_{:08}.bin", folder, file_num);
            let mut file = File::create(&filename).map_err(|e| {
                crate::Error::StorageError(format!("Failed to create file {}: {}", filename, e))
            })?;

            file.write_all(&header_bytes).map_err(|e| {
                crate::Error::StorageError(format!("Failed to write to {}: {}", filename, e))
            })?;

            tracing::debug!(
                file = %filename,
                from_height = height,
                to_height = end_height,
                headers = header_bytes.len() / 80,
                "Exported headers"
            );

            height = end_height + 1;
            file_num += 1;
        }

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

    #[tokio::test]
    async fn test_ingestor_tracking() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // Initially zero
        let info = ct.get_info().await.unwrap();
        assert_eq!(info.bulk_ingestor_count, 0);
        assert_eq!(info.live_ingestor_count, 0);

        // Set counts
        ct.set_bulk_ingestor_count(2).await;
        ct.set_live_ingestor_count(1).await;

        let info = ct.get_info().await.unwrap();
        assert_eq!(info.bulk_ingestor_count, 2);
        assert_eq!(info.live_ingestor_count, 1);
    }

    #[tokio::test]
    async fn test_start_listening() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        assert!(!ct.is_listening().await.unwrap());
        ct.start_listening().await.unwrap();
        assert!(ct.is_listening().await.unwrap());
    }

    #[tokio::test]
    async fn test_validate_empty_storage() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // Empty storage should validate as true
        assert!(ct.validate().await.unwrap());
    }

    #[tokio::test]
    async fn test_background_sync_lifecycle() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // Initially not syncing
        assert!(!ct.is_background_syncing());

        // Start background sync
        ct.start_background_sync().await.unwrap();
        assert!(ct.is_background_syncing());
        assert!(ct.is_listening().await.unwrap());

        // Starting again should be a no-op (warn but not error)
        ct.start_background_sync().await.unwrap();
        assert!(ct.is_background_syncing());

        // Stop background sync
        ct.stop_background_sync().await.unwrap();
        assert!(!ct.is_background_syncing());
        assert!(!ct.is_listening().await.unwrap());

        // Stopping again should be a no-op
        ct.stop_background_sync().await.unwrap();
        assert!(!ct.is_background_syncing());
    }

    #[tokio::test]
    async fn test_validate_ingestor_config() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // Should succeed (just logs warnings, no errors)
        ct.validate_ingestor_config().await.unwrap();

        // Set ingestor counts and validate again
        ct.set_bulk_ingestor_count(1).await;
        ct.set_live_ingestor_count(1).await;
        ct.validate_ingestor_config().await.unwrap();
    }

    #[tokio::test]
    async fn test_require_ingestors_option() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let mut options = ChaintracksOptions::default_testnet();
        options.require_ingestors = true;
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // start_background_sync should call validate_ingestor_config
        ct.start_background_sync().await.unwrap();
        assert!(ct.is_background_syncing());
    }

    #[tokio::test]
    async fn test_readonly_mode() {
        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let mut options = ChaintracksOptions::default_testnet();
        options.readonly = true;
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        // add_header should fail in readonly mode
        let header = BaseBlockHeader {
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "a".repeat(64),
            time: 1234567890,
            bits: 0x1d00ffff,
            nonce: 42,
        };
        let result = ct.add_header(header).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::Error::InvalidOperation(msg) => {
                assert!(msg.contains("readonly"));
            }
            other => panic!("Expected InvalidOperation, got: {:?}", other),
        }

        // start_listening should fail in readonly mode
        let result = ct.start_listening().await;
        assert!(result.is_err());

        // destroy should fail in readonly mode
        let result = ct.destroy().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_readonly_default_is_false() {
        let options = ChaintracksOptions::default();
        assert!(!options.readonly);
        assert!(!options.require_ingestors);
    }

    #[tokio::test]
    async fn test_header_subscriber_notification() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let storage = Box::new(MemoryStorage::new(Chain::Test));
        let options = ChaintracksOptions::default_testnet();
        let ct = Chaintracks::new(options, storage);

        ct.make_available().await.unwrap();

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let _sub_id = ct
            .subscribe_headers(Box::new(move |_header| {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .unwrap();

        // Add a header - should trigger the subscriber callback
        let header = BaseBlockHeader {
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "b".repeat(64),
            time: 1234567890,
            bits: 0x1d00ffff,
            nonce: 99,
        };
        ct.add_header(header).await.unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
