//! CDN Bulk Header Ingestor
//!
//! Downloads bulk block headers from Babbage CDN.
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Ingest/BulkIngestorCDN.ts`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::chaintracks::{
    BlockHeader, BulkIngestor, BulkSyncResult, Chain, ChaintracksStorage, HeightRange,
    LiveBlockHeader,
};
use crate::Result;

/// Default CDN URL for block headers (Babbage Systems)
pub const DEFAULT_CDN_URL: &str = "https://bsv-headers.babbage.systems/";

/// Legacy CDN URL (project babbage)
pub const LEGACY_CDN_URL: &str = "https://cdn.projectbabbage.com/blockheaders/";

/// CDN bulk header file metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkHeaderFileInfo {
    /// File name
    pub file_name: String,
    /// Starting height
    pub from_height: u32,
    /// Ending height
    pub to_height: u32,
    /// Number of headers in file
    pub count: u32,
    /// SHA256 hash of file contents
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_hash: Option<String>,
    /// Chain this file is for
    pub chain: String,
    /// Source URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
}

/// CDN file listing response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkHeaderFilesInfo {
    /// List of available files
    pub files: Vec<BulkHeaderFileInfo>,
    /// Default headers per file
    pub headers_per_file: u32,
    /// Last updated timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<String>,
}

/// Options for CDN bulk ingestor
#[derive(Debug, Clone)]
pub struct BulkCdnOptions {
    /// Chain to ingest
    pub chain: Chain,
    /// CDN base URL
    pub cdn_url: String,
    /// JSON resource name for file listing
    pub json_resource: String,
    /// Maximum headers per file to select
    pub max_per_file: Option<u32>,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// User agent for requests
    pub user_agent: String,
}

impl Default for BulkCdnOptions {
    fn default() -> Self {
        BulkCdnOptions {
            chain: Chain::Main,
            cdn_url: DEFAULT_CDN_URL.to_string(),
            json_resource: "mainNetBlockHeaders.json".to_string(),
            max_per_file: None,
            timeout_secs: 60,
            user_agent: "BsvWalletToolbox/1.0".to_string(),
        }
    }
}

impl BulkCdnOptions {
    /// Create options for mainnet
    pub fn mainnet() -> Self {
        BulkCdnOptions {
            chain: Chain::Main,
            json_resource: "mainNetBlockHeaders.json".to_string(),
            ..Default::default()
        }
    }

    /// Create options for testnet
    pub fn testnet() -> Self {
        BulkCdnOptions {
            chain: Chain::Test,
            json_resource: "testNetBlockHeaders.json".to_string(),
            ..Default::default()
        }
    }
}

/// CDN-based bulk header ingestor
///
/// Downloads binary header files from Babbage CDN.
/// Files are organized by height ranges and contain 80 bytes per header.
pub struct BulkCdnIngestor {
    options: BulkCdnOptions,
    client: reqwest::Client,
    storage: Option<Arc<RwLock<Box<dyn ChaintracksStorage>>>>,
    /// Cached file listing
    available_files: RwLock<Option<BulkHeaderFilesInfo>>,
}

impl BulkCdnIngestor {
    /// Create a new CDN bulk ingestor
    pub fn new(options: BulkCdnOptions) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(options.timeout_secs))
            .user_agent(&options.user_agent)
            .build()?;

        Ok(BulkCdnIngestor {
            options,
            client,
            storage: None,
            available_files: RwLock::new(None),
        })
    }

    /// Create a default mainnet ingestor
    pub fn mainnet() -> Result<Self> {
        Self::new(BulkCdnOptions::mainnet())
    }

    /// Create a default testnet ingestor
    pub fn testnet() -> Result<Self> {
        Self::new(BulkCdnOptions::testnet())
    }

    /// Build full URL for a resource
    fn build_url(&self, resource: &str) -> String {
        let base = self.options.cdn_url.trim_end_matches('/');
        format!("{}/{}", base, resource)
    }

    /// Fetch the JSON file listing from CDN
    async fn fetch_file_listing(&self) -> Result<BulkHeaderFilesInfo> {
        let url = self.build_url(&self.options.json_resource);
        debug!("Fetching CDN file listing from: {}", url);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(crate::Error::NetworkError(format!(
                "CDN returned status {}: {}",
                response.status(),
                url
            )));
        }

        let info: BulkHeaderFilesInfo = response.json().await?;
        info!("CDN has {} files available", info.files.len());

        Ok(info)
    }

    /// Download a binary header file
    async fn download_file(&self, file: &BulkHeaderFileInfo) -> Result<Vec<u8>> {
        let source = file.source_url.as_deref().unwrap_or(&self.options.cdn_url);
        let url = format!("{}/{}", source.trim_end_matches('/'), file.file_name);

        debug!(
            "Downloading header file: {} (heights {}-{})",
            file.file_name, file.from_height, file.to_height
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(crate::Error::NetworkError(format!(
                "Failed to download {}: status {}",
                url,
                response.status()
            )));
        }

        let bytes = response.bytes().await?;
        let data = bytes.to_vec();

        // Validate file size
        let expected_size = (file.count as usize) * 80;
        if data.len() != expected_size {
            warn!(
                "File {} size mismatch: expected {} bytes, got {}",
                file.file_name,
                expected_size,
                data.len()
            );
        }

        debug!(
            "Downloaded {} bytes for heights {}-{}",
            data.len(),
            file.from_height,
            file.to_height
        );

        Ok(data)
    }

    /// Parse headers from binary data
    fn parse_headers(&self, data: &[u8], start_height: u32) -> Vec<BlockHeader> {
        let mut headers = Vec::with_capacity(data.len() / 80);

        for (i, chunk) in data.chunks(80).enumerate() {
            if chunk.len() != 80 {
                warn!("Incomplete header chunk at index {}", i);
                break;
            }

            let header = self.deserialize_header(chunk, start_height + i as u32);
            headers.push(header);
        }

        headers
    }

    /// Deserialize a single 80-byte header
    fn deserialize_header(&self, data: &[u8], height: u32) -> BlockHeader {
        // Version (4 bytes, little-endian)
        let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        // Previous hash (32 bytes)
        let mut prev_hash = [0u8; 32];
        prev_hash.copy_from_slice(&data[4..36]);
        let previous_hash = hex::encode(prev_hash);

        // Merkle root (32 bytes)
        let mut merkle = [0u8; 32];
        merkle.copy_from_slice(&data[36..68]);
        let merkle_root = hex::encode(merkle);

        // Time (4 bytes, little-endian)
        let time = u32::from_le_bytes([data[68], data[69], data[70], data[71]]);

        // Bits (4 bytes, little-endian)
        let bits = u32::from_le_bytes([data[72], data[73], data[74], data[75]]);

        // Nonce (4 bytes, little-endian)
        let nonce = u32::from_le_bytes([data[76], data[77], data[78], data[79]]);

        // Compute block hash
        let hash = self.compute_block_hash(data);

        BlockHeader {
            version,
            previous_hash,
            merkle_root,
            time,
            bits,
            nonce,
            height,
            hash,
        }
    }

    /// Compute double SHA256 hash of header
    fn compute_block_hash(&self, header_bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(header_bytes);
        let first_hash = hasher.finalize();

        let mut hasher = Sha256::new();
        hasher.update(first_hash);
        let second_hash = hasher.finalize();

        // Reverse for display (Bitcoin convention)
        let mut reversed = second_hash.to_vec();
        reversed.reverse();

        hex::encode(reversed)
    }

    /// Select appropriate files for a height range
    fn select_files_for_range(
        &self,
        files: &[BulkHeaderFileInfo],
        range: &HeightRange,
    ) -> Vec<BulkHeaderFileInfo> {
        let chain_str = self.options.chain.as_str();

        files
            .iter()
            .filter(|f| {
                // Must be for correct chain
                if f.chain != chain_str {
                    return false;
                }
                // Must overlap with requested range
                let file_range = HeightRange::new(f.from_height, f.to_height);
                range.overlaps(&file_range)
            })
            .cloned()
            .collect()
    }

    /// Get the available range from cached file listing
    async fn get_available_range_from_cache(&self) -> Result<HeightRange> {
        let cached = self.available_files.read().await;

        if let Some(ref info) = *cached {
            let chain_str = self.options.chain.as_str();
            let mut low = u32::MAX;
            let mut high = 0u32;

            for file in &info.files {
                if file.chain == chain_str {
                    low = low.min(file.from_height);
                    high = high.max(file.to_height);
                }
            }

            if low <= high {
                return Ok(HeightRange::new(low, high));
            }
        }

        Ok(HeightRange::new(0, 0))
    }
}

#[async_trait]
impl BulkIngestor for BulkCdnIngestor {
    async fn get_present_height(&self) -> Result<Option<u32>> {
        // CDN doesn't provide live height - return None
        // Other ingestors (like WOC) should provide this
        Ok(None)
    }

    async fn synchronize(
        &self,
        present_height: u32,
        before: u32,
        prior_live_headers: &[LiveBlockHeader],
    ) -> Result<BulkSyncResult> {
        info!(
            "CDN bulk sync: present_height={}, before={}",
            present_height, before
        );

        // Fetch file listing if not cached
        {
            let mut cached = self.available_files.write().await;
            if cached.is_none() {
                *cached = Some(self.fetch_file_listing().await?);
            }
        }

        let available_range = self.get_available_range_from_cache().await?;

        // Determine what we need
        let needed = HeightRange::new(before, present_height);

        // Check what CDN can provide
        let can_provide = if available_range.overlaps(&needed) {
            available_range
        } else {
            return Ok(BulkSyncResult {
                live_headers: prior_live_headers
                    .iter()
                    .map(|h| h.clone().into())
                    .collect(),
                done: false,
            });
        };

        // Fetch headers
        let fetch_range = HeightRange::new(
            needed.low.max(can_provide.low),
            needed.high.min(can_provide.high),
        );

        let headers = self
            .fetch_headers(
                before,
                fetch_range.clone(),
                Some(fetch_range),
                prior_live_headers,
            )
            .await?;

        let done = headers
            .last()
            .map(|h| h.height >= present_height)
            .unwrap_or(false);

        Ok(BulkSyncResult {
            live_headers: headers,
            done,
        })
    }

    async fn fetch_headers(
        &self,
        _before: u32,
        fetch_range: HeightRange,
        _bulk_range: Option<HeightRange>,
        _prior_live_headers: &[LiveBlockHeader],
    ) -> Result<Vec<BlockHeader>> {
        // Ensure we have file listing
        {
            let mut cached = self.available_files.write().await;
            if cached.is_none() {
                *cached = Some(self.fetch_file_listing().await?);
            }
        }

        let files_info = self.available_files.read().await;
        let files = files_info
            .as_ref()
            .map(|i| i.files.clone())
            .unwrap_or_default();

        // Select files that cover the requested range
        let selected = self.select_files_for_range(&files, &fetch_range);

        if selected.is_empty() {
            debug!("No CDN files available for range {:?}", fetch_range);
            return Ok(vec![]);
        }

        let mut all_headers = Vec::new();

        for file in selected {
            let data = self.download_file(&file).await?;
            let headers = self.parse_headers(&data, file.from_height);

            // Filter to only headers in requested range
            for header in headers {
                if fetch_range.contains(header.height) {
                    all_headers.push(header);
                }
            }
        }

        // Sort by height
        all_headers.sort_by_key(|h| h.height);

        info!(
            "Fetched {} headers from CDN for range {:?}",
            all_headers.len(),
            fetch_range
        );

        Ok(all_headers)
    }

    async fn set_storage(&mut self, storage: Box<dyn ChaintracksStorage>) -> Result<()> {
        self.storage = Some(Arc::new(RwLock::new(storage)));
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        info!("CDN bulk ingestor shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_header() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();

        // Genesis block header bytes (80 bytes)
        let genesis_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let genesis_bytes = hex::decode(genesis_hex).unwrap();

        let header = ingestor.deserialize_header(&genesis_bytes, 0);

        assert_eq!(header.height, 0);
        assert_eq!(header.version, 1);
        assert_eq!(header.nonce, 2083236893);
    }

    #[test]
    fn test_options_creation() {
        let mainnet = BulkCdnOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);
        assert!(mainnet.json_resource.contains("main"));

        let testnet = BulkCdnOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);
        assert!(testnet.json_resource.contains("test"));
    }

    #[tokio::test]
    async fn test_build_url() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();
        let url = ingestor.build_url("test.json");
        assert!(url.ends_with("/test.json"));
        assert!(!url.contains("//test")); // No double slashes
    }

    #[test]
    fn test_default_cdn_url() {
        assert!(DEFAULT_CDN_URL.starts_with("https://"));
        assert!(DEFAULT_CDN_URL.contains("bsv-headers"));
    }

    #[test]
    fn test_legacy_cdn_url() {
        assert!(LEGACY_CDN_URL.starts_with("https://"));
        assert!(LEGACY_CDN_URL.contains("projectbabbage"));
    }

    #[test]
    fn test_options_default() {
        let opts = BulkCdnOptions::default();
        assert_eq!(opts.chain, Chain::Main);
        assert_eq!(opts.timeout_secs, 60);
        assert!(opts.cdn_url.contains("bsv-headers"));
    }

    #[test]
    fn test_bulk_header_file_info_serialization() {
        let info = BulkHeaderFileInfo {
            file_name: "0_99999_headers.bin".to_string(),
            from_height: 0,
            to_height: 99999,
            count: 100000,
            file_hash: Some("abc123".to_string()),
            chain: "main".to_string(),
            source_url: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("fileName"));
        assert!(json.contains("fromHeight"));
        assert!(json.contains("toHeight"));

        let deserialized: BulkHeaderFileInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.from_height, 0);
        assert_eq!(deserialized.to_height, 99999);
    }

    #[test]
    fn test_bulk_header_files_info_serialization() {
        let info = BulkHeaderFilesInfo {
            files: vec![],
            headers_per_file: 100000,
            last_updated: Some("2024-01-01".to_string()),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("headersPerFile"));
        assert!(json.contains("lastUpdated"));
    }

    #[test]
    fn test_select_files_for_range() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();

        let files = vec![
            BulkHeaderFileInfo {
                file_name: "0_99999.bin".to_string(),
                from_height: 0,
                to_height: 99999,
                count: 100000,
                file_hash: None,
                chain: "main".to_string(),
                source_url: None,
            },
            BulkHeaderFileInfo {
                file_name: "100000_199999.bin".to_string(),
                from_height: 100000,
                to_height: 199999,
                count: 100000,
                file_hash: None,
                chain: "main".to_string(),
                source_url: None,
            },
            BulkHeaderFileInfo {
                file_name: "0_99999_test.bin".to_string(),
                from_height: 0,
                to_height: 99999,
                count: 100000,
                file_hash: None,
                chain: "test".to_string(),
                source_url: None,
            },
        ];

        // Test selecting mainnet files in range
        let range = HeightRange::new(50000, 150000);
        let selected = ingestor.select_files_for_range(&files, &range);

        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|f| f.chain == "main"));
    }

    #[test]
    fn test_parse_headers_multiple() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();

        // Create 2 headers worth of data (160 bytes)
        let genesis_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let genesis_bytes = hex::decode(genesis_hex).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&genesis_bytes);
        data.extend_from_slice(&genesis_bytes);

        let headers = ingestor.parse_headers(&data, 0);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].height, 0);
        assert_eq!(headers[1].height, 1);
    }

    #[test]
    fn test_parse_headers_incomplete() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();

        // 90 bytes - 1 complete header + 10 bytes
        let genesis_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let genesis_bytes = hex::decode(genesis_hex).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&genesis_bytes);
        data.extend_from_slice(&[0u8; 10]); // Incomplete header

        let headers = ingestor.parse_headers(&data, 0);

        // Should only parse the complete header
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn test_compute_block_hash() {
        let ingestor = BulkCdnIngestor::new(BulkCdnOptions::mainnet()).unwrap();

        // Genesis block header
        let genesis_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let genesis_bytes = hex::decode(genesis_hex).unwrap();

        let hash = ingestor.compute_block_hash(&genesis_bytes);

        // Genesis block hash (reversed for display)
        assert_eq!(
            hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[tokio::test]
    async fn test_ingestor_creation() {
        let mainnet = BulkCdnIngestor::mainnet();
        assert!(mainnet.is_ok());

        let testnet = BulkCdnIngestor::testnet();
        assert!(testnet.is_ok());
    }

    #[tokio::test]
    async fn test_get_present_height() {
        let ingestor = BulkCdnIngestor::mainnet().unwrap();

        // CDN doesn't provide live height
        let height = ingestor.get_present_height().await;
        assert!(height.is_ok());
        assert!(height.unwrap().is_none());
    }
}
