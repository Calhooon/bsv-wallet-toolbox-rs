//! Chaintracks HTTP service client.
//!
//! Provides block header lookups and merkle root validation via a remote
//! Chaintracks server. Also implements the `ChainTracker` trait from bsv-sdk.

use crate::services::traits::BlockHeader;
use crate::{Error, Result};
use async_trait::async_trait;
use bsv_sdk::transaction::{ChainTracker, ChainTrackerError};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Chaintracks service client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaintracksConfig {
    /// Chaintracks API URL (e.g. `https://api.calhouninfra.com`).
    pub url: String,
    /// Optional API key.
    pub api_key: Option<String>,
}

/// Generic response frame from Chaintracks API.
///
/// All endpoints return `{"status": "success"|"error", "value": T}`.
#[derive(Debug, Deserialize)]
pub struct ResponseFrame<T> {
    pub status: String,
    pub value: Option<T>,
}

impl<T> ResponseFrame<T> {
    fn is_success(&self) -> bool {
        self.status == "success"
    }
}

/// Block header as returned by the Chaintracks API (camelCase JSON).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CtBlockHeader {
    pub version: u32,
    pub previous_hash: String,
    pub merkle_root: String,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    pub height: u32,
    pub hash: String,
}

impl From<CtBlockHeader> for BlockHeader {
    fn from(ct: CtBlockHeader) -> Self {
        BlockHeader {
            version: ct.version,
            previous_hash: ct.previous_hash,
            merkle_root: ct.merkle_root,
            time: ct.time,
            bits: ct.bits,
            nonce: ct.nonce,
            height: ct.height,
            hash: ct.hash,
        }
    }
}

/// Chaintracks HTTP service client.
///
/// Provides block header lookups via a remote Chaintracks server.
/// Follows the same provider pattern as `BlockHeaderService` (BHS).
pub struct ChaintracksServiceClient {
    client: Client,
    config: ChaintracksConfig,
}

impl ChaintracksServiceClient {
    /// Create a new Chaintracks service client.
    pub fn new(config: ChaintracksConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { client, config }
    }

    /// Create from a URL string.
    pub fn from_url(url: &str) -> Self {
        Self::new(ChaintracksConfig {
            url: url.to_string(),
            api_key: None,
        })
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.config.url.trim_end_matches('/'), path)
    }

    fn build_request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url);
        if let Some(ref key) = self.config.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        req
    }

    /// Get the current blockchain height.
    pub async fn get_present_height(&self) -> Result<u32> {
        let url = self.build_url("/getPresentHeight");
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Chaintracks getPresentHeight: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks getPresentHeight failed: HTTP {}",
                response.status()
            )));
        }

        let frame: ResponseFrame<u32> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Chaintracks parse error: {}", e)))?;

        if !frame.is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks getPresentHeight: status={}",
                frame.status
            )));
        }

        frame.value.ok_or_else(|| {
            Error::ServiceError("Chaintracks getPresentHeight: missing value".to_string())
        })
    }

    /// Find block header by height.
    pub async fn find_header_for_height(&self, height: u32) -> Result<BlockHeader> {
        let url = self.build_url(&format!("/findHeaderHexForHeight?height={}", height));
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Chaintracks findHeaderHexForHeight: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForHeight failed: HTTP {}",
                response.status()
            )));
        }

        let frame: ResponseFrame<CtBlockHeader> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Chaintracks header parse: {}", e)))?;

        if !frame.is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForHeight: status={}",
                frame.status
            )));
        }

        frame
            .value
            .map(BlockHeader::from)
            .ok_or_else(|| Error::NotFound {
                entity: "BlockHeader".to_string(),
                id: height.to_string(),
            })
    }

    /// Find the chain tip header.
    pub async fn find_chain_tip_header(&self) -> Result<BlockHeader> {
        let url = self.build_url("/findChainTipHeaderHex");
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("Chaintracks findChainTipHeaderHex: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findChainTipHeaderHex failed: HTTP {}",
                response.status()
            )));
        }

        let frame: ResponseFrame<CtBlockHeader> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Chaintracks tip parse: {}", e)))?;

        if !frame.is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findChainTipHeaderHex: status={}",
                frame.status
            )));
        }

        frame
            .value
            .map(BlockHeader::from)
            .ok_or_else(|| Error::ServiceError("Chaintracks: no chain tip".to_string()))
    }

    /// Find block header by block hash.
    pub async fn find_header_for_block_hash(&self, hash: &str) -> Result<BlockHeader> {
        let url = self.build_url(&format!("/findHeaderHexForBlockHash?hash={}", hash));
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| {
                Error::NetworkError(format!("Chaintracks findHeaderHexForBlockHash: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForBlockHash failed: HTTP {}",
                response.status()
            )));
        }

        let frame: ResponseFrame<CtBlockHeader> = response
            .json()
            .await
            .map_err(|e| Error::ServiceError(format!("Chaintracks header parse: {}", e)))?;

        if !frame.is_success() {
            return Err(Error::ServiceError(format!(
                "Chaintracks findHeaderHexForBlockHash: status={}",
                frame.status
            )));
        }

        frame
            .value
            .map(BlockHeader::from)
            .ok_or_else(|| Error::NotFound {
                entity: "BlockHeader".to_string(),
                id: hash.to_string(),
            })
    }

    /// Validate a merkle root for a given block height.
    pub async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool> {
        let header = self.find_header_for_height(height).await?;
        Ok(header.merkle_root == root)
    }
}

#[async_trait]
impl ChainTracker for ChaintracksServiceClient {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        ChaintracksServiceClient::is_valid_root_for_height(self, root, height)
            .await
            .map_err(|e| ChainTrackerError::Other(e.to_string()))
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        self.get_present_height()
            .await
            .map_err(|e| ChainTrackerError::NetworkError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaintracks_config() {
        let config = ChaintracksConfig {
            url: "https://api.calhouninfra.com".to_string(),
            api_key: None,
        };
        assert!(config.url.contains("calhouninfra"));
    }

    #[test]
    fn test_chaintracks_from_url() {
        let ct = ChaintracksServiceClient::from_url("https://api.calhouninfra.com");
        assert_eq!(ct.config.url, "https://api.calhouninfra.com");
    }

    #[test]
    fn test_response_frame_success() {
        let json = r#"{"status":"success","value":937627}"#;
        let frame: ResponseFrame<u32> = serde_json::from_str(json).unwrap();
        assert!(frame.is_success());
        assert_eq!(frame.value, Some(937627));
    }

    #[test]
    fn test_ct_block_header_deser() {
        let json = r#"{
            "version": 536870912,
            "previousHash": "0000000000000000abc123",
            "merkleRoot": "def456",
            "time": 1700000000,
            "bits": 402917821,
            "nonce": 12345,
            "height": 937627,
            "hash": "000000000000000099887766"
        }"#;
        let ct: CtBlockHeader = serde_json::from_str(json).unwrap();
        assert_eq!(ct.height, 937627);

        let header: BlockHeader = ct.into();
        assert_eq!(header.height, 937627);
        assert_eq!(header.previous_hash, "0000000000000000abc123");
    }
}
