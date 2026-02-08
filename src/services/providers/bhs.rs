//! Block Header Service (BHS) provider.
//!
//! Provides block header lookups and merkle root validation via a dedicated
//! header service API. This is the primary source for header data in production.

use crate::services::traits::BlockHeader;
use crate::{Error, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Block Header Service provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BhsConfig {
    /// BHS API URL.
    pub url: String,
    /// Optional API key.
    pub api_key: Option<String>,
}

impl BhsConfig {
    /// Create a mainnet BHS configuration.
    pub fn mainnet() -> Self {
        Self {
            url: "https://bhs.babbage.systems".to_string(),
            api_key: None,
        }
    }

    /// Create a testnet BHS configuration.
    pub fn testnet() -> Self {
        Self {
            url: "https://bhs-test.babbage.systems".to_string(),
            api_key: None,
        }
    }
}

/// Block Header Service provider.
pub struct BlockHeaderService {
    client: Client,
    config: BhsConfig,
}

impl BlockHeaderService {
    /// Create a new BHS provider.
    pub fn new(config: BhsConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { client, config }
    }

    /// Create a new BHS provider from URL.
    pub fn from_url(url: &str) -> Self {
        Self::new(BhsConfig {
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
    pub async fn current_height(&self) -> Result<u32> {
        let url = self.build_url("/api/v1/chain/tip/height");
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("BHS current_height: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "BHS current_height failed: HTTP {}",
                response.status()
            )));
        }

        let text = response
            .text()
            .await
            .map_err(|e| Error::ServiceError(format!("BHS parse error: {}", e)))?;
        text.trim()
            .parse::<u32>()
            .map_err(|e| Error::ServiceError(format!("BHS height parse: {}", e)))
    }

    /// Get block header by height.
    pub async fn chain_header_by_height(&self, height: u32) -> Result<BlockHeader> {
        let url = self.build_url(&format!("/api/v1/chain/header/byHeight?height={}", height));
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("BHS header_by_height: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "BHS header_by_height failed: HTTP {}",
                response.status()
            )));
        }

        response
            .json::<BlockHeader>()
            .await
            .map_err(|e| Error::ServiceError(format!("BHS header parse: {}", e)))
    }

    /// Validate a merkle root for a given block height.
    pub async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool> {
        let header = self.chain_header_by_height(height).await?;
        Ok(header.merkle_root == root)
    }

    /// Find the chain tip header.
    pub async fn find_chain_tip_header(&self) -> Result<BlockHeader> {
        let url = self.build_url("/api/v1/chain/header/tip");
        let response = self
            .build_request(&url)
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("BHS chain_tip: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::ServiceError(format!(
                "BHS chain_tip failed: HTTP {}",
                response.status()
            )));
        }

        response
            .json::<BlockHeader>()
            .await
            .map_err(|e| Error::ServiceError(format!("BHS tip parse: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bhs_config_mainnet() {
        let config = BhsConfig::mainnet();
        assert!(config.url.contains("bhs.babbage.systems"));
        assert!(config.api_key.is_none());
    }

    #[test]
    fn test_bhs_config_testnet() {
        let config = BhsConfig::testnet();
        assert!(config.url.contains("bhs-test"));
    }

    #[test]
    fn test_bhs_from_url() {
        let bhs = BlockHeaderService::from_url("https://custom-bhs.example.com");
        assert_eq!(bhs.config.url, "https://custom-bhs.example.com");
    }
}
