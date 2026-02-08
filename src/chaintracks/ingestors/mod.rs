//! Chaintracks ingestors
//!
//! This module provides ingestor implementations for fetching blockchain headers
//! from various sources:
//!
//! ## Bulk Ingestors (Historical Data)
//! - [`BulkCdnIngestor`] - Downloads headers from Babbage CDN (fast, preferred)
//! - [`BulkWocIngestor`] - Uses WhatsOnChain API (slower, reliable fallback)
//!
//! ## Live Ingestors (Real-time Updates)
//! - [`LivePollingIngestor`] - Polls WOC API at intervals (simple, reliable)
//! - [`LiveWebSocketIngestor`] - Uses WOC WebSocket for instant notifications (low latency)
//!
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Ingest/`

mod bulk_cdn;
mod bulk_woc;
mod live_polling;
mod live_websocket;

// Re-export bulk ingestors
pub use bulk_cdn::{
    BulkCdnIngestor, BulkCdnOptions, BulkHeaderFileInfo, BulkHeaderFilesInfo, DEFAULT_CDN_URL,
    LEGACY_CDN_URL,
};

pub use bulk_woc::{
    BulkWocIngestor, BulkWocOptions, WocChainInfo, WocHeaderByteFileLinks, WocHeaderResponse,
    WOC_API_URL_MAIN, WOC_API_URL_TEST,
};

// Re-export live ingestors
pub use live_polling::{
    woc_header_to_block_header, LivePollingIngestor, LivePollingOptions, WocGetHeadersHeader,
};

pub use live_websocket::{
    ws_header_to_block_header, LiveWebSocketIngestor, LiveWebSocketOptions, WocWsBlockHeader,
    WocWsMessage, WOC_WS_URL_MAIN, WOC_WS_URL_TEST,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaintracks::Chain;

    #[test]
    fn test_bulk_cdn_options() {
        let mainnet = BulkCdnOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);

        let testnet = BulkCdnOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);
    }

    #[test]
    fn test_bulk_woc_options() {
        let mainnet = BulkWocOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);

        let testnet = BulkWocOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);
    }

    #[test]
    fn test_live_polling_options() {
        let mainnet = LivePollingOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);

        let testnet = LivePollingOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);
    }

    #[test]
    fn test_live_websocket_options() {
        let mainnet = LiveWebSocketOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);

        let testnet = LiveWebSocketOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);
    }

    #[test]
    fn test_cdn_ingestor_creation() {
        let ingestor = BulkCdnIngestor::mainnet();
        assert!(ingestor.is_ok());
    }

    #[test]
    fn test_woc_ingestor_creation() {
        let ingestor = BulkWocIngestor::mainnet();
        assert!(ingestor.is_ok());
    }

    #[test]
    fn test_polling_ingestor_creation() {
        let ingestor = LivePollingIngestor::mainnet();
        assert!(ingestor.is_ok());
    }

    #[test]
    fn test_websocket_ingestor_creation() {
        let ingestor = LiveWebSocketIngestor::mainnet();
        assert!(ingestor.is_ok());
    }

    #[test]
    fn test_cdn_url_constants() {
        assert!(DEFAULT_CDN_URL.starts_with("https://"));
        assert!(DEFAULT_CDN_URL.contains("bsv-headers"));
        assert!(LEGACY_CDN_URL.starts_with("https://"));
        assert!(LEGACY_CDN_URL.contains("projectbabbage"));
    }

    #[test]
    fn test_woc_url_constants() {
        assert!(WOC_API_URL_MAIN.contains("whatsonchain"));
        assert!(WOC_API_URL_MAIN.contains("main"));
        assert!(WOC_API_URL_TEST.contains("whatsonchain"));
        assert!(WOC_API_URL_TEST.contains("test"));
        assert!(WOC_WS_URL_MAIN.contains("whatsonchain"));
        assert!(WOC_WS_URL_TEST.contains("whatsonchain"));
    }

    #[test]
    fn test_all_ingestors_testnet() {
        let cdn = BulkCdnIngestor::testnet();
        assert!(cdn.is_ok());

        let woc = BulkWocIngestor::testnet();
        assert!(woc.is_ok());

        let polling = LivePollingIngestor::testnet();
        assert!(polling.is_ok());

        let ws = LiveWebSocketIngestor::testnet();
        assert!(ws.is_ok());
    }

    #[test]
    fn test_woc_header_to_block_header() {
        let woc = WocGetHeadersHeader {
            hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
            confirmations: 800000,
            size: 285,
            height: 0,
            version: 1,
            version_hex: "00000001".to_string(),
            merkleroot: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
                .to_string(),
            time: 1231006505,
            median_time: 1231006505,
            nonce: 2083236893,
            bits: "1d00ffff".to_string(),
            difficulty: 1.0,
            chainwork: "0".repeat(64),
            previous_block_hash: None,
            next_block_hash: None,
            n_tx: 1,
            num_tx: 1,
        };

        let header = woc_header_to_block_header(&woc);
        assert_eq!(header.height, 0);
        assert_eq!(header.nonce, 2083236893);
        assert_eq!(header.bits, 0x1d00ffff);
    }

    #[test]
    fn test_ws_header_to_block_header() {
        let ws = WocWsBlockHeader {
            hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
            height: 0,
            version: 1,
            previous_block_hash: None,
            merkleroot: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
                .to_string(),
            time: 1231006505,
            bits: 486604799,
            nonce: 2083236893,
            confirmations: 800000,
            size: 285,
        };

        let header = ws_header_to_block_header(&ws);
        assert_eq!(header.height, 0);
        assert_eq!(header.nonce, 2083236893);
        assert_eq!(header.bits, 486604799);
    }
}
