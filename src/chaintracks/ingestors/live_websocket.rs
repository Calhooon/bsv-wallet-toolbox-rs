//! WebSocket-based Live Header Ingestor
//!
//! Connects to WhatsOnChain WebSocket for real-time block header updates.
//! Based on TypeScript: `/Users/johncalhoun/bsv/wallet-toolbox/src/services/chaintracker/chaintracks/Ingest/LiveIngestorWhatsOnChainWs.ts`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::chaintracks::{BlockHeader, Chain, ChaintracksStorage, LiveBlockHeader, LiveIngestor};
use crate::Result;

/// WebSocket URLs for WhatsOnChain
pub const WOC_WS_URL_MAIN: &str = "wss://socket-v2.whatsonchain.com/websocket/blockHeaders";
pub const WOC_WS_URL_TEST: &str = "wss://socket-v2-testnet.whatsonchain.com/websocket/blockHeaders";

/// WebSocket URL for historical headers
#[allow(dead_code)]
pub const WOC_WS_HISTORY_URL_MAIN: &str =
    "wss://socket-v2.whatsonchain.com/websocket/blockheaders/history";
#[allow(dead_code)]
pub const WOC_WS_HISTORY_URL_TEST: &str =
    "wss://socket-v2-testnet.whatsonchain.com/websocket/blockheaders/history";

/// WOC REST API URLs (for header lookup fallback)
pub const WOC_API_URL_MAIN: &str = "https://api.whatsonchain.com/v1/bsv/main";
pub const WOC_API_URL_TEST: &str = "https://api.whatsonchain.com/v1/bsv/test";

/// WebSocket message types from WOC
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum WocWsMessage {
    /// Header data message
    HeaderData {
        channel: Option<String>,
        #[serde(rename = "pub")]
        pub_data: Option<WocPubData>,
        data: Option<WocPubData>,
    },
    /// Typed message (subscribe, unsubscribe, etc.)
    TypedMessage {
        #[serde(rename = "type")]
        msg_type: u32,
        channel: Option<String>,
        data: Option<serde_json::Value>,
    },
    /// Connection info
    Connect { connect: String },
    /// Empty ping response
    Empty {},
}

/// Published header data wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WocPubData {
    pub data: Option<WocWsBlockHeader>,
}

/// Block header from WebSocket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WocWsBlockHeader {
    pub hash: String,
    pub height: u32,
    pub version: u32,
    #[serde(rename = "previousblockhash")]
    pub previous_block_hash: Option<String>,
    pub merkleroot: String,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    #[serde(default)]
    pub confirmations: u32,
    #[serde(default)]
    pub size: u64,
}

/// Options for WebSocket live ingestor
#[derive(Debug, Clone)]
pub struct LiveWebSocketOptions {
    /// Chain to monitor
    pub chain: Chain,
    /// API key (optional)
    pub api_key: Option<String>,
    /// Idle timeout in milliseconds before reconnecting
    pub idle_timeout_ms: u64,
    /// Ping interval in milliseconds
    pub ping_interval_ms: u64,
    /// Maximum reconnection attempts
    pub max_reconnect_attempts: u32,
    /// Reconnect delay in milliseconds
    pub reconnect_delay_ms: u64,
    /// User agent for HTTP requests (fallback)
    pub user_agent: String,
    /// HTTP timeout for fallback requests
    pub http_timeout_secs: u64,
}

impl Default for LiveWebSocketOptions {
    fn default() -> Self {
        LiveWebSocketOptions {
            chain: Chain::Main,
            api_key: None,
            idle_timeout_ms: 100_000,
            ping_interval_ms: 10_000,
            max_reconnect_attempts: 10,
            reconnect_delay_ms: 5000,
            user_agent: "BsvWalletToolbox/1.0".to_string(),
            http_timeout_secs: 30,
        }
    }
}

impl LiveWebSocketOptions {
    /// Create options for mainnet
    pub fn mainnet() -> Self {
        Self::default()
    }

    /// Create options for testnet
    pub fn testnet() -> Self {
        LiveWebSocketOptions {
            chain: Chain::Test,
            ..Default::default()
        }
    }

    /// Set API key
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, ms: u64) -> Self {
        self.idle_timeout_ms = ms;
        self
    }
}

/// Error types specific to WebSocket operations
#[derive(Debug)]
#[allow(dead_code)]
pub enum WsError {
    ConnectionFailed(String),
    MessageParseFailed(String),
    IdleTimeout,
    Stopped,
}

impl std::fmt::Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WsError::ConnectionFailed(msg) => write!(f, "WebSocket connection failed: {}", msg),
            WsError::MessageParseFailed(msg) => write!(f, "Failed to parse message: {}", msg),
            WsError::IdleTimeout => write!(f, "WebSocket idle timeout"),
            WsError::Stopped => write!(f, "WebSocket stopped"),
        }
    }
}

/// WebSocket-based live header ingestor
///
/// Connects to WhatsOnChain WebSocket for real-time block notifications.
/// Provides lower latency than polling but requires persistent connection.
pub struct LiveWebSocketIngestor {
    options: LiveWebSocketOptions,
    http_client: reqwest::Client,
    storage: Option<Arc<RwLock<Box<dyn ChaintracksStorage>>>>,

    /// Whether the ingestor should be running
    running: Arc<AtomicBool>,

    /// Broadcast channel for new headers
    sender: broadcast::Sender<LiveBlockHeader>,

    /// Error count for monitoring
    error_count: Arc<RwLock<Vec<(i32, String)>>>,

    /// Stop signal sender
    stop_signal: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl LiveWebSocketIngestor {
    /// Create a new WebSocket live ingestor
    pub fn new(options: LiveWebSocketOptions) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(options.http_timeout_secs))
            .user_agent(&options.user_agent)
            .build()?;

        let (sender, _) = broadcast::channel(100);

        Ok(LiveWebSocketIngestor {
            options,
            http_client,
            storage: None,
            running: Arc::new(AtomicBool::new(false)),
            sender,
            error_count: Arc::new(RwLock::new(Vec::new())),
            stop_signal: Arc::new(Mutex::new(None)),
        })
    }

    /// Create a default mainnet ingestor
    pub fn mainnet() -> Result<Self> {
        Self::new(LiveWebSocketOptions::mainnet())
    }

    /// Create a default testnet ingestor
    pub fn testnet() -> Result<Self> {
        Self::new(LiveWebSocketOptions::testnet())
    }

    /// Get WebSocket URL for the configured chain
    fn ws_url(&self) -> &'static str {
        match self.options.chain {
            Chain::Main => WOC_WS_URL_MAIN,
            Chain::Test => WOC_WS_URL_TEST,
        }
    }

    /// Get REST API URL for the configured chain
    fn api_url(&self) -> &'static str {
        match self.options.chain {
            Chain::Main => WOC_API_URL_MAIN,
            Chain::Test => WOC_API_URL_TEST,
        }
    }

    /// Fetch a header by hash via HTTP (fallback)
    async fn fetch_header_by_hash_http(&self, hash: &str) -> Result<Option<BlockHeader>> {
        let url = format!("{}/block/{}/header", self.api_url(), hash);
        debug!("Fetching header by hash via HTTP: {}", hash);

        let response = self.http_client.get(&url).send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(crate::Error::NetworkError(format!(
                "WOC header lookup returned status {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct WocHeaderResp {
            hash: String,
            height: u32,
            version: u32,
            merkleroot: String,
            time: u32,
            bits: String,
            nonce: u32,
            previousblockhash: Option<String>,
        }

        let woc: WocHeaderResp = response.json().await?;
        let bits = u32::from_str_radix(&woc.bits, 16).unwrap_or(0);

        Ok(Some(BlockHeader {
            version: woc.version,
            previous_hash: woc.previousblockhash.unwrap_or_else(|| "0".repeat(64)),
            merkle_root: woc.merkleroot,
            time: woc.time,
            bits,
            nonce: woc.nonce,
            height: woc.height,
            hash: woc.hash,
        }))
    }

    /// Run the WebSocket listener loop with reconnection
    async fn websocket_loop(self: Arc<Self>, live_headers: Arc<RwLock<Vec<BlockHeader>>>) {
        info!("Starting WebSocket listener for {:?}", self.options.chain);

        let mut reconnect_attempts = 0;

        while self.running.load(Ordering::SeqCst) {
            match self.connect_and_listen(&live_headers).await {
                Ok(true) => {
                    // Normal shutdown requested
                    info!("WebSocket listener stopped normally");
                    break;
                }
                Ok(false) => {
                    // Connection lost, try to reconnect
                    reconnect_attempts += 1;
                    if reconnect_attempts > self.options.max_reconnect_attempts {
                        error!("Max reconnection attempts reached");
                        break;
                    }

                    warn!(
                        "WebSocket connection lost, reconnecting (attempt {})",
                        reconnect_attempts
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        self.options.reconnect_delay_ms,
                    ))
                    .await;
                }
                Err(e) => {
                    reconnect_attempts += 1;
                    error!(
                        "WebSocket error: {}, reconnecting (attempt {})",
                        e, reconnect_attempts
                    );

                    if reconnect_attempts > self.options.max_reconnect_attempts {
                        error!("Max reconnection attempts reached after error");
                        break;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        self.options.reconnect_delay_ms,
                    ))
                    .await;
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        info!("WebSocket loop terminated");
    }

    /// Connect to WebSocket and listen for messages
    ///
    /// Returns Ok(true) if stopped normally, Ok(false) if connection lost
    async fn connect_and_listen(
        &self,
        live_headers: &Arc<RwLock<Vec<BlockHeader>>>,
    ) -> Result<bool> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let url = self.ws_url();
        info!("Connecting to WebSocket: {}", url);

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| crate::Error::NetworkError(format!("WebSocket connect failed: {}", e)))?;

        info!("WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        // Send initial empty object to trigger connection on server
        write
            .send(Message::Text("{}".to_string()))
            .await
            .map_err(|e| crate::Error::NetworkError(format!("WebSocket send failed: {}", e)))?;

        let mut last_message_time = std::time::Instant::now();
        let idle_timeout = std::time::Duration::from_millis(self.options.idle_timeout_ms);
        let ping_interval = std::time::Duration::from_millis(self.options.ping_interval_ms);
        let mut last_ping_time = std::time::Instant::now();

        loop {
            // Check if we should stop
            if !self.running.load(Ordering::SeqCst) {
                info!("Stop signal received, closing WebSocket");
                let _ = write.close().await;
                return Ok(true);
            }

            // Send ping if needed
            if last_ping_time.elapsed() > ping_interval {
                debug!("Sending ping");
                if write.send(Message::Text("ping".to_string())).await.is_err() {
                    warn!("Failed to send ping");
                    return Ok(false);
                }
                last_ping_time = std::time::Instant::now();
            }

            // Check for idle timeout
            if last_message_time.elapsed() > idle_timeout {
                warn!("WebSocket idle timeout");
                return Ok(false);
            }

            // Try to receive a message with timeout
            let receive_timeout =
                tokio::time::timeout(std::time::Duration::from_secs(1), read.next()).await;

            match receive_timeout {
                Ok(Some(Ok(message))) => {
                    last_message_time = std::time::Instant::now();

                    match message {
                        Message::Text(text) => {
                            if text.is_empty() {
                                // Ping response
                                continue;
                            }

                            if let Err(e) = self.process_message(&text, live_headers).await {
                                warn!("Error processing message: {}", e);
                            }
                        }
                        Message::Binary(data) => {
                            if let Ok(text) = String::from_utf8(data) {
                                if let Err(e) = self.process_message(&text, live_headers).await {
                                    warn!("Error processing binary message: {}", e);
                                }
                            }
                        }
                        Message::Close(_) => {
                            info!("WebSocket closed by server");
                            return Ok(false);
                        }
                        Message::Ping(data) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Message::Pong(_) => {
                            // Pong received
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!("WebSocket receive error: {}", e);
                    return Ok(false);
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    return Ok(false);
                }
                Err(_) => {
                    // Timeout, continue loop
                    continue;
                }
            }
        }
    }

    /// Process a WebSocket message
    async fn process_message(
        &self,
        text: &str,
        live_headers: &Arc<RwLock<Vec<BlockHeader>>>,
    ) -> Result<()> {
        // Handle empty or ping
        if text.is_empty() || text == "{}" {
            return Ok(());
        }

        // Try to parse as JSON
        let msg: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| crate::Error::ValidationError(format!("Invalid JSON: {}", e)))?;

        // Check for typed message
        if let Some(msg_type) = msg.get("type").and_then(|v| v.as_u64()) {
            match msg_type {
                3 => debug!("Unsubscribe message received"),
                5 => debug!("Subscribed to channel"),
                6 => debug!("Subscribe confirmation received"),
                7 => {
                    // Data delivered or error
                    if let Some(data) = msg.get("data") {
                        if let Some(code) = data.get("code").and_then(|v| v.as_i64()) {
                            if code != 200 {
                                let reason = data
                                    .get("reason")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");
                                warn!("WOC message type 7: code={}, reason={}", code, reason);
                            }
                        }
                    }
                }
                _ => debug!("Unknown message type: {}", msg_type),
            }
            return Ok(());
        }

        // Try to extract header data
        let header_data = msg
            .get("pub")
            .and_then(|p| p.get("data"))
            .or_else(|| msg.get("data").and_then(|d| d.get("data")))
            .or_else(|| msg.get("message").and_then(|m| m.get("data")));

        if let Some(data) = header_data {
            let woc_header: WocWsBlockHeader =
                serde_json::from_value(data.clone()).map_err(|e| {
                    crate::Error::ValidationError(format!("Invalid header data: {}", e))
                })?;

            let header = ws_header_to_block_header(&woc_header);
            info!(
                "New block from WebSocket: height={}, hash={}",
                header.height,
                &header.hash[..16]
            );

            // Add to live headers
            {
                let mut live = live_headers.write().await;
                live.push(header.clone());
            }

            // Broadcast to subscribers
            let live_header = block_header_to_live_header(header);
            let _ = self.sender.send(live_header);
        }

        Ok(())
    }
}

#[async_trait]
impl LiveIngestor for LiveWebSocketIngestor {
    async fn get_header_by_hash(&self, hash: &str) -> Result<Option<BlockHeader>> {
        self.fetch_header_by_hash_http(hash).await
    }

    async fn start_listening(&self, live_headers: &mut Vec<BlockHeader>) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            warn!("WebSocket ingestor already running");
            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);

        // Wrap headers in Arc for the WebSocket loop
        let headers_arc = Arc::new(RwLock::new(live_headers.clone()));

        // Clone self into Arc for the spawned task
        let self_arc = Arc::new(Self {
            options: self.options.clone(),
            http_client: self.http_client.clone(),
            storage: self.storage.clone(),
            running: self.running.clone(),
            sender: self.sender.clone(),
            error_count: self.error_count.clone(),
            stop_signal: self.stop_signal.clone(),
        });

        let headers_clone = headers_arc.clone();

        // Spawn the WebSocket loop
        tokio::spawn(async move {
            self_arc.websocket_loop(headers_clone).await;
        });

        // Wait for initial connection
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Copy any new headers back
        let updated = headers_arc.read().await;
        live_headers.clear();
        live_headers.extend(updated.iter().cloned());

        Ok(())
    }

    fn stop_listening(&self) {
        info!("Stopping WebSocket ingestor");
        self.running.store(false, Ordering::SeqCst);
    }

    async fn set_storage(&mut self, storage: Box<dyn ChaintracksStorage>) -> Result<()> {
        self.storage = Some(Arc::new(RwLock::new(storage)));
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.stop_listening();
        // Give the WebSocket loop time to close gracefully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        Ok(())
    }
}

/// Subscribe to headers from the WebSocket ingestor
impl LiveWebSocketIngestor {
    /// Subscribe to new header notifications
    pub fn subscribe(&self) -> broadcast::Receiver<LiveBlockHeader> {
        self.sender.subscribe()
    }

    /// Check if currently running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get recent errors
    pub async fn get_errors(&self) -> Vec<(i32, String)> {
        self.error_count.read().await.clone()
    }
}

/// Convert WebSocket header to BlockHeader
pub fn ws_header_to_block_header(woc: &WocWsBlockHeader) -> BlockHeader {
    let previous_hash = woc
        .previous_block_hash
        .clone()
        .unwrap_or_else(|| "0".repeat(64));

    BlockHeader {
        version: woc.version,
        previous_hash,
        merkle_root: woc.merkleroot.clone(),
        time: woc.time,
        bits: woc.bits,
        nonce: woc.nonce,
        height: woc.height,
        hash: woc.hash.clone(),
    }
}

/// Convert BlockHeader to LiveBlockHeader
fn block_header_to_live_header(header: BlockHeader) -> LiveBlockHeader {
    LiveBlockHeader {
        version: header.version,
        previous_hash: header.previous_hash,
        merkle_root: header.merkle_root,
        time: header.time,
        bits: header.bits,
        nonce: header.nonce,
        height: header.height,
        hash: header.hash,
        chain_work: "0".repeat(64),
        is_chain_tip: true,
        is_active: true,
        header_id: 0,
        previous_header_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_creation() {
        let mainnet = LiveWebSocketOptions::mainnet();
        assert_eq!(mainnet.chain, Chain::Main);

        let testnet = LiveWebSocketOptions::testnet();
        assert_eq!(testnet.chain, Chain::Test);

        let custom = LiveWebSocketOptions::mainnet()
            .with_api_key("test-key")
            .with_idle_timeout(50000);
        assert_eq!(custom.api_key, Some("test-key".to_string()));
        assert_eq!(custom.idle_timeout_ms, 50000);
    }

    #[test]
    fn test_ws_url() {
        let mainnet = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet()).unwrap();
        assert!(mainnet.ws_url().contains("socket-v2.whatsonchain"));

        let testnet = LiveWebSocketIngestor::new(LiveWebSocketOptions::testnet()).unwrap();
        assert!(testnet.ws_url().contains("testnet"));
    }

    #[test]
    fn test_ws_header_conversion() {
        let woc = WocWsBlockHeader {
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

        let header = ws_header_to_block_header(&woc);
        assert_eq!(header.height, 0);
        assert_eq!(header.nonce, 2083236893);
        assert_eq!(header.bits, 486604799);
        assert_eq!(header.previous_hash, "0".repeat(64));
    }

    #[tokio::test]
    async fn test_ingestor_lifecycle() {
        let ingestor = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet()).unwrap();

        assert!(!ingestor.is_running());

        // Don't actually start listening in unit tests
        ingestor.stop_listening();
        assert!(!ingestor.is_running());
    }

    #[test]
    fn test_options_defaults() {
        let opts = LiveWebSocketOptions::default();
        assert_eq!(opts.chain, Chain::Main);
        assert_eq!(opts.idle_timeout_ms, 100_000);
        assert_eq!(opts.ping_interval_ms, 10_000);
        assert_eq!(opts.max_reconnect_attempts, 10);
        assert_eq!(opts.reconnect_delay_ms, 5000);
        assert!(opts.api_key.is_none());
    }

    #[test]
    fn test_ws_url_constants() {
        assert!(WOC_WS_URL_MAIN.starts_with("wss://"));
        assert!(WOC_WS_URL_MAIN.contains("whatsonchain"));
        assert!(!WOC_WS_URL_MAIN.contains("testnet"));

        assert!(WOC_WS_URL_TEST.starts_with("wss://"));
        assert!(WOC_WS_URL_TEST.contains("whatsonchain"));
        assert!(WOC_WS_URL_TEST.contains("testnet"));

        assert!(WOC_WS_HISTORY_URL_MAIN.contains("history"));
        assert!(WOC_WS_HISTORY_URL_TEST.contains("history"));
    }

    #[test]
    fn test_api_url_constants() {
        assert!(WOC_API_URL_MAIN.starts_with("https://"));
        assert!(WOC_API_URL_MAIN.contains("main"));

        assert!(WOC_API_URL_TEST.starts_with("https://"));
        assert!(WOC_API_URL_TEST.contains("test"));
    }

    #[test]
    fn test_ws_header_with_previous() {
        let woc = WocWsBlockHeader {
            hash: "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048".to_string(),
            height: 1,
            version: 1,
            previous_block_hash: Some(
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
            ),
            merkleroot: "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098"
                .to_string(),
            time: 1231469665,
            bits: 486604799,
            nonce: 2573394689,
            confirmations: 799999,
            size: 215,
        };

        let header = ws_header_to_block_header(&woc);
        assert_eq!(header.height, 1);
        assert_eq!(
            header.previous_hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[test]
    fn test_ws_block_header_deserialization() {
        let json = r#"{
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            "height": 0,
            "version": 1,
            "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            "time": 1231006505,
            "bits": 486604799,
            "nonce": 2083236893
        }"#;

        let header: WocWsBlockHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.height, 0);
        assert_eq!(header.nonce, 2083236893);
        assert!(header.previous_block_hash.is_none());
    }

    #[test]
    fn test_ws_error_display() {
        let err1 = WsError::ConnectionFailed("test".to_string());
        assert!(err1.to_string().contains("connection failed"));

        let err2 = WsError::MessageParseFailed("parse error".to_string());
        assert!(err2.to_string().contains("parse"));

        let err3 = WsError::IdleTimeout;
        assert!(err3.to_string().contains("idle"));

        let err4 = WsError::Stopped;
        assert!(err4.to_string().contains("stopped"));
    }

    #[test]
    fn test_block_header_to_live_header() {
        let header = BlockHeader {
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "abc".repeat(21) + "a",
            time: 1231006505,
            bits: 0x1d00ffff,
            nonce: 2083236893,
            height: 0,
            hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
        };

        let live = block_header_to_live_header(header.clone());
        assert_eq!(live.height, header.height);
        assert_eq!(live.hash, header.hash);
        assert!(live.is_chain_tip);
        assert!(live.is_active);
    }

    #[tokio::test]
    async fn test_ingestor_subscribe() {
        let ingestor = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet()).unwrap();
        let _receiver = ingestor.subscribe();
        // Verify subscription works without panicking
    }

    #[tokio::test]
    async fn test_get_errors() {
        let ingestor = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet()).unwrap();
        let errors = ingestor.get_errors().await;
        assert!(errors.is_empty());
    }

    #[test]
    fn test_mainnet_testnet_creation() {
        let mainnet = LiveWebSocketIngestor::mainnet();
        assert!(mainnet.is_ok());

        let testnet = LiveWebSocketIngestor::testnet();
        assert!(testnet.is_ok());
    }

    #[test]
    fn test_api_url_method() {
        let mainnet = LiveWebSocketIngestor::new(LiveWebSocketOptions::mainnet()).unwrap();
        assert!(mainnet.api_url().contains("main"));

        let testnet = LiveWebSocketIngestor::new(LiveWebSocketOptions::testnet()).unwrap();
        assert!(testnet.api_url().contains("test"));
    }
}
