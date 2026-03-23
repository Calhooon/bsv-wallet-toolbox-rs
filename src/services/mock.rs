//! Mock implementation of WalletServices for testing.
//!
//! Provides `MockWalletServices` which implements `WalletServices` with
//! configurable responses for each method. Supports various failure modes
//! including network errors, timeouts, double-spend rejections, and
//! "already known" responses.
//!
//! # Example
//!
//! ```rust,ignore
//! use bsv_wallet_toolbox::services::mock::{MockWalletServices, MockErrorKind};
//!
//! let mock = MockWalletServices::builder()
//!     .post_beef_success()
//!     .get_raw_tx_error(MockErrorKind::NetworkError, "connection refused")
//!     .build();
//! ```

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

use bsv_rs::transaction::{ChainTracker, ChainTrackerError};

use crate::services::traits::{
    sha256, BlockHeader, FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult,
    GetScriptHashHistoryResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat,
    GetUtxoStatusResult, NLockTimeInput, PostBeefResult, PostTxResultForTxid, WalletServices,
};
use crate::{Error, Result};

// =============================================================================
// Mock Error Kind
// =============================================================================

/// Error kinds that the mock can produce.
#[derive(Debug, Clone)]
pub enum MockErrorKind {
    /// Network-level error (connection refused, DNS failure, timeout).
    NetworkError,
    /// Service returned an error response (503, 429, etc.).
    ServiceError,
    /// Broadcast explicitly failed.
    BroadcastFailed,
    /// No services available.
    NoServicesAvailable,
    /// Validation error (invalid input).
    ValidationError,
    /// Invalid argument.
    InvalidArgument,
    /// Entity not found.
    NotFound,
    /// Transaction error.
    TransactionError,
    /// Internal error.
    Internal,
}

impl MockErrorKind {
    /// Create an `Error` from this kind and a message.
    pub fn to_error(&self, message: &str) -> Error {
        match self {
            MockErrorKind::NetworkError => Error::NetworkError(message.to_string()),
            MockErrorKind::ServiceError => Error::ServiceError(message.to_string()),
            MockErrorKind::BroadcastFailed => Error::BroadcastFailed(message.to_string()),
            MockErrorKind::NoServicesAvailable => Error::NoServicesAvailable,
            MockErrorKind::ValidationError => Error::ValidationError(message.to_string()),
            MockErrorKind::InvalidArgument => Error::InvalidArgument(message.to_string()),
            MockErrorKind::NotFound => Error::NotFound {
                entity: "Transaction".to_string(),
                id: message.to_string(),
            },
            MockErrorKind::TransactionError => Error::TransactionError(message.to_string()),
            MockErrorKind::Internal => Error::Internal(message.to_string()),
        }
    }
}

// =============================================================================
// Mock Response Configuration
// =============================================================================

/// Represents a configurable mock response for a service method.
#[derive(Debug, Clone)]
pub enum MockResponse<T: Clone> {
    /// Return a successful result.
    Success(T),
    /// Return an error (kind + message, reconstructed on each resolve).
    Error(MockErrorKind, String),
    /// Sequence of responses, consumed in order. When exhausted, repeats the last.
    Sequence(Vec<MockResponse<T>>),
}

impl<T: Clone> MockResponse<T> {
    /// Resolve this response to a Result, using call_index for sequences.
    fn resolve(&self, call_index: usize) -> std::result::Result<T, Error> {
        match self {
            MockResponse::Success(v) => Ok(v.clone()),
            MockResponse::Error(kind, msg) => Err(kind.to_error(msg)),
            MockResponse::Sequence(seq) => {
                if seq.is_empty() {
                    return Err(Error::ServiceError(
                        "Empty mock response sequence".to_string(),
                    ));
                }
                let idx = call_index.min(seq.len() - 1);
                seq[idx].resolve(0)
            }
        }
    }
}

// =============================================================================
// Call Record for tracking
// =============================================================================

/// Records a single call made to the mock service.
#[derive(Debug, Clone)]
pub struct MockCallRecord {
    /// Name of the method that was called.
    pub method: String,
    /// Arguments serialized as debug strings.
    pub args: Vec<String>,
    /// Whether the call succeeded.
    pub success: bool,
}

// =============================================================================
// Mock Chain Tracker
// =============================================================================

/// A mock ChainTracker that returns configurable height and always validates roots.
struct MockChainTracker {
    height: u32,
}

#[async_trait]
impl ChainTracker for MockChainTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Ok(true)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(self.height)
    }
}

// =============================================================================
// MockWalletServices
// =============================================================================

/// A mock implementation of `WalletServices` for testing.
///
/// All methods return configurable responses. By default, methods return
/// reasonable success values. Use the builder pattern to configure specific
/// responses for specific methods.
pub struct MockWalletServices {
    /// Mock chain tracker for height/header queries.
    chain_tracker: MockChainTracker,

    /// Configured height to return.
    height: u32,

    /// Response for post_beef calls.
    post_beef_response: Mutex<MockResponse<Vec<PostBeefResult>>>,

    /// Response for get_raw_tx calls.
    get_raw_tx_response: Mutex<MockResponse<GetRawTxResult>>,

    /// Response for get_merkle_path calls.
    get_merkle_path_response: Mutex<MockResponse<GetMerklePathResult>>,

    /// Response for get_utxo_status calls.
    get_utxo_status_response: Mutex<MockResponse<GetUtxoStatusResult>>,

    /// Response for get_status_for_txids calls.
    get_status_for_txids_response: Mutex<MockResponse<GetStatusForTxidsResult>>,

    /// Response for get_script_hash_history calls.
    get_script_hash_history_response: Mutex<MockResponse<GetScriptHashHistoryResult>>,

    /// Record of all calls made.
    call_history: Mutex<Vec<MockCallRecord>>,

    /// Call counters by method name.
    call_counts: Mutex<HashMap<String, usize>>,
}

impl MockWalletServices {
    /// Create a new builder for MockWalletServices.
    pub fn builder() -> MockWalletServicesBuilder {
        MockWalletServicesBuilder::default()
    }

    /// Create a MockWalletServices with all-success defaults.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Get a copy of the call history.
    pub fn call_history(&self) -> Vec<MockCallRecord> {
        self.call_history.lock().unwrap().clone()
    }

    /// Get the number of calls made to a specific method.
    pub fn call_count(&self, method: &str) -> usize {
        *self.call_counts.lock().unwrap().get(method).unwrap_or(&0)
    }

    /// Get the total number of calls made.
    pub fn total_calls(&self) -> usize {
        self.call_history.lock().unwrap().len()
    }

    /// Reset call history and counters.
    pub fn reset_history(&self) {
        self.call_history.lock().unwrap().clear();
        self.call_counts.lock().unwrap().clear();
    }

    /// Record a call.
    fn record_call(&self, method: &str, args: Vec<String>, success: bool) {
        let mut history = self.call_history.lock().unwrap();
        history.push(MockCallRecord {
            method: method.to_string(),
            args,
            success,
        });

        let mut counts = self.call_counts.lock().unwrap();
        *counts.entry(method.to_string()).or_insert(0) += 1;
    }

    /// Get the call index for a method (how many times it has been called so far).
    fn get_call_index(&self, method: &str) -> usize {
        *self.call_counts.lock().unwrap().get(method).unwrap_or(&0)
    }
}

impl Default for MockWalletServices {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Builder
// =============================================================================

/// Builder for constructing `MockWalletServices` with configurable responses.
pub struct MockWalletServicesBuilder {
    height: u32,
    post_beef_response: MockResponse<Vec<PostBeefResult>>,
    get_raw_tx_response: MockResponse<GetRawTxResult>,
    get_merkle_path_response: MockResponse<GetMerklePathResult>,
    get_utxo_status_response: MockResponse<GetUtxoStatusResult>,
    get_status_for_txids_response: MockResponse<GetStatusForTxidsResult>,
    get_script_hash_history_response: MockResponse<GetScriptHashHistoryResult>,
}

impl Default for MockWalletServicesBuilder {
    fn default() -> Self {
        Self {
            height: 880_000,
            post_beef_response: MockResponse::Success(vec![PostBeefResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                txid_results: vec![],
                error: None,
                notes: vec![],
            }]),
            get_raw_tx_response: MockResponse::Success(GetRawTxResult {
                name: "MockProvider".to_string(),
                txid: String::new(),
                raw_tx: Some(vec![0x01, 0x00, 0x00, 0x00, 0x00]),
                error: None,
            }),
            get_merkle_path_response: MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some("mock_merkle_path".to_string()),
                header: None,
                error: None,
                notes: vec![],
            }),
            get_utxo_status_response: MockResponse::Success(GetUtxoStatusResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                is_utxo: Some(true),
                details: vec![],
                error: None,
            }),
            get_status_for_txids_response: MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![],
            }),
            get_script_hash_history_response: MockResponse::Success(GetScriptHashHistoryResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                history: vec![],
            }),
        }
    }
}

impl MockWalletServicesBuilder {
    /// Set the blockchain height to return.
    pub fn height(mut self, height: u32) -> Self {
        self.height = height;
        self
    }

    /// Set the response for post_beef calls.
    pub fn post_beef_response(mut self, response: MockResponse<Vec<PostBeefResult>>) -> Self {
        self.post_beef_response = response;
        self
    }

    /// Set post_beef to return a simple success.
    pub fn post_beef_success(self) -> Self {
        self.post_beef_response(MockResponse::Success(vec![PostBeefResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            txid_results: vec![],
            error: None,
            notes: vec![],
        }]))
    }

    /// Set post_beef to return a network error.
    pub fn post_beef_network_error(self, msg: &str) -> Self {
        self.post_beef_response(MockResponse::Error(
            MockErrorKind::NetworkError,
            msg.to_string(),
        ))
    }

    /// Set post_beef to return a double-spend rejection.
    pub fn post_beef_double_spend(self, txid: &str, competing_tx: &str) -> Self {
        self.post_beef_response(MockResponse::Success(vec![PostBeefResult {
            name: "MockProvider".to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: txid.to_string(),
                status: "error".to_string(),
                double_spend: true,
                competing_txs: Some(vec![competing_tx.to_string()]),
                data: Some("DOUBLE_SPEND_ATTEMPTED".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: Some("Double spend detected".to_string()),
            notes: vec![],
        }]))
    }

    /// Set post_beef to return "already known" (treated as success).
    pub fn post_beef_already_known(self, txid: &str) -> Self {
        self.post_beef_response(MockResponse::Success(vec![PostBeefResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: txid.to_string(),
                status: "success".to_string(),
                double_spend: false,
                competing_txs: None,
                data: Some("Already known".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }]))
    }

    /// Set post_beef to return a service unavailable error.
    pub fn post_beef_service_unavailable(self) -> Self {
        self.post_beef_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "Service temporarily unavailable (503)".to_string(),
        ))
    }

    /// Set post_beef to return a rate limit (429) error.
    pub fn post_beef_rate_limited(self) -> Self {
        self.post_beef_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "Rate limited (429)".to_string(),
        ))
    }

    /// Set the response for get_raw_tx calls.
    pub fn get_raw_tx_response(mut self, response: MockResponse<GetRawTxResult>) -> Self {
        self.get_raw_tx_response = response;
        self
    }

    /// Set get_raw_tx to return an error.
    pub fn get_raw_tx_error(self, kind: MockErrorKind, msg: &str) -> Self {
        self.get_raw_tx_response(MockResponse::Error(kind, msg.to_string()))
    }

    /// Set the response for get_merkle_path calls.
    pub fn get_merkle_path_response(mut self, response: MockResponse<GetMerklePathResult>) -> Self {
        self.get_merkle_path_response = response;
        self
    }

    /// Set the response for get_utxo_status calls.
    pub fn get_utxo_status_response(mut self, response: MockResponse<GetUtxoStatusResult>) -> Self {
        self.get_utxo_status_response = response;
        self
    }

    /// Set the response for get_status_for_txids calls.
    pub fn get_status_for_txids_response(
        mut self,
        response: MockResponse<GetStatusForTxidsResult>,
    ) -> Self {
        self.get_status_for_txids_response = response;
        self
    }

    /// Set the response for get_script_hash_history calls.
    pub fn get_script_hash_history_response(
        mut self,
        response: MockResponse<GetScriptHashHistoryResult>,
    ) -> Self {
        self.get_script_hash_history_response = response;
        self
    }

    /// Build the MockWalletServices.
    pub fn build(self) -> MockWalletServices {
        MockWalletServices {
            chain_tracker: MockChainTracker {
                height: self.height,
            },
            height: self.height,
            post_beef_response: Mutex::new(self.post_beef_response),
            get_raw_tx_response: Mutex::new(self.get_raw_tx_response),
            get_merkle_path_response: Mutex::new(self.get_merkle_path_response),
            get_utxo_status_response: Mutex::new(self.get_utxo_status_response),
            get_status_for_txids_response: Mutex::new(self.get_status_for_txids_response),
            get_script_hash_history_response: Mutex::new(self.get_script_hash_history_response),
            call_history: Mutex::new(Vec::new()),
            call_counts: Mutex::new(HashMap::new()),
        }
    }
}

// =============================================================================
// Helper constructors for common PostBeefResult scenarios
// =============================================================================

/// Create a successful PostBeefResult for given txids.
pub fn success_post_beef_result(name: &str, txids: &[&str]) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "success".to_string(),
        txid_results: txids
            .iter()
            .map(|txid| PostTxResultForTxid {
                txid: txid.to_string(),
                status: "success".to_string(),
                double_spend: false,
                competing_txs: None,
                data: None,
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            })
            .collect(),
        error: None,
        notes: vec![],
    }
}

/// Create a failed PostBeefResult with a service error.
pub fn error_post_beef_result(name: &str, error_msg: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![],
        error: Some(error_msg.to_string()),
        notes: vec![],
    }
}

/// Create a double-spend PostBeefResult.
pub fn double_spend_post_beef_result(name: &str, txid: &str, competing_tx: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "error".to_string(),
            double_spend: true,
            competing_txs: Some(vec![competing_tx.to_string()]),
            data: Some("DOUBLE_SPEND_ATTEMPTED".to_string()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: Some("Double spend detected".to_string()),
        notes: vec![],
    }
}

/// Create an "already known" PostBeefResult (treated as success).
pub fn already_known_post_beef_result(name: &str, txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "success".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "success".to_string(),
            double_spend: false,
            competing_txs: None,
            data: Some("Already known".to_string()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

// =============================================================================
// WalletServices Implementation
// =============================================================================

#[async_trait]
impl WalletServices for MockWalletServices {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        self.record_call("get_chain_tracker", vec![], true);
        Ok(&self.chain_tracker)
    }

    async fn get_height(&self) -> Result<u32> {
        self.record_call("get_height", vec![], true);
        Ok(self.height)
    }

    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.record_call("get_header_for_height", vec![format!("{}", height)], true);
        // Return a mock 80-byte header
        Ok(vec![0u8; 80])
    }

    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        self.record_call("hash_to_header", vec![hash.to_string()], true);
        Ok(BlockHeader {
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "0".repeat(64),
            time: 1231006505,
            bits: 486604799,
            nonce: 2083236893,
            hash: hash.to_string(),
            height: self.height,
        })
    }

    async fn get_raw_tx(&self, txid: &str, _use_next: bool) -> Result<GetRawTxResult> {
        let call_index = self.get_call_index("get_raw_tx");
        let response = self.get_raw_tx_response.lock().unwrap();
        let result = response.resolve(call_index);
        match &result {
            Ok(_) => self.record_call("get_raw_tx", vec![txid.to_string()], true),
            Err(_) => self.record_call("get_raw_tx", vec![txid.to_string()], false),
        }
        result
    }

    async fn get_merkle_path(&self, txid: &str, _use_next: bool) -> Result<GetMerklePathResult> {
        let call_index = self.get_call_index("get_merkle_path");
        let response = self.get_merkle_path_response.lock().unwrap();
        let result = response.resolve(call_index);
        match &result {
            Ok(_) => self.record_call("get_merkle_path", vec![txid.to_string()], true),
            Err(_) => self.record_call("get_merkle_path", vec![txid.to_string()], false),
        }
        result
    }

    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        let call_index = self.get_call_index("post_beef");
        let response = self.post_beef_response.lock().unwrap();
        let result = response.resolve(call_index);
        let args = vec![
            format!("beef_len={}", beef.len()),
            format!("txids={:?}", txids),
        ];
        match &result {
            Ok(_) => self.record_call("post_beef", args, true),
            Err(_) => self.record_call("post_beef", args, false),
        }
        result
    }

    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        _use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        let call_index = self.get_call_index("get_utxo_status");
        let response = self.get_utxo_status_response.lock().unwrap();
        let result = response.resolve(call_index);
        let args = vec![
            output.to_string(),
            format!("{:?}", output_format),
            format!("{:?}", outpoint),
        ];
        match &result {
            Ok(_) => self.record_call("get_utxo_status", args, true),
            Err(_) => self.record_call("get_utxo_status", vec![output.to_string()], false),
        }
        result
    }

    async fn get_status_for_txids(
        &self,
        txids: &[String],
        _use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        let call_index = self.get_call_index("get_status_for_txids");
        let response = self.get_status_for_txids_response.lock().unwrap();
        let result = response.resolve(call_index);
        let args = vec![format!("{:?}", txids)];
        match &result {
            Ok(_) => self.record_call("get_status_for_txids", args, true),
            Err(_) => self.record_call("get_status_for_txids", args, false),
        }
        result
    }

    async fn get_script_hash_history(
        &self,
        hash: &str,
        _use_next: bool,
    ) -> Result<GetScriptHashHistoryResult> {
        let call_index = self.get_call_index("get_script_hash_history");
        let response = self.get_script_hash_history_response.lock().unwrap();
        let result = response.resolve(call_index);
        match &result {
            Ok(_) => self.record_call("get_script_hash_history", vec![hash.to_string()], true),
            Err(_) => self.record_call("get_script_hash_history", vec![hash.to_string()], false),
        }
        result
    }

    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.record_call("get_bsv_exchange_rate", vec![], true);
        Ok(50.0)
    }

    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        self.record_call(
            "get_fiat_exchange_rate",
            vec![format!("{:?}", currency), format!("{:?}", base)],
            true,
        );
        match currency {
            FiatCurrency::USD => Ok(1.0),
            FiatCurrency::EUR => Ok(0.85),
            FiatCurrency::GBP => Ok(0.79),
        }
    }

    fn hash_output_script(&self, script: &[u8]) -> String {
        let hash = sha256(script);
        hex::encode(&hash)
    }

    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> Result<bool> {
        self.record_call(
            "is_utxo",
            vec![
                txid.to_string(),
                format!("{}", vout),
                format!("script_len={}", locking_script.len()),
            ],
            true,
        );
        Ok(true)
    }

    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        self.record_call(
            "n_lock_time_is_final",
            vec![format!("{}", n_lock_time)],
            true,
        );
        const BLOCK_LIMIT: u32 = 500_000_000;
        if n_lock_time >= BLOCK_LIMIT {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
            return Ok(n_lock_time < now);
        }
        Ok(n_lock_time < self.height)
    }

    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        self.record_call(
            "n_lock_time_is_final_for_tx",
            vec![format!(
                "lock_time={}, all_final={}",
                input.lock_time, input.all_sequences_final
            )],
            true,
        );
        if input.all_sequences_final {
            return Ok(true);
        }
        self.n_lock_time_is_final(input.lock_time).await
    }

    async fn get_beef(&self, txid: &str, _known_txids: &[String]) -> Result<GetBeefResult> {
        self.record_call("get_beef", vec![txid.to_string()], true);
        Ok(GetBeefResult {
            name: "MockProvider".to_string(),
            txid: txid.to_string(),
            beef: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            has_proof: true,
            error: None,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::collection::{ServiceCall, ServiceCollection};
    use std::sync::Arc;

    // =========================================================================
    // Test 1: All providers fail
    // =========================================================================

    #[tokio::test]
    async fn test_all_providers_fail() {
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "connection refused".to_string(),
            ))
            .build();

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        let result = mock.post_beef(&beef, &txids).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NetworkError(msg) => assert!(msg.contains("connection refused")),
            other => panic!("Expected NetworkError, got: {:?}", other),
        }
    }

    // =========================================================================
    // Test 2: Double-spend rejection
    // =========================================================================

    #[tokio::test]
    async fn test_double_spend_rejection() {
        let mock = MockWalletServices::builder()
            .post_beef_double_spend("tx123", "competing_tx456")
            .build();

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        let results = mock.post_beef(&beef, &txids).await.unwrap();
        assert_eq!(results.len(), 1);

        let result = &results[0];
        assert!(!result.is_success());
        assert_eq!(result.status, "error");
        assert!(result.error.as_ref().unwrap().contains("Double spend"));

        let txid_result = &result.txid_results[0];
        assert!(txid_result.double_spend);
        assert_eq!(
            txid_result.competing_txs,
            Some(vec!["competing_tx456".to_string()])
        );
    }

    // =========================================================================
    // Test 3: Partial success (sequence: first two fail, third succeeds)
    // =========================================================================

    #[tokio::test]
    async fn test_partial_success() {
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Sequence(vec![
                MockResponse::Error(MockErrorKind::NetworkError, "provider1 down".to_string()),
                MockResponse::Error(MockErrorKind::ServiceError, "provider2 error".to_string()),
                MockResponse::Success(vec![success_post_beef_result("provider3", &["tx123"])]),
            ]))
            .build();

        let beef = vec![0x01, 0x02];
        let txids = vec!["tx123".to_string()];

        // First call fails (provider1)
        let r1 = mock.post_beef(&beef, &txids).await;
        assert!(r1.is_err());

        // Second call fails (provider2)
        let r2 = mock.post_beef(&beef, &txids).await;
        assert!(r2.is_err());

        // Third call succeeds (provider3)
        let r3 = mock.post_beef(&beef, &txids).await;
        assert!(r3.is_ok());
        let results = r3.unwrap();
        assert!(results[0].is_success());
    }

    // =========================================================================
    // Test 4: Timeout during broadcast
    // =========================================================================

    #[tokio::test]
    async fn test_timeout_during_broadcast() {
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "request timed out after 30s".to_string(),
            ))
            .build();

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        let result = mock.post_beef(&beef, &txids).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NetworkError(msg) => assert!(msg.contains("timed out")),
            other => panic!("Expected NetworkError with timeout, got: {:?}", other),
        }
    }

    // =========================================================================
    // Test 5: Network error with failover (using ServiceCollection)
    // =========================================================================

    #[tokio::test]
    async fn test_network_error_with_failover() {
        let failing_mock = Arc::new(
            MockWalletServices::builder()
                .post_beef_network_error("connection refused")
                .build(),
        );

        let succeeding_mock = Arc::new(MockWalletServices::builder().post_beef_success().build());

        let mut collection = ServiceCollection::<Arc<MockWalletServices>>::new("postBeef");
        collection.add("failing_provider", Arc::clone(&failing_mock));
        collection.add("succeeding_provider", Arc::clone(&succeeding_mock));

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        // Try first provider (should fail)
        let first_service = collection.current_service().unwrap().clone();
        let first_result = first_service.post_beef(&beef, &txids).await;
        assert!(first_result.is_err());

        // Record failure and advance
        let mut call = ServiceCall::new();
        call.mark_error("connection refused", "NETWORK");
        collection.add_call_error("failing_provider", call);
        collection.next();

        // Try second provider (should succeed)
        let second_service = collection.current_service().unwrap().clone();
        let second_result = second_service.post_beef(&beef, &txids).await;
        assert!(second_result.is_ok());
        assert!(second_result.unwrap()[0].is_success());

        // Record success
        let mut call = ServiceCall::new();
        call.mark_success(None);
        collection.add_call_success("succeeding_provider", call);
    }

    // =========================================================================
    // Test 6: "Already known" counts as success
    // =========================================================================

    #[tokio::test]
    async fn test_already_known_counts_as_success() {
        let mock = MockWalletServices::builder()
            .post_beef_already_known("tx123")
            .build();

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        let results = mock.post_beef(&beef, &txids).await.unwrap();
        assert_eq!(results.len(), 1);

        let result = &results[0];
        assert!(result.is_success());
        assert_eq!(
            result.txid_results[0].data.as_deref(),
            Some("Already known")
        );
        assert!(!result.txid_results[0].double_spend);
    }

    // =========================================================================
    // Test 7: Empty BEEF broadcast
    // =========================================================================

    #[tokio::test]
    async fn test_empty_beef_broadcast() {
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::InvalidArgument,
                "BEEF data is empty".to_string(),
            ))
            .build();

        let empty_beef: Vec<u8> = vec![];
        let txids = vec!["tx123".to_string()];

        let result = mock.post_beef(&empty_beef, &txids).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::InvalidArgument(msg) => assert!(msg.contains("empty")),
            other => panic!("Expected InvalidArgument, got: {:?}", other),
        }

        // Verify the call was recorded with beef_len=0
        let history = mock.call_history();
        assert_eq!(history.len(), 1);
        assert!(history[0].args[0].contains("beef_len=0"));
    }

    // =========================================================================
    // Test 8: Post-broadcast status update
    // =========================================================================

    #[tokio::test]
    async fn test_post_broadcast_status_update() {
        let mock = MockWalletServices::builder()
            .post_beef_success()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![crate::services::TxStatusDetail {
                    txid: "tx123".to_string(),
                    status: "known".to_string(),
                    depth: Some(0),
                }],
            }))
            .build();

        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx123".to_string()];

        // Step 1: Broadcast
        let post_result = mock.post_beef(&beef, &txids).await.unwrap();
        assert!(post_result[0].is_success());

        // Step 2: Check status
        let status_result = mock.get_status_for_txids(&txids, false).await.unwrap();
        assert_eq!(status_result.status, "success");
        assert_eq!(status_result.results.len(), 1);
        assert_eq!(status_result.results[0].status, "known");

        // Verify both calls were recorded
        assert_eq!(mock.call_count("post_beef"), 1);
        assert_eq!(mock.call_count("get_status_for_txids"), 1);
    }

    // =========================================================================
    // Test 9: ServiceCollection failover order
    // =========================================================================

    #[tokio::test]
    async fn test_service_collection_failover() {
        let provider1 = Arc::new(
            MockWalletServices::builder()
                .post_beef_network_error("provider1 down")
                .build(),
        );
        let provider2 = Arc::new(MockWalletServices::builder().post_beef_success().build());
        let provider3 = Arc::new(MockWalletServices::builder().post_beef_success().build());

        let mut collection = ServiceCollection::<Arc<MockWalletServices>>::new("postBeef");
        collection.add("provider1", Arc::clone(&provider1));
        collection.add("provider2", Arc::clone(&provider2));
        collection.add("provider3", Arc::clone(&provider3));

        let beef = vec![0xDE, 0xAD];
        let txids = vec!["tx1".to_string()];

        // Should start with provider1
        assert_eq!(collection.current_name(), Some("provider1"));

        // Provider1 fails
        let service = collection.current_service().unwrap().clone();
        let r = service.post_beef(&beef, &txids).await;
        assert!(r.is_err());

        let mut call = ServiceCall::new();
        call.mark_error("provider1 down", "NETWORK");
        collection.add_call_error("provider1", call);
        collection.next();

        // Should now be at provider2
        assert_eq!(collection.current_name(), Some("provider2"));

        // Provider2 succeeds
        let service = collection.current_service().unwrap().clone();
        let r = service.post_beef(&beef, &txids).await;
        assert!(r.is_ok());

        let mut call = ServiceCall::new();
        call.mark_success(None);
        collection.add_call_success("provider2", call);

        // Verify history
        let history = collection.get_call_history(false);
        let p1_history = history.history_by_provider.get("provider1").unwrap();
        assert_eq!(p1_history.total_counts.error, 1);
        assert_eq!(p1_history.total_counts.failure, 1);

        let p2_history = history.history_by_provider.get("provider2").unwrap();
        assert_eq!(p2_history.total_counts.success, 1);
    }

    // =========================================================================
    // Test 10: ServiceCollection all providers fail
    // =========================================================================

    #[tokio::test]
    async fn test_service_collection_all_fail() {
        let provider1 = Arc::new(
            MockWalletServices::builder()
                .post_beef_network_error("provider1 timeout")
                .build(),
        );
        let provider2 = Arc::new(
            MockWalletServices::builder()
                .post_beef_service_unavailable()
                .build(),
        );

        let mut collection = ServiceCollection::<Arc<MockWalletServices>>::new("postBeef");
        collection.add("provider1", Arc::clone(&provider1));
        collection.add("provider2", Arc::clone(&provider2));

        let beef = vec![0x01, 0x02];
        let txids = vec!["tx1".to_string()];
        let mut last_error = None;

        // Try all providers
        for _ in 0..collection.count() {
            let service = collection.current_service().unwrap().clone();
            let r = service.post_beef(&beef, &txids).await;

            let mut call = ServiceCall::new();
            match r {
                Ok(_) => {
                    call.mark_success(None);
                    let name = collection.current_name().unwrap().to_string();
                    collection.add_call_success(&name, call);
                    last_error = None;
                    break;
                }
                Err(e) => {
                    call.mark_error(&e.to_string(), "ERROR");
                    let name = collection.current_name().unwrap().to_string();
                    collection.add_call_error(&name, call);
                    last_error = Some(e);
                    collection.next();
                }
            }
        }

        // All providers should have failed
        assert!(last_error.is_some());
        let history = collection.get_call_history(false);
        assert_eq!(history.history_by_provider.len(), 2);

        for h in history.history_by_provider.values() {
            assert_eq!(h.total_counts.success, 0);
            assert!(h.total_counts.failure > 0);
        }
    }

    // =========================================================================
    // Test 11: Broadcast retry on 429 (rate limit)
    // =========================================================================

    #[tokio::test]
    async fn test_broadcast_retry_on_429() {
        // First call returns 429, second returns success
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Sequence(vec![
                MockResponse::Error(
                    MockErrorKind::ServiceError,
                    "Rate limited (429)".to_string(),
                ),
                MockResponse::Success(vec![success_post_beef_result("provider", &["tx1"])]),
            ]))
            .build();

        let beef = vec![0x01, 0x02];
        let txids = vec!["tx1".to_string()];

        // First attempt: rate limited
        let r1 = mock.post_beef(&beef, &txids).await;
        assert!(r1.is_err());
        match r1.unwrap_err() {
            Error::ServiceError(msg) => assert!(msg.contains("429")),
            other => panic!("Expected ServiceError with 429, got: {:?}", other),
        }

        // Retry: should succeed
        let r2 = mock.post_beef(&beef, &txids).await;
        assert!(r2.is_ok());
        assert!(r2.unwrap()[0].is_success());

        // Verify two calls were made
        assert_eq!(mock.call_count("post_beef"), 2);
    }

    // =========================================================================
    // Test 12: Post beefs - multiple transactions
    // =========================================================================

    #[tokio::test]
    async fn test_post_beefs_multiple_transactions() {
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Success(vec![PostBeefResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                txid_results: vec![
                    PostTxResultForTxid {
                        txid: "tx1".to_string(),
                        status: "success".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: None,
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![],
                    },
                    PostTxResultForTxid {
                        txid: "tx2".to_string(),
                        status: "success".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: None,
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![],
                    },
                    PostTxResultForTxid {
                        txid: "tx3".to_string(),
                        status: "success".to_string(),
                        double_spend: false,
                        competing_txs: None,
                        data: None,
                        service_error: false,
                        block_hash: None,
                        block_height: None,
                        notes: vec![],
                    },
                ],
                error: None,
                notes: vec![],
            }]))
            .build();

        let beef = vec![0x01, 0x02, 0x03, 0x04]; // Multi-tx BEEF
        let txids = vec!["tx1".to_string(), "tx2".to_string(), "tx3".to_string()];

        let results = mock.post_beef(&beef, &txids).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
        assert_eq!(results[0].txid_results.len(), 3);

        // All transactions should succeed
        for txid_result in &results[0].txid_results {
            assert!(txid_result.is_success());
            assert!(!txid_result.double_spend);
        }
    }

    // =========================================================================
    // Test 13: Service call history tracking
    // =========================================================================

    #[tokio::test]
    async fn test_service_call_history_tracking() {
        let mock = MockWalletServices::builder().build();

        // Make several different calls
        let _ = mock.get_height().await;
        let _ = mock.get_raw_tx("tx1", false).await;
        let _ = mock.get_raw_tx("tx2", false).await;
        let _ = mock.post_beef(&[0x01], &["tx1".to_string()]).await;
        let _ = mock.get_utxo_status("hash1", None, None, false).await;
        let _ = mock.get_bsv_exchange_rate().await;

        // Verify call counts
        assert_eq!(mock.call_count("get_height"), 1);
        assert_eq!(mock.call_count("get_raw_tx"), 2);
        assert_eq!(mock.call_count("post_beef"), 1);
        assert_eq!(mock.call_count("get_utxo_status"), 1);
        assert_eq!(mock.call_count("get_bsv_exchange_rate"), 1);
        assert_eq!(mock.total_calls(), 6);

        // Check history records
        let history = mock.call_history();
        assert_eq!(history.len(), 6);

        // Verify methods recorded correctly
        assert_eq!(history[0].method, "get_height");
        assert_eq!(history[1].method, "get_raw_tx");
        assert_eq!(history[1].args[0], "tx1");
        assert_eq!(history[2].method, "get_raw_tx");
        assert_eq!(history[2].args[0], "tx2");
        assert_eq!(history[3].method, "post_beef");
        assert_eq!(history[4].method, "get_utxo_status");
        assert_eq!(history[5].method, "get_bsv_exchange_rate");

        // Reset and verify
        mock.reset_history();
        assert_eq!(mock.total_calls(), 0);
        assert_eq!(mock.call_count("get_raw_tx"), 0);
    }

    // =========================================================================
    // Test 14: Mock configurable responses
    // =========================================================================

    #[tokio::test]
    async fn test_mock_configurable_responses() {
        // Test get_raw_tx error
        let mock = MockWalletServices::builder()
            .get_raw_tx_error(MockErrorKind::NotFound, "nonexistent")
            .build();

        let result = mock.get_raw_tx("nonexistent", false).await;
        assert!(result.is_err());

        // Test get_merkle_path success with custom data
        let mock = MockWalletServices::builder()
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("CustomProvider".to_string()),
                merkle_path: Some("custom_path_hex".to_string()),
                header: Some(BlockHeader {
                    version: 1,
                    previous_hash: "0".repeat(64),
                    merkle_root: "a".repeat(64),
                    time: 1700000000,
                    bits: 486604799,
                    nonce: 12345,
                    hash: "b".repeat(64),
                    height: 850000,
                }),
                error: None,
                notes: vec![],
            }))
            .build();

        let result = mock.get_merkle_path("tx1", false).await.unwrap();
        assert_eq!(result.name, Some("CustomProvider".to_string()));
        assert!(result.header.is_some());
        assert_eq!(result.header.unwrap().height, 850000);

        // Test UTXO status not found
        let mock = MockWalletServices::builder()
            .get_utxo_status_response(MockResponse::Success(GetUtxoStatusResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                is_utxo: Some(false),
                details: vec![],
                error: None,
            }))
            .build();

        let result = mock
            .get_utxo_status("hash", None, None, false)
            .await
            .unwrap();
        assert_eq!(result.is_utxo, Some(false));

        // Test custom height
        let mock = MockWalletServices::builder().height(999_999).build();
        let height = mock.get_height().await.unwrap();
        assert_eq!(height, 999_999);
    }

    // =========================================================================
    // Test 15: Broadcast error categorization
    // =========================================================================

    #[tokio::test]
    async fn test_broadcast_error_categorization() {
        let beef = vec![0x01, 0x02, 0x03];
        let txids = vec!["tx1".to_string()];

        // NetworkError
        let mock = MockWalletServices::builder()
            .post_beef_network_error("DNS resolution failed")
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(matches!(r, Err(Error::NetworkError(_))));

        // ServiceError (service unavailable)
        let mock = MockWalletServices::builder()
            .post_beef_service_unavailable()
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(matches!(r, Err(Error::ServiceError(_))));

        // BroadcastFailed
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::BroadcastFailed,
                "mempool full".to_string(),
            ))
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(matches!(r, Err(Error::BroadcastFailed(_))));

        // NoServicesAvailable
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::NoServicesAvailable,
                String::new(),
            ))
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(matches!(r, Err(Error::NoServicesAvailable)));

        // ValidationError (invalid BEEF)
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Error(
                MockErrorKind::ValidationError,
                "Invalid BEEF format".to_string(),
            ))
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(matches!(r, Err(Error::ValidationError(_))));

        // Double-spend (returned as PostBeefResult, not as Error)
        let mock = MockWalletServices::builder()
            .post_beef_double_spend("tx1", "competing_tx")
            .build();
        let r = mock.post_beef(&beef, &txids).await;
        assert!(r.is_ok()); // Double-spend comes as Ok with error status
        let results = r.unwrap();
        assert!(!results[0].is_success());
        assert!(results[0].txid_results[0].double_spend);
    }
}
