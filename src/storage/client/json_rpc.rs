//! JSON-RPC 2.0 types and helpers.
//!
//! This module provides the JSON-RPC request/response types used for
//! communication with the storage server.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC 2.0 protocol version.
pub const JSON_RPC_VERSION: &str = "2.0";

/// JSON-RPC request structure.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest {
    /// Protocol version (always "2.0").
    pub jsonrpc: String,
    /// Method name to call.
    pub method: String,
    /// Parameters (positional array).
    pub params: Vec<Value>,
    /// Request ID for correlation.
    pub id: u64,
}

impl JsonRpcRequest {
    /// Creates a new JSON-RPC request.
    pub fn new(method: impl Into<String>, params: Vec<Value>, id: u64) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION.to_string(),
            method: method.into(),
            params,
            id,
        }
    }
}

/// JSON-RPC response structure.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcResponse {
    /// Protocol version.
    pub jsonrpc: String,
    /// Result (on success).
    #[serde(default)]
    pub result: Option<Value>,
    /// Error (on failure).
    #[serde(default)]
    pub error: Option<JsonRpcError>,
    /// Request ID for correlation.
    pub id: u64,
}

impl JsonRpcResponse {
    /// Returns the result if successful, or an error.
    pub fn into_result(self) -> Result<Value, JsonRpcError> {
        if let Some(error) = self.error {
            Err(error)
        } else {
            Ok(self.result.unwrap_or(Value::Null))
        }
    }

    /// Returns true if this response indicates success.
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

/// JSON-RPC error structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// Error code.
    pub code: i32,
    /// Error message.
    pub message: String,
    /// Additional error data.
    #[serde(default)]
    pub data: Option<Value>,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}

/// Standard JSON-RPC error codes.
pub mod error_codes {
    /// Parse error - Invalid JSON.
    pub const PARSE_ERROR: i32 = -32700;
    /// Invalid Request - JSON is not a valid Request object.
    pub const INVALID_REQUEST: i32 = -32600;
    /// Method not found.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// Invalid params.
    pub const INVALID_PARAMS: i32 = -32602;
    /// Internal error.
    pub const INTERNAL_ERROR: i32 = -32603;
    /// Server error range start.
    pub const SERVER_ERROR_START: i32 = -32000;
    /// Server error range end.
    pub const SERVER_ERROR_END: i32 = -32099;
}

impl JsonRpcError {
    /// Creates a new JSON-RPC error.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Creates a new JSON-RPC error with additional data.
    pub fn with_data(code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    /// Creates a parse error.
    pub fn parse_error(details: impl Into<String>) -> Self {
        Self::new(error_codes::PARSE_ERROR, details)
    }

    /// Creates an invalid request error.
    pub fn invalid_request(details: impl Into<String>) -> Self {
        Self::new(error_codes::INVALID_REQUEST, details)
    }

    /// Creates a method not found error.
    pub fn method_not_found(method: impl Into<String>) -> Self {
        Self::new(
            error_codes::METHOD_NOT_FOUND,
            format!("Method not found: {}", method.into()),
        )
    }

    /// Creates an invalid params error.
    pub fn invalid_params(details: impl Into<String>) -> Self {
        Self::new(error_codes::INVALID_PARAMS, details)
    }

    /// Creates an internal error.
    pub fn internal_error(details: impl Into<String>) -> Self {
        Self::new(error_codes::INTERNAL_ERROR, details)
    }

    /// Returns true if this is a server error (-32000 to -32099).
    pub fn is_server_error(&self) -> bool {
        self.code >= error_codes::SERVER_ERROR_END && self.code <= error_codes::SERVER_ERROR_START
    }
}

/// Wallet-specific error codes (from TypeScript WERR_* errors).
pub mod wallet_error_codes {
    /// Invalid operation.
    pub const INVALID_OPERATION: &str = "ERR_INVALID_OPERATION";
    /// Bad request.
    pub const BAD_REQUEST: &str = "ERR_BAD_REQUEST";
    /// Unauthorized.
    pub const UNAUTHORIZED: &str = "ERR_UNAUTHORIZED";
    /// Not found.
    pub const NOT_FOUND: &str = "ERR_NOT_FOUND";
    /// Internal error.
    pub const INTERNAL: &str = "ERR_INTERNAL";
    /// Insufficient funds.
    pub const INSUFFICIENT_FUNDS: &str = "ERR_INSUFFICIENT_FUNDS";
    /// Invalid transaction.
    pub const INVALID_TX: &str = "ERR_INVALID_TX";
}

/// Wallet error deserialized from JSON-RPC error data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletError {
    /// Error code string (e.g., "ERR_INVALID_OPERATION").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Description of the error.
    #[serde(default)]
    pub description: Option<String>,
    /// Stack trace (if available).
    #[serde(default)]
    pub stack: Option<String>,
}

impl WalletError {
    /// Attempts to parse a WalletError from JSON-RPC error data.
    pub fn from_rpc_error(error: &JsonRpcError) -> Option<Self> {
        if let Some(ref data) = error.data {
            serde_json::from_value(data.clone()).ok()
        } else {
            // Try to parse the message itself as JSON
            serde_json::from_str(&error.message).ok()
        }
    }
}

impl std::fmt::Display for WalletError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_rpc_request_serialization() {
        let request = JsonRpcRequest::new(
            "makeAvailable",
            vec![],
            1,
        );

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"method\":\"makeAvailable\""));
        assert!(json.contains("\"id\":1"));
    }

    #[test]
    fn test_json_rpc_response_success() {
        let json = r#"{
            "jsonrpc": "2.0",
            "result": {"key": "value"},
            "id": 1
        }"#;

        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(response.is_success());
        assert!(response.error.is_none());

        let result = response.into_result().unwrap();
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_json_rpc_response_error() {
        let json = r#"{
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": "Invalid Request"
            },
            "id": 1
        }"#;

        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(!response.is_success());

        let err = response.into_result().unwrap_err();
        assert_eq!(err.code, error_codes::INVALID_REQUEST);
        assert_eq!(err.message, "Invalid Request");
    }

    #[test]
    fn test_json_rpc_error_codes() {
        let err = JsonRpcError::method_not_found("testMethod");
        assert_eq!(err.code, error_codes::METHOD_NOT_FOUND);
        assert!(err.message.contains("testMethod"));
    }
}
