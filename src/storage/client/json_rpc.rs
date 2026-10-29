//! JSON-RPC 2.0 types and helpers.
//!
//! This module provides the JSON-RPC request/response types used for
//! communication with the storage server.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

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
///
/// Handles both standard JSON-RPC errors (with `code` field) and non-standard
/// server errors (with `isError`, `name` fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// Error code (standard JSON-RPC). Defaults to -32000 if not present,
    /// a string, or not an `i32`.
    #[serde(default = "default_error_code", deserialize_with = "code_or_default")]
    pub code: i32,
    /// Error message.
    pub message: String,
    /// Additional error data.
    #[serde(default)]
    pub data: Option<Value>,
    /// Non-standard: indicates this is an error (some servers use this).
    #[serde(default, rename = "isError")]
    pub is_error: Option<bool>,
    /// Non-standard: error name/type (e.g., "TypeError").
    #[serde(default)]
    pub name: Option<String>,
}

fn default_error_code() -> i32 {
    error_codes::SERVER_ERROR_START // -32000
}

/// The reference's error object carries no numeric `code` (`isError`,
/// `name`, `message`; `WalletError.ts:145-149` at ts-stack edf6e03), and a
/// client may see a string one; the `name`, or the string `code`, carries
/// the class (`WalletError::from_rpc_error`). Anything but an `i32` reads
/// as the default.
fn code_or_default<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i32, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::Number(code) => code
            .as_i64()
            .and_then(|code| i32::try_from(code).ok())
            .unwrap_or_else(default_error_code),
        _ => default_error_code(),
    })
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref name) = self.name {
            write!(f, "{}: {}", name, self.message)
        } else {
            write!(f, "JSON-RPC error {}: {}", self.code, self.message)
        }
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
            is_error: None,
            name: None,
        }
    }

    /// Creates a new JSON-RPC error with additional data.
    pub fn with_data(code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
            is_error: None,
            name: None,
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
#[allow(dead_code)]
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

/// The server's WalletError as the reference's `StorageServer.sendRpcError`
/// puts it: the JSON-RPC `error` object is `WalletError.unknownToJson`'s
/// object itself (`isError`, `name`, `message`, and a class's own members
/// beside them; `StorageServer.ts:857-867`, `WalletError.ts:143-169` at
/// ts-stack edf6e03). Read with the bounds of `WalletErrorFromJson.ts`.
#[derive(Debug, Clone)]
pub struct WalletError {
    /// The `WERR_*` name (`name`, or a string `code`, its accessor).
    pub name: String,
    /// The server's message, byte for byte.
    pub message: String,
    envelope: Map<String, Value>,
}

/// The reference client's whitelist (`WalletErrorFromJson.ts:157-216`) less
/// `WERR_INTERNAL`, the server's mask for every fault (`StorageServer.ts:122-127`),
/// which stays a fault here as the local door's internal error does.
const REFUSAL_NAMES: [&str; 14] = [
    "WERR_ACTION_BATCH_STATE",
    "WERR_NOT_IMPLEMENTED",
    "WERR_INVALID_OPERATION",
    "WERR_UTXO_REVIEW_INCONCLUSIVE",
    "WERR_BROADCAST_UNAVAILABLE",
    "WERR_INVALID_PARAMETER",
    "WERR_MISSING_PARAMETER",
    "WERR_BAD_REQUEST",
    "WERR_NETWORK_CHAIN",
    "WERR_UNAUTHORIZED",
    "WERR_NOT_ACTIVE",
    "WERR_INSUFFICIENT_FUNDS",
    "WERR_INVALID_PUBLIC_KEY",
    "WERR_REVIEW_ACTIONS",
];

/// `MAX_ERROR_RESULTS` and `MAX_ERROR_BEEF_BYTES` (`WalletErrorFromJson.ts:22-23`).
const MAX_ERROR_RESULTS: usize = 1001;
const MAX_ERROR_BEEF_BYTES: usize = 32 * 1024 * 1024;

/// `dataRecord`: a plain object of at most `max_keys` members.
fn record(value: &Value, max_keys: usize) -> Option<&Map<String, Value>> {
    value.as_object().filter(|map| map.len() <= max_keys)
}

/// `boundedString`: at most `maximum` UTF-16 units, no control character
/// but tab, line feed and carriage return.
fn bounded(value: &Value, maximum: usize) -> Option<&str> {
    let text = value.as_str()?;
    let control = |c: char| {
        c < '\t'
            || ('\u{b}'..='\u{c}').contains(&c)
            || ('\u{e}'..'\u{20}').contains(&c)
            || c == '\u{7f}'
    };
    (text.encode_utf16().count() <= maximum && !text.chars().any(control)).then_some(text)
}

/// `identifier`: a bounded string of at most 128 in `[A-Za-z0-9._:-]+`.
fn identifier(value: &Value) -> Option<&str> {
    bounded(value, 128).filter(|text| {
        !text.is_empty()
            && text
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
    })
}

fn txid(value: &Value) -> Option<[u8; 32]> {
    hex::decode(identifier(value)?).ok()?.try_into().ok()
}

/// `byteArray`: at most 32 MiB of integers 0 to 255.
fn bytes(value: &Value) -> Option<Vec<u8>> {
    let items = value
        .as_array()
        .filter(|a| a.len() <= MAX_ERROR_BEEF_BYTES)?;
    items
        .iter()
        .map(|item| item.as_u64().and_then(|n| u8::try_from(n).ok()))
        .collect()
}

/// `stringArray`: at most `maximum` bounded strings of 256.
fn strings(value: &Value, maximum: usize) -> Option<Vec<&str>> {
    let items = value.as_array().filter(|a| a.len() <= maximum)?;
    items.iter().map(|item| bounded(item, 256)).collect()
}

/// A member absent is `Some(None)`; present, it must read.
fn optional<T>(value: Option<&Value>, read: impl FnOnce(&Value) -> Option<T>) -> Option<Option<T>> {
    match value {
        None => Some(None),
        Some(value) => read(value).map(Some),
    }
}

/// One of `reviewActionResults` (`WalletErrorFromJson.ts:100-122`).
fn review_action_result(value: &Value) -> Option<bsv_rs::wallet::ReviewActionResult> {
    use bsv_rs::wallet::ReviewActionResultStatus as Review;
    let item = record(value, 8)?;
    let status = match bounded(item.get("status")?, 32)? {
        "success" => Review::Success,
        "doubleSpend" => Review::DoubleSpend,
        "serviceError" => Review::ServiceError,
        "invalidTx" => Review::InvalidTx,
        _ => return None,
    };
    let competing_txs = optional(item.get("competingTxs"), |value| {
        strings(value, 1000)?
            .into_iter()
            .map(|text| hex::decode(text).ok()?.try_into().ok())
            .collect::<Option<Vec<[u8; 32]>>>()
    })?;
    let competing_beef = optional(item.get("competingBeef"), bytes)?;
    if status != Review::DoubleSpend && (competing_txs.is_some() || competing_beef.is_some()) {
        return None;
    }
    Some(bsv_rs::wallet::ReviewActionResult {
        txid: txid(item.get("txid")?)?,
        status,
        competing_txs,
        competing_beef,
    })
}

/// One of `sendWithResults` (`WalletErrorFromJson.ts:127-133`).
fn send_with_result(value: &Value) -> Option<bsv_rs::wallet::SendWithResult> {
    use bsv_rs::wallet::SendWithResultStatus as Send;
    let item = record(value, 4)?;
    let status = match bounded(item.get("status")?, 32)? {
        "unproven" => Send::Unproven,
        "sending" => Send::Sending,
        "failed" => Send::Failed,
        _ => return None,
    };
    Some(bsv_rs::wallet::SendWithResult {
        txid: txid(item.get("txid")?)?,
        status,
    })
}

/// A results array of at most `MAX_ERROR_RESULTS`, every entry read.
fn results<T>(value: Option<&Value>, read: fn(&Value) -> Option<T>) -> Option<Vec<T>> {
    let items = value?.as_array().filter(|a| a.len() <= MAX_ERROR_RESULTS)?;
    items.iter().map(read).collect()
}

/// `reviewActionsEnvelope` (`WalletErrorFromJson.ts:89-143`).
fn review_actions(
    obj: &Map<String, Value>,
    message: &str,
) -> Option<bsv_rs::wallet::ReviewActionsRefusal> {
    let no_send_change = optional(obj.get("noSendChange"), |value| {
        strings(value, MAX_ERROR_RESULTS)?
            .into_iter()
            .map(|text| bsv_rs::wallet::Outpoint::from_string(text).ok())
            .collect::<Option<Vec<_>>>()
    })?;
    let mut refusal = bsv_rs::wallet::ReviewActionsRefusal::with_members(
        results(obj.get("reviewActionResults"), review_action_result)?,
        results(obj.get("sendWithResults"), send_with_result)?,
        optional(obj.get("txid"), txid)?,
        optional(obj.get("tx"), bytes)?,
        no_send_change,
    );
    refusal.message = message.to_string();
    Some(refusal)
}

impl WalletError {
    /// Reads the WalletError object a JSON-RPC answer's `error` carries:
    /// an object of at most 64 members, the name in the identifier grammar
    /// and the message a bounded string of 4096 (`WalletErrorFromJson.ts:42-59,156`).
    /// `None` for anything else.
    pub fn from_rpc_error(error: &Value) -> Option<Self> {
        let envelope = record(error, 64)?;
        let name = match envelope.get("name") {
            Some(name) => identifier(name)?,
            None => identifier(envelope.get("code")?)?,
        };
        let message = bounded(envelope.get("message")?, 4096)?;
        Some(Self {
            name: name.to_string(),
            message: message.to_string(),
            envelope: envelope.clone(),
        })
    }

    /// The toolbox error of a whitelisted name: the refusal with its class
    /// and the server's message, `ReviewActions` with its results. `None`
    /// for `WERR_INTERNAL`, a name outside the whitelist, or a review
    /// envelope outside the reference's shape.
    pub fn into_error(self) -> Option<crate::Error> {
        if !REFUSAL_NAMES.contains(&self.name.as_str()) {
            return None;
        }
        if self.name == "WERR_REVIEW_ACTIONS" {
            let refusal = review_actions(&self.envelope, &self.message)?;
            return Some(crate::Error::ReviewActions(Box::new(refusal)));
        }
        let class = bsv_rs::WalletRefusalClass::from_name(&self.name)?;
        Some(crate::Error::SdkError(bsv_rs::Error::WalletRefusal {
            class,
            message: self.message,
        }))
    }
}

impl std::fmt::Display for WalletError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.name, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_rpc_request_serialization() {
        let request = JsonRpcRequest::new("makeAvailable", vec![], 1);

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

    /// The reference's error object has no numeric `code`
    /// (`WalletError.ts:145-149` at ts-stack edf6e03); absent, a string or
    /// out of range, it reads as the default.
    #[test]
    fn test_json_rpc_error_code_forms() {
        let read = |error: Value| -> JsonRpcError {
            let answer = serde_json::json!({"jsonrpc": "2.0", "error": error, "id": 1});
            serde_json::from_value::<JsonRpcResponse>(answer)
                .unwrap()
                .into_result()
                .unwrap_err()
        };
        let absent = read(serde_json::json!({
            "isError": true, "name": "WERR_BAD_REQUEST", "message": "m"
        }));
        assert_eq!(absent.code, error_codes::SERVER_ERROR_START);
        assert_eq!(absent.name.as_deref(), Some("WERR_BAD_REQUEST"));
        let numeric =
            read(serde_json::json!({"name": "WERR_INSUFFICIENT_FUNDS", "message": "m", "code": 7}));
        assert_eq!(numeric.code, 7);
        let string = read(serde_json::json!({"code": "WERR_BAD_REQUEST", "message": "m"}));
        assert_eq!(string.code, error_codes::SERVER_ERROR_START);
        assert_eq!(string.message, "m");
        for code in [
            serde_json::json!(null),
            serde_json::json!(1u64 << 40),
            serde_json::json!(1.5),
        ] {
            let other = read(serde_json::json!({"code": code, "message": "m"}));
            assert_eq!(other.code, error_codes::SERVER_ERROR_START);
        }
        let plain = read(serde_json::json!({"code": -32601, "message": "Method not found: x"}));
        assert_eq!(plain.code, error_codes::METHOD_NOT_FOUND);
        assert_eq!(plain.name, None);
    }

    #[test]
    fn test_json_rpc_error_codes() {
        let err = JsonRpcError::method_not_found("testMethod");
        assert_eq!(err.code, error_codes::METHOD_NOT_FOUND);
        assert!(err.message.contains("testMethod"));
    }
}
