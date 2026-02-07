# Storage Client

> Remote wallet storage via JSON-RPC over HTTPS with BRC-31/BRC-104 authentication.

## Overview

This module provides `StorageClient`, a remote storage implementation that communicates with
BSV wallet storage servers (e.g., `storage.babbage.systems`) using JSON-RPC 2.0 over HTTPS.
It implements the full `WalletStorageProvider` trait hierarchy, enabling wallets to persist
state to remote infrastructure with BRC-104 mutual authentication via `Peer`.

The client is the recommended approach for production wallet applications that need reliable,
persistent storage without managing local databases.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports and usage documentation |
| `auth.rs` | BRC-31 authentication: nonce creation, request signing, response verification |
| `json_rpc.rs` | JSON-RPC 2.0 protocol types (`JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`) |
| `storage_client.rs` | `StorageClient` implementation of `WalletStorageProvider` |

## Key Exports

### Types

| Type | Description |
|------|-------------|
| `StorageClient<W>` | Main client, generic over wallet type `W: WalletInterface` |
| `ValidCreateActionArgs` | Validated CreateAction args with internal state flags (see below) |
| `JsonRpcRequest` | JSON-RPC 2.0 request structure with `new()` constructor |
| `JsonRpcResponse` | JSON-RPC 2.0 response with `into_result()` and `is_success()` methods |
| `JsonRpcError` | JSON-RPC error with code, message, optional data, and factory methods |
| `json_rpc::WalletError` | Wallet-specific error parsed from JSON-RPC error data (code, message, description, stack) |
| `UpdateProvenTxReqWithNewProvenTxArgs` | Arguments for updating proven tx requests |
| `UpdateProvenTxReqWithNewProvenTxResult` | Result containing `txs_updated`, `reqs_updated`, `proven_tx_id` |
| `AuthHeaders` | Authentication headers for a BRC-31 request |
| `ResponseAuthHeaders` | Parsed auth headers from server response |
| `AuthVerificationResult` | Result of verifying server response authentication |

### ValidCreateActionArgs

The `ValidCreateActionArgs` struct wraps SDK's `CreateActionArgs` and adds internal state flags
required by the storage server. This mirrors TypeScript's `ValidCreateActionArgs` class.

**Important**: The server expects these flags to be present. Without them, it returns "internal error".

| Field | Type | Description |
|-------|------|-------------|
| `is_new_tx` | `bool` | True when creating a new transaction (typically true for createAction) |
| `is_no_send` | `bool` | True when `options.noSend` is true - creates tx but doesn't broadcast |
| `is_delayed` | `bool` | True when `options.acceptDelayedBroadcast` is true |
| `is_send_with` | `bool` | True when `options.sendWith` has items |
| `is_remix_change` | `bool` | True for change-only remix transactions (no explicit inputs/outputs) |
| `is_sign_action` | `bool` | True when `options.signAndProcess` is true |
| `include_all_source_transactions` | `bool` | True to include all ancestor transactions in BEEF |

**Usage**: The `create_action` method automatically converts `CreateActionArgs` to `ValidCreateActionArgs`:

```rust
// This happens automatically inside StorageClient::create_action
let valid_args = ValidCreateActionArgs::from(args);

// Or with custom flag overrides:
let valid_args = ValidCreateActionArgs::with_flags(args, is_new_tx, is_no_send, is_delayed, is_send_with);
```

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `MAINNET_URL` | `https://storage.babbage.systems` | Production storage endpoint |
| `TESTNET_URL` | `https://staging-storage.babbage.systems` | Testnet storage endpoint |
| `JSON_RPC_VERSION` | `"2.0"` | JSON-RPC protocol version (in `json_rpc` module) |
| `AUTH_VERSION` | `"0.1"` | BRC-31 authentication protocol version |
| `AUTH_PROTOCOL_ID` | `"storage auth"` | Protocol ID for BRC-31 request signatures |
| `NONCE_PROTOCOL_ID` | `"storage nonce"` | Protocol ID for nonce HMAC computation |

### JSON-RPC Error Codes

Standard codes in `json_rpc::error_codes`:

| Constant | Value | Meaning |
|----------|-------|---------|
| `PARSE_ERROR` | -32700 | Invalid JSON |
| `INVALID_REQUEST` | -32600 | Not a valid Request object |
| `METHOD_NOT_FOUND` | -32601 | Method does not exist |
| `INVALID_PARAMS` | -32602 | Invalid method parameters |
| `INTERNAL_ERROR` | -32603 | Internal JSON-RPC error |
| `SERVER_ERROR_START` | -32000 | Server error range start |
| `SERVER_ERROR_END` | -32099 | Server error range end |

Wallet-specific codes in `json_rpc::wallet_error_codes`:

| Constant | Value |
|----------|-------|
| `INVALID_OPERATION` | `ERR_INVALID_OPERATION` |
| `BAD_REQUEST` | `ERR_BAD_REQUEST` |
| `UNAUTHORIZED` | `ERR_UNAUTHORIZED` |
| `NOT_FOUND` | `ERR_NOT_FOUND` |
| `INTERNAL` | `ERR_INTERNAL` |
| `INSUFFICIENT_FUNDS` | `ERR_INSUFFICIENT_FUNDS` |
| `INVALID_TX` | `ERR_INVALID_TX` |

### JsonRpcError Factory Methods

`JsonRpcError` provides convenient constructors:

| Method | Creates |
|--------|---------|
| `new(code, message)` | Basic error |
| `with_data(code, message, data)` | Error with additional JSON data |
| `parse_error(details)` | Parse error (-32700) |
| `invalid_request(details)` | Invalid request error (-32600) |
| `method_not_found(method)` | Method not found error (-32601) |
| `invalid_params(details)` | Invalid params error (-32602) |
| `internal_error(details)` | Internal error (-32603) |
| `is_server_error()` | Returns true if code is in server error range |

### WalletError Parsing

`WalletError` can be extracted from JSON-RPC errors:

```rust
if let Some(wallet_err) = WalletError::from_rpc_error(&rpc_error) {
    println!("Code: {}, Message: {}", wallet_err.code, wallet_err.message);
    if let Some(desc) = &wallet_err.description {
        println!("Description: {}", desc);
    }
}
```

## Architecture

### StorageClient Structure

```
StorageClient<W: WalletInterface>
├── endpoint_url: String              # Server URL
├── peer: Arc<Peer<W, SimplifiedFetchTransport>>  # BRC-104 authenticated peer
├── wallet: W                          # Wallet for auth and signing
├── http_client: reqwest::Client       # HTTP client (unauthenticated fallback)
├── next_id: AtomicU64                # Request ID counter
├── settings: Arc<RwLock<...>>        # Cached TableSettings
├── use_auth: bool                     # Enable/disable BRC-104 auth
├── verify_responses: bool             # Enable/disable response signature verification
└── server_identity_key: Arc<RwLock<..>> # Cached server identity key
```

### Trait Implementation Hierarchy

`StorageClient` implements the full trait hierarchy from `crate::storage::traits`:

```
WalletStorageReader     ← find_certificates, find_outputs, list_actions, etc.
        ↑
WalletStorageWriter     ← make_available, create_action, process_action, etc.
        ↑
WalletStorageSync       ← get_sync_chunk, process_sync_chunk, set_active
        ↑
WalletStorageProvider   ← Full provider interface
```

### JSON-RPC Methods Called

The client calls these remote methods via `rpc_call()`:

| Method | Trait | Description |
|--------|-------|-------------|
| `makeAvailable` | Writer | Initialize connection, get settings |
| `migrate` | Writer | Run database migrations |
| `destroy` | Writer | Delete all user data |
| `findOrInsertUser` | Writer | Find/create user by identity key |
| `findCertificatesAuth` | Reader | Query certificates |
| `findOutputBaskets` | Reader | Query output baskets |
| `findOutputsAuth` | Reader | Query transaction outputs |
| `findProvenTxReqs` | Reader | Query proven tx requests |
| `listActions` | Reader | List user transactions |
| `listCertificates` | Reader | List user certificates |
| `listOutputs` | Reader | List user outputs |
| `abortAction` | Writer | Cancel in-progress action |
| `createAction` | Writer | Start new transaction |
| `processAction` | Writer | Process signed transaction |
| `internalizeAction` | Writer | Import external transaction |
| `insertCertificateAuth` | Writer | Add certificate |
| `insertCertificateFieldAuth` | Writer | Add certificate field |
| `relinquishCertificate` | Writer | Release certificate |
| `relinquishOutput` | Writer | Release output |
| `updateTransactionStatusAfterBroadcast` | Writer | Update tx status post-broadcast |
| `reviewStatus` | Writer | Review aged transaction statuses |
| `purgeData` | Writer | Purge old/completed data |
| `findOrInsertSyncStateAuth` | Sync | Find/create sync state |
| `setActive` | Sync | Set active storage |
| `getSyncChunk` | Sync | Get sync data chunk |
| `processSyncChunk` | Sync | Apply sync data chunk |
| `updateProvenTxReqWithNewProvenTx` | Helper | Update proven tx request |

### StorageClient Helper Methods

Beyond trait implementations, `StorageClient` provides these convenience methods:

| Method | Description |
|--------|-------------|
| `new(wallet, endpoint_url)` | Create client with BRC-104 auth via Peer |
| `new_unauthenticated(wallet, url)` | Create client without auth (testing) |
| `mainnet(wallet)` | Create client for mainnet |
| `testnet(wallet)` | Create client for testnet |
| `endpoint_url()` | Get the server URL |
| `peer()` | Get reference to BRC-104 Peer |
| `wallet()` | Get reference to wallet |
| `get_identity_key()` | Get wallet's identity key (hex) |
| `get_settings_async()` | Get cached settings or fetch if needed |
| `is_available_async()` | Check if make_available() has succeeded |
| `create_auth_id()` | Create AuthId for current wallet |
| `create_auth_id_with_user(user_id)` | Create AuthId with user ID |
| `get_storage_info(user_id, is_active)` | Build WalletStorageInfo for this client |
| `set_verify_responses(verify)` | Enable/disable response signature verification |
| `set_server_identity_key(key)` | Set server's identity key for signing |
| `get_server_identity_key()` | Get cached server identity key |
| `update_proven_tx_req_with_new_proven_tx(args)` | Update proven tx request with proof |

## Usage

### Basic Connection

```rust
use bsv_wallet_toolbox::storage::client::StorageClient;
use bsv_sdk::wallet::ProtoWallet;
use bsv_sdk::primitives::PrivateKey;

// Create wallet for authentication
let private_key = PrivateKey::from_wif("...")?;
let wallet = ProtoWallet::new(Some(private_key));

// Connect to mainnet storage
let client = StorageClient::mainnet(wallet);

// Or use testnet
// let client = StorageClient::testnet(wallet);

// Or specify custom endpoint
// let client = StorageClient::new(wallet, "https://custom.storage.example");

// Initialize connection
let settings = client.make_available().await?;
println!("Connected to: {} (chain: {})", settings.storage_name, settings.chain);
```

### User Authentication Flow

```rust
// Get wallet's identity key
let identity_key = client.get_identity_key().await?;

// Find or create user
let (user, is_new) = client.find_or_insert_user(&identity_key).await?;
if is_new {
    println!("Created new user: {}", user.user_id);
}

// Create authenticated ID for subsequent operations
let auth = client.create_auth_id_with_user(user.user_id).await?;
```

### Unauthenticated Mode (Testing)

```rust
// For testing without BRC-104 authentication
let client = StorageClient::new_unauthenticated(wallet, "http://localhost:8080");
```

## Error Handling

The client converts JSON-RPC errors to `crate::error::Error`:

```rust
match client.make_available().await {
    Ok(settings) => { /* success */ }
    Err(Error::NetworkError(msg)) => {
        // HTTP-level failure or timeout
        eprintln!("Network error: {}", msg);
    }
    Err(Error::StorageError(msg)) => {
        // JSON-RPC error from server
        eprintln!("Storage error: {}", msg);
    }
    Err(Error::AuthenticationRequired) => {
        // Failed to get identity key
    }
    Err(Error::InvalidOperation(msg)) => {
        // E.g., calling get_services() on remote client
    }
    Err(e) => { /* other errors */ }
}
```

## BRC-104 Peer-Based Authentication

### Overview

When `use_auth` is enabled (the default), all requests are sent through a `Peer<W, SimplifiedFetchTransport>`
which handles BRC-104 mutual authentication automatically. This replaces the older direct
HTTP + BRC-31 header approach with proper session management.

### Authenticated Request Flow

1. `rpc_call<T>()` increments request ID atomically
2. Creates `JsonRpcRequest` with method and params, serializes to JSON
3. Wraps in `HttpRequest` with 32 random bytes as request nonce
4. Converts to payload via `http_request.to_payload()`
5. Registers a `oneshot` channel listener via `peer.listen_for_general_messages()`
6. Sends payload via `peer.to_peer()` with 30s timeout and server identity key
7. Listener matches response by comparing first 32 bytes (request nonce)
8. Parses `HttpResponse` from payload, checks HTTP status
9. Deserializes `JsonRpcResponse`, validates response ID matches
10. Returns deserialized result `T` or converts error to `Error::StorageError`

### Unauthenticated Fallback

When `use_auth` is false, `rpc_call_unauthenticated()` sends plain HTTP POST via `reqwest::Client`
without any BRC-104 headers. Used for testing.

### Settings Caching

`make_available()` caches `TableSettings` in an `RwLock` to avoid repeated
server calls. Also caches the server's identity key from settings for future
request signing. Use `get_settings_async()` for safe async access.

### Thread Safety

`StorageClient` uses:
- `AtomicU64` for request IDs (lock-free)
- `Arc<RwLock<...>>` for cached settings and server identity key
- `Arc<Peer<...>>` for shared peer access
- All methods take `&self` (immutable borrows)

This allows sharing across async tasks safely.

## Auth Module (auth.rs)

The `auth` module provides lower-level BRC-31 authentication primitives. While `StorageClient`
now uses `Peer` for transport-level auth, these functions remain available for custom usage.

### Request Headers (BRC-104)

| Header | Description |
|--------|-------------|
| `x-bsv-auth-version` | Protocol version ("0.1") |
| `x-bsv-auth-identity-key` | Client's 33-byte compressed public key (hex) |
| `x-bsv-auth-nonce` | Random 32-byte nonce (base64) |
| `x-bsv-auth-timestamp` | Unix timestamp in milliseconds |
| `x-bsv-auth-signature` | Signature over canonical request data (hex) |

### Signature Format

The signature covers: `method || path || SHA256(body) || timestamp || nonce`

### Nonce Creation

| Method | Use Case |
|--------|----------|
| `create_simple_nonce()` | Simple random 32-byte nonce (sync) |
| `create_nonce()` | HMAC-verified nonce for mutual auth (async) |

The HMAC nonce format is: `base64(random_16_bytes || hmac_16_bytes)` where HMAC uses BRC-42 key derivation.

### Timestamp Validation

- Must be within 5 minutes of current time (300,000 ms)
- Slight future timestamps (up to 1 minute) allowed for clock skew

### Auth Module Exports

| Export | Description |
|--------|-------------|
| `AuthHeaders` | Struct containing all auth header values |
| `ResponseAuthHeaders` | Parsed auth headers from server response |
| `AuthVerificationResult` | Result of response verification |
| `create_auth_headers()` | Create signed auth headers for a request (async) |
| `create_nonce()` | Create HMAC-verified nonce for BRC-31 (async) |
| `create_simple_nonce()` | Generate random 32-byte nonce (base64) |
| `current_timestamp_ms()` | Get current Unix timestamp in milliseconds |
| `validate_timestamp()` | Validate timestamp is within acceptable range |
| `create_signing_data()` | Create canonical signing data |
| `sign_request()` | Sign a request using wallet identity key (async) |
| `verify_response()` | Verify a response signature from server (async) |
| `verify_response_auth()` | Full response auth verification with header checking (async) |
| `AUTH_VERSION` | Protocol version constant ("0.1") |
| `AUTH_PROTOCOL_ID` | Protocol ID for request signatures |
| `NONCE_PROTOCOL_ID` | Protocol ID for nonce HMAC computation |
| `headers::*` | Header name constants |

## Testing

The module includes comprehensive unit tests:

### Test Categories

| Category | Count | Coverage |
|----------|-------|----------|
| JSON-RPC serialization | 4 | Request/response format, error handling |
| Auth module | 35+ | Nonce, timestamp, signing data, headers, replay protection |
| Storage client BRC-31 | 9 | Header names, version, nonce creation, timestamps, signing |
| Method formats | 22+ | All JSON-RPC method request/response formats |
| Entity types | 15+ | Table entity serialization/deserialization |
| ValidCreateActionArgs | 7 | Flag derivation, serialization, custom flags |

### Running Tests

```bash
# Run all storage client tests
cargo test --features remote storage::client::

# Run auth module tests specifically
cargo test --features remote storage::client::auth::

# Run BRC-31 integration tests
cargo test --features remote brc31
```

## Related

- `../traits.rs` - `WalletStorageProvider` trait hierarchy and related types
- `../entities/` - Table entity types (`TableUser`, `TableOutput`, etc.)
- `../mod.rs` - Parent storage module with trait re-exports
- `../sqlx/` - Alternative local SQLite/MySQL storage implementation

## Feature Flag

This module requires the `remote` feature:

```toml
[dependencies]
bsv-wallet-toolbox = { version = "...", features = ["remote"] }
```
