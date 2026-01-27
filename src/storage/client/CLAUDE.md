# Storage Client

> Remote wallet storage via JSON-RPC over HTTPS with BRC-31 authentication.

## Overview

This module provides `StorageClient`, a remote storage implementation that communicates with
BSV wallet storage servers (e.g., `storage.babbage.systems`) using JSON-RPC 2.0 over HTTPS.
It implements the full `WalletStorageProvider` trait hierarchy, enabling wallets to persist
state to remote infrastructure with BRC-31 (Authrite) mutual authentication.

The client is the recommended approach for production wallet applications that need reliable,
persistent storage without managing local databases.

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports and usage documentation |
| `json_rpc.rs` | JSON-RPC 2.0 protocol types (`JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`) |
| `storage_client.rs` | `StorageClient` implementation of `WalletStorageProvider` |

## Key Exports

### Types

| Type | Description |
|------|-------------|
| `StorageClient<W>` | Main client, generic over wallet type `W: WalletInterface` |
| `JsonRpcRequest` | JSON-RPC 2.0 request structure with `new()` constructor |
| `JsonRpcResponse` | JSON-RPC 2.0 response with `into_result()` and `is_success()` methods |
| `JsonRpcError` | JSON-RPC error with code, message, optional data, and factory methods |
| `json_rpc::WalletError` | Wallet-specific error parsed from JSON-RPC error data (code, message, description, stack) |
| `UpdateProvenTxReqWithNewProvenTxArgs` | Arguments for updating proven tx requests |
| `UpdateProvenTxReqWithNewProvenTxResult` | Result containing `txs_updated`, `reqs_updated`, `proven_tx_id` |

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `MAINNET_URL` | `https://storage.babbage.systems` | Production storage endpoint |
| `TESTNET_URL` | `https://staging-storage.babbage.systems` | Testnet storage endpoint |
| `JSON_RPC_VERSION` | `"2.0"` | JSON-RPC protocol version (in `json_rpc` module) |

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
├── endpoint_url: String          # Server URL
├── peer: Arc<Peer<W, ...>>       # BRC-31 authenticated peer
├── wallet: W                      # Wallet for auth
├── http_client: reqwest::Client   # HTTP client
├── next_id: AtomicU64            # Request ID counter
├── settings: Arc<RwLock<...>>    # Cached TableSettings
└── use_auth: bool                 # Enable/disable BRC-31 auth
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
| `relinquishCertificate` | Writer | Release certificate |
| `relinquishOutput` | Writer | Release output |
| `findOrInsertSyncStateAuth` | Sync | Find/create sync state |
| `setActive` | Sync | Set active storage |
| `getSyncChunk` | Sync | Get sync data chunk |
| `processSyncChunk` | Sync | Apply sync data chunk |
| `updateProvenTxReqWithNewProvenTx` | Helper | Update proven tx request |

### StorageClient Helper Methods

Beyond trait implementations, `StorageClient` provides these convenience methods:

| Method | Description |
|--------|-------------|
| `new(wallet, endpoint_url)` | Create client with BRC-31 auth |
| `new_unauthenticated(wallet, url)` | Create client without auth (testing) |
| `mainnet(wallet)` | Create client for mainnet |
| `testnet(wallet)` | Create client for testnet |
| `endpoint_url()` | Get the server URL |
| `peer()` | Get reference to BRC-31 Peer |
| `wallet()` | Get reference to wallet |
| `get_identity_key()` | Get wallet's identity key (hex) |
| `get_settings_async()` | Get cached settings or fetch if needed |
| `is_available_async()` | Check if make_available() has succeeded |
| `create_auth_id()` | Create AuthId for current wallet |
| `create_auth_id_with_user(user_id)` | Create AuthId with user ID |
| `get_storage_info(user_id, is_active)` | Build WalletStorageInfo for this client |

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

### Querying Data

```rust
use bsv_wallet_toolbox::storage::traits::{FindOutputsArgs, FindCertificatesArgs};
use bsv_sdk::wallet::ListOutputsArgs;

// List outputs using SDK args
let list_result = client.list_outputs(&auth, ListOutputsArgs::default()).await?;
println!("Total outputs: {}", list_result.total_outputs);

// Find outputs with custom criteria
let outputs = client.find_outputs(&auth, FindOutputsArgs {
    basket_id: Some(1),
    ..Default::default()
}).await?;

// Find certificates
let certs = client.find_certificates(&auth, FindCertificatesArgs {
    certifiers: Some(vec!["certifier_pubkey".to_string()]),
    ..Default::default()
}).await?;
```

### Creating Transactions

```rust
use bsv_sdk::wallet::CreateActionArgs;

// Create action returns inputs/outputs for transaction construction
let create_result = client.create_action(&auth, CreateActionArgs {
    description: Some("Payment".to_string()),
    outputs: vec![/* output specs */],
    ..Default::default()
}).await?;

// After signing, process the action
use bsv_wallet_toolbox::storage::traits::StorageProcessActionArgs;
let process_result = client.process_action(&auth, StorageProcessActionArgs {
    is_new_tx: true,
    raw_tx: Some(signed_tx_bytes),
    txid: Some(txid_hex),
    reference: Some(create_result.reference),
    ..Default::default()
}).await?;
```

### Unauthenticated Mode (Testing)

```rust
// For testing without BRC-31 authentication
let client = StorageClient::new_unauthenticated(wallet, "http://localhost:8080");
```

### Getting Storage Info

```rust
// Get info about this storage for display/debugging
let info = client.get_storage_info(user.user_id, true /* is_active */).await?;
println!("Storage: {} at {}", info.storage_name, info.endpoint_url.unwrap());
```

## Error Handling

The client converts JSON-RPC errors to `crate::error::Error`:

```rust
match client.make_available().await {
    Ok(settings) => { /* success */ }
    Err(Error::NetworkError(msg)) => {
        // HTTP-level failure
        eprintln!("Network error: {}", msg);
    }
    Err(Error::StorageError(msg)) => {
        // JSON-RPC error from server
        eprintln!("Storage error: {}", msg);
    }
    Err(Error::AuthenticationRequired) => {
        // Failed to get identity key
    }
    Err(e) => { /* other errors */ }
}
```

### Parsing Wallet Errors

```rust
use bsv_wallet_toolbox::storage::client::JsonRpcError;
use bsv_wallet_toolbox::storage::client::json_rpc::WalletError;

// If you have access to the raw JsonRpcError:
if let Some(wallet_err) = WalletError::from_rpc_error(&rpc_error) {
    match wallet_err.code.as_str() {
        "ERR_INSUFFICIENT_FUNDS" => { /* handle */ }
        "ERR_UNAUTHORIZED" => { /* handle */ }
        _ => {}
    }
}
```

## Internal Details

### Request Flow

1. `rpc_call<T>()` increments request ID atomically
2. Creates `JsonRpcRequest` with method and params
3. Serializes to JSON bytes
4. If `use_auth` is enabled, adds `x-bsv-auth-identity-key` header
5. POSTs to `endpoint_url` via `reqwest`
6. Parses response as `JsonRpcResponse`
7. Validates response ID matches request
8. Deserializes result to type `T` or returns error

### Settings Caching

`make_available()` caches `TableSettings` in an `RwLock` to avoid repeated
server calls. Use `get_settings_async()` for safe async access.

### Thread Safety

`StorageClient` uses:
- `AtomicU64` for request IDs (lock-free)
- `Arc<RwLock<...>>` for cached settings
- All methods take `&self` (immutable borrows)

This allows sharing across async tasks safely.

## Testing

The module includes unit tests for JSON-RPC serialization and endpoint URLs.
Integration tests require a running storage server or mock.

```rust
#[test]
fn test_endpoint_urls() {
    assert_eq!(MAINNET_URL, "https://storage.babbage.systems");
    assert_eq!(TESTNET_URL, "https://staging-storage.babbage.systems");
}
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
