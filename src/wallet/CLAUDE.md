# src/wallet/ - Wallet Implementation
> Full WalletInterface implementation combining storage, services, and cryptography

## Overview

This module provides the main `Wallet<S, V>` struct that implements the complete `WalletInterface` trait from `bsv_sdk::wallet`. The Wallet orchestrates three components: `ProtoWallet` for cryptographic operations (key derivation, signing, encryption), a storage backend for persistent state (UTXOs, transactions, certificates), and a services backend for blockchain interaction (broadcasting, merkle proofs, chain height).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Wallet<S, V>                            │
│  (Implements WalletInterface with full storage and services)    │
├─────────────────────────────────────────────────────────────────┤
│  ProtoWallet     │    Storage (S)     │    Services (V)         │
│  - Key derivation│    - UTXOs         │    - Broadcasting       │
│  - Signing       │    - Transactions  │    - Merkle proofs      │
│  - Encryption    │    - Certificates  │    - UTXO status        │
│  - HMAC          │    - Labels/Tags   │    - Chain height       │
├──────────────────┴───────────────────┴──────────────────────────┤
│                       WalletSigner                              │
│  - BIP-143 sighash computation                                  │
│  - P2PKH / P2PK unlocking script generation                     │
│  - Key derivation via protocol + counterparty                   │
├─────────────────────────────────────────────────────────────────┤
│                    PendingTransaction Cache                     │
│  - Caches unsigned transactions for deferred signing            │
│  - 24-hour TTL with automatic cleanup                           │
├─────────────────────────────────────────────────────────────────┤
│                  Certificate Issuance Protocol                  │
│  - BRC-104 HTTP communication with certifiers                   │
│  - Field encryption and master keyring creation                 │
│  - HMAC-based serial number verification                        │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | ~97 | Module declaration with documentation, exports `Wallet`, `WalletOptions`, `WalletSigner`, `PendingTransaction`, `SignerInput` |
| `wallet.rs` | ~2184 | Main `Wallet<S, V>` struct implementing `WalletInterface` with 28 methods, plus `PendingTransaction` and helper functions |
| `signer.rs` | ~737 | `WalletSigner` for transaction signing with BIP-143 sighash, transaction parsing, and script generation |
| `certificate_issuance.rs` | ~1095 | Certificate issuance protocol implementation (BRC-104) for acquiring certificates from certifier services |

## Key Exports

### Wallet Struct

```rust
pub struct Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
{
    proto_wallet: ProtoWallet,
    storage: Arc<S>,
    services: Arc<V>,
    identity_key: String,
    chain: Chain,
    options: WalletOptions,
    signer: WalletSigner,
    pending_transactions: Arc<RwLock<HashMap<String, PendingTransaction>>>,
}
```

Generic wallet implementation parameterized by:
- `S` - Storage backend (e.g., `StorageSqlx`, `StorageClient`)
- `V` - Services backend (e.g., `Services`)

### PendingTransaction

```rust
pub struct PendingTransaction {
    pub reference: String,              // Unique reference for this pending transaction
    pub raw_tx: Vec<u8>,                // The unsigned transaction bytes
    pub inputs: Vec<SignerInput>,       // Input metadata for signing
    pub input_beef: Option<Vec<u8>>,    // BEEF data for broadcasting
    pub is_no_send: bool,               // Whether to skip broadcast
    pub is_delayed: bool,               // Whether delayed broadcast was requested
    pub send_with: Vec<String>,         // TXIDs to send with
    pub created_at: DateTime<Utc>,      // When this pending transaction was created
}
```

Cached transaction awaiting signature via `sign_action`. Created when `create_action` is called with `sign_and_process = false`. Expires after 24 hours (configurable via `PENDING_TRANSACTION_TTL_SECS`).

### WalletOptions

```rust
pub struct WalletOptions {
    pub include_all_source_transactions: bool,  // Default: true
    pub auto_known_txids: bool,                 // Default: false
    pub trust_self: Option<String>,             // Default: Some("known")
}
```

Configuration for wallet behavior:
- `include_all_source_transactions` - Include source transactions for all inputs in signable transactions
- `auto_known_txids` - TXIDs known to wallet's party beef don't need to be returned from storage
- `trust_self` - Input BEEF validation behavior ("known" = trust wallet's known TXIDs)

### WalletSigner

```rust
pub struct WalletSigner {
    root_key: Option<PrivateKey>,
}
```

Handles transaction signing using key derivation from `ProtoWallet`. Signs inputs based on their derivation prefix/suffix, deriving keys via `Protocol` and `Counterparty` from `bsv_sdk::wallet`.

**Methods:**
- `new(root_key: Option<PrivateKey>)` - Create a new signer (uses "anyone" key if None)
- `sign_transaction(&self, unsigned_tx, inputs, proto_wallet) -> Result<Vec<u8>>` - Sign all inputs in a transaction, returns fully signed transaction bytes
- `sign_input(&self, tx_data, input_index, input, proto_wallet) -> Result<Vec<u8>>` - Sign a single input, returns unlocking script bytes

### SignerInput

```rust
pub struct SignerInput {
    pub vin: u32,
    pub source_txid: String,
    pub source_vout: u32,
    pub satoshis: u64,
    pub source_locking_script: Option<Vec<u8>>,
    pub unlocking_script: Option<Vec<u8>>,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
}
```

Input metadata required for signing. Passed from storage results to the signer.

## WalletInterface Methods

The `Wallet` implements all 28 methods from `WalletInterface`:

### Cryptographic Operations (delegated to ProtoWallet)

| Method | Description |
|--------|-------------|
| `get_public_key` | Get identity or derived public key |
| `encrypt` / `decrypt` | AES-GCM encryption with derived keys |
| `create_hmac` / `verify_hmac` | HMAC-SHA256 operations |
| `create_signature` / `verify_signature` | ECDSA signatures |
| `reveal_counterparty_key_linkage` | Reveal key linkage to verifier for counterparty |
| `reveal_specific_key_linkage` | Reveal specific protocol/key_id linkage |

### Action Operations (coordinated with storage and services)

| Method | Description |
|--------|-------------|
| `create_action` | Create Bitcoin transaction with automatic or deferred signing/broadcast |
| `sign_action` | Sign previously created transaction from pending cache |
| `abort_action` | Cancel transaction in progress |
| `list_actions` | List transactions matching labels |
| `internalize_action` | Import external transaction into wallet |

### Output Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `list_outputs` | List spendable outputs in a basket |
| `relinquish_output` | Remove output from basket tracking |

### Certificate Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `acquire_certificate` | Acquire identity certificate (direct protocol) |
| `list_certificates` | List certificates by certifier/type |
| `prove_certificate` | Prove certificate fields to verifier by creating a verifier-specific keyring |
| `relinquish_certificate` | Remove certificate from wallet |

### Discovery Operations

| Method | Description |
|--------|-------------|
| `discover_by_identity_key` | Find certificates by identity key (local storage query, filters by subject) |
| `discover_by_attributes` | Find certificates by attributes (local-only, returns empty - requires overlay service) |

### Chain/Status Operations

| Method | Description |
|--------|-------------|
| `is_authenticated` | Always returns true (wallet has key) |
| `wait_for_authentication` | Always returns true |
| `get_height` | Current blockchain height from services |
| `get_header_for_height` | Block header at height from services |
| `get_network` | Network (mainnet/testnet) |
| `get_version` | Returns "bsv-wallet-toolbox-0.1.0" |

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `WALLET_VERSION` | "bsv-wallet-toolbox-0.1.0" | Version string returned by `get_version` |
| `PENDING_TRANSACTION_TTL_SECS` | 86400 (24 hours) | TTL for pending transactions in cache |

## Transaction Signing Flow

### Immediate Signing (sign_and_process = true, default)

```
CreateActionArgs → storage.create_action() → StorageCreateActionResult
    → build_unsigned_transaction() → WalletSigner.sign_transaction()
    → storage.process_action() → services.post_beef() (if not no_send)
    → CreateActionResult { txid, tx, ... }
```

For each input: derive signing key from protocol/counterparty → compute BIP-143 sighash → sign and build unlocking script.

### Deferred Signing (sign_and_process = false)

```
CreateActionArgs → storage.create_action() → build_unsigned_transaction()
    → Cache in pending_transactions → CreateActionResult { signable_transaction }

Later via sign_action:
SignActionArgs → Lookup from cache → Merge client unlocking scripts
    → WalletSigner.sign_transaction() → storage.process_action()
    → services.post_beef() → SignActionResult { txid, tx, ... }
```

## Sighash Computation

The signer implements BIP-143 style sighash (required for BSV) in `signer.rs`:

```rust
fn compute_sighash(
    tx_data: &[u8],
    input_index: u32,
    locking_script: &[u8],
    satoshis: u64,
) -> Result<[u8; 32]>
```

Computes:
1. `hashPrevouts` - Double SHA256 of all outpoints (txid + vout pairs)
2. `hashSequence` - Double SHA256 of all sequences
3. `hashOutputs` - Double SHA256 of all outputs (satoshis + script for each)
4. Preimage construction: version (4 bytes) + hashPrevouts + hashSequence + outpoint (36 bytes) + scriptCode + value (8 bytes) + sequence (4 bytes) + hashOutputs + locktime (4 bytes) + sighash type (4 bytes)
5. Final hash: Double SHA256 of preimage
6. SIGHASH_ALL | SIGHASH_FORKID (0x41)

## Script Types Supported

The signer recognizes and signs (implemented in `build_unlocking_script`):

| Script Type | Detection | Unlocking Script |
|-------------|-----------|------------------|
| P2PKH | 25 bytes: `76 a9 14 <20-byte hash> 88 ac` (OP_DUP OP_HASH160 PUSH20 \<hash\> OP_EQUALVERIFY OP_CHECKSIG) | `<sig+0x41> <pubkey>` |
| P2PK | ≥35 bytes, starts with pubkey length (33 or 65), ends with `ac` (OP_CHECKSIG) | `<sig+0x41>` |

Note: Signature includes SIGHASH_ALL \| SIGHASH_FORKID (0x41) appended.

## Usage

### Creating a Wallet

```rust
use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
use bsv_sdk::primitives::PrivateKey;

let storage = StorageSqlx::open("wallet.db").await?;
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

let services = Services::mainnet();
let wallet = Wallet::new(Some(PrivateKey::random()), storage, services).await?;

// Use WalletInterface methods
let result = wallet.create_action(args, "app.example.com").await?;
```

### Testnet / Custom Options

```rust
// With custom options
let wallet = Wallet::with_options(root_key, storage, services, WalletOptions {
    include_all_source_transactions: false,
    auto_known_txids: true,
    trust_self: Some("known".to_string()),
}).await?;

// With specific chain
let wallet = Wallet::with_chain(root_key, storage, services, WalletOptions::default(), Chain::Test).await?;
```

## Accessor Methods

| Method | Returns |
|--------|---------|
| `identity_key()` | `&str` - Wallet's identity public key (hex) |
| `chain()` | `Chain` - Network (Main/Test) |
| `storage()` | `&S` - Reference to storage backend |
| `services()` | `&V` - Reference to services backend |
| `options()` | `&WalletOptions` - Configuration options |
| `auth()` | `AuthId` - Creates an AuthId for the current user (internal) |

## Originator Validation

All `WalletInterface` methods require an `originator` string parameter:
- Must not be empty
- Must not exceed 253 characters
- Typically a domain name (e.g., "app.example.com")

## Implementation Notes

### Limited Implementations

Some methods have limited or local-only implementations:
- `discover_by_identity_key` - Local-only: queries storage and filters by subject matching identity_key
- `discover_by_attributes` - Local-only: returns empty results (full implementation requires overlay service)

### Pending Transaction Cache

The wallet maintains an in-memory cache of unsigned transactions:
- Keyed by `reference` string from `StorageCreateActionResult`
- Automatically cleaned up: expired transactions (>24 hours) removed on access
- Thread-safe via `Arc<RwLock<HashMap<...>>>`

### Wallet Constructors

| Constructor | Description |
|-------------|-------------|
| `new(root_key, storage, services)` | Default options, mainnet |
| `with_options(root_key, storage, services, options)` | Custom options, mainnet |
| `with_chain(root_key, storage, services, options, chain)` | Full control over all parameters |

Initialization flow: Creates `ProtoWallet` → gets identity key → creates `WalletSigner` → verifies storage is available → ensures user exists via `find_or_insert_user`.

### Thread Safety

`Wallet<S, V>` uses `Arc<S>` and `Arc<V>` for storage/services. `WalletInterface` impl requires `S: WalletStorageProvider + Send + Sync + 'static` and `V: WalletServices + Send + Sync + 'static`.

### ProtoWallet Delegation

Cryptographic methods delegate to `ProtoWallet` with type conversions:
- `proto_get_public_key`, `proto_encrypt`/`proto_decrypt`, `proto_create_hmac`/`proto_verify_hmac`, `proto_create_signature`/`proto_verify_signature`
- Key linkage methods parse hex public keys back to `PublicKey` for result structs

## Helper Functions (wallet.rs)

Internal helper functions in `wallet.rs`:

| Function | Purpose |
|----------|---------|
| `validate_originator(originator: &str)` | Validates originator string (non-empty, max 253 chars) |
| `compute_txid(raw_tx: &[u8])` | Computes txid (double SHA256, reversed) from raw transaction |
| `build_unsigned_transaction(result: &StorageCreateActionResult)` | Builds unsigned transaction bytes from storage result |
| `write_varint(output: &mut Vec<u8>, value: u64)` | Writes a Bitcoin varint to output buffer |
| `build_wallet_certificate_from_args(args: &AcquireCertificateArgs)` | Builds WalletCertificate from acquisition args |
| `create_keyring_for_verifier(...)` | Creates verifier-specific keyring for selective field disclosure (BRC-52/53) |

### Certificate Field Encryption (prove_certificate)

The `prove_certificate` method implements BRC-52/53 selective attribute disclosure:

1. **Query certificate** - Finds unique certificate matching args via `list_certificates`
2. **Validate keyring** - Ensures storage has master keyring for decryption
3. **Create verifier keyring** - For each field to reveal:
   - Decrypts master symmetric key using certifier as counterparty with key_id = field_name
   - Re-encrypts for verifier using key_id = "{serial_number} {field_name}"
4. **Return keyring** - Field names mapped to base64-encoded encrypted symmetric keys

**Constants:**
- `CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL` = "certificate field encryption"

**Encryption Protocol:**
- Security Level: Counterparty (level 2)
- Master Key ID: `"{field_name}"` (decryption from certifier)
- Verifiable Key ID: `"{serial_number_base64} {field_name}"` (encryption for verifier)

## Signer Helper Functions (signer.rs)

Internal helper functions in `signer.rs`:

| Function | Purpose |
|----------|---------|
| `compute_sighash(tx_data, input_index, locking_script, satoshis)` | Computes BIP-143 sighash for input |
| `parse_transaction(tx_data)` | Parses transaction into (version, inputs, outputs, locktime) |
| `read_varint(data)` | Reads a Bitcoin varint, returns (value, bytes_read) |
| `double_sha256(data)` | Computes double SHA256 hash |
| `build_unlocking_script(locking_script, signature, pubkey)` | Builds P2PKH/P2PK unlocking script |
| `insert_unlocking_script(tx_data, input_index, unlocking_script)` | Inserts unlocking script into transaction |

## Certificate Issuance Protocol (certificate_issuance.rs)

The `certificate_issuance` module implements the BRC-104 certificate issuance protocol for acquiring identity certificates from certifier services. This provides 1:1 parity with Go and TypeScript implementations.

### Protocol Flow

1. **PrepareIssuanceActionData**: Generate random 32-byte nonce, encrypt fields using subject-to-certifier encryption, build JSON request body
2. **HTTP POST**: Send request to certifier URL with BRC-104 headers (`x-bsv-auth-version: 0.1`, `x-bsv-identity-key`)
3. **ParseCertificateResponse**: Parse JSON response, validate headers and certificate components
4. **VerifyCertificateIssuance**: Verify serial number via HMAC, validate certificate type/subject/certifier/fields, verify signature
5. **StoreCertificate**: Save certificate to storage with encrypted fields and master keyring

### Key Types

```rust
// Request sent to certifier
pub struct ProtocolIssuanceRequest {
    pub cert_type: String,           // Certificate type (base64)
    pub client_nonce: String,        // Random nonce (base64)
    pub fields: HashMap<String, String>,      // Encrypted field values
    pub master_keyring: HashMap<String, String>, // Master keyring
}

// Response from certifier
pub struct ProtocolIssuanceResponse {
    pub protocol: String,
    pub certificate: Option<CertificateResponse>,
    pub server_nonce: String,        // Server nonce (base64)
    pub timestamp: String,
    pub version: String,
}
```

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `BRC104_AUTH_VERSION` | "0.1" | BRC-104 authentication version |
| `HEADER_AUTH_VERSION` | "x-bsv-auth-version" | Version header name |
| `HEADER_IDENTITY_KEY` | "x-bsv-identity-key" | Identity key header name |
| `CERTIFICATE_ISSUANCE_PROTOCOL` | "certificate issuance" | Protocol for HMAC verification |
| `NONCE_HMAC_SIZE` | 32 | Expected HMAC/serial number size |
| `NONCE_SIZE` | 32 | Random nonce size in bytes |

### Main Entry Point

```rust
pub async fn acquire_certificate_issuance<W, S>(
    wallet: &W,
    storage: &S,
    auth: &AuthId,
    args: AcquireCertificateArgs,
    identity_key: &str,
    originator: &str,
) -> Result<WalletCertificate>
where
    W: WalletInterface + Send + Sync,
    S: WalletStorageProvider + Send + Sync,
```

Called by `Wallet::acquire_certificate` when `acquisition_protocol` is `Issuance`.

### HMAC Verification

The serial number is verified via HMAC:
- **Data**: `clientNonceBytes || serverNonceBytes`
- **Key ID**: `serverNonce + clientNonce` (concatenated base64 strings)
- **Protocol**: "certificate issuance" at SecurityLevel::Counterparty
- **Counterparty**: The certifier's public key

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main crate documentation
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer details, `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer, `WalletServices` trait
