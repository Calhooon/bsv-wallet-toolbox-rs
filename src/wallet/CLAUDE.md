# src/wallet/ - Wallet Implementation
> Full WalletInterface implementation combining storage, services, and cryptography

## Overview

This module provides the main `Wallet<S, V>` struct that implements the complete `WalletInterface` trait from `bsv-sdk`. The Wallet orchestrates three components: `ProtoWallet` for cryptographic operations (key derivation, signing, encryption), a storage backend for persistent state (UTXOs, transactions, certificates), and a services backend for blockchain interaction (broadcasting, merkle proofs, chain height).

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
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module declaration with documentation, exports `Wallet`, `WalletOptions`, `WalletSigner` |
| `wallet.rs` | Main `Wallet<S, V>` struct implementing `WalletInterface` with 28 methods |
| `signer.rs` | `WalletSigner` for transaction signing with BIP-143 sighash and script generation |

## Key Exports

### Wallet Struct

```rust
pub struct Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
```

Generic wallet implementation parameterized by:
- `S` - Storage backend (e.g., `StorageSqlx`, `StorageClient`)
- `V` - Services backend (e.g., `Services`)

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

Handles transaction signing using key derivation from `ProtoWallet`. Signs inputs based on their derivation prefix/suffix.

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
| `create_action` | Create Bitcoin transaction with automatic signing/broadcast |
| `sign_action` | Sign previously created transaction (stub) |
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
| `prove_certificate` | Prove certificate fields to verifier (stub) |
| `relinquish_certificate` | Remove certificate from wallet |

### Discovery Operations

| Method | Description |
|--------|-------------|
| `discover_by_identity_key` | Find certificates by identity key (stub) |
| `discover_by_attributes` | Find certificates by attributes (stub) |

### Chain/Status Operations

| Method | Description |
|--------|-------------|
| `is_authenticated` | Always returns true (wallet has key) |
| `wait_for_authentication` | Always returns true |
| `get_height` | Current blockchain height from services |
| `get_header_for_height` | Block header at height from services |
| `get_network` | Network (mainnet/testnet) |
| `get_version` | Returns "bsv-wallet-toolbox-0.1.0" |

## Transaction Signing Flow

The `create_action` method orchestrates transaction creation:

```
CreateActionArgs
       │
       ▼
storage.create_action()
       │
       ├─► StorageCreateActionResult
       │   - inputs (with derivation info)
       │   - outputs
       │   - reference
       │   - input_beef
       │
       ▼
build_unsigned_transaction()
       │
       ▼
WalletSigner.sign_transaction()
       │
       ├─► For each input:
       │   1. Derive signing key from protocol/counterparty
       │   2. Compute BIP-143 sighash
       │   3. Sign and build unlocking script
       │
       ▼
storage.process_action()
       │
       ▼
services.post_beef() (if not no_send)
       │
       ▼
CreateActionResult
```

## Sighash Computation

The signer implements BIP-143 style sighash (required for BSV):

```rust
fn compute_sighash(
    tx_data: &[u8],
    input_index: u32,
    locking_script: &[u8],
    satoshis: u64,
) -> Result<[u8; 32]>
```

Computes:
1. `hashPrevouts` - Double SHA256 of all outpoints
2. `hashSequence` - Double SHA256 of all sequences
3. `hashOutputs` - Double SHA256 of all outputs
4. Preimage with version, hashes, outpoint, scriptCode, value, sequence, locktime
5. SIGHASH_ALL | SIGHASH_FORKID (0x41)

## Script Types Supported

The signer recognizes and signs:

| Script Type | Detection | Unlocking Script |
|-------------|-----------|------------------|
| P2PKH | 25 bytes: `76 a9 14 <20-byte hash> 88 ac` | `<sig> <pubkey>` |
| P2PK | Ends with `ac`, starts with pubkey push | `<sig>` |

## Usage

### Creating a Wallet

```rust
use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
use bsv_sdk::primitives::PrivateKey;
use bsv_sdk::wallet::WalletInterface;

let storage = StorageSqlx::open("wallet.db").await?;
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

let services = Services::mainnet();
let root_key = PrivateKey::random();
let wallet = Wallet::new(Some(root_key), storage, services).await?;

// Get public key
let result = wallet.get_public_key(args, "app.example.com").await?;
```

### Creating a Transaction

```rust
use bsv_sdk::wallet::CreateActionArgs;

let args = CreateActionArgs {
    description: "Payment".to_string(),
    inputs: vec![...],
    outputs: vec![...],
    labels: vec!["payment".to_string()],
    options: Some(CreateActionOptions {
        sign_and_process: Some(true),
        no_send: Some(false),
        ..Default::default()
    }),
    ..Default::default()
};

let result = wallet.create_action(args, "app.example.com").await?;
// result.txid - Transaction ID
// result.tx - Signed transaction bytes
```

### With Custom Options

```rust
let options = WalletOptions {
    include_all_source_transactions: false,
    auto_known_txids: true,
    trust_self: Some("known".to_string()),
};

let wallet = Wallet::with_options(
    Some(root_key),
    storage,
    services,
    options
).await?;
```

### Testnet Wallet

```rust
use bsv_wallet_toolbox::services::Chain;

let wallet = Wallet::with_chain(
    Some(root_key),
    storage,
    services,
    WalletOptions::default(),
    Chain::Test
).await?;
```

## Accessor Methods

| Method | Returns |
|--------|---------|
| `identity_key()` | `&str` - Wallet's identity public key (hex) |
| `chain()` | `Chain` - Network (Main/Test) |
| `storage()` | `&S` - Reference to storage backend |
| `services()` | `&V` - Reference to services backend |
| `options()` | `&WalletOptions` - Configuration options |

## Originator Validation

All `WalletInterface` methods require an `originator` string parameter:
- Must not be empty
- Must not exceed 253 characters
- Typically a domain name (e.g., "app.example.com")

## Implementation Notes

### Stub Methods

Some methods return stubs or errors:
- `sign_action` - Requires pending transaction cache; use `create_action` with `sign_and_process=true`
- `prove_certificate` - Requires keyring handling
- `discover_by_identity_key` / `discover_by_attributes` - Require overlay lookup service

### Storage Initialization

The `Wallet::new` constructor:
1. Creates `ProtoWallet` from root key
2. Gets identity key from ProtoWallet
3. Creates `WalletSigner` with root key
4. Verifies storage is available
5. Ensures user exists in storage via `find_or_insert_user`

### Thread Safety

`Wallet<S, V>` stores storage and services in `Arc<S>` and `Arc<V>`, making it shareable across threads when both `S` and `V` are `Send + Sync + 'static`.

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main crate documentation
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer details, `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer, `WalletServices` trait
