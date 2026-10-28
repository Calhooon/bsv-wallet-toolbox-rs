# src/wallet/ - Wallet Implementation
> Full WalletInterface implementation combining storage, services, and cryptography

## Overview

`Wallet::process_action(StorageProcessActionArgs, &str)` posts the references or txids in `send_with` without `spend_lock`; `create_action_offering` with `no_send` builds, and its one-call form releases that lock before posting (#66).

`create_action` refuses an empty caller `unlocking_script` or a zero `unlocking_script_length` with `Error::ValidationError` before BEEF parsing or storage (#212). The texts name the parameters bare: `The unlockingScript parameter must be hexadecimal string.` is the reference's byte for byte (`ts-stack@edf6e03` `validationHelpers.ts:522-530,595,758` and `WERR_INVALID_PARAMETER.ts:14`); `The unlockingScriptLength parameter must be a positive integer.` is the port's own in the reference's sentence shape. Hosts reserving a caller input must omit its script and supply a positive length.

For a caller-named stored input, the create door checks change, then the presence of a locking script and integer satoshis, then the live spendability check, including under `options.no_send = true` (#245). In `ts-stack@edf6e03` `packages/wallet/wallet-toolbox/src/storage/methods/createAction.ts`, the `:982` gate is disabled by default at `:61`; `markKnownInputsSpent` (`:399-436`, called at `:503`) decides. The port re-reads the stored output under the write lock before marking it spent. A spender with a txid requires double-spend review regardless of its status, including failed; it answers `Error::ReviewActions` (#264, the owner's ruling 18.4), the reference's `WERR_REVIEW_ACTIONS` text `Undelayed createAction or signAction results require review.` with one `doubleSpend` `ReviewActionResult` whose `competing_txs` names the spender (`:503-509`); its `txid` is all zeros for the reference's `''`, and `competing_beef` is absent. Through `WalletInterface` it is bsv-rs's `Error::WalletReviewActions`. A spender without a txid falls through to the output's `spendable` flag. Without a competing txid, an unspendable row uses `Error::ValidationError` with the live `:420-423` text: `The inputs[N] parameter must be spendable output. output <txid>:<vout> appears to have been spent (spendable=false).` Refusal preserves the original spender through a later abort. A pruned script can still be recovered from stored transaction bytes; zero satoshis and an empty stored script count as present. Missing data uses `Error::ValidationError` with `The <txid>.<vout> parameter must be output with valid lockingScript and satoshis`, without a trailing period (`:979-980`, `sdk/WERR_errors.ts:105`). The change guard and its `:972-976` text remain, and the failed-creator guard still applies.

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
│  - Key derivation via BRC-29 protocol + counterparty            │
│  - Unlocking script templates for deferred signing              │
├─────────────────────────────────────────────────────────────────┤
│                    PendingTransaction Cache                     │
│  - Caches unsigned transactions for deferred signing            │
│  - 24-hour TTL with automatic cleanup                           │
├─────────────────────────────────────────────────────────────────┤
│              BRC-98/99 Permission Enforcement                   │
│  - Protocol access control (DPACP)                              │
│  - Basket access control (DBAP)                                 │
│  - Certificate access control (DCAP)                            │
│  - Spending authorization (DSAP)                                │
├─────────────────────────────────────────────────────────────────┤
│              Balance & Sweep Helpers (P3-07)                    │
│  - balance() / balance_and_utxos() for UTXO summaries           │
│  - sweep_to_address() for full-wallet sweeps                    │
├─────────────────────────────────────────────────────────────────┤
│                  Certificate Issuance Protocol                  │
│  - BRC-104 HTTP communication with certifiers                   │
│  - Field encryption and master keyring creation                 │
│  - HMAC-based serial number verification                        │
├─────────────────────────────────────────────────────────────────┤
│              Overlay Service Discovery (lookup.rs)              │
│  - OverlayLookupResolver trait for SLAP/SHIP queries            │
│  - HttpLookupResolver with endpoint failover                    │
│  - BEEF/PushDrop certificate parsing                            │
├─────────────────────────────────────────────────────────────────┤
│              Scaffolding Types                                  │
│  - PrivilegedKeyManager trait (two-factor auth, wired in)       │
│  - LookupResolver (legacy stub in wallet.rs)                    │
│  - WalletLogger (operation diagnostics)                         │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | ~102 | Module declaration with documentation, exports `Wallet`, `WalletOptions`, `WalletSigner`, `PendingTransaction`, `SignerInput`, `ScriptType`, `UnlockingScriptTemplate`, `WalletBalance`, `UtxoInfo`, `HttpLookupResolver`, `OverlayCertificate`, `OverlayLookupResolver` |
| `wallet.rs` | ~3547 | Main `Wallet<S, V>` struct implementing `WalletInterface` with 28 methods, plus `PendingTransaction`, `WalletBalance`, `UtxoInfo`, `PrivilegedKeyManager`, `LookupResolver`, `WalletLogger`, balance/sweep helpers, permission checks, and helper functions |
| `signer.rs` | ~993 | `WalletSigner` for transaction signing with BIP-143 sighash, `UnlockingScriptTemplate` for deferred signing, transaction parsing, and script generation |
| `certificate_issuance.rs` | ~1090 | Certificate issuance protocol implementation (BRC-104) for acquiring certificates from certifier services |
| `direct_acquire.rs` | ~120 | `validateAcquireDirectCertificateArgs` for the direct `acquire_certificate` (#311) |
| `lookup.rs` | ~610 | Overlay service discovery: `OverlayLookupResolver` trait, `HttpLookupResolver` implementation, BEEF/PushDrop certificate parsing, deduplication |

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
    pending_transactions: Arc<RwLock<HashMap<String, PendingSignAction>>>,
    user_id: i64,
    privileged_key_manager: Option<Arc<dyn PrivilegedKeyManager>>,
    permissions_manager: Option<Arc<WalletPermissionsManager>>,
}
```

Generic wallet implementation parameterized by:
- `S` - Storage backend (e.g., `StorageSqlx`, `StorageClient`)
- `V` - Services backend (e.g., `Services`)

The `user_id` field is resolved during construction via `storage.find_or_insert_user()` and used to build `AuthId` for all storage calls.

The `privileged_key_manager` field is optional; when set via `set_privileged_key_manager()`, crypto operations at SecurityLevel >= 2 are routed through it for two-factor authentication.

The `permissions_manager` field is optional; when set via `set_permissions_manager()`, wallet operations check BRC-98/99 permissions (protocol, basket, certificate, spending) before proceeding.

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

`SignableTransaction.reference` holds the storage reference's decoded bytes: 12 random bytes, not the UTF-8 bytes of its base64 text (#285). Base64-encode that field for `SignActionArgs.reference`; the pending cache and storage RPC use the original base64 string. A malformed base64 storage reference fails the deferred create result before caching with `WalletError("Invalid storage reference: ...")`.

The binary wallet wire carries the raw bytes after their varint length. Since the `bsv-rs =0.9.0-beta.2` pin (bsv-rs edfc535) direct JSON serde writes this field as base64, the reference's JSON string, which is the stored reference text byte for byte; at beta.1 it was hex.

### WalletBalance & UtxoInfo

```rust
pub struct WalletBalance {
    pub total: u64,              // Total satoshis across spendable outputs
    pub utxos: Vec<UtxoInfo>,    // Individual UTXOs (empty for balance(), populated for balance_and_utxos())
}

pub struct UtxoInfo {
    pub satoshis: u64,           // Satoshi value of this output
    pub outpoint: String,        // Outpoint in "txid.vout" format
}
```

Returned by `balance()` and `balance_and_utxos()`. Both use `#[serde(rename_all = "camelCase")]` for cross-SDK wire compatibility.

### WalletOptions

```rust
pub struct WalletOptions {
    pub rng_seed: Option<[u8; 32]>,              // Default: None (OS entropy); separate from the storage seed
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

Handles transaction signing using key derivation from `ProtoWallet`. The wallet authorizes automatic signing only for storage-allocated inputs, using their derivation prefix/suffix and BRC-29 protocol (`"3241645161d8"` at SecurityLevel::Counterparty). Caller-named inputs use the caller's unlocking scripts, even when their stored outputs carry derivation metadata.

**Methods:**
- `new(root_key: Option<PrivateKey>)` - Create a new signer (uses "anyone" key if None)
- `sign_transaction(&self, unsigned_tx, inputs, proto_wallet) -> Result<Vec<u8>>` - Sign inputs without unlocking scripts using their derivation metadata, and insert supplied scripts; the wallet checks signing authority before calling it
- `sign_input(&self, tx_data, input_index, input, proto_wallet) -> Result<Vec<u8>>` - Sign a single input, returns unlocking script bytes
- `create_unlock_template(&self, prefix, suffix, script_type, satoshis) -> UnlockingScriptTemplate` - Create a template for deferred signing
- `apply_templates(&self, raw_tx, templates, proto_wallet) -> Result<Vec<u8>>` - Apply unlocking script templates to sign inputs

### UnlockingScriptTemplate & ScriptType

```rust
pub struct UnlockingScriptTemplate {
    pub derivation_prefix: String,
    pub derivation_suffix: String,
    pub script_type: ScriptType,
    pub satoshis: u64,
}

pub enum ScriptType {
    P2PKH,
    P2PK,
}
```

Templates for deferred signing where the wallet controls the keys. Captures BRC-29 key derivation parameters, script type, and satoshi value (required for BIP-143 sighash) for later unlocking script generation.

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

## Scaffolding Types (wallet.rs)

### PrivilegedKeyManager Trait

```rust
#[async_trait]
pub trait PrivilegedKeyManager: Send + Sync {
    async fn get_public_key(...) -> Result<GetPublicKeyResult>;
    async fn encrypt(...) -> Result<EncryptResult>;
    async fn decrypt(...) -> Result<DecryptResult>;
    async fn create_hmac(...) -> Result<CreateHmacResult>;
    async fn verify_hmac(...) -> Result<VerifyHmacResult>;
    async fn create_signature(...) -> Result<CreateSignatureResult>;
    async fn verify_signature(...) -> Result<VerifySignatureResult>;
}
```

For two-factor authentication where crypto operations at SecurityLevel >= 2 are routed through a separate key manager. The wallet stores an optional `Arc<dyn PrivilegedKeyManager>` and provides `set_privileged_key_manager()` / `privileged_key_manager()` accessors.

### LookupResolver (legacy stub)

Overlay certificate discovery via HTTP in wallet.rs. Has `new(hosts)` and `query(attributes)` methods. Returns empty results with a `tracing::debug!` log. Superseded by the `OverlayLookupResolver` trait in `lookup.rs`.

### WalletLogger

Structured operation logger with indented groups, timestamps, and log levels. Provides `group()`/`group_end()`, `log()`, `error()`, and `to_log_string()` methods.

## Overlay Service Discovery (lookup.rs)

The `lookup.rs` module provides trait-based overlay service lookups for `discover_by_identity_key` and `discover_by_attributes`. Exported via `mod.rs` as `HttpLookupResolver`, `OverlayCertificate`, and `OverlayLookupResolver`.

### OverlayCertificate

Wire format for certificates returned from overlay SLAP/SHIP services. Fields: `type_id`, `serial_number`, `subject`, `certifier`, `revocation_outpoint`, `fields`, `keyring`, `signature`, `decrypted_fields`. Provides:
- `to_identity_certificate()` - Convert to SDK's `IdentityCertificate` format
- `dedup_key()` - Returns `(type_id, serial_number)` tuple for deduplication

### OverlayLookupResolver Trait

```rust
#[async_trait]
pub trait OverlayLookupResolver: Send + Sync {
    async fn lookup_by_identity_key(&self, identity_key: &str) -> Result<Vec<OverlayCertificate>>;
    async fn lookup_by_attributes(&self, attributes: &HashMap<String, String>) -> Result<Vec<OverlayCertificate>>;
}
```

### HttpLookupResolver

Default implementation that queries overlay `/lookup` endpoints via HTTP POST. Features:
- `new(endpoint)` / `with_endpoints(endpoints)` - Single or multi-endpoint
- `for_network(preset)` / `mainnet()` / `testnet()` - Network preset constructors
- Endpoint failover: tries each endpoint in order, skips on error
- Parses BEEF-encoded PushDrop transaction outputs into `OverlayCertificate` structs
- Default hosts: `https://lookup.babbage.systems` (mainnet), `https://staging-lookup.babbage.systems` (testnet)
- 10-second request timeout per endpoint

### Helper Functions

- `parse_overlay_answer(answer)` - Parse `LookupAnswer` into certificates (handles `OutputList` variant)
- `parse_single_output(beef, output_index)` - Parse single BEEF output via PushDrop decode
- `dedup_certificates(certs)` - Deduplicate by `(type_id, serial_number)`, keeping first occurrence

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
| `internalize_action` | Import external transaction into wallet (validates BRC-29 derivation params) |

### Output Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `list_outputs` | List spendable outputs in a basket |
| `relinquish_output` | Remove output from basket tracking |

### Certificate Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `acquire_certificate` | Acquire identity certificate (direct or issuance protocol); a direct acquire validates and normalizes its arguments in the reference's order before storing (#291, #311) |
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

## Additional Wallet Methods (beyond WalletInterface)

| Method | Description |
|--------|-------------|
| `set_privileged_key_manager(manager)` | Set the privileged key manager for 2FA |
| `privileged_key_manager()` | Get reference to privileged key manager |
| `set_permissions_manager(manager)` | Set BRC-98/99 permissions manager |
| `permissions_manager()` | Get reference to permissions manager |
| `balance()` | Total spendable satoshis from default basket (paginated) |
| `balance_and_utxos()` | Total + individual `UtxoInfo` list from default basket |
| `sweep_to_address(address)` | Sweep all funds to a BSV address via `create_action` |
| `list_failed_actions()` | List txids with Failed status |
| `list_no_send_actions()` | List txids with NoSend status |
| `get_known_txids()` | List txids with Completed/Unproven/Sending status |
| `destroy()` | Destroy wallet storage (delegates to `storage.destroy()`) |
| `get_identity_key()` | Identity key as owned String |
| `get_storage_identity()` | Returns `(storage_identity_key, storage_name)` tuple |

### Permission Check Helpers (internal)

When `permissions_manager` is set, operations automatically check permissions:
- `check_protocol_permission(originator, protocol, counterparty, usage_type)` - DPACP enforcement
- `check_basket_permission(originator, basket, usage_type)` - DBAP enforcement
- `check_spending_permission(originator, satoshis)` - DSAP enforcement
- `check_certificate_permission(originator, privileged, verifier, cert_type, fields, usage_type)` - DCAP enforcement

All check helpers return `Ok(())` if no permissions manager is set.

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `WALLET_VERSION` | "bsv-wallet-toolbox-0.1.0" | Version string returned by `get_version` |
| `PENDING_TRANSACTION_TTL_SECS` | 86400 (24 hours) | TTL for pending transactions in cache |
| `CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL` | "certificate field encryption" | Protocol for BRC-52/53 field encryption |

## Transaction Signing Flow

### Immediate Signing (sign_and_process = true, default)

```
CreateActionArgs → storage.create_action_at_door(args, CreateActionDoor { offer, sign_and_process })
    ├─ one write transaction: the create step's rows
    │     → the signer closure: build_unsigned_transaction(result, key_deriver)
    │                           → WalletSigner.sign_transaction()
    │     → processAction's writes → ONE commit
    → services.post_beef() (if not no_send/delayed)
    → storage.update_transaction_status_after_broadcast() (a failure is logged, never the call's error)
    → CreateActionResult { txid, tx, ... }
```

For each storage-allocated input: derive the signing key from the BRC-29 protocol/counterparty, compute the BIP-143 sighash, sign and build the unlocking script. Caller-named inputs retain their supplied scripts.

**Error recovery:** a refusal inside the door (the create step, the signer, processAction's writes) rolls the one transaction back: nothing was written, so a busy is `WalletBusy` and any other refusal keeps its text (`Signing failed: ...`, `Process action failed: ...`). After the broadcast a store fault is logged with the marker `post_broadcast_status_write_failed` and the call answers its success with the txid; the monitor's send-waiting task heals the row (bsv-wallet-cli #44). The same rule after any committed write: `abort_action` on a broadcast transaction answers `{ aborted: true }` when the descendants' retirement after its commit fails (marker `post_commit_retire_failed`; the review-status pass retires them). Coins held only by other calls in flight are `WalletBusy` (`Store busy: coins held by calls in flight`), never `Insufficient funds`. `create_action_offering(args, originator, offer)` is the same call with the change coins limited to the host's offered output ids.

### Deferred Signing (sign_and_process = false or a named input lacks a script)

```
CreateActionArgs → storage.create_action() → build_unsigned_transaction()
    → Cache in pending_transactions → CreateActionResult { signable_transaction }

Later via sign_action:
SignActionArgs → Lookup from cache → Merge and require caller unlocking scripts
    → WalletSigner.sign_transaction() → storage.process_action()
    → services.post_beef() → storage.update_transaction_status_after_broadcast()
    → SignActionResult { txid, tx, ... }
```

The private cache retains which inputs storage allocated (`provided_by = "storage"`), separately from the public `PendingTransaction` data. A caller-named input is never authorized for wallet signing. Its non-empty script from `create_action` or `sign_action` is preserved. This port refuses a missing or empty named-input script before signing or processing, using the reference's `The inputs[N].unlockScript parameter must be valid.` text and mapping its `WERR_INVALID_PARAMETER` to `Error::ValidationError`; `ts-stack@edf6e03` completes and processes the action before `verifyUnlockScripts` reports a missing script's refusal, and refuses an empty script earlier, at its argument gate, as not a hexadecimal string (`validationHelpers.ts:522-530`), the same class. The WalletInterface boundary wraps this as `WalletError("Validation error: ...")`. The action stays unsigned and its inputs stay reserved, so the caller can supply the script on a later call. Reference: `ts-stack@edf6e03`'s `signer/methods/signAction.ts:65-90`, `buildSignableTransaction.ts:176-189`, `completeSignedTransaction.ts:15-32,50-79`, and `verifyUnlockScripts.ts:51-64`.

For #211, `sign_action` accepts spends only for caller inputs deferred with `unlockingScriptLength`, refusing storage-allocated or absent vins and replacements for scripts supplied at `create_action` with `Error::ValidationError` and the text `The args parameter must be spend does not correspond to prior input with valid unlockingScriptLength.`, without changing storage or the pending action, as in `ts-stack@edf6e03`'s `completeSignedTransaction.ts:19-24` and `WERR_errors.ts:105`.

For #216, the private pending-action cache also retains each caller's declared `unlockingScriptLength` from the create args and `sign_action` refuses a longer script before signing or writing with `Error::ValidationError`, while accepting equal or shorter scripts (`ts-stack@edf6e03` `completeSignedTransaction.ts:25-30`, whose rendered message has no trailing period).

### Broadcast Flow

When broadcasting (not no_send, not delayed):
1. Parse `input_beef` into BEEF structure
2. Merge signed transaction into BEEF (`merge_raw_tx`)
3. Downgrade BEEF V2 to V1 for ARC compatibility (if all txs have full data)
4. Broadcast via `services.post_beef()`
5. Check results: at least one provider must return "success"
6. Call `storage.update_transaction_status_after_broadcast(txid, success)` to update status
7. On broadcast failure: return error with "Transaction marked as failed and inputs restored"

### Change Output Locking Scripts

`build_unsigned_transaction` accepts an optional `KeyDeriverApi` reference. For outputs with an empty `locking_script` but a `derivation_suffix`, it derives the P2PKH locking script using BRC-29 key derivation (protocol `"3241645161d8"`, key_id `"{prefix} {suffix}"`, counterparty Self).

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
| P2PK | >=35 bytes, starts with pubkey length (33 or 65), ends with `ac` (OP_CHECKSIG) | `<sig+0x41>` |

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

## Wallet Constructors

| Constructor | Description |
|-------------|-------------|
| `new(root_key, storage, services)` | Default options, mainnet |
| `with_options(root_key, storage, services, options)` | Custom options, mainnet |
| `with_chain(root_key, storage, services, options, chain)` | Full control over all parameters |

Initialization flow: Creates `ProtoWallet` -> gets identity key -> creates `WalletSigner` -> verifies storage is available -> ensures user exists via `find_or_insert_user` -> stores `user_id` -> sets `privileged_key_manager` and `permissions_manager` to None.

## Originator Validation

All `WalletInterface` methods take `originator: Option<&str>` (bsv-rs #329,
from the `=0.9.0-beta.2` pin), as do `Wallet::process_action` and
`Wallet::create_action_offering`. `None` is the reference's `undefined` and is
accepted (`validationHelpers.ts:1003-1004`); `Some("")` is a supplied string
and is refused by the length rule at `:1006` (#284). The permission checks
receive an absent originator as `""`.

Supplied strings are checked after ECMAScript trimming and lowercasing, using
`ts-stack@edf6e03` `packages/sdk/src/wallet/validationHelpers.ts:1005-1036`:

- The normalized string has 1 to 250 UTF-8 bytes.
- An optional port has 1 to 5 ASCII digits and is at most 65535.
- The hostname has 1 to 250 bytes; each dot-separated label has 1 to 63 bytes
  and contains lowercase ASCII letters, digits or internal hyphens.
- Single-label hostnames are accepted. Validation leaves the caller's original
  string for downstream permission checks, as the reference wallet doors do.

An empty or whitespace-only string is refused with
`The originator parameter must be at least 1 length.` inside `Validation error:`.
Other length errors name `originator`, `originator hostname` or `originator part`.
Malformed hostnames and ports use
`The originator parameter must be a canonical DNS hostname with an optional port`.

The storage client's BRC-31 peer passes a fixed internal originator,
`Some("bsv-wallet-toolbox")`.

## Implementation Notes

### Limited Implementations

Some methods have limited or local-only implementations:
- `discover_by_identity_key` - Local-only: queries storage and filters by subject matching identity_key
- `discover_by_attributes` - Local-only: returns empty results (full implementation requires overlay service via `lookup.rs`)

### Pending Transaction Cache

The wallet maintains an in-memory cache of unsigned transactions:
- Keyed by `reference` string from `StorageCreateActionResult`
- Automatically cleaned up: expired transactions (>24 hours) removed on access
- Thread-safe via `Arc<RwLock<HashMap<...>>>`

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
| `validate_originator(originator: Option<&str>)` | `None` passes; a supplied string follows the pinned reference's hostname-and-port rules |
| `compute_txid(raw_tx: &[u8])` | Computes txid (double SHA256, reversed) from raw transaction |
| `build_unsigned_transaction(result, key_deriver)` | Builds unsigned transaction bytes from storage result; optionally derives P2PKH locking scripts for change outputs via BRC-29 |
| `direct_acquire::validate_acquire_direct_args(args)` | The direct acquire's argument checks (`validationHelpers.ts:1405-1439`) and the normalized certificate, verifier and keyring (#311) |
| `create_keyring_for_verifier(...)` | Creates verifier-specific keyring for selective field disclosure (BRC-52/53) |
| `address_to_p2pkh_script(address: &str)` | Decodes Base58Check BSV address to P2PKH locking script bytes (used by `sweep_to_address`) |

### Certificate Field Encryption (prove_certificate)

The `prove_certificate` method implements BRC-52/53 selective attribute disclosure:

Each field's Base64 text encodes ciphertext under an independent 32-byte
`SymmetricKey`. The master keyring holds the wallet-encrypted field key, under
protocol `[2, "certificate field encryption"]` and key ID `field_name`.
The verifier unwraps that key under `"{serial_number_base64} {field_name}"`,
Base64-decodes the field ciphertext, decrypts it with the recovered key, and
reads the plaintext as UTF-8. The keyring does not carry the field plaintext.

1. **Validate arguments** - After permission, check privilegedReason, the `certificate.fields` names (1478-1480), type, serial, certifier, subject, revocation outpoint, signature, the 100000-item array bound, requested name lengths, verifier, then the 100-name count and uniqueness before the lookup (`ts-stack@edf6e03 validationHelpers.ts:1477-1518`)
2. **Query certificate** - Finds unique certificate matching normalized args via `list_certificates`
3. **Validate keyring** - Ensures storage has master keyring for decryption
4. **Create verifier keyring** - For each field to reveal:
   - Decrypts master symmetric key using certifier as counterparty with key_id = field_name
   - Re-encrypts for verifier using key_id = "{serial_number} {field_name}"
5. **Return keyring** - Field names mapped to base64-encoded encrypted symmetric keys

**Encryption Protocol:**
- Security Level: Counterparty (level 2)
- Master Key ID: `"{field_name}"` (decryption from certifier)
- Verifiable Key ID: `"{serial_number_base64} {field_name}"` (encryption for verifier)

## Signer Helper Functions (signer.rs)

Internal helper functions in `signer.rs` (the varint, sha256d and hash160 are bsv-rs's: `bsv_rs::primitives::{from_var_int, to_var_int, sha256d, hash160}`):

| Function | Purpose |
|----------|---------|
| `compute_sighash(tx_data, input_index, locking_script, satoshis)` | Computes BIP-143 sighash for input |
| `parse_transaction(tx_data)` | Parses transaction into (version, inputs, outputs, locktime) |
| `build_unlocking_script(locking_script, signature, pubkey)` | Builds P2PKH/P2PK unlocking script |
| `insert_unlocking_script(tx_data, input_index, unlocking_script)` | Inserts unlocking script into transaction |

## Certificate Issuance Protocol (certificate_issuance.rs)

The `certificate_issuance` module implements the BRC-104 certificate issuance
protocol for acquiring identity certificates from certifier services. Its
field scheme follows `ts-stack@edf6e03`,
`MasterCertificate.ts:277-293,380-420`: draw a fresh `SymmetricKey` per field,
encrypt the plaintext with it, and wallet-encrypt the key to the certifier.
Both ciphertexts are Base64 text in the request and storage. Intake preserves
that text's bytes for the signed representation (BRC-52 `0052.md:97`, #197);
bsv-rs 0.9.0-beta.2's `set_field` takes the Base64 string as received (#261).
Decode a field's Base64 only when decrypting it.

For #283, `Wallet::acquire_certificate` selects the issuance helper and its
argument guards follow `ts-stack@edf6e03 validationHelpers.ts:1307-1349`:
privilege/reason (5-50 UTF-8 bytes), issuance protocol, direct-only serial,
signature, revocation outpoint, revealer and subject keyring, URL presence,
type, certifier, URL syntax, then fields. Empty direct-only strings pass;
even an empty subject keyring refuses. Rust's typed struct supplies the
record/boolean/string shape guarantees. The later privilege validators at
1342-1348 repeat the earlier check and cannot refuse an otherwise valid call.

The shared `validate_certificate_base64_arg` and `validate_public_key_arg`
retain #261/#271's type-before-certifier order, ECMAScript trimming and
canonical public-key text. The new guards share the argument error constructor
and UTF-8 length helper. URL validation requires exact whitespace, an absolute
HTTPS URL, no credentials/query/fragment and a public DNS hostname, in that
order, then removes trailing slashes. Fields permit at most 100000 entries,
reject unsafe record keys before checking names, and require 1-50 UTF-8 bytes
per name. Every argument guard runs before identity parsing, encryption or
HTTP. Both `Wallet.ts:607` (ordinary telemetry-disabled execution) and `:617`
call the same validator via `:770`.

Response guards follow `Wallet.ts:794-817`: certificate presence before nonce
presence, the server nonce helper, the serial HMAC, type, subject, certifier,
truthy revocation outpoint, field count, missing field, field value, signature,
then master-field decryption. Certificate and nonce presence errors end in `!`.
The parser retains the supplied strings; decoding for the SDK occurs only
after metadata checks, and the stored outpoint keeps its supplied text.
The signature step decodes as `Certificate.verify` does (`Certificate.ts:425-441`
over `toBinary`): type and serial through the strict decoder
(`:233,237`; `WalletByteEncoding.ts:39-52`), so a non-canonical type, admitted
by the argument gate, or a non-canonical serial, admitted by the lenient HMAC
decode at `Wallet.ts:808`, answers `Certificate verification failed`. The
outpoint follows `toBinary`'s grammar (`:260-270`): `txid` or `txid.vout`,
64 hex digits, a decimal index without leading zeros up to `0xffffffff`; a
txid-only text is vout 0 over its txid, and `unparsable` or `<txid>.07`
answers `Certificate verification failed`. So does a signature bsv-rs's
`Certificate::verify` answers `Ok(false)` to (absent, non-DER, wrong signer;
bsv-rs #299, BRC-52's derived key from #251), as `Wallet.ts:816` (#188).
`Wallet::reverify_stored_certificates` re-reads every stored certificate
through the same check once, logs each failure and answers
`StoredCertificateReverification { checked, failed }`; it deletes nothing
(the owner's word on #188). An empty master keyring answers `A MasterCertificate
must have a valid masterKeyring!` before decryption and storage
(`MasterCertificate.ts:522-524`, outside the decrypt try).

The server nonce helper matches `verifyNonce.ts:26-57`, using the requested
certifier, `[2, 'server hmac']`, and the UTF-8-decoded random part as key ID, including TextDecoder's leading-BOM removal.
As at `Wallet.ts:806`, its false return is ignored; an error propagates before
the serial HMAC. This differs from the pinned SDK nonce helper (security
level 1, a base64 key ID), so that helper cannot be substituted.

For #300, the client nonce is `createNonce.ts:21-44` as `Wallet.ts:771` calls
it: 16 random bytes, their HMAC through the wallet's `create_hmac` with
`[2, 'server hmac']`, the certifier as counterparty and the bytes'
TextDecoder text as key ID (`nonce_key_id`, shared with the server nonce
check: `from_utf8_lossy`, one U+FFFD per maximal subpart, then one leading
BOM removed), base64 of the 48 bytes. A certifier receives a 64-character
`clientNonce` without padding (it was 44, padded, with no HMAC), which
`verifyNonce(clientNonce, wallet, subject)` accepts and which joins a server
nonce as one base64 string. Tests: `certificate_issuance_nonce_tests.rs`.
Serial decoding and length errors follow the earlier Base64/HMAC validators:
the serial, then `clientNonce + serverNonce` decoded as one string
(`Wallet.ts:808-809`, so a bad server nonce character is named at its index
in the joined text), then the keyID `serverNonce + clientNonce` past 800
UTF-8 bytes (`The keyID parameter must be no more than 800 length.`,
`validationHelpers.ts:227` via `:1772`), then the hmac length
(`validationHelpers.ts:1774`);
the ordinary verifier throws `HMAC is not valid` on a bad tag. A verifier's
false return uses exactly `Invalid serialNumber` at `:814`.

Response field errors are `Fields mismatch! Objects have different numbers
of keys.`, `Missing field: <field> in certificate.fields`, and
`Invalid field! Expected: <sent>, Received: <received>`. Signature errors use
`Certificate verification failed`; decryption errors use
`Failed to decrypt all master certificate fields.` (`MasterCertificate.ts:560`).
The existing error envelope is retained; the internal identity-key parse is
unchanged.

For #269, the certifier's identity header is read as `Wallet.ts:789-793`
reads it, after the HTTP status check and before the body is read: the name
is `x-bsv-auth-identity-key` (the name `AuthFetch.ts:479` sets on the
response from the authenticated sender's key, and the auth middleware sends
at `auth-express-middleware/src/index.ts:936,1008`), it is required, and it
must equal the normalized certifier. An absent header refuses with
`Invalid certifier! Expected: <certifier>, Received: null`, so a body that
would fail the JSON parse is never the error named. The value is read as
fetch's `Headers.get` reads it: each byte one code point (a non-ASCII value
is compared, and refused, rather than dropped) and repeated values joined
with `, `. The old `x-bsv-identity-key` response header is not read; the
reference reads one name. The request still sends `x-bsv-identity-key`.
Tests: `certificate_issuance_header_tests.rs`.

`certificate_issuance_guard_tests.rs` checks the representable argument guards
and their precedence. `certificate_issuance_response_tests.rs` exercises the
response order, texts, nonce calls, raw storage and the verify-time decode through
loopback HTTP. A task-scoped test transport override routes an already
validated public URL to the local fixture; production has no override.

The bsv-rs #261 fixture is pinned byte for byte under
`conformance/upstream/261-certificate-field-value/`. The four tests in
`certificate_issuance_tests.rs` cover intake bytes and reference signature
verification, the stored certificate's `prove_certificate` path, local
issuance through preparation/validation/storage/proof, and fresh keys for
equal and empty plaintexts. They implement the reference's verifier and
decrypt procedures because the pinned SDK's helpers still use the older
signature and field schemes; #199 owns that dependency update.

Hosts must re-issue every certificate produced by the toolbox's own issuance
before #203: those fields and keyrings do not decrypt with the reference, Go
or bsv-rs's corrected scheme. Stored certificates from the reference now
prove and decrypt through the toolbox.
For #222, `prove_certificate` decrypts each requested field under its recovered
master key before wrapping it for the verifier, refusing a mismatched pair with
`bsv_rs::Error::WalletError("Failed to decrypt certificate field!")` as the
reference's plain `Error` does (`ts-stack@edf6e03`,
`MasterCertificate.ts:382-393,612-622`).
For #228 and #229, this recovery restores legacy keys to 32 bytes before wrapping them for the verifier and uses the same `WalletError` for every recovery failure, including non-UTF-8 plaintext (`ts-stack@edf6e03`, `MasterCertificate.ts:573-622`, `VerifiableCertificate.ts:54-60,203`); the plaintext size bound remains with #223.
For #233, the requested name must contain 1 to 50 UTF-8 bytes and differ from
`__proto__`, `constructor`, and `prototype`. The check inside recovery remains
(`ts-stack@edf6e03`, `MasterCertificate.ts:23-29,573-574,621-622`,
`SafeRecord.ts:2-3`). For #236, the gate is here too: before the subset loop,
`create_keyring_for_verifier` checks the requested names, then the keys of
`fields` and the master keyring, then every value in `fields` and the master
keyring, in that order (`MasterCertificate.ts:348-364`). The helper's invalid names
read `Certificate field name must be a safe UTF-8 string of 1–50 bytes`;
duplicate requests read `fieldsToReveal contains duplicate field: <name>`.
Requests and maps are limited to 100 entries (`:43-45,125-142`). Non-canonical
Base64 reads `<label> <name> must use canonical base64 encoding`, and empty
values read `<label> <name> is empty, oversized, or non-canonical`, with
`Certificate field` or `Master certificate keyring field` as the label
(`:63-73,351-364`). The subset check includes the empty-value condition and
uses `Fields to reveal must be a subset of the certificate fields. Missing the "<name>" field.`
(`:370-379`); empty values are refused by the earlier value check.
The gate's plain JavaScript `Error` maps to `WalletError`, as does the
requested-array limit's `TypeError`. Empty requests with empty maps return an empty
keyring. Failures reached inside recovery keep its one sentence; the value
size caps beyond canonical Base64 remain with #223.

For #240, the helper validates the stored serial number after the map values and before the
subset loop (`ts-stack@edf6e03`, `MasterCertificate.ts:365,63-78`). It must be
canonical Base64 encoding exactly 32 bytes. Invalid encoding reads
`Certificate serial number must use canonical base64 encoding`; empty or
oversized values read `Certificate serial number is empty, oversized, or non-canonical`;
shorter nonempty values read `Certificate serial number must encode exactly 32 bytes`.
Each is a `WalletError`, following the gate's mapping of the reference's `Error`.

An empty `fields_to_reveal` is passed unchanged to the helper: it returns an
empty verifier keyring even for a populated certificate, without recovering
any field. Requested names, both maps, and the serial still pass through the
gate. The reference preserves the empty array at
`validationHelpers.ts:1506-1508` and `signer/methods/proveCertificate.ts:31-43`;
`MasterCertificate.ts:348,370-415` runs no field iterations. The serial and
empty-request tests in `wallet.rs::prove_certificate_tests::preloop_gate`
cover both the helper and the storage-backed prove call.

For #249 and #260, `prove_certificate` checks type, serial, certifier,
subject, signature, each requested name's UTF-8 length, then verifier after
permission and before lookup. This is the ported checks' order in
`ts-stack@edf6e03 validationHelpers.ts:1483-1509`, called by
`Wallet.ts:852-858` before `signer/methods/proveCertificate.ts:28`. The
validator runs in both telemetry branches (`Wallet.ts:601-617`); default
telemetry is disabled (`sdk/src/telemetry/Telemetry.ts:601-609`). All Rust
argument checks run without a feature flag. Each key is trimmed with the ECMAScript
WhiteSpace and LineTerminator set and lowercased, as `validateHexString`
does at `validationHelpers.ts:524,533`. This accepts U+FEFF padding but
retains U+0085, which fails the hexadecimal string check. The normalized
value must be 66 hex characters encoding a compressed secp256k1 key; the
refusals are the reference's
`WERR_INVALID_PARAMETER` texts (`validationHelpers.ts:185-191,522-534`),
`The <name> parameter must be <rule>` with `<name>` one of
`certificate.certifier`, `certificate.subject`, or `verifier` and `<rule>` one of `even length, not N.`,
`hexadecimal string.`, `at least 66 length.`, `no more than 66 length.` or
`a valid compressed secp256k1 public key`, as `Error::ValidationError`, so the
WalletInterface reads `Validation error: ...`. An empty request refuses the
same way. The lookup binds the normalized certifier, as
`signer/methods/proveCertificate.ts:16` does. In SQLite, `certifier IN (?)`
on an uncollated TEXT column matches the stored certifier to that value byte
for byte; the remote storage server decides its own collation. The stored
certifier is not parsed, and `create_keyring_for_verifier` receives the keys
parsed from the normalized arguments, as the reference passes the stored
certifier unparsed to `decrypt` (`MasterCertificate.ts:383-392,592-597`). Tests:
`wallet.rs::prove_certificate_tests::preloop_gate::key_gate`.

For #260, type and serial use the reference's structural Base64 check and
32-byte bound (`validationHelpers.ts:439-497,1483-1489`), with rules
`valid base64 string`, `at least 32 bytes`, or `no more than 32 bytes`.
They are trimmed before lookup; the helper still checks the stored serial's
canonical encoding at `MasterCertificate.ts:365`. Subject uses the same
compressed-key check as certifier. Signature uses the hex rules above,
without the key length bounds, then requires canonical DER with in-range
nonzero scalars (`validationHelpers.ts:536-541,1502-1505`,
`Secp256k1Validation.ts:33-50`); high S remains accepted. Its DER refusal is
`The certificate.signature parameter must be a canonical DER-encoded ECDSA signature`.
Subject and signature are normalized before matching stored rows.
The argument name gate (`validationHelpers.ts:348-355,1506-1508`) uses
`The fieldsToReveal <name> parameter must be at least 1 length.` or
`The fieldsToReveal <name> parameter must be no more than 50 length.`.
All these argument refusals use `Validation error: ` at the wallet boundary;
invalid subject or signature previously fell through to the lookup, and
empty or overlong requested names previously reached the helper's safe-name
text. The helper's own name, map and serial gates remain in their existing
order. The permission check still receives the raw verifier, just as
`wallet-toolbox/src/WalletPermissionsManager.ts:5424-5434,1659-1686` does;
that manager normalizes only the originator before checking permission.
Tests: `key_gate::arg_gate` under the same `preloop_gate` module.

For #270, `certificate.revocationOutpoint` is checked after subject and before
signature (`validationHelpers.ts:1498-1501`, its validator at 1083-1116): an
untrimmed 64-hex txid, `.`, and a canonical ASCII-digit vout, else
`The certificate.revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`.
A vout past 2^53 - 1 reads `The certificate.revocationOutpoint vout parameter must be a safe integer`;
one past 4294967295 reads `... vout parameter must be no more than 4294967295 length.`
(`validateInteger` at 306-324). The txid is lowercased and the lookup
matches that normalized outpoint (`signer/methods/proveCertificate.ts:18`).
Ahead of `certificate.type`, `validatePrivilege(args, false)` runs
(`validationHelpers.ts:1477`, its body at 207-222): a present
`privileged_reason` must be 5 to 50 UTF-8 bytes (212-217, via 247-255 and
348-355), `The privilegedReason parameter must be at least 5 length.` or
`... must be no more than 50 length.`; then `privileged: Some(true)` with no
reason reads
`The privilegedReason parameter must be a value when 'privileged' is true`
(218-220). The check at 1511 repeats 1477 on the same value and never fires.
After every vargs check, more
than 100 requested names read
`The fieldsToReveal parameter must be at most 100 fields` and a repeated name
`The fieldsToReveal parameter must be unique field names` (1513-1518, the
bound at 70). These precede the helper's own entry-limit and duplicate texts,
which its direct tests still assert.

For #280, after signature and ahead of the requested names, more than 100000
requested names read
`The fieldsToReveal parameter must be an array of at most 100000 items`
(`validateArray` at 105-114, its default bound at 67, called at 1506); 100000
pass it and meet the count above. No earlier helper bounds the array.
The reference's other `validateArray` callers (createAction's inputs, outputs,
labels, output tags, knownTxids and noSendChange; internalizeAction's outputs
and labels; basket insertion tags; listCertificates' certifiers and types;
listOutputs' tags; listActions' labels) validate outside `wallet.rs` in this
port and are not bounded here. `sendWith` (137) carries its own bound,
`MAXIMUM_SEND_WITH_TRANSACTIONS` (1000), and is #309's.

For #291, a direct `acquire_certificate` passes `revocationOutpoint` through
the same outpoint validator before storing, as `validateAcquireDirectCertificateArgs`
does at `validationHelpers.ts:1435` (called by `Wallet.ts:697` through
`validateAuthAndArgs` in both telemetry branches, 601-617): the txid is
lowercased (533) and the stored row and the returned certificate carry that
value (`signer/methods/acquireDirectCertificate.ts:23,47`). A row acquired
with an uppercase txid now proves; before, it answered
`Certificate not found with the provided arguments` under any argument. A
malformed outpoint is refused with the #270 texts under the bare name:
`The revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`,
`The revocationOutpoint vout parameter must be a safe integer` or
`... must be no more than 4294967295 length.`, and nothing is stored.

For #311, the direct acquire runs the whole of
`validateAcquireDirectCertificateArgs` (`validationHelpers.ts:1401-1450`)
before any write, in `direct_acquire.rs` (`validate_acquire_direct_args`), in
this order: `validatePrivilege(args, false)` (1405, as `prove_certificate`'s);
the presence of `serialNumber`, `signature` and `revocationOutpoint` (absent
or empty), `keyringRevealer` and `keyringForSubject` (absent; an empty
keyring is present), each `The <name> parameter must be valid when
acquisitionProtocol is "direct"` (1409-1430); `type` and `serialNumber` as
base64 of 32 bytes (1432-1433, `valid base64 string`, `at least 32 bytes`,
`no more than 32 bytes`); `certifier` as a compressed key (1434, the key texts
above); `revocationOutpoint` (1435); `fields` (1436, the record and name
texts below); `signature` as hex then canonical DER (1437); then the keyring
(1439, `validateKeyringForSubject` at 1248-1259): `The keyringForSubject
parameter must be an object with at most 100000 properties` or `... an object
with safe string keys`, then per name `The keyringForSubject field name
parameter must be at least 1 length.` or `... no more than 50 length.` and
`The keyringForSubject field value parameter must be valid base64 string`
(structure only, no size bound). The revealer's key (1438) is a parsed
`PublicKey` in the typed arguments and cannot fail; 1440-1446 repeat 1405.
The row stores the normalized values (type and serial trimmed, certifier and
signature lowercased) and `verifier` is the normalized certifier for
`Certifier`, else the revealer's key (`acquireDirectCertificate.ts:19`).
A map's names are walked in sorted order where the reference walks
`Object.keys` (`docs/ARCHITECTURE.md` D12). The toolbox's own checks after the
validator, the signature verification and the field decryption
(`Wallet.ts:705-728`), are not ported: #374. Tests: `direct_acquire_tests.rs`.

`prove_certificate` checks the argument's `certificate.fields` after
`validatePrivilege` and ahead of `certificate.type`
(`validationHelpers.ts:1478-1480`, `validateCertificateFields` at 1232-1241):
more than 100000 keys reads
`The fields parameter must be an object with at most 100000 properties` and a
`__proto__`, `constructor` or `prototype` key
`The fields parameter must be an object with safe string keys`
(`validateRecord` at 81-103), then each name must be 1 to 50 UTF-8 bytes,
`The field name parameter must be at least 1 length.` or
`... must be no more than 50 length.` (348-355); names are checked in sorted
order since a map keeps no insertion order. The helper's own map check on
the stored row stays (`MasterCertificate.ts:349-350`, which the reference
runs on `storageCert.fields`, `signer/methods/proveCertificate.ts:35`).

### Protocol Flow

1. **PrepareIssuanceActionData**: Create the client nonce as `createNonce` (16 random bytes and their 32-byte HMAC, 64 base64 characters), encrypt fields under independent symmetric keys, wrap the keys from subject to certifier, build JSON request body
2. **HTTP POST**: Send request to certifier URL with BRC-104 headers (`x-bsv-auth-version: 0.1`, `x-bsv-identity-key`)
3. **Identity header**: Require `x-bsv-auth-identity-key` equal to the certifier before the body is read (#269)
4. **ParseCertificateResponse**: Require the certificate, then nonce, and retain the supplied certificate text
5. **VerifyCertificateIssuance**: Run the nonce helper, verify the serial HMAC, validate metadata and fields, verify the signature, then decrypt every master field
6. **StoreCertificate**: Save certificate to storage with encrypted fields and master keyring

### Key Types

```rust
// Request sent to certifier
pub struct ProtocolIssuanceRequest {
    pub cert_type: String,           // Certificate type (base64)
    pub client_nonce: String,        // createNonce's 48 bytes (64 base64 chars, no padding)
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
| `HEADER_IDENTITY_KEY` | "x-bsv-identity-key" | Request identity key header name |
| `HEADER_CERTIFIER_IDENTITY_KEY` | "x-bsv-auth-identity-key" | Response header the certifier's key must be sent under (#269) |
| `CERTIFICATE_ISSUANCE_PROTOCOL` | "certificate issuance" | Protocol for HMAC verification |
| `NONCE_HMAC_SIZE` | 32 | Expected HMAC/serial number size |
| `NONCE_SIZE` | 16 | The client nonce's random part in bytes (`createNonce.ts:27`) |
| `MAX_KEY_ID_BYTES` | 800 | verifyHmac's keyID bound (`validationHelpers.ts:227`) |

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
- **Data**: `clientNonce + serverNonce` decoded as one base64 string
- **Key ID**: `serverNonce + clientNonce` (concatenated base64 strings)
- **Protocol**: "certificate issuance" at SecurityLevel::Counterparty
- **Counterparty**: The certifier's public key

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main crate documentation
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer details, `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer, `WalletServices` trait
