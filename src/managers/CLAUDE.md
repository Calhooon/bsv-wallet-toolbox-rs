# src/managers/ - Wallet Manager Components
> Higher-level wallet management abstractions for authentication, settings, storage sync, and permissions

## Overview

This module provides manager components that sit above the core storage, services, and wallet layers. These managers handle cross-cutting concerns like multi-storage synchronization, two-factor authentication, multi-profile support, settings persistence, permission enforcement, WAB authentication, and operation logging. All managers are designed for 1:1 parity with the TypeScript `@bsv/wallet-toolbox`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Managers                                │
├─────────────────────────────────────────────────────────────────┤
│  WalletStorageManager          - Multi-storage sync, active/backup│
│  WalletSettingsManager         - Settings persistence            │
│  SimpleWalletManager           - Primary key + PKM authentication│
│  CWIStyleWalletManager         - Multi-profile, password-based   │
│  WalletAuthenticationManager   - WAB authentication integration  │
│  WalletPermissionsManager      - BRC-98/99 permission enforcement│
│  WalletLogger                  - Operation logging               │
├─────────────────────────────────────────────────────────────────┤
│                      Wallet + Storage + Services                │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 319 | Module declarations, re-exports, `WalletLogger`, `SetupWalletOptions`, `setup_wallet()` |
| `storage_manager.rs` | 1392 | Multi-storage orchestration with active/backup semantics, lock queues, MonitorStorage impl with task locking, sync-safe cached settings/services |
| `cwi_style_wallet_manager.rs` | 800 | CWI-compatible multi-profile manager: the v3 key file, the v1 read and migration, JSON import/export (tests in `cwi_style_wallet_manager_tests.rs`, the KDF bound's in `cwi_kdf_permits_tests.rs`) |
| `ump_token.rs` | 656 | The reference's UMP token and snapshot layouts, the key-file records (tests in `ump_token_tests.rs`) |
| `ump_kdf.rs` | 284 | The token's password KDF, a child module of `ump_token.rs`: `PasswordKdf`, `KdfAlgorithm`, pbkdf2-sha512 and argon2id (#593), `kdf_permits` and `off_worker` (the blocking pool under the `with_kdf_in_flight` permits, #624, #631) |
| `permissions_manager.rs` | 1978 | BRC-98/99 permission enforcement with DPACP/DBAP/DCAP/DSAP, in-memory cache (5-min TTL), permission request handler |
| `settings_manager.rs` | 354 | Persistent wallet settings with mainnet/testnet defaults and string serialization |
| `simple_wallet_manager.rs` | 336 | Two-factor authentication manager (primary key + privileged key) |
| `auth_manager.rs` | 58 | WAB (Wallet Authentication Backend) integration wrapper |

## Key Exports

### Storage Management

```rust
pub use storage_manager::{ManagedStorage, WalletStorageManager};
```

- `WalletStorageManager` - Orchestrates multiple `WalletStorageProvider` instances with active/backup partitioning
- `ManagedStorage` - Wrapper around a storage provider with cached state (settings, user, availability)

### Wallet Managers

```rust
pub use simple_wallet_manager::SimpleWalletManager;
pub use cwi_style_wallet_manager::{
    CWIStyleWalletManager, CWIStyleWalletManagerConfig, Profile, DEFAULT_KDF_IN_FLIGHT,
};
pub use ump_token::{KdfAlgorithm, PasswordKdf, UmpToken, WalletSnapshot};
pub use auth_manager::WalletAuthenticationManager;
```

- `SimpleWalletManager` - Two-factor authentication: primary key (32 bytes) + privileged key manager
- `CWIStyleWalletManager` - Multi-profile wallet with password-based key derivation (argon2id tokens, pbkdf2 read, v1 PBKDF2)
- `WalletAuthenticationManager` - WAB integration wrapper over `CWIStyleWalletManager`
- `Profile` - Profile data with name, ID, primary pad, privileged pad, and timestamps
- `UmpToken` - the reference's User Management Protocol token, binary layout (#560)
- `PasswordKdf` / `KdfAlgorithm` - the token's `passwordKdf` metadata
- `WalletSnapshot` - the reference's state snapshot layout (version 2)

### Settings Management

```rust
pub use settings_manager::{
    Certifier, TrustSettings, WalletSettings, WalletSettingsManager,
    WalletSettingsManagerConfig, WalletTheme, DEFAULT_SETTINGS, TESTNET_DEFAULT_SETTINGS,
};
```

- `WalletSettingsManager` - In-memory settings storage with JSON serialization
- `WalletSettings` - Trust settings, theme, currency, and permission mode
- `Certifier` - Trusted identity certifier with name, description, public key, and trust level
- `DEFAULT_SETTINGS` / `TESTNET_DEFAULT_SETTINGS` - Pre-configured default settings

### Permissions Management

```rust
pub use permissions_manager::{
    BasketUsageType, CertificateUsageType, GroupedPermissions, PermissionRequest,
    PermissionRequestHandler, PermissionToken, PermissionUsageType, PermissionsModule,
    WalletPermissionsManager, WalletPermissionsManagerConfig,
};
```

- `WalletPermissionsManager` - BRC-98/99 enforcement with `ensure_protocol_permission()`, `ensure_basket_access()`, `ensure_certificate_access()`, `ensure_spending_permission()`, plus legacy `check_permission()` and `check_permission_with_token()`
- `PermissionRequestHandler` - Async callback for user consent flows (returns `PermissionToken`)
- `PermissionUsageType` - Protocol usage categories: `Signing`, `Encrypting`, `Hmac`, `PublicKey`, `IdentityKey`, `LinkageRevelation`, `Generic`
- `BasketUsageType` - Basket operation categories: `Insertion`, `Removal`, `Listing`
- `CertificateUsageType` - Certificate operation categories: `Disclosure`
- `GroupedPermissions` - BRC-73 grouped permissions (spending, protocol, basket, certificate)
- `PermissionRequest` - Permission request from an application (includes `operation` field)
- `PermissionToken` - On-chain permission token (BRC-98/99)
- `PermissionsModule` - Trait for request/response transformation by scheme; read by the seven key calls (#726, below)

### Logging

```rust
// Defined directly in mod.rs
pub struct WalletLogger { indent, logs, is_origin, is_error }
pub struct WalletLogEntry { timestamp, level, message, indent }
```

- `WalletLogger` - Structured operation logger with group nesting, JSON serialization
- Methods: `new()`, `group(name)`, `group_end()`, `log(level, msg)`, `error(msg)`, `to_log_string()`, `to_json()`

### Setup Helpers

```rust
pub struct SetupWalletOptions { root_key, storage_path, chain }
pub async fn setup_wallet(options: SetupWalletOptions) -> Result<()>  // Stub
```

## WalletStorageManager Details

The storage manager handles multi-storage synchronization with sophisticated concurrency control:

### Lock Hierarchy

```rust
// Four lock queues with increasing exclusivity
reader_locks   // Multiple readers allowed
writer_locks   // Exclusive with readers
sync_locks     // Exclusive with readers + writers
provider_locks // Highest precedence (currently unused)
```

Lock timeout: 30 seconds (`LOCK_TIMEOUT_SECS`). Returns `Error::LockTimeout` on expiration.

### Storage Partitioning

Stores are partitioned into three categories based on user's `activeStorage` setting:
- **Active**: The primary storage for all write operations
- **Backups**: Stores that agree on which storage should be active
- **Conflicting**: Stores that disagree (require manual resolution)

### Key Methods

```rust
// Initialization
make_available() -> Result<TableSettings>  // Initialize all stores, partition, return active settings
get_auth(must_be_active: bool) -> Result<AuthId>  // Get auth ID, optionally require active

// Locking wrappers
run_as_reader(f) -> Result<R>   // Execute with reader lock
run_as_writer(f) -> Result<R>   // Execute with writer lock
run_as_sync(f) -> Result<R>     // Execute with sync lock

// Synchronization
sync_from_reader(identity_key, reader) -> Result<SyncResult>
sync_to_writer(identity_key, writer) -> Result<SyncResult>
update_backups() -> Result<String>  // Sync active to all backups
set_active(storage_identity_key) -> Result<String>  // Switch active storage

// Store management
add_wallet_storage_provider(provider) -> Result<()>
get_stores() -> Vec<WalletStorageInfo>
get_active_store() -> Result<String>
get_backup_stores() -> Vec<String>
get_conflicting_stores() -> Vec<String>
set_services(services) / get_services() -> Result<Arc<dyn WalletServices>>
```

### SyncResult

```rust
SyncResult { inserts: u32, updates: u32, log: String }
```

Returned by `sync_from_reader` and `sync_to_writer`. Contains counts and a human-readable log of sync operations performed.

### Sync-Safe Cached State

The manager maintains two `std::sync::RwLock` caches for synchronous trait methods:
- `cached_settings` - Populated by `make_available()`, used by `WalletStorageReader::get_settings()` (returns an owned `TableSettings`)
- `services_sync` - Populated by `set_services()`, used by `WalletStorageReader::get_services()` (returns `Result<Arc<dyn WalletServices>>`)

These use `std::sync::RwLock` (not `tokio::sync::RwLock`) because the trait methods are synchronous. Before `make_available()` is called, `get_settings()` returns `TableSettings::default()`.

### Trait Implementations

`WalletStorageManager` implements the full storage trait hierarchy:
- `WalletStorageReader` - Delegates reads to active storage with reader lock; `get_settings()` and `get_services()` use sync caches
- `WalletStorageWriter` - Delegates writes to active storage with writer lock (includes `review_status`, `purge_data`, `update_transaction_status_after_broadcast`, `begin_transaction`, `commit_transaction`, `rollback_transaction`)
- `WalletStorageSync` - Delegates sync operations with sync lock
- `WalletStorageProvider` - Partial implementation (`storage_identity_key()` and `storage_name()` panic with "use async method" message)
- `MonitorStorage` - Delegates monitor operations (`synchronize_transaction_statuses`, `send_waiting_transactions`, `abort_abandoned`, `un_fail`, `review_status`, `purge_data`, `try_acquire_task_lock`, `release_task_lock`) with writer lock

## SimpleWalletManager Authentication Flow

```
1. Create manager with admin_originator and wallet_builder
2. provide_primary_key(key: Vec<u8>)     // 32 bytes required
3. provide_privileged_key_manager(pkm)   // PrivateKey from bsv-sdk
4. -> try_build_underlying() auto-called  // Builds wallet when both present
5. wallet() -> Arc<dyn WalletInterface>  // Access authenticated wallet
```

### Snapshot Persistence

The manager supports encrypted state snapshots:
```rust
save_snapshot() -> Result<Vec<u8>>   // Encrypts primary key with random key
load_snapshot(data) -> Result<()>    // Restores primary key, still needs PKM
```

Snapshot format: `[32-byte key][encrypted payload]` where payload is `[1-byte version][1-byte length][primary key]`

## CWIStyleWalletManager Features

### Password Key Derivation

A token's KDF is read from its metadata (`ump_kdf.rs`): argon2id (RustCrypto `argon2` =0.5.3, version 0x13, the token's iterations, memoryKiB, parallelism, hashLength) or `ring::pbkdf2` with SHA-512 at the token's iterations (7777, the reference's `PBKDF2_NUM_ROUNDS`, for a token with no metadata). New tokens are argon2id at the reference's defaults, 7 / 131072 / 1 / 32 (#593); `config.password_rounds` is the v1 records' PBKDF2 alone. A KDF outside `isValidKdfConfig` is refused with the reference's text. Derived key material is wrapped in `zeroize::Zeroizing<Vec<u8>>` to ensure automatic zeroing on drop, preventing sensitive key material from lingering in memory:
```rust
// v3 (#560, #593): password_key = KDF(password, token.password_salt, token.password_kdf), argon2id for new tokens
// Salt = the token's passwordSalt (32 bytes from rng::os_fill), never the profile id
// root_primary = open(token.password_presentation_primary, presentation_key XOR password_key)
// profile key  = root_primary XOR primary_pad (the pad read from the token's profilesEncrypted)
// v1 (read, kept beside its token until an unlock opens the token): primary_key = v1 pad XOR PBKDF2(password, salt = profile.id)
```

### Profile Management

```rust
create_profile(name, password) -> Result<Profile>  // Generate random primary key
switch_profile(profile_id, password) -> Result<()> // Authenticate and build wallet
delete_profile(profile_id) -> Result<()>           // Remove (can't delete active)
get_profiles() -> Vec<Profile>                     // List all profiles
get_active_profile_id() -> Option<Vec<u8>>
get_default_profile_id() -> Option<Vec<u8>>
set_default_profile_id(profile_id) -> Result<()>
export_profile(profile_id) -> Result<Vec<u8>>      // Encrypt profile data
import_profile(data, password) -> Result<Profile>  // Decrypt and store
export_profile_json(profile_id) -> Result<Vec<u8>> // Unencrypted JSON export
import_profile_json(data) -> Result<Profile>       // Unencrypted JSON import
backup_all_profiles() -> Result<Vec<u8>>           // JSON array of all profiles
restore_all_profiles(data) -> Result<Vec<Profile>> // Restore from JSON array
destroy()                                          // Return to unauthenticated
```

### Profile Structure

```rust
Profile {
    name: String,
    id: Vec<u8>,              // 16 bytes, base64-encoded in JSON
    primary_pad: Vec<u8>,     // 32 bytes, XOR pad for key derivation
    privileged_pad: Vec<u8>,  // 32 bytes, for two-factor auth
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}
```

The `privileged_pad` field uses `serde(default)` for backward compatibility with older profiles that lack it.

### UMP Token & Wallet Snapshot (`ump_token.rs`)

```rust
UmpToken { password_salt, password_presentation_primary, password_recovery_primary,
           presentation_recovery_primary, password_primary_privileged,
           presentation_recovery_privileged, presentation_hash, recovery_hash,
           presentation_key_encrypted, password_key_encrypted, recovery_key_encrypted,
           profiles_encrypted, ump_version, password_kdf, current_outpoint }
UmpToken::to_bytes() / from_bytes()   // the reference's serializeUMPToken layout (v1-v3 read)
PasswordKdf { algorithm: KdfAlgorithm, iterations, memory_kib, parallelism, hash_length }
WalletSnapshot { version, snapshot_key, active_profile_id (16 bytes), encrypted_payload }  // the reference's saveSnapshot layout
```

The reference's `UMPToken` (ts-stack edf6e03 `CWIStyleWalletManager.ts:415-498`) field for field, in its binary layout; the design is `docs/design/ump-token-format.md`. Each profile of the key file is one reference token (the toolbox's profiles carry their own passwords); the presentation key is held in the file beside it, the recovery key is `recovery_key(id, password)`. `PasswordKdf::pbkdf2(n)` carries the reference's full params (`memoryKiB` 131072, `parallelism` 1, `hashLength` 32), so the params JSON is the reference's byte for byte; `PasswordKdf::argon2id()` (crate-private) is the reference's default for new tokens, its params JSON likewise. The host's privileged key manager still builds the wallet; the token's root privileged key wraps its factor copies. argon2id derives at the token's parameters (#593); the vectors both ways are `test_vectors/keys/ump-token-*-argon2id*.json`.

### Key File

```rust
save_snapshot() -> Result<Vec<u8>>   // the v3 key file: {"keyFileVersion":3, "defaultProfileId", "profiles":[{...profile, "ump":{presentationKey, token}, "v1"?:{primaryPad}}]}
load_snapshot(data) -> Result<()>    // v3, or v1 (32-byte key | GCM(JSON {profiles, default_profile_id}))
open_key_file(path) -> Result<()>    // zero and remove a stale `<name>.tmp-<16 hex>`, read (absent = empty), remember the path
save_key_file() -> Result<()>        // sibling `<name>.tmp-<16 hex>`, sync, rename, sync the directory (0600 on unix)
recovery_key(id, password) -> Result<Zeroizing<Vec<u8>>>  // the reference's getRecoveryKey (:2356)
```

The migration (#594 (a), design doc 4.3): a v1 record opens with the profile-id salt; once its wallet is built a token joins it (a fresh root, the pad moved to `key XOR root`, a fresh salt) and the v1 record stays (`"v1"`), written with a key file open. A later unlock tries the token before v1: if it opens (its GCM tag the proof of the same password), `"v1"` leaves and the file is written; if it refuses, v1 opens and the token is re-sealed under the arriving password and written beside the unchanged v1 record (#594 (ii)). Only a token that opens drops v1, so the same wrong password at two consecutive unlocks drops it (the loss condition); a host's builder, which runs before the write, closes it by checking the derived key and returning an error. The commit refuses when the profile changed during the KDF. The KDF, and what its key opens or seals, runs on `tokio::task::spawn_blocking` (`off_worker`, over a `Zeroizing` copy of the password, a `JoinError` an `Error::Internal`) in `create_profile`, `switch_profile` and `recovery_key`, so argon2id's 128 MiB and ~330 ms never hold an async worker (#624); the unlock still derives, builds, then writes. The derivations at once are bounded by a `tokio::sync::Semaphore` (#631): `new` starts at `DEFAULT_KDF_IN_FLIGHT` (4, 512 MiB at argon2id's 128 MiB per derivation), and the consuming builder `with_kdf_in_flight(permits) -> Result<Self>`, run once before use, replaces it with `permits`; it refuses 0 (and a count past `Semaphore::MAX_PERMITS`) with `Error::InvalidArgument` (`with_kdf_in_flight(0): kdf_in_flight must be in 1..=...`) before any derivation. The permit is taken in order before `spawn_blocking` and held in the blocking task until the key is back; the witnesses are `cwi_kdf_permits_tests.rs`. A failed unlock writes nothing. The file is one factor (the presentation key sits beside the token): an offline guess is one derivation of the token's KDF against field 1's tag (argon2id at 128 MiB and 7 passes for a token written since #593). Key material (factors, presentation key, v1 pad, `RefProfile` pads, the file's bytes) is `Zeroizing` or zeroed on drop. A token not on chain is written with `UNPUBLISHED_OUTPOINT` (the null txid, `.0`), which the reference's reader takes. An older toolbox cannot read a v3 file. Exports (`export_profile*`, `backup_all_profiles`) carry the `ump` record beside the profile's fields.

## WalletAuthenticationManager

Thin wrapper over `CWIStyleWalletManager` for WAB (Wallet Authentication Backend) integration:

```rust
WalletAuthenticationManager::new(admin_originator, wallet_builder, config)
manager.inner() -> &CWIStyleWalletManager      // Access inner manager
manager.inner_mut() -> &mut CWIStyleWalletManager
```

## WalletSettingsManager

### Default Settings

Includes pre-configured trusted certifiers:
- **Metanet Trust Services** - Registry of protocols, baskets, certificate types (trust: 4)
- **SocialCert** - Certifies social media handles, phones and emails (trust: 3)

### Settings Structure

```rust
WalletSettings {
    trust_settings: TrustSettings {
        trust_level: u32,           // 1-2560, minimum combined trust per subject
        trusted_certifiers: Vec<Certifier>,
    },
    theme: Option<WalletTheme>,     // "dark" or "light"
    currency: Option<String>,       // "USD", "EUR", etc.
    permission_mode: Option<String>, // "simple", etc.
}
```

The manager is in memory. A wallet reads an optional shared manager through
`WalletOptions.settings_manager` on every discovery call. Without one, Main
uses `DEFAULT_SETTINGS`; Test and Regtest use `TESTNET_DEFAULT_SETTINGS`.
The TS name mapping applies only to SocialCert among the defaults and leaves
its key unchanged at the pin. Settings are not stored in the wallet's basket;
the decided `over_store(...)` constructor follows bsv-rs #441.

```rust
new(config: Option<WalletSettingsManagerConfig>) -> Result<WalletSettingsManager>
get() -> Result<WalletSettings>
set(settings: WalletSettings) -> Result<()>
```

`new` and `set` validate the trust settings. Thresholds are 1 to 2560; each
certifier's `trust: u32` is 1 to 10. At most 256 distinct canonical lowercase
compressed public keys are allowed. Names use 1 to 100 UTF-8 bytes and
descriptions 1 to 500. Optional URLs use 1 to 500 bytes and must be absolute,
credential-free HTTP(S) URLs. Failures return `Error::ValidationError` with
the TypeScript text and preserve the previous settings. Both JSON load
methods use the same validation; `get` returns an `Ok` snapshot.

### Persistence Methods

```rust
save() -> Result<Vec<u8>>              // Serialize to JSON bytes
load(data: &[u8]) -> Result<()>        // Deserialize from JSON bytes
save_to_string() -> Result<String>     // Serialize to JSON string
load_from_string(json: &str) -> Result<()>  // Deserialize from JSON string
reset()                                // Reset to default settings
```

## WalletPermissionsManager

Full BRC-98/99 permission enforcement with four categories, in-memory caching, and pluggable consent flows.

A grant the port makes lives only in the five-minute cache; the reference keeps it as a PushDrop token in an admin basket. The two shapes for a stored grant (#751), for the owner's word: `docs/design/permission-token-store.md`. Step 1 of that note is in: the manager reads the tokens a wallet already holds (`find_stored_token`, crate-private, the reference's `find*Token` at `WalletPermissionsManager.ts:3080-3239`) after the cache and before the seek-false refusal and the prompt, for the protocol, basket and certificate kinds: `list_outputs` on `admin protocol-permission`, `admin basket-access` or `admin certificate-access` with the reference's tags, each trimmed and lowercased as its `validateTag` stores and queries them (`originator <o>` for the trimmed and normalized originator, then untagged), entire transactions, as the admin originator; the source checked (one satoshi, a canonical lock-before PushDrop under the wallet's `[2, 'admin permission token encryption']` key `"1"` for `self`, its signature over the fields); each field decrypted under that protocol; the domain normalized against the request, then the kind's fields and the expiry (0 never). A live token admits and is cached; an expired protocol token refuses a seek-false call with the reference's `:1549` text; otherwise the path is as before. The manager's `underlying` must be a full wallet for the read: a `ProtoWallet`'s `list_outputs` refusal answers no token (the reference throws), so only a host over a full wallet sees the change (no in-tree wiring and the CLI at beta construct the manager over one), and every other host pays one refused list per uncached call. Any other list fault is returned as the wallet's own (`Error::SdkError`, its class kept), never the `:1533` absent text and never a prompt; a field that does not decrypt is read raw, as the reference's `decryptPermissionTokenField` does. Reads per uncached call: two `list_outputs` for an absent token, plus, on the untagged pass, every other originator's token for the same protocol, basket or certificate, each a PushDrop decode, one signature verify and one `decrypt` (the absent result is not cached); one `list_outputs`, one `get_public_key` (once per manager) and six `decrypt` (three for a basket) for a live one. Nothing mints, renews or revokes, and spending reads the cache alone (step 2). The witnesses are `permission_token_lookup_tests.rs`.

### Permission Categories

| Category | Abbrev | Method | Protects |
|----------|--------|--------|----------|
| Protocol Access | DPACP | `ensure_protocol_permission()` | Protocol usage at security levels |
| Basket Access | DBAP | `ensure_basket_access()` | Output basket operations |
| Certificate Access | DCAP | `ensure_certificate_access()` | Certificate field disclosure |
| Spending Auth | DSAP | `ensure_spending_permission()` | Spending operations |

### Enforcement Flow

Each `ensure_*` method follows the same pattern:
1. **Admin bypass** - Admin originator always allowed
2. **Level/reserved check** - Security level 0 bypasses protocol checks; admin-reserved names blocked; a level-1 protocol's counterparty is dropped (the reference's `WalletPermissionsManager.ts:1505`), so its grant and prompt name none (#581)
3. **Config flag bypass** - Per-usage-type config flags can disable specific checks; for a protocol, an everyday call (privileged reads as everyday without `differentiate_privileged_operations`) whose counterparty is whitelisted for the protocol's name is admitted with no prompt (the reference's `WalletPermissionsManager.ts:1516`, #725; level 1 names no counterparty, so it never matches)
4. **Cache check** - In-memory cache with 5-minute TTL (`CACHE_TTL_SECS`), then the stored token in the kind's admin basket (protocol, basket, certificate; #751 step 1)
5. **Permission request flow** - Invokes `PermissionRequestHandler` callback for user consent
6. **Cache result** - Granted permissions are cached for subsequent calls

### Permission Modules (#726)

A key call (`get_public_key` with a protocol id, `encrypt`, `decrypt`, `create_hmac`, `verify_hmac`, `create_signature`, `verify_signature`) whose protocol name starts `p ` is delegated before any step of the enforcement flow, admin included (`WalletPermissionsManager.ts:843-885`, `:5142-5385`). The crate-private `delegate_key_call` takes the scheme as the name's second word (`module_scheme`), refuses `nosend` with `:800`'s text without reading the map (the reference's built-in module overwrites a host's), refuses an unregistered scheme with `Unsupported P-module scheme: p <scheme>`, then runs `on_request` (its returned `Value` is the arguments object itself, not `{ args }`), the key route on the parsed arguments, and `on_response`. Every refusal is `WalletRefusal { class: InvalidParameter }` with the text alone. The JSON mappings to the reference's objects are `ModuleCall`, one `module_call!` row per call (`protocolID` `[level, name]`, counterparty `self`/`anyone`/hex, bytes as number arrays, absent options omitted). The linkage reveals do not delegate; `handleRequest` is not ported (the owner's word).

The five basket and label calls delegate too (#757). `list_outputs` and `relinquish_output` run `delegate_key_call` on the request's basket (`:5080-5140`), the basket check skipped. `create_action`, `list_actions` and `internalize_action` take `module_chain(baskets, labels, nosend)`: one module per scheme in the order met, the outputs' or insertions' baskets then the labels (`:4436-4451`, `:4970-4989`), a label's scheme by `splitLabelsByPermissionModule`'s grammar (`label_scheme`, `Invalid P-label format: <label>`, `:920-958`), an unregistered scheme `Unsupported P-basket scheme: p <scheme>` or `Unsupported P-label scheme: p <scheme>` (`:890-901`); `chain_request` runs `on_request` in order and `chain_response` `on_response` in reverse (`:4500-4520`). `NoSend` says what a chain does with `nosend`, whose host entry is never read: `Refuse(NOSEND_KEY_CALL)` on internalize, `Refuse(BRC177_UNSUPPORTED)` on create (the built-in module is not ported), `Skip` on list_actions (its transforms are identity). The five types' mappings are `module_call_serde!` rows over bsv-rs's camelCase serde.

### Whitelisted Counterparties

`WalletPermissionsManager::new(...).with_whitelisted_counterparties(map)` takes a `HashMap<String, Vec<String>>`, a counterparty identity key's hex to protocol names, the reference's `whitelistedCounterparties` (`WalletPermissionsManager.ts:581-586`, #725). It is a builder method returning `Self`, not a field on `WalletPermissionsManagerConfig`: the config is a constructible public struct, and a field added to it is a MAJOR change to `cargo semver-checks` (#631's `with_kdf_in_flight` shape). The key matches as passed, lowercased or uppercased; `self`, `anyone` and none never match; the protocol name compares case-blind (`:5731-5742`). Empty by default.

### Permission Request Handler

A key call's protocol prompt (`PermissionRequest`, `request_type: Protocol`) carries the request's `counterparty` (`self`, `anyone` or the key's hex; `self` when absent; none at level 1) and its `privilegedReason` as `reason` (#581). The wallet's seven key calls pass them as the reference's `requestArgs.counterparty || 'self'` and `requestArgs.privilegedReason` (`WalletPermissionsManager.ts:5165-5384`); before #581 both were `None`.

```rust
pub type PermissionRequestHandler = Arc<
    dyn Fn(PermissionRequest) -> Pin<Box<dyn Future<Output = Result<PermissionToken>> + Send>>
        + Send + Sync,
>;

manager.set_permission_request_handler(handler).await;
manager.clear_permission_request_handler().await;
```

### Admin-Reserved Names

```rust
WalletPermissionsManager::is_admin_protocol(protocol)  // name starts with "admin"
WalletPermissionsManager::is_admin_basket(basket)       // "default" or starts with "admin"
```

### Cache Operations

```rust
build_cache_key(type, originator, privileged, protocol, counterparty, basket, cert, satoshis) -> String
is_permission_cached(key) -> bool          // Checks TTL + token expiry
cache_permission(key, expiry)              // Store with expiry timestamp
purge_expired_tokens()                     // Remove expired entries
```

Cache key formats: `proto:originator:privileged:level,name:counterparty` (`undefined` at level 1), `basket:originator:name`, `cert:originator:privileged:verifier:type:field1|field2`, `spend:originator:amount`

### Legacy Operation-Level Checking

`check_permission(request)` evaluates operation-level flags when `enforce_permissions` is true:

```rust
// Per-operation flags in WalletPermissionsManagerConfig:
allow_create_action, allow_sign_action, allow_abort_action, allow_list_actions,
allow_internalize_action, allow_list_outputs, allow_relinquish_output,
allow_acquire_certificate, allow_list_certificates, allow_prove_certificate,
allow_relinquish_certificate, allow_discover, allow_crypto
```

### Token Verification

```rust
verify_token(token) -> Result<()>  // Validates txid, output_script non-empty, expiry not past
check_permission_with_token(request, token) -> bool  // Verifies token + matches permission type
```

### Configuration Options

`WalletPermissionsManagerConfig` has boolean flags for each permission category:
- `seek_protocol_permissions_for_signing/encrypting/hmac`
- `seek_permissions_for_key_linkage/public_key/identity_key_revelation`
- `seek_permissions_for_identity_resolution`
- `seek_basket_insertion/removal/listing_permissions`
- `seek_permission_when_applying_action_labels/listing_actions_by_label`
- `seek_certificate_disclosure/acquisition/relinquishment/listing_permissions`
- `seek_spending_permissions`, `seek_grouped_permission`
- `differentiate_privileged_operations`, `encrypt_wallet_metadata`
- `enforce_permissions` + per-operation `allow_*` flags

Helper constructors:
```rust
WalletPermissionsManagerConfig::all_enabled()   // Most secure (all flags true)
WalletPermissionsManagerConfig::all_disabled()   // Permissive (default)
```

### Additional Permission Types

```rust
PermissionType { Protocol, Basket, Certificate, Spending }
PermissionUsageType { Signing, Encrypting, Hmac, PublicKey, IdentityKey, LinkageRevelation, Generic }
BasketUsageType { Insertion, Removal, Listing }
CertificateUsageType { Disclosure }
SpendingAuthorization { amount: u64, description }
ProtocolPermission { protocol_id, counterparty, description }
BasketAccess { basket, description }
CertificateAccess { cert_type, fields, verifier_public_key, description }
CertificatePermissionDetails { verifier, cert_type, fields }
SpendingPermissionDetails { satoshis, line_items }
SpendingLineItem { item_type, description, satoshis }
```

## Usage

### Multi-Storage with Active/Backup

```rust
use bsv_wallet_toolbox::managers::WalletStorageManager;

let manager = WalletStorageManager::new(
    identity_key.to_string(),
    Some(active_storage),
    Some(vec![backup_storage]),
);

let settings = manager.make_available().await?;

let result = manager.run_as_writer(|writer| async {
    writer.create_action(&auth, args).await
}).await?;

let log = manager.update_backups().await?;
```

### Two-Factor Authentication

```rust
use bsv_wallet_toolbox::managers::SimpleWalletManager;

let manager = SimpleWalletManager::new(
    "wallet.admin".to_string(),
    wallet_builder,
    None, // Optional state snapshot
);

manager.provide_primary_key(primary_key_bytes).await?;
manager.provide_privileged_key_manager(pkm_private_key).await?;

let wallet = manager.wallet().await?;
let result = wallet.create_action(args, "app.example.com").await?;
```

### Multi-Profile Wallet

```rust
use bsv_wallet_toolbox::managers::{CWIStyleWalletManager, CWIStyleWalletManagerConfig};

let manager = CWIStyleWalletManager::new(
    "admin.wallet".to_string(),
    wallet_builder,
    CWIStyleWalletManagerConfig::default(),
)
.with_kdf_in_flight(2)?; // optional: DEFAULT_KDF_IN_FLIGHT (4) otherwise

manager.provide_privileged_key_manager(pkm_key).await;

let profile = manager.create_profile("Work", "password123").await?;
manager.switch_profile(&profile.id, "password123").await?;

let wallet = manager.wallet().await?;
```

### Settings Management

```rust
use bsv_wallet_toolbox::managers::{WalletSettingsManager, WalletSettings};

let manager = WalletSettingsManager::new(None)?; // Uses DEFAULT_SETTINGS

let settings = manager.get().await?;

let mut new_settings = settings;
new_settings.currency = Some("EUR".to_string());
manager.set(new_settings).await?;

let bytes = manager.save().await?;
manager.load(&bytes).await?;
```

### Permission Enforcement

```rust
use bsv_wallet_toolbox::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig, PermissionUsageType};

let manager = WalletPermissionsManager::new(
    underlying_wallet,
    "admin.wallet".to_string(),
    WalletPermissionsManagerConfig::all_enabled(),
);

// Set up consent handler
manager.set_permission_request_handler(handler).await;

// Check protocol permission (admin bypasses, SecurityLevel::Silent bypasses)
let allowed = manager.ensure_protocol_permission(
    "app.example.com", false, &protocol, None, None, PermissionUsageType::Signing,
).await?;

// Check basket access (admin-reserved baskets like "default" are blocked)
let allowed = manager.ensure_basket_access(
    "app.example.com", "my_basket", None, BasketUsageType::Listing,
).await?;

// Check spending authorization
let allowed = manager.ensure_spending_permission(
    "app.example.com", 50000, Some("Payment for service"),
).await?;
```

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent module with crate overview
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage traits and entities used by managers
- [../wallet/CLAUDE.md](../wallet/CLAUDE.md) - Wallet implementation that managers wrap

## Development Notes

### TypeScript Parity

These managers match the TypeScript `@bsv/wallet-toolbox` implementations:
- `WalletStorageManager` - Storage synchronization logic
- `SimpleWalletManager` - Two-factor authentication pattern
- `CWIStyleWalletManager` - CWI multi-profile pattern, the reference's UMP token and KDFs
- `WalletSettingsManager` - Settings persistence pattern
- `WalletLogger` - Operation logging interface
- `WalletAuthenticationManager` - WAB authentication integration
- `WalletPermissionsManager` - BRC-98/99 permission enforcement

### Stub / Skeleton Implementations

- `WalletAuthenticationManager` - Thin wrapper; WAB protocol flow not yet implemented
- `setup_wallet()` - Stub that logs setup intent only

### Concurrency Model

All managers use `tokio::sync::RwLock` for async-safe interior mutability. The storage manager's lock queue system prevents deadlocks through ordered acquisition (reader -> writer -> sync). Lock timeout is 30 seconds.

### Encryption

Managers use `bsv_sdk::primitives::SymmetricKey` for AES encryption of snapshots and exports. Keys are randomly generated per operation. Every random byte the managers draw (the CWI token's 32-byte password salt and keys, the profile id, the pads, the snapshot keys) comes from the OS CSPRNG through `crate::rng::os_fill`, never a seed (#191). The `CWIStyleWalletManager` additionally uses `zeroize::Zeroizing` for derived key material memory safety.
