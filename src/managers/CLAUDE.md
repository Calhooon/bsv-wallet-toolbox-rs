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
| `mod.rs` | 317 | Module declarations, re-exports, `WalletLogger`, `SetupWalletOptions`, `setup_wallet()` |
| `storage_manager.rs` | 1392 | Multi-storage orchestration with active/backup semantics, lock queues, MonitorStorage impl with task locking, sync-safe cached settings/services |
| `cwi_style_wallet_manager.rs` | 765 | CWI-compatible multi-profile manager with PBKDF2 password derivation, UMP tokens, snapshots, JSON import/export |
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
    CWIStyleWalletManager, CWIStyleWalletManagerConfig, Profile,
    UmpToken, WalletSnapshot,
};
pub use auth_manager::WalletAuthenticationManager;
```

- `SimpleWalletManager` - Two-factor authentication: primary key (32 bytes) + privileged key manager
- `CWIStyleWalletManager` - Multi-profile wallet with PBKDF2 password-based key derivation
- `WalletAuthenticationManager` - WAB integration wrapper over `CWIStyleWalletManager`
- `Profile` - Profile data with name, ID, primary pad, privileged pad, and timestamps
- `UmpToken` - Universal Message Protocol token for cross-device wallet transfer
- `WalletSnapshot` - Encrypted wallet state for persistence and recovery (version 2)

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
- `PermissionsModule` - Trait for request/response transformation by scheme

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
- `cached_settings` - Populated by `make_available()`, used by `WalletStorageReader::get_settings()` (returns `&TableSettings`)
- `services_sync` - Populated by `set_services()`, used by `WalletStorageReader::get_services()` (returns `Result<Arc<dyn WalletServices>>`)

These use `std::sync::RwLock` (not `tokio::sync::RwLock`) because the trait methods are synchronous. Before `make_available()` is called, `get_settings()` returns a default `TableSettings` via `OnceLock` fallback.

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

### PBKDF2 Key Derivation

Uses `ring::pbkdf2` with SHA-512 and configurable rounds (default: 7777). Derived key material is wrapped in `zeroize::Zeroizing<Vec<u8>>` to ensure automatic zeroing on drop, preventing sensitive key material from lingering in memory:
```rust
// Password -> derived_key (32 bytes, Zeroizing<Vec<u8>>)
// primary_key = primary_pad XOR derived_key
// Salt = profile.id (16 bytes)
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

### UMP Token & Wallet Snapshot

```rust
UmpToken { version: u32, key_encrypted, profiles_encrypted }  // Cross-device transfer
WalletSnapshot { version: u8, snapshot_key, active_profile_id, encrypted_payload }  // V2 persistence
```

### Snapshot System

```rust
save_snapshot() -> Result<Vec<u8>>   // Encrypt all profiles + default_profile_id
load_snapshot(data) -> Result<()>    // Restore profile collection
```

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
- **Metanet Trust Services** - Registry for protocols, baskets, certificate types (trust: 4)
- **SocialCert** - Certifies social media handles, phone numbers, emails (trust: 3)

### Settings Structure

```rust
WalletSettings {
    trust_settings: TrustSettings {
        trust_level: u32,           // 1-4, higher = more trusted
        trusted_certifiers: Vec<Certifier>,
    },
    theme: Option<WalletTheme>,     // "dark" or "light"
    currency: Option<String>,       // "USD", "EUR", etc.
    permission_mode: Option<String>, // "simple", etc.
}
```

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
2. **Level/reserved check** - Security level 0 bypasses protocol checks; admin-reserved names blocked
3. **Config flag bypass** - Per-usage-type config flags can disable specific checks
4. **Cache check** - In-memory cache with 5-minute TTL (`CACHE_TTL_SECS`)
5. **Permission request flow** - Invokes `PermissionRequestHandler` callback for user consent
6. **Cache result** - Granted permissions are cached for subsequent calls

### Permission Request Handler

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

Cache key formats: `proto:originator:privileged:level,name:counterparty`, `basket:originator:name`, `cert:originator:privileged:verifier:type:field1|field2`, `spend:originator:amount`

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
);

manager.provide_privileged_key_manager(pkm_key).await;

let profile = manager.create_profile("Work", "password123").await?;
manager.switch_profile(&profile.id, "password123").await?;

let wallet = manager.wallet().await?;
```

### Settings Management

```rust
use bsv_wallet_toolbox::managers::{WalletSettingsManager, WalletSettings};

let manager = WalletSettingsManager::new(None); // Uses DEFAULT_SETTINGS

let settings = manager.get().await;

let mut new_settings = settings;
new_settings.currency = Some("EUR".to_string());
manager.set(new_settings).await;

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
- `CWIStyleWalletManager` - CWI multi-profile pattern with PBKDF2
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

Managers use `bsv_sdk::primitives::SymmetricKey` for AES encryption of snapshots and exports. Keys are randomly generated per operation. The `CWIStyleWalletManager` additionally uses `zeroize::Zeroizing` for derived key material memory safety.
