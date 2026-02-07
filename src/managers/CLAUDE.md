# src/managers/ - Wallet Manager Components
> Higher-level wallet management abstractions for authentication, settings, storage sync, and permissions

## Overview

This module provides manager components that sit above the core storage, services, and wallet layers. These managers handle cross-cutting concerns like multi-storage synchronization, two-factor authentication, multi-profile support, settings persistence, permission control, WAB authentication, and operation logging. All managers are designed for 1:1 parity with the TypeScript `@bsv/wallet-toolbox`.

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
│  WalletPermissionsManager      - BRC-98/99 permissions (stub)    │
│  WalletLogger                  - Operation logging               │
├─────────────────────────────────────────────────────────────────┤
│                      Wallet + Storage + Services                │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 317 | Module declarations, re-exports, `WalletLogger`, `SetupWalletOptions`, `setup_wallet()` |
| `storage_manager.rs` | 1131 | Multi-storage orchestration with active/backup semantics and lock queues |
| `cwi_style_wallet_manager.rs` | 709 | CWI-compatible multi-profile manager with PBKDF2 password derivation, UMP tokens, snapshots |
| `permissions_manager.rs` | 600 | BRC-98/99 permission types, operation-level flags, and stub manager |
| `settings_manager.rs` | 339 | Persistent wallet settings with mainnet/testnet defaults |
| `simple_wallet_manager.rs` | 336 | Two-factor authentication manager (primary key + privileged key) |
| `auth_manager.rs` | 56 | WAB (Wallet Authentication Backend) integration wrapper |

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

### Permissions Management (Stub)

```rust
pub use permissions_manager::{
    GroupedPermissions, PermissionRequest, PermissionToken, PermissionsModule,
    WalletPermissionsManager, WalletPermissionsManagerConfig,
};
```

- `WalletPermissionsManager` - Stub implementation with `check_permission()` for operation-level gating
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
provider_locks // Highest precedence
```

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

### Trait Implementations

`WalletStorageManager` implements the full storage provider trait hierarchy:
- `WalletStorageReader` - Delegates reads to active storage with reader lock
- `WalletStorageWriter` - Delegates writes to active storage with writer lock (includes `review_status`, `purge_data`, `update_transaction_status_after_broadcast`)
- `WalletStorageSync` - Delegates sync operations with sync lock
- `WalletStorageProvider` - Partial implementation (some sync methods unimplemented)

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

Uses `ring::pbkdf2` with SHA-512 and configurable rounds (default: 7777):
```rust
// Password -> derived_key (32 bytes)
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

## WalletPermissionsManager (Stub)

**Security Warning**: This is a stub that does not enforce BRC-98/99 on-chain permission tokens. However, it does support basic operation-level permission gating via `check_permission()`.

### Permission Types (BRC-98/99)

```rust
enum PermissionType {
    Protocol,    // DPACP - Protocol access
    Basket,      // DBAP - Basket access
    Certificate, // DCAP - Certificate access
    Spending,    // DSAP - Spending authorization
}
```

### Operation-Level Permission Checking

`check_permission(request)` evaluates operation-level flags when `enforce_permissions` is true:

```rust
// Per-operation flags in WalletPermissionsManagerConfig:
allow_create_action, allow_sign_action, allow_abort_action, allow_list_actions,
allow_internalize_action, allow_list_outputs, allow_relinquish_output,
allow_acquire_certificate, allow_list_certificates, allow_prove_certificate,
allow_relinquish_certificate, allow_discover, allow_crypto
```

Admin originator always bypasses all checks.

### Configuration Options

`WalletPermissionsManagerConfig` has boolean flags for each permission category:
- `seek_protocol_permissions_for_signing/encrypting/hmac`
- `seek_basket_insertion/removal/listing_permissions`
- `seek_certificate_disclosure/acquisition/relinquishment/listing_permissions`
- `seek_spending_permissions`
- `differentiate_privileged_operations`
- `encrypt_wallet_metadata`
- `enforce_permissions` + per-operation `allow_*` flags

Helper constructors:
```rust
WalletPermissionsManagerConfig::all_enabled()   // Most secure (all flags true)
WalletPermissionsManagerConfig::all_disabled()   // Permissive (default)
```

### Additional Permission Types

```rust
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

### Stub Implementations

- `WalletPermissionsManager` - Has operation-level `check_permission()` but does not enforce full BRC-98/99 on-chain tokens
- `WalletAuthenticationManager` - Thin wrapper; WAB protocol flow not yet implemented
- `setup_wallet()` - Stub that logs setup intent only

### Concurrency Model

All managers use `tokio::sync::RwLock` for async-safe interior mutability. The storage manager's lock queue system prevents deadlocks through ordered acquisition.

### Encryption

Managers use `bsv_sdk::primitives::SymmetricKey` for AES encryption of snapshots and exports. Keys are randomly generated per operation.
