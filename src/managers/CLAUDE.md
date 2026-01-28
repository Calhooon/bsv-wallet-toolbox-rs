# src/managers/ - Wallet Manager Components
> Higher-level wallet management abstractions for authentication, settings, storage sync, and permissions

## Overview

This module provides manager components that sit above the core storage, services, and wallet layers. These managers handle cross-cutting concerns like multi-storage synchronization, two-factor authentication, multi-profile support, settings persistence, and permission control. All managers are designed for 1:1 parity with the TypeScript `@bsv/wallet-toolbox`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Managers                                │
├─────────────────────────────────────────────────────────────────┤
│  WalletStorageManager     - Multi-storage sync, active/backup   │
│  WalletSettingsManager    - Settings persistence                │
│  SimpleWalletManager      - Primary key + PKM authentication    │
│  CWIStyleWalletManager    - Multi-profile, password-based       │
│  WalletPermissionsManager - BRC-98/99 permissions (stub)        │
├─────────────────────────────────────────────────────────────────┤
│                      Wallet + Storage + Services                │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 59 | Module declarations, re-exports, and architecture documentation |
| `storage_manager.rs` | 1103 | Multi-storage orchestration with active/backup semantics and lock queues |
| `cwi_style_wallet_manager.rs` | 585 | CWI-compatible multi-profile manager with PBKDF2 password derivation |
| `permissions_manager.rs` | 470 | BRC-98/99 permission types and stub manager (not yet implemented) |
| `settings_manager.rs` | 339 | Persistent wallet settings with mainnet/testnet defaults |
| `simple_wallet_manager.rs` | 336 | Two-factor authentication manager (primary key + privileged key) |

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
pub use cwi_style_wallet_manager::{CWIStyleWalletManager, CWIStyleWalletManagerConfig, Profile};
```

- `SimpleWalletManager` - Two-factor authentication: primary key (32 bytes) + privileged key manager
- `CWIStyleWalletManager` - Multi-profile wallet with PBKDF2 password-based key derivation
- `Profile` - Profile data structure with name, ID, primary pad, and timestamps

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

- `WalletPermissionsManager` - Stub implementation (passes through to underlying wallet)
- `GroupedPermissions` - BRC-73 grouped permissions (spending, protocol, basket, certificate)
- `PermissionRequest` - Permission request from an application
- `PermissionToken` - On-chain permission token (BRC-98/99)
- `PermissionsModule` - Trait for request/response transformation by scheme

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
```

### Trait Implementations

`WalletStorageManager` implements the full storage provider trait hierarchy:
- `WalletStorageReader` - Delegates reads to active storage with reader lock
- `WalletStorageWriter` - Delegates writes to active storage with writer lock
- `WalletStorageSync` - Delegates sync operations with sync lock
- `WalletStorageProvider` - Partial implementation (some sync methods unimplemented)

## SimpleWalletManager Authentication Flow

```
1. Create manager with admin_originator and wallet_builder
2. provide_primary_key(key: Vec<u8>)     // 32 bytes required
3. provide_privileged_key_manager(pkm)   // PrivateKey from bsv-sdk
4. → try_build_underlying() auto-called  // Builds wallet when both present
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
// Password → derived_key (32 bytes)
// primary_key = primary_pad XOR derived_key
// Salt = profile.id (16 bytes)
```

### Profile Management

```rust
create_profile(name, password) -> Result<Profile>  // Generate random primary key
switch_profile(profile_id, password) -> Result<()> // Authenticate and build wallet
delete_profile(profile_id) -> Result<()>           // Remove (can't delete active)
get_profiles() -> Vec<Profile>                     // List all profiles
export_profile(profile_id) -> Result<Vec<u8>>      // Encrypt profile data
import_profile(data, password) -> Result<Profile>  // Decrypt and store
```

### Snapshot System

```rust
save_snapshot() -> Result<Vec<u8>>   // Encrypt all profiles + default_profile_id
load_snapshot(data) -> Result<()>    // Restore profile collection
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

**Security Warning**: This is a stub that does not perform permission checks. All operations pass through to the underlying wallet.

### Permission Types (BRC-98/99)

```rust
enum PermissionType {
    Protocol,    // DPACP - Protocol access
    Basket,      // DBAP - Basket access
    Certificate, // DCAP - Certificate access
    Spending,    // DSAP - Spending authorization
}
```

### Configuration Options

`WalletPermissionsManagerConfig` has boolean flags for each permission type:
- `seek_protocol_permissions_for_signing/encrypting/hmac`
- `seek_basket_insertion/removal/listing_permissions`
- `seek_certificate_disclosure/acquisition/relinquishment/listing_permissions`
- `seek_spending_permissions`
- `differentiate_privileged_operations`

Helper constructors:
```rust
WalletPermissionsManagerConfig::all_enabled()   // Most secure
WalletPermissionsManagerConfig::all_disabled()  // Permissive (default)
```

## Usage

### Multi-Storage with Active/Backup

```rust
use bsv_wallet_toolbox::managers::WalletStorageManager;

// Create manager with active and backup storages
let manager = WalletStorageManager::new(
    identity_key.to_string(),
    Some(active_storage),
    Some(vec![backup_storage]),
);

// Initialize and make available
let settings = manager.make_available().await?;

// Use with automatic locking
let result = manager.run_as_writer(|writer| async {
    writer.create_action(&auth, args).await
}).await?;

// Sync to all backups
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

// Provide both authentication factors
manager.provide_primary_key(primary_key_bytes).await?;
manager.provide_privileged_key_manager(pkm_private_key).await?;

// Now authenticated
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

// Provide privileged key manager first
manager.provide_privileged_key_manager(pkm_key).await;

// Create and switch to a profile
let profile = manager.create_profile("Work", "password123").await?;
manager.switch_profile(&profile.id, "password123").await?;

// Access authenticated wallet
let wallet = manager.wallet().await?;
```

### Settings Management

```rust
use bsv_wallet_toolbox::managers::{WalletSettingsManager, WalletSettings};

let manager = WalletSettingsManager::new(None); // Uses DEFAULT_SETTINGS

// Read settings
let settings = manager.get().await;

// Update settings
let mut new_settings = settings;
new_settings.currency = Some("EUR".to_string());
manager.set(new_settings).await;

// Persist to storage
let bytes = manager.save().await?;

// Restore from storage
manager.load(&bytes).await?;
```

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Parent module with crate overview
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage traits and entities used by managers
- [../wallet/CLAUDE.md](../wallet/CLAUDE.md) - Wallet implementation that managers wrap

## Development Notes

### TypeScript Parity

These managers match the TypeScript `@bsv/wallet-toolbox` implementations:
- `WalletStorageManager` - Storage synchronization logic matches TypeScript
- `SimpleWalletManager` - Two-factor authentication pattern
- `CWIStyleWalletManager` - CWI multi-profile pattern with PBKDF2
- `WalletSettingsManager` - Settings persistence pattern

### Stub Implementations

`WalletPermissionsManager` and `CWIStyleWalletManager` are partially implemented:
- Permission checking is not enforced (all operations pass through)
- Full BRC-98/99 on-chain permission token handling is not implemented
- Types and structures are complete for API compatibility

### Concurrency Model

All managers use `tokio::sync::RwLock` for async-safe interior mutability. The storage manager's lock queue system prevents deadlocks through ordered acquisition.

### Encryption

Managers use `bsv_sdk::primitives::SymmetricKey` for AES encryption of snapshots and exports. Keys are randomly generated per operation.
