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
| `storage_manager.rs` | 2067 | Multi-storage orchestration with active/backup semantics, lock queues, MonitorStorage impl with task locking, sync-safe cached settings/services |
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

### Sync rules

- Every drain (`drain_sync_chunks`: `sync_from_reader`, `sync_to_writer`, `update_backups`, and `set_active`'s backups) refuses a reader and a writer whose `settings.chain` are not one chain (`same_chain`: `mainnet` = `main`, `testnet` = `test`, any other word equals itself alone; `Live sync requires matching declared network chains on both storage providers.`), then makes the writer's user (`find_or_insert_user`) before any chunk: a chunk never creates its user. A writer's answer with `error` set fails the drain ahead of `done`; `done = false` to a chunk that carried no row is refused (`Sync provider reported an unfinished page without advancing its durable checkpoint.`).
- Every drain makes its reader available and runs inside `crate::storage::sync_scope::scope(..)`; around each chunk it asks the reader `checks_received_beef_in_process` before and after `get_sync_chunk` and marks the chunk local (`sync_scope::set_local_reader`) only when both answers are true (a manager reader answers by its active, which another task may move). A local writer's proof preflight validates every proof row it does not hold as the same proof (new rows too) unless the scope marks the chunk in hand as from a local reader; outside any drain (a direct `process_sync_chunk`, the manager's own `process_sync_chunk`) it validates too. One root check and one header call of the store's services per height per drain (per chunk outside one), eight in flight, no retry. Drive several chunks through `process_sync_chunk_mapped` with one `SyncMap`, as the drain does.
- `sync_from_reader` writes the wallet's own active storage (the active store's cached user row) into the chunk's user row: a reader does not choose the active store. When that cached row names no active (a row the manager could not read: `UserActiveStorage::Keep(None)`), the chunk goes to the writer with no user row (`chunk.user = None`): neither an empty active nor the reader's reaches it, the writer's row of the user is left as it was, every other row of the chunk is merged and its checkpoint advances (`tests/sync_held_rows_tests.rs`). A push (`sync_to_writer`) carries the reader's.
- Every sync entry that takes an identity (`sync_from_reader`, `sync_to_writer`, and the `WalletStorageSync` surface: `get_sync_chunk`, `process_sync_chunk`, `find_or_insert_sync_state`, `set_active`) calls `check_sync_identity` before anything else: `Access denied: Identity key mismatch`.
- `set_active` reads every store's row of the wallet's user again (`refresh_user_rows`, into the cached rows) under `set_active_lock`, and answers `setActive to <key> unchanged` only when the key is the manager's active and every row names it (re-partitioning before it answers when `auth_id.is_active` is false, which then makes it true, or when `rows_in_doubt` was set at the call's entry, so whatever a dropped call left of the manager's fields is rebuilt from the rows just read). Otherwise the transition: `update_backups`, the rows read again, then each store whose row does not name the key is written by that store's own user id (`AuthId::with_user_id(identity, store.user.user_id)`), each store and its cached row copied out under the stores read lock and the calls made with no lock held, then the cached row of each store whose call succeeded is set under the write lock, before `partition` runs (it reads the cached rows). `partition` is the one partition of the stores into active, backups and conflicting (the indices, `auth_id.user_id` and `is_active`, `cached_settings`, `is_available`): `make_available` calls it at open, the transition as its last step, the no-op when `auth_id.is_active` is false or the mark was set at entry, and `set_active` after the re-read below. It is all or nothing against a drop: it computes every field under the stores read lock, then, that lock still held, takes `active_index`, `backup_indices`, `conflicting_indices`, `auth_id` and `is_available` for write in that order and writes every field (`cached_settings` last) with no await between its writes. It holds the stores read lock from its read of the rows through its last write, so no cached row moves between its read and its writes and a partition from older rows never writes after one from newer rows (`src/managers/storage_manager_partition_tests.rs`). The manager's locks, in their one order: the transition lock (`set_active_lock`), then the queues' permits (reader, then writer, then sync: `run_as_*`, a transition's backup syncs included), then the open lock (`open_lock`, one open of the stores at a time), then the state locks. The transition lock, the permits and the open lock are held across store calls, by design; none is ever taken while a state lock is held, an open takes no permit and never the transition lock. An open changes the manager's roles only through the `partition` at its end, from the rows it read: `open_locked` resets nothing, and `add_wallet_storage_provider` pushes its store without marking the manager unavailable, so while the store it adds is opened (and after that open fails) a manager already available answers on the active and backups it had, its writes, `update_backups` and `set_active` included; the failed store stays listed, not open, and the next `make_available` opens it again (`tests/manager_open_roles_tests.rs`). So `set_active` takes the open lock only through `make_available` when the manager is not available (never opened, or every open so far failed), and decides its no-op over the open stores without it: a store being added that never answers keeps no `set_active` waiting on an available manager. `run_as_*` waits for an open only while the manager is not available, as on 0.13.8; the state readers (`is_active_enabled`, `is_available`, `get_stores`, `get_active_store`) wait for none. `make_available` answers the active's settings without the open lock when the manager is available and every store is open (`opened_settings`); otherwise it takes the open lock, asks again, and only then copies out the stores still to open, so a second open that waited calls no store the one ahead of it opened, and each store's `find_or_insert_user` is called once (a store answers every caller of one identity alike since its own call is one write transaction). `partition` reads only the stores that are open (`ManagedStorage::is_open`: answered `make_available`, user and settings held), with no unwrap on a store's user or settings: a store pushed by `add_wallet_storage_provider` and not yet opened is in no role and shows in `get_stores` with an empty key and user id 0 until its open ends. `set_active` takes only the open stores too: its re-reads (`refresh_user_rows`, `reread_user_rows_after_error`, through `open_storages`, each answer written back by the store's index), `every_row_names`, `rows_name_one_store` and the transition's loop, which uses the manager's `auth_id` as the call's entry made it rather than `get_auth`, so a no-op or a transition beside an add neither calls the store being added nor waits for its open. The add's own partition then takes the store in as it would after the switch: a store whose row names the old active is conflicting, the active not enabled, and a `set_active` answers `Access denied: Operation requires active storage` until each store's own `WalletStorageSync::set_active` to one store and a new manager over them. `update_backups` reads the manager's active and stores again under the sync lock before each sync (`sync_to_writer_unless_active`) and passes over a store that is now the active or no longer one of the stores, with a log line. A state lock (`stores`, `active_index`, `backup_indices`, `conflicting_indices`, `auth_id`, `is_available`, `services`) is held only to copy out what a function needs (the stores it will call, cloned; the indices; the auth) or to write the manager's own fields, never across a store's call and never across a call that takes a manager lock again (`update_backups` copies its backups out before it syncs them; `make_available`, the transition, `destroy` and `set_services` copy the stores out before they call them). A taker of two state locks (`is_active_enabled`, `get_stores`, `get_settings`, `get_active`, `get_backup_stores`, `get_conflicting_stores`, `make_available`'s answer when available, `checks_received_beef_in_process`, `partition`) takes `stores` ahead of the five and these in that order, and while it holds one awaits nothing but the next in that order. A backup waiting inside its sync, or a store that never answers `make_available`, keeps no other task waiting on a state lock (`tests/sync_lock_tests.rs`). `is_active_enabled` (and `is_enabled` in `get_stores`, one computation: `active_enabled`) is true only while every open store's cached row names the active and `rows_in_doubt` is clear: `set_active` sets it just before `set_active_transition` (the no-op path never sets it) and clears it when the transition returns, after the re-read on an error, and on the no-op path once every row read names the active; a future dropped at an await (a host's timeout) or a store's panic leaves it set, and the `set_active_lock` guard is released with the dropped future, so the next `set_active` clears it by completing. `get_auth` does not read it (the retry's `update_backups` needs `auth_id.is_active`): `get_auth(true)` answers Ok while `is_active_enabled` answers false after a call that failed part way or was dropped, so `update_backups` and plain writes go on from and to the active the manager had; a host gates its writes on `is_active_enabled` (or `get_stores`' `is_enabled`), not on `get_auth`. A call that fails, panics or is dropped while it only reads the rows, before its transition has begun, leaves the answer as it was. It refuses while the stores are in conflict (`update_backups` needs an enabled active: `auth_id.is_active`). One transition at a time (`set_active_lock`, a lock no other operation takes; the no-op decision is under it too). A transition that fails part way (a store's `set_active`, or a backup's sync inside `update_backups`) leaves the manager available on its partition, `auth_id.is_active` as it was (unless the re-read below partitions it again), and the cached rows of the stores whose call succeeded naming the new active, so `is_active_enabled` is false while the rows disagree, and a retry, or a call back to the old active, completes whichever store failed (`tests/sync_set_active_tests.rs`). After any error inside the transition `set_active` calls `reread_user_rows_after_error`: every store's row is read again, store by store, and a store that cannot be read keeps its cached user with `active_storage = None`, so `is_active_enabled` is false (a store may write its row and lose its answer: the lost-answer cells of the matrix). When every store was read and every row names one of the manager's stores (`rows_name_one_store`), `partition` runs: that store is the active, enabled (the target after a completed switch whose last answer was lost; the old active when nothing was written); otherwise the manager stays on its active, not enabled. The call still answers its error: after a failure whose rows all name one store the host holds an error for a switch that holds (or one that never began), and reads `is_active_enabled` and `get_active_store`, or retries the same call, which answers unchanged when the switch holds; with three stores whose rows all name a store neither the target nor the old active, the manager is on that store, enabled. The `every_read` condition is a safety net: an unread store's held row names no active, so `rows_name_one_store` is already false. The matrix (`tests/sync_set_active_tests.rs`, its cells in `tests/sync_support/cells.rs`) has 63 cells. Reads per store (`find_or_insert_user`): 1 for the no-op, 2 for the active and 3 for each other store for a switch, 1 more after an error; so a no-op answers a store's read error. A new manager over stores that disagree is not enabled and refuses both, and so does this manager after `add_wallet_storage_provider` re-partitions it following a part-way failure; each store's own `WalletStorageSync::set_active` to one store, then a new manager, is the way through.
- A backup's request keeps the status it was given when the backup received it (the reference's request merge), so a wallet made active on that backup asks or sends again. Replacing a held proof needs the backup store's services.

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

All managers use `tokio::sync::RwLock` for async-safe interior mutability. The storage manager's lock queue system prevents deadlocks through ordered acquisition (reader -> writer -> sync). Lock timeout is 30 seconds. Its state locks are held only to copy out or to write its own fields, never across a store's call or a call that takes one of its locks again, and a taker of two takes `stores` ahead of `active_index`, `backup_indices`, `conflicting_indices`, `auth_id` and `is_available`, in that order; `set_active_lock`, then the queues' permits, then `open_lock`, are the only locks held across store calls (see Sync rules).

### Encryption

Managers use `bsv_sdk::primitives::SymmetricKey` for AES encryption of snapshots and exports. Keys are randomly generated per operation. The `CWIStyleWalletManager` additionally uses `zeroize::Zeroizing` for derived key material memory safety.
