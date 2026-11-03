//! CWI Style Wallet Manager
//!
//! A wallet manager compatible with the Common Wallet Interface (CWI) pattern,
//! providing multi-profile support with PBKDF2 password-based key derivation.
//!
//! Once a profile is switched to with the correct password, the manager becomes
//! authenticated and delegates all wallet operations to an underlying wallet instance.
//!
//! The key file (`save_snapshot`, `save_key_file`) holds one reference UMP
//! token per profile, each with its own random salt (#560,
//! `docs/design/ump-token-format.md`); a v1 record migrates at its unlock.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use bsv_rs::primitives::{PrivateKey, SymmetricKey};
use bsv_rs::wallet::WalletInterface;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use zeroize::Zeroizing;

use super::ump_token::{
    self, decode_key_file, encode_key_file, Entries, Entry, EntryRecord, Factors, PasswordKdf,
    RefProfile, UmpEntry,
};
use crate::error::{Error, Result};

/// Random bytes from the OS CSPRNG, never seeded (#191).
fn generate_random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    crate::rng::os_fill(&mut bytes);
    bytes
}

/// A 32-byte OS draw that is a valid secp256k1 scalar (the root privileged key).
fn generate_scalar() -> Zeroizing<Vec<u8>> {
    loop {
        let bytes = Zeroizing::new(generate_random_bytes(32));
        if PrivateKey::from_bytes(&bytes).is_ok() {
            return bytes;
        }
    }
}

/// Default number of PBKDF2 rounds (matches TypeScript).
pub const DEFAULT_PASSWORD_ROUNDS: u32 = 7777;

/// The profile id's length, and the primary and privileged pads', in bytes.
const PROFILE_ID_LENGTH: usize = 16;
const PRIMARY_PAD_LENGTH: usize = 32;
const PRIVILEGED_PAD_LENGTH: usize = 32;

/// Configuration for the CWI-style wallet manager.
#[derive(Debug, Clone)]
pub struct CWIStyleWalletManagerConfig {
    /// PBKDF2 rounds of a v1 record's derivation; new tokens are argon2id (#593).
    pub password_rounds: u32,
    /// Whether to use PBKDF2 for key derivation.
    pub use_pbkdf2: bool,
}

impl Default for CWIStyleWalletManagerConfig {
    fn default() -> Self {
        Self {
            password_rounds: DEFAULT_PASSWORD_ROUNDS,
            use_pbkdf2: true,
        }
    }
}

/// A wallet profile for multi-profile support.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    /// Display name for the profile.
    pub name: String,
    /// Unique identifier (16 bytes).
    #[serde(with = "base64_bytes")]
    pub id: Vec<u8>,
    /// Primary pad XOR'd with derived key to get root key.
    #[serde(with = "base64_bytes")]
    pub primary_pad: Vec<u8>,
    /// Privileged key pad (32 bytes) for two-factor authentication.
    #[serde(default, with = "base64_bytes")]
    pub privileged_pad: Vec<u8>,
    /// When the profile was created.
    pub created_at: DateTime<Utc>,
    /// When the profile was last updated.
    pub updated_at: DateTime<Utc>,
}

impl Profile {
    /// Creates a new profile with random id, primary pad, and privileged pad.
    pub fn new(name: String) -> Self {
        Self {
            name,
            id: generate_random_bytes(PROFILE_ID_LENGTH),
            primary_pad: generate_random_bytes(PRIMARY_PAD_LENGTH),
            privileged_pad: generate_random_bytes(PRIVILEGED_PAD_LENGTH),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Creates a profile from a name and existing primary key.
    pub fn from_primary_key(name: String, primary_key: &[u8], derived_key: &[u8]) -> Self {
        Self {
            name,
            id: generate_random_bytes(PROFILE_ID_LENGTH),
            primary_pad: ump_token::xor(primary_key, derived_key).to_vec(),
            privileged_pad: generate_random_bytes(PRIVILEGED_PAD_LENGTH),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Derives the primary key from the profile pad and derived password key.
    pub fn derive_primary_key(&self, derived_key: &[u8]) -> Vec<u8> {
        ump_token::xor(&self.primary_pad, derived_key).to_vec()
    }
}

/// Base64 encoding helper for serde; the base64 text, key material for the
/// pads and the presentation key, is zeroed on drop both ways.
pub(super) mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};
    use zeroize::Zeroizing;

    pub fn serialize<S: Serializer, B: AsRef<[u8]>>(
        bytes: &B,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&Zeroizing::new(STANDARD.encode(bytes)))
    }

    /// Into a `Vec<u8>`, or straight into a `Zeroizing<Vec<u8>>` for key material.
    pub fn deserialize<'de, D: Deserializer<'de>, B: From<Vec<u8>>>(
        deserializer: D,
    ) -> Result<B, D::Error> {
        let s = Zeroizing::new(String::deserialize(deserializer)?);
        STANDARD
            .decode(s.as_bytes())
            .map(B::from)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

/// The prefix of a key file's write sibling, `<name>.tmp-<16 hex>`.
fn tmp_prefix(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .ok_or_else(|| Error::InvalidArgument("Key file path has no file name".into()))?;
    Ok(format!("{}.tmp-", name.to_string_lossy()))
}

fn parent_dir(path: &Path) -> &Path {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    dir.unwrap_or(Path::new("."))
}

/// Removes the write siblings an interrupted write left beside `path` (an absent
/// directory has none). A writer in the key directory can already replace the key
/// file, so this never follows a link nor writes through a shared inode: zeroing
/// (synced; best effort on copy-on-write) is a courtesy for the manager's own
/// sibling of one link, before and after the open (a link made between the two
/// would share the inode with the write), done only when the open reached it; a
/// link is unlinked, as is a sibling the process cannot open (another uid's
/// leftover), unzeroed.
async fn remove_stale_siblings(path: &Path) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let prefix = tmp_prefix(path)?;
    let stale = |tag: &str| tag.len() == 16 && tag.bytes().all(|b| b.is_ascii_hexdigit());
    #[cfg(unix)] // the manager's sibling is created fresh: one link
    let sole = |m: &std::fs::Metadata| std::os::unix::fs::MetadataExt::nlink(m) == 1;
    #[cfg(not(unix))] // std gives no link count: removed, never zeroed
    let sole = |_: &std::fs::Metadata| false;
    let mut dir = match tokio::fs::read_dir(parent_dir(path)).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        dir => dir?,
    };
    while let Some(item) = dir.next_entry().await? {
        let name = item.file_name().to_string_lossy().into_owned();
        if !name.strip_prefix(&prefix).is_some_and(stale) {
            continue;
        }
        let meta = tokio::fs::symlink_metadata(item.path()).await?;
        let mut open = tokio::fs::OpenOptions::new();
        if meta.is_file() && sole(&meta) {
            if let Ok(mut file) = open.write(true).open(item.path()).await {
                let opened = file.metadata().await?;
                if !same_file(&meta, &opened) {
                    continue;
                }
                if sole(&opened) {
                    let len = usize::try_from(meta.len()).unwrap_or(0);
                    file.write_all(&vec![0u8; len]).await?;
                    file.sync_all().await?;
                }
            }
        } else if !meta.is_file() && !meta.file_type().is_symlink() {
            continue;
        }
        tokio::fs::remove_file(item.path()).await?;
    }
    Ok(())
}

/// Whether the entry read without following links is the file opened (unix:
/// device and inode; std has no open that refuses a link without `libc`).
#[cfg(unix)]
fn same_file(entry: &std::fs::Metadata, opened: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    (entry.dev(), entry.ino()) == (opened.dev(), opened.ino())
}

#[cfg(not(unix))]
fn same_file(_: &std::fs::Metadata, opened: &std::fs::Metadata) -> bool {
    opened.is_file()
}

/// Writes `bytes` over `path` atomically: a sibling in the same directory,
/// synced, renamed over the file (owner-only on unix), and the directory
/// synced so the rename survives a crash. std's blocking write, on the
/// blocking pool, writes from `bytes` itself, zeroed when the write ends;
/// tokio's file would copy them into a buffer of its own, freed unzeroed.
async fn write_key_file(path: &Path, bytes: Zeroizing<Vec<u8>>) -> Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let path = path.to_path_buf();
    let tmp = path.with_file_name(format!(
        "{}{}",
        tmp_prefix(&path)?,
        hex::encode(generate_random_bytes(8))
    ));
    let write = move || -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let written = options.open(&tmp).and_then(|mut file| {
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &path)
        });
        if written.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        written?;
        #[cfg(unix)]
        std::fs::File::open(parent_dir(&path))?.sync_all()?;
        Ok(())
    };
    let joined = tokio::task::spawn_blocking(write).await;
    Ok(joined.map_err(std::io::Error::other)??)
}

type Unlocked = (Zeroizing<Vec<u8>>, Option<Entry>);

/// A password KDF and what it opens or seals, on the blocking pool over a zeroed
/// copy of the password: argon2id's 128 MiB and ~330 ms stay off the worker (#624).
async fn off_worker<T: Send + 'static>(
    password: &str,
    kdf: impl FnOnce(&str) -> Result<T> + Send + 'static,
) -> Result<T> {
    let password = Zeroizing::new(password.to_owned());
    tokio::task::spawn_blocking(move || kdf(&password))
        .await
        .map_err(|_| Error::Internal("the password KDF task panicked or was cancelled".into()))?
}

/// Type alias for the wallet builder function.
pub type CWIWalletBuilder = Arc<
    dyn Fn(
            Vec<u8>,
            PrivateKey,
        )
            -> Pin<Box<dyn Future<Output = Result<Arc<dyn WalletInterface + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;

/// CWI-style wallet manager with multi-profile support.
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::managers::{CWIStyleWalletManager, CWIStyleWalletManagerConfig};
///
/// let manager = CWIStyleWalletManager::new(
///     "admin.wallet".to_string(),
///     wallet_builder,
///     CWIStyleWalletManagerConfig::default(),
/// );
///
/// manager.provide_privileged_key_manager(pkm_key).await;
///
/// // Open (or start) the key file, then create a new profile
/// manager.open_key_file("wallet.keys").await?;
/// let profile = manager.create_profile("Work", "password123").await?;
/// manager.save_key_file().await?;
/// manager.switch_profile(&profile.id, "password123").await?;
/// let wallet = manager.wallet().await?;
/// let result = wallet.create_action(args, "app.example.com").await?;
/// ```
pub struct CWIStyleWalletManager {
    config: CWIStyleWalletManagerConfig,
    /// All profiles by ID, with their tokens.
    entries: RwLock<Entries>,
    default_profile_id: RwLock<Option<Vec<u8>>>,
    active_profile_id: RwLock<Option<Vec<u8>>>,
    admin_originator: String,
    wallet_builder: CWIWalletBuilder,
    /// Underlying wallet (built after authentication).
    underlying: RwLock<Option<Arc<dyn WalletInterface + Send + Sync>>>,
    /// Privileged key manager (provided separately).
    privileged_key_manager: RwLock<Option<PrivateKey>>,
    /// The key file a migrating unlock re-writes (`open_key_file`).
    key_file: RwLock<Option<PathBuf>>,
}

impl CWIStyleWalletManager {
    /// Creates a new CWI-style wallet manager.
    pub fn new(
        admin_originator: String,
        wallet_builder: CWIWalletBuilder,
        config: CWIStyleWalletManagerConfig,
    ) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            default_profile_id: RwLock::new(None),
            active_profile_id: RwLock::new(None),
            admin_originator,
            wallet_builder,
            underlying: RwLock::new(None),
            privileged_key_manager: RwLock::new(None),
            key_file: RwLock::new(None),
        }
    }

    /// Provides the privileged key manager.
    pub async fn provide_privileged_key_manager(&self, manager: PrivateKey) {
        *self.privileged_key_manager.write().await = Some(manager);
    }

    /// Returns whether there is an active authenticated profile.
    pub async fn is_authenticated(&self) -> bool {
        self.underlying.read().await.is_some()
    }

    /// The underlying wallet; `Error::AuthenticationRequired` before an unlock.
    pub async fn wallet(&self) -> Result<Arc<dyn WalletInterface + Send + Sync>> {
        self.underlying
            .read()
            .await
            .clone()
            .ok_or(Error::AuthenticationRequired)
    }

    /// Gets the underlying wallet, checking originator is not admin.
    pub async fn wallet_for_originator(
        &self,
        originator: &str,
    ) -> Result<Arc<dyn WalletInterface + Send + Sync>> {
        if originator == self.admin_originator {
            return Err(Error::AccessDenied(
                "External applications cannot use the admin originator".to_string(),
            ));
        }
        self.wallet().await
    }

    /// Returns the admin originator.
    pub fn admin_originator(&self) -> &str {
        &self.admin_originator
    }

    /// The v1 derivation: PBKDF2 salted with the profile id, zeroed on drop.
    fn derive_key_from_password(
        config: &CWIStyleWalletManagerConfig,
        password: &str,
        salt: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>> {
        if config.use_pbkdf2 {
            return ump_token::pbkdf2_sha512(password, salt, config.password_rounds);
        }
        // Simple fallback: just hash the password (not secure, for testing only)
        use ring::digest::{digest, SHA256};
        Ok(Zeroizing::new(
            digest(&SHA256, password.as_bytes()).as_ref().to_vec(),
        ))
    }

    /// Builds a profile's token around `root_primary` with a fresh salt, fresh
    /// presentation, recovery and root privileged keys, and argon2id at the
    /// reference's defaults, its KDF for new tokens (`createNewUser`, :1832-1876).
    fn seal(
        mut profile: Profile,
        root_primary: Zeroizing<Vec<u8>>,
        password: &str,
    ) -> Result<(Profile, UmpEntry)> {
        let salt = generate_random_bytes(32);
        let kdf = PasswordKdf::argon2id();
        let factors = Factors {
            password_key: ump_token::derive_password_key(&salt, Some(&kdf), password)?,
            presentation_key: Zeroizing::new(generate_random_bytes(32)),
            recovery_key: Zeroizing::new(generate_random_bytes(32)),
            root_primary,
            root_privileged: generate_scalar(),
        };
        if profile.privileged_pad.is_empty() {
            profile.privileged_pad = generate_random_bytes(PRIVILEGED_PAD_LENGTH);
        }
        let reference = RefProfile {
            name: profile.name.clone(),
            id: profile.id.clone(),
            primary_pad: profile.primary_pad.clone(),
            privileged_pad: profile.privileged_pad.clone(),
            created_at: profile.created_at.timestamp(),
        };
        let token = ump_token::build_token(salt, kdf, &factors, &[reference])?;
        let ump = UmpEntry {
            presentation_key: factors.presentation_key,
            token,
        };
        Ok((profile, ump))
    }

    /// Creates a new profile.
    ///
    /// The profile gets a random ID and pads and its own UMP token around a
    /// random root key; its wallet key is `root XOR primary_pad`, the
    /// reference's derivation for a profile.
    pub async fn create_profile(&self, name: &str, password: &str) -> Result<Profile> {
        let profile = Profile::new(name.to_string());
        let root = Zeroizing::new(generate_random_bytes(32));
        let (profile, ump) = off_worker(password, |pw| Self::seal(profile, root, pw)).await?;
        let mut entries = self.entries.write().await;
        let entry = Entry {
            profile: profile.clone(),
            ump: Some(ump),
            v1_pad: None,
        };
        entries.insert(profile.id.clone(), entry);
        if entries.len() == 1 {
            *self.default_profile_id.write().await = Some(profile.id.clone());
        }
        Ok(profile)
    }

    async fn entry(&self, profile_id: &[u8]) -> Result<Entry> {
        self.entries
            .read()
            .await
            .get(profile_id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            })
    }

    /// The profile's wallet key, and the record that follows the unlock, which
    /// the file takes: the four cases of `switch_profile` (design 4.3).
    fn unlock_entry(
        config: &CWIStyleWalletManagerConfig,
        entry: &Entry,
        password: &str,
    ) -> Result<Unlocked> {
        if let Some(ump) = &entry.ump {
            match Self::open_ump(entry, ump, password) {
                Ok(key) if entry.v1_pad.is_some() => {
                    let mut next = entry.clone();
                    next.v1_pad = None;
                    return Ok((key, Some(next)));
                }
                Ok(key) => return Ok((key, None)),
                Err(Error::AccessDenied(_)) if entry.v1_pad.is_some() => {}
                Err(e) => return Err(e),
            }
        }
        let v1_pad = entry
            .v1_pad
            .as_ref()
            .ok_or_else(|| Error::ValidationError("Profile has neither record".into()))?;
        let derived = Self::derive_key_from_password(config, password, &entry.profile.id)?;
        let key = ump_token::xor(v1_pad, &derived);
        // A fresh root: the token and its blobs depend on the new salt alone,
        // and the pad moves so that root XOR pad is still the v1 key.
        let root = Zeroizing::new(generate_random_bytes(32));
        let mut profile = entry.profile.clone();
        profile.primary_pad = ump_token::xor(&key, &root).to_vec();
        let (profile, ump) = Self::seal(profile, root, password)?;
        let next = Entry {
            profile,
            ump: Some(ump),
            v1_pad: entry.v1_pad.clone(),
        };
        Ok((key, Some(next)))
    }

    /// The profile's key from its token: root primary XOR the token's pad.
    fn open_ump(entry: &Entry, ump: &UmpEntry, password: &str) -> Result<Zeroizing<Vec<u8>>> {
        let opened = ump_token::open_token(&ump.token, &ump.presentation_key, password)?;
        let pad = opened
            .profiles
            .iter()
            .find(|p| p.id == entry.profile.id)
            .map(|p| &p.primary_pad)
            .ok_or_else(|| Error::ValidationError("UMP token: profile not in its token".into()))?;
        Ok(ump_token::xor(&opened.root_primary, pad))
    }

    /// Installs `next` in place of `read`, the record the unlock read before
    /// its KDF ran; with a key file open, the file is written before memory
    /// moves, under the same lock. The profiles are those held now, after the
    /// KDF; a record replaced or deleted meanwhile refuses the commit.
    async fn commit_entry(&self, read: &Entry, next: Entry) -> Result<()> {
        let mut entries = self.entries.write().await;
        if !entries
            .get(&read.profile.id)
            .is_some_and(|e| e.is_same_record(read))
        {
            return Err(Error::InvalidOperation(
                "The profile changed during its unlock".into(),
            ));
        }
        let mut now = entries.clone();
        now.insert(next.profile.id.clone(), next);
        let path = self.key_file.read().await.clone();
        if let Some(path) = path {
            let default_id = self.default_profile_id.read().await.clone();
            write_key_file(&path, encode_key_file(&now, default_id.as_deref())?).await?;
        }
        *entries = now;
        Ok(())
    }

    /// Switches to a profile by ID.
    ///
    /// The four cases (design 4.3): a token alone opens with the password
    /// (a wrong one is `Error::AccessDenied`) and nothing is written. A v1
    /// record alone opens as before, salted with the profile id, and a token
    /// joins it with a fresh salt and root. With both, a token that opens
    /// drops the v1 record; a token that refuses is re-sealed under this
    /// password around the key v1 opens to, the v1 record unchanged. Only a
    /// token that opens drops v1, so the same wrong password at two
    /// consecutive unlocks seals the token under it and drops v1: a host
    /// whose builder checks the key and errs refuses the unlock with nothing
    /// written. With a key file open, the last three re-write it atomically
    /// before the wallet is installed. A failed unlock writes nothing. Of two
    /// that write one profile at once, the later is refused
    /// (`Error::InvalidOperation`) and the caller retries.
    pub async fn switch_profile(&self, profile_id: &[u8], password: &str) -> Result<()> {
        let entry = self.entry(profile_id).await?;
        let (config, read) = (self.config.clone(), entry.clone());
        let (primary_key, next) =
            off_worker(password, move |pw| Self::unlock_entry(&config, &read, pw)).await?;
        let pkm = self
            .privileged_key_manager
            .read()
            .await
            .clone()
            .ok_or(Error::AuthenticationRequired)?;
        let wallet = (self.wallet_builder)(primary_key.to_vec(), pkm).await?;
        if let Some(next) = next {
            self.commit_entry(&entry, next).await?;
        }
        *self.underlying.write().await = Some(wallet);
        *self.active_profile_id.write().await = Some(profile_id.to_vec());
        Ok(())
    }

    /// The profile's recovery key, unwrapped from its token with the root
    /// privileged key the password opens: the reference's manager hands it to
    /// a host through `getRecoveryKey` (`CWIStyleWalletManager.ts:2356-2361`
    /// at ts-stack edf6e03) and its `recoveryKeySaver` (:1840).
    ///
    /// # Errors
    ///
    /// `Error::AccessDenied` for a wrong password; `Error::InvalidOperation`
    /// for a v1 record not yet unlocked (it has no recovery key).
    pub async fn recovery_key(
        &self,
        profile_id: &[u8],
        password: &str,
    ) -> Result<Zeroizing<Vec<u8>>> {
        let entry = self.entry(profile_id).await?;
        let ump = entry
            .ump
            .ok_or_else(|| Error::InvalidOperation("v1 profile: unlock it to migrate".into()))?;
        off_worker(password, move |pw| {
            let opened = ump_token::open_token(&ump.token, &ump.presentation_key, pw)?;
            let root_privileged = ump_token::open_root_privileged(&ump.token, &opened)?;
            ump_token::unwrap_factor(&root_privileged, &ump.token.recovery_key_encrypted)
        })
        .await
    }

    /// Deletes a profile by ID.
    pub async fn delete_profile(&self, profile_id: &[u8]) -> Result<()> {
        let mut entries = self.entries.write().await;
        if !entries.contains_key(profile_id) {
            return Err(Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            });
        }
        if self.active_profile_id.read().await.as_deref() == Some(profile_id) {
            return Err(Error::InvalidOperation(
                "Cannot delete the active profile".to_string(),
            ));
        }
        entries.remove(profile_id);
        if self.default_profile_id.read().await.as_deref() == Some(profile_id) {
            let new_default = entries.keys().next().cloned();
            *self.default_profile_id.write().await = new_default;
        }
        Ok(())
    }

    /// Gets all profiles.
    pub async fn get_profiles(&self) -> Vec<Profile> {
        let entries = self.entries.read().await;
        entries.values().map(|e| e.profile.clone()).collect()
    }

    /// Gets the active profile ID.
    pub async fn get_active_profile_id(&self) -> Option<Vec<u8>> {
        self.active_profile_id.read().await.clone()
    }

    /// Gets the default profile ID.
    pub async fn get_default_profile_id(&self) -> Option<Vec<u8>> {
        self.default_profile_id.read().await.clone()
    }

    /// Sets the default profile ID.
    pub async fn set_default_profile_id(&self, profile_id: Vec<u8>) -> Result<()> {
        if !self.entries.read().await.contains_key(&profile_id) {
            return Err(Error::NotFound {
                entity: "Profile".to_string(),
                id: hex::encode(profile_id),
            });
        }
        *self.default_profile_id.write().await = Some(profile_id);
        Ok(())
    }

    /// Exports a profile as encrypted bytes (its fields and its UMP token).
    pub async fn export_profile(&self, profile_id: &[u8]) -> Result<Vec<u8>> {
        let json = self.export_profile_json(profile_id).await?;
        // Encrypt with random key (key is prepended to output)
        let key = generate_random_bytes(32);
        let sym_key = SymmetricKey::from_bytes(&key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let encrypted = sym_key
            .encrypt(&json)
            .map_err(|e| Error::InvalidOperation(format!("Encryption failed: {:?}", e)))?;
        let mut result = key;
        result.extend_from_slice(&encrypted);
        Ok(result)
    }

    /// Imports a profile from encrypted bytes.
    pub async fn import_profile(&self, data: &[u8], _password: &str) -> Result<Profile> {
        if data.len() < 33 {
            return Err(Error::InvalidArgument("Export data too short".to_string()));
        }
        let key = &data[0..32];
        let encrypted = &data[32..];
        let sym_key = SymmetricKey::from_bytes(key)
            .map_err(|e| Error::InvalidOperation(format!("Failed to create key: {:?}", e)))?;
        let decrypted = sym_key
            .decrypt(encrypted)
            .map_err(|e| Error::InvalidOperation(format!("Decryption failed: {:?}", e)))?;
        self.import_profile_json(&decrypted).await
    }

    /// Exports a single profile to JSON bytes (unencrypted).
    ///
    /// The profile's fields with its UMP token beside them (`ump`), the
    /// key file's record; a v1 record has no `ump`. Unlike `export_profile()`,
    /// this does not encrypt the data.
    pub async fn export_profile_json(&self, profile_id: &[u8]) -> Result<Vec<u8>> {
        let entry = self.entry(profile_id).await?;
        serde_json::to_vec(&entry.to_record()).map_err(Error::JsonError)
    }

    /// Imports a profile from JSON bytes (unencrypted).
    ///
    /// Reads a key-file record (a v1 export, with no `ump`, reads as a v1
    /// record) and stores it, overwriting a profile with the same ID.
    pub async fn import_profile_json(&self, data: &[u8]) -> Result<Profile> {
        let record: EntryRecord = serde_json::from_slice(data).map_err(Error::JsonError)?;
        let entry = Entry::from_record(record)?;
        let profile = entry.profile.clone();
        self.entries.write().await.insert(profile.id.clone(), entry);
        Ok(profile)
    }

    /// Backs up all profiles, with their tokens, to a JSON array.
    ///
    /// For the whole key file, use `save_snapshot()`.
    pub async fn backup_all_profiles(&self) -> Result<Vec<u8>> {
        let entries = self.entries.read().await;
        let records: Vec<EntryRecord> = entries.values().map(Entry::to_record).collect();
        serde_json::to_vec(&records).map_err(Error::JsonError)
    }

    /// Restores profiles from a JSON array backup.
    ///
    /// Existing profiles with the same IDs will be overwritten.
    pub async fn restore_all_profiles(&self, data: &[u8]) -> Result<Vec<Profile>> {
        let records: Vec<EntryRecord> = serde_json::from_slice(data).map_err(Error::JsonError)?;
        let restored = records
            .into_iter()
            .map(Entry::from_record)
            .collect::<Result<Vec<_>>>()?;
        let mut entries = self.entries.write().await;
        for entry in &restored {
            entries.insert(entry.profile.id.clone(), entry.clone());
        }
        Ok(restored.into_iter().map(|e| e.profile).collect())
    }

    /// Destroys the manager state, returning to unauthenticated.
    pub async fn destroy(&self) {
        *self.underlying.write().await = None;
        *self.active_profile_id.write().await = None;
        *self.privileged_key_manager.write().await = None;
    }

    /// The key file's bytes: version 3, one UMP token per profile (v1 records
    /// not yet unlocked carried as they are).
    pub async fn save_snapshot(&self) -> Result<Vec<u8>> {
        let entries = self.entries.read().await;
        let default_id = self.default_profile_id.read().await;
        let mut bytes = encode_key_file(&entries, default_id.as_deref())?;
        Ok(std::mem::take(&mut *bytes))
    }

    /// Loads a key file's bytes, v3 or v1, replacing the profiles.
    pub async fn load_snapshot(&self, data: &[u8]) -> Result<()> {
        let (entries, default_id) = decode_key_file(data)?;
        *self.entries.write().await = entries;
        *self.default_profile_id.write().await = default_id;
        Ok(())
    }

    /// Opens the key file at `path` (v3 or v1; an absent file is no profiles)
    /// and remembers it: `save_key_file` and a migrating unlock write it. A
    /// write sibling an interrupted write left beside it is zeroed and removed.
    ///
    /// # Errors
    ///
    /// An I/O error other than not-found, or a file neither version reads.
    pub async fn open_key_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        remove_stale_siblings(&path).await?;
        match tokio::fs::read(&path).await.map(Zeroizing::new) {
            Ok(data) => self.load_snapshot(&data).await?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        *self.key_file.write().await = Some(path);
        Ok(())
    }

    /// Writes the key file `open_key_file` named, atomically (a sibling,
    /// synced, renamed over it); the bytes stay zeroed on drop to the write.
    ///
    /// # Errors
    ///
    /// `Error::InvalidOperation` when no key file is open; an I/O error.
    pub async fn save_key_file(&self) -> Result<()> {
        let path = self
            .key_file
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::InvalidOperation("No key file is open".into()))?;
        let entries = self.entries.read().await;
        let default_id = self.default_profile_id.read().await.clone();
        let bytes = encode_key_file(&entries, default_id.as_deref())?;
        drop(entries);
        write_key_file(&path, bytes).await
    }
}

#[cfg(test)]
#[path = "cwi_style_wallet_manager_tests.rs"]
mod tests;
