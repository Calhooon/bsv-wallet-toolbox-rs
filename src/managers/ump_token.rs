//! The reference's UMP token and snapshot layouts (#560).
//!
//! `UmpToken` is the reference's `UMPToken` (`CWIStyleWalletManager.ts:415-498`
//! at ts-stack edf6e03) in its binary serialization (`serializeUMPToken`,
//! :2648-2716; `deserializeUMPToken`, :2721-2817); `WalletSnapshot` is its
//! `saveSnapshot` layout (:1960-2004). The design is
//! `docs/design/ump-token-format.md`.

use bsv_rs::primitives::{sha256, PrivateKey, SymmetricKey, Writer};
use bsv_rs::wallet::{Counterparty, KeyDeriver, Protocol, SecurityLevel};
use ring::pbkdf2;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use super::cwi_style_wallet_manager::Profile;
use crate::error::{Error, Result};

/// PBKDF2 rounds of a token with no KDF metadata (`PBKDF2_NUM_ROUNDS`, :83).
pub const PBKDF2_NUM_ROUNDS: u32 = 7777;

/// The on-chain UMP version of a token with KDF metadata (:157).
pub const UMP_VERSION: u8 = 3;

/// The largest field the reference reads (`MAX_STATE_SNAPSHOT_BYTES`, :97).
const MAX_FIELD_BYTES: usize = 16 * 1024 * 1024;

/// The reference's key-wrapping protocol and key id (:1856-1858).
const WRAP_PROTOCOL: &str = "admin key wrapping";
const WRAP_KEY_ID: &str = "1";

/// A token not on chain is written with the null txid at index 0: the
/// reference's pattern takes it (:2786-2789) and no transaction has it; the
/// reference publishes before it serializes (:1900, :1972) (design 4.1).
pub(crate) const UNPUBLISHED_OUTPOINT: &str =
    "0000000000000000000000000000000000000000000000000000000000000000.0";

/// A password KDF the reference names (`passwordKdf.algorithm`, :487).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KdfAlgorithm {
    /// `pbkdf2-sha512`: PBKDF2 HMAC-SHA512 at `iterations`, 32 bytes out.
    Pbkdf2Sha512,
    /// `argon2id`: parsed and carried; deriving with it is stage two (#560).
    Argon2id,
}

impl KdfAlgorithm {
    /// The reference's name for the algorithm.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pbkdf2Sha512 => "pbkdf2-sha512",
            Self::Argon2id => "argon2id",
        }
    }
}

/// The reference's `passwordKdf` metadata (:486-492).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordKdf {
    /// The algorithm.
    pub algorithm: KdfAlgorithm,
    /// Rounds (PBKDF2) or passes (argon2id).
    pub iterations: u32,
    /// Argon2id memory in KiB (`memoryKiB`).
    pub memory_kib: Option<u32>,
    /// Argon2id lanes (`parallelism`).
    pub parallelism: Option<u32>,
    /// Output length in bytes (`hashLength`); PBKDF2 derives 32 whatever it says.
    pub hash_length: Option<u32>,
}

/// The KDF params JSON, keys in the reference's order (:2692-2703).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KdfParams {
    iterations: u32,
    #[serde(rename = "memoryKiB", skip_serializing_if = "Option::is_none")]
    memory_kib: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash_length: Option<u32>,
}

fn in_range(value: Option<u32>, max: u32) -> bool {
    value.is_some_and(|v| v > 0 && v <= max)
}

impl PasswordKdf {
    /// `pbkdf2-sha512` at `iterations`, the other three at the argon2id
    /// defaults the reference's constructor fills into its `Required<KdfConfig>`
    /// (:1679-1686, :88-91), so the params JSON is the reference's byte for byte.
    #[must_use]
    pub fn pbkdf2(iterations: u32) -> Self {
        Self {
            algorithm: KdfAlgorithm::Pbkdf2Sha512,
            iterations,
            memory_kib: Some(131_072),
            parallelism: Some(1),
            hash_length: Some(32),
        }
    }

    /// The reference's `isValidKdfConfig` bounds (:103-122).
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self.algorithm {
            KdfAlgorithm::Pbkdf2Sha512 => {
                in_range(Some(self.iterations), 10_000_000)
                    && (self.hash_length.is_none() || in_range(self.hash_length, 64))
            }
            KdfAlgorithm::Argon2id => {
                in_range(Some(self.iterations), 20)
                    && in_range(self.memory_kib, 262_144)
                    && in_range(self.parallelism, 16)
                    && in_range(self.hash_length, 64)
            }
        }
    }

    fn params_json(&self) -> Vec<u8> {
        let params = KdfParams {
            iterations: self.iterations,
            memory_kib: self.memory_kib,
            parallelism: self.parallelism,
            hash_length: self.hash_length,
        };
        serde_json::to_vec(&params).unwrap_or_default()
    }

    fn from_parts(algorithm: &[u8], params: &[u8]) -> Result<Self> {
        let algorithm = match algorithm {
            b"pbkdf2-sha512" => KdfAlgorithm::Pbkdf2Sha512,
            b"argon2id" => KdfAlgorithm::Argon2id,
            _ => return Err(token_error("unsupported KDF algorithm")),
        };
        let p: KdfParams = serde_json::from_slice(params)?;
        let kdf = Self {
            algorithm,
            iterations: p.iterations,
            memory_kib: p.memory_kib,
            parallelism: p.parallelism,
            hash_length: p.hash_length,
        };
        if kdf.is_valid() {
            Ok(kdf)
        } else {
            Err(token_error("unsupported or unsafe KDF parameters"))
        }
    }
}

/// The reference's User Management Protocol token (`UMPToken`, :415-498),
/// in its binary layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UmpToken {
    /// Field 0: the KDF salt, 32 random bytes for a new token.
    pub password_salt: Vec<u8>,
    /// Field 1: root primary key under presentation XOR password key.
    pub password_presentation_primary: Vec<u8>,
    /// Field 2: root primary key under recovery XOR password key.
    pub password_recovery_primary: Vec<u8>,
    /// Field 3: root primary key under presentation XOR recovery key.
    pub presentation_recovery_primary: Vec<u8>,
    /// Field 4: root privileged key under root primary XOR password key.
    pub password_primary_privileged: Vec<u8>,
    /// Field 5: root privileged key under presentation XOR recovery key.
    pub presentation_recovery_privileged: Vec<u8>,
    /// Field 6: SHA-256 of the presentation key.
    pub presentation_hash: Vec<u8>,
    /// Field 7: SHA-256 of the recovery key.
    pub recovery_hash: Vec<u8>,
    /// Field 8: the presentation key wrapped under the root privileged key.
    pub presentation_key_encrypted: Vec<u8>,
    /// Field 9: the password key wrapped under the root privileged key.
    pub password_key_encrypted: Vec<u8>,
    /// Field 10: the recovery key wrapped under the root privileged key.
    pub recovery_key_encrypted: Vec<u8>,
    /// Field 11: the profiles' JSON under the root primary key.
    pub profiles_encrypted: Option<Vec<u8>>,
    /// The on-chain UMP version, 3 when `password_kdf` is present.
    pub ump_version: Option<u8>,
    /// The KDF metadata; `None` on a legacy token (PBKDF2 at 7777).
    pub password_kdf: Option<PasswordKdf>,
    /// The token's outpoint once published; the toolbox publishes nothing.
    pub current_outpoint: Option<String>,
}

fn token_error(what: &str) -> Error {
    Error::ValidationError(format!("UMP token: {what}"))
}

/// A cursor over the reference's binary layouts.
struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.bytes.len() {
            return Err(token_error("truncated"));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// A Bitcoin varint in its shortest form.
    fn varint(&mut self) -> Result<usize> {
        let (width, floor) = match self.byte()? {
            0xfd => (2, 0xfd),
            0xfe => (4, 0x1_0000),
            0xff => (8, 0x1_0000_0000),
            n => return Ok(usize::from(n)),
        };
        let mut le = [0u8; 8];
        le[..width].copy_from_slice(self.take(width)?);
        let n = u64::from_le_bytes(le);
        if n < floor {
            return Err(token_error("non-minimal varint"));
        }
        usize::try_from(n).map_err(|_| token_error("length overflow"))
    }

    /// A field of 1 to 16 MiB (`readArray`, :2728-2734).
    fn field(&mut self) -> Result<Vec<u8>> {
        let n = self.varint()?;
        if n == 0 || n > MAX_FIELD_BYTES {
            return Err(token_error("field exceeds allowed bounds"));
        }
        Ok(self.take(n)?.to_vec())
    }
}

/// The reference's outpoint check (:2786-2789): `^[^\s.:]+[.:]\d+$`, 128 at most.
fn is_outpoint(s: &str) -> bool {
    let Some(at) = s.rfind(['.', ':']) else {
        return false;
    };
    let (txid, index) = (&s[..at], &s[at + 1..]);
    s.chars().count() <= 128
        && !txid.is_empty()
        && !txid.contains(|c: char| c.is_whitespace() || c == '.' || c == ':')
        && !index.is_empty()
        && index.bytes().all(|b| b.is_ascii_digit())
}

impl UmpToken {
    /// The eleven fixed fields in the serialization's order (:2663-2673).
    fn fixed_fields(&self) -> [&Vec<u8>; 11] {
        [
            &self.password_salt,
            &self.password_presentation_primary,
            &self.password_recovery_primary,
            &self.presentation_recovery_primary,
            &self.password_primary_privileged,
            &self.presentation_recovery_privileged,
            &self.presentation_hash,
            &self.recovery_hash,
            &self.presentation_key_encrypted,
            &self.password_key_encrypted,
            &self.recovery_key_encrypted,
        ]
    }

    /// The reference's `serializeUMPToken` (:2648-2716): version 3 with KDF
    /// metadata, else 2. A token with no outpoint is written with
    /// `UNPUBLISHED_OUTPOINT`, which the reference's reader takes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let kdf = match (self.ump_version, &self.password_kdf) {
            (Some(UMP_VERSION), Some(kdf)) => Some(kdf),
            _ => None,
        };
        let mut out = Writer::new();
        out.write_u8(if kdf.is_some() { 3 } else { 2 });
        for field in self.fixed_fields() {
            out.write_var_bytes(field);
        }
        match &self.profiles_encrypted {
            Some(p) if !p.is_empty() => out.write_u8(1).write_var_bytes(p),
            _ => out.write_u8(0),
        };
        if let Some(kdf) = kdf {
            out.write_u8(1).write_u8(UMP_VERSION);
            out.write_var_bytes(kdf.algorithm.as_str().as_bytes());
            out.write_var_bytes(&kdf.params_json());
        }
        let outpoint = self.current_outpoint.as_deref();
        out.write_var_bytes(outpoint.unwrap_or(UNPUBLISHED_OUTPOINT).as_bytes());
        out.into_bytes()
    }

    /// The reference's `deserializeUMPToken` (:2721-2817): versions 1 to 3,
    /// its field bounds, its KDF bounds and its outpoint pattern.
    /// `UNPUBLISHED_OUTPOINT` reads as `current_outpoint: None`.
    ///
    /// # Errors
    ///
    /// `Error::ValidationError` for a layout or bound the reference refuses;
    /// `Error::JsonError` for KDF params that are not JSON.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Cursor { bytes };
        let version = r.byte()?;
        if !(1..=3).contains(&version) {
            return Err(token_error("unsupported serialization version"));
        }
        let mut f = Vec::with_capacity(11);
        for _ in 0..11 {
            f.push(r.field()?);
        }
        let profiles_encrypted = if version >= 2 && r.byte()? == 1 {
            Some(r.field()?)
        } else {
            None
        };
        let (mut ump_version, mut password_kdf) = (None, None);
        if version == 3 && r.byte()? == 1 {
            ump_version = Some(r.byte()?);
            let algorithm = r.field()?;
            password_kdf = Some(PasswordKdf::from_parts(&algorithm, &r.field()?)?);
        }
        let n = r.varint()?;
        let outpoint = String::from_utf8(r.take(n)?.to_vec())
            .map_err(|_| token_error("outpoint is not UTF-8"))?;
        if !is_outpoint(&outpoint) {
            return Err(token_error("invalid outpoint"));
        }
        let mut f = f.into_iter();
        let mut next = || f.next().unwrap_or_default();
        Ok(Self {
            password_salt: next(),
            password_presentation_primary: next(),
            password_recovery_primary: next(),
            presentation_recovery_primary: next(),
            password_primary_privileged: next(),
            presentation_recovery_privileged: next(),
            presentation_hash: next(),
            recovery_hash: next(),
            presentation_key_encrypted: next(),
            password_key_encrypted: next(),
            recovery_key_encrypted: next(),
            profiles_encrypted,
            ump_version,
            password_kdf,
            current_outpoint: (outpoint != UNPUBLISHED_OUTPOINT).then_some(outpoint),
        })
    }
}

/// The reference's state snapshot (`saveSnapshot`, :1960-2004): `version`
/// (2; 1 has no profile id), `snapshot_key` (32), `active_profile_id` (16),
/// `encrypted_payload` (the root primary key and a serialized token under
/// the snapshot key). The key sits beside its ciphertext: a session cache,
/// not a password-protected file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletSnapshot {
    /// Snapshot format version.
    pub version: u8,
    /// The 32-byte key the payload is encrypted under.
    pub snapshot_key: Vec<u8>,
    /// The active profile's 16-byte id (all zero for the default profile).
    pub active_profile_id: Vec<u8>,
    /// The encrypted payload.
    pub encrypted_payload: Vec<u8>,
}

impl WalletSnapshot {
    /// Current snapshot version.
    pub const CURRENT_VERSION: u8 = 2;

    /// A version 2 snapshot.
    #[must_use]
    pub fn new(
        snapshot_key: Vec<u8>,
        active_profile_id: Vec<u8>,
        encrypted_payload: Vec<u8>,
    ) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            snapshot_key,
            active_profile_id,
            encrypted_payload,
        }
    }

    /// The reference's byte layout (:1984-1989).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![self.version];
        out.extend_from_slice(&self.snapshot_key);
        if self.version >= 2 {
            out.extend_from_slice(&self.active_profile_id);
        }
        out.extend_from_slice(&self.encrypted_payload);
        out
    }

    /// The reference's `loadSnapshot` framing (:2023-2036), versions 1 and 2.
    ///
    /// # Errors
    ///
    /// `Error::ValidationError` for another version or a short snapshot.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Cursor { bytes };
        let version = r.byte()?;
        let id_len = match version {
            1 => 0,
            2 => 16,
            _ => return Err(token_error("unsupported snapshot version")),
        };
        let snapshot_key = r.take(32)?.to_vec();
        let mut active_profile_id = r.take(id_len)?.to_vec();
        if version == 1 {
            active_profile_id = vec![0; 16];
        }
        Ok(Self {
            version,
            snapshot_key,
            active_profile_id,
            encrypted_payload: r.bytes.to_vec(),
        })
    }
}

/// A profile as the reference stores it inside `profilesEncrypted` (`Profile`,
/// :361-386): byte arrays as JSON arrays, `createdAt` in seconds; pads zeroed.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RefProfile {
    pub(crate) name: String,
    pub(crate) id: Vec<u8>,
    pub(crate) primary_pad: Vec<u8>,
    pub(crate) privileged_pad: Vec<u8>,
    pub(crate) created_at: i64,
}

impl Drop for RefProfile {
    fn drop(&mut self) {
        self.primary_pad.zeroize();
        self.privileged_pad.zeroize();
    }
}

impl ZeroizeOnDrop for RefProfile {}

/// The five keys a token is built from; each is zeroed on drop.
pub(crate) struct Factors {
    pub(crate) password_key: Zeroizing<Vec<u8>>,
    pub(crate) presentation_key: Zeroizing<Vec<u8>>,
    pub(crate) recovery_key: Zeroizing<Vec<u8>>,
    pub(crate) root_primary: Zeroizing<Vec<u8>>,
    pub(crate) root_privileged: Zeroizing<Vec<u8>>,
}

pub(crate) fn xor(a: &[u8], b: &[u8]) -> Zeroizing<Vec<u8>> {
    Zeroizing::new(a.iter().zip(b).map(|(x, y)| x ^ y).collect())
}

fn seal(key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    Ok(SymmetricKey::from_bytes(key)?.encrypt(plaintext)?)
}

/// Opens a GCM field; a tag that does not verify is a wrong factor.
fn open(key: &[u8], ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let plain = SymmetricKey::from_bytes(key)?
        .decrypt(ciphertext)
        .map_err(|_| Error::AccessDenied("the password does not open this UMP token".into()))?;
    Ok(Zeroizing::new(plain))
}

/// The BRC-42 key `ProtoWallet::encrypt` derives under `[2, 'admin key
/// wrapping']`, key `1`, counterparty self, taken here so the plaintext is
/// borrowed, never copied into an `EncryptArgs` that drops it unzeroed.
fn wrapping_key(root_privileged: &[u8]) -> Result<SymmetricKey> {
    let deriver = KeyDeriver::new(Some(PrivateKey::from_bytes(root_privileged)?));
    let protocol = Protocol::new(SecurityLevel::Counterparty, WRAP_PROTOCOL);
    Ok(deriver.derive_symmetric_key(&protocol, WRAP_KEY_ID, &Counterparty::Self_)?)
}

/// `PrivilegedKeyManager.encrypt` under `[2, 'admin key wrapping']`, key `1`,
/// counterparty self (:1856-1858).
fn wrap(root_privileged: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    Ok(wrapping_key(root_privileged)?.encrypt(plaintext)?)
}

/// The reference's `getFactor` (:2456-2510): a wrapped copy opened with the
/// root privileged key.
pub(crate) fn unwrap_factor(
    root_privileged: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    Ok(Zeroizing::new(
        wrapping_key(root_privileged)?.decrypt(ciphertext)?,
    ))
}

/// PBKDF2 HMAC-SHA512, 32 bytes out.
pub(crate) fn pbkdf2_sha512(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<Zeroizing<Vec<u8>>> {
    let rounds = std::num::NonZeroU32::new(iterations)
        .ok_or_else(|| Error::InvalidArgument("PBKDF2 rounds must be positive".into()))?;
    let mut derived = Zeroizing::new(vec![0u8; 32]);
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA512,
        rounds,
        salt,
        password.as_bytes(),
        &mut derived,
    );
    Ok(derived)
}

/// The reference's `derivePasswordKey` (:303-358) over a salt and a KDF: none
/// is the legacy 7777 rounds; argon2id is stage two and refused here.
pub(crate) fn derive_password_key(
    salt: &[u8],
    kdf: Option<&PasswordKdf>,
    password: &str,
) -> Result<Zeroizing<Vec<u8>>> {
    match kdf {
        None => pbkdf2_sha512(password, salt, PBKDF2_NUM_ROUNDS),
        Some(kdf) if !kdf.is_valid() => Err(token_error("unsupported or unsafe KDF parameters")),
        Some(kdf) if kdf.algorithm == KdfAlgorithm::Pbkdf2Sha512 => {
            pbkdf2_sha512(password, salt, kdf.iterations)
        }
        Some(_) => Err(Error::InvalidOperation(
            "UMP token asks argon2id; this toolbox derives pbkdf2-sha512 only (#560 stage two)"
                .into(),
        )),
    }
}

/// Builds a v3 token from its factors, as `createNewUser` and `updateFactors`
/// do (:1860-1876, :2557-2597).
pub(crate) fn build_token(
    salt: Vec<u8>,
    kdf: PasswordKdf,
    k: &Factors,
    profiles: &[RefProfile],
) -> Result<UmpToken> {
    let pres_pass = xor(&k.presentation_key, &k.password_key);
    let pres_rec = xor(&k.presentation_key, &k.recovery_key);
    let rec_pass = xor(&k.recovery_key, &k.password_key);
    let prim_pass = xor(&k.root_primary, &k.password_key);
    let profiles_json = Zeroizing::new(serde_json::to_vec(profiles)?);
    Ok(UmpToken {
        password_salt: salt,
        password_presentation_primary: seal(&pres_pass, &k.root_primary)?,
        password_recovery_primary: seal(&rec_pass, &k.root_primary)?,
        presentation_recovery_primary: seal(&pres_rec, &k.root_primary)?,
        password_primary_privileged: seal(&prim_pass, &k.root_privileged)?,
        presentation_recovery_privileged: seal(&pres_rec, &k.root_privileged)?,
        presentation_hash: sha256(&k.presentation_key).to_vec(),
        recovery_hash: sha256(&k.recovery_key).to_vec(),
        presentation_key_encrypted: wrap(&k.root_privileged, &k.presentation_key)?,
        password_key_encrypted: wrap(&k.root_privileged, &k.password_key)?,
        recovery_key_encrypted: wrap(&k.root_privileged, &k.recovery_key)?,
        profiles_encrypted: Some(seal(&k.root_primary, &profiles_json)?),
        ump_version: Some(UMP_VERSION),
        password_kdf: Some(kdf),
        current_outpoint: None,
    })
}

/// The root keys and profiles a token opens to with the presentation key and
/// the password (`unlockExisting`, :1805-1830, and `setupRoot`, :2823-2895).
pub(crate) struct Opened {
    pub(crate) password_key: Zeroizing<Vec<u8>>,
    pub(crate) root_primary: Zeroizing<Vec<u8>>,
    pub(crate) profiles: Vec<RefProfile>,
}

/// Opens a token with the presentation key and the password.
pub(crate) fn open_token(
    token: &UmpToken,
    presentation_key: &[u8],
    password: &str,
) -> Result<Opened> {
    let password_key =
        derive_password_key(&token.password_salt, token.password_kdf.as_ref(), password)?;
    let root_primary = open(
        &xor(presentation_key, &password_key),
        &token.password_presentation_primary,
    )?;
    let profiles = match &token.profiles_encrypted {
        Some(p) if !p.is_empty() => serde_json::from_slice(&open(&root_primary, p)?)?,
        _ => Vec::new(),
    };
    Ok(Opened {
        password_key,
        root_primary,
        profiles,
    })
}

/// The root privileged key under `root primary XOR password key` (field 4).
pub(crate) fn open_root_privileged(
    token: &UmpToken,
    opened: &Opened,
) -> Result<Zeroizing<Vec<u8>>> {
    open(
        &xor(&opened.root_primary, &opened.password_key),
        &token.password_primary_privileged,
    )
}

// The key file (docs/design/ump-token-format.md 4.2).

/// The key-file version written since #560; v1 had no version field.
const KEY_FILE_VERSION: u32 = 3;

/// A profile's reference UMP token and the presentation key that opens it with
/// the password.
#[derive(Clone)]
pub(crate) struct UmpEntry {
    pub(crate) presentation_key: Zeroizing<Vec<u8>>,
    pub(crate) token: UmpToken,
}

/// A profile of the key file: its clear index and, once migrated, its token;
/// `ump: None` is a v1 record, salted with the profile id, until its unlock.
#[derive(Clone)]
pub(crate) struct Entry {
    pub(crate) profile: Profile,
    pub(crate) ump: Option<UmpEntry>,
}

/// `Entry` on disk: the profile's fields with an `ump` object beside them.
#[derive(Serialize, Deserialize)]
pub(crate) struct EntryRecord {
    #[serde(flatten)]
    pub(crate) profile: Profile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ump: Option<UmpRecord>,
}

/// The token's record; the presentation key is zeroed on drop.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UmpRecord {
    #[serde(with = "super::cwi_style_wallet_manager::base64_bytes")]
    pub(crate) presentation_key: Zeroizing<Vec<u8>>,
    /// The reference's `serializeUMPToken` bytes.
    #[serde(with = "super::cwi_style_wallet_manager::base64_bytes")]
    pub(crate) token: Vec<u8>,
}

impl Entry {
    pub(crate) fn to_record(&self) -> EntryRecord {
        EntryRecord {
            profile: self.profile.clone(),
            ump: self.ump.as_ref().map(|u| UmpRecord {
                presentation_key: u.presentation_key.clone(),
                token: u.token.to_bytes(),
            }),
        }
    }

    pub(crate) fn from_record(record: EntryRecord) -> Result<Self> {
        let ump = match record.ump {
            Some(u) => Some(UmpEntry {
                presentation_key: u.presentation_key,
                token: UmpToken::from_bytes(&u.token)?,
            }),
            None => None,
        };
        Ok(Self {
            profile: record.profile,
            ump,
        })
    }
}

/// The v3 key file (`docs/design/ump-token-format.md` 4.2).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KeyFile {
    pub(crate) key_file_version: u32,
    pub(crate) default_profile_id: Option<String>,
    pub(crate) profiles: Vec<EntryRecord>,
}

/// The v1 key file's payload, under the 32-byte key written before it.
#[derive(Deserialize)]
struct LegacySnapshot {
    profiles: Vec<Profile>,
    default_profile_id: Option<Vec<u8>>,
}

pub(crate) type Entries = HashMap<Vec<u8>, Entry>;

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// The key file's bytes, zeroed on drop. The buffer is sized ahead (8 KiB a
/// profile; a token's record is under 3 KiB) so serde does not grow it and
/// leave a freed copy behind.
pub(crate) fn encode_key_file(
    entries: &Entries,
    default_id: Option<&[u8]>,
) -> Result<Zeroizing<Vec<u8>>> {
    use base64::Engine;
    let mut profiles: Vec<&Entry> = entries.values().collect();
    profiles.sort_by(|a, b| a.profile.id.cmp(&b.profile.id));
    let file = KeyFile {
        key_file_version: KEY_FILE_VERSION,
        default_profile_id: default_id.map(|id| b64().encode(id)),
        profiles: profiles.into_iter().map(Entry::to_record).collect(),
    };
    let mut out = Zeroizing::new(Vec::with_capacity(1024 + 8192 * file.profiles.len()));
    serde_json::to_writer_pretty(&mut *out, &file)?;
    Ok(out)
}

/// Reads a v3 key file, else a v1 one.
pub(crate) fn decode_key_file(data: &[u8]) -> Result<(Entries, Option<Vec<u8>>)> {
    use base64::Engine;
    if let Ok(file) = serde_json::from_slice::<KeyFile>(data) {
        if file.key_file_version != KEY_FILE_VERSION {
            return Err(Error::InvalidArgument(
                "Unsupported key-file version".into(),
            ));
        }
        let mut entries = Entries::new();
        for record in file.profiles {
            let entry = Entry::from_record(record)?;
            entries.insert(entry.profile.id.clone(), entry);
        }
        let default_id = match file.default_profile_id {
            Some(id) => Some(
                b64()
                    .decode(id)
                    .map_err(|e| Error::InvalidArgument(e.to_string()))?,
            ),
            None => None,
        };
        return Ok((entries, default_id));
    }
    if data.len() < 33 {
        return Err(Error::InvalidArgument("Snapshot too short".to_string()));
    }
    let decrypted = SymmetricKey::from_bytes(&data[0..32])
        .and_then(|key| key.decrypt(&data[32..]))
        .map(Zeroizing::new)
        .map_err(|e| Error::InvalidOperation(format!("Decryption failed: {e:?}")))?;
    let legacy: LegacySnapshot = serde_json::from_slice(&decrypted)?;
    let entries = legacy
        .profiles
        .into_iter()
        .map(|profile| (profile.id.clone(), Entry { profile, ump: None }))
        .collect();
    Ok((entries, legacy.default_profile_id))
}

#[cfg(test)]
#[path = "ump_token_tests.rs"]
mod tests;
