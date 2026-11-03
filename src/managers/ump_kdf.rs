//! The password KDF of a UMP token (#560, #593): the reference's
//! `derivePasswordKey` (`CWIStyleWalletManager.ts:303-358` at ts-stack
//! edf6e03) and its `passwordKdf` metadata. The design is
//! `docs/design/ump-token-format.md`.

use ring::pbkdf2;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::token_error;
use crate::error::{Error, Result};

/// PBKDF2 rounds of a token with no KDF metadata (`PBKDF2_NUM_ROUNDS`, :83).
pub const PBKDF2_NUM_ROUNDS: u32 = 7777;

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

    pub(super) fn params_json(&self) -> Vec<u8> {
        let params = KdfParams {
            iterations: self.iterations,
            memory_kib: self.memory_kib,
            parallelism: self.parallelism,
            hash_length: self.hash_length,
        };
        serde_json::to_vec(&params).unwrap_or_default()
    }

    pub(super) fn from_parts(algorithm: &[u8], params: &[u8]) -> Result<Self> {
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
