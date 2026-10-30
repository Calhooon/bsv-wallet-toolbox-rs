//! The reference's bounds on an overlay identity certificate: an entry that
//! fails one is dropped there (`decodeIdentityOutput` answers `null`,
//! ts-stack@edf6e03 `identityUtils.ts:493-552`), never filled with defaults.

use super::OverlayCertificate;
use crate::Result;

/// `MAX_IDENTITY_FIELDS` (`identityUtils.ts:24`).
const MAX_IDENTITY_FIELDS: usize = 100;
/// `MAX_KEYRING_VALUE_BYTES` (`VerifiableCertificate.ts:18`), the bound
/// `certificateRecord` also puts on the value's length.
const MAX_KEYRING_VALUE_BYTES: usize = 2048;
/// The certificate's own properties, each required, none other allowed
/// (`identityUtils.ts:90-106`).
const CERTIFICATE_PROPERTIES: [&str; 8] = [
    "type",
    "serialNumber",
    "subject",
    "certifier",
    "revocationOutpoint",
    "fields",
    "keyring",
    "signature",
];

impl OverlayCertificate {
    /// `certificateRecord` (:87-129) wants the revocation outpoint and the
    /// signature present, 1 to 100 fields and keyring entries, every keyring
    /// name a field and every value at most 2048 long; `decryptFields` wants
    /// each keyring value canonical base64 of 1 to 2048 bytes
    /// (`VerifiableCertificate.ts:41-52,182-186`).
    pub(crate) fn is_reference_record(&self) -> bool {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let cardinality = |n: usize| (1..=MAX_IDENTITY_FIELDS).contains(&n);
        let key = |value: &String| {
            value.encode_utf16().count() <= MAX_KEYRING_VALUE_BYTES
                && STANDARD
                    .decode(value)
                    .is_ok_and(|bytes| (1..=MAX_KEYRING_VALUE_BYTES).contains(&bytes.len()))
        };
        self.revocation_outpoint.is_some()
            && self.signature.is_some()
            && cardinality(self.fields.len())
            && cardinality(self.keyring.len())
            && self
                .keyring
                .iter()
                .all(|(name, value)| self.fields.contains_key(name) && key(value))
    }
}

/// An overlay output's certificate JSON as the reference's `certificateRecord`
/// reads it: exactly its eight properties, then the bounds above.
pub(super) fn parse(json: &str) -> Result<OverlayCertificate> {
    let record: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json)?;
    if record.len() != CERTIFICATE_PROPERTIES.len()
        || !CERTIFICATE_PROPERTIES
            .iter()
            .all(|p| record.contains_key(*p))
    {
        return Err(crate::Error::ServiceError(
            "Identity certificate properties are not the reference's".into(),
        ));
    }
    let cert: OverlayCertificate = serde_json::from_value(serde_json::Value::Object(record))?;
    if !cert.is_reference_record() {
        return Err(crate::Error::ServiceError(
            "Identity certificate is outside the reference's bounds".into(),
        ));
    }
    Ok(cert)
}
