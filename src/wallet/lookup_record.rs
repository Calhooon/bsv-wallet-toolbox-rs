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
    /// (`VerifiableCertificate.ts:41-52,182-186`); `verify()` (:550) wants
    /// the values `toBinary` writes ([`Self::is_reference_binary`]).
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
            && self.is_reference_binary()
    }

    /// `Certificate.verify()` answers false on any throw of `toBinary(false)`
    /// or of the signature's parse (ts-sdk `Certificate.ts:425-441` at
    /// ts-stack@edf6e03), and `decodeIdentityOutput` then drops the entry.
    /// The port verifies no signature yet (#495); these are the values the
    /// reference refuses before it verifies.
    fn is_reference_binary(&self) -> bool {
        let (Some(outpoint), Some(signature)) = (&self.revocation_outpoint, &self.signature) else {
            return false;
        };
        let mut length = 32 * 3 + 33 * 2;
        let Some(index) = revocation_index(outpoint) else {
            return false;
        };
        length += compact_size(index as usize) + compact_size(self.fields.len());
        for (name, value) in &self.fields {
            // :73-81,117-126,288-289: a name of 1 to 50 bytes, not an object's
            // own structure; a value of at most 1 MiB.
            if !(1..=50).contains(&name.len())
                || matches!(name.as_str(), "__proto__" | "constructor" | "prototype")
                || value.len() > MAX_CERTIFICATE_FIELD_VALUE_BYTES
            {
                return false;
            }
            length += compact_size(name.len()) + name.len();
            length += compact_size(value.len()) + value.len();
        }
        let Some(signature) = signature_bytes(signature) else {
            return false;
        };
        base64_32(&self.type_id)
            && base64_32(&self.serial_number)
            && compressed_key(&self.subject)
            && compressed_key(&self.certifier)
            // :315: the written binary's bound.
            && length + signature.len() <= MAX_CERTIFICATE_BINARY_BYTES
    }
}

/// `MAX_CERTIFICATE_FIELD_VALUE_BYTES` (`Certificate.ts:27`).
const MAX_CERTIFICATE_FIELD_VALUE_BYTES: usize = 1024 * 1024;
/// `MAX_CERTIFICATE_BINARY_BYTES` (`Certificate.ts:25`).
const MAX_CERTIFICATE_BINARY_BYTES: usize = 16 * 1024 * 1024;

/// `type` and `serialNumber`: canonical base64 of 32 bytes
/// (`Certificate.ts:30-43,233,237`; `base64ToBytes`,
/// `WalletByteEncoding.ts:39-52`).
fn base64_32(value: &str) -> bool {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD
        .decode(value)
        .is_ok_and(|bytes| bytes.len() == 32 && STANDARD.encode(bytes) == value)
}

/// `subject` and `certifier`: a compressed public key (`Certificate.ts:241-254`).
fn compressed_key(value: &str) -> bool {
    value.len() == 66
        && (value.starts_with("02") || value.starts_with("03"))
        && bsv_rs::primitives::PublicKey::from_hex(value).is_ok()
}

/// `<txid>[.<index>]` (`Certificate.ts:260-274`): 64 hex digits, then an
/// index in canonical decimal of at most `0xffffffff`, `0` when absent.
fn revocation_index(value: &str) -> Option<u32> {
    let mut parts = value.split('.');
    let txid = parts.next()?;
    let index = parts.next().unwrap_or("0");
    if parts.next().is_some()
        || txid.len() != 64
        || !txid.bytes().all(|b| b.is_ascii_hexdigit())
        || index.is_empty()
        || !index.bytes().all(|b| b.is_ascii_digit())
        || (index.len() > 1 && index.starts_with('0'))
    {
        return None;
    }
    index.parse().ok()
}

/// The signature `verify()` parses (`Certificate.ts:434`): hex of a DER
/// signature (`Signature.fromDER`, `Signature.ts:44-97`), at most 72 bytes
/// as `toBinary` writes it (:300-311). An empty signature fails the parse.
/// The toolbox wants the reference client's canon, strict DER with each
/// integer minimally encoded and in 1..n (`isCanonicalDERSignature`,
/// `Secp256k1Validation.ts:33-51`, applied at
/// `WalletResultValidation.ts:1529-1531`): `fromDER` also reads an integer
/// whose high bit lacks its zero pad, an entry the reference keeps and its
/// client refuses (`docs/ARCHITECTURE.md` section 7).
fn signature_bytes(value: &str) -> Option<Vec<u8>> {
    let bytes = hex::decode(value).ok()?;
    let canonical = bytes.len() >= 8
        && bytes.len() <= 72
        && bytes[0] == 0x30
        && usize::from(bytes[1]) == bytes.len() - 2
        && der_integer(&bytes, 2).and_then(|at| der_integer(&bytes, at)) == Some(bytes.len());
    canonical.then_some(bytes)
}

/// One canonical DER integer at `at`: tag, a length of 1 to 33, no sign bit,
/// no needless zero pad, a value in 1..n. Answers the offset past it.
fn der_integer(bytes: &[u8], at: usize) -> Option<usize> {
    if *bytes.get(at)? != 0x02 {
        return None;
    }
    let length = usize::from(*bytes.get(at + 1)?);
    let scalar = bytes.get(at + 2..at + 2 + length)?;
    if !(1..=33).contains(&length)
        || scalar[0] & 0x80 != 0
        || (length > 1 && scalar[0] == 0 && scalar[1] & 0x80 == 0)
    {
        return None;
    }
    let digits = &scalar[scalar.iter().position(|&b| b != 0)?..];
    let mut padded = [0u8; 32];
    padded
        .get_mut(32usize.checked_sub(digits.len())?..)?
        .copy_from_slice(digits);
    // A secret key's range is the scalar's, 1..n.
    bsv_rs::primitives::PrivateKey::from_bytes(&padded).ok()?;
    Some(at + 2 + length)
}

fn compact_size(value: usize) -> usize {
    match value {
        0..=252 => 1,
        253..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
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

#[cfg(test)]
mod tests {
    /// An overlay output's record with every value well formed; the
    /// signature is the smallest canonical DER one (r = s = 1).
    fn record() -> serde_json::Value {
        serde_json::json!({
            "type": "A".repeat(43) + "=",
            "serialNumber": "B".repeat(42) + "A=",
            "subject": format!("02{}", "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"),
            "certifier": format!("02{}", "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"),
            "revocationOutpoint": format!("{}.0", "00".repeat(32)),
            "fields": {"name": "AA=="},
            "keyring": {"name": "AA=="},
            "signature": "3006020101020101",
        })
    }

    /// The wire parser drops an empty signature as `verify()` does
    /// (`Certificate.ts:434,436-439`); before the fix it parsed.
    #[test]
    fn the_wire_parser_drops_an_empty_signature() {
        let mut wire = record();
        super::parse(&wire.to_string()).unwrap();
        wire["signature"] = serde_json::Value::String(String::new());
        assert!(super::parse(&wire.to_string()).is_err());
    }
}
