//! The checks the lookup resolver holds on what an answering SLAP tracker or
//! overlay host names, ported from the reference where bsv-rs exports no
//! function doing the same check:
//!
//! - a tracker's advertisement (`OverlayAdminTokenTemplate.ts`
//!   `decodeAndVerifyOverlayAdvertisement`, called by `LookupResolver.ts`
//!   `extractHostsFromAnswer`);
//! - a host's identity output (`identityUtils.ts` `decodeIdentityOutput` and
//!   `certificateRecord`, with `Certificate.ts` `verify`);
//! - the lookup a certificate answers (`identityUtils.ts`
//!   `filterCertificatesByIdentityKey` and `filterCertificatesByAttributes`).

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::primitives::{sha256, to_var_int, PublicKey, Signature, SymmetricKey};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::TransactionOutput;
use bsv_rs::wallet::{
    Counterparty, DecryptArgs, GetPublicKeyArgs, KeyDeriver, ProtoWallet, Protocol, SecurityLevel,
    VerifySignatureArgs,
};

use super::lookup::OverlayCertificate;

// =============================================================================
// A tracker's advertisement
// =============================================================================

/// The most bytes one field of an advertisement may carry (the reference's
/// `MAX_DISCOVERY_FIELD_BYTES`).
pub(crate) const MAX_ADVERTISEMENT_FIELD_BYTES: usize = 4096;

/// The most bytes an advertisement's fields may carry together (the
/// reference's `MAX_DISCOVERY_PAYLOAD_BYTES`).
pub(crate) const MAX_ADVERTISEMENT_PAYLOAD_BYTES: usize = 8192;

/// The fields of `script` when it is a PushDrop written in the canonical
/// form (its lock before its fields, each field pushed minimally): `script`
/// is what `PushDrop::lock` writes of its own decode.
fn canonical_pushdrop(script: &LockingScript) -> Option<PushDrop> {
    let decoded = PushDrop::decode(script).ok()?;
    let relocked = PushDrop::new(decoded.locking_public_key.clone(), decoded.fields.clone()).lock();
    (relocked.to_binary() == script.to_binary()).then_some(decoded)
}

/// A DER signature of 8 to 80 bytes written in its canonical form.
fn canonical_signature(bytes: &[u8]) -> Option<Signature> {
    if !(8..=80).contains(&bytes.len()) {
        return None;
    }
    let signature = Signature::from_der(bytes).ok()?;
    (signature.to_der() == bytes).then_some(signature)
}

/// The domain an advertisement names, when the advertisement is a SLAP one
/// for `service`, signed by its identity key's linked key and locked with
/// it, as the reference decodes and verifies it at the resolver: one output
/// of one satoshi; five fields of at most 4096 bytes each and 8192 together;
/// the protocol `SLAP`; a compressed identity key; the service asked for; a
/// canonical DER signature over the four data fields by the identity key's
/// child for `anyone` (BRC-42, invoice `2-service lookup availability-1`);
/// the same child as the locking key. The domain itself is the caller's to
/// check (`named_host`).
pub(crate) fn slap_advertisement_domain(
    output: &TransactionOutput,
    service: &str,
) -> Option<String> {
    if output.satoshis != Some(1) {
        return None;
    }
    let token = canonical_pushdrop(&output.locking_script)?;
    let [protocol, identity, domain, advertised, signature] = token.fields.as_slice() else {
        return None;
    };
    if token
        .fields
        .iter()
        .any(|f| f.len() > MAX_ADVERTISEMENT_FIELD_BYTES)
        || token.fields.iter().map(Vec::len).sum::<usize>() > MAX_ADVERTISEMENT_PAYLOAD_BYTES
    {
        return None;
    }
    if protocol.as_slice() != b"SLAP" || advertised.as_slice() != service.as_bytes() {
        return None;
    }
    if identity.len() != 33 || !matches!(identity[0], 2 | 3) {
        return None;
    }
    let identity = PublicKey::from_bytes(identity).ok()?;
    let domain = std::str::from_utf8(domain).ok()?;
    if domain.is_empty() || domain.chars().any(char::is_control) {
        return None;
    }
    let signature = canonical_signature(signature)?;
    let linked = KeyDeriver::new(None)
        .derive_public_key(
            &Protocol::new(SecurityLevel::Counterparty, "service lookup availability"),
            "1",
            &Counterparty::Other(identity),
            false,
        )
        .ok()?;
    let signed = sha256(&token.fields[..4].concat());
    if !linked.verify(&signed, &signature) || linked != token.locking_public_key {
        return None;
    }
    Some(domain.to_string())
}

/// Whether an advertised domain's host name is one the reference admits for
/// an `https` advertisement (`isAllowedPublicHostname`): not `localhost`,
/// and no label in the `xn--` form.
pub(crate) fn allowed_public_hostname(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host != "localhost" && host.split('.').all(|label| !label.starts_with("xn--"))
}

// =============================================================================
// A host's identity output
// =============================================================================

/// The keys an answered certificate carries, each of them and no other
/// (`certificateRecord`'s `allowed`).
const CERTIFICATE_KEYS: [&str; 8] = [
    "type",
    "serialNumber",
    "subject",
    "certifier",
    "revocationOutpoint",
    "fields",
    "keyring",
    "signature",
];

/// Why an identity output is left out (for the resolver's debug log only).
pub(crate) type Refusal = &'static str;

fn identity_protocol() -> Protocol {
    Protocol::new(SecurityLevel::App, "identity")
}

/// A compressed public key in hex, `02` or `03` and 64 hex digits.
fn compressed_key(text: &str) -> Option<PublicKey> {
    let shaped = text.len() == 66
        && (text.starts_with("02") || text.starts_with("03"))
        && text.bytes().all(|b| b.is_ascii_hexdigit());
    shaped.then(|| PublicKey::from_hex(text).ok()).flatten()
}

/// Base64 text written canonically that decodes to a length in `lengths`.
fn canonical_base64(text: &str, lengths: std::ops::RangeInclusive<usize>) -> Option<Vec<u8>> {
    let bytes = BASE64.decode(text).ok()?;
    (lengths.contains(&bytes.len()) && BASE64.encode(&bytes) == text).then_some(bytes)
}

/// The type of an existing certificate: canonical base64 of at most 172
/// characters encoding 1 to 128 bytes (`CertificateType.ts`
/// `existingCertificateTypeBytes`), written into the preimage as is.
fn existing_type_bytes(text: &str) -> Option<Vec<u8>> {
    if text.len() > 172 {
        return None;
    }
    canonical_base64(text, 1..=128)
}

/// A field name the reference writes (`assertCertificateFieldName`): 1 to
/// 50 bytes and not a name it holds unsafe for a record key.
fn field_name_allowed(name: &str) -> bool {
    (1..=50).contains(&name.len()) && !matches!(name, "__proto__" | "constructor" | "prototype")
}

/// The order the reference writes a certificate's fields in, its
/// `localeCompare`, for names of letters and digits: case-insensitive,
/// then lower case before upper case.
fn field_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_lowercase().cmp(&b.to_lowercase()).then_with(|| {
        let flip = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_lowercase() {
                        c.to_ascii_uppercase()
                    } else {
                        c.to_ascii_lowercase()
                    }
                })
                .collect()
        };
        flip(a).cmp(&flip(b))
    })
}

/// The bytes the certifier signs (`Certificate.ts` `verify` 437-456 and
/// `#serialize(false, true)` 235-300): type, serial number, subject, certifier, the revocation outpoint's txid
/// and index, then each field's name and its value's base64 text.
pub(crate) fn certificate_preimage(cert: &OverlayCertificate) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(existing_type_bytes(&cert.type_id)?);
    out.extend(canonical_base64(&cert.serial_number, 32..=32)?);
    out.extend(compressed_key(&cert.subject)?.to_compressed());
    out.extend(compressed_key(&cert.certifier)?.to_compressed());
    let outpoint = cert.revocation_outpoint.as_deref()?;
    let mut parts = outpoint.split('.');
    let txid = parts.next()?;
    let index = parts.next().unwrap_or("0");
    let index_ok =
        index == "0" || (!index.starts_with('0') && index.bytes().all(|b| b.is_ascii_digit()));
    if parts.next().is_some() || txid.len() != 64 || !index_ok {
        return None;
    }
    out.extend(hex::decode(txid).ok()?);
    let index: u32 = index.parse().ok()?;
    out.extend(to_var_int(index as u64));
    let mut names: Vec<&String> = cert.fields.keys().collect();
    names.sort_by(|a, b| field_order(a, b));
    out.extend(to_var_int(names.len() as u64));
    for name in names {
        if !field_name_allowed(name) {
            return None;
        }
        out.extend(to_var_int(name.len() as u64));
        out.extend(name.as_bytes());
        let value = cert.fields[name].as_bytes();
        out.extend(to_var_int(value.len() as u64));
        out.extend(value);
    }
    Some(out)
}

/// Whether the certificate's signature verifies under its certifier, as the
/// reference's `Certificate.verify` checks it: a DER signature by the
/// certifier's child for `anyone` (BRC-42, invoice
/// `2-certificate signature-{type} {serialNumber}`) over the preimage.
pub(crate) fn certificate_signature_verifies(cert: &OverlayCertificate) -> bool {
    let check = || -> Option<bool> {
        let preimage = certificate_preimage(cert)?;
        let signature = Signature::from_der(&hex::decode(cert.signature.as_deref()?).ok()?).ok()?;
        let certifier = compressed_key(&cert.certifier)?;
        let key = KeyDeriver::new(None)
            .derive_public_key(
                &Protocol::new(SecurityLevel::Counterparty, "certificate signature"),
                &format!("{} {}", cert.type_id, cert.serial_number),
                &Counterparty::Other(certifier),
                false,
            )
            .ok()?;
        Some(key.verify(&sha256(&preimage), &signature))
    };
    check().unwrap_or(false)
}

/// The certificate record of an identity output's first field, by the
/// reference's `certificateRecord`: strict UTF-8 JSON of exactly the eight
/// keys, `fields` and `keyring` objects of at least one entry each, every
/// keyring name a field's, every value a string.
fn certificate_record(bytes: &[u8]) -> std::result::Result<OverlayCertificate, Refusal> {
    let text = std::str::from_utf8(bytes).map_err(|_| "certificate is not UTF-8")?;
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "certificate is not JSON")?;
    let record = value.as_object().ok_or("certificate is not an object")?;
    if record.len() != CERTIFICATE_KEYS.len()
        || CERTIFICATE_KEYS.iter().any(|k| !record.contains_key(*k))
    {
        return Err("certificate keys are not the certificate's eight");
    }
    let fields = record["fields"]
        .as_object()
        .ok_or("fields is not an object")?;
    let keyring = record["keyring"]
        .as_object()
        .ok_or("keyring is not an object")?;
    if fields.is_empty() || keyring.is_empty() {
        return Err("fields or keyring is empty");
    }
    for (name, key) in keyring {
        if !fields.contains_key(name) {
            return Err("keyring names an absent field");
        }
        key.as_str().ok_or("keyring value is not a string")?;
    }
    let text_of = |k: &str| record[k].as_str().map(str::to_string);
    let strings = |m: &serde_json::Map<String, serde_json::Value>| {
        m.iter()
            .map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect::<Option<HashMap<String, String>>>()
    };
    Ok(OverlayCertificate {
        type_id: text_of("type").ok_or("type is not a string")?,
        serial_number: text_of("serialNumber").ok_or("serialNumber is not a string")?,
        subject: text_of("subject").ok_or("subject is not a string")?,
        certifier: text_of("certifier").ok_or("certifier is not a string")?,
        revocation_outpoint: Some(
            text_of("revocationOutpoint").ok_or("revocationOutpoint is not a string")?,
        ),
        fields: strings(fields).ok_or("a field value is not a string")?,
        keyring: strings(keyring).ok_or("a keyring value is not a string")?,
        signature: Some(text_of("signature").ok_or("signature is not a string")?),
        decrypted_fields: HashMap::new(),
    })
}

/// The fields `anyone` may read: each keyring entry decrypted to its
/// field's key (counterparty the subject, key ID `{serialNumber} {name}`),
/// then the field's value decrypted with that key, as the reference's
/// `VerifiableCertificate.decryptFields` reads them.
fn decrypt_revealed_fields(
    cert: &OverlayCertificate,
    subject: &PublicKey,
) -> Option<HashMap<String, String>> {
    let anyone = ProtoWallet::anyone();
    let mut out = HashMap::new();
    for (name, key) in &cert.keyring {
        let revealed = anyone
            .decrypt(DecryptArgs {
                ciphertext: BASE64.decode(key).ok()?,
                protocol_id: Protocol::new(
                    SecurityLevel::Counterparty,
                    "certificate field encryption",
                ),
                key_id: format!("{} {}", cert.serial_number, name),
                counterparty: Some(Counterparty::Other(subject.clone())),
            })
            .ok()?
            .plaintext;
        let value = SymmetricKey::from_bytes(&revealed)
            .ok()?
            .decrypt(&BASE64.decode(cert.fields.get(name)?).ok()?)
            .ok()?;
        out.insert(name.clone(), String::from_utf8(value).ok()?);
    }
    Some(out)
}

/// The certificate an overlay host's identity output carries, when the
/// output and its certificate pass the reference's `decodeIdentityOutput`:
/// a canonical PushDrop of two fields (the certificate, and the subject's
/// signature over it, 8 to 80 bytes) locked with the
/// subject's identity child for `anyone` (protocol `[1, identity]`, key
/// ID `1`); that signature valid; the certificate record well formed
/// ([`certificate_record`]); its certifier's signature valid
/// ([`certificate_signature_verifies`]); and at least one field revealed to
/// `anyone`, whose values are the certificate's decrypted fields.
pub(crate) fn identity_output_certificate(
    script: &LockingScript,
) -> std::result::Result<OverlayCertificate, Refusal> {
    let token = canonical_pushdrop(script).ok_or("not a canonical PushDrop")?;
    let [certificate, signature] = token.fields.as_slice() else {
        return Err("PushDrop does not carry two fields");
    };
    if certificate.is_empty() {
        return Err("certificate field is empty");
    }
    if !(8..=80).contains(&signature.len()) {
        return Err("signature field out of bounds");
    }
    let mut cert = certificate_record(certificate)?;
    let subject = compressed_key(&cert.subject).ok_or("subject is not a public key")?;
    let anyone = ProtoWallet::anyone();
    let expected_lock = anyone
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(identity_protocol()),
            key_id: Some("1".to_string()),
            counterparty: Some(Counterparty::Other(subject.clone())),
            for_self: Some(false),
        })
        .map_err(|_| "no identity key for the subject")?
        .public_key;
    if expected_lock != token.locking_public_key.to_hex() {
        return Err("locking key is not the subject's identity key");
    }
    let signed = anyone
        .verify_signature(VerifySignatureArgs {
            data: Some(certificate.clone()),
            hash_to_directly_verify: None,
            signature: signature.clone(),
            protocol_id: identity_protocol(),
            key_id: "1".to_string(),
            counterparty: Some(Counterparty::Other(subject.clone())),
            for_self: Some(false),
        })
        .map(|r| r.valid)
        .unwrap_or(false);
    if !signed {
        return Err("the subject's signature does not verify");
    }
    if !certificate_signature_verifies(&cert) {
        return Err("the certifier's signature does not verify");
    }
    let revealed = decrypt_revealed_fields(&cert, &subject).ok_or("a field does not decrypt")?;
    if revealed.is_empty() {
        return Err("no field revealed");
    }
    cert.decrypted_fields = revealed;
    Ok(cert)
}

// =============================================================================
// The lookup a certificate answers
// =============================================================================

/// Whether a certificate answers a lookup by `identity_key`: its subject is
/// that key, compared case-insensitively (`filterCertificatesByIdentityKey`).
pub(crate) fn answers_identity_key(cert: &OverlayCertificate, identity_key: &str) -> bool {
    cert.subject.to_lowercase() == identity_key.to_lowercase()
}

#[path = "lookup_attribute_match.rs"]
mod attribute_match;
pub(crate) use attribute_match::answers_attributes;
