//! The checks the lookup resolver holds on what an answering SLAP tracker or
//! overlay host names, ported from the reference where bsv-rs exports no
//! function doing the same check:
//!
//! - a tracker's advertisement (`OverlayAdminTokenTemplate.ts`
//!   `decodeAndVerifyOverlayAdvertisement`, called by `LookupResolver.ts`
//!   `extractHostsFromAnswer`).

use bsv_rs::primitives::{sha256, PublicKey, Signature};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::TransactionOutput;
use bsv_rs::wallet::{Counterparty, KeyDeriver, Protocol, SecurityLevel};

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
