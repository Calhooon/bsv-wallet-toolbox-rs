//! The overlay's identity outputs read as certificates: each output of an
//! output-list answer decoded, an entry that fails dropped
//! (`decodeIdentityOutput`, ts-stack@edf6e03 `identityUtils.ts:493-557`).

use bsv_rs::overlay::LookupAnswer;
use bsv_rs::primitives::PublicKey;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    Counterparty, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel, VerifySignatureArgs,
};

use super::{record, OverlayCertificate};
use crate::Result;

/// Parse a `LookupAnswer` into a list of `OverlayCertificate`s.
///
/// Iterates over output-list results, decodes PushDrop from each transaction
/// output, and deserializes the first field as a JSON certificate.
/// Invalid outputs are silently skipped (matching TypeScript behavior).
pub(super) fn parse_overlay_answer(answer: &LookupAnswer) -> Result<Vec<OverlayCertificate>> {
    match answer {
        LookupAnswer::OutputList { outputs } => {
            let mut certs = Vec::new();
            for output in outputs {
                match parse_single_output(&output.beef, output.output_index) {
                    Ok(cert) => certs.push(cert),
                    Err(e) => {
                        tracing::debug!("Skipping invalid overlay output: {}", e);
                    }
                }
            }
            Ok(certs)
        }
        _ => Ok(vec![]),
    }
}

/// Parse a single BEEF output into an `OverlayCertificate`.
fn parse_single_output(beef: &[u8], output_index: u32) -> Result<OverlayCertificate> {
    let tx = Transaction::from_beef(beef, None)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to parse BEEF: {}", e)))?;
    let output = tx
        .outputs
        .get(output_index as usize)
        .ok_or_else(|| crate::Error::ServiceError("Output index out of bounds".into()))?;
    decode_identity_output(&output.locking_script)
}

/// `MAX_IDENTITY_CERTIFICATE_BYTES` (`identityUtils.ts:23`).
const MAX_IDENTITY_CERTIFICATE_BYTES: usize = 256 * 1024;
/// `IDENTITY_KEY_ID` (`identityUtils.ts:26`).
const IDENTITY_KEY_ID: &str = "1";
const OP_CHECKSIG: u8 = 0xac;
const OP_2DROP: u8 = 0x6d;

/// `IDENTITY_PROTOCOL`, `[1, 'identity']` (`identityUtils.ts:25`).
fn identity_protocol() -> Protocol {
    Protocol::new(SecurityLevel::App, "identity")
}

fn dropped(why: &str) -> crate::Error {
    crate::Error::ServiceError(format!("Identity output dropped: {why}"))
}

/// `decodeIdentityOutput`'s checks on the output (`identityUtils.ts:496-545`):
/// two PushDrop fields, the record of 1 byte to 256 KiB and the field
/// signature of 8 to 80; exactly five chunks, the 33-byte locking key,
/// `OP_CHECKSIG`, each field under the push opcode its length asks
/// (`pushOpcode`, :80-85) and `OP_2DROP`; the record as `certificateRecord`
/// reads it; the locking key the one an `anyone` wallet derives for the
/// subject under the identity protocol; the field signature the subject's
/// over the record's bytes. The certificate's own signature and its fields
/// are the door's (`OverlayCertificate::verified_and_decrypted`).
pub(super) fn decode_identity_output(script: &LockingScript) -> Result<OverlayCertificate> {
    let decoded = PushDrop::decode(script)
        .map_err(|e| crate::Error::ServiceError(format!("Failed to decode PushDrop: {}", e)))?;
    let [certificate, field_signature] = decoded.fields.as_slice() else {
        return Err(dropped("not two fields"));
    };
    if !(1..=MAX_IDENTITY_CERTIFICATE_BYTES).contains(&certificate.len())
        || !(8..=80).contains(&field_signature.len())
    {
        return Err(dropped("a field outside its bounds"));
    }
    let chunks = script.chunks();
    let shaped = chunks.len() == 5
        && chunks[0].op == 33
        && chunks[0].data.as_ref().map(Vec::len) == Some(33)
        && chunks[1].op == OP_CHECKSIG
        && chunks[2].op == push_opcode(certificate)
        && chunks[3].op == push_opcode(field_signature)
        && chunks[4].op == OP_2DROP;
    if !shaped {
        return Err(dropped("not the identity token's chunks"));
    }
    // `toUTF8Strict`, then `certificateRecord`.
    let json = std::str::from_utf8(certificate)
        .map_err(|e| crate::Error::ServiceError(format!("Invalid UTF-8 in certificate: {}", e)))?;
    let record = record::parse(json)?;
    let subject = Counterparty::Other(
        PublicKey::from_hex(&record.subject).map_err(|_| dropped("a subject not a key"))?,
    );
    let anyone = ProtoWallet::anyone();
    let expected = anyone
        .get_public_key(GetPublicKeyArgs {
            protocol_id: Some(identity_protocol()),
            key_id: Some(IDENTITY_KEY_ID.into()),
            counterparty: Some(subject.clone()),
            ..Default::default()
        })
        .map_err(|_| dropped("no locking key derived"))?;
    if decoded.locking_public_key.to_hex() != expected.public_key {
        return Err(dropped("a locking key not the subject's"));
    }
    let verified = anyone.verify_signature(VerifySignatureArgs {
        data: Some(certificate.clone()),
        signature: field_signature.clone(),
        protocol_id: identity_protocol(),
        key_id: IDENTITY_KEY_ID.into(),
        counterparty: Some(subject),
        ..Default::default()
    });
    if !verified.is_ok_and(|v| v.valid) {
        return Err(dropped("a field signature not the subject's"));
    }
    Ok(record)
}

/// `pushOpcode` (`identityUtils.ts:80-85`).
fn push_opcode(value: &[u8]) -> u8 {
    match u8::try_from(value.len()) {
        Ok(n) if n <= 75 => n,
        Ok(_) => 0x4c,
        Err(_) if value.len() <= 0xffff => 0x4d,
        Err(_) => 0x4e,
    }
}

#[cfg(test)]
#[path = "lookup_decode_tests.rs"]
mod tests;
