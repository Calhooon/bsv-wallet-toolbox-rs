//! Identity outputs as an overlay host answers them, for the resolver's
//! tests: a certificate signed by its certifier, its fields revealed to
//! `anyone`, in a PushDrop the subject signs and locks.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::primitives::{sha256, PrivateKey, SymmetricKey};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, EncryptArgs, GetPublicKeyArgs, KeyDeriver, ProtoWallet,
    Protocol, SecurityLevel,
};

use super::lookup::OverlayCertificate;
use super::lookup_checks::certificate_preimage;

/// The subject the tests' lookups ask for.
pub(crate) fn subject_key() -> PrivateKey {
    PrivateKey::from_hex(&"11".repeat(32)).unwrap()
}

/// The certifier of the tests' certificates.
pub(crate) fn certifier_key() -> PrivateKey {
    PrivateKey::from_hex(&"22".repeat(32)).unwrap()
}

/// The certificate record (the eight keys) of a certificate about
/// `subject`, signed by `certifier`, serial number from `tag`, its
/// `fields` revealed to `anyone`.
pub(crate) fn certificate_record(
    subject: &PrivateKey,
    certifier: &PrivateKey,
    tag: &str,
    fields: &[(&str, &str)],
) -> serde_json::Value {
    let serial = BASE64.encode(sha256(tag.as_bytes()));
    let cert_type = BASE64.encode([7u8; 32]);
    let anyone = KeyDeriver::anyone_key().1;
    let owner = ProtoWallet::new(Some(subject.clone()));
    let mut encrypted = HashMap::new();
    let mut keyring = HashMap::new();
    for (name, value) in fields {
        let key = SymmetricKey::random();
        encrypted.insert(
            name.to_string(),
            BASE64.encode(key.encrypt(value.as_bytes()).unwrap()),
        );
        let revealed = owner
            .encrypt(EncryptArgs {
                plaintext: key.as_bytes().to_vec(),
                protocol_id: Protocol::new(
                    SecurityLevel::Counterparty,
                    "certificate field encryption",
                ),
                key_id: format!("{serial} {name}"),
                counterparty: Some(Counterparty::Other(anyone.clone())),
            })
            .unwrap();
        keyring.insert(name.to_string(), BASE64.encode(revealed.ciphertext));
    }
    let mut cert = OverlayCertificate {
        type_id: cert_type,
        serial_number: serial,
        subject: subject.public_key().to_hex(),
        certifier: certifier.public_key().to_hex(),
        revocation_outpoint: Some(format!("{}.0", "00".repeat(32))),
        fields: encrypted,
        keyring,
        signature: None,
        decrypted_fields: HashMap::new(),
    };
    let signing = KeyDeriver::new(Some(certifier.clone()))
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, "certificate signature"),
            &format!("{} {}", cert.type_id, cert.serial_number),
            &Counterparty::Anyone,
        )
        .unwrap();
    let preimage = certificate_preimage(&cert).unwrap();
    cert.signature = Some(hex::encode(
        signing.sign(&sha256(&preimage)).unwrap().to_der(),
    ));
    serde_json::json!({
        "type": cert.type_id,
        "serialNumber": cert.serial_number,
        "subject": cert.subject,
        "certifier": cert.certifier,
        "revocationOutpoint": cert.revocation_outpoint,
        "fields": cert.fields,
        "keyring": cert.keyring,
        "signature": cert.signature,
    })
}

/// The identity output of `record`, signed and locked by `subject` as the
/// reference's identity client publishes it.
pub(crate) fn identity_lock(subject: &PrivateKey, record: &serde_json::Value) -> LockingScript {
    identity_lock_of_bytes(subject, serde_json::to_vec(record).unwrap())
}

/// The identity output of a certificate field of `bytes`, signed and
/// locked by `subject`.
pub(crate) fn identity_lock_of_bytes(subject: &PrivateKey, bytes: Vec<u8>) -> LockingScript {
    let owner = ProtoWallet::new(Some(subject.clone()));
    let protocol = Protocol::new(SecurityLevel::App, "identity");
    let lock_key = owner
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol.clone()),
            key_id: Some("1".to_string()),
            counterparty: Some(Counterparty::Anyone),
            for_self: Some(true),
        })
        .unwrap()
        .public_key;
    let signature = owner
        .create_signature(CreateSignatureArgs {
            data: Some(bytes.clone()),
            hash_to_directly_sign: None,
            protocol_id: protocol,
            key_id: "1".to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap()
        .signature;
    PushDrop::new(
        bsv_rs::primitives::PublicKey::from_hex(&lock_key).unwrap(),
        vec![bytes, signature],
    )
    .lock()
}

/// The identity output of the tests' subject's certificate tagged `tag`.
pub(crate) fn tagged_identity_lock(tag: &str) -> LockingScript {
    let subject = subject_key();
    identity_lock(
        &subject,
        &certificate_record(&subject, &certifier_key(), tag, &[("tag", tag)]),
    )
}
