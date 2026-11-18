//! Identity outputs as an overlay host answers them, for the lookup's
//! integration tests: a certificate signed by its certifier (the
//! reference's `Certificate.sign`), its fields revealed to `anyone`, in a
//! PushDrop the subject signs and locks (the reference identity client's
//! publication).

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bsv_rs::primitives::{sha256, to_var_int, PrivateKey, PublicKey, SymmetricKey};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, EncryptArgs, GetPublicKeyArgs, KeyDeriver, ProtoWallet,
    Protocol, SecurityLevel,
};
use std::collections::BTreeMap;

/// The serial number of the certificate tagged `tag`.
pub fn serial_of(tag: &str) -> String {
    BASE64.encode(sha256(tag.as_bytes()))
}

/// The bytes the certifier signs, for a 32-byte type and field names of
/// lower-case letters and digits (`Certificate.ts` `toBinary(false)`).
fn preimage(record: &serde_json::Value) -> Vec<u8> {
    let text = |k: &str| record[k].as_str().unwrap().to_string();
    let mut out = BASE64.decode(text("type")).unwrap();
    out.extend(BASE64.decode(text("serialNumber")).unwrap());
    out.extend(hex::decode(text("subject")).unwrap());
    out.extend(hex::decode(text("certifier")).unwrap());
    let outpoint = text("revocationOutpoint");
    let (txid, vout) = outpoint.split_once('.').unwrap();
    out.extend(hex::decode(txid).unwrap());
    out.extend(to_var_int(vout.parse().unwrap()));
    let fields: BTreeMap<String, String> = record["fields"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
        .collect();
    out.extend(to_var_int(fields.len() as u64));
    for (name, value) in &fields {
        out.extend(to_var_int(name.len() as u64));
        out.extend(name.as_bytes());
        out.extend(to_var_int(value.len() as u64));
        out.extend(value.as_bytes());
    }
    out
}

/// The certificate record of `subject`'s certificate tagged `tag`, signed
/// by `certifier`, its `fields` revealed to `anyone`.
pub fn certificate_record(
    subject: &PrivateKey,
    certifier: &PrivateKey,
    tag: &str,
    fields: &[(&str, &str)],
) -> serde_json::Value {
    let serial = serial_of(tag);
    let anyone = KeyDeriver::anyone_key().1;
    let owner = ProtoWallet::new(Some(subject.clone()));
    let mut encrypted = serde_json::Map::new();
    let mut keyring = serde_json::Map::new();
    for (name, value) in fields {
        let key = SymmetricKey::random();
        encrypted.insert(
            name.to_string(),
            BASE64.encode(key.encrypt(value.as_bytes()).unwrap()).into(),
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
        keyring.insert(name.to_string(), BASE64.encode(revealed.ciphertext).into());
    }
    let mut record = serde_json::json!({
        "type": BASE64.encode([7u8; 32]),
        "serialNumber": serial,
        "subject": subject.public_key().to_hex(),
        "certifier": certifier.public_key().to_hex(),
        "revocationOutpoint": format!("{}.0", "00".repeat(32)),
        "fields": encrypted,
        "keyring": keyring,
        "signature": "",
    });
    let signing = KeyDeriver::new(Some(certifier.clone()))
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, "certificate signature"),
            &format!("{} {}", record["type"].as_str().unwrap(), serial),
            &Counterparty::Anyone,
        )
        .unwrap();
    let signature = signing.sign(&sha256(&preimage(&record))).unwrap().to_der();
    record["signature"] = hex::encode(signature).into();
    record
}

/// The identity output of `record`, signed and locked by `subject`.
pub fn identity_lock(subject: &PrivateKey, record: &serde_json::Value) -> LockingScript {
    let bytes = serde_json::to_vec(record).unwrap();
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
        PublicKey::from_hex(&lock_key).unwrap(),
        vec![bytes, signature],
    )
    .lock()
}

/// A signed SLAP advertisement for `ls_identity` naming `host`.
pub fn slap_lock(host: &str) -> LockingScript {
    bsv_rs::overlay::create_signed_overlay_admin_token(
        &PrivateKey::random(),
        bsv_rs::overlay::Protocol::Slap,
        host,
        "ls_identity",
    )
}
