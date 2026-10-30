//! Test fixtures: an overlay identity record and its token as the reference's
//! `IdentityClient.publiclyRevealAttributes` publishes them (ts-stack@edf6e03
//! `IdentityClient.ts:451-509`; bsv-rs v0.9.0-beta.4
//! `src/identity/client.rs:451-500`, `reveal.rs` `identity_token`): a
//! certificate the certifier signs over encrypted fields, a keyring for the
//! `anyone` verifier, and the `PushDrop` the subject locks and signs under
//! `[1, 'identity']` key `1`.

use bsv_rs::auth::MasterCertificate;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Transaction, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, GetPublicKeyArgs, Outpoint, ProtoWallet, Protocol,
};
use indexmap::IndexMap;

use super::OverlayCertificate;

pub(crate) fn key(n: u8) -> PrivateKey {
    PrivateKey::from_bytes(&[n; 32]).unwrap()
}

/// The record `certifier` issues `subject` with `fields` in plaintext, its
/// type `[1; 32]` and serial `serial`, every field revealed to `anyone`. Its `decrypted_fields` are empty: only the door fills them.
pub(crate) async fn revealed(
    certifier: &PrivateKey,
    subject: &PrivateKey,
    serial: [u8; 32],
    fields: &[(&str, &str)],
) -> OverlayCertificate {
    let plain: IndexMap<String, String> = fields
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    let master = MasterCertificate::issue_for_subject(
        &ProtoWallet::new(Some(certifier.clone())),
        certifier,
        subject.public_key(),
        plain.clone(),
        [1; 32],
        Some(serial),
        None,
    )
    .await
    .unwrap();
    // Signed again over a revocation outpoint, which issuance leaves unset.
    let mut signed = master.certificate;
    signed.set_revocation_outpoint(Some(Outpoint::new([0xaa; 32], 0)));
    signed.set_signature(None);
    signed.sign(certifier).unwrap();
    let names: Vec<String> = plain.keys().cloned().collect();
    let keyring = MasterCertificate::create_keyring_for_verifier(
        &ProtoWallet::new(Some(subject.clone())),
        &certifier.public_key(),
        &ProtoWallet::anyone().identity_key(),
        &signed.fields,
        &names,
        &master.master_keyring,
        &serial,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let certificate = signed.to_wallet_certificate();
    OverlayCertificate {
        type_id: certificate.certificate_type,
        serial_number: certificate.serial_number,
        subject: certificate.subject.to_hex(),
        certifier: certificate.certifier.to_hex(),
        revocation_outpoint: certificate.revocation_outpoint.map(|o| o.to_string()),
        signature: certificate.signature.map(hex::encode),
        fields: certificate.fields,
        keyring,
        decrypted_fields: IndexMap::new(),
    }
}

/// The record's eight properties as the token carries them.
pub(crate) fn record_json(record: &OverlayCertificate) -> Vec<u8> {
    let mut value = serde_json::to_value(record).unwrap();
    value.as_object_mut().unwrap().remove("decryptedFields");
    serde_json::to_vec(&value).unwrap()
}

/// The identity token over `json`, locked and signed by `subject`'s wallet
/// for `anyone` under the identity protocol (`PushDrop.lock(fields,
/// protocolID, keyID, 'anyone', true, true)`).
pub(crate) fn token(subject: &PrivateKey, json: Vec<u8>) -> LockingScript {
    let wallet = ProtoWallet::new(Some(subject.clone()));
    let protocol = Protocol::from_tuple((1, "identity")).unwrap();
    let key = wallet
        .get_public_key(GetPublicKeyArgs {
            protocol_id: Some(protocol.clone()),
            key_id: Some("1".into()),
            counterparty: Some(Counterparty::Anyone),
            for_self: Some(true),
            ..Default::default()
        })
        .unwrap();
    let signature = wallet
        .create_signature(CreateSignatureArgs {
            data: Some(json.clone()),
            protocol_id: protocol,
            key_id: "1".into(),
            counterparty: Some(Counterparty::Anyone),
            ..Default::default()
        })
        .unwrap()
        .signature;
    let key = bsv_rs::primitives::PublicKey::from_hex(&key.public_key).unwrap();
    PushDrop::new(key, vec![json, signature]).lock()
}

/// A transaction whose output 0 is `script`, as BEEF.
pub(crate) fn beef(script: LockingScript) -> Vec<u8> {
    let mut tx = Transaction::new();
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1),
        locking_script: script,
        change: false,
    });
    tx.to_beef(true).unwrap()
}
