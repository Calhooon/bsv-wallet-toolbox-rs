//! #495: the overlay output read as the reference's `decodeIdentityOutput`
//! reads it (ts-stack@edf6e03 `identityUtils.ts:493-557`), each failing
//! output dropped from the answer; #494: a record carrying `decryptedFields`
//! is not the reference's eight properties and is dropped.

use bsv_rs::overlay::{LookupAnswer, OutputListItem};
use bsv_rs::script::{LockingScript, ScriptChunk};

use super::super::fixture::{beef, key, record_json, revealed, token};
use super::{decode_identity_output, parse_overlay_answer};

const CERTIFIER: u8 = 9;
const SUBJECT: u8 = 8;

/// The answer's entries for `scripts`, one output each.
fn answered(scripts: Vec<LockingScript>) -> Vec<String> {
    let outputs = scripts
        .into_iter()
        .map(|script| OutputListItem {
            beef: beef(script),
            output_index: 0,
            context: None,
        })
        .collect();
    parse_overlay_answer(&LookupAnswer::OutputList { outputs })
        .unwrap()
        .into_iter()
        .map(|c| c.serial_number)
        .collect()
}

#[tokio::test]
async fn the_reference_token_decodes_to_its_record() {
    let record = revealed(&key(CERTIFIER), &key(SUBJECT), 2, &[("name", "Alice")]).await;
    let decoded = decode_identity_output(&token(&key(SUBJECT), record_json(&record))).unwrap();
    assert_eq!(record_json(&decoded), record_json(&record));
    assert!(decoded.decrypted_fields.is_empty());
}

/// `identityUtils.ts:531-535`: the output's key is not the one an `anyone`
/// wallet derives for the record's subject; before the fix it decoded.
#[tokio::test]
async fn a_token_locked_by_another_key_is_dropped() {
    let record = revealed(&key(CERTIFIER), &key(SUBJECT), 2, &[("name", "Alice")]).await;
    let good = revealed(&key(CERTIFIER), &key(SUBJECT), 3, &[("name", "Alice")]).await;
    // Another identity locks and signs the subject's record.
    let other = token(&key(7), record_json(&record));
    assert_eq!(
        answered(vec![other, token(&key(SUBJECT), record_json(&good))]),
        vec![good.serial_number]
    );
}

/// `identityUtils.ts:536-545`: the subject's key, but a field signature
/// over other bytes; before the fix it decoded.
#[tokio::test]
async fn a_bad_field_signature_is_dropped() {
    let record = revealed(&key(CERTIFIER), &key(SUBJECT), 2, &[("name", "Alice")]).await;
    let good = revealed(&key(CERTIFIER), &key(SUBJECT), 3, &[("name", "Alice")]).await;
    let mut chunks = token(&key(SUBJECT), record_json(&record)).chunks();
    let signed_elsewhere = token(&key(SUBJECT), record_json(&good)).chunks();
    chunks[3] = signed_elsewhere[3].clone();
    let forged = LockingScript::from_chunks(chunks);
    assert_eq!(
        answered(vec![forged, token(&key(SUBJECT), record_json(&good))]),
        vec![good.serial_number]
    );
}

/// `identityUtils.ts:497,506-516`: a third field, or a field pushed with a
/// longer opcode than its length asks; before the fix each decoded.
#[tokio::test]
async fn a_token_not_of_the_reference_shape_is_dropped() {
    let record = revealed(&key(CERTIFIER), &key(SUBJECT), 2, &[("name", "Alice")]).await;
    let json = record_json(&record);
    let mut third = token(&key(SUBJECT), json.clone()).chunks();
    third.insert(4, ScriptChunk::new_push(vec![1, 2, 3]));
    third.push(ScriptChunk::new_opcode(0x75));
    let mut wide = token(&key(SUBJECT), json.clone()).chunks();
    wide[2] = ScriptChunk::new(0x4e, Some(json));
    for (case, chunks) in [("a third field", third), ("a wide push", wide)] {
        let script = LockingScript::from_chunks(chunks);
        assert!(decode_identity_output(&script).is_err(), "{case}");
    }
}

/// #494: a record carrying `decryptedFields`, a key the reference's
/// `certificateRecord` refuses (`identityUtils.ts:90-106`), is dropped even
/// with a good token; the door fills that map from the decryption alone.
#[tokio::test]
async fn an_entry_carrying_decrypted_fields_is_dropped() {
    let record = revealed(&key(CERTIFIER), &key(SUBJECT), 2, &[("name", "Alice")]).await;
    let mut wire = serde_json::to_value(&record).unwrap();
    wire["decryptedFields"] = serde_json::json!({"name": "Mallory"});
    let carrying = token(&key(SUBJECT), serde_json::to_vec(&wire).unwrap());
    assert!(decode_identity_output(&carrying).is_err());
}
