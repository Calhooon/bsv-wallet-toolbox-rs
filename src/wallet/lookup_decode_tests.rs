//! #495: the overlay output read as the reference's `decodeIdentityOutput`
//! reads it (ts-stack@edf6e03 `identityUtils.ts:493-557`), each failing
//! output dropped from the answer; #494: a record carrying `decryptedFields`
//! is not the reference's eight properties and is dropped.

use bsv_rs::block::ChainParams;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::script::{LockingScript, ScriptChunk};
use bsv_rs::transaction::MockChainTracker;

use super::super::fixture::{beef, key, record_json, revealed, token, HEIGHT};
use super::super::overlay_certificates;
use super::{decode_identity_output, OverlayCertificate};

const CERTIFIER: u8 = 9;
const SUBJECT: u8 = 8;

/// The answer's entries for `scripts`, one output each.
async fn answered(scripts: Vec<LockingScript>) -> Vec<String> {
    let outputs = scripts
        .into_iter()
        .map(|script| OutputListItem {
            beef: beef(script),
            output_index: 0,
            context: None,
        })
        .collect();
    let tracker = MockChainTracker::always_valid(HEIGHT);
    overlay_certificates(outputs, &tracker, &ChainParams::mainnet())
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.serial_number)
        .collect()
}

async fn alice(serial: u8) -> OverlayCertificate {
    revealed(
        &key(CERTIFIER),
        &key(SUBJECT),
        [serial; 32],
        &[("name", "Alice")],
    )
    .await
}

/// `bad` is dropped from an answer beside `good`'s own token.
async fn only_the_good_token(bad: LockingScript, good: &OverlayCertificate) {
    let kept = answered(vec![bad, token(&key(SUBJECT), record_json(good))]).await;
    assert_eq!(kept, vec![good.serial_number.clone()]);
}

#[tokio::test]
async fn the_reference_token_decodes_to_its_record() {
    let record = alice(2).await;
    let decoded = decode_identity_output(&token(&key(SUBJECT), record_json(&record))).unwrap();
    assert_eq!(record_json(&decoded), record_json(&record));
    assert!(decoded.decrypted_fields.is_empty());
}

/// Another identity locks the subject's record and signs it with its own
/// key, so the field signature is not the subject's and drops it
/// (`identityUtils.ts:532-539`) whatever the locking key; before the fix it
/// decoded. The locking key comparison alone is
/// `a_token_with_only_its_locking_key_swapped_is_dropped`.
#[tokio::test]
async fn a_token_locked_by_another_key_is_dropped() {
    let record = alice(2).await;
    let good = alice(3).await;
    only_the_good_token(token(&key(7), record_json(&record)), &good).await;
}

/// `identityUtils.ts:526-531`: the subject's own record and field
/// signature, only the locking key another identity's, so the output's key
/// is not the one an `anyone` wallet derives for the subject.
#[tokio::test]
async fn a_token_with_only_its_locking_key_swapped_is_dropped() {
    let record = alice(2).await;
    let good = alice(3).await;
    let mut chunks = token(&key(SUBJECT), record_json(&record)).chunks();
    chunks[0] = token(&key(7), record_json(&record)).chunks()[0].clone();
    only_the_good_token(LockingScript::from_chunks(chunks), &good).await;
}

/// `identityUtils.ts:532-539`: the subject's key, but a field signature
/// over other bytes; before the fix it decoded.
#[tokio::test]
async fn a_bad_field_signature_is_dropped() {
    let record = alice(2).await;
    let good = alice(3).await;
    let mut chunks = token(&key(SUBJECT), record_json(&record)).chunks();
    let signed_elsewhere = token(&key(SUBJECT), record_json(&good)).chunks();
    chunks[3] = signed_elsewhere[3].clone();
    only_the_good_token(LockingScript::from_chunks(chunks), &good).await;
}

/// `identityUtils.ts:502,516`: a third field, or a field pushed with a
/// longer opcode than its length asks; before the fix each decoded.
#[tokio::test]
async fn a_token_not_of_the_reference_shape_is_dropped() {
    let record = alice(2).await;
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

/// `identityUtils.ts:502`: the subject's locking key and `OP_CHECKSIG`, a
/// `PushDrop` with no field, is refused for its field count before the
/// chunk shape (`:511-521`) is read; the base's parser indexed its absent
/// field 0 and panicked.
#[tokio::test]
async fn a_token_with_no_field_is_dropped() {
    let good = alice(3).await;
    let chunks = token(&key(SUBJECT), record_json(&good)).chunks();
    let bare = LockingScript::from_chunks(chunks[..2].to_vec());
    assert!(decode_identity_output(&bare).is_err());
    only_the_good_token(bare, &good).await;
}

/// #494: a record carrying `decryptedFields`, a key the reference's
/// `certificateRecord` refuses (`identityUtils.ts:90-106`), is dropped even
/// with a good token; the door fills that map from the decryption alone.
#[tokio::test]
async fn an_entry_carrying_decrypted_fields_is_dropped() {
    let record = alice(2).await;
    let mut wire = serde_json::to_value(&record).unwrap();
    wire["decryptedFields"] = serde_json::json!({"name": "Mallory"});
    let carrying = token(&key(SUBJECT), serde_json::to_vec(&wire).unwrap());
    assert!(decode_identity_output(&carrying).is_err());
}

/// bsv-rs 0.9.0-beta.5's `from_hex` reads the reference's lenient key forms
/// (bsv-rs #498). A subject written as the odd-length or hybrid text of the
/// subject's key is still dropped: the record's compressed-key check
/// (`lookup_record.rs`, `compressed_key`: 66 characters, `02` or `03`) refuses
/// it before the decode parses it, where the reference reads the text with
/// `PublicKey.fromString` for the derivation (`identityUtils.ts:526-539`) and
/// drops the entry at `verifiableCert.verify()` (`toBinary`'s
/// `^(02|03)[0-9a-fA-F]{64}$`, Certificate.ts:240-246). The same verdict at
/// another step, green on beta.4 and at beta.5; a `05` tag (bsv-rs #550) the
/// same.
#[tokio::test]
async fn a_lenient_subject_text_is_dropped_as_the_reference_drops_it() {
    let good = alice(5).await;
    let subject = key(SUBJECT).public_key();
    let uncompressed = subject.to_hex_uncompressed();
    let y_odd = u8::from_str_radix(&uncompressed[128..], 16).unwrap() & 1 == 1;
    let hybrid = format!("{}{}", if y_odd { "07" } else { "06" }, &uncompressed[2..]);
    let tag_05 = format!("05{}", &subject.to_hex()[2..]);
    for text in [subject.to_hex()[1..].to_string(), hybrid, tag_05] {
        let mut record = alice(6).await;
        record.subject = text.clone();
        let script = token(&key(SUBJECT), record_json(&record));
        let refused = decode_identity_output(&script).unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("outside the reference's bounds"),
            "{text}: {refused}"
        );
        only_the_good_token(script, &good).await;
    }
}
