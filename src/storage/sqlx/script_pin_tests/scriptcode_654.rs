//! bsv-rs #654: the scriptCode of a signature check built from the script's
//! bytes as bitcoin-sv v1.2.3 builds it (`interpreter.cpp:1470-1484`), so a
//! check in the unlocking script past a data-carrying top-level `OP_RETURN`
//! keeps the locking script under a post-Chronicle word. The sibling's
//! shapes (`tests/sighash_scriptcode_654.rs` at bsv-rs v0.9.0-beta.7: U2, U3,
//! M2, P1; lock `OP_VERIFY OP_1`, version 2, key `11..11`) re-signed over a
//! real parent, so the live internalize door (`verify_beef_spends`, the
//! spend's word at tip + 1) judges them. At bsv-rs 0.9.0-beta.6 the door
//! refused the node-signed U2 and M2 and accepted the ones signed over the
//! scriptCode without the locking script; the node's verdict is the reverse.

use super::{payment, verify_at_height};
use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_dispatched_for_signing, parse_transaction, SighashParams,
};
use bsv_rs::primitives::bsv::TransactionSignature;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{ProtocolEra, Script, UnlockingScript};

const LOCK: [u8; 2] = [0x69, 0x51]; // OP_VERIFY OP_1
const OP_VERIFY_FAILS: &str = "OP_VERIFY requires the top stack value to be truthy.";

fn push(bytes: &[u8]) -> Vec<u8> {
    let mut script = Script::new();
    script.write_bin(bytes);
    script.to_binary()
}

fn key() -> PrivateKey {
    PrivateKey::from_hex(&"11".repeat(32)).unwrap()
}

/// The door's verdict on a version-2 spend of `OP_VERIFY OP_1` whose
/// unlocking script is `unlock(signature)`, the signature taken over `code`
/// under `scope`, the parent proven at `height`.
async fn verdict(
    code: &[u8],
    scope: u32,
    unlock: impl Fn(&[u8]) -> Vec<u8>,
    height: u32,
) -> crate::Result<()> {
    let (mut parent, mut child) = payment(&hex::encode(LOCK));
    parent.version = 2;
    child.inputs[0].source_txid = Some(parent.id());
    child.version = 2;
    let raw = parse_transaction(&child.to_binary()).unwrap();
    let digest = compute_sighash_dispatched_for_signing(&SighashParams {
        version: raw.version,
        inputs: &raw.inputs,
        outputs: &raw.outputs,
        locktime: raw.locktime,
        input_index: 0,
        subscript: code,
        satoshis: parent.outputs[0].satoshis.unwrap(),
        scope,
    });
    let signature = TransactionSignature::new(key().sign(&digest).unwrap(), scope);
    let unlocking = unlock(&signature.to_checksig_format());
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_binary(&unlocking).unwrap());
    child.invalidate_caches();
    verify_at_height(parent, child, height).await
}

/// `<sig> <P> OP_CODESEPARATOR <tail>`.
fn checksig_unlock(tail: &'static [u8]) -> impl Fn(&[u8]) -> Vec<u8> {
    move |sig| {
        let p = key().public_key().to_compressed().to_vec();
        [push(sig), push(&p), vec![0xab], tail.to_vec()].concat()
    }
}

fn refused_by_op_verify(result: crate::Result<()>, name: &str) {
    let refused = result.expect_err(name).to_string();
    assert!(refused.contains(OP_VERIFY_FAILS), "{name}: {refused}");
}

const POST_CHRONICLE: u32 = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;

/// U2, `<sig> <P> OP_CODESEPARATOR OP_CHECKSIG OP_RETURN 01 00`: the node
/// signs `ac6a01006951` under the post-Chronicle word; the door accepts the
/// spend signed so under both scopes and refuses the one signed over
/// `ac6a0100`, the scriptCode bsv-rs built until #654. Red at beta.6 on
/// both halves.
#[tokio::test]
async fn the_door_accepts_the_node_signed_u2_and_refuses_the_old_digest_654() {
    const TAIL: &[u8] = &[0xac, 0x6a, 0x01, 0x00];
    let node = [TAIL, &LOCK].concat();
    for scope in [0x41, 0x61] {
        let result = verdict(&node, scope, checksig_unlock(TAIL), POST_CHRONICLE).await;
        assert!(result.is_ok(), "U2 {scope:#x} node-signed: {result:?}");
    }
    let old = verdict(TAIL, 0x41, checksig_unlock(TAIL), POST_CHRONICLE).await;
    refused_by_op_verify(old, "U2 0x41 old-signed");
}

/// M2, U2's form through `OP_CHECKMULTISIG`: the same verdicts.
#[tokio::test]
async fn the_door_accepts_the_node_signed_m2_and_refuses_the_old_digest_654() {
    const TAIL: &[u8] = &[0xae, 0x6a, 0x01, 0x00];
    let unlock = |sig: &[u8]| {
        let p = key().public_key().to_compressed().to_vec();
        [
            vec![0x00],
            push(sig),
            vec![0x51],
            push(&p),
            vec![0x51, 0xab],
            TAIL.to_vec(),
        ]
        .concat()
    };
    let node = [TAIL, &LOCK].concat();
    let result = verdict(&node, 0x41, unlock, POST_CHRONICLE).await;
    assert!(result.is_ok(), "M2 node-signed: {result:?}");
    let old = verdict(TAIL, 0x41, unlock, POST_CHRONICLE).await;
    refused_by_op_verify(old, "M2 old-signed");
}

/// U3, a bare `OP_RETURN` ending the unlocking script: `ac6a6951` before
/// #654, after it and at the node, so the door's verdict does not move.
#[tokio::test]
async fn the_door_accepts_u3_before_and_after_654() {
    const TAIL: &[u8] = &[0xac, 0x6a];
    let code = [TAIL, &LOCK].concat();
    let result = verdict(&code, 0x41, checksig_unlock(TAIL), POST_CHRONICLE).await;
    assert!(result.is_ok(), "U3: {result:?}");
}

/// P1, `<sig> <P> OP_CODESEPARATOR OP_CHECKSIG`: the sibling's pre-Chronicle
/// case runs under the block word without `SIGPUSHONLY`; the door's word is
/// the transaction-level one, whose push-only rule refuses the unlocking
/// script before the check under a pre-Chronicle spend era, whichever
/// scriptCode signed it, before and after #654. Post-Chronicle, the node and
/// bsv-rs (both pins) sign `ac6951`.
#[tokio::test]
async fn p1_is_push_only_refused_pre_chronicle_and_accepted_post_chronicle_654() {
    const TAIL: &[u8] = &[0xac];
    let pre_chronicle = ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;
    for code in [vec![0xac], vec![0xac, 0x69, 0x51]] {
        let refused = verdict(&code, 0x41, checksig_unlock(TAIL), pre_chronicle)
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.to_lowercase().contains("push"), "P1: {refused}");
    }
    let result = verdict(
        &[0xac, 0x69, 0x51],
        0x41,
        checksig_unlock(TAIL),
        POST_CHRONICLE,
    )
    .await;
    assert!(result.is_ok(), "P1 Chronicle-signed: {result:?}");
}
