//! #450: the wallet's own newly signed transaction is judged under the
//! node's word per input before it is posted.
//!
//! At ts-stack@edf6e03 (wallet-toolbox/src/signer/methods/),
//! createAction.ts:120 calls `verifyUnlockScripts(r.txid!, beef,
//! wallet.scriptVerifier)` before `processAction` at :138, and
//! signAction.ts:90 calls it after `processAction` at :76.
//! verifyUnlockScripts.ts:316-321 skips an input whose source transaction is
//! absent; :329-331 judges the others with `{ consensus: true, utxoHeight }`.
//! The port judges with the two-word rule (#422): the spend's era at tip+1,
//! the coin's UTXO bits from its proof height. A signature without
//! SIGHASH_FORKID fails under every post-Genesis word.

use super::busy_door_tests::store_snapshot;
use super::named_input_tests::{
    caller_script, complete, defer_named, named_input_scene, payment, script_for_create_action,
    sign_named, spend_map, stored_action, NamedInputScene, ORIGINATOR,
};
use bsv_rs::wallet::WalletInterface;

/// `script` with its leading signature's sighash byte stripped of
/// SIGHASH_FORKID (0x40); the DER bytes are unchanged.
fn without_fork_id(mut script: Vec<u8>) -> Vec<u8> {
    let sighash = usize::from(script[0]);
    assert_ne!(script[sighash] & 0x40, 0, "the wallet signs with FORKID");
    script[sighash] &= !0x40;
    script
}

/// The refusal is bsv-rs's spend refusal, class InvalidParameter.
fn assert_spend_refused<T>(result: bsv_rs::Result<T>) -> String {
    match result {
        Err(bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            message,
        }) => {
            assert!(
                message.starts_with("spend refused at offset ")
                    && message.contains(", input 0 of "),
                "{message}"
            );
            message
        }
        Ok(_) => panic!("the wallet accepted its own transaction the node refuses"),
        Err(error) => panic!("unexpected refusal: {error}"),
    }
}

fn posts(scene: &NamedInputScene) -> usize {
    scene.wallet.services().call_count("post_beef")
}

async fn create_with_caller_script(
    scene: &NamedInputScene,
    script: Vec<u8>,
) -> bsv_rs::Result<bsv_rs::wallet::CreateActionResult> {
    let mut args = payment(1_000);
    args.inputs = Some(vec![scene.caller_input(
        Some(script),
        None,
        "caller unlocks an owned output",
    )]);
    scene.wallet.create_action(args, Some(ORIGINATOR)).await
}

#[tokio::test]
async fn create_refuses_its_own_fork_id_less_input_before_the_post() {
    let scene = named_input_scene().await;
    let before = store_snapshot(&scene.wallet).await;
    let script = without_fork_id(script_for_create_action(&scene));
    let result = create_with_caller_script(&scene, script).await;
    assert_spend_refused(result);
    assert_eq!(
        store_snapshot(&scene.wallet).await,
        before,
        "nothing stored"
    );
    assert_eq!(posts(&scene), 0, "nothing posted");
}

#[tokio::test]
async fn create_accepts_its_own_input_valid_under_the_word() {
    let scene = named_input_scene().await;
    let script = script_for_create_action(&scene);
    let made = create_with_caller_script(&scene, script).await.unwrap();
    assert!(made.txid.is_some());
}

#[tokio::test]
async fn sign_action_refuses_its_own_fork_id_less_input() {
    let scene = named_input_scene().await;
    let signable = defer_named(&scene, 1_000, None).await;
    let reference = bsv_rs::primitives::to_base64(&signable.reference);
    let stored = stored_action(&scene, &reference).await;
    assert_eq!(stored.0, "unsigned");
    let before = store_snapshot(&scene.wallet).await;
    let bad = without_fork_id(caller_script(&scene, &signable));
    let result = sign_named(&scene, reference.clone(), spend_map(0, bad, None)).await;
    assert_spend_refused(result);
    assert_eq!(
        store_snapshot(&scene.wallet).await,
        before,
        "nothing stored"
    );
    assert_eq!(stored_action(&scene, &reference).await, stored);
    assert_eq!(posts(&scene), 0, "nothing posted");
    // The action stays unsigned and reserved: the caller may sign again.
    let script = caller_script(&scene, &signable);
    let signed = complete(&scene, signable, Some(script)).await;
    assert_eq!(stored_action(&scene, &reference).await.0, "nosend");
    assert_eq!(stored_action(&scene, &reference).await.1, Some(signed.id()));
}
