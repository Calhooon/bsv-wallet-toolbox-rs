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
//! `SIGHASH_FORKID` fails under every post-Genesis word.

use super::busy_door_tests::{fund_tagged, own_lock, store_snapshot};
use super::named_input_tests::{
    caller_script, complete, defer_named, forget_source_bytes, named_input_scene, payment,
    script_for_create_action, sign_named, spend_map, stored_action, NamedInputScene, ORIGINATOR,
};
use bsv_rs::script::ProtocolEra;
use bsv_rs::transaction::{Beef, MerklePath};
use bsv_rs::wallet::{CreateActionArgs, Outpoint, SignableTransaction, WalletInterface};

/// `script` with its leading signature's sighash byte stripped of
/// `SIGHASH_FORKID` (0x40); the DER bytes are unchanged.
fn without_fork_id(mut script: Vec<u8>) -> Vec<u8> {
    let sighash = usize::from(script[0]);
    assert_ne!(script[sighash] & 0x40, 0, "the wallet signs with FORKID");
    script[sighash] &= !0x40;
    script
}

/// The refusal is bsv-rs's spend refusal, class `InvalidParameter`.
fn assert_spend_refused<T>(result: bsv_rs::Result<T>) -> String {
    match result {
        Err(bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            message,
        }) => {
            assert!(
                message.starts_with("spend refused at offset ")
                    && message.contains(", input 0 of ")
                    && message.ends_with(": The signature must use SIGHASH_FORKID."),
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

/// What a signAction refused before the writer must leave: the store and
/// the action as they were.
struct Kept {
    reference: String,
    stored: (String, Option<String>, Option<Vec<u8>>),
    before: Vec<String>,
}

impl Kept {
    async fn read(scene: &NamedInputScene, signable: &SignableTransaction) -> Self {
        let reference = bsv_rs::primitives::to_base64(&signable.reference);
        let stored = stored_action(scene, &reference).await;
        assert_eq!(stored.0, "unsigned");
        let before = store_snapshot(&scene.wallet).await;
        Kept {
            reference,
            stored,
            before,
        }
    }

    /// Nothing stored or posted and the action not aborted, so the caller
    /// signs it again with `script`.
    async fn then_signs(
        self,
        scene: &NamedInputScene,
        signable: SignableTransaction,
        script: Vec<u8>,
    ) {
        let after = store_snapshot(&scene.wallet).await;
        assert_eq!(after, self.before, "nothing stored");
        assert_eq!(stored_action(scene, &self.reference).await, self.stored);
        assert_eq!(posts(scene), 0, "nothing posted");
        let signed = complete(scene, signable, Some(script)).await;
        let (status, txid, _) = stored_action(scene, &self.reference).await;
        assert_eq!((status.as_str(), txid), ("nosend", Some(signed.id())));
    }
}

async fn create_with_caller_script(
    scene: &NamedInputScene,
    script: Vec<u8>,
    no_send: bool,
) -> bsv_rs::Result<bsv_rs::wallet::CreateActionResult> {
    let mut args = payment(1_000);
    args.options.as_mut().unwrap().no_send = Some(no_send);
    args.inputs = Some(vec![scene.caller_input(
        Some(script),
        None,
        "caller unlocks an owned output",
    )]);
    scene.wallet.create_action(args, Some(ORIGINATOR)).await
}

#[tokio::test]
async fn create_refuses_its_own_fork_id_less_input_before_the_post() {
    for no_send in [false, true] {
        let scene = named_input_scene().await;
        let before = store_snapshot(&scene.wallet).await;
        let script = without_fork_id(script_for_create_action(&scene));
        let result = create_with_caller_script(&scene, script, no_send).await;
        eprintln!("refused: {}", assert_spend_refused(result));
        assert_eq!(
            store_snapshot(&scene.wallet).await,
            before,
            "nothing stored"
        );
        assert_eq!(posts(&scene), 0, "nothing posted");
    }
}

#[tokio::test]
async fn create_accepts_its_own_input_valid_under_the_word() {
    for no_send in [false, true] {
        let scene = named_input_scene().await;
        let script = script_for_create_action(&scene);
        let made = create_with_caller_script(&scene, script, no_send)
            .await
            .unwrap();
        assert!(made.txid.is_some());
        assert_eq!(posts(&scene), usize::from(!no_send));
    }
}

#[tokio::test]
async fn sign_action_refuses_its_own_fork_id_less_input() {
    let scene = named_input_scene().await;
    let signable = defer_named(&scene, 1_000, None).await;
    let kept = Kept::read(&scene, &signable).await;
    let bad = without_fork_id(caller_script(&scene, &signable));
    let result = sign_named(&scene, kept.reference.clone(), spend_map(0, bad, None)).await;
    assert_spend_refused(result);
    let script = caller_script(&scene, &signable);
    kept.then_signs(&scene, signable, script).await;
}

/// An input whose source bytes are neither in the action's input BEEF nor
/// in the store is skipped, as `verifyUnlockScripts.ts:316-321` skips it:
/// the same fork-ID-less script is accepted, the node judging it at
/// broadcast.
#[tokio::test]
async fn sign_action_skips_an_input_without_source_bytes() {
    let scene = named_input_scene().await;
    let signable = defer_named(&scene, 1_000, None).await;
    let reference = bsv_rs::primitives::to_base64(&signable.reference);
    forget_source_bytes(&scene, &reference).await;
    let bad = without_fork_id(caller_script(&scene, &signable));
    let signed = complete(&scene, signable, Some(bad)).await;
    assert_eq!(stored_action(&scene, &reference).await.0, "nosend");
    assert_eq!(stored_action(&scene, &reference).await.1, Some(signed.id()));
}

/// A tracker that answers roots and height and supplies no mining tip.
struct NoTip;

#[async_trait::async_trait]
impl bsv_rs::transaction::ChainTracker for NoTip {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> Result<bool, bsv_rs::transaction::ChainTrackerError> {
        Ok(true)
    }

    async fn current_height(&self) -> Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(880_000)
    }
}

/// The word's tip cannot be read: signAction holds with the service fault,
/// nothing written or posted, and the action is not aborted, so the same
/// call succeeds once the header service answers.
#[tokio::test]
async fn sign_action_holds_without_a_mining_tip_and_keeps_the_action() {
    let scene = named_input_scene().await;
    let signable = defer_named(&scene, 1_000, None).await;
    let kept = Kept::read(&scene, &signable).await;
    let storage = scene.wallet.storage();
    storage.set_chain_tracker(std::sync::Arc::new(NoTip)).await;
    let script = caller_script(&scene, &signable);
    let spends = spend_map(0, script.clone(), None);
    match sign_named(&scene, kept.reference.clone(), spends).await {
        Err(bsv_rs::Error::WalletError(message)) => assert_eq!(
            message,
            "Service error: ChainTracker verification failed: the header service supplied no mining tip"
        ),
        other => panic!("expected the header hold, got {other:?}"),
    }
    storage.clear_chain_tracker().await;
    kept.then_signs(&scene, signable, script).await;
}

/// The scene's source transaction moved out of the store into a caller's
/// inputBEEF, proven at a post-Genesis height: the only bytes of it the
/// wallet sees are the ones the caller supplied (the cold review's
/// `review_supplied_sources` probe).
async fn supply_source(scene: &NamedInputScene) -> Vec<u8> {
    let raw: Vec<u8> = sqlx::query_scalar("SELECT raw_tx FROM transactions WHERE txid = ?")
        .bind(&scene.input.source_txid)
        .fetch_one(scene.wallet.storage().pool())
        .await
        .unwrap();
    let mut beef = Beef::new();
    let height = ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;
    let index = beef.merge_bump(MerklePath::from_coinbase_txid(
        &scene.input.source_txid,
        height,
    ));
    beef.merge_raw_tx(raw, Some(index));
    forget_source_bytes(scene, "no action").await;
    beef.to_binary()
}

/// A response option that touches the BEEF or the post.
#[derive(Clone, Copy, Debug)]
enum Respond {
    KnownTxid,
    TxidOnly,
    NoSend,
    NoSendChange,
    Broadcast,
    Delayed,
}

/// The scene's caller input, its source supplied in `input_beef`, under
/// `respond`; `no_send_change` the outpoints a `NoSendChange` call names.
fn supplied_args(
    scene: &NamedInputScene,
    input_beef: Vec<u8>,
    script: Option<Vec<u8>>,
    respond: Respond,
    no_send_change: &[Outpoint],
) -> CreateActionArgs {
    let mut args = payment(1_000);
    args.input_beef = Some(input_beef);
    let length = script.is_none().then_some(110);
    args.inputs = Some(vec![scene.caller_input(
        script,
        length,
        "caller supplied the source bytes",
    )]);
    let options = args.options.as_mut().unwrap();
    match respond {
        Respond::KnownTxid => {
            let txid = hex::decode(&scene.input.source_txid).unwrap();
            options.known_txids = Some(vec![txid.try_into().unwrap()]);
        }
        Respond::TxidOnly => options.return_txid_only = Some(true),
        Respond::NoSend => {}
        Respond::NoSendChange => options.no_send_change = Some(no_send_change.to_vec()),
        Respond::Broadcast => options.no_send = Some(false),
        Respond::Delayed => {
            options.no_send = Some(false);
            options.accept_delayed_broadcast = Some(true);
        }
    }
    args
}

/// A noSend action's change, for a later call's `noSendChange`.
async fn no_send_change(scene: &NamedInputScene) -> Vec<Outpoint> {
    fund_tagged(&scene.wallet, &own_lock(&scene.wallet), 1).await;
    let made = scene
        .wallet
        .create_action(payment(500), Some(ORIGINATOR))
        .await
        .unwrap();
    let change = made.no_send_change.unwrap();
    assert!(!change.is_empty(), "the noSend action left change");
    change
}

const RESPONSES: [Respond; 6] = [
    Respond::KnownTxid,
    Respond::TxidOnly,
    Respond::NoSend,
    Respond::NoSendChange,
    Respond::Broadcast,
    Respond::Delayed,
];

/// The cold review's P1 on PR #462: the check judged the BEEF after the
/// `knownTxids` trim, so a supplied source the caller also named known was
/// gone and its input skipped. The sources are the untrimmed ones (the
/// assembly, else the caller's inputBEEF as supplied, then the store), so
/// no response option shrinks the judged set: each refuses the fork-ID-less
/// input with nothing written and nothing posted.
#[tokio::test]
async fn create_judges_supplied_sources_under_every_response_option() {
    for respond in RESPONSES {
        let scene = named_input_scene().await;
        let change = match respond {
            Respond::NoSendChange => no_send_change(&scene).await,
            _ => Vec::new(),
        };
        let input_beef = supply_source(&scene).await;
        scene.wallet.services().reset_history();
        let before = store_snapshot(&scene.wallet).await;
        let bad = without_fork_id(script_for_create_action(&scene));
        let args = supplied_args(&scene, input_beef, Some(bad), respond, &change);
        let result = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
        eprintln!("{respond:?}: {}", assert_spend_refused(result));
        let after = store_snapshot(&scene.wallet).await;
        assert_eq!(after, before, "{respond:?}: nothing stored");
        assert_eq!(posts(&scene), 0, "{respond:?}: nothing posted");
    }
}

/// The same at signAction: the action's stored input BEEF is the untrimmed
/// assembly, so a source the caller supplied and named known is judged.
#[tokio::test]
async fn sign_action_judges_a_supplied_source_named_known() {
    let scene = named_input_scene().await;
    let input_beef = supply_source(&scene).await;
    let mut args = supplied_args(&scene, input_beef, None, Respond::KnownTxid, &[]);
    args.options.as_mut().unwrap().sign_and_process = Some(false);
    let made = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
    let signable = made.unwrap().signable_transaction.unwrap();
    let kept = Kept::read(&scene, &signable).await;
    let bad = without_fork_id(caller_script(&scene, &signable));
    let result = sign_named(&scene, kept.reference.clone(), spend_map(0, bad, None)).await;
    eprintln!("signAction: {}", assert_spend_refused(result));
    let script = caller_script(&scene, &signable);
    kept.then_signs(&scene, signable, script).await;
}
