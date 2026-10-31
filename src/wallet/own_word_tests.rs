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
//!
//! #506: services with no header service (the CLI's `CHAINTRACKS_URL=off`)
//! and no tracker give the latest era, as a tracker with no mining tip does
//! (#464, rule 22); beta.4 held every signed createAction there with
//! `Service error: ChainTracker verification failed: Service error:
//! ChainTracker not configured in Services (no chaintracks_url)`.

use super::busy_door_tests::{fund_tagged, own_lock, store_snapshot};
use super::named_input_tests::{
    caller_script, complete, defer_named, forget_source_bytes, named_input_scene, payment,
    script_for_create_action, sign_named, spend_map, stored_action, NamedInputScene, ORIGINATOR,
};
use bsv_rs::script::ProtocolEra;
use bsv_rs::transaction::{Beef, MerklePath};
use bsv_rs::wallet::{CreateActionArgs, Outpoint, SignableTransaction, WalletInterface};
use std::sync::Arc;

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

/// The header source the storage's services carry.
#[derive(Clone, Copy, Debug)]
enum Header {
    /// The mock's tracker, its tip past the scene's coins.
    Tracker,
    /// The real `Services` built with no `chaintracks_url`, whose
    /// `get_chain_tracker` answers its not-configured error, and no tracker
    /// on the caller or the storage (#506).
    Off,
}

const HEADERS: [Header; 2] = [Header::Tracker, Header::Off];

/// `named_input_scene`, the storage's services as `header` says; the
/// wallet keeps the mock, so a post is counted.
async fn scene_with(header: Header) -> NamedInputScene {
    let scene = named_input_scene().await;
    if let Header::Off = header {
        use crate::services::WalletServices;
        use crate::storage::WalletStorageProvider;
        let services = crate::services::Services::mainnet().unwrap();
        assert!(services.get_chain_tracker().await.is_err());
        scene.wallet.storage().set_services(Arc::new(services));
    }
    scene
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
    for (header, no_send) in HEADERS.into_iter().flat_map(|h| [(h, false), (h, true)]) {
        let scene = scene_with(header).await;
        let before = store_snapshot(&scene.wallet).await;
        let script = without_fork_id(script_for_create_action(&scene));
        let result = create_with_caller_script(&scene, script, no_send).await;
        eprintln!("{header:?} refused: {}", assert_spend_refused(result));
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
    for (header, no_send) in HEADERS.into_iter().flat_map(|h| [(h, false), (h, true)]) {
        let scene = scene_with(header).await;
        let script = script_for_create_action(&scene);
        let made = create_with_caller_script(&scene, script, no_send)
            .await
            .unwrap();
        let txid = hex::encode(made.txid.unwrap());
        let status: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(scene.wallet.storage().pool())
            .await
            .unwrap();
        eprintln!("{header:?} no_send={no_send}: {status}");
        assert_eq!(status, if no_send { "nosend" } else { "unproven" });
        assert_eq!(posts(&scene), usize::from(!no_send));
    }
}

#[tokio::test]
async fn sign_action_refuses_its_own_fork_id_less_input() {
    for header in HEADERS {
        let scene = scene_with(header).await;
        let signable = defer_named(&scene, 1_000, None).await;
        let kept = Kept::read(&scene, &signable).await;
        let bad = without_fork_id(caller_script(&scene, &signable));
        let result = sign_named(&scene, kept.reference.clone(), spend_map(0, bad, None)).await;
        eprintln!("{header:?} refused: {}", assert_spend_refused(result));
        let script = caller_script(&scene, &signable);
        kept.then_signs(&scene, signable, script).await;
    }
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

/// The input BEEF processAction carried from the action to its request,
/// by the txid the create answered.
async fn stored_input_beef(scene: &NamedInputScene, txid: &str) -> Option<Vec<u8>> {
    sqlx::query_scalar("SELECT input_beef FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(scene.wallet.storage().pool())
        .await
        .unwrap()
}

/// #463: under `returnTXIDOnly` the create door stores the input BEEF it
/// assembles, as under the other options (the door's response stays
/// txid-only: `test_create_action_txid_only_stores_the_input_beef`), as
/// the reference stores `storageBeef` whatever the caller asks back
/// (ts-stack@edf6e03 `storage/methods/createAction.ts:725`) and drops the
/// response's BEEF alone (`trimInputBeef`, :1725-1726). So the door's own
/// signing is processed, where it was refused for want of an inputBEEF,
/// and the stored copy is untrimmed: a source named in `knownTxids` keeps
/// its bytes.
#[tokio::test]
async fn create_txid_only_signs_and_stores_the_untrimmed_assembly() {
    // The immediate post is the wallet's, built from the door's response
    // (`wallet.rs`), outside the door: the stored and delayed paths here.
    for (no_send, delayed) in [(true, false), (false, true)] {
        let scene = named_input_scene().await;
        let mut args = payment(1_000);
        let options = args.options.as_mut().unwrap();
        options.no_send = Some(no_send);
        options.accept_delayed_broadcast = Some(delayed);
        options.return_txid_only = Some(true);
        let source = hex::decode(&scene.input.source_txid).unwrap();
        options.known_txids = Some(vec![source.try_into().unwrap()]);
        let script = script_for_create_action(&scene);
        args.inputs = Some(vec![scene.caller_input(
            Some(script),
            None,
            "caller unlocks an owned output",
        )]);
        let made = scene
            .wallet
            .create_action(args, Some(ORIGINATOR))
            .await
            .unwrap();
        let txid = hex::encode(made.txid.unwrap());
        assert_eq!(posts(&scene), 0, "stored or delayed, nothing posted");
        let stored = stored_input_beef(&scene, &txid).await;
        let beef = Beef::from_binary(&stored.expect("the input BEEF is stored")).unwrap();
        let entry = beef.find_txid(&scene.input.source_txid).unwrap();
        assert!(!entry.is_txid_only(), "the stored copy is untrimmed");
    }
}

/// The same action deferred: `signAction`'s processAction reads the stored
/// input BEEF and signs, where it was refused with the "no inputBEEF" text.
#[tokio::test]
async fn sign_action_signs_a_txid_only_action() {
    let scene = named_input_scene().await;
    let mut args = payment(1_000);
    let options = args.options.as_mut().unwrap();
    options.sign_and_process = Some(false);
    options.return_txid_only = Some(true);
    args.inputs = Some(vec![scene.caller_input(
        None,
        Some(110),
        "caller unlocks an owned output",
    )]);
    let made = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
    let signable = made.unwrap().signable_transaction.unwrap();
    let script = caller_script(&scene, &signable);
    let reference = bsv_rs::primitives::to_base64(&signable.reference);
    let signed = complete(&scene, signable, Some(script)).await;
    let (status, txid, _) = stored_action(&scene, &reference).await;
    assert_eq!((status.as_str(), txid), ("nosend", Some(signed.id())));
}

/// The door's own signing under `returnTXIDOnly` is judged on the stored
/// assembly as under the other options: a fork-ID-less input is refused
/// with nothing written and nothing posted, the caller's inputBEEF absent.
#[tokio::test]
async fn create_txid_only_judges_its_own_input_on_the_assembly() {
    let scene = named_input_scene().await;
    let before = store_snapshot(&scene.wallet).await;
    let mut args = payment(1_000);
    args.options.as_mut().unwrap().return_txid_only = Some(true);
    let bad = without_fork_id(script_for_create_action(&scene));
    args.inputs = Some(vec![scene.caller_input(
        Some(bad),
        None,
        "caller unlocks an owned output",
    )]);
    let result = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
    eprintln!("TxidOnly, store: {}", assert_spend_refused(result));
    assert_eq!(
        store_snapshot(&scene.wallet).await,
        before,
        "nothing stored"
    );
    assert_eq!(posts(&scene), 0, "nothing posted");
}

/// A tracker that answers roots and height and supplies no mining tip, or
/// with `fault` one whose tip read faults.
#[derive(Clone, Copy, Debug)]
struct NoTip {
    fault: bool,
}

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

    async fn mining_tip(
        &self,
    ) -> Result<
        Option<bsv_rs::transaction::beef_stream::ChainTip>,
        bsv_rs::transaction::ChainTrackerError,
    > {
        if self.fault {
            return Err(bsv_rs::transaction::ChainTrackerError::NetworkError(
                "tip unavailable".into(),
            ));
        }
        Ok(None)
    }
}

const NO_TIPS: [NoTip; 2] = [NoTip { fault: false }, NoTip { fault: true }];

/// #464, the keeper's reading of 2026-10-29: bsv-rs's constants name no
/// activation height still ahead, so with no mining tip (or a tip fault)
/// createAction judges its own transaction under the latest era's
/// transaction-level word (post-Chronicle), the supplied source's UTXO bits
/// from its proof height (Genesis + 100), where it held with `Service error:
/// ChainTracker verification failed: the header service supplied no mining
/// tip`: the fork-ID-less input refused before the post with bsv-rs's text,
/// the valid one created and posted.
#[tokio::test]
async fn create_judges_its_own_input_at_the_latest_era_without_a_tip() {
    for no_tip in NO_TIPS {
        for bad in [true, false] {
            let scene = named_input_scene().await;
            let input_beef = supply_source(&scene).await;
            let storage = scene.wallet.storage();
            storage.set_chain_tracker(std::sync::Arc::new(no_tip)).await;
            scene.wallet.services().reset_history();
            let before = store_snapshot(&scene.wallet).await;
            let mut script = script_for_create_action(&scene);
            if bad {
                script = without_fork_id(script);
            }
            let args = supplied_args(&scene, input_beef, Some(script), Respond::Broadcast, &[]);
            let result = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
            if bad {
                eprintln!("{no_tip:?}: {}", assert_spend_refused(result));
                let after = store_snapshot(&scene.wallet).await;
                assert_eq!(after, before, "{no_tip:?}: nothing stored");
                assert_eq!(posts(&scene), 0, "{no_tip:?}: nothing posted");
            } else {
                assert!(result.unwrap().txid.is_some(), "{no_tip:?}");
                assert_eq!(posts(&scene), 1, "{no_tip:?}: posted");
            }
        }
    }
}

/// The same at signAction, where the action was kept for a retry: the
/// fork-ID-less input refused with the action kept, the valid one signed,
/// with no mining tip throughout.
#[tokio::test]
async fn sign_action_judges_its_own_input_at_the_latest_era_without_a_tip() {
    for no_tip in NO_TIPS {
        let scene = named_input_scene().await;
        let input_beef = supply_source(&scene).await;
        let mut args = supplied_args(&scene, input_beef, None, Respond::KnownTxid, &[]);
        args.options.as_mut().unwrap().sign_and_process = Some(false);
        let made = scene.wallet.create_action(args, Some(ORIGINATOR)).await;
        let signable = made.unwrap().signable_transaction.unwrap();
        let kept = Kept::read(&scene, &signable).await;
        let storage = scene.wallet.storage();
        storage.set_chain_tracker(std::sync::Arc::new(no_tip)).await;
        let bad = without_fork_id(caller_script(&scene, &signable));
        let result = sign_named(&scene, kept.reference.clone(), spend_map(0, bad, None)).await;
        eprintln!("{no_tip:?}: {}", assert_spend_refused(result));
        let script = caller_script(&scene, &signable);
        kept.then_signs(&scene, signable, script).await;
    }
}
