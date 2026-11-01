//! #502: each overlay output's chain evidence verified with the tracker
//! before its token is read and fenced after the certificate's crypto, as
//! the reference's `IdentityEvidenceVerifier.parse` verifies it
//! (ts-stack@edf6e03 `identityUtils.ts:600-635`); before #502 the door read
//! the token from any BEEF and asked no tracker.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use bsv_rs::block::ChainParams;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::beef_stream::ChainTip;
use bsv_rs::transaction::{Beef, ChainTracker, ChainTrackerError, TransactionInput};

use super::super::fixture::{
    key, proven, proven_at, published, record_json, revealed, token, transaction, HEIGHT,
};
use super::{overlay_certificates, OverlayCertificate, EVIDENCE_LIMIT};
use crate::spend_verifier::{DOOR_WORK_BUDGET, WORK_OVERRIDE};

/// Holds every root but those at `refused`, and none after its opening
/// `holds` answers; its height is `height`.
struct Tracker {
    refused: u32,
    holds: usize,
    asked: AtomicUsize,
    height: u32,
}

fn tracker() -> Tracker {
    Tracker {
        refused: 0,
        holds: usize::MAX,
        asked: AtomicUsize::new(0),
        height: HEIGHT + 1000,
    }
}

#[async_trait]
impl ChainTracker for Tracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        let n = self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(height != self.refused && n < self.holds)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(self.height)
    }
}

async fn alice(serial: u8) -> OverlayCertificate {
    revealed(&key(9), &key(8), [serial; 32], &[("name", "Alice")]).await
}

fn alices_token(record: &OverlayCertificate) -> LockingScript {
    token(&key(8), record_json(record))
}

fn at(beef: Vec<u8>, output_index: u32) -> OutputListItem {
    OutputListItem {
        beef,
        output_index,
        context: None,
    }
}

/// The serials the door keeps from `outputs` against `tracker`.
async fn kept(outputs: Vec<OutputListItem>, tracker: &dyn ChainTracker) -> Vec<String> {
    let answer = overlay_certificates(outputs, tracker, &ChainParams::mainnet()).await;
    let serial = |c: OverlayCertificate| c.serial_number;
    answer.unwrap().into_iter().map(serial).collect()
}

/// `bad` is dropped from an answer beside a good output.
async fn only_the_good_output(bad: OutputListItem, tracker: &Tracker) {
    let good = alice(3).await;
    let answer = kept(vec![bad, published(&good)], tracker).await;
    assert_eq!(answer, vec![good.serial_number]);
}

/// The coordinator's `isValidRootForHeight` (`TransactionEvidenceCoordinator.ts:317-327`):
/// the subject's own token, proven at a height whose root the tracker does
/// not hold, is dropped.
#[tokio::test]
async fn an_output_whose_root_the_tracker_refuses_is_dropped() {
    let tx = transaction(alices_token(&alice(2).await));
    let tracker = Tracker {
        refused: HEIGHT + 1,
        ..tracker()
    };
    only_the_good_output(at(proven(&tx, HEIGHT + 1), 0), &tracker).await;
}

/// `assertUnconfirmedGraph` (`TransactionEvidence.ts:155-177`): the token's
/// transaction unproven and its parent not in the BEEF, or an unproven
/// transaction with no input and no proof (the base's fixture), is no
/// evidence; before #502 each was decoded.
#[tokio::test]
async fn an_output_without_its_chain_in_its_beef_is_dropped() {
    let record = alice(2).await;
    let orphan = transaction(alices_token(&record));
    let mut beef = Beef::new();
    beef.merge_raw_tx(orphan.to_binary(), None);
    only_the_good_output(at(beef.to_binary(), 0), &tracker()).await;
    let mut bare = transaction(alices_token(&record));
    bare.inputs.clear();
    only_the_good_output(at(bare.to_beef(true).unwrap(), 0), &tracker()).await;
    only_the_good_output(at(vec![], 0), &tracker()).await;
}

/// `loadEvidenceTransaction` (`TransactionEvidence.ts:118-121`): an Atomic
/// BEEF naming the token's transaction, which it does not carry, is dropped.
#[tokio::test]
async fn an_output_whose_transaction_is_not_in_its_beef_is_dropped() {
    let record = alice(2).await;
    let named = transaction(alices_token(&record));
    let mut subject = hex::decode(named.id()).unwrap();
    subject.reverse();
    let other = proven(&transaction(alices_token(&alice(4).await)), HEIGHT);
    let atomic = [&[1, 1, 1, 1][..], &subject, &other].concat();
    only_the_good_output(at(atomic, 0), &tracker()).await;
}

/// `loadEvidenceTransaction` (:126): an output index past the transaction's
/// outputs is dropped.
#[tokio::test]
async fn an_output_index_past_the_outputs_is_dropped() {
    let good = alice(2).await;
    let past = published(&good).beef;
    only_the_good_output(at(past, 1), &tracker()).await;
}

/// The fence after the certificate's crypto (`identityUtils.ts:611-612`,
/// the coordinator's `recheck`, :505-533): a root the tracker held on the
/// earlier pass and no longer holds drops the output.
#[tokio::test]
async fn an_output_whose_root_is_gone_at_the_fence_is_dropped() {
    let good = alice(2).await;
    let once = Tracker {
        holds: 1,
        ..tracker()
    };
    assert!(kept(vec![published(&good)], &once).await.is_empty());
    assert_eq!(once.asked.load(Ordering::SeqCst), 2);
    let always = tracker();
    assert_eq!(
        kept(vec![published(&good)], &always).await,
        vec![good.serial_number]
    );
}

/// Answers a root ask by its pass: `opening` until the evidence reads the
/// tracker's mining tip, which it does once its opening pass has held every
/// root (`held_roots`), and `fence` after; counts each pass's asks.
struct Flipping {
    opening: bool,
    fence: bool,
    tip_read: AtomicBool,
    asks: [AtomicUsize; 2],
}

fn flipping(opening: bool, fence: bool) -> Flipping {
    Flipping {
        opening,
        fence,
        tip_read: AtomicBool::new(false),
        asks: Default::default(),
    }
}

impl Flipping {
    /// The asks of the opening pass and of the fence.
    fn asks(&self) -> (usize, usize) {
        let [opening, fence] = &self.asks;
        (opening.load(Ordering::SeqCst), fence.load(Ordering::SeqCst))
    }
}

#[async_trait]
impl ChainTracker for Flipping {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        let fenced = self.tip_read.load(Ordering::SeqCst);
        self.asks[usize::from(fenced)].fetch_add(1, Ordering::SeqCst);
        Ok(if fenced { self.fence } else { self.opening })
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(HEIGHT + 1000)
    }

    async fn mining_tip(&self) -> std::result::Result<Option<ChainTip>, ChainTrackerError> {
        self.tip_read.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

/// #519: the opening ask's own witness. A root the tracker refuses at the
/// opening pass, and would hold at the fence, drops the output at the
/// opening: one ask, the fence never asked, so the read never reached the
/// identity token (the fence follows the token's decode and the
/// certificate's crypto). Red where the opening's answer is not taken,
/// since the fence would then hold the root and keep the output.
#[tokio::test]
async fn an_output_whose_root_is_refused_at_the_opening_is_dropped_before_its_token() {
    let good = alice(2).await;
    let tracker = flipping(false, true);
    assert!(kept(vec![published(&good)], &tracker).await.is_empty());
    assert_eq!(tracker.asks(), (1, 0));
}

/// `MerklePath.verify` (`MerklePath.ts:488-497`): the token's transaction at
/// its block's offset 0 is kept only 100 blocks deep.
#[tokio::test]
async fn a_coinbase_output_is_kept_only_100_blocks_deep() {
    let good = alice(2).await;
    let tx = transaction(alices_token(&good));
    let output = at(proven_at(&tx, HEIGHT, 0), 0);
    for (height, expected) in [(HEIGHT + 99, 0), (HEIGHT + 100, 1)] {
        let tracker = Tracker {
            height,
            ..tracker()
        };
        assert_eq!(kept(vec![output.clone()], &tracker).await.len(), expected);
    }
}

/// `queryOverlayEvidence`'s intake (`identityUtils.ts:422-437`): an answer
/// of 513 outputs is refused whole, the lookup's `limit`.
#[tokio::test]
async fn an_answer_past_the_output_limit_is_refused() {
    let outputs = vec![at(vec![0], 0); 513];
    let error = overlay_certificates(outputs, &tracker(), &ChainParams::mainnet())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("Service error: {EVIDENCE_LIMIT}")
    );
}

/// The unproven spend judged under the network's word (`tx.verify`,
/// `TransactionEvidenceCoordinator.ts:345-350`): the token's transaction
/// spending its proven parent's `OP_EQUAL` with `1 1` is kept, with `1 2`
/// dropped.
#[tokio::test]
async fn an_unproven_output_is_kept_only_when_its_spend_verifies() {
    let good = alice(2).await;
    let parent = transaction(LockingScript::from_hex("87").unwrap());
    let mut answer = Vec::new();
    for unlocking in ["5151", "5152"] {
        let mut child = transaction(alices_token(&good));
        let mut input = TransactionInput::new(parent.id(), 0);
        input.set_unlocking_script(bsv_rs::script::UnlockingScript::from_hex(unlocking).unwrap());
        child.inputs = vec![input];
        let mut beef = Beef::from_binary(&proven(&parent, HEIGHT)).unwrap();
        beef.merge_raw_tx(child.to_binary(), None);
        answer.push(kept(vec![at(beef.to_binary(), 0)], &tracker()).await.len());
    }
    assert_eq!(answer, vec![1, 0]);
}

/// Alice's token in a transaction spending outputs 0 to `inputs - 1` of
/// `parent` (each `OP_EQUAL`) with `1 1`, unproven, beside its proven parent.
async fn spending(parent: &bsv_rs::transaction::Transaction, inputs: u32) -> OutputListItem {
    let mut child = transaction(alices_token(&alice(2).await));
    child.inputs = (0..inputs)
        .map(|vout| {
            let mut input = TransactionInput::new(parent.id(), vout);
            input.set_unlocking_script(bsv_rs::script::UnlockingScript::from_hex("5151").unwrap());
            input
        })
        .collect();
    let mut beef = Beef::from_binary(&proven(parent, HEIGHT)).unwrap();
    beef.merge_raw_tx(child.to_binary(), None);
    at(beef.to_binary(), 0)
}

/// How many of `output` the door keeps with the doors' policy's
/// per-transaction figure lowered to `limit` through the test seam.
async fn kept_under(output: OutputListItem, limit: u64) -> usize {
    let budget = DOOR_WORK_BUDGET.with_per_transaction(Some(limit));
    WORK_OVERRIDE.with(|cell| cell.set(Some(budget)));
    let answer = kept(vec![output], &tracker()).await.len();
    WORK_OVERRIDE.with(|cell| cell.set(None));
    answer
}

/// The evidence judges an unproven transaction under both figures of the
/// doors' one policy (`DOOR_WORK_BUDGET`, bsv-rs's `WorkBudget`), as the
/// doors do: two inputs that each fit the transaction figure but together
/// pass it are not judged and the output is dropped, while twice the figure
/// keeps it. No test could spend 2^31, so the figure is lowered through
/// `WORK_OVERRIDE` (test builds only). Red where the evidence passed the
/// per-input figure alone (`with_work_budget`, before this fix).
#[tokio::test]
async fn an_output_past_the_transaction_work_budget_is_dropped() {
    let mut parent = transaction(LockingScript::from_hex("87").unwrap());
    parent.outputs.push(parent.outputs[0].clone());
    let fits = {
        let mut limit = 0;
        while kept_under(spending(&parent, 1).await, limit).await == 0 {
            limit += 1;
            assert!(limit < 1024, "one input's spend never fit");
        }
        limit
    };
    assert_eq!(kept_under(spending(&parent, 2).await, fits).await, 0);
    assert_eq!(kept_under(spending(&parent, 2).await, 2 * fits).await, 1);
}
