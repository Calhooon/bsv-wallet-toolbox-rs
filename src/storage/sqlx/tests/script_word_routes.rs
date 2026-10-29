//! #426 under the one-door rule (#454): the create route judges the
//! caller's unproven `inputBEEF` transactions the store does not hold under
//! the node's word, as the internalize door judges received bytes since
//! #422; the process route judges nothing the store holds a second time, so
//! a stored chain's unproven ancestry is accepted as stored. A new signed
//! transaction's own inputs are judged at its signing (#450).
//!
//! Mainnet fixtures: the proven grandparent at Genesis + 100 (post-Genesis,
//! pre-Chronicle), the tracker tip six blocks on, so every spend takes the
//! post-Genesis era of the block after the tip. The unproven parent spends
//! the grandparent's P2PK coin with an original-digest signature that has no
//! `SIGHASH_FORKID` (0x01): the structure and the roots are valid, and the
//! standard word refuses it with bsv-rs's text.

use super::process_action_internal;
use crate::services::mock::MockWalletServices;
use crate::storage::sqlx::create_action::create_action_internal;
use crate::storage::sqlx::StorageSqlx;
use crate::storage::traits::{
    StorageProcessActionArgs, WalletStorageProvider, WalletStorageWriter,
};
use crate::Error;
use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_original_for_signing, parse_transaction, SighashParams,
};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::beef_stream::SpendRefusal;
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, MockChainTracker, Transaction,
    TransactionInput, TransactionOutput,
};
use serde_json::json;
use std::sync::Arc;

const HEIGHT: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT + 100;
const FORKID: &str = "The signature must use SIGHASH_FORKID.";

/// One input spending `source:0` with `unlock`, one output.
fn spend(source: &str, unlock: &str, satoshis: u64, lock: &str) -> Transaction {
    let mut input = TransactionInput::new(source.to_string(), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex(unlock).unwrap());
    let output = TransactionOutput::new(satoshis, LockingScript::from_hex(lock).unwrap());
    Transaction::with_params(1, vec![input], vec![output], 0)
}

/// `child`'s input 0 signed over the original digest with hash type 0x01,
/// no `SIGHASH_FORKID`, for a coin of `value` locked by `coin`.
fn sign_without_forkid(
    child: &mut Transaction,
    coin: &LockingScript,
    value: u64,
    key: &PrivateKey,
) {
    let parsed = parse_transaction(&child.to_binary()).unwrap();
    let subscript = coin.to_binary();
    let digest = compute_sighash_original_for_signing(&SighashParams {
        version: parsed.version,
        inputs: &parsed.inputs,
        outputs: &parsed.outputs,
        locktime: parsed.locktime,
        input_index: 0,
        subscript: &subscript,
        satoshis: value,
        scope: 0x01,
    });
    let mut checksig = key.sign(&digest).unwrap().to_der();
    checksig.push(0x01);
    let mut push = vec![u8::try_from(checksig.len()).unwrap()];
    push.extend(checksig);
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_binary(&push).unwrap());
    child.invalidate_caches();
}

/// The unproven parent, its ancestry (the grandparent proven at `HEIGHT`)
/// and a tracker holding the grandparent's root. With `bad` the parent
/// spends a P2PK coin without `SIGHASH_FORKID`; without it an `OP_TRUE`
/// coin with an empty script, which the word passes.
fn lineage(bad: bool, parent_lock: &str) -> (Transaction, Beef, MockChainTracker) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let coin = if bad {
        format!("21{}ac", key.public_key().to_hex())
    } else {
        "51".to_string()
    };
    let grandparent = spend(&"11".repeat(32), "00", 10_000, &coin);
    let mut parent = spend(&grandparent.id(), "", 9_000, parent_lock);
    if bad {
        let coin = &grandparent.outputs[0].locking_script;
        sign_without_forkid(&mut parent, coin, 10_000, &key);
    }
    let bump = MerklePath::from_coinbase_txid(&grandparent.id(), HEIGHT);
    let mut tracker = MockChainTracker::new(HEIGHT + 6);
    tracker.add_root(HEIGHT, bump.compute_root(None).unwrap());
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(grandparent.to_binary(), Some(index));
    beef.merge_raw_tx(parent.to_binary(), None);
    (parent, beef, tracker)
}

async fn store() -> (StorageSqlx, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("script-word", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02script_word_user")
        .await
        .unwrap();
    (storage, user.user_id)
}

/// The refusal names input 0 of `refused` with bsv-rs's text, and the
/// host meets it as an argument refusal, as at internalize.
fn assert_forkid_refusal(error: Error, refused: &Transaction) {
    let Error::SpendRefused {
        txid,
        input: Some(0),
        why: SpendRefusal::Script(message),
        ..
    } = &error
    else {
        panic!("expected the input's script refusal, got {error:?}");
    };
    assert_eq!((txid, message.as_str()), (&refused.id(), FORKID));
    eprintln!("refused: {error}");
    let refusal = bsv_rs::Error::from(error);
    eprintln!("host: {refusal}");
    assert!(
        matches!(
            refusal,
            bsv_rs::Error::WalletRefusal {
                class: bsv_rs::WalletRefusalClass::InvalidParameter,
                ..
            }
        ),
        "{refusal:?}"
    );
}

/// Which input the post's fixture spends without `SIGHASH_FORKID`: none,
/// the unproven parent's, or the posted transaction's own (the parent's
/// coin then P2PK).
#[derive(Clone, Copy)]
enum Bad {
    None,
    Parent,
    Own,
}

/// The lineage and the transaction the wallet posts, spending the parent's
/// coin as `bad` says.
fn posted(bad: Bad) -> (Transaction, Beef, MockChainTracker, Transaction) {
    let key = PrivateKey::from_hex(&format!("{:064x}", 7)).unwrap();
    let parent_lock = match bad {
        Bad::Own => format!("21{}ac", key.public_key().to_hex()),
        _ => "51".to_string(),
    };
    let (parent, beef, tracker) = lineage(matches!(bad, Bad::Parent), &parent_lock);
    let mut subject = spend(&parent.id(), "", 8_000, "51");
    if let Bad::Own = bad {
        let coin = &parent.outputs[0].locking_script;
        sign_without_forkid(&mut subject, coin, 9_000, &key);
    }
    (parent, beef, tracker, subject)
}

// processAction: the stored post-BEEF (sendWith)

/// A stored `nosend` action whose input BEEF carries the parent unproven,
/// posted by `sendWith`, queued with `delayed`.
async fn post(
    bad: Bad,
    delayed: bool,
) -> (
    StorageSqlx,
    Arc<MockWalletServices>,
    Transaction,
    crate::Result<String>,
) {
    let (storage, user_id) = store().await;
    let (parent, mut beef, tracker, subject) = posted(bad);
    let services = Arc::new(MockWalletServices::new());
    storage.set_services(services.clone());
    storage.set_chain_tracker(Arc::new(tracker)).await;
    let txid = stored_nosend(&storage, user_id, &subject, &mut beef).await;
    let posted = send_with(&storage, user_id, txid, delayed).await;
    (storage, services, parent, posted)
}

/// A `sendWith` post of the stored `txid`, queued with `delayed`; its status.
async fn send_with(
    storage: &StorageSqlx,
    user_id: i64,
    txid: String,
    delayed: bool,
) -> crate::Result<String> {
    let args = json!({ "isNewTx": false, "isSendWith": true, "isNoSend": false,
        "isDelayed": delayed, "sendWith": [txid] });
    process_action_internal(storage, user_id, serde_json::from_value(args).unwrap())
        .await
        .map(|result| result.send_with_results.unwrap()[0].status.clone())
}

/// A stored `nosend` action signed as `subject`, its request carrying
/// `beef` as its input BEEF; its txid.
async fn stored_nosend(
    storage: &StorageSqlx,
    user_id: i64,
    subject: &Transaction,
    beef: &mut Beef,
) -> String {
    let (txid, raw, now) = (subject.id(), subject.to_binary(), chrono::Utc::now());
    let action = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
         lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?1, 'nosend', 'script-word', 1, -1000, 1, 0, 'script word', ?3, ?4, ?2, ?2)",
    )
    .bind(user_id)
    .bind(now)
    .bind(&txid)
    .bind(&raw)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef, history, notify, \
         created_at, updated_at) VALUES (?1, 'nosend', ?2, ?3, '{}', ?4, ?5, ?5)",
    )
    .bind(&txid)
    .bind(&raw)
    .bind(beef.to_binary())
    .bind(json!({ "transactionIds": [action] }).to_string())
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
    subject.id()
}

/// The one-door rule: the stored post-BEEF's unproven parent spends its
/// coin without `SIGHASH_FORKID`, and the store holds it (judged at its own
/// door), so the post judges it no second time: posted, the node's answer
/// being the host's.
#[tokio::test]
async fn process_post_accepts_a_stored_unproven_parent_without_forkid() {
    let (_, services, _, posted) = post(Bad::Parent, false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
}

/// The same on the queued path: queued, nothing posted.
#[tokio::test]
async fn process_post_delayed_accepts_a_stored_unproven_parent_without_forkid() {
    let (_, services, _, posted) = post(Bad::Parent, true).await;
    assert_eq!(posted.unwrap(), "sending");
    assert_eq!(services.call_count("post_beef"), 0);
}

/// Control: the same shape queued, every input passing the word.
#[tokio::test]
async fn process_post_delayed_queues_an_unproven_parent_the_word_passes() {
    let (_, services, _, posted) = post(Bad::None, true).await;
    assert_eq!(posted.unwrap(), "sending");
    assert_eq!(services.call_count("post_beef"), 0);
}

/// Control: the same shape, every input passing the word, is posted.
#[tokio::test]
async fn process_post_accepts_an_unproven_parent_the_word_passes() {
    let (_, services, _, posted) = post(Bad::None, false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
}

/// The posted action is the wallet's own signed transaction, judged at its
/// own signing (#450), not at a later post: a stored action's post judges
/// it no second time, the broadcaster's answer being the host's refusal.
#[tokio::test]
async fn process_post_leaves_the_posted_actions_inputs_to_the_node() {
    let (_, services, _, posted) = post(Bad::Own, false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
}

// processAction: a new signed transaction's post

/// A tracker that answers roots and height and supplies no mining tip, or
/// with `fault` one whose tip read faults.
struct NoTip {
    tracker: MockChainTracker,
    fault: bool,
}

#[async_trait::async_trait]
impl ChainTracker for NoTip {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        self.tracker.is_valid_root_for_height(root, height).await
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        self.tracker.current_height().await
    }

    async fn mining_tip(
        &self,
    ) -> Result<Option<bsv_rs::transaction::beef_stream::ChainTip>, ChainTrackerError> {
        if self.fault {
            return Err(ChainTrackerError::NetworkError("tip unavailable".into()));
        }
        Ok(None)
    }
}

/// How the fixture's tracker answers the mining tip.
#[derive(Clone, Copy, Debug)]
enum Tip {
    /// The tip six blocks past the grandparent's proof.
    Present,
    /// No mining tip.
    Missing,
    /// The tip read faults.
    Fault,
}

impl Tip {
    fn tracker(self, tracker: MockChainTracker) -> Arc<dyn ChainTracker> {
        match self {
            Tip::Present => Arc::new(tracker),
            Tip::Missing => Arc::new(NoTip {
                tracker,
                fault: false,
            }),
            Tip::Fault => Arc::new(NoTip {
                tracker,
                fault: true,
            }),
        }
    }
}

/// An unsigned action whose stored input BEEF carries the parent unproven,
/// signed as a child spending the parent's coin and processed for a post,
/// queued with `delayed`, the tracker answering the tip as `tip` says. The
/// signed child is returned with the call's answer.
async fn sign(
    bad: Bad,
    delayed: bool,
    tip: Tip,
) -> (StorageSqlx, Transaction, crate::Result<String>) {
    let (storage, user_id) = store().await;
    let (_, mut beef, tracker, child) = posted(bad);
    storage.set_services(Arc::new(MockWalletServices::new()));
    storage.set_chain_tracker(tip.tracker(tracker)).await;
    let args = StorageProcessActionArgs {
        is_new_tx: true,
        is_send_with: false,
        is_no_send: false,
        is_delayed: delayed,
        reference: Some("script-word-new".into()),
        txid: Some(child.id()),
        raw_tx: Some(child.to_binary()),
        send_with: vec![],
    };
    unsigned_action(&storage, user_id, &mut beef).await;
    let processed = process_action_internal(&storage, user_id, args)
        .await
        .map(|result| result.send_with_results.unwrap()[0].status.clone());
    (storage, child, processed)
}

/// The unsigned action `script-word-new`, its input BEEF `beef`.
async fn unsigned_action(storage: &StorageSqlx, user_id: i64, beef: &mut Beef) {
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
         lock_time, description, input_beef, created_at, updated_at) \
         VALUES (?1, 'unsigned', 'script-word-new', 1, -1000, 1, 0, 'script word', ?3, ?2, ?2)",
    )
    .bind(user_id)
    .bind(chrono::Utc::now())
    .bind(beef.to_binary())
    .execute(storage.writer())
    .await
    .unwrap();
}

/// The action's status and its request count after `sign`.
async fn signed_rows(storage: &StorageSqlx) -> (String, i64) {
    sqlx::query_as(
        "SELECT t.status, (SELECT COUNT(*) FROM proven_tx_reqs) FROM transactions t \
         WHERE t.reference = 'script-word-new'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap()
}

/// The one-door rule: the new transaction's stored input BEEF carries an
/// unproven parent that spends its coin without `SIGHASH_FORKID`. The
/// store holds it (the create door judged the caller's bytes once), so the
/// process route judges it no second time: signed and sent.
#[tokio::test]
async fn process_new_tx_accepts_a_stored_unproven_parent_without_forkid() {
    let (storage, _, processed) = sign(Bad::Parent, false, Tip::Present).await;
    assert_eq!(processed.unwrap(), "sending");
    assert_eq!(signed_rows(&storage).await, ("sending".into(), 1));
}

/// The same on the queued path.
#[tokio::test]
async fn process_new_tx_delayed_accepts_a_stored_unproven_parent_without_forkid() {
    let (storage, _, processed) = sign(Bad::Parent, true, Tip::Present).await;
    assert_eq!(processed.unwrap(), "sending");
    assert_eq!(signed_rows(&storage).await, ("sending".into(), 1));
}

/// The wallet's own signed transaction is judged at its signing (#450),
/// before the writer: its input signed without `SIGHASH_FORKID` is refused
/// with bsv-rs's text, class `InvalidParameter`, nothing written, the action
/// still `unsigned`. The reference judges after `processAction`
/// (`signAction.ts:76`, :90); here the refusal precedes the writes and the
/// post.
#[tokio::test]
async fn process_new_tx_refuses_its_own_input_without_forkid() {
    for delayed in [false, true] {
        let (storage, child, processed) = sign(Bad::Own, delayed, Tip::Present).await;
        assert_forkid_refusal(processed.unwrap_err(), &child);
        assert_eq!(signed_rows(&storage).await, ("unsigned".into(), 0));
    }
}

// processAction: a new signed transaction's call joining stored members

/// A transaction proven at `HEIGHT + 1`, its root added to `tracker`, and a
/// BEEF holding it alone.
fn proven(tracker: &mut MockChainTracker) -> (Transaction, Beef) {
    let source = spend(&"22".repeat(32), "00", 5_000, "51");
    let bump = MerklePath::from_coinbase_txid(&source.id(), HEIGHT + 1);
    tracker.add_root(HEIGHT + 1, bump.compute_root(None).unwrap());
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(source.to_binary(), Some(index));
    (source, beef)
}

/// A new transaction whose input BEEF holds a proven transaction alone, its
/// call joining the stored `nosend` member of `post` (its parent as `bad`
/// says) by `sendWith`, queued with `delayed`; the call's statuses.
async fn joined(bad: Bad, delayed: bool) -> (StorageSqlx, Transaction, crate::Result<Vec<String>>) {
    let (storage, user_id) = store().await;
    let (parent, mut beef, mut tracker, member) = posted(bad);
    let (source, mut input) = proven(&mut tracker);
    storage.set_services(Arc::new(MockWalletServices::new()));
    storage.set_chain_tracker(Arc::new(tracker)).await;
    let member = stored_nosend(&storage, user_id, &member, &mut beef).await;
    unsigned_action(&storage, user_id, &mut input).await;
    let child = spend(&source.id(), "", 4_000, "51");
    let args = StorageProcessActionArgs {
        is_new_tx: true,
        is_send_with: true,
        is_no_send: false,
        is_delayed: delayed,
        reference: Some("script-word-new".into()),
        txid: Some(child.id()),
        raw_tx: Some(child.to_binary()),
        send_with: vec![member],
    };
    let processed = process_action_internal(&storage, user_id, args)
        .await
        .map(|result| {
            let results = result.send_with_results.unwrap();
            results.into_iter().map(|sent| sent.status).collect()
        });
    (storage, parent, processed)
}

/// The new action's status, the member's two statuses and the request count.
async fn joined_rows(storage: &StorageSqlx) -> (String, String, String, i64) {
    sqlx::query_as(
        "SELECT n.status, t.status, r.status, (SELECT COUNT(*) FROM proven_tx_reqs) \
         FROM transactions n, transactions t JOIN proven_tx_reqs r ON r.txid = t.txid \
         WHERE n.reference = 'script-word-new' AND t.reference = 'script-word'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap()
}

/// The joined member's parent without `SIGHASH_FORKID`, sent now or
/// queued: the member and its stored ancestry are the store's, judged at
/// their own doors, so the call judges them no second time and sends both.
async fn assert_joined_accepted(delayed: bool) {
    let (storage, _, processed) = joined(Bad::Parent, delayed).await;
    assert_eq!(processed.unwrap(), ["sending", "sending"]);
    assert_eq!(
        joined_rows(&storage).await,
        ("sending".into(), "sending".into(), "unsent".into(), 2)
    );
}

/// The one-door rule: a new transaction's call joins a stored member whose
/// unproven parent spends its coin without `SIGHASH_FORKID`; accepted.
#[tokio::test]
async fn process_new_tx_accepts_a_joined_members_stored_parent_without_forkid() {
    assert_joined_accepted(false).await;
}

/// The same on the queued path.
#[tokio::test]
async fn process_new_tx_delayed_accepts_a_joined_members_stored_parent_without_forkid() {
    assert_joined_accepted(true).await;
}

/// Control: the joined member's parent passes the word; both are sent.
#[tokio::test]
async fn process_new_tx_sends_a_joined_member_the_word_passes() {
    let (storage, _, processed) = joined(Bad::None, false).await;
    assert_eq!(processed.unwrap(), ["sending", "sending"]);
    assert_eq!(
        joined_rows(&storage).await,
        ("sending".into(), "sending".into(), "unsent".into(), 2)
    );
}

// processAction: a post with nothing to judge

/// A stored `nosend` action spending a proven transaction, its input BEEF
/// holding that transaction alone, posted by `sendWith` (queued with
/// `delayed`) under a tracker that answers roots and supplies no mining tip.
async fn post_unjudged(delayed: bool) -> (Arc<MockWalletServices>, crate::Result<String>) {
    let (storage, user_id) = store().await;
    let mut tracker = MockChainTracker::new(HEIGHT + 6);
    let (source, mut beef) = proven(&mut tracker);
    let services = Arc::new(MockWalletServices::new());
    storage.set_services(services.clone());
    storage
        .set_chain_tracker(Tip::Missing.tracker(tracker))
        .await;
    let subject = spend(&source.id(), "", 4_000, "51");
    let txid = stored_nosend(&storage, user_id, &subject, &mut beef).await;
    (services, send_with(&storage, user_id, txid, delayed).await)
}

/// The witness: processAction judges nothing (the one-door rule, #454), so
/// no mining tip is asked and the post goes out.
#[tokio::test]
async fn process_post_with_nothing_to_judge_asks_no_tip() {
    let (services, posted) = post_unjudged(false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
}

/// The same on the queued path.
#[tokio::test]
async fn process_post_delayed_with_nothing_to_judge_asks_no_tip() {
    let (services, posted) = post_unjudged(true).await;
    assert_eq!(posted.unwrap(), "sending");
    assert_eq!(services.call_count("post_beef"), 0);
}

// createAction: the caller's inputBEEF

/// An action spending the caller's unproven parent, its ancestry in the
/// caller's `inputBEEF`; the caller signs later. With `stored` the store
/// holds the parent beforehand, as internalize leaves a received transaction;
/// with `txid_only` the caller asks `returnTXIDOnly`.
async fn create(
    bad: bool,
    stored: bool,
    txid_only: bool,
) -> (StorageSqlx, Transaction, crate::Result<()>) {
    let (storage, user_id) = store().await;
    let p2pkh = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";
    let (parent, mut beef, tracker) = lineage(bad, p2pkh);
    if stored {
        internalized(&storage, user_id, &parent, &mut beef).await;
    }
    let args = caller_args(&parent, &beef.to_binary(), txid_only);
    let created = create_action_internal(&storage, Some(&tracker), user_id, args)
        .await
        .map(|_| ());
    (storage, parent, created)
}

fn caller_args(
    parent: &Transaction,
    bytes: &[u8],
    txid_only: bool,
) -> bsv_rs::wallet::CreateActionArgs {
    serde_json::from_value(json!({
        "description": "script word create",
        "inputBEEF": bytes,
        "inputs": [{ "outpoint": format!("{}.0", parent.id()),
            "inputDescription": "the caller's coin", "unlockingScriptLength": 107 }],
        "outputs": [{ "lockingScript": parent.outputs[0].locking_script.to_hex(),
            "satoshis": 1_000, "outputDescription": "the recipient" }],
        "options": { "returnTXIDOnly": txid_only },
    }))
    .unwrap()
}

/// The witness: the caller's inputBEEF brings an unproven parent that
/// spends its coin without `SIGHASH_FORKID`. Refused before the write lock:
/// no action is written.
#[tokio::test]
async fn create_refuses_a_caller_unproven_parent_without_forkid() {
    assert_create_refused(false).await;
}

/// The judged set does not depend on the returned format: with
/// `returnTXIDOnly` no BEEF is returned, and the caller's unproven parent
/// is judged all the same, refused before anything is written.
#[tokio::test]
async fn create_txid_only_refuses_a_caller_unproven_parent_without_forkid() {
    assert_create_refused(true).await;
}

async fn assert_create_refused(txid_only: bool) {
    let (storage, parent, created) = create(true, false, txid_only).await;
    let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    eprintln!("returnTXIDOnly={txid_only}; action rows={actions}");
    assert_forkid_refusal(created.unwrap_err(), &parent);
    assert_eq!(actions, 0);
}

/// Control: the same shape, every input passing the word, is created.
#[tokio::test]
async fn create_accepts_a_caller_unproven_parent_the_word_passes() {
    let (_, _, created) = create(false, false, false).await;
    created.unwrap();
}

/// Control: with `returnTXIDOnly`, every input passing the word, created.
#[tokio::test]
async fn create_txid_only_accepts_a_caller_unproven_parent_the_word_passes() {
    let (_, _, created) = create(false, false, true).await;
    created.unwrap();
}

/// `parent` as internalize stores a received unproven transaction: its
/// action row and its request, `beef` its input BEEF.
async fn internalized(storage: &StorageSqlx, user_id: i64, parent: &Transaction, beef: &mut Beef) {
    let (txid, raw, now) = (parent.id(), parent.to_binary(), chrono::Utc::now());
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
         lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?1, 'unproven', 'script-word-in', 0, 9000, 1, 0, 'received', ?3, ?4, ?2, ?2)",
    )
    .bind(user_id)
    .bind(now)
    .bind(&txid)
    .bind(&raw)
    .execute(storage.writer())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef, history, notify, \
         created_at, updated_at) VALUES (?1, 'unmined', ?2, ?3, '{}', '{}', ?4, ?4)",
    )
    .bind(&txid)
    .bind(&raw)
    .bind(beef.to_binary())
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
}

/// The one-door rule: the caller's unproven parent without `SIGHASH_FORKID`
/// is one the store already holds (internalized earlier, judged at that
/// door), so the create door judges it no second time: created.
#[tokio::test]
async fn create_accepts_a_caller_parent_the_store_holds() {
    let (_, _, created) = create(true, true, false).await;
    created.unwrap();
}

/// The same with `returnTXIDOnly`: the store holds the parent, created.
#[tokio::test]
async fn create_txid_only_accepts_a_caller_parent_the_store_holds() {
    let (_, _, created) = create(true, true, true).await;
    created.unwrap();
}

/// An exactly framed caller BEEF can still lack the parent's ancestry.
/// Both returned formats refuse that chain before writing an action.
#[tokio::test]
async fn create_txid_only_refuses_missing_caller_ancestry() {
    for txid_only in [false, true] {
        let (storage, user_id) = store().await;
        let p2pkh = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";
        let (parent, complete, tracker) = lineage(false, p2pkh);
        let missing = complete
            .txs
            .iter()
            .find(|tx| tx.bump_index().is_some())
            .unwrap()
            .txid();
        let mut incomplete = Beef::new();
        incomplete.merge_raw_tx(parent.to_binary(), None);
        let bytes = incomplete.to_binary();
        let mut parsed = Beef::from_binary(&bytes).unwrap();
        assert!(!parsed.verify_valid(true).valid);
        let args = caller_args(&parent, &bytes, txid_only);
        let result = create_action_internal(&storage, Some(&tracker), user_id, args).await;
        let error = result.unwrap_err();
        let Error::ValidationError(message) = &error else {
            panic!("expected an inputBEEF structure refusal, got {error:?}");
        };
        assert!(message.starts_with("inputBEEF: BEEF structure is invalid:"));
        assert!(message.contains(&missing));
        let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        eprintln!("returnTXIDOnly={txid_only}: {error}; action rows={actions}");
        assert_eq!(actions, 0);
        assert!(matches!(
            bsv_rs::Error::from(error),
            bsv_rs::Error::WalletRefusal {
                class: bsv_rs::WalletRefusalClass::InvalidParameter,
                ..
            }
        ));
    }
}

#[path = "no_tip_word.rs"]
mod no_tip_word;
