//! #426: the create and process routes judge the unproven transactions they
//! accept under the node's word, as the internalize door does since #422.
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

/// The refusal names the parent's input 0 with bsv-rs's text, and the
/// host meets it as an argument refusal, as at internalize.
fn assert_forkid_refusal(error: Error, parent: &Transaction) {
    let Error::SpendRefused {
        txid,
        input: Some(0),
        why: SpendRefusal::Script(message),
        ..
    } = &error
    else {
        panic!("expected the parent's script refusal, got {error:?}");
    };
    assert_eq!((txid, message.as_str()), (&parent.id(), FORKID));
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

/// The parent without `SIGHASH_FORKID`, posted now or queued: refused
/// before anything is written, queued or posted.
async fn assert_post_refused(delayed: bool) {
    let (storage, services, parent, posted) = post(Bad::Parent, delayed).await;
    assert_forkid_refusal(posted.unwrap_err(), &parent);
    assert_eq!(services.call_count("post_beef"), 0);
    let statuses: (String, String) = sqlx::query_as(
        "SELECT t.status, r.status FROM transactions t JOIN proven_tx_reqs r ON r.txid = t.txid",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(statuses, ("nosend".into(), "nosend".into()));
}

/// The witness: the post-BEEF's unproven parent spends its coin without
/// `SIGHASH_FORKID`. Refused before anything is written or posted.
#[tokio::test]
async fn process_post_refuses_an_unproven_parent_without_forkid() {
    assert_post_refused(false).await;
}

/// The witness on the queued path: a delayed `sendWith` takes the same
/// verdict before anything is queued.
#[tokio::test]
async fn process_post_delayed_refuses_an_unproven_parent_without_forkid() {
    assert_post_refused(true).await;
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

/// The posted action is the wallet's own signed transaction, outside the
/// judged set as at a new transaction's post (#450): accepted and posted,
/// the broadcaster's answer being the host's refusal.
#[tokio::test]
async fn process_post_leaves_the_posted_actions_inputs_to_the_node() {
    let (_, services, _, posted) = post(Bad::Own, false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
}

// processAction: a new signed transaction's post

/// A tracker that answers roots and height and supplies no mining tip.
struct NoTip(MockChainTracker);

#[async_trait::async_trait]
impl ChainTracker for NoTip {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        self.0.is_valid_root_for_height(root, height).await
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        self.0.current_height().await
    }
}

/// An unsigned action whose stored input BEEF carries the parent unproven,
/// signed as a child spending the parent's coin and processed for a post,
/// queued with `delayed`; without `tip` the tracker supplies no mining tip.
async fn sign(
    bad: Bad,
    delayed: bool,
    tip: bool,
) -> (StorageSqlx, Transaction, crate::Result<String>) {
    let (storage, user_id) = store().await;
    let (parent, mut beef, tracker, child) = posted(bad);
    storage.set_services(Arc::new(MockWalletServices::new()));
    if tip {
        storage.set_chain_tracker(Arc::new(tracker)).await;
    } else {
        storage.set_chain_tracker(Arc::new(NoTip(tracker))).await;
    }
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
    (storage, parent, processed)
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

/// The witness: the new transaction's post-BEEF carries an unproven parent
/// that spends its coin without `SIGHASH_FORKID`. Refused before the
/// writer is taken: the action stays unsigned and no request is written.
#[tokio::test]
async fn process_new_tx_refuses_an_unproven_parent_without_forkid() {
    let (storage, parent, processed) = sign(Bad::Parent, false, true).await;
    assert_forkid_refusal(processed.unwrap_err(), &parent);
    assert_eq!(signed_rows(&storage).await, ("unsigned".into(), 0));
}

/// The witness on the queued path: a delayed send takes the same verdict
/// before anything is queued.
#[tokio::test]
async fn process_new_tx_delayed_refuses_an_unproven_parent_without_forkid() {
    let (storage, parent, processed) = sign(Bad::Parent, true, true).await;
    assert_forkid_refusal(processed.unwrap_err(), &parent);
    assert_eq!(signed_rows(&storage).await, ("unsigned".into(), 0));
}

/// The wallet's own transaction is outside the judged set: its input signed
/// without `SIGHASH_FORKID` is accepted by the door. Under the keeper's
/// reading of #450 the node judges the wallet's own signed transaction at
/// broadcast, and the broadcaster's answer is the host's refusal.
#[tokio::test]
async fn process_new_tx_leaves_its_own_inputs_to_the_node() {
    let (storage, _, processed) = sign(Bad::Own, false, true).await;
    assert_eq!(processed.unwrap(), "sending");
    assert_eq!(signed_rows(&storage).await, ("sending".into(), 1));
}

/// A tracker with no mining tip holds the post when a transaction is there
/// to judge; the host meets the `WalletError` text with its `Service error: `
/// prefix, as the CHANGELOG quotes it.
#[tokio::test]
async fn process_new_tx_holds_without_a_mining_tip() {
    let (storage, _, processed) = sign(Bad::Parent, false, false).await;
    let refusal = bsv_rs::Error::from(processed.unwrap_err());
    eprintln!("host: {refusal:?}");
    let bsv_rs::Error::WalletError(message) = &refusal else {
        panic!("expected WalletError, got {refusal:?}");
    };
    assert_eq!(
        message,
        "Service error: ChainTracker verification failed: the header service supplied no mining tip"
    );
    assert_eq!(signed_rows(&storage).await, ("unsigned".into(), 0));
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
/// queued: refused before the writer is taken, nothing written or queued.
async fn assert_joined_refused(delayed: bool) {
    let (storage, parent, processed) = joined(Bad::Parent, delayed).await;
    assert_forkid_refusal(processed.unwrap_err(), &parent);
    assert_eq!(
        joined_rows(&storage).await,
        ("unsigned".into(), "nosend".into(), "nosend".into(), 1)
    );
}

/// The witness: a new transaction's call joins a stored member whose
/// unproven parent spends its coin without `SIGHASH_FORKID`; the member's
/// ancestry is judged with the new transaction's.
#[tokio::test]
async fn process_new_tx_refuses_a_joined_members_parent_without_forkid() {
    assert_joined_refused(false).await;
}

/// The witness on the queued path.
#[tokio::test]
async fn process_new_tx_delayed_refuses_a_joined_members_parent_without_forkid() {
    assert_joined_refused(true).await;
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
    storage.set_chain_tracker(Arc::new(NoTip(tracker))).await;
    let subject = spend(&source.id(), "", 4_000, "51");
    let txid = stored_nosend(&storage, user_id, &subject, &mut beef).await;
    (services, send_with(&storage, user_id, txid, delayed).await)
}

/// The witness: nothing unproven beside the posted root, so the judged set
/// is empty; no mining tip is asked and the post goes out.
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
/// caller's `inputBEEF`; the caller signs later.
async fn create(bad: bool) -> (StorageSqlx, Transaction, crate::Result<()>) {
    let (storage, user_id) = store().await;
    let p2pkh = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";
    let (parent, mut beef, tracker) = lineage(bad, p2pkh);
    let args = json!({
        "description": "script word create",
        "inputBEEF": beef.to_binary(),
        "inputs": [{ "outpoint": format!("{}.0", parent.id()),
            "inputDescription": "the caller's coin", "unlockingScriptLength": 107 }],
        "outputs": [{ "lockingScript": p2pkh, "satoshis": 1_000,
            "outputDescription": "the recipient" }],
    });
    let args = serde_json::from_value(args).unwrap();
    let created = create_action_internal(&storage, Some(&tracker), user_id, args)
        .await
        .map(|_| ());
    (storage, parent, created)
}

/// The witness: the caller's inputBEEF brings an unproven parent that
/// spends its coin without `SIGHASH_FORKID`. Refused before the write lock:
/// no action is written.
#[tokio::test]
async fn create_refuses_a_caller_unproven_parent_without_forkid() {
    let (storage, parent, created) = create(true).await;
    assert_forkid_refusal(created.unwrap_err(), &parent);
    let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(actions, 0);
}

/// Control: the same shape, every input passing the word, is created.
#[tokio::test]
async fn create_accepts_a_caller_unproven_parent_the_word_passes() {
    let (_, _, created) = create(false).await;
    created.unwrap();
}
