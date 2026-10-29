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
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
use crate::Error;
use bsv_rs::primitives::bsv::sighash::{
    compute_sighash_original_for_signing, parse_transaction, SighashParams,
};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::beef_stream::SpendRefusal;
use bsv_rs::transaction::{
    Beef, MerklePath, MockChainTracker, Transaction, TransactionInput, TransactionOutput,
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

// processAction: the stored post-BEEF (sendWith)

/// A stored `nosend` action whose input BEEF carries the parent unproven,
/// posted by `sendWith`.
async fn post(
    bad: bool,
) -> (
    StorageSqlx,
    Arc<MockWalletServices>,
    Transaction,
    crate::Result<String>,
) {
    let (storage, user_id) = store().await;
    let (parent, mut beef, tracker) = lineage(bad, "51");
    let subject = spend(&parent.id(), "", 8_000, "51");
    let services = Arc::new(MockWalletServices::new());
    storage.set_services(services.clone());
    storage.set_chain_tracker(Arc::new(tracker)).await;
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
    let args = json!({ "isNewTx": false, "isSendWith": true, "isNoSend": false,
        "isDelayed": false, "sendWith": [txid] });
    let posted = process_action_internal(&storage, user_id, serde_json::from_value(args).unwrap())
        .await
        .map(|result| result.send_with_results.unwrap()[0].status.clone());
    (storage, services, parent, posted)
}

/// The witness: the post-BEEF's unproven parent spends its coin without
/// `SIGHASH_FORKID`. Refused before anything is written or posted.
#[tokio::test]
async fn process_post_refuses_an_unproven_parent_without_forkid() {
    let (storage, services, parent, posted) = post(true).await;
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

/// Control: the same shape, every input passing the word, is posted.
#[tokio::test]
async fn process_post_accepts_an_unproven_parent_the_word_passes() {
    let (_, services, _, posted) = post(false).await;
    assert_eq!(posted.unwrap(), "unproven");
    assert_eq!(services.call_count("post_beef"), 1);
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
