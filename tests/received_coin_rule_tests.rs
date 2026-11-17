//! The wallet charter's received-coin rule (R1 (d)) at `Wallet::create_action`
//! and `sign_action`: a received wallet payment whose transaction is not
//! `completed` is never taken by the action's own selection of change
//! unless the action names the rule with the label `underReceivedRule`.
//!
//! - an action without the label does not take it: the refusal, nothing
//!   recorded, the coin untouched; a host's offer of it is held the same;
//! - with the label (as given, lowercased, uppercased, padded) it is taken
//!   and the spend signs, run against the coin's P2PKH;
//! - a received payment whose transaction is `completed` is taken with no
//!   label;
//! - the wallet's own unproven change is taken with no label, and a chain
//!   of the wallet's own unconfirmed actions still builds;
//! - with both held, the wallet's own change is taken and the received coin
//!   passed over; past the change, the refusal without the label;
//! - the rows earlier releases wrote: the wallet's own change restored by
//!   internalizing it again is a received coin; a row that came by sync is
//!   stored as change and is selected as before;
//! - an input the caller names by outpoint is the caller's word and answers
//!   as before.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::collections::HashMap;

use bsv_rs::primitives::bsv::{parse_transaction, TxInput, TxOutput};
use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, Spend, SpendParams, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, MockChainTracker, Transaction, TransactionInput, TransactionOutput,
};
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionInput, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, Outpoint, SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::{RequestSyncChunkArgs, SyncOffset, WalletStorageSync};
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageProvider, WalletStorageWriter};

use common::{derived_key, other_lock, p2pkh_bytes, paying_derived, payment};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "cmVjZWl2ZWQtcHJlZml4";
const SUFFIX: &str = "cmVjZWl2ZWQtc3VmZml4";
const RULE: &str = "underReceivedRule";
/// The height a proven payment's proof names.
const PROVEN_AT: u32 = 100_010;
/// What an action without the label answers when the wallet's only coin is
/// an unproven received payment.
const HELD: &str = "wallet error: Insufficient funds: need 1005, have 0";

/// A received payment to `root` from a fresh sender: the AtomicBEEF, its
/// txid, the coin's lock. `proven`: the paying transaction carries its own
/// proof (stored `completed`); otherwise its parent does (stored
/// `unproven`). `salt` makes each payment distinct.
fn received(root: &PrivateKey, proven: bool, salt: u8) -> (Vec<u8>, String, Vec<u8>, PublicKey) {
    let sender = PrivateKey::random().public_key();
    let lock = p2pkh_bytes(&derived_key(root, &sender, PREFIX, SUFFIX));
    if !proven {
        let (tx, txid) = paying_derived(root, &sender, PREFIX, SUFFIX);
        return (tx, txid, lock, sender);
    }
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(format!("{salt:02x}").repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(9_500),
        locking_script: LockingScript::from_binary(&lock).unwrap(),
        change: false,
    });
    let txid = tx.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&txid, PROVEN_AT));
    beef.merge_raw_tx(tx.to_binary(), Some(bump));
    (beef.to_binary_atomic(&txid).unwrap(), txid, lock, sender)
}

fn internalize_args(tx: Vec<u8>, sender: &PublicKey) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![payment(0, PREFIX, SUFFIX, &sender.to_hex())],
        description: "a payment to the wallet".to_string(),
        labels: None,
        seek_permission: None,
    }
}

/// A wallet of `root` that internalized `tx` from `sender`. A payment
/// carrying its own proof (`received(.., true, ..)`) is read against a
/// storage tracker that knows its root, so it is stored `completed`.
async fn funded(root: &PrivateKey, tx: Vec<u8>, txid: &str, sender: &PublicKey) -> TestWallet {
    let wallet = common::wallet("received-coin-rule-tests", root, txid).await;
    let beef = Beef::from_binary(&tx).unwrap();
    if beef
        .find_txid(txid)
        .is_some_and(|t| t.bump_index().is_some())
    {
        let mut tracker = MockChainTracker::new(100_100);
        let root_hex = MerklePath::from_coinbase_txid(txid, PROVEN_AT)
            .compute_root(Some(txid))
            .unwrap();
        tracker.add_root(PROVEN_AT, root_hex);
        // The root of the proven parent every unproven payment spends
        // (`common::beef_paying`), for a payment internalized later.
        let (pending, _) = common::beef_paying(other_lock());
        let pending = Beef::from_binary(&pending).unwrap();
        let bump = &pending.bumps[0];
        tracker.add_root(bump.block_height, bump.compute_root(None).unwrap());
        wallet
            .storage()
            .set_chain_tracker(std::sync::Arc::new(tracker))
            .await;
        // The proof gate open, so the validated proof is stored.
        bsv_wallet_toolbox_rs::MonitorStorage::set_max_acceptable_proof_height(
            wallet.storage(),
            u32::MAX,
        )
        .await
        .unwrap();
    }
    wallet
        .internalize_action(internalize_args(tx, sender), "test.local")
        .await
        .expect("the payment");
    wallet
}

/// An action paying `satoshis` to another key under `labels`, funded by the
/// wallet's own selection (or the caller's `inputs`).
fn action(satoshis: u64, labels: &[&str], deferred: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a spend of the wallet's coins".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: other_lock().to_binary(),
            satoshis,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: (!labels.is_empty()).then(|| labels.iter().map(|l| l.to_string()).collect()),
        options: Some(CreateActionOptions {
            sign_and_process: Some(!deferred),
            no_send: Some(true),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
    }
}

/// `args` created (and with `deferred`, signed by `signAction`): the signed
/// transaction, or the refusal's text.
async fn spend(wallet: &TestWallet, args: CreateActionArgs) -> Result<Vec<u8>, String> {
    let deferred = args
        .options
        .as_ref()
        .and_then(|o| o.sign_and_process)
        .is_some_and(|s| !s);
    let created = wallet
        .create_action(args, "test.local")
        .await
        .map_err(|e| e.to_string())?;
    if !deferred {
        return Ok(created.tx.expect("the signed transaction"));
    }
    let signable = created
        .signable_transaction
        .expect("a signable transaction");
    let signed = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: String::from_utf8(signable.reference).unwrap(),
                options: None,
            },
            "test.local",
        )
        .await
        .map_err(|e| e.to_string())?;
    let beef = Beef::from_binary(&signed.tx.expect("the signed transaction")).unwrap();
    let txid = beef.atomic_txid.clone().unwrap();
    Ok(beef.find_txid(&txid).unwrap().raw_tx().unwrap().to_vec())
}

/// Whether `raw` spends `txid`.`vout` with an unlocking script that runs
/// clean against `lock` holding `satoshis`; `None` when it does not spend it.
fn spends(raw: &[u8], txid: &str, vout: u32, satoshis: u64, lock: &[u8]) -> Option<bool> {
    let parsed = parse_transaction(raw).expect("a transaction");
    let mut outpoint: [u8; 32] = hex::decode(txid).unwrap().try_into().unwrap();
    outpoint.reverse();
    let index = parsed
        .inputs
        .iter()
        .position(|i| i.txid == outpoint && i.output_index == vout)?;
    let ours = &parsed.inputs[index];
    let other_inputs: Vec<TxInput> = parsed
        .inputs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, i)| i.clone())
        .collect();
    let outputs: Vec<TxOutput> = parsed.outputs.clone();
    let mut spend = Spend::new(SpendParams {
        source_txid: ours.txid,
        source_output_index: vout,
        source_satoshis: satoshis,
        locking_script: LockingScript::from_binary(lock).unwrap(),
        transaction_version: parsed.version,
        other_inputs,
        outputs,
        input_index: index,
        unlocking_script: UnlockingScript::from_binary(&ours.script).unwrap(),
        input_sequence: ours.sequence,
        lock_time: parsed.locktime,
        memory_limit: None,
        work_limit: None,
    });
    Some(matches!(spend.validate(), Ok(true)))
}

/// The coin's transaction status, purpose, spendable flag and spender.
async fn coin(wallet: &TestWallet, txid: &str) -> (String, String, bool, Option<i64>) {
    sqlx::query_as(
        "SELECT t.status, o.purpose, o.spendable, o.spent_by FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.txid = ? AND o.vout = 0",
    )
    .bind(txid)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

/// The number of actions the wallet created.
async fn actions(wallet: &TestWallet) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE is_outgoing = 1")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// The txid of a signed transaction.
fn txid_of(raw: &[u8]) -> String {
    Transaction::from_binary(raw).unwrap().id()
}

/// The wallet's change output of the action `txid`, made spendable as if
/// the action had been sent (`unproven`): its vout, satoshis and lock.
async fn sent(wallet: &TestWallet, txid: &str) -> (u32, u64, Vec<u8>) {
    let pool = wallet.storage().writer();
    for sql in [
        "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
        "UPDATE outputs SET spendable = 1 WHERE txid = ? AND change = 1",
    ] {
        sqlx::query(sql).bind(txid).execute(pool).await.unwrap();
    }
    let (vout, satoshis, lock): (u32, i64, Vec<u8>) = sqlx::query_as(
        "SELECT vout, satoshis, locking_script FROM outputs WHERE txid = ? AND change = 1",
    )
    .bind(txid)
    .fetch_one(pool)
    .await
    .unwrap();
    (vout, satoshis as u64, lock)
}

#[tokio::test]
async fn an_unproven_received_coin_is_not_taken_without_the_label() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let (tx, txid, _, sender) = received(&root, false, 1);
        let wallet = funded(&root, tx, &txid, &sender).await;
        let before = coin(&wallet, &txid).await;
        assert_eq!(before, ("unproven".into(), "receive".into(), true, None));
        let answer = spend(&wallet, action(1_000, &[], deferred)).await;
        let err = wallet
            .create_action(action(1_000, &["other"], deferred), "test.local")
            .await
            .unwrap_err();
        println!(
            "unproven received, no label, deferred={deferred}: {answer:?}; class {err:?}; coin {:?}; actions {}",
            coin(&wallet, &txid).await,
            actions(&wallet).await
        );
        assert_eq!(answer.map(|_| ()), Err(HELD.to_string()));
        assert!(matches!(err, bsv_rs::Error::WalletError(ref m)
            if m == "Insufficient funds: need 1005, have 0"));
        assert_eq!(coin(&wallet, &txid).await, before, "the coin is untouched");
        assert_eq!(actions(&wallet).await, 0, "nothing is recorded");
        assert_eq!(wallet.balance().await.unwrap().total, 9_500);
    }
}

#[tokio::test]
async fn a_hosts_offer_of_an_unproven_received_coin_is_held_without_the_label() {
    let root = PrivateKey::random();
    let (tx, txid, lock, sender) = received(&root, false, 1);
    let wallet = funded(&root, tx, &txid, &sender).await;
    let output_id: i64 = sqlx::query_scalar("SELECT output_id FROM outputs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap();
    let without = wallet
        .create_action_offering(action(1_000, &[], false), "test.local", &[output_id])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    println!("offered, no label: {without:?}");
    assert_eq!(without, Err(HELD.to_string()));
    assert_eq!(actions(&wallet).await, 0);
    let with = wallet
        .create_action_offering(action(1_000, &[RULE], false), "test.local", &[output_id])
        .await
        .expect("offered under the label");
    let raw = with.tx.expect("signed");
    assert_eq!(spends(&raw, &txid, 0, 9_500, &lock), Some(true));
}

#[tokio::test]
async fn an_unproven_received_coin_is_taken_under_the_label_and_signs() {
    for label in [
        RULE,
        "underreceivedrule",
        "UNDERRECEIVEDRULE",
        " underReceivedRule\t",
    ] {
        for deferred in [false, true] {
            let root = PrivateKey::random();
            let (tx, txid, lock, sender) = received(&root, false, 1);
            let wallet = funded(&root, tx, &txid, &sender).await;
            let signed = spend(&wallet, action(1_000, &["other", label], deferred)).await;
            println!(
                "unproven received, label {label:?}, deferred={deferred}: {:?}",
                signed.as_ref().map(|_| "signed")
            );
            let signed = signed.unwrap();
            assert_eq!(spends(&signed, &txid, 0, 9_500, &lock), Some(true));
            // The interpreter refuses the same script against another lock
            // and another amount.
            assert_eq!(
                spends(&signed, &txid, 0, 9_500, &other_lock().to_binary()),
                Some(false)
            );
            assert_eq!(spends(&signed, &txid, 0, 9_499, &lock), Some(false));
        }
    }
}

#[tokio::test]
async fn a_completed_received_coin_is_taken_without_the_label() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let (tx, txid, lock, sender) = received(&root, true, 3);
        let wallet = funded(&root, tx, &txid, &sender).await;
        let row = coin(&wallet, &txid).await;
        println!("received, the row: {row:?}");
        assert_eq!(row, ("completed".into(), "receive".into(), true, None));
        let signed = spend(&wallet, action(1_000, &[], deferred)).await;
        println!(
            "completed received, no label, deferred={deferred}: {:?}",
            signed.as_ref().map(|_| "signed")
        );
        assert_eq!(spends(&signed.unwrap(), &txid, 0, 9_500, &lock), Some(true));
    }
}

#[tokio::test]
async fn the_wallets_own_unproven_change_is_taken_and_a_chain_builds() {
    for deferred in [false, true] {
        let root = PrivateKey::random();
        let (tx, txid, _, sender) = received(&root, true, 4);
        let wallet = funded(&root, tx, &txid, &sender).await;
        let mut parent = txid_of(&spend(&wallet, action(1_000, &[], false)).await.unwrap());
        // Three of the wallet's own actions, each spending the unproven
        // change of the one before, none labelled.
        for step in 0..3 {
            let (vout, satoshis, lock) = sent(&wallet, &parent).await;
            let row = coin_at(&wallet, &parent, vout).await;
            assert_eq!((row.0.as_str(), row.1.as_str()), ("unproven", "change"));
            let signed = spend(&wallet, action(1_000, &[], deferred)).await;
            println!(
                "own unproven change, step {step}, deferred={deferred}: {:?}",
                signed.as_ref().map(|_| "signed")
            );
            let signed = signed.unwrap();
            assert_eq!(spends(&signed, &parent, vout, satoshis, &lock), Some(true));
            parent = txid_of(&signed);
        }
    }
}

#[tokio::test]
async fn with_both_held_the_own_change_is_taken_and_the_received_coin_passed_over() {
    let root = PrivateKey::random();
    let (tx, txid, _, sender) = received(&root, true, 5);
    let wallet = funded(&root, tx, &txid, &sender).await;
    let spent_once = txid_of(&spend(&wallet, action(1_000, &[], false)).await.unwrap());
    let (vout, change, lock) = sent(&wallet, &spent_once).await;
    // An unproven received payment beside the wallet's own unproven change.
    let (pending, pending_txid, _, pending_sender) = received(&root, false, 1);
    wallet
        .internalize_action(internalize_args(pending, &pending_sender), "test.local")
        .await
        .expect("the pending payment");
    let held = coin(&wallet, &pending_txid).await;
    assert_eq!(held, ("unproven".into(), "receive".into(), true, None));
    println!("own change {change}, received 9500 pending");

    // Covered by the change: the change is taken, the received coin is not.
    let signed = spend(&wallet, action(1_000, &[], false)).await.unwrap();
    assert_eq!(
        spends(&signed, &spent_once, vout, change, &lock),
        Some(true)
    );
    assert_eq!(spends(&signed, &pending_txid, 0, 9_500, &lock), None);
    assert_eq!(coin(&wallet, &pending_txid).await, held);

    // Past what the change holds: the refusal without the label, the
    // received coin untouched.
    let second = txid_of(&signed);
    let (_, left, _) = sent(&wallet, &second).await;
    let made = actions(&wallet).await;
    let answer = spend(&wallet, action(left, &[], false)).await.map(|_| ());
    println!("past the own change ({left}): {answer:?}");
    assert_eq!(
        answer,
        Err(format!(
            "wallet error: Insufficient funds: need {}, have {left}",
            left + 20
        ))
    );
    assert_eq!(actions(&wallet).await, made, "nothing is recorded");
    assert_eq!(coin(&wallet, &pending_txid).await, held);
}

#[tokio::test]
async fn own_change_restored_by_internalizing_it_again_is_a_received_coin() {
    let root = PrivateKey::random();
    let (tx, txid, _, sender) = received(&root, true, 6);
    let wallet = funded(&root, tx.clone(), &txid, &sender).await;
    let raw = spend(&wallet, action(1_000, &[], false)).await.unwrap();
    let action_txid = txid_of(&raw);
    let (vout, satoshis, lock) = sent(&wallet, &action_txid).await;
    let (prefix, suffix): (String, String) = sqlx::query_as(
        "SELECT derivation_prefix, derivation_suffix FROM outputs WHERE txid = ? AND change = 1",
    )
    .bind(&action_txid)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap();
    // The row as an earlier release wrote it: reclassified out of change.
    sqlx::query(
        "UPDATE outputs SET change = 0, type = 'custom', derivation_prefix = NULL, \
         derivation_suffix = NULL WHERE txid = ? AND vout = ?",
    )
    .bind(&action_txid)
    .bind(vout)
    .execute(wallet.storage().writer())
    .await
    .unwrap();
    let mut beef = Beef::from_binary(&tx).unwrap();
    beef.merge_raw_tx(raw, None);
    let atomic = beef.to_binary_atomic(&action_txid).unwrap();
    wallet
        .internalize_action(
            InternalizeActionArgs {
                tx: atomic,
                outputs: vec![payment(vout, &prefix, &suffix, "")],
                description: "the wallet's own change".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .expect("restored");
    let (status, purpose, _, _) = coin_at(&wallet, &action_txid, vout).await;
    println!("restored own change: status {status}, purpose {purpose}");
    assert_eq!((status.as_str(), purpose.as_str()), ("unproven", "receive"));
    let without = spend(&wallet, action(1_000, &[], false)).await.map(|_| ());
    println!("restored own change, no label: {without:?}");
    assert!(without
        .as_ref()
        .is_err_and(|e| e.starts_with("wallet error: Insufficient funds: need ")));
    let with = spend(&wallet, action(1_000, &[RULE], false)).await.unwrap();
    assert_eq!(
        spends(&with, &action_txid, vout, satoshis, &lock),
        Some(true)
    );
}

/// `coin` for output `vout`.
async fn coin_at(
    wallet: &TestWallet,
    txid: &str,
    vout: u32,
) -> (String, String, bool, Option<i64>) {
    sqlx::query_as(
        "SELECT t.status, o.purpose, o.spendable, o.spent_by FROM outputs o \
         JOIN transactions t ON t.transaction_id = o.transaction_id \
         WHERE o.txid = ? AND o.vout = ?",
    )
    .bind(txid)
    .bind(vout)
    .fetch_one(wallet.storage().writer())
    .await
    .unwrap()
}

#[tokio::test]
async fn a_received_coin_that_came_by_sync_is_stored_as_change_and_selected_as_before() {
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    let (tx, txid, lock, sender) = received(&root, false, 1);
    let source = funded(&root, tx, &txid, &sender).await;
    let target = common::wallet("received-coin-rule-tests", &root, &txid).await;
    let args = RequestSyncChunkArgs {
        from_storage_identity_key: source.storage().storage_identity_key().to_string(),
        to_storage_identity_key: target.storage().storage_identity_key().to_string(),
        identity_key: identity.clone(),
        since: None,
        max_rough_size: 10_000_000,
        max_items: 1_000,
        offsets: [
            "outputBasket",
            "provenTx",
            "provenTxReq",
            "txLabel",
            "outputTag",
            "transaction",
            "output",
            "txLabelMap",
            "outputTagMap",
        ]
        .iter()
        .map(|name| SyncOffset {
            name: name.to_string(),
            offset: 0,
        })
        .collect(),
    };
    let chunk = source
        .storage()
        .get_sync_chunk(args.clone())
        .await
        .expect("chunk");
    target
        .storage()
        .process_sync_chunk(args, chunk)
        .await
        .expect("processed");
    let row = coin(&target, &txid).await;
    println!("synced received coin, the row: {row:?}");
    assert_eq!(row, ("unproven".into(), "change".into(), true, None));
    let answer = spend(&target, action(1_000, &[], false)).await;
    println!(
        "synced received coin, no label: {:?}",
        answer.as_ref().map(|_| "signed")
    );
    assert_eq!(spends(&answer.unwrap(), &txid, 0, 9_500, &lock), Some(true));
}

#[tokio::test]
async fn an_input_the_caller_names_answers_as_before() {
    // A received wallet payment is wallet-managed change: the caller cannot
    // name it, with the label or without, whatever its transaction's status.
    for proven in [false, true] {
        let root = PrivateKey::random();
        let (tx, txid, _, sender) = received(&root, proven, 7);
        let wallet = funded(&root, tx, &txid, &sender).await;
        for labels in [&[][..], &[RULE]] {
            let mut args = action(1_000, labels, false);
            args.inputs = Some(vec![CreateActionInput {
                outpoint: Outpoint::from_string(&format!("{txid}.0")).unwrap(),
                input_description: "the received coin".to_string(),
                unlocking_script: None,
                unlocking_script_length: Some(108),
                sequence_number: None,
            }]);
            let answer = spend(&wallet, args).await.map(|_| ());
            println!("named by outpoint, proven={proven}, labels {labels:?}: {answer:?}");
            assert_eq!(
                answer,
                Err(format!(
                    "wallet error: Validation error: inputs[0]: cannot spend change output \
                     {txid}:0 directly. Change is managed by the wallet."
                ))
            );
        }
    }
}

/// The storage door answers the same as the wallet's.
#[tokio::test]
async fn the_storage_door_holds_it_without_the_label() {
    let root = PrivateKey::random();
    let (tx, txid, _, sender) = received(&root, false, 1);
    let wallet = funded(&root, tx, &txid, &sender).await;
    let auth = common::auth(&wallet, &root).await;
    let without = wallet
        .storage()
        .create_action(&auth, action(1_000, &[], true))
        .await
        .map(|r| r.inputs.len())
        .map_err(|e| e.to_string());
    println!("storage door, no label: {without:?}");
    assert_eq!(
        without,
        Err("Insufficient funds: need 1005, have 0".to_string())
    );
    let with = wallet
        .storage()
        .create_action(&auth, action(1_000, &[RULE], true))
        .await
        .map(|r| r.inputs.len())
        .map_err(|e| e.to_string());
    println!("storage door, the label: {with:?}");
    assert_eq!(with, Ok(1));
}
