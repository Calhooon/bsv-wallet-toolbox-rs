//! A coin of the wallet's own change, or a received payment, carried
//! through to a signed spend: a change row whose stored sender identity
//! key is an empty string, and the restore of a row an earlier basket
//! insertion reclassified, each ending in `Wallet::create_action` (or
//! `sign_action` where the action is deferred) signing the coin, the
//! unlocking script checked against the coin's P2PKH under the node's word.
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.
//!
//! The spend selects the coin through the allocator: it is the only
//! spendable change, its transaction `unproven`, and the action carries the
//! `underReceivedRule` label, so a received coin (purpose `receive`) whose
//! transaction is not `completed` is eligible (R1 (d),
//! `create_action/allocation.rs`). The wallet's own change from a `noSend`
//! action is moved from `nosend` to `unproven`, as a broadcast would leave
//! it, since the allocator takes only `completed` or `unproven` parents.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/insertion.rs"]
mod insertion;

use std::collections::HashMap;
use std::sync::Arc;

use bsv_rs::primitives::bsv::sighash::{parse_transaction, TxSighashCache};
use bsv_rs::primitives::{to_base64, PrivateKey};
use bsv_rs::script::{LockingScript, ProtocolEra, ScriptFlags, Spend, TxSpendParams};
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, SignActionArgs, WalletInterface,
};

use common::{args, other_lock};
use insertion::*;

const SPEND: &str = "a spend of the restored coin";

/// Moves the action `txid` to `unproven`, as its broadcast would.
async fn unproven(wallet: &TestWallet, txid: &str) {
    sqlx::query("UPDATE transactions SET status = 'unproven' WHERE txid = ?")
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

/// The coin's spendable flag, spender and satoshis.
async fn coin(wallet: &TestWallet, txid: &str, vout: u32) -> (i64, Option<i64>, i64) {
    sqlx::query_as("SELECT spendable, spent_by, satoshis FROM outputs WHERE txid = ? AND vout = ?")
        .bind(txid)
        .bind(vout)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

/// The number of actions recorded with the spend's description.
async fn spends_recorded(wallet: &TestWallet) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE description = ?")
        .bind(SPEND)
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

fn spend_args(sign_and_process: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: SPEND.to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: other_lock().to_binary(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["underReceivedRule".to_string()]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(sign_and_process),
            ..Default::default()
        }),
    }
}

/// The transaction in an answer's `tx`: atomic BEEF, or the raw bytes.
fn signed_tx(bytes: &[u8]) -> Transaction {
    Transaction::from_atomic_beef(bytes)
        .or_else(|_| Transaction::from_binary(bytes))
        .expect("the signed transaction")
}

/// A spend of one 1000-satoshi output that needs change: through
/// `create_action` with signing, or deferred and signed by `sign_action`.
/// Answers the signed transaction, or the refusal's text.
async fn spend(wallet: &TestWallet, deferred: bool) -> Result<Transaction, String> {
    let made = wallet
        .create_action(spend_args(!deferred), Some("test.local"))
        .await
        .map_err(|e| format!("createAction: {e}"))?;
    if !deferred {
        return Ok(signed_tx(&made.tx.expect("the signed transaction")));
    }
    let signable = made.signable_transaction.expect("a signable transaction");
    let signed = wallet
        .sign_action(
            SignActionArgs {
                spends: HashMap::new(),
                reference: to_base64(&signable.reference),
                options: None,
            },
            Some("test.local"),
        )
        .await
        .map_err(|e| format!("signAction: {e}"))?;
    Ok(signed_tx(&signed.tx.expect("the signed transaction")))
}

/// The input of `tx` spending `txid.vout`, its unlocking script run against
/// the coin's stored P2PKH under the node's post-Chronicle word.
async fn spends_the_coin(wallet: &TestWallet, tx: &Transaction, txid: &str, vout: u32) {
    let (script, satoshis): (Vec<u8>, i64) =
        sqlx::query_as("SELECT locking_script, satoshis FROM outputs WHERE txid = ? AND vout = ?")
            .bind(txid)
            .bind(vout)
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap();
    let lock = LockingScript::from_binary(&script).unwrap();
    assert_eq!(script.len(), 25, "a P2PKH lock");
    assert_eq!(tx.inputs().len(), 1, "the coin alone funds the spend");
    let input = &tx.inputs()[0];
    assert_eq!(
        (input.get_source_txid().unwrap(), input.source_output_index),
        (txid.to_string(), vout),
        "the spend selects the coin"
    );
    let raw = parse_transaction(&tx.to_binary()).unwrap();
    let cache = Arc::new(TxSighashCache::new(
        raw.version,
        raw.inputs,
        raw.outputs,
        raw.locktime,
    ));
    let mut check = Spend::with_transaction(TxSpendParams {
        transaction: cache,
        input_index: 0,
        source_satoshis: u64::try_from(satoshis).unwrap(),
        locking_script: lock,
        unlocking_script: input.unlocking_script.clone().expect("an unlocking script"),
        memory_limit: None,
        work_limit: None,
    })
    .unwrap();
    check.set_flags(ScriptFlags::standard(ProtocolEra::PostChronicle));
    assert_eq!(check.validate().map_err(|e| e.message), Ok(true));
}

/// A wallet whose only spendable coin is the change of an action it made,
/// that action `unproven`: the wallet, the action's atomic BEEF, its txid
/// and the change's index.
async fn with_own_change(root: &PrivateKey) -> (TestWallet, Vec<u8>, String, u32) {
    let (wallet, funding) = funded(root).await;
    let (bytes, txid, vout) = own_change(&wallet, &funding).await;
    unproven(&wallet, &txid).await;
    (wallet, bytes, txid, vout)
}

/// A change row whose stored sender identity key is an empty string, as
/// the stable line's door stores an empty sender (written here by SQL): a
/// spend that must select it signs it with the wallet's own derivation.
async fn empty_sender_row(deferred: bool) {
    let (wallet, _, txid, vout) = with_own_change(&PrivateKey::random()).await;
    sqlx::query("UPDATE outputs SET sender_identity_key = '' WHERE txid = ? AND vout = ?")
        .bind(&txid)
        .bind(vout)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    let answer = spend(&wallet, deferred).await;
    let tx = answer.expect("the empty sender's row signs");
    spends_the_coin(&wallet, &tx, &txid, vout).await;
}

#[tokio::test]
async fn a_change_row_with_an_empty_sender_is_signed_by_create_action() {
    empty_sender_row(false).await;
}

#[tokio::test]
async fn a_change_row_with_an_empty_sender_is_signed_by_sign_action() {
    empty_sender_row(true).await;
}

/// The wallet's own change an earlier basket insertion reclassified,
/// restored by a wallet payment of the output with the prefix and suffix
/// it was derived with and the wallet's own identity key as the sender
/// (the sender `a_reclassified_change_row_is_restored_only_with_its_own_derivation`
/// gives): the stored sender is that key, and a spend that selects the
/// coin signs it.
#[tokio::test]
async fn a_restored_change_row_with_the_wallets_identity_key_is_spent() {
    let root = PrivateKey::random();
    let (wallet, bytes, txid, vout) = with_own_change(&root).await;
    let (_, _, _, prefix, suffix, _) = row(&wallet, &txid, vout).await;
    let (prefix, suffix) = (prefix.unwrap(), suffix.unwrap());
    reclassify(&wallet, &txid, vout).await;
    assert_eq!(balance(&wallet).await, 0);

    // An empty sender, the stable line's restore, is refused on this line
    // before anything is stored.
    let mut empty = common::payment(&root.public_key(), &prefix, &suffix);
    empty.output_index = vout;
    if let Some(remittance) = empty.payment_remittance.as_mut() {
        remittance.sender_identity_key = String::new();
    }
    let refused = at_wallet(&wallet, args(bytes.clone(), empty))
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        (
            bsv_rs::WalletRefusalClass::InvalidParameter,
            "Validation error: The senderIdentityKey parameter must be hexadecimal string."
                .to_string()
        )
    );
    assert_eq!(row(&wallet, &txid, vout).await.1, "custom");

    let me = root.public_key();
    assert_eq!(me.to_hex(), wallet.identity_key());
    let mut payment = common::payment(&me, &prefix, &suffix);
    payment.output_index = vout;
    assert_eq!(at_wallet(&wallet, args(bytes, payment)).await, Ok(true));
    let restored = row(&wallet, &txid, vout).await;
    assert_eq!(restored.5, Some(me.to_hex()), "the stored sender");
    assert_eq!(origin(&wallet, &txid, vout).await.1, "receive");

    // The restored row's purpose is `receive`: while its transaction is
    // unproven an action without the `underReceivedRule` label does not
    // select it.
    let mut ordinary = spend_args(true);
    ordinary.labels = None;
    let unselected = wallet
        .create_action(ordinary, Some("test.local"))
        .await
        .map(|made| made.txid.map(hex::encode))
        .map_err(|e| e.to_string());
    println!("an ordinary action while unproven: {unselected:?}");
    let unselected = unselected.expect_err("not selected while unproven");
    assert!(unselected.contains("Insufficient funds"), "{unselected}");
    assert_eq!(spends_recorded(&wallet).await, 0);

    let tx = spend(&wallet, false)
        .await
        .expect("the restored coin signs");
    spends_the_coin(&wallet, &tx, &txid, vout).await;
}

/// A received payment an earlier basket insertion reclassified: the same
/// prefix and suffix under the wallet's own key is refused, and the wallet
/// payment with the prefix, suffix and the sender identity key the row held
/// restores it; a spend that selects the coin signs it.
#[tokio::test]
async fn a_restored_received_payment_with_its_sender_is_spent() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = common::beef_paying(lock);
    let wallet = wallet(&root, &txid).await;
    let payment = common::payment(&sender, PREFIX, SUFFIX);
    assert_eq!(
        at_wallet(&wallet, args(bytes.clone(), payment.clone())).await,
        Ok(true)
    );
    reclassify(&wallet, &txid, 0).await;
    assert_eq!(balance(&wallet).await, 0);

    // The same prefix and suffix under the wallet's own key is refused: the
    // door derives with the remittance's sender as the counterparty.
    let under_own_key = common::payment(&root.public_key(), PREFIX, SUFFIX);
    let refused = at_wallet(&wallet, args(bytes.clone(), under_own_key))
        .await
        .unwrap_err();
    println!("restored under the wallet's own key: {refused:?}");
    assert!(
        refused
            .1
            .starts_with("Derivation mismatch for output 0: derived "),
        "{}",
        refused.1
    );
    assert_eq!(row(&wallet, &txid, 0).await.1, "custom");
    assert_eq!(balance(&wallet).await, 0);

    assert_eq!(at_wallet(&wallet, args(bytes, payment)).await, Ok(true));
    let restored = row(&wallet, &txid, 0).await;
    assert_eq!(restored.5, Some(sender.to_hex()), "the stored sender");
    assert_eq!(balance(&wallet).await, 4500);

    let tx = spend(&wallet, false)
        .await
        .expect("the restored coin signs");
    spends_the_coin(&wallet, &tx, &txid, 0).await;
}

/// The actions recorded with the spend's description, with their status.
async fn spend_actions(wallet: &TestWallet) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT status FROM transactions WHERE description = ? ORDER BY transaction_id",
    )
    .bind(SPEND)
    .fetch_all(wallet.storage().writer())
    .await
    .unwrap()
}

/// A received payment's change row (derived for another counterparty) whose
/// stored sender identity key is an empty string: read as the wallet's own,
/// its key does not match the coin's lock. A spend that selects it is
/// refused at signing, nothing broadcast, the coin left as a failed
/// signing leaves any coin.
async fn received_with_an_empty_sender(deferred: bool) {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let derived = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&derived)).unwrap();
    let (bytes, txid) = common::beef_paying(lock);
    let wallet = wallet(&root, &txid).await;
    let payment = common::payment(&sender, PREFIX, SUFFIX);
    assert_eq!(at_wallet(&wallet, args(bytes, payment)).await, Ok(true));
    sqlx::query("UPDATE outputs SET sender_identity_key = '' WHERE txid = ? AND vout = 0")
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    let held = coin(&wallet, &txid, 0).await;
    let posted = wallet.services().call_count("post_beef");
    let answer = spend(&wallet, deferred).await;
    let after = coin(&wallet, &txid, 0).await;
    let actions = spend_actions(&wallet).await;
    println!(
        "received coin, empty sender, {}: {:?}; coin (spendable, spent_by, satoshis) before \
         {held:?}, after {after:?}; spend actions {actions:?}; posts {posted} -> {}",
        if deferred {
            "signAction"
        } else {
            "createAction"
        },
        answer.as_ref().map(|tx| tx.id()),
        wallet.services().call_count("post_beef"),
    );
    let refused = answer.expect_err("a coin whose key does not match its lock is not signed");
    let step = if deferred {
        "signAction: "
    } else {
        "createAction: "
    };
    assert!(refused.starts_with(step), "{refused}");
    assert!(
        refused.contains("does not match locking script hash160")
            && refused.ends_with("Transaction aborted and UTXOs released."),
        "{refused}"
    );
    // Through createAction nothing is recorded; a deferred action is failed.
    let recorded: &[&str] = if deferred { &["failed"] } else { &[] };
    assert_eq!(actions, recorded);
    assert_eq!(after, held, "the coin left spendable, no spender");
    assert_eq!(
        wallet.services().call_count("post_beef"),
        posted,
        "nothing broadcast"
    );
}

#[tokio::test]
async fn a_received_coin_with_an_empty_sender_is_refused_by_create_action() {
    received_with_an_empty_sender(false).await;
}

#[tokio::test]
async fn a_received_coin_with_an_empty_sender_is_refused_by_sign_action() {
    received_with_an_empty_sender(true).await;
}
