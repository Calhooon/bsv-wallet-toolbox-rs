//! A `noSend` action's change withheld at its send for an unread row, then
//! tried again by the unfail task: the record ends only on proof (the
//! output reopened, a spender found, a proven spend, its transaction
//! failed). A provider's spent hint, which a transaction the explorers have
//! not indexed yet answers, renews it, and no purge deletes it. One
//! in-memory store, mock services; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::storage::PurgeParams;
use bsv_wallet_toolbox_rs::{
    AuthId, MonitorStorage, StorageSqlx, Wallet, WalletStorageProvider, WalletStorageWriter,
};

use common::{other_lock, paying_derived, payment};

const RULE: &str = "underReceivedRule";
const SIK: &str = "1111111111111111111111111111111111111111111111111111111111111111";

type W = Wallet<StorageSqlx, MockWalletServices>;

/// A wallet of `root` on a fresh store, posting accepted, funded by two
/// received coins of 9500, its change basket wanting one coin.
async fn funded(root: &PrivateKey) -> (W, StorageSqlx) {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("nosend-withheld", SIK).await.unwrap();
    store.make_available().await.unwrap();
    let services = MockWalletServices::builder().post_beef_success().build();
    let wallet = Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap();
    for (prefix, suffix) in [
        ("cHJlZml4QQ==", "c3VmZml4QQ=="),
        ("cHJlZml4Qg==", "c3VmZml4Qg=="),
    ] {
        let sender = PrivateKey::random().public_key();
        let (tx, _) = paying_derived(root, &sender, prefix, suffix);
        wallet
            .internalize_action(
                InternalizeActionArgs {
                    tx,
                    outputs: vec![payment(0, prefix, suffix, &sender.to_hex())],
                    description: "a payment to the wallet".to_string(),
                    labels: None,
                    seek_permission: None,
                },
                "test.local",
            )
            .await
            .expect("the payment");
    }
    ::sqlx::query(
        "UPDATE output_baskets SET number_of_desired_utxos = 1, minimum_desired_utxo_value = 1000",
    )
    .execute(store.writer())
    .await
    .unwrap();
    (wallet, store)
}

/// An action paying `satoshis` to another key, funded by the selection.
fn pay(satoshis: u64, delayed: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a payment from the wallet".to_string(),
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
        labels: Some(vec![RULE.to_string()]),
        options: Some(CreateActionOptions {
            accept_delayed_broadcast: Some(delayed),
            randomize_outputs: Some(false),
            ..Default::default()
        }),
    }
}

fn pay_no_send(satoshis: u64) -> CreateActionArgs {
    let mut args = pay(satoshis, false);
    args.options.as_mut().unwrap().no_send = Some(true);
    args
}

/// `args` created: its txid and the outpoints it spends, or the refusal.
async fn create(wallet: &W, args: CreateActionArgs) -> Result<(String, Vec<String>), String> {
    let created = wallet
        .create_action(args, "test.local")
        .await
        .map_err(|e| e.to_string())?;
    let tx = Transaction::from_binary(&created.tx.expect("the signed transaction")).unwrap();
    let spends = tx
        .inputs
        .iter()
        .map(|i| format!("{}.{}", i.get_source_txid().unwrap(), i.source_output_index))
        .collect();
    Ok((tx.id(), spends))
}

/// The change row of `txid`: `sats spendable spent_by status`.
async fn change_of(store: &StorageSqlx, txid: &str) -> String {
    let (vout, sats, spendable, spent_by, status) =
        ::sqlx::query_as::<_, (i64, i64, i64, Option<i64>, String)>(
            "SELECT o.vout, o.satoshis, o.spendable, o.spent_by, t.status FROM outputs o \
             JOIN transactions t ON t.transaction_id = o.transaction_id \
             WHERE o.change = 1 AND t.txid = ?",
        )
        .bind(txid)
        .fetch_one(store.pool())
        .await
        .unwrap();
    format!("vout={vout} {sats} spendable={spendable} spent_by={spent_by:?} {status}")
}

/// The records of outputs withheld for an unread row.
async fn withheld(s: &StorageSqlx) -> Vec<String> {
    ::sqlx::query_scalar("SELECT details FROM monitor_events WHERE event = 'outputWithheld'")
        .fetch_all(s.pool())
        .await
        .unwrap()
}

fn hex_txid(hex: &str) -> [u8; 32] {
    hex::decode(hex).unwrap().try_into().unwrap()
}

/// The send-waiting task posts what `sendWith` named, its re-ask pause
/// passed (the requests' stamps an hour back).
async fn send_waiting(store: &StorageSqlx) {
    ::sqlx::query(
        "UPDATE proven_tx_reqs SET created_at = datetime('now', '-1 hour'), \
         updated_at = datetime('now', '-1 hour')",
    )
    .execute(store.writer())
    .await
    .unwrap();
    store
        .send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .expect("the send-waiting task");
}

/// One pass of the unfail task, the provider answering `unspent` for every
/// outpoint (`false` is the explorers' hint: not in the unspent set,
/// unproven).
async fn unfail_pass(store: &StorageSqlx, unspent: bool) {
    let services = MockWalletServices::builder()
        .is_utxo_response(MockResponse::Success(unspent))
        .post_beef_success()
        .build();
    WalletStorageProvider::set_services(store, Arc::new(services));
    store.un_fail().await.expect("the unfail task");
}

/// A row that could hide a spender and has no bytes: a bare `unproven`
/// transaction with no bytes and no `input_beef` (as a sync brings one).
async fn hold_unread_row(store: &StorageSqlx) -> i64 {
    let user_id: i64 = ::sqlx::query_scalar("SELECT user_id FROM users LIMIT 1")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let now = chrono::Utc::now();
    ::sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
         version, lock_time, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, 'unproven', 'unread-row', 'an unread row', 0, 1, 0, 1, ?, ?)",
    )
    .bind(user_id)
    .bind("ab".repeat(32))
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

/// A `noSend` change sent while an unread row is held is withheld and
/// recorded; the row ends; the retry is answered a hint (renewed, the
/// record kept through both purges), then unspent at a later pass: the
/// change is reopened and an action takes it, accepting delayed broadcast
/// or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withheld_no_send_change_answered_a_hint_is_tried_again_and_taken_delayed() {
    withheld_change_hint_then_unspent(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withheld_no_send_change_answered_a_hint_is_tried_again_and_taken_at_once() {
    withheld_change_hint_then_unspent(false).await;
}

async fn withheld_change_hint_then_unspent(delayed: bool) {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let (one, _) = create(&wallet, pay_no_send(1_000)).await.unwrap();
    let unread = hold_unread_row(&store).await;
    let mut send = pay(9_000, false);
    send.options.as_mut().unwrap().send_with = Some(vec![hex_txid(&one)]);
    create(&wallet, send).await.expect("the sendWith action");
    send_waiting(&store).await;
    let after_send = change_of(&store, &one).await;
    let records_at_send = withheld(&store).await;
    let held = create(&wallet, pay(8_000, delayed)).await;

    // The unread row ends.
    ::sqlx::query("UPDATE transactions SET status = 'failed' WHERE transaction_id = ?")
        .bind(unread)
        .execute(store.writer())
        .await
        .unwrap();
    unfail_pass(&store, false).await;
    let after_hint = change_of(&store, &one).await;
    let records_after_hint = withheld(&store).await;

    // Neither purge reaches the record, whatever its age.
    ::sqlx::query("UPDATE monitor_events SET created_at = datetime('now', '-400 days')")
        .execute(store.writer())
        .await
        .unwrap();
    let (user_id, identity): (i64, String) =
        ::sqlx::query_as("SELECT user_id, identity_key FROM users LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    WalletStorageWriter::purge_data(
        &store,
        &AuthId::with_user_id(identity, user_id),
        PurgeParams {
            max_age_days: 0,
            purge_completed: false,
            purge_failed: false,
        },
    )
    .await
    .expect("the purge");
    let cleaned = store
        .cleanup_monitor_events(std::time::Duration::ZERO)
        .await
        .unwrap();
    let records_after_purge = withheld(&store).await;

    unfail_pass(&store, true).await;
    let after_unspent = change_of(&store, &one).await;
    let records_after_unspent = withheld(&store).await;
    let taken = create(&wallet, pay(8_000, delayed)).await;
    println!(
        "STEP delayed={delayed}\n  after send: {after_send} records={records_at_send:?}\n  \
         action of 8000 while the row is held: {held:?}\n  retry answered a hint: \
         {after_hint} records={records_after_hint:?}\n  after both purges (cleanup \
         deleted {cleaned}): records={records_after_purge:?}\n  retry answered \
         unspent: {after_unspent} records={records_after_unspent:?}\n  action of \
         8000: {taken:?}"
    );
    assert!(
        after_send.ends_with("spendable=0 spent_by=None unproven"),
        "{after_send}"
    );
    assert_eq!(records_at_send.len(), 1, "{records_at_send:?}");
    assert!(held.unwrap_err().contains("Insufficient funds"));
    assert!(
        after_hint.ends_with("spendable=0 spent_by=None unproven"),
        "{after_hint}"
    );
    assert_eq!(records_after_hint, records_at_send);
    assert_eq!(records_after_purge, records_at_send);
    assert!(
        after_unspent.ends_with("spendable=1 spent_by=None unproven"),
        "{after_unspent}"
    );
    assert!(
        records_after_unspent.is_empty(),
        "{records_after_unspent:?}"
    );
    let (_, spends) = taken.expect("the action of 8000");
    assert!(spends.contains(&format!("{one}.1")), "{spends:?}");
}

/// A `noSend` action that also puts an output of its own in a basket, then
/// a later action that sends it through `sendWith`: the basket output is
/// listed from the send on (not before), and a later action names it as an
/// input. The output it paid to another key keeps its row as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sent_no_send_actions_basket_output_is_listed_and_spent() {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let mut args = pay_no_send(1_000);
    args.outputs.as_mut().unwrap().push(CreateActionOutput {
        locking_script: common::anyone_lock().to_binary(),
        satoshis: 600,
        output_description: "kept by the wallet".to_string(),
        basket: Some("kept".to_string()),
        custom_instructions: None,
        tags: None,
    });
    let (one, _) = create(&wallet, args).await.unwrap();
    let before = kept(&wallet).await;
    let mut send = pay(1_000, false);
    send.options.as_mut().unwrap().send_with = Some(vec![hex_txid(&one)]);
    create(&wallet, send).await.expect("the sendWith action");
    let after = kept(&wallet).await;
    let paid: (i64, Option<i64>) =
        ::sqlx::query_as("SELECT spendable, basket_id FROM outputs WHERE txid = ? AND vout = 0")
            .bind(&one)
            .fetch_one(store.pool())
            .await
            .unwrap();
    send_waiting(&store).await;
    let mut spend = pay(300, true);
    spend.inputs = Some(vec![bsv_rs::wallet::CreateActionInput {
        outpoint: bsv_rs::wallet::Outpoint::from_string(&format!("{one}.1")).unwrap(),
        input_description: "the kept output".to_string(),
        unlocking_script: Some(common::anyone_unlock().to_binary()),
        unlocking_script_length: None,
        sequence_number: Some(0xffff_ffff),
    }]);
    let named = create(&wallet, spend).await;
    println!(
        "STEP basket output\n  before the send: {before:?}\n  after: {after:?}\n  \
         the output paid away (spendable, basket): {paid:?}\n  a later action \
         naming {one}.1: {named:?}"
    );
    assert!(before.is_empty(), "{before:?}");
    assert_eq!(after, vec![format!("{one}.1")]);
    assert_eq!(paid, (0, None));
    let (_, spends) = named.expect("the action naming the kept output");
    assert!(spends.contains(&format!("{one}.1")), "{spends:?}");
}

/// The outpoints `listOutputs` shows in the basket `kept`.
async fn kept(wallet: &W) -> Vec<String> {
    wallet
        .list_outputs(
            bsv_rs::wallet::ListOutputsArgs {
                basket: "kept".to_string(),
                tags: None,
                tag_query_mode: None,
                include: None,
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: Some(100),
                offset: Some(0),
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .expect("the listing")
        .outputs
        .iter()
        .map(|o| o.outpoint.to_string())
        .collect()
}
