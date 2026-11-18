//! The change of a `noSend` action through the wallet: once a later action
//! sends that transaction through `sendWith`, its change is a coin an
//! action takes, accepting delayed broadcast or not. One in-memory store,
//! a mock broadcaster that accepts; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    AbortActionArgs, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, Outpoint, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, Wallet, WalletStorageWriter};

use common::{other_lock, paying_derived, payment};

const RULE: &str = "underReceivedRule";
const SIK: &str = "1111111111111111111111111111111111111111111111111111111111111111";

type W = Wallet<StorageSqlx, MockWalletServices>;

/// A wallet of `root` on a fresh store, posting accepted, funded by two
/// received coins of 9500, its change basket wanting one coin.
async fn funded(root: &PrivateKey) -> (W, StorageSqlx) {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("nosend-change", SIK).await.unwrap();
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
    // One desired change coin, so each action takes one coin and makes one
    // change output.
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

/// A `noSend` action paying `satoshis`, spending `no_send_change`.
fn pay_no_send(satoshis: u64, no_send_change: Option<Vec<Outpoint>>) -> CreateActionArgs {
    let mut args = pay(satoshis, false);
    let options = args.options.as_mut().unwrap();
    options.no_send = Some(true);
    options.no_send_change = no_send_change;
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

/// A `noSend` action, then an action posted at once that names it in
/// `sendWith`, the send-waiting task that posts it, then an action that
/// needs the noSend action's change: taken, accepting delayed broadcast or
/// not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sent_no_send_actions_change_is_taken_by_an_action_posted_at_once() {
    sent_no_send_chain(false).await;
}

/// The same chain, the last action accepting delayed broadcast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sent_no_send_actions_change_is_taken_by_a_delayed_action() {
    sent_no_send_chain(true).await;
}

async fn sent_no_send_chain(delayed: bool) {
    {
        let root = PrivateKey::random();
        let (wallet, store) = funded(&root).await;
        let (one, _) = create(&wallet, pay_no_send(1_000, None)).await.unwrap();
        let after_one = change_of(&store, &one).await;
        let mut send = pay(9_000, false);
        send.options.as_mut().unwrap().send_with = Some(vec![hex_txid(&one)]);
        let (two, _) = create(&wallet, send).await.unwrap();
        let after_two = change_of(&store, &one).await;
        // The send-waiting task posts the transaction `sendWith` named, once
        // its re-ask pause has passed (the requests' stamps an hour back).
        ::sqlx::query(
            "UPDATE proven_tx_reqs SET created_at = datetime('now', '-1 hour'), \
             updated_at = datetime('now', '-1 hour')",
        )
        .execute(store.writer())
        .await
        .unwrap();
        let waited = store
            .send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .map(|r| r.and_then(|r| r.send_with_results).map(|v| v.len()));
        let after_wait = change_of(&store, &one).await;
        let three = create(&wallet, pay(8_000, delayed)).await;
        let after_three = change_of(&store, &one).await;
        println!(
            "STEP delayed={delayed}\n  noSend action: {after_one}\n  \
             sendWith action {two}: {after_two}\n  send-waiting task {waited:?}: \
             {after_wait}\n  action of 8000: {three:?}\n  then: {after_three}"
        );
        assert!(
            after_one.ends_with("spendable=0 spent_by=None nosend"),
            "{after_one}"
        );
        assert!(
            after_two.ends_with("spendable=1 spent_by=None sending"),
            "{after_two}"
        );
        assert!(
            after_wait.ends_with("spendable=1 spent_by=None unproven"),
            "{after_wait}"
        );
        let (_, spends) = three.expect("the action of 8000");
        assert!(spends.contains(&format!("{one}.1")), "{spends:?}");
        assert!(
            after_three.contains("spendable=0 spent_by=Some("),
            "{after_three}"
        );
    }
}

/// A `noSend` action, then a `noSend` action naming its change in
/// `noSendChange` and needing that change's value: refused, and the
/// earlier change keeps its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_no_send_chain_through_no_send_change() {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let created = wallet
        .create_action(pay_no_send(1_000, None), "test.local")
        .await
        .unwrap();
    let one = Transaction::from_binary(&created.tx.unwrap()).unwrap().id();
    let change = created.no_send_change.expect("the noSend change");
    let after_one = change_of(&store, &one).await;
    let two = create(&wallet, pay_no_send(15_000, Some(change))).await;
    let after_two = change_of(&store, &one).await;
    println!(
        "STEP noSendChange\n  noSend action: {after_one}\n  \
         noSend action of 15000 naming its change: {two:?}\n  then: {after_two}"
    );
    assert!(
        after_one.ends_with("spendable=0 spent_by=None nosend"),
        "{after_one}"
    );
    assert!(two.unwrap_err().contains("Insufficient funds"));
    assert_eq!(after_two, after_one);
}

/// A `noSend` action aborted before it is sent: its change never becomes
/// spendable, and an action needing 8000 takes the coin the abort released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_no_send_actions_change_stays_not_spendable() {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let (one, _) = create(&wallet, pay_no_send(1_000, None)).await.unwrap();
    let reference: String =
        ::sqlx::query_scalar("SELECT reference FROM transactions WHERE txid = ?")
            .bind(&one)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let aborted = wallet
        .abort_action(AbortActionArgs { reference }, "test.local")
        .await
        .map(|r| r.aborted);
    let after_abort = change_of(&store, &one).await;
    let three = create(&wallet, pay(8_000, true)).await;
    let after_three = change_of(&store, &one).await;
    println!(
        "STEP abort\n  abort: {aborted:?}\n  then: {after_abort}\n  \
         delayed action of 8000: {three:?}\n  then: {after_three}"
    );
    assert_eq!(aborted, Ok(true));
    assert!(
        after_abort.contains("spendable=0 spent_by=None"),
        "{after_abort}"
    );
    let (_, spends) = three.expect("the action of 8000");
    assert!(!spends.contains(&format!("{one}.1")), "{spends:?}");
    assert_eq!(after_three, after_abort);
}

/// The txid `hex` as the SDK's `TxId` bytes.
fn hex_txid(hex: &str) -> [u8; 32] {
    hex::decode(hex).unwrap().try_into().unwrap()
}

/// A `noSend` action whose transaction the host broadcast itself and then
/// internalizes (its payment output into a basket): the transaction leaves
/// `nosend`, its change is a coin, and an action needing 8000 takes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_no_send_actions_change_is_a_coin_once_the_host_internalizes_it_sent() {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let created = wallet
        .create_action(pay_no_send(1_000, None), "test.local")
        .await
        .unwrap();
    let one = Transaction::from_binary(&created.tx.expect("the transaction"))
        .unwrap()
        .id();
    let mut whole =
        bsv_rs::transaction::Beef::from_binary(&created.beef.expect("the BEEF")).unwrap();
    let beef = whole.to_binary_atomic(&one).unwrap();
    let after_one = change_of(&store, &one).await;
    let taken = wallet
        .internalize_action(
            InternalizeActionArgs {
                tx: beef,
                outputs: vec![bsv_rs::wallet::InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(bsv_rs::wallet::BasketInsertion {
                        basket: "sent".to_string(),
                        custom_instructions: None,
                        tags: None,
                    }),
                }],
                description: "the noSend action, broadcast".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    let after_internalize = change_of(&store, &one).await;
    let three = create(&wallet, pay(8_000, false)).await;
    println!(
        "STEP internalize\n  noSend action: {after_one}\n  internalize {taken:?}: \
         {after_internalize}\n  action of 8000: {three:?}"
    );
    assert_eq!(taken, Ok(true));
    assert!(
        after_internalize.ends_with("spendable=1 spent_by=None unproven"),
        "{after_internalize}"
    );
    let (_, spends) = three.expect("the action of 8000");
    assert!(spends.contains(&format!("{one}.1")), "{spends:?}");
}
