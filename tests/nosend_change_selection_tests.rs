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

/// A `noSend` action its owner aborted (its transaction `failed`, its
/// request left `nosend`), then named in a later action's `sendWith`: its
/// change stays not spendable, and an action needing 8000 does not take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_no_send_action_named_in_a_later_send_with_keeps_its_change_not_spendable() {
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
    let mut send = pay(1_000, true);
    send.options.as_mut().unwrap().send_with = Some(vec![hex_txid(&one)]);
    let sent = create(&wallet, send).await;
    let after_send = change_of(&store, &one).await;
    println!(
        "STEP abort then sendWith\n  abort: {aborted:?}\n  then: {after_abort}\n  \
         action naming it in sendWith: {sent:?}\n  then: {after_send}"
    );
    assert_eq!(aborted, Ok(true));
    sent.expect("the sendWith action");
    assert!(
        after_send.contains("spendable=0 spent_by=None"),
        "{after_send}"
    );
}

/// A pending unsigned action of the wallet's own (`signAndProcess` false,
/// `returnTXIDOnly`: no BEEF of its inputs is kept) while a `noSend`
/// action is sent through `sendWith`: the pending action named itself as
/// the spender of each coin it took, so it cannot spend the noSend
/// action's change, and an action needing that change takes it after the
/// send, posted at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_own_unsigned_action_with_no_input_beef_does_not_withhold_a_sent_change_at_once()
{
    pending_own_unsigned_action(false, false).await;
}

/// The same, the last action accepting delayed broadcast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_own_unsigned_action_with_no_input_beef_does_not_withhold_a_sent_change_delayed()
{
    pending_own_unsigned_action(true, false).await;
}

/// The same, the pending action made BEFORE the `noSend` action: it took a
/// coin of the store and could not have named the `noSend` action's change,
/// which did not exist yet, so the change is still not withheld.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_own_unsigned_action_made_before_the_no_send_action_does_not_withhold_its_change()
{
    pending_own_unsigned_action(true, true).await;
}

async fn pending_own_unsigned_action(delayed: bool, pending_first: bool) {
    let root = PrivateKey::random();
    let (wallet, store) = funded(&root).await;
    let sender = PrivateKey::random().public_key();
    let (tx, _) = paying_derived(&root, &sender, "cHJlZml4Qw==", "c3VmZml4Qw==");
    wallet
        .internalize_action(
            InternalizeActionArgs {
                tx,
                outputs: vec![payment(0, "cHJlZml4Qw==", "c3VmZml4Qw==", &sender.to_hex())],
                description: "a third payment to the wallet".to_string(),
                labels: None,
                seek_permission: None,
            },
            "test.local",
        )
        .await
        .expect("the third payment");
    let mut one = String::new();
    if !pending_first {
        one = create(&wallet, pay_no_send(1_000, None)).await.unwrap().0;
    }
    let mut unsigned = pay(500, true);
    {
        let options = unsigned.options.as_mut().unwrap();
        options.sign_and_process = Some(false);
        options.return_txid_only = Some(true);
    }
    let pending = wallet
        .create_action(unsigned, "test.local")
        .await
        .map(|c| c.signable_transaction.is_some())
        .map_err(|e| e.to_string());
    if pending_first {
        one = create(&wallet, pay_no_send(1_000, None)).await.unwrap().0;
    }
    let (beef_len, named): (i64, i64) = ::sqlx::query_as(
        "SELECT length(COALESCE(t.input_beef, X'')), \
         (SELECT COUNT(*) FROM outputs o WHERE o.spent_by = t.transaction_id) \
         FROM transactions t WHERE t.status = 'unsigned'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let mut send = pay(600, true);
    send.options.as_mut().unwrap().send_with = Some(vec![hex_txid(&one)]);
    let sent = create(&wallet, send).await.map(|r| r.0);
    let after_send = change_of(&store, &one).await;
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
        .expect("send-waiting");
    let after_wait = change_of(&store, &one).await;
    let withheld: i64 =
        ::sqlx::query_scalar("SELECT COUNT(*) FROM monitor_events WHERE event = 'outputWithheld'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    let last = create(&wallet, pay(16_000, delayed)).await;
    println!(
        "STEP pending unsigned delayed={delayed} pending_first={pending_first}\n  pending {pending:?}: input_beef {beef_len} \
         bytes, named as spender on {named} rows\n  sendWith {sent:?}: {after_send}\n  \
         send-waiting: {after_wait}, withheld records {withheld}\n  action of 16000: {last:?}"
    );
    assert_eq!(pending, Ok(true));
    assert_eq!(beef_len, 0, "returnTXIDOnly keeps no input BEEF");
    assert!(named > 0, "the pending action took a coin");
    sent.expect("the sendWith action");
    assert!(
        after_wait.ends_with("spendable=1 spent_by=None unproven"),
        "{after_wait}"
    );
    assert_eq!(withheld, 0);
    let (_, spends) = last.expect("the action of 16000");
    assert!(spends.contains(&format!("{one}.1")), "{spends:?}");
}
