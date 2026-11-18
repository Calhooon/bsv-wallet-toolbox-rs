//! A txid named in `sendWith` through the wallet: it is sent only when the
//! caller's own transaction of that txid reads `nosend`. For any other the
//! call writes nothing and answers, per txid, what the caller's row reads:
//! an action the wallet ended is not sent (`failed`), one already on its
//! way is answered from its stored word, and a txid the caller holds no row
//! of is answered as an unknown txid is. One in-memory store, a mock
//! broadcaster that accepts; nothing touches the network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/sendwith_seed.rs"]
mod seed;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::{
    AbortActionArgs, CreateActionArgs, CreateActionOptions, CreateActionOutput, WalletInterface,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{MonitorStorage, StorageSqlx, Wallet, WalletStorageWriter};

use common::other_lock;

const RULE: &str = "underReceivedRule";
const SIK: &str = "1111111111111111111111111111111111111111111111111111111111111111";

type W = Wallet<StorageSqlx, MockWalletServices>;

async fn store() -> StorageSqlx {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("send-with", SIK).await.unwrap();
    store.make_available().await.unwrap();
    store
}

/// A wallet of `root` on `store`, posting accepted, funded by three proven
/// coins of its own of 9500 and up, its change basket wanting one coin.
async fn funded_on(root: &PrivateKey, store: &StorageSqlx) -> W {
    let services = MockWalletServices::builder().post_beef_success().build();
    let wallet = Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap();
    let identity = root.public_key().to_hex();
    let (user, _) = store.find_or_insert_user(&identity).await.unwrap();
    seed::seed_coins(store, root, user.user_id, 3, 9500).await;
    ::sqlx::query(
        "UPDATE output_baskets SET number_of_desired_utxos = 1, minimum_desired_utxo_value = 1000",
    )
    .execute(store.writer())
    .await
    .unwrap();
    wallet
}

fn pay(satoshis: u64, delayed: bool, no_send: bool, send_with: &[String]) -> CreateActionArgs {
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
            no_send: Some(no_send),
            send_with: (!send_with.is_empty()).then(|| {
                send_with
                    .iter()
                    .map(|t| hex::decode(t).unwrap().try_into().unwrap())
                    .collect()
            }),
            ..Default::default()
        }),
    }
}

/// An action created: its txid, the outpoints it spends, and its
/// `sendWithResults` as `txid word` lines.
struct Made {
    txid: String,
    spends: Vec<String>,
    results: Vec<(String, String)>,
}

async fn create(wallet: &W, args: CreateActionArgs) -> Made {
    let created = wallet
        .create_action(args, Some("test.local"))
        .await
        .expect("the action");
    let tx = Transaction::from_binary(&created.tx.expect("the signed transaction")).unwrap();
    Made {
        txid: tx.id(),
        spends: tx
            .inputs()
            .iter()
            .map(|i| format!("{}.{}", i.get_source_txid().unwrap(), i.source_output_index))
            .collect(),
        results: created
            .send_with_results
            .unwrap_or_default()
            .into_iter()
            .map(|r| (hex::encode(r.txid), format!("{:?}", r.status)))
            .collect(),
    }
}

fn answer_for(made: &Made, txid: &str) -> Option<String> {
    made.results
        .iter()
        .find(|(t, _)| t == txid)
        .map(|(_, w)| w.clone())
}

/// Every row of `txid` the store holds, stamps included: the transaction
/// rows, the request, and the outputs.
async fn rows_of(store: &StorageSqlx, txid: &str) -> String {
    let tx: Vec<(i64, String, String)> = ::sqlx::query_as(
        "SELECT user_id, status, CAST(updated_at AS TEXT) FROM transactions WHERE txid = ? \
         ORDER BY transaction_id",
    )
    .bind(txid)
    .fetch_all(store.pool())
    .await
    .unwrap();
    let req: Vec<(String, Option<String>, String)> = ::sqlx::query_as(
        "SELECT status, batch, CAST(updated_at AS TEXT) FROM proven_tx_reqs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_all(store.pool())
    .await
    .unwrap();
    let outs: Vec<(i64, i64, Option<i64>, String)> = ::sqlx::query_as(
        "SELECT vout, spendable, spent_by, CAST(updated_at AS TEXT) FROM outputs WHERE txid = ? \
         ORDER BY output_id",
    )
    .bind(txid)
    .fetch_all(store.pool())
    .await
    .unwrap();
    format!("tx={tx:?} req={req:?} outputs={outs:?}")
}

async fn statuses(store: &StorageSqlx, txid: &str) -> (Vec<String>, Vec<String>) {
    let tx = ::sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_all(store.pool())
        .await
        .unwrap();
    let req = ::sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_all(store.pool())
        .await
        .unwrap();
    (tx, req)
}

/// The txids each `post_beef` the mock was asked named.
fn posts(wallet: &W) -> Vec<String> {
    wallet
        .services()
        .call_history()
        .into_iter()
        .filter(|c| c.method == "post_beef")
        .map(|c| c.args.join(" "))
        .collect()
}

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
        .expect("send-waiting");
}

async fn abort(wallet: &W, store: &StorageSqlx, txid: &str) {
    let reference: String =
        ::sqlx::query_scalar("SELECT reference FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let aborted = wallet
        .abort_action(AbortActionArgs { reference }, Some("test.local"))
        .await
        .map(|r| r.aborted);
    assert_eq!(aborted.map_err(|e| e.to_string()), Ok(true));
}

/// The coin `outpoint` (`txid.vout`): its spender's txid, if any.
async fn spender_of(store: &StorageSqlx, outpoint: &str) -> Option<String> {
    let (txid, vout) = outpoint.split_once('.').unwrap();
    ::sqlx::query_scalar(
        "SELECT t.txid FROM outputs o JOIN transactions t ON t.transaction_id = o.spent_by \
         WHERE o.txid = ? AND o.vout = ?",
    )
    .bind(txid)
    .bind(vout.parse::<i64>().unwrap())
    .fetch_optional(store.pool())
    .await
    .unwrap()
    .flatten()
}

/// A `noSend` action X its owner aborted, then named in a later action's
/// `sendWith`. `old_rows`: X's request put back to `nosend`, as an earlier
/// release's abort left it.
async fn aborted_then_named(old_rows: bool) {
    let root = PrivateKey::random();
    let store = store().await;
    let wallet = funded_on(&root, &store).await;
    let x = create(&wallet, pay(1_000, false, true, &[])).await;
    abort(&wallet, &store, &x.txid).await;
    if old_rows {
        ::sqlx::query("UPDATE proven_tx_reqs SET status = 'nosend' WHERE txid = ?")
            .bind(&x.txid)
            .execute(store.writer())
            .await
            .unwrap();
    }
    let after_abort = statuses(&store, &x.txid).await;
    let before = rows_of(&store, &x.txid).await;
    let later = create(
        &wallet,
        pay(1_000, false, false, std::slice::from_ref(&x.txid)),
    )
    .await;
    let after_send = rows_of(&store, &x.txid).await;
    send_waiting(&store).await;
    let still_later = create(&wallet, pay(9_000, true, false, &[])).await;
    let released = &x.spends[0];
    let taken_by = spender_of(&store, released).await;
    let posted = posts(&wallet);
    println!(
        "STEP aborted noSend named in sendWith old_rows={old_rows}\n  after abort: {after_abort:?}\n  \
         later action {}: results {:?}\n  X before {before}\n  X after  {after_send}\n  \
         posts {posted:?}\n  coin {released} the abort released: spent by {taken_by:?} \
         (later {}, still later {})",
        later.txid, later.results, later.txid, still_later.txid
    );
    assert_eq!(after_abort.0, vec!["failed".to_string()]);
    let ended = if old_rows { "nosend" } else { "invalid" };
    assert_eq!(after_abort.1, vec![ended.to_string()], "the request ended");
    assert_eq!(answer_for(&later, &x.txid).as_deref(), Some("Failed"));
    assert_eq!(after_send, before, "nothing of X written");
    assert_eq!(statuses(&store, &x.txid).await, after_abort);
    assert!(!posted.iter().any(|p| p.contains(&x.txid)), "{posted:?}");
    assert!(
        posted.iter().any(|p| p.contains(&later.txid)),
        "the later action is sent: {posted:?}"
    );
    assert!(
        taken_by == Some(later.txid.clone()) || taken_by == Some(still_later.txid.clone()),
        "{taken_by:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_no_send_action_named_in_a_later_send_with_is_not_sent() {
    aborted_then_named(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_no_send_action_whose_request_still_reads_nosend_is_not_sent() {
    aborted_then_named(true).await;
}

/// A `noSend` action sent by an earlier `sendWith` and named again: it is
/// answered from its stored word (`sending` while it waits, `unproven` once
/// posted) and nothing of it is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_txid_sent_by_an_earlier_call_named_again_is_answered_from_its_word() {
    let root = PrivateKey::random();
    let store = store().await;
    let wallet = funded_on(&root, &store).await;
    let a = create(&wallet, pay(1_000, false, true, &[])).await;
    let earlier = create(
        &wallet,
        pay(500, true, false, std::slice::from_ref(&a.txid)),
    )
    .await;
    assert_eq!(answer_for(&earlier, &a.txid).as_deref(), Some("Sending"));
    let waiting = rows_of(&store, &a.txid).await;
    let again = create(
        &wallet,
        pay(500, true, false, std::slice::from_ref(&a.txid)),
    )
    .await;
    let after_again = rows_of(&store, &a.txid).await;
    send_waiting(&store).await;
    let posted = rows_of(&store, &a.txid).await;
    let third = create(
        &wallet,
        pay(500, false, false, std::slice::from_ref(&a.txid)),
    )
    .await;
    let after_third = rows_of(&store, &a.txid).await;
    println!(
        "STEP named again\n  waiting {waiting}\n  again {:?}: {after_again}\n  \
         posted {posted}\n  third {:?}: {after_third}",
        again.results, third.results
    );
    assert_eq!(answer_for(&again, &a.txid).as_deref(), Some("Sending"));
    assert_eq!(after_again, waiting);
    assert_eq!(
        statuses(&store, &a.txid).await.0,
        vec!["unproven".to_string()]
    );
    assert_eq!(answer_for(&third, &a.txid).as_deref(), Some("Unproven"));
    // The third action may take A's change; A's own rows are not written.
    let own = |r: &str| r.split(" outputs=").next().unwrap().to_string();
    assert_eq!(own(&after_third), own(&posted));
}

/// Two users on one store. A txid the caller holds no row of is answered
/// as an unknown txid is, whether or not the store holds a row of it, and
/// nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_txid_the_caller_holds_no_row_of_is_answered_as_unknown() {
    let store = store().await;
    let one = funded_on(&PrivateKey::random(), &store).await;
    let two = funded_on(&PrivateKey::random(), &store).await;
    let held = create(&one, pay(1_000, false, true, &[])).await;
    let unknown = "ab".repeat(32);
    let before = rows_of(&store, &held.txid).await;
    let named_held = create(
        &two,
        pay(500, false, false, std::slice::from_ref(&held.txid)),
    )
    .await;
    let named_unknown = create(&two, pay(500, false, false, std::slice::from_ref(&unknown))).await;
    let after = rows_of(&store, &held.txid).await;
    send_waiting(&store).await;
    let after_wait = statuses(&store, &held.txid).await;
    let posted = [posts(&one), posts(&two)].concat();
    println!(
        "STEP no row of the caller's\n  held {:?}\n  unknown {:?}\n  before {before}\n  \
         after  {after}\n  posts {posted:?}",
        named_held.results, named_unknown.results
    );
    let a = answer_for(&named_held, &held.txid);
    let b = answer_for(&named_unknown, &unknown);
    assert_eq!(a.as_deref(), Some("Failed"));
    assert_eq!(a, b, "the same answer");
    assert_eq!(after, before, "nothing written");
    assert_eq!(
        after_wait,
        (vec!["nosend".to_string()], vec!["nosend".to_string()])
    );
    assert!(!posted.iter().any(|p| p.contains(&held.txid)), "{posted:?}");
    assert!(posted.iter().any(|p| p.contains(&named_held.txid)));
}

fn _unused(_: &dyn MonitorStorage, _: &dyn WalletStorageWriter) {}
