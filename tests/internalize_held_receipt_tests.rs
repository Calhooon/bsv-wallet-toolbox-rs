//! A received payment with no proof that no broadcaster took (the owner's
//! ruling of 2026-11-17, DECISIONS 29.1, and the keeper's readings 4 and 5
//! of section 30): the door stores its transaction `sending` with its
//! request `unsent`; the balance and `listOutputs` leave its outputs out;
//! `listActions` lists the action `sending`; no rule offers the coin, the
//! `underReceivedRule` label included. One an instance took is `unproven`
//! and counted, as before. A store written before this change moves such a
//! receipt from `unproven` to `sending` on open, and leaves a taken one and
//! a coin the wallet's own action already spent as they are.
//!
//! The call's answer is unchanged by this change: `accepted: true` in every
//! case below.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, InternalizeActionArgs,
    ListActionsArgs, ListOutputsArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::monitor::tasks::{MonitorTask, SendWaitingTask};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    MonitorStorage, PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "aGVsZC1wcmVmaXg=";
const LABEL: &str = "heldreceipt";
const ORIGIN: &str = "test.local";
/// The one payment output the tests' BEEFs carry (`common::beef_paying`).
const PAID: u64 = 4500;

/// One instance's refusal of `txid` (a 465, a definitive word).
fn refused(name: &str, txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "465".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(format!("{name} answered 465")),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    }
}

/// One instance's acceptance of `txid`.
fn took(name: &str, txid: &str) -> PostBeefResult {
    let mut r = common::accepted(txid);
    r.name = name.to_string();
    r
}

/// The three outcomes of the door's post.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Every instance refuses.
    EveryRefuses,
    /// No instance answers (`post_beef` is an error).
    NoAnswer,
    /// One instance of three takes it.
    OneTakes,
}

fn post_words(outcome: Outcome, txid: &str) -> MockResponse<Vec<PostBeefResult>> {
    match outcome {
        Outcome::EveryRefuses => MockResponse::Success(vec![
            refused("arcade", txid),
            refused("bitails", txid),
            refused("woc", txid),
        ]),
        Outcome::NoAnswer => {
            MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
        }
        Outcome::OneTakes => MockResponse::Success(vec![
            refused("arcade", txid),
            took("bitails", txid),
            refused("woc", txid),
        ]),
    }
}

/// A wallet of `root` over in-memory SQLite whose broadcaster answers `post`.
async fn wallet_posting(root: &PrivateKey, post: MockResponse<Vec<PostBeefResult>>) -> TestWallet {
    let storage = StorageSqlx::in_memory().await.expect("in-memory storage");
    storage
        .migrate("held-receipt-tests", &root.public_key().to_hex())
        .await
        .expect("migrate");
    storage.make_available().await.expect("make_available");
    let services = MockWalletServices::builder()
        .post_beef_response(post)
        .build();
    Wallet::new(Some(root.clone()), storage, services)
        .await
        .expect("wallet")
}

/// An atomic BEEF paying `PAID` satoshis to `root`'s BRC-29 key derived from
/// `sender` with `suffix`, and the wallet payment's arguments for it.
fn paying(root: &PrivateKey, suffix: &str) -> (InternalizeActionArgs, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, suffix);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap();
    let (beef, txid) = common::beef_paying(lock);
    let mut args = common::args(beef, common::payment(&sender, PREFIX, suffix));
    args.description = "a received payment".to_string();
    args.labels = Some(vec![LABEL.to_string()]);
    (args, txid)
}

/// What the store and the wallet say of the received transaction `txid`.
#[derive(Debug, PartialEq)]
struct Seen {
    tx: String,
    req: String,
    broadcast_rows: Vec<(String, String)>,
    balance: u64,
    listed_outputs: u32,
    listed_action: String,
}

async fn seen(wallet: &TestWallet, txid: &str) -> Seen {
    let storage = wallet.storage();
    let tx: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let req: String = sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let broadcast_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT provider, status FROM broadcast_seen WHERE txid = ? ORDER BY provider",
    )
    .bind(txid)
    .fetch_all(storage.pool())
    .await
    .unwrap();
    let balance = wallet.balance().await.expect("balance").total;
    let outputs = wallet
        .list_outputs(
            ListOutputsArgs {
                basket: "default".to_string(),
                tags: None,
                tag_query_mode: None,
                include: None,
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
            Some(ORIGIN),
        )
        .await
        .expect("listOutputs");
    let actions = wallet
        .list_actions(
            ListActionsArgs {
                labels: vec![LABEL.to_string()],
                label_query_mode: None,
                include_labels: None,
                include_inputs: None,
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: None,
                include_outputs: None,
                include_output_locking_scripts: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
            Some(ORIGIN),
        )
        .await
        .expect("listActions");
    let listed_action = actions
        .actions
        .iter()
        .find(|a| hex::encode(a.txid) == txid)
        .map_or_else(|| "absent".to_string(), |a| format!("{:?}", a.status));
    Seen {
        tx,
        req,
        broadcast_rows,
        balance,
        listed_outputs: outputs.total_outputs,
        listed_action,
    }
}

/// A spend of 1000 satoshis, not sent, with or without the
/// `underReceivedRule` label.
fn spend(label: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a spend of a received coin".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: common::other_lock().to_binary(),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: label.then(|| vec!["underReceivedRule".to_string()]),
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(true),
            ..Default::default()
        }),
    }
}

/// `Ok` when an action took the coin, else the refusal's text.
async fn offered(wallet: &TestWallet, label: bool) -> Result<(), String> {
    wallet
        .create_action(spend(label), Some(ORIGIN))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The output id of `txid`'s output 0.
async fn coin_id(wallet: &TestWallet, txid: &str) -> i64 {
    sqlx::query_scalar("SELECT output_id FROM outputs WHERE txid = ? AND vout = 0")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

/// The must-spend of `txid`'s output 0 under the label: `Ok` when taken,
/// else the refusal's text.
async fn must_spend(wallet: &TestWallet, txid: &str) -> Result<(), String> {
    let id = coin_id(wallet, txid).await;
    wallet
        .create_action_offering(spend(true), Some(ORIGIN), &[id], &[id])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Internalize a payment whose post meets `outcome`; answers the wallet,
/// the txid and the call's answer.
async fn internalized(outcome: Outcome) -> (TestWallet, String, bool) {
    let root = PrivateKey::random();
    let (args, txid) = paying(&root, "c3VmZml4");
    let wallet = wallet_posting(&root, post_words(outcome, &txid)).await;
    let answer = wallet
        .internalize_action(args, Some(ORIGIN))
        .await
        .expect("the call's answer is unchanged here: accepted")
        .accepted;
    (wallet, txid, answer)
}

/// Section B of the lane, printed: each outcome's answer, words, counts and
/// offers. Asserts nothing the change moves; the witnesses below do.
#[tokio::test]
async fn held_receipt_observed_under_each_post_outcome() {
    for outcome in [Outcome::EveryRefuses, Outcome::NoAnswer, Outcome::OneTakes] {
        let (wallet, txid, answer) = internalized(outcome).await;
        let s = seen(&wallet, &txid).await;
        let posts = wallet.services().call_count("post_beef");
        let no_label = offered(&wallet, false).await;
        let label = offered(&wallet, true).await;
        // The must-spend on a wallet of its own, the coin not yet taken.
        let (other, other_txid, _) = internalized(outcome).await;
        let must = must_spend(&other, &other_txid).await;
        println!(
            "HELD {outcome:?}: accepted={answer} posts={posts} {s:?} \
             no_label={no_label:?} must_spend_under_label={must:?} under_label={label:?}"
        );
    }
}

/// RED at the base (the door wrote `unproven` and the balance counted it).
#[tokio::test]
async fn internalize_refused_by_every_instance_is_held_sending_and_not_in_the_balance() {
    let (wallet, txid, answer) = internalized(Outcome::EveryRefuses).await;
    assert!(answer, "the call's answer is unchanged here");
    let s = seen(&wallet, &txid).await;
    println!("every instance refused: {s:?}");
    assert_eq!((s.tx.as_str(), s.req.as_str()), ("sending", "unsent"));
    assert_eq!(s.broadcast_rows, vec![], "nothing taken, nothing recorded");
    assert_eq!((s.balance, s.listed_outputs), (0, 0), "not counted");
    assert_eq!(s.listed_action, "Sending", "listed as an action, sending");
    let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outputs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(kept, 1, "the output row is kept");
}

/// RED at the base (the label offered the coin and the must-spend took it).
#[tokio::test]
async fn internalize_with_no_answer_is_held_sending_and_not_offered_even_under_the_label() {
    let (wallet, txid, answer) = internalized(Outcome::NoAnswer).await;
    assert!(answer, "the call's answer is unchanged here");
    let s = seen(&wallet, &txid).await;
    println!("no instance answered: {s:?}");
    assert_eq!((s.tx.as_str(), s.req.as_str()), ("sending", "unsent"));
    assert_eq!((s.balance, s.listed_outputs), (0, 0));

    let no_label = offered(&wallet, false).await.unwrap_err();
    assert!(no_label.contains("Insufficient funds"), "{no_label}");
    let label = offered(&wallet, true).await.unwrap_err();
    assert!(label.contains("Insufficient funds"), "{label}");
    let named = must_spend(&wallet, &txid).await.unwrap_err();
    println!("the named must-spend: {named}");
    assert!(
        named.contains(&format!(
            "mustSpend[0]: output {txid}.0 is not spendable: its transaction is sending"
        )),
        "{named}"
    );
    let spent: Option<i64> =
        sqlx::query_scalar("SELECT spent_by FROM outputs WHERE txid = ? AND vout = 0")
            .bind(&txid)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(spent, None, "the held coin is untouched");
}

/// RED at the base only for the refusal of the coin without the label being
/// the same: here the taken receipt is `unproven`, counted, and offered
/// under the label alone, as before. Pins the door's success path moving
/// its own `sending` to `unproven`.
#[tokio::test]
async fn internalize_taken_by_one_instance_is_unproven_and_counted() {
    let (wallet, txid, answer) = internalized(Outcome::OneTakes).await;
    assert!(answer);
    let s = seen(&wallet, &txid).await;
    println!("one instance took it: {s:?}");
    assert_eq!((s.tx.as_str(), s.req.as_str()), ("unproven", "unmined"));
    assert_eq!((s.balance, s.listed_outputs), (PAID, 1), "counted");
    assert_eq!(s.listed_action, "Unproven");
    let no_label = offered(&wallet, false).await.unwrap_err();
    assert!(no_label.contains("Insufficient funds"), "{no_label}");
    offered(&wallet, true)
        .await
        .expect("offered under the action's label");
}

/// Backdate `txid`'s request so the send-waiting task finds it due.
async fn due(wallet: &TestWallet, txid: &str) {
    let past = chrono::Utc::now() - chrono::Duration::hours(3);
    sqlx::query("UPDATE proven_tx_reqs SET created_at = ?, updated_at = ? WHERE txid = ?")
        .bind(past)
        .bind(past)
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

/// The send-waiting task over the wallet's store.
fn send_waiting(wallet: &TestWallet) -> SendWaitingTask<StorageSqlx, MockWalletServices> {
    SendWaitingTask::new(
        Arc::new(wallet.storage().clone()),
        Arc::new(MockWalletServices::builder().build()),
    )
}

/// The ticks of `send_waiting` the count drives, each with the request due.
const TICKS: usize = 4;

/// The sending does not move: a held receipt of the every-refuses case
/// draws one post at the door and one per due send-waiting tick, the same
/// count at the base and at the tip; its words after the ticks.
#[tokio::test]
async fn held_receipt_draws_the_same_posts_from_the_send_waiting_task() {
    let (wallet, txid, _) = internalized(Outcome::EveryRefuses).await;
    let task = send_waiting(&wallet);
    for _ in 0..TICKS {
        due(&wallet, &txid).await;
        task.run().await.expect("send_waiting");
    }
    let posts = wallet.services().call_count("post_beef");
    let s = seen(&wallet, &txid).await;
    println!("posts after the door and {TICKS} due ticks: {posts}; {s:?}");
    assert_eq!(posts, 1 + TICKS, "one at the door, one per due tick");
    assert_eq!(s.req, "unsent", "still not taken");
}

/// A held receipt an instance takes later, through the send-waiting task's
/// success: `unproven`, counted, offered under the label. RED at the base
/// (the receipt was never `sending`).
#[tokio::test]
async fn held_receipt_taken_later_by_send_waiting_is_unproven_counted_and_offered() {
    let root = PrivateKey::random();
    let (args, txid) = paying(&root, "bGF0ZXI=");
    let post = MockResponse::Sequence(vec![
        post_words(Outcome::EveryRefuses, &txid),
        MockResponse::Success(vec![took("bitails", &txid)]),
    ]);
    let wallet = wallet_posting(&root, post).await;
    assert!(
        wallet
            .internalize_action(args, Some(ORIGIN))
            .await
            .unwrap()
            .accepted
    );
    let before = seen(&wallet, &txid).await;
    assert_eq!((before.tx.as_str(), before.balance), ("sending", 0));

    due(&wallet, &txid).await;
    send_waiting(&wallet).run().await.expect("send_waiting");
    let after = seen(&wallet, &txid).await;
    println!("taken later: {after:?}");
    assert_eq!(
        (after.tx.as_str(), after.req.as_str()),
        ("unproven", "unmined")
    );
    assert_eq!((after.balance, after.listed_outputs), (PAID, 1));
    offered(&wallet, true)
        .await
        .expect("offered under the label");
}

/// A held receipt the Arcade push reports seen: `unproven` and counted.
/// RED at the base (never `sending`).
#[tokio::test]
async fn held_receipt_seen_by_the_push_is_unproven_and_counted() {
    let (wallet, txid, _) = internalized(Outcome::NoAnswer).await;
    assert_eq!(seen(&wallet, &txid).await.tx, "sending");
    MonitorStorage::mark_transaction_seen_on_network_by(wallet.storage(), &txid, "arcade")
        .await
        .expect("the push's write");
    let s = seen(&wallet, &txid).await;
    assert_eq!((s.tx.as_str(), s.req.as_str()), ("unproven", "unmined"));
    assert_eq!(s.balance, PAID);
}

/// The wallet's own outgoing change under `sending` (a broadcast no
/// instance answered) is counted exactly as before. Green at the base and
/// the tip: the guard that the held clause reads `purpose = 'receive'`.
#[tokio::test]
async fn own_change_of_a_sending_action_is_still_counted() {
    let root = PrivateKey::random();
    let (args, txid) = paying(&root, "b3du");
    let post = MockResponse::Sequence(vec![
        MockResponse::Success(vec![took("bitails", &txid)]),
        MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into()),
    ]);
    let wallet = wallet_posting(&root, post).await;
    assert!(
        wallet
            .internalize_action(args, Some(ORIGIN))
            .await
            .unwrap()
            .accepted
    );
    assert_eq!(seen(&wallet, &txid).await.balance, PAID);

    let mut sent = spend(true);
    sent.options = Some(CreateActionOptions {
        accept_delayed_broadcast: Some(false),
        ..Default::default()
    });
    let made = wallet
        .create_action(sent, Some(ORIGIN))
        .await
        .expect("a broadcast no instance answered is not an error");
    let own = hex::encode(made.txid.expect("the txid"));
    let (status, change): (String, i64) = sqlx::query_as(
        "SELECT t.status, COALESCE(SUM(o.satoshis), 0) FROM transactions t \
         JOIN outputs o ON o.transaction_id = t.transaction_id \
         WHERE t.txid = ? AND o.purpose = 'change' AND o.spendable = 1",
    )
    .bind(&own)
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    println!("own action {status}, its change {change}");
    assert_eq!(status, "sending");
    assert!(change > 0);
    assert_eq!(
        wallet.balance().await.unwrap().total,
        u64::try_from(change).unwrap(),
        "the own change under sending is counted"
    );
}

/// Write `txid` as a store from 0.14.0-beta.8 holds it: `unproven`.
async fn as_beta8(wallet: &TestWallet, txid: &str) {
    sqlx::query("UPDATE transactions SET status = 'unproven' WHERE txid = ?")
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

async fn tx_status(wallet: &TestWallet, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

/// The spender of the coin `output_id` and that action's status.
async fn spender_and_word(wallet: &TestWallet, output_id: i64) -> (Option<i64>, String) {
    let storage = wallet.storage();
    let spender: Option<i64> =
        sqlx::query_scalar("SELECT spent_by FROM outputs WHERE output_id = ?")
            .bind(output_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let word: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(spender)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    (spender, word)
}

/// A store as 0.14.0-beta.8 wrote it: four received payments `unproven`
/// with no proof (held, spent, taken, recorded), the taken one's request
/// `unmined`, the recorded one `accepted` in `broadcast_seen`, and the
/// spent one's coin spent by the wallet's own action under the label, as
/// 0.14.0-beta.8 allowed. Answers the wallet, the four txids and the coin.
async fn beta8_store(root: &PrivateKey) -> (TestWallet, [String; 4], i64) {
    let paid = ["aGVsZA==", "c3BlbnQ=", "dGFrZW4=", "cmVjb3JkZWQ="].map(|s| paying(root, s));
    let wallet = wallet_posting(
        root,
        MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into()),
    )
    .await;
    let txids = paid.clone().map(|(_, txid)| txid);
    for (args, txid) in paid {
        let answer = wallet.internalize_action(args, Some(ORIGIN)).await;
        assert!(answer.unwrap().accepted);
        as_beta8(&wallet, &txid).await;
    }
    let storage = wallet.storage();
    sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined' WHERE txid = ?")
        .bind(&txids[2])
        .execute(storage.writer())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arcade', 'accepted')",
    )
    .bind(&txids[3])
    .execute(storage.writer())
    .await
    .unwrap();
    let id = coin_id(&wallet, &txids[1]).await;
    wallet
        .create_action_offering(spend(true), Some(ORIGIN), &[id], &[id])
        .await
        .expect("beta.8 offered it under the label");
    (wallet, txids, id)
}

/// Rows written by 0.14.0-beta.8 (`beta8_store`). On open the two held
/// ones move to `sending`; the taken ones are not touched; the spent coin
/// stays spent by the same action, which keeps its word; a second open
/// changes nothing; `migrate` applies the same statement. RED at the base
/// (nothing moves).
#[tokio::test]
async fn held_receipt_from_beta8_is_moved_to_sending_on_open_and_a_taken_one_is_not() {
    let root = PrivateKey::random();
    let (wallet, txids, id) = beta8_store(&root).await;
    let storage = wallet.storage();
    let (spender, child) = spender_and_word(&wallet, id).await;
    assert!(spender.is_some());

    for open in 0..2 {
        storage.make_available().await.expect("open");
        let mut words = Vec::new();
        for txid in &txids {
            words.push(tx_status(&wallet, txid).await);
        }
        assert_eq!(
            words,
            ["sending", "sending", "unproven", "unproven"],
            "open {open}: held, spent, taken, recorded"
        );
        assert_eq!(
            spender_and_word(&wallet, id).await,
            (spender, child.clone()),
            "a coin the wallet's own action spent stays spent; the action keeps its word"
        );
    }
    as_beta8(&wallet, &txids[0]).await;
    storage
        .migrate("held-receipt-tests", &root.public_key().to_hex())
        .await
        .expect("migrate");
    assert_eq!(tx_status(&wallet, &txids[0]).await, "sending");
    let own_change: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(satoshis), 0) FROM outputs \
         WHERE transaction_id = ? AND purpose = 'change' AND spendable = 1",
    )
    .bind(spender)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert!(own_change > 0);
    assert_eq!(
        wallet.balance().await.unwrap().total,
        2 * PAID + u64::try_from(own_change).unwrap(),
        "the taken two and the spending action's own change; the held two are out"
    );
}
