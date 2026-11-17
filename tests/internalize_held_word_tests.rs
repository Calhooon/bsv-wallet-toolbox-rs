//! The held word's reading (the owner's ruling of 2026-11-17, DECISIONS
//! 29.1, the keeper's reading 4 of section 30): what counts as taken, every
//! output a held transaction gives the wallet, the wallet's own `sending`
//! action, and the repeat call's answer as this piece leaves it.
//!
//! Taken is one fact, read by the door and by migration 006: the txid's
//! request is `unmined`, `callback`, `unconfirmed` or `completed`, or
//! `broadcast_seen` holds `accepted`, `seen` or `mined`. A received
//! transaction with no checked proof that was not taken is `sending`.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Beef;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, ListActionsArgs, ListOutputsArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::monitor::tasks::{MonitorTask, SendWaitingTask};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{PostBeefResult, StorageSqlx, Wallet, WalletStorageWriter};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "aGVsZC1wcmVmaXg=";
const LABEL: &str = "heldword";
const ORIGIN: &str = "test.local";
const BASKET: &str = "tokens";
/// The one output the tests' BEEFs carry (`common::beef_paying`).
const PAID: u64 = 4500;

fn no_answer() -> MockResponse<Vec<PostBeefResult>> {
    MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
}

/// A paying BEEF to `root`'s BRC-29 key from a new sender, its proven twin
/// (the same txid with a BUMP of its own), the wallet payment output and
/// the txid.
fn paying(root: &PrivateKey, suffix: &str) -> (Vec<u8>, Vec<u8>, InternalizeOutput, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, suffix);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap();
    let (beef, txid) = common::beef_paying(lock.clone());
    let (proven, same) = common::proven_beef_paying(lock);
    assert_eq!(txid, same);
    (beef, proven, common::payment(&sender, PREFIX, suffix), txid)
}

fn args(tx: Vec<u8>, output: InternalizeOutput) -> InternalizeActionArgs {
    let mut args = common::args(tx, output);
    args.description = "a received transaction".to_string();
    args.labels = Some(vec![LABEL.to_string()]);
    args
}

/// A basket insertion of output 0 into [`BASKET`].
fn insertion() -> InternalizeOutput {
    InternalizeOutput {
        output_index: 0,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: BASKET.to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }
}

async fn internalize(wallet: &TestWallet, args: InternalizeActionArgs) {
    let answer = wallet.internalize_action(args, Some(ORIGIN)).await;
    assert!(answer.expect("accepted").accepted);
}

async fn tx_status(wallet: &TestWallet, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap()
}

async fn sql(wallet: &TestWallet, statement: &str, txid: &str) {
    sqlx::query(statement)
        .bind(txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
}

/// The request another user's row on the same store holds for `txid`.
async fn foreign_request(wallet: &TestWallet, txid: &str, word: &str) {
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, \
         created_at, updated_at) VALUES (?, ?, 0, '{}', 0, '{}', X'01', ?, ?)",
    )
    .bind(txid)
    .bind(word)
    .bind(chrono::Utc::now())
    .bind(chrono::Utc::now())
    .execute(wallet.storage().writer())
    .await
    .unwrap();
}

async fn listed(wallet: &TestWallet, basket: &str) -> u32 {
    wallet
        .list_outputs(
            ListOutputsArgs {
                basket: basket.to_string(),
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
        .expect("listOutputs")
        .total_outputs
}

/// The listed action of `txid` under [`LABEL`]: its status and its
/// outputs' count.
async fn action(wallet: &TestWallet, txid: &str) -> (String, usize) {
    let actions = wallet
        .list_actions(
            ListActionsArgs {
                labels: vec![LABEL.to_string()],
                label_query_mode: None,
                include_labels: None,
                include_inputs: None,
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: None,
                include_outputs: Some(true),
                include_output_locking_scripts: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
            Some(ORIGIN),
        )
        .await
        .expect("listActions");
    let a = actions
        .actions
        .iter()
        .find(|a| hex::encode(a.txid) == txid)
        .expect("listed");
    (
        format!("{:?}", a.status),
        a.outputs.as_ref().map_or(0, Vec::len),
    )
}

/// `Ok` when an action under the `underReceivedRule` label took a coin.
async fn offered_under_label(wallet: &TestWallet) -> Result<(), String> {
    wallet
        .create_action(spend(None, true), Some(ORIGIN))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// A spend of 1000 satoshis to `lock` (another key's when `None`), not sent
/// when `no_send`, else posted at once.
fn spend(lock: Option<Vec<u8>>, no_send: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a spend".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: lock.unwrap_or_else(|| common::other_lock().to_binary()),
            satoshis: 1_000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["underReceivedRule".to_string(), LABEL.to_string()]),
        options: Some(if no_send {
            CreateActionOptions {
                no_send: Some(true),
                sign_and_process: Some(true),
                ..Default::default()
            }
        } else {
            CreateActionOptions {
                accept_delayed_broadcast: Some(false),
                ..Default::default()
            }
        }),
    }
}

/// A receipt whose txid already holds another user's request in `word`,
/// internalized with no proof: held. RED at the base for `nosend` and
/// `invalid` (the door read every word but `unsent`/`sending` as a take).
async fn receipt_beside_a_foreign_request(word: &str) {
    let root = PrivateKey::random();
    let (beef, _, output, txid) = paying(&root, "Zm9yZWlnbg==");
    let wallet = common::wallet_posting("held-word", &root, no_answer()).await;
    foreign_request(&wallet, &txid, word).await;
    internalize(&wallet, args(beef, output)).await;
    let balance = wallet.balance().await.unwrap().total;
    let label = offered_under_label(&wallet).await;
    println!(
        "beside a {word} request: tx {} balance {balance} listed {} label {label:?} posts {}",
        tx_status(&wallet, &txid).await,
        listed(&wallet, "default").await,
        wallet.services().call_count("post_beef"),
    );
    assert_eq!(tx_status(&wallet, &txid).await, "sending", "{word}");
    assert_eq!(
        (balance, listed(&wallet, "default").await),
        (0, 0),
        "{word}"
    );
    assert!(label.unwrap_err().contains("Insufficient funds"), "{word}");
    let req: String = sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(req, word, "the other user's request keeps its word");
}

#[tokio::test]
async fn receipt_beside_another_users_nosend_request_is_held() {
    receipt_beside_a_foreign_request("nosend").await;
}

#[tokio::test]
async fn receipt_beside_another_users_invalid_request_is_held() {
    receipt_beside_a_foreign_request("invalid").await;
}

/// The taken words still make the new row `unproven`: a foreign request
/// `unmined`, or no request and `broadcast_seen` `accepted`.
#[tokio::test]
async fn receipt_beside_a_taken_request_or_a_recorded_take_is_unproven() {
    let root = PrivateKey::random();
    let (beef, _, output, txid) = paying(&root, "dGFrZW4=");
    let wallet = common::wallet_posting("held-word", &root, no_answer()).await;
    foreign_request(&wallet, &txid, "unmined").await;
    internalize(&wallet, args(beef, output)).await;
    assert_eq!(tx_status(&wallet, &txid).await, "unproven");
    assert_eq!(wallet.balance().await.unwrap().total, PAID);

    let (beef, _, output, txid) = paying(&root, "c2Vlbg==");
    sql(
        &wallet,
        "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arcade', 'accepted')",
        &txid,
    )
    .await;
    internalize(&wallet, args(beef, output)).await;
    assert_eq!(tx_status(&wallet, &txid).await, "unproven");
}

/// Migration 006 reads the same fact: a row a store wrote `unproven` beside
/// another user's `nosend` request moves to `sending` on open. RED at the
/// base (the statement moved only `unsent`/`sending`).
#[tokio::test]
async fn migration_moves_a_receipt_beside_another_users_nosend_request() {
    let root = PrivateKey::random();
    let (beef, _, output, txid) = paying(&root, "bWlncmF0ZQ==");
    let wallet = common::wallet_posting("held-word", &root, no_answer()).await;
    internalize(&wallet, args(beef, output)).await;
    sql(
        &wallet,
        "UPDATE proven_tx_reqs SET status = 'nosend' WHERE txid = ?",
        &txid,
    )
    .await;
    sql(
        &wallet,
        "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
        &txid,
    )
    .await;
    assert_eq!(wallet.balance().await.unwrap().total, PAID);
    wallet.storage().make_available().await.expect("open");
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
    assert_eq!(wallet.balance().await.unwrap().total, 0);
}

/// A basket insertion internalized with no proof and every instance
/// refusing: out of its basket's listing; listActions lists the action
/// `sending` with its output; one instance takes it; listed.
#[tokio::test]
async fn held_basket_insertion_leaves_its_basket_until_an_instance_takes_it() {
    let root = PrivateKey::random();
    let (beef, _, _, txid) = paying(&root, "dG9rZW4=");
    let post = MockResponse::Sequence(vec![
        no_answer(),
        MockResponse::Success(vec![common::accepted(&txid)]),
    ]);
    let wallet = common::wallet_posting("held-word", &root, post).await;
    internalize(&wallet, args(beef, insertion())).await;
    let held = (
        tx_status(&wallet, &txid).await,
        listed(&wallet, BASKET).await,
        action(&wallet, &txid).await,
    );
    println!("held insertion: {held:?}");
    assert_eq!(held, ("sending".into(), 0, ("Sending".into(), 1)));

    let backdated = chrono::Utc::now() - chrono::Duration::hours(3);
    sqlx::query("UPDATE proven_tx_reqs SET created_at = ?, updated_at = ? WHERE txid = ?")
        .bind(backdated)
        .bind(backdated)
        .bind(&txid)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
    SendWaitingTask::new(
        Arc::new(wallet.storage().clone()),
        Arc::new(MockWalletServices::builder().build()),
    )
    .run()
    .await
    .expect("send_waiting");
    assert_eq!(tx_status(&wallet, &txid).await, "unproven");
    assert_eq!(listed(&wallet, BASKET).await, 1, "taken: listed");
}

/// The mirror for migration 006: a store that held a basket insertion
/// `unproven` with its request `unsent` moves it to `sending` on open, out
/// of its basket's listing, the action listed with its output, nothing
/// deleted.
#[tokio::test]
async fn migration_takes_a_held_basket_insertion_out_of_its_basket() {
    let root = PrivateKey::random();
    let (beef, _, _, txid) = paying(&root, "b3Blbg==");
    let wallet = common::wallet_posting("held-word", &root, no_answer()).await;
    internalize(&wallet, args(beef, insertion())).await;
    sql(
        &wallet,
        "UPDATE transactions SET status = 'unproven' WHERE txid = ?",
        &txid,
    )
    .await;
    assert_eq!(
        listed(&wallet, BASKET).await,
        1,
        "as 0.14.0-beta.8 lists it"
    );
    wallet.storage().make_available().await.expect("open");
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
    assert_eq!(listed(&wallet, BASKET).await, 0);
    assert_eq!(action(&wallet, &txid).await, ("Sending".into(), 1));
}

/// The wallet's own `sending` action (posted, no instance answered) with a
/// wallet payment merged into it: its change and the merged payment are
/// listed and counted, as 0.14.0-beta.8.1 counts them. RED at the base
/// (the merged payment, `purpose = 'receive'`, was left out).
#[tokio::test]
async fn own_sending_action_with_a_merged_payment_is_listed_and_counted() {
    let root = PrivateKey::random();
    let (beef, _, output, txid) = paying(&root, "ZnVuZA==");
    let post = MockResponse::Sequence(vec![
        MockResponse::Success(vec![common::accepted(&txid)]),
        no_answer(),
    ]);
    let wallet = common::wallet_posting("held-word", &root, post).await;
    internalize(&wallet, args(beef.clone(), output)).await;
    assert_eq!(wallet.balance().await.unwrap().total, PAID);

    let sender = PrivateKey::random().public_key();
    let suffix = "b3du";
    let own_key = common::derived_key(&root, &sender, PREFIX, suffix);
    let made = wallet
        .create_action(
            spend(Some(common::p2pkh_bytes(&own_key)), false),
            Some(ORIGIN),
        )
        .await
        .expect("a post no instance answered is not an error");
    let own = hex::encode(made.txid.expect("txid"));
    assert_eq!(tx_status(&wallet, &own).await, "sending");
    let (raw, paid_vout): (Vec<u8>, i64) = sqlx::query_as(
        "SELECT t.raw_tx, o.vout FROM transactions t JOIN outputs o \
         ON o.transaction_id = t.transaction_id WHERE t.txid = ? AND o.satoshis = 1000",
    )
    .bind(&own)
    .fetch_one(wallet.storage().pool())
    .await
    .unwrap();
    let change = wallet.balance().await.unwrap().total;
    assert!(change > 0);

    let mut atomic = Beef::from_binary(&beef).unwrap();
    atomic.merge_raw_tx(raw, None).unwrap();
    let mut payment = common::payment(&sender, PREFIX, suffix);
    payment.output_index = u32::try_from(paid_vout).unwrap();
    internalize(
        &wallet,
        args(atomic.to_binary_atomic(&own).unwrap(), payment),
    )
    .await;
    let after = (
        tx_status(&wallet, &own).await,
        wallet.balance().await.unwrap().total,
        listed(&wallet, "default").await,
    );
    println!("own sending action, change {change}, after the merge: {after:?}");
    assert_eq!(after, ("sending".into(), change + 1_000, 2));
}

/// THE REPEAT PIECE'S TO MOVE: a repeat `internalizeAction` of a held
/// receipt that now carries a checked BUMP merges and leaves the row
/// `sending`, no proof stored (the merge moves only a `nosend` target).
/// Pinned as this piece leaves it; a proof arrives here by the monitor's
/// proof path alone.
#[tokio::test]
async fn repeat_piece_pin_a_repeat_call_with_a_proof_leaves_the_held_receipt_sending() {
    let root = PrivateKey::random();
    let (beef, proven, output, txid) = paying(&root, "cmVwZWF0");
    let wallet = common::wallet_posting("held-word", &root, no_answer()).await;
    internalize(&wallet, args(beef, output.clone())).await;
    assert_eq!(tx_status(&wallet, &txid).await, "sending");
    let answer = wallet
        .internalize_action(args(proven, output), Some(ORIGIN))
        .await
        .expect("the repeat call's answer");
    let (status, proof): (String, Option<i64>) =
        sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    let balance = wallet.balance().await.unwrap().total;
    println!(
        "repeat with a proof: accepted {} status {status} proof {proof:?} balance {balance}",
        answer.accepted
    );
    assert!(answer.accepted);
    assert_eq!((status.as_str(), proof, balance), ("sending", None, 0));
}
