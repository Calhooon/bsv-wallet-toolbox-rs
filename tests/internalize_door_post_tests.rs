//! The internalize door's post for each user's presentation (the owner's
//! ruling of 2026-11-17, DECISIONS 29.1, and the keeper's reading 11 of
//! section 30): a new received row of this user with no checked proof and
//! no recorded take is posted once to each named instance, whatever request
//! the store already holds for the txid. Another user's `nosend`, `unsent`,
//! `sending` or `unmined` request, or an `invalid` or `doubleSpend` word,
//! is no verdict for this user.
//!
//! What a take moves (one rule, `process_action`'s success arm's for the
//! rows): every transaction row of the txid that is neither `failed` nor
//! `completed` to `unproven`, the request to `unmined` from every word but
//! `completed`, the take recorded in the same write. What a post nobody
//! took leaves: this user's row `sending`, each instance's words on the
//! request's history; a request `unsent` or `sending` is left `unsent`
//! (the door's to send), any other word (`nosend`, `unmined`, `invalid`,
//! `doubleSpend`) keeps its status and attempts.
//!
//! Real SQLite storage shared by two users, a mock broadcaster with two
//! named instances and a mock tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::WalletInterface;
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "ZG9vci1wb3N0";
const ORIGIN: &str = "test.local";

/// What the named instances answer to the door's one post.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// One instance takes it, the other refuses.
    Taken,
    /// Every instance refuses, none naming a competitor.
    Refused,
    /// No instance answers (the post itself faults).
    NoAnswer,
}

fn refused(name: &str, txid: &str) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: "rejected".to_string(),
            double_spend: false,
            orphan_mempool: false,
            competing_txs: None,
            data: Some(format!("{name} says no")),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: Some("rejected".to_string()),
        notes: vec![],
    }
}

fn answer_to(outcome: Outcome, txid: &str) -> MockResponse<Vec<PostBeefResult>> {
    match outcome {
        Outcome::Taken => {
            let mut taken = common::accepted(txid);
            taken.name = "arcade".to_string();
            MockResponse::Success(vec![taken, refused("bitails", txid)])
        }
        Outcome::Refused => {
            MockResponse::Success(vec![refused("arcade", txid), refused("bitails", txid)])
        }
        Outcome::NoAnswer => {
            MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
        }
    }
}

/// The other user's request word and the row its act wrote beside it.
const CELLS: [(Option<&str>, &str); 7] = [
    (Some("nosend"), "nosend"),
    (Some("unsent"), "sending"),
    (Some("sending"), "sending"),
    (Some("unmined"), "unproven"),
    (Some("invalid"), "failed"),
    (Some("doubleSpend"), "sending"),
    (None, ""),
];

/// One cell's reading after the call.
#[derive(Debug, PartialEq, Eq)]
struct Reading {
    posts: usize,
    answer: String,
    mine: String,
    other: Option<String>,
    request: String,
    notes: usize,
    taken: bool,
}

async fn scalar(wallet: &TestWallet, sql: &str, txid: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(txid)
        .fetch_optional(wallet.storage().pool())
        .await
        .unwrap()
}

/// The call's answer as a host reads it: `accepted`, or the review-actions
/// refusal's review status and send status.
fn read_answer(answer: bsv_rs::Result<bsv_rs::wallet::InternalizeActionResult>) -> String {
    match answer {
        Ok(result) if result.accepted => "accepted".to_string(),
        Ok(_) => "not accepted".to_string(),
        Err(bsv_rs::Error::WalletReviewActions(refusal)) => format!(
            "review {:?} {:?}",
            refusal
                .review_action_results
                .iter()
                .map(|r| r.status)
                .collect::<Vec<_>>(),
            refusal
                .send_with_results
                .iter()
                .map(|r| r.status)
                .collect::<Vec<_>>()
        ),
        Err(other) => panic!("unexpected answer: {other:?}"),
    }
}

/// User B presents a payment whose txid the store already holds under user
/// A's `request` (and A's row), the door's post answered by `outcome`.
async fn cell(request: Option<&str>, other_row: &str, outcome: Outcome) -> Reading {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, "c2VsbA==");
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let wallet = common::wallet_posting("door-post", &root, answer_to(outcome, &txid)).await;
    let store = wallet.storage();
    if let Some(word) = request {
        let other = PrivateKey::random().public_key().to_hex();
        let (user, _) = store.find_or_insert_user(&other).await.unwrap();
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
             version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
             VALUES (?, ?, ?, 'other-ref', 'the other user', 0, 1, 0, X'01', 1, ?, ?)",
        )
        .bind(user.user_id)
        .bind(&txid)
        .bind(other_row)
        .bind(now)
        .bind(now)
        .execute(store.writer())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
             raw_tx, created_at, updated_at) VALUES (?, ?, 0, '{}', 0, '{}', X'01', ?, ?)",
        )
        .bind(&txid)
        .bind(word)
        .bind(now)
        .bind(now)
        .execute(store.writer())
        .await
        .unwrap();
    }
    let answer = wallet
        .internalize_action(
            common::args(beef, common::payment(&sender, PREFIX, "c2VsbA==")),
            Some(ORIGIN),
        )
        .await;
    let me = wallet
        .storage()
        .find_or_insert_user(&root.public_key().to_hex())
        .await;
    let me = me.unwrap().0.user_id;
    let mine: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND user_id = ?")
            .bind(&txid)
            .bind(me)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    let other =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND user_id <> ?")
            .bind(&txid)
            .bind(me)
            .fetch_optional(wallet.storage().pool())
            .await
            .unwrap();
    let history = scalar(
        &wallet,
        "SELECT history FROM proven_tx_reqs WHERE txid = ?",
        &txid,
    )
    .await
    .unwrap_or_default();
    Reading {
        posts: wallet.services().call_count("post_beef"),
        answer: read_answer(answer),
        mine,
        other,
        request: scalar(
            &wallet,
            "SELECT status FROM proven_tx_reqs WHERE txid = ?",
            &txid,
        )
        .await
        .unwrap_or_default(),
        notes: history.matches("immediateBroadcastHint").count(),
        taken: scalar(
            &wallet,
            "SELECT status FROM broadcast_seen WHERE txid = ? AND status = 'accepted'",
            &txid,
        )
        .await
        .is_some(),
    }
}

/// What a take moves of a row the other user's act wrote.
fn moved_by_a_take(row: &str) -> String {
    match row {
        "failed" | "completed" => row.to_string(),
        _ => "unproven".to_string(),
    }
}

/// What a post nobody took leaves of the request's word.
fn left_by_no_take(word: Option<&str>) -> String {
    match word {
        None | Some("unsent") | Some("sending") => "unsent".to_string(),
        Some(word) => word.to_string(),
    }
}

/// The answer the call gives for `outcome`: taken, `accepted`; taken by
/// nobody, the review-actions refusal with the txid `sending` and the
/// review status of reading 10.
fn answer_for(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Taken => "accepted",
        Outcome::Refused => "review [InvalidTx] [Sending]",
        Outcome::NoAnswer => "review [ServiceError] [Sending]",
    }
}

async fn matrix(outcome: Outcome) {
    let mut wrong = Vec::new();
    for (request, other_row) in CELLS {
        let got = cell(request, other_row, outcome).await;
        let taken = matches!(outcome, Outcome::Taken);
        let want = Reading {
            posts: 1,
            answer: answer_for(outcome).to_string(),
            mine: if taken { "unproven" } else { "sending" }.to_string(),
            other: request.map(|_| {
                if taken {
                    moved_by_a_take(other_row)
                } else {
                    other_row.to_string()
                }
            }),
            request: if taken {
                "unmined".to_string()
            } else {
                left_by_no_take(request)
            },
            notes: usize::from(!taken),
            taken,
        };
        println!(
            "MATRIX {outcome:?} request {:<11} other {:<8} -> posts {} answer {} mine {} other {:?} request {} notes {} take recorded {}",
            request.unwrap_or("none"),
            if other_row.is_empty() { "none" } else { other_row },
            got.posts,
            got.answer,
            got.mine,
            got.other,
            got.request,
            got.notes,
            got.taken
        );
        if got != want {
            wrong.push(format!("{request:?}: got {got:?}, want {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{outcome:?}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn door_posts_each_presentation_whatever_request_stands_an_instance_takes_it() {
    matrix(Outcome::Taken).await;
}

#[tokio::test]
async fn door_posts_each_presentation_whatever_request_stands_every_instance_refuses() {
    matrix(Outcome::Refused).await;
}

#[tokio::test]
async fn door_posts_each_presentation_whatever_request_stands_no_instance_answers() {
    matrix(Outcome::NoAnswer).await;
}

/// THE LOAD (28.11): one post per presentation and none per repeat. The
/// same user presenting the same body again is a merge, posting nothing.
#[tokio::test]
async fn one_post_per_presentation_none_per_repeat() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, "bG9hZA==");
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let wallet =
        common::wallet_posting("door-post", &root, answer_to(Outcome::NoAnswer, &txid)).await;
    let args = common::args(beef, common::payment(&sender, PREFIX, "bG9hZA=="));
    let _ = wallet.internalize_action(args.clone(), Some(ORIGIN)).await;
    let presented = wallet.services().call_count("post_beef");
    let _ = wallet.internalize_action(args, Some(ORIGIN)).await;
    let repeated = wallet.services().call_count("post_beef");
    println!("LOAD posts after the presentation {presented}, after the repeat {repeated}");
    assert_eq!((presented, repeated), (1, 1));
}
