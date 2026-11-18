//! A competitor's checked proof stands against any take (the keeper's
//! ruling of 2026-11-18 on the tracker charter's section 12 item 3 and
//! reading 11's second case of DECISIONS section 30).
//!
//! The matrix: the standing request of another user (`doubleSpend`,
//! `invalid`, `unsent`, `sending`, `unmined`, none), by the competitor's
//! checked proof (held and checking against our headers, held and its root
//! gone by a reorg, none held), by the act (this user's presentation at the
//! door, a take applied by the send-waiting task, by the retire path's
//! status source, by the unfail task, by the wallet's own post). Each
//! cell: posts made, the request's word after, the rows' words after, the
//! call's answer (the door's), and whether a take was recorded.
//!
//! The rule: with a competitor's checked proof held and checking, no act
//! raises a word (no request becomes `unmined`, no row `unproven`, no take
//! recorded) and the door posts nothing and answers reading 7's answer
//! (`doubleSpend` naming the competitor, `failed`); with its root gone, or
//! none held, every act's take counts as it does with none.
//!
//! The competitor is held as the store holds one: the payer's output row
//! whose `spent_by` names the competitor, the competitor's bytes and its
//! merkle path in `proven_txs`. Real SQLite, a mock broadcaster that takes
//! the transaction, a status source that knows it, a tracker that refutes
//! the competitor's root after the reorg; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, ProtocolEra};
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath};
use bsv_rs::wallet::WalletInterface;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    BroadcastOutcome, GetStatusForTxidsResult, MonitorStorage, StorageSqlx, TxStatusDetail, Wallet,
    WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "Y29tcGV0aXRvcg==";
const SUFFIX: &str = "c3RhbmRz";
const ORIGIN: &str = "test.local";

/// A door note of another user's presentation on the standing request.
const FOREIGN_NOTE: &str = r#"{"notes":[{"what":"doorPost","door":"internalize","user":999999,"instance":"arcade","word":"another user's words"}]}"#;

/// The reference's sentence of the review-actions refusal.
const SENTENCE: &str = "Undelayed createAction or signAction results require review.";

/// The height of the competitor's block.
const COMPETITOR_HEIGHT: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT + 150;

/// Our headers: every root checks but `gone`, the competitor's root after
/// a reorg took its block away.
struct Headers {
    gone: Option<(String, u32)>,
}

#[async_trait]
impl ChainTracker for Headers {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(self
            .gone
            .as_ref()
            .is_none_or(|(r, h)| !(r == root && *h == height)))
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(COMPETITOR_HEIGHT + 100)
    }
}

/// The competitor's checked proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Proof {
    /// Held, its root on our headers now.
    Checking,
    /// Held, its root gone by a reorg.
    Gone,
    /// None held.
    None,
}

/// The act that reaches a take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Door,
    SendWaiting,
    Retire,
    Unfail,
    OwnPost,
}

const WORDS: [Option<&str>; 6] = [
    Some("doubleSpend"),
    Some("invalid"),
    Some("unsent"),
    Some("sending"),
    Some("unmined"),
    None,
];

const PROOFS: [Proof; 3] = [Proof::Checking, Proof::Gone, Proof::None];

const ACTS: [Act; 5] = [
    Act::Door,
    Act::SendWaiting,
    Act::Retire,
    Act::Unfail,
    Act::OwnPost,
];

/// The other user's row beside its request word.
fn row_for(word: &str) -> &'static str {
    match word {
        "doubleSpend" | "invalid" => "failed",
        "unmined" => "unproven",
        _ => "sending",
    }
}

/// One cell's reading.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reading {
    posts: usize,
    answer: String,
    request: Option<String>,
    other: Option<String>,
    mine: Option<String>,
    taken: bool,
    /// The refusal's message, when the call answered one.
    message: Option<String>,
}

async fn scalar(wallet: &TestWallet, sql: &str, txid: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(txid)
        .fetch_optional(wallet.storage().pool())
        .await
        .unwrap()
}

/// The answer as a host reads it (`common::read_answer`), with the count
/// of competitors the review results name.
fn read_answer(answer: bsv_rs::Result<bsv_rs::wallet::InternalizeActionResult>) -> String {
    let competitors = match &answer {
        Err(bsv_rs::Error::WalletReviewActions(r)) => Some(
            r.review_action_results
                .iter()
                .map(|r| r.competing_txs.as_ref().map_or(0, Vec::len))
                .sum::<usize>(),
        ),
        _ => None,
    };
    let read = common::read_answer(answer);
    match competitors {
        Some(n) => format!("{read} competitors {n}"),
        None => read,
    }
}

/// Insert a transaction row of `user` and return its id.
async fn insert_tx(store: &StorageSqlx, user: i64, txid: &str, status: &str, raw: &[u8]) -> i64 {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
         version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'a row of the matrix', 0, 1, 0, ?, 1, ?, ?)",
    )
    .bind(user)
    .bind(txid)
    .bind(status)
    .bind(uuid_like(txid, status))
    .bind(raw)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

fn uuid_like(txid: &str, status: &str) -> String {
    format!("{}-{status}", &txid[..16])
}

/// One cell.
async fn cell(word: Option<&str>, proof: Proof, act: Act) -> Reading {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let txid = parts.txid.clone();
    let mut taken = common::accepted(&txid);
    taken.name = "arcade".to_string();
    let services = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![taken.clone()]))
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "status".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.clone(),
                status: "known".to_string(),
                ..Default::default()
            }],
        }))
        .build();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("competitor", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let wallet = Wallet::new(Some(root.clone()), storage, services)
        .await
        .unwrap();
    let store = wallet.storage();

    // The competitor: spends the payer's output 0 of the parent.
    let competitor = common::competitor_of(&parts.parent_txid, 0);
    let competitor_txid = competitor.id();
    let path = MerklePath::from_coinbase_txid(&competitor_txid, COMPETITOR_HEIGHT);
    let competitor_root = path.compute_root(Some(&competitor_txid)).unwrap();
    store
        .set_chain_tracker(Arc::new(Headers {
            gone: (proof == Proof::Gone).then(|| (competitor_root.clone(), COMPETITOR_HEIGHT)),
        }))
        .await;
    if proof != Proof::None {
        let payer = PrivateKey::random().public_key().to_hex();
        let (payer, _) = store.find_or_insert_user(&payer).await.unwrap();
        let parent_row =
            insert_tx(store, payer.user_id, &parts.parent_txid, "completed", &[1]).await;
        let competitor_row = insert_tx(
            store,
            payer.user_id,
            &competitor_txid,
            "completed",
            &competitor.to_binary(),
        )
        .await;
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, \
             provided_by, purpose, type, txid, spent_by) \
             VALUES (?, ?, 0, 1, 0, 1000, 'storage', 'change', 'custom', ?, ?)",
        )
        .bind(payer.user_id)
        .bind(parent_row)
        .bind(&parts.parent_txid)
        .bind(competitor_row)
        .execute(store.writer())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
             VALUES (?, ?, 0, ?, ?, ?, ?)",
        )
        .bind(&competitor_txid)
        .bind(COMPETITOR_HEIGHT)
        .bind("bb".repeat(32))
        .bind(&competitor_root)
        .bind(path.to_binary().unwrap())
        .bind(competitor.to_binary())
        .execute(store.writer())
        .await
        .unwrap();
    }

    // The other user's request word and row, written two hours ago.
    if let Some(word) = word {
        let other = PrivateKey::random().public_key().to_hex();
        let (other, _) = store.find_or_insert_user(&other).await.unwrap();
        insert_tx(store, other.user_id, &txid, row_for(word), &parts.raw).await;
        let then = chrono::Utc::now() - chrono::Duration::hours(2);
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
             raw_tx, input_beef, created_at, updated_at) VALUES (?, ?, 0, ?, 0, '{}', ?, ?, ?, ?)",
        )
        .bind(&txid)
        .bind(word)
        .bind(FOREIGN_NOTE)
        .bind(&parts.raw)
        .bind(&parts.beef)
        .bind(then)
        .bind(then)
        .execute(store.writer())
        .await
        .unwrap();
    }

    let mut answer = "-".to_string();
    let mut message = None;
    match act {
        Act::Door => {
            let got = wallet
                .internalize_action(
                    common::args(parts.beef.clone(), common::payment(&sender, PREFIX, SUFFIX)),
                    Some(ORIGIN),
                )
                .await;
            if let Err(bsv_rs::Error::WalletReviewActions(r)) = &got {
                message = Some(r.message.clone());
            }
            answer = read_answer(got);
        }
        Act::SendWaiting => {
            store
                .send_waiting_transactions(std::time::Duration::ZERO)
                .await
                .unwrap();
        }
        Act::Retire => {
            let services = wallet.services();
            let _ = store
                .retire_undeliverable_txid(services, &txid, "invalid")
                .await;
        }
        Act::Unfail => {
            store.un_fail().await.unwrap();
        }
        Act::OwnPost => {
            let _ = store
                .update_transaction_status_after_post(
                    &txid,
                    &BroadcastOutcome::Success,
                    &[taken],
                    None,
                )
                .await;
        }
    }

    let me = store
        .find_or_insert_user(&root.public_key().to_hex())
        .await
        .unwrap()
        .0
        .user_id;
    let mine: Option<String> =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND user_id = ?")
            .bind(&txid)
            .bind(me)
            .fetch_optional(store.pool())
            .await
            .unwrap();
    let other: Option<String> = sqlx::query_scalar(
        "SELECT status FROM transactions WHERE txid = ? AND user_id <> ? LIMIT 1",
    )
    .bind(&txid)
    .bind(me)
    .fetch_optional(store.pool())
    .await
    .unwrap();
    Reading {
        posts: wallet.services().call_count("post_beef"),
        answer,
        request: scalar(&wallet, "SELECT status FROM proven_tx_reqs WHERE txid = ?", &txid).await,
        other,
        mine,
        taken: scalar(
            &wallet,
            "SELECT status FROM broadcast_seen WHERE txid = ? AND status IN ('accepted', 'seen', 'mined') LIMIT 1",
            &txid,
        )
        .await
        .is_some(),
        message,
    }
}

/// Did the act raise a word (a request `unmined`, a row `unproven`, or a
/// take recorded) that the store did not hold before it?
fn raised(word: Option<&str>, got: &Reading) -> bool {
    let request_raised = got.request.as_deref() == Some("unmined") && word != Some("unmined");
    let other_raised =
        got.other.as_deref() == Some("unproven") && word.map(row_for) != Some("unproven");
    request_raised || other_raised || got.mine.as_deref() == Some("unproven") || got.taken
}

/// Does the act reach a take for a standing `word` with no competitor held
/// (the column the rule leaves as it was)?
fn reaches(act: Act, word: Option<&str>) -> bool {
    match act {
        Act::Door => true,
        Act::SendWaiting => matches!(word, Some("unsent") | Some("sending")),
        Act::Retire => matches!(word, Some(w) if w != "unmined"),
        Act::Unfail => matches!(word, Some("invalid") | Some("doubleSpend")),
        // With no request and no row the store holds no bytes of the
        // transaction: the take is recorded and moves nothing.
        Act::OwnPost => true,
    }
}

async fn matrix(act: Act) {
    let mut wrong = Vec::new();
    for word in WORDS {
        let mut none = None;
        for proof in [Proof::None, Proof::Gone, Proof::Checking] {
            let got = cell(word, proof, act).await;
            println!(
                "MATRIX {act:?} request {:<11} proof {:<8} -> posts {} request {:?} other {:?} mine {:?} taken {} answer {}",
                word.unwrap_or("none"),
                format!("{proof:?}"),
                got.posts,
                got.request,
                got.other,
                got.mine,
                got.taken,
                got.answer
            );
            match proof {
                Proof::None => {
                    if raised(word, &got) != reaches(act, word) {
                        wrong.push(format!(
                            "{word:?} none held: raised {} want {}: {got:?}",
                            raised(word, &got),
                            reaches(act, word)
                        ));
                    }
                    none = Some(got);
                }
                Proof::Gone => {
                    if Some(&got) != none.as_ref() {
                        wrong.push(format!(
                            "{word:?} root gone: {got:?}, want as none held {none:?}"
                        ));
                    }
                }
                Proof::Checking => {
                    // The lookup reads the transaction's inputs from its
                    // bytes; for a txid the store holds no bytes of (the
                    // wallet's own post with no request and no row) it has
                    // nothing to compare, so the take is recorded as with
                    // none held. Such a take moves no word, and the door's
                    // own lookup on the presented bytes still refutes any
                    // later presentation (the door column).
                    let no_bytes = act == Act::OwnPost && word.is_none();
                    if raised(word, &got) && !no_bytes {
                        wrong.push(format!("{word:?} checking: a word raised: {got:?}"));
                    }
                    if act == Act::Door {
                        let want = (
                            0,
                            "review [DoubleSpend] [Failed] competitors 1",
                            None::<&str>,
                        );
                        if (got.posts, got.answer.as_str(), got.mine.as_deref()) != want {
                            wrong.push(format!("{word:?} checking at the door: {got:?}"));
                        }
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{act:?}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_competitors_checked_proof_stands_against_the_door() {
    matrix(Act::Door).await;
}

#[tokio::test]
async fn a_competitors_checked_proof_stands_against_send_waiting() {
    matrix(Act::SendWaiting).await;
}

#[tokio::test]
async fn a_competitors_checked_proof_stands_against_the_retire_path() {
    matrix(Act::Retire).await;
}

#[tokio::test]
async fn a_competitors_checked_proof_stands_against_unfail() {
    matrix(Act::Unfail).await;
}

#[tokio::test]
async fn a_competitors_checked_proof_stands_against_the_wallets_own_post() {
    matrix(Act::OwnPost).await;
}

/// THE LOAD (28.11) with the guard: no post at all for a presentation a
/// held, checking competitor's proof refutes; the lookup's own cost is one
/// header read for the one competitor found, and none when none is held.
#[tokio::test]
async fn the_door_posts_nothing_for_a_refuted_presentation() {
    let refuted = cell(None, Proof::Checking, Act::Door).await;
    let unrefuted = cell(None, Proof::None, Act::Door).await;
    println!(
        "LOAD refuted: posts {} answer {}; none held: posts {} answer {}",
        refuted.posts, refuted.answer, unrefuted.posts, unrefuted.answer
    );
    assert_eq!((refuted.posts, unrefuted.posts), (0, 1));
}

/// ITEM 3: a call that made no post (a competitor's checked proof
/// refutes it) finds none of its own notes: its message is the sentence
/// alone, never another user's presentation's words on the request.
#[tokio::test]
async fn a_call_that_made_no_post_gives_the_sentence_alone() {
    let got = cell(Some("unsent"), Proof::Checking, Act::Door).await;
    println!(
        "ITEM3 no post: posts {} message {:?}",
        got.posts, got.message
    );
    assert_eq!((got.posts, got.message.as_deref()), (0, Some(SENTENCE)));
}

/// `ACTS` and `PROOFS` name every column the matrix runs.
#[test]
fn the_matrix_names_every_act_and_proof() {
    assert_eq!(ACTS.len() * PROOFS.len() * WORDS.len(), 90);
}
