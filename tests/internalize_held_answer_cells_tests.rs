//! Cells of the held payment's answer that the answer's own witnesses do
//! not write (the standard read of the answer's PR, committed here as
//! witnesses), and the wallet's answer over a `WalletStorageManager`, so
//! the wallet-level answer is witnessed at all three doors (a local store,
//! a `StorageClient` in `internalize_held_answer_tests.rs`, a manager
//! here).
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::{
    PostBeefResult, PostTxResultForTxid, StorageSqlx, Wallet, WalletStorageManager,
    WalletStorageWriter,
};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

const PREFIX: &str = "Y2VsbHM=";
const ORIGIN: &str = "test.local";

fn no_answer() -> MockResponse<Vec<PostBeefResult>> {
    MockResponse::Error(MockErrorKind::NetworkError, "no instance answered".into())
}

async fn scalar(wallet: &TestWallet, sql: &str, txid: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(txid)
        .fetch_optional(wallet.storage().pool())
        .await
        .unwrap()
}

/// This user's row for `txid` (the newest).
async fn mine(wallet: &TestWallet, txid: &str) -> Option<String> {
    scalar(
        wallet,
        "SELECT status FROM transactions WHERE txid = ? ORDER BY transaction_id DESC LIMIT 1",
        txid,
    )
    .await
}

/// A payment to `root` with no BUMP of its own, the same transaction
/// carrying one, and the txid.
fn pay(root: &PrivateKey, suffix: &str) -> (InternalizeActionArgs, InternalizeActionArgs, String) {
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(root, &sender, PREFIX, suffix);
    let lock = LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap();
    let (beef, txid) = common::beef_paying(lock.clone());
    let (proven, _) = common::proven_beef_paying(lock);
    let args = common::args(beef, common::payment(&sender, PREFIX, suffix));
    let mut with_proof = args.clone();
    with_proof.tx = proven;
    (args, with_proof, txid)
}

/// Another user's request `req` and row `row` for `txid`.
async fn other_user_rows(wallet: &TestWallet, txid: &str, req: &str, row: &str) {
    let store = wallet.storage();
    let other = PrivateKey::random().public_key().to_hex();
    let (user, _) = store.find_or_insert_user(&other).await.unwrap();
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
         version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, ?, 'other', 'the other user', 0, 1, 0, X'01', 1, ?, ?)",
    )
    .bind(user.user_id)
    .bind(txid)
    .bind(row)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, \
         created_at, updated_at) VALUES (?, ?, 0, '{}', 0, '{}', X'01', ?, ?)",
    )
    .bind(txid)
    .bind(req)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap();
}

/// A stored proof whose root was checked (a `proven_txs` row that
/// `proof_root_checks` records at its height with its root): accepted, no
/// post.
#[tokio::test]
async fn a_stored_root_checked_proof_is_accepted_with_no_post() {
    let root = PrivateKey::random();
    let (args, _, txid) = pay(&root, "YQ==");
    let wallet = common::wallet_posting("cells", &root, no_answer()).await;
    other_user_rows(&wallet, &txid, "completed", "completed").await;
    let store = wallet.storage();
    let proof: i64 = sqlx::query_scalar(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, 100, 0, 'bh', 'ABCD', X'00', X'01') RETURNING proven_tx_id",
    )
    .bind(&txid)
    .fetch_one(store.writer())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO proof_root_checks (txid, height, merkle_root) VALUES (?, 100, 'abcd')",
    )
    .bind(&txid)
    .execute(store.writer())
    .await
    .unwrap();
    sqlx::query("UPDATE proven_tx_reqs SET proven_tx_id = ? WHERE txid = ?")
        .bind(proof)
        .bind(&txid)
        .execute(store.writer())
        .await
        .unwrap();
    let answer = common::read_answer(wallet.internalize_action(args, Some(ORIGIN)).await);
    let posts = wallet.services().call_count("post_beef");
    let row = mine(&wallet, &txid).await;
    println!("CELL stored checked proof: answer {answer} posts {posts} mine {row:?}");
    assert_eq!(
        (answer.as_str(), posts, row.as_deref()),
        ("accepted", 0, Some("unproven"))
    );
}

/// Another user's `unmined` request with a recorded take: accepted, no
/// post, this user's row `unproven`.
#[tokio::test]
async fn a_recorded_take_beside_an_unmined_request_is_accepted_with_no_post() {
    let root = PrivateKey::random();
    let (args, _, txid) = pay(&root, "Yg==");
    let wallet = common::wallet_posting("cells", &root, no_answer()).await;
    other_user_rows(&wallet, &txid, "unmined", "unproven").await;
    sqlx::query(
        "INSERT INTO broadcast_seen (txid, provider, status) VALUES (?, 'arcade', 'accepted')",
    )
    .bind(&txid)
    .execute(wallet.storage().writer())
    .await
    .unwrap();
    let answer = common::read_answer(wallet.internalize_action(args, Some(ORIGIN)).await);
    let posts = wallet.services().call_count("post_beef");
    let row = mine(&wallet, &txid).await;
    println!("CELL unmined and a take: answer {answer} posts {posts} mine {row:?}");
    assert_eq!(
        (answer.as_str(), posts, row.as_deref()),
        ("accepted", 0, Some("unproven"))
    );
}

/// Two users present one transaction nobody takes: one post each, both
/// held; the door keeps one note for the instance (`*`, none answered).
/// The same user repeating is a merge: `accepted: true` today, no post (the
/// repeat piece changes that answer; it does not mean taken).
#[tokio::test]
async fn two_users_present_nobody_takes_then_a_repeat() {
    let root_a = PrivateKey::random();
    let root_b = PrivateKey::random();
    let (args_a, _, txid) = pay(&root_a, "Yw==");
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("cells", &root_a.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    let services = || {
        MockWalletServices::builder()
            .post_beef_response(no_answer())
            .build()
    };
    let wallet_a = Wallet::new(Some(root_a), store.clone(), services())
        .await
        .unwrap();
    let a = common::read_answer(
        wallet_a
            .internalize_action(args_a.clone(), Some(ORIGIN))
            .await,
    );
    let wallet_b = Wallet::new(Some(root_b), store.clone(), services())
        .await
        .unwrap();
    let mut args_b = args_a;
    args_b.outputs = vec![InternalizeOutput {
        output_index: 0,
        protocol: "basket insertion".to_string(),
        payment_remittance: None,
        insertion_remittance: Some(BasketInsertion {
            basket: "theirs".to_string(),
            custom_instructions: None,
            tags: None,
        }),
    }];
    let b = common::read_answer(
        wallet_b
            .internalize_action(args_b.clone(), Some(ORIGIN))
            .await,
    );
    let posts = || {
        wallet_a.services().call_count("post_beef") + wallet_b.services().call_count("post_beef")
    };
    let after_two = posts();
    let repeat = common::read_answer(wallet_b.internalize_action(args_b, Some(ORIGIN)).await);
    let after_repeat = posts();
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? ORDER BY user_id")
            .bind(&txid)
            .fetch_all(store.pool())
            .await
            .unwrap();
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let notes = history.matches("\"doorPost\"").count();
    println!(
        "CELL two users: a {a} b {b} repeat {repeat} posts {after_two} then {after_repeat} rows {rows:?} door notes {notes}"
    );
    let held = "review [ServiceError] [Sending]";
    assert_eq!((a.as_str(), b.as_str()), (held, held));
    assert_eq!((after_two, after_repeat), (2, 2));
    assert_eq!(repeat, "accepted");
    assert_eq!(rows, vec!["sending".to_string(), "sending".to_string()]);
    assert_eq!(notes, 1);
}

/// A BUMP the proof gate defers: accepted, no post, `unproven`.
#[tokio::test]
async fn a_deferred_proof_is_accepted_with_no_post() {
    let root = PrivateKey::random();
    let (_, with_proof, txid) = pay(&root, "ZA==");
    let wallet = common::wallet_posting("cells", &root, no_answer()).await;
    let answer = common::read_answer(wallet.internalize_action(with_proof, Some(ORIGIN)).await);
    let posts = wallet.services().call_count("post_beef");
    let row = mine(&wallet, &txid).await;
    println!("CELL deferred proof: answer {answer} posts {posts} row {row:?}");
    assert_eq!(
        (answer.as_str(), posts, row.as_deref()),
        ("accepted", 0, Some("unproven"))
    );
}

/// One instance takes it while another names a competitor: a take beats a
/// refusal (reading 10), accepted, one post, `unproven`.
#[tokio::test]
async fn a_take_beats_a_refusal_naming_a_competitor() {
    let root = PrivateKey::random();
    let (args, _, txid) = pay(&root, "ZQ==");
    let mut took = common::accepted(&txid);
    took.name = "arcade".into();
    let no = PostBeefResult {
        name: "bitails".into(),
        status: "error".into(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.clone(),
            status: "error".into(),
            double_spend: true,
            orphan_mempool: false,
            competing_txs: Some(vec!["cc".repeat(32)]),
            data: Some("a competitor".into()),
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: None,
        notes: vec![],
    };
    let wallet =
        common::wallet_posting("cells", &root, MockResponse::Success(vec![no, took])).await;
    let answer = common::read_answer(wallet.internalize_action(args, Some(ORIGIN)).await);
    let posts = wallet.services().call_count("post_beef");
    let row = mine(&wallet, &txid).await;
    println!("CELL a take and a competitor: answer {answer} posts {posts} row {row:?}");
    assert_eq!(
        (answer.as_str(), posts, row.as_deref()),
        ("accepted", 1, Some("unproven"))
    );
}

/// A wallet over a `WalletStorageManager`, built off the runtime (a std
/// thread entering the runtime's handle, `Wallet::new` driven by
/// `futures::executor::block_on`: issue #855 is why it cannot be built on
/// the runtime). Nobody takes the payment: the review-actions refusal, the
/// txid `sending`, `serviceError`, the txid named.
#[test]
fn a_wallet_over_a_manager_answers_held() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, "bQ==");
    let (beef, txid) =
        common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let manager = runtime.block_on(async {
        let store = Arc::new(StorageSqlx::in_memory().await.unwrap());
        store.migrate("cells", &identity).await.unwrap();
        store.make_available().await.unwrap();
        let manager = WalletStorageManager::new(identity.clone(), Some(store), None);
        manager.make_available().await.unwrap();
        manager
    });
    let services = MockWalletServices::builder()
        .post_beef_response(no_answer())
        .build();
    let handle = runtime.handle().clone();
    let key_root = root.clone();
    let wallet = std::thread::spawn(move || {
        let _entered = handle.enter();
        futures::executor::block_on(Wallet::new(Some(key_root), manager, services)).unwrap()
    })
    .join()
    .unwrap();
    let args = common::args(beef, common::payment(&sender, PREFIX, "bQ=="));
    let answer = runtime.block_on(wallet.internalize_action(args, Some(ORIGIN)));
    let named = match &answer {
        Err(bsv_rs::Error::WalletReviewActions(r)) => r.txid.map(hex::encode),
        _ => None,
    };
    let read = common::read_answer(answer);
    println!(
        "CELL manager wallet: {read} txid named {}",
        named.as_deref() == Some(txid.as_str())
    );
    assert_eq!(read, "review [ServiceError] [Sending]");
    assert_eq!(named.as_deref(), Some(txid.as_str()));
}
