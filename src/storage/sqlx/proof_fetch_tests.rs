//! The create door's broadcast with a proof in hand (the BEEF walk's own
//! fetch, `create_action/broadcast.rs` `fetch_and_store_merkle_path`) over a
//! txid with a `failed` row (the walk reads that txid only beside another
//! user's row of it that is not `failed`): the fetched proof, its root checked by the
//! tracker, completes every row of the txid in the walk's one write
//! (`proof_completion`): the `failed` row `completed`, its input marked
//! spent by it, its own output counted. A completion that fails rolls the
//! whole write back: no proof stored, the row as it was.

use std::sync::Arc;

use bsv_rs::transaction::MockChainTracker;
use chrono::Utc;

use super::proof_root_tests::walk;
use super::reorg_tests::{
    child_spending, insert_req, open_gate, proven_row, req_state, seed_user, single_leaf_bump,
    storage, txid_of, COINBASE_HEX, COINBASE_TXID,
};
use super::StorageSqlx;
use crate::services::mock::{MockResponse, MockWalletServices};
use crate::services::{BlockHeader, GetMerklePathResult, WalletServices};
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};

const HEIGHT: u32 = 100;

/// A store with the sender's aborted action A (spending the coinbase's
/// output 0, `failed`, the coin released, its own output closed), another
/// user's held receipt of A (`sending`), A's request `unmined`, services that serve A's proof and a tracker that
/// confirms its root. Returns the store, A's txid, the coin and A's output.
async fn aborted_with_a_fetchable_proof() -> (StorageSqlx, String, i64, i64) {
    let s = storage().await;
    let (user, _) = seed_user(&s).await;
    let raw = child_spending(COINBASE_TXID);
    let a = txid_of(&raw);
    // Old enough for the walk to ask for a proof.
    let now = Utc::now() - chrono::Duration::hours(2);
    let row = |user: i64, txid: &str, status: &'static str, raw: Option<Vec<u8>>| {
        let s = s.clone();
        let txid = txid.to_string();
        async move {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
                 description, txid, version, lock_time, raw_tx, created_at, updated_at) \
                 VALUES (?, ?, ?, 1, 0, 'a fetched proof', ?, 1, 0, ?, ?, ?) RETURNING transaction_id",
            )
            .bind(user)
            .bind(status)
            .bind(format!("ref-{}-{user}", &txid[..8]))
            .bind(&txid)
            .bind(raw)
            .bind(now)
            .bind(now)
            .fetch_one(s.writer())
            .await
            .unwrap()
        }
    };
    let output = |transaction_id: i64, txid: &str, spendable: bool| {
        let s = s.clone();
        let txid = txid.to_string();
        async move {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, \
                 provided_by, purpose, type, txid) VALUES (?, ?, ?, 1, 0, 1000, 'storage', \
                 'change', 'P2PKH', ?) RETURNING output_id",
            )
            .bind(user)
            .bind(transaction_id)
            .bind(spendable)
            .bind(&txid)
            .fetch_one(s.writer())
            .await
            .unwrap()
        }
    };
    let parent = row(
        user,
        COINBASE_TXID,
        "completed",
        Some(hex::decode(COINBASE_HEX).unwrap()),
    )
    .await;
    let coin = output(parent, COINBASE_TXID, true).await;
    let action = row(user, &a, "failed", Some(raw.clone())).await;
    let own = output(action, &a, false).await;
    let (other, _) = s.find_or_insert_user(&"b".repeat(66)).await.unwrap();
    sqlx::query("UPDATE transactions SET is_outgoing = 0 WHERE transaction_id = ?")
        .bind(row(other.user_id, &a, "sending", Some(raw.clone())).await)
        .execute(s.writer())
        .await
        .unwrap();
    insert_req(&s, &a, &raw, "unmined", 0, None, now).await;
    let header = BlockHeader {
        height: HEIGHT,
        hash: "a".repeat(64),
        merkle_root: a.clone(),
        previous_hash: "p".repeat(64),
        version: 0x2000_0000,
        time: 1_700_000_000,
        bits: 0,
        nonce: 0,
    };
    let found = GetMerklePathResult {
        merkle_path: Some(hex::encode(single_leaf_bump(HEIGHT, &a))),
        header: Some(header),
        name: Some("mock".to_string()),
        notes: vec![],
        error: None,
    };
    let services = MockWalletServices::builder()
        .get_merkle_path_response(MockResponse::Success(found))
        .build();
    s.set_services(Arc::new(services) as Arc<dyn WalletServices>);
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(HEIGHT, a.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 200).await;
    (s, a, coin, own)
}

async fn reading(
    s: &StorageSqlx,
    a: &str,
    coin: i64,
    own: i64,
) -> (String, (bool, Option<i64>), bool) {
    let status: String =
        sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ? AND is_outgoing = 1")
            .bind(a)
            .fetch_one(s.pool())
            .await
            .unwrap();
    let coin: (bool, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(coin)
            .fetch_one(s.pool())
            .await
            .unwrap();
    let own: bool = sqlx::query_scalar("SELECT spendable FROM outputs WHERE output_id = ?")
        .bind(own)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (status, coin, own)
}

#[tokio::test]
async fn a_fetched_checked_proof_completes_a_failed_row() {
    let (s, a, coin, own) = aborted_with_a_fetchable_proof().await;
    let action: i64 = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions WHERE txid = ? AND is_outgoing = 1",
    )
    .bind(&a)
    .fetch_one(s.pool())
    .await
    .unwrap();
    let beef = walk(&s, &a, None).await;
    let got = reading(&s, &a, coin, own).await;
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&a)
        .fetch_one(s.pool())
        .await
        .unwrap();
    println!("fetched: {got:?} {history}");
    assert!(beef.find_txid(&a).and_then(|t| t.bump_index()).is_some());
    assert_eq!(got, ("completed".to_string(), (false, Some(action)), true));
    assert_eq!(req_state(&s, &a).await.unwrap().0, "completed");
    assert!(history.contains("provenOverFailed"));
}

/// A completion that fails (a planted fault on the output write) rolls the
/// whole write back: nothing stored, the row and its coins as they were.
#[tokio::test]
async fn a_completion_that_fails_rolls_the_fetched_proof_back() {
    let (s, a, coin, own) = aborted_with_a_fetchable_proof().await;
    let before = reading(&s, &a, coin, own).await;
    sqlx::query(
        "CREATE TRIGGER planted BEFORE UPDATE OF spendable ON outputs \
         BEGIN SELECT RAISE(ABORT, 'planted'); END",
    )
    .execute(s.writer())
    .await
    .unwrap();
    let beef = walk(&s, &a, None).await;
    let got = reading(&s, &a, coin, own).await;
    println!("rolled back: {got:?}");
    assert!(beef.find_txid(&a).and_then(|t| t.bump_index()).is_none());
    assert_eq!(got, before);
    assert!(proven_row(&s, &a).await.is_none());
    assert_eq!(req_state(&s, &a).await.unwrap().0, "unmined");
}
