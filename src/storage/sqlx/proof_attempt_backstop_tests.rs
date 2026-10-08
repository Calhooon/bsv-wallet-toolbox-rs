//! The attempt backstop (Calgooon/zanaadu-v2#368): a status pass that finds
//! no proof is an attempt for EVERY req the proof task checks, as in the
//! reference (wallet-toolbox src/monitor/tasks/TaskCheckForProofs.ts:154-165,
//! 235; `unprovenAttemptsLimitMain: 144`, src/monitor/Monitor.ts:106), and
//! past the limit a req no source holds is written off through the release
//! rule. A req a source still holds is never written off by the count alone.

use std::sync::Arc;

use bsv_rs::transaction::MockChainTracker;
use chrono::{Duration, Utc};
use sqlx::Row;

use super::reorg_tests::{
    child_spending, insert_req, open_gate, req_state, seed_user, single_leaf_bump, storage,
    tx_state, txid_of,
};
use super::storage_sqlx::{PROOF_ATTEMPTS_LIMIT_MAIN, PROOF_ATTEMPTS_LIMIT_TEST};
use super::StorageSqlx;
use crate::services::broadcast_memory::{
    BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_SEEN, PROVIDER_ARCADE_V2, PROVIDER_GORILLAPOOL_ARC,
};
use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServicesBuilder};
use crate::services::traits::{GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail};
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::traits::WalletStorageProvider;
use crate::storage::MonitorStorage;

const LIMIT: i64 = PROOF_ATTEMPTS_LIMIT_MAIN;

async fn insert_tx(s: &StorageSqlx, user_id: i64, txid: &str, status: &str) -> i64 {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'backstop test tx', 0, 1, 0, 1, ?, ?)",
    )
    .bind(user_id)
    .bind(txid)
    .bind(status)
    .bind(format!("ref-{}", &txid[..8]))
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn insert_output(
    s: &StorageSqlx,
    user_id: i64,
    transaction_id: i64,
    txid: &str,
    spendable: bool,
    spent_by: Option<i64>,
) -> i64 {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, spent_by, created_at, updated_at) \
         VALUES (?, ?, ?, 1, 0, 9000, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(spendable)
    .bind(txid)
    .bind(spent_by)
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn output_state(s: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
    let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (row.get("spendable"), row.get("spent_by"))
}

/// A broadcast transaction of this wallet: `unproven`, its change spendable,
/// the coin it spends locked by it, its req `unmined` at `attempts`.
struct Broadcast {
    txid: String,
    transaction_id: i64,
    change: i64,
    input: i64,
}

async fn seed_broadcast(s: &StorageSqlx, seed: &str, attempts: i64) -> Broadcast {
    let (user_id, _) = seed_user(s).await;
    let prev_txid = seed.repeat(32);
    let raw = child_spending(&prev_txid);
    let txid = txid_of(&raw);
    let prev_tx_id = insert_tx(s, user_id, &prev_txid, "completed").await;
    let transaction_id = insert_tx(s, user_id, &txid, "unproven").await;
    let input = insert_output(
        s,
        user_id,
        prev_tx_id,
        &prev_txid,
        false,
        Some(transaction_id),
    )
    .await;
    let change = insert_output(s, user_id, transaction_id, &txid, true, None).await;
    insert_req(s, &txid, &raw, "unmined", attempts, None, Utc::now()).await;
    open_gate(s, 1000).await;
    Broadcast {
        txid,
        transaction_id,
        change,
        input,
    }
}

fn status(detail: TxStatusDetail) -> MockResponse<GetStatusForTxidsResult> {
    MockResponse::Success(GetStatusForTxidsResult {
        name: "mock".to_string(),
        status: "success".to_string(),
        error: None,
        results: vec![detail],
    })
}

fn no_proof() -> MockResponse<GetMerklePathResult> {
    MockResponse::Success(GetMerklePathResult {
        name: Some("mock".to_string()),
        merkle_path: None,
        header: None,
        error: None,
        notes: vec![],
    })
}

/// Every source says it has never heard of `txid`; the coins are unspent.
fn nobody_holds(s: &StorageSqlx, txid: &str) {
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail::new(txid, "unknown", None)))
        .get_merkle_path_response(no_proof())
        .is_utxo_response(MockResponse::Success(true))
        .build();
    WalletStorageProvider::set_services(s, Arc::new(mock));
}

async fn attempts_of(s: &StorageSqlx, txid: &str) -> (String, i64) {
    let (status, attempts, _, _) = req_state(s, txid).await.unwrap();
    (status, attempts)
}

async fn age_req(s: &StorageSqlx, txid: &str, hours: i64) {
    sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
        .bind(Utc::now() - Duration::hours(hours))
        .bind(txid)
        .execute(s.pool())
        .await
        .unwrap();
}

#[test]
fn the_limits_are_the_references() {
    assert_eq!(PROOF_ATTEMPTS_LIMIT_MAIN, 144);
    assert_eq!(PROOF_ATTEMPTS_LIMIT_TEST, 10);
}

/// The #368 shape replayed from attempts 0: no broadcaster refused it, no
/// source holds it, it is never mined. Every pass counts; the pass that
/// finds `attempts > 144` fails it and its inputs go to the release rule.
/// Before the fix: `unmined`, attempts 0, change spendable, forever.
#[tokio::test]
async fn a_never_held_never_mined_req_fails_past_the_limit_and_releases_its_inputs() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a1", 0).await;
    nobody_holds(&s, &b.txid);

    for pass in 1..=(LIMIT + 1) {
        open_gate(&s, 1000 + pass as u32).await;
        let out = s.synchronize_transaction_statuses().await.unwrap();
        assert!(out.is_empty(), "pass {pass} writes nothing off");
        assert_eq!(
            attempts_of(&s, &b.txid).await,
            ("unmined".to_string(), pass),
            "a pass that finds no proof is an attempt"
        );
    }
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
    assert_eq!(output_state(&s, b.change).await, (true, None));

    let out = s.synchronize_transaction_statuses().await.unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].txid, b.txid);
    assert_eq!(out[0].status, ProvenTxReqStatus::Invalid);
    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 0));
    assert_eq!(tx_state(&s, &b.txid).await.0, "failed");
    assert_eq!(
        output_state(&s, b.change).await,
        (false, None),
        "its change never funds anything"
    );
    assert_eq!(
        output_state(&s, b.input).await,
        (true, None),
        "the coin it locked is released (is_utxo said unspent)"
    );
    assert_eq!(
        s.broadcast_status_of(&b.txid, PROVIDER_ARCADE_V2)
            .await
            .unwrap()
            .map(|r| r.status),
        None,
        "no broadcaster row is invented"
    );

    // Written off once: the next pass has nothing to do with it.
    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 0));
}

/// The release rule, not a blind release: an input the chain does not vouch
/// for stays locked when the backstop fails the transaction.
#[tokio::test]
async fn the_backstop_keeps_an_unverified_input_locked() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a2", LIMIT + 1).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail::new(&b.txid, "unknown", None)))
        .is_utxo_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "utxo source down".to_string(),
        ))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));

    s.synchronize_transaction_statuses().await.unwrap();

    assert_eq!(tx_state(&s, &b.txid).await.0, "failed");
    assert_eq!(
        output_state(&s, b.input).await,
        (false, Some(b.transaction_id))
    );
}

/// The limit is "greater than", as in the reference: at exactly 144 the req
/// is checked once more.
#[tokio::test]
async fn at_exactly_the_limit_the_req_is_checked_once_more() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a3", LIMIT).await;
    nobody_holds(&s, &b.txid);

    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        attempts_of(&s, &b.txid).await,
        ("unmined".to_string(), LIMIT + 1)
    );
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");

    assert_eq!(s.synchronize_transaction_statuses().await.unwrap().len(), 1);
    assert_eq!(tx_state(&s, &b.txid).await.0, "failed");
}

/// The #357 incident shape, seen from the backstop: Arcade REJECTED, another
/// broadcaster (GorillaPool) SEEN, the status sources silent, the count long
/// past the limit. The fresh SEEN holds it: still `unproven`, change
/// spendable. Then the network's MINED arrives with its proof and it proves.
#[tokio::test]
async fn the_357_incident_shape_stays_unproven_past_the_limit_and_proves() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a4", LIMIT + 50).await;
    s.record_broadcast_status(&b.txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_REJECTED)
        .await
        .unwrap();
    s.record_broadcast_status(&b.txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_SEEN)
        .await
        .unwrap();
    nobody_holds(&s, &b.txid);

    for pass in 1..=3 {
        open_gate(&s, 1000 + pass as u32).await;
        assert!(s
            .synchronize_transaction_statuses()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            attempts_of(&s, &b.txid).await,
            ("unmined".to_string(), LIMIT + 50 + pass),
            "held: counted, not written off"
        );
    }
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
    assert_eq!(output_state(&s, b.change).await, (true, None));
    assert_eq!(
        output_state(&s, b.input).await,
        (false, Some(b.transaction_id))
    );

    // MINED at 900, the proof in the status answer, the tracker knows the root.
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, b.txid.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail {
            txid: b.txid.clone(),
            status: "mined".to_string(),
            depth: Some(100),
            merkle_path: Some(hex::encode(single_leaf_bump(900, &b.txid))),
            block_height: Some(900),
            block_hash: Some("c".repeat(64)),
        }))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));

    let out = s.synchronize_transaction_statuses().await.unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].status, ProvenTxReqStatus::Completed);
    assert_eq!(attempts_of(&s, &b.txid).await.0, "completed");
    assert_eq!(tx_state(&s, &b.txid).await.0, "completed");
    assert_eq!(output_state(&s, b.change).await, (true, None));
}

/// A req the status sources still hold (mempool) is not failed at the limit
/// or past it, however long it waits.
#[tokio::test]
async fn a_req_the_status_sources_hold_is_not_failed_past_the_limit() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a5", LIMIT).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail::new(&b.txid, "known", Some(0))))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));

    for pass in 1..=5 {
        open_gate(&s, 1000 + pass as u32).await;
        assert!(s
            .synchronize_transaction_statuses()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            attempts_of(&s, &b.txid).await,
            ("unmined".to_string(), LIMIT + pass)
        );
    }
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
    assert_eq!(output_state(&s, b.change).await, (true, None));
}

/// A req the status sources call mined but that no proof arrives for is held
/// too: counted by the fetch, never written off by the count. Before the fix
/// this arm set the req `invalid` at 144 and left the transaction `unproven`.
#[tokio::test]
async fn a_mined_req_with_no_proof_yet_is_not_failed_past_the_limit() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a6", LIMIT + 1).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail::new(&b.txid, "mined", Some(3))))
        .get_merkle_path_response(no_proof())
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));

    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());

    assert_eq!(
        attempts_of(&s, &b.txid).await,
        ("unmined".to_string(), LIMIT + 2)
    );
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
}

/// The freshness window: a SEEN older than 2 hours no longer holds the req
/// (the window of #357's memory evidence); only a live word would.
#[tokio::test]
async fn a_stale_seen_does_not_hold_the_req() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a7", LIMIT + 1).await;
    s.record_broadcast_status(&b.txid, PROVIDER_GORILLAPOOL_ARC, BROADCAST_STATUS_SEEN)
        .await
        .unwrap();
    sqlx::query("UPDATE broadcast_seen SET seen_at = ? WHERE txid = ?")
        .bind(Utc::now() - Duration::hours(3))
        .bind(&b.txid)
        .execute(s.pool())
        .await
        .unwrap();
    nobody_holds(&s, &b.txid);

    assert_eq!(s.synchronize_transaction_statuses().await.unwrap().len(), 1);

    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 0));
    assert_eq!(tx_state(&s, &b.txid).await.0, "failed");
    assert_eq!(output_state(&s, b.input).await, (true, None));
}

/// The release rule asks the status sources once more before it writes
/// anything: a transaction they hold by then is promoted, not failed.
#[tokio::test]
async fn a_live_word_at_the_last_moment_keeps_it() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a8", LIMIT + 1).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(MockResponse::Sequence(vec![
            status(TxStatusDetail::new(&b.txid, "unknown", None)),
            status(TxStatusDetail::new(&b.txid, "known", Some(0))),
        ]))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));

    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());

    assert_eq!(attempts_of(&s, &b.txid).await.0, "unmined");
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
    assert_eq!(output_state(&s, b.change).await, (true, None));
}

/// What does not count: a status source that cannot answer, a closed proof
/// gate, and a req `send_waiting_transactions` still owns.
#[tokio::test]
async fn a_silent_source_a_closed_gate_and_a_sending_req_count_nothing() {
    let s = storage().await;
    let b = seed_broadcast(&s, "a9", LIMIT + 1).await;

    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(MockResponse::Error(
            MockErrorKind::ServiceError,
            "status source down".to_string(),
        ))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));
    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        attempts_of(&s, &b.txid).await,
        ("unmined".to_string(), LIMIT + 1)
    );

    nobody_holds(&s, &b.txid);
    open_gate(&s, 0).await;
    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        attempts_of(&s, &b.txid).await,
        ("unmined".to_string(), LIMIT + 1)
    );

    open_gate(&s, 1000).await;
    sqlx::query("UPDATE proven_tx_reqs SET status = 'sending' WHERE txid = ?")
        .bind(&b.txid)
        .execute(s.pool())
        .await
        .unwrap();
    assert!(s
        .synchronize_transaction_statuses()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        attempts_of(&s, &b.txid).await,
        ("sending".to_string(), LIMIT + 1)
    );
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
}

/// The canary's view of a backstopped req: `invalid` with attempts 0 on a
/// `failed` transaction, so it is on the canary's HOURLY schedule (a req
/// left at 145 would have been asked once a day). No word: re-stamped and
/// watched. The network's word: recovered, the input locked again, a whole
/// attempt budget back.
#[tokio::test]
async fn the_canary_watches_a_backstopped_req_hourly_and_recovers_it() {
    let s = storage().await;
    let b = seed_broadcast(&s, "aa", LIMIT + 1).await;
    nobody_holds(&s, &b.txid);
    assert_eq!(s.synchronize_transaction_statuses().await.unwrap().len(), 1);
    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 0));
    assert_eq!(output_state(&s, b.input).await, (true, None));

    // Inside the hour: not asked yet.
    MonitorStorage::un_fail(&s).await.unwrap();
    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 0));

    // An hour on, still nobody holds it: re-stamped, still failed.
    age_req(&s, &b.txid, 2).await;
    MonitorStorage::un_fail(&s).await.unwrap();
    assert_eq!(attempts_of(&s, &b.txid).await, ("invalid".to_string(), 1));
    assert_eq!(tx_state(&s, &b.txid).await.0, "failed");
    assert_eq!(output_state(&s, b.change).await, (false, None));

    // Another hour on the network has it after all.
    age_req(&s, &b.txid, 2).await;
    let mock = MockWalletServicesBuilder::default()
        .get_status_for_txids_response(status(TxStatusDetail::new(&b.txid, "mined", Some(1))))
        .is_utxo_response(MockResponse::Success(true))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(mock));
    MonitorStorage::un_fail(&s).await.unwrap();

    assert_eq!(attempts_of(&s, &b.txid).await, ("unmined".to_string(), 0));
    assert_eq!(tx_state(&s, &b.txid).await.0, "unproven");
    assert_eq!(output_state(&s, b.change).await, (true, None));
    assert_eq!(
        output_state(&s, b.input).await,
        (false, Some(b.transaction_id))
    );
}

/// One attempt per processed header, as the reference counts only the
/// header-triggered run: a second pass at the same gate (an Arcade MINED
/// word, the fallback timer, a `tick`) asks but counts nothing.
#[tokio::test]
async fn a_second_pass_at_the_same_header_counts_nothing() {
    let s = storage().await;
    let b = seed_broadcast(&s, "ab", 0).await;
    nobody_holds(&s, &b.txid);

    s.synchronize_transaction_statuses().await.unwrap();
    s.synchronize_transaction_statuses().await.unwrap();
    assert_eq!(attempts_of(&s, &b.txid).await, ("unmined".to_string(), 1));

    open_gate(&s, 1001).await;
    s.synchronize_transaction_statuses().await.unwrap();
    assert_eq!(attempts_of(&s, &b.txid).await, ("unmined".to_string(), 2));
}
