//! The reorg below the user (the stack's rulings 5 and 7 of 2026-10-20;
//! bsv-stack-lean `corpus/scenarios/2026-10-20-reorg-below-the-user.md`).
//! Each test names the ruling and the draft's step it replays: F, mined at
//! 100, gives the coin T spends; T is mined at 101 in block A1 and its proof
//! stored; the reorg deactivates A1.

use std::sync::Arc;

use bsv_rs::transaction::{Beef, MockChainTracker};
use chrono::Utc;
use sqlx::Row;

use super::create_action::{
    allocate_change_input, beef_bfs_walk, find_or_insert_output_basket, rebuild_beef_for_broadcast,
};
use super::reorg_tests::{
    child_spending, insert_proven, insert_req, mark_checked, open_gate, proven_row, seed_user,
    single_leaf_bump, storage, tx_state, txid_of, COINBASE_HEX, COINBASE_TXID,
};
use super::StorageSqlx;
use crate::monitor::reorg_ops::{reprove_block_hash, LostProofReask, ReproveTally};
use crate::monitor::tasks::ReviewProvenTxsTask;
use crate::services::mock::{MockResponse, MockWalletServices, MockWalletServicesBuilder};
use crate::services::traits::{GetMerklePathResult, GetRawTxResult};
use crate::services::{BlockHeader, BroadcastStatus, WalletServices};
use crate::storage::traits::WalletStorageProvider;
use crate::storage::{MonitorStorage, ProofIngestOutcome};

const F_HEIGHT: u32 = 100;
const H: u32 = 101;

fn a1() -> String {
    "a1".repeat(32)
}

/// What the fixture holds: T and the ids of its input and its change.
struct Fixture {
    t: String,
    t_raw: Vec<u8>,
    user_id: i64,
    t_id: i64,
    basket_id: i64,
    input: i64,
    change: i64,
}

/// F proven at 100, T spending F:0 proven at 101 in A1 (both checked), T
/// `completed` with its request, F:0 allocated to T, T:0 spendable change.
async fn seed(s: &StorageSqlx) -> Fixture {
    let (user_id, _) = seed_user(s).await;
    let f_raw = hex::decode(COINBASE_HEX).unwrap();
    let t_raw = child_spending(COINBASE_TXID);
    let t = txid_of(&t_raw);
    let f_proof = insert_proven(
        s,
        COINBASE_TXID,
        F_HEIGHT,
        &"f0".repeat(32),
        &f_raw,
        &single_leaf_bump(F_HEIGHT, COINBASE_TXID),
    )
    .await;
    mark_checked(s, COINBASE_TXID, F_HEIGHT).await;
    let t_proof = insert_proven(s, &t, H, &a1(), &t_raw, &single_leaf_bump(H, &t)).await;
    mark_checked(s, &t, H).await;
    insert_req(s, &t, &t_raw, "completed", 3, Some(t_proof), Utc::now()).await;
    let mut ids = Vec::new();
    for (txid, proof, raw, outgoing) in [
        (COINBASE_TXID, f_proof, &f_raw, false),
        (t.as_str(), t_proof, &t_raw, true),
    ] {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, proven_tx_id, raw_tx, is_outgoing, created_at, updated_at) \
             VALUES (?, ?, 'completed', ?, 'reorg test tx', 0, 1, 0, ?, ?, ?, ?, ?) RETURNING transaction_id",
        )
        .bind(user_id)
        .bind(txid)
        .bind(format!("ref-{}", &txid[..8]))
        .bind(proof)
        .bind(raw)
        .bind(outgoing)
        .bind(Utc::now())
        .bind(Utc::now())
        .fetch_one(s.writer())
        .await
        .unwrap();
        ids.push(id);
    }
    let (f_id, t_id) = (ids[0], ids[1]);
    let basket_id = {
        let mut conn = s.pool().acquire().await.unwrap();
        find_or_insert_output_basket(&mut conn, user_id, "default")
            .await
            .unwrap()
            .basket_id
    };
    let mut outputs = Vec::new();
    for (owner, txid, spendable, spent_by) in [
        (f_id, COINBASE_TXID, false, Some(t_id)),
        (t_id, t.as_str(), true, None),
    ] {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, spent_by, locking_script, derivation_prefix, derivation_suffix, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 1, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, X'76a914000000000000000000000000000000000000000088ac', 'prefix', 'suffix', ?, ?) RETURNING output_id",
        )
        .bind(user_id)
        .bind(owner)
        .bind(basket_id)
        .bind(spendable)
        .bind(txid)
        .bind(spent_by)
        .bind(Utc::now())
        .bind(Utc::now())
        .fetch_one(s.writer())
        .await
        .unwrap();
        outputs.push(id);
    }
    Fixture {
        t,
        t_raw,
        user_id,
        t_id,
        basket_id,
        input: outputs[0],
        change: outputs[1],
    }
}

fn header(height: u32, merkle_root: &str) -> BlockHeader {
    BlockHeader {
        version: 1,
        previous_hash: "0".repeat(64),
        merkle_root: merkle_root.to_string(),
        time: 1_700_000_000,
        bits: 0x207f_ffff,
        nonce: 0,
        hash: format!("{height:064x}"),
        height,
    }
}

/// The wallet's own headers after the reorg: F's root at 100, and at 101
/// either T's root (a reorg that keeps it) or B1's (a reorg that loses it).
/// The storage's tracker reads the same headers.
async fn after_reorg(
    s: &StorageSqlx,
    keeps_root: bool,
    builder: MockWalletServicesBuilder,
    t: &str,
) -> Arc<MockWalletServices> {
    let mock = Arc::new(builder.height(1000).build());
    mock.set_header_for_height(header(F_HEIGHT, COINBASE_TXID));
    let root_at_h = if keeps_root {
        t.to_string()
    } else {
        "b1".repeat(32)
    };
    mock.set_header_for_height(header(H, &root_at_h));
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(F_HEIGHT, COINBASE_TXID.to_string());
    tracker.add_root(H, root_at_h);
    s.set_chain_tracker(Arc::new(tracker)).await;
    WalletStorageProvider::set_services(s, mock.clone() as Arc<dyn WalletServices>);
    mock
}

fn statuses(status: BroadcastStatus) -> MockWalletServicesBuilder {
    MockWalletServicesBuilder::default().broadcaster_statuses(vec![("ARC".to_string(), status)])
}

async fn output_state(s: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
    let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (row.get("spendable"), row.get("spent_by"))
}

async fn req_status(s: &StorageSqlx, txid: &str) -> String {
    sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap()
}

async fn notes(s: &StorageSqlx, txid: &str) -> serde_json::Value {
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    serde_json::from_str(&history).unwrap()
}

fn noted(history: &serde_json::Value, what: &str) -> Vec<serde_json::Value> {
    history["notes"]
        .as_array()
        .map(|n| n.iter().filter(|x| x["what"] == what).cloned().collect())
        .unwrap_or_default()
}

/// Does coin selection offer `f`'s change to a new spender?
async fn change_is_offered(s: &StorageSqlx, f: &Fixture) -> bool {
    let spender: i64 = sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, version, lock_time, created_at, updated_at) VALUES (?, 'unsigned', ?, 1, 0, 'T2', 1, 0, ?, ?) RETURNING transaction_id",
    )
    .bind(f.user_id)
    .bind(format!("t2-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0)))
    .bind(Utc::now())
    .bind(Utc::now())
    .fetch_one(s.writer())
    .await
    .unwrap();
    let mut conn = s.writer().acquire().await.unwrap();
    let picked = allocate_change_input(&mut conn, f.user_id, f.basket_id, spender, 500, true)
        .await
        .unwrap();
    let held_by: Option<i64> =
        sqlx::query_scalar("SELECT spent_by FROM outputs WHERE output_id = ?")
            .bind(f.change)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    // Leave the coin as it was for the assertions after this one.
    sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL WHERE output_id = ?")
        .bind(f.change)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM transactions WHERE transaction_id = ?")
        .bind(spender)
        .execute(&mut *conn)
        .await
        .unwrap();
    picked.is_some() && held_by == Some(spender)
}

/// Ruling 5, the draft's step 3 with the root kept: A1 is deactivated but
/// the active header at 101 still carries T's root. One header read; no
/// proof fetch, no courier read, nothing moves, and the header is not
/// asked again.
#[tokio::test]
async fn ruling_5_step_3_a_reorg_that_keeps_the_root_moves_nothing() {
    let s = storage().await;
    let f = seed(&s).await;
    let mock = after_reorg(&s, true, statuses(BroadcastStatus::Unknown), &f.t).await;

    let tally = reprove_block_hash(&s, mock.as_ref(), &a1(), H).await;

    assert_eq!(
        tally,
        ReproveTally {
            unchanged: 1,
            ..Default::default()
        }
    );
    assert!(!tally.wants_retry());
    assert_eq!(mock.call_count("get_header_for_height"), 1);
    assert_eq!(mock.call_count("get_merkle_path"), 0, "no proof fetch");
    assert_eq!(mock.call_count("get_broadcaster_statuses"), 0);
    assert_eq!(mock.call_count("post_beef"), 0);
    assert!(proven_row(&s, &f.t).await.is_some(), "the proof is kept");
    assert_eq!(tx_state(&s, &f.t).await.0, "completed");
}

/// Rulings 5, 7a, 7b and 7d, the draft's steps 3, 4 (ii) and 5: A1's root is
/// gone from our headers and no provider serves another path. T is
/// `unproven` again ("confirming"), F:0 stays allocated to T, T's change is
/// still offered to a new spender, nothing is failed or released; no
/// broadcaster knows T, so it is announced once, and only once in the
/// episode.
#[tokio::test]
async fn rulings_5_7a_7b_7d_steps_3_to_5_a_lost_root_keeps_the_coin_and_the_action_confirming() {
    let arc = Arc::new(storage().await);
    let s: &StorageSqlx = arc.as_ref();
    let f = seed(s).await;
    let mock = after_reorg(
        s,
        false,
        statuses(BroadcastStatus::Unknown).post_beef_success(),
        &f.t,
    )
    .await;

    let tally = reprove_block_hash(s, mock.as_ref(), &a1(), H).await;

    assert_eq!((tally.demoted, tally.announced), (1, 1), "{tally:?}");
    assert!(!tally.wants_retry());
    assert!(proven_row(s, &f.t).await.is_none());
    assert_eq!(tx_state(s, &f.t).await.0, "unproven", "7b: confirming");
    assert_eq!(req_status(s, &f.t).await, "unmined");
    assert_eq!(
        output_state(s, f.input).await,
        (false, Some(f.t_id)),
        "7b: the input stays allocated to T, nothing released"
    );
    assert_eq!(output_state(s, f.change).await, (true, None));
    assert!(change_is_offered(s, &f).await, "7a: step 5, C1 offered");
    let failed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status = 'failed'")
            .fetch_one(s.pool())
            .await
            .unwrap();
    assert_eq!(failed, 0, "7b: nothing is marked failed");
    assert_eq!(
        mock.call_count("get_broadcaster_statuses"),
        1,
        "7d: one read"
    );
    assert_eq!(mock.call_count("post_beef"), 1, "7d: one announce");
    assert_eq!(noted(&notes(s, &f.t).await, "lostProofAnnounce").len(), 1);

    // The same header again, and the review's net: the proof row is gone,
    // the episode is under way, no second announce.
    reprove_block_hash(s, mock.as_ref(), &a1(), H).await;
    ReviewProvenTxsTask::new(arc.clone(), mock.clone())
        .review(H)
        .await
        .unwrap();
    assert_eq!(mock.call_count("post_beef"), 1, "7d: never a second");

    // Step 7: T is mined again in B3 at 103; its new path checks.
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(F_HEIGHT, COINBASE_TXID.to_string());
    tracker.add_root(103, f.t.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(s, 1000).await;
    let ingested = s
        .ingest_push_proof(&f.t, &single_leaf_bump(103, &f.t), 103, &"b3".repeat(32))
        .await
        .unwrap();
    assert!(
        matches!(ingested, Some(ProofIngestOutcome::Ingested(_))),
        "{ingested:?}"
    );
    assert_eq!(tx_state(s, &f.t).await.0, "completed", "confirmed again");
    assert_eq!(output_state(s, f.input).await, (false, Some(f.t_id)));
}

/// Ruling 7d, the draft's step 4 (i): a broadcaster knows T. The status
/// read ends the matter; nothing is announced.
#[tokio::test]
async fn ruling_7d_step_4i_a_broadcaster_that_knows_it_is_not_told_again() {
    let s = storage().await;
    let f = seed(&s).await;
    let mock = after_reorg(
        &s,
        false,
        statuses(BroadcastStatus::Seen).post_beef_success(),
        &f.t,
    )
    .await;

    let tally = reprove_block_hash(&s, mock.as_ref(), &a1(), H).await;

    assert_eq!((tally.demoted, tally.announced), (1, 0));
    assert_eq!(mock.call_count("get_broadcaster_statuses"), 1);
    assert_eq!(mock.call_count("post_beef"), 0, "7d: no announce");
    assert_eq!(tx_state(&s, &f.t).await.0, "unproven");
}

/// Ruling 7d, a refusal at the status read: written on the request's
/// history, never answered with an announce, and no status moves.
#[tokio::test]
async fn ruling_7d_step_4_a_refusal_is_written_and_never_announced() {
    let s = storage().await;
    let f = seed(&s).await;
    let mock = after_reorg(
        &s,
        false,
        statuses(BroadcastStatus::Rejected).post_beef_success(),
        &f.t,
    )
    .await;

    let anchors = s.find_proven_txs_by_block_hash(&a1()).await.unwrap();
    let outcome =
        crate::monitor::reorg_ops::reprove_moved_anchor(&s, mock.as_ref(), &anchors[0]).await;

    assert_eq!(
        outcome,
        crate::monitor::reorg_ops::ReproveOutcome::Demoted {
            reask: LostProofReask::Refused
        }
    );
    assert_eq!(mock.call_count("post_beef"), 0);
    assert_eq!(noted(&notes(&s, &f.t).await, "lostProofRefusal").len(), 1);
    assert_eq!(
        tx_state(&s, &f.t).await.0,
        "unproven",
        "a refusal fails nothing"
    );
    assert_eq!(output_state(&s, f.input).await, (false, Some(f.t_id)));
}

/// Rulings 7a and 7b, the draft's step 8 (C): the one announce draws a
/// double-spend word naming X, which queues X and fails nothing; X's own
/// checked proof is the conflict: T failed, its change unspendable, F:0 not
/// released to a new spender.
#[tokio::test]
async fn rulings_7a_7b_step_8_only_a_competitors_checked_proof_fails_the_action() {
    let s = storage().await;
    let f = seed(&s).await;
    let mut x_raw = child_spending(COINBASE_TXID);
    let value_at = 4 + 1 + 36 + 1 + 4 + 1;
    x_raw[value_at..value_at + 8].copy_from_slice(&777u64.to_le_bytes());
    let x = txid_of(&x_raw);
    let mock = after_reorg(
        &s,
        false,
        statuses(BroadcastStatus::Unknown).post_beef_double_spend(&f.t, &x),
        &f.t,
    )
    .await;

    let tally = reprove_block_hash(&s, mock.as_ref(), &a1(), H).await;
    assert_eq!(tally.demoted, 1, "{tally:?}");
    assert_eq!(
        tx_state(&s, &f.t).await.0,
        "unproven",
        "the word fails nothing"
    );
    assert_eq!(output_state(&s, f.change).await, (true, None));
    let queued = notes(&s, &f.t).await["competitorsQueued"].clone();
    assert_eq!(queued, serde_json::json!([x.clone()]));

    // X mined in B1 at 101, its path checks against our headers.
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(F_HEIGHT, COINBASE_TXID.to_string());
    tracker.add_root(H, x.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    let serving = MockWalletServicesBuilder::default()
        .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
            name: "mock".to_string(),
            txid: x.clone(),
            raw_tx: Some(x_raw.clone()),
            error: None,
            could_not_look: false,
        }))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("mock".to_string()),
            merkle_path: Some(hex::encode(single_leaf_bump(H, &x))),
            header: None,
            error: None,
            notes: vec![],
        }))
        .is_utxo_response(MockResponse::Success(false))
        .build();
    WalletStorageProvider::set_services(&s, Arc::new(serving) as Arc<dyn WalletServices>);
    s.synchronize_transaction_statuses().await.unwrap();

    assert_eq!(tx_state(&s, &f.t).await.0, "failed", "the conflict");
    assert_eq!(
        output_state(&s, f.change).await,
        (false, None),
        "T's change is unspendable"
    );
    assert_eq!(
        output_state(&s, f.input).await,
        (false, Some(f.t_id)),
        "F:0, spent by X, is not offered again"
    );
}

/// Ruling 7c, the draft's step 6: with A1's stored path still in storage
/// and checked at the time, a BEEF built by a caller that passes no tracker
/// (the broadcast rebuild) carries T as raw bytes and F with its path,
/// never A1's path. The reference merges it as stored
/// (getBeefForTransaction.ts:103-105, 270-272).
#[tokio::test]
async fn ruling_7c_step_6_the_beef_carries_the_stale_ancestor_as_bytes() {
    let s = storage().await;
    let f = seed(&s).await;
    let _mock = after_reorg(&s, false, statuses(BroadcastStatus::Unknown), &f.t).await;
    assert!(proven_row(&s, &f.t).await.is_some(), "A1's path is stored");

    let mut conn = s.pool().acquire().await.unwrap();
    let beef =
        rebuild_beef_for_broadcast(&mut conn, s.writer(), std::slice::from_ref(&f.t), Some(&s))
            .await
            .unwrap();
    let t = beef.find_txid(&f.t).expect("T rides in the BEEF");
    assert!(t.bump_index().is_none(), "T as bytes, no BUMP");
    assert_eq!(t.raw_tx().map(<[u8]>::to_vec), Some(f.t_raw.clone()));
    let parent = beef.find_txid(COINBASE_TXID).expect("F is walked");
    assert!(parent.bump_index().is_some(), "F with its path at 100");
    assert!(
        beef.bumps.iter().all(|b| b.block_height != H),
        "never A1's path"
    );

    // The same with the walk itself and no tracker from its caller.
    let mut beef = Beef::new();
    let mut pending = vec![(f.t.clone(), 0usize)];
    let mut processed = std::collections::HashSet::new();
    beef_bfs_walk(
        &mut conn,
        s.writer(),
        &mut beef,
        &mut pending,
        &mut processed,
        Some(&s),
        None,
    )
    .await
    .unwrap();
    assert!(beef.find_txid(&f.t).unwrap().bump_index().is_none());
    assert!(beef.bumps.iter().all(|b| b.block_height != H));
    drop(conn);
    assert!(
        proven_row(&s, &f.t).await.is_some(),
        "the walk mutates no checked row"
    );
}

/// Ruling 7c's control: when the headers carry the stored root, the stored
/// path is merged and ends the walk.
#[tokio::test]
async fn ruling_7c_control_a_kept_root_merges_the_stored_path() {
    let s = storage().await;
    let f = seed(&s).await;
    let _mock = after_reorg(&s, true, statuses(BroadcastStatus::Unknown), &f.t).await;

    let mut conn = s.pool().acquire().await.unwrap();
    let beef =
        rebuild_beef_for_broadcast(&mut conn, s.writer(), std::slice::from_ref(&f.t), Some(&s))
            .await
            .unwrap();
    assert!(beef.find_txid(&f.t).unwrap().bump_index().is_some());
    assert!(
        beef.find_txid(COINBASE_TXID).is_none(),
        "the path ends the walk"
    );
}
