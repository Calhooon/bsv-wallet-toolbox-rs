//! A broadcaster's definitive word is a hint (bsv-stack-lean #66;
//! `docs/charters/tracker.md` section 2 there): a 465, every other
//! `is_rejection` code and a double-spend word keep the transaction's word
//! and schedule the re-ask. A competitor the word names is queued for a
//! proof ask through the proof path (`synchronize_transaction_statuses`:
//! `get_raw_tx` and `get_merkle_path`, the sources the proof pass asks; no
//! spender lookup), and its proof, checked against our headers, is the one
//! evidence that writes the double-spend word for ours.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bsv_rs::transaction::{Beef, MockChainTracker};
use chrono::{Duration, Utc};
use sqlx::Row;

use super::reorg_tests::{
    child_spending, open_gate, seed_user, single_leaf_bump, storage, txid_of, COINBASE_HEX,
    COINBASE_TXID,
};
use super::StorageSqlx;
use crate::monitor::tasks::ArcadeEventsTask;
use crate::services::mock::{MockResponse, MockWalletServices, MockWalletServicesBuilder};
use crate::services::providers::arcade::{statuses, ArcadeStatusEvent};
use crate::services::traits::{
    GetMerklePathResult, GetRawTxResult, PostBeefResult, PostTxResultForTxid,
};
use crate::services::WalletServices;
use crate::storage::entities::ProvenTxReqStatus;
use crate::storage::traits::WalletStorageProvider;
use crate::storage::MonitorStorage;

/// A transaction spending `parent_txid`:0 that differs from ours by its value.
fn competitor_spending(parent_txid: &str) -> Vec<u8> {
    let mut raw = child_spending(parent_txid);
    // version 4, vin count 1, outpoint 36, script len 1, sequence 4, vout count 1
    let value_at = 4 + 1 + 36 + 1 + 4 + 1;
    raw[value_at..value_at + 8].copy_from_slice(&777u64.to_le_bytes());
    raw
}

/// Our announced transaction: `sending`, its request `unsent` and due for a
/// re-ask, the coin it spends locked by it, its change spendable.
struct Announced {
    txid: String,
    transaction_id: i64,
    input: i64,
    change: i64,
}

async fn seed_announced(s: &StorageSqlx) -> Announced {
    let (user_id, _) = seed_user(s).await;
    let parent_raw = hex::decode(COINBASE_HEX).unwrap();
    let raw = child_spending(COINBASE_TXID);
    let txid = txid_of(&raw);
    let old = Utc::now() - Duration::days(2);
    let mut transaction_ids = Vec::new();
    for (t, status, r, outgoing) in [
        (COINBASE_TXID, "completed", &parent_raw, false),
        (txid.as_str(), "sending", &raw, true),
    ] {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 'hint test tx', 0, 1, 0, ?, ?, ?, ?) RETURNING transaction_id",
        )
        .bind(user_id)
        .bind(t)
        .bind(status)
        .bind(format!("ref-{}", &t[..8]))
        .bind(r)
        .bind(outgoing)
        .bind(old)
        .bind(old)
        .fetch_one(s.pool())
        .await
        .unwrap();
        transaction_ids.push(id);
    }
    let (parent_id, transaction_id) = (transaction_ids[0], transaction_ids[1]);
    let mut outputs = Vec::new();
    for (owner, t, spendable, spent_by) in [
        (parent_id, COINBASE_TXID, false, Some(transaction_id)),
        (transaction_id, txid.as_str(), true, None),
    ] {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, spent_by, locking_script, created_at, updated_at) \
             VALUES (?, ?, ?, 1, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, x'51', ?, ?) RETURNING output_id",
        )
        .bind(user_id)
        .bind(owner)
        .bind(spendable)
        .bind(t)
        .bind(spent_by)
        .bind(old)
        .bind(old)
        .fetch_one(s.pool())
        .await
        .unwrap();
        outputs.push(id);
    }
    let mut beef = Beef::new();
    beef.merge_raw_tx(parent_raw, None);
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, 'unsent', 0, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(&txid)
    .bind(&raw)
    .bind(beef.to_binary())
    .bind(old)
    .bind(old)
    .execute(s.pool())
    .await
    .unwrap();
    Announced {
        txid,
        transaction_id,
        input: outputs[0],
        change: outputs[1],
    }
}

/// One broadcaster's definitive word on `txid`.
fn word(txid: &str, status: &str, double_spend: bool, competitor: Option<&str>) -> PostBeefResult {
    PostBeefResult {
        name: "ARC".to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: status.to_string(),
            double_spend,
            competing_txs: competitor.map(|c| vec![c.to_string()]),
            data: Some(status.to_string()),
            orphan_mempool: false,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: Some(status.to_string()),
        notes: vec![],
    }
}

fn posting(result: PostBeefResult) -> MockWalletServicesBuilder {
    MockWalletServicesBuilder::default()
        .post_beef_response(MockResponse::Success(vec![result]))
        .is_utxo_response(MockResponse::Success(true))
}

fn set(s: &StorageSqlx, builder: MockWalletServicesBuilder) -> Arc<MockWalletServices> {
    let mock = Arc::new(builder.build());
    WalletStorageProvider::set_services(s, mock.clone() as Arc<dyn WalletServices>);
    mock
}

/// The bytes and the proof of `raw` as the proof path's sources serve them.
fn serving(raw: &[u8], proof: Option<Vec<u8>>) -> MockWalletServicesBuilder {
    MockWalletServicesBuilder::default()
        .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
            name: "mock".to_string(),
            txid: txid_of(raw),
            raw_tx: Some(raw.to_vec()),
            error: None,
            could_not_look: false,
        }))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("mock".to_string()),
            merkle_path: proof.map(hex::encode),
            header: None,
            error: None,
            notes: vec![],
        }))
        .is_utxo_response(MockResponse::Success(false))
}

async fn words_of(s: &StorageSqlx, a: &Announced) -> (String, String, i64) {
    let t: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(&a.txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    let row = sqlx::query("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
        .bind(&a.txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (t, row.get("status"), row.get("attempts"))
}

async fn output_state(s: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
    let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (row.get("spendable"), row.get("spent_by"))
}

async fn history_of(s: &StorageSqlx, txid: &str) -> serde_json::Value {
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    serde_json::from_str(&history).unwrap()
}

async fn queued(s: &StorageSqlx, txid: &str) -> Vec<String> {
    history_of(s, txid).await["competitorsQueued"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn age(s: &StorageSqlx, txid: &str) {
    sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
        .bind(Utc::now() - Duration::days(2))
        .bind(txid)
        .execute(s.pool())
        .await
        .unwrap();
}

/// The word kept: announced, re-askable, the input locked, the change not
/// written off.
async fn assert_kept(s: &StorageSqlx, a: &Announced, why: &str) {
    let (t, r, _) = words_of(s, a).await;
    assert_eq!(
        (t.as_str(), r.as_str()),
        ("sending", "unsent"),
        "{why}: the word is kept"
    );
    assert_eq!(
        output_state(s, a.input).await,
        (false, Some(a.transaction_id)),
        "{why}: the input stays locked"
    );
    assert_eq!(
        output_state(s, a.change).await,
        (true, None),
        "{why}: the change is not written off"
    );
}

/// RED at 0.7.3 (`8deedaf`): a 465 naming a competitor took the release
/// rule (`failed`, req `invalid`). GREEN: no word; the competitor is queued
/// and the next proof pass asks the proof path for its bytes and its proof;
/// with no proof served, still no word.
#[tokio::test]
async fn a_465_naming_a_competitor_without_its_proof_writes_no_word_and_queues_it() {
    let s = storage().await;
    let a = seed_announced(&s).await;
    let competitor = competitor_spending(COINBASE_TXID);
    let c = txid_of(&competitor);
    let mock = set(&s, posting(word(&a.txid, "465", false, Some(&c))));

    s.send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(mock.call_count("post_beef"), 1);
    assert_kept(&s, &a, "a 465 naming a competitor").await;
    assert_eq!(queued(&s, &a.txid).await, vec![c.clone()]);
    let notes = history_of(&s, &a.txid).await["notes"].clone();
    assert!(
        notes
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["what"] == "sendWaitingHint" && n["nextReaskMinutes"].is_number()),
        "the re-ask is scheduled: {notes}"
    );

    // The proof pass asks for the competitor; no proof is served.
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, c.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    let mock = set(&s, serving(&competitor, None));
    let out = s.synchronize_transaction_statuses().await.unwrap();

    assert!(out.is_empty(), "no word: {out:?}");
    assert!(mock
        .call_history()
        .iter()
        .any(|call| call.method == "get_merkle_path" && call.args == vec![c.clone()]));
    assert_kept(&s, &a, "a competitor with no proof").await;
    assert_eq!(queued(&s, &a.txid).await, vec![c], "still queued");
}

/// RED at 0.7.3: as above, `failed` at the post. GREEN: the competitor's
/// bytes spend our input and its proof meets our header at its height: the
/// existing double-spend word is written by the existing path (the release
/// rule, `retire_undeliverable_tx`), the input spent by the competitor kept.
#[tokio::test]
async fn a_465_naming_a_competitor_whose_proof_meets_our_header_writes_the_double_spend_word() {
    let s = storage().await;
    let a = seed_announced(&s).await;
    let competitor = competitor_spending(COINBASE_TXID);
    let c = txid_of(&competitor);
    set(&s, posting(word(&a.txid, "465", false, Some(&c))));
    s.send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_kept(&s, &a, "the 465").await;

    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, c.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    set(&s, serving(&competitor, Some(single_leaf_bump(900, &c))));
    let out = s.synchronize_transaction_statuses().await.unwrap();

    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0].txid, a.txid);
    assert_eq!(out[0].status, ProvenTxReqStatus::DoubleSpend);
    let (t, r, _) = words_of(&s, &a).await;
    assert_eq!((t.as_str(), r.as_str()), ("failed", "doubleSpend"));
    assert_eq!(
        output_state(&s, a.input).await,
        (false, Some(a.transaction_id)),
        "the coin the competitor spent is not released"
    );
    assert_eq!(output_state(&s, a.change).await, (false, None));
}

/// A competitor's proof that does not meet our header, and a named
/// competitor whose bytes spend none of our inputs: no word either way.
#[tokio::test]
async fn a_competitor_proof_off_our_headers_or_bytes_that_spend_nothing_of_ours_write_no_word() {
    for case in ["root off our header", "spends nothing of ours"] {
        let s = storage().await;
        let a = seed_announced(&s).await;
        let competitor = if case == "spends nothing of ours" {
            competitor_spending(&"cd".repeat(32))
        } else {
            competitor_spending(COINBASE_TXID)
        };
        let c = txid_of(&competitor);
        set(&s, posting(word(&a.txid, "465", false, Some(&c))));
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();

        let mut tracker = MockChainTracker::new(1000);
        if case != "root off our header" {
            tracker.add_root(900, c.clone());
        }
        s.set_chain_tracker(Arc::new(tracker)).await;
        open_gate(&s, 1000).await;
        set(&s, serving(&competitor, Some(single_leaf_bump(900, &c))));
        let out = s.synchronize_transaction_statuses().await.unwrap();

        assert!(out.is_empty(), "{case}: {out:?}");
        assert_kept(&s, &a, case).await;
    }
}

/// RED at 0.7.3: every `is_rejection` code took the release rule. GREEN:
/// each is a hint; the word is kept and the re-ask is scheduled.
#[tokio::test]
async fn every_rejection_code_is_a_hint_that_keeps_the_word() {
    use crate::services::providers::arc::status_codes::is_rejection;
    let codes: Vec<u16> = (400..=499).filter(|c| is_rejection(*c)).collect();
    assert!(codes.contains(&465) && codes.len() > 10, "{codes:?}");
    for code in codes {
        let s = storage().await;
        let a = seed_announced(&s).await;
        let mock = set(&s, posting(word(&a.txid, &code.to_string(), false, None)));
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(mock.call_count("post_beef"), 1, "{code}");
        assert_kept(&s, &a, &format!("ARC {code}")).await;
        assert!(queued(&s, &a.txid).await.is_empty());
    }
}

/// RED at 0.7.3: a double-spend word naming no competitor (Arcade's
/// DOUBLE_SPEND_ATTEMPTED, and classic ARC's flag) failed the transaction.
/// GREEN: no word; the next re-ask waits for the cadence and then runs.
#[tokio::test]
async fn a_double_spend_word_naming_no_competitor_writes_no_word_and_schedules_the_reask() {
    for status in ["rejected", "error"] {
        let s = storage().await;
        let a = seed_announced(&s).await;
        set(&s, posting(word(&a.txid, status, true, None)));
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_kept(&s, &a, status).await;
        assert_eq!(words_of(&s, &a).await.2, 1);

        // Inside the cadence nothing is posted; past it, the re-ask runs.
        let mock = set(&s, posting(word(&a.txid, status, true, None)));
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            mock.call_count("post_beef"),
            0,
            "{status}: the re-ask waits"
        );
        age(&s, &a.txid).await;
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(mock.call_count("post_beef"), 1, "{status}: the re-ask runs");
        assert_kept(&s, &a, status).await;
        assert_eq!(words_of(&s, &a).await.2, 2);
    }
}

/// RED at 0.7.3: Arcade's pushed DOUBLE_SPEND_ATTEMPTED (and a REJECTED
/// 465) marked the transaction `failed`. GREEN: a hint; the word is kept, the
/// named competitor is queued, the refusing broadcaster remembered.
#[tokio::test]
async fn an_arcade_double_spend_or_rejected_event_is_a_hint() {
    for (status, code, competitor) in [
        (statuses::DOUBLE_SPEND_ATTEMPTED, None, true),
        (statuses::REJECTED, Some(466), true),
        (statuses::REJECTED, Some(465), false),
    ] {
        let s = storage().await;
        let a = seed_announced(&s).await;
        set(&s, MockWalletServicesBuilder::default());
        let c = txid_of(&competitor_spending(COINBASE_TXID));
        let ev = ArcadeStatusEvent {
            txid: a.txid.clone(),
            tx_status: status.to_string(),
            timestamp: None,
            block_hash: None,
            block_height: None,
            merkle_path: None,
            extra_info: None,
            status_code: code,
            competing_txs: competitor.then(|| vec![c.clone()]),
            event_id: None,
        };
        let applied =
            ArcadeEventsTask::<StorageSqlx>::apply_event(&s, &ev, &AtomicBool::new(false))
                .await
                .unwrap();

        assert!(!applied, "{status} {code:?}: no word applied");
        let (t, r, _) = words_of(&s, &a).await;
        assert_eq!((t.as_str(), r.as_str()), ("sending", "unsent"), "{status}");
        assert_eq!(output_state(&s, a.change).await, (true, None), "{status}");
        let expected: Vec<String> = if competitor { vec![c] } else { vec![] };
        assert_eq!(queued(&s, &a.txid).await, expected, "{status}");
    }
}
