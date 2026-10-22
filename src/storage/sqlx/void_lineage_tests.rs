//! The void lineage (bsv-wallet-toolbox-rs #42; the reading is
//! bsv-stack-lean `docs/p0/toolbox-void-lineage.md`, sections 3 to 6 and 9):
//! a parent P spending coin C, a competitor R spending C, our child D
//! spending P's change. What the store does with P, its change and D under
//! the courier words of bsv-low #818 and #831 (an orphan-mempool word, a
//! REJECTED word, a status source that answers "known") and under R's
//! proof. The five witnesses of the reading, ported; each fix of #42 turned
//! its witness from a statement of 0.11.0 into a test of the rule.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bsv_rs::transaction::{Beef, MockChainTracker};
use chrono::{Duration, Utc};
use sqlx::Row;

use super::create_action::{allocate_change_input, find_or_insert_output_basket};
use super::reorg_tests::{
    child_spending, open_gate, seed_user, single_leaf_bump, storage, txid_of, COINBASE_HEX,
    COINBASE_TXID,
};
use super::StorageSqlx;
use crate::monitor::tasks::ArcadeEventsTask;
use crate::services::mock::{MockResponse, MockWalletServices, MockWalletServicesBuilder};
use crate::services::providers::arcade::{
    status_detail, statuses, ArcadeStatusEvent, ArcadeTxInfo,
};
use crate::services::traits::{
    GetMerklePathResult, GetRawTxResult, GetStatusForTxidsResult, PostBeefResult,
    PostTxResultForTxid,
};
use crate::services::{TxStatusDetail, WalletServices};
use crate::storage::traits::WalletStorageProvider;
use crate::storage::MonitorStorage;

fn competitor_spending(parent_txid: &str) -> Vec<u8> {
    let mut raw = child_spending(parent_txid);
    let value_at = 4 + 1 + 36 + 1 + 4 + 1;
    raw[value_at..value_at + 8].copy_from_slice(&777u64.to_le_bytes());
    raw
}

struct Lineage {
    user_id: i64,
    basket_id: i64,
    p: String,
    p_id: i64,
    c: i64,
    p0: i64,
    d: Option<(String, i64, i64)>,
}

async fn insert_tx(s: &StorageSqlx, user_id: i64, txid: &str, status: &str, raw: &[u8]) -> i64 {
    let old = Utc::now() - Duration::days(2);
    sqlx::query_scalar(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'void lineage', 0, 1, 0, ?, 1, ?, ?) RETURNING transaction_id",
    )
    .bind(user_id)
    .bind(txid)
    .bind(status)
    .bind(format!("ref-{}", &txid[..8]))
    .bind(raw)
    .bind(old)
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

async fn insert_change(
    s: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    owner: i64,
    txid: &str,
    spendable: bool,
    spent_by: Option<i64>,
) -> i64 {
    let old = Utc::now() - Duration::days(2);
    sqlx::query_scalar(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, spent_by, locking_script, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 1, 0, 1000, 'storage', 'change', 'P2PKH', ?, ?, x'51', ?, ?) RETURNING output_id",
    )
    .bind(user_id)
    .bind(owner)
    .bind(basket_id)
    .bind(spendable)
    .bind(txid)
    .bind(spent_by)
    .bind(old)
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

async fn insert_req(s: &StorageSqlx, txid: &str, status: &str, raw: &[u8], beef: &mut Beef) {
    let old = Utc::now() - Duration::days(2);
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, ?, 0, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(txid)
    .bind(status)
    .bind(raw)
    .bind(beef.to_binary())
    .bind(old)
    .bind(old)
    .execute(s.writer())
    .await
    .unwrap();
}

/// C (a mined coin) spent by P (`sending`, req `unsent`); P's change P0
/// spendable. With `child`, D (in `child` status, req `unmined`) spends P0.
async fn seed(s: &StorageSqlx, child: Option<&str>) -> Lineage {
    let (user_id, _) = seed_user(s).await;
    let basket_id = {
        let mut conn = s.writer().acquire().await.unwrap();
        find_or_insert_output_basket(&mut conn, user_id, "default")
            .await
            .unwrap()
            .basket_id
    };
    let coinbase = hex::decode(COINBASE_HEX).unwrap();
    let p_raw = child_spending(COINBASE_TXID);
    let p = txid_of(&p_raw);
    let cb_id = insert_tx(s, user_id, COINBASE_TXID, "completed", &coinbase).await;
    let p_id = insert_tx(s, user_id, &p, "sending", &p_raw).await;
    let c = insert_change(
        s,
        user_id,
        basket_id,
        cb_id,
        COINBASE_TXID,
        false,
        Some(p_id),
    )
    .await;
    let mut beef = Beef::new();
    beef.merge_raw_tx(coinbase, None);
    insert_req(s, &p, "unsent", &p_raw, &mut beef).await;
    let mut d = None;
    let p0;
    if let Some(status) = child {
        let d_raw = child_spending(&p);
        let d_txid = txid_of(&d_raw);
        let d_id = insert_tx(s, user_id, &d_txid, status, &d_raw).await;
        p0 = insert_change(s, user_id, basket_id, p_id, &p, false, Some(d_id)).await;
        let d0 = insert_change(s, user_id, basket_id, d_id, &d_txid, true, None).await;
        let mut d_beef = Beef::new();
        d_beef.merge_raw_tx(p_raw.clone(), None);
        insert_req(s, &d_txid, "unmined", &d_raw, &mut d_beef).await;
        d = Some((d_txid, d_id, d0));
    } else {
        p0 = insert_change(s, user_id, basket_id, p_id, &p, true, None).await;
    }
    Lineage {
        user_id,
        basket_id,
        p,
        p_id,
        c,
        p0,
        d,
    }
}

fn courier(name: &str, txid: &str, status: &str, orphan: bool) -> PostBeefResult {
    PostBeefResult {
        name: name.to_string(),
        status: "error".to_string(),
        txid_results: vec![PostTxResultForTxid {
            txid: txid.to_string(),
            status: status.to_string(),
            double_spend: false,
            competing_txs: None,
            data: Some(status.to_string()),
            orphan_mempool: orphan,
            service_error: false,
            block_hash: None,
            block_height: None,
            notes: vec![],
        }],
        error: Some(status.to_string()),
        notes: vec![],
    }
}

fn status_source(txid: &str, status: &str) -> MockResponse<GetStatusForTxidsResult> {
    MockResponse::Success(GetStatusForTxidsResult {
        name: "ArcadeV2".to_string(),
        status: "success".to_string(),
        error: None,
        results: vec![TxStatusDetail {
            txid: txid.to_string(),
            status: status.to_string(),
            depth: if status == "known" { Some(0) } else { None },
            ..Default::default()
        }],
    })
}

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

fn set(s: &StorageSqlx, builder: MockWalletServicesBuilder) -> Arc<MockWalletServices> {
    let mock = Arc::new(builder.build());
    WalletStorageProvider::set_services(s, mock.clone() as Arc<dyn WalletServices>);
    mock
}

/// Arcade's pushed REJECTED 466 for `txid`, naming `competitor`: the
/// competitor is queued for the proof ask.
async fn apply_rejected_naming(s: &StorageSqlx, txid: &str, competitor: &str) {
    let ev = ArcadeStatusEvent {
        txid: txid.to_string(),
        tx_status: statuses::REJECTED.to_string(),
        timestamp: None,
        block_hash: None,
        block_height: None,
        merkle_path: None,
        extra_info: None,
        status_code: Some(466),
        competing_txs: Some(vec![competitor.to_string()]),
        event_id: None,
    };
    ArcadeEventsTask::<StorageSqlx>::apply_event(s, &ev, &AtomicBool::new(false))
        .await
        .unwrap();
}

async fn words(s: &StorageSqlx, txid: &str) -> (String, String) {
    let t: String = sqlx::query_scalar("SELECT status FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    let r: String = sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (t, r)
}

async fn output(s: &StorageSqlx, output_id: i64) -> (bool, Option<i64>) {
    let row = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
        .bind(output_id)
        .fetch_one(s.pool())
        .await
        .unwrap();
    (row.get("spendable"), row.get("spent_by"))
}

/// The coin selector's pick for a new action (createAction's allocator).
async fn pick(s: &StorageSqlx, l: &Lineage) -> Option<String> {
    let spender: i64 = sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, version, lock_time, created_at, updated_at) \
         VALUES (?, 'unsigned', ?, 1, 0, 'next action', 1, 0, ?, ?) RETURNING transaction_id",
    )
    .bind(l.user_id)
    .bind(format!("next-{}", Utc::now().timestamp_nanos_opt().unwrap()))
    .bind(Utc::now())
    .bind(Utc::now())
    .fetch_one(s.writer())
    .await
    .unwrap();
    let mut conn = s.writer().acquire().await.unwrap();
    allocate_change_input(&mut conn, l.user_id, l.basket_id, spender, 500, true)
        .await
        .unwrap()
        .map(|a| format!("{a:?}"))
}

/// The spendable set as `listOutputs` reads it: `spendable = 1` and the
/// creator in `completed`, `unproven`, `nosend` or `sending`.
async fn listed(s: &StorageSqlx, l: &Lineage) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT o.txid FROM outputs o JOIN transactions t ON o.transaction_id = t.transaction_id \
         WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ('completed', 'unproven', 'nosend', 'sending')",
    )
    .bind(l.user_id)
    .fetch_all(s.pool())
    .await
    .unwrap()
}

/// Step 2 alone (#818's shape): ARC answers P SEEN_IN_ORPHAN_MEMPOOL and no
/// status source holds it. P stays `sending`, C stays locked, P0 stays
/// `spendable = 1` (listed) and the allocator passes it over (`sending`).
#[tokio::test]
async fn an_orphan_word_keeps_the_parent_sending_and_its_change_listed_not_picked() {
    let s = storage().await;
    let l = seed(&s, None).await;
    set(
        &s,
        MockWalletServicesBuilder::default()
            .post_beef_response(MockResponse::Success(vec![courier(
                "ARC", &l.p, "error", true,
            )]))
            .get_status_for_txids_response(status_source(&l.p, "unknown")),
    );
    s.send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(words(&s, &l.p).await, ("sending".into(), "unsent".into()));
    assert_eq!(
        output(&s, l.c).await,
        (false, Some(l.p_id)),
        "C locked to P"
    );
    assert_eq!(output(&s, l.p0).await, (true, None), "P0 spendable = 1");
    assert_eq!(listed(&s, &l).await, vec![l.p.clone()], "P0 listed");
    assert_eq!(
        pick(&s, &l).await,
        None,
        "the allocator passes a sending creator"
    );
}

/// ARC answers SEEN_IN_ORPHAN_MEMPOOL, Arcade REJECTED, and a status source
/// answers "known": the status source holds P, so P is promoted to
/// `unproven` and the allocator then funds the next action from P0. Since
/// #42 point 2 a status source's "known" is a network word (Arcade's
/// `SEEN_*` and `STUMP_PROCESSING`); at 0.11.0 Arcade's RECEIVED answered it
/// too (#831's shape, `arcade_received_alone_promotes_nothing`).
#[tokio::test]
async fn a_known_from_one_status_source_promotes_a_refused_parent_and_its_change_funds_the_next_action(
) {
    let s = storage().await;
    let l = seed(&s, None).await;
    set(
        &s,
        MockWalletServicesBuilder::default()
            .post_beef_response(MockResponse::Success(vec![
                courier("ARC", &l.p, "error", true),
                courier("ArcadeV2", &l.p, "rejected", false),
            ]))
            .get_status_for_txids_response(status_source(&l.p, "known")),
    );
    s.send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(words(&s, &l.p).await, ("unproven".into(), "unmined".into()));
    assert!(
        pick(&s, &l).await.unwrap().contains(&l.p),
        "P0 funds the next action"
    );
}

/// Arcade's status document for `txid` carrying `word`, read through the
/// provider's own mapping (`status_detail`), as the one status source.
fn arcade_status(txid: &str, word: &str) -> MockResponse<GetStatusForTxidsResult> {
    let info: ArcadeTxInfo =
        serde_json::from_value(serde_json::json!({ "txid": txid, "txStatus": word })).unwrap();
    MockResponse::Success(GetStatusForTxidsResult {
        name: "ArcadeV2".to_string(),
        status: "success".to_string(),
        error: None,
        results: vec![status_detail(txid, &info)],
    })
}

/// #831's root (#42 point 2): ARC answers SEEN_IN_ORPHAN_MEMPOOL and Arcade's
/// status document for P says RECEIVED (Arcade holds it, the network may
/// not). A holding word is a hint: P stays `sending` / `unsent`, C locked,
/// and P0 funds nothing. Each of the three holding words, one store each.
/// At 0.11.0 RECEIVED answered "known" and P was promoted to `unproven`.
#[tokio::test]
async fn arcade_received_alone_promotes_nothing() {
    for word in [
        statuses::RECEIVED,
        statuses::SENT_TO_NETWORK,
        statuses::ACCEPTED_BY_NETWORK,
    ] {
        let s = storage().await;
        let l = seed(&s, None).await;
        set(
            &s,
            MockWalletServicesBuilder::default()
                .post_beef_response(MockResponse::Success(vec![courier(
                    "ARC", &l.p, "error", true,
                )]))
                .get_status_for_txids_response(arcade_status(&l.p, word)),
        );
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            words(&s, &l.p).await,
            ("sending".into(), "unsent".into()),
            "{word}"
        );
        assert_eq!(
            output(&s, l.c).await,
            (false, Some(l.p_id)),
            "{word}: C locked"
        );
        assert_eq!(pick(&s, &l).await, None, "{word}: P0 funds nothing");
    }
}

/// The network word does promote: Arcade's SEEN_ON_NETWORK (and
/// STUMP_PROCESSING, in a block) for P is a status source holding it.
#[tokio::test]
async fn arcade_seen_on_network_promotes() {
    for word in [statuses::SEEN_ON_NETWORK, statuses::STUMP_PROCESSING] {
        let s = storage().await;
        let l = seed(&s, None).await;
        set(
            &s,
            MockWalletServicesBuilder::default()
                .post_beef_response(MockResponse::Success(vec![courier(
                    "ARC", &l.p, "error", true,
                )]))
                .get_status_for_txids_response(arcade_status(&l.p, word)),
        );
        s.send_waiting_transactions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            words(&s, &l.p).await,
            ("unproven".into(), "unmined".into()),
            "{word}"
        );
    }
}

/// Step 3 (R mined, no courier names R): the proof pass never asks for R; P
/// keeps its word, C stays locked and nothing is written about P or D. A
/// statement of today: #42 point 3 (the orphan re-ask climbs to the
/// topmost unproven ancestor and queues the competitor its answer names)
/// is open, with the tracker's climb.
#[tokio::test]
async fn a_mined_competitor_no_courier_names_is_never_asked_for() {
    let s = storage().await;
    let l = seed(&s, Some("unproven")).await;
    let r = competitor_spending(COINBASE_TXID);
    let r_txid = txid_of(&r);
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, r_txid.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    let mock = set(
        &s,
        serving(&r, Some(single_leaf_bump(900, &r_txid)))
            .get_status_for_txids_response(status_source(&l.p, "unknown")),
    );
    let out = s.synchronize_transaction_statuses().await.unwrap();
    assert!(out.is_empty(), "no word: {out:?}");
    assert!(!mock
        .call_history()
        .iter()
        .any(|c| c.args.iter().any(|a| a.contains(&r_txid))));
    assert_eq!(words(&s, &l.p).await, ("sending".into(), "unsent".into()));
    let (d, _, d0) = l.d.clone().unwrap();
    assert_eq!(words(&s, &d).await, ("unproven".into(), "unmined".into()));
    assert_eq!(output(&s, d0).await, (true, None), "D's change spendable");
    assert!(
        pick(&s, &l).await.unwrap().contains(&d),
        "D's change funds the next action"
    );
}

/// R named (Arcade REJECTED 466 naming R) and R's proof meets our header,
/// while one status source still answers "known" for P: the competitor's
/// checked proof is the word (#42 point 1; at 0.11.0 the `known` outranked
/// it). P `failed` / `doubleSpend`, C kept, D `failed` / `invalid`, D's
/// change dead, and no status source was asked.
#[tokio::test]
async fn a_checked_competitor_proof_outranks_a_known_word() {
    let s = storage().await;
    let l = seed(&s, Some("unproven")).await;
    let r = competitor_spending(COINBASE_TXID);
    let r_txid = txid_of(&r);
    set(&s, MockWalletServicesBuilder::default());
    apply_rejected_naming(&s, &l.p, &r_txid).await;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, r_txid.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    let mock = set(
        &s,
        serving(&r, Some(single_leaf_bump(900, &r_txid)))
            .get_status_for_txids_response(status_source(&l.p, "known")),
    );
    let out = s.synchronize_transaction_statuses().await.unwrap();
    assert!(
        out.iter().any(|o| o.txid == l.p),
        "the double-spend word for P: {out:?}"
    );
    assert_eq!(
        words(&s, &l.p).await,
        ("failed".into(), "doubleSpend".into())
    );
    assert_eq!(
        output(&s, l.c).await,
        (false, Some(l.p_id)),
        "C not released"
    );
    let (d, _, d0) = l.d.clone().unwrap();
    assert_eq!(words(&s, &d).await, ("failed".into(), "invalid".into()));
    assert!(!output(&s, d0).await.0, "D's change dead");
    assert_eq!(
        pick(&s, &l).await,
        None,
        "nothing of the lineage funds an action"
    );
    assert!(
        !mock
            .call_history()
            .iter()
            .any(|c| c.method == "get_status_for_txids" && c.args.iter().any(|a| a.contains(&l.p))),
        "no status source asked about P on the competitor's proof"
    );
}

/// R named and proven, no status source holds P: the evidence path. P
/// `failed` / `doubleSpend`, C kept (spent by R), P's descendants retired:
/// D `failed` / `invalid`, D's change dead.
#[tokio::test]
async fn a_checked_competitor_proof_retires_the_lineage() {
    let s = storage().await;
    let l = seed(&s, Some("unproven")).await;
    let r = competitor_spending(COINBASE_TXID);
    let r_txid = txid_of(&r);
    set(&s, MockWalletServicesBuilder::default());
    apply_rejected_naming(&s, &l.p, &r_txid).await;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(900, r_txid.clone());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    set(
        &s,
        serving(&r, Some(single_leaf_bump(900, &r_txid)))
            .get_status_for_txids_response(status_source(&l.p, "unknown")),
    );
    s.synchronize_transaction_statuses().await.unwrap();
    assert_eq!(
        words(&s, &l.p).await,
        ("failed".into(), "doubleSpend".into())
    );
    assert_eq!(
        output(&s, l.c).await,
        (false, Some(l.p_id)),
        "C not released"
    );
    let (d, _, d0) = l.d.clone().unwrap();
    assert_eq!(words(&s, &d).await, ("failed".into(), "invalid".into()));
    assert!(!output(&s, d0).await.0, "D's change dead");
    assert_eq!(
        pick(&s, &l).await,
        None,
        "nothing of the lineage funds an action"
    );
}
