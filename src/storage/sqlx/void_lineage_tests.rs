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

use super::create_action::{
    allocate_change_input, find_or_insert_output_basket, rebuild_beef_for_broadcast,
};
use super::reorg_tests::{
    child_spending, insert_proven, mark_checked, open_gate, seed_user, single_leaf_bump, storage,
    txid_of, COINBASE_HEX, COINBASE_TXID,
};
use super::{RetireOutcome, StorageSqlx};
use crate::error::Error;
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
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
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
/// D `failed` / `invalid`, D's change dead. Then the BEEF builder over D
/// refuses the lineage.
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

    // The BEEF builder over D (a pre-signed refund spending D's change)
    // refuses the dead lineage (#42 point 4; at 0.11.0 it assembled D and
    // P's bytes and the post went out).
    let mut conn = s.pool().acquire().await.unwrap();
    let refused =
        rebuild_beef_for_broadcast(&mut conn, s.writer(), std::slice::from_ref(&d), Some(&s)).await;
    match refused {
        Err(Error::FailedAncestor { txid }) => assert_eq!(txid, d),
        other => panic!("expected FailedAncestor, got {other:?}"),
    }
}

/// A non-change output (a pot, a receipt) of `owner` at `vout`, spendable.
async fn insert_pot(s: &StorageSqlx, l: &Lineage, owner: i64, txid: &str, vout: u32) -> i64 {
    let old = Utc::now() - Duration::days(2);
    sqlx::query_scalar(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, locking_script, created_at, updated_at) \
         VALUES (?, ?, ?, 1, 0, ?, 5000, 'you', '', 'custom', ?, x'51', ?, ?) RETURNING output_id",
    )
    .bind(l.user_id)
    .bind(owner)
    .bind(l.basket_id)
    .bind(vout)
    .bind(txid)
    .bind(old)
    .bind(old)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

/// createAction spending `txid:vout` explicitly, one 100 sat output.
fn spend_args(txid: &str, vout: u32) -> bsv_rs::wallet::CreateActionArgs {
    use bsv_rs::wallet::{CreateActionInput, CreateActionOutput, Outpoint};
    let mut id = [0u8; 32];
    id.copy_from_slice(&hex::decode(txid).unwrap());
    bsv_rs::wallet::CreateActionArgs {
        description: "spend a pot".to_string(),
        input_beef: None,
        inputs: Some(vec![CreateActionInput {
            outpoint: Outpoint { txid: id, vout },
            input_description: "the pot".to_string(),
            unlocking_script: None,
            unlocking_script_length: Some(107),
            sequence_number: None,
        }]),
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x51],
            satoshis: 100,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    }
}

async fn action_rows(s: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE description = 'spend a pot'")
        .fetch_one(s.pool())
        .await
        .unwrap()
}

fn posted(mock: &MockWalletServices) -> bool {
    mock.call_history().iter().any(|c| c.method == "post_beef")
}

/// C's source (the coinbase) proven and its root checked, so the walk ends
/// there and only P's status decides the BEEF.
async fn prove_coinbase(s: &StorageSqlx) {
    let coinbase = hex::decode(COINBASE_HEX).unwrap();
    let bump = single_leaf_bump(900, COINBASE_TXID);
    insert_proven(s, COINBASE_TXID, 900, &"bb".repeat(32), &coinbase, &bump).await;
    mark_checked(s, COINBASE_TXID, 900).await;
}

async fn fail(s: &StorageSqlx, transaction_id: i64) {
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE transaction_id = ?")
        .bind(transaction_id)
        .execute(s.writer())
        .await
        .unwrap();
}

/// #42 point 4, the explicit input: a pot of P (spendable = 1, P `failed`)
/// spent by name. createAction refuses it with the typed error before
/// anything is written or posted. At 0.11.0 the check read only the flag.
#[tokio::test]
async fn create_action_refuses_an_input_whose_creator_is_failed() {
    let s = storage().await;
    let l = seed(&s, None).await;
    prove_coinbase(&s).await;
    fail(&s, l.p_id).await;
    insert_pot(&s, &l, l.p_id, &l.p, 1).await;
    let mock = set(&s, MockWalletServicesBuilder::default());
    let (_, auth) = seed_user(&s).await;
    let refused = WalletStorageWriter::create_action(&s, &auth, spend_args(&l.p, 1)).await;
    match refused {
        Err(Error::FailedAncestor { txid }) => assert_eq!(txid, l.p),
        other => panic!("expected FailedAncestor, got {other:?}"),
    }
    assert_eq!(action_rows(&s).await, 0, "the action wrote nothing");
    assert!(!posted(&mock), "nothing posted");
}

/// #42 point 4, the walk: D (`unproven`) spends P's change and P is
/// `failed` (a descendant retire not yet finished). A pot of D spent by
/// name: the BEEF walk reaches P and refuses it, nothing written or posted.
/// At 0.11.0 the walk assembled P's bytes.
#[tokio::test]
async fn create_action_refuses_a_child_of_a_failed_parent() {
    let s = storage().await;
    let l = seed(&s, Some("unproven")).await;
    prove_coinbase(&s).await;
    fail(&s, l.p_id).await;
    let (d, d_id, _) = l.d.clone().unwrap();
    insert_pot(&s, &l, d_id, &d, 1).await;
    let mock = set(&s, MockWalletServicesBuilder::default());
    let (_, auth) = seed_user(&s).await;
    let refused = WalletStorageWriter::create_action(&s, &auth, spend_args(&d, 1)).await;
    match refused {
        Err(Error::FailedAncestor { txid }) => assert_eq!(txid, l.p),
        other => panic!("expected FailedAncestor, got {other:?}"),
    }
    assert_eq!(action_rows(&s).await, 0, "the action wrote nothing");
    assert!(!posted(&mock), "nothing posted");
}

/// #42 point 4, the re-send: D's request is `unsent` and P is `failed`.
/// send-waiting's rebuild refuses the lineage and does not fall back to
/// the stored inputBEEF (which carries P): nothing is posted. At 0.11.0
/// the fallback posted D over P.
#[tokio::test]
async fn send_waiting_posts_nothing_over_a_failed_parent() {
    let s = storage().await;
    let l = seed(&s, Some("sending")).await;
    let (d, _, _) = l.d.clone().unwrap();
    fail(&s, l.p_id).await;
    sqlx::query("UPDATE proven_tx_reqs SET status = 'invalid' WHERE txid = ?")
        .bind(&l.p)
        .execute(s.writer())
        .await
        .unwrap();
    sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent' WHERE txid = ?")
        .bind(&d)
        .execute(s.writer())
        .await
        .unwrap();
    let mock = set(
        &s,
        MockWalletServicesBuilder::default()
            .post_beef_response(MockResponse::Success(vec![courier(
                "ARC", &d, "error", true,
            )]))
            .get_status_for_txids_response(status_source(&d, "unknown")),
    );
    s.send_waiting_transactions(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(!posted(&mock), "nothing posted over a failed parent");
    assert_eq!(words(&s, &d).await, ("sending".into(), "unsent".into()));
}

async fn check_verdict(s: &StorageSqlx, output_id: i64) -> Option<String> {
    sqlx::query_scalar("SELECT last_verdict FROM locked_input_checks WHERE output_id = ?")
        .bind(output_id)
        .fetch_optional(s.pool())
        .await
        .unwrap()
        .flatten()
}

/// #42 point 5, the host's retire by txid: P never reached the network, no
/// status source holds it, and an explorer lists C (which a mined
/// competitor spent) as unspent. C stays locked to P, scheduled for the
/// re-check, never released at retire on the explorer's word. At 0.11.0
/// `is_utxo`'s unspent hint gave C back to coin selection.
#[tokio::test]
async fn the_hosts_retire_releases_nothing_on_an_explorers_unspent_word() {
    let s = storage().await;
    let l = seed(&s, None).await;
    let mock = set(
        &s,
        MockWalletServicesBuilder::default()
            .get_status_for_txids_response(status_source(&l.p, "unknown"))
            .is_utxo_response(MockResponse::Success(true)),
    );
    let outcome = s
        .retire_undeliverable_txid(mock.as_ref(), &l.p, "invalid")
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Some(RetireOutcome::Retired {
            restored: 0,
            kept: 1
        })
    );
    assert_eq!(words(&s, &l.p).await, ("failed".into(), "invalid".into()));
    assert_eq!(
        output(&s, l.c).await,
        (false, Some(l.p_id)),
        "C stays locked"
    );
    assert_eq!(check_verdict(&s, l.c).await.as_deref(), Some("unknown"));
    assert!(
        !mock.call_history().iter().any(|c| c.method == "is_utxo"),
        "no explorer asked at retire"
    );
}

/// #42 point 5, the competitor's proof: R's checked proof spends C, and an
/// explorer still lists C as unspent. C stays locked and is lost to R
/// (`spent-proven`, terminal: the re-check never asks about it again). At
/// 0.11.0 the explorer's hint released C.
#[tokio::test]
async fn a_coin_a_proven_competitor_spent_is_lost_whatever_an_explorer_says() {
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
            .get_status_for_txids_response(status_source(&l.p, "unknown"))
            .is_utxo_response(MockResponse::Success(true)),
    );
    s.synchronize_transaction_statuses().await.unwrap();
    assert_eq!(
        words(&s, &l.p).await,
        ("failed".into(), "doubleSpend".into())
    );
    assert_eq!(
        output(&s, l.c).await,
        (false, Some(l.p_id)),
        "C stays locked"
    );
    assert_eq!(
        check_verdict(&s, l.c).await.as_deref(),
        Some("spent-proven"),
        "C lost to R"
    );
    assert_eq!(
        s.locked_inputs_pending().await.unwrap(),
        0,
        "nothing left to re-check"
    );
}
