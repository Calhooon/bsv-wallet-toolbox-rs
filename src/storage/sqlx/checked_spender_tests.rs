//! The spender of a `spent-proven` outpoint is a proof row this store
//! checked whose own bytes spend it, found by the outpoint; a request's
//! history is not where it is read.
//!
//! The store of `held_receipt_scope_tests`: X's action P spending X's coin
//! C (the coinbase's output 0), Y's received row of P, R another spend of
//! C, the competitor whose checked proof the proof task's ask keeps.

use std::sync::Arc;

use bsv_rs::transaction::MockChainTracker;
use chrono::Utc;

use super::checked_evidence_tests::take;
use super::checked_mark_tests::spending;
use super::competitor_proof::TakeApplied;
use super::held_receipt_scope_tests::{
    output, p_raw, row, seed, user, write_word, Cell, Writer, HEIGHT, PAYMENT,
};
use super::reorg_tests::{single_leaf_bump, txid_of, COINBASE_TXID};
use super::void_lineage_tests::{apply_rejected_naming, serving};
use crate::services::WalletServices;
use crate::storage::traits::PurgeParams;
use crate::storage::traits::WalletStorageProvider;
use crate::storage::MonitorStorage;

/// Q: a third spend of C, R's and P's rival.
fn q_raw() -> Vec<u8> {
    spending(&[(COINBASE_TXID, 0)], &[333])
}

pub(super) async fn status_of(c: &Cell, transaction_id: i64) -> String {
    sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
        .bind(transaction_id)
        .fetch_one(c.s.pool())
        .await
        .unwrap()
}

/// A request for `raw`'s txid at `word`, its history empty.
pub(super) async fn request(c: &Cell, raw: &[u8], word: &str) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, ?, 0, '{}', 0, '{}', ?, NULL, ?, ?)",
    )
    .bind(txid_of(raw))
    .bind(word)
    .bind(raw)
    .bind(now)
    .bind(now)
    .execute(c.s.writer())
    .await
    .unwrap();
}

/// P retired on R's checked proof; the purge deletes P's request past its
/// age; another user W presents P again (a held received row and a fresh
/// request, as the door writes them): a take of P is refused on R's
/// checked proof, read by the outpoint.
#[tokio::test]
async fn a_purged_request_leaves_the_checked_spender_on_record() {
    let c = seed("sending").await;
    write_word(&c, Writer::Competitor).await;
    assert_eq!(status_of(&c, c.x_row).await, "failed");
    let purged = MonitorStorage::purge_data(
        &c.s,
        PurgeParams {
            max_age_days: 0,
            purge_completed: false,
            purge_failed: true,
        },
    )
    .await
    .unwrap();
    assert!(purged.count >= 1, "{purged:?}");
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    assert_eq!(left, 0, "the purge deleted P's request");

    let (w, wb) = user(&c.s, &"d".repeat(66)).await;
    let w_row = row(&c.s, &w, &c.p, "sending", &p_raw(), false).await;
    output(
        &c.s,
        &w,
        wb,
        w_row,
        &c.p,
        1,
        PAYMENT,
        "receive",
        (true, None),
    )
    .await;
    request(&c, &p_raw(), "unsent").await;

    assert!(
        matches!(take(&c).await, TakeApplied::Refused { .. }),
        "R's checked proof spends P's input"
    );
    assert_eq!(status_of(&c, w_row).await, "sending");
}

/// Q also spends C, the outpoint R's checked proof marked: Y's received row
/// of Q, `unproven`, beside a word on Q ends on that evidence.
#[tokio::test]
async fn a_second_spender_of_a_marked_outpoint_ends_on_the_evidence() {
    let c = seed("sending").await;
    write_word(&c, Writer::Competitor).await;
    let q = txid_of(&q_raw());
    let q_row = row(&c.s, &c.y, &q, "unproven", &q_raw(), false).await;
    let basket: i64 = sqlx::query_scalar("SELECT basket_id FROM outputs WHERE output_id = ?")
        .bind(c.y_out)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    output(
        &c.s,
        &c.y,
        basket,
        q_row,
        &q,
        0,
        333,
        "receive",
        (true, None),
    )
    .await;
    request(&c, &q_raw(), "unmined").await;

    MonitorStorage::mark_transaction_rejected(&c.s, &q, false)
        .await
        .unwrap();
    MonitorStorage::review_status(&c.s).await.unwrap();
    assert_eq!(
        status_of(&c, q_row).await,
        "failed",
        "R's checked proof spends Q's input"
    );
}

/// A coin of nobody's that P does not spend.
const ELSEWHERE: &str = "2222222222222222222222222222222222222222222222222222222222222222";

/// P's request carries T's bytes (T spends ELSEWHERE, not C); K, queued as
/// P's competitor, spends ELSEWHERE and its proof checks. The ask reads P's
/// inputs from bytes that hash to P: K spends none of them, and no row of P
/// is ended.
#[tokio::test]
async fn the_ask_reads_our_inputs_from_bytes_that_hash_to_our_txid() {
    let c = seed("sending").await;
    let t_raw = spending(&[(ELSEWHERE, 0)], &[555]);
    let k_raw = spending(&[(ELSEWHERE, 0)], &[777]);
    let k = txid_of(&k_raw);
    sqlx::query("UPDATE proven_tx_reqs SET raw_tx = ? WHERE txid = ?")
        .bind(&t_raw)
        .bind(&c.p)
        .execute(c.s.writer())
        .await
        .unwrap();
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(HEIGHT, k.clone());
    c.s.set_chain_tracker(Arc::new(tracker)).await;
    let services: Arc<dyn WalletServices> =
        Arc::new(serving(&k_raw, Some(single_leaf_bump(HEIGHT, &k))).build());
    WalletStorageProvider::set_services(&c.s, services);

    apply_rejected_naming(&c.s, &c.p, &k).await;
    c.s.synchronize_transaction_statuses().await.unwrap();
    assert_eq!(
        (
            status_of(&c, c.x_row).await.as_str(),
            status_of(&c, c.y_row).await.as_str()
        ),
        ("sending", "sending"),
        "K spends none of P's inputs"
    );
    let word: String = sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    assert_ne!(word, "doubleSpend");
}

/// The verdict on X's coin C.
async fn c_verdict(c: &Cell) -> Option<String> {
    sqlx::query_scalar(
        "SELECT l.last_verdict FROM locked_input_checks l JOIN outputs o ON o.output_id = l.output_id \
         WHERE o.txid = ? AND o.vout = 0",
    )
    .bind(COINBASE_TXID)
    .fetch_optional(c.s.pool())
    .await
    .unwrap()
}

/// R's checked proof, which the ask kept, leaves the headers: its proof row
/// is demoted as the reorg path demotes one, no request is made for R (the
/// store holds no transaction of R), and the mark it bound on C stays
/// `spent-proven`.
#[tokio::test]
async fn a_stale_competitor_proof_makes_no_request_for_its_txid() {
    let c = seed("sending").await;
    write_word(&c, Writer::Competitor).await;
    let r = txid_of(&super::held_receipt_scope_tests::r_raw());
    assert_eq!(c_verdict(&c).await.as_deref(), Some("spent-proven"));
    c.s.set_chain_tracker(Arc::new(MockChainTracker::new(1000)))
        .await;
    assert!(MonitorStorage::demote_stale_proof(&c.s, &r).await.unwrap());
    let (proofs, requests): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM proven_txs WHERE txid = ?1), \
                (SELECT COUNT(*) FROM proven_tx_reqs WHERE txid = ?1)",
    )
    .bind(&r)
    .fetch_one(c.s.pool())
    .await
    .unwrap();
    assert_eq!((proofs, requests), (0, 0), "R demoted, no request for R");
    assert_eq!(c_verdict(&c).await.as_deref(), Some("spent-proven"));
}
