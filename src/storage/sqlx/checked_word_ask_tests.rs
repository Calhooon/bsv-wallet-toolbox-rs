//! A word beside a stored competitor proof with no root-check record asks
//! the tracker before its write (`word_scope`, `StorageSqlx::word_ask`).
//!
//! The store of `held_receipt_scope_tests`: X's action P spending X's coin
//! C (the coinbase's output 0), Y's received row of P, raised to `unproven`
//! by a take; then Z's R, a stored competitor spending C, with a proof row
//! whose path is valid and no root-check record; then a broadcaster's word
//! on P.

use std::sync::Arc;

use bsv_rs::transaction::MockChainTracker;

use super::checked_evidence_tests::{
    checked, p_proven, take, y_counted, z_r_proven_unrecorded, ErringTracker,
};
use super::competitor_proof::TakeApplied;
use super::held_receipt_scope_tests::{
    p_raw, read, seed, write_word, Cell, Writer, HEIGHT, P_HEIGHT,
};
use super::reorg_tests::COINBASE_TXID;
use super::word_scope::ENDED_BESIDE_UNCHECKED_PROOF;
use crate::storage::MonitorStorage;

/// The tracker the word asks.
#[derive(Clone, Copy, Debug)]
enum Asked {
    /// R's root at `HEIGHT` confirmed (the seed's tracker).
    Confirms,
    /// R's proof at a height whose root the tracker refutes.
    Refutes,
    /// A tracker that errs.
    Errs,
    /// No tracker on the store and no services.
    None,
}

/// Y's row raised by a take, then R's unrecorded proof row; the tracker the
/// word will ask set. Returns R's txid.
async fn taken_then_competitor(c: &Cell, asked: Asked) -> String {
    assert_eq!(take(c).await, TakeApplied::Applied);
    assert_eq!(read(c).await.y_row, "unproven");
    let height = match asked {
        Asked::Refutes => 950,
        _ => HEIGHT,
    };
    let r = z_r_proven_unrecorded(c, height).await;
    match asked {
        Asked::Confirms | Asked::Refutes => {}
        Asked::Errs => c.s.set_chain_tracker(Arc::new(ErringTracker)).await,
        Asked::None => {
            c.s.clear_chain_tracker().await;
            c.s.clear_services();
        }
    }
    r
}

/// The notes on P's request whose `what` is `what`.
async fn notes(c: &Cell, what: &str) -> Vec<serde_json::Value> {
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&c.p)
        .fetch_one(c.s.pool())
        .await
        .unwrap();
    let history: serde_json::Value = serde_json::from_str(&history).unwrap();
    history["notes"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|n| n["what"] == what)
        .collect()
}

/// The `spent-proven` marks the store holds.
async fn spent_proven_marks(c: &Cell) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM locked_input_checks WHERE last_verdict = ?")
        .bind(super::LOCKED_VERDICT_SPENT_PROVEN)
        .fetch_one(c.s.pool())
        .await
        .unwrap()
}

/// X's coin C: (`spendable`, `spent_by`).
async fn x_coin(c: &Cell) -> (bool, Option<i64>) {
    sqlx::query_as(
        "SELECT spendable, spent_by FROM outputs WHERE txid = ? AND vout = 0 \
         AND user_id = (SELECT user_id FROM transactions WHERE transaction_id = ?)",
    )
    .bind(COINBASE_TXID)
    .bind(c.x_row)
    .fetch_one(c.s.pool())
    .await
    .unwrap()
}

/// (h1) The tracker confirms R's root: Y's row ends on it, and the word's
/// write records the root.
#[tokio::test]
async fn a_word_beside_an_unrecorded_proof_the_tracker_confirms_ends_the_row() {
    let c = seed("sending").await;
    let r = taken_then_competitor(&c, Asked::Confirms).await;
    assert!(write_word(&c, Writer::Refusal).await);
    let got = read(&c).await;
    assert_eq!(
        (got.y_row.as_str(), got.y_out_spendable),
        ("failed", false),
        "{got:?}"
    );
    assert!(checked(&c, &r).await, "the word's write recorded R's root");
    assert!(notes(&c, ENDED_BESIDE_UNCHECKED_PROOF).await.is_empty());
}

/// (h2) The tracker refutes R's root: no evidence, Y's row keeps its word.
#[tokio::test]
async fn a_word_beside_an_unrecorded_proof_the_tracker_refutes_keeps_the_row() {
    let c = seed("sending").await;
    let r = taken_then_competitor(&c, Asked::Refutes).await;
    assert!(write_word(&c, Writer::Refusal).await);
    let got = read(&c).await;
    assert_eq!(
        (got.x_row.as_str(), got.y_row.as_str(), got.y_out_spendable),
        ("failed", "unproven", true),
        "{got:?}"
    );
    assert!(!checked(&c, &r).await, "no record");
    assert!(notes(&c, ENDED_BESIDE_UNCHECKED_PROOF).await.is_empty());
}

/// (h3) No tracker, and one that errs: R's proof counts as evidence (Y's
/// row `failed`), recorded on the request as a proof this store had not
/// checked, under its keys; no root-check record and no `spent-proven`
/// mark written on its account. P's own checked proof then completes Y's
/// row.
#[tokio::test]
async fn a_word_beside_an_unrecorded_proof_no_tracker_answers_ends_the_row_unchecked() {
    for asked in [Asked::None, Asked::Errs] {
        let c = seed("sending").await;
        let r = taken_then_competitor(&c, asked).await;
        assert!(write_word(&c, Writer::Refusal).await, "{asked:?}");
        let got = read(&c).await;
        assert_eq!(
            (got.y_row.as_str(), got.y_out_spendable),
            ("failed", false),
            "{asked:?}: {got:?}"
        );
        let found = notes(&c, ENDED_BESIDE_UNCHECKED_PROOF).await;
        assert_eq!(found.len(), 1, "{asked:?}: {found:?}");
        assert_eq!(found[0]["why"], "trackerNotAnswered", "{asked:?}");
        assert_eq!(found[0]["competitor"], r.as_str(), "{asked:?}");
        assert_eq!(
            found[0]["transactionIds"],
            serde_json::json!([c.y_row]),
            "{asked:?}"
        );
        assert!(!checked(&c, &r).await, "{asked:?}: no record");
        assert_eq!(spent_proven_marks(&c).await, 0, "{asked:?}");

        let mut tracker = MockChainTracker::new(1000);
        tracker.add_root(P_HEIGHT, c.p.clone());
        c.s.set_chain_tracker(Arc::new(tracker)).await;
        p_proven(&c).await;
        y_counted(&c).await;
    }
}

/// (h4) A received row failed beside a word before its own proof arrives
/// (P retired as undelivered, its coin marked spent with no spender on
/// record, then a broadcaster's word), then P's own proof arrives: the row
/// completes, its payment counts, and X's coin reads spent by P.
#[tokio::test]
async fn a_row_failed_on_a_word_completes_on_its_own_proof() {
    let c = seed("sending").await;
    super::checked_evidence_tests::retired_then_marked(&c, &p_raw()).await;
    MonitorStorage::mark_transaction_rejected(&c.s, &c.p, false)
        .await
        .unwrap();
    assert_eq!(read(&c).await.y_row, "failed");
    p_proven(&c).await;
    assert!(checked(&c, &c.p).await);
    y_counted(&c).await;
    assert_eq!(x_coin(&c).await, (false, Some(c.x_row)));
}
