//! The proof task's adoption (`adopt_unproven_transactions`, step 1) beside
//! a proof row this store has not checked: the row is linked and its status
//! left, no header read; the status review's pass then checks the root
//! (confirmed: completed; refuted: the link cleared, the status left; no
//! tracker: nothing written). A proof this store checked: linked and
//! completed as before.
//!
//! The road: a manager's drain from a local reader whose rows of the txid
//! are under other references; W's own row is left unlinked by the drain,
//! the proof row inserted with no root-check record.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::*;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;

const Y: u8 = 41;

/// W holds Y `unproven` under `wy` with no request; the reader's row of Y
/// (under `ry`, `completed` and linked, no request) comes in a drain; then
/// the proof task's pass and the review's pass. Returns W's row `wy` after
/// each step and the header reads of each.
async fn cell(tracker: Tracker) -> [String; 6] {
    let headers = Arc::new(Headers::default());
    if tracker == Tracker::Confirms {
        headers.carry(Y);
    }
    let w = w_store(tracker, &headers).await;
    held_at(&w, Y, "unproven", "wy", NOW).await;
    let r = r_store().await;
    reader_rows(&r, Y, "ry", OLD, "completed", true, false).await;
    drain(&w, r).await;
    let drained = row(&w, "wy").await;
    let drain_reads = headers.reads();
    w.synchronize_transaction_statuses().await.unwrap();
    let adopted = row(&w, "wy").await;
    let adopt_reads = headers.reads() - drain_reads;
    MonitorStorage::review_status(&*w).await.unwrap();
    let reviewed = row(&w, "wy").await;
    let review_reads = headers.reads() - drain_reads - adopt_reads;
    println!(
        "WITNESS adoption {tracker:?}: drained {drained} ({drain_reads} reads), proof task {adopted} ({adopt_reads} reads), review {reviewed} ({review_reads} reads) | {}",
        view(&w, Y).await
    );
    [
        drained,
        adopted,
        reviewed,
        drain_reads.to_string(),
        adopt_reads.to_string(),
        review_reads.to_string(),
    ]
}

#[tokio::test]
async fn the_adoption_links_an_unchecked_proof_and_leaves_the_status() {
    for tracker in [Tracker::Confirms, Tracker::Refutes, Tracker::None] {
        let v = cell(tracker).await;
        assert_eq!(v[0], "unproven:-", "the drain leaves W's row");
        assert_eq!(v[1], "unproven:linked", "{tracker:?}: the link alone");
        assert_eq!(v[3], "0");
        assert_eq!(v[4], "0", "the adoption reads no header");
    }
}

#[tokio::test]
async fn the_review_then_checks_the_root() {
    let v = cell(Tracker::Confirms).await;
    assert_eq!(v[2], "completed:linked");
    assert_eq!(v[5], "1", "one header read");
    let v = cell(Tracker::Refutes).await;
    assert_eq!(
        v[2], "unproven:-",
        "refuted: the link cleared, the status left"
    );
    let v = cell(Tracker::None).await;
    assert_eq!(v[2], "unproven:linked", "no tracker: nothing written");
}

/// A proof this store checked: the adoption links and completes the row,
/// and a proof-eligible request beside it, as before; no header read.
#[tokio::test]
async fn a_proof_this_store_checked_is_adopted_and_completed() {
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Refutes, &headers).await;
    held_at(&w, Y, "unproven", "wy", NOW).await;
    held_at(&w, Y + 1, "unproven", "wz", NOW).await;
    checked_proof_row(&w, Y).await;
    checked_proof_row(&w, Y + 1).await;
    proof_request(&w, Y + 1, "unmined", None).await;
    let out = w.adopt_unproven_transactions().await.unwrap();
    println!(
        "WITNESS adoption checked: {} || {}",
        view(&w, Y).await,
        view(&w, Y + 1).await
    );
    assert_eq!(out.linked_from_proven_txs, 2);
    assert_eq!(
        view(&w, Y).await,
        "wy:completed:linked | req none | checked 1"
    );
    assert_eq!(
        view(&w, Y + 1).await,
        "wz:completed:linked | req completed+link | checked 1"
    );
    assert_eq!(headers.reads(), 0);
}

/// An unchecked proof beside a proof-eligible request: the link alone, the
/// request left for the task's own walk (which asks through the store
/// funnel); beside any other request: nothing, the request's owner's.
#[tokio::test]
async fn beside_a_request_the_adoption_moves_no_word() {
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Confirms, &headers).await;
    for (n, req) in [(51u8, "unmined"), (52, "nosend"), (53, "completed")] {
        held_at(&w, n, "unproven", &format!("w{n}"), NOW).await;
        proof_row(&w, n, OLD).await;
        proof_request(&w, n, req, None).await;
    }
    w.adopt_unproven_transactions().await.unwrap();
    let got = [view(&w, 51).await, view(&w, 52).await, view(&w, 53).await];
    println!("WITNESS adoption beside requests: {got:?}");
    assert_eq!(got[0], "w51:unproven:linked | req unmined | checked 0");
    assert_eq!(got[1], "w52:unproven:- | req nosend | checked 0");
    assert_eq!(got[2], "w53:unproven:- | req completed | checked 0");
    assert_eq!(headers.reads(), 0);
}
