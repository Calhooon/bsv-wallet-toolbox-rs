//! A proof row this store has not checked, linked by a transaction with no
//! request, whose root this store's tracker refutes: the status review's
//! pass demotes it as this line demotes a refuted unchecked proof
//! everywhere (the proof row and its record removed, the link cleared) and
//! makes a request that asks for the proof. The proof task's adoption
//! finds no proof row to link, so two full cycles (review, proof task,
//! review, proof task) read one header in all and leave the row `unproven`
//! with no link and a request asking. When the headers arrive, the proof
//! task brings the proof, the store funnel checks it against the tracker
//! and records it, and the row completes.
//!
//! Two shapes: the proof row inserted by a sync with no record and linked
//! by the adoption, and a row linked to its own proof row with no record
//! and no request.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;
use sync_support::exec;

#[derive(Clone, Copy, Debug)]
enum Road {
    /// A sync inserts the proof row; the proof task's adoption links it.
    Synced,
    /// The row links its own proof row with no record.
    Linked,
}

/// The shape, as each road leaves it: W's `wy` linked to an unchecked
/// proof row of `n`, no request. Returns W and the counting headers.
async fn shape(road: Road, n: u8) -> (Arc<StorageSqlx>, Arc<Headers>) {
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Refutes, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    match road {
        Road::Synced => {
            let r = r_store().await;
            reader_rows(&r, n, "ry", OLD, "completed", true, false).await;
            drain(&w, r).await;
            w.synchronize_transaction_statuses().await.unwrap();
        }
        Road::Linked => {
            let p = proof_row(&w, n, OLD).await;
            exec(
                &w,
                &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
            )
            .await;
        }
    }
    (w, headers)
}

async fn two_cycles_then_the_headers_arrive(road: Road, n: u8) {
    let (w, headers) = shape(road, n).await;
    let start = format!("{} | {}", row(&w, "wy").await, proof_state(&w, n).await);
    println!("WITNESS {road:?} start: {start} | {}", view(&w, n).await);
    assert!(
        start.starts_with("unproven:linked | proof rows 1 record none"),
        "{start}"
    );
    let base = headers.reads();
    let mut steps = Vec::new();
    for cycle in 1..=2 {
        MonitorStorage::review_status(&*w).await.unwrap();
        steps.push(format!(
            "cycle {cycle} review: {} ({} reads)",
            view(&w, n).await,
            headers.reads() - base
        ));
        w.synchronize_transaction_statuses().await.unwrap();
        steps.push(format!(
            "cycle {cycle} proof task: {} ({} reads)",
            view(&w, n).await,
            headers.reads() - base
        ));
    }
    for s in &steps {
        println!("WITNESS {road:?} {s}");
    }
    assert_eq!(
        headers.reads() - base,
        1,
        "one header read in all: {steps:?}"
    );
    assert_eq!(row(&w, "wy").await, "unproven:-");
    let (txid, _, _, _) = mined(n);
    let req: String = ::sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(w.pool())
        .await
        .unwrap();
    assert_eq!(req, "unmined", "a request asks for the proof");
    assert_eq!(
        proof_state(&w, n).await,
        "proof rows 0 record none",
        "the proof row and its record removed"
    );

    // The headers arrive: the tracker carries the root, the services
    // serve the proof; the proof task's own path completes the row.
    headers.carry(n);
    w.set_services(Arc::new(chain_holds(n)));
    let before = headers.reads();
    w.synchronize_transaction_statuses().await.unwrap();
    MonitorStorage::review_status(&*w).await.unwrap();
    let end = format!("{} | {}", view(&w, n).await, proof_state(&w, n).await);
    println!(
        "WITNESS {road:?} the headers arrive, proof task then review: {end} ({} reads)",
        headers.reads() - before
    );
    assert_eq!(row(&w, "wy").await, "completed:linked");
    assert_eq!(proof_state(&w, n).await, "proof rows 1 record checked");
}

#[tokio::test]
async fn a_refuted_proof_a_sync_inserted_is_removed_and_asked_once() {
    two_cycles_then_the_headers_arrive(Road::Synced, 71).await;
}

#[tokio::test]
async fn a_refuted_proof_a_row_links_is_removed_and_asked_once() {
    two_cycles_then_the_headers_arrive(Road::Linked, 72).await;
}

/// Beside a request whose word no proof, send or unfail task asks, the
/// refuted proof row and its record are removed, the row keeps its status
/// with its link cleared, and the request keeps its word. A second review
/// reads no header.
#[tokio::test]
async fn beside_a_request_no_task_asks_the_row_and_the_request_keep_their_words() {
    let mut n = 73u8;
    for word in ["nosend", "doubleSpend", "notFound"] {
        for status in ["unproven", "nosend", "sending"] {
            n += 1;
            let headers = Arc::new(Headers::default());
            let w = w_store(Tracker::Refutes, &headers).await;
            let t = held_at(&w, n, status, "wy", NOW).await;
            let p = proof_row(&w, n, OLD).await;
            exec(
                &w,
                &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
            )
            .await;
            proof_request(&w, n, word, None).await;
            MonitorStorage::review_status(&*w).await.unwrap();
            MonitorStorage::review_status(&*w).await.unwrap();
            let after = format!("{} | {}", view(&w, n).await, proof_state(&w, n).await);
            println!(
                "WITNESS held request {word}, row {status}: {after} ({} reads)",
                headers.reads()
            );
            assert_eq!(headers.reads(), 1);
            assert_eq!(
                after,
                format!("wy:{status}:- | req {word} | checked 0 | proof rows 0 record none")
            );
        }
    }
}
