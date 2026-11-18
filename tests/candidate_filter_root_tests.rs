//! The status review pass's linked-proof kind takes a row by what its
//! write reads: whether the proof row is checked is decided by the root
//! its stored path computes, never by its `merkle_root` column. A proof
//! row whose column names a root this store recorded as checked, while its
//! path computes another, is a candidate: the tracker is asked for the
//! computed root and, confirming, the row completes.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::*;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;
use sync_support::exec;

#[tokio::test]
async fn a_column_root_with_a_record_does_not_hide_a_path_that_computes_another() {
    let n = 91;
    let headers = Arc::new(Headers::default());
    headers.carry(n);
    let w = w_store(Tracker::Confirms, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    let p = proof_row(&w, n, OLD).await;
    let (txid, _, _, _) = mined(n);
    let other = "cd".repeat(32);
    exec(
        &w,
        &format!("UPDATE proven_txs SET merkle_root = '{other}' WHERE proven_tx_id = {p}"),
    )
    .await;
    exec(&w, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{other}',{NOW})")).await;
    exec(
        &w,
        &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
    )
    .await;
    MonitorStorage::review_status(&*w).await.unwrap();
    let got = row(&w, "wy").await;
    println!(
        "WITNESS column root recorded, path computes another: {got} ({} reads) | {}",
        headers.reads(),
        proof_state(&w, n).await
    );
    assert_eq!(got, "completed:linked");
    assert_eq!(headers.reads(), 1, "the computed root is asked");
    assert_eq!(proof_state(&w, n).await, "proof rows 1 record checked");
}

/// The same reader, on a row that is not `completed` and links a proof row
/// this store checked (no `failed` row of the txid): the pass completes it
/// with no header read.
#[tokio::test]
async fn a_row_linking_a_checked_proof_completes_with_no_header_read() {
    let n = 92;
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Refutes, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    let p = checked_proof_row(&w, n).await;
    exec(
        &w,
        &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
    )
    .await;
    MonitorStorage::review_status(&*w).await.unwrap();
    let got = row(&w, "wy").await;
    println!(
        "WITNESS linked to a checked proof: {got} ({} reads)",
        headers.reads()
    );
    assert_eq!(got, "completed:linked");
    assert_eq!(headers.reads(), 0);
}
