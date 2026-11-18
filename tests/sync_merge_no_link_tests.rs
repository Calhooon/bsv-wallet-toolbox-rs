//! The sync's merge of a newer transaction row reading `completed` that
//! carries no proof link this store can resolve. An incoming `completed`
//! moves a held status only beside a proof this store has checked, and a
//! merge that brings no link never clears the one this store holds: held
//! `unproven` beside its own checked proof reads `completed` on that link;
//! held `unproven` with no link, or beside an unchecked proof, keeps its
//! status (and its link). The row's other properties merge as before.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

mod sync_support;
use sync_support::exec;
mod proof_fixture;
use proof_fixture::*;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Held {
    /// W's row linked to its proof row, root recorded as checked.
    CheckedLink,
    /// W's row linked to a proof row with no record.
    UncheckedLink,
    /// W's row with no link.
    NoLink,
}

/// W holds `n` `unproven` under `wy` as `held`; the reader's row under the
/// same reference is newer, reads `completed`, links no proof and says
/// `paid`. Returns W's `wy` after the drain, its description and the
/// header reads.
async fn cell(n: u8, held: Held) -> (String, String, usize) {
    let headers = Arc::new(Headers::default());
    headers.carry(n);
    let w = w_store(Tracker::Confirms, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    let link = match held {
        Held::CheckedLink => Some(checked_proof_row(&w, n).await),
        Held::UncheckedLink => Some(proof_row(&w, n, OLD).await),
        Held::NoLink => None,
    };
    if let Some(p) = link {
        exec(
            &w,
            &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
        )
        .await;
    }
    let r = r_store().await;
    reader_rows(&r, n, "wy", LATER, "completed", false, false).await;
    exec(&r, "UPDATE transactions SET description = 'paid'").await;
    drain(&w, r).await;
    let got = row(&w, "wy").await;
    let description: String =
        ::sqlx::query_scalar("SELECT description FROM transactions WHERE reference = 'wy'")
            .fetch_one(w.pool())
            .await
            .unwrap();
    println!(
        "WITNESS merge no link, held {held:?}: {got} described {description} ({} reads) | {}",
        headers.reads(),
        view(&w, n).await
    );
    (got, description, headers.reads())
}

#[tokio::test]
async fn beside_its_own_checked_proof_the_row_reads_completed_on_that_link() {
    let (got, description, reads) = cell(61, Held::CheckedLink).await;
    assert_eq!(got, "completed:linked");
    assert_eq!(description, "paid", "the other properties merge");
    assert_eq!(reads, 0);
}

#[tokio::test]
async fn with_no_link_the_held_status_stays() {
    let (got, description, reads) = cell(62, Held::NoLink).await;
    assert_eq!(got, "unproven:-");
    assert_eq!(description, "paid", "the other properties merge");
    assert_eq!(reads, 0);
}

#[tokio::test]
async fn beside_an_unchecked_proof_the_status_and_the_link_stay() {
    let (got, description, reads) = cell(63, Held::UncheckedLink).await;
    assert_eq!(got, "unproven:linked");
    assert_eq!(description, "paid", "the other properties merge");
    assert_eq!(reads, 0, "the merge reads no header");
}
