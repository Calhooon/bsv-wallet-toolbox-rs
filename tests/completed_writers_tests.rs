//! Every writer of a transaction's `completed` on this line, each driven
//! through its real path beside a proof row this store has not checked (or,
//! for the BEEF walk's fetch, no proof row) and a tracker that counts its
//! reads: after each, a row this store held as not `completed` reads
//! `completed` only if a header was read and this store wrote its
//! root-check record. Each writer runs under a confirming tracker, a
//! refuting one and none.
//!
//! The unchecked proof row comes by the road a store meets it on: a
//! manager's drain from a local reader whose rows of the txid are under
//! other references (W's own rows untouched), or under the same references
//! where the merge is the writer.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::transaction::{Beef, MerklePath};
use bsv_rs::wallet::{
    BasketInsertion, InternalizeActionArgs, InternalizeOutput, ListOutputsArgs, OutputInclude,
};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    BlockHeader, GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail,
};
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::{count, exec, ALICE};
mod proof_fixture;
use proof_fixture::*;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Writer {
    /// The proof task's walk of a request (`ingest_merkle_proof`, the funnel).
    ProofFunnel,
    /// The BEEF walk reading a stored proof it has not checked.
    BeefWalkStored,
    /// The BEEF walk fetching a proof the store does not hold (the funnel's
    /// twin); no proof row before it.
    BeefWalkFetched,
    /// The internalize door, a proven AtomicBEEF of a txid held `nosend`.
    InternalizeDoor,
    /// The monitor review's check 3: a `completed` request with a link.
    Check3,
    /// The monitor review's check 1: an `invalid` request beside a proof.
    Check1,
    /// The monitor review pass's linked kind: a row linking an unchecked
    /// proof beside a request no task asks.
    PassLinked,
    /// The per-user status review.
    PerUserReview,
    /// The sync's merge of an older row carrying a link (proof evidence).
    MergeOlder,
    /// The sync's merge of a newer row (ordinary properties).
    MergeNewer,
    /// The sync's merge of a newer `completed` row that links no proof.
    MergeNewerNoLink,
    /// The proof task's adoption (step 1 of `adopt_unproven_transactions`).
    Adoption,
    /// LIMIT, outside the rule: the sync's INSERT of a transaction row this
    /// store did not hold takes its reader's status and proof link as they
    /// are; it moves no status this store held. Not driven here.
    SyncInsertOfARowNotHeld,
}

/// Every writer the two rounds' tables list, and the one outside the rule.
const WRITERS: [Writer; 13] = [
    Writer::ProofFunnel,
    Writer::BeefWalkStored,
    Writer::BeefWalkFetched,
    Writer::InternalizeDoor,
    Writer::Check3,
    Writer::Check1,
    Writer::PassLinked,
    Writer::PerUserReview,
    Writer::MergeOlder,
    Writer::MergeNewer,
    Writer::MergeNewerNoLink,
    Writer::Adoption,
    Writer::SyncInsertOfARowNotHeld,
];

/// Whether the writer itself completes the held row under a confirming
/// tracker. The others leave the status to a writer that checks: the BEEF
/// walk's read check records the root and moves no status; the per-user
/// review, the merges and the adoption read no header and leave it for the
/// monitor review's pass.
fn completes_when_confirmed(writer: Writer) -> bool {
    matches!(
        writer,
        Writer::ProofFunnel
            | Writer::BeefWalkFetched
            | Writer::InternalizeDoor
            | Writer::Check3
            | Writer::Check1
            | Writer::PassLinked
    )
}

/// Services that hold `n` mined and its proof at HEIGHT.
fn services_holding(n: u8) -> MockWalletServices {
    let (txid, _, path, root) = mined(n);
    MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid,
                status: "mined".to_string(),
                depth: Some(1),
                merkle_path: None,
                block_height: None,
                block_hash: None,
            }],
        }))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("MockProvider".to_string()),
            merkle_path: Some(hex::encode(path)),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "00".repeat(32),
                merkle_root: root,
                time: 1,
                bits: 1,
                nonce: 1,
                hash: "e".repeat(64),
                height: HEIGHT,
            }),
            error: None,
            notes: vec![],
        }))
        .build()
}

/// The reader's rows of `n` under `reference` (`completed`, linked unless
/// `linked` is false), with a `completed` request when `with_request`,
/// drained into W.
async fn bring(
    w: &Arc<StorageSqlx>,
    n: u8,
    reference: &str,
    stamp: &str,
    linked: bool,
    with_request: bool,
) {
    let r = r_store().await;
    reader_rows(&r, n, reference, stamp, "completed", linked, with_request).await;
    drain(w, r).await;
}

/// Alice's held rows of `n` as `(transaction_id, status)`.
async fn held_rows(w: &StorageSqlx, n: u8) -> Vec<(i64, String)> {
    ::sqlx::query_as(
        "SELECT transaction_id, status FROM transactions WHERE txid = ? ORDER BY transaction_id",
    )
    .bind(mined(n).0)
    .fetch_all(w.pool())
    .await
    .unwrap()
}

/// Sets W up for `writer` (held rows, the unchecked proof row), then runs
/// it. Returns the rows held not `completed` before the writer, the rows
/// after, the header reads of the writer, and whether this store holds a
/// root-check record of `n` after it.
async fn run(
    writer: Writer,
    n: u8,
    tracker: Tracker,
) -> (Vec<(i64, String)>, Vec<(i64, String)>, usize, bool) {
    let headers = Arc::new(Headers::default());
    if tracker == Tracker::Confirms {
        headers.carry(n);
    }
    let w = w_store(tracker, &headers).await;
    let own = format!("w{n}");
    let other = format!("r{n}");
    match writer {
        Writer::ProofFunnel => {
            held_at(&w, n, "unproven", &own, NOW).await;
            proof_request(&w, n, "unmined", None).await;
            bring(&w, n, &other, OLD, true, false).await;
            w.set_services(Arc::new(services_holding(n)));
        }
        Writer::BeefWalkStored | Writer::BeefWalkFetched => {
            // Old enough for the walk to ask for a proof it does not hold.
            held_at(&w, n, "unproven", &own, OLD).await;
            proof_request(&w, n, "unmined", None).await;
            exec(&w, "UPDATE outputs SET basket_id = (SELECT basket_id FROM output_baskets WHERE user_id = 1 AND name = 'default')").await;
            if writer == Writer::BeefWalkStored {
                bring(&w, n, &other, OLD, true, false).await;
            } else {
                w.set_services(Arc::new(services_holding(n)));
            }
        }
        Writer::InternalizeDoor => {
            held_at(&w, n, "nosend", &own, NOW).await;
            proof_request(&w, n, "nosend", None).await;
            bring(&w, n, &other, OLD, true, false).await;
        }
        Writer::Check3 | Writer::PerUserReview => {
            held_at(&w, n, "unproven", &own, NOW).await;
            bring(&w, n, &other, OLD, true, true).await;
        }
        Writer::Check1 => {
            held_at(&w, n, "unproven", &own, NOW).await;
            proof_request(&w, n, "invalid", None).await;
            bring(&w, n, &other, OLD, true, false).await;
        }
        Writer::PassLinked => {
            held_at(&w, n, "nosend", &own, NOW).await;
            proof_request(&w, n, "nosend", None).await;
            bring(&w, n, &own, OLD, true, false).await;
        }
        Writer::MergeOlder | Writer::MergeNewer | Writer::MergeNewerNoLink | Writer::Adoption => {
            held_at(&w, n, "unproven", &own, NOW).await;
        }
        Writer::SyncInsertOfARowNotHeld => unreachable!("outside the rule, not driven"),
    }
    let before: Vec<(i64, String)> = held_rows(&w, n)
        .await
        .into_iter()
        .filter(|(_, s)| s != "completed")
        .collect();
    let reads_before = headers.reads();
    match writer {
        Writer::ProofFunnel | Writer::Adoption => {
            if writer == Writer::Adoption {
                bring(&w, n, &other, OLD, true, false).await;
            }
            w.synchronize_transaction_statuses().await.unwrap();
        }
        Writer::BeefWalkStored | Writer::BeefWalkFetched => {
            let auth = AuthId::with_user_id(ALICE, 1);
            let args = ListOutputsArgs {
                basket: "default".to_string(),
                tags: None,
                tag_query_mode: None,
                include: Some(OutputInclude::EntireTransactions),
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: Some(10),
                offset: Some(0),
                seek_permission: None,
            };
            let _ = WalletStorageReader::list_outputs(&*w, &auth, args).await;
        }
        Writer::InternalizeDoor => {
            let (txid, raw, path, _) = mined(n);
            let mut beef = Beef::new();
            beef.merge_bump(MerklePath::from_binary(&path).unwrap());
            beef.merge_raw_tx(raw, Some(0));
            let tx = beef.to_binary_atomic(&txid).unwrap();
            let args = InternalizeActionArgs {
                tx,
                outputs: vec![InternalizeOutput {
                    output_index: 0,
                    protocol: "basket insertion".to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "kept".to_string(),
                        custom_instructions: None,
                        tags: None,
                    }),
                }],
                description: "an output to keep".to_string(),
                labels: None,
                seek_permission: None,
            };
            let auth = AuthId::with_user_id(ALICE, 1);
            let r = WalletStorageWriter::internalize_action(&*w, &auth, args).await;
            println!("  internalize {tracker:?}: {:?}", r.as_ref().map(|_| "ok"));
        }
        Writer::Check3 | Writer::Check1 | Writer::PassLinked => {
            MonitorStorage::review_status(&*w).await.unwrap();
        }
        Writer::PerUserReview => {
            let auth = AuthId::with_user_id(ALICE, 1);
            WalletStorageWriter::review_status(&*w, &auth, chrono::Utc::now())
                .await
                .unwrap();
        }
        Writer::MergeOlder => bring(&w, n, &own, OLD, true, false).await,
        Writer::MergeNewer => bring(&w, n, &own, LATER, true, false).await,
        Writer::MergeNewerNoLink => bring(&w, n, &own, LATER, false, false).await,
        Writer::SyncInsertOfARowNotHeld => unreachable!(),
    }
    let reads = headers.reads() - reads_before;
    let after = held_rows(&w, n).await;
    let recorded = count(
        &w,
        &format!(
            "SELECT COUNT(*) FROM proof_root_checks WHERE txid = '{}' AND merkle_root NOT LIKE 'refuted:%'",
            mined(n).0
        ),
    )
    .await
        > 0;
    (before, after, reads, recorded)
}

#[tokio::test]
async fn a_held_row_reads_completed_only_after_a_header_read_and_a_record() {
    let mut completed_somewhere = 0;
    for (i, writer) in WRITERS.into_iter().enumerate() {
        if writer == Writer::SyncInsertOfARowNotHeld {
            println!("WITNESS {writer:?}: outside the rule, a limit; not driven");
            continue;
        }
        for (j, tracker) in [Tracker::Confirms, Tracker::Refutes, Tracker::None]
            .into_iter()
            .enumerate()
        {
            let n = 100 + (i * 3 + j) as u8;
            let (before, after, reads, recorded) = run(writer, n, tracker).await;
            assert!(!before.is_empty(), "{writer:?}: a row held not completed");
            let mut answers = Vec::new();
            let all_completed = before
                .iter()
                .all(|(id, _)| after.iter().any(|(a, s)| a == id && s == "completed"));
            assert_eq!(
                all_completed,
                tracker == Tracker::Confirms && completes_when_confirmed(writer),
                "{writer:?} {tracker:?}: {after:?}"
            );
            for (id, was) in &before {
                let now = &after.iter().find(|(a, _)| a == id).unwrap().1;
                answers.push(format!("{was}->{now}"));
                if now == "completed" {
                    completed_somewhere += 1;
                    assert!(
                        reads > 0 && recorded,
                        "{writer:?} {tracker:?}: row {id} {was} -> completed with {reads} header reads, record {recorded}"
                    );
                }
            }
            println!(
                "WITNESS {writer:?} {tracker:?}: {} | {reads} header reads | record {recorded}",
                answers.join(", ")
            );
        }
    }
    assert!(completed_somewhere > 0, "some writer completed a held row");
}
