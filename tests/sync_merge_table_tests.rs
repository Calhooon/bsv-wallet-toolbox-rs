//! The whole table of the sync's merge of a transaction row this store
//! holds: held status by incoming status by link by arm, the status and the
//! link after. The rule, in each cell: a row this store holds `completed`
//! on a proof whose root it has checked stays `completed` and keeps its
//! link, whatever the incoming status; an incoming `completed` moves a held
//! status only beside a proof this store has checked; every other incoming
//! status is written as the reader has it (the newer row's arm), or, by the
//! older row's arm, only where that arm brings a link this store's row
//! lacks. No header is read by the merge.
//!
//! The two arms: the reader's row newer (the merge of ordinary
//! properties), the reader's row older (the merge of proof evidence, which
//! writes only where the reader's row links a proof and this store's row
//! links none).
#![cfg(feature = "sqlite")]

use std::sync::Arc;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;
use sync_support::exec;

const STATUSES: [&str; 6] = [
    "completed",
    "unproven",
    "failed",
    "sending",
    "nosend",
    "unsigned",
];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Link {
    /// This store's row links a proof row with this store's root-check
    /// record; the reader's row links none.
    HeldChecked,
    /// This store's row links a proof row with no record; the reader's row
    /// links none.
    HeldUnchecked,
    /// This store holds a checked proof row its row does not link; the
    /// reader's row links the proof.
    IncomingChecked,
    /// This store holds no proof row; the reader's row links its own (the
    /// drain brings it with no record).
    IncomingUnchecked,
    /// Both rows link; this store's proof row is checked.
    BothChecked,
    /// Both rows link; this store's proof row has no record.
    BothUnchecked,
    /// Neither row links a proof, and this store holds none.
    Neither,
}

const LINKS: [Link; 7] = [
    Link::HeldChecked,
    Link::HeldUnchecked,
    Link::IncomingChecked,
    Link::IncomingUnchecked,
    Link::BothChecked,
    Link::BothUnchecked,
    Link::Neither,
];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Arm {
    Newer,
    Older,
}

impl Link {
    fn held_links(self) -> bool {
        matches!(
            self,
            Link::HeldChecked | Link::HeldUnchecked | Link::BothChecked | Link::BothUnchecked
        )
    }
    fn incoming_links(self) -> bool {
        matches!(
            self,
            Link::IncomingChecked
                | Link::IncomingUnchecked
                | Link::BothChecked
                | Link::BothUnchecked
        )
    }
    fn checked(self) -> bool {
        matches!(
            self,
            Link::HeldChecked | Link::IncomingChecked | Link::BothChecked
        )
    }
}

/// The rule's answer for one cell: the status and whether the row is
/// linked after the merge.
fn expected(held: &str, incoming: &str, link: Link, arm: Arm) -> (String, bool) {
    let linked_after = link.held_links() || link.incoming_links();
    let arm_writes = match arm {
        Arm::Newer => true,
        Arm::Older => link.incoming_links() && !link.held_links(),
    };
    if !arm_writes {
        return (held.to_string(), link.held_links());
    }
    // Every link this table holds names the one proof row of the txid,
    // checked or not.
    let checked = linked_after && link.checked();
    let status = if held == "completed" && checked {
        "completed"
    } else if incoming == "completed" && !checked {
        held
    } else {
        incoming
    };
    (status.to_string(), linked_after)
}

/// One store and one drain for every held and incoming status of one link
/// shape and one arm: transaction `n` for each pair. Returns each cell's
/// `held > incoming: status:linked` and the header reads.
async fn run(link: Link, arm: Arm) -> (Vec<(String, String, String)>, usize) {
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::None, &headers).await;
    let r = r_store().await;
    let (held_stamp, reader_stamp) = match arm {
        Arm::Newer => (NOW, LATER),
        Arm::Older => (NOW, OLD),
    };
    let mut cells = Vec::new();
    let mut n: u8 = 1;
    for held in STATUSES {
        for incoming in STATUSES {
            let reference = format!("w{n}");
            let t = held_at(&w, n, held, &reference, held_stamp).await;
            let held_proof = match link {
                Link::HeldChecked | Link::IncomingChecked | Link::BothChecked => {
                    Some(checked_proof_row(&w, n).await)
                }
                Link::HeldUnchecked | Link::BothUnchecked => Some(proof_row(&w, n, OLD).await),
                Link::IncomingUnchecked | Link::Neither => None,
            };
            if link.held_links() {
                exec(
                    &w,
                    &format!(
                        "UPDATE transactions SET proven_tx_id = {} WHERE transaction_id = {t}",
                        held_proof.unwrap()
                    ),
                )
                .await;
            }
            if link.incoming_links() {
                reader_rows(&r, n, &reference, reader_stamp, incoming, true, false).await;
            } else {
                reader_row_unlinked(&r, n, &reference, reader_stamp, incoming).await;
            }
            cells.push((held.to_string(), incoming.to_string(), reference));
            n += 1;
        }
    }
    drain(&w, r).await;
    let mut out = Vec::new();
    for (held, incoming, reference) in cells {
        out.push((held, incoming, row(&w, &reference).await));
    }
    (out, headers.reads())
}

/// The reader's row of `n` under `reference`, reading `status`, no proof
/// row and no link.
async fn reader_row_unlinked(
    r: &bsv_wallet_toolbox_rs::StorageSqlx,
    n: u8,
    reference: &str,
    stamp: &str,
    status: &str,
) {
    let (txid, raw, _, _) = mined(n);
    exec(r, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'{status}','{reference}',1,-1000,1,0,'pay','{txid}',X'{}',{stamp},{stamp})", hex::encode(raw))).await;
}

#[tokio::test]
async fn the_merge_table_keeps_a_held_completed_on_a_checked_proof() {
    let mut table = Vec::new();
    let mut wrong = Vec::new();
    for arm in [Arm::Newer, Arm::Older] {
        for link in LINKS {
            let (cells, reads) = run(link, arm).await;
            assert_eq!(reads, 0, "{arm:?} {link:?}: the merge reads no header");
            for (held, incoming, got) in cells {
                let (status, linked) = expected(&held, &incoming, link, arm);
                let want = format!("{status}:{}", if linked { "linked" } else { "-" });
                table.push(format!("TABLE {arm:?} {link:?} {held} > {incoming}: {got}"));
                if got != want {
                    wrong.push(format!(
                        "{arm:?} {link:?} {held} > {incoming}: got {got}, want {want}"
                    ));
                }
                // No held word moves against a proof this store checked.
                if held == "completed"
                    && link.checked()
                    && (link.held_links() || link.incoming_links())
                    && !got.starts_with("completed:")
                {
                    wrong.push(format!(
                        "{arm:?} {link:?} completed > {incoming}: {got}, moved against a checked proof"
                    ));
                }
                // No held word moves TO `completed` without one.
                if held != "completed"
                    && got.starts_with("completed:")
                    && !(link.checked() && got.ends_with(":linked"))
                {
                    wrong.push(format!(
                        "{arm:?} {link:?} {held} > {incoming}: {got} with no checked proof"
                    ));
                }
            }
        }
    }
    for line in &table {
        println!("{line}");
    }
    println!("WITNESS merge table: {} cells", table.len());
    assert_eq!(table.len(), 2 * 7 * 6 * 6);
    assert!(
        wrong.is_empty(),
        "cells off the rule:\n{}",
        wrong.join("\n")
    );
}
