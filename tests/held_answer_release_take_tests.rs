//! A RELEASE MADE ON NO EVIDENCE DOES NOT STAND AGAINST A TAKE (the
//! keeper's reading 20 of section 30, Q1, under reading 18).
//!
//! The rule is by the row's state, not by who released it. In the one
//! write that applies a take, behind the competitor guard, each `failed`
//! outgoing row of the txid reads:
//!
//! - released, no evidence on record (the host's abort with its mark, or a
//!   release with none: an abort of a released version, an end by count, a
//!   fail on a broadcaster's word): recovered, the transaction `unproven`,
//!   the request `unmined`, the inputs marked spent by it again, its own
//!   outputs spendable again, the cause on the request's history;
//! - its inputs still locked (the retire path's `failed`), all of them or
//!   some, the locked-input re-check having given the others back: it stays
//!   ended, every free input of it is marked spent by it again, the take is
//!   a hint, and the request keeps its word unless a live row of another
//!   user needs it raised;
//! - evidence on record (a `spent-proven` check on one of its inputs): the
//!   take is refused as a standing competitor's checked proof refuses it:
//!   nothing is recovered, no row of the txid lifted (another user's held
//!   receipt keeps `sending`), no take recorded, the request keeps its word
//!   and the note is `takeNotCounted`.
//!
//! Never the half state: a request `unmined` beside a `failed` row whose
//! inputs are released.
//!
//! The matrix: the five writers of a take by the row's five states by
//! another user's live row of the same txid (a held receipt, `sending`)
//! present or not. The door's presenter is itself a live row of another
//! user, so the door's cells read a live row in both columns.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use abort_take::*;

/// The row's states of the rule.
const STATES: [Cause; 5] = [
    Cause::AbortFree,
    Cause::Unmarked,
    Cause::Refusal,
    Cause::Mixed,
    Cause::Evidence,
];

/// The cell's rule; `None` when it holds, else what is wrong.
fn judge(act: Act, cause: Cause, other_live: bool, got: &Reading) -> Option<String> {
    let name = format!("{act:?} {cause:?} other_live={other_live}");
    if got.half() {
        return Some(format!("{name}: the half state: {got:?}"));
    }
    let own = vec![(false, "own".to_string()), (false, "own".to_string())];
    // Whether the take was heard at all: send-waiting selects only a
    // request `unsent` or `sending`, and the retire path's `invalid` is
    // not one.
    let heard = !(act == Act::SendWaiting && matches!(cause, Cause::Refusal | Cause::Mixed));
    // Evidence on record refuses the take: another user's row is not
    // lifted (the retire path's own write after its refused promotion
    // fails every row of the txid, as it does for a standing competitor).
    let other_want = match cause {
        Cause::Evidence if act == Act::Retire => "failed",
        Cause::Evidence => "sending",
        _ if heard => "unproven",
        _ => "sending",
    };
    let other_ok = match (&got.other_row, other_live) {
        (None, false) => true,
        (Some(row), true) => row == other_want,
        _ => false,
    };
    let ok = match cause {
        Cause::AbortFree | Cause::Unmarked => {
            got.row.as_deref() == Some("unproven")
                && got.request.as_deref() == Some("unmined")
                && got.inputs == own
                && got.own_output_spendable == Some(true)
                && got.recovered_note
                && got.taken_after_abort == (cause == Cause::AbortFree)
                && got.balance == 500
                && got.listed.as_deref() == Some("Unproven")
        }
        Cause::Refusal | Cause::Mixed => {
            let lifted = act == Act::Unfail;
            let raised = lifted || (heard && (other_live || act == Act::Door));
            // Every free input of the row is marked spent by it again on a
            // take; unheard, the re-check's restored input stays free.
            let inputs = if heard || cause == Cause::Refusal {
                own.clone()
            } else {
                vec![(true, "none".to_string()), (false, "own".to_string())]
            };
            got.row.as_deref() == Some(if lifted { "unproven" } else { "failed" })
                && got.request.as_deref() == Some(if raised { "unmined" } else { "invalid" })
                && got.inputs == inputs
                && !got.recovered_note
                && got.held_as_hint == (heard && !raised)
        }
        Cause::Evidence => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref() == Some("invalid")
                // As the release left them; the door consumes the
                // outpoints its presentation spends (no spender recorded).
                && got.inputs.iter().all(|(s, w)| *s != (act == Act::Door) && w == "none")
                && got.own_output_spendable == Some(false)
                && !got.recovered_note
                && !got.held_as_hint
                && got.take_not_counted
        }
        _ => false,
    };
    (!(ok && other_ok)).then(|| format!("{name}: {got:?}"))
}

async fn matrix(act: Act) {
    let mut wrong = Vec::new();
    for cause in STATES {
        for other_live in [false, true] {
            let got = cell_with(act, cause, other_live).await;
            println!(
                "MATRIX {act:?} {cause:?} other_live={other_live} -> row {:?} request {:?} inputs {:?} own output {:?} recovered {} takenAfterAbort {} hint {} other {:?} balance {} listActions {:?}",
                got.row,
                got.request,
                got.inputs,
                got.own_output_spendable,
                got.recovered_note,
                got.taken_after_abort,
                got.held_as_hint,
                got.other_row,
                got.balance,
                got.listed
            );
            if let Some(w) = judge(act, cause, other_live, &got) {
                wrong.push(w);
            }
        }
    }
    assert!(wrong.is_empty(), "{act:?}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_take_at_the_door_reads_the_failed_row_by_its_state() {
    matrix(Act::Door).await;
}

#[tokio::test]
async fn a_take_heard_by_send_waiting_reads_the_failed_row_by_its_state() {
    matrix(Act::SendWaiting).await;
}

#[tokio::test]
async fn a_take_heard_by_the_retire_path_reads_the_failed_row_by_its_state() {
    matrix(Act::Retire).await;
}

#[tokio::test]
async fn a_take_heard_by_unfail_reads_the_failed_row_by_its_state() {
    matrix(Act::Unfail).await;
}

#[tokio::test]
async fn a_take_of_the_wallets_own_post_reads_the_failed_row_by_its_state() {
    matrix(Act::OwnPost).await;
}

/// Evidence on record beside another user's live row: the take lifts
/// nothing, so that row reads its held word after the take (the retire
/// path's own write fails it); the status review's check 1 then fails it on
/// the request's `invalid` word, at every writer.
#[tokio::test]
async fn evidence_on_record_lifts_no_row_through_a_review_pass() {
    let mut wrong = Vec::new();
    for act in ACTS {
        let got = cell_full(act, Cause::Evidence, true, true).await;
        println!(
            "EVIDENCE {act:?} -> row {:?} request {:?} other {:?} after review {:?} takeNotCounted {}",
            got.row, got.request, got.other_row, got.other_after_review, got.take_not_counted
        );
        let after_take = if act == Act::Retire {
            "failed"
        } else {
            "sending"
        };
        if got.other_row.as_deref() != Some(after_take)
            || got.other_after_review.as_deref() != Some("failed")
            || got.request.as_deref() != Some("invalid")
            || !got.take_not_counted
        {
            wrong.push(format!("{act:?}: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn the_matrix_names_every_writer_state_and_column() {
    assert_eq!(ACTS.len() * STATES.len() * 2, 50);
}
