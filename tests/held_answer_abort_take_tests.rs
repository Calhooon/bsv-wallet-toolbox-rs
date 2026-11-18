//! A take heard after the wallet's own abort recovers the transaction, in
//! the one write that applies the take (the keeper's reading 18 of
//! section 30, the broadcaster charter's B4 (d)).
//!
//! The state: the wallet's own action is posted; `abortAction` lands while
//! the post is in flight (the transaction `failed`, its request `invalid`,
//! its inputs released); then an instance's take arrives. The rule: a
//! request `unmined` beside a `failed` row with its inputs released never
//! stands. A `failed` the host's abort wrote (its mark on the request's
//! history) goes back to `unproven` with each input still free marked
//! spent by it again and the outputs it closed opened again, the cause on
//! the request's history (`takenAfterAbort`); an input another action
//! spent meanwhile stays with that action. A `failed` that is not a
//! release on no evidence (a competitor's checked proof, the retire path's
//! `failed` with its inputs locked, a received transaction) is not
//! recovered by a take.
//!
//! The matrix: the take's writer (the door for another user's
//! presentation, send-waiting with the abort landing during its post, the
//! retire path's status source, the unfail task, the wallet's own post) by
//! the row's cause and its inputs (the abort with every input free, with
//! one spent meanwhile by another action, with one spent by an action
//! that is itself proven; a `failed` on a competitor's checked proof; the
//! retire path's `failed`, its inputs locked; a received transaction of
//! another user). The fixture is `common/abort_take.rs`.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use abort_take::*;

/// The cell's rule; `None` when it holds, else what is wrong.
fn judge(act: Act, cause: Cause, got: &Reading) -> Option<String> {
    if got.half() {
        return Some(format!(
            "{act:?} {cause:?}: the half state, the request unmined beside a failed row with an input released: {got:?}"
        ));
    }
    let want_inputs = |zero: &str| vec![(false, zero.to_string()), (false, "own".to_string())];
    let ok = match cause {
        Cause::AbortFree | Cause::AbortSpentByOther => {
            let zero = if cause == Cause::AbortFree {
                "own"
            } else {
                "other"
            };
            got.row.as_deref() == Some("unproven")
                && got.request.as_deref() == Some("unmined")
                && got.inputs == want_inputs(zero)
                && got.own_output_spendable == Some(true)
                && got.taken_after_abort
                && got.balance == 500
        }
        // The guard holds: the proven spender's proof stands.
        Cause::AbortSpentByProven => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref() == Some("invalid")
                && got.inputs[0] == (false, "other".to_string())
                && !got.taken_after_abort
        }
        // The retire path's write after its refused promotion writes its own
        // `invalid` over the verdict's word, as it did.
        Cause::Competitor => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref()
                    == Some(if act == Act::Retire {
                        "invalid"
                    } else {
                        "doubleSpend"
                    })
                && got.inputs == want_inputs("own")
                && !got.taken_after_abort
        }
        // Its inputs still locked (the retire path's `failed`): a take does
        // not lift the row and the request keeps its word, raised only for
        // a live row of another user (the door's presenter); the unfail
        // task's recovery on chain evidence lifts it, as it did.
        Cause::Refusal => {
            let lifted = act == Act::Unfail;
            let request = match act {
                Act::Door | Act::Unfail => "unmined",
                _ => "invalid",
            };
            got.row.as_deref() == Some(if lifted { "unproven" } else { "failed" })
                && got.request.as_deref() == Some(request)
                && got.inputs == want_inputs("own")
                && !got.taken_after_abort
        }
        Cause::Received => {
            let lifted = act == Act::Unfail;
            let request = match act {
                Act::SendWaiting => "invalid",
                _ => "unmined",
            };
            got.row.as_deref() == Some(if lifted { "unproven" } else { "failed" })
                && got.request.as_deref() == Some(request)
                && got.inputs.iter().all(|(s, w)| !*s && w == "none")
                && !got.taken_after_abort
        }
        // The rule by state: `held_answer_release_take_tests.rs`.
        Cause::Unmarked | Cause::Evidence | Cause::Mixed => false,
    };
    (!ok).then(|| format!("{act:?} {cause:?}: {got:?}"))
}

async fn matrix(act: Act) {
    let mut wrong = Vec::new();
    for cause in CAUSES {
        let got = cell(act, cause).await;
        println!(
            "MATRIX {act:?} {cause:?} -> row {:?} request {:?} inputs {:?} own output {:?} takenAfterAbort {} takeNotCounted {} balance {} listActions {:?}",
            got.row,
            got.request,
            got.inputs,
            got.own_output_spendable,
            got.taken_after_abort,
            got.take_not_counted,
            got.balance,
            got.listed
        );
        if let Some(w) = judge(act, cause, &got) {
            wrong.push(w);
        }
    }
    assert!(wrong.is_empty(), "{act:?}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_take_at_the_door_after_the_hosts_abort_recovers_it() {
    matrix(Act::Door).await;
}

#[tokio::test]
async fn a_take_heard_by_send_waiting_after_the_hosts_abort_recovers_it() {
    matrix(Act::SendWaiting).await;
}

#[tokio::test]
async fn a_take_heard_by_the_retire_path_after_the_hosts_abort_recovers_it() {
    matrix(Act::Retire).await;
}

#[tokio::test]
async fn a_take_heard_by_unfail_after_the_hosts_abort_recovers_it() {
    matrix(Act::Unfail).await;
}

#[tokio::test]
async fn a_take_of_the_wallets_own_post_after_the_hosts_abort_recovers_it() {
    matrix(Act::OwnPost).await;
}

/// The recovered action reads `unproven` to the host, and a second take
/// recovers nothing twice.
#[tokio::test]
async fn the_host_reads_its_abort_undone_and_why() {
    let got = cell(Act::OwnPost, Cause::AbortFree).await;
    assert_eq!(got.listed.as_deref(), Some("Unproven"), "{got:?}");
    assert!(got.taken_after_abort, "{got:?}");
}

#[test]
fn the_matrix_names_every_act_and_cause() {
    assert_eq!(ACTS.len() * CAUSES.len(), 30);
}
