//! What the internalize door answers for a held presentation (the owner's
//! ruling of 2026-11-17, DECISIONS 29.1, and the keeper's readings 1 and 10
//! of section 30).
//!
//! The door posts a held presentation once to each named instance. Each
//! instance's answer is one of three: it TOOK the transaction (an
//! acceptance), it SAID NO (a refusal: a definitive rejection, an invalid
//! word or a double-spend word, possibly naming a competitor), or it DID NOT
//! ANSWER (a transport fault, a service error, an orphan-mempool hold, a
//! word that is neither). Taken by any instance, or held by a status source
//! after a refusal: `accepted: true`. Taken by none, the transaction stays
//! held (`sending`) and the storage result carries:
//!
//! - `send_with_results`: the txid with `sending` (never `failed` at this
//!   door: no broadcaster's word writes `failed`);
//! - `not_delayed_results`: the txid with `doubleSpend` when any refusal
//!   named a competitor (the competitors in `competing_txs`), `invalidTx`
//!   only when every named instance said no and none named a competitor,
//!   otherwise `serviceError` (no instance answered, or some said no and
//!   some did not answer).
//!
//! The wallet raises the review-actions refusal from these two fields.

// The refusal predicate stays beside the door (`internalize_action.rs`),
// where the architecture ratchet counts its two text sites.
use super::internalize_action::is_refusal;
use crate::services::PostBeefResult;
use crate::storage::traits::{ReviewActionResult, ReviewActionResultStatus, SendWithResult};

/// The door's post of a held presentation, classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DoorAnswer {
    /// An instance took it, or a status source holds it.
    Taken,
    /// Taken by nobody: the review status and any competitors named.
    Held {
        status: ReviewActionResultStatus,
        competitors: Vec<String>,
    },
    /// A competitor's checked proof stands against it (reading 11's second
    /// case, reading 7's answer): `doubleSpend` naming the competitor (none
    /// when the store keeps the proof's verdict alone), the send result
    /// `failed`.
    Refuted { competitor: Option<String> },
}

impl DoorAnswer {
    /// No instance answered (the post faulted, or no services to post to).
    pub(crate) fn no_answer() -> Self {
        Self::Held {
            status: ReviewActionResultStatus::ServiceError,
            competitors: Vec::new(),
        }
    }

    /// The storage result's two fields: `None` for a take, else the send
    /// result `sending` and the review result for `txid`.
    pub(crate) fn results(
        &self,
        txid: &str,
    ) -> (Option<Vec<SendWithResult>>, Option<Vec<ReviewActionResult>>) {
        match self {
            Self::Taken => (None, None),
            Self::Held {
                status,
                competitors,
            } => (
                Some(vec![SendWithResult {
                    txid: txid.to_string(),
                    status: "sending".to_string(),
                }]),
                Some(vec![ReviewActionResult {
                    txid: txid.to_string(),
                    status: *status,
                    competing_txs: (!competitors.is_empty()).then(|| competitors.clone()),
                    competing_beef: None,
                }]),
            ),
            Self::Refuted { competitor } => (
                Some(vec![SendWithResult {
                    txid: txid.to_string(),
                    status: "failed".to_string(),
                }]),
                Some(vec![ReviewActionResult {
                    txid: txid.to_string(),
                    status: ReviewActionResultStatus::DoubleSpend,
                    competing_txs: competitor.clone().map(|c| vec![c]),
                    competing_beef: None,
                }]),
            ),
        }
    }
}

/// One instance's answer to the door's post.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstanceWord {
    Took,
    SaidNo { named_competitor: bool },
    NoAnswer,
}

/// The word of a review status, as the request's history records the
/// door's outcome: the history's word and the answer's status are one.
pub(crate) fn status_word(status: ReviewActionResultStatus) -> &'static str {
    match status {
        ReviewActionResultStatus::Success => "success",
        ReviewActionResultStatus::DoubleSpend => "doubleSpend",
        ReviewActionResultStatus::InvalidTx => "invalidTx",
        ReviewActionResultStatus::ServiceError => "serviceError",
    }
}

/// The instances whose word in `words` accepted `txid`: an acceptance with
/// no per-txid result, or one whose result for `txid` is a success. The one
/// reader of a take in an instance's answer: the door, the wallet's own
/// post (`process_action`), the send-waiting task and [`instance_word`]
/// read it.
pub(crate) fn accepting_instances<'a>(words: &'a [PostBeefResult], txid: &str) -> Vec<&'a str> {
    words
        .iter()
        .filter(|w| takes(w, txid))
        .map(|w| w.name.as_str())
        .collect()
}

/// Did `result` take `txid` ([`accepting_instances`]'s rule)?
fn takes(result: &PostBeefResult, txid: &str) -> bool {
    result.is_success()
        && (result.txid_results.is_empty()
            || result
                .txid_results
                .iter()
                .any(|r| r.txid == txid && r.is_success()))
}

/// One instance's word on `txid`.
pub(crate) fn instance_word(result: &PostBeefResult, txid: &str) -> InstanceWord {
    if takes(result, txid) {
        return InstanceWord::Took;
    }
    let refusals: Vec<_> = result
        .txid_results
        .iter()
        .filter(|tr| is_refusal(tr))
        .collect();
    if refusals.is_empty() {
        return InstanceWord::NoAnswer;
    }
    InstanceWord::SaidNo {
        named_competitor: refusals
            .iter()
            .any(|tr| tr.competing_txs.as_ref().is_some_and(|c| !c.is_empty())),
    }
}

/// The review status of a post no instance took (reading 10): `doubleSpend`
/// when any refusal named a competitor; `invalidTx` only when every named
/// instance said no; otherwise `serviceError`.
pub(crate) fn held_status(results: &[PostBeefResult], txid: &str) -> ReviewActionResultStatus {
    let words: Vec<_> = results.iter().map(|r| instance_word(r, txid)).collect();
    if words.iter().any(|w| {
        matches!(
            w,
            InstanceWord::SaidNo {
                named_competitor: true
            }
        )
    }) {
        ReviewActionResultStatus::DoubleSpend
    } else if !words.is_empty()
        && words
            .iter()
            .all(|w| matches!(w, InstanceWord::SaidNo { .. }))
    {
        ReviewActionResultStatus::InvalidTx
    } else {
        ReviewActionResultStatus::ServiceError
    }
}

/// Whether any instance said no (the door then asks the status sources).
pub(crate) fn any_refusal(results: &[PostBeefResult], txid: &str) -> bool {
    results
        .iter()
        .any(|r| matches!(instance_word(r, txid), InstanceWord::SaidNo { .. }))
}

/// The competitors the refusals named, each once, in order.
pub(crate) fn named_competitors(results: &[PostBeefResult]) -> Vec<String> {
    let mut named = Vec::new();
    for tr in results
        .iter()
        .flat_map(|r| r.txid_results.iter())
        .filter(|tr| is_refusal(tr))
    {
        for competitor in tr.competing_txs.iter().flatten() {
            if !named.contains(competitor) {
                named.push(competitor.clone());
            }
        }
    }
    named
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One instance's word on one txid: `status`, a double-spend word
    /// naming `competitor` when given, a service error for `serviceError`.
    fn answer(
        name: &str,
        status: &str,
        double_spend: bool,
        competitor: Option<&str>,
    ) -> PostBeefResult {
        let mut r =
            crate::services::mock::double_spend_post_beef_result(name, &"aa".repeat(32), "cc");
        r.status = if status == "success" {
            "success"
        } else {
            "error"
        }
        .to_string();
        let tr = &mut r.txid_results[0];
        tr.status = status.to_string();
        tr.double_spend = double_spend;
        tr.competing_txs = competitor.map(|c| vec![c.to_string()]);
        tr.service_error = status == "serviceError";
        r
    }

    #[test]
    fn the_three_arms_of_the_held_status() {
        use ReviewActionResultStatus::*;
        let no = answer("a", "rejected", false, None);
        let rival = answer("b", "error", true, Some("cc"));
        let silent = answer("c", "serviceError", false, None);
        assert_eq!(
            held_status(&[no.clone(), rival.clone()], &"aa".repeat(32)),
            DoubleSpend
        );
        assert_eq!(
            held_status(&[silent.clone(), rival], &"aa".repeat(32)),
            DoubleSpend
        );
        assert_eq!(
            held_status(&[no.clone(), no.clone()], &"aa".repeat(32)),
            InvalidTx
        );
        assert_eq!(
            held_status(&[no, silent.clone()], &"aa".repeat(32)),
            ServiceError
        );
        assert_eq!(held_status(&[silent], &"aa".repeat(32)), ServiceError);
        assert_eq!(held_status(&[], &"aa".repeat(32)), ServiceError);
        // A double-spend word that names nobody is a no, not a competitor.
        let unnamed = answer("d", "error", true, None);
        assert_eq!(
            held_status(&[unnamed.clone(), unnamed], &"aa".repeat(32)),
            InvalidTx
        );
    }

    #[test]
    fn the_history_word_is_the_answer_status() {
        let txid = "aa".repeat(32);
        let unnamed = answer("d", "error", true, None);
        let no = answer("a", "rejected", false, None);
        let silent = answer("c", "serviceError", false, None);
        // An unnamed double-spend word and a mix of refusals and silence:
        // the two arms where the history's word once differed.
        assert_eq!(status_word(held_status(&[unnamed], &txid)), "invalidTx");
        assert_eq!(
            status_word(held_status(&[no, silent], &txid)),
            "serviceError"
        );
    }

    #[test]
    fn a_held_answer_is_sending_never_failed() {
        let (send, review) = DoorAnswer::no_answer().results("ab");
        assert_eq!(send.unwrap()[0].status, "sending");
        assert_eq!(
            review.unwrap()[0].status,
            ReviewActionResultStatus::ServiceError
        );
        assert_eq!(DoorAnswer::Taken.results("ab").0.map(|v| v.len()), None);
    }
}
