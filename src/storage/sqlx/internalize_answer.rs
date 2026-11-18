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

use super::internalize_action::is_refusal;

/// One instance's word.
pub(crate) fn instance_word(result: &PostBeefResult) -> InstanceWord {
    if result.is_success() {
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
pub(crate) fn held_status(results: &[PostBeefResult]) -> ReviewActionResultStatus {
    let words: Vec<_> = results.iter().map(instance_word).collect();
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
pub(crate) fn any_refusal(results: &[PostBeefResult]) -> bool {
    results
        .iter()
        .any(|r| matches!(instance_word(r), InstanceWord::SaidNo { .. }))
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
        assert_eq!(held_status(&[no.clone(), rival.clone()]), DoubleSpend);
        assert_eq!(held_status(&[silent.clone(), rival]), DoubleSpend);
        assert_eq!(held_status(&[no.clone(), no.clone()]), InvalidTx);
        assert_eq!(held_status(&[no, silent.clone()]), ServiceError);
        assert_eq!(held_status(&[silent]), ServiceError);
        assert_eq!(held_status(&[]), ServiceError);
        // A double-spend word that names nobody is a no, not a competitor.
        let unnamed = answer("d", "error", true, None);
        assert_eq!(held_status(&[unnamed.clone(), unnamed]), InvalidTx);
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
