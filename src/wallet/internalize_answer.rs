//! The wallet's answer to an internalize the storage door holds (the
//! owner's ruling of 2026-11-17, DECISIONS 29.1, and the keeper's readings
//! 1 and 10 of section 30).
//!
//! The storage result's `send_with_results` and `not_delayed_results`
//! (the reference's `sendWithResults` and `notDelayedResults`) come back
//! filled when no broadcaster took the transaction. Whenever a send result
//! is not `unproven`, the wallet raises the review-actions refusal
//! (`Error::ReviewActions`, bsv-rs's `WalletReviewActions`, wire code 5)
//! with the results and the txid, as the reference's
//! `throwIfUnsuccessfulInternalizeAction` does (ts-stack@11faa7d1
//! `Wallet.ts:1604-1612`). The message is the reference's sentence, then
//! each instance's words as the door recorded them, bounded: each
//! instance's words to [`MAX_INSTANCE_WORDS_BYTES`], the message to
//! [`MAX_MESSAGE_BYTES`]. A host reads the class and the statuses, never
//! the message.

use crate::storage::{
    ReviewActionResult, ReviewActionResultStatus, SendWithResult, StorageInternalizeActionResult,
};

/// The bound on one instance's words in the refusal's message, in bytes.
pub const MAX_INSTANCE_WORDS_BYTES: usize = 256;

/// The bound on the refusal's whole message, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 1024;

/// `text` cut to at most `max` bytes on a character boundary.
fn cut(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// A hex txid as the SDK's 32 bytes (all zeros for a malformed one, as the
/// create door's send results read it).
fn txid_bytes(txid: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    if let Ok(bytes) = hex::decode(txid) {
        if bytes.len() == 32 {
            out.copy_from_slice(&bytes);
        }
    }
    out
}

fn send_result(r: &SendWithResult) -> bsv_rs::wallet::SendWithResult {
    use bsv_rs::wallet::SendWithResultStatus as Send;
    bsv_rs::wallet::SendWithResult {
        txid: txid_bytes(&r.txid),
        status: match r.status.as_str() {
            "unproven" => Send::Unproven,
            "sending" => Send::Sending,
            _ => Send::Failed,
        },
    }
}

fn review_result(r: &ReviewActionResult) -> bsv_rs::wallet::ReviewActionResult {
    use bsv_rs::wallet::ReviewActionResultStatus as Review;
    bsv_rs::wallet::ReviewActionResult {
        txid: txid_bytes(&r.txid),
        status: match r.status {
            ReviewActionResultStatus::Success => Review::Success,
            ReviewActionResultStatus::DoubleSpend => Review::DoubleSpend,
            ReviewActionResultStatus::ServiceError => Review::ServiceError,
            ReviewActionResultStatus::InvalidTx => Review::InvalidTx,
        },
        competing_txs: r
            .competing_txs
            .as_ref()
            .map(|txids| txids.iter().map(|t| txid_bytes(t)).collect()),
        competing_beef: r.competing_beef.clone(),
    }
}

/// The words of the last door note on a request's history
/// (`immediateBroadcastHint`), each instance's a string.
pub(crate) fn last_door_words(history: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(history) else {
        return Vec::new();
    };
    value
        .get("notes")
        .and_then(|n| n.as_array())
        .and_then(|notes| {
            notes
                .iter()
                .rev()
                .find(|n| n.get("what").and_then(|w| w.as_str()) == Some("immediateBroadcastHint"))
        })
        .and_then(|note| note.get("words"))
        .and_then(|w| w.as_array())
        .map(|words| {
            words
                .iter()
                .map(|w| match w.as_str() {
                    Some(text) => text.to_string(),
                    None => w.to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The refusal's message: the reference's sentence, then each instance's
/// words, bounded.
pub(crate) fn message(sentence: &str, words: &[String]) -> String {
    let mut message = sentence.to_string();
    for (i, word) in words.iter().enumerate() {
        message.push_str(if i == 0 { " " } else { "; " });
        message.push_str(cut(word, MAX_INSTANCE_WORDS_BYTES));
    }
    cut(&message, MAX_MESSAGE_BYTES).to_string()
}

/// The review-actions refusal for a storage result whose send results are
/// not all `unproven`, else `None` (the call answers `accepted: true`).
pub(crate) fn held_refusal(
    result: &StorageInternalizeActionResult,
    words: &[String],
) -> Option<bsv_rs::wallet::ReviewActionsRefusal> {
    let sends = result.send_with_results.as_ref()?;
    if sends.iter().all(|r| r.status == "unproven") {
        return None;
    }
    let mut refusal = bsv_rs::wallet::ReviewActionsRefusal::with_members(
        result
            .not_delayed_results
            .iter()
            .flatten()
            .map(review_result)
            .collect(),
        sends.iter().map(send_result).collect(),
        Some(txid_bytes(&result.txid)),
        None,
        None,
    );
    refusal.message = message(&refusal.message, words);
    Some(refusal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_is_bounded() {
        let long = "x".repeat(4 * MAX_INSTANCE_WORDS_BYTES);
        let words = vec![long; 8];
        let text = message("The sentence.", &words);
        assert_eq!(text.len(), MAX_MESSAGE_BYTES);
        assert!(text.starts_with("The sentence. xxx"));
        let one = message("S.", &["é".repeat(300)]);
        assert!(one.len() <= 3 + MAX_INSTANCE_WORDS_BYTES);
    }

    #[test]
    fn the_last_door_note_is_read() {
        let history = r#"{"notes":[{"what":"immediateBroadcastHint","words":["a: old"]},{"what":"other"},{"what":"immediateBroadcastHint","words":["a: rejected [no]","b: 465 []"]}]}"#;
        assert_eq!(
            last_door_words(history),
            vec!["a: rejected [no]".to_string(), "b: 465 []".to_string()]
        );
        assert!(last_door_words("{}").is_empty());
    }
}
