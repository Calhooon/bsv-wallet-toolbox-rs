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

/// The bound on an instance's name in a note and in the message, in bytes.
pub(crate) const MAX_INSTANCE_NAME_BYTES: usize = 64;

/// Text an instance chose (or its name), cleaned and bounded: every
/// control character out (a newline, a carriage return, an escape), at most
/// `max` bytes, cut on a character boundary. The door writes its notes with
/// it and the wallet reads them with it again, so a storage server's words
/// are bounded and cleaned on arrival too.
pub(crate) fn clean_words(text: &str, max: usize) -> String {
    let cleaned: String = text.chars().filter(|c| !c.is_control()).collect();
    cut(&cleaned, max).to_string()
}

/// One instance's line of the message: its name cleaned, its words cleaned
/// and quoted, each `"` and `\` escaped, so no instance's words can read as
/// another instance's line; the words bounded to
/// [`MAX_INSTANCE_WORDS_BYTES`] before the quotes.
fn instance_line(instance: &str, word: &str) -> String {
    let mut quoted = String::new();
    for c in clean_words(word, MAX_INSTANCE_WORDS_BYTES).chars() {
        if c == '"' || c == '\\' {
            quoted.push('\\');
        }
        quoted.push(c);
    }
    format!(
        "{}: \"{quoted}\"",
        clean_words(instance, MAX_INSTANCE_NAME_BYTES)
    )
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

/// The door's notes on a request's history (`doorPost`, one per instance)
/// that name `user`, the user whose call this is, each instance's line
/// ([`instance_line`]). The storage result carries no words (a field there
/// would change the public result), so the words are read back from the
/// request: only this user's notes are read, never another user's
/// presentation's, and a call that made no post (no services, a
/// competitor's checked proof) finds none of its own and the message is
/// the reference's sentence alone. This user's earlier note for an
/// instance this call's post did not reach can still be read.
pub(crate) fn door_words_for(history: &str, user: i64) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(history) else {
        return Vec::new();
    };
    value
        .get("notes")
        .and_then(|n| n.as_array())
        .map(|notes| {
            notes
                .iter()
                .filter(|n| n.get("what").and_then(|w| w.as_str()) == Some("doorPost"))
                .filter(|n| n.get("user").and_then(|u| u.as_i64()) == Some(user))
                .map(|n| {
                    let text = |k: &str| n.get(k).and_then(|v| v.as_str()).unwrap_or("");
                    instance_line(text("instance"), text("word"))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The refusal's message: the reference's sentence, then each instance's
/// line ([`instance_line`], already bounded), the whole bounded to
/// [`MAX_MESSAGE_BYTES`].
pub(crate) fn message(sentence: &str, lines: &[String]) -> String {
    let mut message = sentence.to_string();
    for (i, line) in lines.iter().enumerate() {
        message.push_str(if i == 0 { " " } else { "; " });
        message.push_str(line);
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
        let lines: Vec<String> = (0..8)
            .map(|i| instance_line(&format!("i{i}"), &long))
            .collect();
        let text = message("The sentence.", &lines);
        assert_eq!(text.len(), MAX_MESSAGE_BYTES);
        assert!(text.starts_with("The sentence. i0: \"xxx"));
        let one = instance_line("a", &"é".repeat(300));
        assert!(one.len() <= "a: \"\"".len() + MAX_INSTANCE_WORDS_BYTES);
    }

    #[test]
    fn an_instances_words_are_cleaned_and_quoted() {
        let line = instance_line("a\nb", "no\r\n\u{7}; b: \"success\" \\ [ok]");
        assert_eq!(line, "ab: \"no; b: \\\"success\\\" \\\\ [ok]\"");
        assert!(!line.chars().any(char::is_control));
        let lossy = String::from_utf8_lossy(&[0xff, 0xfe, 0x07, b'o', b'k']).to_string();
        assert_eq!(clean_words(&lossy, 256), "\u{fffd}\u{fffd}ok");
    }

    #[test]
    fn the_door_notes_are_read() {
        let history = r#"{"notes":[{"what":"immediateBroadcastHint","words":["a: old"]},{"what":"doorPost","user":7,"instance":"a","word":"rejected [no]"},{"what":"other"},{"what":"doorPost","user":8,"instance":"c","word":"theirs"},{"what":"doorPost","user":7,"instance":"b","word":"465 []"}]}"#;
        assert_eq!(
            door_words_for(history, 7),
            vec![
                "a: \"rejected [no]\"".to_string(),
                "b: \"465 []\"".to_string()
            ]
        );
        assert!(door_words_for("{}", 7).is_empty());
        assert!(door_words_for(history, 9).is_empty());
    }
}
