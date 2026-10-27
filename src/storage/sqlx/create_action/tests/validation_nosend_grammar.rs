//! The BRC-177 label grammar's own refusals at the door (#359). The reference
//! parses every request carrying a `p nosend ` label
//! (ts-stack@edf6e03 `Wallet.ts:1162-1163`, `parseBrc177NoSendExpiryLabels`
//! at `brc177NoSendExpiry.ts:92-116`) ahead of the capability refusal
//! (`createNoSendExpiryAction.ts:201-203`), each refusal a
//! `WERR_INVALID_PARAMETER('labels', mustBe)` whose text is
//! `The labels parameter must be <mustBe>` (`WERR_errors.ts:105`), no full
//! stop. A label the grammar accepts still meets the capability refusal.

use super::super::create_action_internal;
use super::nosend_tests::{funded_store, no_send_args, transaction_rows};
use crate::error::Error;

/// `brc177NoSendExpiry.ts:97`.
const EXACTLY_ONE: &str = "The labels parameter must be exactly one BRC-177 noSend expiry label";
/// `brc177NoSendExpiry.ts:104`.
const VALID_LABEL: &str = "The labels parameter must be a valid BRC-177 noSend expiry label";
/// `brc177NoSendExpiry.ts:109`.
const SUPPORTED_MODE: &str = "The labels parameter must be a supported BRC-177 expiry mode";
/// `brc177NoSendExpiry.ts:83`.
const CANONICAL: &str = "The labels parameter must be a canonical unsigned BRC-177 expiry value";
/// `brc177NoSendExpiry.ts:87`.
const SAFE: &str = "The labels parameter must be a safely supported BRC-177 expiry value";
/// `brc177NoSendExpiry.ts:113`.
const SECONDS_ZERO: &str =
    "The labels parameter must be a BRC-177 seconds duration greater than zero";
/// `createNoSendExpiryAction.ts:202`.
const CAPABILITY: &str = "Active storage does not support BRC-177 noSend expiry";

/// The door refuses `labels` and writes no row; returns the error.
async fn refusal(labels: &[&str]) -> Error {
    let (storage, user_id) = funded_store().await;
    let before = transaction_rows(&storage).await;
    let result = create_action_internal(&storage, None, user_id, no_send_args(labels)).await;
    assert_eq!(transaction_rows(&storage).await, before, "{labels:?}");
    match result {
        Err(e) => e,
        Ok(_) => panic!("{labels:?}: the action was created"),
    }
}

/// The grammar refuses `labels` with `text`, byte for byte.
async fn assert_grammar(labels: &[&str], text: &str) {
    match refusal(labels).await {
        Error::ValidationError(message) => assert_eq!(message, text, "{labels:?}"),
        other => panic!("{labels:?}: refused with another error: {other}"),
    }
}

/// The grammar accepts `labels`; the capability refusal answers.
async fn assert_capability(labels: &[&str]) {
    match refusal(labels).await {
        Error::InvalidOperation(message) => assert_eq!(message, CAPABILITY, "{labels:?}"),
        other => panic!("{labels:?}: refused with another error: {other}"),
    }
}

// :96-98, a module label with no expiry label, two expiry labels, two module
// labels.

#[tokio::test]
async fn batch_label_alone_needs_exactly_one_expiry_label() {
    assert_grammar(&["p nosend batch"], EXACTLY_ONE).await;
}

#[tokio::test]
async fn bare_expiry_word_needs_exactly_one_expiry_label() {
    assert_grammar(&["p nosend expiry"], EXACTLY_ONE).await;
}

#[tokio::test]
async fn second_expiry_label_needs_exactly_one_expiry_label() {
    assert_grammar(
        &["p nosend expiry seconds 300", "p nosend expiry seconds 600"],
        EXACTLY_ONE,
    )
    .await;
    assert_grammar(
        &["p nosend expiry seconds 300", "p nosend expiry seconds 300"],
        EXACTLY_ONE,
    )
    .await;
}

#[tokio::test]
async fn two_module_labels_need_exactly_one_expiry_label() {
    assert_grammar(
        &["p nosend expiry seconds 300", "p nosend batch"],
        EXACTLY_ONE,
    )
    .await;
}

// :103-104, the separator.

#[tokio::test]
async fn expiry_without_separator_is_not_a_valid_label() {
    assert_grammar(&["p nosend expiry never"], VALID_LABEL).await;
    assert_grammar(&["p nosend expiry seconds"], VALID_LABEL).await;
}

/// The SDK trims the label first, so a trailing separator leaves none.
#[tokio::test]
async fn trailing_separator_is_trimmed_and_not_a_valid_label() {
    assert_grammar(&["p nosend expiry seconds "], VALID_LABEL).await;
}

#[tokio::test]
async fn empty_mode_is_not_a_valid_label() {
    assert_grammar(&["p nosend expiry  300"], VALID_LABEL).await;
}

#[tokio::test]
async fn second_space_is_not_a_valid_label() {
    assert_grammar(&["p nosend expiry seconds 300 extra"], VALID_LABEL).await;
    assert_grammar(&["p nosend expiry seconds  300"], VALID_LABEL).await;
}

// :108-109, the mode.

#[tokio::test]
async fn unknown_mode_is_not_supported() {
    assert_grammar(&["p nosend expiry weeks 3"], SUPPORTED_MODE).await;
}

/// The mode is checked ahead of the value.
#[tokio::test]
async fn unknown_mode_is_refused_ahead_of_its_value() {
    assert_grammar(&["p nosend expiry weeks 03"], SUPPORTED_MODE).await;
}

// :81-90, the canonical unsigned integer.

#[tokio::test]
async fn leading_zero_is_not_canonical() {
    assert_grammar(&["p nosend expiry seconds 0300"], CANONICAL).await;
    assert_grammar(&["p nosend expiry timestamp 00"], CANONICAL).await;
}

#[tokio::test]
async fn sign_is_not_canonical() {
    assert_grammar(&["p nosend expiry seconds +300"], CANONICAL).await;
    assert_grammar(&["p nosend expiry blockheight -1"], CANONICAL).await;
}

#[tokio::test]
async fn non_digits_are_not_canonical() {
    assert_grammar(&["p nosend expiry seconds 3e2"], CANONICAL).await;
    assert_grammar(&["p nosend expiry seconds 1.5"], CANONICAL).await;
    assert_grammar(&["p nosend expiry seconds 0x10"], CANONICAL).await;
    assert_grammar(&["p nosend expiry seconds ３００"], CANONICAL).await;
}

/// `Number.isSafeInteger`: past 2^53 - 1, and past u64 too.
#[tokio::test]
async fn unsafe_integer_is_not_safely_supported() {
    assert_grammar(&["p nosend expiry seconds 9007199254740992"], SAFE).await;
    assert_grammar(&["p nosend expiry timestamp 99999999999999999999999"], SAFE).await;
}

// :112-113, a zero duration.

#[tokio::test]
async fn zero_seconds_is_refused() {
    assert_grammar(&["p nosend expiry seconds 0"], SECONDS_ZERO).await;
}

// Labels the grammar accepts meet the capability refusal, as before #359.

#[tokio::test]
async fn grammatical_labels_still_meet_the_capability_refusal() {
    assert_capability(&["p nosend expiry seconds 300"]).await;
    assert_capability(&["p nosend expiry timestamp 1900000000"]).await;
    assert_capability(&["p nosend expiry blockheight 900000"]).await;
    assert_capability(&["p nosend expiry timestamp 0"]).await;
    assert_capability(&["p nosend expiry blockheight 9007199254740991"]).await;
    assert_capability(&["invoice", "P NoSend Expiry Seconds 300"]).await;
}
