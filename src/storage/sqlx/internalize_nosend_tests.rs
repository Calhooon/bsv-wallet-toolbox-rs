//! The BRC-177 `nosend` module at the internalize door (#357). The reference
//! refuses any label starting `p nosend ` on internalizeAction
//! (ts-stack@edf6e03 `Wallet.ts:1248-1250`, `startsWith` at
//! `brc177NoSendExpiry.ts:119`) after validateInternalizeActionArgs has
//! trimmed and lowercased every label (validationHelpers.ts:1054), so the
//! label never lands as metadata on an incoming action.

use super::tests::{
    create_test_atomic_beef, create_test_storage, create_test_user, wallet_payment_args,
};
use super::*;

/// The reference's refusal, byte for byte (`WERR_errors.ts:105`).
const REFUSAL: &str =
    "The labels parameter must be BRC-177 noSend expiry labels only on outgoing createAction requests";

/// The rows an internalize writes: its action, its outputs, its labels.
async fn rows(storage: &StorageSqlx) -> Vec<i64> {
    let mut counts = Vec::new();
    for table in ["transactions", "outputs", "tx_labels", "tx_labels_map"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(storage.pool())
            .await
            .unwrap();
        counts.push(n);
    }
    counts
}

async fn internalize(labels: &[&str]) -> (StorageSqlx, Vec<i64>, Result<()>) {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    let before = rows(&storage).await;
    let (bytes, _txid, _satoshis) = create_test_atomic_beef();
    let mut args = wallet_payment_args(bytes, "a BRC-177 label test");
    args.labels = Some(labels.iter().map(ToString::to_string).collect());
    let result = internalize_action_internal(&storage, user_id, args)
        .await
        .map(|_| ());
    (storage, before, result)
}

/// The door refuses `labels` with the reference's text and writes no row.
async fn assert_refused(labels: &[&str]) {
    assert_refused_with(labels, REFUSAL).await;
}

/// The door refuses `labels` with `text` and writes no row.
async fn assert_refused_with(labels: &[&str], text: &str) {
    let (storage, before, result) = internalize(labels).await;
    match result {
        Err(Error::ValidationError(message)) => assert_eq!(message, text, "{labels:?}"),
        Err(other) => panic!("{labels:?}: refused with another error: {other}"),
        Ok(()) => panic!("{labels:?}: the action was internalized"),
    }
    assert_eq!(rows(&storage).await, before, "{labels:?}");
}

/// The door internalizes the action with `label` as ordinary metadata.
async fn assert_internalized(label: &str) {
    let (storage, _before, result) = internalize(&[label]).await;
    result.unwrap_or_else(|e| panic!("{label:?}: {e}"));
    let stored: Vec<String> = sqlx::query_scalar("SELECT label FROM tx_labels")
        .fetch_all(storage.pool())
        .await
        .unwrap();
    assert_eq!(stored, vec![label.to_string()]);
}

#[tokio::test]
async fn seconds_expiry_label_is_refused_on_internalize() {
    assert_refused(&["p nosend expiry seconds 300"]).await;
}

#[tokio::test]
async fn nosend_label_is_matched_trimmed_and_lowercased() {
    assert_refused(&["  P NOSEND expiry seconds 300 "]).await;
}

#[tokio::test]
async fn nosend_label_beside_an_ordinary_label_is_refused() {
    assert_refused(&["invoice", "p nosend batch"]).await;
}

#[tokio::test]
async fn label_continuing_the_word_is_ordinary_metadata() {
    assert_internalized("p nosendx").await;
}

#[tokio::test]
async fn bare_module_word_is_ordinary_metadata() {
    // No trailing space: the prefix `p nosend ` is not matched.
    assert_internalized("p nosend").await;
}

/// validateLabel's texts (ts-stack@edf6e03 validationHelpers.ts:385-387 and
/// 409-417, `WERR_errors.ts:105`): 1 to 300 UTF-8 bytes once trimmed and
/// lowercased.
const LABEL_TOO_LONG: &str = "The label parameter must be no more than 300 length.";
const LABEL_EMPTY: &str = "The label parameter must be at least 1 length.";

/// #365: validateInternalizeActionArgs maps every label through
/// validateLabel (validationHelpers.ts:1054) before Wallet.ts:1248-1250
/// refuses a `p nosend ` label, so the over-long label answers, on either
/// side of the BRC-177 label.
#[tokio::test]
async fn over_long_label_answers_ahead_of_the_nosend_refusal() {
    let long = "a".repeat(301);
    assert_refused_with(&["p nosend expiry seconds 300", &long], LABEL_TOO_LONG).await;
    assert_refused_with(&[&long, "p nosend expiry seconds 300"], LABEL_TOO_LONG).await;
}

#[tokio::test]
async fn label_bound_is_bytes_of_the_trimmed_label() {
    assert_refused_with(&[&"a".repeat(301)], LABEL_TOO_LONG).await;
    // 151 two-byte characters are 302 bytes.
    assert_refused_with(&[&"\u{e9}".repeat(151)], LABEL_TOO_LONG).await;
    assert_refused_with(&["   "], LABEL_EMPTY).await;
    assert_refused_with(&[""], LABEL_EMPTY).await;
    assert_internalized(&format!(" {} ", "a".repeat(300))).await;
}

/// #365: the reference refuses a wallet payment whose derivation does not
/// lock the output (`signer/methods/internalizeAction.ts:84-89`) only after
/// the BRC-177 refusal (Wallet.ts:1248-1252). The door derives no key, so a
/// well-formed derivation that locks nothing of ours meets the BRC-177 text.
#[tokio::test]
async fn nosend_refusal_answers_ahead_of_a_derivation_that_locks_nothing() {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    let before = rows(&storage).await;
    let (bytes, _txid, _satoshis) = create_test_atomic_beef();
    let mut args = wallet_payment_args(bytes, "a BRC-177 label test");
    args.outputs[0].payment_remittance = Some(bsv_rs::wallet::WalletPayment {
        derivation_prefix: "cHJlZml4".to_string(),
        derivation_suffix: "c3VmZml4".to_string(),
        sender_identity_key: SENDER.to_string(),
    });
    args.labels = Some(vec!["p nosend expiry seconds 300".to_string()]);
    match internalize_action_internal(&storage, user_id, args).await {
        Err(Error::ValidationError(message)) => assert_eq!(message, REFUSAL),
        Err(other) => panic!("refused with another error: {other}"),
        Ok(_) => panic!("the action was internalized"),
    }
    assert_eq!(rows(&storage).await, before);
}

/// A valid compressed public key (the secp256k1 generator).
const SENDER: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

/// #379: validateLabel trims with ECMAScript's `String.prototype.trim`
/// (WhiteSpace and LineTerminator): U+FEFF trims to nothing and is refused
/// as empty; U+0085 is kept, 2 bytes, and the label is stored as given.
#[tokio::test]
async fn byte_order_mark_label_is_refused_as_empty() {
    assert_refused_with(&["\u{FEFF}"], LABEL_EMPTY).await;
}

#[tokio::test]
async fn next_line_label_is_internalized() {
    assert_internalized("\u{0085}").await;
}

/// #379: the BRC-177 match reads the label as ECMAScript's trim leaves it.
#[tokio::test]
async fn nosend_label_behind_a_byte_order_mark_is_refused() {
    assert_refused(&["\u{FEFF}p nosend expiry seconds 300"]).await;
}

#[tokio::test]
async fn nosend_label_behind_a_next_line_is_ordinary_metadata() {
    assert_internalized("\u{0085}p nosend batch").await;
}

/// #379: the whitespace both trims strip is stripped as before.
#[tokio::test]
async fn label_bound_trims_shared_whitespace() {
    assert_refused_with(&["\t\u{A0}\u{3000}\n"], LABEL_EMPTY).await;
    assert_refused(&["\u{2028}P NOSEND batch\u{2029}"]).await;
}
