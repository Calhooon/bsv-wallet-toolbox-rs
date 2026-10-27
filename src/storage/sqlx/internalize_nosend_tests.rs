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
    let (storage, before, result) = internalize(labels).await;
    match result {
        Err(Error::ValidationError(message)) => assert_eq!(message, REFUSAL, "{labels:?}"),
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
