//! The create label bound through `WalletInterface` (#386).
//! ts-stack@edf6e03 validationHelpers.ts:762 maps labels through validateLabel
//! (385-387, 409-417) before options and the BRC-177 grammar in Wallet.ts:1163.

use super::description_tests::{assert_refused, wallet};
use super::nosend_tests::{no_send_args, transaction_rows};
use bsv_rs::wallet::{ListActionsArgs, WalletInterface};

const TOO_SHORT: &str = "Validation error: The label parameter must be at least 1 length.";
const TOO_LONG: &str = "Validation error: The label parameter must be no more than 300 length.";

async fn assert_label_accepted(label: &str) {
    let wallet = wallet().await;
    let before = transaction_rows(wallet.storage()).await;
    let created = WalletInterface::create_action(&wallet, no_send_args(&[label]), None)
        .await
        .unwrap();
    assert!(created.signable_transaction.is_some());
    assert_eq!(transaction_rows(wallet.storage()).await, before + 1);
    let stored: String = sqlx::query_scalar("SELECT label FROM tx_labels")
        .fetch_one(wallet.storage().pool())
        .await
        .unwrap();
    assert_eq!(stored, label);

    let query: ListActionsArgs = serde_json::from_value(serde_json::json!({
        "labels": [label],
        "includeLabels": true,
    }))
    .unwrap();
    let listed = WalletInterface::list_actions(&wallet, query, None)
        .await
        .unwrap();
    assert_eq!(listed.total_actions, 1);
    assert_eq!(listed.actions[0].labels, Some(vec![label.to_string()]));
}

#[tokio::test]
async fn create_label_refuses_spaces_with_reference_text() {
    assert_refused(no_send_args(&["   "]), TOO_SHORT).await;
}

#[tokio::test]
async fn create_label_refuses_byte_order_mark_with_reference_text() {
    assert_refused(no_send_args(&["\u{FEFF}"]), TOO_SHORT).await;
}

#[tokio::test]
async fn create_label_refuses_301_utf8_bytes_with_reference_text() {
    let multibyte = format!("{}a", "é".repeat(150));
    assert_eq!(multibyte.len(), 301);
    assert_refused(no_send_args(&[&multibyte]), TOO_LONG).await;
    assert_refused(no_send_args(&[&"a".repeat(301)]), TOO_LONG).await;
}

#[tokio::test]
async fn create_label_accepts_300_utf8_bytes() {
    assert_label_accepted(&"a".repeat(300)).await;
    assert_label_accepted(&"é".repeat(150)).await;
}

#[tokio::test]
async fn create_label_keeps_supplied_form_in_storage_and_listing() {
    assert_label_accepted("  MiXeD Label  ").await;
    assert_label_accepted("\u{FEFF}\t MiXeD Label \u{3000}\n").await;
    assert_label_accepted("\u{0085}").await;
}

#[tokio::test]
async fn create_label_measures_bytes_after_trimming() {
    assert_label_accepted(&format!("\u{FEFF} {} \u{3000}", "é".repeat(150))).await;
}

#[tokio::test]
async fn create_label_measures_bytes_after_lowercasing() {
    // U+0130 expands from two UTF-8 bytes to three when lowercased.
    assert_label_accepted(&"İ".repeat(100)).await;
    assert_refused(no_send_args(&[&"İ".repeat(101)]), TOO_LONG).await;
}

#[tokio::test]
async fn create_label_validation_precedes_brc177_grammar_and_capability() {
    for brc177 in ["p nosend batch", "p nosend expiry seconds 300"] {
        assert_refused(no_send_args(&[brc177, "   "]), TOO_SHORT).await;
        assert_refused(no_send_args(&["\u{FEFF}", brc177]), TOO_SHORT).await;
    }
}
