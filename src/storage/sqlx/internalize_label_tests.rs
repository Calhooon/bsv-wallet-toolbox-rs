//! Stored and filtered labels follow validateLabel's return value (#392).
//! ts-stack@edf6e03 validationHelpers.ts:1054,1691; internalizeAction.ts:742-744;
//! listActionsKnex.ts:127-140. Existing rows retain their stored spelling.

use super::tests::{
    create_test_atomic_beef, create_test_storage, create_test_user, wallet_payment_args,
};
use super::*;
use crate::storage::AuthId;
use bsv_rs::wallet::{ListActionsArgs, QueryMode};

fn query(labels: &[&str], mode: QueryMode) -> ListActionsArgs {
    serde_json::from_value(serde_json::json!({
        "labels": labels,
        "labelQueryMode": mode,
        "includeLabels": true,
    }))
    .unwrap()
}

async fn stored_labels(storage: &StorageSqlx) -> Vec<String> {
    sqlx::query_scalar("SELECT label FROM tx_labels ORDER BY label")
        .fetch_all(storage.pool())
        .await
        .unwrap()
}

async fn receive(labels: &[&str]) -> (StorageSqlx, AuthId, InternalizeActionArgs) {
    let storage = create_test_storage().await;
    let user_id = create_test_user(&storage).await;
    let (bytes, _, _) = create_test_atomic_beef();
    let mut args = wallet_payment_args(bytes, "Stored label fixture");
    args.labels = Some(labels.iter().map(ToString::to_string).collect());
    internalize_action_internal(&storage, user_id, args.clone())
        .await
        .unwrap();
    (
        storage,
        AuthId::with_user_id("label-fixture", user_id),
        args,
    )
}

#[tokio::test]
async fn internalize_stored_labels_are_normalized_on_insert_and_merge() {
    let (storage, auth, mut args) = receive(&["  Label ", "\u{FEFF}LABEL\u{3000}"]).await;
    assert_eq!(stored_labels(&storage).await, ["label"]);

    args.labels = Some(vec![" LABEL ".into(), "\u{FEFF} SECOND \u{3000}".into()]);
    let merged = internalize_action_internal(&storage, auth.user_id.unwrap(), args)
        .await
        .unwrap();
    assert!(merged.is_merge);
    assert_eq!(stored_labels(&storage).await, ["label", "second"]);
    let maps: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tx_labels_map")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(maps, 2);
    let listed = storage
        .list_actions(&auth, query(&[], QueryMode::Any))
        .await
        .unwrap();
    assert_eq!(listed.total_actions, 1);
    let mut labels = listed.actions[0].labels.clone().unwrap();
    labels.sort();
    assert_eq!(labels, ["label", "second"]);
}

#[tokio::test]
async fn stored_label_filters_normalize_in_any_and_all_modes() {
    let (storage, auth, _) = receive(&["label", "second"]).await;
    for (labels, mode, count) in [
        (vec!["LABEL", "missing"], QueryMode::Any, 1),
        (
            vec!["\u{FEFF} Label \u{3000}", " SECOND "],
            QueryMode::All,
            1,
        ),
        (vec!["LABEL", "missing"], QueryMode::All, 0),
        // The reference compares the number of found ids to the filter length.
        (vec!["label", " LABEL "], QueryMode::All, 0),
    ] {
        let listed = storage
            .list_actions(&auth, query(&labels, mode))
            .await
            .unwrap();
        assert_eq!(listed.total_actions, count, "{labels:?}");
        if count > 0 {
            let mut labels = listed.actions[0].labels.clone().unwrap();
            labels.sort();
            assert_eq!(labels, ["label", "second"]);
        }
    }
}

#[tokio::test]
async fn stored_label_filters_validate_the_normalized_byte_bound() {
    let (storage, auth, _) = receive(&["label"]).await;
    for (label, text) in [
        ("\u{FEFF} \t".to_string(), "at least 1 length."),
        ("İ".repeat(101), "no more than 300 length."),
    ] {
        let error = storage
            .list_actions(&auth, query(&[&label], QueryMode::Any))
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("Validation error: The label parameter must be {text}")
        );
    }
}

#[tokio::test]
async fn stored_label_legacy_rows_keep_their_spelling_and_do_not_match() {
    let (storage, auth, _) = receive(&["label"]).await;
    sqlx::query("UPDATE tx_labels SET label = '  LaBeL '")
        .execute(storage.writer())
        .await
        .unwrap();

    let listed = storage
        .list_actions(&auth, query(&[], QueryMode::Any))
        .await
        .unwrap();
    assert_eq!(listed.actions[0].labels, Some(vec!["  LaBeL ".into()]));
    for label in ["label", "LABEL", "  LaBeL "] {
        let listed = storage
            .list_actions(&auth, query(&[label], QueryMode::Any))
            .await
            .unwrap();
        assert_eq!(listed.total_actions, 0, "{label:?}");
    }
    assert_eq!(stored_labels(&storage).await, ["  LaBeL "]);
}
