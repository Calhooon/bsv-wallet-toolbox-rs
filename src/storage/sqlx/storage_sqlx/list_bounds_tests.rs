//! The reference's array bounds at the list doors (#309): ts-stack@edf6e03
//! validationHelpers.ts validateArray (105-114, 100000 at 67) for
//! listCertificates' `certifiers` (1195) and `types` (1198), listOutputs'
//! `tags` (1637) and listActions' `labels` (1691). In this port the storage
//! finders are the list doors: `Wallet::list_*` delegates to them unchanged.

use super::super::*;

const LIMIT: usize = 100_000;

fn bound_text(name: &str) -> String {
    format!("The {name} parameter must be an array of at most {LIMIT} items")
}

async fn store() -> (StorageSqlx, AuthId) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("list-bounds", "0".repeat(64).as_str())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let identity_key = "a".repeat(66);
    let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
    (storage, AuthId::with_user_id(&identity_key, user.user_id))
}

/// At the bound the next check answers. Here that is the finder's SQL: one
/// placeholder per item, past SQLite's 32766 the store refuses with
/// `too many SQL variables` (a finding of #309, not the bound's).
fn passes_the_bound<T: std::fmt::Debug>(result: Result<T>, name: &str) {
    match result {
        Err(Error::ValidationError(text)) if text == bound_text(name) => {
            panic!("refused by the {name} bound at the bound")
        }
        Ok(_) => {}
        Err(Error::SqlxError(text)) => assert!(text.contains("too many SQL variables"), "{text}"),
        Err(other) => panic!("unexpected refusal {other:?}"),
    }
}

fn bound_refusal<T: std::fmt::Debug>(result: Result<T>) -> String {
    match result {
        Err(Error::ValidationError(text)) => text,
        other => panic!("expected a ValidationError, got {other:?}"),
    }
}

fn certificates(certifiers: usize, types: usize) -> ListCertificatesArgs {
    ListCertificatesArgs {
        certifiers: vec!["02".to_string() + &"11".repeat(32); certifiers],
        types: vec!["A".repeat(43) + "="; types],
        limit: None,
        offset: None,
        privileged: None,
        privileged_reason: None,
    }
}

fn outputs(tags: usize, basket: &str) -> ListOutputsArgs {
    ListOutputsArgs {
        basket: basket.to_string(),
        tags: Some(vec!["tag".to_string(); tags]),
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: None,
        offset: None,
        seek_permission: None,
    }
}

fn actions(labels: usize) -> ListActionsArgs {
    ListActionsArgs {
        labels: vec!["label".to_string(); labels],
        label_query_mode: None,
        include_labels: None,
        include_inputs: None,
        include_input_source_locking_scripts: None,
        include_input_unlocking_scripts: None,
        include_outputs: None,
        include_output_locking_scripts: None,
        limit: None,
        offset: None,
        seek_permission: None,
    }
}

// listCertificates: certifiers at 1195, types at 1198.

#[tokio::test]
async fn certifiers_over_the_bound_are_refused_by_the_bound() {
    let (storage, auth) = store().await;
    let result = storage
        .list_certificates(&auth, certificates(LIMIT + 1, 0))
        .await;
    assert_eq!(bound_refusal(result), bound_text("certifiers"));
}

#[tokio::test]
async fn certifiers_at_the_bound_pass_it() {
    let (storage, auth) = store().await;
    let result = storage
        .list_certificates(&auth, certificates(LIMIT, 0))
        .await;
    passes_the_bound(result, "certifiers");
}

#[tokio::test]
async fn certifiers_bound_runs_ahead_of_the_types_bound() {
    let (storage, auth) = store().await;
    let result = storage
        .list_certificates(&auth, certificates(LIMIT + 1, LIMIT + 1))
        .await;
    assert_eq!(bound_refusal(result), bound_text("certifiers"));
}

#[tokio::test]
async fn types_over_the_bound_are_refused_by_the_bound() {
    let (storage, auth) = store().await;
    let result = storage
        .list_certificates(&auth, certificates(0, LIMIT + 1))
        .await;
    assert_eq!(bound_refusal(result), bound_text("types"));
}

#[tokio::test]
async fn types_at_the_bound_pass_it() {
    let (storage, auth) = store().await;
    let result = storage
        .list_certificates(&auth, certificates(0, LIMIT))
        .await;
    passes_the_bound(result, "types");
}

// listOutputs: tags at 1637, after basket (1636), which checks only the
// basket's text: a basket the store does not hold is no answer ahead of it.

#[tokio::test]
async fn tags_over_the_bound_are_refused_by_the_bound() {
    let (storage, auth) = store().await;
    let result = storage
        .list_outputs(&auth, outputs(LIMIT + 1, "default"))
        .await;
    assert_eq!(bound_refusal(result), bound_text("tags"));
}

#[tokio::test]
async fn tags_over_the_bound_are_refused_for_a_basket_not_held() {
    let (storage, auth) = store().await;
    let result = storage
        .list_outputs(&auth, outputs(LIMIT + 1, "no-such-basket"))
        .await;
    assert_eq!(bound_refusal(result), bound_text("tags"));
}

#[tokio::test]
async fn tags_at_the_bound_pass_it() {
    let (storage, auth) = store().await;
    let result = storage.list_outputs(&auth, outputs(LIMIT, "default")).await;
    passes_the_bound(result, "tags");
}

// listActions: labels at 1691.

#[tokio::test]
async fn labels_over_the_bound_are_refused_by_the_bound() {
    let (storage, auth) = store().await;
    let result = storage.list_actions(&auth, actions(LIMIT + 1)).await;
    assert_eq!(bound_refusal(result), bound_text("labels"));
}

#[tokio::test]
async fn labels_at_the_bound_pass_it() {
    let (storage, auth) = store().await;
    let result = storage.list_actions(&auth, actions(LIMIT)).await;
    passes_the_bound(result, "labels");
}
