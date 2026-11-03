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

// #226: the finders pass a limit past the door's 10000 through, as the
// reference's storage does (listCertificates.ts:14, listOutputsKnex.ts:254,
// listActionsKnex.ts:102); the wallet's door refuses it. 10001 rows seeded,
// 10001 answered. `?1` is the user's id.

const ROWS: &str =
    "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 10000)";

async fn seeded(inserts: &[&str]) -> (StorageSqlx, AuthId) {
    let (storage, auth) = store().await;
    for insert in inserts {
        sqlx::query(insert)
            .bind(auth.user_id)
            .execute(storage.writer())
            .await
            .unwrap();
    }
    (storage, auth)
}

#[tokio::test]
async fn certificates_limit_past_ten_thousand_passes_through() {
    let insert = format!(
        "{ROWS} INSERT INTO certificates (user_id, serial_number, type, certifier, subject, \
         revocation_outpoint, signature) \
         SELECT ?1, 'serial-' || x, 'type', 'certifier', 'subject', 'outpoint', 'signature' FROM n"
    );
    let (storage, auth) = seeded(&[&insert]).await;
    let mut args = certificates(0, 0);
    args.limit = Some(10_001);
    let result = storage.list_certificates(&auth, args).await.unwrap();
    assert_eq!(
        (result.certificates.len(), result.total_certificates),
        (10_001, 10_001)
    );
}

#[tokio::test]
async fn outputs_limit_past_ten_thousand_passes_through() {
    let insert = format!(
        "{ROWS} INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, vout, \
         satoshis, provided_by, purpose, type) \
         SELECT ?1, t.transaction_id, b.basket_id, 1, n.x, 1, 'you', '', 'custom' \
         FROM n, transactions t, output_baskets b \
         WHERE t.reference = 'limit-226' AND b.user_id = ?1 AND b.name = 'default'"
    );
    let (storage, auth) = seeded(&[
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, description) \
         VALUES (?1, 'completed', 'limit-226', 0, 'limit witness')",
        &insert,
    ])
    .await;
    let mut args = outputs(0, "default");
    args.limit = Some(10_001);
    let result = storage.list_outputs(&auth, args).await.unwrap();
    assert_eq!(
        (result.outputs.len(), result.total_outputs),
        (10_001, 10_001)
    );
}

#[tokio::test]
async fn actions_limit_past_ten_thousand_passes_through() {
    let insert = format!(
        "{ROWS} INSERT INTO transactions (user_id, status, reference, is_outgoing, description) \
         SELECT ?1, 'completed', 'limit-226-' || x, 0, 'limit witness' FROM n"
    );
    let (storage, auth) = seeded(&[&insert]).await;
    let mut args = actions(0);
    args.limit = Some(10_001);
    let result = storage.list_actions(&auth, args).await.unwrap();
    assert_eq!(
        (result.actions.len(), result.total_actions),
        (10_001, 10_001)
    );
}

// #697: an offset past i32::MAX reaches SQL unchanged (the bind is `i64`, as
// #226 made the limit's) and the total's sum never overflows. Bound as i32,
// 2^31 wrapped negative, SQLite skipped nothing and answered the head of the
// store, and u32::MAX plus that page panicked in a debug build. Three rows;
// the reference's `validateInteger(args.offset, 'offset', 0, 0)` has no upper
// bound (ts-stack@edf6e03 validationHelpers.ts:1200, :1709), so no refusal.

const THREE: &str = "WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM n WHERE x < 2)";
const PAST_I32: [u32; 2] = [i32::MAX as u32 + 1, u32::MAX];

#[tokio::test]
async fn certificates_offset_past_i32_max_is_an_empty_page_counted() {
    let insert = format!(
        "{THREE} INSERT INTO certificates (user_id, serial_number, type, certifier, subject, \
         revocation_outpoint, signature) \
         SELECT ?1, 'serial-' || x, 'type', 'certifier', 'subject', 'outpoint', 'signature' FROM n"
    );
    let (storage, auth) = seeded(&[&insert]).await;
    for offset in PAST_I32 {
        let mut args = certificates(0, 0);
        args.offset = Some(offset);
        let result = storage.list_certificates(&auth, args).await.unwrap();
        let page = (result.certificates.len(), result.total_certificates);
        assert_eq!(page, (0, 3), "offset {offset}");
    }
}

#[tokio::test]
async fn outputs_offset_at_the_i32_ends_is_an_empty_page_counted() {
    let insert = format!(
        "{THREE} INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, vout, \
         satoshis, provided_by, purpose, type) \
         SELECT ?1, t.transaction_id, b.basket_id, 1, n.x, 1, 'you', '', 'custom' \
         FROM n, transactions t, output_baskets b \
         WHERE t.reference = 'offset-697' AND b.user_id = ?1 AND b.name = 'default'"
    );
    let (storage, auth) = seeded(&[
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, description) \
         VALUES (?1, 'completed', 'offset-697', 0, 'offset witness')",
        &insert,
    ])
    .await;
    // listOutputs' offset is i32: its ends normalize to 2147483646 and
    // 2147483647 (#230), the largest offsets this finder binds.
    for offset in [i32::MAX, i32::MIN] {
        let mut args = outputs(0, "default");
        args.offset = Some(offset);
        let result = storage.list_outputs(&auth, args).await.unwrap();
        let page = (result.outputs.len(), result.total_outputs);
        assert_eq!(page, (0, 3), "offset {offset}");
    }
}

#[tokio::test]
async fn actions_offset_past_i32_max_is_an_empty_page_at_the_offset() {
    let insert = format!(
        "{THREE} INSERT INTO transactions (user_id, status, reference, is_outgoing, description) \
         SELECT ?1, 'completed', 'offset-697-' || x, 0, 'offset witness' FROM n"
    );
    let (storage, auth) = seeded(&[&insert]).await;
    // listActions' own rule (listActionsKnex.ts 233-238, #674): an empty
    // page past the end answers the offset plus the page, the offset itself.
    for offset in PAST_I32 {
        let mut args = actions(0);
        args.offset = Some(offset);
        let result = storage.list_actions(&auth, args).await.unwrap();
        let page = (result.actions.len(), result.total_actions);
        assert_eq!(page, (0, offset), "offset {offset}");
    }
}

/// Past a bound offset the sums are unreachable through SQL (a non-empty page
/// at offset k needs k plus the page rows); the checked sums answer the
/// count where the `u32` would not hold them.
#[test]
fn a_sum_past_u32_max_answers_the_count() {
    use super::super::list_outputs::short_page_total;
    use super::super::transactions::list_actions_total;
    assert_eq!(short_page_total(1, 10, u32::MAX), None);
    assert_eq!(short_page_total(1, 10, u32::MAX - 1), Some(u32::MAX));
    assert_eq!(list_actions_total(1, 10, u32::MAX, 3), 3);
    assert_eq!(list_actions_total(1, 10, u32::MAX - 1, 3), u32::MAX);
}
