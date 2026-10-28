use super::super::*;
use bsv_rs::wallet::{CreateActionOptions, CreateActionOutput};

fn named_input_args(txid: &str, no_send: Option<bool>) -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "Stored input guard regression".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(txid, vec![0x51])]),
        outputs: Some(vec![p2pkh_output(1_000, "stored input payment")]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            no_send,
            sign_and_process: Some(false),
            ..CreateActionOptions::default()
        }),
    }
}

async fn guard_named_input(
    storage: &StorageSqlx,
    user_id: i64,
    txid: &str,
    no_send: Option<bool>,
) -> Result<Vec<ExtendedInput>> {
    let mut conn = storage.writer().acquire().await.unwrap();
    validate_and_extend_inputs(
        storage,
        &mut conn,
        user_id,
        &named_input_args(txid, no_send),
    )
    .await
}

async fn unspendable_named_input() -> (StorageSqlx, i64, String) {
    let (storage, user_id) = caller_complete_storage().await;
    let txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "245").await;
    sqlx::query("UPDATE outputs SET spendable = 0 WHERE txid = ?")
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();
    (storage, user_id, txid)
}

async fn named_input_refusal(
    storage: &StorageSqlx,
    user_id: i64,
    txid: &str,
    no_send: Option<bool>,
) -> String {
    match guard_named_input(storage, user_id, txid, no_send)
        .await
        .expect_err("named input must be refused")
    {
        Error::ValidationError(message) => message,
        error => panic!("expected ValidationError, got {error:?}"),
    }
}

fn live_spendability_refusal(vin: usize, txid: &str) -> String {
    format!("The inputs[{vin}] parameter must be spendable output. output {txid}:0 appears to have been spent (spendable=false).")
}

async fn stored_input_state(
    storage: &StorageSqlx,
    txid: &str,
) -> (bool, Option<i64>, Option<String>, String) {
    sqlx::query_as(
        "SELECT spendable, spent_by, spending_description, CAST(updated_at AS TEXT)
         FROM outputs WHERE txid = ? AND vout = 0",
    )
    .bind(txid)
    .fetch_one(storage.pool())
    .await
    .unwrap()
}

async fn transaction_count(storage: &StorageSqlx) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn set_stored_input_spender(
    storage: &StorageSqlx,
    txid: &str,
    spendable: bool,
    spent_by: i64,
    description: Option<&str>,
) {
    sqlx::query(
        "UPDATE outputs SET spendable = ?, spent_by = ?, spending_description = ? WHERE txid = ?",
    )
    .bind(spendable)
    .bind(spent_by)
    .bind(description)
    .bind(txid)
    .execute(storage.writer())
    .await
    .unwrap();
}

async fn assert_live_spendability_refusal(no_send: Option<bool>) {
    let (storage, user_id, txid) = unspendable_named_input().await;
    let before = stored_input_state(&storage, &txid).await;
    let count = transaction_count(&storage).await;
    let error = create_action_internal(&storage, None, user_id, named_input_args(&txid, no_send))
        .await
        .expect_err("markKnownInputsSpent refuses under noSend too");
    match error {
        Error::ValidationError(message) => assert_eq!(message, live_spendability_refusal(0, &txid)),
        error => panic!("expected live spendability refusal, got {error:?}"),
    }
    assert_eq!(stored_input_state(&storage, &txid).await, before);
    assert_eq!(transaction_count(&storage).await, count);
}

#[tokio::test]
async fn caller_named_no_send_unspendable_is_refused_without_writes() {
    // ts-stack@edf6e03 createAction.ts:61 disables the :982 gate;
    // :419-423 is live under noSend, called at :503.
    assert_live_spendability_refusal(Some(true)).await;
}

#[tokio::test]
async fn caller_named_unspendable_uses_live_refusal() {
    for no_send in [None, Some(false)] {
        assert_live_spendability_refusal(no_send).await;
    }
}

async fn seed_spender(
    storage: &StorageSqlx,
    user_id: i64,
    source_txid: &str,
    status: &str,
    has_txid: bool,
) -> (i64, String) {
    let (raw_tx, txid) = test_support::synth_tx(source_txid, 0, 99_000);
    let now = Utc::now();
    let id = sqlx::query(
        "INSERT INTO transactions
         (user_id, status, reference, is_outgoing, satoshis, version, lock_time,
          description, txid, raw_tx, created_at, updated_at)
         VALUES (?, ?, 'stored-spender', 1, 0, 1, 0, 'Stored spender', ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(status)
    .bind(has_txid.then_some(&txid))
    .bind(has_txid.then_some(raw_tx))
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    (id, txid)
}

async fn assert_spender_requires_review(status: &str) {
    // :412-417 returns a competing txid without testing the spender's status;
    // :503-509 throws WERR_REVIEW_ACTIONS, also for failed/aborted spenders.
    for no_send in [Some(true), None, Some(false)] {
        let (storage, user_id, txid) = unspendable_named_input().await;
        let other_txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "246").await;
        let other = create_action_internal(
            &storage,
            None,
            user_id,
            named_input_args(&other_txid, Some(true)),
        )
        .await
        .unwrap();
        let (spender, competing_txid) = seed_spender(&storage, user_id, &txid, status, true).await;
        set_stored_input_spender(&storage, &txid, false, spender, Some("prior spend")).await;
        let before = stored_input_state(&storage, &txid).await;
        let count = transaction_count(&storage).await;
        let result =
            create_action_internal(&storage, None, user_id, named_input_args(&txid, no_send)).await;
        // On the regression, abort the accepted template to expose the lost
        // lock. On refusal, abort an independent action: the old lock stays.
        let reference = result
            .as_ref()
            .map(|created| &created.reference)
            .unwrap_or(&other.reference);
        crate::storage::sqlx::abort_action::abort_action_internal(
            &storage,
            user_id,
            bsv_rs::wallet::AbortActionArgs {
                reference: reference.clone(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            stored_input_state(&storage, &txid).await,
            before,
            "refusal and a later abort must preserve the completed/failed spender"
        );
        assert_eq!(transaction_count(&storage).await, count);
        match result.expect_err("an existing spender requires review, even under noSend") {
            Error::ReviewActions(refusal) => {
                assert_eq!(
                    refusal.message,
                    "Undelayed createAction or signAction results require review."
                );
                let [review] = refusal.review_action_results.as_slice() else {
                    panic!("one review result: {refusal:?}");
                };
                assert_eq!(review.txid, [0; 32]);
                assert_eq!(
                    review.status,
                    bsv_rs::wallet::ReviewActionResultStatus::DoubleSpend
                );
                assert_eq!(
                    review
                        .competing_txs
                        .as_deref()
                        .map(|txs| txs.iter().map(hex::encode).collect::<Vec<_>>()),
                    Some(vec![competing_txid.clone()])
                );
                assert!(refusal.send_with_results.is_empty());
            }
            error => panic!("expected double-spend review, got {error:?}"),
        }
    }
}

#[tokio::test]
async fn caller_named_completed_spender_is_preserved_after_refusal_and_abort() {
    assert_spender_requires_review("completed").await;
}

#[tokio::test]
async fn caller_named_failed_spender_with_txid_requires_review() {
    assert_spender_requires_review("failed").await;
}

#[tokio::test]
async fn caller_named_spender_without_txid_follows_spendable_flag() {
    // :412-419: a reservation with no txid falls through to spendability.
    // An abort stores status 'failed'; that word alone never releases a coin.
    for status in ["unsigned", "failed"] {
        for spendable in [false, true] {
            let (storage, user_id, txid) = unspendable_named_input().await;
            let (spender, _) = seed_spender(&storage, user_id, &txid, status, false).await;
            set_stored_input_spender(&storage, &txid, spendable, spender, None).await;
            let before = stored_input_state(&storage, &txid).await;
            let count = transaction_count(&storage).await;
            let result = create_action_internal(
                &storage,
                None,
                user_id,
                named_input_args(&txid, Some(true)),
            )
            .await;
            if spendable {
                let created = result.expect("a txid-less reservation is not a competing spend");
                let new_spender: i64 = sqlx::query_scalar(
                    "SELECT transaction_id FROM transactions WHERE reference = ?",
                )
                .bind(&created.reference)
                .fetch_one(storage.pool())
                .await
                .unwrap();
                let after = stored_input_state(&storage, &txid).await;
                assert!(!after.0);
                assert_eq!(after.1, Some(new_spender));
            } else {
                match result.expect_err("a txid-less reservation is still unspendable") {
                    Error::ValidationError(message) => {
                        assert_eq!(message, live_spendability_refusal(0, &txid));
                    }
                    error => panic!("expected live spendability refusal, got {error:?}"),
                }
                assert_eq!(stored_input_state(&storage, &txid).await, before);
                assert_eq!(transaction_count(&storage).await, count);
            }
        }
    }
}

#[tokio::test]
async fn caller_named_later_unspendable_rolls_back_earlier_input() {
    let (storage, user_id, txid) = unspendable_named_input().await;
    let spendable_txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "246").await;
    let before = stored_input_state(&storage, &spendable_txid).await;
    let mut args = named_input_args(&txid, Some(true));
    args.inputs
        .as_mut()
        .unwrap()
        .insert(0, covenant_input(&spendable_txid, vec![0x51]));
    let error = create_action_internal(&storage, None, user_id, args)
        .await
        .expect_err("the later input must refuse the entire action");
    match error {
        Error::ValidationError(message) => assert_eq!(message, live_spendability_refusal(1, &txid)),
        error => panic!("expected live spendability refusal, got {error:?}"),
    }
    assert_eq!(stored_input_state(&storage, &spendable_txid).await, before);
}

#[derive(Clone, Copy, Debug)]
enum MissingStoredField {
    LockingScript,
    Satoshis,
}

async fn remove_stored_field(storage: &StorageSqlx, txid: &str, field: MissingStoredField) {
    match field {
        MissingStoredField::LockingScript => {
            sqlx::query("UPDATE outputs SET locking_script = NULL WHERE txid = ?")
                .bind(txid)
                .execute(storage.writer())
                .await
                .unwrap();
        }
        MissingStoredField::Satoshis => {
            // Only this in-memory fixture permits NULL. The production schema
            // has satoshis INTEGER NOT NULL (migrations/001_initial.sql:157),
            // so the schema already supplies this half of the guard;
            // TableOutput also requires an integer.
            sqlx::raw_sql(
                "ALTER TABLE outputs ADD COLUMN nullable_satoshis INTEGER;
                 UPDATE outputs SET nullable_satoshis = satoshis;
                 ALTER TABLE outputs DROP COLUMN satoshis;
                 ALTER TABLE outputs RENAME COLUMN nullable_satoshis TO satoshis;",
            )
            .execute(storage.writer())
            .await
            .unwrap();
            sqlx::query("UPDATE outputs SET satoshis = NULL WHERE txid = ?")
                .bind(txid)
                .execute(storage.writer())
                .await
                .unwrap();
        }
    }
}

async fn assert_missing_field_uses_reference_refusal(field: MissingStoredField) {
    // ts-stack@edf6e03 createAction.ts:979-980, WERR_errors.ts:105.
    // Missing data must win over spendability, including under noSend.
    for no_send in [None, Some(false), Some(true)] {
        for spendable in [false, true] {
            let (storage, user_id) = caller_complete_storage().await;
            let txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "245").await;
            remove_stored_field(&storage, &txid, field).await;
            sqlx::query("UPDATE outputs SET spendable = ? WHERE txid = ?")
                .bind(spendable)
                .bind(&txid)
                .execute(storage.writer())
                .await
                .unwrap();
            let message = named_input_refusal(&storage, user_id, &txid, no_send).await;
            assert_eq!(
                message,
                format!(
                    "The {txid}.0 parameter must be output with valid lockingScript and satoshis"
                ),
                "field={field:?}, spendable={spendable}, no_send={no_send:?}"
            );
        }
    }
}

#[tokio::test]
async fn caller_named_missing_locking_script_uses_reference_refusal() {
    assert_missing_field_uses_reference_refusal(MissingStoredField::LockingScript).await;
}

#[tokio::test]
async fn caller_named_missing_satoshis_uses_reference_refusal() {
    assert_missing_field_uses_reference_refusal(MissingStoredField::Satoshis).await;
}

#[tokio::test]
async fn caller_named_change_precedes_missing_fields() {
    for field in [
        MissingStoredField::LockingScript,
        MissingStoredField::Satoshis,
    ] {
        for no_send in [None, Some(false), Some(true)] {
            let (storage, user_id) = caller_complete_storage().await;
            let txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "245").await;
            remove_stored_field(&storage, &txid, field).await;
            sqlx::query("UPDATE outputs SET change = 1, spendable = 0 WHERE txid = ?")
                .bind(&txid)
                .execute(storage.writer())
                .await
                .unwrap();
            let message = named_input_refusal(&storage, user_id, &txid, no_send).await;
            assert_eq!(message, "The inputs[0] parameter must be an unmanaged input. Change outputs are managed by your wallet.");
        }
    }
}

#[tokio::test]
async fn caller_named_zero_satoshis_and_empty_script_are_present() {
    let (storage, user_id) = caller_complete_storage().await;
    let txid = seed_custom_output(&storage, user_id, 0, &[], "245").await;
    let inputs = guard_named_input(&storage, user_id, &txid, None)
        .await
        .expect("zero satoshis and an empty stored script are present");
    assert_eq!(inputs[0].satoshis, 0);
    assert!(inputs[0].locking_script.is_empty());
}

#[tokio::test]
async fn caller_named_pruned_script_is_recovered() {
    let (storage, user_id) = caller_complete_storage().await;
    let txid = seed_custom_output(&storage, user_id, 100_000, &[0x51], "245").await;
    sqlx::query(
        "UPDATE outputs SET locking_script = NULL, script_length = 1,
         script_offset = (SELECT length(raw_tx) - 5 FROM transactions WHERE txid = ?)
         WHERE txid = ?",
    )
    .bind(&txid)
    .bind(&txid)
    .execute(storage.writer())
    .await
    .unwrap();
    let inputs = guard_named_input(&storage, user_id, &txid, None)
        .await
        .expect("the stored raw transaction supplies the pruned script");
    assert_eq!(inputs[0].locking_script, vec![0x51]);
}

async fn assert_named_change_uses_reference_refusal(spendable: bool) {
    // ts-stack@edf6e03 createAction.ts:972-983 checks change before
    // spendability; WERR_errors.ts:105 renders WERR_INVALID_PARAMETER.
    let messages = [
        "The inputs[0] parameter must be an unmanaged input. Change outputs are managed by your wallet.",
        "The inputs[1] parameter must be an unmanaged input. Change outputs are managed by your wallet.",
    ];
    for (index, expected) in messages.into_iter().enumerate() {
        let (storage, user_id) = caller_complete_storage().await;
        seed_change_output(&storage, user_id, 100_000).await;
        let change_txid = txid_of(&seed_raw_tx());
        sqlx::query("UPDATE outputs SET spendable = ? WHERE user_id = ? AND txid = ?")
            .bind(spendable)
            .bind(user_id)
            .bind(&change_txid)
            .execute(storage.writer())
            .await
            .unwrap();

        let mut inputs = Vec::new();
        if index == 1 {
            let custom_txid = seed_custom_output(&storage, user_id, 1, &[0x51], "193").await;
            inputs.push(covenant_input(&custom_txid, vec![0x51]));
        }
        inputs.push(covenant_input(&change_txid, vec![0x51]));
        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Refuse caller-named change".to_string(),
            input_beef: None,
            inputs: Some(inputs),
            outputs: Some(vec![p2pkh_output(1_000, "refused payment")]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let error = create_action_internal(&storage, None, user_id, args)
            .await
            .expect_err("caller-named change must be refused");
        let Error::ValidationError(message) = error else {
            panic!("expected ValidationError, got {error:?}");
        };
        assert_eq!(message, expected, "spendable={spendable}, input={index}");
    }
}

#[tokio::test]
async fn caller_named_spendable_change_uses_reference_refusal() {
    assert_named_change_uses_reference_refusal(true).await;
}

#[tokio::test]
async fn caller_named_unspendable_change_uses_reference_refusal() {
    assert_named_change_uses_reference_refusal(false).await;
}

/// Calgooon/zanaadu-v2#357: a pf head spend names its head output (a row
/// in the `pf-name-head` basket) and the wallet funds the rest. Both
/// inputs, the caller's and the wallet's change, are marked `spent_by` at
/// `create_action`, so `list_actions` lists every input from storage.
#[tokio::test]
async fn test_caller_named_basket_input_is_marked_spent_by_and_listed() {
    use crate::storage::traits::{AuthId, WalletStorageReader};

    let (storage, user_id) = caller_complete_storage().await;
    let head_lock = vec![0x51u8];
    let head_txid = seed_custom_output(&storage, user_id, 1, &head_lock, "9e4d").await;
    let head_basket = find_or_insert_output_basket(
        &mut storage.writer().acquire().await.unwrap(),
        user_id,
        "pf-name-head",
    )
    .await
    .unwrap();
    sqlx::query("UPDATE outputs SET basket_id = ? WHERE txid = ?")
        .bind(head_basket.basket_id)
        .bind(&head_txid)
        .execute(storage.writer())
        .await
        .unwrap();
    seed_change_output(&storage, user_id, 100_000).await;

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "pf head spend".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&head_txid, p2pkh_unlock(0xc3))]),
        outputs: Some(vec![
            CreateActionOutput {
                locking_script: head_lock.clone(),
                satoshis: 1,
                output_description: "next head".to_string(),
                basket: Some("pf-name-head".to_string()),
                custom_instructions: None,
                tags: None,
            },
            p2pkh_output(1_000, "platform fee"),
        ]),
        lock_time: None,
        version: None,
        labels: Some(vec!["pf-name".to_string()]),
        options: None,
    };
    let result = create_action_internal(&storage, None, user_id, args)
        .await
        .expect("a funded head spend");
    assert_eq!(result.inputs.len(), 2, "the head and one funding input");

    let transaction_id: i64 =
        sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE reference = ?")
            .bind(&result.reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let spent: Vec<(String, Option<i64>, bool)> = sqlx::query_as(
        "SELECT txid, spent_by, spendable FROM outputs WHERE txid IN (?, ?) ORDER BY txid",
    )
    .bind(&head_txid)
    .bind(txid_of(&seed_raw_tx()))
    .fetch_all(storage.pool())
    .await
    .unwrap();
    assert_eq!(spent.len(), 2);
    for (txid, spent_by, spendable) in &spent {
        assert_eq!(*spent_by, Some(transaction_id), "{txid} spent_by");
        assert!(!spendable, "{txid} spendable");
    }

    let listed = storage
        .list_actions(
            &AuthId::with_user_id("02user_identity_key", user_id),
            bsv_rs::wallet::ListActionsArgs {
                labels: vec!["pf-name".to_string()],
                label_query_mode: None,
                include_labels: None,
                include_inputs: Some(true),
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: None,
                include_outputs: None,
                include_output_locking_scripts: None,
                limit: Some(10),
                offset: Some(0),
                seek_permission: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.actions.len(), 1);
    let inputs = listed.actions[0].inputs.as_ref().expect("inputs listed");
    assert_eq!(inputs.len(), 2, "every input from storage: {:?}", inputs);
    assert!(inputs
        .iter()
        .any(|i| hex::encode(i.source_outpoint.txid) == head_txid));
}
