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

#[tokio::test]
async fn caller_named_no_send_unspendable_builds_unsigned() {
    let (storage, user_id, txid) = unspendable_named_input().await;

    let result =
        create_action_internal(&storage, None, user_id, named_input_args(&txid, Some(true)))
            .await
            .expect("noSend permits an unspendable stored input");
    assert_eq!(result.inputs.len(), 1);
    assert_eq!(result.inputs[0].source_txid, txid);
    assert!(result.no_send_change_output_vouts.is_some());
    let (status, raw_tx): (String, Option<Vec<u8>>) =
        sqlx::query_as("SELECT status, raw_tx FROM transactions WHERE reference = ?")
            .bind(&result.reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(status, "unsigned");
    assert!(raw_tx.is_none(), "no transaction was signed or sent");
}

#[tokio::test]
async fn caller_named_unspendable_requires_no_send() {
    for no_send in [None, Some(false)] {
        let (storage, user_id, txid) = unspendable_named_input().await;
        let message = named_input_refusal(&storage, user_id, &txid, no_send).await;
        assert_eq!(
            message,
            format!("inputs[0]: output {txid}:0 is not spendable")
        );
    }
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
            // Only this in-memory fixture permits NULL: the production schema
            // and public TableOutput keep their required integer satoshis.
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
