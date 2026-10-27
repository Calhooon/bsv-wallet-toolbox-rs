use super::super::*;
use bsv_rs::wallet::CreateActionOutput;

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
