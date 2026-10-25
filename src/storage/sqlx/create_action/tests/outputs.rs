use super::super::*;
use crate::storage::traits::WalletStorageWriter;
use bsv_rs::wallet::CreateActionOutput;

#[tokio::test]
async fn test_create_action_with_labels() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    // First, seed the wallet with some change
    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction with labels".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["payment".to_string(), "test".to_string()]),
        options: None,
    };

    let result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    assert!(!result.reference.is_empty());
    assert_eq!(result.version, 1);
    assert_eq!(result.lock_time, 0);
    assert!(!result.outputs.is_empty());
    assert!(!result.derivation_prefix.is_empty());
}

#[tokio::test]
async fn test_create_action_with_tags_and_basket() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Test transaction with basket and tags".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script,
            satoshis: 1000,
            output_description: "Test output".to_string(),
            basket: Some("payments".to_string()),
            custom_instructions: Some("{\"type\":\"custom\"}".to_string()),
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    // Verify the output has the basket and tags
    assert!(!result.outputs.is_empty());
    let first_output = &result.outputs[0];
    assert_eq!(first_output.basket, Some("payments".to_string()));
    assert_eq!(
        first_output.tags,
        vec!["tag1".to_string(), "tag2".to_string()]
    );
    assert_eq!(
        first_output.custom_instructions,
        Some("{\"type\":\"custom\"}".to_string())
    );
}

/// Review F6 of bsv-wallet-cli #44: two outputs naming one new basket.
/// The basket is found on the transaction's own connection, so the second
/// output finds the first one's insert: one basket row, both outputs in
/// it. Before, the find ran on the reader: a file store failed `UNIQUE
/// constraint failed`, an in-memory store hung.
async fn two_outputs_to_one_new_basket(storage: StorageSqlx) {
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    seed_change_output(&storage, user.user_id, 100_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
    let output = |satoshis| CreateActionOutput {
        locking_script: locking_script.clone(),
        satoshis,
        output_description: "to the new basket".to_string(),
        basket: Some("brand-new".to_string()),
        custom_instructions: None,
        tags: None,
    };
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Two outputs, one new basket".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![output(1000), output(2000)]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        create_action_internal(&storage, None, user.user_id, args),
    )
    .await
    .expect("no hang")
    .expect("no UNIQUE failure");
    assert_eq!(
        result
            .outputs
            .iter()
            .filter(|o| o.basket.as_deref() == Some("brand-new"))
            .count(),
        2
    );

    let (baskets,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM output_baskets WHERE user_id = ? AND name = 'brand-new'",
    )
    .bind(user.user_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(baskets, 1);
    let (in_basket,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM outputs o JOIN output_baskets b ON o.basket_id = b.basket_id \
             WHERE b.name = 'brand-new'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(in_basket, 2);
}

#[tokio::test]
async fn two_outputs_to_one_new_basket_on_a_file_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f6.db");
    let storage = StorageSqlx::open(&path.to_string_lossy()).await.unwrap();
    two_outputs_to_one_new_basket(storage).await;
}

#[tokio::test]
async fn two_outputs_to_one_new_basket_in_memory() {
    two_outputs_to_one_new_basket(StorageSqlx::in_memory().await.unwrap()).await;
}

#[tokio::test]
async fn test_create_action_multiple_outputs() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();

    seed_change_output(&storage, user.user_id, 200_000).await;

    let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "Multiple outputs transaction".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![
            CreateActionOutput {
                locking_script: locking_script.clone(),
                satoshis: 1000,
                output_description: "Output 1".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            },
            CreateActionOutput {
                locking_script: locking_script.clone(),
                satoshis: 2000,
                output_description: "Output 2".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            },
            CreateActionOutput {
                locking_script,
                satoshis: 3000,
                output_description: "Output 3".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            },
        ]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user.user_id, args)
        .await
        .unwrap();

    // Should have at least 3 outputs (user outputs) + change
    assert!(result.outputs.len() >= 3);

    // Verify first 3 outputs are the user's
    assert_eq!(result.outputs[0].satoshis, 1000);
    assert_eq!(result.outputs[1].satoshis, 2000);
    assert_eq!(result.outputs[2].satoshis, 3000);
}
