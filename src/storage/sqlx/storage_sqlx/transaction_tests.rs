#[cfg(test)]
mod tests {
    use super::super::{
        AuthId, Duration, ListActionsArgs, MonitorStorage, Row, StorageSqlx, WalletStorageReader,
        WalletStorageWriter,
    };

    #[tokio::test]
    async fn test_list_actions_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List actions (should be empty)
        let result = storage
            .list_actions(
                &auth,
                ListActionsArgs {
                    labels: vec![],
                    label_query_mode: None,
                    include_labels: Some(true),
                    include_inputs: None,
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

        assert_eq!(result.total_actions, 0);
        assert!(result.actions.is_empty());
    }

    #[tokio::test]
    async fn test_list_actions_with_data() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Insert a test transaction
        let txid = "b".repeat(64);
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at)
            VALUES (?, 'completed', 'test-ref-1', 1, 1000, 'Test transaction', ?, 1, 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();

        // List actions
        let result = storage
            .list_actions(
                &auth,
                ListActionsArgs {
                    labels: vec![],
                    label_query_mode: None,
                    include_labels: Some(true),
                    include_inputs: Some(true),
                    include_input_source_locking_scripts: None,
                    include_input_unlocking_scripts: None,
                    include_outputs: Some(true),
                    include_output_locking_scripts: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_actions, 1);
        assert_eq!(result.actions.len(), 1);
        assert_eq!(result.actions[0].satoshis, 1000);
        assert!(result.actions[0].is_outgoing);
        assert_eq!(result.actions[0].description, "Test transaction");
    }

    #[tokio::test]
    async fn test_list_actions_with_labels() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Insert test transaction
        let txid = "c".repeat(64);
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at)
            VALUES (?, 'completed', 'test-ref-2', 0, 500, 'Labeled transaction', ?, 1, 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();

        // Get the transaction ID
        let tx_row =
            sqlx::query("SELECT transaction_id FROM transactions WHERE reference = 'test-ref-2'")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let transaction_id: i64 = tx_row.get("transaction_id");

        // Insert a label
        sqlx::query(
            r#"
            INSERT INTO tx_labels (user_id, label, is_deleted, created_at, updated_at)
            VALUES (?, 'test_label', 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.writer())
        .await
        .unwrap();

        // Get the label ID
        let label_row = sqlx::query("SELECT tx_label_id FROM tx_labels WHERE label = 'test_label'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let label_id: i64 = label_row.get("tx_label_id");

        // Map label to transaction
        sqlx::query(
            r#"
            INSERT INTO tx_labels_map (tx_label_id, transaction_id, is_deleted, created_at, updated_at)
            VALUES (?, ?, 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(label_id)
        .bind(transaction_id)
        .execute(storage.writer())
        .await
        .unwrap();

        // List actions with label filter
        let result = storage
            .list_actions(
                &auth,
                ListActionsArgs {
                    labels: vec!["test_label".to_string()],
                    label_query_mode: None,
                    include_labels: Some(true),
                    include_inputs: None,
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

        assert_eq!(result.total_actions, 1);
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0]
            .labels
            .as_ref()
            .unwrap()
            .contains(&"test_label".to_string()));

        // Query with non-existing label
        let result2 = storage
            .list_actions(
                &auth,
                ListActionsArgs {
                    labels: vec!["nonexistent_label".to_string()],
                    label_query_mode: None,
                    include_labels: Some(true),
                    include_inputs: None,
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

        assert_eq!(result2.total_actions, 0);
        assert!(result2.actions.is_empty());
    }

    #[tokio::test]
    async fn test_abort_abandoned_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no abandoned transactions, should complete without error
        let result = storage.abort_abandoned(Duration::from_secs(3600)).await;
        assert!(result.is_ok());
    }
}
