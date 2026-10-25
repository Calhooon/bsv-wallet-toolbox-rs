#[cfg(test)]
mod tests {
    use super::super::{StorageSqlx, WalletStorageWriter};

    #[tokio::test]
    async fn test_insert_commission() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        // Insert a test transaction first (commission requires a transaction)
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, description, is_outgoing, satoshis, reference, created_at, updated_at)
            VALUES (?, 'completed', 'Test transaction', 1, 1000, 'ref123', datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.writer())
        .await
        .unwrap();

        let tx_id: i64 = sqlx::query_scalar(
            "SELECT transaction_id FROM transactions WHERE reference = 'ref123'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();

        // Insert commission
        let locking_script = vec![0x76, 0xa9, 0x14]; // Partial P2PKH
        let commission_id = storage
            .insert_commission(user.user_id, tx_id, 500, &locking_script, "offset_123")
            .await
            .unwrap();

        assert!(commission_id > 0);

        // Get unredeemed commissions
        let commissions = storage
            .get_unredeemed_commissions(user.user_id)
            .await
            .unwrap();
        assert_eq!(commissions.len(), 1);
        assert_eq!(commissions[0].satoshis, 500);
        assert_eq!(commissions[0].key_offset, "offset_123");
        assert!(!commissions[0].is_redeemed);

        // Redeem commission
        storage.redeem_commission(commission_id).await.unwrap();

        // Verify redeemed
        let commissions = storage
            .get_unredeemed_commissions(user.user_id)
            .await
            .unwrap();
        assert_eq!(commissions.len(), 0);
    }
}
