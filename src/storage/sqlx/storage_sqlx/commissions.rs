use super::{Result, Row, StorageSqlx, TableCommission, Utc};

impl StorageSqlx {
    /// Insert a commission record for a transaction.
    ///
    /// Commissions track fees or royalties associated with transactions.
    ///
    /// # Arguments
    /// * `user_id` - The user who owns the commission
    /// * `transaction_id` - The transaction this commission is for
    /// * `satoshis` - Amount of the commission in satoshis
    /// * `locking_script` - The locking script for the commission output
    /// * `key_offset` - Key derivation offset for the commission
    pub async fn insert_commission(
        &self,
        user_id: i64,
        transaction_id: i64,
        satoshis: i64,
        locking_script: &[u8],
        key_offset: &str,
    ) -> Result<i64> {
        let now = chrono::Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO commissions (user_id, transaction_id, satoshis, locking_script, key_offset, is_redeemed, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(satoshis)
        .bind(locking_script)
        .bind(key_offset)
        .bind(now)
        .bind(now)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Mark a commission as redeemed.
    pub async fn redeem_commission(&self, commission_id: i64) -> Result<()> {
        let now = chrono::Utc::now();

        sqlx::query(
            r#"
            UPDATE commissions
            SET is_redeemed = 1, updated_at = ?
            WHERE commission_id = ?
            "#,
        )
        .bind(now)
        .bind(commission_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(())
    }

    /// Get unredeemed commissions for a user.
    pub async fn get_unredeemed_commissions(&self, user_id: i64) -> Result<Vec<TableCommission>> {
        let rows = sqlx::query(
            r#"
            SELECT commission_id, user_id, transaction_id, satoshis, locking_script, key_offset, is_redeemed, created_at, updated_at
            FROM commissions
            WHERE user_id = ? AND is_redeemed = 0
            ORDER BY created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.reader)
        .await?;

        let commissions = rows
            .iter()
            .map(|row| TableCommission {
                commission_id: row.get("commission_id"),
                user_id: row.get("user_id"),
                transaction_id: row.get("transaction_id"),
                satoshis: row.get("satoshis"),
                payer_locking_script: row.get("locking_script"),
                key_offset: row.get("key_offset"),
                is_redeemed: row.get::<i64, _>("is_redeemed") != 0,
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
            .collect();

        Ok(commissions)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

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
