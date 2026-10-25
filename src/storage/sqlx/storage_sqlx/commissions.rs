use super::{Result, Row, StorageSqlx, TableCommission};

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
