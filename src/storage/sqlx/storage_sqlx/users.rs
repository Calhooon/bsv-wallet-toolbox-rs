use super::{Result, Row, StorageSqlx, TableUser, Utc, WalletStorageReader};

impl StorageSqlx {
    // =========================================================================
    // User Repository
    // =========================================================================

    /// Find a user by identity key.
    pub async fn find_user(&self, identity_key: &str) -> Result<Option<TableUser>> {
        let row = sqlx::query(
            r#"
            SELECT user_id, identity_key, active_storage, created_at, updated_at
            FROM users
            WHERE identity_key = ?
            "#,
        )
        .bind(identity_key)
        .fetch_optional(&self.reader)
        .await?;

        match row {
            Some(row) => Ok(Some(TableUser {
                user_id: row.get("user_id"),
                identity_key: row.get("identity_key"),
                active_storage: row.get("active_storage"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    /// Find a user by ID.
    pub async fn find_user_by_id(&self, user_id: i64) -> Result<Option<TableUser>> {
        let row = sqlx::query(
            r#"
            SELECT user_id, identity_key, active_storage, created_at, updated_at
            FROM users
            WHERE user_id = ?
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.reader)
        .await?;

        match row {
            Some(row) => Ok(Some(TableUser {
                user_id: row.get("user_id"),
                identity_key: row.get("identity_key"),
                active_storage: row.get("active_storage"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    /// Insert a new user.
    pub async fn insert_user(&self, identity_key: &str, active_storage: &str) -> Result<TableUser> {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO users (identity_key, active_storage, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(identity_key)
        .bind(active_storage)
        .bind(now)
        .bind(now)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        let user_id = result.last_insert_rowid();

        Ok(TableUser {
            user_id,
            identity_key: identity_key.to_string(),
            active_storage: Some(active_storage.to_string()),
            created_at: now,
            updated_at: now,
        })
    }

    /// Update user's active storage.
    pub async fn update_user_active_storage(
        &self,
        user_id: i64,
        active_storage: &str,
    ) -> Result<()> {
        let now = Utc::now();

        sqlx::query(
            r#"
            UPDATE users
            SET active_storage = ?, updated_at = ?
            WHERE user_id = ?
            "#,
        )
        .bind(active_storage)
        .bind(now)
        .bind(user_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(())
    }

    pub(super) async fn find_or_insert_user_impl(
        &self,
        identity_key: &str,
    ) -> Result<(TableUser, bool)> {
        // Try to find existing user
        if let Some(user) = self.find_user(identity_key).await? {
            return Ok((user, false));
        }

        // Get storage identity key for new user's active storage
        let settings = self.get_settings();
        let active_storage = settings.storage_identity_key.clone();

        // Create new user
        let user = self.insert_user(identity_key, &active_storage).await?;

        // Also create default basket for user
        self.find_or_create_default_basket(user.user_id).await?;

        Ok((user, true))
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[tokio::test]
    async fn test_find_or_insert_user() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66); // Mock public key

        // First insert
        let (user1, is_new1) = storage.find_or_insert_user(&identity_key).await.unwrap();
        assert!(is_new1);
        assert_eq!(user1.identity_key, identity_key);

        // Second lookup should find existing
        let (user2, is_new2) = storage.find_or_insert_user(&identity_key).await.unwrap();
        assert!(!is_new2);
        assert_eq!(user2.user_id, user1.user_id);
    }
}
