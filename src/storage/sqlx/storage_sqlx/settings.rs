use super::{
    async_trait, lock_read, lock_write, Arc, Error, Result, Row, StorageSqlx, TableSettings, Utc,
    WalletServices, WalletStorageProvider, DEFAULT_MAX_OUTPUT_SCRIPT, MIGRATIONS,
};

impl StorageSqlx {
    // =========================================================================
    // Settings Repository
    // =========================================================================

    /// Read settings from the database.
    async fn read_settings(&self) -> Result<Option<TableSettings>> {
        let row = sqlx::query(
            r#"
            SELECT settings_id, storage_identity_key, storage_name, chain, max_output_script, created_at, updated_at
            FROM settings
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.reader)
        .await?;

        match row {
            Some(row) => Ok(Some(TableSettings {
                settings_id: row.get("settings_id"),
                storage_identity_key: row.get("storage_identity_key"),
                storage_name: row.get("storage_name"),
                chain: row.get("chain"),
                max_output_script: row.get("max_output_script"),
                dbtype: None,
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    /// Save settings to the database.
    async fn save_settings(&self, settings: &TableSettings) -> Result<()> {
        let now = Utc::now();

        // Check if settings exist
        let existing = self.read_settings().await?;

        if existing.is_some() {
            // Update existing
            sqlx::query(
                r#"
                UPDATE settings
                SET storage_identity_key = ?, storage_name = ?, chain = ?, max_output_script = ?, updated_at = ?
                WHERE settings_id = 1
                "#,
            )
            .bind(&settings.storage_identity_key)
            .bind(&settings.storage_name)
            .bind(&settings.chain)
            .bind(settings.max_output_script)
            .bind(now)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        } else {
            // Insert new
            sqlx::query(
                r#"
                INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script, created_at, updated_at)
                VALUES (?, ?, ?, 'SQLite', ?, ?, ?)
                "#,
            )
            .bind(&settings.storage_identity_key)
            .bind(&settings.storage_name)
            .bind(&settings.chain)
            .bind(settings.max_output_script)
            .bind(now)
            .bind(now)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        }

        // Update cache
        self.cache_settings(settings)?;

        Ok(())
    }
    pub(super) async fn make_available_impl(&self) -> Result<TableSettings> {
        let settings = self.read_settings().await?;

        if let Some(settings) = settings {
            // Additive schema (0.3.56): a database created before migration
            // 002 gets the broadcast tables on open, not only on `migrate`.
            self.ensure_broadcast_schema().await?;
            self.ensure_locked_inputs_schema().await?;
            super::monitor_state::ensure_monitor_state_schema(self.writer()).await?;
            super::proof_root_checks::ensure_proof_root_checks_schema(self.writer()).await?;
            self.cache_settings(&settings)?;
            Ok(settings)
        } else {
            Err(Error::StorageNotAvailable)
        }
    }

    pub(super) async fn migrate_impl(
        &self,
        storage_name: &str,
        storage_identity_key: &str,
    ) -> Result<String> {
        // Run migrations
        self.run_migrations().await?;

        // Save settings
        let settings = TableSettings {
            settings_id: 1,
            storage_identity_key: storage_identity_key.to_string(),
            storage_name: storage_name.to_string(),
            chain: "mainnet".to_string(),
            max_output_script: DEFAULT_MAX_OUTPUT_SCRIPT,
            dbtype: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.save_settings(&settings).await?;

        // Update internal state
        *lock_write(&self.storage_identity_key)? = storage_identity_key.to_string();
        *lock_write(&self.storage_name)? = storage_name.to_string();

        Ok(MIGRATIONS
            .last()
            .map(|(name, _)| name.to_string())
            .unwrap_or_default())
    }

    pub(super) async fn destroy_impl(&self) -> Result<()> {
        // Drop all tables in reverse dependency order
        let tables = [
            "broadcast_seen",
            "broadcast_prefs",
            "sync_states",
            "monitor_events",
            "tx_labels_map",
            "tx_labels",
            "output_tags_map",
            "output_tags",
            "outputs",
            "commissions",
            "transactions",
            "output_baskets",
            "certificate_fields",
            "certificates",
            "users",
            "proven_tx_reqs",
            "proven_txs",
            "settings",
        ];

        for table in tables {
            let sql = format!("DROP TABLE IF EXISTS {}", table);
            sqlx::query(&sql)
                .execute(&mut *self.writer_conn().await?)
                .await?;
        }

        // Clear cached settings
        let mut cached = lock_write(&self.settings)?;
        *cached = None;

        Ok(())
    }
}

// =============================================================================
// WalletStorageProvider Implementation
// =============================================================================

#[async_trait]
impl WalletStorageProvider for StorageSqlx {
    fn storage_identity_key(&self) -> String {
        // Empty on a poisoned lock.
        lock_read(&self.storage_identity_key)
            .map(|key| key.clone())
            .unwrap_or_default()
    }

    fn storage_name(&self) -> String {
        // Empty on a poisoned lock.
        lock_read(&self.storage_name)
            .map(|name| name.clone())
            .unwrap_or_default()
    }

    fn broadcast_memory(
        &self,
    ) -> Option<Arc<dyn crate::services::broadcast_memory::BroadcastMemory>> {
        Some(Arc::clone(&self.broadcast_memory)
            as Arc<
                dyn crate::services::broadcast_memory::BroadcastMemory,
            >)
    }

    fn set_services(&self, services: Arc<dyn WalletServices>) {
        // The services get this storage's persisted broadcast memory, so
        // broadcasts send each provider only what it has not seen (0.3.56).
        services.set_broadcast_memory(Arc::clone(&self.broadcast_memory)
            as Arc<dyn crate::services::broadcast_memory::BroadcastMemory>);
        // If lock is poisoned, log and skip rather than panicking
        match lock_write(&self.services) {
            Ok(mut guard) => *guard = Some(services),
            Err(e) => tracing::error!("Failed to set services: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[tokio::test]
    async fn test_in_memory_storage() {
        let storage = StorageSqlx::in_memory().await.unwrap();

        // Migrate
        let version = storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        assert_eq!(version, "005_proof_root_checks");

        // Make available
        let settings = storage.make_available().await.unwrap();
        assert_eq!(settings.storage_name, "test-storage");
        assert!(storage.is_available());
    }
}
