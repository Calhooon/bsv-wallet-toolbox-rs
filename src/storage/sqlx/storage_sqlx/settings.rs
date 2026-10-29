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
                chain: match row.get::<String, _>("chain").as_str() {
                    "mainnet" => "main",
                    "testnet" => "test",
                    chain => chain,
                }
                .to_string(),
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
}

impl StorageSqlx {
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

        // A fresh row takes the configured chain. Existing rows keep their
        // network, with read_settings mapping legacy words before save_settings
        // rewrites them to the reference's spelling (ts-stack edf6e03).
        let chain = match self.read_settings().await? {
            Some(existing) => existing.chain,
            None => self.options.chain.as_str().to_string(),
        };

        // Save settings
        let settings = TableSettings {
            settings_id: 1,
            storage_identity_key: storage_identity_key.to_string(),
            storage_name: storage_name.to_string(),
            chain,
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
    use super::super::{StorageSqlx, WalletStorageReader, WalletStorageWriter};
    use crate::services::Chain;
    use crate::storage::sqlx::StorageSqlxOptions;

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

    async fn stored_chain(storage: &StorageSqlx) -> String {
        sqlx::query_scalar("SELECT chain FROM settings")
            .fetch_one(storage.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_new_row_stores_main() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        assert_eq!(stored_chain(&storage).await, "main");
        assert_eq!(storage.get_settings().chain, "main");
    }

    async fn configured_storage(chain: Chain) -> StorageSqlx {
        let options = StorageSqlxOptions {
            chain,
            ..Default::default()
        };
        let storage = StorageSqlx::new_with_options("sqlite::memory:", options)
            .await
            .unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage
    }

    #[tokio::test]
    async fn a_new_row_uses_the_configured_test_chain() {
        let storage = configured_storage(Chain::Test).await;
        assert_eq!(storage.get_settings().chain, "test");
        assert_eq!(stored_chain(&storage).await, "test");
        assert_eq!(storage.make_available().await.unwrap().chain, "test");
    }

    #[tokio::test]
    async fn a_new_row_uses_the_configured_regtest_chain() {
        assert_eq!(Chain::Regtest.as_str(), "regtest");
        let storage = configured_storage(Chain::Regtest).await;
        assert_eq!(storage.get_settings().chain, "regtest");
        assert_eq!(stored_chain(&storage).await, "regtest");
        assert_eq!(storage.make_available().await.unwrap().chain, "regtest");
    }

    async fn remigrate_keeps(chain: Chain) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("chain.sqlite");
        let path = path.to_str().unwrap();
        let options = StorageSqlxOptions {
            chain,
            ..Default::default()
        };
        let storage = StorageSqlx::open_with_options(path, options).await.unwrap();
        let key = "0".repeat(64);
        storage.migrate("test-storage", &key).await.unwrap();
        assert_eq!(stored_chain(&storage).await, chain.as_str());
        drop(storage);

        // Reopening with the default must keep the existing network.
        let reopened = StorageSqlx::open(path).await.unwrap();
        assert_eq!(
            reopened.make_available().await.unwrap().chain,
            chain.as_str()
        );
        reopened.migrate("renamed-storage", &key).await.unwrap();
        assert_eq!(reopened.get_settings().chain, chain.as_str());
        assert_eq!(stored_chain(&reopened).await, chain.as_str());
    }

    #[tokio::test]
    async fn remigrate_keeps_a_test_row() {
        remigrate_keeps(Chain::Test).await;
    }

    #[tokio::test]
    async fn remigrate_keeps_a_regtest_row() {
        remigrate_keeps(Chain::Regtest).await;
    }

    #[test]
    fn default_settings_use_main() {
        assert_eq!(super::TableSettings::default().chain, "main");
    }

    async fn legacy_word_is_mapped_and_rewritten(legacy: &str, expected: &str) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let key = "0".repeat(64);
        storage.migrate("test-storage", &key).await.unwrap();
        // Only a legacy-row fixture writes the chain directly.
        sqlx::query("UPDATE settings SET chain = ?")
            .bind(legacy)
            .execute(storage.writer())
            .await
            .unwrap();

        assert_eq!(storage.make_available().await.unwrap().chain, expected);
        assert_eq!(storage.get_settings().chain, expected);
        assert_eq!(stored_chain(&storage).await, legacy);

        storage.migrate("renamed-storage", &key).await.unwrap();
        assert_eq!(stored_chain(&storage).await, expected);
        assert_eq!(storage.get_settings().chain, expected);
        assert_eq!(storage.get_settings().storage_name, "renamed-storage");
    }

    #[tokio::test]
    async fn legacy_mainnet_reads_main_and_migrate_rewrites_it() {
        legacy_word_is_mapped_and_rewritten("mainnet", "main").await;
    }

    #[tokio::test]
    async fn legacy_testnet_reads_test_and_migrate_rewrites_it() {
        legacy_word_is_mapped_and_rewritten("testnet", "test").await;
    }

    #[test]
    fn regtest_chain_serde_word_is_regtest() {
        let chain: crate::services::Chain = serde_json::from_str("\"regtest\"").unwrap();
        assert_eq!(chain.as_str(), "regtest");
        assert_eq!(serde_json::to_string(&chain).unwrap(), "\"regtest\"");
    }
}
