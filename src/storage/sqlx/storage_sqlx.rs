//! SQLx-based storage provider implementation.
//!
//! This module provides a storage backend using SQLx with SQLite support.
//! It implements the `WalletStorageProvider` trait hierarchy.

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::entities::*;
use crate::storage::traits::*;

use bsv_sdk::transaction::ChainTracker;
use bsv_sdk::wallet::{
    AbortActionArgs, AbortActionResult, InternalizeActionArgs,
    ListActionsArgs, ListActionsResult, ListCertificatesArgs, ListCertificatesResult,
    ListOutputsArgs, ListOutputsResult, RelinquishCertificateArgs, RelinquishOutputArgs,
};

/// Default maximum length for output scripts stored in the outputs table.
/// Scripts longer than this will be retrieved from the raw transaction.
pub const DEFAULT_MAX_OUTPUT_SCRIPT: i32 = 10_000;

/// SQLite storage provider.
///
/// Implements the full `WalletStorageProvider` trait hierarchy using SQLx
/// with SQLite as the backend.
pub struct StorageSqlx {
    /// Database connection pool.
    pool: Pool<Sqlite>,
    /// Cached settings (loaded on make_available).
    settings: std::sync::RwLock<Option<TableSettings>>,
    /// Storage identity key (set during migration).
    storage_identity_key: std::sync::RwLock<String>,
    /// Storage name (set during migration).
    storage_name: std::sync::RwLock<String>,
    /// Optional ChainTracker for BEEF verification.
    /// When set, create_action will verify BEEF merkle roots against the chain.
    chain_tracker: RwLock<Option<Arc<dyn ChainTracker>>>,
}

impl StorageSqlx {
    /// Create a new SQLite storage provider.
    ///
    /// # Arguments
    ///
    /// * `database_url` - SQLite database URL (e.g., "sqlite:wallet.db" or "sqlite::memory:")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let storage = StorageSqlx::new("sqlite:wallet.db").await?;
    /// ```
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;

        Ok(Self {
            pool,
            settings: std::sync::RwLock::new(None),
            storage_identity_key: std::sync::RwLock::new(String::new()),
            storage_name: std::sync::RwLock::new(String::new()),
            chain_tracker: RwLock::new(None),
        })
    }

    /// Set the ChainTracker for BEEF verification.
    ///
    /// When set, `create_action` will verify BEEF merkle roots against the chain
    /// before returning. This matches TypeScript/Go behavior.
    ///
    /// # Arguments
    /// * `tracker` - The chain tracker to use for verification
    ///
    /// # Example
    /// ```rust,ignore
    /// let storage = StorageSqlx::in_memory().await?;
    /// storage.set_chain_tracker(Arc::new(my_chaintracks)).await;
    /// ```
    pub async fn set_chain_tracker(&self, tracker: Arc<dyn ChainTracker>) {
        let mut ct = self.chain_tracker.write().await;
        *ct = Some(tracker);
    }

    /// Clear the ChainTracker (disable BEEF verification).
    pub async fn clear_chain_tracker(&self) {
        let mut ct = self.chain_tracker.write().await;
        *ct = None;
    }

    /// Get a reference to the current ChainTracker, if set.
    pub(crate) async fn get_chain_tracker(&self) -> Option<Arc<dyn ChainTracker>> {
        let ct = self.chain_tracker.read().await;
        ct.clone()
    }

    /// Open an in-memory SQLite database.
    ///
    /// Useful for testing.
    pub async fn in_memory() -> Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Open a file-based SQLite database.
    ///
    /// Creates the database file if it doesn't exist.
    pub async fn open(path: &str) -> Result<Self> {
        let url = format!("sqlite:{}?mode=rwc", path);
        Self::new(&url).await
    }

    /// Get the database connection pool.
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }

    /// Run the initial migration SQL.
    async fn run_migrations(&self) -> Result<()> {
        let sql = include_str!("migrations/001_initial.sql");

        // Remove comments and split by semicolons
        let sql_without_comments: String = sql
            .lines()
            .filter(|line| !line.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        // Split by semicolons and execute each statement
        for statement in sql_without_comments.split(';') {
            let statement = statement.trim();
            if !statement.is_empty() {
                sqlx::query(statement)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| Error::MigrationError(format!("{}: {}", e, statement)))?;
            }
        }

        Ok(())
    }

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
        .fetch_optional(&self.pool)
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
        .fetch_optional(&self.pool)
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
    pub async fn insert_user(
        &self,
        identity_key: &str,
        active_storage: &str,
    ) -> Result<TableUser> {
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
        .execute(&self.pool)
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
        .execute(&self.pool)
        .await?;

        Ok(())
    }

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
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(TableSettings {
                settings_id: row.get("settings_id"),
                storage_identity_key: row.get("storage_identity_key"),
                storage_name: row.get("storage_name"),
                chain: row.get("chain"),
                max_output_script: row.get("max_output_script"),
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
            .execute(&self.pool)
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
            .execute(&self.pool)
            .await?;
        }

        // Update cache
        let mut cached = self.settings.write().unwrap();
        *cached = Some(settings.clone());

        Ok(())
    }

    // =========================================================================
    // Output Repository
    // =========================================================================

    /// Find outputs matching the given criteria.
    pub async fn find_outputs_internal(
        &self,
        user_id: i64,
        args: &FindOutputsArgs,
    ) -> Result<Vec<TableOutput>> {
        let mut sql = String::from(
            r#"
            SELECT o.output_id, o.user_id, o.transaction_id, o.basket_id, o.txid, o.vout,
                   o.satoshis, o.locking_script, o.script_length, o.script_offset,
                   o.type, o.spendable, o.change, o.derivation_prefix, o.derivation_suffix,
                   o.sender_identity_key, o.custom_instructions, o.created_at, o.updated_at
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.user_id = ?
            "#,
        );

        let mut binds: Vec<String> = vec![user_id.to_string()];

        if let Some(basket_id) = args.basket_id {
            sql.push_str(" AND o.basket_id = ?");
            binds.push(basket_id.to_string());
        }

        if let Some(txid) = &args.txid {
            sql.push_str(" AND o.txid = ?");
            binds.push(txid.clone());
        }

        if let Some(vout) = args.vout {
            sql.push_str(" AND o.vout = ?");
            binds.push(vout.to_string());
        }

        if let Some(statuses) = &args.tx_status {
            if !statuses.is_empty() {
                let placeholders: Vec<&str> = statuses.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND t.status IN ({})", placeholders.join(",")));
                for status in statuses {
                    binds.push(status.as_str().to_string());
                }
            }
        }

        if let Some(ref base) = args.base.since {
            sql.push_str(" AND o.updated_at > ?");
            binds.push(base.to_rfc3339());
        }

        // Order by updated_at
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY o.updated_at DESC");
        } else {
            sql.push_str(" ORDER BY o.updated_at ASC");
        }

        // Pagination
        if let Some(ref paged) = args.base.paged {
            if let Some(limit) = paged.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = paged.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        // Build the query dynamically - we need to use a different approach
        // since sqlx doesn't support dynamic binding easily
        let mut query = sqlx::query(&sql);

        // Bind user_id first
        query = query.bind(user_id);

        // Bind other parameters based on what was added
        if args.basket_id.is_some() {
            query = query.bind(args.basket_id.unwrap());
        }
        if args.txid.is_some() {
            query = query.bind(args.txid.as_ref().unwrap());
        }
        if args.vout.is_some() {
            query = query.bind(args.vout.unwrap() as i32);
        }
        if let Some(statuses) = &args.tx_status {
            for status in statuses {
                query = query.bind(status.as_str());
            }
        }
        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut outputs = Vec::new();
        for row in rows {
            let locking_script: Option<Vec<u8>> = if args.no_script.unwrap_or(false) {
                None
            } else {
                row.get("locking_script")
            };

            outputs.push(TableOutput {
                output_id: row.get("output_id"),
                user_id: row.get("user_id"),
                transaction_id: row.get("transaction_id"),
                basket_id: row.get("basket_id"),
                txid: row.get("txid"),
                vout: row.get("vout"),
                satoshis: row.get("satoshis"),
                locking_script,
                script_length: row.get("script_length"),
                script_offset: row.get("script_offset"),
                output_type: row.get("type"),
                spendable: row.get("spendable"),
                change: row.get("change"),
                derivation_prefix: row.get("derivation_prefix"),
                derivation_suffix: row.get("derivation_suffix"),
                sender_identity_key: row.get("sender_identity_key"),
                custom_instructions: row.get("custom_instructions"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(outputs)
    }

    /// Find output by txid and vout.
    pub async fn find_output_by_outpoint(
        &self,
        user_id: i64,
        txid: &str,
        vout: u32,
    ) -> Result<Option<TableOutput>> {
        let row = sqlx::query(
            r#"
            SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                   satoshis, locking_script, script_length, script_offset,
                   type, spendable, change, derivation_prefix, derivation_suffix,
                   sender_identity_key, custom_instructions, created_at, updated_at
            FROM outputs
            WHERE user_id = ? AND txid = ? AND vout = ?
            "#,
        )
        .bind(user_id)
        .bind(txid)
        .bind(vout as i32)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(TableOutput {
                output_id: row.get("output_id"),
                user_id: row.get("user_id"),
                transaction_id: row.get("transaction_id"),
                basket_id: row.get("basket_id"),
                txid: row.get("txid"),
                vout: row.get("vout"),
                satoshis: row.get("satoshis"),
                locking_script: row.get("locking_script"),
                script_length: row.get("script_length"),
                script_offset: row.get("script_offset"),
                output_type: row.get("type"),
                spendable: row.get("spendable"),
                change: row.get("change"),
                derivation_prefix: row.get("derivation_prefix"),
                derivation_suffix: row.get("derivation_suffix"),
                sender_identity_key: row.get("sender_identity_key"),
                custom_instructions: row.get("custom_instructions"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    // =========================================================================
    // Output Basket Repository
    // =========================================================================

    /// Find output baskets for a user.
    pub async fn find_output_baskets_internal(
        &self,
        user_id: i64,
        args: &FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let mut sql = String::from(
            r#"
            SELECT basket_id, user_id, name, number_of_desired_utxos, minimum_desired_utxo_value,
                   created_at, updated_at
            FROM output_baskets
            WHERE user_id = ? AND is_deleted = 0
            "#,
        );

        if args.name.is_some() {
            sql.push_str(" AND name = ?");
        }

        if args.base.since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }

        // Order
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY updated_at DESC");
        } else {
            sql.push_str(" ORDER BY updated_at ASC");
        }

        // Pagination
        if let Some(ref paged) = args.base.paged {
            if let Some(limit) = paged.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = paged.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        let mut query = sqlx::query(&sql).bind(user_id);

        if let Some(ref name) = args.name {
            query = query.bind(name);
        }

        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut baskets = Vec::new();
        for row in rows {
            baskets.push(TableOutputBasket {
                basket_id: row.get("basket_id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                number_of_desired_utxos: row.get("number_of_desired_utxos"),
                minimum_desired_utxo_value: row.get("minimum_desired_utxo_value"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(baskets)
    }

    /// Find or create a default basket for a user.
    pub async fn find_or_create_default_basket(&self, user_id: i64) -> Result<TableOutputBasket> {
        // Try to find existing
        let args = FindOutputBasketsArgs {
            user_id: Some(user_id),
            name: Some("default".to_string()),
            ..Default::default()
        };

        let baskets = self.find_output_baskets_internal(user_id, &args).await?;

        if let Some(basket) = baskets.into_iter().next() {
            return Ok(basket);
        }

        // Create new default basket
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
            VALUES (?, 'default', 6, 10000, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(TableOutputBasket {
            basket_id: result.last_insert_rowid(),
            user_id,
            name: "default".to_string(),
            number_of_desired_utxos: 6,
            minimum_desired_utxo_value: 10000,
            created_at: now,
            updated_at: now,
        })
    }

    // =========================================================================
    // Certificate Repository
    // =========================================================================

    /// Find certificates matching criteria.
    pub async fn find_certificates_internal(
        &self,
        user_id: i64,
        args: &FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        let mut sql = String::from(
            r#"
            SELECT certificate_id, user_id, type, serial_number, certifier, subject,
                   verifier, revocation_outpoint, signature, created_at, updated_at
            FROM certificates
            WHERE user_id = ? AND is_deleted = 0
            "#,
        );

        if let Some(ref certifiers) = args.certifiers {
            if !certifiers.is_empty() {
                let placeholders: Vec<&str> = certifiers.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND certifier IN ({})", placeholders.join(",")));
            }
        }

        if let Some(ref types) = args.types {
            if !types.is_empty() {
                let placeholders: Vec<&str> = types.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND type IN ({})", placeholders.join(",")));
            }
        }

        if args.base.since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }

        // Order
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY updated_at DESC");
        } else {
            sql.push_str(" ORDER BY updated_at ASC");
        }

        // Pagination
        if let Some(ref paged) = args.base.paged {
            if let Some(limit) = paged.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = paged.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        let mut query = sqlx::query(&sql).bind(user_id);

        if let Some(ref certifiers) = args.certifiers {
            for certifier in certifiers {
                query = query.bind(certifier);
            }
        }

        if let Some(ref types) = args.types {
            for t in types {
                query = query.bind(t);
            }
        }

        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut certificates = Vec::new();
        for row in rows {
            certificates.push(TableCertificate {
                certificate_id: row.get("certificate_id"),
                user_id: row.get("user_id"),
                cert_type: row.get("type"),
                serial_number: row.get("serial_number"),
                certifier: row.get("certifier"),
                subject: row.get("subject"),
                verifier: row.get("verifier"),
                revocation_outpoint: row.get("revocation_outpoint"),
                signature: row.get("signature"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(certificates)
    }

    // =========================================================================
    // ProvenTxReq Repository
    // =========================================================================

    /// Find proven tx requests.
    pub async fn find_proven_tx_reqs_internal(
        &self,
        args: &FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        let mut sql = String::from(
            r#"
            SELECT proven_tx_req_id, proven_tx_id, txid, status, attempts, history,
                   notified, created_at, updated_at
            FROM proven_tx_reqs
            WHERE 1=1
            "#,
        );

        if let Some(ref statuses) = args.status {
            if !statuses.is_empty() {
                let placeholders: Vec<&str> = statuses.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND status IN ({})", placeholders.join(",")));
            }
        }

        if let Some(ref txids) = args.txids {
            if !txids.is_empty() {
                let placeholders: Vec<&str> = txids.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND txid IN ({})", placeholders.join(",")));
            }
        }

        if args.base.since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }

        // Order
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY updated_at DESC");
        } else {
            sql.push_str(" ORDER BY updated_at ASC");
        }

        // Pagination
        if let Some(ref paged) = args.base.paged {
            if let Some(limit) = paged.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = paged.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }

        let mut query = sqlx::query(&sql);

        if let Some(ref statuses) = args.status {
            for status in statuses {
                query = query.bind(format!("{:?}", status).to_lowercase());
            }
        }

        if let Some(ref txids) = args.txids {
            for txid in txids {
                query = query.bind(txid);
            }
        }

        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut reqs = Vec::new();
        for row in rows {
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "pending" => ProvenTxReqStatus::Pending,
                "inprogress" | "in_progress" => ProvenTxReqStatus::InProgress,
                "completed" => ProvenTxReqStatus::Completed,
                "failed" => ProvenTxReqStatus::Failed,
                "notfound" | "not_found" => ProvenTxReqStatus::NotFound,
                _ => ProvenTxReqStatus::Pending,
            };

            let notified_val: i32 = row.get("notified");
            let notify_txid = if notified_val != 0 {
                Some(row.get::<String, _>("txid"))
            } else {
                None
            };

            reqs.push(TableProvenTxReq {
                proven_tx_req_id: row.get("proven_tx_req_id"),
                txid: row.get("txid"),
                status,
                attempts: row.get("attempts"),
                history: row.get("history"),
                notify_txid,
                proven_tx_id: row.get("proven_tx_id"),
                batch: row.try_get("batch").ok(),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(reqs)
    }

    // =========================================================================
    // Sync State Repository
    // =========================================================================

    /// Find or create a sync state.
    pub async fn find_or_insert_sync_state_internal(
        &self,
        user_id: i64,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)> {
        // Try to find existing
        let row = sqlx::query(
            r#"
            SELECT sync_state_id, user_id, storage_identity_key, storage_name, status, init,
                   ref_num, sync_map, when_last_sync_started, error_local, error_other,
                   created_at, updated_at
            FROM sync_states
            WHERE user_id = ? AND storage_identity_key = ?
            "#,
        )
        .bind(user_id)
        .bind(storage_identity_key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let sync_state = TableSyncState {
                sync_state_id: row.get("sync_state_id"),
                user_id: row.get("user_id"),
                storage_identity_key: row.get("storage_identity_key"),
                storage_name: row.get("storage_name"),
                status: row.get("status"),
                init: row.get("init"),
                ref_num: row.get("ref_num"),
                sync_map: row.get("sync_map"),
                when_last_sync_started: row.get("when_last_sync_started"),
                error_local: row.get("error_local"),
                error_other: row.get("error_other"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            };
            return Ok((sync_state, false));
        }

        // Create new
        let now = Utc::now();
        let ref_num = uuid::Uuid::new_v4().to_string();

        let result = sqlx::query(
            r#"
            INSERT INTO sync_states (user_id, storage_identity_key, storage_name, status, init, ref_num, sync_map, created_at, updated_at)
            VALUES (?, ?, ?, 'unknown', 0, ?, '{}', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(storage_identity_key)
        .bind(storage_name)
        .bind(&ref_num)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let sync_state = TableSyncState {
            sync_state_id: result.last_insert_rowid(),
            user_id,
            storage_identity_key: storage_identity_key.to_string(),
            storage_name: storage_name.to_string(),
            status: "unknown".to_string(),
            init: false,
            ref_num,
            sync_map: "{}".to_string(),
            when_last_sync_started: None,
            error_local: None,
            error_other: None,
            created_at: now,
            updated_at: now,
        };

        Ok((sync_state, true))
    }
}

// =============================================================================
// WalletStorageReader Implementation
// =============================================================================

#[async_trait]
impl WalletStorageReader for StorageSqlx {
    fn is_available(&self) -> bool {
        self.settings.read().unwrap().is_some()
    }

    fn get_settings(&self) -> &TableSettings {
        // This is a bit awkward due to the RwLock, but the trait requires &self
        // In practice, make_available() should be called first
        static DEFAULT_SETTINGS: std::sync::OnceLock<TableSettings> = std::sync::OnceLock::new();
        let guard = self.settings.read().unwrap();
        if let Some(ref settings) = *guard {
            // SAFETY: This is a workaround for the trait signature
            // The settings are effectively static once loaded
            unsafe { &*(settings as *const TableSettings) }
        } else {
            DEFAULT_SETTINGS.get_or_init(TableSettings::default)
        }
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;
        self.find_certificates_internal(user_id, &args).await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;
        self.find_output_baskets_internal(user_id, &args).await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;
        self.find_outputs_internal(user_id, &args).await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.find_proven_tx_reqs_internal(&args).await
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        use bsv_sdk::wallet::{
            ActionStatus, Outpoint, QueryMode, WalletAction, WalletActionInput, WalletActionOutput,
        };

        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let label_query_mode = args.label_query_mode.unwrap_or(QueryMode::Any);
        let include_labels = args.include_labels.unwrap_or(false);
        let include_inputs = args.include_inputs.unwrap_or(false);
        let include_outputs = args.include_outputs.unwrap_or(false);
        let include_input_source_locking_scripts =
            args.include_input_source_locking_scripts.unwrap_or(false);
        let include_input_unlocking_scripts = args.include_input_unlocking_scripts.unwrap_or(false);
        let include_output_locking_scripts = args.include_output_locking_scripts.unwrap_or(false);

        // Valid transaction statuses to include
        let valid_statuses = vec![
            "completed",
            "unprocessed",
            "sending",
            "unproven",
            "unsigned",
            "nosend",
            "nonfinal",
        ];

        // If labels are provided, look up their IDs
        let mut label_ids: Vec<i64> = Vec::new();
        if !args.labels.is_empty() {
            let placeholders: Vec<&str> = args.labels.iter().map(|_| "?").collect();
            let sql = format!(
                r#"
                SELECT tx_label_id FROM tx_labels
                WHERE user_id = ? AND is_deleted = 0 AND label IN ({})
                "#,
                placeholders.join(",")
            );

            let mut query = sqlx::query(&sql).bind(user_id);
            for label in &args.labels {
                query = query.bind(label);
            }

            let rows = query.fetch_all(&self.pool).await?;
            label_ids = rows
                .iter()
                .map(|r| r.get::<i64, _>("tx_label_id"))
                .collect();

            // If using 'all' mode and not all labels exist, return empty
            if label_query_mode == QueryMode::All && label_ids.len() < args.labels.len() {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }

            // If using 'any' mode and no labels exist, return empty
            if label_query_mode == QueryMode::Any && label_ids.is_empty() && !args.labels.is_empty()
            {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }
        }

        // Build the main query
        let (transactions, total_count): (Vec<_>, u32) = if label_ids.is_empty() {
            // No label filtering - simple query
            let status_placeholders: Vec<&str> = valid_statuses.iter().map(|_| "?").collect();
            let sql = format!(
                r#"
                SELECT transaction_id, txid, satoshis, status, is_outgoing, description, version, lock_time
                FROM transactions
                WHERE user_id = ? AND status IN ({})
                ORDER BY transaction_id ASC
                LIMIT ? OFFSET ?
                "#,
                status_placeholders.join(",")
            );

            let mut query = sqlx::query(&sql).bind(user_id);
            for status in &valid_statuses {
                query = query.bind(*status);
            }
            query = query.bind(limit as i32).bind(offset as i32);

            let rows = query.fetch_all(&self.pool).await?;

            // Get total count
            let count_sql = format!(
                r#"
                SELECT COUNT(*) as total FROM transactions
                WHERE user_id = ? AND status IN ({})
                "#,
                status_placeholders.join(",")
            );
            let mut count_query = sqlx::query(&count_sql).bind(user_id);
            for status in &valid_statuses {
                count_query = count_query.bind(*status);
            }
            let count_row = count_query.fetch_one(&self.pool).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        } else {
            // Label filtering with CTE
            let status_placeholders: String = valid_statuses
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(",");
            let label_id_list: String = label_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let required_count = if label_query_mode == QueryMode::All {
                label_ids.len()
            } else {
                1
            };

            let sql = format!(
                r#"
                WITH txs_with_labels AS (
                    SELECT t.transaction_id, t.txid, t.satoshis, t.status, t.is_outgoing,
                           t.description, t.version, t.lock_time,
                           (SELECT COUNT(*) FROM tx_labels_map m
                            WHERE m.transaction_id = t.transaction_id
                            AND m.tx_label_id IN ({})
                            AND m.is_deleted = 0) as label_count
                    FROM transactions t
                    WHERE t.user_id = ? AND t.status IN ({})
                )
                SELECT * FROM txs_with_labels WHERE label_count >= ?
                ORDER BY transaction_id ASC
                LIMIT ? OFFSET ?
                "#,
                label_id_list, status_placeholders
            );

            let query = sqlx::query(&sql)
                .bind(user_id)
                .bind(required_count as i32)
                .bind(limit as i32)
                .bind(offset as i32);

            let rows = query.fetch_all(&self.pool).await?;

            // Get total count
            let count_sql = format!(
                r#"
                WITH txs_with_labels AS (
                    SELECT t.transaction_id,
                           (SELECT COUNT(*) FROM tx_labels_map m
                            WHERE m.transaction_id = t.transaction_id
                            AND m.tx_label_id IN ({})
                            AND m.is_deleted = 0) as label_count
                    FROM transactions t
                    WHERE t.user_id = ? AND t.status IN ({})
                )
                SELECT COUNT(*) as total FROM txs_with_labels WHERE label_count >= ?
                "#,
                label_id_list, status_placeholders
            );
            let count_query = sqlx::query(&count_sql)
                .bind(user_id)
                .bind(required_count as i32);
            let count_row = count_query.fetch_one(&self.pool).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        };

        // Convert rows to WalletAction
        let mut actions: Vec<WalletAction> = Vec::new();
        for row in &transactions {
            let transaction_id: i64 = row.get("transaction_id");
            let txid_str: Option<String> = row.get("txid");
            let satoshis: i64 = row.get("satoshis");
            let status_str: String = row.get("status");
            let is_outgoing: i32 = row.get("is_outgoing");
            let description: String = row.get("description");
            let version: Option<i32> = row.get("version");
            let lock_time: Option<i64> = row.get("lock_time");

            // Parse txid to [u8; 32]
            let txid: [u8; 32] = if let Some(ref txid_hex) = txid_str {
                let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                let mut arr = [0u8; 32];
                if bytes.len() == 32 {
                    arr.copy_from_slice(&bytes);
                }
                arr
            } else {
                [0u8; 32]
            };

            // Parse status
            let status = match status_str.as_str() {
                "completed" => ActionStatus::Completed,
                "unprocessed" => ActionStatus::Unprocessed,
                "sending" => ActionStatus::Sending,
                "unproven" => ActionStatus::Unproven,
                "unsigned" => ActionStatus::Unsigned,
                "nosend" => ActionStatus::NoSend,
                "nonfinal" => ActionStatus::NonFinal,
                "failed" => ActionStatus::Failed,
                _ => ActionStatus::Unprocessed,
            };

            let mut action = WalletAction {
                txid,
                satoshis: satoshis as u64,
                status,
                is_outgoing: is_outgoing != 0,
                description,
                labels: None,
                version: version.unwrap_or(1) as u32,
                lock_time: lock_time.unwrap_or(0) as u32,
                inputs: None,
                outputs: None,
            };

            // Fetch labels if requested
            if include_labels {
                let labels_sql = r#"
                    SELECT l.label FROM tx_labels l
                    JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id
                    WHERE m.transaction_id = ? AND m.is_deleted = 0 AND l.is_deleted = 0
                "#;
                let label_rows = sqlx::query(labels_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.pool)
                    .await?;
                let labels: Vec<String> = label_rows.iter().map(|r| r.get("label")).collect();
                action.labels = Some(labels);
            }

            // Fetch outputs if requested
            if include_outputs {
                let outputs_sql = r#"
                    SELECT o.output_id, o.vout, o.satoshis, o.spendable, o.locking_script,
                           o.custom_instructions, o.output_description, ob.name as basket_name
                    FROM outputs o
                    LEFT JOIN output_baskets ob ON o.basket_id = ob.basket_id
                    WHERE o.transaction_id = ?
                    ORDER BY o.vout ASC
                "#;
                let output_rows = sqlx::query(outputs_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.pool)
                    .await?;

                let mut wallet_outputs: Vec<WalletActionOutput> = Vec::new();
                for o_row in output_rows {
                    let output_id: i64 = o_row.get("output_id");
                    let vout: i32 = o_row.get("vout");
                    let o_satoshis: i64 = o_row.get("satoshis");
                    let spendable: i32 = o_row.get("spendable");
                    let locking_script: Option<Vec<u8>> = if include_output_locking_scripts {
                        o_row.get("locking_script")
                    } else {
                        None
                    };
                    let custom_instructions: Option<String> = o_row.get("custom_instructions");
                    let output_description: Option<String> = o_row.get("output_description");
                    let basket_name: Option<String> = o_row.get("basket_name");

                    // Get tags for this output
                    let tags_sql = r#"
                        SELECT t.tag FROM output_tags t
                        JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id
                        WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0
                    "#;
                    let tag_rows = sqlx::query(tags_sql)
                        .bind(output_id)
                        .fetch_all(&self.pool)
                        .await?;
                    let tags: Vec<String> = tag_rows.iter().map(|r| r.get("tag")).collect();

                    wallet_outputs.push(WalletActionOutput {
                        satoshis: o_satoshis as u64,
                        locking_script,
                        spendable: spendable != 0,
                        custom_instructions,
                        tags,
                        output_index: vout as u32,
                        output_description: output_description.unwrap_or_default(),
                        basket: basket_name.unwrap_or_default(),
                    });
                }
                action.outputs = Some(wallet_outputs);
            }

            // Fetch inputs if requested
            if include_inputs {
                // Inputs are outputs from other transactions spent by this transaction
                let inputs_sql = r#"
                    SELECT o.txid, o.vout, o.satoshis, o.locking_script, o.output_description, o.sequence_number
                    FROM outputs o
                    WHERE o.spent_by = ?
                    ORDER BY o.sequence_number ASC
                "#;
                let input_rows = sqlx::query(inputs_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.pool)
                    .await?;

                let mut wallet_inputs: Vec<WalletActionInput> = Vec::new();

                // If we need unlocking scripts, we need to parse the raw transaction
                let _raw_tx: Option<Vec<u8>> = if include_input_unlocking_scripts {
                    let tx_row =
                        sqlx::query("SELECT raw_tx FROM transactions WHERE transaction_id = ?")
                            .bind(transaction_id)
                            .fetch_optional(&self.pool)
                            .await?;
                    tx_row.and_then(|r| r.get("raw_tx"))
                } else {
                    None
                };

                for i_row in input_rows.iter() {
                    let source_txid: Option<String> = i_row.get("txid");
                    let source_vout: i32 = i_row.get("vout");
                    let source_satoshis: i64 = i_row.get("satoshis");
                    let source_locking_script: Option<Vec<u8>> =
                        if include_input_source_locking_scripts {
                            i_row.get("locking_script")
                        } else {
                            None
                        };
                    let input_description: Option<String> = i_row.get("output_description");
                    let sequence_number: Option<i32> = i_row.get("sequence_number");

                    // Parse source txid
                    let source_txid_bytes: [u8; 32] = if let Some(ref txid_hex) = source_txid {
                        let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        arr
                    } else {
                        [0u8; 32]
                    };

                    // Get unlocking script from raw tx if available
                    // Note: Full implementation would parse the raw transaction
                    let unlocking_script: Option<Vec<u8>> = None;

                    wallet_inputs.push(WalletActionInput {
                        source_outpoint: Outpoint {
                            txid: source_txid_bytes,
                            vout: source_vout as u32,
                        },
                        source_satoshis: source_satoshis as u64,
                        source_locking_script,
                        unlocking_script,
                        input_description: input_description.unwrap_or_default(),
                        sequence_number: sequence_number.unwrap_or(0xffffffff_u32 as i32) as u32,
                    });
                }
                action.inputs = Some(wallet_inputs);
            }

            actions.push(action);
        }

        // Calculate total based on whether we hit the limit
        let total_actions = if (actions.len() as u32) < limit {
            offset + actions.len() as u32
        } else {
            total_count
        };

        Ok(ListActionsResult {
            total_actions,
            actions,
        })
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        use bsv_sdk::wallet::CertificateResult;
        use std::collections::HashMap;

        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);

        // Build query with filters
        let mut sql = String::from(
            r#"
            SELECT certificate_id, type, serial_number, certifier, subject,
                   verifier, revocation_outpoint, signature
            FROM certificates
            WHERE user_id = ? AND is_deleted = 0
            "#,
        );

        // Add certifiers filter
        if !args.certifiers.is_empty() {
            let placeholders: Vec<&str> = args.certifiers.iter().map(|_| "?").collect();
            sql.push_str(&format!(" AND certifier IN ({})", placeholders.join(",")));
        }

        // Add types filter
        if !args.types.is_empty() {
            let placeholders: Vec<&str> = args.types.iter().map(|_| "?").collect();
            sql.push_str(&format!(" AND type IN ({})", placeholders.join(",")));
        }

        sql.push_str(" ORDER BY certificate_id ASC LIMIT ? OFFSET ?");

        let mut query = sqlx::query(&sql).bind(user_id);

        for certifier in &args.certifiers {
            query = query.bind(certifier);
        }
        for cert_type in &args.types {
            query = query.bind(cert_type);
        }

        query = query.bind(limit as i32).bind(offset as i32);

        let rows = query.fetch_all(&self.pool).await?;

        // Get total count
        let mut count_sql = String::from(
            r#"
            SELECT COUNT(*) as total FROM certificates
            WHERE user_id = ? AND is_deleted = 0
            "#,
        );

        if !args.certifiers.is_empty() {
            let placeholders: Vec<&str> = args.certifiers.iter().map(|_| "?").collect();
            count_sql.push_str(&format!(" AND certifier IN ({})", placeholders.join(",")));
        }

        if !args.types.is_empty() {
            let placeholders: Vec<&str> = args.types.iter().map(|_| "?").collect();
            count_sql.push_str(&format!(" AND type IN ({})", placeholders.join(",")));
        }

        let mut count_query = sqlx::query(&count_sql).bind(user_id);
        for certifier in &args.certifiers {
            count_query = count_query.bind(certifier);
        }
        for cert_type in &args.types {
            count_query = count_query.bind(cert_type);
        }

        let count_row = count_query.fetch_one(&self.pool).await?;
        let total: i64 = count_row.get("total");

        // Build certificate results with fields
        let mut certificates: Vec<CertificateResult> = Vec::new();

        for row in rows {
            let certificate_id: i64 = row.get("certificate_id");
            let cert_type: String = row.get("type");
            let serial_number: String = row.get("serial_number");
            let certifier: String = row.get("certifier");
            let subject: String = row.get("subject");
            let verifier: Option<String> = row.get("verifier");
            let revocation_outpoint: String = row.get("revocation_outpoint");
            let signature: String = row.get("signature");

            // Get certificate fields
            let fields_sql = r#"
                SELECT field_name, field_value, master_key
                FROM certificate_fields
                WHERE certificate_id = ? AND user_id = ?
            "#;
            let field_rows = sqlx::query(fields_sql)
                .bind(certificate_id)
                .bind(user_id)
                .fetch_all(&self.pool)
                .await?;

            let mut fields: HashMap<String, String> = HashMap::new();
            let mut keyring: HashMap<String, String> = HashMap::new();

            for f_row in field_rows {
                let field_name: String = f_row.get("field_name");
                let field_value: String = f_row.get("field_value");
                let master_key: String = f_row.get("master_key");

                fields.insert(field_name.clone(), field_value);
                if !master_key.is_empty() {
                    keyring.insert(field_name, master_key);
                }
            }

            let wallet_cert = bsv_sdk::wallet::WalletCertificate {
                certificate_type: cert_type,
                subject,
                serial_number,
                certifier,
                revocation_outpoint,
                signature,
                fields,
            };

            certificates.push(CertificateResult {
                certificate: wallet_cert,
                keyring: if keyring.is_empty() {
                    None
                } else {
                    Some(keyring)
                },
                verifier,
            });
        }

        // Calculate total based on whether we hit the limit
        let total_certificates = if (certificates.len() as u32) < limit {
            offset + certificates.len() as u32
        } else {
            total as u32
        };

        Ok(ListCertificatesResult {
            total_certificates,
            certificates,
        })
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        use bsv_sdk::wallet::{Outpoint, OutputInclude, QueryMode, WalletOutput};

        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let order_by = if offset < 0 { "DESC" } else { "ASC" };
        let actual_offset = if offset < 0 {
            (-offset - 1) as u32
        } else {
            offset as u32
        };

        let tag_query_mode = args.tag_query_mode.unwrap_or(QueryMode::Any);
        let include_custom_instructions = args.include_custom_instructions.unwrap_or(false);
        let include_tags = args.include_tags.unwrap_or(false);
        let include_labels = args.include_labels.unwrap_or(false);
        let include_locking_scripts = args.include == Some(OutputInclude::LockingScripts);
        let include_transactions = args.include == Some(OutputInclude::EntireTransactions);

        // Find the basket ID
        let basket_id: Option<i64> = if !args.basket.is_empty() {
            let basket_row = sqlx::query(
                r#"
                SELECT basket_id FROM output_baskets
                WHERE user_id = ? AND name = ? AND is_deleted = 0
                "#,
            )
            .bind(user_id)
            .bind(&args.basket)
            .fetch_optional(&self.pool)
            .await?;

            match basket_row {
                Some(row) => Some(row.get("basket_id")),
                None => {
                    // Basket doesn't exist, return empty result
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }
            }
        } else {
            None
        };

        // If tags are provided, look up their IDs
        let mut tag_ids: Vec<i64> = Vec::new();
        if let Some(ref tags) = args.tags {
            if !tags.is_empty() {
                let placeholders: Vec<&str> = tags.iter().map(|_| "?").collect();
                let sql = format!(
                    r#"
                    SELECT output_tag_id FROM output_tags
                    WHERE user_id = ? AND is_deleted = 0 AND tag IN ({})
                    "#,
                    placeholders.join(",")
                );

                let mut query = sqlx::query(&sql).bind(user_id);
                for tag in tags {
                    query = query.bind(tag);
                }

                let rows = query.fetch_all(&self.pool).await?;
                tag_ids = rows
                    .iter()
                    .map(|r| r.get::<i64, _>("output_tag_id"))
                    .collect();

                // If using 'all' mode and not all tags exist, return empty
                if tag_query_mode == QueryMode::All && tag_ids.len() < tags.len() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }

                // If using 'any' mode and no tags exist, return empty
                if tag_query_mode == QueryMode::Any && tag_ids.is_empty() && !tags.is_empty() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }
            }
        }

        // Valid transaction statuses for outputs
        let valid_statuses = "'completed', 'unproven', 'nosend', 'sending'";

        // Build the main query
        let (output_rows, total_count): (Vec<_>, u32) = if tag_ids.is_empty() {
            // No tag filtering
            let mut sql = format!(
                r#"
                SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable,
                       o.locking_script, o.custom_instructions
                FROM outputs o
                JOIN transactions t ON o.transaction_id = t.transaction_id
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({})
                "#,
                valid_statuses
            );

            if let Some(bid) = basket_id {
                sql.push_str(&format!(" AND o.basket_id = {}", bid));
            }

            sql.push_str(&format!(
                " ORDER BY o.output_id {} LIMIT {} OFFSET {}",
                order_by, limit, actual_offset
            ));

            let query = sqlx::query(&sql).bind(user_id);
            let rows = query.fetch_all(&self.pool).await?;

            // Get total count
            let mut count_sql = format!(
                r#"
                SELECT COUNT(*) as total FROM outputs o
                JOIN transactions t ON o.transaction_id = t.transaction_id
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({})
                "#,
                valid_statuses
            );

            if let Some(bid) = basket_id {
                count_sql.push_str(&format!(" AND o.basket_id = {}", bid));
            }

            let count_row = sqlx::query(&count_sql)
                .bind(user_id)
                .fetch_one(&self.pool)
                .await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        } else {
            // Tag filtering with CTE
            let tag_id_list: String = tag_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let required_count = if tag_query_mode == QueryMode::All {
                tag_ids.len()
            } else {
                1
            };

            let basket_filter = if let Some(bid) = basket_id {
                format!(" AND o.basket_id = {}", bid)
            } else {
                String::new()
            };

            let sql = format!(
                r#"
                WITH outputs_with_tags AS (
                    SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable,
                           o.locking_script, o.custom_instructions,
                           (SELECT COUNT(*) FROM output_tags_map m
                            WHERE m.output_id = o.output_id
                            AND m.output_tag_id IN ({})
                            AND m.is_deleted = 0) as tag_count
                    FROM outputs o
                    JOIN transactions t ON o.transaction_id = t.transaction_id
                    WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){}
                )
                SELECT * FROM outputs_with_tags WHERE tag_count >= ?
                ORDER BY output_id {} LIMIT ? OFFSET ?
                "#,
                tag_id_list, valid_statuses, basket_filter, order_by
            );

            let query = sqlx::query(&sql)
                .bind(user_id)
                .bind(required_count as i32)
                .bind(limit as i32)
                .bind(actual_offset as i32);

            let rows = query.fetch_all(&self.pool).await?;

            // Get total count
            let count_sql = format!(
                r#"
                WITH outputs_with_tags AS (
                    SELECT o.output_id,
                           (SELECT COUNT(*) FROM output_tags_map m
                            WHERE m.output_id = o.output_id
                            AND m.output_tag_id IN ({})
                            AND m.is_deleted = 0) as tag_count
                    FROM outputs o
                    JOIN transactions t ON o.transaction_id = t.transaction_id
                    WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){}
                )
                SELECT COUNT(*) as total FROM outputs_with_tags WHERE tag_count >= ?
                "#,
                tag_id_list, valid_statuses, basket_filter
            );

            let count_query = sqlx::query(&count_sql)
                .bind(user_id)
                .bind(required_count as i32);
            let count_row = count_query.fetch_one(&self.pool).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        };

        // Convert rows to WalletOutput
        let mut outputs: Vec<WalletOutput> = Vec::new();
        let mut _txids_for_beef: Vec<String> = Vec::new();

        for row in &output_rows {
            let output_id: i64 = row.get("output_id");
            let transaction_id: i64 = row.get("transaction_id");
            let txid_str: Option<String> = row.get("txid");
            let vout: i32 = row.get("vout");
            let satoshis: i64 = row.get("satoshis");
            let spendable: i32 = row.get("spendable");
            let locking_script: Option<Vec<u8>> = if include_locking_scripts {
                row.get("locking_script")
            } else {
                None
            };
            let custom_instructions: Option<String> = if include_custom_instructions {
                row.get("custom_instructions")
            } else {
                None
            };

            // Parse txid to [u8; 32]
            let txid: [u8; 32] = if let Some(ref txid_hex) = txid_str {
                let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                let mut arr = [0u8; 32];
                if bytes.len() == 32 {
                    arr.copy_from_slice(&bytes);
                }
                if include_transactions {
                    _txids_for_beef.push(txid_hex.clone());
                }
                arr
            } else {
                [0u8; 32]
            };

            // Get tags if requested
            let tags: Option<Vec<String>> = if include_tags {
                let tags_sql = r#"
                    SELECT t.tag FROM output_tags t
                    JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id
                    WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0
                "#;
                let tag_rows = sqlx::query(tags_sql)
                    .bind(output_id)
                    .fetch_all(&self.pool)
                    .await?;
                Some(tag_rows.iter().map(|r| r.get("tag")).collect())
            } else {
                None
            };

            // Get labels if requested (from transaction)
            let labels: Option<Vec<String>> = if include_labels {
                let labels_sql = r#"
                    SELECT l.label FROM tx_labels l
                    JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id
                    WHERE m.transaction_id = ? AND m.is_deleted = 0 AND l.is_deleted = 0
                "#;
                let label_rows = sqlx::query(labels_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.pool)
                    .await?;
                Some(label_rows.iter().map(|r| r.get("label")).collect())
            } else {
                None
            };

            outputs.push(WalletOutput {
                satoshis: satoshis as u64,
                locking_script,
                spendable: spendable != 0,
                custom_instructions,
                tags,
                outpoint: Outpoint {
                    txid,
                    vout: vout as u32,
                },
                labels,
            });
        }

        // Build BEEF if requested
        // Note: Full BEEF implementation would require building a proper BEEF structure
        // For now, we return None and let the caller construct BEEF if needed
        let beef: Option<Vec<u8>> = if include_transactions {
            // TODO: Implement BEEF construction
            None
        } else {
            None
        };

        // Calculate total based on whether we hit the limit
        let total_outputs = if (outputs.len() as u32) < limit {
            actual_offset + outputs.len() as u32
        } else {
            total_count
        };

        Ok(ListOutputsResult {
            total_outputs,
            beef,
            outputs,
        })
    }
}

// =============================================================================
// WalletStorageWriter Implementation
// =============================================================================

#[async_trait]
impl WalletStorageWriter for StorageSqlx {
    async fn make_available(&self) -> Result<TableSettings> {
        let settings = self.read_settings().await?;

        if let Some(settings) = settings {
            let mut cached = self.settings.write().unwrap();
            *cached = Some(settings.clone());
            Ok(settings)
        } else {
            Err(Error::StorageNotAvailable)
        }
    }

    async fn migrate(&self, storage_name: &str, storage_identity_key: &str) -> Result<String> {
        // Run migrations
        self.run_migrations().await?;

        // Save settings
        let settings = TableSettings {
            settings_id: 1,
            storage_identity_key: storage_identity_key.to_string(),
            storage_name: storage_name.to_string(),
            chain: "mainnet".to_string(),
            max_output_script: DEFAULT_MAX_OUTPUT_SCRIPT,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.save_settings(&settings).await?;

        // Update internal state
        {
            let mut key = self.storage_identity_key.write().unwrap();
            *key = storage_identity_key.to_string();
        }
        {
            let mut name = self.storage_name.write().unwrap();
            *name = storage_name.to_string();
        }

        Ok("001_initial".to_string())
    }

    async fn destroy(&self) -> Result<()> {
        // Drop all tables in reverse dependency order
        let tables = [
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
            sqlx::query(&sql).execute(&self.pool).await?;
        }

        // Clear cached settings
        let mut cached = self.settings.write().unwrap();
        *cached = None;

        Ok(())
    }

    async fn find_or_insert_user(&self, identity_key: &str) -> Result<(TableUser, bool)> {
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

    async fn abort_action(
        &self,
        auth: &AuthId,
        args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        super::abort_action::abort_action_internal(self, user_id, args).await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_sdk::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        // Get the ChainTracker for BEEF verification (if set)
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());

        super::create_action::create_action_internal(self, tracker_ref, user_id, args).await
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        super::process_action::process_action_internal(self, user_id, args).await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        super::internalize_action::internalize_action_internal(self, user_id, args).await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        // Verify the certificate belongs to this user
        if certificate.user_id != user_id {
            return Err(Error::AccessDenied(
                "Certificate user_id does not match auth".to_string(),
            ));
        }

        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, verifier, revocation_outpoint, signature, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&certificate.cert_type)
        .bind(&certificate.serial_number)
        .bind(&certificate.certifier)
        .bind(&certificate.subject)
        .bind(&certificate.verifier)
        .bind(&certificate.revocation_outpoint)
        .bind(&certificate.signature)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    async fn relinquish_certificate(
        &self,
        auth: &AuthId,
        args: RelinquishCertificateArgs,
    ) -> Result<i64> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        let now = Utc::now();

        // Soft delete by setting is_deleted = 1
        let result = sqlx::query(
            r#"
            UPDATE certificates
            SET is_deleted = 1, updated_at = ?
            WHERE user_id = ? AND type = ? AND certifier = ? AND serial_number = ?
            "#,
        )
        .bind(now)
        .bind(user_id)
        .bind(&args.certificate_type)
        .bind(&args.certifier)
        .bind(&args.serial_number)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    async fn relinquish_output(&self, auth: &AuthId, args: RelinquishOutputArgs) -> Result<i64> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        let now = Utc::now();

        // Get txid (convert from [u8; 32] to hex string) and vout from the Outpoint struct
        let txid = hex::encode(args.output.txid);
        let vout: i32 = args.output.vout as i32;

        // Remove from basket by setting basket_id to NULL
        let result = sqlx::query(
            r#"
            UPDATE outputs
            SET basket_id = NULL, updated_at = ?
            WHERE user_id = ? AND txid = ? AND vout = ?
            "#,
        )
        .bind(now)
        .bind(user_id)
        .bind(&txid)
        .bind(vout)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }
}

// =============================================================================
// WalletStorageSync Implementation
// =============================================================================

#[async_trait]
impl WalletStorageSync for StorageSqlx {
    async fn find_or_insert_sync_state(
        &self,
        auth: &AuthId,
        storage_identity_key: &str,
        storage_name: &str,
    ) -> Result<(TableSyncState, bool)> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        self.find_or_insert_sync_state_internal(user_id, storage_identity_key, storage_name)
            .await
    }

    async fn set_active(&self, auth: &AuthId, new_active_storage_identity_key: &str) -> Result<i64> {
        let user_id = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        self.update_user_active_storage(user_id, new_active_storage_identity_key)
            .await?;

        Ok(1)
    }

    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk> {
        super::sync::get_sync_chunk_internal(self, args).await
    }

    async fn process_sync_chunk(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        super::sync::process_sync_chunk_internal(self, args, chunk).await
    }
}

// =============================================================================
// WalletStorageProvider Implementation
// =============================================================================

#[async_trait]
impl WalletStorageProvider for StorageSqlx {
    fn storage_identity_key(&self) -> &str {
        // SAFETY: Similar workaround as get_settings
        let guard = self.storage_identity_key.read().unwrap();
        unsafe { &*(&*guard as *const String) }
    }

    fn storage_name(&self) -> &str {
        // SAFETY: Similar workaround as get_settings
        let guard = self.storage_name.read().unwrap();
        unsafe { &*(&*guard as *const String) }
    }
}

// =============================================================================
// MonitorStorage Implementation
// =============================================================================

use std::time::Duration;
use crate::storage::traits::{MonitorStorage, TxSynchronizedStatus};

#[async_trait]
impl MonitorStorage for StorageSqlx {
    async fn synchronize_transaction_statuses(&self) -> Result<Vec<TxSynchronizedStatus>> {
        // Query proven_tx_reqs with statuses that need synchronization
        let statuses = vec![
            ProvenTxReqStatus::Unmined,
            ProvenTxReqStatus::Unknown,
            ProvenTxReqStatus::Callback,
            ProvenTxReqStatus::Sending,
            ProvenTxReqStatus::Unconfirmed,
        ];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = self.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(vec![]);
        }

        // Note: Full implementation would:
        // 1. Check if already synchronized for current block height
        // 2. For each req, call services.get_merkle_path(txid)
        // 3. On proof found: update proven_tx_req and transaction status
        // 4. On not found: increment attempts
        // 5. Mark as invalid after max attempts exceeded
        //
        // This requires a services reference which is not available on the storage layer.
        // The monitor tasks handle this logic externally using both storage and services.
        //
        // For now, return empty - the monitor tasks handle the actual synchronization.
        tracing::debug!(
            "synchronize_transaction_statuses: found {} transactions needing sync",
            reqs.len()
        );

        Ok(vec![])
    }

    async fn send_waiting_transactions(
        &self,
        _min_transaction_age: Duration,
    ) -> Result<Option<StorageProcessActionResults>> {
        // Query proven_tx_reqs with unsent/sending status
        let statuses = vec![ProvenTxReqStatus::Unsent, ProvenTxReqStatus::Sending];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = self.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(None);
        }

        // Note: Full implementation would:
        // 1. Filter by min_transaction_age
        // 2. Group by batch_id
        // 3. For each batch, build BEEF and call services.post_beef()
        // 4. Update status on success/failure
        //
        // This requires services access which is handled by the monitor tasks.
        tracing::debug!(
            "send_waiting_transactions: found {} transactions waiting to send",
            reqs.len()
        );

        Ok(None)
    }

    async fn abort_abandoned(&self, timeout: Duration) -> Result<()> {
        // Calculate cutoff time
        let cutoff = Utc::now() - chrono::Duration::from_std(timeout).unwrap_or_default();

        // Find abandoned transactions (unsigned or unprocessed older than cutoff)
        // Note: This is a cross-user admin operation which requires special handling.
        //
        // Full implementation would:
        // 1. Query all transactions with status 'unsigned' or 'unprocessed'
        //    where created_at < cutoff
        // 2. For each, call abort_action to release locked UTXOs
        //
        // For now, log the intent and return success.
        tracing::debug!(
            "abort_abandoned: checking for transactions older than {:?}",
            cutoff
        );

        // Query transactions older than cutoff in abortable statuses
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            r#"
            SELECT transaction_id, user_id, reference
            FROM transactions
            WHERE status IN ('unsigned', 'unprocessed')
              AND is_outgoing = 1
              AND created_at < ?
            "#,
        )
        .bind(cutoff)
        .fetch_all(self.pool())
        .await?;

        let count = rows.len();
        if count == 0 {
            return Ok(());
        }

        tracing::info!(
            "abort_abandoned: found {} abandoned transactions to abort",
            count
        );

        // Abort each abandoned transaction
        for (tx_id, user_id, reference) in rows {
            let auth = AuthId::with_user_id("admin", user_id);
            let args = AbortActionArgs { reference };

            match self.abort_action(&auth, args).await {
                Ok(_) => {
                    tracing::debug!(
                        "abort_abandoned: aborted transaction {}",
                        tx_id
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "abort_abandoned: failed to abort transaction {}: {}",
                        tx_id, e
                    );
                }
            }
        }

        Ok(())
    }

    async fn un_fail(&self) -> Result<()> {
        // Query proven_tx_reqs with unfail status
        let args = FindProvenTxReqsArgs {
            status: Some(vec![ProvenTxReqStatus::Unfail]),
            ..Default::default()
        };

        let reqs = self.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(());
        }

        // Note: Full implementation would:
        // 1. For each req, check if transaction has merkle path on-chain
        // 2. If found: update to unmined status, restore UTXOs
        // 3. If not found: mark as invalid
        //
        // This requires services access which is handled by the monitor tasks.
        tracing::debug!(
            "un_fail: found {} transactions marked for unfail processing",
            reqs.len()
        );

        Ok(())
    }
}

// =============================================================================
// Commission and Event Logging Methods
// =============================================================================

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
        .execute(&self.pool)
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
        .execute(&self.pool)
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
        .fetch_all(&self.pool)
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

    /// Log a monitor event.
    ///
    /// Monitor events track background task execution, errors, and status changes.
    ///
    /// # Arguments
    /// * `event` - Event type/name (e.g., "task_started", "sync_completed", "error")
    /// * `details` - Optional JSON details about the event
    pub async fn log_monitor_event(&self, event: &str, details: Option<&str>) -> Result<i64> {
        let now = chrono::Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO monitor_events (event, details, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(event)
        .bind(details)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Get recent monitor events.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of events to return
    /// * `event_filter` - Optional filter by event type
    pub async fn get_monitor_events(
        &self,
        limit: i64,
        event_filter: Option<&str>,
    ) -> Result<Vec<TableMonitorEvent>> {
        let rows = if let Some(event) = event_filter {
            sqlx::query(
                r#"
                SELECT event_id, event, details, created_at
                FROM monitor_events
                WHERE event = ?
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(event)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT event_id, event, details, created_at
                FROM monitor_events
                ORDER BY created_at DESC
                LIMIT ?
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        let events = rows
            .iter()
            .map(|row| TableMonitorEvent {
                event_id: row.get("event_id"),
                event_type: row.get("event"),
                event_data: row.get::<Option<String>, _>("details").unwrap_or_default(),
                created_at: row.get("created_at"),
            })
            .collect();

        Ok(events)
    }

    /// Clear old monitor events.
    ///
    /// # Arguments
    /// * `older_than` - Remove events older than this duration
    pub async fn cleanup_monitor_events(&self, older_than: std::time::Duration) -> Result<u64> {
        let cutoff = chrono::Utc::now() - chrono::Duration::from_std(older_than)
            .map_err(|e| Error::StorageError(format!("Invalid duration: {}", e)))?;

        let result = sqlx::query(
            r#"
            DELETE FROM monitor_events
            WHERE created_at < ?
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_storage() {
        let storage = StorageSqlx::in_memory().await.unwrap();

        // Migrate
        let version = storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        assert_eq!(version, "001_initial");

        // Make available
        let settings = storage.make_available().await.unwrap();
        assert_eq!(settings.storage_name, "test-storage");
        assert!(storage.is_available());
    }

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

    #[tokio::test]
    async fn test_find_outputs() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Find outputs (should be empty)
        let outputs = storage
            .find_outputs(&auth, FindOutputsArgs::default())
            .await
            .unwrap();
        assert!(outputs.is_empty());
    }

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
        .execute(storage.pool())
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
        .execute(storage.pool())
        .await
        .unwrap();

        // Get the transaction ID
        let tx_row = sqlx::query("SELECT transaction_id FROM transactions WHERE reference = 'test-ref-2'")
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
        .execute(storage.pool())
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
        .execute(storage.pool())
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
        assert!(result.actions[0].labels.as_ref().unwrap().contains(&"test_label".to_string()));

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
    async fn test_list_outputs_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List outputs from default basket (should be empty)
        let result = storage
            .list_outputs(
                &auth,
                ListOutputsArgs {
                    basket: "default".to_string(),
                    tags: None,
                    tag_query_mode: None,
                    include: None,
                    include_custom_instructions: None,
                    include_tags: None,
                    include_labels: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_outputs, 0);
        assert!(result.outputs.is_empty());
    }

    #[tokio::test]
    async fn test_list_outputs_nonexistent_basket() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List outputs from non-existent basket
        let result = storage
            .list_outputs(
                &auth,
                ListOutputsArgs {
                    basket: "nonexistent_basket".to_string(),
                    tags: None,
                    tag_query_mode: None,
                    include: None,
                    include_custom_instructions: None,
                    include_tags: None,
                    include_labels: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_outputs, 0);
        assert!(result.outputs.is_empty());
    }

    #[tokio::test]
    async fn test_list_certificates_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List certificates (should be empty)
        let result = storage
            .list_certificates(
                &auth,
                ListCertificatesArgs {
                    certifiers: vec![],
                    types: vec![],
                    limit: Some(10),
                    offset: Some(0),
                    privileged: None,
                    privileged_reason: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_certificates, 0);
        assert!(result.certificates.is_empty());
    }

    #[tokio::test]
    async fn test_list_certificates_with_data() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Insert a test certificate
        sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, revocation_outpoint, signature, is_deleted, created_at, updated_at)
            VALUES (?, 'test_type', 'serial123', 'certifier_pubkey', 'subject_pubkey', 'outpoint123', 'sig123', 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.pool())
        .await
        .unwrap();

        // Get certificate ID
        let cert_row = sqlx::query("SELECT certificate_id FROM certificates WHERE serial_number = 'serial123'")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let cert_id: i64 = cert_row.get("certificate_id");

        // Insert certificate fields
        sqlx::query(
            r#"
            INSERT INTO certificate_fields (user_id, certificate_id, field_name, field_value, master_key, created_at, updated_at)
            VALUES (?, ?, 'name', 'John Doe', 'master_key_123', datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .bind(cert_id)
        .execute(storage.pool())
        .await
        .unwrap();

        // List certificates
        let result = storage
            .list_certificates(
                &auth,
                ListCertificatesArgs {
                    certifiers: vec![],
                    types: vec![],
                    limit: Some(10),
                    offset: Some(0),
                    privileged: None,
                    privileged_reason: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_certificates, 1);
        assert_eq!(result.certificates.len(), 1);
        assert_eq!(result.certificates[0].certificate.certificate_type, "test_type");
        assert_eq!(result.certificates[0].certificate.serial_number, "serial123");
        assert_eq!(result.certificates[0].certificate.fields.get("name").unwrap(), "John Doe");
        assert!(result.certificates[0].keyring.is_some());
        assert_eq!(result.certificates[0].keyring.as_ref().unwrap().get("name").unwrap(), "master_key_123");
    }

    #[tokio::test]
    async fn test_list_certificates_with_filters() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Insert test certificates
        sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, revocation_outpoint, signature, is_deleted, created_at, updated_at)
            VALUES (?, 'type_a', 'serial_a', 'certifier_1', 'subject_1', 'outpoint_a', 'sig_a', 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.pool())
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, revocation_outpoint, signature, is_deleted, created_at, updated_at)
            VALUES (?, 'type_b', 'serial_b', 'certifier_2', 'subject_2', 'outpoint_b', 'sig_b', 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.pool())
        .await
        .unwrap();

        // Filter by certifier
        let result = storage
            .list_certificates(
                &auth,
                ListCertificatesArgs {
                    certifiers: vec!["certifier_1".to_string()],
                    types: vec![],
                    limit: Some(10),
                    offset: Some(0),
                    privileged: None,
                    privileged_reason: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_certificates, 1);
        assert_eq!(result.certificates[0].certificate.certifier, "certifier_1");

        // Filter by type
        let result2 = storage
            .list_certificates(
                &auth,
                ListCertificatesArgs {
                    certifiers: vec![],
                    types: vec!["type_b".to_string()],
                    limit: Some(10),
                    offset: Some(0),
                    privileged: None,
                    privileged_reason: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result2.total_certificates, 1);
        assert_eq!(result2.certificates[0].certificate.certificate_type, "type_b");
    }

    // =============================================================================
    // MonitorStorage Tests
    // =============================================================================

    #[tokio::test]
    async fn test_synchronize_transaction_statuses_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no proven_tx_reqs, should return empty vec
        let result = storage.synchronize_transaction_statuses().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_send_waiting_transactions_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no unsent transactions, should return None
        let result = storage
            .send_waiting_transactions(Duration::from_secs(0))
            .await
            .unwrap();
        assert!(result.is_none());
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

    #[tokio::test]
    async fn test_un_fail_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // With no unfail transactions, should complete without error
        let result = storage.un_fail().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_tx_synchronized_status_struct() {
        // Test that TxSynchronizedStatus can be created and serialized
        let status = TxSynchronizedStatus {
            txid: "abc123".to_string(),
            status: ProvenTxReqStatus::Completed,
            block_height: Some(800000),
            block_hash: Some("0000...".to_string()),
            merkle_root: Some("merkle...".to_string()),
            merkle_path: Some(vec![1, 2, 3]),
        };

        assert_eq!(status.txid, "abc123");
        assert_eq!(status.status, ProvenTxReqStatus::Completed);
        assert_eq!(status.block_height, Some(800000));
    }

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
        .execute(storage.pool())
        .await
        .unwrap();

        let tx_id: i64 = sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE reference = 'ref123'")
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
        let commissions = storage.get_unredeemed_commissions(user.user_id).await.unwrap();
        assert_eq!(commissions.len(), 1);
        assert_eq!(commissions[0].satoshis, 500);
        assert_eq!(commissions[0].key_offset, "offset_123");
        assert!(!commissions[0].is_redeemed);

        // Redeem commission
        storage.redeem_commission(commission_id).await.unwrap();

        // Verify redeemed
        let commissions = storage.get_unredeemed_commissions(user.user_id).await.unwrap();
        assert_eq!(commissions.len(), 0);
    }

    #[tokio::test]
    async fn test_monitor_events() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Log some events
        let event_id1 = storage
            .log_monitor_event("task_started", Some(r#"{"task": "sync"}"#))
            .await
            .unwrap();
        let event_id2 = storage
            .log_monitor_event("task_completed", Some(r#"{"task": "sync", "duration_ms": 100}"#))
            .await
            .unwrap();
        let event_id3 = storage
            .log_monitor_event("error", Some(r#"{"message": "Connection failed"}"#))
            .await
            .unwrap();

        assert!(event_id1 > 0);
        assert!(event_id2 > event_id1);
        assert!(event_id3 > event_id2);

        // Get all events
        let events = storage.get_monitor_events(10, None).await.unwrap();
        assert_eq!(events.len(), 3);

        // Get events by type
        let error_events = storage.get_monitor_events(10, Some("error")).await.unwrap();
        assert_eq!(error_events.len(), 1);
        assert_eq!(error_events[0].event_type, "error");
        assert!(error_events[0].event_data.contains("Connection failed"));
    }

    #[tokio::test]
    async fn test_cleanup_monitor_events() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Log an event
        storage
            .log_monitor_event("test_event", None)
            .await
            .unwrap();

        // Cleanup events older than 1 hour (should not delete recent event)
        let deleted = storage
            .cleanup_monitor_events(std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(deleted, 0);

        // Verify event still exists
        let events = storage.get_monitor_events(10, None).await.unwrap();
        assert_eq!(events.len(), 1);
    }
}
