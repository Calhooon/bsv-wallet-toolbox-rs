//! SQLx-based storage provider implementation.
//!
//! This module provides a storage backend using SQLx with SQLite support.
//! It implements the `WalletStorageProvider` trait hierarchy.

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::entities::*;
use crate::storage::traits::*;

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
    settings: RwLock<Option<TableSettings>>,
    /// Storage identity key (set during migration).
    storage_identity_key: RwLock<String>,
    /// Storage name (set during migration).
    storage_name: RwLock<String>,
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
            settings: RwLock::new(None),
            storage_identity_key: RwLock::new(String::new()),
            storage_name: RwLock::new(String::new()),
        })
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
        _args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        // TODO: Implement full list_actions
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Ok(ListActionsResult {
            total_actions: 0,
            actions: vec![],
        })
    }

    async fn list_certificates(
        &self,
        auth: &AuthId,
        _args: ListCertificatesArgs,
    ) -> Result<ListCertificatesResult> {
        // TODO: Implement full list_certificates
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Ok(ListCertificatesResult {
            total_certificates: 0,
            certificates: vec![],
        })
    }

    async fn list_outputs(
        &self,
        auth: &AuthId,
        _args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        // TODO: Implement full list_outputs
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Ok(ListOutputsResult {
            total_outputs: 0,
            beef: None,
            outputs: vec![],
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
        _args: AbortActionArgs,
    ) -> Result<AbortActionResult> {
        // TODO: Implement abort_action
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Ok(AbortActionResult { aborted: false })
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        _args: bsv_sdk::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        // TODO: Implement create_action
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Err(Error::StorageError("Not implemented".to_string()))
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        _args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        // TODO: Implement process_action
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Err(Error::StorageError("Not implemented".to_string()))
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        _args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        // TODO: Implement internalize_action
        let _ = auth
            .user_id
            .ok_or_else(|| Error::AuthenticationRequired)?;

        Err(Error::StorageError("Not implemented".to_string()))
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
        // TODO: Implement get_sync_chunk
        Ok(SyncChunk {
            from_storage_identity_key: args.from_storage_identity_key,
            to_storage_identity_key: args.to_storage_identity_key,
            user_identity_key: args.identity_key,
            ..Default::default()
        })
    }

    async fn process_sync_chunk(
        &self,
        _args: RequestSyncChunkArgs,
        _chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        // TODO: Implement process_sync_chunk
        Ok(ProcessSyncChunkResult {
            done: true,
            max_updated_at: None,
            updates: 0,
            inserts: 0,
            error: None,
        })
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
}
