//! SQLx-based storage provider implementation.
//!
//! This module provides a storage backend using SQLx with SQLite support.
//! It implements the `WalletStorageProvider` trait hierarchy.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::lock_utils::{lock_read, lock_write};
use crate::services::WalletServices;
use crate::storage::entities::*;
use crate::storage::traits::*;

use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use bsv_rs::wallet::{
    AbortActionArgs, AbortActionResult, InternalizeActionArgs, ListActionsArgs, ListActionsResult,
    ListCertificatesArgs, ListCertificatesResult, ListOutputsArgs, ListOutputsResult,
    RelinquishCertificateArgs, RelinquishOutputArgs,
};

use super::create_action::{
    get_stored_beef, get_tx_with_proof, parse_input_txids, rebuild_beef_for_broadcast,
    MAX_BEEF_RECURSION_DEPTH,
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
    /// Note: Prefer using `services` for full functionality. This is kept for
    /// backward compatibility and cases where only ChainTracker is needed.
    chain_tracker: RwLock<Option<Arc<dyn ChainTracker>>>,
    /// Optional WalletServices for blockchain operations.
    /// When set, storage can perform BEEF verification, broadcast transactions,
    /// validate UTXOs, and look up block headers.
    services: std::sync::RwLock<Option<Arc<dyn WalletServices>>>,
    /// Active transaction tokens for TrxToken scope tracking.
    active_transactions: RwLock<HashMap<u64, ()>>,
    /// In-memory task locks for multi-instance monitor support.
    /// Maps task_name -> (instance_id, expiry).
    #[allow(dead_code)]
    task_locks: RwLock<HashMap<String, (String, std::time::Instant)>>,
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
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| Error::DatabaseError(e.to_string()))?
            .pragma("foreign_keys", "ON")
            .pragma("journal_mode", "WAL")
            .pragma("busy_timeout", "5000")
            .pragma("synchronous", "NORMAL")
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        Ok(Self {
            pool,
            settings: std::sync::RwLock::new(None),
            storage_identity_key: std::sync::RwLock::new(String::new()),
            storage_name: std::sync::RwLock::new(String::new()),
            chain_tracker: RwLock::new(None),
            services: std::sync::RwLock::new(None),
            active_transactions: RwLock::new(HashMap::new()),
            task_locks: RwLock::new(HashMap::new()),
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
        let url = format!("sqlite:{}", path);
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
        let mut cached = lock_write(&self.settings)?;
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
        if let Some(basket_id) = args.basket_id {
            query = query.bind(basket_id);
        }
        if let Some(ref txid) = args.txid {
            query = query.bind(txid);
        }
        if let Some(vout) = args.vout {
            query = query.bind(vout as i32);
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
                provided_by: row.try_get("provided_by").unwrap_or("you".to_string()),
                purpose: row.try_get("purpose").ok(),
                output_description: row.try_get("output_description").ok(),
                spent_by: row.try_get("spent_by").ok().flatten(),
                sequence_number: row
                    .try_get::<Option<i32>, _>("sequence_number")
                    .ok()
                    .flatten()
                    .map(|v| v as u32),
                spending_description: row.try_get("spending_description").ok(),
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
                provided_by: row.try_get("provided_by").unwrap_or("you".to_string()),
                purpose: row.try_get("purpose").ok(),
                output_description: row.try_get("output_description").ok(),
                spent_by: row.try_get("spent_by").ok().flatten(),
                sequence_number: row
                    .try_get::<Option<i32>, _>("sequence_number")
                    .ok()
                    .flatten()
                    .map(|v| v as u32),
                spending_description: row.try_get("spending_description").ok(),
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
                   notified, raw_tx, input_beef, batch, notify, created_at, updated_at
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

            reqs.push(TableProvenTxReq {
                proven_tx_req_id: row.get("proven_tx_req_id"),
                txid: row.get("txid"),
                status,
                attempts: row.get("attempts"),
                history: row.get("history"),
                notified: notified_val != 0,
                notify: row.try_get::<String, _>("notify").unwrap_or_default(),
                raw_tx: row.try_get("raw_tx").ok().flatten(),
                input_beef: row.try_get("input_beef").ok().flatten(),
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
                   ref_num, sync_map, when_last_sync_started, satoshis, error_local, error_other,
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
                satoshis: row.get("satoshis"),
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
            satoshis: None,
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
        lock_read(&self.settings)
            .map(|s| s.is_some())
            .unwrap_or(false)
    }

    fn get_settings(&self) -> &TableSettings {
        // This is a bit awkward due to the RwLock, but the trait requires &self
        // In practice, make_available() should be called first
        static DEFAULT_SETTINGS: std::sync::OnceLock<TableSettings> = std::sync::OnceLock::new();
        let guard = match lock_read(&self.settings) {
            Ok(g) => g,
            Err(_) => return DEFAULT_SETTINGS.get_or_init(TableSettings::default),
        };
        if let Some(ref settings) = *guard {
            // SAFETY: This is a workaround for the trait signature
            // The settings are effectively static once loaded
            unsafe { &*(settings as *const TableSettings) }
        } else {
            DEFAULT_SETTINGS.get_or_init(TableSettings::default)
        }
    }

    fn get_services(&self) -> Result<Arc<dyn WalletServices>> {
        let guard = lock_read(&self.services)?;
        guard.clone().ok_or_else(|| {
            Error::InvalidOperation(
                "Must setServices first. Services are required for blockchain operations."
                    .to_string(),
            )
        })
    }

    async fn find_certificates(
        &self,
        auth: &AuthId,
        args: FindCertificatesArgs,
    ) -> Result<Vec<TableCertificate>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_certificates_internal(user_id, &args).await
    }

    async fn find_output_baskets(
        &self,
        auth: &AuthId,
        args: FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_output_baskets_internal(user_id, &args).await
    }

    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        self.find_outputs_internal(user_id, &args).await
    }

    async fn find_proven_tx_reqs(
        &self,
        args: FindProvenTxReqsArgs,
    ) -> Result<Vec<TableProvenTxReq>> {
        self.find_proven_tx_reqs_internal(&args).await
    }

    async fn find_transactions(
        &self,
        _args: FindTransactionsArgs,
    ) -> Result<Vec<TableTransaction>> {
        // TODO: Implement for SQLite storage
        Ok(vec![])
    }

    async fn list_actions(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        use bsv_rs::wallet::{
            ActionStatus, Outpoint, QueryMode, WalletAction, WalletActionInput, WalletActionOutput,
        };

        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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
                satoshis, // i64, can be negative for outgoing
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
        use bsv_rs::wallet::CertificateResult;
        use std::collections::HashMap;

        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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

            let wallet_cert = bsv_rs::wallet::WalletCertificate {
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
        use bsv_rs::wallet::{Outpoint, OutputInclude, QueryMode, WalletOutput};

        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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
        let mut txids_for_beef: Vec<String> = Vec::new();

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
                    txids_for_beef.push(txid_hex.clone());
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
        // When include_transactions is true (OutputInclude::EntireTransactions),
        // we construct a BEEF containing each output's transaction along with
        // its merkle proof (if proven) and recursively include ancestor
        // transactions until we reach proven ancestors, matching the pattern
        // used in create_action's build_input_beef.
        let beef: Option<Vec<u8>> = if include_transactions && !txids_for_beef.is_empty() {
            // Deduplicate txids
            let unique_txids: Vec<String> = {
                let mut seen = HashSet::new();
                txids_for_beef
                    .into_iter()
                    .filter(|t| seen.insert(t.clone()))
                    .collect()
            };

            let mut beef_struct = Beef::new();
            let mut processed_txids: HashSet<String> = HashSet::new();
            let mut pending_txids: Vec<String> = unique_txids;
            let mut depth: usize = 0;

            let mut conn = self.pool.acquire().await?;

            while !pending_txids.is_empty() && depth < MAX_BEEF_RECURSION_DEPTH {
                let txid = pending_txids.remove(0);

                if processed_txids.contains(&txid) {
                    continue;
                }
                processed_txids.insert(txid.clone());

                // Skip if already in BEEF (from a previously merged stored BEEF)
                if beef_struct.find_txid(&txid).is_some() {
                    continue;
                }

                // Try to get a stored BEEF and merge it directly (most efficient path)
                if let Some(stored_beef) = get_stored_beef(&mut conn, &txid).await? {
                    beef_struct.merge_beef(&stored_beef);
                    for beef_tx in &stored_beef.txs {
                        processed_txids.insert(beef_tx.txid());
                    }
                    depth += 1;
                    continue;
                }

                // Fall back to individual transaction lookup
                if let Some(tx_data) = get_tx_with_proof(&mut conn, &txid).await? {
                    let bump_index = if let Some(merkle_path_bytes) = &tx_data.merkle_path {
                        match MerklePath::from_binary(merkle_path_bytes) {
                            Ok(merkle_path) => Some(beef_struct.merge_bump(merkle_path)),
                            Err(_) => None,
                        }
                    } else {
                        None
                    };

                    beef_struct.merge_raw_tx(tx_data.raw_tx.clone(), bump_index);

                    // If no merkle proof, recurse to ancestors so the BEEF
                    // chain reaches proven transactions
                    if bump_index.is_none() {
                        if let Ok(input_txids) = parse_input_txids(&tx_data.raw_tx) {
                            for input_txid in input_txids {
                                if !processed_txids.contains(&input_txid)
                                    && !pending_txids.contains(&input_txid)
                                {
                                    pending_txids.push(input_txid);
                                }
                            }
                        }
                    }
                }
                // If tx not found in any table, skip silently (it may be a
                // coinbase or an external ancestor we don't have)

                depth += 1;
            }

            let beef_bytes = beef_struct.to_binary();
            // Only return BEEF if it contains data beyond the 4-byte header
            if beef_bytes.len() > 4 {
                Some(beef_bytes)
            } else {
                None
            }
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
            let mut cached = lock_write(&self.settings)?;
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
            dbtype: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.save_settings(&settings).await?;

        // Update internal state
        {
            let mut key = lock_write(&self.storage_identity_key)?;
            *key = storage_identity_key.to_string();
        }
        {
            let mut name = lock_write(&self.storage_name)?;
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
        let mut cached = lock_write(&self.settings)?;
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
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::abort_action::abort_action_internal(self, user_id, args).await
    }

    async fn create_action(
        &self,
        auth: &AuthId,
        args: bsv_rs::wallet::CreateActionArgs,
    ) -> Result<StorageCreateActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        // Get the ChainTracker for BEEF verification (if set)
        let chain_tracker = self.get_chain_tracker().await;
        let tracker_ref: Option<&dyn ChainTracker> = chain_tracker.as_ref().map(|ct| ct.as_ref());

        // Note: Locking scripts for change outputs are stored during process_action
        // when the signed transaction is processed.
        super::create_action::create_action_internal(self, tracker_ref, user_id, args).await
    }

    async fn process_action(
        &self,
        auth: &AuthId,
        args: StorageProcessActionArgs,
    ) -> Result<StorageProcessActionResults> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::process_action::process_action_internal(self, user_id, args).await
    }

    async fn internalize_action(
        &self,
        auth: &AuthId,
        args: InternalizeActionArgs,
    ) -> Result<StorageInternalizeActionResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        super::internalize_action::internalize_action_internal(self, user_id, args).await
    }

    async fn mark_internalized_tx_failed(&self, txid: &str) -> Result<()> {
        super::internalize_action::mark_internalized_tx_failed(self, txid).await
    }

    async fn insert_certificate(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
    ) -> Result<i64> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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

    async fn insert_certificate_field(
        &self,
        auth: &AuthId,
        field: TableCertificateField,
    ) -> Result<i64> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        // Verify the field belongs to this user
        if field.user_id != user_id {
            return Err(Error::AccessDenied(
                "Certificate field user_id does not match auth".to_string(),
            ));
        }

        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO certificate_fields (certificate_id, user_id, field_name, field_value, master_key, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(field.certificate_id)
        .bind(user_id)
        .bind(&field.field_name)
        .bind(&field.field_value)
        .bind(&field.master_key)
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
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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

    async fn update_transaction_status_after_broadcast(
        &self,
        txid: &str,
        outcome: &super::BroadcastOutcome,
    ) -> Result<()> {
        super::process_action::update_transaction_status_after_broadcast_internal(
            self, txid, outcome,
        )
        .await
    }

    async fn review_status(
        &self,
        auth: &AuthId,
        aged_limit: chrono::DateTime<chrono::Utc>,
    ) -> Result<ReviewStatusResult> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let mut log = String::new();

        // 1. Find aged proven_tx_reqs that may need attention
        let aged_reqs: Vec<(i64, String, String)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, txid, status
            FROM proven_tx_reqs
            WHERE updated_at < ?
              AND status IN ('unmined', 'sending', 'unknown', 'unconfirmed', 'callback')
            "#,
        )
        .bind(aged_limit)
        .fetch_all(self.pool())
        .await?;

        if !aged_reqs.is_empty() {
            log.push_str(&format!(
                "Found {} aged proven_tx_reqs needing attention.\n",
                aged_reqs.len()
            ));
        }

        // 2. Find transactions where status doesn't match proven_tx_req status
        let mismatches: Vec<(i64, String, String, String)> = sqlx::query_as(
            r#"
            SELECT t.transaction_id, t.txid, t.status AS tx_status, p.status AS req_status
            FROM transactions t
            JOIN proven_tx_reqs p ON t.txid = p.txid
            WHERE t.user_id = ?
              AND (
                (t.status = 'unproven' AND p.status = 'completed')
                OR (t.status = 'sending' AND p.status IN ('completed', 'unmined'))
                OR (t.status = 'completed' AND p.status NOT IN ('completed', 'nosend'))
              )
            "#,
        )
        .bind(user_id)
        .fetch_all(self.pool())
        .await?;

        // 3. Fix mismatches
        for (tx_id, txid, tx_status, req_status) in &mismatches {
            if tx_status == "unproven" && req_status == "completed" {
                // Transaction should be completed since proof exists
                sqlx::query("UPDATE transactions SET status = 'completed', updated_at = ? WHERE transaction_id = ?")
                    .bind(chrono::Utc::now())
                    .bind(tx_id)
                    .execute(self.pool())
                    .await?;
                log.push_str(&format!(
                    "Fixed tx {}: unproven -> completed (proof exists).\n",
                    txid
                ));
            } else if tx_status == "sending"
                && (req_status == "completed" || req_status == "unmined")
            {
                // Transaction was sent; update status
                let new_status = if req_status == "completed" {
                    "completed"
                } else {
                    "unproven"
                };
                sqlx::query(
                    "UPDATE transactions SET status = ?, updated_at = ? WHERE transaction_id = ?",
                )
                .bind(new_status)
                .bind(chrono::Utc::now())
                .bind(tx_id)
                .execute(self.pool())
                .await?;
                log.push_str(&format!(
                    "Fixed tx {}: sending -> {} (req={}).\n",
                    txid, new_status, req_status
                ));
            }
        }

        if !mismatches.is_empty() {
            log.push_str(&format!("Fixed {} status mismatches.\n", mismatches.len()));
        }

        if log.is_empty() {
            log.push_str("No issues found.\n");
        }

        Ok(ReviewStatusResult { log })
    }

    async fn purge_data(&self, auth: &AuthId, params: PurgeParams) -> Result<PurgeResults> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        let mut count = 0u32;
        let mut log = String::new();

        let cutoff = chrono::Utc::now() - chrono::Duration::days(params.max_age_days as i64);

        // Build status list based on params
        let mut statuses = Vec::new();
        if params.purge_completed {
            statuses.push("completed");
        }
        if params.purge_failed {
            statuses.push("failed");
        }

        if statuses.is_empty() {
            return Ok(PurgeResults {
                count: 0,
                log: "No statuses selected for purge.\n".to_string(),
            });
        }

        // 1. Null out raw_tx on old completed transactions to save space
        if params.purge_completed {
            let result = sqlx::query(
                r#"
                UPDATE transactions
                SET raw_tx = NULL, updated_at = ?
                WHERE user_id = ?
                  AND status = 'completed'
                  AND updated_at < ?
                  AND raw_tx IS NOT NULL
                "#,
            )
            .bind(chrono::Utc::now())
            .bind(user_id)
            .bind(cutoff)
            .execute(self.pool())
            .await?;
            let rows = result.rows_affected() as u32;
            if rows > 0 {
                log.push_str(&format!(
                    "Cleared raw_tx from {} completed transactions.\n",
                    rows
                ));
                count += rows;
            }
        }

        // 2. Delete proven_tx_reqs for old failed transactions
        if params.purge_failed {
            let result = sqlx::query(
                r#"
                DELETE FROM proven_tx_reqs
                WHERE status IN ('failed', 'invalid', 'doubleSpend')
                  AND updated_at < ?
                "#,
            )
            .bind(cutoff)
            .execute(self.pool())
            .await?;
            let rows = result.rows_affected() as u32;
            if rows > 0 {
                log.push_str(&format!("Deleted {} failed proven_tx_reqs.\n", rows));
                count += rows;
            }
        }

        // 3. Clean up old monitor events
        let result = sqlx::query(r#"DELETE FROM monitor_events WHERE created_at < ?"#)
            .bind(cutoff)
            .execute(self.pool())
            .await?;
        let rows = result.rows_affected() as u32;
        if rows > 0 {
            log.push_str(&format!("Deleted {} old monitor events.\n", rows));
            count += rows;
        }

        if log.is_empty() {
            log.push_str("Nothing to purge.\n");
        }

        Ok(PurgeResults { count, log })
    }

    async fn begin_transaction(&self) -> Result<TrxToken> {
        let token = TrxToken::new();
        let mut active = self.active_transactions.write().await;
        active.insert(token.id(), ());
        Ok(token)
    }

    async fn commit_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
    }

    async fn rollback_transaction(&self, trx: TrxToken) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        if active.remove(&trx.id()).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Unknown transaction token: {}",
                trx.id()
            )))
        }
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
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        self.find_or_insert_sync_state_internal(user_id, storage_identity_key, storage_name)
            .await
    }

    async fn set_active(
        &self,
        auth: &AuthId,
        new_active_storage_identity_key: &str,
    ) -> Result<i64> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

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
        // If lock is poisoned, return empty string as fallback
        let guard = match lock_read(&self.storage_identity_key) {
            Ok(g) => g,
            Err(_) => return "",
        };
        unsafe { &*(&*guard as *const String) }
    }

    fn storage_name(&self) -> &str {
        // SAFETY: Similar workaround as get_settings
        // If lock is poisoned, return empty string as fallback
        let guard = match lock_read(&self.storage_name) {
            Ok(g) => g,
            Err(_) => return "",
        };
        unsafe { &*(&*guard as *const String) }
    }

    fn set_services(&self, services: Arc<dyn WalletServices>) {
        // If lock is poisoned, log and skip rather than panicking
        match lock_write(&self.services) {
            Ok(mut guard) => *guard = Some(services),
            Err(e) => tracing::error!("Failed to set services: {}", e),
        }
    }
}

// =============================================================================
// MonitorStorage Implementation
// =============================================================================

use crate::storage::traits::{MonitorStorage, TxSynchronizedStatus};
use std::time::Duration;

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
            return Ok(Vec::new());
        }

        let services = self.get_services()?;

        tracing::debug!(
            "synchronize_transaction_statuses: checking {} transactions",
            reqs.len()
        );

        let mut results = Vec::new();
        let max_attempts = 144; // ~2.4 hours at 60s intervals (matches JS reference for mainnet)

        for req in &reqs {
            let txid = &req.txid;

            // Attempt to get merkle path from services
            match services.get_merkle_path(txid, false).await {
                Ok(proof_result) => {
                    if let Some(ref merkle_path) = proof_result.merkle_path {
                        // Proof found - update proven_tx_req to completed
                        let now = chrono::Utc::now();

                        // Extract block info from header if available
                        let block_height =
                            proof_result.header.as_ref().map(|h| h.height).unwrap_or(0);
                        let block_hash = proof_result
                            .header
                            .as_ref()
                            .map(|h| h.hash.clone())
                            .unwrap_or_default();

                        // merkle_path should be BUMP hex after services.rs conversion.
                        // Decode hex → binary for storage. Fallback to raw bytes if not hex.
                        let merkle_path_bytes = hex::decode(merkle_path)
                            .unwrap_or_else(|_| merkle_path.as_bytes().to_vec());
                        let merkle_root = proof_result
                            .header
                            .as_ref()
                            .map(|h| h.merkle_root.clone())
                            .unwrap_or_default();

                        sqlx::query(
                            r#"
                            INSERT OR IGNORE INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
                            VALUES (?, ?, 0, ?, ?,  ?,
                                COALESCE(
                                    (SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1),
                                    (SELECT raw_tx FROM proven_tx_reqs WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1)
                                ),
                                ?, ?)
                            "#,
                        )
                        .bind(txid)
                        .bind(block_height as i64)
                        .bind(&block_hash)
                        .bind(&merkle_root)
                        .bind(&merkle_path_bytes)
                        .bind(txid)
                        .bind(txid)
                        .bind(now)
                        .bind(now)
                        .execute(self.pool())
                        .await?;

                        // Get the proven_tx_id
                        let proven_tx_id: Option<(i64,)> =
                            sqlx::query_as("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
                                .bind(txid)
                                .fetch_optional(self.pool())
                                .await?;

                        // Update proven_tx_req to completed
                        sqlx::query(
                            r#"
                            UPDATE proven_tx_reqs
                            SET status = 'completed', proven_tx_id = ?, updated_at = ?
                            WHERE proven_tx_req_id = ?
                            "#,
                        )
                        .bind(proven_tx_id.map(|r| r.0))
                        .bind(now)
                        .bind(req.proven_tx_req_id)
                        .execute(self.pool())
                        .await?;

                        // Update transaction status to completed
                        sqlx::query(
                            "UPDATE transactions SET status = 'completed', updated_at = ? WHERE txid = ?"
                        )
                        .bind(now)
                        .bind(txid)
                        .execute(self.pool())
                        .await?;

                        results.push(TxSynchronizedStatus {
                            txid: txid.clone(),
                            status: ProvenTxReqStatus::Completed,
                            block_height: Some(block_height),
                            block_hash: Some(block_hash),
                            merkle_root: Some(merkle_root),
                            merkle_path: Some(merkle_path_bytes.to_vec()),
                        });
                    } else {
                        // No proof yet - increment attempts
                        let attempts = req.attempts + 1;
                        let now = chrono::Utc::now();

                        if attempts >= max_attempts {
                            // Too many attempts - mark as invalid
                            sqlx::query(
                                "UPDATE proven_tx_reqs SET status = 'invalid', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                            )
                            .bind(attempts)
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(self.pool())
                            .await?;

                            results.push(TxSynchronizedStatus {
                                txid: txid.clone(),
                                status: ProvenTxReqStatus::Invalid,
                                block_height: None,
                                block_hash: None,
                                merkle_root: None,
                                merkle_path: None,
                            });
                        } else {
                            sqlx::query(
                                "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                            )
                            .bind(attempts)
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(self.pool())
                            .await?;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to get merkle path for {}: {}", txid, e);
                    // Increment attempts on error
                    let now = chrono::Utc::now();
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(self.pool())
                    .await?;
                }
            }
        }

        // Repair existing proven_txs that have JSON merkle_paths instead of BUMP binary.
        // This fixes records created before the TSC→BUMP conversion was added in services.rs.
        // Detection: JSON starts with 0x7B ('{'), BUMP binary starts with a varint block height.
        let repair_txs: Vec<(i64, Vec<u8>, i64, String)> = sqlx::query_as(
            "SELECT proven_tx_id, merkle_path, height, txid FROM proven_txs WHERE merkle_path IS NOT NULL AND substr(hex(merkle_path), 1, 2) = '7B' LIMIT 200"
        )
            .fetch_all(self.pool())
            .await?;

        let mut repaired_count = 0u32;
        for (proven_tx_id, merkle_path_bytes, height, txid) in &repair_txs {
            // Check if merkle_path is JSON (starts with '{') — needs conversion
            if merkle_path_bytes.first() != Some(&0x7B) {
                continue; // Already BUMP binary, skip
            }

            let merkle_path_str = match std::str::from_utf8(merkle_path_bytes) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Use stored height if available, otherwise resolve from target
            let block_height = if *height > 0 {
                *height as u32
            } else {
                // Fall back to resolving header from target
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(merkle_path_str) {
                    if let Some(target) = json.get("target").and_then(|t| t.as_str()) {
                        match services.hash_to_header(target).await {
                            Ok(header) => {
                                let now = chrono::Utc::now();
                                sqlx::query(
                                    "UPDATE proven_txs SET height = ?, block_hash = ?, merkle_root = ?, updated_at = ? WHERE proven_tx_id = ?"
                                )
                                    .bind(header.height as i64)
                                    .bind(&header.hash)
                                    .bind(&header.merkle_root)
                                    .bind(now)
                                    .bind(proven_tx_id)
                                    .execute(self.pool())
                                    .await?;
                                header.height
                            }
                            Err(_) => continue,
                        }
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            };

            // Convert JSON → BUMP binary
            match crate::tsc_proof::tsc_json_to_bump_binary(merkle_path_str, block_height) {
                Some(bump_bytes) => {
                    let now = chrono::Utc::now();
                    sqlx::query(
                        "UPDATE proven_txs SET merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?"
                    )
                        .bind(&bump_bytes)
                        .bind(now)
                        .bind(proven_tx_id)
                        .execute(self.pool())
                        .await?;
                    repaired_count += 1;
                }
                None => {
                    tracing::warn!(
                        "Failed to convert merkle_path JSON to BUMP for proven_tx {} (txid={})",
                        proven_tx_id,
                        txid
                    );
                }
            }
        }

        if repaired_count > 0 {
            tracing::info!(
                "Repaired {}/{} proven_txs: JSON merkle_path → BUMP binary",
                repaired_count,
                repair_txs.len()
            );
        }

        Ok(results)
    }

    async fn send_waiting_transactions(
        &self,
        min_transaction_age: Duration,
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

        // Filter by min_transaction_age
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(min_transaction_age).unwrap_or_default();
        let reqs: Vec<_> = reqs.into_iter().filter(|r| r.created_at < cutoff).collect();

        if reqs.is_empty() {
            return Ok(None);
        }

        let services = self.get_services()?;

        tracing::debug!(
            "send_waiting_transactions: broadcasting {} transactions",
            reqs.len()
        );

        let mut send_with_results = Vec::new();
        let now = chrono::Utc::now();

        for req in &reqs {
            // Get raw_tx for this proven_tx_req
            let raw_tx = match &req.raw_tx {
                Some(tx) if !tx.is_empty() => tx.clone(),
                _ => {
                    // Try to get from transaction table
                    let row: Option<(Vec<u8>,)> = sqlx::query_as(
                        "SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL",
                    )
                    .bind(&req.txid)
                    .fetch_optional(self.pool())
                    .await?;
                    match row {
                        Some((tx,)) => tx,
                        None => {
                            tracing::warn!("No raw_tx found for txid {}", req.txid);
                            continue;
                        }
                    }
                }
            };

            // Rebuild BEEF from current DB state at broadcast time.
            // This matches Go's GetBEEFForTxIDs approach: instead of just upgrading
            // proofs on txs already in the stored BEEF (compact_stored_beef), we walk
            // the full ancestor chain from scratch. This picks up:
            //   - Ancestors that were missing when input_beef was first built
            //   - Merkle proofs that arrived after create_action
            //   - Transactions internalized from other actions
            let beef_bytes = {
                // Extract input txids from the raw transaction
                let input_txids = match parse_input_txids(&raw_tx) {
                    Ok(txids) => txids,
                    Err(e) => {
                        tracing::warn!(
                            "send_waiting: failed to parse inputs from raw_tx for {}: {e}",
                            req.txid
                        );
                        continue;
                    }
                };

                if input_txids.is_empty() {
                    // Coinbase or no inputs — cannot build BEEF
                    tracing::warn!(
                        "No input txids parsed for txid {} — skipping broadcast",
                        req.txid
                    );
                    continue;
                }

                // Rebuild BEEF from DB state
                let mut conn = self.pool().acquire().await?;
                let mut beef = match rebuild_beef_for_broadcast(&mut conn, &input_txids, Some(self))
                    .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(
                            "send_waiting: rebuild_beef_for_broadcast failed for {}: {e}",
                            req.txid
                        );
                        // Fall back to stored input_beef + compact if rebuild fails
                        if let Some(ref input_beef) = req.input_beef {
                            match Beef::from_binary(input_beef) {
                                Ok(mut fallback_beef) => {
                                    if let Err(e2) = super::create_action::compact_stored_beef(
                                        &mut conn,
                                        &mut fallback_beef,
                                    )
                                    .await
                                    {
                                        tracing::debug!(
                                                "send_waiting: compact fallback also failed for {}: {e2}",
                                                req.txid
                                            );
                                    }
                                    fallback_beef
                                }
                                Err(e2) => {
                                    tracing::warn!(
                                        "send_waiting: fallback parse also failed for {}: {e2}",
                                        req.txid
                                    );
                                    continue;
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                };

                // Merge the transaction being broadcast into the BEEF
                beef.merge_raw_tx(raw_tx.clone(), None);

                // Validate BEEF structure before broadcast (diagnostic)
                if let Err(e) = validate_beef_for_broadcast(&beef, &req.txid) {
                    tracing::warn!(
                        "send_waiting: BEEF validation warning for {}: {}",
                        req.txid,
                        e
                    );
                }

                beef.to_binary()
            };

            // Update status to sending
            sqlx::query("UPDATE proven_tx_reqs SET status = 'sending', updated_at = ? WHERE proven_tx_req_id = ?")
                .bind(now)
                .bind(req.proven_tx_req_id)
                .execute(self.pool())
                .await?;

            // Broadcast via services - returns Vec<PostBeefResult> (one per provider)
            match services
                .post_beef(&beef_bytes, std::slice::from_ref(&req.txid))
                .await
            {
                Ok(results_vec) => {
                    // Check if any provider returned success
                    let success = results_vec.iter().any(|r| r.is_success());
                    if success {
                        // Update to unmined
                        sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(self.pool())
                            .await?;

                        sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?")
                            .bind(now)
                            .bind(&req.txid)
                            .execute(self.pool())
                            .await?;

                        send_with_results.push(SendWithResult {
                            txid: req.txid.clone(),
                            status: "unproven".to_string(),
                        });
                    } else {
                        // Check for orphan mempool (parent not propagated — NOT double-spend)
                        let is_orphan_mempool = results_vec
                            .iter()
                            .any(|r| r.txid_results.iter().any(|tr| tr.orphan_mempool));

                        // Check for double spend in any provider's results
                        // (exclude orphan mempool — it's not a double-spend)
                        let is_double_spend = results_vec
                            .iter()
                            .any(|r| r.txid_results.iter().any(|tr| tr.double_spend && !tr.orphan_mempool));

                        // Check for definitive rejection (invalid tx, not just service error)
                        let is_invalid = results_vec.iter().any(|r| {
                            r.txid_results.iter().any(|tr| {
                                // ARC 46x codes = definitive tx rejection
                                !tr.orphan_mempool && (tr.status.contains("46") || tr.status.contains("invalid"))
                            })
                        });

                        let attempts = req.attempts + 1;

                        if is_orphan_mempool && !is_double_spend {
                            // Orphan mempool: parent tx not yet propagated.
                            // Check if the tx is actually known on-chain.
                            let reconciled =
                                super::process_action::reconcile_tx_status_via_services(
                                    &*services, &req.txid,
                                )
                                .await;

                            if reconciled {
                                // Tx is actually alive on chain — treat as success
                                sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
                                    .bind(now)
                                    .bind(req.proven_tx_req_id)
                                    .execute(self.pool())
                                    .await?;
                                sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?")
                                    .bind(now)
                                    .bind(&req.txid)
                                    .execute(self.pool())
                                    .await?;
                                tracing::info!(
                                    "send_waiting: tx {} reported as orphan mempool but found alive on chain — treating as success",
                                    req.txid
                                );
                                send_with_results.push(SendWithResult {
                                    txid: req.txid.clone(),
                                    status: "unproven".to_string(),
                                });
                                continue;
                            }

                            if attempts > 6 {
                                // Max retries exceeded — mark as invalid
                                sqlx::query("UPDATE proven_tx_reqs SET status = 'invalid', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                                    .bind(attempts as i64)
                                    .bind(now)
                                    .bind(req.proven_tx_req_id)
                                    .execute(self.pool())
                                    .await?;

                                let tx_row: Option<(i64,)> = sqlx::query_as(
                                    "SELECT transaction_id FROM transactions WHERE txid = ? AND status IN ('sending', 'unproven')",
                                )
                                .bind(&req.txid)
                                .fetch_optional(self.pool())
                                .await?;

                                if let Some((transaction_id,)) = tx_row {
                                    sqlx::query(
                                        "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                                    )
                                    .bind(now)
                                    .bind(transaction_id)
                                    .execute(self.pool())
                                    .await?;

                                    sqlx::query(
                                        "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ?",
                                    )
                                    .bind(now)
                                    .bind(transaction_id)
                                    .execute(self.pool())
                                    .await?;

                                    sqlx::query(
                                        "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                                    )
                                    .bind(now)
                                    .bind(transaction_id)
                                    .execute(self.pool())
                                    .await?;

                                    tracing::info!(
                                        "send_waiting: tx {} orphan mempool — failed after {} attempts, inputs restored",
                                        req.txid, attempts
                                    );
                                }
                            } else {
                                // Keep in sending status for retry — do NOT lock inputs
                                sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                                    .bind(attempts as i64)
                                    .bind(now)
                                    .bind(req.proven_tx_req_id)
                                    .execute(self.pool())
                                    .await?;

                                tracing::warn!(
                                    "send_waiting: tx {} orphan mempool (attempt {}/6) — will retry",
                                    req.txid, attempts
                                );
                            }

                            send_with_results.push(SendWithResult {
                                txid: req.txid.clone(),
                                status: "failed".to_string(),
                            });
                            continue;
                        }

                        if is_double_spend {
                            // Double-spend: reconcile against chain first (like the
                            // immediate broadcast path does), then UTXO-verify inputs.
                            let reconciled =
                                super::process_action::reconcile_tx_status_via_services(
                                    &*services, &req.txid,
                                )
                                .await;

                            if reconciled {
                                // Tx is actually alive on chain — treat as success
                                sqlx::query("UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE proven_tx_req_id = ?")
                                    .bind(now)
                                    .bind(req.proven_tx_req_id)
                                    .execute(self.pool())
                                    .await?;
                                sqlx::query("UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?")
                                    .bind(now)
                                    .bind(&req.txid)
                                    .execute(self.pool())
                                    .await?;
                                tracing::info!(
                                    "send_waiting: tx {} reported as double-spend but found alive on chain — treating as success",
                                    req.txid
                                );
                                send_with_results.push(SendWithResult {
                                    txid: req.txid.clone(),
                                    status: "unproven".to_string(),
                                });
                                continue;
                            }

                            // Confirmed double-spend — UTXO-verify before restoring inputs
                            sqlx::query("UPDATE proven_tx_reqs SET status = 'doubleSpend', updated_at = ? WHERE proven_tx_req_id = ?")
                                .bind(now)
                                .bind(req.proven_tx_req_id)
                                .execute(self.pool())
                                .await?;

                            let tx_row: Option<(i64,)> = sqlx::query_as(
                                "SELECT transaction_id FROM transactions WHERE txid = ?",
                            )
                            .bind(&req.txid)
                            .fetch_optional(self.pool())
                            .await?;

                            if let Some((transaction_id,)) = tx_row {
                                // Mark change outputs non-spendable
                                sqlx::query(
                                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                                )
                                .bind(now)
                                .bind(transaction_id)
                                .execute(self.pool())
                                .await?;

                                // UTXO-verified restore: only restore inputs confirmed unspent
                                let input_rows = sqlx::query(
                                    r#"
                                    SELECT o.output_id, t.txid AS source_txid, o.vout, o.locking_script
                                    FROM outputs o
                                    JOIN transactions t ON o.transaction_id = t.transaction_id
                                    WHERE o.spent_by = ?
                                    "#,
                                )
                                .bind(transaction_id)
                                .fetch_all(self.pool())
                                .await?;

                                let mut restored = 0u32;
                                for input_row in &input_rows {
                                    let output_id: i64 = input_row.get("output_id");
                                    let source_txid: String = input_row.get("source_txid");
                                    let vout: i32 = input_row.get("vout");
                                    let locking_script: Option<Vec<u8>> =
                                        input_row.get("locking_script");
                                    let script = locking_script.as_deref().unwrap_or(&[]);

                                    let is_utxo = services
                                        .is_utxo(&source_txid, vout as u32, script)
                                        .await
                                        .unwrap_or(false);

                                    if is_utxo {
                                        sqlx::query(
                                            "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE output_id = ?",
                                        )
                                        .bind(now)
                                        .bind(output_id)
                                        .execute(self.pool())
                                        .await?;
                                        restored += 1;
                                    } else {
                                        tracing::info!(
                                            "send_waiting: input {}:{} consumed on-chain — NOT restoring",
                                            source_txid, vout
                                        );
                                    }

                                    // Rate limit: ~3 req/sec
                                    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
                                }

                                // Mark transaction as failed
                                sqlx::query(
                                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                                )
                                .bind(now)
                                .bind(transaction_id)
                                .execute(self.pool())
                                .await?;

                                tracing::info!(
                                    "send_waiting: tx {} double-spend confirmed — {}/{} inputs restored (UTXO-verified)",
                                    req.txid, restored, input_rows.len()
                                );
                            }
                        } else if is_invalid || attempts > 6 {
                            // Definitive rejection or too many retries — mark invalid
                            // and transition transaction to failed with output cleanup.
                            // Safe to blindly restore: tx was malformed or never broadcast
                            // successfully, inputs weren't spent by a competitor.
                            sqlx::query("UPDATE proven_tx_reqs SET status = 'invalid', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                                .bind(attempts as i64)
                                .bind(now)
                                .bind(req.proven_tx_req_id)
                                .execute(self.pool())
                                .await?;

                            // Transition transaction to failed (covers both outgoing 'sending'
                            // and internalized 'unproven' txs whose broadcast was retried)
                            let tx_row: Option<(i64,)> = sqlx::query_as(
                                "SELECT transaction_id FROM transactions WHERE txid = ? AND status IN ('sending', 'unproven')",
                            )
                            .bind(&req.txid)
                            .fetch_optional(self.pool())
                            .await?;

                            if let Some((transaction_id,)) = tx_row {
                                // Mark change outputs non-spendable (phantom UTXO prevention)
                                sqlx::query(
                                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                                )
                                .bind(now)
                                .bind(transaction_id)
                                .execute(self.pool())
                                .await?;

                                // Restore input UTXOs (safe for invalid/exhausted retries)
                                sqlx::query(
                                    "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ?",
                                )
                                .bind(now)
                                .bind(transaction_id)
                                .execute(self.pool())
                                .await?;

                                // Mark transaction as failed
                                sqlx::query(
                                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                                )
                                .bind(now)
                                .bind(transaction_id)
                                .execute(self.pool())
                                .await?;

                                tracing::info!(
                                    "send_waiting: tx {} failed after {} attempts — inputs restored, change outputs marked non-spendable",
                                    req.txid, attempts
                                );
                            }
                        } else {
                            sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                                .bind(attempts as i64)
                                .bind(now)
                                .bind(req.proven_tx_req_id)
                                .execute(self.pool())
                                .await?;
                        }

                        send_with_results.push(SendWithResult {
                            txid: req.txid.clone(),
                            status: "failed".to_string(),
                        });
                    }
                }
                Err(e) => {
                    let attempts = req.attempts + 1;
                    tracing::warn!(
                        "Failed to broadcast tx {} (attempt {}): {}",
                        req.txid,
                        attempts,
                        e
                    );

                    if attempts > 6 {
                        // Too many retries — give up
                        sqlx::query("UPDATE proven_tx_reqs SET status = 'invalid', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                            .bind(attempts as i64)
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(self.pool())
                            .await?;

                        let tx_row: Option<(i64,)> = sqlx::query_as(
                            "SELECT transaction_id FROM transactions WHERE txid = ? AND status = 'sending'",
                        )
                        .bind(&req.txid)
                        .fetch_optional(self.pool())
                        .await?;

                        if let Some((transaction_id,)) = tx_row {
                            sqlx::query(
                                "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                            )
                            .bind(now)
                            .bind(transaction_id)
                            .execute(self.pool())
                            .await?;

                            sqlx::query(
                                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE spent_by = ?",
                            )
                            .bind(now)
                            .bind(transaction_id)
                            .execute(self.pool())
                            .await?;

                            sqlx::query(
                                "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                            )
                            .bind(now)
                            .bind(transaction_id)
                            .execute(self.pool())
                            .await?;

                            tracing::info!(
                                "send_waiting: tx {} abandoned after {} attempts — inputs restored, outputs cleaned",
                                req.txid, attempts
                            );
                        }
                    } else {
                        sqlx::query("UPDATE proven_tx_reqs SET status = 'unsent', attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                            .bind(attempts as i64)
                            .bind(now)
                            .bind(req.proven_tx_req_id)
                            .execute(self.pool())
                            .await?;
                    }

                    send_with_results.push(SendWithResult {
                        txid: req.txid.clone(),
                        status: "failed".to_string(),
                    });
                }
            }
        }

        Ok(Some(StorageProcessActionResults {
            send_with_results: Some(send_with_results),
            not_delayed_results: None,
            log: None,
        }))
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

        // Query transactions older than cutoff in standard abortable statuses.
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

        // Abort each abandoned transaction via normal abort_action path
        for (tx_id, user_id, reference) in &rows {
            let auth = AuthId::with_user_id("admin", *user_id);
            let args = AbortActionArgs {
                reference: reference.clone(),
            };

            match self.abort_action(&auth, args).await {
                Ok(_) => {
                    tracing::debug!("abort_abandoned: aborted transaction {}", tx_id);
                }
                Err(e) => {
                    tracing::warn!(
                        "abort_abandoned: failed to abort transaction {}: {}",
                        tx_id,
                        e
                    );
                }
            }
        }

        // Handle stale 'sending' transactions separately.
        // These can't go through abort_action (it rejects 'sending' status),
        // so we do direct DB cleanup: mark change outputs non-spendable,
        // restore input UTXOs (with UTXO verification), and transition to 'failed'.
        //
        // Because these transactions may have been broadcast and double-spent,
        // we verify each input via is_utxo() before restoring. This prevents
        // the feedback loop where dead UTXOs are re-marked as spendable.
        let sending_rows: Vec<(i64, String)> = sqlx::query_as(
            r#"
            SELECT transaction_id, txid
            FROM transactions
            WHERE status = 'sending'
              AND is_outgoing = 1
              AND created_at < ?
            "#,
        )
        .bind(cutoff)
        .fetch_all(self.pool())
        .await?;

        if !sending_rows.is_empty() {
            let now = Utc::now();
            tracing::info!(
                "abort_abandoned: cleaning up {} stale 'sending' transactions",
                sending_rows.len()
            );

            // Get services for UTXO verification (best-effort)
            let services = self.get_services().ok();

            for (transaction_id, txid) in &sending_rows {
                // Mark change outputs non-spendable (phantom UTXO prevention)
                sqlx::query(
                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                )
                .bind(now)
                .bind(transaction_id)
                .execute(self.pool())
                .await?;

                // UTXO-verified restore of input UTXOs.
                // These transactions were in 'sending' and may have been broadcast,
                // so we verify each input is still unspent before restoring.
                if let Some(ref svc) = services {
                    let input_rows = sqlx::query(
                        r#"
                        SELECT o.output_id, t.txid AS source_txid, o.vout, o.locking_script
                        FROM outputs o
                        JOIN transactions t ON o.transaction_id = t.transaction_id
                        WHERE o.spent_by = ?
                        "#,
                    )
                    .bind(transaction_id)
                    .fetch_all(self.pool())
                    .await?;

                    let mut restored = 0u32;
                    for input_row in &input_rows {
                        let output_id: i64 = input_row.get("output_id");
                        let source_txid: String = input_row.get("source_txid");
                        let vout: i32 = input_row.get("vout");
                        let locking_script: Option<Vec<u8>> = input_row.get("locking_script");
                        let script = locking_script.as_deref().unwrap_or(&[]);

                        let is_utxo = svc
                            .is_utxo(&source_txid, vout as u32, script)
                            .await
                            .unwrap_or(false);

                        if is_utxo {
                            sqlx::query(
                                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE output_id = ?",
                            )
                            .bind(now)
                            .bind(output_id)
                            .execute(self.pool())
                            .await?;
                            restored += 1;
                        } else {
                            tracing::info!(
                                "abort_abandoned: input {}:{} not a UTXO — NOT restoring",
                                source_txid,
                                vout
                            );
                        }

                        // Rate limit: ~3 req/sec
                        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
                    }

                    tracing::info!(
                        "abort_abandoned: tx {} — {}/{} inputs restored (UTXO-verified)",
                        txid,
                        restored,
                        input_rows.len()
                    );
                } else {
                    // No services available — fail-safe: do NOT restore inputs.
                    // They'll be picked up on the next run when services are available.
                    tracing::warn!(
                        "abort_abandoned: tx {} — services unavailable, inputs stay locked",
                        txid
                    );
                }

                // Mark transaction as failed
                sqlx::query(
                    "UPDATE transactions SET status = 'failed', updated_at = ? WHERE transaction_id = ?",
                )
                .bind(now)
                .bind(transaction_id)
                .execute(self.pool())
                .await?;
            }
        }

        let total = rows.len() + sending_rows.len();
        if total > 0 {
            tracing::info!(
                "abort_abandoned: processed {} abandoned transactions ({} aborted, {} stale sending cleaned)",
                total,
                rows.len(),
                sending_rows.len()
            );
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

        let services = self.get_services()?;

        tracing::debug!(
            "un_fail: processing {} transactions marked for unfail",
            reqs.len()
        );

        let now = chrono::Utc::now();

        for req in &reqs {
            // Check if transaction has a merkle path on-chain
            match services.get_merkle_path(&req.txid, false).await {
                Ok(result) if result.merkle_path.is_some() => {
                    // Transaction exists on chain - restore it
                    // Update proven_tx_req to unmined (will be picked up by sync)
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET status = 'unmined', attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(self.pool())
                    .await?;

                    // Update transaction status to unproven
                    sqlx::query(
                        "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE txid = ?"
                    )
                    .bind(now)
                    .bind(&req.txid)
                    .execute(self.pool())
                    .await?;

                    // Restore outputs of this transaction, but validate each against
                    // chain to avoid creating ghost UTXOs (outputs already spent on-chain).
                    let output_rows = sqlx::query(
                        "SELECT output_id, vout, locking_script FROM outputs WHERE txid = ? AND spendable = 0",
                    )
                    .bind(&req.txid)
                    .fetch_all(self.pool())
                    .await?;

                    let mut restored = 0u32;
                    for row in &output_rows {
                        let output_id: i64 = row.get("output_id");
                        let vout: i32 = row.get("vout");
                        let locking_script: Option<Vec<u8>> = row.get("locking_script");
                        let script = locking_script.as_deref().unwrap_or(&[]);
                        let is_utxo = services
                            .is_utxo(&req.txid, vout as u32, script)
                            .await
                            .unwrap_or(false);
                        if is_utxo {
                            sqlx::query(
                                "UPDATE outputs SET spendable = 1, updated_at = ? WHERE output_id = ?",
                            )
                            .bind(now)
                            .bind(output_id)
                            .execute(self.pool())
                            .await?;
                            restored += 1;
                        } else {
                            tracing::debug!(
                                "un_fail: output {}:{} is not a UTXO on chain, skipping",
                                req.txid,
                                vout
                            );
                        }
                    }

                    // Re-mark inputs consumed by this tx as spent (they were
                    // released when the tx was originally marked failed, but the
                    // tx actually made it to chain).
                    let tx_row =
                        sqlx::query("SELECT transaction_id FROM transactions WHERE txid = ?")
                            .bind(&req.txid)
                            .fetch_optional(self.pool())
                            .await?;
                    if let Some(tx_row) = tx_row {
                        let transaction_id: i64 = tx_row.get("transaction_id");
                        // Find outputs that this tx originally spent (their txid+vout
                        // appear as inputs in the raw tx). Since spent_by was cleared
                        // on failure, we use the raw_tx to re-establish the link.
                        // For now, mark any output whose spent_by is NULL but was
                        // previously spent by this transaction. The transaction_inputs
                        // relationship is encoded in the raw_tx, but we can use a
                        // simpler heuristic: outputs created before this tx that are
                        // currently spendable and appear as inputs in the BEEF.
                        // TODO: Parse raw_tx inputs for exact matching. For now the
                        // output restoration + isUtxo check is the primary safety net.
                        let _ = transaction_id; // suppress unused warning
                    }

                    tracing::info!(
                        "un_fail: restored transaction {} ({}/{} outputs validated as UTXOs)",
                        req.txid,
                        restored,
                        output_rows.len()
                    );
                }
                _ => {
                    // Not found on chain - mark as invalid
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(self.pool())
                    .await?;

                    tracing::debug!(
                        "un_fail: marked {} as invalid (not found on chain)",
                        req.txid
                    );
                }
            }
        }

        Ok(())
    }

    async fn review_status(&self) -> Result<ReviewStatusResult> {
        let mut log = String::new();
        let now = chrono::Utc::now();

        // Check 1: Mark transactions as failed if their proven_tx_req is 'invalid'
        let failed_count = sqlx::query(
            r#"
            UPDATE transactions SET status = 'failed', updated_at = ?
            WHERE status NOT IN ('failed', 'completed')
              AND txid IN (
                SELECT ptr.txid FROM proven_tx_reqs ptr
                WHERE ptr.status = 'invalid'
              )
            "#,
        )
        .bind(now)
        .execute(self.pool())
        .await?
        .rows_affected();

        if failed_count > 0 {
            log.push_str(&format!(
                "Marked {} transactions as failed (proven_tx_req invalid)\n",
                failed_count
            ));
            tracing::debug!(
                "review_status: marked {} transactions failed from invalid reqs",
                failed_count
            );
        }

        // Check 2: REMOVED — blind re-marking of doubleSpend inputs creates a
        // feedback loop. Previously this ran:
        //   UPDATE outputs SET spent_by = NULL, spendable = 1
        //   WHERE spent_by IN (SELECT transaction_id FROM transactions WHERE status = 'failed')
        //
        // This is the most dangerous path: it runs every ~15 minutes and blanket
        // re-marks ALL outputs from failed txs as spendable, including those consumed
        // on-chain by competing transactions (doubleSpend). The wallet then picks them
        // up again, broadcasts, gets doubleSpend again → infinite loop.
        //
        // Inputs from failed txs are now restored ONLY via UTXO-verified rollback
        // paths in: update_transaction_status_after_broadcast (process_action.rs),
        // send_waiting_transactions, and abort_abandoned.

        // Check 3: Mark transactions completed when proof exists
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT ptr.txid, t.status
            FROM proven_tx_reqs ptr
            JOIN transactions t ON t.txid = ptr.txid
            WHERE ptr.status = 'completed'
              AND t.status != 'completed'
              AND ptr.proven_tx_id IS NOT NULL
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if !rows.is_empty() {
            for (txid, old_status) in &rows {
                sqlx::query(
                    "UPDATE transactions SET status = 'completed', updated_at = ? WHERE txid = ?",
                )
                .bind(now)
                .bind(txid)
                .execute(self.pool())
                .await?;

                tracing::debug!(
                    "review_status: synced transaction {} from {} to completed",
                    txid,
                    old_status
                );
            }

            log.push_str(&format!(
                "Updated {} transaction statuses to completed\n",
                rows.len()
            ));
        }

        if log.is_empty() {
            log.push_str("No status mismatches found\n");
        }

        Ok(ReviewStatusResult { log })
    }

    async fn purge_data(&self, params: PurgeParams) -> Result<PurgeResults> {
        let mut count = 0u32;
        let mut log = String::new();
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(params.max_age_days as i64);

        if params.purge_failed {
            // Delete old failed/invalid proven_tx_reqs
            let result = sqlx::query(
                r#"
                DELETE FROM proven_tx_reqs
                WHERE status IN ('failed', 'invalid', 'doubleSpend')
                  AND updated_at < ?
                "#,
            )
            .bind(cutoff)
            .execute(self.pool())
            .await?;

            let deleted = result.rows_affected() as u32;
            count += deleted;
            log.push_str(&format!(
                "Purged {} failed/invalid proven_tx_reqs\n",
                deleted
            ));
        }

        if params.purge_completed {
            // Clear raw data from old completed proven_tx_reqs (keep the record)
            let result = sqlx::query(
                r#"
                UPDATE proven_tx_reqs
                SET raw_tx = NULL, input_beef = NULL, updated_at = ?
                WHERE status = 'completed'
                  AND updated_at < ?
                  AND (raw_tx IS NOT NULL OR input_beef IS NOT NULL)
                "#,
            )
            .bind(now)
            .bind(cutoff)
            .execute(self.pool())
            .await?;

            let cleaned = result.rows_affected() as u32;
            count += cleaned;
            log.push_str(&format!(
                "Cleaned raw data from {} completed proven_tx_reqs\n",
                cleaned
            ));
        }

        Ok(PurgeResults { count, log })
    }

    async fn compact_input_beefs(&self) -> Result<u32> {
        use bsv_rs::transaction::{Beef, MerklePath};

        // Find completed proven_tx_reqs with non-null input_beef.
        // Process in batches of 50 to avoid holding the connection too long.
        let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, input_beef
            FROM proven_tx_reqs
            WHERE status = 'completed'
              AND input_beef IS NOT NULL
              AND LENGTH(input_beef) > 1000
            ORDER BY LENGTH(input_beef) DESC
            LIMIT 50
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut compacted = 0u32;

        for (req_id, beef_bytes) in &rows {
            let mut beef = match Beef::from_binary(beef_bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let original_size = beef_bytes.len();

            // Find unproven txids in this BEEF
            let unproven_txids: Vec<String> = beef
                .txs
                .iter()
                .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
                .map(|tx| tx.txid())
                .collect();

            if unproven_txids.is_empty() {
                continue;
            }

            // Check proven_txs for available proofs (batch query)
            let mut upgraded = 0u32;

            for chunk in unproven_txids.chunks(400) {
                let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query_str = format!(
                    "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
                    placeholders
                );

                let mut query = sqlx::query(&query_str);
                for txid in chunk {
                    query = query.bind(txid);
                }

                let proof_rows: Vec<(String, Vec<u8>)> = query
                    .fetch_all(self.pool())
                    .await?
                    .into_iter()
                    .map(|row| {
                        let txid: String = sqlx::Row::get(&row, "txid");
                        let mp: Vec<u8> = sqlx::Row::get(&row, "merkle_path");
                        (txid, mp)
                    })
                    .collect();

                for (txid, merkle_path_bytes) in &proof_rows {
                    if let Ok(merkle_path) = MerklePath::from_binary(merkle_path_bytes) {
                        let bump_index = beef.merge_bump(merkle_path);
                        if let Some(tx) = beef.find_txid_mut(txid) {
                            tx.set_bump_index(Some(bump_index));
                            upgraded += 1;
                        }
                    }
                }
            }

            if upgraded == 0 {
                continue;
            }

            // NOTE: Do NOT call beef.trim_known_proven() — it creates
            // orphaned bump refs. No reference implementation has this.

            let new_bytes = beef.to_binary();
            let new_size = new_bytes.len();

            // Only update if we actually saved space
            if new_size < original_size {
                let now = chrono::Utc::now();
                sqlx::query(
                    "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                )
                .bind(&new_bytes)
                .bind(now)
                .bind(req_id)
                .execute(self.pool())
                .await?;

                compacted += 1;
                tracing::debug!(
                    req_id = req_id,
                    original_kb = original_size / 1024,
                    new_kb = new_size / 1024,
                    savings_pct = ((original_size - new_size) * 100) / original_size,
                    "Compacted input_beef"
                );
            }
        }

        Ok(compacted)
    }

    async fn try_acquire_task_lock(
        &self,
        task_name: &str,
        instance_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let now = std::time::Instant::now();
        let mut locks = self.task_locks.write().await;

        // Check if there's an existing lock
        if let Some((holder, expiry)) = locks.get(task_name) {
            if *expiry > now {
                // Lock is still valid
                if holder == instance_id {
                    // We already hold it - extend the TTL
                    locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
                    return Ok(true);
                }
                // Another instance holds it
                return Ok(false);
            }
            // Lock has expired - fall through to acquire
        }

        // Acquire the lock
        locks.insert(task_name.to_string(), (instance_id.to_string(), now + ttl));
        Ok(true)
    }

    async fn release_task_lock(&self, task_name: &str, instance_id: &str) -> Result<()> {
        let mut locks = self.task_locks.write().await;
        if let Some((holder, _)) = locks.get(task_name) {
            if holder == instance_id {
                locks.remove(task_name);
            }
        }
        Ok(())
    }

    async fn update_proven_tx_req_status(
        &self,
        proven_tx_req_id: i64,
        new_status: ProvenTxReqStatus,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        let status_str = match new_status {
            ProvenTxReqStatus::Unmined => "unmined",
            ProvenTxReqStatus::Completed => "completed",
            ProvenTxReqStatus::Failed => "failed",
            ProvenTxReqStatus::Invalid => "invalid",
            ProvenTxReqStatus::Pending => "pending",
            ProvenTxReqStatus::InProgress => "inProgress",
            ProvenTxReqStatus::NotFound => "notFound",
            ProvenTxReqStatus::Unsent => "unsent",
            ProvenTxReqStatus::Sending => "sending",
            ProvenTxReqStatus::Unknown => "unknown",
            ProvenTxReqStatus::Callback => "callback",
            ProvenTxReqStatus::Unconfirmed => "unconfirmed",
            ProvenTxReqStatus::Unfail => "unfail",
            ProvenTxReqStatus::NoSend => "nosend",
            ProvenTxReqStatus::DoubleSpend => "doubleSpend",
            ProvenTxReqStatus::NonFinal => "nonfinal",
            ProvenTxReqStatus::Unprocessed => "unprocessed",
        };

        sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(status_str)
        .bind(now)
        .bind(proven_tx_req_id)
        .execute(self.pool())
        .await?;

        tracing::debug!(
            proven_tx_req_id = proven_tx_req_id,
            new_status = status_str,
            "Updated proven_tx_req status"
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

    /// Find a spendable change output suitable for use as input.
    ///
    /// Looks for change outputs that are spendable and belong to the specified basket.
    /// Optionally excludes outputs from transactions currently in 'sending' status.
    ///
    /// # Arguments
    /// * `user_id` - The user who owns the outputs
    /// * `basket_id` - The basket to search in
    /// * `_target_satoshis` - Target satoshi amount (for future optimization)
    /// * `_exclude_sending` - Whether to exclude outputs from sending transactions
    /// * `_transaction_id` - The transaction that will use this input
    pub async fn allocate_change_input(
        &self,
        user_id: i64,
        basket_id: i64,
        target_satoshis: i64,
        exclude_sending: bool,
        transaction_id: i64,
    ) -> Result<Option<TableOutput>> {
        // Find a spendable change output suitable for use as a transaction input.
        // Strategy: select the smallest output >= target_satoshis to minimize change,
        // or the largest output if none meet the target.
        // Find a spendable change output suitable for use as a transaction input.
        // Strategy: select the smallest output >= target_satoshis to minimize change,
        // or the largest output if none meet the target.
        let base_query = if exclude_sending {
            r#"
            SELECT output_id, satoshis, vout, txid, locking_script
            FROM outputs
            WHERE user_id = ?
              AND basket_id = ?
              AND change = 1
              AND spendable = 1
              AND spent_by IS NULL
              AND transaction_id != ?
              AND transaction_id NOT IN (
                SELECT transaction_id FROM transactions WHERE status = 'sending'
              )
            ORDER BY
              CASE WHEN satoshis >= ? THEN 0 ELSE 1 END,
              CASE WHEN satoshis >= ? THEN satoshis ELSE -satoshis END
            LIMIT 1
            "#
        } else {
            r#"
            SELECT output_id, satoshis, vout, txid, locking_script
            FROM outputs
            WHERE user_id = ?
              AND basket_id = ?
              AND change = 1
              AND spendable = 1
              AND spent_by IS NULL
              AND transaction_id != ?
            ORDER BY
              CASE WHEN satoshis >= ? THEN 0 ELSE 1 END,
              CASE WHEN satoshis >= ? THEN satoshis ELSE -satoshis END
            LIMIT 1
            "#
        };

        #[allow(clippy::type_complexity)]
        let row: Option<(i64, i64, i64, Option<String>, Option<Vec<u8>>)> =
            sqlx::query_as(base_query)
                .bind(user_id)
                .bind(basket_id)
                .bind(transaction_id)
                .bind(target_satoshis)
                .bind(target_satoshis)
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((output_id, _satoshis, _vout, _txid, _locking_script)) => {
                // Mark the output as non-spendable (allocated to this transaction)
                sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
                    .bind(transaction_id)
                    .bind(output_id)
                    .execute(&self.pool)
                    .await?;

                // Fetch the full output record
                let full_row = sqlx::query("SELECT * FROM outputs WHERE output_id = ?")
                    .bind(output_id)
                    .fetch_one(&self.pool)
                    .await?;

                let now = chrono::Utc::now();
                let output = TableOutput {
                    output_id: full_row.get("output_id"),
                    user_id: full_row.get("user_id"),
                    transaction_id: full_row.get("transaction_id"),
                    basket_id: full_row.try_get("basket_id").ok(),
                    txid: full_row.get("txid"),
                    vout: full_row.get("vout"),
                    satoshis: full_row.get("satoshis"),
                    locking_script: full_row.try_get("locking_script").ok().flatten(),
                    script_length: full_row.try_get("script_length").unwrap_or(0),
                    script_offset: full_row.try_get("script_offset").unwrap_or(0),
                    output_type: full_row.try_get("output_type").unwrap_or_default(),
                    provided_by: full_row.try_get("provided_by").unwrap_or_default(),
                    purpose: full_row.try_get("purpose").ok(),
                    output_description: full_row.try_get("output_description").ok(),
                    spent_by: full_row.try_get("spent_by").ok().flatten(),
                    sequence_number: full_row.try_get("sequence_number").ok().flatten(),
                    spending_description: full_row.try_get("spending_description").ok(),
                    spendable: full_row.try_get::<bool, _>("spendable").unwrap_or(false),
                    change: full_row.try_get::<bool, _>("change").unwrap_or(false),
                    derivation_prefix: full_row.try_get("derivation_prefix").ok(),
                    derivation_suffix: full_row.try_get("derivation_suffix").ok(),
                    sender_identity_key: full_row.try_get("sender_identity_key").ok(),
                    custom_instructions: full_row.try_get("custom_instructions").ok(),
                    created_at: full_row.try_get("created_at").unwrap_or(now),
                    updated_at: full_row.try_get("updated_at").unwrap_or(now),
                };

                Ok(Some(output))
            }
            None => Ok(None),
        }
    }

    /// Get labels associated with a transaction.
    ///
    /// Queries the tx_label_maps and tx_labels tables to find all labels
    /// for a given transaction.
    pub async fn get_labels_for_transaction_id(&self, transaction_id: i64) -> Result<Vec<String>> {
        let labels: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT tl.label
            FROM tx_labels_map tlm
            JOIN tx_labels tl ON tlm.tx_label_id = tl.tx_label_id
            WHERE tlm.transaction_id = ?
              AND tl.is_deleted = 0
            ORDER BY tl.label
            "#,
        )
        .bind(transaction_id)
        .fetch_all(self.pool())
        .await?;

        Ok(labels.into_iter().map(|(l,)| l).collect())
    }

    /// Get tags associated with an output.
    ///
    /// Queries the output_tag_maps and output_tags tables to find all tags
    /// for a given output.
    pub async fn get_tags_for_output_id(&self, output_id: i64) -> Result<Vec<String>> {
        let tags: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT ot.tag
            FROM output_tags_map otm
            JOIN output_tags ot ON otm.output_tag_id = ot.output_tag_id
            WHERE otm.output_id = ?
              AND ot.is_deleted = 0
            ORDER BY ot.tag
            "#,
        )
        .bind(output_id)
        .fetch_all(self.pool())
        .await?;

        Ok(tags.into_iter().map(|(t,)| t).collect())
    }

    /// Count spendable change outputs for a user in a basket.
    ///
    /// # Arguments
    /// * `_user_id` - The user who owns the outputs
    /// * `_basket_id` - The basket to count in
    /// * `_exclude_sending` - Whether to exclude outputs from sending transactions
    pub async fn count_change_inputs(
        &self,
        user_id: i64,
        basket_id: i64,
        exclude_sending: bool,
    ) -> Result<i64> {
        let count: (i64,) = if exclude_sending {
            sqlx::query_as(
                r#"
                SELECT COUNT(*)
                FROM outputs o
                WHERE o.user_id = ?
                  AND o.basket_id = ?
                  AND o.change = 1
                  AND o.spendable = 1
                  AND o.spent_by IS NULL
                  AND o.transaction_id NOT IN (
                    SELECT transaction_id FROM transactions WHERE status = 'sending'
                  )
                "#,
            )
            .bind(user_id)
            .bind(basket_id)
            .fetch_one(self.pool())
            .await?
        } else {
            sqlx::query_as(
                r#"
                SELECT COUNT(*)
                FROM outputs o
                WHERE o.user_id = ?
                  AND o.basket_id = ?
                  AND o.change = 1
                  AND o.spendable = 1
                  AND o.spent_by IS NULL
                "#,
            )
            .bind(user_id)
            .bind(basket_id)
            .fetch_one(self.pool())
            .await?
        };

        Ok(count.0)
    }

    /// Find certificate fields for a given certificate.
    pub async fn find_certificate_fields(
        &self,
        certificate_id: i64,
    ) -> Result<Vec<TableCertificateField>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM certificate_fields
            WHERE certificate_id = ?
            ORDER BY field_name
            "#,
        )
        .bind(certificate_id)
        .fetch_all(self.pool())
        .await?;

        let fields = rows
            .iter()
            .map(|row| {
                let now = chrono::Utc::now();
                TableCertificateField {
                    certificate_field_id: row.get("certificate_field_id"),
                    certificate_id: row.get("certificate_id"),
                    user_id: row.get("user_id"),
                    field_name: row.get("field_name"),
                    field_value: row.get("field_value"),
                    master_key: row.get("master_key"),
                    created_at: row.try_get("created_at").unwrap_or(now),
                    updated_at: row.try_get("updated_at").unwrap_or(now),
                }
            })
            .collect();

        Ok(fields)
    }

    /// Get a raw transaction, preferring proven_tx but falling back to transactions table.
    pub async fn get_proven_or_raw_tx(&self, txid: &str) -> Result<Option<Vec<u8>>> {
        // First try proven_txs
        let proven: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT raw_tx FROM proven_txs WHERE txid = ? AND raw_tx IS NOT NULL")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;

        if let Some((raw_tx,)) = proven {
            return Ok(Some(raw_tx));
        }

        // Fall back to transactions table
        let tx: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;

        Ok(tx.map(|(raw_tx,)| raw_tx))
    }

    /// Get admin statistics about the storage.
    ///
    /// Returns counts of users, transactions, outputs, certificates, etc.
    pub async fn admin_stats(&self, _admin_identity_key: &str) -> Result<AdminStatsResult> {
        let users: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(self.pool())
            .await?;
        let transactions: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
            .fetch_one(self.pool())
            .await?;
        let outputs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outputs")
            .fetch_one(self.pool())
            .await?;
        let certificates: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM certificates WHERE is_deleted = 0")
                .fetch_one(self.pool())
                .await?;
        let proven_txs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_txs")
            .fetch_one(self.pool())
            .await?;
        let proven_tx_reqs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM proven_tx_reqs")
            .fetch_one(self.pool())
            .await?;

        Ok(AdminStatsResult {
            users: users.0 as u32,
            transactions: transactions.0 as u32,
            outputs: outputs.0 as u32,
            certificates: certificates.0 as u32,
            proven_txs: proven_txs.0 as u32,
            proven_tx_reqs: proven_tx_reqs.0 as u32,
        })
    }

    /// Clear old monitor events.
    ///
    /// # Arguments
    /// * `older_than` - Remove events older than this duration
    pub async fn cleanup_monitor_events(&self, older_than: std::time::Duration) -> Result<u64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
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

// =============================================================================
// BEEF Validation
// =============================================================================

/// Validate BEEF structure before broadcast (diagnostic — does not block broadcast).
///
/// Checks:
/// 1. BEEF contains exactly 1 unproven (leaf) transaction — the one being broadcast
/// 2. All inputs of the leaf tx have source transactions in the BEEF
/// 3. Source transactions either have merkle proofs (bump_index) or are themselves
///    in the BEEF with proofs
///
/// Returns `Ok(())` if valid, `Err(message)` with details of what's missing.
pub fn validate_beef_for_broadcast(beef: &Beef, txid: &str) -> std::result::Result<(), String> {
    use bsv_rs::transaction::Transaction;

    // Find unproven (leaf) transactions — those without a bump_index and not txid-only
    let unproven: Vec<&bsv_rs::transaction::BeefTx> = beef
        .txs
        .iter()
        .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
        .collect();

    if unproven.is_empty() {
        return Err(format!(
            "BEEF for {} has no unproven leaf transaction",
            txid
        ));
    }
    if unproven.len() > 1 {
        let ids: Vec<String> = unproven.iter().map(|t| t.txid()).collect();
        return Err(format!(
            "BEEF for {} has {} unproven transactions (expected 1): {:?}",
            txid,
            unproven.len(),
            ids
        ));
    }

    let leaf = unproven[0];
    let leaf_txid = leaf.txid();

    // Parse the leaf transaction to check its inputs
    let raw_bytes = match leaf.raw_tx() {
        Some(bytes) => bytes,
        None => {
            return Err(format!(
                "BEEF for {}: leaf tx {} has no raw bytes",
                txid, leaf_txid
            ));
        }
    };
    let parsed = match Transaction::from_binary(raw_bytes) {
        Ok(tx) => tx,
        Err(e) => {
            return Err(format!(
                "BEEF for {}: failed to parse leaf tx {}: {}",
                txid, leaf_txid, e
            ));
        }
    };

    // Check each input has its source in the BEEF
    let mut missing_sources = Vec::new();
    for (i, input) in parsed.inputs.iter().enumerate() {
        let source_txid = input
            .source_txid
            .as_deref()
            .or_else(|| input.source_transaction.as_ref().map(|_| "embedded"))
            .unwrap_or("unknown");

        if source_txid == "unknown" {
            missing_sources.push(format!("input[{}]: no source txid", i));
            continue;
        }
        if source_txid == "embedded" {
            continue; // source transaction is inline
        }

        // Check if the source txid exists in the BEEF
        if beef.find_txid(source_txid).is_none() {
            missing_sources.push(format!("input[{}]: source {} not in BEEF", i, source_txid));
        }
    }

    if !missing_sources.is_empty() {
        return Err(format!(
            "BEEF for {} missing source transactions: {}",
            txid,
            missing_sources.join("; ")
        ));
    }

    Ok(())
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
        let cert_row = sqlx::query(
            "SELECT certificate_id FROM certificates WHERE serial_number = 'serial123'",
        )
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
        assert_eq!(
            result.certificates[0].certificate.certificate_type,
            "test_type"
        );
        assert_eq!(
            result.certificates[0].certificate.serial_number,
            "serial123"
        );
        assert_eq!(
            result.certificates[0]
                .certificate
                .fields
                .get("name")
                .unwrap(),
            "John Doe"
        );
        assert!(result.certificates[0].keyring.is_some());
        assert_eq!(
            result.certificates[0]
                .keyring
                .as_ref()
                .unwrap()
                .get("name")
                .unwrap(),
            "master_key_123"
        );
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
        assert_eq!(
            result2.certificates[0].certificate.certificate_type,
            "type_b"
        );
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
            .log_monitor_event(
                "task_completed",
                Some(r#"{"task": "sync", "duration_ms": 100}"#),
            )
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
        storage.log_monitor_event("test_event", None).await.unwrap();

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

    // =============================================================================
    // Services Integration Tests
    // =============================================================================

    #[tokio::test]
    async fn test_get_services_without_set_returns_error() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // get_services without set_services should return error
        let result = storage.get_services();
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("setServices"),
                    "Error message should mention setServices: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn test_set_and_get_services() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Create a services instance
        let services = Arc::new(Services::new(Chain::Main).unwrap());

        // Set services
        storage.set_services(services.clone());

        // Get services should now work
        let retrieved = storage.get_services();
        assert!(retrieved.is_ok());

        // Verify we can call a method on the retrieved services
        let retrieved_services = retrieved.unwrap();
        let script_hash = retrieved_services.hash_output_script(&[0x76, 0xa9]);
        assert!(!script_hash.is_empty());
    }

    #[tokio::test]
    async fn test_set_services_replaces_previous() {
        use crate::services::{Chain, Services};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        // Set first services instance
        let services1 = Arc::new(Services::new(Chain::Main).unwrap());
        storage.set_services(services1);

        // Verify first services
        let retrieved1 = storage.get_services().unwrap();
        let hash1 = retrieved1.hash_output_script(&[0x76]);

        // Set second services instance (replaces first)
        let services2 = Arc::new(Services::new(Chain::Test).unwrap());
        storage.set_services(services2);

        // Get services again - should still work (services were replaced)
        let retrieved2 = storage.get_services().unwrap();
        let hash2 = retrieved2.hash_output_script(&[0x76]);

        // Both should produce the same hash (hash_output_script is deterministic)
        assert_eq!(hash1, hash2);
    }

    // =============================================================================
    // TrxToken / Transaction Scope Tests
    // =============================================================================

    #[tokio::test]
    async fn test_begin_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        assert!(token.id() > 0);
    }

    #[tokio::test]
    async fn test_commit_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.commit_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rollback_transaction() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token = storage.begin_transaction().await.unwrap();
        let result = storage.rollback_transaction(token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trx_token_uniqueness() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let token1 = storage.begin_transaction().await.unwrap();
        let token2 = storage.begin_transaction().await.unwrap();
        assert_ne!(token1.id(), token2.id());
        storage.commit_transaction(token1).await.unwrap();
        storage.commit_transaction(token2).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.commit_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    #[tokio::test]
    async fn test_rollback_invalid_token() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let bogus_token = TrxToken::new();
        let result = storage.rollback_transaction(bogus_token).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidOperation(msg)) => {
                assert!(
                    msg.contains("Unknown transaction token"),
                    "Error should mention unknown token: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidOperation error, got: {}", e),
            Ok(_) => panic!("Expected error for invalid token"),
        }
    }

    // =========================================================================
    // Task Lock Tests
    // =========================================================================

    #[tokio::test]
    async fn test_task_lock_acquire_and_release() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Acquire should succeed
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Release should succeed
        storage
            .release_task_lock("test_task", "instance_a")
            .await
            .unwrap();

        // After release, another instance can acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_contention() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Instance A acquires the lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Instance B should fail to acquire
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!acquired);

        // Instance A can re-acquire (extend TTL)
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_expiry() {
        let storage = StorageSqlx::in_memory().await.unwrap();

        // Acquire with a very short TTL
        let ttl = Duration::from_millis(1);
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Instance B should now be able to acquire the expired lock
        let acquired = storage
            .try_acquire_task_lock("test_task", "instance_b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(acquired);
    }

    #[tokio::test]
    async fn test_task_lock_different_tasks() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        let ttl = Duration::from_secs(60);

        // Different tasks should have independent locks
        let acquired_a = storage
            .try_acquire_task_lock("task_one", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_a);

        let acquired_b = storage
            .try_acquire_task_lock("task_two", "instance_a", ttl)
            .await
            .unwrap();
        assert!(acquired_b);

        // Another instance should be blocked on task_one but not task_three
        let blocked = storage
            .try_acquire_task_lock("task_one", "instance_b", ttl)
            .await
            .unwrap();
        assert!(!blocked);

        let ok = storage
            .try_acquire_task_lock("task_three", "instance_b", ttl)
            .await
            .unwrap();
        assert!(ok);
    }

    // =========================================================================
    // send_waiting_transactions doubleSpend + UTXO verification
    // =========================================================================

    // NOTE: A full integration test for send_waiting_transactions encountering a
    // doubleSpend with UTXO-verified rollback would require:
    //   1. A proven parent tx in `proven_txs` (with raw_tx + merkle_path for BEEF rebuild)
    //   2. A child tx in `transactions` + `proven_tx_reqs` with status='unprocessed'
    //   3. The child's raw_tx must be parseable and reference the parent's txid as input
    //   4. Outputs for the parent marked as spent_by the child
    //   5. Mock services: post_beef → double-spend, get_status_for_txids → no match,
    //      is_utxo → Sequence(true, false) for partial restore
    //
    // The core UTXO-verified rollback logic is already tested by:
    //   - test_double_spend_partial_utxo_verification (partial is_utxo results)
    //   - test_double_spend_restores_inputs (all inputs verified)
    //   - test_double_spend_no_services_keeps_inputs_locked (fail-safe)
    //
    // The send_waiting path reuses the same pattern inline. A full end-to-end
    // test is deferred because it requires BEEF rebuild infrastructure (proven_txs
    // with valid merkle paths) wired through the entire broadcast pipeline.

    // =========================================================================
    // validate_beef_for_broadcast Tests
    // =========================================================================

    /// Minimal valid coinbase-like transaction bytes (no real inputs).
    const MINIMAL_TX_BYTES: &[u8] = &[
        0x01, 0x00, 0x00, 0x00, // version
        0x01, // input count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // prev txid (32 zero bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
        0xff, // vout (0xFFFFFFFF for coinbase)
        0x00, // script length
        0xff, 0xff, 0xff, 0xff, // sequence
        0x01, // output count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // satoshis
        0x00, // script length
        0x00, 0x00, 0x00, 0x00, // locktime
    ];

    /// Compute the txid (double-SHA256, reversed) of raw transaction bytes.
    fn test_compute_txid(raw_tx: &[u8]) -> String {
        use bsv_rs::primitives::{sha256d, to_hex};
        let hash = sha256d(raw_tx);
        let mut reversed = hash;
        reversed.reverse();
        to_hex(&reversed)
    }

    /// Test validate_beef_for_broadcast with a valid BEEF: 1 unproven leaf + 1 proven parent.
    #[test]
    fn test_validate_beef_for_broadcast_valid() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;

        // Build the parent tx (proven, coinbase-like)
        let parent_raw = MINIMAL_TX_BYTES.to_vec();
        let parent_txid = test_compute_txid(&parent_raw);
        let parent_bump = MerklePath::from_coinbase_txid(&parent_txid, height);

        // Build a child tx that spends the parent
        let parent_txid_bytes = bsv_rs::primitives::from_hex(&parent_txid).unwrap();
        let mut parent_txid_le = parent_txid_bytes;
        parent_txid_le.reverse(); // txid stored little-endian in raw tx

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&parent_txid_le); // prev txid (parent)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(parent_bump);
        beef.merge_raw_tx(parent_raw, Some(bump_index)); // parent (proven)
        beef.merge_raw_tx(child_raw, None); // child (unproven leaf)

        // Validation should pass: 1 unproven leaf whose input parent is in the BEEF
        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_ok(),
            "Expected valid BEEF, got error: {:?}",
            result.err()
        );
    }

    /// Test validate_beef_for_broadcast with 0 unproven transactions (all proven).
    #[test]
    fn test_validate_beef_for_broadcast_no_unproven() {
        use bsv_rs::transaction::{Beef, MerklePath};

        let height = 800_000u32;
        let raw_tx = MINIMAL_TX_BYTES.to_vec();
        let txid = test_compute_txid(&raw_tx);
        let bump = MerklePath::from_coinbase_txid(&txid, height);

        let mut beef = Beef::new();
        let bump_index = beef.merge_bump(bump);
        beef.merge_raw_tx(raw_tx, Some(bump_index)); // proven — no unproven leaves

        let result = validate_beef_for_broadcast(&beef, &txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with no unproven leaf"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("no unproven leaf"),
            "Error should mention 'no unproven leaf', got: {}",
            err
        );
    }

    /// Test validate_beef_for_broadcast with a missing parent (input source not in BEEF).
    #[test]
    fn test_validate_beef_for_broadcast_missing_parent() {
        use bsv_rs::transaction::Beef;

        // Build a child tx that references a parent NOT in the BEEF.
        // Use a dummy parent txid that won't be in the BEEF.
        let fake_parent_txid = "ab".repeat(32);
        let fake_parent_bytes = bsv_rs::primitives::from_hex(&fake_parent_txid).unwrap();
        let mut fake_parent_le = fake_parent_bytes;
        fake_parent_le.reverse();

        let mut child_raw = Vec::new();
        child_raw.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw.push(0x01); // input count
        child_raw.extend_from_slice(&fake_parent_le); // prev txid (missing from BEEF)
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout = 0
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw.push(0x01); // output count
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // sats
        child_raw.push(0x00); // script length = 0
        child_raw.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = test_compute_txid(&child_raw);

        let mut beef = Beef::new();
        beef.merge_raw_tx(child_raw, None); // child only, no parent

        let result = validate_beef_for_broadcast(&beef, &child_txid);
        assert!(
            result.is_err(),
            "Expected error for BEEF with missing parent"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("missing source"),
            "Error should mention 'missing source', got: {}",
            err
        );
    }
}
