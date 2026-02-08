//! SQLite-based Chaintracks storage
//!
//! Provides persistent storage for blockchain headers using SQLite.
//! Based on Go implementation: `pkg/services/chaintracks/gormstorage/`

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::sync::RwLock;
use tracing::{debug, info, warn};

use crate::chaintracks::{
    calculate_work, BlockHeader, Chain, ChaintracksStorage, ChaintracksStorageIngest,
    ChaintracksStorageQuery, HeightRange, InsertHeaderResult, LiveBlockHeader,
};
use crate::lock_utils::{lock_read, lock_write};
use crate::Result;

/// SQLite storage for Chaintracks
///
/// Provides persistent storage for blockchain headers with the following features:
/// - Live headers with fork tracking
/// - Efficient lookups by hash, height, and merkle root
/// - Reorg handling with deactivation tracking
pub struct SqliteStorage {
    pool: Pool<Sqlite>,
    chain: Chain,
    live_height_threshold: u32,
    reorg_height_threshold: u32,
    available: RwLock<bool>,
}

impl SqliteStorage {
    /// Create a new SQLite storage
    ///
    /// # Arguments
    /// * `database_url` - SQLite database URL (e.g., "sqlite:chaintracks.db" or "sqlite::memory:")
    /// * `chain` - The blockchain network to track
    pub async fn new(database_url: &str, chain: Chain) -> Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;

        Ok(Self {
            pool,
            chain,
            live_height_threshold: 2000,
            reorg_height_threshold: 400,
            available: RwLock::new(false),
        })
    }

    /// Create with custom thresholds
    pub async fn with_thresholds(
        database_url: &str,
        chain: Chain,
        live_height_threshold: u32,
        reorg_height_threshold: u32,
    ) -> Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;

        Ok(Self {
            pool,
            chain,
            live_height_threshold,
            reorg_height_threshold,
            available: RwLock::new(false),
        })
    }

    /// Open in-memory database (for testing)
    pub async fn in_memory(chain: Chain) -> Result<Self> {
        Self::new("sqlite::memory:", chain).await
    }

    /// Get the database pool
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }

    /// Create the database schema
    async fn create_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chaintracks_live_headers (
                header_id INTEGER PRIMARY KEY AUTOINCREMENT,
                previous_header_id INTEGER,
                previous_hash TEXT NOT NULL,
                height INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 0,
                is_chain_tip INTEGER NOT NULL DEFAULT 0,
                hash TEXT NOT NULL UNIQUE,
                chain_work TEXT NOT NULL,
                version INTEGER NOT NULL,
                merkle_root TEXT NOT NULL,
                time INTEGER NOT NULL,
                bits INTEGER NOT NULL,
                nonce INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (previous_header_id) REFERENCES chaintracks_live_headers(header_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for efficient lookups
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_live_headers_height ON chaintracks_live_headers(height)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_live_headers_active ON chaintracks_live_headers(is_active)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_live_headers_tip ON chaintracks_live_headers(is_chain_tip)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_live_headers_merkle ON chaintracks_live_headers(merkle_root) WHERE is_active = 1",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Map a database row to LiveBlockHeader
    fn row_to_header(row: &sqlx::sqlite::SqliteRow) -> LiveBlockHeader {
        LiveBlockHeader {
            header_id: row.get("header_id"),
            previous_header_id: row.get("previous_header_id"),
            previous_hash: row.get("previous_hash"),
            height: row.get::<i64, _>("height") as u32,
            is_active: row.get::<i32, _>("is_active") != 0,
            is_chain_tip: row.get::<i32, _>("is_chain_tip") != 0,
            hash: row.get("hash"),
            chain_work: row.get("chain_work"),
            version: row.get::<i64, _>("version") as u32,
            merkle_root: row.get("merkle_root"),
            time: row.get::<i64, _>("time") as u32,
            bits: row.get::<i64, _>("bits") as u32,
            nonce: row.get::<i64, _>("nonce") as u32,
        }
    }

    /// Get the current chain tip
    async fn get_tip(&self) -> Result<Option<LiveBlockHeader>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE is_chain_tip = 1
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Self::row_to_header(&r)))
    }

    /// Set the chain tip (clears old tip, sets new tip)
    async fn set_tip(&self, header_id: i64) -> Result<()> {
        // Clear old tip
        sqlx::query("UPDATE chaintracks_live_headers SET is_chain_tip = 0 WHERE is_chain_tip = 1")
            .execute(&self.pool)
            .await?;

        // Set new tip
        sqlx::query("UPDATE chaintracks_live_headers SET is_chain_tip = 1 WHERE header_id = ?")
            .bind(header_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Handle a chain reorganization
    async fn handle_reorg(
        &self,
        new_tip: &LiveBlockHeader,
        old_tip: &LiveBlockHeader,
    ) -> Result<Vec<LiveBlockHeader>> {
        let mut deactivated = Vec::new();

        // Find common ancestor
        let ancestor = self.find_common_ancestor(new_tip, old_tip).await?;
        let ancestor_height = ancestor.as_ref().map(|a| a.height).unwrap_or(0);

        // Deactivate old chain from tip down to ancestor
        let old_chain_rows = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE is_active = 1 AND height > ?
            ORDER BY height DESC
            "#,
        )
        .bind(ancestor_height as i64)
        .fetch_all(&self.pool)
        .await?;

        for row in old_chain_rows {
            let header = Self::row_to_header(&row);
            deactivated.push(header.clone());

            sqlx::query("UPDATE chaintracks_live_headers SET is_active = 0 WHERE header_id = ?")
                .bind(header.header_id)
                .execute(&self.pool)
                .await?;
        }

        // Activate new chain from new_tip down to ancestor
        // Walk back from new_tip following previous_header_id
        let mut current = Some(new_tip.clone());
        while let Some(header) = current {
            if header.height <= ancestor_height {
                break;
            }

            sqlx::query("UPDATE chaintracks_live_headers SET is_active = 1 WHERE header_id = ?")
                .bind(header.header_id)
                .execute(&self.pool)
                .await?;

            // Get previous header
            if let Some(prev_id) = header.previous_header_id {
                let row = sqlx::query("SELECT * FROM chaintracks_live_headers WHERE header_id = ?")
                    .bind(prev_id)
                    .fetch_optional(&self.pool)
                    .await?;

                current = row.map(|r| Self::row_to_header(&r));
            } else {
                current = None;
            }
        }

        info!(
            "Reorg handled: deactivated {} headers, new tip at height {}",
            deactivated.len(),
            new_tip.height
        );

        Ok(deactivated)
    }

    /// Get header count
    pub async fn header_count(&self) -> Result<usize> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chaintracks_live_headers")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0 as usize)
    }

    /// Check if a header exists by hash (optimized for existence check only)
    ///
    /// More efficient than `find_live_header_for_block_hash` when you only need
    /// to know if a header exists, not its full data.
    pub async fn live_header_exists(&self, hash: &str) -> Result<bool> {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM chaintracks_live_headers WHERE hash = ? LIMIT 1")
                .bind(hash)
                .fetch_one(&self.pool)
                .await?;

        Ok(row.0 > 0)
    }

    /// Find headers at or below a given height, sorted by height ascending
    ///
    /// Used for bulk operations and migrations.
    pub async fn find_headers_for_height_less_than_or_equal_sorted(
        &self,
        height: u32,
        limit: u32,
    ) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE height <= ?
            ORDER BY height ASC
            LIMIT ?
            "#,
        )
        .bind(height as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Delete headers by their IDs
    ///
    /// Handles foreign key constraints by first clearing references.
    pub async fn delete_live_headers_by_ids(&self, ids: &[i64]) -> Result<u32> {
        if ids.is_empty() {
            return Ok(0);
        }

        // Build placeholders for IN clause
        let placeholders: String = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        // First, clear previous_header_id references to prevent FK constraint violations
        let clear_refs_sql = format!(
            "UPDATE chaintracks_live_headers SET previous_header_id = NULL WHERE previous_header_id IN ({})",
            placeholders
        );
        let mut clear_query = sqlx::query(&clear_refs_sql);
        for id in ids {
            clear_query = clear_query.bind(*id);
        }
        clear_query.execute(&self.pool).await?;

        // Now delete the headers
        let delete_sql = format!(
            "DELETE FROM chaintracks_live_headers WHERE header_id IN ({})",
            placeholders
        );
        let mut delete_query = sqlx::query(&delete_sql);
        for id in ids {
            delete_query = delete_query.bind(*id);
        }
        let result = delete_query.execute(&self.pool).await?;

        let count = result.rows_affected() as u32;
        if count > 0 {
            debug!("Deleted {} headers by IDs", count);
        }

        Ok(count)
    }

    /// Set the is_chain_tip flag for a header by ID
    pub async fn set_chain_tip_by_id(&self, header_id: i64, is_chain_tip: bool) -> Result<()> {
        sqlx::query(
            "UPDATE chaintracks_live_headers SET is_chain_tip = ?, updated_at = ? WHERE header_id = ?",
        )
        .bind(if is_chain_tip { 1 } else { 0 })
        .bind(Utc::now())
        .bind(header_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Set the is_active flag for a header by ID
    pub async fn set_active_by_id(&self, header_id: i64, is_active: bool) -> Result<()> {
        sqlx::query(
            "UPDATE chaintracks_live_headers SET is_active = ?, updated_at = ? WHERE header_id = ?",
        )
        .bind(if is_active { 1 } else { 0 })
        .bind(Utc::now())
        .bind(header_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Batch insert headers for efficient bulk ingestion
    ///
    /// This method is optimized for the bulk ingestor which needs to insert
    /// 10k+ headers at a time. It uses a transaction and batch inserts for
    /// performance.
    ///
    /// Note: This method does NOT update the chain tip or handle reorgs.
    /// It's designed for initial sync where headers are inserted in order.
    /// The caller should call `update_chain_tip_to_highest()` after batch insert.
    ///
    /// # Arguments
    /// * `headers` - Headers to insert (should be in height order for efficiency)
    ///
    /// # Returns
    /// * Number of headers actually inserted (excludes duplicates)
    pub async fn insert_headers_batch(&self, headers: &[LiveBlockHeader]) -> Result<u32> {
        if headers.is_empty() {
            return Ok(0);
        }

        let mut inserted = 0u32;
        let now = Utc::now();

        // Use a transaction for atomicity and performance
        let mut tx = self.pool.begin().await?;

        // Build a set of existing hashes for duplicate detection
        // For very large batches, we check in chunks
        let chunk_size = 500;
        let mut existing_hashes = std::collections::HashSet::new();

        for chunk in headers.chunks(chunk_size) {
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT hash FROM chaintracks_live_headers WHERE hash IN ({})",
                placeholders
            );
            let mut query = sqlx::query_scalar::<_, String>(&sql);
            for h in chunk {
                query = query.bind(&h.hash);
            }
            let existing: Vec<String> = query.fetch_all(&mut *tx).await?;
            existing_hashes.extend(existing);
        }

        // Insert headers in batches
        for header in headers {
            // Skip duplicates
            if existing_hashes.contains(&header.hash) {
                continue;
            }

            // Calculate chain work if not set
            let chain_work = if header.chain_work.is_empty() || header.chain_work == "0" {
                calculate_work(header.bits)
            } else {
                header.chain_work.clone()
            };

            // Find previous header ID if we have the previous hash
            let previous_header_id: Option<i64> = if header.previous_hash != "0".repeat(64) {
                let row: Option<(i64,)> =
                    sqlx::query_as("SELECT header_id FROM chaintracks_live_headers WHERE hash = ?")
                        .bind(&header.previous_hash)
                        .fetch_optional(&mut *tx)
                        .await?;
                row.map(|r| r.0)
            } else {
                None
            };

            // Insert the header (not as chain tip - we'll update that separately)
            sqlx::query(
                r#"
                INSERT INTO chaintracks_live_headers (
                    previous_header_id, previous_hash, height, is_active, is_chain_tip,
                    hash, chain_work, version, merkle_root, time, bits, nonce,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(previous_header_id)
            .bind(&header.previous_hash)
            .bind(header.height as i64)
            .bind(1i32) // is_active = true for bulk insert
            .bind(0i32) // is_chain_tip = false (will update after)
            .bind(&header.hash)
            .bind(&chain_work)
            .bind(header.version as i64)
            .bind(&header.merkle_root)
            .bind(header.time as i64)
            .bind(header.bits as i64)
            .bind(header.nonce as i64)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;

            inserted += 1;
        }

        // Commit the transaction
        tx.commit().await?;

        if inserted > 0 {
            info!("Batch inserted {} headers", inserted);
        }

        Ok(inserted)
    }

    /// Update the chain tip to the header with the highest height
    ///
    /// Call this after `insert_headers_batch` to set the correct chain tip.
    pub async fn update_chain_tip_to_highest(&self) -> Result<Option<LiveBlockHeader>> {
        // First, clear any existing chain tip
        sqlx::query("UPDATE chaintracks_live_headers SET is_chain_tip = 0 WHERE is_chain_tip = 1")
            .execute(&self.pool)
            .await?;

        // Find the header with the highest height among active headers
        let row = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE is_active = 1
            ORDER BY height DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => {
                let header = Self::row_to_header(&r);

                // Set it as the chain tip
                sqlx::query(
                    "UPDATE chaintracks_live_headers SET is_chain_tip = 1, updated_at = ? WHERE header_id = ?",
                )
                .bind(Utc::now())
                .bind(header.header_id)
                .execute(&self.pool)
                .await?;

                debug!(
                    "Updated chain tip to height {} hash {}",
                    header.height,
                    &header.hash[..16]
                );

                Ok(Some(header))
            }
            None => Ok(None),
        }
    }

    /// Get headers in a height range (inclusive), active chain only
    ///
    /// Returns headers ordered by height ascending.
    pub async fn get_headers_by_height_range(
        &self,
        start_height: u32,
        end_height: u32,
    ) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE height >= ? AND height <= ? AND is_active = 1
            ORDER BY height ASC
            "#,
        )
        .bind(start_height as i64)
        .bind(end_height as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Get all headers at a specific height (including forks)
    pub async fn get_headers_at_height(&self, height: u32) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query("SELECT * FROM chaintracks_live_headers WHERE height = ?")
            .bind(height as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Get active headers only
    pub async fn get_active_headers(&self) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query(
            "SELECT * FROM chaintracks_live_headers WHERE is_active = 1 ORDER BY height ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Get inactive (fork) headers only
    pub async fn get_fork_headers(&self) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query(
            "SELECT * FROM chaintracks_live_headers WHERE is_active = 0 ORDER BY height ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Find headers that build on a given hash (children)
    pub async fn find_children(&self, parent_hash: &str) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query("SELECT * FROM chaintracks_live_headers WHERE previous_hash = ?")
            .bind(parent_hash)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    /// Mark all headers at or above a height as inactive (for reorg handling)
    ///
    /// Returns the number of headers marked inactive.
    pub async fn mark_headers_inactive_above_height(&self, height: u32) -> Result<u32> {
        let result = sqlx::query(
            "UPDATE chaintracks_live_headers SET is_active = 0, is_chain_tip = 0, updated_at = ? WHERE height >= ? AND is_active = 1",
        )
        .bind(Utc::now())
        .bind(height as i64)
        .execute(&self.pool)
        .await?;

        let count = result.rows_affected() as u32;
        if count > 0 {
            info!("Marked {} headers inactive above height {}", count, height);
        }

        Ok(count)
    }
}

#[async_trait]
impl ChaintracksStorageQuery for SqliteStorage {
    fn chain(&self) -> Chain {
        self.chain
    }

    fn live_height_threshold(&self) -> u32 {
        self.live_height_threshold
    }

    fn reorg_height_threshold(&self) -> u32 {
        self.reorg_height_threshold
    }

    async fn find_chain_tip_header(&self) -> Result<Option<LiveBlockHeader>> {
        self.get_tip().await
    }

    async fn find_chain_tip_hash(&self) -> Result<Option<String>> {
        Ok(self.get_tip().await?.map(|h| h.hash))
    }

    async fn find_header_for_height(&self, height: u32) -> Result<Option<BlockHeader>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE height = ? AND is_active = 1
            LIMIT 1
            "#,
        )
        .bind(height as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Self::row_to_header(&r).into()))
    }

    async fn find_live_header_for_block_hash(&self, hash: &str) -> Result<Option<LiveBlockHeader>> {
        let row = sqlx::query("SELECT * FROM chaintracks_live_headers WHERE hash = ?")
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| Self::row_to_header(&r)))
    }

    async fn find_live_header_for_merkle_root(
        &self,
        merkle_root: &str,
    ) -> Result<Option<LiveBlockHeader>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE merkle_root = ? AND is_active = 1
            LIMIT 1
            "#,
        )
        .bind(merkle_root)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Self::row_to_header(&r)))
    }

    async fn get_headers_bytes(&self, height: u32, count: u32) -> Result<Vec<u8>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM chaintracks_live_headers
            WHERE height >= ? AND height < ? AND is_active = 1
            ORDER BY height ASC
            "#,
        )
        .bind(height as i64)
        .bind((height + count) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut bytes = Vec::with_capacity(rows.len() * 80);
        for row in rows {
            let header = Self::row_to_header(&row);
            // Serialize header to 80 bytes manually (LiveBlockHeader doesn't have to_bytes)
            bytes.extend_from_slice(&header.version.to_le_bytes());
            if let Ok(prev) = hex::decode(&header.previous_hash) {
                if prev.len() == 32 {
                    bytes.extend_from_slice(&prev);
                } else {
                    bytes.extend_from_slice(&[0u8; 32]);
                }
            } else {
                bytes.extend_from_slice(&[0u8; 32]);
            }
            if let Ok(merkle) = hex::decode(&header.merkle_root) {
                if merkle.len() == 32 {
                    bytes.extend_from_slice(&merkle);
                } else {
                    bytes.extend_from_slice(&[0u8; 32]);
                }
            } else {
                bytes.extend_from_slice(&[0u8; 32]);
            }
            bytes.extend_from_slice(&header.time.to_le_bytes());
            bytes.extend_from_slice(&header.bits.to_le_bytes());
            bytes.extend_from_slice(&header.nonce.to_le_bytes());
        }

        Ok(bytes)
    }

    async fn get_live_headers(&self) -> Result<Vec<LiveBlockHeader>> {
        let rows = sqlx::query("SELECT * FROM chaintracks_live_headers ORDER BY height DESC")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.iter().map(Self::row_to_header).collect())
    }

    async fn get_available_height_ranges(&self) -> Result<Vec<HeightRange>> {
        // SQLite storage only tracks live headers, no bulk ranges
        Ok(vec![])
    }

    async fn find_live_height_range(&self) -> Result<Option<HeightRange>> {
        let row: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
            r#"
            SELECT MIN(height), MAX(height)
            FROM chaintracks_live_headers
            WHERE is_active = 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((Some(min), Some(max))) => Ok(Some(HeightRange::new(min as u32, max as u32))),
            _ => Ok(None),
        }
    }

    async fn find_common_ancestor(
        &self,
        header1: &LiveBlockHeader,
        header2: &LiveBlockHeader,
    ) -> Result<Option<LiveBlockHeader>> {
        // Walk back from both headers until we find a common hash
        let mut h1 = Some(header1.clone());
        let mut h2 = Some(header2.clone());

        while let (Some(ref a), Some(ref b)) = (&h1, &h2) {
            if a.hash == b.hash {
                return Ok(h1);
            }

            // Move the higher one back
            match a.height.cmp(&b.height) {
                std::cmp::Ordering::Greater => {
                    h1 = if let Some(prev_id) = a.previous_header_id {
                        let row = sqlx::query(
                            "SELECT * FROM chaintracks_live_headers WHERE header_id = ?",
                        )
                        .bind(prev_id)
                        .fetch_optional(&self.pool)
                        .await?;
                        row.map(|r| Self::row_to_header(&r))
                    } else {
                        None
                    };
                }
                std::cmp::Ordering::Less => {
                    h2 = if let Some(prev_id) = b.previous_header_id {
                        let row = sqlx::query(
                            "SELECT * FROM chaintracks_live_headers WHERE header_id = ?",
                        )
                        .bind(prev_id)
                        .fetch_optional(&self.pool)
                        .await?;
                        row.map(|r| Self::row_to_header(&r))
                    } else {
                        None
                    };
                }
                std::cmp::Ordering::Equal => {
                    // Same height but different hashes - move both back
                    h1 = if let Some(prev_id) = a.previous_header_id {
                        let row = sqlx::query(
                            "SELECT * FROM chaintracks_live_headers WHERE header_id = ?",
                        )
                        .bind(prev_id)
                        .fetch_optional(&self.pool)
                        .await?;
                        row.map(|r| Self::row_to_header(&r))
                    } else {
                        None
                    };

                    h2 = if let Some(prev_id) = b.previous_header_id {
                        let row = sqlx::query(
                            "SELECT * FROM chaintracks_live_headers WHERE header_id = ?",
                        )
                        .bind(prev_id)
                        .fetch_optional(&self.pool)
                        .await?;
                        row.map(|r| Self::row_to_header(&r))
                    } else {
                        None
                    };
                }
            }
        }

        Ok(None)
    }

    async fn find_reorg_depth(&self, new_header: &LiveBlockHeader) -> Result<u32> {
        let tip = self.get_tip().await?;
        match tip {
            None => Ok(0),
            Some(current_tip) => {
                if new_header.previous_hash == current_tip.hash {
                    // Extends current tip, no reorg
                    Ok(0)
                } else {
                    // Find common ancestor
                    let ancestor = self.find_common_ancestor(new_header, &current_tip).await?;
                    match ancestor {
                        Some(a) => Ok(current_tip.height - a.height),
                        None => Ok(current_tip.height),
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ChaintracksStorageIngest for SqliteStorage {
    async fn insert_header(&self, mut header: LiveBlockHeader) -> Result<InsertHeaderResult> {
        // Check for duplicate
        let existing = self.find_live_header_for_block_hash(&header.hash).await?;
        if existing.is_some() {
            return Ok(InsertHeaderResult {
                added: false,
                dupe: true,
                ..Default::default()
            });
        }

        // Calculate chain work if not set
        if header.chain_work.is_empty() || header.chain_work == "0" {
            header.chain_work = calculate_work(header.bits);
        }

        // Find previous header
        let previous_header = if header.previous_hash != "0".repeat(64) {
            self.find_live_header_for_block_hash(&header.previous_hash)
                .await?
        } else {
            None
        };

        let previous_header_id = previous_header.as_ref().map(|h| h.header_id);

        // Get current tip
        let current_tip = self.get_tip().await?;

        // Determine if this becomes the new tip
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => header.height > tip.height,
        };

        // Insert the header
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO chaintracks_live_headers (
                previous_header_id, previous_hash, height, is_active, is_chain_tip,
                hash, chain_work, version, merkle_root, time, bits, nonce,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(previous_header_id)
        .bind(&header.previous_hash)
        .bind(header.height as i64)
        .bind(if becomes_tip { 1 } else { 0 })
        .bind(if becomes_tip { 1 } else { 0 })
        .bind(&header.hash)
        .bind(&header.chain_work)
        .bind(header.version as i64)
        .bind(&header.merkle_root)
        .bind(header.time as i64)
        .bind(header.bits as i64)
        .bind(header.nonce as i64)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let header_id = result.last_insert_rowid();
        header.header_id = header_id;
        header.previous_header_id = previous_header_id;

        let mut insert_result = InsertHeaderResult {
            added: true,
            no_prev: previous_header.is_none() && header.height > 0,
            no_tip: current_tip.is_none(),
            is_active_tip: becomes_tip,
            ..Default::default()
        };

        // Handle chain tip changes
        if becomes_tip {
            // Check for reorg
            if let Some(ref tip) = current_tip {
                if header.previous_hash != tip.hash {
                    // This is a reorg
                    let deactivated = self.handle_reorg(&header, tip).await?;
                    insert_result.reorg_depth = deactivated.len() as u32;
                    insert_result.deactivated_headers = deactivated;
                    insert_result.prior_tip = Some(tip.clone());
                }
            }

            // Set this as the new tip
            self.set_tip(header_id).await?;

            // Ensure the header is marked active
            sqlx::query("UPDATE chaintracks_live_headers SET is_active = 1 WHERE header_id = ?")
                .bind(header_id)
                .execute(&self.pool)
                .await?;
        }

        debug!(
            "Inserted header at height {} with hash {}",
            header.height,
            &header.hash[..16]
        );

        Ok(insert_result)
    }

    async fn prune_live_block_headers(&self, active_tip_height: u32) -> Result<u32> {
        let threshold = active_tip_height.saturating_sub(self.live_height_threshold);

        // First, clear previous_header_id references for headers that will be pruned
        // This prevents foreign key constraint violations
        sqlx::query(
            r#"
            UPDATE chaintracks_live_headers
            SET previous_header_id = NULL
            WHERE previous_header_id IN (
                SELECT header_id FROM chaintracks_live_headers
                WHERE is_active = 0 AND height < ?
            )
            "#,
        )
        .bind(threshold as i64)
        .execute(&self.pool)
        .await?;

        // Now delete the inactive headers below threshold
        let result = sqlx::query(
            r#"
            DELETE FROM chaintracks_live_headers
            WHERE is_active = 0 AND height < ?
            "#,
        )
        .bind(threshold as i64)
        .execute(&self.pool)
        .await?;

        let count = result.rows_affected() as u32;
        if count > 0 {
            info!(
                "Pruned {} inactive headers below height {}",
                count, threshold
            );
        }

        Ok(count)
    }

    async fn migrate_live_to_bulk(&self, _count: u32) -> Result<u32> {
        // SQLite storage doesn't support bulk migration
        // Headers remain in live storage
        Ok(0)
    }

    async fn delete_older_live_block_headers(&self, max_height: u32) -> Result<u32> {
        // First, clear previous_header_id references to prevent FK constraint violations
        sqlx::query(
            r#"
            UPDATE chaintracks_live_headers
            SET previous_header_id = NULL
            WHERE previous_header_id IN (
                SELECT header_id FROM chaintracks_live_headers
                WHERE height <= ?
            )
            "#,
        )
        .bind(max_height as i64)
        .execute(&self.pool)
        .await?;

        // Now delete the headers
        let result = sqlx::query("DELETE FROM chaintracks_live_headers WHERE height <= ?")
            .bind(max_height as i64)
            .execute(&self.pool)
            .await?;

        let count = result.rows_affected() as u32;
        if count > 0 {
            warn!(
                "Deleted {} headers at or below height {}",
                count, max_height
            );
        }

        Ok(count)
    }

    async fn make_available(&self) -> Result<()> {
        let mut available = lock_write(&self.available)?;
        *available = true;
        Ok(())
    }

    async fn migrate_latest(&self) -> Result<()> {
        self.create_tables().await
    }

    async fn drop_all_data(&self) -> Result<()> {
        sqlx::query("DELETE FROM chaintracks_live_headers")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        self.drop_all_data().await
    }
}

#[async_trait]
impl ChaintracksStorage for SqliteStorage {
    fn storage_type(&self) -> &str {
        "sqlite"
    }

    async fn is_available(&self) -> bool {
        lock_read(&self.available).map(|v| *v).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_storage() -> SqliteStorage {
        let storage = SqliteStorage::in_memory(Chain::Test).await.unwrap();
        storage.migrate_latest().await.unwrap();
        storage.make_available().await.unwrap();
        storage
    }

    fn create_test_header(height: u32, prev_hash: &str, hash: &str) -> LiveBlockHeader {
        LiveBlockHeader {
            header_id: 0,
            previous_header_id: None,
            previous_hash: prev_hash.to_string(),
            height,
            is_active: false,
            is_chain_tip: false,
            hash: hash.to_string(),
            chain_work: "".to_string(),
            version: 1,
            merkle_root: format!("merkle_{}", hash),
            time: 1234567890 + height,
            bits: 0x1d00ffff,
            nonce: 12345,
        }
    }

    #[tokio::test]
    async fn test_storage_type() {
        let storage = create_test_storage().await;
        assert_eq!(storage.storage_type(), "sqlite");
    }

    #[tokio::test]
    async fn test_is_available() {
        let storage = create_test_storage().await;
        assert!(storage.is_available().await);
    }

    #[tokio::test]
    async fn test_insert_header() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        let result = storage.insert_header(header).await.unwrap();

        assert!(result.added);
        assert!(!result.dupe);
        assert!(result.is_active_tip);
        assert!(result.no_tip);
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header.clone()).await.unwrap();

        let result = storage.insert_header(header).await.unwrap();
        assert!(!result.added);
        assert!(result.dupe);
    }

    #[tokio::test]
    async fn test_find_by_hash() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        let found = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().height, 0);
    }

    #[tokio::test]
    async fn test_find_by_height() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        let found = storage.find_header_for_height(0).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().hash, "hash_0");
    }

    #[tokio::test]
    async fn test_chain_growth() {
        let storage = create_test_storage().await;

        // Insert genesis
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        // Insert block 1
        let block1 = create_test_header(1, "hash_0", "hash_1");
        let result = storage.insert_header(block1).await.unwrap();

        assert!(result.added);
        assert!(result.is_active_tip);
        assert_eq!(result.reorg_depth, 0);

        // Verify tip
        let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(tip.height, 1);
        assert_eq!(tip.hash, "hash_1");
    }

    #[tokio::test]
    async fn test_find_merkle_root() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        let found = storage
            .find_live_header_for_merkle_root("merkle_hash_0")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().hash, "hash_0");
    }

    #[tokio::test]
    async fn test_prune_inactive() {
        let storage = create_test_storage().await;

        // Insert a chain of headers
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        // Manually mark genesis as inactive (simulating a reorg)
        sqlx::query("UPDATE chaintracks_live_headers SET is_active = 0 WHERE hash = 'hash_0'")
            .execute(storage.pool())
            .await
            .unwrap();

        // Prune with tip at height 2002 (threshold 2000)
        let pruned = storage.prune_live_block_headers(2002).await.unwrap();

        // Genesis (height 0) should be pruned since it's inactive and below threshold
        assert_eq!(pruned, 1);
    }

    #[tokio::test]
    async fn test_drop_all_data() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        assert_eq!(storage.header_count().await.unwrap(), 1);

        storage.drop_all_data().await.unwrap();

        assert_eq!(storage.header_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_find_live_height_range() {
        let storage = create_test_storage().await;

        // Empty storage
        let range = storage.find_live_height_range().await.unwrap();
        assert!(range.is_none());

        // Insert headers
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        let block1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(block1).await.unwrap();

        let block2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(block2).await.unwrap();

        let range = storage.find_live_height_range().await.unwrap().unwrap();
        assert_eq!(range.low, 0);
        assert_eq!(range.high, 2);
    }

    #[tokio::test]
    async fn test_get_headers_bytes() {
        let storage = create_test_storage().await;

        // Insert genesis
        let genesis = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(genesis).await.unwrap();

        // Get header bytes
        let bytes = storage.get_headers_bytes(0, 1).await.unwrap();

        // Each header is 80 bytes
        assert_eq!(bytes.len(), 80);
    }

    #[tokio::test]
    async fn test_live_header_exists() {
        let storage = create_test_storage().await;

        // Initially doesn't exist
        assert!(!storage.live_header_exists("hash_0").await.unwrap());

        // Insert header
        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        // Now exists
        assert!(storage.live_header_exists("hash_0").await.unwrap());
        assert!(!storage.live_header_exists("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_find_headers_for_height_less_than_or_equal_sorted() {
        let storage = create_test_storage().await;

        // Insert several headers
        for i in 0..5 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Find headers at or below height 2
        let headers = storage
            .find_headers_for_height_less_than_or_equal_sorted(2, 10)
            .await
            .unwrap();

        assert_eq!(headers.len(), 3); // heights 0, 1, 2
        assert_eq!(headers[0].height, 0);
        assert_eq!(headers[1].height, 1);
        assert_eq!(headers[2].height, 2);
    }

    #[tokio::test]
    async fn test_find_headers_with_limit() {
        let storage = create_test_storage().await;

        // Insert several headers
        for i in 0..10 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Find with limit
        let headers = storage
            .find_headers_for_height_less_than_or_equal_sorted(9, 3)
            .await
            .unwrap();

        assert_eq!(headers.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_live_headers_by_ids() {
        let storage = create_test_storage().await;

        // Insert headers
        let h0 = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(h0).await.unwrap();

        let h1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(h1).await.unwrap();

        let h2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(h2).await.unwrap();

        assert_eq!(storage.header_count().await.unwrap(), 3);

        // Get the IDs
        let header0 = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        let header1 = storage
            .find_live_header_for_block_hash("hash_1")
            .await
            .unwrap()
            .unwrap();

        // Delete headers 0 and 1
        let deleted = storage
            .delete_live_headers_by_ids(&[header0.header_id, header1.header_id])
            .await
            .unwrap();

        assert_eq!(deleted, 2);
        assert_eq!(storage.header_count().await.unwrap(), 1);

        // Verify remaining header
        let remaining = storage
            .find_live_header_for_block_hash("hash_2")
            .await
            .unwrap();
        assert!(remaining.is_some());
    }

    #[tokio::test]
    async fn test_delete_empty_ids() {
        let storage = create_test_storage().await;

        let deleted = storage.delete_live_headers_by_ids(&[]).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_set_chain_tip_by_id() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        let h = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        assert!(h.is_chain_tip);

        // Clear chain tip
        storage
            .set_chain_tip_by_id(h.header_id, false)
            .await
            .unwrap();

        let h = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        assert!(!h.is_chain_tip);

        // Set it back
        storage
            .set_chain_tip_by_id(h.header_id, true)
            .await
            .unwrap();

        let h = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        assert!(h.is_chain_tip);
    }

    #[tokio::test]
    async fn test_set_active_by_id() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        let h = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        assert!(h.is_active);

        // Mark inactive
        storage.set_active_by_id(h.header_id, false).await.unwrap();

        let h = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        assert!(!h.is_active);
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let storage = create_test_storage().await;

        // Create batch of headers
        let mut headers = Vec::new();
        for i in 0..100 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{:04}", i - 1)
            };
            headers.push(create_test_header(i, &prev_hash, &format!("hash_{:04}", i)));
        }

        // Batch insert
        let inserted = storage.insert_headers_batch(&headers).await.unwrap();
        assert_eq!(inserted, 100);
        assert_eq!(storage.header_count().await.unwrap(), 100);

        // Update chain tip
        let tip = storage
            .update_chain_tip_to_highest()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tip.height, 99);
    }

    #[tokio::test]
    async fn test_batch_insert_with_duplicates() {
        let storage = create_test_storage().await;

        // Insert some headers first
        let h0 = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(h0).await.unwrap();

        let h1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(h1).await.unwrap();

        // Batch insert with duplicates
        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"), // duplicate
            create_test_header(1, "hash_0", "hash_1"),        // duplicate
            create_test_header(2, "hash_1", "hash_2"),        // new
            create_test_header(3, "hash_2", "hash_3"),        // new
        ];

        let inserted = storage.insert_headers_batch(&headers).await.unwrap();
        assert_eq!(inserted, 2); // Only 2 new headers
        assert_eq!(storage.header_count().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_batch_insert_empty() {
        let storage = create_test_storage().await;

        let inserted = storage.insert_headers_batch(&[]).await.unwrap();
        assert_eq!(inserted, 0);
    }

    #[tokio::test]
    async fn test_update_chain_tip_to_highest() {
        let storage = create_test_storage().await;

        // Insert headers without setting tip
        let headers = vec![
            create_test_header(0, &"0".repeat(64), "hash_0"),
            create_test_header(1, "hash_0", "hash_1"),
            create_test_header(2, "hash_1", "hash_2"),
        ];

        storage.insert_headers_batch(&headers).await.unwrap();

        // Chain tip should be none or not the highest yet
        // Update to highest
        let tip = storage
            .update_chain_tip_to_highest()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tip.height, 2);
        assert_eq!(tip.hash, "hash_2");

        // Verify tip is set
        let fetched_tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(fetched_tip.height, 2);
    }

    #[tokio::test]
    async fn test_update_chain_tip_empty_storage() {
        let storage = create_test_storage().await;

        let tip = storage.update_chain_tip_to_highest().await.unwrap();
        assert!(tip.is_none());
    }

    #[tokio::test]
    async fn test_get_headers_by_height_range() {
        let storage = create_test_storage().await;

        // Insert headers
        for i in 0..10 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Get range
        let headers = storage.get_headers_by_height_range(3, 7).await.unwrap();

        assert_eq!(headers.len(), 5);
        assert_eq!(headers[0].height, 3);
        assert_eq!(headers[4].height, 7);
    }

    #[tokio::test]
    async fn test_get_headers_at_height() {
        let storage = create_test_storage().await;

        let h0 = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(h0).await.unwrap();

        let headers = storage.get_headers_at_height(0).await.unwrap();
        assert_eq!(headers.len(), 1);

        let headers = storage.get_headers_at_height(1).await.unwrap();
        assert!(headers.is_empty());
    }

    #[tokio::test]
    async fn test_get_active_headers() {
        let storage = create_test_storage().await;

        // Insert chain
        for i in 0..3 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Mark one as inactive
        let h1 = storage
            .find_live_header_for_block_hash("hash_1")
            .await
            .unwrap()
            .unwrap();
        storage.set_active_by_id(h1.header_id, false).await.unwrap();

        let active = storage.get_active_headers().await.unwrap();
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn test_get_fork_headers() {
        let storage = create_test_storage().await;

        // Insert chain
        for i in 0..3 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Initially no forks
        let forks = storage.get_fork_headers().await.unwrap();
        assert!(forks.is_empty());

        // Mark one as inactive (simulated fork)
        let h1 = storage
            .find_live_header_for_block_hash("hash_1")
            .await
            .unwrap()
            .unwrap();
        storage.set_active_by_id(h1.header_id, false).await.unwrap();

        let forks = storage.get_fork_headers().await.unwrap();
        assert_eq!(forks.len(), 1);
        assert_eq!(forks[0].hash, "hash_1");
    }

    #[tokio::test]
    async fn test_find_children() {
        let storage = create_test_storage().await;

        let h0 = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(h0).await.unwrap();

        let h1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(h1).await.unwrap();

        let children = storage.find_children("hash_0").await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].hash, "hash_1");

        let no_children = storage.find_children("hash_1").await.unwrap();
        assert!(no_children.is_empty());
    }

    #[tokio::test]
    async fn test_mark_headers_inactive_above_height() {
        let storage = create_test_storage().await;

        // Insert chain
        for i in 0..5 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // Mark headers at or above height 3 as inactive
        let marked = storage.mark_headers_inactive_above_height(3).await.unwrap();
        assert_eq!(marked, 2); // heights 3 and 4

        // Verify
        let h2 = storage
            .find_live_header_for_block_hash("hash_2")
            .await
            .unwrap()
            .unwrap();
        assert!(h2.is_active);

        let h3 = storage
            .find_live_header_for_block_hash("hash_3")
            .await
            .unwrap()
            .unwrap();
        assert!(!h3.is_active);

        let h4 = storage
            .find_live_header_for_block_hash("hash_4")
            .await
            .unwrap()
            .unwrap();
        assert!(!h4.is_active);
    }

    #[tokio::test]
    async fn test_reorg_handling() {
        let storage = create_test_storage().await;

        // Build main chain: 0 -> 1 -> 2
        let h0 = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(h0).await.unwrap();

        let h1 = create_test_header(1, "hash_0", "hash_1");
        storage.insert_header(h1).await.unwrap();

        let h2 = create_test_header(2, "hash_1", "hash_2");
        storage.insert_header(h2).await.unwrap();

        // Verify tip is at height 2
        let tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(tip.height, 2);

        // Create a competing chain: 0 -> 1' -> 2' -> 3'
        // First we need to manually insert a fork at height 1
        // Fork header at height 1, same previous as original h1
        let fork1 = create_test_header(1, "hash_0", "fork_hash_1");
        // This won't become tip because same height as existing tip chain

        // Create fork chain that's longer
        let fork2 = create_test_header(2, "fork_hash_1", "fork_hash_2");
        let fork3 = create_test_header(3, "fork_hash_2", "fork_hash_3");

        // Insert fork headers
        storage.insert_header(fork1).await.unwrap();
        storage.insert_header(fork2).await.unwrap();

        // Fork 3 should trigger reorg since it's higher than current tip
        let result = storage.insert_header(fork3).await.unwrap();
        assert!(result.added);
        assert!(result.is_active_tip);
        assert!(result.reorg_depth > 0);

        // Verify new tip
        let new_tip = storage.find_chain_tip_header().await.unwrap().unwrap();
        assert_eq!(new_tip.height, 3);
        assert_eq!(new_tip.hash, "fork_hash_3");
    }

    #[tokio::test]
    async fn test_empty_database_queries() {
        let storage = create_test_storage().await;

        // All queries should handle empty database gracefully
        assert!(storage.find_chain_tip_header().await.unwrap().is_none());
        assert!(storage.find_chain_tip_hash().await.unwrap().is_none());
        assert!(storage.find_header_for_height(0).await.unwrap().is_none());
        assert!(storage
            .find_live_header_for_block_hash("any")
            .await
            .unwrap()
            .is_none());
        assert!(storage
            .find_live_header_for_merkle_root("any")
            .await
            .unwrap()
            .is_none());
        assert!(storage.get_headers_bytes(0, 10).await.unwrap().is_empty());
        assert!(storage.get_live_headers().await.unwrap().is_empty());
        assert!(storage.find_live_height_range().await.unwrap().is_none());
        assert_eq!(storage.header_count().await.unwrap(), 0);
        assert!(!storage.live_header_exists("any").await.unwrap());
        assert!(storage
            .get_headers_by_height_range(0, 10)
            .await
            .unwrap()
            .is_empty());
        assert!(storage.get_headers_at_height(0).await.unwrap().is_empty());
        assert!(storage.get_active_headers().await.unwrap().is_empty());
        assert!(storage.get_fork_headers().await.unwrap().is_empty());
        assert!(storage.find_children("any").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_common_ancestor_detection() {
        let storage = create_test_storage().await;

        // Build chain: 0 -> 1 -> 2
        for i in 0..3 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        let h0 = storage
            .find_live_header_for_block_hash("hash_0")
            .await
            .unwrap()
            .unwrap();
        let h2 = storage
            .find_live_header_for_block_hash("hash_2")
            .await
            .unwrap()
            .unwrap();

        let ancestor = storage
            .find_common_ancestor(&h0, &h2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ancestor.hash, "hash_0");
    }

    #[tokio::test]
    async fn test_reorg_depth_calculation() {
        let storage = create_test_storage().await;

        // Build chain: 0 -> 1 -> 2
        for i in 0..3 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{}", i - 1)
            };
            let header = create_test_header(i, &prev_hash, &format!("hash_{}", i));
            storage.insert_header(header).await.unwrap();
        }

        // A new header extending the tip has 0 reorg depth
        let extending = create_test_header(3, "hash_2", "hash_3");
        let depth = storage
            .find_reorg_depth(&LiveBlockHeader {
                previous_hash: "hash_2".to_string(),
                ..extending
            })
            .await
            .unwrap();
        assert_eq!(depth, 0);
    }

    #[tokio::test]
    async fn test_batch_insert_large() {
        let storage = create_test_storage().await;

        // Create a large batch (simulating bulk ingestor)
        let mut headers = Vec::new();
        for i in 0..1000 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("hash_{:06}", i - 1)
            };
            headers.push(create_test_header(i, &prev_hash, &format!("hash_{:06}", i)));
        }

        let inserted = storage.insert_headers_batch(&headers).await.unwrap();
        assert_eq!(inserted, 1000);

        // Update chain tip
        let tip = storage
            .update_chain_tip_to_highest()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tip.height, 999);

        // Verify some lookups
        let h500 = storage.find_header_for_height(500).await.unwrap().unwrap();
        assert_eq!(h500.height, 500);
    }

    #[tokio::test]
    async fn test_headers_bytes_multiple() {
        let storage = create_test_storage().await;

        // Insert a chain
        for i in 0..3 {
            let prev_hash = if i == 0 {
                "0".repeat(64)
            } else {
                format!("{:064x}", i - 1)
            };
            let mut header = create_test_header(i, &prev_hash, &format!("{:064x}", i));
            header.merkle_root = format!("{:064x}", i + 100);
            storage.insert_header(header).await.unwrap();
        }

        let bytes = storage.get_headers_bytes(0, 3).await.unwrap();
        assert_eq!(bytes.len(), 240); // 3 * 80 bytes
    }

    #[tokio::test]
    async fn test_destroy() {
        let storage = create_test_storage().await;

        let header = create_test_header(0, &"0".repeat(64), "hash_0");
        storage.insert_header(header).await.unwrap();

        assert_eq!(storage.header_count().await.unwrap(), 1);

        storage.destroy().await.unwrap();

        assert_eq!(storage.header_count().await.unwrap(), 0);
    }
}
