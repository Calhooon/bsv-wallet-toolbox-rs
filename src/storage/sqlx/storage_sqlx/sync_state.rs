use super::{
    async_trait, AuthId, Error, ProcessSyncChunkResult, RequestSyncChunkArgs, Result, Row,
    StorageSqlx, SyncChunk, SyncMap, TableSyncState, Utc, WalletStorageSync,
};

impl StorageSqlx {
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
        .fetch_optional(&self.reader)
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
        .execute(&mut *self.writer_conn().await?)
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
        super::sync::process_sync_chunk_internal(
            self,
            args,
            chunk,
            &mut crate::storage::SyncMap::default(),
        )
        .await
    }

    async fn process_sync_chunk_mapped(
        &self,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
        sync_map: &mut crate::storage::SyncMap,
    ) -> Result<ProcessSyncChunkResult> {
        super::sync::process_sync_chunk_internal(self, args, chunk, sync_map).await
    }
}
