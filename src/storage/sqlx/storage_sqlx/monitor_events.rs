use super::{Error, Result, Row, StorageSqlx, TableMonitorEvent};

impl StorageSqlx {
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
        .execute(&mut *self.writer_conn().await?)
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
            .fetch_all(&self.reader)
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
            .fetch_all(&self.reader)
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
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.rows_affected())
    }
}
