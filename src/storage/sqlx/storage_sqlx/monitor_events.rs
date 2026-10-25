use super::{Duration, Error, Result, Row, StorageSqlx, TableMonitorEvent, Utc};

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

#[cfg(test)]
mod tests {
    use super::super::*;

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
}
