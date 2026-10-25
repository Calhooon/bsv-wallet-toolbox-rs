#[cfg(test)]
mod tests {
    use super::super::{StorageSqlx, WalletStorageWriter};

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
