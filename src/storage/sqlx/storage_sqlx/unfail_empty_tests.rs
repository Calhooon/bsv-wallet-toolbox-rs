#[cfg(test)]
mod tests {
    use super::super::{MonitorStorage, StorageSqlx, WalletStorageWriter};

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
}
