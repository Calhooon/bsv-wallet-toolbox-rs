#[cfg(test)]
mod tests {
    use super::super::{StorageSqlx, WalletStorageWriter};

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
}
