#[cfg(test)]
mod tests {
    use super::super::*;

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
        .execute(storage.writer())
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
        .execute(storage.writer())
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
        .execute(storage.writer())
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO certificates (user_id, type, serial_number, certifier, subject, revocation_outpoint, signature, is_deleted, created_at, updated_at)
            VALUES (?, 'type_b', 'serial_b', 'certifier_2', 'subject_2', 'outpoint_b', 'sig_b', 0, datetime('now'), datetime('now'))
            "#,
        )
        .bind(user.user_id)
        .execute(storage.writer())
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
}
