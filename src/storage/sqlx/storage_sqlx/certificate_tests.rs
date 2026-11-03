#[cfg(test)]
mod tests {
    use super::super::*;

    #[tokio::test]
    async fn relinquish_certificate_duplicate_matches_are_bad_request_and_unchanged() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("duplicate-certificate", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let identity = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
        let auth = AuthId::with_user_id(&identity, user.user_id);

        // The real schema forbids duplicates. This test-only copy removes its
        // unique constraint to exercise verify-one against inconsistent storage.
        sqlx::raw_sql(
            "CREATE TABLE non_unique_certificates AS SELECT * FROM certificates; \
             DROP TABLE certificates; \
             ALTER TABLE non_unique_certificates RENAME TO certificates;",
        )
        .execute(storage.writer())
        .await
        .unwrap();
        for id in [1, 2] {
            sqlx::query(
                "INSERT INTO certificates (certificate_id, user_id, type, serial_number, \
                 certifier, subject, revocation_outpoint, signature, is_deleted, created_at, updated_at) \
                 VALUES (?, ?, 'type', 'serial', 'certifier', 'subject', 'outpoint', 'signature', \
                 0, '2026-01-02 03:04:05', '2026-01-02 03:04:05')",
            )
            .bind(id)
            .bind(user.user_id)
            .execute(storage.writer())
            .await
            .unwrap();
        }
        let snapshot = "SELECT certificate_id, is_deleted, updated_at FROM certificates ORDER BY certificate_id";
        let before: Vec<(i64, i64, String)> = sqlx::query_as(snapshot)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(before.len(), 2);

        let error = storage
            .relinquish_certificate_impl(
                &auth,
                RelinquishCertificateArgs {
                    certificate_type: "type".into(),
                    serial_number: "serial".into(),
                    certifier: "certifier".into(),
                },
            )
            .await
            .expect_err("a non-unique certificate must be refused");
        assert!(matches!(
            &error,
            Error::SdkError(bsv_rs::Error::WalletRefusal {
                class: bsv_rs::WalletRefusalClass::BadRequest,
                message,
            }) if message == "Result must exist and be unique."
        ));
        assert_eq!(error.to_string(), "Result must exist and be unique.");
        let after: Vec<(i64, i64, String)> = sqlx::query_as(snapshot)
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert_eq!(after, before);
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

    /// The total's three branches (#686, `short_page_total`), BRC-100's
    /// total in each: a full page counts; a short page is the offset plus
    /// the page, where the reference's listCertificates.ts:67 answers the
    /// page alone (1 at offset 2 over three); an empty page past the start
    /// counts; an empty store is 0.
    #[tokio::test]
    async fn test_list_certificates_total_per_page_shape() {
        async fn total(storage: &StorageSqlx, auth: &AuthId, page: (u32, u32)) -> (usize, u32) {
            let args = ListCertificatesArgs {
                certifiers: vec![],
                types: vec![],
                limit: Some(page.1),
                offset: Some(page.0),
                privileged: None,
                privileged_reason: None,
            };
            let result = storage.list_certificates(auth, args).await.unwrap();
            (result.certificates.len(), result.total_certificates)
        }
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("totals", &"2".repeat(64)).await.unwrap();
        storage.make_available().await.unwrap();
        let identity_key = "c".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);
        assert_eq!(total(&storage, &auth, (0, 10)).await, (0, 0), "empty store");

        let insert = "INSERT INTO certificates (user_id, type, serial_number, certifier, \
             subject, revocation_outpoint, signature, is_deleted) \
             VALUES (?, 'type_t', ?, 'certifier_t', 'subject_t', 'outpoint_t', 'sig_t', 0)";
        for serial in ["s0", "s1", "s2"] {
            let row = sqlx::query(insert).bind(user.user_id).bind(serial);
            row.execute(storage.writer()).await.unwrap();
        }
        assert_eq!(total(&storage, &auth, (0, 2)).await, (2, 3), "full page");
        assert_eq!(total(&storage, &auth, (2, 10)).await, (1, 3), "short page");
        let past = total(&storage, &auth, (100, 10)).await;
        assert_eq!(past, (0, 3), "empty page past the end");
        assert_eq!(
            total(&storage, &auth, (0, 0)).await,
            (0, 3),
            "limit 0 answers the count, as both pins do"
        );
    }
}
