#[cfg(test)]
use super::StoreBusyReason;

use super::{
    commit, AuthId, Error, FindCertificatesArgs, ListCertificatesArgs, ListCertificatesResult,
    RelinquishCertificateArgs, Result, Row, StorageSqlx, TableCertificate, TableCertificateField,
    Utc,
};

impl StorageSqlx {
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

        let rows = query.fetch_all(&self.reader).await?;

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
}

impl StorageSqlx {
    pub(super) async fn list_certificates_impl(
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

        let rows = query.fetch_all(&self.reader).await?;

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

        let count_row = count_query.fetch_one(&self.reader).await?;
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
                .fetch_all(&self.reader)
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

    pub(super) async fn insert_certificate_impl(
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
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub(super) async fn insert_certificate_field_impl(
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
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub(super) async fn insert_certificate_with_fields_impl(
        &self,
        auth: &AuthId,
        certificate: TableCertificate,
        fields: Vec<TableCertificateField>,
    ) -> Result<i64> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;
        if certificate.user_id != user_id {
            return Err(Error::AccessDenied(
                "Certificate user_id does not match auth".to_string(),
            ));
        }
        if fields.iter().any(|f| f.user_id != user_id) {
            return Err(Error::AccessDenied(
                "Certificate field user_id does not match auth".to_string(),
            ));
        }

        let now = Utc::now();

        // One write transaction: the certificate and every field, or none.
        let mut tx = self.write_tx().await?;
        let certificate_id = sqlx::query(
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
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();

        for field in &fields {
            #[cfg(test)]
            if self.take_busy_next_certificate_field() {
                return Err(Error::StoreBusy {
                    reason: StoreBusyReason::SqliteBusy { code: 5 },
                });
            }
            sqlx::query(
                r#"
                INSERT INTO certificate_fields (certificate_id, user_id, field_name, field_value, master_key, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(certificate_id)
            .bind(user_id)
            .bind(&field.field_name)
            .bind(&field.field_value)
            .bind(&field.master_key)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        commit(tx).await?;

        Ok(certificate_id)
    }

    pub(super) async fn relinquish_certificate_impl(
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
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.rows_affected() as i64)
    }
}
