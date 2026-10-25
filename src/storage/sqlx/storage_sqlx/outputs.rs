use super::{
    AuthId, Error, FindOutputsArgs, RelinquishOutputArgs, Result, Row, StorageSqlx, TableOutput,
    Utc,
};

impl StorageSqlx {
    // =========================================================================
    // Output Repository
    // =========================================================================

    /// Find outputs matching the given criteria.
    pub async fn find_outputs_internal(
        &self,
        user_id: i64,
        args: &FindOutputsArgs,
    ) -> Result<Vec<TableOutput>> {
        let mut sql = String::from(
            r#"
            SELECT o.output_id, o.user_id, o.transaction_id, o.basket_id, o.txid, o.vout,
                   o.satoshis, o.locking_script, o.script_length, o.script_offset,
                   o.type, o.spendable, o.change, o.derivation_prefix, o.derivation_suffix,
                   o.sender_identity_key, o.custom_instructions, o.created_at, o.updated_at
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.user_id = ?
            "#,
        );

        let mut binds: Vec<String> = vec![user_id.to_string()];

        if let Some(basket_id) = args.basket_id {
            sql.push_str(" AND o.basket_id = ?");
            binds.push(basket_id.to_string());
        }

        if let Some(txid) = &args.txid {
            sql.push_str(" AND o.txid = ?");
            binds.push(txid.clone());
        }

        if let Some(vout) = args.vout {
            sql.push_str(" AND o.vout = ?");
            binds.push(vout.to_string());
        }

        if let Some(statuses) = &args.tx_status {
            if !statuses.is_empty() {
                let placeholders: Vec<&str> = statuses.iter().map(|_| "?").collect();
                sql.push_str(&format!(" AND t.status IN ({})", placeholders.join(",")));
                for status in statuses {
                    binds.push(status.as_str().to_string());
                }
            }
        }

        if let Some(ref base) = args.base.since {
            sql.push_str(" AND o.updated_at > ?");
            binds.push(base.to_rfc3339());
        }

        // Order by updated_at
        if args.base.order_descending.unwrap_or(false) {
            sql.push_str(" ORDER BY o.updated_at DESC");
        } else {
            sql.push_str(" ORDER BY o.updated_at ASC");
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

        // Build the query dynamically - we need to use a different approach
        // since sqlx doesn't support dynamic binding easily
        let mut query = sqlx::query(&sql);

        // Bind user_id first
        query = query.bind(user_id);

        // Bind other parameters based on what was added
        if let Some(basket_id) = args.basket_id {
            query = query.bind(basket_id);
        }
        if let Some(ref txid) = args.txid {
            query = query.bind(txid);
        }
        if let Some(vout) = args.vout {
            query = query.bind(vout as i32);
        }
        if let Some(statuses) = &args.tx_status {
            for status in statuses {
                query = query.bind(status.as_str());
            }
        }
        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.reader).await?;

        let mut outputs = Vec::new();
        for row in rows {
            let locking_script: Option<Vec<u8>> = if args.no_script.unwrap_or(false) {
                None
            } else {
                row.get("locking_script")
            };

            outputs.push(TableOutput {
                output_id: row.get("output_id"),
                user_id: row.get("user_id"),
                transaction_id: row.get("transaction_id"),
                basket_id: row.get("basket_id"),
                txid: row.get("txid"),
                vout: row.get("vout"),
                satoshis: row.get("satoshis"),
                locking_script,
                script_length: row.get("script_length"),
                script_offset: row.get("script_offset"),
                output_type: row.get("type"),
                provided_by: row.try_get("provided_by").unwrap_or("you".to_string()),
                purpose: row.try_get("purpose").ok(),
                output_description: row.try_get("output_description").ok(),
                spent_by: row.try_get("spent_by").ok().flatten(),
                sequence_number: row
                    .try_get::<Option<i32>, _>("sequence_number")
                    .ok()
                    .flatten()
                    .map(|v| v as u32),
                spending_description: row.try_get("spending_description").ok(),
                spendable: row.get("spendable"),
                change: row.get("change"),
                derivation_prefix: row.get("derivation_prefix"),
                derivation_suffix: row.get("derivation_suffix"),
                sender_identity_key: row.get("sender_identity_key"),
                custom_instructions: row.get("custom_instructions"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(outputs)
    }

    /// Find output by txid and vout.
    pub async fn find_output_by_outpoint(
        &self,
        user_id: i64,
        txid: &str,
        vout: u32,
    ) -> Result<Option<TableOutput>> {
        let row = sqlx::query(
            r#"
            SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                   satoshis, locking_script, script_length, script_offset,
                   type, spendable, change, derivation_prefix, derivation_suffix,
                   sender_identity_key, custom_instructions, created_at, updated_at
            FROM outputs
            WHERE user_id = ? AND txid = ? AND vout = ?
            "#,
        )
        .bind(user_id)
        .bind(txid)
        .bind(vout as i32)
        .fetch_optional(&self.reader)
        .await?;

        match row {
            Some(row) => Ok(Some(TableOutput {
                output_id: row.get("output_id"),
                user_id: row.get("user_id"),
                transaction_id: row.get("transaction_id"),
                basket_id: row.get("basket_id"),
                txid: row.get("txid"),
                vout: row.get("vout"),
                satoshis: row.get("satoshis"),
                locking_script: row.get("locking_script"),
                script_length: row.get("script_length"),
                script_offset: row.get("script_offset"),
                output_type: row.get("type"),
                provided_by: row.try_get("provided_by").unwrap_or("you".to_string()),
                purpose: row.try_get("purpose").ok(),
                output_description: row.try_get("output_description").ok(),
                spent_by: row.try_get("spent_by").ok().flatten(),
                sequence_number: row
                    .try_get::<Option<i32>, _>("sequence_number")
                    .ok()
                    .flatten()
                    .map(|v| v as u32),
                spending_description: row.try_get("spending_description").ok(),
                spendable: row.get("spendable"),
                change: row.get("change"),
                derivation_prefix: row.get("derivation_prefix"),
                derivation_suffix: row.get("derivation_suffix"),
                sender_identity_key: row.get("sender_identity_key"),
                custom_instructions: row.get("custom_instructions"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    pub(super) async fn relinquish_output_impl(
        &self,
        auth: &AuthId,
        args: RelinquishOutputArgs,
    ) -> Result<i64> {
        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        let now = Utc::now();

        // Get txid (convert from [u8; 32] to hex string) and vout from the Outpoint struct
        let txid = hex::encode(args.output.txid);
        let vout: i32 = args.output.vout as i32;

        // Remove from basket by setting basket_id to NULL
        let result = sqlx::query(
            r#"
            UPDATE outputs
            SET basket_id = NULL, updated_at = ?
            WHERE user_id = ? AND txid = ? AND vout = ?
            "#,
        )
        .bind(now)
        .bind(user_id)
        .bind(&txid)
        .bind(vout)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(result.rows_affected() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[tokio::test]
    async fn test_find_outputs() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // Find outputs (should be empty)
        let outputs = storage
            .find_outputs(&auth, FindOutputsArgs::default())
            .await
            .unwrap();
        assert!(outputs.is_empty());
    }
}
