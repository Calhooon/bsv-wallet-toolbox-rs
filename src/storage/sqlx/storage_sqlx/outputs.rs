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
                   o.type, o.provided_by, o.purpose, o.output_description, o.spent_by,
                   o.sequence_number, o.spending_description, o.spendable, o.change,
                   o.derivation_prefix, o.derivation_suffix,
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
            let mut output = output_from_row(&row)?;
            if args.no_script.unwrap_or(false) {
                output.locking_script = None;
            }
            outputs.push(output);
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
        Ok(self
            .find_output_row_by_outpoint(user_id, txid, vout)
            .await?
            .as_ref()
            .map(output_from_row)
            .transpose()?)
    }

    /// Keep nullable columns intact for stored-input validation before mapping.
    pub(crate) async fn find_output_row_by_outpoint(
        &self,
        user_id: i64,
        txid: &str,
        vout: u32,
    ) -> Result<Option<sqlx::sqlite::SqliteRow>> {
        let row = sqlx::query(
            r#"
            SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                   satoshis, locking_script, script_length, script_offset,
                   type, provided_by, purpose, output_description, spent_by, sequence_number,
                   spending_description, spendable, change, derivation_prefix, derivation_suffix,
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

        Ok(row)
    }
}

impl StorageSqlx {
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

/// One `outputs` row as a `TableOutput`, every column the SELECT lists; the
/// shared shape of the readers that fetch whole rows. Missing or invalid columns
/// return a SQLx error; SQL NULL stays absent for optional fields.
pub(crate) fn output_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> std::result::Result<TableOutput, sqlx::Error> {
    Ok(TableOutput {
        output_id: row.try_get("output_id")?,
        user_id: row.try_get("user_id")?,
        transaction_id: row.try_get("transaction_id")?,
        basket_id: row.try_get("basket_id")?,
        txid: row.try_get("txid")?,
        vout: row.try_get("vout")?,
        satoshis: row.try_get("satoshis")?,
        locking_script: row.try_get("locking_script")?,
        script_length: row.try_get("script_length")?,
        script_offset: row.try_get("script_offset")?,
        output_type: row.try_get("type")?,
        provided_by: row.try_get("provided_by")?,
        purpose: row.try_get("purpose")?,
        output_description: row.try_get("output_description")?,
        spent_by: row.try_get("spent_by")?,
        sequence_number: sequence_number_from_row(row)?,
        spending_description: row.try_get("spending_description")?,
        spendable: row.try_get("spendable")?,
        change: row.try_get("change")?,
        derivation_prefix: row.try_get("derivation_prefix")?,
        derivation_suffix: row.try_get("derivation_suffix")?,
        sender_identity_key: row.try_get("sender_identity_key")?,
        custom_instructions: row.try_get("custom_instructions")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The `sequence_number` column as the u32 it holds, NULL as `None`. Writers
/// store the full u32 (the sync INSERT binds it), so the column is decoded as
/// an i64; a negative i32 that an earlier i32 writer left reads as the same
/// bits (-1 is 0xFFFFFFFF). A value outside both ranges is a decode error.
pub(crate) fn sequence_number_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> std::result::Result<Option<u32>, sqlx::Error> {
    let Some(stored) = row.try_get::<Option<i64>, _>("sequence_number")? else {
        return Ok(None);
    };
    u32::try_from(stored)
        .or_else(|_| i32::try_from(stored).map(|legacy| legacy as u32))
        .map(Some)
        .map_err(|source| sqlx::Error::ColumnDecode {
            index: "\"sequence_number\"".into(),
            source: Box::new(source),
        })
}

#[cfg(test)]
mod tests {
    use super::super::{
        AuthId, FindOutputsArgs, ListOutputsArgs, StorageSqlx, TableOutput, WalletStorageReader,
        WalletStorageWriter,
    };
    use bsv_rs::wallet::OutputInclude;
    use sqlx::{Column, Row};

    async fn stored_received_output() -> (StorageSqlx, AuthId, String, i64) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("output-origin", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let identity = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
        let auth = AuthId::with_user_id(identity, user.user_id);
        let txid = "1".repeat(64);
        let transaction_id = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, description, txid)
             VALUES (?, 'completed', 'output-origin', 0, 'Received payment', ?)",
        )
        .bind(user.user_id)
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap()
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, provided_by, purpose,
                                  type, spendable, txid, script_length, script_offset,
                                  locking_script, custom_instructions)
             VALUES (?, ?, 0, 42, 'storage', 'receive', 'P2PKH', 1, ?, 1, 0, X'51', 'Keep origin')",
        )
        .bind(user.user_id)
        .bind(transaction_id)
        .bind(&txid)
        .execute(storage.writer())
        .await
        .unwrap();
        (storage, auth, txid, transaction_id)
    }

    fn assert_received_origin(output: &TableOutput) {
        assert_eq!(
            (output.purpose.as_deref(), output.provided_by.as_str()),
            (Some("receive"), "storage")
        );
    }

    #[tokio::test]
    async fn test_output_decoder_rejects_missing_columns() {
        let (storage, auth, txid, _) = stored_received_output().await;
        let full_row = storage
            .find_output_row_by_outpoint(auth.user_id.unwrap(), &txid, 0)
            .await
            .unwrap()
            .unwrap();
        let columns: Vec<_> = full_row.columns().iter().map(|c| c.name()).collect();
        for missing in &columns {
            let selected = columns
                .iter()
                .filter(|name| *name != missing)
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let row = sqlx::query(&format!("SELECT {selected} FROM outputs WHERE txid = ?"))
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            let error = super::output_from_row(&row).unwrap_err();
            assert!(
                matches!(&error, sqlx::Error::ColumnNotFound(name) if name.as_str() == *missing),
                "omitted {missing}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_find_outputs_preserves_output_origin() {
        let (storage, auth, _, _) = stored_received_output().await;
        for no_script in [None, Some(false), Some(true)] {
            let args = FindOutputsArgs {
                no_script,
                ..Default::default()
            };
            let direct = storage
                .find_outputs_internal(auth.user_id.unwrap(), &args)
                .await
                .unwrap();
            let authenticated = storage.find_outputs(&auth, args).await.unwrap();
            for outputs in [direct, authenticated] {
                assert_eq!(outputs.len(), 1);
                let output = &outputs[0];
                assert_received_origin(output);
                assert_eq!(output.output_description, None);
                assert_eq!(output.spent_by, None);
                assert_eq!(output.sequence_number, None);
                assert_eq!(output.spending_description, None);
                assert_eq!(
                    output.locking_script,
                    (no_script != Some(true)).then(|| vec![0x51])
                );
                let wire = serde_json::to_value(output).unwrap();
                assert_eq!(wire["purpose"], "receive");
                assert_eq!(wire["providedBy"], "storage");
            }
        }
    }

    #[tokio::test]
    async fn test_find_output_by_outpoint_preserves_output_origin() {
        let (storage, auth, txid, _) = stored_received_output().await;
        let output = storage
            .find_output_by_outpoint(auth.user_id.unwrap(), &txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert_received_origin(&output);
        assert_eq!(output.locking_script, Some(vec![0x51]));
    }

    #[tokio::test]
    async fn test_output_finders_preserve_optional_metadata() {
        let (storage, auth, txid, transaction_id) = stored_received_output().await;
        sqlx::query(
            "UPDATE outputs SET output_description = 'Received payment', spent_by = ?,
             sequence_number = -1, spending_description = 'Spent payment', spendable = 0",
        )
        .bind(transaction_id)
        .execute(storage.writer())
        .await
        .unwrap();
        let mut outputs = storage
            .find_outputs(&auth, FindOutputsArgs::default())
            .await
            .unwrap();
        outputs.push(
            storage
                .find_output_by_outpoint(auth.user_id.unwrap(), &txid, 0)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(outputs.len(), 2);
        for output in outputs {
            assert_eq!(
                output.output_description.as_deref(),
                Some("Received payment")
            );
            assert_eq!(output.spent_by, Some(transaction_id));
            assert_eq!(output.sequence_number, Some(u32::MAX));
            assert_eq!(
                output.spending_description.as_deref(),
                Some("Spent payment")
            );
        }
    }

    #[tokio::test]
    async fn test_list_outputs_preserves_wallet_output_shape() {
        let (storage, auth, txid, _) = stored_received_output().await;
        let result = storage
            .list_outputs(
                &auth,
                ListOutputsArgs {
                    basket: String::new(),
                    tags: None,
                    tag_query_mode: None,
                    include: Some(OutputInclude::LockingScripts),
                    include_custom_instructions: Some(true),
                    include_tags: None,
                    include_labels: None,
                    limit: None,
                    offset: None,
                    seek_permission: None,
                },
            )
            .await
            .unwrap();
        let wire = serde_json::to_value(result).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "totalOutputs": 1,
                "outputs": [{
                    "satoshis": 42,
                    "lockingScript": "51",
                    "spendable": true,
                    "customInstructions": "Keep origin",
                    "outpoint": format!("{txid}.0")
                }]
            })
        );
    }

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
