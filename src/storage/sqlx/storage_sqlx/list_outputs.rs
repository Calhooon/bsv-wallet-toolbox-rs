use super::{
    beef_bfs_walk, prune_beef_to_roots, AuthId, Beef, Error, HashSet, ListOutputsArgs,
    ListOutputsResult, Result, Row, StorageSqlx,
};

impl StorageSqlx {
    pub(super) async fn list_outputs_impl(
        &self,
        auth: &AuthId,
        args: ListOutputsArgs,
    ) -> Result<ListOutputsResult> {
        use bsv_rs::wallet::{Outpoint, OutputInclude, QueryMode, WalletOutput};

        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let order_by = if offset < 0 { "DESC" } else { "ASC" };
        let actual_offset = if offset < 0 {
            (-offset - 1) as u32
        } else {
            offset as u32
        };

        let tag_query_mode = args.tag_query_mode.unwrap_or(QueryMode::Any);
        let include_custom_instructions = args.include_custom_instructions.unwrap_or(false);
        let include_tags = args.include_tags.unwrap_or(false);
        let include_labels = args.include_labels.unwrap_or(false);
        let include_locking_scripts = args.include == Some(OutputInclude::LockingScripts);
        let include_transactions = args.include == Some(OutputInclude::EntireTransactions);

        // Find the basket ID
        let basket_id: Option<i64> = if !args.basket.is_empty() {
            let basket_row = sqlx::query(
                r#"
                SELECT basket_id FROM output_baskets
                WHERE user_id = ? AND name = ? AND is_deleted = 0
                "#,
            )
            .bind(user_id)
            .bind(&args.basket)
            .fetch_optional(&self.reader)
            .await?;

            match basket_row {
                Some(row) => Some(row.get("basket_id")),
                None => {
                    // Basket doesn't exist, return empty result
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }
            }
        } else {
            None
        };

        // If tags are provided, look up their IDs
        let mut tag_ids: Vec<i64> = Vec::new();
        if let Some(ref tags) = args.tags {
            if !tags.is_empty() {
                let placeholders: Vec<&str> = tags.iter().map(|_| "?").collect();
                let sql = format!(
                    r#"
                    SELECT output_tag_id FROM output_tags
                    WHERE user_id = ? AND is_deleted = 0 AND tag IN ({})
                    "#,
                    placeholders.join(",")
                );

                let mut query = sqlx::query(&sql).bind(user_id);
                for tag in tags {
                    query = query.bind(tag);
                }

                let rows = query.fetch_all(&self.reader).await?;
                tag_ids = rows
                    .iter()
                    .map(|r| r.get::<i64, _>("output_tag_id"))
                    .collect();

                // If using 'all' mode and not all tags exist, return empty
                if tag_query_mode == QueryMode::All && tag_ids.len() < tags.len() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }

                // If using 'any' mode and no tags exist, return empty
                if tag_query_mode == QueryMode::Any && tag_ids.is_empty() && !tags.is_empty() {
                    return Ok(ListOutputsResult {
                        total_outputs: 0,
                        beef: None,
                        outputs: vec![],
                    });
                }
            }
        }

        // Valid transaction statuses for outputs
        let valid_statuses = "'completed', 'unproven', 'nosend', 'sending'";

        // Build the main query
        let (output_rows, total_count): (Vec<_>, u32) = if tag_ids.is_empty() {
            // No tag filtering
            let mut sql = format!(
                r#"
                SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable,
                       o.locking_script, o.custom_instructions
                FROM outputs o
                JOIN transactions t ON o.transaction_id = t.transaction_id
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({})
                "#,
                valid_statuses
            );

            if let Some(bid) = basket_id {
                sql.push_str(&format!(" AND o.basket_id = {}", bid));
            }

            sql.push_str(&format!(
                " ORDER BY o.output_id {} LIMIT {} OFFSET {}",
                order_by, limit, actual_offset
            ));

            let query = sqlx::query(&sql).bind(user_id);
            let rows = query.fetch_all(&self.reader).await?;

            // Get total count
            let mut count_sql = format!(
                r#"
                SELECT COUNT(*) as total FROM outputs o
                JOIN transactions t ON o.transaction_id = t.transaction_id
                WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({})
                "#,
                valid_statuses
            );

            if let Some(bid) = basket_id {
                count_sql.push_str(&format!(" AND o.basket_id = {}", bid));
            }

            let count_row = sqlx::query(&count_sql)
                .bind(user_id)
                .fetch_one(&self.reader)
                .await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        } else {
            // Tag filtering with CTE
            let tag_id_list: String = tag_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let required_count = if tag_query_mode == QueryMode::All {
                tag_ids.len()
            } else {
                1
            };

            let basket_filter = if let Some(bid) = basket_id {
                format!(" AND o.basket_id = {}", bid)
            } else {
                String::new()
            };

            let sql = format!(
                r#"
                WITH outputs_with_tags AS (
                    SELECT o.output_id, o.transaction_id, o.txid, o.vout, o.satoshis, o.spendable,
                           o.locking_script, o.custom_instructions,
                           (SELECT COUNT(*) FROM output_tags_map m
                            WHERE m.output_id = o.output_id
                            AND m.output_tag_id IN ({})
                            AND m.is_deleted = 0) as tag_count
                    FROM outputs o
                    JOIN transactions t ON o.transaction_id = t.transaction_id
                    WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){}
                )
                SELECT * FROM outputs_with_tags WHERE tag_count >= ?
                ORDER BY output_id {} LIMIT ? OFFSET ?
                "#,
                tag_id_list, valid_statuses, basket_filter, order_by
            );

            let query = sqlx::query(&sql)
                .bind(user_id)
                .bind(required_count as i32)
                .bind(limit as i32)
                .bind(actual_offset as i32);

            let rows = query.fetch_all(&self.reader).await?;

            // Get total count
            let count_sql = format!(
                r#"
                WITH outputs_with_tags AS (
                    SELECT o.output_id,
                           (SELECT COUNT(*) FROM output_tags_map m
                            WHERE m.output_id = o.output_id
                            AND m.output_tag_id IN ({})
                            AND m.is_deleted = 0) as tag_count
                    FROM outputs o
                    JOIN transactions t ON o.transaction_id = t.transaction_id
                    WHERE o.user_id = ? AND o.spendable = 1 AND t.status IN ({}){}
                )
                SELECT COUNT(*) as total FROM outputs_with_tags WHERE tag_count >= ?
                "#,
                tag_id_list, valid_statuses, basket_filter
            );

            let count_query = sqlx::query(&count_sql)
                .bind(user_id)
                .bind(required_count as i32);
            let count_row = count_query.fetch_one(&self.reader).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        };

        // Convert rows to WalletOutput
        let mut outputs: Vec<WalletOutput> = Vec::new();
        let mut txids_for_beef: Vec<String> = Vec::new();

        for row in &output_rows {
            let output_id: i64 = row.get("output_id");
            let transaction_id: i64 = row.get("transaction_id");
            let txid_str: Option<String> = row.get("txid");
            let vout: i32 = row.get("vout");
            let satoshis: i64 = row.get("satoshis");
            let spendable: i32 = row.get("spendable");
            let locking_script: Option<Vec<u8>> = if include_locking_scripts {
                row.get("locking_script")
            } else {
                None
            };
            let custom_instructions: Option<String> = if include_custom_instructions {
                row.get("custom_instructions")
            } else {
                None
            };

            // Parse txid to [u8; 32]
            let txid: [u8; 32] = if let Some(ref txid_hex) = txid_str {
                let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                let mut arr = [0u8; 32];
                if bytes.len() == 32 {
                    arr.copy_from_slice(&bytes);
                }
                if include_transactions {
                    txids_for_beef.push(txid_hex.clone());
                }
                arr
            } else {
                [0u8; 32]
            };

            // Get tags if requested
            let tags: Option<Vec<String>> = if include_tags {
                let tags_sql = r#"
                    SELECT t.tag FROM output_tags t
                    JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id
                    WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0
                "#;
                let tag_rows = sqlx::query(tags_sql)
                    .bind(output_id)
                    .fetch_all(&self.reader)
                    .await?;
                Some(tag_rows.iter().map(|r| r.get("tag")).collect())
            } else {
                None
            };

            // Get labels if requested (from transaction)
            let labels: Option<Vec<String>> = if include_labels {
                let labels_sql = r#"
                    SELECT l.label FROM tx_labels l
                    JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id
                    WHERE m.transaction_id = ? AND m.is_deleted = 0 AND l.is_deleted = 0
                "#;
                let label_rows = sqlx::query(labels_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.reader)
                    .await?;
                Some(label_rows.iter().map(|r| r.get("label")).collect())
            } else {
                None
            };

            outputs.push(WalletOutput {
                satoshis: satoshis as u64,
                locking_script,
                spendable: spendable != 0,
                custom_instructions,
                tags,
                outpoint: Outpoint {
                    txid,
                    vout: vout as u32,
                },
                labels,
            });
        }

        // Build BEEF if requested
        // When include_transactions is true (OutputInclude::EntireTransactions),
        // we construct a BEEF containing each output's transaction along with
        // its merkle proof (if proven) and recursively include ancestor
        // transactions until we reach proven ancestors, matching the pattern
        // used in create_action's build_input_beef.
        let beef: Option<Vec<u8>> = if include_transactions && !txids_for_beef.is_empty() {
            // Deduplicate txids
            let unique_txids: Vec<String> = {
                let mut seen = HashSet::new();
                txids_for_beef
                    .into_iter()
                    .filter(|t| seen.insert(t.clone()))
                    .collect()
            };

            // Same walk create_action uses, so a proven ancestor terminates the
            // chain here too instead of dragging its own history along.
            let mut beef_struct = Beef::new();
            let mut processed_txids: HashSet<String> = HashSet::new();
            let mut pending_txids: Vec<(String, usize)> =
                unique_txids.iter().map(|t| (t.clone(), 0)).collect();

            let mut conn = self.reader.acquire().await?;

            beef_bfs_walk(
                &mut conn,
                Some(self.writer()),
                &mut beef_struct,
                &mut pending_txids,
                &mut processed_txids,
                Some(self),
                None,
            )
            .await?;

            prune_beef_to_roots(&mut beef_struct, &unique_txids);

            let beef_bytes = beef_struct.to_binary();
            // Only return BEEF if it contains data beyond the 4-byte header
            if beef_bytes.len() > 4 {
                Some(beef_bytes)
            } else {
                None
            }
        } else {
            None
        };

        // Calculate total based on whether we hit the limit
        let total_outputs = if (outputs.len() as u32) < limit {
            actual_offset + outputs.len() as u32
        } else {
            total_count
        };

        Ok(ListOutputsResult {
            total_outputs,
            beef,
            outputs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        AuthId, ListOutputsArgs, StorageSqlx, WalletStorageReader, WalletStorageWriter,
    };

    #[tokio::test]
    async fn test_list_outputs_empty() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List outputs from default basket (should be empty)
        let result = storage
            .list_outputs(
                &auth,
                ListOutputsArgs {
                    basket: "default".to_string(),
                    tags: None,
                    tag_query_mode: None,
                    include: None,
                    include_custom_instructions: None,
                    include_tags: None,
                    include_labels: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_outputs, 0);
        assert!(result.outputs.is_empty());
    }

    #[tokio::test]
    async fn test_list_outputs_nonexistent_basket() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", "0".repeat(64).as_str())
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        // List outputs from non-existent basket
        let result = storage
            .list_outputs(
                &auth,
                ListOutputsArgs {
                    basket: "nonexistent_basket".to_string(),
                    tags: None,
                    tag_query_mode: None,
                    include: None,
                    include_custom_instructions: None,
                    include_tags: None,
                    include_labels: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.total_outputs, 0);
        assert!(result.outputs.is_empty());
    }
}
