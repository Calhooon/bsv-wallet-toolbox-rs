use super::{
    sequence_number_from_row, AbortActionArgs, AuthId, Duration, Error, ListActionsArgs,
    ListActionsResult, Result, Row, StorageSqlx, Utc, WalletStorageWriter,
};

impl StorageSqlx {
    /// Get a raw transaction, preferring proven_tx but falling back to transactions table.
    pub async fn get_proven_or_raw_tx(&self, txid: &str) -> Result<Option<Vec<u8>>> {
        // First try proven_txs
        let proven: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT raw_tx FROM proven_txs WHERE txid = ? AND raw_tx IS NOT NULL")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;

        if let Some((raw_tx,)) = proven {
            return Ok(Some(raw_tx));
        }

        // Fall back to transactions table
        let tx: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;

        Ok(tx.map(|(raw_tx,)| raw_tx))
    }
}

impl StorageSqlx {
    pub(super) async fn list_actions_impl(
        &self,
        auth: &AuthId,
        args: ListActionsArgs,
    ) -> Result<ListActionsResult> {
        use bsv_rs::wallet::{
            ActionStatus, Outpoint, QueryMode, WalletAction, WalletActionInput, WalletActionOutput,
        };

        let user_id = auth.user_id.ok_or(Error::AuthenticationRequired)?;

        let limit = args.limit.unwrap_or(10).min(10000);
        let offset = args.offset.unwrap_or(0);
        let label_query_mode = args.label_query_mode.unwrap_or(QueryMode::Any);
        let include_labels = args.include_labels.unwrap_or(false);
        let include_inputs = args.include_inputs.unwrap_or(false);
        let include_outputs = args.include_outputs.unwrap_or(false);
        let include_input_source_locking_scripts =
            args.include_input_source_locking_scripts.unwrap_or(false);
        let include_input_unlocking_scripts = args.include_input_unlocking_scripts.unwrap_or(false);
        let include_output_locking_scripts = args.include_output_locking_scripts.unwrap_or(false);

        // Valid transaction statuses to include
        let valid_statuses = vec![
            "completed",
            "unprocessed",
            "sending",
            "unproven",
            "unsigned",
            "nosend",
            "nonfinal",
        ];

        // If labels are provided, look up their IDs
        let mut label_ids: Vec<i64> = Vec::new();
        if !args.labels.is_empty() {
            let placeholders: Vec<&str> = args.labels.iter().map(|_| "?").collect();
            let sql = format!(
                r#"
                SELECT tx_label_id FROM tx_labels
                WHERE user_id = ? AND is_deleted = 0 AND label IN ({})
                "#,
                placeholders.join(",")
            );

            let mut query = sqlx::query(&sql).bind(user_id);
            for label in &args.labels {
                query = query.bind(label);
            }

            let rows = query.fetch_all(&self.reader).await?;
            label_ids = rows
                .iter()
                .map(|r| r.get::<i64, _>("tx_label_id"))
                .collect();

            // If using 'all' mode and not all labels exist, return empty
            if label_query_mode == QueryMode::All && label_ids.len() < args.labels.len() {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }

            // If using 'any' mode and no labels exist, return empty
            if label_query_mode == QueryMode::Any && label_ids.is_empty() && !args.labels.is_empty()
            {
                return Ok(ListActionsResult {
                    total_actions: 0,
                    actions: vec![],
                });
            }
        }

        // Build the main query
        let (transactions, total_count): (Vec<_>, u32) = if label_ids.is_empty() {
            // No label filtering - simple query
            let status_placeholders: Vec<&str> = valid_statuses.iter().map(|_| "?").collect();
            let sql = format!(
                r#"
                SELECT transaction_id, txid, satoshis, status, is_outgoing, description, version, lock_time
                FROM transactions
                WHERE user_id = ? AND status IN ({})
                ORDER BY transaction_id ASC
                LIMIT ? OFFSET ?
                "#,
                status_placeholders.join(",")
            );

            let mut query = sqlx::query(&sql).bind(user_id);
            for status in &valid_statuses {
                query = query.bind(*status);
            }
            query = query.bind(limit as i32).bind(offset as i32);

            let rows = query.fetch_all(&self.reader).await?;

            // Get total count
            let count_sql = format!(
                r#"
                SELECT COUNT(*) as total FROM transactions
                WHERE user_id = ? AND status IN ({})
                "#,
                status_placeholders.join(",")
            );
            let mut count_query = sqlx::query(&count_sql).bind(user_id);
            for status in &valid_statuses {
                count_query = count_query.bind(*status);
            }
            let count_row = count_query.fetch_one(&self.reader).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        } else {
            // Label filtering with CTE
            let status_placeholders: String = valid_statuses
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(",");
            let label_id_list: String = label_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let required_count = if label_query_mode == QueryMode::All {
                label_ids.len()
            } else {
                1
            };

            let sql = format!(
                r#"
                WITH txs_with_labels AS (
                    SELECT t.transaction_id, t.txid, t.satoshis, t.status, t.is_outgoing,
                           t.description, t.version, t.lock_time,
                           (SELECT COUNT(*) FROM tx_labels_map m
                            WHERE m.transaction_id = t.transaction_id
                            AND m.tx_label_id IN ({})
                            AND m.is_deleted = 0) as label_count
                    FROM transactions t
                    WHERE t.user_id = ? AND t.status IN ({})
                )
                SELECT * FROM txs_with_labels WHERE label_count >= ?
                ORDER BY transaction_id ASC
                LIMIT ? OFFSET ?
                "#,
                label_id_list, status_placeholders
            );

            let query = sqlx::query(&sql)
                .bind(user_id)
                .bind(required_count as i32)
                .bind(limit as i32)
                .bind(offset as i32);

            let rows = query.fetch_all(&self.reader).await?;

            // Get total count
            let count_sql = format!(
                r#"
                WITH txs_with_labels AS (
                    SELECT t.transaction_id,
                           (SELECT COUNT(*) FROM tx_labels_map m
                            WHERE m.transaction_id = t.transaction_id
                            AND m.tx_label_id IN ({})
                            AND m.is_deleted = 0) as label_count
                    FROM transactions t
                    WHERE t.user_id = ? AND t.status IN ({})
                )
                SELECT COUNT(*) as total FROM txs_with_labels WHERE label_count >= ?
                "#,
                label_id_list, status_placeholders
            );
            let count_query = sqlx::query(&count_sql)
                .bind(user_id)
                .bind(required_count as i32);
            let count_row = count_query.fetch_one(&self.reader).await?;
            let total: i64 = count_row.get("total");

            (rows, total as u32)
        };

        // Convert rows to WalletAction
        let mut actions: Vec<WalletAction> = Vec::new();
        for row in &transactions {
            let transaction_id: i64 = row.get("transaction_id");
            let txid_str: Option<String> = row.get("txid");
            let satoshis: i64 = row.get("satoshis");
            let status_str: String = row.get("status");
            let is_outgoing: i32 = row.get("is_outgoing");
            let description: String = row.get("description");
            let version: Option<i32> = row.get("version");
            let lock_time: Option<i64> = row.get("lock_time");

            // Parse txid to [u8; 32]
            let txid: [u8; 32] = if let Some(ref txid_hex) = txid_str {
                let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                let mut arr = [0u8; 32];
                if bytes.len() == 32 {
                    arr.copy_from_slice(&bytes);
                }
                arr
            } else {
                [0u8; 32]
            };

            // Parse status
            let status = match status_str.as_str() {
                "completed" => ActionStatus::Completed,
                "unprocessed" => ActionStatus::Unprocessed,
                "sending" => ActionStatus::Sending,
                "unproven" => ActionStatus::Unproven,
                "unsigned" => ActionStatus::Unsigned,
                "nosend" => ActionStatus::NoSend,
                "nonfinal" => ActionStatus::NonFinal,
                "failed" => ActionStatus::Failed,
                _ => ActionStatus::Unprocessed,
            };

            let mut action = WalletAction {
                txid,
                satoshis, // i64, can be negative for outgoing
                status,
                is_outgoing: is_outgoing != 0,
                description,
                labels: None,
                version: version.unwrap_or(1) as u32,
                lock_time: lock_time.unwrap_or(0) as u32,
                inputs: None,
                outputs: None,
            };

            // Fetch labels if requested
            if include_labels {
                let labels_sql = r#"
                    SELECT l.label FROM tx_labels l
                    JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id
                    WHERE m.transaction_id = ? AND m.is_deleted = 0 AND l.is_deleted = 0
                "#;
                let label_rows = sqlx::query(labels_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.reader)
                    .await?;
                let labels: Vec<String> = label_rows.iter().map(|r| r.get("label")).collect();
                action.labels = Some(labels);
            }

            // Fetch outputs if requested
            if include_outputs {
                let outputs_sql = r#"
                    SELECT o.output_id, o.vout, o.satoshis, o.spendable, o.locking_script,
                           o.custom_instructions, o.output_description, ob.name as basket_name
                    FROM outputs o
                    LEFT JOIN output_baskets ob ON o.basket_id = ob.basket_id
                    WHERE o.transaction_id = ?
                    ORDER BY o.vout ASC
                "#;
                let output_rows = sqlx::query(outputs_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.reader)
                    .await?;

                let mut wallet_outputs: Vec<WalletActionOutput> = Vec::new();
                for o_row in output_rows {
                    let output_id: i64 = o_row.get("output_id");
                    let vout: i32 = o_row.get("vout");
                    let o_satoshis: i64 = o_row.get("satoshis");
                    let spendable: i32 = o_row.get("spendable");
                    let locking_script: Option<Vec<u8>> = if include_output_locking_scripts {
                        o_row.get("locking_script")
                    } else {
                        None
                    };
                    let custom_instructions: Option<String> = o_row.get("custom_instructions");
                    let output_description: Option<String> = o_row.get("output_description");
                    let basket_name: Option<String> = o_row.get("basket_name");

                    // Get tags for this output
                    let tags_sql = r#"
                        SELECT t.tag FROM output_tags t
                        JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id
                        WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0
                    "#;
                    let tag_rows = sqlx::query(tags_sql)
                        .bind(output_id)
                        .fetch_all(&self.reader)
                        .await?;
                    let tags: Vec<String> = tag_rows.iter().map(|r| r.get("tag")).collect();

                    wallet_outputs.push(WalletActionOutput {
                        satoshis: o_satoshis as u64,
                        locking_script,
                        spendable: spendable != 0,
                        custom_instructions,
                        tags,
                        output_index: vout as u32,
                        output_description: output_description.unwrap_or_default(),
                        basket: basket_name.unwrap_or_default(),
                    });
                }
                action.outputs = Some(wallet_outputs);
            }

            // Fetch inputs if requested
            if include_inputs {
                // Inputs are outputs from other transactions spent by this transaction
                let inputs_sql = r#"
                    SELECT o.txid, o.vout, o.satoshis, o.locking_script, o.output_description, o.sequence_number
                    FROM outputs o
                    WHERE o.spent_by = ?
                    ORDER BY o.sequence_number ASC
                "#;
                let input_rows = sqlx::query(inputs_sql)
                    .bind(transaction_id)
                    .fetch_all(&self.reader)
                    .await?;

                let mut wallet_inputs: Vec<WalletActionInput> = Vec::new();

                // If we need unlocking scripts, we need to parse the raw transaction
                let _raw_tx: Option<Vec<u8>> = if include_input_unlocking_scripts {
                    let tx_row =
                        sqlx::query("SELECT raw_tx FROM transactions WHERE transaction_id = ?")
                            .bind(transaction_id)
                            .fetch_optional(&self.reader)
                            .await?;
                    tx_row.and_then(|r| r.get("raw_tx"))
                } else {
                    None
                };

                for i_row in input_rows.iter() {
                    let source_txid: Option<String> = i_row.get("txid");
                    let source_vout: i32 = i_row.get("vout");
                    let source_satoshis: i64 = i_row.get("satoshis");
                    let source_locking_script: Option<Vec<u8>> =
                        if include_input_source_locking_scripts {
                            i_row.get("locking_script")
                        } else {
                            None
                        };
                    let input_description: Option<String> = i_row.get("output_description");
                    let sequence_number = sequence_number_from_row(i_row)?;

                    // Parse source txid
                    let source_txid_bytes: [u8; 32] = if let Some(ref txid_hex) = source_txid {
                        let bytes = hex::decode(txid_hex).unwrap_or_else(|_| vec![0u8; 32]);
                        let mut arr = [0u8; 32];
                        if bytes.len() == 32 {
                            arr.copy_from_slice(&bytes);
                        }
                        arr
                    } else {
                        [0u8; 32]
                    };

                    // Get unlocking script from raw tx if available
                    // Note: Full implementation would parse the raw transaction
                    let unlocking_script: Option<Vec<u8>> = None;

                    wallet_inputs.push(WalletActionInput {
                        source_outpoint: Outpoint {
                            txid: source_txid_bytes,
                            vout: source_vout as u32,
                        },
                        source_satoshis: source_satoshis as u64,
                        source_locking_script,
                        unlocking_script,
                        input_description: input_description.unwrap_or_default(),
                        sequence_number: sequence_number.unwrap_or(u32::MAX),
                    });
                }
                action.inputs = Some(wallet_inputs);
            }

            actions.push(action);
        }

        // Calculate total based on whether we hit the limit
        let total_actions = if (actions.len() as u32) < limit {
            offset + actions.len() as u32
        } else {
            total_count
        };

        Ok(ListActionsResult {
            total_actions,
            actions,
        })
    }

    pub(super) async fn abort_abandoned_impl(&self, timeout: Duration) -> Result<()> {
        // Calculate cutoff time
        let cutoff = Utc::now() - chrono::Duration::from_std(timeout).unwrap_or_default();

        // Find abandoned transactions (unsigned or unprocessed older than cutoff)
        // Note: This is a cross-user admin operation which requires special handling.
        //
        // Full implementation would:
        // 1. Query all transactions with status 'unsigned' or 'unprocessed'
        //    where created_at < cutoff
        // 2. For each, call abort_action to release locked UTXOs
        //
        // For now, log the intent and return success.
        tracing::debug!(
            "abort_abandoned: checking for transactions older than {:?}",
            cutoff
        );

        // Query transactions older than cutoff in standard abortable statuses.
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            r#"
            SELECT transaction_id, user_id, reference
            FROM transactions
            WHERE status IN ('unsigned', 'unprocessed')
              AND is_outgoing = 1
              AND created_at < ?
            "#,
        )
        .bind(cutoff)
        .fetch_all(self.pool())
        .await?;

        // Abort each abandoned transaction via normal abort_action path
        for (tx_id, user_id, reference) in &rows {
            let auth = AuthId::with_user_id("admin", *user_id);
            let args = AbortActionArgs {
                reference: reference.clone(),
            };

            match self.abort_action(&auth, args).await {
                Ok(_) => {
                    tracing::debug!("abort_abandoned: aborted transaction {}", tx_id);
                }
                Err(e) => {
                    tracing::warn!(
                        "abort_abandoned: failed to abort transaction {}: {}",
                        tx_id,
                        e
                    );
                }
            }
        }

        // A `sending` transaction is not aged out here. It was handed to the
        // broadcasters, so a broadcaster may hold it and it may mine; its
        // words are hints that `send_waiting_transactions` records and
        // re-asks on the cadence, and the one retire is the host's explicit
        // act (`StorageSqlx::retire_undeliverable_txid`). Until 0.7.3 this
        // sweep failed every `sending` row past the timeout, released its
        // inputs on a hint and wrote off its change (bsv-stack-lean #65;
        // the reference ages out only `unprocessed` and `unsigned`,
        // wallet-toolbox src/monitor/tasks/TaskFailAbandoned.ts:41).
        if !rows.is_empty() {
            tracing::info!(
                "abort_abandoned: processed {} abandoned transactions",
                rows.len()
            );
        }

        Ok(())
    }
}
