use super::{Result, Row, StorageSqlx, TableOutput};
use crate::storage::sqlx::create_action::MANAGED_CHANGE_CLAUSE;

impl StorageSqlx {
    /// Find a spendable change output suitable for use as input.
    ///
    /// Looks for wallet-managed change in the specified basket: spendable,
    /// `P2PKH`, `provided_by = 'storage'`, purpose `change` or `receive`,
    /// with a derivation prefix and suffix, its parent `completed`,
    /// `unproven`, or `sending` unless `exclude_sending` (the reference's
    /// filters, #295).
    ///
    /// The claim is the guarded `UPDATE` (`spendable = 1 AND spent_by IS
    /// NULL`), not the read: two callers never allocate the same coin. A
    /// candidate another caller took between the read and the claim is
    /// skipped and the next one is read.
    ///
    /// # Arguments
    /// * `user_id` - The user who owns the outputs
    /// * `basket_id` - The basket to search in
    /// * `_target_satoshis` - Target satoshi amount (for future optimization)
    /// * `_exclude_sending` - Whether to exclude outputs from sending transactions
    /// * `_transaction_id` - The transaction that will use this input
    pub async fn allocate_change_input(
        &self,
        user_id: i64,
        basket_id: i64,
        target_satoshis: i64,
        exclude_sending: bool,
        transaction_id: i64,
    ) -> Result<Option<TableOutput>> {
        // Strategy: select the smallest output >= target_satoshis to minimize change,
        // or the largest output if none meet the target. The filters are the
        // reference's (#295): its managed-change WHERE and its parent statuses
        // (`StorageKnex.allocateChangeInput` 2230-2231, 2241-2250, 2256).
        let statuses = if exclude_sending {
            "('completed', 'unproven')"
        } else {
            "('completed', 'unproven', 'sending')"
        };
        let base_query = format!(
            r#"
            SELECT o.output_id, o.satoshis, o.vout, o.txid, o.locking_script
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.user_id = ?
              AND o.basket_id = ?
              AND o.change = 1
              AND o.spendable = 1
              AND o.spent_by IS NULL
              AND o.transaction_id != ?
              AND t.status IN {statuses}
              {MANAGED_CHANGE_CLAUSE}
            ORDER BY
              CASE WHEN o.satoshis >= ? THEN 0 ELSE 1 END,
              CASE WHEN o.satoshis >= ? THEN o.satoshis ELSE -o.satoshis END
            LIMIT 1
            "#
        );

        loop {
            #[allow(clippy::type_complexity)]
            let row: Option<(i64, i64, i64, Option<String>, Option<Vec<u8>>)> =
                sqlx::query_as(&base_query)
                    .bind(user_id)
                    .bind(basket_id)
                    .bind(transaction_id)
                    .bind(target_satoshis)
                    .bind(target_satoshis)
                    .fetch_optional(&self.reader)
                    .await?;

            let Some((output_id, _satoshis, _vout, _txid, _locking_script)) = row else {
                return Ok(None);
            };

            // Claim the output (allocated to this transaction) only if it is
            // still free: the read above holds no lock.
            let claimed = sqlx::query(
                "UPDATE outputs SET spendable = 0, spent_by = ? \
                 WHERE output_id = ? AND spendable = 1 AND spent_by IS NULL",
            )
            .bind(transaction_id)
            .bind(output_id)
            .execute(&mut *self.writer_conn().await?)
            .await?
            .rows_affected();
            if claimed == 0 {
                continue;
            }

            // Fetch the full output record
            let full_row = sqlx::query(
                r#"
                SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                       satoshis, locking_script, script_length, script_offset,
                       type, provided_by, purpose, output_description, spent_by, sequence_number,
                       spending_description, spendable, change, derivation_prefix, derivation_suffix,
                       sender_identity_key, custom_instructions, created_at, updated_at
                FROM outputs WHERE output_id = ?
                "#,
            )
            .bind(output_id)
            .fetch_one(&self.reader)
            .await?;

            let now = chrono::Utc::now();
            let output = TableOutput {
                output_id: full_row.get("output_id"),
                user_id: full_row.get("user_id"),
                transaction_id: full_row.get("transaction_id"),
                basket_id: full_row.try_get("basket_id").ok(),
                txid: full_row.get("txid"),
                vout: full_row.get("vout"),
                satoshis: full_row.get("satoshis"),
                locking_script: full_row.try_get("locking_script").ok().flatten(),
                script_length: full_row.try_get("script_length").unwrap_or(0),
                script_offset: full_row.try_get("script_offset").unwrap_or(0),
                output_type: full_row.get("type"),
                provided_by: full_row.try_get("provided_by").unwrap_or_default(),
                purpose: full_row.try_get("purpose").ok(),
                output_description: full_row.try_get("output_description").ok(),
                spent_by: full_row.try_get("spent_by").ok().flatten(),
                sequence_number: full_row.try_get("sequence_number").ok().flatten(),
                spending_description: full_row.try_get("spending_description").ok(),
                spendable: full_row.try_get::<bool, _>("spendable").unwrap_or(false),
                change: full_row.try_get::<bool, _>("change").unwrap_or(false),
                derivation_prefix: full_row.try_get("derivation_prefix").ok(),
                derivation_suffix: full_row.try_get("derivation_suffix").ok(),
                sender_identity_key: full_row.try_get("sender_identity_key").ok(),
                custom_instructions: full_row.try_get("custom_instructions").ok(),
                created_at: full_row.try_get("created_at").unwrap_or(now),
                updated_at: full_row.try_get("updated_at").unwrap_or(now),
            };

            return Ok(Some(output));
        }
    }

    /// Count the change outputs `allocate_change_input` would take for a
    /// user in a basket.
    ///
    /// # Arguments
    /// * `_user_id` - The user who owns the outputs
    /// * `_basket_id` - The basket to count in
    /// * `_exclude_sending` - Whether to exclude outputs from sending transactions
    pub async fn count_change_inputs(
        &self,
        user_id: i64,
        basket_id: i64,
        exclude_sending: bool,
    ) -> Result<i64> {
        let statuses = if exclude_sending {
            "('completed', 'unproven')"
        } else {
            "('completed', 'unproven', 'sending')"
        };
        let count: (i64,) = sqlx::query_as(&format!(
            r#"
            SELECT COUNT(*)
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            WHERE o.user_id = ?
              AND o.basket_id = ?
              AND o.change = 1
              AND o.spendable = 1
              AND o.spent_by IS NULL
              AND t.status IN {statuses}
              {MANAGED_CHANGE_CLAUSE}
            "#
        ))
        .bind(user_id)
        .bind(basket_id)
        .fetch_one(self.pool())
        .await?;

        Ok(count.0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Arc, StorageSqlx, Utc, WalletStorageWriter};

    /// Review F7 of bsv-wallet-cli #44: the public `allocate_change_input`
    /// read on the reader and wrote on the writer with no guard, so two
    /// callers could allocate the same coin. Sixteen callers race for three
    /// coins: exactly three win, three distinct coins, each coin's `spent_by`
    /// the winner's transaction.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn racing_callers_never_allocate_the_same_change_coin() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f7.db");
        let storage = Arc::new(StorageSqlx::open(&path.to_string_lossy()).await.unwrap());
        storage.migrate("f7", &"0".repeat(64)).await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user(&"a".repeat(66)).await.unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        let now = Utc::now();
        let insert_tx = |status: &'static str, reference: String| {
            let storage = Arc::clone(&storage);
            async move {
                sqlx::query(
                    "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
                     version, lock_time, description, created_at, updated_at) \
                     VALUES (?, ?, ?, 0, 0, 1, 0, 'f7', ?, ?)",
                )
                .bind(user.user_id)
                .bind(status)
                .bind(reference)
                .bind(now)
                .bind(now)
                .execute(storage.writer())
                .await
                .unwrap()
                .last_insert_rowid()
            }
        };
        let funding = insert_tx("completed", "f7-funding".to_string()).await;
        for vout in 0..3 {
            sqlx::query(
                "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, \
                 locking_script, txid, type, spendable, change, provided_by, purpose, \
                 derivation_prefix, derivation_suffix, output_description, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, 1000, X'51', ?, 'P2PKH', 1, 1, 'storage', 'change', \
                 'prefix', 'suffix', 'f7', ?, ?)",
            )
            .bind(user.user_id)
            .bind(funding)
            .bind(basket.basket_id)
            .bind(vout)
            .bind("f7".repeat(32))
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
        }
        let mut spenders = Vec::new();
        for i in 0..16 {
            spenders.push(insert_tx("unsigned", format!("f7-spender-{i}")).await);
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(spenders.len()));
        let calls: Vec<_> = spenders
            .iter()
            .map(|&spender| {
                let storage = Arc::clone(&storage);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    let got = storage
                        .allocate_change_input(user.user_id, basket.basket_id, 500, false, spender)
                        .await
                        .unwrap();
                    (spender, got.map(|o| o.output_id))
                })
            })
            .collect();
        let mut won = Vec::new();
        for call in calls {
            if let (spender, Some(output_id)) = call.await.unwrap() {
                won.push((spender, output_id));
            }
        }

        assert_eq!(won.len(), 3, "three coins, three winners: {won:?}");
        let mut coins: Vec<i64> = won.iter().map(|w| w.1).collect();
        coins.sort();
        coins.dedup();
        assert_eq!(coins.len(), 3, "no coin allocated twice: {won:?}");
        for (spender, output_id) in won {
            let (spent_by,): (Option<i64>,) =
                sqlx::query_as("SELECT spent_by FROM outputs WHERE output_id = ?")
                    .bind(output_id)
                    .fetch_one(storage.pool())
                    .await
                    .unwrap();
            assert_eq!(spent_by, Some(spender));
        }
    }
}
