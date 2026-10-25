use super::{FindOutputBasketsArgs, Result, Row, StorageSqlx, TableOutputBasket, Utc};

impl StorageSqlx {
    // =========================================================================
    // Output Basket Repository
    // =========================================================================

    /// Find output baskets for a user.
    pub async fn find_output_baskets_internal(
        &self,
        user_id: i64,
        args: &FindOutputBasketsArgs,
    ) -> Result<Vec<TableOutputBasket>> {
        let mut sql = String::from(
            r#"
            SELECT basket_id, user_id, name, number_of_desired_utxos, minimum_desired_utxo_value,
                   created_at, updated_at
            FROM output_baskets
            WHERE user_id = ? AND is_deleted = 0
            "#,
        );

        if args.name.is_some() {
            sql.push_str(" AND name = ?");
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

        if let Some(ref name) = args.name {
            query = query.bind(name);
        }

        if let Some(ref since) = args.base.since {
            query = query.bind(since);
        }

        let rows = query.fetch_all(&self.reader).await?;

        let mut baskets = Vec::new();
        for row in rows {
            baskets.push(TableOutputBasket {
                basket_id: row.get("basket_id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                number_of_desired_utxos: row.get("number_of_desired_utxos"),
                minimum_desired_utxo_value: row.get("minimum_desired_utxo_value"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(baskets)
    }

    /// Find or create a default basket for a user.
    pub async fn find_or_create_default_basket(&self, user_id: i64) -> Result<TableOutputBasket> {
        // Try to find existing
        let args = FindOutputBasketsArgs {
            user_id: Some(user_id),
            name: Some("default".to_string()),
            ..Default::default()
        };

        let baskets = self.find_output_baskets_internal(user_id, &args).await?;

        if let Some(basket) = baskets.into_iter().next() {
            return Ok(basket);
        }

        // Create new default basket
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
            VALUES (?, 'default', 6, 10000, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(now)
        .bind(now)
        .execute(&mut *self.writer_conn().await?)
        .await?;

        Ok(TableOutputBasket {
            basket_id: result.last_insert_rowid(),
            user_id,
            name: "default".to_string(),
            number_of_desired_utxos: 6,
            minimum_desired_utxo_value: 10000,
            created_at: now,
            updated_at: now,
        })
    }
}
