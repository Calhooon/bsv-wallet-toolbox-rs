use super::{Result, StorageSqlx};

impl StorageSqlx {
    /// Get labels associated with a transaction.
    ///
    /// Queries the tx_label_maps and tx_labels tables to find all labels
    /// for a given transaction.
    pub async fn get_labels_for_transaction_id(&self, transaction_id: i64) -> Result<Vec<String>> {
        let labels: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT tl.label
            FROM tx_labels_map tlm
            JOIN tx_labels tl ON tlm.tx_label_id = tl.tx_label_id
            WHERE tlm.transaction_id = ?
              AND tl.is_deleted = 0
            ORDER BY tl.label
            "#,
        )
        .bind(transaction_id)
        .fetch_all(self.pool())
        .await?;

        Ok(labels.into_iter().map(|(l,)| l).collect())
    }
}
