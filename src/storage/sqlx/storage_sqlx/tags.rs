use super::{Result, StorageSqlx};

impl StorageSqlx {
    /// Get tags associated with an output.
    ///
    /// Queries the output_tag_maps and output_tags tables to find all tags
    /// for a given output.
    pub async fn get_tags_for_output_id(&self, output_id: i64) -> Result<Vec<String>> {
        let tags: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT ot.tag
            FROM output_tags_map otm
            JOIN output_tags ot ON otm.output_tag_id = ot.output_tag_id
            WHERE otm.output_id = ?
              AND ot.is_deleted = 0
            ORDER BY ot.tag
            "#,
        )
        .bind(output_id)
        .fetch_all(self.pool())
        .await?;

        Ok(tags.into_iter().map(|(t,)| t).collect())
    }
}
