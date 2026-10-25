use super::{parse_input_txids, prune_beef_to_roots, Result, StorageSqlx};

impl StorageSqlx {
    pub(super) async fn compact_input_beefs_impl(&self) -> Result<u32> {
        use bsv_rs::transaction::{Beef, MerklePath};

        // Find completed proven_tx_reqs with non-null input_beef.
        // Process in batches of 50 to avoid holding the connection too long.
        let rows: Vec<(i64, Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(
            r#"
            SELECT proven_tx_req_id, input_beef, raw_tx
            FROM proven_tx_reqs
            WHERE status = 'completed'
              AND input_beef IS NOT NULL
              AND LENGTH(input_beef) > 1000
            ORDER BY LENGTH(input_beef) DESC
            LIMIT 50
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut compacted = 0u32;

        for (req_id, beef_bytes, raw_tx) in &rows {
            let mut beef = match Beef::from_binary(beef_bytes) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let original_size = beef_bytes.len();

            // Find unproven txids in this BEEF
            let unproven_txids: Vec<String> = beef
                .txs
                .iter()
                .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
                .map(|tx| tx.txid())
                .collect();

            if unproven_txids.is_empty() {
                continue;
            }

            // Check proven_txs for available proofs (batch query)
            let mut upgraded = 0u32;

            for chunk in unproven_txids.chunks(400) {
                let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query_str = format!(
                    "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
                    placeholders
                );

                let mut query = sqlx::query(&query_str);
                for txid in chunk {
                    query = query.bind(txid);
                }

                let proof_rows: Vec<(String, Vec<u8>)> = query
                    .fetch_all(self.pool())
                    .await?
                    .into_iter()
                    .map(|row| {
                        let txid: String = sqlx::Row::get(&row, "txid");
                        let mp: Vec<u8> = sqlx::Row::get(&row, "merkle_path");
                        (txid, mp)
                    })
                    .collect();

                let mut conn = self.pool().acquire().await?;
                let tracker = self.get_chain_tracker().await;
                for (txid, merkle_path_bytes) in &proof_rows {
                    if let Ok(merkle_path) = MerklePath::from_binary(merkle_path_bytes) {
                        // P0-1: a stored proof that was never checked is
                        // checked here, once; a refuted one is demoted. The
                        // rewritten BEEF is stored, so it takes only what
                        // the ingest would store (P0-1c): with no tracker or
                        // a tracker fault the proof is not merged and the
                        // row stays unchecked for a later pass.
                        if !super::proof_root_checks::check_unchecked_stored_proof_on(
                            &mut conn,
                            Some(self.writer()),
                            tracker.as_deref(),
                            txid,
                            &merkle_path,
                        )
                        .await?
                        .may_store()
                        {
                            continue;
                        }
                        // P0-1d: linked with its own leaf flagged, so the
                        // stored link survives the next merge.
                        let Some(bump_index) =
                            super::create_action::merge_proof_for(&mut beef, merkle_path, txid)
                        else {
                            continue;
                        };
                        if let Some(tx) = beef.find_txid_mut(txid) {
                            tx.set_bump_index(Some(bump_index));
                            upgraded += 1;
                        }
                    }
                }
            }

            if upgraded == 0 {
                continue;
            }

            // Now that ancestors carry proofs, drop everything the transaction's
            // own inputs no longer reach: a BUMP terminates its branch. Rooted at
            // the inputs, so an input that is still unproven keeps its history.
            // (`Beef::trim_known_proven` is root-blind: it walks from whichever
            // transactions nothing else spends, which can drop an unproven input
            // that a proven sibling happens to descend from.)
            if let Some(raw) = raw_tx {
                if let Ok(roots) = parse_input_txids(raw) {
                    prune_beef_to_roots(&mut beef, &roots);
                }
            }

            let new_bytes = beef.to_binary();
            let new_size = new_bytes.len();

            // Only update if we actually saved space
            if new_size < original_size {
                let now = chrono::Utc::now();
                sqlx::query(
                    "UPDATE proven_tx_reqs SET input_beef = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                )
                .bind(&new_bytes)
                .bind(now)
                .bind(req_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;

                compacted += 1;
                tracing::debug!(
                    req_id = req_id,
                    original_kb = original_size / 1024,
                    new_kb = new_size / 1024,
                    savings_pct = ((original_size - new_size) * 100) / original_size,
                    "Compacted input_beef"
                );
            }
        }

        Ok(compacted)
    }
}
