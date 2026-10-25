use super::{
    append_req_history_note, GetRawTxResult, MerklePath, MonitorStorage, ProvenTxReqStatus, Result,
    RetireBasis, RetireOutcome, StorageSqlx, TxSynchronizedStatus, WalletServices,
    WalletStorageReader,
};

/// The key of a request's history that holds the competitors queued for a
/// proof ask (bsv-stack-lean #66).
const COMPETITORS_QUEUED: &str = "competitorsQueued";

/// A request with competitors queued: its id, txid, attempts, history and
/// the raw bytes of our transaction.
type QueuedCompetitorRow = (i64, String, i64, String, Option<Vec<u8>>);

/// The competitors queued on a request's history.
fn queued_competitors(history: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(history)
        .ok()
        .and_then(|h| {
            h.get(COMPETITORS_QUEUED)?.as_array().map(|a| {
                a.iter()
                    .filter_map(|c| c.as_str().map(str::to_string))
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Is `competitor`'s proof, as `get_merkle_path` served it, for the root our
/// header names at its height? A proof for a block the header gate has not
/// let through yet is not checked (as `ingest_merkle_proof` defers it).
async fn competitor_proof_meets_our_header(
    tracker: &dyn bsv_rs::transaction::ChainTracker,
    competitor: &str,
    merkle_path_hex: &str,
    gate: u32,
) -> bool {
    let Ok(bytes) = hex::decode(merkle_path_hex) else {
        return false;
    };
    let Ok(bump) = MerklePath::from_binary(&bytes) else {
        return false;
    };
    let Ok(root) = bump.compute_root(Some(competitor)) else {
        return false;
    };
    bump.block_height <= gate
        && matches!(
            tracker
                .is_valid_root_for_height(&root, bump.block_height)
                .await,
            Ok(true)
        )
}

/// The competitor's word for our transaction: the proof path's evidence.
impl StorageSqlx {
    /// Append `note` to the history of `txid`'s request (if it has one).
    pub(super) async fn note_on_req_history(
        &self,
        txid: &str,
        note: serde_json::Value,
    ) -> Result<()> {
        let row: Option<(i64, String)> =
            sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;
        if let Some((proven_tx_req_id, history)) = row {
            sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
                .bind(append_req_history_note(&history, note))
                .bind(proven_tx_req_id)
                .execute(&mut *self.writer_conn().await?)
                .await?;
        }
        Ok(())
    }

    /// Queue `competitors` (well-formed txids other than `txid`) on the
    /// history of `txid`'s request for a proof ask. Returns how many are
    /// queued now.
    pub(super) async fn queue_competitors(
        &self,
        txid: &str,
        competitors: &[String],
    ) -> Result<usize> {
        let fresh: Vec<String> = competitors
            .iter()
            .map(|c| c.trim().to_ascii_lowercase())
            .filter(|c| c.len() == 64 && c.chars().all(|ch| ch.is_ascii_hexdigit()))
            .filter(|c| !c.eq_ignore_ascii_case(txid))
            .collect();
        if fresh.is_empty() {
            return Ok(0);
        }
        let row: Option<(i64, String)> =
            sqlx::query_as("SELECT proven_tx_req_id, history FROM proven_tx_reqs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(self.pool())
                .await?;
        let Some((proven_tx_req_id, history)) = row else {
            return Ok(0);
        };
        let mut object = match serde_json::from_str::<serde_json::Value>(&history) {
            Ok(serde_json::Value::Object(map)) => map,
            _ if history.trim().is_empty() => serde_json::Map::new(),
            _ => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "previous".to_string(),
                    serde_json::Value::from(history.as_str()),
                );
                map
            }
        };
        let mut queued = queued_competitors(&history);
        for c in fresh {
            if !queued.contains(&c) {
                queued.push(c);
            }
        }
        object.insert(
            COMPETITORS_QUEUED.to_string(),
            serde_json::Value::from(queued.clone()),
        );
        sqlx::query("UPDATE proven_tx_reqs SET history = ? WHERE proven_tx_req_id = ?")
            .bind(serde_json::Value::Object(object).to_string())
            .bind(proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        tracing::info!(
            txid = %txid,
            competitors = ?queued,
            marker = "competitor_proof_ask_queued",
            "a broadcaster's word named a competitor: queued for a proof ask"
        );
        Ok(queued.len())
    }

    /// Does `competitor` spend one of the inputs of `our_raw_tx`, by its own
    /// bytes, and is it mined, by its proof checked against our headers?
    /// `Some` with the outpoints of ours it spends (`txid`, `vout`), lost to
    /// it, when it is; `None` otherwise.
    ///
    /// The existing proof path's two asks (`get_raw_tx`, `get_merkle_path`,
    /// the sources the proof pass asks) and the check of
    /// `ingest_merkle_proof` (the root at the proof's height, from our
    /// tracker), bound as Rule 28's `is_utxo` binds a named spender: the
    /// name is the broadcaster's word, the bytes and the proof the evidence.
    /// No spender is looked up. No tracker, no check, and nothing asked.
    async fn competitor_is_proven(
        &self,
        services: &dyn WalletServices,
        our_raw_tx: &[u8],
        competitor: &str,
        gate: u32,
    ) -> Option<Vec<(String, u32)>> {
        let tracker = self.get_chain_tracker().await?;
        let ours = bsv_rs::transaction::Transaction::from_binary(our_raw_tx).ok()?;
        let raw = match services.get_raw_tx(competitor, false).await {
            Ok(GetRawTxResult {
                raw_tx: Some(raw), ..
            }) => raw,
            _ => return None,
        };
        let theirs = bsv_rs::transaction::Transaction::from_binary(&raw).ok()?;
        if !theirs.id().eq_ignore_ascii_case(competitor) {
            tracing::warn!(
                competitor = %competitor,
                "competitor proof ask: the bytes served are not the competitor's; dropped"
            );
            return None;
        }
        let lost: Vec<(String, u32)> = ours
            .inputs
            .iter()
            .filter_map(|ours_in| {
                let source = ours_in.get_source_txid().ok()?;
                theirs
                    .inputs
                    .iter()
                    .any(|theirs_in| {
                        theirs_in.source_output_index == ours_in.source_output_index
                            && matches!(theirs_in.get_source_txid(), Ok(a) if a.eq_ignore_ascii_case(&source))
                    })
                    .then(|| (source.to_ascii_lowercase(), ours_in.source_output_index))
            })
            .collect();
        if lost.is_empty() {
            tracing::warn!(
                competitor = %competitor,
                "competitor proof ask: the competitor's bytes spend none of our inputs; no evidence"
            );
            return None;
        }
        let merkle_path = match services.get_merkle_path(competitor, false).await {
            Ok(found) => found.merkle_path?,
            Err(_) => return None,
        };
        competitor_proof_meets_our_header(&*tracker, competitor, &merkle_path, gate)
            .await
            .then_some(lost)
    }

    /// The proof path's first step (bsv-stack-lean #66): every request with
    /// a queued competitor and no word yet asks for that competitor's bytes
    /// and proof. A competitor proven to spend one of our inputs writes the
    /// double-spend word through the release rule (`retire_undeliverable_tx`
    /// with `doubleSpend`); anything less writes nothing and the competitor
    /// stays queued for the next pass.
    pub(super) async fn ask_queued_competitor_proofs(&self) -> Result<Vec<TxSynchronizedStatus>> {
        let rows: Vec<QueuedCompetitorRow> = sqlx::query_as(
            "SELECT r.proven_tx_req_id, r.txid, r.attempts, r.history, COALESCE(r.raw_tx, t.raw_tx) \
             FROM proven_tx_reqs r LEFT JOIN transactions t ON t.txid = r.txid \
             WHERE r.status IN ('unsent', 'sending', 'unmined', 'unknown', 'callback', 'unconfirmed') \
               AND r.history LIKE '%\"competitorsQueued\"%'",
        )
        .fetch_all(self.pool())
        .await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let Ok(services) = self.get_services() else {
            return Ok(Vec::new());
        };
        let gate = self.max_acceptable_proof_height().await?;
        if gate == 0 {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        for (proven_tx_req_id, txid, attempts, history, raw_tx) in rows {
            let Some(raw_tx) = raw_tx else {
                continue;
            };
            for competitor in queued_competitors(&history) {
                let Some(lost) = self
                    .competitor_is_proven(services.as_ref(), &raw_tx, &competitor, gate)
                    .await
                else {
                    tracing::info!(
                        txid = %txid,
                        competitor = %competitor,
                        marker = "competitor_not_proven",
                        "competitor proof ask: no checked proof of a conflicting spend; no word, asked again next pass"
                    );
                    continue;
                };
                let outcome = self
                    .retire_undeliverable_tx(
                        services.as_ref(),
                        &txid,
                        proven_tx_req_id,
                        attempts,
                        "doubleSpend",
                        RetireBasis::CompetitorProven {
                            competitor: &competitor,
                            lost: &lost,
                        },
                        None,
                        chrono::Utc::now(),
                    )
                    .await?;
                if let RetireOutcome::Retired { restored, kept } = outcome {
                    tracing::warn!(
                        txid = %txid,
                        competitor = %competitor,
                        restored,
                        kept,
                        marker = "competitor_proven",
                        "a competitor's proof met our header and its bytes spend our input: doubleSpend"
                    );
                    results.push(TxSynchronizedStatus {
                        txid: txid.clone(),
                        status: ProvenTxReqStatus::DoubleSpend,
                        block_height: None,
                        block_hash: None,
                        merkle_root: None,
                        merkle_path: None,
                    });
                }
                break;
            }
        }
        Ok(results)
    }
}
