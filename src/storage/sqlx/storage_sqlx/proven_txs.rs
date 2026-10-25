use super::same_proof_anchor;
use super::{
    commit, MerklePath, MonitorStorage, ProofIngestOutcome, ProvenTxAnchor, ProvenTxReqStatus,
    Result, StorageSqlx, TxSynchronizedStatus,
};

/// The level-0 leaf offset of `txid` inside `bump` (the `proven_txs.idx`
/// column); 0 when the leaf cannot be found (a proof whose root computed
/// for `txid` always contains it, so this is only a defensive default).
pub(crate) fn bump_leaf_index(bump: &MerklePath, txid: &str) -> i64 {
    bump.path
        .first()
        .and_then(|level| {
            level.iter().find(|leaf| {
                leaf.hash
                    .as_deref()
                    .is_some_and(|h| h.eq_ignore_ascii_case(txid))
            })
        })
        .map(|leaf| leaf.offset as i64)
        .unwrap_or(0)
}

/// A merkle proof the chain tracker has VALIDATED for its block: the input
/// of the ONE proof-store funnel, [`store_validated_proof_on`]. Every path
/// that writes a `proven_txs` row goes through it: `ingest_merkle_proof`
/// (the webhook, the SSE stream, the relay drain, the polling fetch), the
/// BEEF walk's own fetch in `create_action.rs`, and `internalize_action`'s
/// BUMP. Callers keep their own request/transaction completion; the funnel
/// owns the gate, the anchor comparison, the bytes, the `mined` memory and
/// the root-check record. Every caller builds one only after its
/// ChainTracker answered `Ok(true)` for `checked_root` at `block_height`;
/// a caller with no tracker never reaches the funnel (P0-1).
pub(crate) struct ValidatedProofRow<'a> {
    pub txid: &'a str,
    pub block_height: u32,
    /// The root `merkle_path` computes for `txid`, which the caller's
    /// ChainTracker confirmed at `block_height`. Recorded in
    /// `proof_root_checks`, so the stored row reads as checked.
    pub checked_root: &'a str,
    /// Empty when the caller had no header in hand (an internalize whose
    /// header could not be read); the review task backfills it once the
    /// row's root is canonical, and a later proof with a known hash fills
    /// it too.
    pub block_hash: &'a str,
    pub merkle_root: &'a str,
    pub merkle_path: &'a [u8],
    /// The transaction's level-0 leaf offset.
    pub idx: i64,
    /// The raw bytes when the caller holds them; otherwise sourced from the
    /// `transactions` / `proven_tx_reqs` rows.
    pub raw_tx: Option<&'a [u8]>,
}

/// What [`store_validated_proof_on`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreProofOutcome {
    /// A row was written: new, or the stored anchor was replaced by this
    /// proof's block (`replaced`).
    Stored { proven_tx_id: i64, replaced: bool },
    /// The stored anchor already matched this proof (an empty stored block
    /// hash may have been filled).
    Unchanged { proven_tx_id: i64 },
    /// Above the proof LAG gate: nothing written, no attempt counted.
    Deferred {
        block_height: u32,
        processed_height: u32,
    },
    /// The transaction's raw bytes are nowhere in storage: nothing written.
    NoRawTx,
}

/// THE proof-store funnel, on an open connection (inside the caller's
/// transaction): the gate read on this same connection (one point read of
/// the one-row `monitor_state` table), replace-on-differ, the no-raw guard,
/// the empty-hash backfill, and the `mined` broadcast memory.
pub(crate) async fn store_validated_proof_on(
    conn: &mut sqlx::SqliteConnection,
    p: &ValidatedProofRow<'_>,
) -> Result<StoreProofOutcome> {
    let gate = super::monitor_state::read_proof_gate_on(&mut *conn).await?;
    if p.block_height > gate {
        tracing::debug!(
            txid = %p.txid,
            block_height = p.block_height,
            processed_height = gate,
            marker = "proof_deferred_above_processed_height",
            "store_validated_proof: deferred until the header has aged one cycle"
        );
        return Ok(StoreProofOutcome::Deferred {
            block_height: p.block_height,
            processed_height: gate,
        });
    }
    let now = chrono::Utc::now();
    let existing: Option<(i64, String, i64, String)> = sqlx::query_as(
        "SELECT proven_tx_id, block_hash, height, merkle_root FROM proven_txs WHERE txid = ?",
    )
    .bind(p.txid)
    .fetch_optional(&mut *conn)
    .await?;
    let outcome = match existing {
        Some((proven_tx_id, stored_hash, stored_height, stored_root))
            if same_proof_anchor(
                &stored_hash,
                stored_height,
                &stored_root,
                p.block_hash,
                p.block_height,
                p.merkle_root,
            ) =>
        {
            if stored_hash.is_empty() && !p.block_hash.is_empty() {
                sqlx::query(
                    "UPDATE proven_txs SET block_hash = ?, updated_at = ? WHERE proven_tx_id = ?",
                )
                .bind(p.block_hash)
                .bind(now)
                .bind(proven_tx_id)
                .execute(&mut *conn)
                .await?;
                tracing::info!(
                    txid = %p.txid,
                    block_height = p.block_height,
                    block_hash = %p.block_hash,
                    marker = "proof_block_hash_backfilled",
                    "store_validated_proof: the stored anchor had no block hash; filled from this proof"
                );
            } else {
                tracing::debug!(
                    txid = %p.txid,
                    block_height = p.block_height,
                    "store_validated_proof: the stored anchor already matches; nothing to write"
                );
            }
            StoreProofOutcome::Unchanged { proven_tx_id }
        }
        Some((proven_tx_id, stored_hash, stored_height, stored_root)) => {
            tracing::info!(
                txid = %p.txid,
                stored_height,
                stored_block_hash = %stored_hash,
                stored_merkle_root = %stored_root,
                block_height = p.block_height,
                block_hash = %p.block_hash,
                merkle_root = %p.merkle_root,
                marker = "proof_replaced",
                "store_validated_proof: a validated proof for a different block replaces the stored anchor (reorg re-anchor)"
            );
            sqlx::query(
                "UPDATE proven_txs SET height = ?, idx = ?, block_hash = ?, merkle_root = ?, merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?",
            )
            .bind(p.block_height as i64)
            .bind(p.idx)
            .bind(p.block_hash)
            .bind(p.merkle_root)
            .bind(p.merkle_path)
            .bind(now)
            .bind(proven_tx_id)
            .execute(&mut *conn)
            .await?;
            StoreProofOutcome::Stored {
                proven_tx_id,
                replaced: true,
            }
        }
        None => {
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?,
                    COALESCE(
                        ?,
                        (SELECT raw_tx FROM transactions WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1),
                        (SELECT raw_tx FROM proven_tx_reqs WHERE txid = ? AND raw_tx IS NOT NULL LIMIT 1)
                    ),
                    ?, ?)
                "#,
            )
            .bind(p.txid)
            .bind(p.block_height as i64)
            .bind(p.idx)
            .bind(p.block_hash)
            .bind(p.merkle_root)
            .bind(p.merkle_path)
            .bind(p.raw_tx)
            .bind(p.txid)
            .bind(p.txid)
            .bind(now)
            .bind(now)
            .execute(&mut *conn)
            .await?;
            let inserted: Option<(i64,)> =
                sqlx::query_as("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
                    .bind(p.txid)
                    .fetch_optional(&mut *conn)
                    .await?;
            match inserted {
                Some((proven_tx_id,)) => StoreProofOutcome::Stored {
                    proven_tx_id,
                    replaced: false,
                },
                None => return Ok(StoreProofOutcome::NoRawTx),
            }
        }
    };
    // The row now holds (or already held) a proof whose root the caller's
    // tracker confirmed. An Unchanged row keeps its stored path: when that
    // path computes a different root the record does not match it and the
    // row still reads as unchecked.
    super::proof_root_checks::record_root_checked_on(
        &mut *conn,
        p.txid,
        p.block_height,
        p.checked_root,
    )
    .await?;
    // Mined: every provider has it. Remember it for reduced sends; never a
    // reason to fail the store.
    if let Err(e) = super::broadcast_seen::record_broadcast_status_on(
        conn,
        p.txid,
        crate::services::broadcast_memory::BROADCAST_PROVIDER_CHAIN,
        crate::services::broadcast_memory::BROADCAST_STATUS_MINED,
    )
    .await
    {
        tracing::debug!(txid = %p.txid, error = %e, "broadcast_seen: mined record skipped");
    }
    Ok(outcome)
}

fn anchor_from_row(row: (String, i64, String, String)) -> ProvenTxAnchor {
    ProvenTxAnchor {
        txid: row.0,
        height: row.1.max(0) as u32,
        block_hash: row.2,
        merkle_root: row.3,
    }
}

impl StorageSqlx {
    /// Validate and store a merkle proof for a transaction, completing its
    /// proven_tx_req and transaction records.
    ///
    /// This is the single convergence point for EVERY proof-delivery path:
    /// - the polling `synchronize_transaction_statuses` (CheckForProofs task),
    /// - an Arcade/ARC `MINED` webhook carrying `merklePath`,
    /// - an SSE-MINED-triggered focused fetch,
    /// - a `bsv-wallet-relay` queue drain.
    ///
    /// Steps:
    /// 1. Parse the BUMP binary and compute its merkle root for `txid`.
    /// 2. Validate the root against the storage's ChainTracker at
    ///    `block_height`. With no tracker wired nothing is stored and the
    ///    outcome is [`ProofIngestOutcome::TrackerUnavailable`]: a storage
    ///    that cannot check a root never calls a coin proven.
    /// 3. `INSERT OR IGNORE INTO proven_txs` (raw_tx sourced from existing
    ///    transactions / proven_tx_reqs rows), and record that this row's
    ///    root was checked.
    /// 4. Mark the proven_tx_req and transaction `completed`.
    ///
    /// Invalid or unverifiable proofs are NEVER stored; the outcome tells the
    /// caller whether to retry (see [`ProofIngestOutcome`]).
    ///
    /// `header_merkle_root` is stored when provided (e.g. from a block header
    /// lookup); otherwise the computed root — which the ChainTracker just
    /// validated — is stored.
    pub async fn ingest_merkle_proof(
        &self,
        txid: &str,
        merkle_path_bytes: &[u8],
        block_height: u32,
        block_hash: &str,
        header_merkle_root: Option<&str>,
    ) -> Result<ProofIngestOutcome> {
        let bump = match MerklePath::from_binary(merkle_path_bytes) {
            Ok(b) => b,
            Err(e) => {
                return Ok(ProofIngestOutcome::InvalidProof(format!(
                    "failed to parse merkle path: {}",
                    e
                )))
            }
        };

        let computed_root = match bump.compute_root(Some(txid)) {
            Ok(r) => r,
            Err(e) => {
                return Ok(ProofIngestOutcome::InvalidProof(format!(
                    "failed to compute merkle root: {}",
                    e
                )))
            }
        };

        // The proof LAG gate (ts-stack `TaskCheckForProofs.maxAcceptableHeight`):
        // a proof for a block newer than the last header the monitor watched
        // stay the chain tip for a full cycle is DEFERRED, never stored and
        // never an attempt. This is what keeps an orphan's proof out of
        // `proven_txs` (2026-09-07: a 34 MB block at 965771 lost to a 58-tx
        // block within seconds, and Arcade's inline proofs had arrived 5 s
        // after the block). The gate is ONE persisted row (migration 004)
        // that every instance and process reads; absent or 0 is CLOSED. This
        // early read only spares the tracker call: the funnel below re-reads
        // the gate on the write connection and is the authority.
        let processed_height = self.max_acceptable_proof_height().await?;
        if block_height > processed_height {
            tracing::debug!(
                txid = %txid,
                block_height,
                processed_height,
                marker = "proof_deferred_above_processed_height",
                "ingest_merkle_proof: deferred until the header has aged one cycle"
            );
            return Ok(ProofIngestOutcome::DeferredAboveProcessedHeight {
                block_height,
                processed_height,
            });
        }

        // Layer 1: Validate the merkle proof before storing. Bad proofs
        // (e.g. from provider rate limiting) are rejected instead of stored
        // permanently. No tracker, no proof (P0-1): the reference cannot
        // reach a store without one, its `getChainTracker` throws when none
        // is configured (ts-stack@fb1b2da
        // packages/wallet/wallet-toolbox/src/services/Services.ts:267-275).
        let Some(tracker) = self.get_chain_tracker().await else {
            tracing::error!(
                txid = %txid,
                block_height,
                marker = "proof_refused_no_chain_tracker",
                "ingest_merkle_proof: no ChainTracker is wired, so the merkle root cannot be checked against the active chain; the proof was NOT stored and the transaction stays unproven"
            );
            return Ok(ProofIngestOutcome::TrackerUnavailable);
        };
        match tracker
            .is_valid_root_for_height(&computed_root, block_height)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Ok(ProofIngestOutcome::InvalidMerkleRoot { computed_root });
            }
            Err(e) => {
                return Ok(ProofIngestOutcome::TrackerError(e.to_string()));
            }
        }

        let merkle_root = header_merkle_root
            .map(|s| s.to_string())
            .unwrap_or_else(|| computed_root.clone());

        // ONE funnel for every proof store (`store_validated_proof_on`): the
        // gate read on the write connection, replace-on-differ (ts-stack
        // `internalizeAction`: "in the event of a reorg, the proof in this
        // beef should replace the proof in storage"; Arcade's `reorg_reanchor`
        // MINED is the same message), the no-raw guard and the empty-hash
        // backfill, in one transaction with the record completion below.
        let mut tx = self.write_tx().await?;
        let stored = store_validated_proof_on(
            &mut tx,
            &ValidatedProofRow {
                txid,
                block_height,
                checked_root: &computed_root,
                block_hash,
                merkle_root: &merkle_root,
                merkle_path: merkle_path_bytes,
                idx: bump_leaf_index(&bump, txid),
                raw_tx: None,
            },
        )
        .await?;
        let proven_tx_id = match stored {
            StoreProofOutcome::Deferred {
                block_height,
                processed_height,
            } => {
                tx.rollback().await?;
                return Ok(ProofIngestOutcome::DeferredAboveProcessedHeight {
                    block_height,
                    processed_height,
                });
            }
            StoreProofOutcome::NoRawTx => {
                // `INSERT OR IGNORE` also swallows a NOT NULL violation, so a
                // proof for a transaction whose raw bytes are nowhere in
                // storage used to report `Ingested` while storing nothing and
                // then complete the request with no proof link. Say so
                // instead; nothing is latched.
                tx.rollback().await?;
                tracing::warn!(
                    txid = %txid,
                    block_height,
                    marker = "proof_not_stored_no_raw_tx",
                    "ingest_merkle_proof: no raw transaction bytes in storage; the proof was not stored"
                );
                return Ok(ProofIngestOutcome::InvalidProof(format!(
                    "proof for {} not stored: no raw transaction bytes in storage",
                    txid
                )));
            }
            StoreProofOutcome::Stored { proven_tx_id, .. }
            | StoreProofOutcome::Unchanged { proven_tx_id } => proven_tx_id,
        };
        let now = chrono::Utc::now();

        // Update proven_tx_req to completed
        sqlx::query(
            r#"
            UPDATE proven_tx_reqs
            SET status = 'completed', proven_tx_id = ?, updated_at = ?
            WHERE txid = ?
            "#,
        )
        .bind(proven_tx_id)
        .bind(now)
        .bind(txid)
        .execute(&mut *tx)
        .await?;

        // Update transaction status to completed and link proof
        sqlx::query(
            "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?"
        )
        .bind(proven_tx_id)
        .bind(now)
        .bind(txid)
        .execute(&mut *tx)
        .await?;
        commit(tx).await?;

        Ok(ProofIngestOutcome::Ingested(TxSynchronizedStatus {
            txid: txid.to_string(),
            status: ProvenTxReqStatus::Completed,
            block_height: Some(block_height),
            block_hash: Some(block_hash.to_string()),
            merkle_root: Some(merkle_root),
            merkle_path: Some(merkle_path_bytes.to_vec()),
        }))
    }
}

impl StorageSqlx {
    pub(super) async fn set_proven_tx_block_hash_if_empty_impl(
        &self,
        txid: &str,
        block_hash: &str,
    ) -> Result<bool> {
        if block_hash.is_empty() {
            return Ok(false);
        }
        let done = sqlx::query(
            "UPDATE proven_txs SET block_hash = ?, updated_at = ? WHERE txid = ? AND block_hash = ''",
        )
        .bind(block_hash)
        .bind(chrono::Utc::now())
        .bind(txid)
        .execute(&mut *self.writer_conn().await?)
        .await?;
        Ok(done.rows_affected() > 0)
    }

    pub(super) async fn find_proven_txs_by_block_hash_impl(
        &self,
        block_hash: &str,
    ) -> Result<Vec<ProvenTxAnchor>> {
        let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
            "SELECT txid, height, block_hash, merkle_root FROM proven_txs WHERE lower(block_hash) = lower(?) ORDER BY height ASC, proven_tx_id ASC",
        )
        .bind(block_hash)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(anchor_from_row).collect())
    }

    pub(super) async fn find_proven_txs_in_heights_impl(
        &self,
        min_height: u32,
        max_height: u32,
    ) -> Result<Vec<ProvenTxAnchor>> {
        let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
            "SELECT txid, height, block_hash, merkle_root FROM proven_txs WHERE height >= ? AND height <= ? ORDER BY height ASC, proven_tx_id ASC",
        )
        .bind(min_height as i64)
        .bind(max_height as i64)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(anchor_from_row).collect())
    }

    pub(super) async fn ingest_push_proof_impl(
        &self,
        txid: &str,
        merkle_path: &[u8],
        block_height: u32,
        block_hash: &str,
    ) -> Result<Option<ProofIngestOutcome>> {
        // Same funnel as the webhook path: BUMP parse → compute root →
        // ChainTracker validation → proven_txs insert → complete records.
        self.ingest_merkle_proof(txid, merkle_path, block_height, block_hash, None)
            .await
            .map(Some)
    }
}
