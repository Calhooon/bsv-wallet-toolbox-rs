use super::{
    commit, Duration, FindProvenTxReqsArgs, HashMap, HashSet, MonitorStorage, ProofIngestOutcome,
    ProvenTxReqStatus, Result, Row, StorageSqlx, TxSynchronizedStatus, Utc, WalletStorageReader,
};

/// The reference's `unprovenAttemptsLimitMain` / `unprovenAttemptsLimitTest`
/// (wallet-toolbox src/monitor/Monitor.ts:105-106). The reference writes a
/// req off once `attempts` is GREATER than the limit
/// (src/monitor/tasks/TaskCheckForProofs.ts:158); here, since 0.7.4
/// (bsv-stack-lean #66), past the limit is only a log line: a count is no
/// evidence and writes no word.
pub(crate) const PROOF_ATTEMPTS_LIMIT_MAIN: i64 = 144;
pub(crate) const PROOF_ATTEMPTS_LIMIT_TEST: i64 = 10;

impl StorageSqlx {
    /// `MonitorStorage::synchronize_transaction_statuses` for the requests
    /// of this wallet's own transactions: the triage, the attempt count and
    /// the proof fetch.
    pub(super) async fn synchronize_own_reqs(&self) -> Result<Vec<TxSynchronizedStatus>> {
        // Receive-proof gap (2026-08-29): a proof-less transaction with NO
        // req is invisible to the walk below. Repair first (bounded,
        // idempotent), so this very pass can triage what it adopted. A
        // failure here degrades to exactly the pre-existing behaviour —
        // never fewer proofs than before — and is logged, not swallowed.
        match self.adopt_unproven_transactions().await {
            Ok(adopted) if adopted.reqs_created > 0 || adopted.linked_from_proven_txs > 0 => {
                tracing::info!(
                    reqs_created = adopted.reqs_created,
                    linked_from_proven_txs = adopted.linked_from_proven_txs,
                    marker = "unproven_tx_adopted",
                    "synchronize_transaction_statuses: adopted proof-less transactions into the proof set"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "synchronize_transaction_statuses: adopt_unproven_transactions failed; continuing with the existing req set"
                );
            }
        }

        // Query proven_tx_reqs with statuses that need synchronization
        let statuses = vec![
            ProvenTxReqStatus::Unmined,
            ProvenTxReqStatus::Unknown,
            ProvenTxReqStatus::Callback,
            ProvenTxReqStatus::Sending,
            ProvenTxReqStatus::Unconfirmed,
        ];

        let args = FindProvenTxReqsArgs {
            status: Some(statuses),
            ..Default::default()
        };

        let reqs = self.find_proven_tx_reqs(args).await?;

        if reqs.is_empty() {
            return Ok(Vec::new());
        }

        let services = self.get_services()?;

        tracing::debug!(
            "synchronize_transaction_statuses: checking {} transactions",
            reqs.len()
        );

        // =====================================================================
        // TRIAGE STEP (Go pattern: filterTxsByConfirmationDepth)
        // Batch-check tx statuses BEFORE fetching individual merkle proofs.
        // Only fetch proofs for confirmed txs (depth >= 1).
        // This avoids hammering getMerklePath for unconfirmed/missing txs.
        // =====================================================================
        let txids: Vec<String> = reqs.iter().map(|r| r.txid.clone()).collect();
        let triage_result = services.get_status_for_txids(&txids, false).await;

        // Build txid -> status lookup from triage results, plus the proofs
        // the triage answer already carried (Arcade's MINED document does;
        // WhatsOnChain's and Bitails' batch status answers never do).
        #[allow(clippy::type_complexity)]
        let (confirmed_txids, triage_proofs): (
            std::collections::HashSet<String>,
            std::collections::HashMap<String, (Vec<u8>, u32, String)>,
        ) = match &triage_result {
            Ok(status_result) if status_result.status == "success" => {
                let mut confirmed = 0u32;
                let mut mempool = 0u32;
                let mut missing = 0u32;

                let confirmed_set: std::collections::HashSet<String> = status_result
                    .results
                    .iter()
                    .filter_map(|detail| {
                        match detail.status.as_str() {
                            "mined" => {
                                let depth = detail.depth.unwrap_or(0);
                                if depth >= 1 {
                                    confirmed += 1;
                                    Some(detail.txid.clone())
                                } else {
                                    mempool += 1;
                                    None
                                }
                            }
                            "known" => {
                                mempool += 1;
                                None // In mempool, no proof yet
                            }
                            _ => {
                                missing += 1;
                                None // Unknown / not found
                            }
                        }
                    })
                    .collect();

                // A status provider that answers with the proof attached
                // saves a whole getMerklePath round trip per transaction.
                // Only a well-formed BUMP hex is taken; the proof is still a
                // hint, validated against our own headers by
                // `ingest_merkle_proof` before anything is latched.
                let proofs: std::collections::HashMap<String, (Vec<u8>, u32, String)> =
                    status_result
                        .results
                        .iter()
                        .filter(|detail| detail.status == "mined")
                        .filter_map(|detail| {
                            let bytes = hex::decode(detail.merkle_path.as_deref()?).ok()?;
                            Some((
                                detail.txid.clone(),
                                (
                                    bytes,
                                    detail.block_height?,
                                    detail.block_hash.clone().unwrap_or_default(),
                                ),
                            ))
                        })
                        .collect();

                tracing::info!(
                    total = reqs.len(),
                    confirmed,
                    mempool,
                    missing,
                    with_proof = proofs.len(),
                    "synchronize_transaction_statuses: triage complete"
                );

                (confirmed_set, proofs)
            }
            Ok(status_result) => {
                // Status service returned non-success — skip sync (Go pattern)
                tracing::warn!(
                    error = ?status_result.error,
                    "synchronize_transaction_statuses: status service returned error, skipping synchronization"
                );
                return Ok(Vec::new());
            }
            Err(e) => {
                // Status service failed — skip sync entirely (Go pattern:
                // "Return empty slice to skip synchronization when we can't get the status")
                tracing::warn!(
                    error = %e,
                    count = reqs.len(),
                    "synchronize_transaction_statuses: failed to get status for txIDs, skipping synchronization"
                );
                return Ok(Vec::new());
            }
        };

        // What the status sources hold right now (mempool or a block), for
        // the attempt backstop below.
        let held_txids: std::collections::HashSet<String> = match &triage_result {
            Ok(status_result) => status_result
                .results
                .iter()
                .filter(|d| d.status == "known" || d.status == "mined")
                .map(|d| d.txid.clone())
                .collect(),
            Err(_) => Default::default(),
        };

        // The proof LAG gate: while it is CLOSED (no header processed yet)
        // nothing can be stored, so nothing is fetched and no attempt is
        // counted, exactly as the reference returns before its loop while
        // `maxAcceptableHeight` is undefined. Said once per pass.
        let proof_gate = self.max_acceptable_proof_height().await?;
        if proof_gate == 0 {
            tracing::info!(
                confirmed = confirmed_txids.len(),
                marker = "proof_gate_closed",
                "synchronize_transaction_statuses: the proof gate is closed (no header has been processed yet); proofs are not fetched this pass"
            );
            return Ok(Vec::new());
        }

        // One attempt per processed header: the reference counts only the
        // header-triggered run (`countsAsAttempt = checkNow`,
        // TaskCheckForProofs.ts:49), so its 144 is about a day of blocks. A
        // pass at a gate already counted (an Arcade MINED word, the 2-hour
        // fallback, a `tick`, a nosend proof) asks but counts nothing.
        let counts_as_attempt = {
            let mut tx = self.write_tx().await?;
            let counted = super::monitor_state::read_proof_attempt_height_on(&mut tx).await?;
            if proof_gate > counted {
                super::monitor_state::write_proof_attempt_height_on(&mut tx, proof_gate).await?;
            }
            commit(tx).await?;
            proof_gate > counted
        };

        let mut results = Vec::new();

        // =====================================================================
        // THE ATTEMPT BACKSTOP (Calgooon/zanaadu-v2#368)
        // The reference counts a proof attempt for ANY req its proof task
        // checks and finds no proof for, and writes the req off once
        // `attempts > unprovenAttemptsLimitMain` (144; 10 off mainnet):
        // wallet-toolbox src/monitor/tasks/TaskCheckForProofs.ts:154-165,235
        // and src/monitor/Monitor.ts:105-106. Before this pass only reqs the
        // triage called mined were counted, so a transaction nobody holds and
        // nobody refused stayed `unproven` with attempts 0 forever.
        //
        // Here: a status pass that does not call the req mined is an attempt
        // (the mined ones are counted by the fetch loop below). Past the
        // limit nothing is written (bsv-stack-lean #66: a count is never
        // evidence, and the tracker's charter there, section 10, allows no
        // age-out word): the req stays in the proof set, every header's pass
        // asks for it again without end, and the hold checks below only say
        // in the log whether a source still holds it. Until 0.7.4 the count
        // wrote the req `invalid` and the transaction `failed` through the
        // release rule when no source held it. A transaction ends without a
        // proof only by the host's explicit act
        // (`StorageSqlx::retire_undeliverable_txid`).
        //
        // A `sending` req is not counted: `send_waiting_transactions` owns
        // its attempts (the re-broadcast budget) until it leaves `sending`.
        // Read from the column: `find_proven_tx_reqs` does not carry these
        // statuses through (`TableProvenTxReq.status` is `Pending` for them).
        // =====================================================================
        let attempts_limit = self.proof_attempts_limit();
        let sending_ids: std::collections::HashSet<i64> = sqlx::query_scalar(
            "SELECT proven_tx_req_id FROM proven_tx_reqs WHERE status = 'sending'",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .collect();
        for req in &reqs {
            if sending_ids.contains(&req.proven_tx_req_id) {
                continue;
            }
            let now = chrono::Utc::now();
            if i64::from(req.attempts) > attempts_limit && !held_txids.contains(&req.txid) {
                // #357's hold checks, no refusing provider: a fresh seen/mined
                // memory row, or a broadcaster's own status read. They decide
                // nothing now; they say which case the log line is.
                let evidence = match self.memory_holds(&req.txid, None, false).await {
                    Some(e) => Some(e),
                    None => {
                        Self::network_holds(services.as_ref(), &req.txid, None, false, false).await
                    }
                };
                match evidence {
                    Some(evidence) => tracing::info!(
                        txid = %req.txid,
                        attempts = req.attempts,
                        evidence = %evidence,
                        marker = "proof_backstop_held",
                        "synchronize_transaction_statuses: past the attempt count and a source holds it; no word, asked again next pass"
                    ),
                    None => tracing::warn!(
                        txid = %req.txid,
                        attempts = req.attempts,
                        marker = "proof_backstop_past_count",
                        "synchronize_transaction_statuses: past the attempt count and no source holds it; a count is no evidence, so no word, asked again next pass"
                    ),
                }
            }
            if confirmed_txids.contains(&req.txid) || !counts_as_attempt {
                continue;
            }
            sqlx::query(
                "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
            )
            .bind(now)
            .bind(req.proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        }

        // Filter reqs to only confirmed transactions
        let confirmed_reqs: Vec<&_> = reqs
            .iter()
            .filter(|r| confirmed_txids.contains(&r.txid))
            .collect();

        if confirmed_reqs.is_empty() {
            tracing::debug!(
                "synchronize_transaction_statuses: no confirmed transactions to fetch proofs for"
            );
            return Ok(results);
        }

        tracing::debug!(
            "synchronize_transaction_statuses: fetching proofs for {} confirmed transactions",
            confirmed_reqs.len()
        );

        for req in &confirmed_reqs {
            let txid = &req.txid;

            // The triage answer already carried this transaction's proof:
            // ingest it here, with no second lookup and no third-party
            // indexer anywhere in the path. Same funnel as the webhook and
            // SSE deliveries (BUMP parse -> compute root -> ChainTracker),
            // so an inline proof that does not validate simply falls through
            // to the fetch path below and can never leave the wallet worse
            // off than before.
            if let Some((merkle_path_bytes, block_height, block_hash)) =
                triage_proofs.get(txid.as_str())
            {
                match self
                    .ingest_merkle_proof(txid, merkle_path_bytes, *block_height, block_hash, None)
                    .await?
                {
                    ProofIngestOutcome::Ingested(status) => {
                        tracing::info!(
                            txid = %txid,
                            block_height = *block_height,
                            marker = "triage_inline_proof",
                            "synchronize_transaction_statuses: proof taken from the status answer, no merkle path fetch"
                        );
                        results.push(status);
                        // No network fetch happened: no throttle to serve.
                        continue;
                    }
                    ProofIngestOutcome::DeferredAboveProcessedHeight {
                        block_height: deferred_height,
                        processed_height,
                    } => {
                        // M19 R1: the header has not aged one cycle yet. No
                        // fetch (it would be deferred too) and no attempt.
                        tracing::debug!(
                            txid = %txid,
                            block_height = deferred_height,
                            processed_height,
                            "synchronize_transaction_statuses: inline triage proof deferred (proof lag), next cycle"
                        );
                        continue;
                    }
                    ProofIngestOutcome::TrackerUnavailable => {
                        // P0-1: nothing can be stored without a tracker, so
                        // a fetch would be refused too. No attempt; the
                        // ingest logged the refusal at error level.
                        continue;
                    }
                    not_ingested => {
                        tracing::warn!(
                            txid = %txid,
                            outcome = ?not_ingested,
                            "synchronize_transaction_statuses: inline triage proof rejected, falling back to merkle path fetch"
                        );
                    }
                }
            }

            // Attempt to get merkle path from services
            match services.get_merkle_path(txid, false).await {
                Ok(proof_result) => {
                    if let Some(ref merkle_path) = proof_result.merkle_path {
                        // Proof found - update proven_tx_req to completed
                        let now = chrono::Utc::now();

                        // Extract block info from header if available
                        let block_height =
                            proof_result.header.as_ref().map(|h| h.height).unwrap_or(0);
                        let block_hash = proof_result
                            .header
                            .as_ref()
                            .map(|h| h.hash.clone())
                            .unwrap_or_default();

                        // merkle_path should be BUMP hex after services.rs conversion.
                        // Decode hex → binary for storage. Fallback to raw bytes if not hex.
                        let merkle_path_bytes = hex::decode(merkle_path)
                            .unwrap_or_else(|_| merkle_path.as_bytes().to_vec());
                        let header_merkle_root =
                            proof_result.header.as_ref().map(|h| h.merkle_root.clone());

                        // Layer 1 validation + storage via the shared ingest
                        // path — the same function the Arcade webhook and
                        // SSE-MINED delivery converge on. Bad proofs (e.g. from
                        // WoC rate limiting) are rejected instead of stored.
                        match self
                            .ingest_merkle_proof(
                                txid,
                                &merkle_path_bytes,
                                block_height,
                                &block_hash,
                                header_merkle_root.as_deref(),
                            )
                            .await?
                        {
                            ProofIngestOutcome::Ingested(status) => {
                                results.push(status);
                            }
                            ProofIngestOutcome::DeferredAboveProcessedHeight {
                                block_height: deferred_height,
                                processed_height,
                            } => {
                                // M19 R1: no attempt counted; the next header
                                // cycle re-presents this proof.
                                tracing::debug!(
                                    txid = %txid,
                                    block_height = deferred_height,
                                    processed_height,
                                    "synchronize_transaction_statuses: fetched proof deferred (proof lag), next cycle"
                                );
                                continue;
                            }
                            ProofIngestOutcome::TrackerUnavailable => {
                                // P0-1: the proof is not the fault, the
                                // missing tracker is. No attempt counted.
                                continue;
                            }
                            not_ingested => {
                                match &not_ingested {
                                    ProofIngestOutcome::DeferredAboveProcessedHeight { .. }
                                    | ProofIngestOutcome::TrackerUnavailable => {
                                        unreachable!()
                                    }
                                    ProofIngestOutcome::InvalidMerkleRoot { computed_root } => {
                                        tracing::warn!(
                                            "synchronize_transaction_statuses: invalid merkle root for txid {} at height {} (computed {}). ChainTracker rejected root. Skipping.",
                                            txid, block_height, computed_root
                                        );
                                    }
                                    ProofIngestOutcome::TrackerError(e) => {
                                        tracing::warn!(
                                            "synchronize_transaction_statuses: ChainTracker error for txid {} at height {}: {}. Skipping.",
                                            txid, block_height, e
                                        );
                                    }
                                    ProofIngestOutcome::InvalidProof(e) => {
                                        tracing::warn!(
                                            "synchronize_transaction_statuses: {} for txid {}. Skipping.",
                                            e, txid
                                        );
                                    }
                                    ProofIngestOutcome::Ingested(_) => unreachable!(),
                                }
                                let attempts = req.attempts + 1;
                                sqlx::query(
                                    "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                                )
                                .bind(attempts)
                                .bind(now)
                                .bind(req.proven_tx_req_id)
                                .execute(&mut *self.writer_conn().await?)
                                .await?;
                                continue;
                            }
                        }
                    } else if counts_as_attempt {
                        // No proof yet: an attempt. No count writes a word
                        // (#368; bsv-stack-lean #66).
                        sqlx::query(
                            "UPDATE proven_tx_reqs SET attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?"
                        )
                        .bind(req.attempts + 1)
                        .bind(chrono::Utc::now())
                        .bind(req.proven_tx_req_id)
                        .execute(&mut *self.writer_conn().await?)
                        .await?;
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to get merkle path for {}: {}", txid, e);
                    // Increment attempts on error
                    let now = chrono::Utc::now();
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                }
            }

            // Throttle between proof requests (TS pattern: msecsWaitPerMerkleProofServiceReq = 500).
            // WoC rate limit is 3 req/sec; 500ms keeps us at 2 req/sec, safely under the limit.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // Repair existing proven_txs that have JSON merkle_paths instead of BUMP binary.
        // This fixes records created before the TSC→BUMP conversion was added in services.rs.
        // Detection: JSON starts with 0x7B ('{'), BUMP binary starts with a varint block height.
        let repair_txs: Vec<(i64, Vec<u8>, i64, String)> = sqlx::query_as(
            "SELECT proven_tx_id, merkle_path, height, txid FROM proven_txs WHERE merkle_path IS NOT NULL AND substr(hex(merkle_path), 1, 2) = '7B' LIMIT 200"
        )
            .fetch_all(self.pool())
            .await?;

        let mut repaired_count = 0u32;
        for (proven_tx_id, merkle_path_bytes, height, txid) in &repair_txs {
            // Check if merkle_path is JSON (starts with '{') — needs conversion
            if merkle_path_bytes.first() != Some(&0x7B) {
                continue; // Already BUMP binary, skip
            }

            let merkle_path_str = match std::str::from_utf8(merkle_path_bytes) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Use stored height if available, otherwise resolve from target
            let block_height = if *height > 0 {
                *height as u32
            } else {
                // Fall back to resolving header from target
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(merkle_path_str) {
                    if let Some(target) = json.get("target").and_then(|t| t.as_str()) {
                        match services.hash_to_header(target).await {
                            Ok(header) => {
                                let now = chrono::Utc::now();
                                sqlx::query(
                                    "UPDATE proven_txs SET height = ?, block_hash = ?, merkle_root = ?, updated_at = ? WHERE proven_tx_id = ?"
                                )
                                    .bind(header.height as i64)
                                    .bind(&header.hash)
                                    .bind(&header.merkle_root)
                                    .bind(now)
                                    .bind(proven_tx_id)
                                    .execute(&mut *self.writer_conn().await?)
                                    .await?;
                                header.height
                            }
                            Err(_) => continue,
                        }
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            };

            // Convert JSON → BUMP binary
            match crate::tsc_proof::tsc_json_to_bump_binary(merkle_path_str, block_height) {
                Some(bump_bytes) => {
                    let now = chrono::Utc::now();
                    sqlx::query(
                        "UPDATE proven_txs SET merkle_path = ?, updated_at = ? WHERE proven_tx_id = ?"
                    )
                        .bind(&bump_bytes)
                        .bind(now)
                        .bind(proven_tx_id)
                        .execute(&mut *self.writer_conn().await?)
                        .await?;
                    repaired_count += 1;
                }
                None => {
                    tracing::warn!(
                        "Failed to convert merkle_path JSON to BUMP for proven_tx {} (txid={})",
                        proven_tx_id,
                        txid
                    );
                }
            }
        }

        if repaired_count > 0 {
            tracing::info!(
                "Repaired {}/{} proven_txs: JSON merkle_path → BUMP binary",
                repaired_count,
                repair_txs.len()
            );
        }

        Ok(results)
    }
    fn proof_attempts_limit(&self) -> i64 {
        if self.get_settings().chain.starts_with("main") {
            PROOF_ATTEMPTS_LIMIT_MAIN
        } else {
            PROOF_ATTEMPTS_LIMIT_TEST
        }
    }
}
