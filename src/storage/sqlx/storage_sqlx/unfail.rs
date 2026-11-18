use super::{
    FindProvenTxReqsArgs, ProvenTxReqStatus, Result, Row, StorageSqlx, UtxoVerdict, WalletServices,
    WalletStorageReader,
};

/// How long a broadcast-memory row counts as another source holding a
/// transaction one broadcaster refused (Calgooon/zanaadu-v2#357).
const MEMORY_EVIDENCE_MAX_AGE_SECS: i64 = 2 * 60 * 60;

/// Private helpers for the `MonitorStorage::un_fail` recovery path.
impl StorageSqlx {
    /// THE FINAL-VERSUS-TRANSIENT RULE for one broadcaster's refusal
    /// (Calgooon/zanaadu-v2#357), the memory half: the broadcast memory's
    /// evidence that a source other than `refusing` holds `txid`. Another
    /// broadcaster's `seen` / `mined` row or the network's / chain's
    /// `seen` / `mined` row always counts; another broadcaster's bare
    /// `accepted` row counts only when `accepted_counts` (a plain REJECTED:
    /// the transaction is valid to a broadcaster that took it). A conflict
    /// verdict passes `false`: two broadcasters can each take one side of a
    /// double spend, so only the network's word overrules it.
    pub(super) async fn memory_holds(
        &self,
        txid: &str,
        refusing: Option<&str>,
        accepted_counts: bool,
    ) -> Option<String> {
        use crate::services::broadcast_memory::{is_global_provider, BroadcastStatus};
        let records = match self
            .broadcast_records(None, std::slice::from_ref(&txid.to_string()))
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::debug!(txid = %txid, error = %e, "broadcast memory unreadable: no memory evidence");
                return None;
            }
        };
        // A memory row is evidence only while fresh: a SEEN from hours ago
        // says nothing about a mempool that has since evicted the
        // transaction, and an unbounded row would hold an evicted
        // transaction (and its inputs) forever. Past the window only the
        // live read below decides.
        let fresh_after =
            chrono::Utc::now() - chrono::Duration::seconds(MEMORY_EVIDENCE_MAX_AGE_SECS);
        records.into_iter().find_map(|r| {
            if Some(r.provider.as_str()) == refusing || r.seen_at < fresh_after {
                return None;
            }
            let status = r.ladder_status()?;
            let counts = status.is_network_evidence()
                || (accepted_counts
                    && status == BroadcastStatus::Accepted
                    && !is_global_provider(&r.provider));
            counts.then(|| format!("{} {}", r.provider, status.as_str()))
        })
    }

    /// The rule's live half: whether the network holds `txid` by any source
    /// but `refusing`, asked now. `ask_status`: the status sources
    /// (`get_status_for_txids`, a `known` / `mined` answer under its merge
    /// rule). Always: every broadcaster's own status read
    /// (`get_broadcaster_statuses`, `seen` / `mined` only; a broadcaster's
    /// "accepted" is not the network's word). `ask_proof`: a merkle path,
    /// which `Services` serves only after the chain tracker accepted its
    /// root. A source that cannot answer is no evidence either way.
    pub(super) async fn network_holds(
        services: &dyn WalletServices,
        txid: &str,
        refusing: Option<&str>,
        ask_status: bool,
        ask_proof: bool,
    ) -> Option<String> {
        let txids = [txid.to_string()];
        if ask_status {
            if let Ok(result) = services.get_status_for_txids(&txids, false).await {
                if let Some(detail) = result
                    .results
                    .iter()
                    .find(|d| d.txid == txid && (d.status == "known" || d.status == "mined"))
                {
                    return Some(format!("status sources {}", detail.status));
                }
            }
        }
        for (provider, status) in services.get_broadcaster_statuses(txid).await {
            if Some(provider.as_str()) != refusing && status.is_network_evidence() {
                return Some(format!("{} {}", provider, status.as_str()));
            }
        }
        if ask_proof {
            if let Ok(result) = services.get_merkle_path(txid, false).await {
                if result.merkle_path.is_some() {
                    return Some("a merkle path the chain tracker accepted".to_string());
                }
            }
        }
        None
    }

    /// Restore a false-failed transaction whose chain evidence says it is
    /// known/mined: req → 'unmined' (attempts 0, so
    /// `synchronize_transaction_statuses` re-polls it and completes it with
    /// a proof), tx → 'unproven', created outputs re-validated against the
    /// chain (is_utxo) before re-enabling, and the tx's consumed inputs
    /// re-marked spent (they were released when the tx was wrongly failed).
    /// Shared by the Phase A canary and Phase B explicit-'unfail' requests.
    async fn recover_false_failed_req(
        &self,
        req_id: i64,
        txid: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let services = self.get_services()?;

        // The chain evidence that recovered it, through the one write that
        // applies a take (`competitor_proof::apply_take_on`, the rows rule
        // that lifts `failed`; 'completed' rows never move backward), the
        // attempts reset in the same write. Nothing raised while a
        // competitor's checked proof stands.
        let check = self.competitor_check(txid, None).await?;
        let mut tx = self.write_tx().await?;
        let applied = super::super::competitor_proof::apply_take_on(
            &mut tx,
            &check,
            &[(
                crate::services::broadcast_memory::BROADCAST_PROVIDER_CHAIN,
                crate::services::broadcast_memory::BROADCAST_STATUS_SEEN,
            )],
            super::super::competitor_proof::TakeRows::LiftFailed,
            now,
        )
        .await?;
        if !applied.applied() {
            super::commit(tx).await?;
            return Ok(());
        }
        sqlx::query(
            "UPDATE proven_tx_reqs SET attempts = 0, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(now)
        .bind(req_id)
        .execute(&mut *tx)
        .await?;
        super::commit(tx).await?;

        // Restore outputs of this transaction, but validate each against
        // chain to avoid creating ghost UTXOs (outputs already spent
        // on-chain).
        let output_rows = sqlx::query(
            "SELECT output_id, vout, locking_script FROM outputs WHERE txid = ? AND spendable = 0",
        )
        .bind(txid)
        .fetch_all(self.pool())
        .await?;

        let mut restored = 0u32;
        for row in &output_rows {
            let output_id: i64 = row.get("output_id");
            let vout: i32 = row.get("vout");
            let locking_script: Option<Vec<u8>> = row.get("locking_script");
            let script = locking_script.as_deref().unwrap_or(&[]);
            match services.is_utxo(txid, vout as u32, script).await {
                UtxoVerdict::UnspentHint => {
                    sqlx::query(
                        "UPDATE outputs SET spendable = 1, updated_at = ? WHERE output_id = ?",
                    )
                    .bind(now)
                    .bind(output_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;
                    restored += 1;
                }
                UtxoVerdict::Spent | UtxoVerdict::SpentHint => {
                    tracing::debug!(
                        "un_fail: output {}:{} is in no unspent set, skipping",
                        txid,
                        vout
                    );
                }
                UtxoVerdict::Unknown => {
                    tracing::warn!(
                        "un_fail: output {}:{} could not be looked up (not a spend); left unspendable",
                        txid,
                        vout
                    );
                }
            }
        }

        tracing::info!(
            "un_fail: restored transaction {} ({}/{} outputs validated as UTXOs)",
            txid,
            restored,
            output_rows.len()
        );
        Ok(())
    }

    /// Backoff re-stamp for a canary candidate with no chain evidence:
    /// attempts+1 and a fresh updated_at, no status change.
    async fn restamp_false_fail_candidate(
        &self,
        req_id: i64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE proven_tx_reqs SET attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(now)
        .bind(req_id)
        .execute(&mut *self.writer_conn().await?)
        .await?;
        Ok(())
    }
}

impl StorageSqlx {
    pub(super) async fn un_fail_impl(&self) -> Result<()> {
        let now = chrono::Utc::now();

        // ---- Phase A: auto-unfail canary (the producer) ----
        //
        // Canary invariant: `failed` means never-on-chain. Every failed
        // transaction with an 'invalid'/'doubleSpend' req is periodically
        // re-verified against the chain, UNBOUNDED — a tx the network still
        // reports known/mined must never stop being re-checked. Backoff:
        // hourly for the first 24 checks, daily after (attempts counts
        // canary re-stamps) — bounded provider load without ever abandoning
        // a row.
        //
        // Without this sweep the explicit-'unfail' consumer below is dead
        // code: every demotion path writes 'invalid'/'doubleSpend', never
        // 'unfail', so a false-fail (a broadcast reject during an index
        // disagreement window — the 2026-07-28 btc-relay fuel-coin
        // incident) stayed `failed` forever while mined on-chain.
        // Production reference: rust-wallet-infra monitor auto-unfail
        // canary; TS TaskReviewDoubleSpends covers only the doubleSpend
        // half of this.
        //
        // julianday() on BOTH sides of the time compares: updated_at holds
        // RFC3339 'T' stamps (sqlx chrono) but CURRENT_TIMESTAMP defaults
        // write space-format — a raw string compare silently breaks the
        // backoff windows.
        //
        // The LEFT JOIN + OR arm: a 'doubleSpend' req whose transactions
        // row missed its 'failed' write (partial batch) must still be
        // re-checked.
        let candidates = sqlx::query(
            "SELECT DISTINCT r.proven_tx_req_id, r.txid \
             FROM proven_tx_reqs r \
             LEFT JOIN transactions t ON t.txid = r.txid \
             WHERE r.status IN ('invalid', 'doubleSpend') \
               AND (t.status = 'failed' OR r.status = 'doubleSpend') \
               AND julianday(r.updated_at) < julianday(?) \
               AND (r.attempts < 24 OR julianday(r.updated_at) < julianday(?)) \
             ORDER BY r.updated_at ASC \
             LIMIT 20",
        )
        .bind(now - chrono::Duration::hours(1))
        .bind(now - chrono::Duration::days(1))
        .fetch_all(self.pool())
        .await?;

        // Both phases are chain-evidence driven; with no work, don't
        // require services to be configured (parity with the pre-canary
        // no-op path).
        let unfail_reqs = self
            .find_proven_tx_reqs(FindProvenTxReqsArgs {
                status: Some(vec![ProvenTxReqStatus::Unfail]),
                ..Default::default()
            })
            .await?;
        if candidates.is_empty() && unfail_reqs.is_empty() {
            return Ok(());
        }
        let services = self.get_services()?;

        for row in &candidates {
            let req_id: i64 = row.get("proven_tx_req_id");
            let txid: String = row.get("txid");
            if txid.is_empty() {
                continue;
            }
            // EVERY status source, not only the one that refused it
            // (Calgooon/zanaadu-v2#357): the status sources first, then each
            // broadcaster's own status read and a tracker-checked merkle
            // path. A failed transaction is usually failed on ONE
            // broadcaster's word; the one that accepted it is the likeliest
            // to know better.
            let status_word = match services
                .get_status_for_txids(std::slice::from_ref(&txid), false)
                .await
            {
                Ok(result) => result
                    .results
                    .iter()
                    .find(|d| d.txid == txid && (d.status == "known" || d.status == "mined"))
                    .map(|d| format!("status sources {}", d.status)),
                // Provider error: no evidence from these sources.
                Err(_) => None,
            };
            let evidence = match status_word {
                Some(word) => Some(word),
                None => Self::network_holds(services.as_ref(), &txid, None, false, true).await,
            };
            match evidence {
                Some(evidence) => {
                    self.recover_false_failed_req(req_id, &txid, now).await?;
                    tracing::warn!(
                        "un_fail canary: false-failed tx is on network ({}) — recovered txid={}",
                        evidence,
                        txid
                    );
                }
                None => {
                    // No source holds it — factually failed (so far).
                    // Re-stamp for the backoff; stays invalid, stays
                    // watched, forever.
                    self.restamp_false_fail_candidate(req_id, now).await?;
                }
            }
        }

        // ---- Phase B: explicit 'unfail' requests (the consumer) ----
        let reqs = unfail_reqs;

        if reqs.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "un_fail: processing {} transactions marked for unfail",
            reqs.len()
        );

        for req in &reqs {
            // Check if transaction has a merkle path on-chain
            match services.get_merkle_path(&req.txid, false).await {
                Ok(result) if result.merkle_path.is_some() => {
                    self.recover_false_failed_req(req.proven_tx_req_id, &req.txid, now)
                        .await?;
                }
                Ok(_) => {
                    // Positive network answer, no proof — back to 'invalid'.
                    // NOT terminal: the Phase A canary keeps re-checking
                    // 'invalid' reqs whose tx is failed, so a proof that
                    // shows up later still recovers it.
                    sqlx::query(
                        "UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE proven_tx_req_id = ?"
                    )
                    .bind(now)
                    .bind(req.proven_tx_req_id)
                    .execute(&mut *self.writer_conn().await?)
                    .await?;

                    tracing::debug!(
                        "un_fail: marked {} as invalid (not found on chain)",
                        req.txid
                    );
                }
                Err(e) => {
                    // Provider error — no evidence, no state change: hold at
                    // 'unfail' for the next pass. Terminalizing on an outage
                    // is the incident class where a multi-day provider
                    // outage turns a recovery request permanent-'invalid'.
                    tracing::warn!(
                        "un_fail: provider error for {} — holding at 'unfail': {}",
                        req.txid,
                        e
                    );
                }
            }
        }

        Ok(())
    }
}
