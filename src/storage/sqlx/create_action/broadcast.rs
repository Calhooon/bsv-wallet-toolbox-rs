use super::*;

/// Do not ask the chain for a proof of a transaction this wallet only just
/// recorded. Mining is monotonic along a chain (an ancestor is older than
/// its descendant), so skipping a young ancestor costs nothing: if it were
/// mined, the next hop back is mined too and answers there instead.
pub(super) const MIN_AGE_FOR_PROOF_LOOKUP_SECS: i64 = 90;

/// Seconds since this wallet first recorded the transaction, if it knows it.
pub(super) async fn local_record_age_secs(conn: &mut SqliteConnection, txid: &str) -> Option<i64> {
    let row: Option<(chrono::DateTime<Utc>,)> = sqlx::query_as(
        r#"
        SELECT created_at FROM transactions WHERE txid = ?
        UNION ALL
        SELECT created_at FROM proven_tx_reqs WHERE txid = ?
        ORDER BY created_at ASC
        LIMIT 1
        "#,
    )
    .bind(txid)
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await
    .ok()
    .flatten();

    row.map(|(created,)| (Utc::now() - created).num_seconds())
}

/// Asks services for a transaction's merkle path and records it.
///
/// A BEEF only ever gets smaller by acquiring bumps and proofs. The broadcast
/// rebuild (`rebuild_beef_for_broadcast`) asks; `create_action` does not
/// (bsv-wallet-cli #44: its door is one step and awaits no service; the
/// monitor's backstop fetches proofs). This is the same fall-through the
/// TypeScript builder takes when storage cannot prove a transaction, and it
/// records the result so the cost is paid once per ancestor rather than on
/// every rebuild.
///
/// The providers and the tracker are asked with no lock held; the store is
/// then one `BEGIN IMMEDIATE` transaction on the writer (the walk runs on a
/// reader and never holds the writer). [`StorageSqlx::ingest_merkle_proof`]
/// does the same and is the path every other proof source uses; this is its
/// twin through the SAME store funnel (`store_validated_proof_on`: the proof LAG
/// gate read on this connection, replace-on-differ, the no-raw guard). A proof
/// the gate DEFERS (the header has not aged one cycle) is not stored and the
/// walk continues raw, exactly as before the fetch existed.
///
/// Returns the BUMP bytes only when the proof validated and was stored.
pub(super) async fn fetch_and_store_merkle_path(
    storage: Option<&StorageSqlx>,
    txid: &str,
) -> Option<Vec<u8>> {
    let storage = storage?;
    // No tracker, no proof (P0-1): a path nobody can check is never stored,
    // so there is nothing to ask the providers for.
    let Some(tracker) = storage.get_chain_tracker().await else {
        tracing::error!(
            txid = %txid,
            marker = "proof_refused_no_chain_tracker",
            "beef walk: no ChainTracker is wired, so a fetched proof could not be checked; none is fetched or stored and the raw-tx leg is walked"
        );
        return None;
    };
    let services = storage.get_services().ok()?;

    let result = match services.get_merkle_path(txid, false).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "merkle path lookup failed");
            return None;
        }
    };

    let bytes = hex::decode(result.merkle_path.as_deref()?).ok()?;
    let header = result.header.as_ref()?;

    let bump = match MerklePath::from_binary(&bytes) {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "merkle path did not parse");
            return None;
        }
    };
    let computed_root = match bump.compute_root(Some(txid)) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "merkle root did not compute");
            return None;
        }
    };

    // Never store a proof the chain rejects. Matches ingest_merkle_proof.
    match tracker
        .is_valid_root_for_height(&computed_root, header.height)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                txid = %txid,
                height = header.height,
                "ChainTracker rejected the merkle root, proof discarded"
            );
            return None;
        }
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "ChainTracker error, proof not stored");
            return None;
        }
    }

    let mut wtx = match storage.write_tx().await {
        Ok(wtx) => wtx,
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "could not open the write transaction for the proof");
            return None;
        }
    };
    let stored = match store_validated_proof_on(
        &mut wtx,
        &ValidatedProofRow {
            txid,
            block_height: header.height,
            checked_root: &computed_root,
            block_hash: &header.hash,
            merkle_root: &header.merkle_root,
            merkle_path: &bytes,
            idx: bump_leaf_index(&bump, txid),
            raw_tx: None,
        },
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "could not record proven tx");
            return None;
        }
    };
    let proven_tx_id = match stored {
        StoreProofOutcome::Stored { proven_tx_id, .. }
        | StoreProofOutcome::Unchanged { proven_tx_id } => proven_tx_id,
        StoreProofOutcome::Deferred {
            block_height,
            processed_height,
        } => {
            tracing::debug!(
                txid = %txid,
                block_height,
                processed_height,
                marker = "proof_deferred_above_processed_height",
                "beef walk: the fetched proof is above the proof gate; not stored, the raw-tx leg is walked"
            );
            return None;
        }
        StoreProofOutcome::NoRawTx => {
            tracing::debug!(txid = %txid, "beef walk: no raw bytes in storage for the proof; not stored");
            return None;
        }
    };
    let now = Utc::now();

    // A proven transaction is completed by definition. Same two updates
    // ingest_merkle_proof makes, so a later monitor pass finds nothing to do.
    let _ = sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(proven_tx_id)
    .bind(now)
    .bind(txid)
    .execute(&mut *wtx)
    .await;

    let _ = sqlx::query(
        "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(proven_tx_id)
    .bind(now)
    .bind(txid)
    .execute(&mut *wtx)
    .await;

    if let Err(e) = wtx.commit().await {
        tracing::debug!(txid = %txid, error = %e, "could not commit the proven tx");
        return None;
    }

    tracing::info!(
        txid = %txid,
        height = header.height,
        "recorded merkle proof during BEEF assembly, the ancestor chain terminates here"
    );

    Some(bytes)
}

/// Rebuild BEEF from current DB state for the given input txids.
///
/// Used by `send_waiting_transactions` to build fresh BEEF at broadcast time,
/// matching Go's `GetBEEFForTxIDs` approach. Instead of just upgrading proofs
/// on txs already in the stored BEEF (compact_stored_beef), this rebuilds the
/// full ancestor chain from scratch using current DB state.
///
/// # Arguments
/// * `conn` - SQLite connection
/// * `input_txids` - Input txids extracted from the raw transaction being broadcast
/// * `storage` - Optional storage backend for network fallback
///
/// # Returns
/// A fully-built `Beef` containing all ancestors with their merkle proofs.
pub(in crate::storage::sqlx) async fn rebuild_beef_for_broadcast(
    conn: &mut SqliteConnection,
    writer: &SqlitePool,
    input_txids: &[String],
    storage: Option<&StorageSqlx>,
) -> Result<Beef> {
    let mut beef = Beef::new();
    let mut processed_txids: HashSet<String> = HashSet::new();
    let mut pending_txids: Vec<(String, usize)> = Vec::new();

    for txid in input_txids {
        if !processed_txids.contains(txid) && !pending_txids.iter().any(|(t, _)| t == txid) {
            pending_txids.push((txid.clone(), 0));
        }
    }

    if pending_txids.is_empty() {
        return Ok(beef);
    }

    let root_txids: Vec<String> = pending_txids.iter().map(|(t, _)| t.clone()).collect();

    beef_bfs_walk(
        conn,
        Some(writer),
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        storage,
        None, // the walk reads every stored root through the storage's own tracker
    )
    .await?;

    prune_beef_to_roots(&mut beef, &root_txids);

    Ok(beef)
}

/// Attempts to fetch a missing transaction from the network via wallet services.
///
/// This is the network fallback for BEEF construction. When a transaction is not
/// found in local storage (proven_txs, transactions, or proven_tx_reqs), we try
/// to fetch it from WoC/ARC via the WalletServices trait. If found, the raw tx
/// is stored locally for future use.
///
/// This matches the Go toolbox's `TxGetterFcn` callback and the TS toolbox's
/// `getProvenOrRawTxFromServices()` behavior.
///
/// # Arguments
/// * `storage` - Optional storage backend (provides services access; the
///   fetched tx is stored through its writer, after the fetch)
/// * `txid` - The transaction ID to fetch
///
/// # Returns
/// * `Ok(Some(BeefTxData))` - If the tx was fetched from the network
/// * `Ok(None)` - If services are unavailable or the tx was not found
pub(super) async fn try_network_fallback(
    storage: Option<&StorageSqlx>,
    txid: &str,
) -> Result<Option<BeefTxData>> {
    let storage = match storage {
        Some(s) => s,
        None => return Ok(None),
    };

    let services: std::sync::Arc<dyn WalletServices> = match storage.get_services() {
        Ok(s) => s,
        Err(_) => {
            tracing::debug!(txid = %txid, "No services available for network fallback");
            return Ok(None);
        }
    };

    tracing::info!(txid = %txid, "Attempting network fallback for missing transaction");

    // Try to get the raw transaction from the network
    match services.get_raw_tx(txid, false).await {
        Ok(result) => {
            if let Some(ref raw_tx) = result.raw_tx {
                tracing::info!(
                    txid = %txid,
                    provider = %result.name,
                    size = raw_tx.len(),
                    "Fetched raw tx from network"
                );

                // Store locally for future use (best effort: a busy writer
                // or a failed insert is ignored)
                let now = Utc::now();
                if let Ok(mut writer) = storage.writer_conn().await {
                    let _ = sqlx::query(
                        r#"
                        INSERT OR IGNORE INTO proven_tx_reqs
                            (txid, status, raw_tx, history, notify, created_at, updated_at)
                        VALUES (?, 'unmined', ?, '{}', '{}', ?, ?)
                        "#,
                    )
                    .bind(txid)
                    .bind(raw_tx)
                    .bind(now)
                    .bind(now)
                    .execute(&mut *writer)
                    .await;
                }

                // Also try to get the merkle proof from the network, and record
                // it: a proof fetched and thrown away has to be fetched again on
                // every later spend.
                let merkle_path = fetch_and_store_merkle_path(Some(storage), txid).await;

                return Ok(Some(BeefTxData {
                    raw_tx: raw_tx.clone(),
                    merkle_path,
                }));
            }

            if result.could_not_look {
                tracing::warn!(
                    txid = %txid,
                    error = ?result.error,
                    "Network fallback could not look (not a 'no such transaction')"
                );
            } else {
                tracing::debug!(txid = %txid, "Network fallback: no explorer has the transaction");
            }
        }
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "Network fallback failed");
        }
    }

    Ok(None)
}
#[cfg(test)]
#[path = "tests/broadcast.rs"]
mod tests;
