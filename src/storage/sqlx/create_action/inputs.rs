use super::*;

// =============================================================================
// Internal Types
// =============================================================================

/// Extended input with additional context for transaction creation.
#[derive(Debug, Clone)]
pub(super) struct ExtendedInput {
    pub(super) vin: u32,
    pub(super) txid: String,
    pub(super) vout: u32,
    pub(super) satoshis: u64,
    pub(super) locking_script: Vec<u8>,
    pub(super) unlocking_script_length: u32,
    pub(super) input_description: Option<String>,
    /// Associated output record if this input spends a known output.
    pub(super) output: Option<TableOutput>,
}

/// Validates and extends input specifications, looking up associated outputs.
/// For external inputs not in storage, looks up source output info from the input_beef.
pub(super) async fn validate_and_extend_inputs(
    storage: &StorageSqlx,
    conn: &mut SqliteConnection,
    user_id: i64,
    args: &bsv_rs::wallet::CreateActionArgs,
) -> Result<Vec<ExtendedInput>> {
    use bsv_rs::transaction::Beef;

    let mut extended = Vec::new();

    // Parse input_beef if provided (needed for external inputs)
    let input_beef = if let Some(ref beef_bytes) = args.input_beef {
        Beef::from_binary(beef_bytes).ok()
    } else {
        None
    };

    if let Some(ref inputs) = args.inputs {
        for (i, input) in inputs.iter().enumerate() {
            let txid = hex::encode(input.outpoint.txid);
            let vout = input.outpoint.vout;

            // Try to find the output being spent in storage first
            let output = storage
                .find_output_by_outpoint(user_id, &txid, vout)
                .await?;

            let (satoshis, locking_script) = if let Some(ref out) = output {
                let script = if let Some(ref s) = out.locking_script {
                    s.clone()
                } else if out.script_offset > 0 && out.script_length > 0 && !out.txid.is_empty() {
                    // Script was cleared from output (too long), read from rawTx
                    get_locking_script_from_raw_tx(
                        &mut *conn,
                        &out.txid,
                        out.script_offset as usize,
                        out.script_length as usize,
                    )
                    .await
                    .unwrap_or_default()
                } else {
                    vec![]
                };
                (out.satoshis as u64, script)
            } else if let Some(ref beef) = input_beef {
                // Output not in storage - look it up in the input_beef
                if let Some(beef_tx) = beef.find_txid(&txid) {
                    if let Some(tx) = beef_tx.tx() {
                        if let Some(out) = tx.outputs.get(vout as usize) {
                            let sats = out.satoshis.unwrap_or(0);
                            let script = out.locking_script.to_binary();
                            (sats, script)
                        } else {
                            return Err(Error::ValidationError(format!(
                                "inputs[{}]: output {}:{} not found in BEEF transaction",
                                i, txid, vout
                            )));
                        }
                    } else {
                        return Err(Error::ValidationError(format!(
                            "inputs[{}]: transaction {} found in BEEF but has no parsed data",
                            i, txid
                        )));
                    }
                } else {
                    return Err(Error::ValidationError(format!(
                        "inputs[{}]: output {}:{} not found in storage or input_beef",
                        i, txid, vout
                    )));
                }
            } else {
                // No beef provided and output not in storage
                return Err(Error::ValidationError(format!(
                    "inputs[{}]: output {}:{} not found in storage. Provide input_beef for external inputs.",
                    i, txid, vout
                )));
            };

            // Check that output is spendable
            if let Some(ref out) = output {
                if !out.spendable {
                    return Err(Error::ValidationError(format!(
                        "inputs[{}]: output {}:{} is not spendable",
                        i, txid, vout
                    )));
                }
                if out.change {
                    return Err(Error::ValidationError(format!(
                        "inputs[{}]: cannot spend change output {}:{} directly. Change is managed by the wallet.",
                        i, txid, vout
                    )));
                }
                // A failed creator's output is a phantom, whatever its flag
                // says (bsv-wallet-toolbox-rs #42 point 4).
                let creator: Option<String> =
                    sqlx::query_scalar("SELECT status FROM transactions WHERE transaction_id = ?")
                        .bind(out.transaction_id)
                        .fetch_optional(&mut *conn)
                        .await?;
                if creator.as_deref() == Some("failed") {
                    return Err(Error::FailedAncestor { txid });
                }
            }

            let unlocking_script_length = input
                .unlocking_script_length
                .or_else(|| input.unlocking_script.as_ref().map(|s| s.len() as u32))
                .ok_or_else(|| {
                    Error::ValidationError(format!(
                        "inputs[{}]: unlockingScript or unlockingScriptLength required",
                        i
                    ))
                })?;

            extended.push(ExtendedInput {
                vin: i as u32,
                txid,
                vout,
                satoshis,
                locking_script,
                unlocking_script_length,
                input_description: Some(input.input_description.clone()),
                output,
            });
        }
    }

    Ok(extended)
}

/// Reads locking script from rawTx in proven_tx_reqs table.
/// This is needed when the output's locking_script was cleared (set to NULL)
/// because it exceeded maxOutputScript length.
pub(super) async fn get_locking_script_from_raw_tx(
    conn: &mut SqliteConnection,
    txid: &str,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>> {
    // First try proven_tx_reqs (for pending transactions)
    let row = sqlx::query("SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    if let Some(row) = row {
        let raw_tx: Vec<u8> = row.get("raw_tx");
        if offset + length <= raw_tx.len() {
            return Ok(raw_tx[offset..offset + length].to_vec());
        }
    }

    // Then try transactions table
    let row = sqlx::query("SELECT raw_tx FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    if let Some(row) = row {
        let raw_tx: Option<Vec<u8>> = row.get("raw_tx");
        if let Some(raw_tx) = raw_tx {
            if offset + length <= raw_tx.len() {
                return Ok(raw_tx[offset..offset + length].to_vec());
            }
        }
    }

    // Finally try proven_txs table
    let row = sqlx::query("SELECT raw_tx FROM proven_txs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    if let Some(row) = row {
        let raw_tx: Vec<u8> = row.get("raw_tx");
        if offset + length <= raw_tx.len() {
            return Ok(raw_tx[offset..offset + length].to_vec());
        }
    }

    Err(Error::TransactionError(format!(
        "Could not read locking script from rawTx for txid {}",
        txid
    )))
}

/// Marks an output as spent.
pub(super) async fn update_output_spent(
    conn: &mut SqliteConnection,
    output_id: i64,
    spent_by: i64,
    spending_description: Option<&str>,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query(
        r#"
        UPDATE outputs
        SET spendable = 0, spent_by = ?, spending_description = ?, updated_at = ?
        WHERE output_id = ?
        "#,
    )
    .bind(spent_by)
    .bind(spending_description)
    .bind(now)
    .bind(output_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}
#[cfg(test)]
#[path = "tests/inputs.rs"]
mod tests;
