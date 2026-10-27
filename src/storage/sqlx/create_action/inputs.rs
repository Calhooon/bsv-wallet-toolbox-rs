use super::StorageSqlx;
use crate::error::{Error, Result};
use crate::storage::entities::TableOutput;
use crate::storage::sqlx::storage_sqlx::output_from_row;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use sqlx::Row;

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
    let is_no_send = args
        .options
        .as_ref()
        .and_then(|o| o.no_send)
        .unwrap_or(false);

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

            // Keep nullable fields until the stored-input guards have run;
            // TableOutput represents satoshis as a required integer.
            let output_row = sqlx::query(
                "SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
                        satoshis, locking_script, script_length, script_offset,
                        type, spendable, change, derivation_prefix, derivation_suffix,
                        sender_identity_key, custom_instructions, created_at, updated_at
                 FROM outputs WHERE user_id = ? AND txid = ? AND vout = ?",
            )
            .bind(user_id)
            .bind(&txid)
            .bind(vout as i32)
            .fetch_optional(storage.pool())
            .await?;

            let (satoshis, locking_script, output) = if let Some(ref row) = output_row {
                let (out, script) =
                    apply_stored_input_script(conn, row, i, &txid, vout, is_no_send).await?;
                (out.satoshis as u64, script, Some(out))
            } else if let Some(ref beef) = input_beef {
                // Output not in storage - look it up in the input_beef
                if let Some(beef_tx) = beef.find_txid(&txid) {
                    if let Some(tx) = beef_tx.tx() {
                        if let Some(out) = tx.outputs.get(vout as usize) {
                            let sats = out.satoshis.unwrap_or(0);
                            let script = out.locking_script.to_binary();
                            (sats, script, None)
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

            if let Some(ref out) = output {
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

// ts-stack@edf6e03 createAction.ts:972-983: change, stored data, then
// spendability unless noSend. Convert to TableOutput after checking presence.
async fn apply_stored_input_script(
    conn: &mut SqliteConnection,
    row: &SqliteRow,
    vin: usize,
    txid: &str,
    vout: u32,
    is_no_send: bool,
) -> Result<(TableOutput, Vec<u8>)> {
    if row.get::<bool, _>("change") {
        return Err(Error::ValidationError(format!(
            "The inputs[{}] parameter must be an unmanaged input. Change outputs are managed by your wallet.",
            vin
        )));
    }
    let script: Option<Vec<u8>> = row.get("locking_script");
    let offset: i32 = row.get("script_offset");
    let length: i32 = row.get("script_length");
    let script = if script.is_some() {
        script
    } else if offset > 0 && length > 0 && !txid.is_empty() {
        get_locking_script_from_raw_tx(conn, txid, offset as usize, length as usize)
            .await
            .ok()
    } else {
        None
    };
    let satoshis = row.try_get::<Option<i64>, _>("satoshis").ok().flatten();
    let Some(script) = script.filter(|_| satoshis.is_some()) else {
        return Err(Error::ValidationError(format!(
            "The {txid}.{vout} parameter must be output with valid lockingScript and satoshis"
        )));
    };
    if !row.get::<bool, _>("spendable") && !is_no_send {
        return Err(Error::ValidationError(format!(
            "inputs[{}]: output {}:{} is not spendable",
            vin, txid, vout
        )));
    }
    Ok((output_from_row(row), script))
}

/// Reads locking script from rawTx in proven_tx_reqs table.
/// This is needed when the output's locking_script was cleared (set to NULL)
/// because it exceeded maxOutputScript length.
async fn get_locking_script_from_raw_tx(
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
