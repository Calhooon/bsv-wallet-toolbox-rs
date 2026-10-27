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
            let output_row = storage
                .find_output_row_by_outpoint(user_id, &txid, vout)
                .await?;

            let (satoshis, locking_script, output) = if let Some(ref row) = output_row {
                let (out, script) = apply_stored_input_script(conn, row, i, &txid, vout).await?;
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

// ts-stack@edf6e03 createAction.ts:972-980: change, then stored data.
// Spendability is checked when marking inputs spent (:399-436, called at :503).
// Convert to TableOutput after checking presence.
async fn apply_stored_input_script(
    conn: &mut SqliteConnection,
    row: &SqliteRow,
    vin: usize,
    txid: &str,
    vout: u32,
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
    Ok((output_from_row(row)?, script))
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

// ts-stack@edf6e03 createAction.ts:399-436 re-reads known inputs before
// marking them spent. The :982 gate is disabled by default at :61, so this
// live check has no noSend exemption. A txid names a competing spend even
// when its transaction is failed; a reservation without a txid falls through.
async fn check_stored_input_spendable(
    conn: &mut SqliteConnection,
    input: &ExtendedInput,
    output_id: i64,
) -> Result<()> {
    let (spendable, spent_by): (bool, Option<i64>) =
        sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(output_id)
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Internal(format!("missing outputId {output_id}")))?;
    if let Some(spender) = spent_by {
        let competing_txid: Option<String> =
            sqlx::query_scalar("SELECT txid FROM transactions WHERE transaction_id = ?")
                .bind(spender)
                .fetch_optional(&mut *conn)
                .await?
                .flatten();
        if let Some(txid) = competing_txid.filter(|txid| !txid.is_empty()) {
            // :503-509 requires double-spend review. Keep the existing error
            // shape, with the reference's review text and the competing txid.
            return Err(Error::InvalidOperation(format!(
                "Undelayed createAction or signAction results require review. doubleSpend: {txid}"
            )));
        }
    }
    if !spendable {
        return Err(Error::ValidationError(format!(
            "The inputs[{}] parameter must be spendable output. output {}:{} appears to have been spent (spendable=false).",
            input.vin, input.txid, input.vout
        )));
    }
    Ok(())
}

/// Checks a known input's current reservation, then marks it spent.
pub(super) async fn update_output_spent(
    conn: &mut SqliteConnection,
    input: &ExtendedInput,
    spent_by: i64,
) -> Result<()> {
    let Some(output) = input.output.as_ref() else {
        return Ok(());
    };
    check_stored_input_spendable(conn, input, output.output_id).await?;
    let now = Utc::now();

    sqlx::query(
        r#"
        UPDATE outputs
        SET spendable = 0, spent_by = ?, spending_description = ?, updated_at = ?
        WHERE output_id = ?
        "#,
    )
    .bind(spent_by)
    .bind(input.input_description.as_deref())
    .bind(now)
    .bind(output.output_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}
#[cfg(test)]
#[path = "tests/inputs.rs"]
mod tests;
