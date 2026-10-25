use crate::error::{Error, Result};
use bsv_rs::transaction::Beef;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;

// =============================================================================
// BEEF Construction
// =============================================================================

/// Data for a transaction to include in BEEF.
pub(in crate::storage::sqlx) struct BeefTxData {
    pub(in crate::storage::sqlx) raw_tx: Vec<u8>,
    pub(in crate::storage::sqlx) merkle_path: Option<Vec<u8>>,
}

/// Parses a raw transaction and extracts the txids of its inputs.
///
/// # Arguments
/// * `raw_tx` - The raw transaction bytes
///
/// # Returns
/// * `Ok(Vec<String>)` - List of input txids (hex strings)
/// * `Err` - If parsing fails
pub(in crate::storage::sqlx) fn parse_input_txids(raw_tx: &[u8]) -> Result<Vec<String>> {
    let mut txids = Vec::new();
    let mut offset = 4; // Skip version

    if raw_tx.len() < 5 {
        return Ok(txids);
    }

    // Read input count
    let input_count = read_var_int_for_beef(raw_tx, &mut offset)?;

    for _ in 0..input_count {
        if offset + 32 > raw_tx.len() {
            break;
        }

        // Read the 32-byte txid (little-endian in transaction, we need to reverse for display)
        let mut txid_bytes = raw_tx[offset..offset + 32].to_vec();
        txid_bytes.reverse();
        let txid = hex::encode(&txid_bytes);

        // Skip past txid (32) + vout (4)
        offset += 36;

        // Skip script
        let script_len = read_var_int_for_beef(raw_tx, &mut offset)? as usize;
        offset += script_len;

        // Skip sequence (4 bytes)
        offset += 4;

        // Don't include coinbase inputs (all zeros)
        if txid != "0000000000000000000000000000000000000000000000000000000000000000" {
            txids.push(txid);
        }
    }

    Ok(txids)
}

/// Reads a variable-length integer from transaction data.
pub(in crate::storage::sqlx) fn read_var_int_for_beef(
    data: &[u8],
    offset: &mut usize,
) -> Result<u64> {
    if *offset >= data.len() {
        return Err(Error::ValidationError(
            "Unexpected end of transaction data".to_string(),
        ));
    }

    let first = data[*offset];
    *offset += 1;

    if first < 0xfd {
        Ok(first as u64)
    } else if first == 0xfd {
        if *offset + 2 > data.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let val = u16::from_le_bytes([data[*offset], data[*offset + 1]]) as u64;
        *offset += 2;
        Ok(val)
    } else if first == 0xfe {
        if *offset + 4 > data.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let val = u32::from_le_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]) as u64;
        *offset += 4;
        Ok(val)
    } else {
        if *offset + 8 > data.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let val = u64::from_le_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
            data[*offset + 4],
            data[*offset + 5],
            data[*offset + 6],
            data[*offset + 7],
        ]);
        *offset += 8;
        Ok(val)
    }
}

/// Fetches transaction raw bytes and merkle proof from storage.
///
/// Checks multiple sources in priority order:
/// 1. `proven_txs` table - transactions with confirmed merkle proofs
/// 2. `transactions` table - may have raw_tx and/or input_beef with ancestor data
/// 3. `proven_tx_reqs` table - may have input_beef for pending proof requests
///
/// The key insight is that even without merkle proofs, we can construct valid
/// BEEFs for spending by chaining raw transactions. The `input_beef` column
/// stores the complete BEEF that was provided during internalize_action,
/// which contains all ancestor transactions needed for SPV validation.
///
/// # Arguments
/// * `storage` - The storage backend to query
/// * `txid` - The transaction ID (hex string)
///
/// A transaction this wallet holds as `failed` with no proof is refused
/// (`Error::FailedAncestor`): its bytes never enter a BEEF, so nothing
/// built on a dead lineage is assembled or broadcast (bsv-wallet-toolbox-rs
/// #42 point 4). A proof in `proven_txs` outranks the word.
///
/// # Returns
/// * `Ok(Some(BeefTxData))` - Transaction data if found
/// * `Ok(None)` - If transaction not found in any table
/// * `Err(Error::FailedAncestor)` - If the transaction is `failed` here
pub(in crate::storage::sqlx) async fn get_tx_with_proof(
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Option<BeefTxData>> {
    // First try proven_txs table - these have merkle proofs
    let proven_row = sqlx::query(
        r#"
        SELECT raw_tx, merkle_path
        FROM proven_txs
        WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = proven_row {
        let raw_tx: Vec<u8> = row.get("raw_tx");
        let merkle_path: Vec<u8> = row.get("merkle_path");

        return Ok(Some(BeefTxData {
            raw_tx,
            merkle_path: Some(merkle_path),
        }));
    }

    // Try transactions table - check both raw_tx and input_beef. A row
    // that is not failed is read before a failed one (the same txid under
    // two users).
    let tx_row = sqlx::query(
        r#"
        SELECT raw_tx, input_beef, status
        FROM transactions
        WHERE txid = ?
        ORDER BY status = 'failed'
        LIMIT 1
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = tx_row {
        let status: String = row.get("status");
        if status == "failed" {
            return Err(Error::FailedAncestor {
                txid: txid.to_string(),
            });
        }
        let raw_tx: Option<Vec<u8>> = row.get("raw_tx");
        let input_beef: Option<Vec<u8>> = row.get("input_beef");

        // If we have input_beef, try to extract the transaction and any proof from it
        if let Some(beef_bytes) = input_beef {
            if let Ok(beef) = Beef::from_binary(&beef_bytes) {
                // Check if BEEF contains a merkle proof for this txid
                if let Some(bump) = beef.find_bump(txid) {
                    // Found merkle proof in stored BEEF!
                    if let Some(beef_tx) = beef.find_txid(txid) {
                        // Use raw_tx() to preserve original bytes — do NOT
                        // re-serialize via tx().to_binary() as it can produce
                        // different bytes for non-standard scripts (PushDrop etc.)
                        if let Some(raw) = beef_tx.raw_tx() {
                            return Ok(Some(BeefTxData {
                                raw_tx: raw.to_vec(),
                                merkle_path: Some(bump.to_binary()),
                            }));
                        }
                    }
                }

                // No proof in BEEF, but we can still get the raw transaction
                if let Some(beef_tx) = beef.find_txid(txid) {
                    if let Some(raw) = beef_tx.raw_tx() {
                        return Ok(Some(BeefTxData {
                            raw_tx: raw.to_vec(),
                            merkle_path: None,
                        }));
                    }
                }
            }
        }

        // Fall back to raw_tx if input_beef didn't have what we need
        if let Some(raw_tx) = raw_tx {
            return Ok(Some(BeefTxData {
                raw_tx,
                merkle_path: None, // Unproven transaction
            }));
        }
    }

    // Also check proven_tx_reqs table - may have input_beef for pending requests
    let req_row = sqlx::query(
        r#"
        SELECT raw_tx, input_beef
        FROM proven_tx_reqs
        WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = req_row {
        let raw_tx: Option<Vec<u8>> = row.get("raw_tx");
        let input_beef: Option<Vec<u8>> = row.get("input_beef");

        // Try input_beef first
        if let Some(beef_bytes) = input_beef {
            if let Ok(beef) = Beef::from_binary(&beef_bytes) {
                // Check if BEEF contains a merkle proof for this txid
                if let Some(bump) = beef.find_bump(txid) {
                    if let Some(beef_tx) = beef.find_txid(txid) {
                        if let Some(raw) = beef_tx.raw_tx() {
                            return Ok(Some(BeefTxData {
                                raw_tx: raw.to_vec(),
                                merkle_path: Some(bump.to_binary()),
                            }));
                        }
                    }
                }

                // No proof, but try to get raw tx from BEEF
                if let Some(beef_tx) = beef.find_txid(txid) {
                    if let Some(raw) = beef_tx.raw_tx() {
                        return Ok(Some(BeefTxData {
                            raw_tx: raw.to_vec(),
                            merkle_path: None,
                        }));
                    }
                }
            }
        }

        // Fall back to raw_tx
        if let Some(raw_tx) = raw_tx {
            return Ok(Some(BeefTxData {
                raw_tx,
                merkle_path: None,
            }));
        }
    }

    Ok(None)
}

/// Retrieves a stored BEEF for a transaction if available.
///
/// This is an optimization - instead of recursively fetching each ancestor,
/// we can merge an entire stored BEEF that already contains the full ancestor chain.
/// This matches the TypeScript approach where `input_beef` is stored and later
/// merged directly during BEEF construction.
///
/// # Arguments
/// * `storage` - The storage backend to query
/// * `txid` - The transaction ID (hex string)
///
/// # Returns
/// * `Ok(Some(Beef))` - Stored BEEF if available
/// * `Ok(None)` - If no stored BEEF found
pub(in crate::storage::sqlx) async fn get_stored_beef(
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Option<Beef>> {
    // Try transactions table first
    let tx_row = sqlx::query(
        r#"
        SELECT input_beef
        FROM transactions
        WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = tx_row {
        let input_beef: Option<Vec<u8>> = row.get("input_beef");
        if let Some(beef_bytes) = input_beef {
            if let Ok(beef) = Beef::from_binary(&beef_bytes) {
                return Ok(Some(beef));
            }
        }
    }

    // Also try proven_tx_reqs table
    let req_row = sqlx::query(
        r#"
        SELECT input_beef
        FROM proven_tx_reqs
        WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = req_row {
        let input_beef: Option<Vec<u8>> = row.get("input_beef");
        if let Some(beef_bytes) = input_beef {
            if let Ok(beef) = Beef::from_binary(&beef_bytes) {
                return Ok(Some(beef));
            }
        }
    }

    Ok(None)
}
#[cfg(test)]
#[path = "tests/beef_storage.rs"]
mod tests;
