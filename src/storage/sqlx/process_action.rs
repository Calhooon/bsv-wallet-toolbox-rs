//! Process Action Implementation
//!
//! This module contains the full implementation of the `process_action` method
//! for the `StorageSqlx` wallet storage backend.
//!
//! process_action is called AFTER create_action, when the user has signed the transaction.
//! It receives the signed raw_tx and txid, then:
//! 1. Updates the transaction record with txid and raw_tx
//! 2. Updates output records with script offsets
//! 3. Updates transaction status based on args
//! 4. Creates proven_tx_req record for broadcast tracking (if needed)

use crate::error::{Error, Result};
use crate::storage::entities::TransactionStatus;
use crate::storage::traits::{
    SendWithResult, StorageProcessActionArgs, StorageProcessActionResults, WalletStorageReader,
};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::Row;

use super::StorageSqlx;

// =============================================================================
// Constants
// =============================================================================

mod proven_tx_req_status {
    pub const NOSEND: &str = "nosend";
    pub const UNSENT: &str = "unsent";
    pub const UNPROCESSED: &str = "unprocessed";
    pub const UNMINED: &str = "unmined";
    #[allow(dead_code)]
    pub const SENDING: &str = "sending";
}

// =============================================================================
// Internal Types
// =============================================================================

#[derive(Debug, Clone)]
struct TxScriptOffset {
    offset: usize,
    length: usize,
}

#[derive(Debug)]
struct TxScriptOffsets {
    #[allow(dead_code)]
    inputs: Vec<TxScriptOffset>,
    outputs: Vec<TxScriptOffset>,
}

#[derive(Debug)]
struct TransactionRecord {
    transaction_id: i64,
    #[allow(dead_code)]
    user_id: i64,
    status: String,
    is_outgoing: bool,
    input_beef: Option<Vec<u8>>,
    #[allow(dead_code)]
    txid: Option<String>,
}

#[derive(Debug)]
struct OutputRecord {
    output_id: i64,
    vout: i32,
    locking_script: Option<Vec<u8>>,
    change: bool,
}

// =============================================================================
// Validation
// =============================================================================

fn validate_process_action_args(args: &StorageProcessActionArgs) -> Result<()> {
    if !args.is_new_tx {
        if args.txid.is_none() {
            return Err(Error::ValidationError(
                "txid is required when is_new_tx is false".to_string(),
            ));
        }
        return Ok(());
    }

    if args.reference.is_none() {
        return Err(Error::ValidationError(
            "reference is required for new transactions".to_string(),
        ));
    }

    if args.txid.is_none() {
        return Err(Error::ValidationError(
            "txid is required for new transactions".to_string(),
        ));
    }

    if args.raw_tx.is_none() {
        return Err(Error::ValidationError(
            "raw_tx is required for new transactions".to_string(),
        ));
    }

    let raw_tx = args.raw_tx.as_ref().unwrap();
    if raw_tx.is_empty() {
        return Err(Error::ValidationError(
            "raw_tx cannot be empty".to_string(),
        ));
    }

    Ok(())
}

fn compute_txid(raw_tx: &[u8]) -> String {
    let hash1 = Sha256::digest(raw_tx);
    let hash2 = Sha256::digest(hash1);
    let mut reversed = hash2.to_vec();
    reversed.reverse();
    hex::encode(reversed)
}

fn validate_txid_matches_raw_tx(txid: &str, raw_tx: &[u8]) -> Result<()> {
    let computed = compute_txid(raw_tx);
    if computed != txid {
        return Err(Error::ValidationError(format!(
            "txid mismatch: provided {}, computed from raw_tx: {}",
            txid, computed
        )));
    }
    Ok(())
}

// =============================================================================
// Transaction Parsing
// =============================================================================

fn read_var_int(data: &[u8], offset: &mut usize) -> Result<u64> {
    if *offset >= data.len() {
        return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
    }

    let first = data[*offset];
    *offset += 1;

    if first < 0xfd {
        Ok(first as u64)
    } else if first == 0xfd {
        if *offset + 2 > data.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        let val = u16::from_le_bytes([data[*offset], data[*offset + 1]]) as u64;
        *offset += 2;
        Ok(val)
    } else if first == 0xfe {
        if *offset + 4 > data.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        let val = u32::from_le_bytes([
            data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
        ]) as u64;
        *offset += 4;
        Ok(val)
    } else {
        if *offset + 8 > data.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        let val = u64::from_le_bytes([
            data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
            data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7],
        ]);
        *offset += 8;
        Ok(val)
    }
}

fn parse_tx_script_offsets(raw_tx: &[u8]) -> Result<TxScriptOffsets> {
    let mut offset = 0;

    if raw_tx.len() < 4 {
        return Err(Error::ValidationError("Transaction too short".to_string()));
    }
    offset += 4;

    let input_count = read_var_int(raw_tx, &mut offset)?;
    let mut inputs = Vec::with_capacity(input_count as usize);

    for _ in 0..input_count {
        if offset + 36 > raw_tx.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        offset += 36;

        let script_len = read_var_int(raw_tx, &mut offset)? as usize;
        let script_offset = offset;

        if offset + script_len > raw_tx.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        offset += script_len;

        if offset + 4 > raw_tx.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        offset += 4;

        inputs.push(TxScriptOffset { offset: script_offset, length: script_len });
    }

    let output_count = read_var_int(raw_tx, &mut offset)?;
    let mut outputs = Vec::with_capacity(output_count as usize);

    for _ in 0..output_count {
        if offset + 8 > raw_tx.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        offset += 8;

        let script_len = read_var_int(raw_tx, &mut offset)? as usize;
        let script_offset = offset;

        if offset + script_len > raw_tx.len() {
            return Err(Error::ValidationError("Unexpected end of transaction data".to_string()));
        }
        offset += script_len;

        outputs.push(TxScriptOffset { offset: script_offset, length: script_len });
    }

    Ok(TxScriptOffsets { inputs, outputs })
}

// =============================================================================
// Database Operations
// =============================================================================

async fn find_transaction_by_reference(
    storage: &StorageSqlx, user_id: i64, reference: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        "SELECT transaction_id, user_id, status, is_outgoing, input_beef, txid FROM transactions WHERE user_id = ? AND reference = ?",
    )
    .bind(user_id)
    .bind(reference)
    .fetch_optional(storage.pool())
    .await?;

    match row {
        Some(row) => Ok(Some(TransactionRecord {
            transaction_id: row.get("transaction_id"),
            user_id: row.get("user_id"),
            status: row.get("status"),
            is_outgoing: row.get("is_outgoing"),
            input_beef: row.get("input_beef"),
            txid: row.get("txid"),
        })),
        None => Ok(None),
    }
}

#[allow(dead_code)]
async fn find_transaction_by_txid(
    storage: &StorageSqlx, txid: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        "SELECT transaction_id, user_id, status, is_outgoing, input_beef, txid FROM transactions WHERE txid = ?",
    )
    .bind(txid)
    .fetch_optional(storage.pool())
    .await?;

    match row {
        Some(row) => Ok(Some(TransactionRecord {
            transaction_id: row.get("transaction_id"),
            user_id: row.get("user_id"),
            status: row.get("status"),
            is_outgoing: row.get("is_outgoing"),
            input_beef: row.get("input_beef"),
            txid: row.get("txid"),
        })),
        None => Ok(None),
    }
}

async fn find_outputs_for_transaction(storage: &StorageSqlx, transaction_id: i64) -> Result<Vec<OutputRecord>> {
    let rows = sqlx::query(
        "SELECT output_id, vout, locking_script, change FROM outputs WHERE transaction_id = ? ORDER BY vout",
    )
    .bind(transaction_id)
    .fetch_all(storage.pool())
    .await?;

    Ok(rows.iter().map(|row| OutputRecord {
        output_id: row.get("output_id"),
        vout: row.get("vout"),
        locking_script: row.get("locking_script"),
        change: row.get("change"),
    }).collect())
}

async fn update_transaction_with_signed_data(
    storage: &StorageSqlx, transaction_id: i64, txid: &str, status: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        "UPDATE transactions SET txid = ?, status = ?, raw_tx = NULL, input_beef = NULL, updated_at = ? WHERE transaction_id = ?",
    )
    .bind(txid)
    .bind(status)
    .bind(now)
    .bind(transaction_id)
    .execute(storage.pool())
    .await?;
    Ok(())
}

async fn update_output_with_script_offset(
    storage: &StorageSqlx, output_id: i64, txid: &str,
    script_offset: i32, script_length: i32, max_output_script: i32, should_clear_script: bool,
) -> Result<()> {
    let now = Utc::now();
    let clear_script = should_clear_script && script_length > max_output_script;

    if clear_script {
        sqlx::query(
            "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, locking_script = NULL, spendable = 1, updated_at = ? WHERE output_id = ?",
        )
        .bind(txid).bind(script_offset).bind(script_length).bind(now).bind(output_id)
        .execute(storage.pool()).await?;
    } else {
        sqlx::query(
            "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, spendable = 1, updated_at = ? WHERE output_id = ?",
        )
        .bind(txid).bind(script_offset).bind(script_length).bind(now).bind(output_id)
        .execute(storage.pool()).await?;
    }
    Ok(())
}

async fn create_or_update_proven_tx_req(
    storage: &StorageSqlx, txid: &str, raw_tx: &[u8], input_beef: Option<&[u8]>,
    status: &str, transaction_id: i64,
) -> Result<i64> {
    let now = Utc::now();
    let notify = format!(r#"{{"transactionIds":[{}]}}"#, transaction_id);

    let existing = sqlx::query("SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(storage.pool())
        .await?;

    if let Some(row) = existing {
        let id: i64 = row.get("proven_tx_req_id");
        sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, raw_tx = ?, input_beef = ?, notify = ?, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(status).bind(raw_tx).bind(input_beef).bind(&notify).bind(now).bind(id)
        .execute(storage.pool()).await?;
        Ok(id)
    } else {
        let result = sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef, history, notify, created_at, updated_at) VALUES (?, ?, ?, ?, '{}', ?, ?, ?)",
        )
        .bind(txid).bind(status).bind(raw_tx).bind(input_beef).bind(&notify).bind(now).bind(now)
        .execute(storage.pool()).await?;
        Ok(result.last_insert_rowid())
    }
}

async fn find_proven_tx_req_by_txid(storage: &StorageSqlx, txid: &str) -> Result<Option<(i64, String)>> {
    let row = sqlx::query("SELECT proven_tx_req_id, status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(storage.pool())
        .await?;

    match row {
        Some(row) => Ok(Some((row.get("proven_tx_req_id"), row.get("status")))),
        None => Ok(None),
    }
}

async fn update_proven_tx_req_status(
    storage: &StorageSqlx, proven_tx_req_id: i64, status: &str, batch: Option<&str>,
) -> Result<()> {
    let now = Utc::now();
    if let Some(batch) = batch {
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, batch = ?, attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(status).bind(batch).bind(now).bind(proven_tx_req_id)
            .execute(storage.pool()).await?;
    } else {
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(status).bind(now).bind(proven_tx_req_id)
            .execute(storage.pool()).await?;
    }
    Ok(())
}

async fn update_transaction_status_by_txid(storage: &StorageSqlx, txid: &str, status: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query("UPDATE transactions SET status = ?, updated_at = ? WHERE txid = ?")
        .bind(status).bind(now).bind(txid)
        .execute(storage.pool()).await?;
    Ok(())
}

// =============================================================================
// Status Determination
// =============================================================================

fn determine_statuses(args: &StorageProcessActionArgs) -> (&'static str, &'static str) {
    if args.is_no_send && !args.is_send_with {
        (TransactionStatus::NoSend.as_str(), proven_tx_req_status::NOSEND)
    } else if args.is_delayed {
        (TransactionStatus::Unprocessed.as_str(), proven_tx_req_status::UNSENT)
    } else {
        (TransactionStatus::Unprocessed.as_str(), proven_tx_req_status::UNPROCESSED)
    }
}

// =============================================================================
// Main Implementation
// =============================================================================

pub async fn process_action_internal(
    storage: &StorageSqlx, user_id: i64, args: StorageProcessActionArgs,
) -> Result<StorageProcessActionResults> {
    validate_process_action_args(&args)?;

    let mut send_with_results: Vec<SendWithResult> = Vec::new();
    let txids_to_broadcast: Vec<String>;

    if args.is_new_tx {
        let reference = args.reference.as_ref().unwrap();
        let txid = args.txid.as_ref().unwrap();
        let raw_tx = args.raw_tx.as_ref().unwrap();

        validate_txid_matches_raw_tx(txid, raw_tx)?;
        let script_offsets = parse_tx_script_offsets(raw_tx)?;

        let tx = find_transaction_by_reference(storage, user_id, reference).await?;
        let tx = tx.ok_or_else(|| Error::NotFound {
            entity: "Transaction".to_string(),
            id: format!("reference={}", reference),
        })?;

        if !tx.is_outgoing {
            return Err(Error::ValidationError(format!(
                "transaction with reference ({}) is not outgoing", reference
            )));
        }

        // Validate inputBEEF exists - if missing, tx may have already been processed
        if tx.input_beef.is_none() || tx.input_beef.as_ref().map(|b| b.is_empty()).unwrap_or(true) {
            return Err(Error::ValidationError(format!(
                "transaction with reference ({}) has no inputBEEF. This suggests the transaction may have already been processed. Try with (is_new_tx = false)",
                reference
            )));
        }

        if tx.status != TransactionStatus::Unsigned.as_str()
            && tx.status != TransactionStatus::Unprocessed.as_str()
        {
            return Err(Error::InvalidTransactionStatus(format!(
                "transaction with reference ({}) is not in a valid status for processing (status: {})",
                reference, tx.status
            )));
        }

        let outputs = find_outputs_for_transaction(storage, tx.transaction_id).await?;

        for output in &outputs {
            if output.change { continue; }
            if let Some(ref db_script) = output.locking_script {
                let vout = output.vout as usize;
                if vout >= script_offsets.outputs.len() {
                    return Err(Error::ValidationError(format!("Output vout {} is out of range", vout)));
                }
                let offset = &script_offsets.outputs[vout];
                let raw_script = &raw_tx[offset.offset..offset.offset + offset.length];
                if raw_script != db_script.as_slice() {
                    return Err(Error::ValidationError(format!(
                        "Locking script mismatch at vout {}", vout
                    )));
                }
            }
        }

        let (tx_status, req_status) = determine_statuses(&args);
        update_transaction_with_signed_data(storage, tx.transaction_id, txid, tx_status).await?;

        let settings = storage.get_settings();
        for output in &outputs {
            let vout = output.vout as usize;
            if vout < script_offsets.outputs.len() {
                let offset = &script_offsets.outputs[vout];
                update_output_with_script_offset(
                    storage, output.output_id, txid,
                    offset.offset as i32, offset.length as i32,
                    settings.max_output_script, true,
                ).await?;
            }
        }

        create_or_update_proven_tx_req(storage, txid, raw_tx, tx.input_beef.as_deref(), req_status, tx.transaction_id).await?;

        if args.is_no_send && !args.is_send_with {
            txids_to_broadcast = Vec::new();
        } else {
            let mut txids = args.send_with.clone();
            txids.push(txid.clone());
            txids_to_broadcast = txids;
        }
    } else {
        let txid = args.txid.as_ref().unwrap();
        txids_to_broadcast = vec![txid.clone()];
    }

    if txids_to_broadcast.is_empty() {
        return Ok(StorageProcessActionResults {
            send_with_results: Some(Vec::new()),
            not_delayed_results: None,
            log: None,
        });
    }

    let batch = if txids_to_broadcast.len() > 1 {
        Some(generate_batch_id())
    } else {
        None
    };

    for txid in &txids_to_broadcast {
        let req = find_proven_tx_req_by_txid(storage, txid).await?;

        if let Some((req_id, current_status)) = req {
            let already_sent = current_status == proven_tx_req_status::UNMINED
                || current_status == "completed"
                || current_status == "unproven";

            if already_sent {
                send_with_results.push(SendWithResult { txid: txid.clone(), status: "unproven".to_string() });
            } else if args.is_delayed {
                update_proven_tx_req_status(storage, req_id, proven_tx_req_status::UNSENT, batch.as_deref()).await?;
                update_transaction_status_by_txid(storage, txid, TransactionStatus::Sending.as_str()).await?;
                send_with_results.push(SendWithResult { txid: txid.clone(), status: "sending".to_string() });
            } else {
                update_proven_tx_req_status(storage, req_id, proven_tx_req_status::UNMINED, batch.as_deref()).await?;
                update_transaction_status_by_txid(storage, txid, TransactionStatus::Unproven.as_str()).await?;
                send_with_results.push(SendWithResult { txid: txid.clone(), status: "unproven".to_string() });
            }
        } else {
            send_with_results.push(SendWithResult { txid: txid.clone(), status: "failed".to_string() });
        }
    }

    Ok(StorageProcessActionResults {
        send_with_results: Some(send_with_results),
        not_delayed_results: None,
        log: None,
    })
}

fn generate_batch_id() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_txid() {
        let raw_tx = hex::decode("01000000000000000000").unwrap();
        let txid = compute_txid(&raw_tx);
        assert_eq!(txid.len(), 64);
        assert!(txid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_validate_txid_matches_raw_tx_success() {
        let raw_tx = hex::decode("01000000000000000000").unwrap();
        let txid = compute_txid(&raw_tx);
        assert!(validate_txid_matches_raw_tx(&txid, &raw_tx).is_ok());
    }

    #[test]
    fn test_validate_txid_matches_raw_tx_failure() {
        let raw_tx = hex::decode("01000000000000000000").unwrap();
        let wrong_txid = "0000000000000000000000000000000000000000000000000000000000000000";
        let result = validate_txid_matches_raw_tx(wrong_txid, &raw_tx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("txid mismatch"));
    }

    #[test]
    fn test_read_var_int_single_byte() {
        let data = vec![0x05, 0x00];
        let mut offset = 0;
        assert_eq!(read_var_int(&data, &mut offset).unwrap(), 5);
        assert_eq!(offset, 1);
    }

    #[test]
    fn test_read_var_int_two_bytes() {
        let data = vec![0xfd, 0x00, 0x01];
        let mut offset = 0;
        assert_eq!(read_var_int(&data, &mut offset).unwrap(), 256);
        assert_eq!(offset, 3);
    }

    #[test]
    fn test_parse_tx_script_offsets_minimal() {
        let raw_tx = hex::decode("01000000000000000000").unwrap();
        let result = parse_tx_script_offsets(&raw_tx).unwrap();
        assert_eq!(result.inputs.len(), 0);
        assert_eq!(result.outputs.len(), 0);
    }

    #[test]
    fn test_validate_process_action_args_new_tx_missing_reference() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: None, txid: Some("abc".to_string()), raw_tx: Some(vec![1, 2, 3]), send_with: vec![],
        };
        assert!(validate_process_action_args(&args).unwrap_err().to_string().contains("reference is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_missing_txid() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some("ref123".to_string()), txid: None, raw_tx: Some(vec![1, 2, 3]), send_with: vec![],
        };
        assert!(validate_process_action_args(&args).unwrap_err().to_string().contains("txid is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_missing_raw_tx() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some("ref123".to_string()), txid: Some("abc".to_string()), raw_tx: None, send_with: vec![],
        };
        assert!(validate_process_action_args(&args).unwrap_err().to_string().contains("raw_tx is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_empty_raw_tx() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some("ref123".to_string()), txid: Some("abc".to_string()), raw_tx: Some(vec![]), send_with: vec![],
        };
        assert!(validate_process_action_args(&args).unwrap_err().to_string().contains("raw_tx cannot be empty"));
    }

    #[test]
    fn test_validate_process_action_args_not_new_tx_missing_txid() {
        let args = StorageProcessActionArgs {
            is_new_tx: false, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: None, txid: None, raw_tx: None, send_with: vec![],
        };
        assert!(validate_process_action_args(&args).unwrap_err().to_string().contains("txid is required"));
    }

    #[test]
    fn test_validate_process_action_args_success() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some("ref123".to_string()), txid: Some("abc".to_string()), raw_tx: Some(vec![1, 2, 3]), send_with: vec![],
        };
        assert!(validate_process_action_args(&args).is_ok());
    }

    #[test]
    fn test_determine_statuses_no_send() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: true, is_delayed: false,
            reference: None, txid: None, raw_tx: None, send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "nosend");
        assert_eq!(req_status, "nosend");
    }

    #[test]
    fn test_determine_statuses_delayed() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: true,
            reference: None, txid: None, raw_tx: None, send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "unprocessed");
        assert_eq!(req_status, "unsent");
    }

    #[test]
    fn test_determine_statuses_immediate() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: None, txid: None, raw_tx: None, send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "unprocessed");
        assert_eq!(req_status, "unprocessed");
    }

    #[test]
    fn test_determine_statuses_send_with_overrides_no_send() {
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: true, is_no_send: true, is_delayed: false,
            reference: None, txid: None, raw_tx: None, send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "unprocessed");
        assert_eq!(req_status, "unprocessed");
    }

    #[test]
    fn test_generate_batch_id() {
        let id1 = generate_batch_id();
        let id2 = generate_batch_id();
        assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &id1).is_ok());
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_parse_tx_script_offsets_single_input_output() {
        let raw_tx = hex::decode(
            "0100000001\
             0000000000000000000000000000000000000000000000000000000000000000\
             00000000\
             02aabb\
             ffffffff\
             01\
             0100000000000000\
             03ccddee\
             00000000"
        ).unwrap();

        let result = parse_tx_script_offsets(&raw_tx).unwrap();

        assert_eq!(result.inputs.len(), 1);
        assert_eq!(result.outputs.len(), 1);
        assert_eq!(result.inputs[0].length, 2);
        assert_eq!(result.outputs[0].length, 3);
    }

    // =========================================================================
    // Integration Tests
    // =========================================================================

    use super::super::StorageSqlx;
    use crate::storage::traits::WalletStorageWriter;
    use bsv_sdk::wallet::CreateActionOutput;

    fn create_raw_transaction(output_scripts: &[&[u8]]) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]);
        tx.push(0x00);
        tx.push(output_scripts.len() as u8);
        for script in output_scripts {
            tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            tx.push(script.len() as u8);
            tx.extend_from_slice(script);
        }
        tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        tx
    }

    async fn setup_storage_with_action() -> (StorageSqlx, i64, String) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();
        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction for process_action".to_string(),
            input_beef: None, inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script, satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None, custom_instructions: None, tags: None,
            }]),
            lock_time: None, version: None, labels: None, options: None,
        };

        let result = storage.create_action(
            &crate::storage::traits::AuthId::with_user_id("02user_identity_key", user.user_id), args,
        ).await.unwrap();

        // NOTE: create_action returns input_beef in result but doesn't store it in DB.
        // For process_action to work, we need to populate input_beef on the transaction.
        // In production, this would happen as part of a complete create_action impl.
        // Here we simulate it with a minimal valid BEEF (just a version byte).
        let dummy_input_beef = vec![0x00, 0x01, 0x00]; // minimal BEEF: version 0 with no content
        sqlx::query("UPDATE transactions SET input_beef = ? WHERE reference = ?")
            .bind(&dummy_input_beef)
            .bind(&result.reference)
            .execute(storage.pool())
            .await
            .unwrap();

        (storage, user.user_id, result.reference)
    }

    async fn seed_change_output(storage: &StorageSqlx, user_id: i64, satoshis: i64) {
        let now = Utc::now();
        let basket = storage.find_or_create_default_basket(user_id).await.unwrap();
        let tx_result = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at) VALUES (?, 'completed', 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?)",
        )
        .bind(user_id).bind(satoshis)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();

        let transaction_id = tx_result.last_insert_rowid();
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'seeded change', ?, ?)",
        )
        .bind(user_id).bind(transaction_id).bind(basket.basket_id).bind(satoshis).bind(&locking_script)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
    }

    #[tokio::test]
    async fn test_process_action_missing_reference() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some("nonexistent_ref".to_string()), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user.user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_process_action_invalid_txid() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let wrong_txid = "0000000000000000000000000000000000000000000000000000000000000000";

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(wrong_txid.to_string()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("txid mismatch"));
    }

    #[tokio::test]
    async fn test_process_action_with_nosend() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: true, is_delayed: false,
            reference: Some(reference), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await.unwrap();
        assert!(result.send_with_results.is_some());
        assert!(result.send_with_results.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_process_action_with_delayed() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: true,
            reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await.unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        assert_eq!(send_results[0].status, "sending");
    }

    #[tokio::test]
    async fn test_process_action_immediate_broadcast() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await.unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        assert_eq!(send_results[0].status, "unproven");
    }

    #[tokio::test]
    async fn test_process_action_verify_tx_updated() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: true, is_delayed: false,
            reference: Some(reference.clone()), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        process_action_internal(&storage, user_id, args).await.unwrap();

        let row = sqlx::query("SELECT txid, status FROM transactions WHERE reference = ?")
            .bind(&reference).fetch_one(storage.pool()).await.unwrap();
        let db_txid: String = row.get("txid");
        let db_status: String = row.get("status");
        assert_eq!(db_txid, txid);
        assert_eq!(db_status, "nosend");
    }

    #[tokio::test]
    async fn test_process_action_verify_proven_tx_req_created() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx.clone()), send_with: vec![],
        };

        process_action_internal(&storage, user_id, args).await.unwrap();

        let row = sqlx::query("SELECT txid, raw_tx FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid).fetch_one(storage.pool()).await.unwrap();
        let db_raw_tx: Vec<u8> = row.get("raw_tx");
        assert_eq!(db_raw_tx, raw_tx);
    }

    #[tokio::test]
    async fn test_process_action_already_processed() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        sqlx::query("UPDATE transactions SET status = 'completed' WHERE reference = ?")
            .bind(&reference).execute(storage.pool()).await.unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("status"));
    }

    /// Test re-processing a transaction with is_new_tx=false (Go: TestProcessActionTwice)
    #[tokio::test]
    async fn test_process_action_twice_with_is_new_tx_false() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // First process with is_new_tx=true
        let args1 = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference.clone()), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };
        let result1 = process_action_internal(&storage, user_id, args1).await.unwrap();
        assert!(result1.send_with_results.is_some());
        assert_eq!(result1.send_with_results.as_ref().unwrap().len(), 1);
        assert_eq!(result1.send_with_results.as_ref().unwrap()[0].status, "unproven");

        // Second process with is_new_tx=false (re-broadcast)
        let args2 = StorageProcessActionArgs {
            is_new_tx: false, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: None, txid: Some(txid.clone()), raw_tx: None, send_with: vec![],
        };
        let result2 = process_action_internal(&storage, user_id, args2).await.unwrap();
        assert!(result2.send_with_results.is_some());
        let send_results = result2.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        // Already sent tx should return "unproven" status
        assert_eq!(send_results[0].status, "unproven");
    }

    /// Test error when is_new_tx=false for non-existent tx (Go: TestProcessActionErrorCases)
    #[tokio::test]
    async fn test_process_action_is_new_tx_false_for_unstored_tx() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        let nonexistent_txid = "0000000000000000000000000000000000000000000000000000000000000001";
        let args = StorageProcessActionArgs {
            is_new_tx: false, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: None, txid: Some(nonexistent_txid.to_string()), raw_tx: None, send_with: vec![],
        };

        let result = process_action_internal(&storage, user.user_id, args).await.unwrap();
        // Non-existent tx should return "failed" status
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].status, "failed");
    }

    /// Test error when inputBEEF is missing (Go: validateStateOfTableTx)
    #[tokio::test]
    async fn test_process_action_missing_input_beef() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // Clear the input_beef to simulate already processed state
        sqlx::query("UPDATE transactions SET input_beef = NULL WHERE reference = ?")
            .bind(&reference).execute(storage.pool()).await.unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("inputBEEF") || err.contains("already been processed"));
    }

    /// Test error when transaction is not outgoing (Go: validateStateOfTableTx)
    #[tokio::test]
    async fn test_process_action_not_outgoing() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // Set is_outgoing to false
        sqlx::query("UPDATE transactions SET is_outgoing = 0 WHERE reference = ?")
            .bind(&reference).execute(storage.pool()).await.unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference.clone()), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not outgoing"));
    }

    /// Test send_with with multiple txids creates batch (Go: setBatchForTxs)
    #[tokio::test]
    async fn test_process_action_with_send_with_batch() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // Create another proven_tx_req to use in send_with
        let other_txid = "1111111111111111111111111111111111111111111111111111111111111111";
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, raw_tx, history, notify, created_at, updated_at) VALUES (?, 'unprocessed', X'01', '{}', '{}', ?, ?)",
        )
        .bind(other_txid).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: true, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid.clone()),
            raw_tx: Some(raw_tx), send_with: vec![other_txid.to_string()],
        };

        let result = process_action_internal(&storage, user_id, args).await.unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        // Should have results for both txids
        assert_eq!(send_results.len(), 2);

        // Verify batch was set (multiple txs get a batch)
        let row = sqlx::query("SELECT batch FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid).fetch_one(storage.pool()).await.unwrap();
        let batch: Option<String> = row.get("batch");
        assert!(batch.is_some(), "Batch should be set for multiple txs");
    }

    /// Test send_with overrides no_send (Go: SendWith overrides IsNoSend)
    #[tokio::test]
    async fn test_process_action_send_with_overrides_no_send() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // is_no_send=true but is_send_with=true should still broadcast
        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: true, is_no_send: true, is_delayed: false,
            reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await.unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        // Should still have broadcast result since send_with overrides no_send
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        // Status should be unproven (immediate broadcast) not nosend
        assert_eq!(send_results[0].status, "unproven");
    }

    /// Test locking script mismatch error (Go: validateNewTxOutputs)
    #[tokio::test]
    async fn test_process_action_locking_script_mismatch() {
        let (storage, user_id, reference) = setup_storage_with_action().await;

        // Use a different locking script than what's stored
        let different_script = hex::decode("76a914000000000000000000000000000000000000000088ac").unwrap();
        let raw_tx = create_raw_transaction(&[&different_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
            reference: Some(reference), txid: Some(txid), raw_tx: Some(raw_tx), send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mismatch") || err.contains("script"));
    }

    /// Test that outputs get updated with script offset/length (verify DB state)
    #[tokio::test]
    async fn test_process_action_outputs_updated_with_offsets() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true, is_send_with: false, is_no_send: true, is_delayed: false,
            reference: Some(reference.clone()), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
        };

        process_action_internal(&storage, user_id, args).await.unwrap();

        // Find the transaction to get its ID
        let tx_row = sqlx::query("SELECT transaction_id FROM transactions WHERE reference = ?")
            .bind(&reference).fetch_one(storage.pool()).await.unwrap();
        let transaction_id: i64 = tx_row.get("transaction_id");

        // Verify outputs have script_offset and script_length set
        let output_row = sqlx::query("SELECT txid, script_offset, script_length, spendable FROM outputs WHERE transaction_id = ?")
            .bind(transaction_id).fetch_one(storage.pool()).await.unwrap();

        let output_txid: String = output_row.get("txid");
        let script_offset: Option<i32> = output_row.get("script_offset");
        let script_length: Option<i32> = output_row.get("script_length");
        let spendable: bool = output_row.get("spendable");

        assert_eq!(output_txid, txid);
        assert!(script_offset.is_some(), "script_offset should be set");
        assert!(script_length.is_some(), "script_length should be set");
        assert_eq!(script_length.unwrap(), 25); // P2PKH script length
        assert!(spendable, "output should be marked spendable");
    }

    /// Test proven_tx_req status based on different modes
    #[tokio::test]
    async fn test_process_action_proven_tx_req_status_modes() {
        // Test nosend mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true, is_send_with: false, is_no_send: true, is_delayed: false,
                reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
            };
            process_action_internal(&storage, user_id, args).await.unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid).fetch_one(storage.pool()).await.unwrap();
            let status: String = row.get("status");
            assert_eq!(status, "nosend");
        }

        // Test delayed mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: true,
                reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
            };
            process_action_internal(&storage, user_id, args).await.unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid).fetch_one(storage.pool()).await.unwrap();
            let status: String = row.get("status");
            // After broadcast phase, delayed should be 'unsent' (ready for background broadcaster)
            assert_eq!(status, "unsent");
        }

        // Test immediate mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true, is_send_with: false, is_no_send: false, is_delayed: false,
                reference: Some(reference), txid: Some(txid.clone()), raw_tx: Some(raw_tx), send_with: vec![],
            };
            process_action_internal(&storage, user_id, args).await.unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid).fetch_one(storage.pool()).await.unwrap();
            let status: String = row.get("status");
            // After broadcast phase, immediate should be 'unmined' (broadcast succeeded)
            assert_eq!(status, "unmined");
        }
    }
}
