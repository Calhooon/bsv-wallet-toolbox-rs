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
//!
//! ## Crash Safety
//!
//! Both `process_action_internal` and `update_transaction_status_after_broadcast_internal`
//! wrap all database mutations in a single SQL transaction (BEGIN/COMMIT). If the process
//! crashes mid-operation, SQLite automatically rolls back the incomplete transaction,
//! preventing partial updates that would leave the database in an inconsistent state.

use crate::error::{Error, Result};
use crate::services::traits::{PostBeefResult, PostTxResultForTxid};
use crate::storage::entities::TransactionStatus;
use crate::storage::traits::{
    SendWithResult, StorageProcessActionArgs, StorageProcessActionResults, WalletStorageReader,
};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::StorageSqlx;

// =============================================================================
// Broadcast Outcome Classification
// =============================================================================

/// Classified result of a broadcast attempt.
///
/// Matches the classification pattern used by the TS and Go reference
/// wallet-toolbox implementations. Transient failures (ServiceError) keep
/// inputs locked for background retry; permanent failures (DoubleSpend,
/// InvalidTx) restore inputs immediately.
#[derive(Debug, Clone)]
pub enum BroadcastOutcome {
    /// At least one provider accepted the transaction.
    Success,
    /// All providers returned service/network errors (transient — will retry).
    ServiceError { details: Vec<String> },
    /// A provider reported a double-spend (permanent).
    DoubleSpend {
        competing_txs: Vec<String>,
        details: Vec<String>,
    },
    /// A provider definitively rejected the transaction (permanent).
    InvalidTx { details: Vec<String> },
    /// A provider reported orphan mempool (parent tx not yet propagated).
    /// This is a propagation issue, NOT a double-spend. The miner has the
    /// child tx but not the parent. The tx should stay in 'sending' for
    /// retry — the parent will typically propagate within a few seconds.
    OrphanMempool { details: Vec<String> },
}

impl BroadcastOutcome {
    /// Returns true if the broadcast was accepted by at least one provider.
    pub fn is_success(&self) -> bool {
        matches!(self, BroadcastOutcome::Success)
    }

    /// Returns true if the failure is transient and should be retried.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            BroadcastOutcome::ServiceError { .. } | BroadcastOutcome::OrphanMempool { .. }
        )
    }

    /// Build a human-readable error message with per-provider details.
    pub fn error_message(&self, txid: &str) -> Option<String> {
        match self {
            BroadcastOutcome::Success => None,
            BroadcastOutcome::ServiceError { details } => Some(format!(
                "Transaction broadcast for txid {} returned service errors (will retry): {}",
                txid,
                details.join("; ")
            )),
            BroadcastOutcome::DoubleSpend {
                competing_txs,
                details,
            } => Some(format!(
                "Transaction broadcast failed for txid {}: double spend detected. Competing txs: [{}]. Details: {}",
                txid,
                competing_txs.join(", "),
                details.join("; ")
            )),
            BroadcastOutcome::InvalidTx { details } => Some(format!(
                "Transaction broadcast failed for txid {}: transaction rejected. Details: {}",
                txid,
                details.join("; ")
            )),
            BroadcastOutcome::OrphanMempool { details } => Some(format!(
                "Transaction broadcast for txid {} returned orphan mempool (parent not propagated, will retry): {}",
                txid,
                details.join("; ")
            )),
        }
    }
}

/// Classify broadcast results from multiple providers into a single outcome.
///
/// Priority order (matching TS/Go reference implementations):
/// 1. Any success → Success
/// 2. Any double-spend (but NOT orphan mempool) → DoubleSpend (permanent)
/// 3. Any definitive rejection (ARC 46x codes) → InvalidTx (permanent)
/// 4. Any orphan mempool → OrphanMempool (transient, parent not propagated)
/// 5. Otherwise → ServiceError (transient, will retry)
pub fn classify_broadcast_results(results: &[PostBeefResult]) -> BroadcastOutcome {
    // Collect all per-txid results across providers
    let all_txid_results: Vec<&PostTxResultForTxid> =
        results.iter().flat_map(|r| r.txid_results.iter()).collect();

    // 1. Any success?
    let any_success = results.iter().any(|r| r.is_success());
    if any_success {
        return BroadcastOutcome::Success;
    }

    // Collect error details from all providers
    let details: Vec<String> = results
        .iter()
        .filter(|r| !r.is_success())
        .map(|r| {
            let txid_errors: String = r
                .txid_results
                .iter()
                .filter(|tx| tx.status != "success")
                .map(|tx| tx.data.as_deref().unwrap_or("unknown"))
                .collect::<Vec<_>>()
                .join("; ");
            format!("{}: {} [{}]", r.name, r.status, txid_errors)
        })
        .collect();

    // 2. Any double-spend? (true double-spend, NOT orphan mempool)
    let is_double_spend = all_txid_results
        .iter()
        .any(|tr| tr.double_spend && !tr.orphan_mempool);
    if is_double_spend {
        let competing_txs: Vec<String> = all_txid_results
            .iter()
            .filter_map(|tr| tr.competing_txs.as_ref())
            .flatten()
            .cloned()
            .collect();
        return BroadcastOutcome::DoubleSpend {
            competing_txs,
            details,
        };
    }

    // 3. Any definitive rejection? (ARC 46x status codes = tx-level rejection)
    let is_invalid = all_txid_results.iter().any(|tr| {
        !tr.service_error
            && !tr.orphan_mempool
            && (tr.status.contains("46") || tr.status.contains("invalid"))
    });
    if is_invalid {
        return BroadcastOutcome::InvalidTx { details };
    }

    // 4. Any orphan mempool? (parent not yet propagated — transient)
    let is_orphan = all_txid_results.iter().any(|tr| tr.orphan_mempool);
    if is_orphan {
        return BroadcastOutcome::OrphanMempool { details };
    }

    // 5. Everything else is a transient service error
    BroadcastOutcome::ServiceError { details }
}

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
        return Err(Error::ValidationError("raw_tx cannot be empty".to_string()));
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
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        offset += 36;

        let script_len = read_var_int(raw_tx, &mut offset)? as usize;
        let script_offset = offset;

        if offset + script_len > raw_tx.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        offset += script_len;

        if offset + 4 > raw_tx.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        offset += 4;

        inputs.push(TxScriptOffset {
            offset: script_offset,
            length: script_len,
        });
    }

    let output_count = read_var_int(raw_tx, &mut offset)?;
    let mut outputs = Vec::with_capacity(output_count as usize);

    for _ in 0..output_count {
        if offset + 8 > raw_tx.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        offset += 8;

        let script_len = read_var_int(raw_tx, &mut offset)? as usize;
        let script_offset = offset;

        if offset + script_len > raw_tx.len() {
            return Err(Error::ValidationError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        offset += script_len;

        outputs.push(TxScriptOffset {
            offset: script_offset,
            length: script_len,
        });
    }

    Ok(TxScriptOffsets { inputs, outputs })
}

// =============================================================================
// Database Operations
//
// All helper functions accept `&mut SqliteConnection` so they can participate
// in a caller-managed SQL transaction. The caller begins the transaction and
// passes `&mut tx` to each helper.
// =============================================================================

async fn find_transaction_by_reference(
    conn: &mut SqliteConnection,
    user_id: i64,
    reference: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        "SELECT transaction_id, user_id, status, is_outgoing, input_beef, txid FROM transactions WHERE user_id = ? AND reference = ?",
    )
    .bind(user_id)
    .bind(reference)
    .fetch_optional(&mut *conn)
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
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        "SELECT transaction_id, user_id, status, is_outgoing, input_beef, txid FROM transactions WHERE txid = ?",
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
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

async fn find_outputs_for_transaction(
    conn: &mut SqliteConnection,
    transaction_id: i64,
) -> Result<Vec<OutputRecord>> {
    let rows = sqlx::query(
        "SELECT output_id, vout, locking_script, change FROM outputs WHERE transaction_id = ? ORDER BY vout",
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .iter()
        .map(|row| OutputRecord {
            output_id: row.get("output_id"),
            vout: row.get("vout"),
            locking_script: row.get("locking_script"),
            change: row.get("change"),
        })
        .collect())
}

async fn update_transaction_with_signed_data(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    txid: &str,
    status: &str,
    raw_tx: &[u8],
) -> Result<()> {
    let now = Utc::now();
    // Store raw_tx on the transaction record so child transactions can find it
    // during BEEF construction. Go/TS store raw_tx at create time; we store it
    // at process time because the Rust flow separates create (unsigned template)
    // from process (signed tx). input_beef is cleared because the proven_tx_req
    // record now holds the authoritative copy.
    sqlx::query(
        "UPDATE transactions SET txid = ?, status = ?, raw_tx = ?, input_beef = NULL, updated_at = ? WHERE transaction_id = ?",
    )
    .bind(txid)
    .bind(status)
    .bind(raw_tx)
    .bind(now)
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn update_output_with_script_offset(
    conn: &mut SqliteConnection,
    output_id: i64,
    txid: &str,
    script_offset: i32,
    script_length: i32,
    max_output_script: i32,
    spendable: bool,
) -> Result<()> {
    let now = Utc::now();
    let spendable_val: i32 = if spendable { 1 } else { 0 };
    let clear_script = script_length > max_output_script;

    if clear_script {
        sqlx::query(
            "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, locking_script = NULL, spendable = ?, updated_at = ? WHERE output_id = ?",
        )
        .bind(txid).bind(script_offset).bind(script_length).bind(spendable_val).bind(now).bind(output_id)
        .execute(&mut *conn).await?;
    } else {
        sqlx::query(
            "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, spendable = ?, updated_at = ? WHERE output_id = ?",
        )
        .bind(txid).bind(script_offset).bind(script_length).bind(spendable_val).bind(now).bind(output_id)
        .execute(&mut *conn).await?;
    }
    Ok(())
}

/// Updates a change output with its locking script from the signed transaction.
///
/// Change outputs are created with empty locking scripts during create_action
/// because key derivation happens at sign time. This function stores the
/// actual locking script after signing so it can be used when spending.
async fn update_change_output_with_locking_script(
    conn: &mut SqliteConnection,
    output_id: i64,
    txid: &str,
    script_offset: i32,
    script_length: i32,
    locking_script: &[u8],
    spendable: bool,
) -> Result<()> {
    let now = Utc::now();
    let spendable_val: i32 = if spendable { 1 } else { 0 };
    sqlx::query(
        "UPDATE outputs SET txid = ?, script_offset = ?, script_length = ?, locking_script = ?, spendable = ?, updated_at = ? WHERE output_id = ?",
    )
    .bind(txid)
    .bind(script_offset)
    .bind(script_length)
    .bind(locking_script)
    .bind(spendable_val)
    .bind(now)
    .bind(output_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn create_or_update_proven_tx_req(
    conn: &mut SqliteConnection,
    txid: &str,
    raw_tx: &[u8],
    input_beef: Option<&[u8]>,
    status: &str,
    transaction_id: i64,
) -> Result<i64> {
    let now = Utc::now();
    let notify = format!(r#"{{"transactionIds":[{}]}}"#, transaction_id);

    let existing = sqlx::query("SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    if let Some(row) = existing {
        let id: i64 = row.get("proven_tx_req_id");
        sqlx::query(
            "UPDATE proven_tx_reqs SET status = ?, raw_tx = ?, input_beef = ?, notify = ?, updated_at = ? WHERE proven_tx_req_id = ?",
        )
        .bind(status).bind(raw_tx).bind(input_beef).bind(&notify).bind(now).bind(id)
        .execute(&mut *conn).await?;
        Ok(id)
    } else {
        let result = sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef, history, notify, created_at, updated_at) VALUES (?, ?, ?, ?, '{}', ?, ?, ?)",
        )
        .bind(txid).bind(status).bind(raw_tx).bind(input_beef).bind(&notify).bind(now).bind(now)
        .execute(&mut *conn).await?;
        Ok(result.last_insert_rowid())
    }
}

async fn find_proven_tx_req_by_txid(
    conn: &mut SqliteConnection,
    txid: &str,
) -> Result<Option<(i64, String)>> {
    let row = sqlx::query("SELECT proven_tx_req_id, status FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    match row {
        Some(row) => Ok(Some((row.get("proven_tx_req_id"), row.get("status")))),
        None => Ok(None),
    }
}

async fn update_proven_tx_req_status(
    conn: &mut SqliteConnection,
    proven_tx_req_id: i64,
    status: &str,
    batch: Option<&str>,
) -> Result<()> {
    let now = Utc::now();
    if let Some(batch) = batch {
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, batch = ?, attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(status).bind(batch).bind(now).bind(proven_tx_req_id)
            .execute(&mut *conn).await?;
    } else {
        sqlx::query("UPDATE proven_tx_reqs SET status = ?, attempts = attempts + 1, updated_at = ? WHERE proven_tx_req_id = ?")
            .bind(status).bind(now).bind(proven_tx_req_id)
            .execute(&mut *conn).await?;
    }
    Ok(())
}

async fn update_transaction_status_by_txid(
    conn: &mut SqliteConnection,
    txid: &str,
    status: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query("UPDATE transactions SET status = ?, updated_at = ? WHERE txid = ?")
        .bind(status)
        .bind(now)
        .bind(txid)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

// =============================================================================
// Status Determination
// =============================================================================

fn determine_statuses(args: &StorageProcessActionArgs) -> (&'static str, &'static str) {
    if args.is_no_send && !args.is_send_with {
        (
            TransactionStatus::NoSend.as_str(),
            proven_tx_req_status::NOSEND,
        )
    } else if args.is_delayed {
        (
            TransactionStatus::Unprocessed.as_str(),
            proven_tx_req_status::UNSENT,
        )
    } else {
        (
            TransactionStatus::Unprocessed.as_str(),
            proven_tx_req_status::UNPROCESSED,
        )
    }
}

// =============================================================================
// Main Implementation
// =============================================================================

/// Process a signed transaction action.
///
/// All database mutations are wrapped in a single SQL transaction for crash safety.
/// If any step fails or the process crashes, all changes are rolled back automatically.
pub async fn process_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: StorageProcessActionArgs,
) -> Result<StorageProcessActionResults> {
    validate_process_action_args(&args)?;

    // Begin SQL transaction - all DB mutations go through `tx`.
    // On drop without commit, sqlx automatically rolls back.
    let mut tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    let mut send_with_results: Vec<SendWithResult> = Vec::new();
    let txids_to_broadcast: Vec<String>;

    if args.is_new_tx {
        let reference = args.reference.as_ref().unwrap();
        let txid = args.txid.as_ref().unwrap();
        let raw_tx = args.raw_tx.as_ref().unwrap();

        validate_txid_matches_raw_tx(txid, raw_tx)?;
        let script_offsets = parse_tx_script_offsets(raw_tx)?;

        let found_tx = find_transaction_by_reference(&mut tx, user_id, reference).await?;
        let found_tx = found_tx.ok_or_else(|| Error::NotFound {
            entity: "Transaction".to_string(),
            id: format!("reference={}", reference),
        })?;

        if !found_tx.is_outgoing {
            return Err(Error::ValidationError(format!(
                "transaction with reference ({}) is not outgoing",
                reference
            )));
        }

        // Validate inputBEEF exists - if missing, tx may have already been processed
        if found_tx.input_beef.is_none()
            || found_tx
                .input_beef
                .as_ref()
                .map(|b| b.is_empty())
                .unwrap_or(true)
        {
            return Err(Error::ValidationError(format!(
                "transaction with reference ({}) has no inputBEEF. This suggests the transaction may have already been processed. Try with (is_new_tx = false)",
                reference
            )));
        }

        if found_tx.status != TransactionStatus::Unsigned.as_str()
            && found_tx.status != TransactionStatus::Unprocessed.as_str()
        {
            return Err(Error::InvalidTransactionStatus(format!(
                "transaction with reference ({}) is not in a valid status for processing (status: {})",
                reference, found_tx.status
            )));
        }

        let outputs = find_outputs_for_transaction(&mut tx, found_tx.transaction_id).await?;

        for output in &outputs {
            if output.change {
                continue;
            }
            if let Some(ref db_script) = output.locking_script {
                let vout = output.vout as usize;
                if vout >= script_offsets.outputs.len() {
                    return Err(Error::ValidationError(format!(
                        "Output vout {} is out of range",
                        vout
                    )));
                }
                let offset = &script_offsets.outputs[vout];
                let raw_script = &raw_tx[offset.offset..offset.offset + offset.length];
                if raw_script != db_script.as_slice() {
                    return Err(Error::ValidationError(format!(
                        "Locking script mismatch at vout {}",
                        vout
                    )));
                }
            }
        }

        let (tx_status, req_status) = determine_statuses(&args);
        update_transaction_with_signed_data(
            &mut tx,
            found_tx.transaction_id,
            txid,
            tx_status,
            raw_tx,
        )
        .await?;

        // nosend outputs stay spendable=false until the tx is actually broadcast
        let mark_spendable = !args.is_no_send || args.is_send_with;

        let settings = storage.get_settings();
        for output in &outputs {
            let vout = output.vout as usize;
            if vout < script_offsets.outputs.len() {
                let offset = &script_offsets.outputs[vout];

                if output.change {
                    // For change outputs, extract and store the locking script from the
                    // signed transaction. Change outputs are created with empty locking
                    // scripts during create_action, but we need them stored for later spending.
                    let locking_script = &raw_tx[offset.offset..offset.offset + offset.length];
                    update_change_output_with_locking_script(
                        &mut tx,
                        output.output_id,
                        txid,
                        offset.offset as i32,
                        offset.length as i32,
                        locking_script,
                        mark_spendable,
                    )
                    .await?;
                } else {
                    // For non-change outputs, just update the offset/length info
                    update_output_with_script_offset(
                        &mut tx,
                        output.output_id,
                        txid,
                        offset.offset as i32,
                        offset.length as i32,
                        settings.max_output_script,
                        mark_spendable,
                    )
                    .await?;
                }
            }
        }

        create_or_update_proven_tx_req(
            &mut tx,
            txid,
            raw_tx,
            found_tx.input_beef.as_deref(),
            req_status,
            found_tx.transaction_id,
        )
        .await?;

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
        tx.commit()
            .await
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
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
        let req = find_proven_tx_req_by_txid(&mut tx, txid).await?;

        if let Some((req_id, current_status)) = req {
            let already_sent = current_status == proven_tx_req_status::UNMINED
                || current_status == "completed"
                || current_status == "unproven";

            if already_sent {
                send_with_results.push(SendWithResult {
                    txid: txid.clone(),
                    status: "unproven".to_string(),
                });
            } else if args.is_delayed {
                update_proven_tx_req_status(
                    &mut tx,
                    req_id,
                    proven_tx_req_status::UNSENT,
                    batch.as_deref(),
                )
                .await?;
                update_transaction_status_by_txid(
                    &mut tx,
                    txid,
                    TransactionStatus::Sending.as_str(),
                )
                .await?;
                send_with_results.push(SendWithResult {
                    txid: txid.clone(),
                    status: "sending".to_string(),
                });
            } else {
                // BUG-003 FIX: For immediate broadcast, don't set status to 'unproven' here.
                // The wallet layer will call update_transaction_status_after_broadcast()
                // AFTER the broadcast succeeds or fails. Until then, keep status as 'sending'.
                //
                // BUG-004 FIX: Use 'unsent' instead of 'unprocessed' so that
                // send_waiting_transactions() can retry if the immediate broadcast
                // fails and update_transaction_status_after_broadcast() never runs.
                // With 'unprocessed', the proven_tx_req was invisible to the retry
                // loop, leaving inputs permanently locked on broadcast failure.
                // Matches Go reference: proven_tx_reqs start as 'unsent'.
                update_proven_tx_req_status(
                    &mut tx,
                    req_id,
                    proven_tx_req_status::UNSENT,
                    batch.as_deref(),
                )
                .await?;
                update_transaction_status_by_txid(
                    &mut tx,
                    txid,
                    TransactionStatus::Sending.as_str(),
                )
                .await?;
                send_with_results.push(SendWithResult {
                    txid: txid.clone(),
                    status: "sending".to_string(),
                });
            }
        } else {
            send_with_results.push(SendWithResult {
                txid: txid.clone(),
                status: "failed".to_string(),
            });
        }
    }

    tx.commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

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
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

// =============================================================================
// Post-Broadcast Status Update
// =============================================================================

/// Before rolling back UTXOs on permanent broadcast failure, check whether
/// the tx actually exists in a miner's mempool or has been mined.
/// Returns true if found alive, false otherwise.
/// On any error, returns false (preserving existing rollback behavior).
async fn reconcile_tx_status(storage: &StorageSqlx, txid: &str) -> bool {
    use crate::storage::traits::WalletStorageReader;

    let services = match storage.get_services() {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(
                txid = %txid,
                error = %e,
                "Reconciliation skipped: services unavailable"
            );
            return false;
        }
    };

    reconcile_tx_status_via_services(&*services, txid).await
}

/// Check whether a transaction is alive in the mempool or mined, using the
/// provided services directly. This is the shared implementation used by both
/// the immediate broadcast path and `send_waiting_transactions`.
///
/// Returns `true` if the tx is found alive, `false` otherwise.
pub(super) async fn reconcile_tx_status_via_services(
    services: &dyn crate::services::WalletServices,
    txid: &str,
) -> bool {
    let txids = vec![txid.to_string()];
    match services.get_status_for_txids(&txids, false).await {
        Ok(result) => {
            for detail in &result.results {
                if detail.txid == txid && (detail.status == "known" || detail.status == "mined") {
                    tracing::info!(
                        txid = %txid,
                        status = %detail.status,
                        "Reconciliation: tx found alive despite broadcast failure — treating as success"
                    );
                    return true;
                }
            }
            tracing::debug!(
                txid = %txid,
                "Reconciliation: tx not found in mempool or chain — proceeding with rollback"
            );
            false
        }
        Err(e) => {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "Reconciliation: status check failed — falling through to rollback"
            );
            false
        }
    }
}

/// For a double-spend failure, query which inputs of the failed transaction are
/// still unspent on-chain (i.e., the competing tx didn't consume them).
///
/// Returns the `output_id` values that are safe to restore to `spendable = 1`.
/// Inputs that fail the is_utxo check (or where the check errors) are NOT
/// included — fail-safe: keep them locked rather than risk a re-spend loop.
///
/// Follows the same is_utxo() pattern used by `un_fail()` in storage_sqlx.rs.
/// Rate-limited to ~3 requests/second to avoid WoC throttling.
async fn utxo_verified_input_ids(storage: &StorageSqlx, txid: &str) -> Vec<i64> {
    use crate::storage::traits::WalletStorageReader;

    let services = match storage.get_services() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "UTXO verification skipped (services unavailable) — inputs stay locked"
            );
            return Vec::new();
        }
    };

    // Get the transaction_id for this txid
    let transaction_id: i64 =
        match sqlx::query("SELECT transaction_id FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_optional(storage.pool())
            .await
        {
            Ok(Some(row)) => row.get("transaction_id"),
            _ => return Vec::new(),
        };

    // Query the input outputs (those spent by this transaction)
    let input_rows = match sqlx::query(
        r#"
        SELECT o.output_id, t.txid AS source_txid, o.vout, o.locking_script
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        WHERE o.spent_by = ?
        "#,
    )
    .bind(transaction_id)
    .fetch_all(storage.pool())
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "UTXO verification: failed to query inputs — inputs stay locked"
            );
            return Vec::new();
        }
    };

    let mut verified = Vec::new();

    for row in &input_rows {
        let output_id: i64 = row.get("output_id");
        let source_txid: String = row.get("source_txid");
        let vout: i32 = row.get("vout");
        let locking_script: Option<Vec<u8>> = row.get("locking_script");
        let script = locking_script.as_deref().unwrap_or(&[]);

        match services.is_utxo(&source_txid, vout as u32, script).await {
            Ok(true) => {
                tracing::debug!(
                    txid = %txid,
                    source = %source_txid,
                    vout = vout,
                    "UTXO verified — safe to restore"
                );
                verified.push(output_id);
            }
            Ok(false) => {
                tracing::info!(
                    txid = %txid,
                    source = %source_txid,
                    vout = vout,
                    "Input consumed on-chain — NOT restoring (dead UTXO)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    txid = %txid,
                    source = %source_txid,
                    vout = vout,
                    error = %e,
                    "is_utxo check failed — NOT restoring (fail-safe)"
                );
            }
        }

        // Rate limit: ~3 req/sec to avoid WoC throttling
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    }

    tracing::info!(
        txid = %txid,
        verified = verified.len(),
        total = input_rows.len(),
        "UTXO verification complete for double-spend rollback"
    );

    verified
}

/// Update transaction status after a broadcast attempt.
///
/// This function is called by the wallet layer after attempting to broadcast a transaction.
/// All database mutations are wrapped in a single SQL transaction for crash safety.
///
/// Behavior varies by `BroadcastOutcome`:
/// - **Success**: tx→'unproven', req→'unmined'
/// - **ServiceError** (transient): tx stays 'sending', req→'sending', inputs stay locked.
///   The `SendWaitingTask` background monitor will pick these up and retry.
/// - **DoubleSpend** (permanent): tx→'failed', req→'doubleSpend'. Inputs restored ONLY
///   after is_utxo() verification confirms they're still unspent on-chain.
/// - **InvalidTx** (permanent): tx→'failed', req→'invalid', inputs restored (safe — tx
///   was malformed, inputs weren't spent by a competitor).
pub async fn update_transaction_status_after_broadcast_internal(
    storage: &StorageSqlx,
    txid: &str,
    outcome: &BroadcastOutcome,
) -> Result<()> {
    // Before starting a DB transaction, reconcile permanent failures against
    // the actual chain/mempool state. This avoids holding a DB transaction
    // open during a network call.
    let effective_outcome = match outcome {
        BroadcastOutcome::DoubleSpend { .. } | BroadcastOutcome::InvalidTx { .. } => {
            if reconcile_tx_status(storage, txid).await {
                &BroadcastOutcome::Success
            } else {
                outcome
            }
        }
        BroadcastOutcome::OrphanMempool { .. } => {
            // Orphan mempool: check if the tx actually made it on-chain despite
            // the orphan report. If found → treat as success.
            if reconcile_tx_status(storage, txid).await {
                &BroadcastOutcome::Success
            } else {
                outcome
            }
        }
        _ => outcome,
    };

    // For DoubleSpend, verify which inputs are still unspent on-chain BEFORE
    // starting the SQL transaction. This avoids holding a DB transaction open
    // during network calls, and follows the is_utxo() pattern from un_fail().
    // Only inputs verified as still-unspent will be restored to spendable.
    let verified_input_ids: Vec<i64> =
        if matches!(effective_outcome, BroadcastOutcome::DoubleSpend { .. }) {
            utxo_verified_input_ids(storage, txid).await
        } else {
            Vec::new()
        };

    let mut tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    let now = Utc::now();

    match effective_outcome {
        BroadcastOutcome::Success => {
            // Broadcast succeeded — update to unproven/unmined
            sqlx::query("UPDATE transactions SET status = ?, updated_at = ? WHERE txid = ?")
                .bind(TransactionStatus::Unproven.as_str())
                .bind(now)
                .bind(txid)
                .execute(&mut *tx)
                .await?;

            sqlx::query("UPDATE proven_tx_reqs SET status = ?, updated_at = ? WHERE txid = ?")
                .bind(proven_tx_req_status::UNMINED)
                .bind(now)
                .bind(txid)
                .execute(&mut *tx)
                .await?;
        }

        BroadcastOutcome::ServiceError { .. } => {
            // Transient failure — keep tx in 'sending', set req to 'sending'.
            // Inputs stay locked. SendWaitingTask will retry.
            // (Transaction status is already 'sending' from process_action, so only
            // update proven_tx_req and bump attempts.)
            sqlx::query(
                "UPDATE proven_tx_reqs SET status = ?, attempts = attempts + 1, updated_at = ? WHERE txid = ?",
            )
            .bind(proven_tx_req_status::SENDING)
            .bind(now)
            .bind(txid)
            .execute(&mut *tx)
            .await?;

            tracing::info!(
                txid = %txid,
                "Broadcast returned service error — transaction stays 'sending' for retry"
            );
        }

        BroadcastOutcome::OrphanMempool { details } => {
            // Orphan mempool — parent tx not yet propagated to miner.
            // This is NOT a double-spend. Keep tx in 'sending' for retry.
            // Do NOT call is_utxo() on inputs. Do NOT lock inputs.
            // The parent will typically propagate within a few seconds.
            sqlx::query(
                "UPDATE proven_tx_reqs SET status = ?, attempts = attempts + 1, updated_at = ? WHERE txid = ?",
            )
            .bind(proven_tx_req_status::SENDING)
            .bind(now)
            .bind(txid)
            .execute(&mut *tx)
            .await?;

            tracing::warn!(
                txid = %txid,
                details = ?details,
                "Broadcast returned orphan mempool (parent not propagated) — transaction stays 'sending' for retry"
            );
        }

        BroadcastOutcome::InvalidTx { .. } => {
            // Permanent failure (malformed tx) — mark as failed and restore inputs.
            // Safe to blindly restore: the tx was malformed so inputs weren't spent
            // by a competing transaction.
            let row = sqlx::query("SELECT transaction_id FROM transactions WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&mut *tx)
                .await?;

            if let Some(row) = row {
                let transaction_id: i64 = row.get("transaction_id");

                // Restore spent inputs: set spendable = true and clear spent_by
                sqlx::query(
                    "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE spent_by = ?",
                )
                .bind(now)
                .bind(transaction_id)
                .execute(&mut *tx)
                .await?;

                // Mark this transaction's own outputs (change, etc.) as unspendable
                sqlx::query(
                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                )
                .bind(now)
                .bind(transaction_id)
                .execute(&mut *tx)
                .await?;

                // Update transaction status to failed
                sqlx::query(
                    "UPDATE transactions SET status = ?, updated_at = ? WHERE transaction_id = ?",
                )
                .bind(TransactionStatus::Failed.as_str())
                .bind(now)
                .bind(transaction_id)
                .execute(&mut *tx)
                .await?;
            }

            // Update proven_tx_req status
            sqlx::query("UPDATE proven_tx_reqs SET status = ?, updated_at = ? WHERE txid = ?")
                .bind("invalid")
                .bind(now)
                .bind(txid)
                .execute(&mut *tx)
                .await?;
        }

        BroadcastOutcome::DoubleSpend { .. } => {
            // Double-spend — a competing transaction spent one or more of our inputs.
            // We must NOT blindly re-mark inputs as spendable because they may be
            // permanently consumed on-chain by the competing tx. Instead, verify each
            // input against the chain via is_utxo() before restoring.
            //
            // The UTXO verification was performed before the SQL transaction started
            // (see `verified_input_ids` below). Only verified-spendable inputs are
            // restored here.
            let row = sqlx::query("SELECT transaction_id FROM transactions WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&mut *tx)
                .await?;

            if let Some(row) = row {
                let transaction_id: i64 = row.get("transaction_id");

                // Restore ONLY inputs verified as still-unspent on-chain.
                // `verified_input_ids` was populated before the SQL transaction.
                for output_id in &verified_input_ids {
                    sqlx::query(
                        "UPDATE outputs SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ? WHERE output_id = ? AND spent_by = ?",
                    )
                    .bind(now)
                    .bind(output_id)
                    .bind(transaction_id)
                    .execute(&mut *tx)
                    .await?;
                }

                // Mark this transaction's own outputs (change, etc.) as unspendable
                sqlx::query(
                    "UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ? AND spent_by IS NULL",
                )
                .bind(now)
                .bind(transaction_id)
                .execute(&mut *tx)
                .await?;

                // Update transaction status to failed
                sqlx::query(
                    "UPDATE transactions SET status = ?, updated_at = ? WHERE transaction_id = ?",
                )
                .bind(TransactionStatus::Failed.as_str())
                .bind(now)
                .bind(transaction_id)
                .execute(&mut *tx)
                .await?;
            }

            // Update proven_tx_req status
            sqlx::query("UPDATE proven_tx_reqs SET status = ?, updated_at = ? WHERE txid = ?")
                .bind("doubleSpend")
                .bind(now)
                .bind(txid)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    Ok(())
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
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: None,
            txid: Some("abc".to_string()),
            raw_tx: Some(vec![1, 2, 3]),
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args)
            .unwrap_err()
            .to_string()
            .contains("reference is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_missing_txid() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("ref123".to_string()),
            txid: None,
            raw_tx: Some(vec![1, 2, 3]),
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args)
            .unwrap_err()
            .to_string()
            .contains("txid is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_missing_raw_tx() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("ref123".to_string()),
            txid: Some("abc".to_string()),
            raw_tx: None,
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args)
            .unwrap_err()
            .to_string()
            .contains("raw_tx is required"));
    }

    #[test]
    fn test_validate_process_action_args_new_tx_empty_raw_tx() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("ref123".to_string()),
            txid: Some("abc".to_string()),
            raw_tx: Some(vec![]),
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args)
            .unwrap_err()
            .to_string()
            .contains("raw_tx cannot be empty"));
    }

    #[test]
    fn test_validate_process_action_args_not_new_tx_missing_txid() {
        let args = StorageProcessActionArgs {
            is_new_tx: false,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: None,
            txid: None,
            raw_tx: None,
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args)
            .unwrap_err()
            .to_string()
            .contains("txid is required"));
    }

    #[test]
    fn test_validate_process_action_args_success() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("ref123".to_string()),
            txid: Some("abc".to_string()),
            raw_tx: Some(vec![1, 2, 3]),
            send_with: vec![],
        };
        assert!(validate_process_action_args(&args).is_ok());
    }

    #[test]
    fn test_determine_statuses_no_send() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: None,
            txid: None,
            raw_tx: None,
            send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "nosend");
        assert_eq!(req_status, "nosend");
    }

    #[test]
    fn test_determine_statuses_delayed() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: true,
            reference: None,
            txid: None,
            raw_tx: None,
            send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "unprocessed");
        assert_eq!(req_status, "unsent");
    }

    #[test]
    fn test_determine_statuses_immediate() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: None,
            txid: None,
            raw_tx: None,
            send_with: vec![],
        };
        let (tx_status, req_status) = determine_statuses(&args);
        assert_eq!(tx_status, "unprocessed");
        assert_eq!(req_status, "unprocessed");
    }

    #[test]
    fn test_determine_statuses_send_with_overrides_no_send() {
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: true,
            is_no_send: true,
            is_delayed: false,
            reference: None,
            txid: None,
            raw_tx: None,
            send_with: vec![],
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
             00000000",
        )
        .unwrap();

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
    use bsv_rs::wallet::CreateActionOutput;

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
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction for process_action".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = storage
            .create_action(
                &crate::storage::traits::AuthId::with_user_id("02user_identity_key", user.user_id),
                args,
            )
            .await
            .unwrap();

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
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();
        // Store a coinbase-like raw_tx so BEEF construction can find this transaction
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap();
        let tx_result = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) VALUES (?, 'completed', 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)",
        )
        .bind(user_id).bind(satoshis)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(&raw_tx)
        .bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();

        let transaction_id = tx_result.last_insert_rowid();
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

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
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some("nonexistent_ref".to_string()),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user.user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_process_action_invalid_txid() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let wrong_txid = "0000000000000000000000000000000000000000000000000000000000000000";

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(wrong_txid.to_string()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("txid mismatch"));
    }

    #[tokio::test]
    async fn test_process_action_with_nosend() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.send_with_results.is_some());
        assert!(result.send_with_results.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_process_action_with_delayed() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: true,
            reference: Some(reference),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        assert_eq!(send_results[0].status, "sending");
    }

    #[tokio::test]
    async fn test_process_action_immediate_broadcast() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        assert_eq!(send_results[0].status, "sending");
    }

    #[tokio::test]
    async fn test_process_action_verify_tx_updated() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: Some(reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        process_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        let row = sqlx::query("SELECT txid, status FROM transactions WHERE reference = ?")
            .bind(&reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let db_txid: String = row.get("txid");
        let db_status: String = row.get("status");
        assert_eq!(db_txid, txid);
        assert_eq!(db_status, "nosend");
    }

    #[tokio::test]
    async fn test_process_action_verify_proven_tx_req_created() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx.clone()),
            send_with: vec![],
        };

        process_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        let row = sqlx::query("SELECT txid, raw_tx FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let db_raw_tx: Vec<u8> = row.get("raw_tx");
        assert_eq!(db_raw_tx, raw_tx);
    }

    #[tokio::test]
    async fn test_process_action_already_processed() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        sqlx::query("UPDATE transactions SET status = 'completed' WHERE reference = ?")
            .bind(&reference)
            .execute(storage.pool())
            .await
            .unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("status"));
    }

    /// Test re-processing a transaction with is_new_tx=false (Go: TestProcessActionTwice)
    #[tokio::test]
    async fn test_process_action_twice_with_is_new_tx_false() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // First process with is_new_tx=true
        let args1 = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };
        let result1 = process_action_internal(&storage, user_id, args1)
            .await
            .unwrap();
        assert!(result1.send_with_results.is_some());
        assert_eq!(result1.send_with_results.as_ref().unwrap().len(), 1);
        assert_eq!(
            result1.send_with_results.as_ref().unwrap()[0].status,
            "sending"
        );

        // Second process with is_new_tx=false (re-broadcast)
        let args2 = StorageProcessActionArgs {
            is_new_tx: false,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: None,
            txid: Some(txid.clone()),
            raw_tx: None,
            send_with: vec![],
        };
        let result2 = process_action_internal(&storage, user_id, args2)
            .await
            .unwrap();
        assert!(result2.send_with_results.is_some());
        let send_results = result2.send_with_results.unwrap();
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        // Already sent tx should return "sending" status
        assert_eq!(send_results[0].status, "sending");
    }

    /// Test error when is_new_tx=false for non-existent tx (Go: TestProcessActionErrorCases)
    #[tokio::test]
    async fn test_process_action_is_new_tx_false_for_unstored_tx() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        let nonexistent_txid = "0000000000000000000000000000000000000000000000000000000000000001";
        let args = StorageProcessActionArgs {
            is_new_tx: false,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: None,
            txid: Some(nonexistent_txid.to_string()),
            raw_tx: None,
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user.user_id, args)
            .await
            .unwrap();
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // Clear the input_beef to simulate already processed state
        sqlx::query("UPDATE transactions SET input_beef = NULL WHERE reference = ?")
            .bind(&reference)
            .execute(storage.pool())
            .await
            .unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // Set is_outgoing to false
        sqlx::query("UPDATE transactions SET is_outgoing = 0 WHERE reference = ?")
            .bind(&reference)
            .execute(storage.pool())
            .await
            .unwrap();

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference.clone()),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
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
            is_new_tx: true,
            is_send_with: true,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![other_txid.to_string()],
        };

        let result = process_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        // Should have results for both txids
        assert_eq!(send_results.len(), 2);

        // Verify batch was set (multiple txs get a batch)
        let row = sqlx::query("SELECT batch FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let batch: Option<String> = row.get("batch");
        assert!(batch.is_some(), "Batch should be set for multiple txs");
    }

    /// Test send_with overrides no_send (Go: SendWith overrides IsNoSend)
    #[tokio::test]
    async fn test_process_action_send_with_overrides_no_send() {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        // is_no_send=true but is_send_with=true should still broadcast
        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: true,
            is_no_send: true,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        let result = process_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.send_with_results.is_some());
        let send_results = result.send_with_results.unwrap();
        // Should still have broadcast result since send_with overrides no_send
        assert_eq!(send_results.len(), 1);
        assert_eq!(send_results[0].txid, txid);
        // Status should be sending (immediate broadcast) not nosend
        assert_eq!(send_results[0].status, "sending");
    }

    /// Test locking script mismatch error (Go: validateNewTxOutputs)
    #[tokio::test]
    async fn test_process_action_locking_script_mismatch() {
        let (storage, user_id, reference) = setup_storage_with_action().await;

        // Use a different locking script than what's stored
        let different_script =
            hex::decode("76a914000000000000000000000000000000000000000088ac").unwrap();
        let raw_tx = create_raw_transaction(&[&different_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference),
            txid: Some(txid),
            raw_tx: Some(raw_tx),
            send_with: vec![],
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: Some(reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        process_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        // Find the transaction to get its ID
        let tx_row = sqlx::query("SELECT transaction_id FROM transactions WHERE reference = ?")
            .bind(&reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
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
                                                // nosend outputs are NOT marked spendable until the tx is actually broadcast
        assert!(!spendable, "nosend output should not be marked spendable");
    }

    /// Test proven_tx_req status based on different modes
    #[tokio::test]
    async fn test_process_action_proven_tx_req_status_modes() {
        // Test nosend mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script =
                hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true,
                is_send_with: false,
                is_no_send: true,
                is_delayed: false,
                reference: Some(reference),
                txid: Some(txid.clone()),
                raw_tx: Some(raw_tx),
                send_with: vec![],
            };
            process_action_internal(&storage, user_id, args)
                .await
                .unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            let status: String = row.get("status");
            assert_eq!(status, "nosend");
        }

        // Test delayed mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script =
                hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true,
                is_send_with: false,
                is_no_send: false,
                is_delayed: true,
                reference: Some(reference),
                txid: Some(txid.clone()),
                raw_tx: Some(raw_tx),
                send_with: vec![],
            };
            process_action_internal(&storage, user_id, args)
                .await
                .unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            let status: String = row.get("status");
            // After broadcast phase, delayed should be 'unsent' (ready for background broadcaster)
            assert_eq!(status, "unsent");
        }

        // Test immediate mode
        {
            let (storage, user_id, reference) = setup_storage_with_action().await;
            let locking_script =
                hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
            let raw_tx = create_raw_transaction(&[&locking_script]);
            let txid = compute_txid(&raw_tx);

            let args = StorageProcessActionArgs {
                is_new_tx: true,
                is_send_with: false,
                is_no_send: false,
                is_delayed: false,
                reference: Some(reference),
                txid: Some(txid.clone()),
                raw_tx: Some(raw_tx),
                send_with: vec![],
            };
            process_action_internal(&storage, user_id, args)
                .await
                .unwrap();

            let row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            let status: String = row.get("status");
            // BUG-004 FIX: immediate mode now uses 'unsent' instead of 'unprocessed'
            // so send_waiting_transactions() can retry on broadcast failure.
            assert_eq!(status, "unsent");
        }
    }

    // =========================================================================
    // Broadcast Status Update Tests
    // =========================================================================

    /// Helper: set up a fully processed transaction ready for broadcast status update.
    /// Returns (storage, txid, transaction_id) where:
    /// - The seed (input) output has spendable=0, spent_by=transaction_id
    /// - The transaction's own output has spendable=1 (immediate mode)
    /// - proven_tx_req exists with status 'unsent' (BUG-004: was 'unprocessed')
    async fn setup_processed_transaction() -> (StorageSqlx, String, i64) {
        let (storage, user_id, reference) = setup_storage_with_action().await;
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();
        let raw_tx = create_raw_transaction(&[&locking_script]);
        let txid = compute_txid(&raw_tx);

        let args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx),
            send_with: vec![],
        };

        process_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        let tx_row = sqlx::query("SELECT transaction_id FROM transactions WHERE reference = ?")
            .bind(&reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let transaction_id: i64 = tx_row.get("transaction_id");

        (storage, txid, transaction_id)
    }

    /// Test that broadcast failure restores input outputs, marks own outputs
    /// unspendable, and sets transaction status to 'failed'.
    #[tokio::test]
    async fn test_broadcast_failure_marks_change_outputs_unspendable() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        // Sanity: before broadcast update, the tx's own output should be spendable
        let own_output = sqlx::query(
            "SELECT spendable FROM outputs WHERE transaction_id = ? AND spent_by IS NULL",
        )
        .bind(transaction_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let spendable_before: bool = own_output.get("spendable");
        assert!(
            spendable_before,
            "own output should be spendable before broadcast failure"
        );

        // Sanity: the input (seed) output should be marked as spent (spendable=0, spent_by set)
        let input_output =
            sqlx::query("SELECT spendable, spent_by FROM outputs WHERE spent_by = ?")
                .bind(transaction_id)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let input_spendable: bool = input_output.get("spendable");
        assert!(
            !input_spendable,
            "input output should be non-spendable (spent) before broadcast failure"
        );

        // --- Act: broadcast failed (permanent — InvalidTx) ---
        let outcome = BroadcastOutcome::InvalidTx {
            details: vec!["test: ARC rejected transaction".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // --- Verify: input outputs restored to spendable ---
        // The seed output was spent_by this transaction; it should now be restored
        // (spent_by = NULL, spendable = 1). We find it by its seed txid.
        let restored_input = sqlx::query(
            "SELECT spendable, spent_by FROM outputs WHERE txid = '0000000000000000000000000000000000000000000000000000000000000001'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let restored_spendable: bool = restored_input.get("spendable");
        let restored_spent_by: Option<i64> = restored_input.get("spent_by");
        assert!(
            restored_spendable,
            "input output should be restored to spendable after broadcast failure"
        );
        assert!(
            restored_spent_by.is_none(),
            "input output spent_by should be cleared after broadcast failure"
        );

        // --- Verify: transaction's own outputs marked unspendable (NEW behavior) ---
        let own_output_after =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(transaction_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let own_spendable_after: bool = own_output_after.get("spendable");
        assert!(
            !own_spendable_after,
            "transaction's own output should be marked unspendable after broadcast failure"
        );

        // --- Verify: transaction status is 'failed' ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let status: String = tx_row.get("status");
        assert_eq!(status, "failed");

        // --- Verify: proven_tx_req status is 'invalid' ---
        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let req_status: String = req_row.get("status");
        assert_eq!(req_status, "invalid");
    }

    /// Test that broadcast success keeps outputs spendable and sets
    /// transaction status to 'unproven'.
    #[tokio::test]
    async fn test_broadcast_success_keeps_outputs_spendable() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        // --- Act: broadcast succeeded ---
        update_transaction_status_after_broadcast_internal(
            &storage,
            &txid,
            &BroadcastOutcome::Success,
        )
        .await
        .unwrap();

        // --- Verify: transaction's own output remains spendable ---
        let own_output =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(transaction_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let spendable: bool = own_output.get("spendable");
        assert!(
            spendable,
            "transaction's own output should remain spendable after broadcast success"
        );

        // --- Verify: transaction status is 'unproven' ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let status: String = tx_row.get("status");
        assert_eq!(status, "unproven");

        // --- Verify: proven_tx_req status is 'unmined' ---
        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let req_status: String = req_row.get("status");
        assert_eq!(req_status, "unmined");
    }

    // =========================================================================
    // BroadcastOutcome classification tests
    // =========================================================================

    use crate::services::traits::{PostBeefResult, PostTxResultForTxid};

    fn make_success_result(name: &str) -> PostBeefResult {
        PostBeefResult {
            name: name.to_string(),
            status: "success".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "abc123".to_string(),
                status: "success".to_string(),
                double_spend: false,
                orphan_mempool: false,
                competing_txs: None,
                data: None,
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }
    }

    fn make_error_result(
        name: &str,
        double_spend: bool,
        service_error: bool,
        status: &str,
    ) -> PostBeefResult {
        PostBeefResult {
            name: name.to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "abc123".to_string(),
                status: status.to_string(),
                double_spend,
                orphan_mempool: false,
                competing_txs: if double_spend {
                    Some(vec!["competing_txid_1".to_string()])
                } else {
                    None
                },
                data: Some(format!("Error from {}", name)),
                service_error,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }
    }

    #[test]
    fn test_classify_success() {
        let results = vec![make_success_result("taal")];
        let outcome = classify_broadcast_results(&results);
        assert!(outcome.is_success());
        assert!(!outcome.is_transient());
    }

    #[test]
    fn test_classify_double_spend() {
        let results = vec![make_error_result("taal", true, false, "error")];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::DoubleSpend { .. }));
        if let BroadcastOutcome::DoubleSpend { competing_txs, .. } = &outcome {
            assert_eq!(competing_txs, &["competing_txid_1"]);
        }
    }

    #[test]
    fn test_classify_invalid_tx() {
        let results = vec![make_error_result("taal", false, false, "460")];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::InvalidTx { .. }));
    }

    #[test]
    fn test_classify_service_error() {
        let results = vec![make_error_result("taal", false, true, "error")];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::ServiceError { .. }));
        assert!(outcome.is_transient());
    }

    #[test]
    fn test_classify_mixed_double_spend_takes_priority() {
        // One provider says double-spend, another says service error
        let results = vec![
            make_error_result("taal", true, false, "error"),
            make_error_result("gorilla", false, true, "error"),
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::DoubleSpend { .. }));
    }

    #[test]
    fn test_classify_success_overrides_errors() {
        // One provider succeeds, another fails
        let results = vec![
            make_success_result("taal"),
            make_error_result("gorilla", false, true, "error"),
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(outcome.is_success());
    }

    #[test]
    fn test_classify_empty_results() {
        let results: Vec<PostBeefResult> = vec![];
        let outcome = classify_broadcast_results(&results);
        // No results = no success, no double spend, no invalid → service error
        assert!(matches!(outcome, BroadcastOutcome::ServiceError { .. }));
    }

    fn make_orphan_result(name: &str) -> PostBeefResult {
        PostBeefResult {
            name: name.to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "abc123".to_string(),
                status: "error".to_string(),
                double_spend: false,
                orphan_mempool: true,
                competing_txs: None,
                data: Some(format!("orphan mempool from {}", name)),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }
    }

    #[test]
    fn test_classify_orphan_mempool() {
        let results = vec![make_orphan_result("taal")];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::OrphanMempool { .. }),
            "Expected OrphanMempool but got {:?}",
            outcome
        );
        assert!(
            outcome.is_transient(),
            "OrphanMempool should be classified as transient"
        );
    }

    #[test]
    fn test_classify_orphan_mempool_vs_double_spend_priority() {
        // When both orphan_mempool and double_spend are reported by different
        // providers, double_spend must take priority because it is a permanent
        // failure. Orphan mempool is transient.
        let results = vec![
            make_orphan_result("gorilla"),
            make_error_result("taal", true, false, "error"),
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::DoubleSpend { .. }),
            "DoubleSpend should take priority over OrphanMempool, got {:?}",
            outcome
        );
    }

    #[test]
    fn test_classify_orphan_mempool_vs_success_priority() {
        // When one provider reports success and another reports orphan mempool,
        // success must take priority (the tx was accepted somewhere).
        let results = vec![make_orphan_result("gorilla"), make_success_result("taal")];
        let outcome = classify_broadcast_results(&results);
        assert!(
            outcome.is_success(),
            "Success should take priority over OrphanMempool, got {:?}",
            outcome
        );
    }

    #[test]
    fn test_error_message_includes_details() {
        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["tx1".to_string(), "tx2".to_string()],
            details: vec!["taal: error [DOUBLE_SPEND]".to_string()],
        };
        let msg = outcome.error_message("abc123").unwrap();
        assert!(msg.contains("abc123"));
        assert!(msg.contains("double spend"));
        assert!(msg.contains("tx1"));
        assert!(msg.contains("tx2"));
    }

    // =========================================================================
    // Differentiated broadcast outcome status update tests
    // =========================================================================

    /// Test that ServiceError keeps inputs locked and sets req to 'sending'.
    /// This is the key divergence fix — transient failures no longer restore inputs.
    #[tokio::test]
    async fn test_service_error_keeps_inputs_locked() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        // Verify inputs are currently locked (spendable=0)
        let input_before = sqlx::query("SELECT spendable FROM outputs WHERE spent_by = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(!input_before.get::<bool, _>("spendable"));

        // --- Act: service error (transient) ---
        let outcome = BroadcastOutcome::ServiceError {
            details: vec!["taal: timeout".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // --- Verify: inputs stay LOCKED (not restored) ---
        let input_after = sqlx::query("SELECT spendable FROM outputs WHERE spent_by = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(
            !input_after.get::<bool, _>("spendable"),
            "inputs must stay locked on service error for retry"
        );

        // --- Verify: proven_tx_req status is 'sending' (retry eligible) ---
        let req_row = sqlx::query("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(req_row.get::<String, _>("status"), "sending");
        // Attempts should have been incremented (initial value from process_action + 1)
        assert!(
            req_row.get::<i32, _>("attempts") >= 1,
            "attempts should be bumped on service error"
        );

        // --- Verify: transaction stays in 'sending' (not 'failed') ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        // Transaction was set to 'sending' by process_action; service error doesn't change it
        assert_eq!(tx_row.get::<String, _>("status"), "sending");
    }

    /// Test that DoubleSpend with UTXO-verified inputs restores only verified inputs
    /// and sets req to 'doubleSpend'.
    ///
    /// Mock services are configured so:
    /// - get_status_for_txids returns empty (reconcile fails → proceeds to rollback)
    /// - is_utxo returns true (all inputs verified as still unspent)
    #[tokio::test]
    async fn test_double_spend_restores_inputs() {
        use crate::services::mock::{MockResponse, MockWalletServices};
        use crate::services::traits::{GetStatusForTxidsResult, TxStatusDetail};
        use crate::storage::traits::WalletStorageProvider;

        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        // Configure mock services: is_utxo returns true (default),
        // get_status_for_txids returns no results (reconcile won't override)
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: "no_match".to_string(),
                    status: "unknown".to_string(),
                    depth: None,
                }],
            }))
            .build();
        storage.set_services(std::sync::Arc::new(mock));

        // --- Act: double spend (permanent) ---
        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // --- Verify: inputs restored (is_utxo returned true) ---
        let restored = sqlx::query(
            "SELECT spendable, spent_by FROM outputs WHERE txid = '0000000000000000000000000000000000000000000000000000000000000001'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(restored.get::<bool, _>("spendable"));
        assert!(restored.get::<Option<i64>, _>("spent_by").is_none());

        // --- Verify: own outputs marked unspendable ---
        let own =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(transaction_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(!own.get::<bool, _>("spendable"));

        // --- Verify: transaction is 'failed' ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(tx_row.get::<String, _>("status"), "failed");

        // --- Verify: proven_tx_req is 'doubleSpend' ---
        let req = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(req.get::<String, _>("status"), "doubleSpend");
    }

    /// Test that DoubleSpend WITHOUT services configured keeps inputs locked (fail-safe).
    #[tokio::test]
    async fn test_double_spend_no_services_keeps_inputs_locked() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;
        // No mock services configured — utxo_verified_input_ids returns empty

        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // --- Verify: inputs NOT restored (no services → fail-safe) ---
        let locked = sqlx::query(
            "SELECT spendable, spent_by FROM outputs WHERE txid = '0000000000000000000000000000000000000000000000000000000000000001'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(
            !locked.get::<bool, _>("spendable"),
            "inputs must stay locked when services unavailable (fail-safe)"
        );

        // --- Verify: own outputs marked unspendable ---
        let own =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(transaction_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(!own.get::<bool, _>("spendable"));

        // --- Verify: transaction is 'failed' ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(tx_row.get::<String, _>("status"), "failed");

        // --- Verify: proven_tx_req is 'doubleSpend' ---
        let req = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(req.get::<String, _>("status"), "doubleSpend");
    }

    /// Test that ServiceError does NOT mark own outputs as unspendable.
    #[tokio::test]
    async fn test_service_error_keeps_own_outputs_spendable() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        let outcome = BroadcastOutcome::ServiceError {
            details: vec!["network timeout".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Own outputs (change) should stay spendable — the tx may still succeed
        let own =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(transaction_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(
            own.get::<bool, _>("spendable"),
            "own outputs must stay spendable on service error"
        );
    }

    // =========================================================================
    // Broadcast Reconciliation Tests
    // =========================================================================

    use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
    use crate::services::traits::{GetStatusForTxidsResult, TxStatusDetail};
    use crate::storage::traits::WalletStorageProvider;
    use std::sync::Arc;

    /// Helper: set up a processed transaction with mock services configured.
    async fn setup_with_mock_services(mock: MockWalletServices) -> (StorageSqlx, String, i64) {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;
        storage.set_services(Arc::new(mock));
        (storage, txid, transaction_id)
    }

    /// DoubleSpend but tx is found in mempool ("known") — should treat as success.
    #[tokio::test]
    async fn test_reconciliation_double_spend_found_in_mempool() {
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: String::new(), // will be matched by txid field below
                    status: "known".to_string(),
                    depth: None,
                }],
            }))
            .build();

        let (storage, txid, transaction_id) = setup_with_mock_services(mock).await;

        // Patch the mock result to contain the actual txid
        // (We need to re-set services because we now know the txid)
        let mock2 = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "known".to_string(),
                    depth: None,
                }],
            }))
            .build();
        storage.set_services(Arc::new(mock2));

        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Should be treated as success: tx→'unproven', req→'unmined'
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "unproven",
            "tx should be 'unproven' when reconciliation finds it in mempool"
        );

        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            req_row.get::<String, _>("status"),
            "unmined",
            "req should be 'unmined' when reconciliation finds tx in mempool"
        );

        // Inputs should NOT be restored (still locked by the successful tx)
        let input = sqlx::query("SELECT spendable FROM outputs WHERE spent_by = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(
            !input.get::<bool, _>("spendable"),
            "inputs must stay locked when reconciliation overrides to success"
        );
    }

    /// DoubleSpend but tx is found mined — should treat as success.
    #[tokio::test]
    async fn test_reconciliation_double_spend_found_mined() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "mined".to_string(),
                    depth: Some(3),
                }],
            }))
            .build();
        storage.set_services(Arc::new(mock));

        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Should be treated as success
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "unproven",
            "tx should be 'unproven' when reconciliation finds it mined"
        );

        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            req_row.get::<String, _>("status"),
            "unmined",
            "req should be 'unmined' when reconciliation finds tx mined"
        );
    }

    /// DoubleSpend and tx is truly not found — normal rollback should happen.
    #[tokio::test]
    async fn test_reconciliation_tx_truly_not_found() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "unknown".to_string(),
                    depth: None,
                }],
            }))
            .build();
        storage.set_services(Arc::new(mock));

        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Normal rollback: tx→'failed', inputs restored
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "failed",
            "tx should be 'failed' when reconciliation confirms tx not found"
        );

        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            req_row.get::<String, _>("status"),
            "doubleSpend",
            "req should be 'doubleSpend' when reconciliation confirms tx not found"
        );

        // Inputs should be restored
        let restored = sqlx::query(
            "SELECT spendable, spent_by FROM outputs WHERE txid = '0000000000000000000000000000000000000000000000000000000000000001'",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(
            restored.get::<bool, _>("spendable"),
            "inputs must be restored when tx truly not found"
        );
        assert!(
            restored.get::<Option<i64>, _>("spent_by").is_none(),
            "spent_by must be cleared when tx truly not found"
        );
    }

    /// Status check returns error — should fall through to normal rollback.
    #[tokio::test]
    async fn test_reconciliation_status_check_fails_falls_through() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Error(
                MockErrorKind::NetworkError,
                "connection refused".to_string(),
            ))
            .build();
        storage.set_services(Arc::new(mock));

        let outcome = BroadcastOutcome::InvalidTx {
            details: vec!["ARC rejected".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Should fall through to normal rollback
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "failed",
            "tx should be 'failed' when status check errors"
        );

        let req_row = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            req_row.get::<String, _>("status"),
            "invalid",
            "req should be 'invalid' when status check errors"
        );
    }

    /// No services set — should fall through to normal rollback.
    #[tokio::test]
    async fn test_reconciliation_no_services_falls_through() {
        // setup_processed_transaction does NOT set services
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_abc".to_string()],
            details: vec!["taal: DOUBLE_SPEND_ATTEMPTED".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // Should fall through to normal rollback
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "failed",
            "tx should be 'failed' when no services available"
        );
    }

    /// ServiceError outcome should NOT trigger reconciliation — it's transient.
    #[tokio::test]
    async fn test_reconciliation_does_not_affect_service_error() {
        let (storage, txid, _transaction_id) = setup_processed_transaction().await;

        // Set up mock that would return "known" if called — but it should NOT be called
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: txid.clone(),
                    status: "known".to_string(),
                    depth: None,
                }],
            }))
            .build();
        storage.set_services(Arc::new(mock));

        let outcome = BroadcastOutcome::ServiceError {
            details: vec!["timeout".to_string()],
        };

        update_transaction_status_after_broadcast_internal(&storage, &txid, &outcome)
            .await
            .unwrap();

        // ServiceError path: tx stays 'sending', req→'sending'
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "sending",
            "ServiceError should keep tx in 'sending', not trigger reconciliation"
        );
    }

    // =========================================================================
    // Partial UTXO Verification Tests
    // =========================================================================

    /// Test that doubleSpend with partial is_utxo results restores only verified outputs.
    ///
    /// Sets up a transaction with 2 inputs. Mock is_utxo returns `true` for
    /// the first input and `false` for the second. Only the first should be
    /// restored to spendable.
    #[tokio::test]
    async fn test_double_spend_partial_utxo_verification() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let user_id = user.user_id;
        let now = Utc::now();
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        // --- Create source transaction 1 (will be input 1) ---
        let source_txid1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let source_raw_tx1 = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap();
        let r1 = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) VALUES (?, 'completed', 'source_ref1', 0, 50000, 1, 0, 'Source tx 1', ?, ?, ?, ?)",
        )
        .bind(user_id).bind(source_txid1).bind(&source_raw_tx1).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
        let source_tx_id1 = r1.last_insert_rowid();

        let r_out1 = sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) VALUES (?, ?, ?, 0, 50000, ?, ?, 'P2PKH', 0, 1, 'prefix1', 'suffix1', 'storage', 'change', 'source output 1', ?, ?)",
        )
        .bind(user_id).bind(source_tx_id1).bind(basket.basket_id).bind(&locking_script)
        .bind(source_txid1).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
        let output_id1 = r_out1.last_insert_rowid();

        // --- Create source transaction 2 (will be input 2) ---
        let source_txid2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let r2 = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) VALUES (?, 'completed', 'source_ref2', 0, 30000, 1, 0, 'Source tx 2', ?, ?, ?, ?)",
        )
        .bind(user_id).bind(source_txid2).bind(&source_raw_tx1).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
        let source_tx_id2 = r2.last_insert_rowid();

        let r_out2 = sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) VALUES (?, ?, ?, 0, 30000, ?, ?, 'P2PKH', 0, 1, 'prefix2', 'suffix2', 'storage', 'change', 'source output 2', ?, ?)",
        )
        .bind(user_id).bind(source_tx_id2).bind(basket.basket_id).bind(&locking_script)
        .bind(source_txid2).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
        let output_id2 = r_out2.last_insert_rowid();

        // --- Create the spending transaction ---
        let spending_txid = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let spending_raw_tx = create_raw_transaction(&[&locking_script]);
        let r_spend = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) VALUES (?, 'sending', 'spend_ref', 1, 80000, 1, 0, 'Spending tx', ?, ?, ?, ?)",
        )
        .bind(user_id).bind(spending_txid).bind(&spending_raw_tx).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();
        let spending_tx_id = r_spend.last_insert_rowid();

        // Mark both source outputs as spent by the spending transaction
        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(spending_tx_id)
            .bind(output_id1)
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(spending_tx_id)
            .bind(output_id2)
            .execute(storage.pool())
            .await
            .unwrap();

        // Create a change output for the spending transaction
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) VALUES (?, ?, ?, 0, 79000, ?, ?, 'P2PKH', 1, 1, 'prefix_c', 'suffix_c', 'storage', 'change', 'change output', ?, ?)",
        )
        .bind(user_id).bind(spending_tx_id).bind(basket.basket_id).bind(&locking_script)
        .bind(spending_txid).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();

        // Create proven_tx_req for the spending transaction
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, raw_tx, status, attempts, history, notify, created_at, updated_at) VALUES (?, ?, 'unprocessed', 0, '{}', '{}', ?, ?)",
        )
        .bind(spending_txid).bind(&spending_raw_tx).bind(now).bind(now)
        .execute(storage.pool()).await.unwrap();

        // --- Configure mock services ---
        // is_utxo Sequence: first call returns true, second returns false.
        // This simulates: input 1 is still a UTXO, input 2 was consumed by competitor.
        let mock = MockWalletServices::builder()
            .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
                name: "MockProvider".to_string(),
                status: "success".to_string(),
                error: None,
                results: vec![TxStatusDetail {
                    txid: "no_match".to_string(),
                    status: "unknown".to_string(),
                    depth: None,
                }],
            }))
            .is_utxo_response(MockResponse::Sequence(vec![
                MockResponse::Success(true),
                MockResponse::Success(false),
            ]))
            .build();
        storage.set_services(Arc::new(mock));

        // --- Act: trigger doubleSpend rollback ---
        let outcome = BroadcastOutcome::DoubleSpend {
            competing_txs: vec!["competing_xyz".to_string()],
            details: vec!["double spend detected".to_string()],
        };
        update_transaction_status_after_broadcast_internal(&storage, spending_txid, &outcome)
            .await
            .unwrap();

        // --- Verify: first input (is_utxo=true) should be restored to spendable ---
        let row1 = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(output_id1)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(
            row1.get::<bool, _>("spendable"),
            "Input 1 (is_utxo=true) should be restored to spendable"
        );
        assert!(
            row1.get::<Option<i64>, _>("spent_by").is_none(),
            "Input 1 spent_by should be cleared"
        );

        // --- Verify: second input (is_utxo=false) should stay locked ---
        let row2 = sqlx::query("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
            .bind(output_id2)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert!(
            !row2.get::<bool, _>("spendable"),
            "Input 2 (is_utxo=false) should stay locked (not restored)"
        );
        assert_eq!(
            row2.get::<Option<i64>, _>("spent_by"),
            Some(spending_tx_id),
            "Input 2 spent_by should remain set"
        );

        // --- Verify: own outputs marked unspendable ---
        let own =
            sqlx::query("SELECT spendable FROM outputs WHERE transaction_id = ? AND txid = ?")
                .bind(spending_tx_id)
                .bind(spending_txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(
            !own.get::<bool, _>("spendable"),
            "Spending tx's own output should be marked unspendable"
        );

        // --- Verify: transaction is 'failed' ---
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(spending_tx_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(tx_row.get::<String, _>("status"), "failed");

        // --- Verify: proven_tx_req is 'doubleSpend' ---
        let req = sqlx::query("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(spending_txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(req.get::<String, _>("status"), "doubleSpend");
    }

    /// Success outcome should NOT trigger reconciliation — it's already success.
    #[tokio::test]
    async fn test_reconciliation_does_not_affect_success() {
        let (storage, txid, transaction_id) = setup_processed_transaction().await;

        // No mock services needed — reconciliation should not be called for Success

        update_transaction_status_after_broadcast_internal(
            &storage,
            &txid,
            &BroadcastOutcome::Success,
        )
        .await
        .unwrap();

        // Success path: tx→'unproven'
        let tx_row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(
            tx_row.get::<String, _>("status"),
            "unproven",
            "Success should go straight to 'unproven', not trigger reconciliation"
        );
    }

    // =========================================================================
    // OrphanMempool classification and handling tests
    // =========================================================================

    /// ARC returns orphan_mempool=true, double_spend=false → OrphanMempool outcome.
    #[test]
    fn test_classify_orphan_mempool_from_arc() {
        let results = vec![PostBeefResult {
            name: "arc".to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "abc123".to_string(),
                status: "error".to_string(),
                double_spend: false,
                orphan_mempool: true,
                competing_txs: None,
                data: Some("orphan mempool: parent not found".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::OrphanMempool { .. }));
    }

    /// double_spend=true and orphan_mempool=false → DoubleSpend outcome.
    #[test]
    fn test_classify_real_double_spend_not_orphan() {
        let results = vec![PostBeefResult {
            name: "arc".to_string(),
            status: "error".to_string(),
            txid_results: vec![PostTxResultForTxid {
                txid: "abc123".to_string(),
                status: "error".to_string(),
                double_spend: true,
                orphan_mempool: false,
                competing_txs: Some(vec!["competing_txid_1".to_string()]),
                data: Some("double spend".to_string()),
                service_error: false,
                block_hash: None,
                block_height: None,
                notes: vec![],
            }],
            error: None,
            notes: vec![],
        }];
        let outcome = classify_broadcast_results(&results);
        assert!(matches!(outcome, BroadcastOutcome::DoubleSpend { .. }));
    }

    /// Multiple results: one orphan_mempool, one double_spend → DoubleSpend wins.
    #[test]
    fn test_classify_double_spend_overrides_orphan() {
        let results = vec![
            PostBeefResult {
                name: "arc1".to_string(),
                status: "error".to_string(),
                txid_results: vec![PostTxResultForTxid {
                    txid: "abc123".to_string(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: true,
                    competing_txs: None,
                    data: Some("orphan mempool".to_string()),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![],
                }],
                error: None,
                notes: vec![],
            },
            PostBeefResult {
                name: "arc2".to_string(),
                status: "error".to_string(),
                txid_results: vec![PostTxResultForTxid {
                    txid: "abc123".to_string(),
                    status: "error".to_string(),
                    double_spend: true,
                    orphan_mempool: false,
                    competing_txs: Some(vec!["competing_txid_1".to_string()]),
                    data: Some("double spend".to_string()),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![],
                }],
                error: None,
                notes: vec![],
            },
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(
            matches!(outcome, BroadcastOutcome::DoubleSpend { .. }),
            "DoubleSpend should take priority over OrphanMempool"
        );
    }

    /// Multiple results: one orphan_mempool, one success → Success wins.
    #[test]
    fn test_classify_success_overrides_orphan() {
        let results = vec![
            PostBeefResult {
                name: "arc1".to_string(),
                status: "error".to_string(),
                txid_results: vec![PostTxResultForTxid {
                    txid: "abc123".to_string(),
                    status: "error".to_string(),
                    double_spend: false,
                    orphan_mempool: true,
                    competing_txs: None,
                    data: Some("orphan mempool".to_string()),
                    service_error: false,
                    block_hash: None,
                    block_height: None,
                    notes: vec![],
                }],
                error: None,
                notes: vec![],
            },
            make_success_result("arc2"),
        ];
        let outcome = classify_broadcast_results(&results);
        assert!(
            outcome.is_success(),
            "Success should take priority over OrphanMempool"
        );
    }

    /// OrphanMempool is classified as transient (should be retried).
    #[test]
    fn test_orphan_mempool_is_transient() {
        let outcome = BroadcastOutcome::OrphanMempool { details: vec![] };
        assert!(
            outcome.is_transient(),
            "OrphanMempool should be transient (eligible for retry)"
        );
    }

    /// OrphanMempool error_message returns a non-empty string with relevant info.
    #[test]
    fn test_orphan_mempool_error_message() {
        let outcome = BroadcastOutcome::OrphanMempool {
            details: vec!["test".to_string()],
        };
        let msg = outcome.error_message("abc123");
        assert!(
            msg.is_some(),
            "OrphanMempool should produce an error message"
        );
        let msg = msg.unwrap();
        assert!(!msg.is_empty(), "Error message should not be empty");
        assert!(
            msg.contains("orphan"),
            "Error message should mention orphan: got '{}'",
            msg
        );
        assert!(
            msg.contains("abc123"),
            "Error message should contain the txid: got '{}'",
            msg
        );
        assert!(
            msg.contains("test"),
            "Error message should contain the detail text: got '{}'",
            msg
        );
    }
}
