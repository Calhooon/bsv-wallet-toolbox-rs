//! Internalize Action Implementation
//!
//! This module implements the `internalize_action` method for the `StorageSqlx`
//! wallet storage backend. It allows a wallet to take ownership of outputs in
//! a pre-existing transaction.
//!
//! Two types of outputs are handled:
//! - "wallet payment" - Adds output value to wallet's change balance in "default" basket
//! - "basket insertion" - Custom output in specified basket, no effect on balance

use crate::error::{Error, Result};
use crate::storage::entities::{TableOutput, TableTransaction, TransactionStatus};
use crate::storage::traits::{BeefVerificationMode, StorageInternalizeActionResult};
use bsv_sdk::transaction::Beef;
use bsv_sdk::wallet::{
    BasketInsertion, InternalizeActionArgs, InternalizeActionResult, WalletPayment,
};
use chrono::Utc;
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use super::beef_verification::verify_beef_merkle_proofs;
use super::StorageSqlx;

// =============================================================================
// Constants
// =============================================================================

/// Protocol identifier for wallet payments.
const WALLET_PAYMENT_PROTOCOL: &str = "wallet payment";

/// Protocol identifier for basket insertions.
const BASKET_INSERTION_PROTOCOL: &str = "basket insertion";

// =============================================================================
// Internal Types
// =============================================================================

/// Extracted output data for processing.
#[derive(Debug, Clone)]
struct OutputData {
    vout: u32,
    satoshis: u64,
    locking_script: Vec<u8>,
    protocol: String,
    payment: Option<WalletPayment>,
    insertion: Option<BasketInsertion>,
    existing_output_id: Option<i64>,
    existing_basket_id: Option<i64>,
    existing_is_change: bool,
}

// =============================================================================
// Main Implementation
// =============================================================================

/// Internal implementation of internalize_action.
pub async fn internalize_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: InternalizeActionArgs,
) -> Result<StorageInternalizeActionResult> {
    // Step 1: Parse and validate the AtomicBEEF
    let mut beef = Beef::from_binary(&args.tx).map_err(|e| {
        Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e))
    })?;

    // Get the atomic txid (target transaction)
    let txid = beef.atomic_txid.clone().ok_or_else(|| {
        Error::ValidationError("BEEF is not AtomicBEEF (missing atomic_txid)".to_string())
    })?;

    // Step 1b: Verify BEEF merkle proofs if chain_tracker is set
    // This ensures incoming transactions have valid proofs before internalizing
    if let Some(chain_tracker) = storage.get_chain_tracker().await {
        // Get known txids for TrustKnown mode (transactions already in wallet)
        let known_txids = get_known_txids(storage, user_id).await?;

        // Verify the BEEF merkle proofs against the chain
        // Default to Strict mode for internalize_action
        let _is_valid = verify_beef_merkle_proofs(
            &mut beef,
            chain_tracker.as_ref(),
            BeefVerificationMode::Strict,
            &known_txids,
        ).await?;
        // Note: verify_beef_merkle_proofs returns Err on invalid proofs,
        // Ok(false) if no proofs to verify (empty BEEF), Ok(true) if valid.
        // Both Ok cases are acceptable for internalize_action since unproven
        // transactions are tracked separately.
    }

    // Find the target transaction
    let beef_tx = beef.find_txid(&txid).ok_or_else(|| {
        Error::ValidationError(format!("Could not find transaction {} in AtomicBEEF", txid))
    })?;

    // Extract ALL data from the transaction before any await points.
    // This is necessary because Transaction contains RefCell which is not Send.
    let (tx_outputs_count, tx_version, tx_lock_time, raw_tx, extracted_outputs) = {
        let tx = beef_tx.tx().ok_or_else(|| {
            Error::ValidationError(format!("Transaction {} is txid-only in BEEF", txid))
        })?;

        // Extract all output data from the transaction
        let outputs: Vec<(u64, Vec<u8>)> = tx
            .outputs
            .iter()
            .map(|o| (o.satoshis.unwrap_or(0), o.locking_script.to_binary()))
            .collect();

        (tx.outputs.len(), tx.version, tx.lock_time, tx.to_binary(), outputs)
    };
    // tx is now dropped, safe to await

    // Step 2: Get the user's default (change) basket
    let change_basket = storage.find_or_create_default_basket(user_id).await?;

    // Step 3: Check for existing transaction
    let existing_tx = find_existing_transaction(storage, user_id, &txid).await?;
    let is_merge = existing_tx.is_some();

    // Validate existing transaction status if merging
    if let Some(ref etx) = existing_tx {
        validate_merge_status(&etx.status)?;
    }

    // Step 4: Extract output specifications and validate
    let mut outputs_data: Vec<OutputData> = Vec::new();
    for output_spec in &args.outputs {
        let vout = output_spec.output_index;

        if vout as usize >= tx_outputs_count {
            return Err(Error::ValidationError(format!(
                "Output index {} is out of range (transaction has {} outputs)",
                vout, tx_outputs_count
            )));
        }

        let (satoshis, locking_script) = extracted_outputs[vout as usize].clone();

        let (payment, insertion) = match output_spec.protocol.as_str() {
            WALLET_PAYMENT_PROTOCOL => {
                let p = output_spec.payment_remittance.clone().ok_or_else(|| {
                    Error::ValidationError(format!(
                        "Wallet payment at index {} missing paymentRemittance",
                        vout
                    ))
                })?;
                (Some(p), None)
            }
            BASKET_INSERTION_PROTOCOL => {
                let i = output_spec.insertion_remittance.clone().ok_or_else(|| {
                    Error::ValidationError(format!(
                        "Basket insertion at index {} missing insertionRemittance",
                        vout
                    ))
                })?;
                (None, Some(i))
            }
            _ => {
                return Err(Error::ValidationError(format!(
                    "Unknown protocol: {}",
                    output_spec.protocol
                )));
            }
        };

        outputs_data.push(OutputData {
            vout,
            satoshis,
            locking_script,
            protocol: output_spec.protocol.clone(),
            payment,
            insertion,
            existing_output_id: None,
            existing_basket_id: None,
            existing_is_change: false,
        });
    }

    // Step 5: If merging, load existing outputs
    if is_merge {
        let existing_outputs = load_existing_outputs(storage, user_id, &txid).await?;
        for od in &mut outputs_data {
            if let Some(eo) = existing_outputs.iter().find(|o| o.vout == od.vout as i32) {
                od.existing_output_id = Some(eo.output_id);
                od.existing_basket_id = eo.basket_id;
                od.existing_is_change = eo.change;
            }
        }
    }

    // Step 6: Calculate satoshi changes
    let mut net_satoshis: i64 = 0;
    let change_basket_id = change_basket.basket_id;

    for od in &outputs_data {
        match od.protocol.as_str() {
            WALLET_PAYMENT_PROTOCOL => {
                // Check if already a change output (ignore if so)
                if od.existing_output_id.is_some()
                    && od.existing_basket_id == Some(change_basket_id)
                    && od.existing_is_change
                {
                    // Already a change output, ignore (0 satoshi change)
                } else if od.existing_output_id.is_some() {
                    // Converting non-change output to change
                    net_satoshis += od.satoshis as i64;
                } else {
                    // New output
                    net_satoshis += od.satoshis as i64;
                }
            }
            BASKET_INSERTION_PROTOCOL => {
                if od.existing_basket_id == Some(change_basket_id) && od.existing_is_change {
                    // Converting change to custom basket - reduces balance
                    net_satoshis -= od.satoshis as i64;
                }
            }
            _ => {}
        }
    }

    // Step 7: Process the internalization
    let has_proof = beef.find_bump(&txid).is_some();
    let status = if has_proof { "completed" } else { "unproven" };

    let transaction_id = if is_merge {
        let tx_id = existing_tx.as_ref().unwrap().transaction_id;

        // Update description
        let now = Utc::now();
        sqlx::query(
            "UPDATE transactions SET description = ?, updated_at = ? WHERE transaction_id = ?",
        )
        .bind(&args.description)
        .bind(now)
        .bind(tx_id)
        .execute(storage.pool())
        .await?;

        tx_id
    } else {
        // Create new transaction
        let now = Utc::now();
        let reference = uuid::Uuid::new_v4().to_string();

        // Store the complete incoming BEEF in input_beef column.
        // This is critical for spending - when building output BEEF, we need
        // access to ancestor transactions and any available merkle proofs.
        // Even if the transaction is unconfirmed (no merkle proof), storing
        // the BEEF allows us to construct valid BEEFs for spending by chaining
        // raw transactions together.
        let input_beef_bytes = &args.tx;

        let result = sqlx::query(
            r#"
            INSERT INTO transactions (
                user_id, txid, status, reference, description, satoshis,
                version, lock_time, raw_tx, input_beef, is_outgoing, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&txid)
        .bind(status)
        .bind(&reference)
        .bind(&args.description)
        .bind(net_satoshis)
        .bind(tx_version as i32)
        .bind(tx_lock_time as i64)
        .bind(&raw_tx)
        .bind(input_beef_bytes)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await?;

        result.last_insert_rowid()
    };

    // Step 8: Add labels
    if let Some(ref labels) = args.labels {
        for label in labels {
            add_label(storage, user_id, transaction_id, label).await?;
        }
    }

    // Step 9: Process each output
    let mut baskets_cache: HashMap<String, i64> = HashMap::new();

    for od in &outputs_data {
        match od.protocol.as_str() {
            WALLET_PAYMENT_PROTOCOL => {
                let payment = od.payment.as_ref().unwrap();

                // Skip if already a change output
                if od.existing_output_id.is_some()
                    && od.existing_basket_id == Some(change_basket_id)
                    && od.existing_is_change
                {
                    continue;
                }

                if let Some(output_id) = od.existing_output_id {
                    // Update existing output
                    let now = Utc::now();
                    sqlx::query(
                        r#"
                        UPDATE outputs
                        SET basket_id = ?, type = 'P2PKH', change = 1, spendable = 1,
                            derivation_prefix = ?, derivation_suffix = ?,
                            sender_identity_key = ?, custom_instructions = NULL, updated_at = ?
                        WHERE output_id = ?
                        "#,
                    )
                    .bind(change_basket_id)
                    .bind(&payment.derivation_prefix)
                    .bind(&payment.derivation_suffix)
                    .bind(&payment.sender_identity_key)
                    .bind(now)
                    .bind(output_id)
                    .execute(storage.pool())
                    .await?;
                } else {
                    // Create new output
                    let now = Utc::now();
                    sqlx::query(
                        r#"
                        INSERT INTO outputs (
                            user_id, transaction_id, basket_id, txid, vout, satoshis,
                            locking_script, script_length, type, spendable, change,
                            derivation_prefix, derivation_suffix, sender_identity_key,
                            provided_by, purpose, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, ?, 'storage', 'receive', ?, ?)
                        "#,
                    )
                    .bind(user_id)
                    .bind(transaction_id)
                    .bind(change_basket_id)
                    .bind(&txid)
                    .bind(od.vout as i32)
                    .bind(od.satoshis as i64)
                    .bind(&od.locking_script)
                    .bind(od.locking_script.len() as i32)
                    .bind(&payment.derivation_prefix)
                    .bind(&payment.derivation_suffix)
                    .bind(&payment.sender_identity_key)
                    .bind(now)
                    .bind(now)
                    .execute(storage.pool())
                    .await?;
                }
            }
            BASKET_INSERTION_PROTOCOL => {
                let insertion = od.insertion.as_ref().unwrap();

                // Get or create basket
                let basket_id = if let Some(id) = baskets_cache.get(&insertion.basket) {
                    *id
                } else {
                    let id = get_or_create_basket_id(storage, user_id, &insertion.basket).await?;
                    baskets_cache.insert(insertion.basket.clone(), id);
                    id
                };

                if let Some(output_id) = od.existing_output_id {
                    // Update existing output
                    let now = Utc::now();
                    sqlx::query(
                        r#"
                        UPDATE outputs
                        SET basket_id = ?, type = 'custom', change = 0,
                            custom_instructions = ?, derivation_prefix = NULL,
                            derivation_suffix = NULL, sender_identity_key = NULL, updated_at = ?
                        WHERE output_id = ?
                        "#,
                    )
                    .bind(basket_id)
                    .bind(&insertion.custom_instructions)
                    .bind(now)
                    .bind(output_id)
                    .execute(storage.pool())
                    .await?;

                    // Add tags
                    if let Some(ref tags) = insertion.tags {
                        for tag in tags {
                            add_tag_to_output(storage, user_id, output_id, tag).await?;
                        }
                    }
                } else {
                    // Create new output
                    let now = Utc::now();
                    let result = sqlx::query(
                        r#"
                        INSERT INTO outputs (
                            user_id, transaction_id, basket_id, txid, vout, satoshis,
                            locking_script, script_length, type, spendable, change,
                            custom_instructions, provided_by, purpose, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'custom', 1, 0, ?, 'storage', 'receive', ?, ?)
                        "#,
                    )
                    .bind(user_id)
                    .bind(transaction_id)
                    .bind(basket_id)
                    .bind(&txid)
                    .bind(od.vout as i32)
                    .bind(od.satoshis as i64)
                    .bind(&od.locking_script)
                    .bind(od.locking_script.len() as i32)
                    .bind(&insertion.custom_instructions)
                    .bind(now)
                    .bind(now)
                    .execute(storage.pool())
                    .await?;

                    let output_id = result.last_insert_rowid();

                    // Add tags
                    if let Some(ref tags) = insertion.tags {
                        for tag in tags {
                            add_tag_to_output(storage, user_id, output_id, tag).await?;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Step 10: Create proven_tx_req if no proof
    // Store the complete BEEF in proven_tx_reqs so it can be used when building
    // output BEEFs for spending. This is especially important for unconfirmed
    // transactions where we need the ancestor chain.
    if !has_proof && !is_merge {
        create_proven_tx_req(storage, &txid, &raw_tx, &args.tx).await?;
    }

    Ok(StorageInternalizeActionResult {
        base: InternalizeActionResult { accepted: true },
        is_merge,
        txid,
        satoshis: net_satoshis,
        send_with_results: None,
        not_delayed_results: None,
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Finds an existing transaction by user and txid.
async fn find_existing_transaction(
    storage: &StorageSqlx,
    user_id: i64,
    txid: &str,
) -> Result<Option<TableTransaction>> {
    let row = sqlx::query(
        r#"
        SELECT transaction_id, user_id, txid, status, reference, description,
               satoshis, version, lock_time, raw_tx, input_beef, is_outgoing,
               proven_tx_id, created_at, updated_at
        FROM transactions
        WHERE user_id = ? AND txid = ?
        "#,
    )
    .bind(user_id)
    .bind(txid)
    .fetch_optional(storage.pool())
    .await?;

    match row {
        Some(row) => {
            let status_str: String = row.get("status");
            let status = parse_transaction_status(&status_str);
            // Note: proven_tx_id is a foreign key ID, but proof_txid in the struct
            // represents a txid string. For this query we only need the transaction,
            // not the proof details, so we set proof_txid to None.
            let _proven_tx_id: Option<i64> = row.get("proven_tx_id");

            Ok(Some(TableTransaction {
                transaction_id: row.get("transaction_id"),
                user_id: row.get("user_id"),
                txid: row.get("txid"),
                status,
                reference: row.get("reference"),
                description: row.get("description"),
                satoshis: row.get("satoshis"),
                version: row.get("version"),
                lock_time: row.get("lock_time"),
                raw_tx: row.get("raw_tx"),
                input_beef: row.get("input_beef"),
                is_outgoing: row.get("is_outgoing"),
                proof_txid: None, // Would need to join with proven_txs table to get actual txid
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            }))
        }
        None => Ok(None),
    }
}

fn parse_transaction_status(status: &str) -> TransactionStatus {
    match status {
        "completed" => TransactionStatus::Completed,
        "unprocessed" => TransactionStatus::Unprocessed,
        "sending" => TransactionStatus::Sending,
        "unproven" => TransactionStatus::Unproven,
        "unsigned" => TransactionStatus::Unsigned,
        "nosend" => TransactionStatus::NoSend,
        "nonfinal" => TransactionStatus::NonFinal,
        "failed" => TransactionStatus::Failed,
        "unfail" => TransactionStatus::Unfail,
        _ => TransactionStatus::Unprocessed,
    }
}

fn validate_merge_status(status: &TransactionStatus) -> Result<()> {
    match status {
        TransactionStatus::Completed
        | TransactionStatus::Unproven
        | TransactionStatus::NoSend => Ok(()),
        _ => Err(Error::ValidationError(format!(
            "Target transaction of internalizeAction has invalid status: {:?}",
            status
        ))),
    }
}

/// Loads existing outputs for a transaction.
async fn load_existing_outputs(
    storage: &StorageSqlx,
    user_id: i64,
    txid: &str,
) -> Result<Vec<TableOutput>> {
    let rows = sqlx::query(
        r#"
        SELECT output_id, user_id, transaction_id, basket_id, txid, vout,
               satoshis, locking_script, script_length, script_offset,
               type, spendable, change, derivation_prefix, derivation_suffix,
               sender_identity_key, custom_instructions, created_at, updated_at
        FROM outputs
        WHERE user_id = ? AND txid = ?
        "#,
    )
    .bind(user_id)
    .bind(txid)
    .fetch_all(storage.pool())
    .await?;

    let mut outputs = Vec::new();
    for row in rows {
        outputs.push(TableOutput {
            output_id: row.get("output_id"),
            user_id: row.get("user_id"),
            transaction_id: row.get("transaction_id"),
            basket_id: row.get("basket_id"),
            txid: row.get("txid"),
            vout: row.get("vout"),
            satoshis: row.get("satoshis"),
            locking_script: row.get("locking_script"),
            script_length: row.get("script_length"),
            script_offset: row.get("script_offset"),
            output_type: row.get("type"),
            provided_by: row.try_get("provided_by").unwrap_or("you".to_string()),
            purpose: row.try_get("purpose").ok(),
            output_description: row.try_get("output_description").ok(),
            spent_by: row.try_get("spent_by").ok().flatten(),
            sequence_number: row.try_get::<Option<i32>, _>("sequence_number").ok().flatten().map(|v| v as u32),
            spending_description: row.try_get("spending_description").ok(),
            spendable: row.get("spendable"),
            change: row.get("change"),
            derivation_prefix: row.get("derivation_prefix"),
            derivation_suffix: row.get("derivation_suffix"),
            sender_identity_key: row.get("sender_identity_key"),
            custom_instructions: row.get("custom_instructions"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    Ok(outputs)
}

/// Gets or creates a basket and returns its ID.
async fn get_or_create_basket_id(
    storage: &StorageSqlx,
    user_id: i64,
    name: &str,
) -> Result<i64> {
    // Try to find existing
    let row = sqlx::query(
        "SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = ? AND is_deleted = 0",
    )
    .bind(user_id)
    .bind(name)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = row {
        return Ok(row.get("basket_id"));
    }

    // Create new basket
    let now = Utc::now();
    let result = sqlx::query(
        r#"
        INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
        VALUES (?, ?, 6, 10000, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(name)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(result.last_insert_rowid())
}

/// Adds a label to a transaction.
async fn add_label(
    storage: &StorageSqlx,
    user_id: i64,
    transaction_id: i64,
    label: &str,
) -> Result<()> {
    let now = Utc::now();

    // Find or create label
    let label_row = sqlx::query(
        "SELECT tx_label_id FROM tx_labels WHERE user_id = ? AND label = ? AND is_deleted = 0",
    )
    .bind(user_id)
    .bind(label)
    .fetch_optional(storage.pool())
    .await?;

    let label_id = match label_row {
        Some(row) => row.get::<i64, _>("tx_label_id"),
        None => {
            let result = sqlx::query(
                "INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(label)
            .bind(now)
            .bind(now)
            .execute(storage.pool())
            .await?;

            result.last_insert_rowid()
        }
    };

    // Check if mapping exists
    let existing = sqlx::query(
        "SELECT tx_label_map_id FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?",
    )
    .bind(transaction_id)
    .bind(label_id)
    .fetch_optional(storage.pool())
    .await?;

    if existing.is_none() {
        sqlx::query(
            "INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(transaction_id)
        .bind(label_id)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await?;
    }

    Ok(())
}

/// Adds a tag to an output.
async fn add_tag_to_output(
    storage: &StorageSqlx,
    user_id: i64,
    output_id: i64,
    tag: &str,
) -> Result<()> {
    let now = Utc::now();

    // Find or create tag
    let tag_row = sqlx::query(
        "SELECT output_tag_id FROM output_tags WHERE user_id = ? AND tag = ? AND is_deleted = 0",
    )
    .bind(user_id)
    .bind(tag)
    .fetch_optional(storage.pool())
    .await?;

    let tag_id = match tag_row {
        Some(row) => row.get::<i64, _>("output_tag_id"),
        None => {
            let result = sqlx::query(
                "INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(user_id)
            .bind(tag)
            .bind(now)
            .bind(now)
            .execute(storage.pool())
            .await?;

            result.last_insert_rowid()
        }
    };

    // Check if mapping exists
    let existing = sqlx::query(
        "SELECT output_tag_map_id FROM output_tags_map WHERE output_id = ? AND output_tag_id = ?",
    )
    .bind(output_id)
    .bind(tag_id)
    .fetch_optional(storage.pool())
    .await?;

    if existing.is_none() {
        sqlx::query(
            "INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(output_id)
        .bind(tag_id)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await?;
    }

    Ok(())
}

/// Gets known txids for a user (for TrustKnown BEEF verification mode).
///
/// Returns all txids from transactions owned by the user that are in
/// "completed" or "unproven" status. These are transactions the wallet
/// already knows about and can trust.
async fn get_known_txids(storage: &StorageSqlx, user_id: i64) -> Result<HashSet<String>> {
    let rows: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT txid FROM transactions
        WHERE user_id = ? AND status IN ('completed', 'unproven')
        "#,
    )
    .bind(user_id)
    .fetch_all(storage.pool())
    .await?;

    Ok(rows.into_iter().collect())
}

/// Creates a proven transaction request for proof lookup.
///
/// Stores both the raw transaction and the complete incoming BEEF.
/// The input_beef is crucial for spending - it contains ancestor transactions
/// that are needed to construct valid BEEFs for outputs of this transaction.
async fn create_proven_tx_req(
    storage: &StorageSqlx,
    txid: &str,
    raw_tx: &[u8],
    input_beef: &[u8],
) -> Result<()> {
    let now = Utc::now();

    // Check if already exists
    let existing = sqlx::query("SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(storage.pool())
        .await?;

    if existing.is_some() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO proven_tx_reqs (
            txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at
        )
        VALUES (?, 'unmined', 0, '{}', '{}', 0, ?, ?, ?, ?)
        "#,
    )
    .bind(txid)
    .bind(raw_tx)
    .bind(input_beef)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlx::StorageSqlx;
    use crate::storage::traits::WalletStorageWriter;
    use bsv_sdk::script::{LockingScript, UnlockingScript};
    use bsv_sdk::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
    use bsv_sdk::wallet::{BasketInsertion, InternalizeOutput, WalletPayment};

    /// Helper to create test storage.
    async fn create_test_storage() -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate(
                "test-wallet",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        storage
    }

    /// Helper to create a test user.
    async fn create_test_user(storage: &StorageSqlx) -> i64 {
        let (user, _) = storage
            .find_or_insert_user(
                "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
            )
            .await
            .unwrap();
        user.user_id
    }

    /// Helper to create a simple test transaction and AtomicBEEF.
    fn create_test_atomic_beef() -> (Vec<u8>, String, u64) {
        // Create a simple transaction
        let mut tx = Transaction::new();
        tx.version = 1;
        tx.lock_time = 0;

        // Add a dummy input
        let mut input = TransactionInput::new(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            0,
        );
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        tx.inputs.push(input);

        // Add a P2PKH output with 10000 satoshis
        let satoshis = 10000u64;
        let output = TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: LockingScript::from_hex(
                "76a914000000000000000000000000000000000000000088ac",
            )
            .unwrap(),
            change: false,
        };
        tx.outputs.push(output);

        let txid = tx.id();

        // Create BEEF with this transaction
        let mut beef = Beef::new();
        beef.merge_transaction(tx);

        // Use to_binary_atomic to create AtomicBEEF format with embedded txid
        let beef_bytes = beef.to_binary_atomic(&txid).unwrap();

        (beef_bytes, txid, satoshis)
    }

    #[test]
    fn test_parse_transaction_status() {
        assert_eq!(
            parse_transaction_status("completed"),
            TransactionStatus::Completed
        );
        assert_eq!(
            parse_transaction_status("unproven"),
            TransactionStatus::Unproven
        );
        assert_eq!(
            parse_transaction_status("nosend"),
            TransactionStatus::NoSend
        );
    }

    #[test]
    fn test_validate_merge_status_allowed() {
        assert!(validate_merge_status(&TransactionStatus::Completed).is_ok());
        assert!(validate_merge_status(&TransactionStatus::Unproven).is_ok());
        assert!(validate_merge_status(&TransactionStatus::NoSend).is_ok());
    }

    #[test]
    fn test_validate_merge_status_not_allowed() {
        assert!(validate_merge_status(&TransactionStatus::Unsigned).is_err());
        assert!(validate_merge_status(&TransactionStatus::Failed).is_err());
    }

    #[tokio::test]
    async fn test_internalize_wallet_payment_happy_path() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, txid, satoshis) = create_test_atomic_beef();

        let args = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "test_prefix".to_string(),
                    derivation_suffix: "test_suffix".to_string(),
                    sender_identity_key: "sender_key".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "Test wallet payment".to_string(),
            labels: Some(vec!["payment".to_string()]),
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert!(!result.is_merge);
        assert_eq!(result.txid, txid);
        assert_eq!(result.satoshis, satoshis as i64);
    }

    #[tokio::test]
    async fn test_internalize_basket_insertion_happy_path() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        let args = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: BASKET_INSERTION_PROTOCOL.to_string(),
                payment_remittance: None,
                insertion_remittance: Some(BasketInsertion {
                    basket: "custom_basket".to_string(),
                    custom_instructions: Some("custom instructions".to_string()),
                    tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
                }),
            }],
            description: "Test basket insertion".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert!(!result.is_merge);
        assert_eq!(result.txid, txid);
        assert_eq!(result.satoshis, 0);
    }

    #[tokio::test]
    async fn test_internalize_invalid_beef() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let args = InternalizeActionArgs {
            tx: vec![0x00, 0x01, 0x02, 0x03],
            outputs: vec![],
            description: "Invalid BEEF test".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_internalize_output_index_out_of_range() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, _txid, _satoshis) = create_test_atomic_beef();

        let args = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 999,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix".to_string(),
                    derivation_suffix: "suffix".to_string(),
                    sender_identity_key: "sender".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "Out of range test".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_internalize_existing_transaction_merge() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, _txid, satoshis) = create_test_atomic_beef();

        // First internalization
        let args1 = InternalizeActionArgs {
            tx: beef_bytes.clone(),
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix1".to_string(),
                    derivation_suffix: "suffix1".to_string(),
                    sender_identity_key: "sender1".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "First internalization".to_string(),
            labels: Some(vec!["label1".to_string()]),
            seek_permission: None,
        };

        let result1 = internalize_action_internal(&storage, user_id, args1)
            .await
            .unwrap();

        assert!(!result1.is_merge);
        assert_eq!(result1.satoshis, satoshis as i64);

        // Second internalization (merge)
        let args2 = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix2".to_string(),
                    derivation_suffix: "suffix2".to_string(),
                    sender_identity_key: "sender2".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "Second internalization".to_string(),
            labels: Some(vec!["label2".to_string()]),
            seek_permission: None,
        };

        let result2 = internalize_action_internal(&storage, user_id, args2)
            .await
            .unwrap();

        assert!(result2.is_merge);
        // Already a change output, so ignored (0 satoshi change)
        assert_eq!(result2.satoshis, 0);
    }

    #[tokio::test]
    async fn test_internalize_switch_from_change_to_custom_basket() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, _txid, satoshis) = create_test_atomic_beef();

        // First as wallet payment
        let args1 = InternalizeActionArgs {
            tx: beef_bytes.clone(),
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix".to_string(),
                    derivation_suffix: "suffix".to_string(),
                    sender_identity_key: "sender".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "First - as change".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result1 = internalize_action_internal(&storage, user_id, args1)
            .await
            .unwrap();

        assert!(!result1.is_merge);
        assert_eq!(result1.satoshis, satoshis as i64);

        // Second - switch to custom basket
        let args2 = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: BASKET_INSERTION_PROTOCOL.to_string(),
                payment_remittance: None,
                insertion_remittance: Some(BasketInsertion {
                    basket: "custom_basket".to_string(),
                    custom_instructions: None,
                    tags: None,
                }),
            }],
            description: "Second - switch to custom".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result2 = internalize_action_internal(&storage, user_id, args2)
            .await
            .unwrap();

        assert!(result2.is_merge);
        // Switching from change to custom removes from balance
        assert_eq!(result2.satoshis, -(satoshis as i64));
    }

    #[tokio::test]
    async fn test_internalize_add_labels_during_merge() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        // First with one label
        let args1 = InternalizeActionArgs {
            tx: beef_bytes.clone(),
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix".to_string(),
                    derivation_suffix: "suffix".to_string(),
                    sender_identity_key: "sender".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "First".to_string(),
            labels: Some(vec!["initial_label".to_string()]),
            seek_permission: None,
        };

        internalize_action_internal(&storage, user_id, args1)
            .await
            .unwrap();

        // Second with another label
        let args2 = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                payment_remittance: Some(WalletPayment {
                    derivation_prefix: "prefix".to_string(),
                    derivation_suffix: "suffix".to_string(),
                    sender_identity_key: "sender".to_string(),
                }),
                insertion_remittance: None,
            }],
            description: "Second".to_string(),
            labels: Some(vec!["added_label".to_string()]),
            seek_permission: None,
        };

        let result2 = internalize_action_internal(&storage, user_id, args2)
            .await
            .unwrap();

        assert!(result2.is_merge);

        // Verify both labels exist
        let labels: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT l.label FROM tx_labels l
            JOIN tx_labels_map m ON l.tx_label_id = m.tx_label_id
            JOIN transactions t ON m.transaction_id = t.transaction_id
            WHERE t.txid = ? AND t.user_id = ?
            ORDER BY l.label
            "#,
        )
        .bind(&txid)
        .bind(user_id)
        .fetch_all(storage.pool())
        .await
        .unwrap();

        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"added_label".to_string()));
        assert!(labels.contains(&"initial_label".to_string()));
    }

    #[tokio::test]
    async fn test_internalize_multiple_outputs() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;

        // Create a transaction with multiple outputs
        let mut tx = Transaction::new();
        tx.version = 1;
        tx.lock_time = 0;

        let mut input = TransactionInput::new(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            0,
        );
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        tx.inputs.push(input);

        tx.outputs.push(TransactionOutput {
            satoshis: Some(10000),
            locking_script: LockingScript::from_hex(
                "76a914000000000000000000000000000000000000000088ac",
            )
            .unwrap(),
            change: false,
        });

        tx.outputs.push(TransactionOutput {
            satoshis: Some(5000),
            locking_script: LockingScript::from_hex(
                "76a914010101010101010101010101010101010101010188ac",
            )
            .unwrap(),
            change: false,
        });

        let txid = tx.id();

        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        // Use to_binary_atomic to create AtomicBEEF format
        let beef_bytes = beef.to_binary_atomic(&txid).unwrap();

        let args = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![
                InternalizeOutput {
                    output_index: 0,
                    protocol: WALLET_PAYMENT_PROTOCOL.to_string(),
                    payment_remittance: Some(WalletPayment {
                        derivation_prefix: "prefix0".to_string(),
                        derivation_suffix: "suffix0".to_string(),
                        sender_identity_key: "sender0".to_string(),
                    }),
                    insertion_remittance: None,
                },
                InternalizeOutput {
                    output_index: 1,
                    protocol: BASKET_INSERTION_PROTOCOL.to_string(),
                    payment_remittance: None,
                    insertion_remittance: Some(BasketInsertion {
                        basket: "custom".to_string(),
                        custom_instructions: None,
                        tags: Some(vec!["multi_tag".to_string()]),
                    }),
                },
            ],
            description: "Multiple outputs test".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert!(!result.is_merge);
        // Only wallet payment adds to satoshis
        assert_eq!(result.satoshis, 10000);

        // Verify both outputs were created
        let output_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outputs WHERE user_id = ? AND txid = ?",
        )
        .bind(user_id)
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();

        assert_eq!(output_count, 2);
    }
}
