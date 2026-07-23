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
use crate::storage::traits::{
    BeefVerificationMode, StorageInternalizeActionResult, WalletStorageReader,
};
use bsv_rs::transaction::{Beef, Transaction};
use bsv_rs::wallet::{
    BasketInsertion, InternalizeActionArgs, InternalizeActionResult, WalletPayment,
};
use chrono::Utc;
use sqlx::{Row, SqliteConnection};
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

/// A recorded spendability transition applied by [`mark_user_inputs_spent`].
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:33-37
/// (`SpentInputTransition`). Each entry captures exactly what was changed on
/// one outputs row so that [`restore_inputs_to_spendable`] can revert ONLY
/// those changes if the internalize is rolled back after a failed broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SpentInputTransition {
    /// The `outputs.output_id` whose spendability was cleared.
    output_id: i64,
    /// `true` when `spent_by` was also set (same-user row); `false` when only
    /// `spendable` was cleared (other-user row — `spent_by` untouched).
    set_spent_by: bool,
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
    let mut beef = Beef::from_binary(&args.tx)
        .map_err(|e| Error::ValidationError(format!("Failed to parse AtomicBEEF: {}", e)))?;

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
        )
        .await?;
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
    let (tx_outputs_count, tx_version, tx_lock_time, raw_tx, extracted_outputs, input_outpoints) = {
        let tx = beef_tx.tx().ok_or_else(|| {
            Error::ValidationError(format!("Transaction {} is txid-only in BEEF", txid))
        })?;

        // Extract all output data from the transaction
        let outputs: Vec<(u64, Vec<u8>)> = tx
            .outputs
            .iter()
            .map(|o| (o.satoshis.unwrap_or(0), o.locking_script.to_binary()))
            .collect();

        // Extract the input outpoints — the coins this transaction CONSUMES.
        // Needed to mark any wallet-tracked UTXOs among them as spent
        // (TS parity: internalizeAction.ts:73-97 markUserInputsSpent).
        let input_outpoints = extract_input_outpoints(tx);

        (
            tx.outputs.len(),
            tx.version,
            tx.lock_time,
            tx.to_binary(),
            outputs,
            input_outpoints,
        )
    };
    // tx is now dropped, safe to await

    // Step 2: Get the user's default (change) basket
    // NOTE: This uses its own connection (known limitation).
    let change_basket = storage.find_or_create_default_basket(user_id).await?;

    // Begin a SQL transaction to ensure atomicity of all subsequent DB operations.
    // If any step fails or the function returns early, sqlx will auto-rollback on drop.
    let mut tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Step 3: Check for existing transaction
    let existing_tx = find_existing_transaction(&mut tx, user_id, &txid).await?;
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
        let existing_outputs = load_existing_outputs(&mut tx, user_id, &txid).await?;
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
            BASKET_INSERTION_PROTOCOL
                if od.existing_basket_id == Some(change_basket_id) && od.existing_is_change =>
            {
                // Converting change to custom basket - reduces balance
                net_satoshis -= od.satoshis as i64;
            }
            _ => {}
        }
    }

    // Step 7: Process the internalization
    let has_proof = beef.find_bump(&txid).is_some();
    let status = if has_proof { "completed" } else { "unproven" };

    let transaction_id = if is_merge {
        let etx = existing_tx
            .as_ref()
            .expect("is_merge guarantees existing_tx is Some");
        let tx_id = etx.transaction_id;

        // Update description
        let now = Utc::now();
        sqlx::query(
            "UPDATE transactions SET description = ?, updated_at = ? WHERE transaction_id = ?",
        )
        .bind(&args.description)
        .bind(now)
        .bind(tx_id)
        .execute(&mut *tx)
        .await?;

        // Lifecycle advance when merging into a 'nosend' transaction.
        // TS parity: wallet-toolbox storage/methods/internalizeAction.ts:526-554
        // (mergedInternalize, the `wasNoSend` block).
        //
        // An internalizeAction call against an existing tx in 'nosend' status
        // is the caller asserting the tx has now been externally broadcast
        // (and mined, if the BEEF carries a BUMP). Without this advance the
        // tx stays 'nosend' forever: coin selection requires
        // t.status IN ('completed','unproven') (create_action.rs
        // allocate_change_input), so the merged outputs would be permanently
        // unselectable — 'nosend' has no other retirement path.
        //
        // With BUMP: promote tx -> 'completed' and retire req 'nosend' ->
        // 'completed' (TS also records a proven_txs row here; this crate's
        // internalize path leaves proof ingestion to CheckForProofs, matching
        // its non-merge path which also sets 'completed' without a proven_txs
        // row).
        // Without BUMP: promote tx -> 'unproven' and advance the req to
        // 'unmined' (creating it if absent) so CheckForProofs owns it.
        //
        // Other statuses are intentionally left alone ('sending' is owned by
        // SendWaiting; 'unproven'/'completed' by CheckForProofs) — this
        // mirrors the TS `wasNoSend` gate exactly.
        if etx.status == TransactionStatus::NoSend {
            if has_proof {
                sqlx::query(
                    "UPDATE transactions SET status = 'completed', updated_at = ? WHERE transaction_id = ?",
                )
                .bind(now)
                .bind(tx_id)
                .execute(&mut *tx)
                .await?;

                sqlx::query(
                    "UPDATE proven_tx_reqs SET status = 'completed', updated_at = ? WHERE txid = ? AND status = 'nosend'",
                )
                .bind(now)
                .bind(&txid)
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(
                    "UPDATE transactions SET status = 'unproven', updated_at = ? WHERE transaction_id = ?",
                )
                .bind(now)
                .bind(tx_id)
                .execute(&mut *tx)
                .await?;

                let advanced = sqlx::query(
                    "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ? AND status = 'nosend'",
                )
                .bind(now)
                .bind(&txid)
                .execute(&mut *tx)
                .await?
                .rows_affected();

                if advanced == 0 {
                    // No 'nosend' req to advance. If a req exists in another
                    // status its current owner keeps it (create is a no-op);
                    // if none exists, create one as 'unmined' so
                    // CheckForProofs tracks this now-broadcast txid.
                    create_proven_tx_req(&mut tx, &txid, &raw_tx, &args.tx, "unmined").await?;
                }
            }
        }

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
        .execute(&mut *tx)
        .await?;

        result.last_insert_rowid()
    };

    // Step 8: Add labels
    if let Some(ref labels) = args.labels {
        for label in labels {
            add_label(&mut tx, user_id, transaction_id, label).await?;
        }
    }

    // Step 9: Process each output
    let mut baskets_cache: HashMap<String, i64> = HashMap::new();

    for od in &outputs_data {
        match od.protocol.as_str() {
            WALLET_PAYMENT_PROTOCOL => {
                let payment = od.payment.as_ref().ok_or(Error::ValidationError(
                    "wallet payment missing paymentRemittance".into(),
                ))?;

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
                    .execute(&mut *tx)
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
                    .execute(&mut *tx)
                    .await?;
                }
            }
            BASKET_INSERTION_PROTOCOL => {
                let insertion = od.insertion.as_ref().ok_or(Error::ValidationError(
                    "basket insertion missing insertionRemittance".into(),
                ))?;

                // Get or create basket
                let basket_id = if let Some(id) = baskets_cache.get(&insertion.basket) {
                    *id
                } else {
                    let id = get_or_create_basket_id(&mut tx, user_id, &insertion.basket).await?;
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
                    .execute(&mut *tx)
                    .await?;

                    // Add tags
                    if let Some(ref tags) = insertion.tags {
                        for tag in tags {
                            add_tag_to_output(&mut tx, user_id, output_id, tag).await?;
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
                    .execute(&mut *tx)
                    .await?;

                    let output_id = result.last_insert_rowid();

                    // Add tags
                    if let Some(ref tags) = insertion.tags {
                        for tag in tags {
                            add_tag_to_output(&mut tx, user_id, output_id, tag).await?;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Step 9b: Mark the wallet coins consumed by this transaction's INPUTS as
    // spent, inside the ambient SQL transaction.
    // TS parity: wallet-toolbox storage/methods/internalizeAction.ts:589-595
    // (newInternalize — after the transactions insert so transaction_id is
    // known, before the broadcast attempt) and :496-504 (mergedInternalize —
    // idempotent re-mark after labels; the merge path never restores).
    // Without this, an internalized tx that spends the wallet's own UTXO
    // (e.g. a payment built externally against wallet change) left the
    // consumed coin spendable=1 / spent_by NULL, so coin selection
    // (create_action.rs allocate_change_input) kept re-picking a
    // provably-spent coin — the live incident Calgooon/btc-relay-rs#16.
    let spent_input_transitions =
        mark_user_inputs_spent(&mut tx, user_id, transaction_id, &input_outpoints).await?;

    // Step 10: Create proven_tx_req if no proof
    // Store the complete BEEF in proven_tx_reqs so it can be used when building
    // output BEEFs for spending. This is especially important for unconfirmed
    // transactions where we need the ancestor chain.
    let mut new_req_created = false;
    if !has_proof && !is_merge {
        new_req_created = create_proven_tx_req(&mut tx, &txid, &raw_tx, &args.tx, "unsent").await?;
    }

    // Commit the SQL transaction. All DB operations above are now atomically persisted.
    tx.commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Step 11: Synchronous broadcast for a brand-new req.
    // TS parity: wallet-toolbox storage/methods/internalizeAction.ts:598-623
    // (newInternalize), which calls shareReqsWithWorld inline when `pr.isNew`
    // and aborts the internalize on hard rejection. Deferring the broadcast
    // to the SendWaiting task (previous behavior) meant a minutes-later
    // RE-broadcast of a tx the payer already put on the network, which could
    // be scored as orphan/double-spend and zero the outputs' spendability.
    // Committing the state import first is crash-safe: if we die before the
    // broadcast, the req is 'unsent' and SendWaiting remains the backstop.
    if new_req_created {
        broadcast_new_internalized_req(storage, &txid, &args.tx, &spent_input_transitions).await?;
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
    conn: &mut SqliteConnection,
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
    .fetch_optional(&mut *conn)
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

/// Validates that a pre-existing transaction may be merged into.
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:327-332 —
/// statuses outside completed/unproven/sending/nosend are rejected.
fn validate_merge_status(status: &TransactionStatus) -> Result<()> {
    match status {
        TransactionStatus::Completed
        | TransactionStatus::Unproven
        | TransactionStatus::Sending
        | TransactionStatus::NoSend => Ok(()),
        _ => Err(Error::ValidationError(format!(
            "Target transaction of internalizeAction has invalid status: {:?}",
            status
        ))),
    }
}

/// Loads existing outputs for a transaction.
async fn load_existing_outputs(
    conn: &mut SqliteConnection,
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
    .fetch_all(&mut *conn)
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
            sequence_number: row
                .try_get::<Option<i32>, _>("sequence_number")
                .ok()
                .flatten()
                .map(|v| v as u32),
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
    conn: &mut SqliteConnection,
    user_id: i64,
    name: &str,
) -> Result<i64> {
    // Try to find existing
    let row = sqlx::query(
        "SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = ? AND is_deleted = 0",
    )
    .bind(user_id)
    .bind(name)
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Adds a label to a transaction.
async fn add_label(
    conn: &mut SqliteConnection,
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
    .fetch_optional(&mut *conn)
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
            .execute(&mut *conn)
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
    .fetch_optional(&mut *conn)
    .await?;

    if existing.is_none() {
        sqlx::query(
            "INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(transaction_id)
        .bind(label_id)
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }

    Ok(())
}

/// Adds a tag to an output.
async fn add_tag_to_output(
    conn: &mut SqliteConnection,
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
    .fetch_optional(&mut *conn)
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
            .execute(&mut *conn)
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
    .fetch_optional(&mut *conn)
    .await?;

    if existing.is_none() {
        sqlx::query(
            "INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(output_id)
        .bind(tag_id)
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await?;
    }

    Ok(())
}

/// Gets known txids for a user (for TrustKnown BEEF verification mode).
///
/// Returns all txids from transactions owned by the user that are in
/// "completed" or "unproven" status. These are transactions the wallet
/// already knows about and can trust.
///
/// NOTE: This function runs outside the SQL transaction (before `begin()`),
/// using the pool directly. It is only called during BEEF verification.
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

/// Creates a proven transaction request for an internalized transaction.
///
/// On the non-merge path the req is created `'unsent'` and then immediately
/// broadcast by `broadcast_new_internalized_req` (TS parity: newInternalize's
/// shareReqsWithWorld). If the broadcast is transiently unavailable, the
/// `'unsent'` status keeps `send_waiting_transactions` as the retry backstop.
/// The nosend merge path creates it `'unmined'` (already broadcast externally)
/// so CheckForProofs owns it.
///
/// Stores both the raw transaction and the complete incoming BEEF.
/// The input_beef is crucial for spending — it contains ancestor transactions
/// that are needed to construct valid BEEFs for outputs of this transaction.
///
/// Returns `true` if a new req was created, `false` if one already existed
/// (in which case its current status/owner is left untouched — TS parity
/// with `pr.isNew`).
async fn create_proven_tx_req(
    conn: &mut SqliteConnection,
    txid: &str,
    raw_tx: &[u8],
    input_beef: &[u8],
    status: &str,
) -> Result<bool> {
    let now = Utc::now();

    // Check if already exists
    let existing = sqlx::query("SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(&mut *conn)
        .await?;

    if existing.is_some() {
        return Ok(false);
    }

    sqlx::query(
        r#"
        INSERT INTO proven_tx_reqs (
            txid, status, attempts, history, notify, notified, raw_tx, input_beef, created_at, updated_at
        )
        VALUES (?, ?, 0, '{}', '{}', 0, ?, ?, ?, ?)
        "#,
    )
    .bind(txid)
    .bind(status)
    .bind(raw_tx)
    .bind(input_beef)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(true)
}

// =============================================================================
// Spent-Input Parity (mark inputs spent / restore on failed broadcast)
// =============================================================================

/// Extracts the `(source_txid, vout)` outpoints consumed by a transaction's
/// inputs.
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:73-97 —
/// only inputs that carry a source txid participate; inputs without one are
/// skipped (they cannot name an outpoint to look up).
fn extract_input_outpoints(tx: &Transaction) -> Vec<(String, u32)> {
    tx.inputs
        .iter()
        .filter_map(|i| {
            i.source_txid
                .clone()
                .map(|txid| (txid, i.source_output_index))
        })
        .collect()
}

/// Marks the wallet-tracked coins consumed by an internalized transaction's
/// inputs as spent. Runs inside the ambient SQL transaction.
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:73-97
/// (`markUserInputsSpent`); the outpoint lookup at :39-56 is deliberately NOT
/// filtered by user — an externally-built payment can consume coins tracked by
/// any user of this storage, and every stale row at the outpoint must stop
/// matching coin selection (create_action.rs `allocate_change_input`).
///
/// Per matching row the mark applies ONLY IF the row is currently `spendable`
/// AND its `spent_by` is NULL or already this transaction (merge-path
/// idempotency). A row claimed by a competing transaction is left untouched
/// and is NOT recorded as a transition.
///
/// - Same-user row: `spendable = 0` AND `spent_by = transaction_id`.
/// - Other-user row: `spendable = 0` only — `spent_by` FK-references the
///   OWNING user's transactions table rows, so it stays untouched.
///
/// `spending_description` is never written (TS parity: `markUserInputsSpent`
/// writes none — unlike createAction's spend path, create_action.rs
/// `update_output_spent`).
///
/// An empty `input_outpoints` returns an empty transition list with no DB
/// work. Returns the applied transitions for [`restore_inputs_to_spendable`].
async fn mark_user_inputs_spent(
    conn: &mut SqliteConnection,
    user_id: i64,
    transaction_id: i64,
    input_outpoints: &[(String, u32)],
) -> Result<Vec<SpentInputTransition>> {
    let mut transitions: Vec<SpentInputTransition> = Vec::new();
    if input_outpoints.is_empty() {
        return Ok(transitions);
    }

    let now = Utc::now();
    for (source_txid, vout) in input_outpoints {
        let rows = sqlx::query(
            "SELECT output_id, user_id, spendable, spent_by FROM outputs WHERE txid = ? AND vout = ?",
        )
        .bind(source_txid)
        .bind(*vout as i32)
        .fetch_all(&mut *conn)
        .await?;

        for row in rows {
            let output_id: i64 = row.get("output_id");
            let row_user_id: i64 = row.get("user_id");
            let spendable: bool = row.get("spendable");
            let spent_by: Option<i64> = row.get("spent_by");

            // Only currently-spendable rows not claimed by a COMPETING
            // transaction transition (spent_by == this transaction_id is the
            // idempotent merge re-mark).
            if !spendable || !(spent_by.is_none() || spent_by == Some(transaction_id)) {
                continue;
            }

            if row_user_id == user_id {
                sqlx::query(
                    "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
                )
                .bind(transaction_id)
                .bind(now)
                .bind(output_id)
                .execute(&mut *conn)
                .await?;
                transitions.push(SpentInputTransition {
                    output_id,
                    set_spent_by: true,
                });
            } else {
                sqlx::query("UPDATE outputs SET spendable = 0, updated_at = ? WHERE output_id = ?")
                    .bind(now)
                    .bind(output_id)
                    .execute(&mut *conn)
                    .await?;
                transitions.push(SpentInputTransition {
                    output_id,
                    set_spent_by: false,
                });
            }
        }
    }

    Ok(transitions)
}

/// Reverts the spendability transitions recorded by [`mark_user_inputs_spent`].
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:120-128
/// (`restoreInputsToSpendable`), invoked ONLY on broadcast non-success
/// (:626-636). Reverts ONLY the recorded transitions — rows skipped at mark
/// time (e.g. already spent by a competing transaction) are not in the list,
/// so a competing `spent_by` is never clobbered. An empty list is a no-op;
/// applying the same list twice is harmless (the restore is idempotent).
async fn restore_inputs_to_spendable(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    transitions: &[SpentInputTransition],
) -> Result<()> {
    if transitions.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    for t in transitions {
        if t.set_spent_by {
            sqlx::query(
                "UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = ? WHERE output_id = ?",
            )
            .bind(now)
            .bind(t.output_id)
            .execute(pool)
            .await?;
        } else {
            sqlx::query("UPDATE outputs SET spendable = 1, updated_at = ? WHERE output_id = ?")
                .bind(now)
                .bind(t.output_id)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

// =============================================================================
// Synchronous Broadcast (new-path internalize)
// =============================================================================

/// Synchronously broadcast a newly internalized (proof-less, non-merge)
/// transaction.
///
/// TS parity: wallet-toolbox storage/methods/internalizeAction.ts:598-623
/// (newInternalize → shareReqsWithWorld, rollback on failure at :612-622).
/// Posts the already-validated AtomicBEEF as-is — TS: "Skip looking up txids
/// and building an aggregate beef, just this one txid and the already
/// validated atomic beef."
///
/// Outcome classes (classification mirrors `send_waiting_transactions` in
/// storage_sqlx.rs):
///
/// - SUCCESS — accepted OR "already known / seen on network". The payer
///   usually broadcast this tx already, so a duplicate-submission ack is the
///   normal case (providers report it with status "success"). The req is
///   advanced 'unsent' → 'unmined'; the transaction stays 'unproven' and
///   CheckForProofs owns it from here.
/// - HARD REJECT — double-spend or definitively invalid (46x), confirmed
///   dead by chain reconciliation: `mark_internalized_tx_failed` (tx →
///   'failed', outputs unspendable, req → 'invalid') and the internalize
///   returns an error, so no phantom spendable outputs are left behind.
/// - TRANSIENT — network/service error, orphan-mempool, or no services
///   configured: the internalize still succeeds and the req stays 'unsent';
///   the SendWaiting monitor task remains the retry backstop (degrades to
///   the previous deferred behavior instead of breaking offline
///   internalize).
async fn broadcast_new_internalized_req(
    storage: &StorageSqlx,
    txid: &str,
    atomic_beef: &[u8],
    spent_input_transitions: &[SpentInputTransition],
) -> Result<()> {
    let services = match storage.get_services() {
        Ok(s) => s,
        Err(_) => {
            tracing::debug!(
                txid = %txid,
                "internalize: no services configured — send_waiting will broadcast"
            );
            return Ok(());
        }
    };

    let txids = [txid.to_string()];
    let results_vec = match services.post_beef(atomic_beef, &txids).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                txid = %txid,
                error = %e,
                "internalize: transient broadcast error — req stays 'unsent', send_waiting will retry"
            );
            return Ok(());
        }
    };

    let success = results_vec.iter().any(|r| r.is_success());

    if !success {
        // Same classification as send_waiting_transactions.
        let is_orphan_mempool = results_vec
            .iter()
            .any(|r| r.txid_results.iter().any(|tr| tr.orphan_mempool));

        let is_double_spend = results_vec.iter().any(|r| {
            r.txid_results
                .iter()
                .any(|tr| tr.double_spend && !tr.orphan_mempool)
        });

        let is_invalid = results_vec.iter().any(|r| {
            r.txid_results.iter().any(|tr| {
                !tr.orphan_mempool && (tr.status.contains("46") || tr.status.contains("invalid"))
            })
        });

        if is_orphan_mempool && !is_double_spend {
            // Parent not yet propagated — not a rejection of this tx.
            tracing::warn!(
                txid = %txid,
                "internalize: orphan mempool — req stays 'unsent', send_waiting will retry"
            );
            return Ok(());
        }

        if is_double_spend || is_invalid {
            // Reconcile against the chain before condemning (same as the
            // send_waiting path): the payer may have already broadcast this
            // tx, making the rejection stale.
            let reconciled =
                super::process_action::reconcile_tx_status_via_services(&*services, txid).await;

            if !reconciled {
                // Hard reject confirmed — roll back the internalize so no
                // spendable outputs are left behind (TS :612-622 semantics).
                mark_internalized_tx_failed(storage, txid).await?;
                // The tx never made it to the network, so the coins its
                // inputs consumed are still live — restore exactly the
                // transitions Step 9b applied (TS parity:
                // internalizeAction.ts:626-636 → restoreInputsToSpendable
                // :120-128). Transition-scoped: competing spent_by claims
                // were skipped at mark time and are never touched here.
                restore_inputs_to_spendable(storage.pool(), spent_input_transitions).await?;
                return Err(Error::BroadcastFailed(format!(
                    "internalized tx {} rejected by network (double_spend={}, invalid={})",
                    txid, is_double_spend, is_invalid
                )));
            }
            // Reported dead but alive on chain — fall through to success.
        } else {
            // Service-level error with no definitive verdict — transient.
            tracing::warn!(
                txid = %txid,
                "internalize: broadcast not accepted (service error) — req stays 'unsent', send_waiting will retry"
            );
            return Ok(());
        }
    }

    // Success (accepted, already known, or reconciled-alive): advance the
    // req to 'unmined' so send_waiting never re-broadcasts it and
    // CheckForProofs takes ownership. Matches the send_waiting success
    // transition (req -> 'unmined', tx stays 'unproven').
    let now = Utc::now();
    sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'unmined', updated_at = ? WHERE txid = ? AND status = 'unsent'",
    )
    .bind(now)
    .bind(txid)
    .execute(storage.pool())
    .await?;

    tracing::info!(
        txid = %txid,
        "internalize: tx broadcast/acknowledged — req advanced to 'unmined'"
    );

    Ok(())
}

// =============================================================================
// Failed Broadcast Cleanup
// =============================================================================

/// Mark an internalized transaction as failed after broadcast failure.
///
/// Unlike outgoing transactions (which restore spent inputs on failure),
/// internalized transactions are *incoming* — their created outputs need to be
/// marked unspendable so they don't poison future transactions.
///
/// This function:
/// 1. Sets the transaction status to 'failed'
/// 2. Marks all outputs created by this transaction as unspendable (spendable = 0)
/// 3. Sets the proven_tx_req status to 'invalid'
///
/// NOTE: restoring the INPUT coins this internalize marked spent (Step 9b,
/// `mark_user_inputs_spent`) is NOT done here — it is transition-scoped and
/// the transitions are only known to the internalize call itself, so the
/// broadcast failure path (`broadcast_new_internalized_req`) calls
/// `restore_inputs_to_spendable` alongside this function. Callers reaching
/// this via the storage trait (txid only) cannot restore inputs.
pub async fn mark_internalized_tx_failed(storage: &StorageSqlx, txid: &str) -> Result<()> {
    let now = Utc::now();

    // 1. Mark transaction as failed
    sqlx::query("UPDATE transactions SET status = 'failed', updated_at = ? WHERE txid = ?")
        .bind(now)
        .bind(txid)
        .execute(storage.pool())
        .await?;

    // 2. Mark created outputs as unspendable
    // Use a subquery to find the transaction_id, then update outputs for that tx.
    sqlx::query(
        r#"
        UPDATE outputs SET spendable = 0, updated_at = ?
        WHERE txid = ?
        "#,
    )
    .bind(now)
    .bind(txid)
    .execute(storage.pool())
    .await?;

    // 3. Mark proven_tx_req as invalid
    sqlx::query("UPDATE proven_tx_reqs SET status = 'invalid', updated_at = ? WHERE txid = ?")
        .bind(now)
        .bind(txid)
        .execute(storage.pool())
        .await?;

    tracing::info!(
        txid = %txid,
        "Marked internalized transaction as failed: tx=failed, outputs=unspendable, proven_tx_req=invalid"
    );

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
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
    use bsv_rs::wallet::{BasketInsertion, InternalizeOutput, WalletPayment};

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
        create_test_atomic_beef_spending(
            "0000000000000000000000000000000000000000000000000000000000000000",
            0,
        )
    }

    /// Variant of `create_test_atomic_beef` whose single input spends the
    /// given outpoint — used to build an externally-authored tx that consumes
    /// a SEEDED wallet UTXO (the Calgooon/btc-relay-rs#16 incident shape).
    fn create_test_atomic_beef_spending(
        source_txid: &str,
        source_vout: u32,
    ) -> (Vec<u8>, String, u64) {
        // Create a simple transaction
        let mut tx = Transaction::new();
        tx.version = 1;
        tx.lock_time = 0;

        // Add the input spending the requested outpoint
        let mut input = TransactionInput::new(source_txid.to_string(), source_vout);
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
        // TS parity (internalizeAction.ts:327-332): completed / unproven /
        // sending / nosend are the valid merge targets.
        assert!(validate_merge_status(&TransactionStatus::Completed).is_ok());
        assert!(validate_merge_status(&TransactionStatus::Unproven).is_ok());
        assert!(validate_merge_status(&TransactionStatus::Sending).is_ok());
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
        let output_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outputs WHERE user_id = ? AND txid = ?")
                .bind(user_id)
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();

        assert_eq!(output_count, 2);
    }

    // =========================================================================
    // Fix 1/Fix 2 parity tests: synchronous broadcast + merge lifecycle
    // =========================================================================

    use crate::services::mock::MockWalletServicesBuilder;
    use crate::storage::traits::WalletStorageProvider;
    use std::sync::Arc;

    fn wallet_payment_args(beef_bytes: Vec<u8>, description: &str) -> InternalizeActionArgs {
        InternalizeActionArgs {
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
            description: description.to_string(),
            labels: None,
            seek_permission: None,
        }
    }

    async fn req_status(storage: &StorageSqlx, txid: &str) -> Option<String> {
        sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(storage.pool())
            .await
            .unwrap()
    }

    async fn tx_status(storage: &StorageSqlx, user_id: i64, txid: &str) -> Option<String> {
        sqlx::query_scalar("SELECT status FROM transactions WHERE user_id = ? AND txid = ?")
            .bind(user_id)
            .bind(txid)
            .fetch_optional(storage.pool())
            .await
            .unwrap()
    }

    /// Replicates the coin-selection predicate from create_action.rs
    /// `allocate_change_input`: default basket, change=1, spendable=1,
    /// spent_by IS NULL, t.status IN ('completed','unproven').
    async fn count_selectable_coins(storage: &StorageSqlx, user_id: i64) -> i64 {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM outputs o
            JOIN transactions t ON o.transaction_id = t.transaction_id
            JOIN output_baskets b ON o.basket_id = b.basket_id
            WHERE o.user_id = ?
              AND b.name = 'default'
              AND o.change = 1
              AND o.spendable = 1
              AND o.spent_by IS NULL
              AND t.status IN ('completed', 'unproven')
            "#,
        )
        .bind(user_id)
        .fetch_one(storage.pool())
        .await
        .unwrap()
    }

    /// Inserts a pre-existing transaction row (fixture for merge tests).
    async fn insert_existing_tx(
        storage: &StorageSqlx,
        user_id: i64,
        txid: &str,
        status: &str,
    ) -> i64 {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO transactions (
                user_id, txid, status, reference, description, satoshis,
                version, lock_time, is_outgoing, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 'pre-existing tx', 0, 1, 0, 1, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(txid)
        .bind(status)
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    /// Inserts a pre-existing proven_tx_req row (fixture for merge tests).
    async fn insert_req(storage: &StorageSqlx, txid: &str, status: &str) {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO proven_tx_reqs (
                txid, status, attempts, history, notify, notified, raw_tx, created_at, updated_at
            )
            VALUES (?, ?, 0, '{}', '{}', 0, X'00', ?, ?)
            "#,
        )
        .bind(txid)
        .bind(status)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    /// New-path internalize + broadcast success: req advances 'unsent' ->
    /// 'unmined' (send_waiting will never re-broadcast), tx stays 'unproven',
    /// and the coin is immediately selectable.
    #[tokio::test]
    async fn test_new_internalize_broadcast_success_advances_req() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        let mock = MockWalletServicesBuilder::default()
            .post_beef_success()
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = wallet_payment_args(beef_bytes, "broadcast success");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("unmined")
        );
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("unproven")
        );
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);
    }

    /// New-path internalize + "already known" broadcast response: the payer
    /// usually broadcast this tx already — a duplicate-submission ack counts
    /// as success exactly like an acceptance.
    #[tokio::test]
    async fn test_new_internalize_already_known_counts_as_success() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        let mock = MockWalletServicesBuilder::default()
            .post_beef_already_known(&txid)
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = wallet_payment_args(beef_bytes, "already known");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("unmined")
        );
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("unproven")
        );
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);
    }

    /// New-path internalize + transient broadcast error: the internalize
    /// still succeeds, the req stays 'unsent' (SendWaiting is the documented
    /// retry backstop), and the coin is selectable.
    #[tokio::test]
    async fn test_new_internalize_transient_error_defers_to_send_waiting() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        let mock = MockWalletServicesBuilder::default()
            .post_beef_network_error("connection refused")
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = wallet_payment_args(beef_bytes, "transient error");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.base.accepted);
        assert_eq!(req_status(&storage, &txid).await.as_deref(), Some("unsent"));
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("unproven")
        );
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);
    }

    /// New-path internalize + confirmed double-spend rejection (chain
    /// reconciliation also fails to find the tx alive): the internalize
    /// errors and leaves no spendable outputs behind (TS rollback parity,
    /// internalizeAction.ts:612-622).
    #[tokio::test]
    async fn test_new_internalize_double_spend_hard_reject_rolls_back() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        // Default get_status_for_txids mock returns no results, so the
        // reconcile step finds the tx dead and the rejection stands.
        let mock = MockWalletServicesBuilder::default()
            .post_beef_double_spend(&txid, "competing_txid")
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = wallet_payment_args(beef_bytes, "double spend");
        let result = internalize_action_internal(&storage, user_id, args).await;

        assert!(result.is_err(), "hard reject must fail the internalize");
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("invalid")
        );
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("failed")
        );
        assert_eq!(count_selectable_coins(&storage, user_id).await, 0);

        let spendable_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outputs WHERE user_id = ? AND txid = ? AND spendable = 1",
        )
        .bind(user_id)
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(spendable_count, 0, "no spendable outputs may remain");
    }

    /// Merge into a pre-existing 'nosend' tx without proof: tx is promoted
    /// to 'unproven' and the 'nosend' req is advanced to 'unmined'
    /// (TS mergedInternalize wasNoSend parity, internalizeAction.ts:526-554).
    /// The merged coin becomes selectable.
    #[tokio::test]
    async fn test_merge_nosend_promotes_tx_and_advances_req() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        insert_existing_tx(&storage, user_id, &txid, "nosend").await;
        insert_req(&storage, &txid, "nosend").await;

        let args = wallet_payment_args(beef_bytes, "merge into nosend");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.is_merge);
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("unproven")
        );
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("unmined")
        );
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);
    }

    /// Merge into a 'nosend' tx that has no proven_tx_req at all: a req is
    /// created directly as 'unmined' so CheckForProofs tracks the
    /// externally-broadcast txid.
    #[tokio::test]
    async fn test_merge_nosend_without_req_creates_unmined_req() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        insert_existing_tx(&storage, user_id, &txid, "nosend").await;

        let args = wallet_payment_args(beef_bytes, "merge into nosend, no req");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.is_merge);
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("unproven")
        );
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("unmined")
        );

        // The created req must carry the raw tx for later BEEF building.
        let raw_tx: Vec<u8> =
            sqlx::query_scalar("SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?")
                .bind(&txid)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert!(!raw_tx.is_empty());
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);
    }

    /// Merge into a 'sending' tx is accepted (TS status validation allows
    /// it) but NOT advanced — SendWaiting owns the in-flight broadcast
    /// (TS parity: the advance is gated on `wasNoSend`).
    #[tokio::test]
    async fn test_merge_sending_accepted_but_not_advanced() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        insert_existing_tx(&storage, user_id, &txid, "sending").await;
        insert_req(&storage, &txid, "sending").await;

        let args = wallet_payment_args(beef_bytes, "merge into sending");
        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();

        assert!(result.is_merge);
        assert_eq!(
            tx_status(&storage, user_id, &txid).await.as_deref(),
            Some("sending")
        );
        assert_eq!(
            req_status(&storage, &txid).await.as_deref(),
            Some("sending")
        );
    }

    /// Merge into a tx with a status outside completed/unproven/sending/
    /// nosend is an error (TS parity, internalizeAction.ts:327-332).
    #[tokio::test]
    async fn test_merge_invalid_preexisting_status_errors() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef();

        insert_existing_tx(&storage, user_id, &txid, "failed").await;

        let args = wallet_payment_args(beef_bytes, "merge into failed");
        let result = internalize_action_internal(&storage, user_id, args).await;

        assert!(result.is_err(), "merging into a 'failed' tx must error");
    }

    // =========================================================================
    // Spent-input parity tests (TS: internalizeAction.ts markUserInputsSpent /
    // restoreInputsToSpendable; ts test suite
    // test/storage/internalizeActionMarkInputsSpent.test.ts)
    // =========================================================================

    /// Creates a second test user (cross-user outpoint tests).
    async fn create_second_user(storage: &StorageSqlx) -> i64 {
        let (user, _) = storage
            .find_or_insert_user(
                "03b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2",
            )
            .await
            .unwrap();
        user.user_id
    }

    /// Seeds a confirmed wallet coin: a 'completed' transactions row plus a
    /// default-basket change output (spendable = 1, spent_by NULL) at
    /// (txid, vout). Returns (transaction_id, output_id).
    async fn seed_change_coin(
        storage: &StorageSqlx,
        user_id: i64,
        txid: &str,
        vout: u32,
        satoshis: i64,
    ) -> (i64, i64) {
        let tx_id = insert_existing_tx(storage, user_id, txid, "completed").await;
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, txid, vout, satoshis,
                locking_script, script_length, type, spendable, change,
                provided_by, purpose, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, X'00', 1, 'P2PKH', 1, 1, 'storage', 'change', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(tx_id)
        .bind(basket.basket_id)
        .bind(txid)
        .bind(vout as i32)
        .bind(satoshis)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        (tx_id, result.last_insert_rowid())
    }

    /// Reads (spendable, spent_by, spending_description) for an output row.
    async fn output_state(
        storage: &StorageSqlx,
        output_id: i64,
    ) -> (bool, Option<i64>, Option<String>) {
        let row = sqlx::query(
            "SELECT spendable, spent_by, spending_description FROM outputs WHERE output_id = ?",
        )
        .bind(output_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        (
            row.get("spendable"),
            row.get("spent_by"),
            row.get("spending_description"),
        )
    }

    /// ts case 1: an owned spendable coin at a consumed outpoint is marked
    /// spendable = 0 + spent_by = the internalizing transaction, and the
    /// transition records set_spent_by = true. spending_description is never
    /// written (TS parity: markUserInputsSpent writes none).
    #[tokio::test]
    async fn test_mark_inputs_spent_owned_coin() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let coin_txid = "11".repeat(32);
        let (_coin_tx_id, output_id) =
            seed_change_coin(&storage, user_id, &coin_txid, 0, 5000).await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_id, &"22".repeat(32), "unproven").await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions =
            mark_user_inputs_spent(&mut conn, user_id, spender_tx_id, &[(coin_txid.clone(), 0)])
                .await
                .unwrap();
        drop(conn);

        assert_eq!(
            transitions,
            vec![SpentInputTransition {
                output_id,
                set_spent_by: true
            }]
        );
        let (spendable, spent_by, desc) = output_state(&storage, output_id).await;
        assert!(!spendable);
        assert_eq!(spent_by, Some(spender_tx_id));
        assert_eq!(
            desc, None,
            "internalize must never write spending_description"
        );
    }

    /// ts case 2: an unknown outpoint yields no transitions and no error;
    /// an empty outpoint list is likewise a no-op.
    #[tokio::test]
    async fn test_mark_inputs_spent_unknown_outpoint_noop() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_id, &"22".repeat(32), "unproven").await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions =
            mark_user_inputs_spent(&mut conn, user_id, spender_tx_id, &[("33".repeat(32), 7)])
                .await
                .unwrap();
        assert!(transitions.is_empty());

        let transitions = mark_user_inputs_spent(&mut conn, user_id, spender_tx_id, &[])
            .await
            .unwrap();
        assert!(transitions.is_empty());
    }

    /// ts case 3: a coin already claimed by a COMPETING transaction is
    /// skipped — no overwrite, no transition. Covers both the
    /// spendable = 0 + spent_by = competing shape and the pathological
    /// spendable = 1 + spent_by = competing shape.
    #[tokio::test]
    async fn test_mark_inputs_spent_skips_competing_spent_by() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let coin_txid = "11".repeat(32);
        let (_c1, out1) = seed_change_coin(&storage, user_id, &coin_txid, 0, 5000).await;
        let (_c2, out2) = seed_change_coin(&storage, user_id, &"55".repeat(32), 1, 6000).await;
        let competing_tx_id =
            insert_existing_tx(&storage, user_id, &"66".repeat(32), "unproven").await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_id, &"22".repeat(32), "unproven").await;

        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(competing_tx_id)
            .bind(out1)
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE outputs SET spendable = 1, spent_by = ? WHERE output_id = ?")
            .bind(competing_tx_id)
            .bind(out2)
            .execute(storage.pool())
            .await
            .unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions = mark_user_inputs_spent(
            &mut conn,
            user_id,
            spender_tx_id,
            &[(coin_txid.clone(), 0), ("55".repeat(32), 1)],
        )
        .await
        .unwrap();
        drop(conn);

        assert!(transitions.is_empty(), "competing claims must be skipped");
        let (spendable, spent_by, _) = output_state(&storage, out1).await;
        assert!(!spendable);
        assert_eq!(spent_by, Some(competing_tx_id), "no overwrite");
        let (spendable, spent_by, _) = output_state(&storage, out2).await;
        assert!(spendable);
        assert_eq!(spent_by, Some(competing_tx_id), "no overwrite");
    }

    /// ts case 4: the outpoint lookup is cross-user (TS :39-56). Both users'
    /// rows at the outpoint flip spendable = 0, but ONLY the internalizing
    /// user's row gets spent_by — the other user's spent_by FK-references
    /// THEIR transactions and stays untouched.
    #[tokio::test]
    async fn test_mark_inputs_spent_cross_user_rows() {
        let storage = create_test_storage().await;
        let user_a = create_test_user(&storage).await;
        let user_b = create_second_user(&storage).await;
        let coin_txid = "11".repeat(32);
        let (_ta, out_a) = seed_change_coin(&storage, user_a, &coin_txid, 0, 5000).await;
        let (_tb, out_b) = seed_change_coin(&storage, user_b, &coin_txid, 0, 5000).await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_a, &"22".repeat(32), "unproven").await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions =
            mark_user_inputs_spent(&mut conn, user_a, spender_tx_id, &[(coin_txid.clone(), 0)])
                .await
                .unwrap();
        drop(conn);

        assert_eq!(transitions.len(), 2);
        assert!(
            transitions
                .iter()
                .any(|t| t.output_id == out_a && t.set_spent_by),
            "owner row records set_spent_by = true"
        );
        assert!(
            transitions
                .iter()
                .any(|t| t.output_id == out_b && !t.set_spent_by),
            "other-user row records set_spent_by = false"
        );

        let (spendable, spent_by, _) = output_state(&storage, out_a).await;
        assert!(!spendable);
        assert_eq!(spent_by, Some(spender_tx_id));
        let (spendable, spent_by, _) = output_state(&storage, out_b).await;
        assert!(!spendable);
        assert_eq!(spent_by, None, "cross-user spent_by must stay untouched");
    }

    /// ts case 5: restore reverses both transition shapes — set_spent_by
    /// rows get spendable = 1 + spent_by = NULL; spendable-only rows get
    /// spendable = 1 with spent_by untouched.
    #[tokio::test]
    async fn test_restore_reverses_transitions() {
        let storage = create_test_storage().await;
        let user_a = create_test_user(&storage).await;
        let user_b = create_second_user(&storage).await;
        let coin_txid = "11".repeat(32);
        let (_ta, out_a) = seed_change_coin(&storage, user_a, &coin_txid, 0, 5000).await;
        let (_tb, out_b) = seed_change_coin(&storage, user_b, &coin_txid, 0, 5000).await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_a, &"22".repeat(32), "unproven").await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions =
            mark_user_inputs_spent(&mut conn, user_a, spender_tx_id, &[(coin_txid.clone(), 0)])
                .await
                .unwrap();
        drop(conn);
        assert_eq!(transitions.len(), 2);

        restore_inputs_to_spendable(storage.pool(), &transitions)
            .await
            .unwrap();

        let (spendable, spent_by, _) = output_state(&storage, out_a).await;
        assert!(spendable);
        assert_eq!(spent_by, None);
        let (spendable, spent_by, _) = output_state(&storage, out_b).await;
        assert!(spendable);
        assert_eq!(spent_by, None);
    }

    /// ts case 6: restore never clobbers a pre-existing COMPETING spent_by —
    /// rows skipped at mark time are not in the transition list, so they are
    /// untouched. Double-restore of the recorded list is harmless.
    #[tokio::test]
    async fn test_restore_does_not_clobber_competing_spent_by() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let (_c1, out_marked) =
            seed_change_coin(&storage, user_id, &"11".repeat(32), 0, 5000).await;
        let (_c2, out_competing) =
            seed_change_coin(&storage, user_id, &"55".repeat(32), 0, 6000).await;
        let competing_tx_id =
            insert_existing_tx(&storage, user_id, &"66".repeat(32), "unproven").await;
        let spender_tx_id =
            insert_existing_tx(&storage, user_id, &"22".repeat(32), "unproven").await;

        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(competing_tx_id)
            .bind(out_competing)
            .execute(storage.pool())
            .await
            .unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let transitions = mark_user_inputs_spent(
            &mut conn,
            user_id,
            spender_tx_id,
            &[("11".repeat(32), 0), ("55".repeat(32), 0)],
        )
        .await
        .unwrap();
        drop(conn);
        assert_eq!(transitions.len(), 1, "competing row not in transitions");

        restore_inputs_to_spendable(storage.pool(), &transitions)
            .await
            .unwrap();
        // Double-restore is harmless.
        restore_inputs_to_spendable(storage.pool(), &transitions)
            .await
            .unwrap();

        let (spendable, spent_by, _) = output_state(&storage, out_marked).await;
        assert!(spendable);
        assert_eq!(spent_by, None);
        let (spendable, spent_by, _) = output_state(&storage, out_competing).await;
        assert!(!spendable, "competing row untouched by restore");
        assert_eq!(spent_by, Some(competing_tx_id));
    }

    /// ts case 7: restoring an empty transition list is a no-op.
    #[tokio::test]
    async fn test_restore_empty_list_noop() {
        let storage = create_test_storage().await;
        restore_inputs_to_spendable(storage.pool(), &[])
            .await
            .unwrap();
    }

    /// ts case 8: inputs without a source txid are ignored by the outpoint
    /// extraction (they cannot name an outpoint to look up).
    #[test]
    fn test_extract_input_outpoints_skips_missing_source_txid() {
        let mut tx = Transaction::new();
        tx.inputs.push(TransactionInput::new("aa".repeat(32), 3));
        let mut missing = TransactionInput::new("bb".repeat(32), 1);
        missing.source_txid = None;
        tx.inputs.push(missing);

        let outpoints = extract_input_outpoints(&tx);
        assert_eq!(outpoints, vec![("aa".repeat(32), 3)]);
    }

    /// Incident regression (Calgooon/btc-relay-rs#16, success path): a
    /// default-basket change UTXO consumed by an externally-built
    /// internalized tx must stop matching coin selection — the selectable
    /// count DROPS (the stale-UTXO loop is dead). Basket insertion is used so
    /// the internalized output lands OUTSIDE the default basket, leaving the
    /// seeded coin as the only candidate.
    #[tokio::test]
    async fn test_internalize_spending_wallet_utxo_marks_it_spent() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let coin_txid = "44".repeat(32);
        let (_coin_tx_id, coin_output_id) =
            seed_change_coin(&storage, user_id, &coin_txid, 0, 20000).await;
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);

        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef_spending(&coin_txid, 0);
        let mock = MockWalletServicesBuilder::default()
            .post_beef_success()
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = InternalizeActionArgs {
            tx: beef_bytes,
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: BASKET_INSERTION_PROTOCOL.to_string(),
                payment_remittance: None,
                insertion_remittance: Some(BasketInsertion {
                    basket: "external".to_string(),
                    custom_instructions: None,
                    tags: None,
                }),
            }],
            description: "externally-built spend of wallet change".to_string(),
            labels: None,
            seek_permission: None,
        };

        let result = internalize_action_internal(&storage, user_id, args)
            .await
            .unwrap();
        assert!(result.base.accepted);

        // The consumed coin no longer matches the coin-selection predicate.
        assert_eq!(
            count_selectable_coins(&storage, user_id).await,
            0,
            "consumed coin must drop out of coin selection"
        );

        let internalized_tx_id: i64 = sqlx::query_scalar(
            "SELECT transaction_id FROM transactions WHERE user_id = ? AND txid = ?",
        )
        .bind(user_id)
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();

        let (spendable, spent_by, desc) = output_state(&storage, coin_output_id).await;
        assert!(!spendable);
        assert_eq!(spent_by, Some(internalized_tx_id));
        assert_eq!(desc, None);
    }

    /// Incident regression (broadcast hard-fail): when the internalized tx is
    /// definitively rejected by the network, the internalize errors and the
    /// consumed coin RETURNS to selectable (restore path), while the failed
    /// tx's own outputs stay unspendable.
    #[tokio::test]
    async fn test_internalize_broadcast_hard_fail_restores_consumed_coin() {
        let storage = create_test_storage().await;
        let user_id = create_test_user(&storage).await;
        let coin_txid = "44".repeat(32);
        let (_coin_tx_id, coin_output_id) =
            seed_change_coin(&storage, user_id, &coin_txid, 0, 20000).await;
        assert_eq!(count_selectable_coins(&storage, user_id).await, 1);

        let (beef_bytes, txid, _satoshis) = create_test_atomic_beef_spending(&coin_txid, 0);
        // Default get_status_for_txids mock returns no results, so the
        // reconcile step finds the tx dead and the rejection stands.
        let mock = MockWalletServicesBuilder::default()
            .post_beef_double_spend(&txid, "competing_txid")
            .build();
        WalletStorageProvider::set_services(&storage, Arc::new(mock));

        let args = wallet_payment_args(beef_bytes, "hard-fail spend of wallet change");
        let result = internalize_action_internal(&storage, user_id, args).await;
        assert!(result.is_err(), "hard reject must fail the internalize");

        // The consumed coin is restored to selectable.
        let (spendable, spent_by, _) = output_state(&storage, coin_output_id).await;
        assert!(spendable, "consumed coin must be restored");
        assert_eq!(spent_by, None);
        assert_eq!(
            count_selectable_coins(&storage, user_id).await,
            1,
            "restored coin is selectable again"
        );

        // The failed internalized tx leaves no spendable outputs behind.
        let spendable_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outputs WHERE user_id = ? AND txid = ? AND spendable = 1",
        )
        .bind(user_id)
        .bind(&txid)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(spendable_count, 0);
    }
}
