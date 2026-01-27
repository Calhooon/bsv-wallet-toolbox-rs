//! Create Action Implementation
//!
//! This module contains the full implementation of the `create_action` method
//! for the `StorageSqlx` wallet storage backend.

use crate::error::{Error, Result};
use crate::storage::entities::{
    TableOutput, TableOutputBasket, TableOutputTag, TableTxLabel,
};
use crate::storage::traits::{
    FindOutputBasketsArgs, StorageCreateActionResult, StorageCreateTransactionInput,
    StorageCreateTransactionOutput, StorageProvidedBy,
};
use chrono::Utc;
use sqlx::Row;

use super::StorageSqlx;

// =============================================================================
// Constants
// =============================================================================

/// Maximum satoshi value (total BTC supply in satoshis).
const MAX_SATOSHIS: u64 = 2_100_000_000_000_000;

/// Special satoshi value indicating "use maximum possible".
const MAX_POSSIBLE_SATOSHIS: u64 = 2_099_999_999_999_999;

/// Default fee rate in satoshis per kilobyte.
const DEFAULT_FEE_RATE_SAT_PER_KB: u64 = 10;

/// P2PKH locking script length (25 bytes).
const P2PKH_LOCKING_SCRIPT_LENGTH: u32 = 25;

/// P2PKH unlocking script length (107 bytes typical).
const P2PKH_UNLOCKING_SCRIPT_LENGTH: u32 = 107;

/// Minimum description length.
const MIN_DESCRIPTION_LENGTH: usize = 5;

/// Maximum description length.
const MAX_DESCRIPTION_LENGTH: usize = 2000;

/// Maximum label length.
const MAX_LABEL_LENGTH: usize = 300;

// =============================================================================
// Internal Types
// =============================================================================

/// Extended input with additional context for transaction creation.
#[derive(Debug, Clone)]
struct ExtendedInput {
    vin: u32,
    txid: String,
    vout: u32,
    satoshis: u64,
    locking_script: Vec<u8>,
    unlocking_script_length: u32,
    input_description: Option<String>,
    /// Associated output record if this input spends a known output.
    output: Option<TableOutput>,
}

/// Extended output with additional context for transaction creation.
#[derive(Debug, Clone)]
struct ExtendedOutput {
    vout: u32,
    satoshis: u64,
    locking_script: Vec<u8>,
    output_description: String,
    basket: Option<String>,
    tags: Vec<String>,
    custom_instructions: Option<String>,
    provided_by: StorageProvidedBy,
    purpose: Option<String>,
    derivation_suffix: Option<String>,
}

/// Parameters for change generation.
struct GenerateChangeParams {
    fixed_inputs: Vec<FixedInput>,
    fixed_outputs: Vec<FixedOutput>,
    fee_rate: u64,
    change_initial_satoshis: u64,
    change_first_satoshis: u64,
    change_locking_script_length: u32,
    change_unlocking_script_length: u32,
    target_net_count: Option<i32>,
}

/// A fixed input for fee calculation.
#[derive(Debug, Clone)]
struct FixedInput {
    satoshis: u64,
    unlocking_script_length: u32,
}

/// A fixed output for fee calculation.
#[derive(Debug, Clone)]
struct FixedOutput {
    satoshis: u64,
    locking_script_length: u32,
}

/// Result of change generation.
struct GenerateChangeResult {
    allocated_change_inputs: Vec<AllocatedChangeInput>,
    change_outputs: Vec<ChangeOutput>,
}

/// An allocated change input.
#[derive(Debug, Clone)]
struct AllocatedChangeInput {
    #[allow(dead_code)]
    output_id: i64,
    satoshis: u64,
    txid: String,
    vout: i32,
    locking_script: Vec<u8>,
    derivation_prefix: Option<String>,
    derivation_suffix: Option<String>,
}

/// A generated change output.
#[derive(Debug, Clone)]
struct ChangeOutput {
    satoshis: u64,
    vout: u32,
    derivation_prefix: String,
    derivation_suffix: String,
}

// =============================================================================
// Validation
// =============================================================================

/// Validates create action arguments.
fn validate_create_action_args(args: &bsv_sdk::wallet::CreateActionArgs) -> Result<()> {
    // Validate description length
    if args.description.len() < MIN_DESCRIPTION_LENGTH {
        return Err(Error::ValidationError(format!(
            "description length must be between {} and {}, got {}",
            MIN_DESCRIPTION_LENGTH,
            MAX_DESCRIPTION_LENGTH,
            args.description.len()
        )));
    }
    if args.description.len() > MAX_DESCRIPTION_LENGTH {
        return Err(Error::ValidationError(format!(
            "description length must be between {} and {}, got {}",
            MIN_DESCRIPTION_LENGTH,
            MAX_DESCRIPTION_LENGTH,
            args.description.len()
        )));
    }

    // Validate labels
    if let Some(ref labels) = args.labels {
        for label in labels {
            if label.is_empty() {
                return Err(Error::ValidationError("label cannot be empty".to_string()));
            }
            if label.len() > MAX_LABEL_LENGTH {
                return Err(Error::ValidationError(format!(
                    "label exceeds maximum length of {} characters",
                    MAX_LABEL_LENGTH
                )));
            }
        }
    }

    // Validate outputs
    if let Some(ref outputs) = args.outputs {
        for (i, output) in outputs.iter().enumerate() {
            // Validate locking script is not empty
            if output.locking_script.is_empty() {
                return Err(Error::ValidationError(format!(
                    "outputs[{}]: locking script cannot be empty",
                    i
                )));
            }

            // Validate satoshis
            if output.satoshis > MAX_SATOSHIS && output.satoshis != MAX_POSSIBLE_SATOSHIS {
                return Err(Error::ValidationError(format!(
                    "outputs[{}]: satoshis exceeds maximum value of {}",
                    i, MAX_SATOSHIS
                )));
            }

            // Validate output description
            if output.output_description.len() < MIN_DESCRIPTION_LENGTH {
                return Err(Error::ValidationError(format!(
                    "outputs[{}]: output description length must be between {} and {}",
                    i, MIN_DESCRIPTION_LENGTH, MAX_DESCRIPTION_LENGTH
                )));
            }

            // Validate basket if specified
            if let Some(ref basket) = output.basket {
                if basket.is_empty() {
                    return Err(Error::ValidationError(format!(
                        "outputs[{}]: basket cannot be empty when specified",
                        i
                    )));
                }
            }

            // Validate tags
            if let Some(ref tags) = output.tags {
                for (j, tag) in tags.iter().enumerate() {
                    if tag.is_empty() {
                        return Err(Error::ValidationError(format!(
                            "outputs[{}].tags[{}]: tag cannot be empty",
                            i, j
                        )));
                    }
                }
            }
        }
    }

    // Validate inputs
    if let Some(ref inputs) = args.inputs {
        let mut seen_outpoints = std::collections::HashSet::new();

        for (i, input) in inputs.iter().enumerate() {
            // Check for unlocking script or length
            if input.unlocking_script.is_none() && input.unlocking_script_length.is_none() {
                return Err(Error::ValidationError(format!(
                    "inputs[{}]: unlockingScript or unlockingScriptLength required",
                    i
                )));
            }

            // If both are provided, verify length matches
            if let (Some(ref script), Some(length)) =
                (&input.unlocking_script, input.unlocking_script_length)
            {
                if script.len() as u32 != length {
                    return Err(Error::ValidationError(format!(
                        "inputs[{}]: unlocking script length mismatch: actual {} vs specified {}",
                        i,
                        script.len(),
                        length
                    )));
                }
            }

            // Check for duplicate outpoints
            let outpoint_key = format!(
                "{}.{}",
                hex::encode(input.outpoint.txid),
                input.outpoint.vout
            );
            if !seen_outpoints.insert(outpoint_key.clone()) {
                return Err(Error::ValidationError(format!(
                    "inputs[{}]: duplicate outpoint {}",
                    i, outpoint_key
                )));
            }
        }
    }

    // Validate noSendChange for duplicates
    if let Some(ref options) = args.options {
        if let Some(ref no_send_change) = options.no_send_change {
            let mut seen_outpoints = std::collections::HashSet::new();
            for outpoint in no_send_change {
                let key = format!("{}.{}", hex::encode(outpoint.txid), outpoint.vout);
                if !seen_outpoints.insert(key.clone()) {
                    return Err(Error::ValidationError(format!(
                        "duplicate outpoint in noSendChange: {}",
                        key
                    )));
                }
            }
        }
    }

    Ok(())
}

// =============================================================================
// Size Calculation
// =============================================================================

/// Calculates transaction size given input and output script lengths.
fn calculate_transaction_size(
    input_script_lengths: &[u32],
    output_script_lengths: &[u32],
) -> u64 {
    // Transaction overhead: version (4) + locktime (4) + input count varint + output count varint
    let mut size: u64 = 4 + 4;

    // Input count varint
    size += var_int_size(input_script_lengths.len() as u64);

    // Each input: txid (32) + vout (4) + script length varint + script + sequence (4)
    for script_len in input_script_lengths {
        size += 32 + 4 + var_int_size(*script_len as u64) + *script_len as u64 + 4;
    }

    // Output count varint
    size += var_int_size(output_script_lengths.len() as u64);

    // Each output: satoshis (8) + script length varint + script
    for script_len in output_script_lengths {
        size += 8 + var_int_size(*script_len as u64) + *script_len as u64;
    }

    size
}

/// Returns the size of a varint encoding.
fn var_int_size(value: u64) -> u64 {
    if value < 253 {
        1
    } else if value <= 0xFFFF {
        3
    } else if value <= 0xFFFFFFFF {
        5
    } else {
        9
    }
}

// =============================================================================
// Random Generation
// =============================================================================

/// Generates a random base64-encoded string for derivation paths.
fn random_derivation(count: usize) -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = vec![0u8; count];
    rng.fill_bytes(&mut bytes);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes)
}

/// Generates a random reference string.
fn random_reference() -> String {
    random_derivation(12)
}

// =============================================================================
// Main Implementation
// =============================================================================

/// Internal implementation of create_action.
pub async fn create_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: bsv_sdk::wallet::CreateActionArgs,
) -> Result<StorageCreateActionResult> {
    // Step 1: Validate all inputs
    validate_create_action_args(&args)?;

    // Determine action flags
    let options = args.options.as_ref();
    let is_no_send = options.and_then(|o| o.no_send).unwrap_or(false);
    let is_delayed = options.and_then(|o| o.accept_delayed_broadcast).unwrap_or(false);

    // Step 2: Get or create default output basket
    let change_basket = storage.find_or_create_default_basket(user_id).await?;

    // Step 3: Process caller-provided outputs
    let extended_outputs = validate_and_extend_outputs(&args)?;

    // Step 4: Process caller-provided inputs
    let extended_inputs = validate_and_extend_inputs(storage, user_id, &args).await?;

    // Step 5: Count available change outputs for targeting
    let available_change_count =
        count_change_inputs(storage, user_id, change_basket.basket_id, !is_delayed).await?;

    // Step 6: Create transaction record in DB
    let transaction_id = create_transaction_record(storage, user_id, &args).await?;

    // Step 7: Create transaction labels
    if let Some(ref labels) = args.labels {
        for label in labels {
            let tx_label = find_or_insert_tx_label(storage, user_id, label).await?;
            find_or_insert_tx_label_map(storage, transaction_id, tx_label.label_id).await?;
        }
    }

    // Step 8: Calculate fees and generate change
    let fee_rate = DEFAULT_FEE_RATE_SAT_PER_KB;

    let params = GenerateChangeParams {
        fixed_inputs: extended_inputs
            .iter()
            .map(|i| FixedInput {
                satoshis: i.satoshis,
                unlocking_script_length: i.unlocking_script_length,
            })
            .collect(),
        fixed_outputs: extended_outputs
            .iter()
            .map(|o| FixedOutput {
                satoshis: o.satoshis,
                locking_script_length: o.locking_script.len() as u32,
            })
            .collect(),
        fee_rate,
        change_initial_satoshis: change_basket.minimum_desired_utxo_value as u64,
        change_first_satoshis: std::cmp::max(
            1,
            (change_basket.minimum_desired_utxo_value / 4) as u64,
        ),
        change_locking_script_length: P2PKH_LOCKING_SCRIPT_LENGTH,
        change_unlocking_script_length: P2PKH_UNLOCKING_SCRIPT_LENGTH,
        target_net_count: Some(
            change_basket.number_of_desired_utxos - available_change_count as i32,
        ),
    };

    let derivation_prefix = random_derivation(16);

    let change_result = generate_change(
        storage,
        user_id,
        change_basket.basket_id,
        transaction_id,
        &params,
        &derivation_prefix,
        is_delayed,
    )
    .await?;

    // Step 9: Mark extended inputs as spent
    for input in &extended_inputs {
        if let Some(ref output) = input.output {
            update_output_spent(
                storage,
                output.output_id,
                transaction_id,
                input.input_description.as_deref(),
            )
            .await?;
        }
    }

    // Step 10: Calculate net satoshis (change received - change spent)
    let change_out_sats: i64 = change_result
        .change_outputs
        .iter()
        .map(|o| o.satoshis as i64)
        .sum();
    let change_in_sats: i64 = change_result
        .allocated_change_inputs
        .iter()
        .map(|i| i.satoshis as i64)
        .sum();
    let satoshis = change_out_sats - change_in_sats;

    // Update transaction with calculated satoshis
    update_transaction_satoshis(storage, transaction_id, satoshis).await?;

    // Step 11: Create output records
    let mut result_outputs = Vec::new();
    let mut change_vouts = Vec::new();

    // First, handle user-specified outputs
    for xo in &extended_outputs {
        let basket_id = if let Some(ref basket_name) = xo.basket {
            let basket = find_or_insert_output_basket(storage, user_id, basket_name).await?;
            Some(basket.basket_id)
        } else {
            None
        };

        let output_id = insert_output(
            storage,
            user_id,
            transaction_id,
            basket_id,
            xo.vout as i32,
            xo.satoshis as i64,
            &xo.locking_script,
            &xo.output_description,
            &xo.provided_by,
            xo.purpose.as_deref().unwrap_or(""),
            xo.custom_instructions.as_deref(),
            None, // derivation_prefix for user outputs
            xo.derivation_suffix.as_deref(),
            false, // not change
            true,  // spendable
        )
        .await?;

        // Create tag associations
        for tag in &xo.tags {
            let output_tag = find_or_insert_output_tag(storage, user_id, tag).await?;
            find_or_insert_output_tag_map(storage, output_id, output_tag.tag_id).await?;
        }

        result_outputs.push(StorageCreateTransactionOutput {
            vout: xo.vout,
            satoshis: xo.satoshis,
            locking_script: hex::encode(&xo.locking_script),
            provided_by: xo.provided_by,
            purpose: xo.purpose.clone(),
            derivation_suffix: xo.derivation_suffix.clone(),
            basket: xo.basket.clone(),
            tags: xo.tags.clone(),
            output_description: Some(xo.output_description.clone()),
            custom_instructions: xo.custom_instructions.clone(),
        });
    }

    // Then, handle change outputs
    let base_vout = extended_outputs.len() as u32;
    for (i, co) in change_result.change_outputs.iter().enumerate() {
        let vout = base_vout + i as u32;

        let _output_id = insert_output(
            storage,
            user_id,
            transaction_id,
            Some(change_basket.basket_id),
            vout as i32,
            co.satoshis as i64,
            &[], // locking script will be filled in when signed
            "",
            &StorageProvidedBy::Storage,
            "change",
            None,
            Some(&co.derivation_prefix),
            Some(&co.derivation_suffix),
            true,  // is change
            false, // not spendable until confirmed
        )
        .await?;

        change_vouts.push(vout);

        result_outputs.push(StorageCreateTransactionOutput {
            vout,
            satoshis: co.satoshis,
            locking_script: String::new(), // Will be filled in by signer
            provided_by: StorageProvidedBy::Storage,
            purpose: Some("change".to_string()),
            derivation_suffix: Some(co.derivation_suffix.clone()),
            basket: Some("default".to_string()),
            tags: vec![],
            output_description: None,
            custom_instructions: None,
        });
    }

    // Step 12: Build result inputs
    let mut result_inputs = Vec::new();

    // First, user-specified inputs
    for xi in &extended_inputs {
        let (provided_by, d_prefix, d_suffix, sender_identity_key, input_type) =
            if let Some(ref output) = xi.output {
                let pb = if output.change {
                    StorageProvidedBy::YouAndStorage
                } else {
                    StorageProvidedBy::You
                };
                (
                    pb,
                    output.derivation_prefix.clone(),
                    output.derivation_suffix.clone(),
                    output.sender_identity_key.clone(),
                    output.output_type.clone(),
                )
            } else {
                (
                    StorageProvidedBy::You,
                    None,
                    None,
                    None,
                    "custom".to_string(),
                )
            };

        result_inputs.push(StorageCreateTransactionInput {
            vin: xi.vin,
            source_txid: xi.txid.clone(),
            source_vout: xi.vout,
            source_satoshis: xi.satoshis,
            source_locking_script: hex::encode(&xi.locking_script),
            source_transaction: None,
            unlocking_script_length: xi.unlocking_script_length,
            provided_by,
            input_type,
            spending_description: xi.input_description.clone(),
            derivation_prefix: d_prefix,
            derivation_suffix: d_suffix,
            sender_identity_key,
        });
    }

    // Then, allocated change inputs
    let base_vin = extended_inputs.len() as u32;
    for (i, aci) in change_result.allocated_change_inputs.iter().enumerate() {
        result_inputs.push(StorageCreateTransactionInput {
            vin: base_vin + i as u32,
            source_txid: aci.txid.clone(),
            source_vout: aci.vout as u32,
            source_satoshis: aci.satoshis,
            source_locking_script: hex::encode(&aci.locking_script),
            source_transaction: None,
            unlocking_script_length: P2PKH_UNLOCKING_SCRIPT_LENGTH,
            provided_by: StorageProvidedBy::Storage,
            input_type: "P2PKH".to_string(),
            spending_description: None,
            derivation_prefix: aci.derivation_prefix.clone(),
            derivation_suffix: aci.derivation_suffix.clone(),
            sender_identity_key: None,
        });
    }

    // Get the transaction reference
    let reference = get_transaction_reference(storage, transaction_id).await?;

    // Build final result
    Ok(StorageCreateActionResult {
        reference,
        version: args.version.unwrap_or(1),
        lock_time: args.lock_time.unwrap_or(0),
        inputs: result_inputs,
        outputs: result_outputs,
        derivation_prefix,
        input_beef: None, // Will be populated if needed by higher layer
        no_send_change_output_vouts: if is_no_send {
            Some(change_vouts)
        } else {
            None
        },
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Validates and extends output specifications.
fn validate_and_extend_outputs(
    args: &bsv_sdk::wallet::CreateActionArgs,
) -> Result<Vec<ExtendedOutput>> {
    let mut extended = Vec::new();

    if let Some(ref outputs) = args.outputs {
        for (i, output) in outputs.iter().enumerate() {
            extended.push(ExtendedOutput {
                vout: i as u32,
                satoshis: output.satoshis,
                locking_script: output.locking_script.clone(),
                output_description: output.output_description.clone(),
                basket: output.basket.clone(),
                tags: output.tags.clone().unwrap_or_default(),
                custom_instructions: output.custom_instructions.clone(),
                provided_by: StorageProvidedBy::You,
                purpose: None,
                derivation_suffix: None,
            });
        }
    }

    Ok(extended)
}

/// Validates and extends input specifications, looking up associated outputs.
async fn validate_and_extend_inputs(
    storage: &StorageSqlx,
    user_id: i64,
    args: &bsv_sdk::wallet::CreateActionArgs,
) -> Result<Vec<ExtendedInput>> {
    let mut extended = Vec::new();

    if let Some(ref inputs) = args.inputs {
        for (i, input) in inputs.iter().enumerate() {
            let txid = hex::encode(input.outpoint.txid);
            let vout = input.outpoint.vout;

            // Try to find the output being spent
            let output = storage
                .find_output_by_outpoint(user_id, &txid, vout)
                .await?;

            let (satoshis, locking_script) = if let Some(ref out) = output {
                let script = out.locking_script.clone().unwrap_or_default();
                (out.satoshis as u64, script)
            } else {
                // If output not found, we need to get satoshis/script from BEEF
                // For now, return an error - full BEEF parsing would be needed
                return Err(Error::ValidationError(format!(
                    "inputs[{}]: output {}:{} not found in storage. BEEF validation required.",
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

/// Counts available change inputs in the default basket.
async fn count_change_inputs(
    storage: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    require_spendable: bool,
) -> Result<usize> {
    let spendable_clause = if require_spendable {
        "AND o.spendable = 1"
    } else {
        ""
    };

    let sql = format!(
        r#"
        SELECT COUNT(*) as count
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        WHERE o.user_id = ?
          AND o.basket_id = ?
          AND o.change = 1
          AND t.status IN ('completed', 'unproven')
          {}
        "#,
        spendable_clause
    );

    let row = sqlx::query(&sql)
        .bind(user_id)
        .bind(basket_id)
        .fetch_one(storage.pool())
        .await?;

    let count: i64 = row.get("count");
    Ok(count as usize)
}

/// Creates a new transaction record.
async fn create_transaction_record(
    storage: &StorageSqlx,
    user_id: i64,
    args: &bsv_sdk::wallet::CreateActionArgs,
) -> Result<i64> {
    let now = Utc::now();
    let reference = random_reference();
    let version = args.version.unwrap_or(1) as i32;
    let lock_time = args.lock_time.unwrap_or(0) as i64;

    let result = sqlx::query(
        r#"
        INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, created_at, updated_at)
        VALUES (?, 'unsigned', ?, 1, 0, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(&reference)
    .bind(version)
    .bind(lock_time)
    .bind(&args.description)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(result.last_insert_rowid())
}

/// Gets the reference for a transaction.
async fn get_transaction_reference(storage: &StorageSqlx, transaction_id: i64) -> Result<String> {
    let row = sqlx::query("SELECT reference FROM transactions WHERE transaction_id = ?")
        .bind(transaction_id)
        .fetch_one(storage.pool())
        .await?;

    Ok(row.get("reference"))
}

/// Updates transaction satoshis.
async fn update_transaction_satoshis(
    storage: &StorageSqlx,
    transaction_id: i64,
    satoshis: i64,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query("UPDATE transactions SET satoshis = ?, updated_at = ? WHERE transaction_id = ?")
        .bind(satoshis)
        .bind(now)
        .bind(transaction_id)
        .execute(storage.pool())
        .await?;

    Ok(())
}

/// Marks an output as spent.
async fn update_output_spent(
    storage: &StorageSqlx,
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
    .execute(storage.pool())
    .await?;

    Ok(())
}

/// Finds or creates a transaction label.
async fn find_or_insert_tx_label(
    storage: &StorageSqlx,
    user_id: i64,
    label: &str,
) -> Result<TableTxLabel> {
    let row = sqlx::query(
        "SELECT tx_label_id, user_id, label, created_at, updated_at FROM tx_labels WHERE user_id = ? AND label = ?",
    )
    .bind(user_id)
    .bind(label)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = row {
        return Ok(TableTxLabel {
            label_id: row.get("tx_label_id"),
            user_id: row.get("user_id"),
            label: row.get("label"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(label)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(TableTxLabel {
        label_id: result.last_insert_rowid(),
        user_id,
        label: label.to_string(),
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates a transaction label map entry.
async fn find_or_insert_tx_label_map(
    storage: &StorageSqlx,
    transaction_id: i64,
    label_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT tx_label_map_id FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?",
    )
    .bind(transaction_id)
    .bind(label_id)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = row {
        return Ok(row.get("tx_label_map_id"));
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(transaction_id)
    .bind(label_id)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(result.last_insert_rowid())
}

/// Finds or creates an output basket.
async fn find_or_insert_output_basket(
    storage: &StorageSqlx,
    user_id: i64,
    name: &str,
) -> Result<TableOutputBasket> {
    let args = FindOutputBasketsArgs {
        user_id: Some(user_id),
        name: Some(name.to_string()),
        ..Default::default()
    };

    let baskets = storage.find_output_baskets_internal(user_id, &args).await?;

    if let Some(basket) = baskets.into_iter().next() {
        return Ok(basket);
    }

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

    Ok(TableOutputBasket {
        basket_id: result.last_insert_rowid(),
        user_id,
        name: name.to_string(),
        number_of_desired_utxos: 6,
        minimum_desired_utxo_value: 10000,
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates an output tag.
async fn find_or_insert_output_tag(
    storage: &StorageSqlx,
    user_id: i64,
    tag: &str,
) -> Result<TableOutputTag> {
    let row = sqlx::query(
        "SELECT output_tag_id, user_id, tag, created_at, updated_at FROM output_tags WHERE user_id = ? AND tag = ?",
    )
    .bind(user_id)
    .bind(tag)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = row {
        return Ok(TableOutputTag {
            tag_id: row.get("output_tag_id"),
            user_id: row.get("user_id"),
            tag: row.get("tag"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(tag)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(TableOutputTag {
        tag_id: result.last_insert_rowid(),
        user_id,
        tag: tag.to_string(),
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates an output tag map entry.
async fn find_or_insert_output_tag_map(
    storage: &StorageSqlx,
    output_id: i64,
    tag_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT output_tag_map_id FROM output_tags_map WHERE output_id = ? AND output_tag_id = ?",
    )
    .bind(output_id)
    .bind(tag_id)
    .fetch_optional(storage.pool())
    .await?;

    if let Some(row) = row {
        return Ok(row.get("output_tag_map_id"));
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(output_id)
    .bind(tag_id)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(result.last_insert_rowid())
}

/// Inserts a new output record.
#[allow(clippy::too_many_arguments)]
async fn insert_output(
    storage: &StorageSqlx,
    user_id: i64,
    transaction_id: i64,
    basket_id: Option<i64>,
    vout: i32,
    satoshis: i64,
    locking_script: &[u8],
    output_description: &str,
    provided_by: &StorageProvidedBy,
    purpose: &str,
    custom_instructions: Option<&str>,
    derivation_prefix: Option<&str>,
    derivation_suffix: Option<&str>,
    change: bool,
    spendable: bool,
) -> Result<i64> {
    let now = Utc::now();
    let provided_by_str = match provided_by {
        StorageProvidedBy::You => "you",
        StorageProvidedBy::Storage => "storage",
        StorageProvidedBy::YouAndStorage => "you-and-storage",
    };

    let output_type = if change { "P2PKH" } else { "custom" };
    let script_to_store = if locking_script.is_empty() {
        None
    } else {
        Some(locking_script)
    };

    let result = sqlx::query(
        r#"
        INSERT INTO outputs (
            user_id, transaction_id, basket_id, vout, satoshis, locking_script,
            output_description, provided_by, purpose, type, custom_instructions,
            derivation_prefix, derivation_suffix, change, spendable, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(basket_id)
    .bind(vout)
    .bind(satoshis)
    .bind(script_to_store)
    .bind(output_description)
    .bind(provided_by_str)
    .bind(purpose)
    .bind(output_type)
    .bind(custom_instructions)
    .bind(derivation_prefix)
    .bind(derivation_suffix)
    .bind(change)
    .bind(spendable)
    .bind(now)
    .bind(now)
    .execute(storage.pool())
    .await?;

    Ok(result.last_insert_rowid())
}

// =============================================================================
// Change Generation
// =============================================================================

/// Generates change outputs and allocates change inputs to fund the transaction.
async fn generate_change(
    storage: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    params: &GenerateChangeParams,
    derivation_prefix: &str,
    is_delayed: bool,
) -> Result<GenerateChangeResult> {
    let mut allocated_inputs: Vec<AllocatedChangeInput> = Vec::new();
    let mut change_outputs: Vec<ChangeOutput> = Vec::new();

    // Calculate initial funding requirement
    let fixed_input_sats: u64 = params.fixed_inputs.iter().map(|i| i.satoshis).sum();
    let fixed_output_sats: u64 = params.fixed_outputs.iter().map(|o| o.satoshis).sum();

    // Helper closure to calculate current state
    let calculate_state = |alloc_inputs: &[AllocatedChangeInput],
                           change_outs: &[ChangeOutput]|
     -> (u64, u64, u64, i64) {
        let input_sats: u64 =
            fixed_input_sats + alloc_inputs.iter().map(|i| i.satoshis).sum::<u64>();
        let output_sats: u64 =
            fixed_output_sats + change_outs.iter().map(|o| o.satoshis).sum::<u64>();

        let input_script_lengths: Vec<u32> = params
            .fixed_inputs
            .iter()
            .map(|i| i.unlocking_script_length)
            .chain(std::iter::repeat(params.change_unlocking_script_length).take(alloc_inputs.len()))
            .collect();

        let output_script_lengths: Vec<u32> = params
            .fixed_outputs
            .iter()
            .map(|o| o.locking_script_length)
            .chain(
                std::iter::repeat(params.change_locking_script_length).take(change_outs.len()),
            )
            .collect();

        let size = calculate_transaction_size(&input_script_lengths, &output_script_lengths);
        let fee_required = (size * params.fee_rate + 999) / 1000; // Ceiling division

        let fee_excess = input_sats as i64 - output_sats as i64 - fee_required as i64;

        (input_sats, output_sats, fee_required, fee_excess)
    };

    // Initial state calculation
    let (_, _, _, mut fee_excess) = calculate_state(&allocated_inputs, &change_outputs);

    // If we have excess and want to increase UTXO count, add change outputs
    let target_net = params.target_net_count.unwrap_or(0);
    while fee_excess > 0 || (target_net > 0 && (change_outputs.len() as i32) < target_net) {
        let satoshis = if change_outputs.is_empty() {
            params.change_first_satoshis
        } else {
            params.change_initial_satoshis
        };

        // Check if adding this output is worthwhile
        let (_, _, _, new_excess) = calculate_state(
            &allocated_inputs,
            &[
                change_outputs.clone(),
                vec![ChangeOutput {
                    satoshis,
                    vout: 0,
                    derivation_prefix: String::new(),
                    derivation_suffix: String::new(),
                }],
            ]
            .concat(),
        );

        if new_excess < 0 && fee_excess <= 0 {
            break;
        }

        change_outputs.push(ChangeOutput {
            satoshis,
            vout: (params.fixed_outputs.len() + change_outputs.len()) as u32,
            derivation_prefix: derivation_prefix.to_string(),
            derivation_suffix: random_derivation(16),
        });

        fee_excess = new_excess;

        if fee_excess >= 0 && (target_net <= 0 || (change_outputs.len() as i32) >= target_net) {
            break;
        }
    }

    // If we need more funding, allocate change inputs
    while fee_excess < 0 {
        let target_sats = (-fee_excess) as u64 + params.change_initial_satoshis;

        let allocated = allocate_change_input(
            storage,
            user_id,
            basket_id,
            transaction_id,
            target_sats,
            !is_delayed,
        )
        .await?;

        if let Some(input) = allocated {
            allocated_inputs.push(input);
            let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
            fee_excess = new_excess;

            // If we now have excess, add a change output if beneficial
            if fee_excess > 0 && change_outputs.is_empty() {
                let change_sats = std::cmp::min(fee_excess as u64, params.change_first_satoshis);
                change_outputs.push(ChangeOutput {
                    satoshis: change_sats,
                    vout: params.fixed_outputs.len() as u32,
                    derivation_prefix: derivation_prefix.to_string(),
                    derivation_suffix: random_derivation(16),
                });
                let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
                fee_excess = new_excess;
            }
        } else {
            // No more change inputs available
            // If we can't fund it, remove change outputs and try again
            if !change_outputs.is_empty() {
                change_outputs.pop();
                let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
                fee_excess = new_excess;

                if fee_excess >= 0 {
                    break;
                }
            } else {
                // Truly insufficient funds
                let (input_sats, output_sats, fee_required, _) =
                    calculate_state(&allocated_inputs, &change_outputs);
                return Err(Error::InsufficientFunds {
                    needed: output_sats + fee_required,
                    available: input_sats,
                });
            }
        }
    }

    // Distribute excess fee to change outputs
    if fee_excess > 0 && !change_outputs.is_empty() {
        // Give all excess to the first change output
        change_outputs[0].satoshis += fee_excess as u64;
    }

    Ok(GenerateChangeResult {
        allocated_change_inputs: allocated_inputs,
        change_outputs,
    })
}

/// Allocates a change input from the default basket.
async fn allocate_change_input(
    storage: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    transaction_id: i64,
    target_satoshis: u64,
    require_spendable: bool,
) -> Result<Option<AllocatedChangeInput>> {
    let spendable_clause = if require_spendable {
        "AND o.spendable = 1"
    } else {
        ""
    };

    // Try to find an output with at least target_satoshis
    let sql = format!(
        r#"
        SELECT o.output_id, o.satoshis, o.txid, o.vout, o.locking_script,
               o.derivation_prefix, o.derivation_suffix
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        WHERE o.user_id = ?
          AND o.basket_id = ?
          AND o.change = 1
          AND o.spent_by IS NULL
          AND t.status IN ('completed', 'unproven')
          {}
        ORDER BY
            CASE WHEN o.satoshis >= ? THEN 0 ELSE 1 END,
            ABS(o.satoshis - ?) ASC
        LIMIT 1
        "#,
        spendable_clause
    );

    let row = sqlx::query(&sql)
        .bind(user_id)
        .bind(basket_id)
        .bind(target_satoshis as i64)
        .bind(target_satoshis as i64)
        .fetch_optional(storage.pool())
        .await?;

    if let Some(row) = row {
        let output_id: i64 = row.get("output_id");
        let satoshis: i64 = row.get("satoshis");
        let txid: String = row.get("txid");
        let vout: i32 = row.get("vout");
        let locking_script: Option<Vec<u8>> = row.get("locking_script");
        let derivation_prefix: Option<String> = row.get("derivation_prefix");
        let derivation_suffix: Option<String> = row.get("derivation_suffix");

        // Mark as allocated (spent_by this transaction)
        let now = Utc::now();
        sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
        )
        .bind(transaction_id)
        .bind(now)
        .bind(output_id)
        .execute(storage.pool())
        .await?;

        Ok(Some(AllocatedChangeInput {
            output_id,
            satoshis: satoshis as u64,
            txid,
            vout,
            locking_script: locking_script.unwrap_or_default(),
            derivation_prefix,
            derivation_suffix,
        }))
    } else {
        Ok(None)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_sdk::wallet::{CreateActionOutput, CreateActionOptions};
    use crate::storage::traits::WalletStorageWriter;

    #[test]
    fn test_var_int_size() {
        assert_eq!(var_int_size(0), 1);
        assert_eq!(var_int_size(252), 1);
        assert_eq!(var_int_size(253), 3);
        assert_eq!(var_int_size(0xFFFF), 3);
        assert_eq!(var_int_size(0x10000), 5);
        assert_eq!(var_int_size(0xFFFFFFFF), 5);
        assert_eq!(var_int_size(0x100000000), 9);
    }

    #[test]
    fn test_calculate_transaction_size() {
        // Empty transaction: version (4) + locktime (4) + input count varint (1) + output count varint (1)
        let size = calculate_transaction_size(&[], &[]);
        assert_eq!(size, 10);

        // Transaction with one P2PKH input (32 + 4 + 1 + 107 + 4 = 148) and one P2PKH output (8 + 1 + 25 = 34)
        let size = calculate_transaction_size(&[107], &[25]);
        // 4 + 4 + 1 + 148 + 1 + 34 = 192
        assert_eq!(size, 192);
    }

    #[test]
    fn test_random_derivation() {
        let d1 = random_derivation(16);
        let d2 = random_derivation(16);

        // Should be base64 encoded
        assert!(d1.chars().all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='));

        // Should be different each time
        assert_ne!(d1, d2);
    }

    #[test]
    fn test_validate_description_too_short() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "abc".to_string(),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("description length"));
    }

    #[test]
    fn test_validate_description_too_long() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "a".repeat(2001),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("description length"));
    }

    #[test]
    fn test_validate_description_valid() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        assert!(validate_create_action_args(&args).is_ok());
    }

    #[test]
    fn test_validate_empty_label() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: Some(vec!["".to_string()]),
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("label cannot be empty"));
    }

    #[test]
    fn test_validate_label_too_long() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: None,
            lock_time: None,
            version: None,
            labels: Some(vec!["a".repeat(301)]),
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("label exceeds maximum length"));
    }

    #[test]
    fn test_validate_output_empty_locking_script() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![],
                satoshis: 42000,
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
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("locking script cannot be empty"));
    }

    #[test]
    fn test_validate_output_satoshis_too_high() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14], // Partial P2PKH
                satoshis: MAX_SATOSHIS + 1,
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
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("satoshis exceeds maximum value"));
    }

    #[test]
    fn test_validate_output_description_too_short() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14],
                satoshis: 42000,
                output_description: "abc".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("output description length"));
    }

    #[test]
    fn test_validate_output_empty_basket() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14],
                satoshis: 42000,
                output_description: "Test output".to_string(),
                basket: Some("".to_string()),
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("basket cannot be empty"));
    }

    #[test]
    fn test_validate_output_empty_tag() {
        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: vec![0x76, 0xa9, 0x14],
                satoshis: 42000,
                output_description: "Test output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: Some(vec!["".to_string()]),
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("tag cannot be empty"));
    }

    #[test]
    fn test_validate_valid_output() {
        // Standard P2PKH locking script
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 42000,
                output_description: "Test output".to_string(),
                basket: Some("payments".to_string()),
                custom_instructions: Some("{\"type\":\"BRC29\"}".to_string()),
                tags: Some(vec!["test_tag".to_string()]),
            }]),
            lock_time: None,
            version: None,
            labels: Some(vec!["test_label".to_string()]),
            options: None,
        };
        assert!(validate_create_action_args(&args).is_ok());
    }

    #[test]
    fn test_validate_max_possible_satoshis_allowed() {
        // MAX_POSSIBLE_SATOSHIS is a special sentinel value that should be allowed
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Valid description".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: MAX_POSSIBLE_SATOSHIS,
                output_description: "Max possible output".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        assert!(validate_create_action_args(&args).is_ok());
    }

    #[test]
    fn test_validate_input_missing_unlocking_script() {
        use bsv_sdk::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint { txid: txid_arr, vout: 0 },
                input_description: "Test input".to_string(),
                unlocking_script: None,
                unlocking_script_length: None,
                sequence_number: None,
            }]),
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("unlockingScript or unlockingScriptLength required"));
    }

    #[test]
    fn test_validate_input_unlocking_script_length_mismatch() {
        use bsv_sdk::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint { txid: txid_arr, vout: 0 },
                input_description: "Test input".to_string(),
                unlocking_script: Some(vec![0x00]), // 1 byte
                unlocking_script_length: Some(2),    // but says 2
                sequence_number: None,
            }]),
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("unlocking script length mismatch"));
    }

    #[test]
    fn test_validate_duplicate_input_outpoints() {
        use bsv_sdk::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![
                CreateActionInput {
                    outpoint: Outpoint { txid: txid_arr, vout: 0 },
                    input_description: "Input 1".to_string(),
                    unlocking_script: Some(vec![0x00]),
                    unlocking_script_length: None,
                    sequence_number: None,
                },
                CreateActionInput {
                    outpoint: Outpoint { txid: txid_arr, vout: 0 }, // Same outpoint
                    input_description: "Input 2".to_string(),
                    unlocking_script: Some(vec![0x00]),
                    unlocking_script_length: None,
                    sequence_number: None,
                },
            ]),
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        let err = validate_create_action_args(&args).unwrap_err();
        assert!(err.to_string().contains("duplicate outpoint"));
    }

    #[test]
    fn test_validate_valid_input() {
        use bsv_sdk::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6").unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint { txid: txid_arr, vout: 0 },
                input_description: "Valid input".to_string(),
                unlocking_script: None,
                unlocking_script_length: Some(107), // P2PKH unlocking script length
                sequence_number: None,
            }]),
            outputs: None,
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };
        assert!(validate_create_action_args(&args).is_ok());
    }

    // =========================================================================
    // Integration tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_action_basic() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        // Create a user
        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        // Create a simple action with one output
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: locking_script.clone(),
                satoshis: 42000,
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

        let result = create_action_internal(&storage, user.user_id, args).await;

        // Should fail due to insufficient funds (no change inputs available)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Insufficient funds"));
    }

    #[tokio::test]
    async fn test_create_action_with_labels() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        // First, seed the wallet with some change
        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction with labels".to_string(),
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
            labels: Some(vec!["payment".to_string(), "test".to_string()]),
            options: None,
        };

        let result = create_action_internal(&storage, user.user_id, args).await.unwrap();

        assert!(!result.reference.is_empty());
        assert_eq!(result.version, 1);
        assert_eq!(result.lock_time, 0);
        assert!(!result.outputs.is_empty());
        assert!(!result.derivation_prefix.is_empty());
    }

    #[tokio::test]
    async fn test_create_action_with_tags_and_basket() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test transaction with basket and tags".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script,
                satoshis: 1000,
                output_description: "Test output".to_string(),
                basket: Some("payments".to_string()),
                custom_instructions: Some("{\"type\":\"custom\"}".to_string()),
                tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            }]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, user.user_id, args).await.unwrap();

        // Verify the output has the basket and tags
        assert!(!result.outputs.is_empty());
        let first_output = &result.outputs[0];
        assert_eq!(first_output.basket, Some("payments".to_string()));
        assert_eq!(first_output.tags, vec!["tag1".to_string(), "tag2".to_string()]);
        assert_eq!(first_output.custom_instructions, Some("{\"type\":\"custom\"}".to_string()));
    }

    #[tokio::test]
    async fn test_create_action_no_send() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test noSend transaction".to_string(),
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
            options: Some(CreateActionOptions {
                no_send: Some(true),
                sign_and_process: Some(false),
                accept_delayed_broadcast: None,
                trust_self: None,
                return_txid_only: None,
                known_txids: None,
                no_send_change: None,
                send_with: None,
                randomize_outputs: None,
            }),
        };

        let result = create_action_internal(&storage, user.user_id, args).await.unwrap();

        // For noSend, we should get change vouts
        assert!(result.no_send_change_output_vouts.is_some());
    }

    #[tokio::test]
    async fn test_create_action_multiple_outputs() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        seed_change_output(&storage, user.user_id, 200_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Multiple outputs transaction".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![
                CreateActionOutput {
                    locking_script: locking_script.clone(),
                    satoshis: 1000,
                    output_description: "Output 1".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                },
                CreateActionOutput {
                    locking_script: locking_script.clone(),
                    satoshis: 2000,
                    output_description: "Output 2".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                },
                CreateActionOutput {
                    locking_script,
                    satoshis: 3000,
                    output_description: "Output 3".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                },
            ]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, user.user_id, args).await.unwrap();

        // Should have at least 3 outputs (user outputs) + change
        assert!(result.outputs.len() >= 3);

        // Verify first 3 outputs are the user's
        assert_eq!(result.outputs[0].satoshis, 1000);
        assert_eq!(result.outputs[1].satoshis, 2000);
        assert_eq!(result.outputs[2].satoshis, 3000);
    }

    #[tokio::test]
    async fn test_create_action_with_version_and_locktime() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02user_identity_key").await.unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_sdk::wallet::CreateActionArgs {
            description: "Test with version/locktime".to_string(),
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
            lock_time: Some(500000),
            version: Some(2),
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, user.user_id, args).await.unwrap();

        assert_eq!(result.version, 2);
        assert_eq!(result.lock_time, 500000);
    }

    // Helper function to seed a change output for testing
    async fn seed_change_output(storage: &StorageSqlx, user_id: i64, satoshis: i64) {
        let now = Utc::now();
        let basket = storage.find_or_create_default_basket(user_id).await.unwrap();

        // Create a fake completed transaction
        let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at)
            VALUES (?, 'completed', 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(satoshis)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let transaction_id = tx_result.last_insert_rowid();

        // Create a change output
        let locking_script = hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, derivation_prefix, derivation_suffix,
                provided_by, purpose, output_description, created_at, updated_at
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'seeded change', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(basket.basket_id)
        .bind(satoshis)
        .bind(&locking_script)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }
}

