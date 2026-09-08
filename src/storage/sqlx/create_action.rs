//! Create Action Implementation
//!
//! This module contains the full implementation of the `create_action` method
//! for the `StorageSqlx` wallet storage backend.

use crate::error::{Error, Result};
use crate::services::traits::WalletServices;
use crate::storage::entities::{TableOutput, TableOutputBasket, TableOutputTag, TableTxLabel};
use crate::storage::traits::{
    FindOutputBasketsArgs, StorageCreateActionResult, StorageCreateTransactionInput,
    StorageCreateTransactionOutput, StorageProvidedBy, WalletStorageReader,
};
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use chrono::Utc;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;
use std::collections::HashSet;

use super::storage_sqlx::{
    bump_leaf_index, store_validated_proof_on, StoreProofOutcome, ValidatedProofRow,
};
use super::StorageSqlx;

// =============================================================================
// Constants
// =============================================================================

/// Maximum satoshi value (total BTC supply in satoshis).
const MAX_SATOSHIS: u64 = 2_100_000_000_000_000;

/// How many merkle paths one BEEF assembly may fetch from services.
///
/// The ancestor walk runs newest-first and stops at the first proven
/// ancestor, so in practice one or two fetches settle a chain. The budget
/// only caps the pathological case where nothing on the chain is mined yet.
const MAX_PROOF_FETCHES_PER_WALK: usize = 8;

/// Do not ask the chain for a proof of a transaction this wallet only just
/// recorded. Mining is monotonic along a chain (an ancestor is older than
/// its descendant), so skipping a young ancestor costs nothing: if it were
/// mined, the next hop back is mined too and answers there instead.
const MIN_AGE_FOR_PROOF_LOOKUP_SECS: i64 = 90;

/// Special satoshi value indicating "use maximum possible".
const MAX_POSSIBLE_SATOSHIS: u64 = 2_099_999_999_999_999;

/// Default fee rate in satoshis per kilobyte.
const DEFAULT_FEE_RATE_SAT_PER_KB: u64 = 101;

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
pub(super) struct AllocatedChangeInput {
    #[allow(dead_code)]
    output_id: i64,
    satoshis: u64,
    txid: String,
    vout: i32,
    locking_script: Vec<u8>,
    derivation_prefix: Option<String>,
    derivation_suffix: Option<String>,
    sender_identity_key: Option<String>,
}

/// A generated change output.
#[derive(Debug, Clone)]
struct ChangeOutput {
    satoshis: u64,
    #[allow(dead_code)]
    vout: u32,
    derivation_prefix: String,
    derivation_suffix: String,
}

// =============================================================================
// Validation
// =============================================================================

/// Validates create action arguments.
fn validate_create_action_args(args: &bsv_rs::wallet::CreateActionArgs) -> Result<()> {
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
fn calculate_transaction_size(input_script_lengths: &[u32], output_script_lengths: &[u32]) -> u64 {
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
///
/// # Arguments
/// * `storage` - The storage backend
/// * `chain_tracker` - Optional chain tracker for BEEF verification. If None, skips verification.
/// * `user_id` - The user ID
/// * `args` - The create action arguments
///
/// Note: Change outputs are created with empty locking scripts. The locking scripts
/// are derived during transaction signing and stored via process_action when the
/// signed transaction is processed.
pub async fn create_action_internal(
    storage: &StorageSqlx,
    chain_tracker: Option<&dyn ChainTracker>,
    user_id: i64,
    args: bsv_rs::wallet::CreateActionArgs,
) -> Result<StorageCreateActionResult> {
    // Step 1: Validate all inputs
    validate_create_action_args(&args)?;

    // Determine action flags
    let options = args.options.as_ref();
    let is_no_send = options.and_then(|o| o.no_send).unwrap_or(false);
    let is_delayed = options
        .and_then(|o| o.accept_delayed_broadcast)
        .unwrap_or(false);

    // Step 2: Get or create default output basket
    // NOTE: This uses its own pool connection (known limitation for first pass).
    let change_basket = storage.find_or_create_default_basket(user_id).await?;

    // Begin SQL transaction to ensure atomicity of all subsequent DB writes.
    // If the function returns an error (via `?`), `tx` is dropped and sqlx auto-rollbacks.
    let mut tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Step 3: Process caller-provided outputs
    let extended_outputs = validate_and_extend_outputs(&args)?;

    // Step 4: Process caller-provided inputs
    let extended_inputs = validate_and_extend_inputs(storage, &mut tx, user_id, &args).await?;

    // Step 5: Count available change outputs for targeting
    let available_change_count =
        count_change_inputs(&mut tx, user_id, change_basket.basket_id, !is_delayed).await?;

    // Step 6: Create transaction record in DB
    let transaction_id = create_transaction_record(&mut tx, user_id, &args).await?;

    // Step 7: Create transaction labels
    if let Some(ref labels) = args.labels {
        for label in labels {
            let tx_label = find_or_insert_tx_label(&mut tx, user_id, label).await?;
            find_or_insert_tx_label_map(&mut tx, transaction_id, tx_label.label_id).await?;
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

    // A caller-complete transaction — every provided input already carries a full
    // unlocking script — is decided by two questions (see `caller_complete_plan`):
    //
    // 1. Is it SELF-SUFFICIENT (inputs cover outputs plus the fee the caller
    //    fixed as inputs − outputs)? Then it is byte-frozen and MUST be broadcast
    //    verbatim: no funding input, no change, no re-signing (the signer already
    //    skips inputs that carry an unlocking script). This is the b23a960 path —
    //    a SIGHASH_ALL multi-party covenant spend, an OP_PUSH_TX commit/settle —
    //    where appending anything invalidates every provided signature
    //    (consensus NULLFAIL, ARC 461) and breaks the covenant preimage.
    // 2. Otherwise, do the provided SIGNATURES permit funding? An input signed
    //    ANYONECANPAY|SINGLE (0xc3) or ANYONECANPAY|NONE (0xc2) commits only to
    //    its own input (and, for SINGLE, its same-index output) — precisely so a
    //    wallet can ADD a funding input and a change output afterwards. That is
    //    the Zanaadu V3 publish shape (one covenant input signed 0xc3, 1 sat in,
    //    1,001 sats out: the wallet pays the 1,000-sat platform fee plus the
    //    miner fee), and it is what MetaNet's TS toolbox does with it. When every
    //    provided input is such an input (or carries no signature at all), the
    //    shortfall is funded by `generate_change`, which only APPENDS: provided
    //    inputs keep their indices and order, provided outputs keep theirs, so
    //    every own-index commitment still verifies. A SIGHASH_ALL input
    //    (0x41/0x43, or ALL|ANYONECANPAY 0xc1 whose output set is sealed) can
    //    never be funded — adding an input or an output breaks it — so a
    //    shortfall there stays a hard InsufficientFunds, exactly as before.
    let caller_complete = args
        .inputs
        .as_ref()
        .map(|ins| !ins.is_empty() && ins.iter().all(|i| i.unlocking_script.is_some()))
        .unwrap_or(false);

    let change_result = if caller_complete {
        let in_sats: u64 = extended_inputs.iter().map(|i| i.satoshis).sum();
        let out_sats: u64 = extended_outputs.iter().map(|o| o.satoshis).sum();
        let fundable = caller_inputs_permit_funding(args.inputs.as_deref().unwrap_or(&[]));
        let fee_required = caller_fixed_fee_required(&params);
        match caller_complete_plan(in_sats, out_sats, fee_required, fundable) {
            CallerCompletePlan::Verbatim => {
                // Inputs - outputs is the (caller-fixed) miner fee. No change.
                tracing::debug!(
                    in_sats,
                    out_sats,
                    fundable,
                    "caller-complete tx is self-sufficient — broadcasting verbatim (no funding, no change)"
                );
                GenerateChangeResult {
                    allocated_change_inputs: Vec::new(),
                    change_outputs: Vec::new(),
                }
            }
            CallerCompletePlan::Unfundable => {
                tracing::warn!(
                    in_sats,
                    out_sats,
                    "caller-complete tx has a shortfall but every provided input is signed \
                     SIGHASH_ALL (no ANYONECANPAY): the wallet cannot add a funding input \
                     without invalidating the caller's signatures"
                );
                return Err(Error::InsufficientFunds {
                    needed: out_sats,
                    available: in_sats,
                });
            }
            CallerCompletePlan::Fund => {
                tracing::debug!(
                    in_sats,
                    out_sats,
                    fee_required,
                    "caller-complete tx signed ANYONECANPAY has a shortfall — appending a funding input and change"
                );
                generate_change(
                    &mut tx,
                    user_id,
                    change_basket.basket_id,
                    transaction_id,
                    &params,
                    &derivation_prefix,
                    is_delayed,
                )
                .await?
            }
        }
    } else {
        generate_change(
            &mut tx,
            user_id,
            change_basket.basket_id,
            transaction_id,
            &params,
            &derivation_prefix,
            is_delayed,
        )
        .await?
    };

    // Step 9: Mark extended inputs as spent
    for input in &extended_inputs {
        if let Some(ref output) = input.output {
            update_output_spent(
                &mut tx,
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
    update_transaction_satoshis(&mut tx, transaction_id, satoshis).await?;

    // Step 11: Create output records
    let mut result_outputs = Vec::new();
    let mut change_vouts = Vec::new();

    // First, handle user-specified outputs
    for xo in &extended_outputs {
        let basket_id = if let Some(ref basket_name) = xo.basket {
            let basket =
                find_or_insert_output_basket(storage, &mut tx, user_id, basket_name).await?;
            Some(basket.basket_id)
        } else {
            None
        };

        let output_id = insert_output(
            &mut tx,
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
            // spendable = true is CORRECT and INTENTIONAL — do NOT "fix" it.
            // createAction records every output it builds, INCLUDING payments to
            // third parties, with spendable = true. This matches the reference
            // (ts-stack wallet-toolbox createAction.ts makeDefaultOutput defaults
            // spendable: true; the user-output branch does not override it).
            // `spendable = true` does NOT mean "this wallet can spend it" —
            // that is gated by the BASKET (coin selection is basket-scoped;
            // a basket_id = NULL payment is never selected). A usable balance is
            // Σ spendable in the change/default basket, never SUM(spendable=1).
            // See docs/reading-a-balance.md (bsv-wallet-cli). Changing this to a
            // basket-conditional would DIVERGE from the reference — a parity bug.
            true, // spendable (see the note above before touching this)
        )
        .await?;

        // Create tag associations
        for tag in &xo.tags {
            let output_tag = find_or_insert_output_tag(&mut tx, user_id, tag).await?;
            find_or_insert_output_tag_map(&mut tx, output_id, output_tag.tag_id).await?;
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
    // Note: Change outputs are created with empty locking scripts here.
    // The locking scripts are derived during transaction signing and stored
    // in process_action when the signed transaction is processed.
    let base_vout = extended_outputs.len() as u32;
    for (i, co) in change_result.change_outputs.iter().enumerate() {
        let vout = base_vout + i as u32;

        let _output_id = insert_output(
            &mut tx,
            user_id,
            transaction_id,
            Some(change_basket.basket_id),
            vout as i32,
            co.satoshis as i64,
            &[], // Locking script stored in process_action after signing
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
            locking_script: String::new(), // Filled in by process_action
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
            sender_identity_key: aci.sender_identity_key.clone(),
        });
    }

    // Get the transaction reference
    let reference = get_transaction_reference(&mut tx, transaction_id).await?;

    // Extract BEEF-related options
    let return_txid_only = options.and_then(|o| o.return_txid_only).unwrap_or(false);
    let known_txids: Vec<String> = options
        .and_then(|o| o.known_txids.as_ref())
        .map(|txids| txids.iter().map(hex::encode).collect())
        .unwrap_or_default();

    // Build input BEEF containing all input transactions with their merkle proofs
    // Verify BEEF against ChainTracker if provided (matches TypeScript/Go behavior)
    let input_beef = build_input_beef(
        &mut tx,
        chain_tracker,
        &extended_inputs,
        &change_result.allocated_change_inputs,
        args.input_beef.as_deref(),
        &known_txids,
        return_txid_only,
        Some(storage),
    )
    .await?;

    // Store input_beef in the transaction record (required for process_action)
    // This matches the TypeScript behavior which stores inputBEEF at transaction creation
    if let Some(ref beef_bytes) = input_beef {
        let now = Utc::now();
        sqlx::query(
            "UPDATE transactions SET input_beef = ?, updated_at = ? WHERE transaction_id = ?",
        )
        .bind(beef_bytes)
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *tx)
        .await?;
    }

    // Commit all changes atomically.
    tx.commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Build final result
    Ok(StorageCreateActionResult {
        reference,
        version: args.version.unwrap_or(1),
        lock_time: args.lock_time.unwrap_or(0),
        inputs: result_inputs,
        outputs: result_outputs,
        derivation_prefix,
        input_beef,
        no_send_change_output_vouts: if is_no_send { Some(change_vouts) } else { None },
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Validates and extends output specifications.
fn validate_and_extend_outputs(
    args: &bsv_rs::wallet::CreateActionArgs,
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
/// For external inputs not in storage, looks up source output info from the input_beef.
async fn validate_and_extend_inputs(
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
    conn: &mut SqliteConnection,
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
        .fetch_one(&mut *conn)
        .await?;

    let count: i64 = row.get("count");
    Ok(count as usize)
}

/// Creates a new transaction record.
async fn create_transaction_record(
    conn: &mut SqliteConnection,
    user_id: i64,
    args: &bsv_rs::wallet::CreateActionArgs,
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
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
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

/// Gets the reference for a transaction.
async fn get_transaction_reference(
    conn: &mut SqliteConnection,
    transaction_id: i64,
) -> Result<String> {
    let row = sqlx::query("SELECT reference FROM transactions WHERE transaction_id = ?")
        .bind(transaction_id)
        .fetch_one(&mut *conn)
        .await?;

    Ok(row.get("reference"))
}

/// Updates transaction satoshis.
async fn update_transaction_satoshis(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    satoshis: i64,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query("UPDATE transactions SET satoshis = ?, updated_at = ? WHERE transaction_id = ?")
        .bind(satoshis)
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Marks an output as spent.
async fn update_output_spent(
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

/// Finds or creates a transaction label.
async fn find_or_insert_tx_label(
    conn: &mut SqliteConnection,
    user_id: i64,
    label: &str,
) -> Result<TableTxLabel> {
    let row = sqlx::query(
        "SELECT tx_label_id, user_id, label, created_at, updated_at FROM tx_labels WHERE user_id = ? AND label = ?",
    )
    .bind(user_id)
    .bind(label)
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
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
    conn: &mut SqliteConnection,
    transaction_id: i64,
    label_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT tx_label_map_id FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?",
    )
    .bind(transaction_id)
    .bind(label_id)
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Finds or creates an output basket.
pub(super) async fn find_or_insert_output_basket(
    storage: &StorageSqlx,
    conn: &mut SqliteConnection,
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
    .execute(&mut *conn)
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
    conn: &mut SqliteConnection,
    user_id: i64,
    tag: &str,
) -> Result<TableOutputTag> {
    let row = sqlx::query(
        "SELECT output_tag_id, user_id, tag, created_at, updated_at FROM output_tags WHERE user_id = ? AND tag = ?",
    )
    .bind(user_id)
    .bind(tag)
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
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
    conn: &mut SqliteConnection,
    output_id: i64,
    tag_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT output_tag_map_id FROM output_tags_map WHERE output_id = ? AND output_tag_id = ?",
    )
    .bind(output_id)
    .bind(tag_id)
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Inserts a new output record.
#[allow(clippy::too_many_arguments)]
async fn insert_output(
    conn: &mut SqliteConnection,
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
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

// =============================================================================
// Caller-complete funding rule
// =============================================================================

/// What `create_action` does with a caller-complete transaction (every provided
/// input already carries a full unlocking script).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallerCompletePlan {
    /// Self-sufficient: broadcast the caller's bytes verbatim. No funding input,
    /// no change output. `inputs − outputs` is the miner fee the caller fixed.
    Verbatim,
    /// Short of funds, but every provided signature permits the wallet to append
    /// inputs and outputs: run `generate_change` to fund the shortfall (and the
    /// miner fee) and to collect change. Provided inputs and outputs keep their
    /// indices and order.
    Fund,
    /// Short of funds and at least one provided signature commits to the whole
    /// transaction: appending anything would invalidate it, so the shortfall is
    /// a hard `InsufficientFunds`.
    Unfundable,
}

/// The rule, as a pure function.
///
/// * `fee_required` is the wallet's own fee for the transaction AS PROVIDED
///   (provided inputs and outputs only), so a fundable transaction whose surplus
///   is below it gets a funding input instead of a "fee too low" rejection.
/// * A transaction whose signatures forbid funding is self-sufficient whenever
///   inputs cover outputs: the fee is the caller's to fix, not ours to judge.
fn caller_complete_plan(
    in_sats: u64,
    out_sats: u64,
    fee_required: u64,
    fundable: bool,
) -> CallerCompletePlan {
    let covers_outputs = in_sats >= out_sats;
    let self_sufficient = covers_outputs && (!fundable || in_sats - out_sats >= fee_required);
    if self_sufficient {
        CallerCompletePlan::Verbatim
    } else if fundable {
        CallerCompletePlan::Fund
    } else {
        CallerCompletePlan::Unfundable
    }
}

/// The miner fee the wallet would require for the transaction exactly as the
/// caller provided it (its fixed inputs and outputs only, no change).
fn caller_fixed_fee_required(params: &GenerateChangeParams) -> u64 {
    let input_script_lengths: Vec<u32> = params
        .fixed_inputs
        .iter()
        .map(|i| i.unlocking_script_length)
        .collect();
    let output_script_lengths: Vec<u32> = params
        .fixed_outputs
        .iter()
        .map(|o| o.locking_script_length)
        .collect();
    let size = calculate_transaction_size(&input_script_lengths, &output_script_lengths);
    (size * params.fee_rate).div_ceil(1000)
}

/// Whether every caller-provided input's signatures allow the wallet to append
/// its own funding inputs and change outputs. Empty input lists are not
/// caller-complete and are never "fundable" through this path.
fn caller_inputs_permit_funding(inputs: &[bsv_rs::wallet::CreateActionInput]) -> bool {
    !inputs.is_empty()
        && inputs.iter().all(|input| {
            input
                .unlocking_script
                .as_deref()
                .map(|script| input_commitment(script) != InputCommitment::WholeTx)
                .unwrap_or(false)
        })
}

/// Sighash flag bits of the byte appended to every BSV signature.
const SIGHASH_BASE_MASK: u8 = 0x1f;
const SIGHASH_ALL: u8 = 0x01;
const SIGHASH_NONE: u8 = 0x02;
const SIGHASH_SINGLE: u8 = 0x03;
const SIGHASH_ANYONECANPAY: u8 = 0x80;

/// What the signatures in a caller-provided unlocking script commit to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputCommitment {
    /// A signature commits to the whole transaction: SIGHASH_ALL without
    /// ANYONECANPAY commits to every input and output; ALL|ANYONECANPAY commits
    /// to every output. Appending a funding input or a change output breaks it.
    WholeTx,
    /// Every signature carries ANYONECANPAY with base NONE or SINGLE: it commits
    /// only to its own input (and, for SINGLE, the output at its own index).
    /// Inputs and outputs appended AFTER the provided ones leave it valid.
    OwnOnly,
    /// No signature in the script (a hash puzzle, OP_TRUE, bare data): nothing
    /// constrains the transaction shape.
    Unconstrained,
}

/// Classify a caller-provided unlocking script by the sighash flags of the DER
/// signatures it pushes. Position-independent, so it covers P2PKH
/// (`<sig> <pubkey>`), multisig and OP_PUSH_TX covenant unlocks alike.
fn input_commitment(unlocking_script: &[u8]) -> InputCommitment {
    let mut saw_signature = false;
    for push in script_pushes(unlocking_script) {
        let Some(sighash) = der_signature_sighash(push) else {
            continue;
        };
        saw_signature = true;
        let anyone_can_pay = sighash & SIGHASH_ANYONECANPAY != 0;
        if !anyone_can_pay || sighash & SIGHASH_BASE_MASK == SIGHASH_ALL {
            return InputCommitment::WholeTx;
        }
    }
    if saw_signature {
        InputCommitment::OwnOnly
    } else {
        InputCommitment::Unconstrained
    }
}

/// The data pushes of a script, in order: direct pushes (0x01..0x4b) and
/// OP_PUSHDATA1/2/4. Every other opcode carries no payload and is skipped.
/// Stops at the first push that runs past the end of the script.
fn script_pushes(script: &[u8]) -> Vec<&[u8]> {
    let mut pushes = Vec::new();
    let mut i = 0usize;
    while i < script.len() {
        let op = script[i];
        i += 1;
        let len = match op {
            0x01..=0x4b => op as usize,
            0x4c => {
                let Some(&n) = script.get(i) else { break };
                i += 1;
                n as usize
            }
            0x4d => {
                let Some(bytes) = script.get(i..i + 2) else {
                    break;
                };
                i += 2;
                u16::from_le_bytes([bytes[0], bytes[1]]) as usize
            }
            0x4e => {
                let Some(bytes) = script.get(i..i + 4) else {
                    break;
                };
                i += 4;
                u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
            }
            _ => continue,
        };
        let Some(data) = script.get(i..i + len) else {
            break;
        };
        pushes.push(data);
        i += len;
    }
    pushes
}

/// If `data` is `<DER signature><sighash byte>` (`0x30 len 0x02 rlen r 0x02
/// slen s flags` with a valid base type), return the sighash byte.
fn der_signature_sighash(data: &[u8]) -> Option<u8> {
    let n = data.len();
    // 8-byte minimal DER body (1-byte r and s) .. 72-byte maximal (33 + 33), plus flags.
    if !(9..=73).contains(&n) || data[0] != 0x30 || data[1] as usize != n - 3 || data[2] != 0x02 {
        return None;
    }
    let r_len = data[3] as usize;
    let s_tag = 4 + r_len;
    if s_tag + 1 >= n - 1 || data[s_tag] != 0x02 {
        return None;
    }
    let s_len = data[s_tag + 1] as usize;
    if s_tag + 2 + s_len != n - 1 {
        return None;
    }
    let flags = data[n - 1];
    match flags & SIGHASH_BASE_MASK {
        SIGHASH_ALL | SIGHASH_NONE | SIGHASH_SINGLE => Some(flags),
        _ => None,
    }
}

// =============================================================================
// Change Generation
// =============================================================================

/// Generates change outputs and allocates change inputs to fund the transaction.
///
/// This follows the TypeScript logic:
/// 1. Create change outputs based on targetNetCount first (even if underfunded)
/// 2. Allocate inputs to fund the transaction
/// 3. If can't fund, remove change outputs one at a time
/// 4. Distribute excess to change outputs
async fn generate_change(
    conn: &mut SqliteConnection,
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
            .chain(std::iter::repeat_n(
                params.change_unlocking_script_length,
                alloc_inputs.len(),
            ))
            .collect();

        let output_script_lengths: Vec<u32> = params
            .fixed_outputs
            .iter()
            .map(|o| o.locking_script_length)
            .chain(std::iter::repeat_n(
                params.change_locking_script_length,
                change_outs.len(),
            ))
            .collect();

        let size = calculate_transaction_size(&input_script_lengths, &output_script_lengths);
        let fee_required = (size * params.fee_rate).div_ceil(1000); // Ceiling division

        let fee_excess = input_sats as i64 - output_sats as i64 - fee_required as i64;

        (input_sats, output_sats, fee_required, fee_excess)
    };

    let target_net = params.target_net_count.unwrap_or(0);
    let has_target_net_count = params.target_net_count.is_some();

    // Calculate current net change (outputs created - inputs consumed)
    let net_change_count = |outputs: &[ChangeOutput], inputs: &[AllocatedChangeInput]| -> i32 {
        outputs.len() as i32 - inputs.len() as i32
    };

    // Initial state calculation
    let (_, _, _, mut fee_excess) = calculate_state(&allocated_inputs, &change_outputs);

    // Step 1: Create change outputs based on targetNetCount first
    // "If we'd like to have more change outputs create them now.
    //  They may be removed if it turns out we can't fund them."
    while (has_target_net_count
        && target_net > net_change_count(&change_outputs, &allocated_inputs))
        || (change_outputs.is_empty() && fee_excess > 0)
    {
        let satoshis = if change_outputs.is_empty() {
            params.change_first_satoshis
        } else {
            params.change_initial_satoshis
        };

        change_outputs.push(ChangeOutput {
            satoshis,
            vout: (params.fixed_outputs.len() + change_outputs.len()) as u32,
            derivation_prefix: derivation_prefix.to_string(),
            derivation_suffix: random_derivation(16),
        });

        // Recalculate fee_excess with new output
        let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
        fee_excess = new_excess;
    }

    // Step 2: Fund the transaction (starvation loop with funding loop)
    // This is the outer "for (;;)" loop in TypeScript
    #[allow(clippy::never_loop)]
    loop {
        // Release all allocated inputs (TypeScript: releaseAllocatedChangeInputs)
        for input in allocated_inputs.drain(..) {
            release_change_input(&mut *conn, input.output_id).await?;
        }

        // Recalculate after releasing
        let (_, _, _, initial_excess) = calculate_state(&allocated_inputs, &change_outputs);
        fee_excess = initial_excess;

        // Funding loop: add one change input at a time
        while fee_excess < 0 {
            // Check if we should add an output to balance a new input
            let add_output = has_target_net_count
                && (net_change_count(&change_outputs, &allocated_inputs) - 1) < target_net;

            let extra_for_output = if add_output {
                2 * params.change_initial_satoshis
            } else {
                0
            };
            let target_sats = (-fee_excess) as u64 + extra_for_output;

            let allocated = allocate_change_input(
                &mut *conn,
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

                // If we have excess and should add output (or need at least one)
                if fee_excess > 0 && (add_output || change_outputs.is_empty()) {
                    let satoshis = std::cmp::min(
                        fee_excess as u64,
                        if change_outputs.is_empty() {
                            params.change_first_satoshis
                        } else {
                            params.change_initial_satoshis
                        },
                    );
                    change_outputs.push(ChangeOutput {
                        satoshis,
                        vout: (params.fixed_outputs.len() + change_outputs.len()) as u32,
                        derivation_prefix: derivation_prefix.to_string(),
                        derivation_suffix: random_derivation(16),
                    });
                    let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
                    fee_excess = new_excess;
                }
            } else {
                // No more change inputs available
                break;
            }
        }

        // Check if we're done (balanced/overbalanced or impossible)
        if fee_excess >= 0 || change_outputs.is_empty() {
            break;
        }

        // Remove change outputs one at a time (starvation)
        while !change_outputs.is_empty() && fee_excess < 0 {
            change_outputs.pop();
            let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
            fee_excess = new_excess;
        }

        if fee_excess < 0 {
            // Not enough available funding even with no change outputs
            break;
        }

        // Starvation removed all desired change outputs but we have excess sats.
        // Try to create a single change output to capture the excess instead of
        // donating it all to miners. The change output itself costs extra bytes,
        // so verify we still have a positive excess after adding it.
        if change_outputs.is_empty() && fee_excess > 0 {
            change_outputs.push(ChangeOutput {
                satoshis: 1, // Minimum; Step 3 will distribute the real excess
                vout: params.fixed_outputs.len() as u32,
                derivation_prefix: derivation_prefix.to_string(),
                derivation_suffix: random_derivation(16),
            });
            let (_, _, _, new_excess) = calculate_state(&allocated_inputs, &change_outputs);
            if new_excess >= 0 {
                fee_excess = new_excess;
            } else {
                // Can't afford a change output; excess goes to miners
                change_outputs.pop();
            }
        }

        break;
    }

    // Check if we still can't fund the transaction
    if fee_excess < 0 {
        let (input_sats, output_sats, fee_required, _) =
            calculate_state(&allocated_inputs, &change_outputs);

        // Check if it's because we need more change outputs to capture excess
        if change_outputs.is_empty() && fee_excess > 0 {
            return Err(Error::InsufficientFunds {
                needed: output_sats + fee_required + params.change_first_satoshis,
                available: input_sats,
            });
        }

        return Err(Error::InsufficientFunds {
            needed: output_sats + fee_required,
            available: input_sats,
        });
    }

    // Step 3: Distribute excess fee across change outputs
    while !change_outputs.is_empty() && fee_excess > 0 {
        if change_outputs.len() == 1 {
            // Give all excess to the single change output
            change_outputs[0].satoshis += fee_excess as u64;
            fee_excess = 0;
        } else if change_outputs[0].satoshis < params.change_initial_satoshis {
            // Fill first output up to initial amount
            let add = std::cmp::min(
                fee_excess as u64,
                params.change_initial_satoshis - change_outputs[0].satoshis,
            );
            change_outputs[0].satoshis += add;
            fee_excess -= add as i64;
        } else {
            // Distribute randomly (simplified: just add to first output)
            let add = std::cmp::max(1, fee_excess / 2);
            change_outputs[0].satoshis += add as u64;
            fee_excess -= add;
        }
    }

    Ok(GenerateChangeResult {
        allocated_change_inputs: allocated_inputs,
        change_outputs,
    })
}

/// Releases a previously allocated change input.
async fn release_change_input(conn: &mut SqliteConnection, output_id: i64) -> Result<()> {
    sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL WHERE output_id = ?")
        .bind(output_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Allocates a change input from the default basket.
pub(super) async fn allocate_change_input(
    conn: &mut SqliteConnection,
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
               o.derivation_prefix, o.derivation_suffix, o.sender_identity_key
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
            CASE WHEN t.status = 'completed' THEN 0 ELSE 1 END,
            LENGTH(COALESCE(t.input_beef, X'')) ASC,
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
        .fetch_optional(&mut *conn)
        .await?;

    if let Some(row) = row {
        let output_id: i64 = row.get("output_id");
        let satoshis: i64 = row.get("satoshis");
        let txid: String = row.get("txid");
        let vout: i32 = row.get("vout");
        let locking_script: Option<Vec<u8>> = row.get("locking_script");
        let derivation_prefix: Option<String> = row.get("derivation_prefix");
        let derivation_suffix: Option<String> = row.get("derivation_suffix");
        let sender_identity_key: Option<String> = row.get("sender_identity_key");

        // Mark as allocated (spent_by this transaction)
        let now = Utc::now();
        sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
        )
        .bind(transaction_id)
        .bind(now)
        .bind(output_id)
        .execute(&mut *conn)
        .await?;

        Ok(Some(AllocatedChangeInput {
            output_id,
            satoshis: satoshis as u64,
            txid,
            vout,
            locking_script: locking_script.unwrap_or_default(),
            derivation_prefix,
            derivation_suffix,
            sender_identity_key,
        }))
    } else {
        Ok(None)
    }
}

// =============================================================================
// BEEF Construction
// =============================================================================

/// Maximum recursion depth for ancestor fetching to prevent infinite loops.
/// Matches the TypeScript reference implementation (maxRecursionDepth = 12).
pub(super) const MAX_BEEF_RECURSION_DEPTH: usize = 12;

/// Data for a transaction to include in BEEF.
pub(super) struct BeefTxData {
    pub(super) raw_tx: Vec<u8>,
    pub(super) merkle_path: Option<Vec<u8>>,
}

/// Validate a stored BEEF's merkle proofs against ChainTracker.
///
/// Returns `true` if the stored BEEF is valid (safe to merge), `false` if any
/// merkle root is invalid or the BEEF structure is corrupt. Invalid BEEFs are
/// discarded so the caller can fall through to individual tx+proof lookup and
/// network fallback paths.
///
/// Matches Go's `VerifyBeef()` call in `create_process_inputs.go:144-150`.
async fn validate_stored_beef(
    stored_beef: &mut Beef,
    tracker: &dyn ChainTracker,
    txid: &str,
) -> bool {
    if stored_beef.bumps.is_empty() {
        // No bumps to verify — stored BEEF is just unproven txs, safe to merge
        return true;
    }

    let validation = stored_beef.verify_valid(true);
    if !validation.valid {
        tracing::warn!(
            txid = %txid,
            detail = %describe_invalid_beef(stored_beef, &[txid.to_string()]),
            "Discarding stored input_beef: BEEF structure is invalid. \
             Will fall through to individual tx lookup / network fallback."
        );
        return false;
    }

    for (height, root) in &validation.roots {
        match tracker.is_valid_root_for_height(root, *height).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    txid = %txid,
                    height = %height,
                    root = %root,
                    "Discarding stored input_beef: invalid merkle root at height {}. \
                     Will fall through to individual tx lookup / network fallback.",
                    height,
                );
                return false;
            }
            Err(e) => {
                tracing::warn!(
                    txid = %txid,
                    height = %height,
                    error = %e,
                    "Discarding stored input_beef: ChainTracker error at height {}. \
                     Will fall through to fallback.",
                    height,
                );
                return false;
            }
        }
    }

    true
}

/// Mark the level-0 leaf whose hash is `txid` as a txid leaf. A BUMP merged
/// from a sibling's proof carries our transaction as a plain hash; flagging
/// it is what lets the BEEF's own linker and validator treat it as proven.
pub(super) fn flag_txid_leaf(bump: &mut MerklePath, txid: &str) {
    if let Some(level0) = bump.path.first_mut() {
        for leaf in level0.iter_mut() {
            if leaf.hash.as_deref() == Some(txid) {
                leaf.txid = true;
            }
        }
    }
}

/// Why a BEEF failed structural validation, in one line, and the rejected
/// bytes on disk. "BEEF structure is invalid" alone cost a seat a day of
/// diagnosis (LOW p25, 2026-09-04): the sorter knows exactly which inputs
/// are missing and which entries it could not place, so say so, and write
/// the hex to `/tmp/beef-rejected-<millis>.hex` for a repro.
pub(super) fn describe_invalid_beef(beef: &mut Beef, roots: &[String]) -> String {
    let sr = beef.sort_txs();
    let short = |v: &[String]| -> String {
        if v.is_empty() {
            "none".to_string()
        } else {
            v.iter()
                .map(|t| t.chars().take(16).collect::<String>())
                .collect::<Vec<_>>()
                .join(",")
        }
    };
    let bytes = beef.to_binary();
    let dump = format!("/tmp/beef-rejected-{}.hex", Utc::now().timestamp_millis());
    let dumped = match std::fs::write(&dump, hex::encode(&bytes)) {
        Ok(()) => dump,
        Err(e) => format!("not written ({e})"),
    };
    format!(
        "roots [{}]; missing inputs [{}]; transactions with missing inputs [{}]; \
         not valid [{}]; txid-only [{}]; {} tx(s), {} bump(s), {} bytes; dump {}",
        short(roots),
        short(&sr.missing_inputs),
        short(&sr.with_missing_inputs),
        short(&sr.not_valid),
        short(&sr.txid_only),
        beef.txs.len(),
        beef.bumps.len(),
        bytes.len(),
        dumped
    )
}

/// Prunes a BEEF down to the transactions the given roots actually need.
///
/// A transaction that carries a BUMP is self-proving: its merkle path
/// establishes it is in a block, so none of its ancestors belong in the BEEF.
/// This walks the dependency graph from `roots` and stops at every proven
/// transaction, keeping the artifact to one root transaction plus only its
/// recursive proof dependencies, which is what the TypeScript builder emits.
///
/// Unused BUMPs are dropped with the transactions that referenced them and the
/// surviving bump indices are rediscovered on merge, so the result always
/// serializes with consistent indices.
///
/// Roots that are absent from `beef` are ignored. If that leaves nothing to
/// keep, `beef` is left untouched: pruning must never empty a BEEF.
pub(super) fn prune_beef_to_roots(beef: &mut Beef, roots: &[String]) {
    if beef.txs.is_empty() || roots.is_empty() {
        return;
    }

    // `BeefTx::txid()` re-hashes the raw bytes on every call, so index once.
    let txids: Vec<String> = beef.txs.iter().map(|tx| tx.txid()).collect();
    let mut index: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::with_capacity(txids.len());
    for (i, t) in txids.iter().enumerate() {
        index.insert(t.as_str(), i);
    }

    let mut needed: HashSet<usize> = HashSet::new();
    let mut queue: std::collections::VecDeque<usize> = roots
        .iter()
        .filter_map(|r| index.get(r.as_str()).copied())
        .collect();

    while let Some(i) = queue.pop_front() {
        if !needed.insert(i) {
            continue;
        }
        let tx = &beef.txs[i];
        // Proven, or nothing to walk into: this branch terminates here.
        if tx.bump_index().is_some() || tx.is_txid_only() {
            continue;
        }
        let raw = match tx.raw_tx() {
            Some(r) => r.to_vec(),
            None => continue,
        };
        if let Ok(parents) = parse_input_txids(&raw) {
            for parent in parents {
                if let Some(&pi) = index.get(parent.as_str()) {
                    if !needed.contains(&pi) {
                        queue.push_back(pi);
                    }
                }
            }
        }
    }

    if needed.is_empty() || needed.len() == beef.txs.len() {
        return;
    }

    // Rebuild: bumps first so each merged transaction rediscovers its index.
    let mut pruned = Beef::with_version(beef.version);
    pruned.atomic_txid = beef.atomic_txid.clone();

    let mut kept: Vec<usize> = needed.into_iter().collect();
    kept.sort_unstable();

    let mut bump_indices: Vec<usize> = kept
        .iter()
        .filter_map(|&i| beef.txs[i].bump_index())
        .collect();
    bump_indices.sort_unstable();
    bump_indices.dedup();
    for bi in bump_indices {
        if let Some(bump) = beef.bumps.get(bi) {
            pruned.merge_bump(bump.clone());
        }
    }

    for i in kept {
        let tx = &beef.txs[i];
        if tx.is_txid_only() {
            pruned.merge_txid_only(txids[i].clone());
        } else if let Some(raw) = tx.raw_tx() {
            pruned.merge_raw_tx(raw.to_vec(), None);
        } else if let Some(t) = tx.tx() {
            pruned.merge_transaction(t.clone());
        }
    }

    *beef = pruned;
}

/// Seconds since this wallet first recorded the transaction, if it knows it.
async fn local_record_age_secs(conn: &mut SqliteConnection, txid: &str) -> Option<i64> {
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
/// A BEEF only ever gets smaller by acquiring bumps and proofs, and a wallet
/// serving `create_action` cannot rely on its monitor having run: a wallet
/// started without the monitor (or one holding a transaction it did not build)
/// never records a proof, so every later spend drags the whole unproven chain.
/// This is the same fall-through the TypeScript builder takes when storage
/// cannot prove a transaction, and it records the result so the cost is paid
/// once per ancestor rather than on every spend.
///
/// The write goes through the CALLER'S connection. `create_action` runs inside
/// an open SQLite write transaction, so a second pooled connection would block
/// on its own database lock until `busy_timeout` expired.
/// [`StorageSqlx::ingest_merkle_proof`] does exactly this over the pool and is
/// the path every other proof source uses; this is its connection-scoped twin
/// through the SAME store funnel (`store_validated_proof_on`: the proof LAG
/// gate read on this connection, replace-on-differ, the no-raw guard). A proof
/// the gate DEFERS (the header has not aged one cycle) is not stored and the
/// walk continues raw, exactly as before the fetch existed.
///
/// Returns the BUMP bytes only when the proof validated and was stored.
async fn fetch_and_store_merkle_path(
    conn: &mut SqliteConnection,
    storage: Option<&StorageSqlx>,
    txid: &str,
) -> Option<Vec<u8>> {
    let storage = storage?;
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

    // Never store a proof the chain rejects. Matches ingest_merkle_proof:
    // with no tracker wired, the proof is accepted as the provider gave it.
    if let Some(tracker) = storage.get_chain_tracker().await {
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
    }

    let stored = match store_validated_proof_on(
        &mut *conn,
        &ValidatedProofRow {
            txid,
            block_height: header.height,
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
    .execute(&mut *conn)
    .await;

    let _ = sqlx::query(
        "UPDATE transactions SET status = 'completed', proven_tx_id = ?, updated_at = ? WHERE txid = ?",
    )
    .bind(proven_tx_id)
    .bind(now)
    .bind(txid)
    .execute(&mut *conn)
    .await;

    tracing::info!(
        txid = %txid,
        height = header.height,
        "recorded merkle proof during BEEF assembly, the ancestor chain terminates here"
    );

    Some(bytes)
}

/// Core BFS ancestor walk for BEEF construction.
///
/// Given a queue of `(txid, depth)` pairs, walks the ancestor chain using:
/// - Stored BEEFs (`get_stored_beef`) — merged and compacted
/// - Individual tx+proof lookups (`get_tx_with_proof`)
/// - Network fallback (`try_network_fallback`) when local lookup fails
///
/// For unproven transactions, recurses into their inputs up to `MAX_BEEF_RECURSION_DEPTH`.
/// Direct inputs (depth 0) that cannot be found cause an error; deeper ancestors just warn.
///
/// This is the shared core used by both `build_input_beef` (create_action) and
/// `rebuild_beef_for_broadcast` (send_waiting_transactions).
/// Does the chain tracker DEFINITELY refute a stored bump for `txid`?
///
/// Only `Ok(false)` from the tracker refutes; no tracker, a tracker fault
/// (`Err`, since 0.3.66 what the fallback tracker answers when both header
/// sources fail), or a bump whose root cannot be computed keeps the bump
/// attached and lets the final BEEF verification decide, as before the
/// reorg work. An unknown never reads as a stale proof, and the walk never
/// mutates storage on this answer.
pub(super) async fn stored_bump_refuted(
    chain_tracker: Option<&dyn ChainTracker>,
    merkle_path: &MerklePath,
    txid: &str,
) -> bool {
    let Some(tracker) = chain_tracker else {
        return false;
    };
    let Ok(root) = merkle_path.compute_root(Some(txid)) else {
        return false;
    };
    matches!(
        tracker
            .is_valid_root_for_height(&root, merkle_path.block_height)
            .await,
        Ok(false)
    )
}

pub(super) async fn beef_bfs_walk(
    conn: &mut SqliteConnection,
    beef: &mut Beef,
    pending_txids: &mut Vec<(String, usize)>,
    processed_txids: &mut HashSet<String>,
    storage: Option<&StorageSqlx>,
    chain_tracker: Option<&dyn ChainTracker>,
) -> Result<()> {
    let mut proof_fetch_budget = MAX_PROOF_FETCHES_PER_WALK;

    while let Some((txid, depth)) = pending_txids.first().cloned() {
        pending_txids.remove(0);

        if depth >= MAX_BEEF_RECURSION_DEPTH {
            // Exceeded max chain depth — skip this ancestor.
            // Matches TS which throws; we log and continue to produce a partial BEEF.
            eprintln!(
                "Warning: BEEF recursion depth {} exceeded limit {} for txid {}",
                depth, MAX_BEEF_RECURSION_DEPTH, txid
            );
            continue;
        }

        if processed_txids.contains(&txid) {
            continue;
        }
        processed_txids.insert(txid.clone());

        // A BUMP already in the BEEF proves this transaction regardless of what
        // our own proven_txs table knows: the merkle path is the evidence. Carry
        // its raw bytes if they are still missing, LINK the transaction to that
        // BUMP, and stop: no ancestor of a proven transaction belongs in the BEEF.
        //
        // The link is explicit on purpose. `find_bump` matches any level-0 leaf
        // whose hash is this txid, flagged or not, but a merged BUMP built from a
        // sibling's proof carries this txid as a plain sibling hash (`txid:
        // false`), and the merge's own linker only accepts flagged leaves. Left
        // unlinked, the transaction reads as unproven, the sorter demands its
        // parent, the parent was never walked, and the whole BEEF is refused:
        // LOW seat p25, five roots, two of them siblings in block 964422,
        // "missing inputs [2617c486…]" (2026-09-04).
        if let Some(bump_idx) = beef.bumps.iter().position(|b| b.contains(&txid)) {
            flag_txid_leaf(&mut beef.bumps[bump_idx], &txid);
            // Decide from a copy: no borrow of `beef` may live across the await.
            let existing = beef
                .find_txid(&txid)
                .map(|e| (e.bump_index().is_some(), e.raw_tx().map(<[u8]>::to_vec)));
            let raw = match existing {
                Some((true, _)) => None,
                Some((false, raw)) => raw,
                None => get_tx_with_proof(&mut *conn, &txid)
                    .await?
                    .map(|data| data.raw_tx),
            };
            if let Some(raw) = raw {
                beef.merge_raw_tx(raw, Some(bump_idx));
            }
            continue;
        }

        // Check if already in BEEF (from user inputBEEF or previously merged)
        if beef.find_txid(&txid).is_some() {
            continue;
        }

        // Individual transaction lookup — check for proof FIRST.
        // TS/Go pattern: if a tx has a merkle proof, add tx + BUMP and STOP.
        // Do NOT merge stored input_beef for proven txs — the BUMP terminates
        // the chain and ancestors are irrelevant.
        let mut tx_data_opt = get_tx_with_proof(&mut *conn, &txid).await?;

        // We hold the ancestor but not its proof. Ask the chain for the merkle
        // path once and record it: a BUMP terminates the walk here instead of
        // dragging this ancestor's whole unproven history into the BEEF.
        if let Some(data) = tx_data_opt.as_mut() {
            if data.merkle_path.is_none() && proof_fetch_budget > 0 {
                let old_enough = local_record_age_secs(&mut *conn, &txid)
                    .await
                    .map(|age| age >= MIN_AGE_FOR_PROOF_LOOKUP_SECS)
                    .unwrap_or(true);
                if old_enough {
                    proof_fetch_budget -= 1;
                    data.merkle_path =
                        fetch_and_store_merkle_path(&mut *conn, storage, &txid).await;
                }
            }
        }

        // Only merge stored input_beef for UNPROVEN transactions.
        // TS: "if (r.inputBEEF) beef.mergeBeef(r.inputBEEF)" — only when no proof.
        // Go: merges inputBEEF then checks "if subjectTx.MerklePath != nil { return }".
        // Both skip stored BEEF when a proof exists.
        let has_proof = tx_data_opt
            .as_ref()
            .map(|d| d.merkle_path.is_some())
            .unwrap_or(false);

        if !has_proof {
            if let Some(mut stored_beef) = get_stored_beef(&mut *conn, &txid).await? {
                compact_stored_beef(&mut *conn, &mut stored_beef).await?;

                // A stored input_beef is whatever the chain looked like when the
                // transaction was built; ancestors proven since then make most of
                // it dead weight. Keep only the subject's closure, or, when the
                // stored BEEF holds only ancestors, the closure of its inputs.
                let mut stored_roots = vec![txid.clone()];
                if stored_beef.find_txid(&txid).is_none() {
                    if let Some(data) = tx_data_opt.as_ref() {
                        if let Ok(inputs) = parse_input_txids(&data.raw_tx) {
                            if !inputs.is_empty() {
                                stored_roots = inputs;
                            }
                        }
                    }
                }
                prune_beef_to_roots(&mut stored_beef, &stored_roots);

                let stored_beef_valid = if let Some(tracker) = chain_tracker {
                    validate_stored_beef(&mut stored_beef, tracker, &txid).await
                } else {
                    true
                };

                if stored_beef_valid {
                    beef.merge_beef(&stored_beef);

                    for beef_tx in &stored_beef.txs {
                        processed_txids.insert(beef_tx.txid());
                    }

                    if beef.find_txid(&txid).is_some() {
                        continue;
                    }
                }
            }
        }

        // Fix 3: Network fallback — if not found locally, try fetching from services
        let tx_data_opt = if tx_data_opt.is_none() {
            try_network_fallback(&mut *conn, storage, &txid).await?
        } else {
            tx_data_opt
        };

        if let Some(tx_data) = tx_data_opt {
            // If we have a merkle proof, add both tx and proof - no need to recurse
            let bump_index = if let Some(merkle_path_bytes) = &tx_data.merkle_path {
                match MerklePath::from_binary(merkle_path_bytes) {
                    Ok(merkle_path) => {
                        // ts-stack `getBeefForTransaction` with
                        // `skipInvalidProofs` ("proof is currently invalid,
                        // recurse deeper via the rawTx path"): a STORED bump the
                        // chain tracker DEFINITELY refutes (a reorg moved the
                        // block) is not attached. The transaction rides as a
                        // raw-tx leg and its parents are walked one level
                        // deeper. The walk never mutates storage: a tracker
                        // fault keeps the bump (the final BEEF verification
                        // decides, as before), and the stale row is demoted
                        // only by the reorg or review task on positive network
                        // evidence. Before this, every spend touching such a
                        // row was refused with "Invalid merkle root" (loop 8,
                        // 2026-09-07, 28 seats).
                        if stored_bump_refuted(chain_tracker, &merkle_path, &txid).await {
                            tracing::warn!(
                                txid = %txid,
                                height = merkle_path.block_height,
                                marker = "stale_proof_skipped",
                                "beef walk: the stored merkle proof no longer matches the chain; walking the raw-tx leg instead"
                            );
                            None
                        } else {
                            Some(beef.merge_bump(merkle_path))
                        }
                    }
                    Err(e) => {
                        // Continue without proof - will need to recurse to ancestors
                        tracing::warn!(
                            txid = %txid,
                            error = %e,
                            "Failed to parse merkle path — will recurse to ancestors"
                        );
                        None
                    }
                }
            } else {
                None
            };

            // Add the raw transaction to BEEF
            beef.merge_raw_tx(tx_data.raw_tx.clone(), bump_index);

            // If no merkle proof, we need to recurse to this transaction's inputs
            // so the recipient can trace back to proven transactions
            if bump_index.is_none() {
                // Parse the transaction to get its input txids
                // Children are one level deeper in the chain
                if let Ok(input_txids) = parse_input_txids(&tx_data.raw_tx) {
                    for input_txid in input_txids {
                        if !processed_txids.contains(&input_txid)
                            && !pending_txids.iter().any(|(t, _)| t == &input_txid)
                        {
                            pending_txids.push((input_txid, depth + 1));
                        }
                    }
                }
            }
        } else {
            // Fix 2: Direct inputs (depth 0) MUST be found — error if missing.
            // Deeper ancestors may be proven elsewhere, so just warn.
            if depth == 0 {
                return Err(Error::TransactionError(format!(
                    "Cannot find raw tx for direct input {} — BEEF will be incomplete",
                    txid
                )));
            }
            tracing::warn!(
                txid = %txid,
                depth = depth,
                "Cannot find raw tx for ancestor — BEEF may be incomplete"
            );
        }
    }

    Ok(())
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
pub(super) async fn rebuild_beef_for_broadcast(
    conn: &mut SqliteConnection,
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
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        storage,
        None, // No chain_tracker for broadcast rebuilds — validated at creation time
    )
    .await?;

    prune_beef_to_roots(&mut beef, &root_txids);

    Ok(beef)
}

/// Builds input BEEF containing all input transactions with their merkle proofs.
///
/// Collects unique input txids from both user-provided inputs and allocated change inputs,
/// then recursively fetches all ancestor transactions until we reach transactions with
/// merkle proofs or can't find any more ancestors in storage.
///
/// This matches the TypeScript/Go `validateRequiredInputs` and `getBeefForTransaction` behavior:
/// 1. First merges user-provided inputBEEF (contains proofs for external inputs)
/// 2. Then adds storage transactions for known inputs
/// 3. Verifies BEEF against ChainTracker (merkle roots match block headers)
/// 4. Finally trims known_txids to txid-only format to reduce BEEF size
///
/// # Arguments
/// * `storage` - The storage backend to query
/// * `chain_tracker` - Optional chain tracker for merkle root verification (if None, skips verification)
/// * `extended_inputs` - User-provided inputs
/// * `change_inputs` - Allocated change inputs from storage
/// * `user_input_beef` - Optional user-provided BEEF with proofs for external inputs
/// * `known_txids` - TXIDs the recipient already has (will be trimmed to txid-only)
/// * `return_txid_only` - If true, skip BEEF construction entirely
/// * `storage` - Optional storage backend for network fallback when local lookup fails
///
/// # Returns
/// * `Ok(Some(beef_bytes))` - BEEF binary data if there are inputs
/// * `Ok(None)` - If there are no inputs or return_txid_only is true
///
/// # Errors
/// * `ValidationError` - If BEEF structure is invalid or merkle roots don't match chain
/// * `TransactionError` - If a direct input (depth 0) cannot be found in any source
#[allow(clippy::too_many_arguments)]
async fn build_input_beef(
    conn: &mut SqliteConnection,
    chain_tracker: Option<&dyn ChainTracker>,
    extended_inputs: &[ExtendedInput],
    change_inputs: &[AllocatedChangeInput],
    user_input_beef: Option<&[u8]>,
    known_txids: &[String],
    return_txid_only: bool,
    storage: Option<&StorageSqlx>,
) -> Result<Option<Vec<u8>>> {
    // Gap #3: If return_txid_only, skip BEEF construction entirely
    if return_txid_only {
        return Ok(None);
    }

    // Collect unique input txids with their chain depth.
    // Depth tracks how far each txid is from the direct inputs (depth 0).
    // This matches the TypeScript reference which uses actual recursion depth,
    // not a flat counter — a tx with 10 inputs at depth 0 doesn't count as depth 10.
    let mut pending_txids: Vec<(String, usize)> = Vec::new();
    let mut processed_txids: HashSet<String> = HashSet::new();

    for input in extended_inputs {
        if !processed_txids.contains(&input.txid) {
            pending_txids.push((input.txid.clone(), 0));
        }
    }

    for input in change_inputs {
        if !processed_txids.contains(&input.txid)
            && !pending_txids.iter().any(|(t, _)| t == &input.txid)
        {
            pending_txids.push((input.txid.clone(), 0));
        }
    }

    if pending_txids.is_empty() {
        return Ok(None);
    }

    // Create BEEF structure (V2 format)
    let mut beef = Beef::new();

    // Gap #1: Merge user-provided inputBEEF FIRST
    // This contains proofs for external inputs not in our storage
    if let Some(input_beef_bytes) = user_input_beef {
        if !input_beef_bytes.is_empty() {
            match Beef::from_binary(input_beef_bytes) {
                Ok(user_beef) => {
                    beef.merge_beef(&user_beef);
                    // Mark txids from user BEEF as already processed
                    for tx in &user_beef.txs {
                        processed_txids.insert(tx.txid());
                    }
                }
                Err(e) => {
                    return Err(Error::ValidationError(format!(
                        "inputBEEF: invalid BEEF format: {}",
                        e
                    )));
                }
            }
        }
    }

    // The roots this BEEF has to prove: every direct input of the new
    // transaction. Captured before the walk drains the queue.
    let root_txids: Vec<String> = pending_txids.iter().map(|(t, _)| t.clone()).collect();

    // Delegate the BFS ancestor walk to the shared core function.
    beef_bfs_walk(
        &mut *conn,
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        storage,
        chain_tracker,
    )
    .await?;

    // One root transaction plus only its recursive proof dependencies: drop
    // everything a merged BEEF dragged along that no root reaches through an
    // unproven transaction.
    prune_beef_to_roots(&mut beef, &root_txids);

    // Verify BEEF against ChainTracker before trimming known_txids
    // This matches TypeScript/Go behavior: verify after building, before returning
    if let Some(tracker) = chain_tracker {
        let beef_bytes = beef.to_binary();
        // Only verify if BEEF has content beyond the header (4 bytes)
        // AND there are merkle proofs (bumps) to verify
        if beef_bytes.len() > 4 && !beef.bumps.is_empty() {
            // Verify BEEF structure is valid (allow txid-only entries)
            let validation = beef.verify_valid(true);
            if !validation.valid {
                return Err(Error::ValidationError(format!(
                    "inputBEEF: BEEF structure is invalid: {}",
                    describe_invalid_beef(&mut beef, &root_txids)
                )));
            }

            // Verify each merkle root against the ChainTracker
            for (height, root) in &validation.roots {
                match tracker.is_valid_root_for_height(root, *height).await {
                    Ok(true) => {
                        // Root is valid, continue
                    }
                    Ok(false) => {
                        return Err(Error::ValidationError(format!(
                            "inputBEEF: invalid merkle root {} at height {}",
                            root, height
                        )));
                    }
                    Err(e) => {
                        // ChainTracker error (network, block not found, etc.)
                        // Match Go/TypeScript behavior: treat as validation failure
                        return Err(Error::ValidationError(format!(
                            "inputBEEF: failed to verify merkle root at height {}: {}",
                            height, e
                        )));
                    }
                }
            }
        }
        // Note: If BEEF has no bumps (only unproven transactions), we skip verification
        // since there are no merkle roots to verify against the chain.
        // This is consistent with TypeScript/Go which only verify when there are proofs.
    }

    // Gap #2: Trim known_txids to txid-only format
    // This reduces BEEF size when the recipient already has these transactions
    for known_txid in known_txids {
        beef.make_txid_only(known_txid);
    }

    // Serialize BEEF to binary
    let beef_bytes = beef.to_binary();

    // Only return BEEF if it contains data beyond the header
    // BEEF V2 header is 4 bytes (0x0100EFBE for V1 or 0x0200EFBE for V2)
    if beef_bytes.len() > 4 {
        Ok(Some(beef_bytes))
    } else {
        Ok(None)
    }
}

/// Parses a raw transaction and extracts the txids of its inputs.
///
/// # Arguments
/// * `raw_tx` - The raw transaction bytes
///
/// # Returns
/// * `Ok(Vec<String>)` - List of input txids (hex strings)
/// * `Err` - If parsing fails
pub(super) fn parse_input_txids(raw_tx: &[u8]) -> Result<Vec<String>> {
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
pub(super) fn read_var_int_for_beef(data: &[u8], offset: &mut usize) -> Result<u64> {
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
/// # Returns
/// * `Ok(Some(BeefTxData))` - Transaction data if found
/// * `Ok(None)` - If transaction not found in any table
pub(super) async fn get_tx_with_proof(
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

    // Try transactions table - check both raw_tx and input_beef
    let tx_row = sqlx::query(
        r#"
        SELECT raw_tx, input_beef
        FROM transactions
        WHERE txid = ?
        "#,
    )
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = tx_row {
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
/// * `conn` - Database connection for storing fetched tx locally
/// * `storage` - Optional storage backend (provides services access)
/// * `txid` - The transaction ID to fetch
///
/// # Returns
/// * `Ok(Some(BeefTxData))` - If the tx was fetched from the network
/// * `Ok(None)` - If services are unavailable or the tx was not found
async fn try_network_fallback(
    conn: &mut SqliteConnection,
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

                // Store locally for future use
                let now = Utc::now();
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
                .execute(&mut *conn)
                .await;

                // Also try to get the merkle proof from the network, and record
                // it: a proof fetched and thrown away has to be fetched again on
                // every later spend.
                let merkle_path =
                    fetch_and_store_merkle_path(&mut *conn, Some(storage), txid).await;

                return Ok(Some(BeefTxData {
                    raw_tx: raw_tx.clone(),
                    merkle_path,
                }));
            }

            if let Some(ref error) = result.error {
                tracing::debug!(txid = %txid, error = %error, "Network fetch returned error");
            }
        }
        Err(e) => {
            tracing::debug!(txid = %txid, error = %e, "Network fallback failed");
        }
    }

    Ok(None)
}

/// Compacts a stored BEEF by upgrading unproven transactions with current
/// merkle proofs from the `proven_txs` table, then trimming unnecessary
/// ancestor transactions.
///
/// When a stored BEEF was created, some of its transactions may have been
/// unproven (awaiting mining). Over time, those transactions get mined and
/// their proofs are stored in `proven_txs`. This function:
///
/// 1. Finds unproven transactions in the BEEF
/// 2. Checks `proven_txs` for newly-available merkle proofs
/// 3. Upgrades those transactions with their BUMPs
/// 4. Trims ancestor transactions that are no longer needed (because their
///    dependents are now self-proving via BUMPs)
///
/// This can dramatically reduce BEEF size: a 200KB stored BEEF with long
/// unproven ancestor chains can shrink to a few KB once ancestors are proven.
pub(super) async fn compact_stored_beef(
    conn: &mut SqliteConnection,
    beef: &mut Beef,
) -> Result<()> {
    // Collect unproven txids in the BEEF
    let unproven_txids: Vec<String> = beef
        .txs
        .iter()
        .filter(|tx| tx.bump_index().is_none() && !tx.is_txid_only())
        .map(|tx| tx.txid())
        .collect();

    if unproven_txids.is_empty() {
        return Ok(());
    }

    // Batch query proven_txs for all unproven txids
    // SQLite bind param limit is 999, batch in chunks of 400
    for chunk in unproven_txids.chunks(400) {
        let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query_str = format!(
            "SELECT txid, merkle_path FROM proven_txs WHERE txid IN ({})",
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for txid in chunk {
            query = query.bind(txid);
        }

        let rows = query.fetch_all(&mut *conn).await?;

        for row in &rows {
            let txid: String = row.get("txid");
            let merkle_path_bytes: Vec<u8> = row.get("merkle_path");

            if let Ok(merkle_path) = MerklePath::from_binary(&merkle_path_bytes) {
                let bump_index = beef.merge_bump(merkle_path);
                if let Some(tx) = beef.find_txid_mut(&txid) {
                    tx.set_bump_index(Some(bump_index));
                }
            }
        }
    }

    // Trimming is deliberately left to the caller: only the caller knows which
    // transactions the BEEF has to prove. `prune_beef_to_roots` takes those
    // roots and stops at every BUMP. `Beef::trim_known_proven` is not used:
    // it walks from whichever transactions nothing else spends, so an unproven
    // input can be dropped merely because a proven sibling descends from it.

    Ok(())
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
pub(super) async fn get_stored_beef(
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
    use bsv_rs::wallet::{CreateActionOptions, CreateActionOutput};

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
        assert!(d1
            .chars()
            .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='));

        // Should be different each time
        assert_ne!(d1, d2);
    }

    #[test]
    fn test_validate_description_too_short() {
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let args = bsv_rs::wallet::CreateActionArgs {
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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
        use bsv_rs::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6")
            .unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint {
                    txid: txid_arr,
                    vout: 0,
                },
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
        assert!(err
            .to_string()
            .contains("unlockingScript or unlockingScriptLength required"));
    }

    #[test]
    fn test_validate_input_unlocking_script_length_mismatch() {
        use bsv_rs::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6")
            .unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint {
                    txid: txid_arr,
                    vout: 0,
                },
                input_description: "Test input".to_string(),
                unlocking_script: Some(vec![0x00]), // 1 byte
                unlocking_script_length: Some(2),   // but says 2
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
        use bsv_rs::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6")
            .unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![
                CreateActionInput {
                    outpoint: Outpoint {
                        txid: txid_arr,
                        vout: 0,
                    },
                    input_description: "Input 1".to_string(),
                    unlocking_script: Some(vec![0x00]),
                    unlocking_script_length: None,
                    sequence_number: None,
                },
                CreateActionInput {
                    outpoint: Outpoint {
                        txid: txid_arr,
                        vout: 0,
                    }, // Same outpoint
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
        use bsv_rs::wallet::{CreateActionInput, Outpoint};

        let txid = hex::decode("756754d5ad8f00e05c36d89a852971c0a1dc0c10f20cd7840ead347aff475ef6")
            .unwrap();
        let mut txid_arr = [0u8; 32];
        txid_arr.copy_from_slice(&txid);

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "Test transaction".to_string(),
            input_beef: None,
            inputs: Some(vec![CreateActionInput {
                outpoint: Outpoint {
                    txid: txid_arr,
                    vout: 0,
                },
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
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        // Create a simple action with one output
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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

        let result = create_action_internal(&storage, None, user.user_id, args).await;

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

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        // First, seed the wallet with some change
        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

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

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        // Verify the output has the basket and tags
        assert!(!result.outputs.is_empty());
        let first_output = &result.outputs[0];
        assert_eq!(first_output.basket, Some("payments".to_string()));
        assert_eq!(
            first_output.tags,
            vec!["tag1".to_string(), "tag2".to_string()]
        );
        assert_eq!(
            first_output.custom_instructions,
            Some("{\"type\":\"custom\"}".to_string())
        );
    }

    #[tokio::test]
    async fn test_create_action_no_send() {
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

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        // For noSend, we should get change vouts
        assert!(result.no_send_change_output_vouts.is_some());
    }

    #[tokio::test]
    async fn test_create_action_multiple_outputs() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        seed_change_output(&storage, user.user_id, 200_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

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

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();

        seed_change_output(&storage, user.user_id, 100_000).await;

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let args = bsv_rs::wallet::CreateActionArgs {
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

        let result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        assert_eq!(result.version, 2);
        assert_eq!(result.lock_time, 500000);
    }

    /// Minimal coinbase-like raw_tx for seeded test transactions.
    /// Has one coinbase input (all-zero txid, skipped by parse_input_txids)
    /// and one output, so BEEF construction can include it without recursing.
    fn seed_raw_tx() -> Vec<u8> {
        hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap()
    }

    // Helper function to seed a change output for testing
    async fn seed_change_output(storage: &StorageSqlx, user_id: i64, satoshis: i64) {
        let now = Utc::now();
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();

        // Create a fake completed transaction with raw_tx so BEEF construction can find it
        let raw_tx = seed_raw_tx();
        let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(satoshis)
        .bind("0000000000000000000000000000000000000000000000000000000000000001")
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let transaction_id = tx_result.last_insert_rowid();

        // Create a change output
        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

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

    // Helper function to seed a proven transaction with merkle proof
    async fn seed_proven_tx(storage: &StorageSqlx, txid: &str, raw_tx: &[u8], merkle_path: &[u8]) {
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, 100000, 0, '0000000000000000000000000000000000000000000000000000000000000abc', '0000000000000000000000000000000000000000000000000000000000000def', ?, ?, ?, ?)
            "#,
        )
        .bind(txid)
        .bind(merkle_path)
        .bind(raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_build_input_beef_empty() {
        // Test that empty inputs returns None
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let extended_inputs: Vec<ExtendedInput> = vec![];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification for this test
            &extended_inputs,
            &change_inputs,
            None,  // user_input_beef
            &[],   // known_txids
            false, // return_txid_only
            None,  // storage - no network fallback
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_build_input_beef_with_proven_tx() {
        // Test building BEEF with a proven transaction
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Sample raw transaction (minimal valid tx)
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

        // Sample merkle path (BRC-74 format) - minimal valid structure
        // Format: block_height (4 bytes) + tree_height (1 byte) + leaf_count (varint) + leaf_flags + leaf_hashes
        let merkle_path = hex::decode("a086010001020002").unwrap();

        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification for this test
            &extended_inputs,
            &change_inputs,
            None,  // user_input_beef
            &[],   // known_txids
            false, // return_txid_only
            None,  // storage - no network fallback
        )
        .await
        .unwrap();

        // Should return Some BEEF data
        assert!(result.is_some());
        let beef_bytes = result.unwrap();

        // BEEF V2 starts with magic bytes (little endian: 0x0100BEEF or 0x0200BEEF)
        // In practice, bsv_rs uses 0xEFBE0002 for V2 in little endian
        assert!(beef_bytes.len() > 4);
    }

    #[tokio::test]
    async fn test_build_input_beef_with_unproven_tx() {
        // Test building BEEF with an unproven transaction (from transactions table)
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        // Create a transaction with raw_tx but no proof
        let now = Utc::now();
        let txid = "1111111111111111111111111111111111111111111111111111111111111111";
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref123', 0, 5000000000, 1, 0, 'Test tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return Some BEEF data (even without proof)
        assert!(result.is_some());
        let beef_bytes = result.unwrap();
        assert!(beef_bytes.len() > 4);
    }

    #[tokio::test]
    async fn test_build_input_beef_deduplicates_txids() {
        // Test that duplicate txids are handled correctly
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        let now = Utc::now();
        let txid = "2222222222222222222222222222222222222222222222222222222222222222";
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', 'ref123', 0, 10000000000, 1, 0, 'Test tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        // Same txid appears in both extended_inputs and change_inputs
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs = vec![AllocatedChangeInput {
            output_id: 1,
            satoshis: 5000000000,
            txid: txid.to_string(),
            vout: 1,
            locking_script: vec![],
            derivation_prefix: None,
            derivation_suffix: None,
            sender_identity_key: None,
        }];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return Some BEEF data
        assert!(result.is_some());
        // The transaction should only appear once in the BEEF
        let beef_bytes = result.unwrap();
        assert!(beef_bytes.len() > 4);
    }

    #[tokio::test]
    async fn test_get_tx_with_proof_from_proven_txs() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let txid = "3333333333333333333333333333333333333333333333333333333333333333";
        let raw_tx = vec![1, 2, 3, 4, 5];
        let merkle_path = vec![6, 7, 8, 9, 10];

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = get_tx_with_proof(&mut conn, txid).await.unwrap();

        assert!(result.is_some());
        let tx_data = result.unwrap();
        assert_eq!(tx_data.raw_tx, raw_tx);
        assert_eq!(tx_data.merkle_path, Some(merkle_path));
    }

    #[tokio::test]
    async fn test_get_tx_with_proof_from_transactions() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        let now = Utc::now();
        let txid = "4444444444444444444444444444444444444444444444444444444444444444";
        let raw_tx = vec![11, 12, 13, 14, 15];

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref456', 0, 1000, 1, 0, 'Test tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = get_tx_with_proof(&mut conn, txid).await.unwrap();

        assert!(result.is_some());
        let tx_data = result.unwrap();
        assert_eq!(tx_data.raw_tx, raw_tx);
        assert!(tx_data.merkle_path.is_none()); // No proof from transactions table
    }

    #[tokio::test]
    async fn test_get_tx_with_proof_not_found() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = get_tx_with_proof(&mut conn, "nonexistent_txid")
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_parse_input_txids_coinbase() {
        // Coinbase transaction (input txid is all zeros) - should return empty
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

        let txids = parse_input_txids(&raw_tx).unwrap();
        assert!(txids.is_empty()); // Coinbase inputs (all zeros) should be excluded
    }

    #[test]
    fn test_parse_input_txids_single_input() {
        // Transaction with one non-coinbase input
        // version(4) + input_count(1) + txid(32) + vout(4) + script_len(1) + script + seq(4) + ...
        let mut raw_tx = vec![];
        raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        raw_tx.push(0x01); // 1 input
                           // Input txid (will be reversed when parsed)
        let input_txid_bytes =
            hex::decode("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        raw_tx.extend_from_slice(&input_txid_bytes);
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
        raw_tx.push(0x00); // empty script
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        raw_tx.push(0x00); // 0 outputs
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let txids = parse_input_txids(&raw_tx).unwrap();
        assert_eq!(txids.len(), 1);
        assert_eq!(
            txids[0],
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn test_parse_input_txids_multiple_inputs() {
        // Transaction with two non-coinbase inputs
        let mut raw_tx = vec![];
        raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        raw_tx.push(0x02); // 2 inputs

        // First input
        let input1_txid =
            hex::decode("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        raw_tx.extend_from_slice(&input1_txid);
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
        raw_tx.push(0x00); // empty script
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence

        // Second input
        let input2_txid =
            hex::decode("2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();
        raw_tx.extend_from_slice(&input2_txid);
        raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // vout 1
        raw_tx.push(0x00); // empty script
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence

        raw_tx.push(0x00); // 0 outputs
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let txids = parse_input_txids(&raw_tx).unwrap();
        assert_eq!(txids.len(), 2);
        assert_eq!(
            txids[0],
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(
            txids[1],
            "2222222222222222222222222222222222222222222222222222222222222222"
        );
    }

    #[test]
    fn test_parse_input_txids_empty_tx() {
        // Very short transaction (too short to parse)
        let raw_tx = vec![0x01, 0x00];
        let txids = parse_input_txids(&raw_tx).unwrap();
        assert!(txids.is_empty());
    }

    #[test]
    fn test_read_var_int_for_beef() {
        // Single byte
        let data = vec![0x05];
        let mut offset = 0;
        assert_eq!(read_var_int_for_beef(&data, &mut offset).unwrap(), 5);
        assert_eq!(offset, 1);

        // Two byte (0xfd prefix)
        let data = vec![0xfd, 0x00, 0x01];
        let mut offset = 0;
        assert_eq!(read_var_int_for_beef(&data, &mut offset).unwrap(), 256);
        assert_eq!(offset, 3);
    }

    #[tokio::test]
    async fn test_build_input_beef_recursive_ancestor() {
        // Test that BEEF construction recursively fetches ancestors for unproven transactions
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        // Create a chain: proven_tx -> unproven_tx (which we spend)
        // The unproven_tx's input txid points to proven_tx

        // First, create the proven ancestor transaction (txid1)
        let txid1 = "1111111111111111111111111111111111111111111111111111111111111111";
        let raw_tx1 = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let merkle_path1 = hex::decode("a086010001020002").unwrap();
        seed_proven_tx(&storage, txid1, &raw_tx1, &merkle_path1).await;

        // Create an unproven transaction (txid2) that spends from txid1
        let txid2 = "2222222222222222222222222222222222222222222222222222222222222222";
        // Build a raw tx that has txid1 as its input
        let mut raw_tx2 = vec![];
        raw_tx2.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        raw_tx2.push(0x01); // 1 input
                            // Input txid (txid1 in little-endian bytes)
        let txid1_bytes = hex::decode(txid1).unwrap();
        let mut txid1_le = txid1_bytes.clone();
        txid1_le.reverse(); // Convert to little-endian
        raw_tx2.extend_from_slice(&txid1_le);
        raw_tx2.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout 0
        raw_tx2.push(0x00); // empty script
        raw_tx2.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        raw_tx2.push(0x01); // 1 output
        raw_tx2.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 1000 satoshis
        raw_tx2.push(0x00); // empty script
        raw_tx2.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_tx2', 0, 1000, 1, 0, 'Unproven tx spending proven', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid2)
        .bind(&raw_tx2)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        // Now build BEEF for txid2 - it should include both txid2 (unproven) and txid1 (proven with proof)
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid2.to_string(),
            vout: 0,
            satoshis: 1000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return Some BEEF data containing both transactions
        assert!(result.is_some());
        let beef_bytes = result.unwrap();
        // BEEF should be larger than just one transaction since it includes the ancestor chain
        assert!(beef_bytes.len() > 4);
    }

    // =============================================================================
    // Tests for BEEF Gaps (matching Go tests)
    // =============================================================================

    #[tokio::test]
    async fn test_build_input_beef_return_txid_only() {
        // Gap #3: When return_txid_only is true, should return None
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let txid = "3333333333333333333333333333333333333333333333333333333333333333";
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 1000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // With return_txid_only = true, should return None regardless of inputs
        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            true, // return_txid_only = true
            None,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_build_input_beef_with_known_txids() {
        // Gap #2: Known txids should be trimmed to txid-only format
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Sample raw transaction
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let merkle_path = hex::decode("a086010001020002").unwrap();
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // Build BEEF with the txid marked as known
        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification
            &extended_inputs,
            &change_inputs,
            None,
            &[txid.to_string()], // This txid is known to recipient
            false,
            None,
        )
        .await
        .unwrap();

        // Should still return BEEF (but with txid-only for known tx)
        assert!(result.is_some());
        let beef_bytes = result.unwrap();

        // Parse the BEEF and verify the tx is txid-only
        let beef = Beef::from_binary(&beef_bytes).unwrap();
        let beef_tx = beef.find_txid(txid).unwrap();
        assert!(
            beef_tx.is_txid_only(),
            "Known txid should be converted to txid-only"
        );
    }

    #[tokio::test]
    async fn test_build_input_beef_with_user_input_beef() {
        // Gap #1: User-provided inputBEEF should be merged first
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Create a user-provided BEEF with a transaction
        let user_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let user_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Build user's BEEF
        let mut user_beef = Beef::new();
        user_beef.merge_raw_tx(user_raw_tx, None);
        let user_beef_bytes = user_beef.to_binary();

        // Input references the tx from user BEEF (not in storage)
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: user_txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // Build BEEF with user-provided inputBEEF
        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification
            &extended_inputs,
            &change_inputs,
            Some(&user_beef_bytes), // User provides BEEF for external input
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return BEEF containing the user's transaction
        assert!(result.is_some());
        let beef_bytes = result.unwrap();

        // Parse and verify the tx is included
        let beef = Beef::from_binary(&beef_bytes).unwrap();
        assert!(
            beef.find_txid(user_txid).is_some(),
            "User-provided transaction should be in the BEEF"
        );
    }

    #[tokio::test]
    async fn test_build_input_beef_user_beef_invalid() {
        // Gap #1: Invalid user inputBEEF should return error
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: "4444444444444444444444444444444444444444444444444444444444444444".to_string(),
            vout: 0,
            satoshis: 1000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // Provide invalid BEEF bytes
        let invalid_beef = vec![0x00, 0x01, 0x02, 0x03];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None, // chain_tracker - skip verification
            &extended_inputs,
            &change_inputs,
            Some(&invalid_beef),
            &[],
            false,
            None,
        )
        .await;

        // Should return error for invalid BEEF
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("inputBEEF"),
            "Error should mention inputBEEF"
        );
    }

    // =============================================================================
    // Tests for BEEF Verification against ChainTracker (Gap #4)
    // =============================================================================

    #[tokio::test]
    async fn test_build_input_beef_with_valid_chain_tracker() {
        use bsv_rs::transaction::AlwaysValidChainTracker;

        // Test that BEEF verification passes with AlwaysValidChainTracker
        // We use unproven transactions (no merkle path) to avoid structural validation issues
        // with fake merkle paths in test data
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        // Create an unproven transaction (no merkle path needed)
        let now = Utc::now();
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref123', 0, 5000000000, 1, 0, 'Test tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // Use AlwaysValidChainTracker - BEEF with unproven txs has no roots to verify
        // This tests that the verification code path completes successfully
        let tracker = AlwaysValidChainTracker::new(100000);

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            Some(&tracker),
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return Some BEEF data (verification passes)
        assert!(result.is_some());
        let beef_bytes = result.unwrap();
        assert!(beef_bytes.len() > 4);
    }

    #[tokio::test]
    async fn test_build_input_beef_skips_verification_when_no_bumps() {
        use bsv_rs::transaction::MockChainTracker;

        // Test that BEEF verification is SKIPPED when there are no bumps (merkle proofs)
        // This is because there are no merkle roots to verify against the chain.
        //
        // When the merkle path in storage is malformed (cannot be parsed), the BEEF is built
        // without a bump, and verification is skipped. This is correct behavior - we can't
        // verify what we don't have.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Sample raw transaction with a fake merkle path (will fail to parse, no bump added)
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let merkle_path = hex::decode("a086010001020002").unwrap(); // Invalid/incomplete merkle path
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // Use MockChainTracker with NO valid roots
        // But since the merkle path can't be parsed, no bump is added to BEEF,
        // so verification will be skipped entirely
        let tracker = MockChainTracker::new(100000);

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            Some(&tracker),
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await;

        // Should succeed because there are no bumps to verify
        // The malformed merkle path means no bump was added to BEEF
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // BEEF is built, just without a proof
    }

    #[tokio::test]
    async fn test_build_input_beef_no_verification_without_tracker() {
        // Test that BEEF is built successfully without verification when no tracker is provided
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Sample raw transaction
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let merkle_path = hex::decode("a086010001020002").unwrap();
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path).await;

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: txid.to_string(),
            vout: 0,
            satoshis: 5000000000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        // With chain_tracker = None, verification is skipped
        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // Should return Some BEEF data (no verification attempted)
        assert!(result.is_some());
        let beef_bytes = result.unwrap();
        assert!(beef_bytes.len() > 4);
    }

    // =============================================================================
    // Tests for validate_stored_beef (Layer 4: pre-merge BEEF validation)
    // =============================================================================

    /// Helper: build a valid single-tx BEEF with a real merkle path.
    /// Returns (beef_bytes, txid, height, merkle_root).
    fn build_test_beef_with_bump(height: u32, _fake_root: &str) -> (Vec<u8>, String) {
        use bsv_rs::transaction::MerklePath;

        // Coinbase tx (block 1) — a real transaction for structural validity
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Build a minimal merkle path: single leaf (the txid itself) at index 0
        // The computed root will be the txid itself (single-tx block).
        // We create a MerklePath with the given height so verify_valid extracts
        // the (height, computed_root) pair for validation.
        let mut beef = Beef::new();
        let mp = MerklePath {
            block_height: height,
            path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
                offset: 0,
                hash: Some(txid.to_string()),
                txid: true,
                duplicate: false,
            }]],
        };
        let bump_idx = beef.merge_bump(mp);
        beef.merge_raw_tx(raw_tx, Some(bump_idx));

        (beef.to_binary(), txid.to_string())
    }

    /// Helper: seed a transaction with input_beef in the DB (simulates stored ancestor BEEF).
    async fn seed_tx_with_input_beef(
        storage: &StorageSqlx,
        txid: &str,
        input_beef: &[u8],
        raw_tx: &[u8],
    ) {
        let now = Utc::now();
        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                                      version, lock_time, description, txid, raw_tx, input_beef,
                                      created_at, updated_at)
            VALUES (?, 'completed', ?, 0, 5000000000, 1, 0, 'Test tx', ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(format!("ref_{}", &txid[..8]))
        .bind(txid)
        .bind(raw_tx)
        .bind(input_beef)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_stored_beef_valid_roots() {
        use bsv_rs::transaction::MockChainTracker;

        // Build a BEEF with a bump at height 500
        let (beef_bytes, _txid) = build_test_beef_with_bump(500, "");

        let mut beef = Beef::from_binary(&beef_bytes).unwrap();
        // Get the actual computed root so we can register it with the tracker
        let validation = beef.verify_valid(true);
        assert!(validation.valid);
        let (height, root) = validation.roots.iter().next().unwrap();

        let mut tracker = MockChainTracker::new(1000);
        tracker.add_root(*height, root.clone());

        let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
        assert!(
            result,
            "Valid BEEF with matching root should pass validation"
        );
    }

    #[tokio::test]
    async fn test_validate_stored_beef_invalid_root_rejected() {
        use bsv_rs::transaction::MockChainTracker;

        // Build a BEEF with a bump at height 942926
        let (beef_bytes, _txid) = build_test_beef_with_bump(942926, "");

        let mut beef = Beef::from_binary(&beef_bytes).unwrap();

        // Tracker has a DIFFERENT root for height 942926 (simulates orphaned block)
        let mut tracker = MockChainTracker::new(1000);
        tracker.add_root(
            942926,
            "e6bcdfaf6cdc58d1b98dd9ccd67608b0cea09c08873b6ad4d88aa51f597fc69a".to_string(),
        );

        let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
        assert!(!result, "BEEF with invalid merkle root should be rejected");
    }

    #[tokio::test]
    async fn test_validate_stored_beef_unknown_height_rejected() {
        use bsv_rs::transaction::MockChainTracker;

        // Build a BEEF with a bump at height 999
        let (beef_bytes, _txid) = build_test_beef_with_bump(999, "");

        let mut beef = Beef::from_binary(&beef_bytes).unwrap();

        // Tracker has NO root for height 999 → is_valid_root_for_height returns false
        let tracker = MockChainTracker::new(1000);

        let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
        assert!(!result, "BEEF with unknown height should be rejected");
    }

    #[tokio::test]
    async fn test_validate_stored_beef_no_bumps_passes() {
        use bsv_rs::transaction::MockChainTracker;

        // Empty BEEF with no bumps — should always pass
        let mut beef = Beef::new();
        let tracker = MockChainTracker::new(1000);

        let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
        assert!(result, "BEEF with no bumps should pass validation");
    }

    #[tokio::test]
    async fn test_validate_stored_beef_tracker_error_rejects() {
        use bsv_rs::transaction::ChainTrackerError;

        // Build a BEEF with a bump
        let (beef_bytes, _txid) = build_test_beef_with_bump(500, "");
        let mut beef = Beef::from_binary(&beef_bytes).unwrap();

        // Tracker that always errors
        struct ErrorTracker;

        #[async_trait::async_trait]
        impl ChainTracker for ErrorTracker {
            async fn is_valid_root_for_height(
                &self,
                _root: &str,
                _height: u32,
            ) -> std::result::Result<bool, ChainTrackerError> {
                Err(ChainTrackerError::NetworkError("timeout".to_string()))
            }
            async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
                Ok(1000)
            }
        }

        let result = validate_stored_beef(&mut beef, &ErrorTracker, "test_txid").await;
        assert!(
            !result,
            "ChainTracker error should cause stored BEEF to be rejected (fail-safe)"
        );
    }

    #[tokio::test]
    async fn test_beef_bfs_walk_discards_corrupt_stored_beef_and_falls_through() {
        use bsv_rs::transaction::MockChainTracker;

        // This is the integration test: a transaction has corrupt stored input_beef,
        // but the individual proven_tx has a valid proof. beef_bfs_walk should discard
        // the stored BEEF and fall through to the individual tx+proof lookup.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // The parent transaction (ancestor) — this is what the stored BEEF references
        let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Build corrupt stored BEEF (bad root at height 942926)
        let (corrupt_beef_bytes, _) = build_test_beef_with_bump(942926, "");

        // The child transaction that spends the parent
        // (simple tx spending output 0 of parent)
        let child_txid = format!("{:0>64}", "child01");

        // Seed the corrupt stored BEEF on the child transaction
        seed_tx_with_input_beef(&storage, &child_txid, &corrupt_beef_bytes, &parent_raw_tx).await;

        // Also seed the parent in proven_txs with a VALID proof
        // (this is the fallback path — individual tx lookup should find it)
        let valid_merkle_path = hex::decode("a086010001020002").unwrap();
        seed_proven_tx(&storage, parent_txid, &parent_raw_tx, &valid_merkle_path).await;

        // MockChainTracker that rejects height 942926 (orphaned) but doesn't know
        // about any heights (so any stored BEEF with bumps will be rejected)
        let tracker = MockChainTracker::new(950000);
        // Don't add the corrupt root → is_valid_root_for_height returns false

        let mut beef = Beef::new();
        let mut pending_txids = vec![(child_txid.clone(), 0)];
        let mut processed_txids = HashSet::new();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending_txids,
            &mut processed_txids,
            None, // no network fallback needed — proven_txs has the data
            Some(&tracker as &dyn ChainTracker),
        )
        .await;

        // Should succeed — corrupt stored BEEF was discarded, individual lookup worked
        assert!(
            result.is_ok(),
            "beef_bfs_walk should succeed by falling through to individual tx lookup: {:?}",
            result.err()
        );

        // The child txid should have been processed
        assert!(processed_txids.contains(&child_txid));
    }

    #[tokio::test]
    async fn test_beef_bfs_walk_merges_valid_stored_beef() {
        use bsv_rs::transaction::AlwaysValidChainTracker;

        // Valid stored BEEF should be merged normally
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Build valid stored BEEF
        let (valid_beef_bytes, _) = build_test_beef_with_bump(100, "");

        let child_txid = format!("{:0>64}", "child02");

        seed_tx_with_input_beef(&storage, &child_txid, &valid_beef_bytes, &parent_raw_tx).await;

        // AlwaysValidChainTracker accepts any root
        let tracker = AlwaysValidChainTracker::new(950000);

        let mut beef = Beef::new();
        let mut pending_txids = vec![(child_txid.clone(), 0)];
        let mut processed_txids = HashSet::new();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending_txids,
            &mut processed_txids,
            None,
            Some(&tracker as &dyn ChainTracker),
        )
        .await;

        assert!(result.is_ok());
        // The parent txid from the stored BEEF should have been processed (merged)
        assert!(
            processed_txids.contains(parent_txid),
            "Parent txid should be processed after merging valid stored BEEF"
        );
    }

    #[tokio::test]
    async fn test_beef_bfs_walk_no_tracker_skips_validation() {
        // Without a tracker, stored BEEF is merged unconditionally (backward compat)
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
        let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Corrupt BEEF — but no tracker, so it should still be merged
        let (corrupt_beef_bytes, _) = build_test_beef_with_bump(942926, "");

        let child_txid = format!("{:0>64}", "child03");

        seed_tx_with_input_beef(&storage, &child_txid, &corrupt_beef_bytes, &parent_raw_tx).await;

        let mut beef = Beef::new();
        let mut pending_txids = vec![(child_txid.clone(), 0)];
        let mut processed_txids = HashSet::new();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending_txids,
            &mut processed_txids,
            None,
            None, // No tracker — skip validation
        )
        .await;

        assert!(result.is_ok());
        // Parent txid from stored BEEF should be processed (merged unconditionally)
        assert!(
            processed_txids.contains(parent_txid),
            "Without tracker, corrupt stored BEEF should still be merged"
        );
    }

    // =============================================================================
    // Tests for nosend UTXO exclusion (nosend outputs must NOT be selected)
    // =============================================================================

    /// Helper: seed a change output whose parent transaction has the given status.
    /// Uses a unique txid derived from `tag` to avoid collisions.
    async fn seed_change_output_with_status(
        storage: &StorageSqlx,
        user_id: i64,
        satoshis: i64,
        status: &str,
        tag: &str,
    ) {
        let now = Utc::now();
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();

        // Build a unique 64-hex-char txid from the tag
        let txid = format!("{:0>64}", tag);
        let raw_tx = seed_raw_tx();

        let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, ?, 'seed_ref', 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let transaction_id = tx_result.last_insert_rowid();

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

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
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_nosend_outputs_not_selected_for_spending() {
        // A change output whose parent tx has status 'nosend' must NOT appear
        // in UTXO selection (count_change_inputs should return 0,
        // allocate_change_input should return None).
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Seed a change output with a 'nosend' parent transaction
        seed_change_output_with_status(&storage, user.user_id, 100_000, "nosend", "aa01").await;

        let mut conn = storage.pool().acquire().await.unwrap();

        // count_change_inputs should see 0 available outputs
        let count = count_change_inputs(&mut conn, user.user_id, basket.basket_id, true)
            .await
            .unwrap();
        assert_eq!(count, 0, "nosend outputs must not be counted as available");

        // allocate_change_input should return None
        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            9999, // dummy transaction_id for allocation
            50_000,
            true,
        )
        .await
        .unwrap();
        assert!(
            allocated.is_none(),
            "nosend outputs must not be allocated for spending"
        );
    }

    #[tokio::test]
    async fn test_completed_outputs_still_selected() {
        // Positive control: a change output whose parent tx has status 'completed'
        // MUST be selected by count_change_inputs and allocate_change_input.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Seed a change output with a 'completed' parent transaction
        seed_change_output_with_status(&storage, user.user_id, 100_000, "completed", "bb01").await;

        // Create a dummy "spending" transaction so allocate_change_input can set spent_by
        // without violating the foreign key constraint.
        let now = Utc::now();
        let spending_tx = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at)
            VALUES (?, 'unsigned', 'spending_ref', 1, 0, 1, 0, 'Spending tx', ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind("cc00000000000000000000000000000000000000000000000000000000000001")
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        let spending_tx_id = spending_tx.last_insert_rowid();

        let mut conn = storage.pool().acquire().await.unwrap();

        // count_change_inputs should see 1 available output
        let count = count_change_inputs(&mut conn, user.user_id, basket.basket_id, true)
            .await
            .unwrap();
        assert_eq!(count, 1, "completed outputs must be counted as available");

        // allocate_change_input should return Some
        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            spending_tx_id,
            50_000,
            true,
        )
        .await
        .unwrap();
        assert!(
            allocated.is_some(),
            "completed outputs must be allocatable for spending"
        );

        let input = allocated.unwrap();
        assert_eq!(input.satoshis, 100_000);
    }

    // =============================================================================
    // Tests for BEEF ancestry construction fixes
    // =============================================================================

    #[tokio::test]
    async fn test_build_beef_includes_unconfirmed_parent() {
        // Create a chain: confirmed parent (with merkle proof) -> unconfirmed child -> grandchild
        // Build BEEF for the grandchild and verify it includes both the child (raw) and parent (with proof)
        use sha2::{Digest, Sha256};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        // Helper to compute txid from raw bytes
        let compute_txid = |raw: &[u8]| -> String {
            let h1 = Sha256::digest(raw);
            let h2 = Sha256::digest(h1);
            let mut v = h2.to_vec();
            v.reverse();
            hex::encode(v)
        };

        // 1. Create a confirmed parent transaction with merkle proof
        let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap();
        let parent_txid = compute_txid(&parent_raw_tx);
        let parent_merkle_path = hex::decode("a086010001020002").unwrap();
        seed_proven_tx(&storage, &parent_txid, &parent_raw_tx, &parent_merkle_path).await;

        // 2. Create an unconfirmed child tx that spends from parent
        let parent_txid_bytes = hex::decode(&parent_txid).unwrap();
        let mut parent_le = parent_txid_bytes.clone();
        parent_le.reverse();

        let mut child_raw_tx = vec![];
        child_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw_tx.push(0x01); // 1 input
        child_raw_tx.extend_from_slice(&parent_le); // parent txid LE
        child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout 0
        child_raw_tx.push(0x00); // empty script
        child_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw_tx.push(0x01); // 1 output
        child_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        child_raw_tx.push(0x00); // empty script
        child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = compute_txid(&child_raw_tx);

        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_child', 0, 1000, 1, 0, 'Unconfirmed child', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(&child_txid)
        .bind(&child_raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        // 3. Build BEEF for the grandchild (which spends from child)
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: child_txid.clone(),
            vout: 0,
            satoshis: 1000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await
        .unwrap();

        // BEEF should contain both the child (raw) and parent (with proof)
        assert!(result.is_some());
        let beef_bytes = result.unwrap();
        let beef = Beef::from_binary(&beef_bytes).unwrap();

        // Verify both transactions are in the BEEF
        assert!(
            beef.find_txid(&child_txid).is_some(),
            "BEEF must contain the unconfirmed child tx"
        );
        assert!(
            beef.find_txid(&parent_txid).is_some(),
            "BEEF must contain the confirmed parent tx"
        );

        // Verify the BEEF has at least 2 transactions (child + parent)
        assert!(
            beef.txs.len() >= 2,
            "BEEF must contain at least 2 transactions (child + parent), got {}",
            beef.txs.len()
        );
    }

    #[tokio::test]
    async fn test_build_beef_errors_on_missing_direct_input() {
        // Try to build BEEF for a tx whose direct input (depth 0) has no raw_tx in storage.
        // Should return an error, not silent success.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let missing_txid = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: missing_txid.to_string(),
            vout: 0,
            satoshis: 50_000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await;

        // Should be an error — not silent success
        assert!(result.is_err(), "Must error when direct input is missing");
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot find raw tx for direct input"),
            "Error message must mention the missing direct input: {}",
            err
        );
        assert!(
            err.to_string().contains(missing_txid),
            "Error message must include the txid: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_build_beef_warns_on_missing_deep_ancestor() {
        // Build BEEF where a depth-2 ancestor is missing.
        // Should succeed (with warning logged) — the BEEF may be incomplete but
        // we don't hard-error on missing deep ancestors.
        use sha2::{Digest, Sha256};

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();

        let compute_txid = |raw: &[u8]| -> String {
            let h1 = Sha256::digest(raw);
            let h2 = Sha256::digest(h1);
            let mut v = h2.to_vec();
            v.reverse();
            hex::encode(v)
        };

        // Create a chain: missing_ancestor -> parent_tx -> child_tx (which we spend)
        // The missing_ancestor is NOT stored, so depth-2 lookup will fail.

        // Use a fixed 32-byte LE txid for the missing ancestor (not in any table)
        let missing_ancestor_txid =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let ancestor_bytes = hex::decode(missing_ancestor_txid).unwrap();
        let mut ancestor_le = ancestor_bytes.clone();
        ancestor_le.reverse();

        // Parent tx that references the missing ancestor
        let mut parent_raw_tx = vec![];
        parent_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        parent_raw_tx.push(0x01); // 1 input
        parent_raw_tx.extend_from_slice(&ancestor_le); // references missing ancestor
        parent_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
        parent_raw_tx.push(0x00); // empty script
        parent_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        parent_raw_tx.push(0x01); // 1 output
        parent_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        parent_raw_tx.push(0x00); // empty script
        parent_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let parent_txid = compute_txid(&parent_raw_tx);

        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_parent', 0, 1000, 1, 0, 'Parent tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(&parent_txid)
        .bind(&parent_raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        // Child tx that references parent
        let parent_bytes = hex::decode(&parent_txid).unwrap();
        let mut parent_le = parent_bytes.clone();
        parent_le.reverse();

        let mut child_raw_tx = vec![];
        child_raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        child_raw_tx.push(0x01); // 1 input
        child_raw_tx.extend_from_slice(&parent_le); // references parent
        child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // vout
        child_raw_tx.push(0x00); // empty script
        child_raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        child_raw_tx.push(0x01); // 1 output
        child_raw_tx.extend_from_slice(&[0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        child_raw_tx.push(0x00); // empty script
        child_raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        let child_txid = compute_txid(&child_raw_tx);

        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'unproven', 'ref_child', 0, 1000, 1, 0, 'Child tx', ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(&child_txid)
        .bind(&child_raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        // Build BEEF for child_tx — depth 0 = child_tx (found), depth 1 = parent_tx (found),
        // depth 2 = missing_ancestor (NOT found — should warn, not error)
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: child_txid.clone(),
            vout: 0,
            satoshis: 1000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            None,
        )
        .await;

        // Should succeed (missing deep ancestor is a warning, not an error)
        assert!(
            result.is_ok(),
            "Missing deep ancestor should be a warning, not an error: {:?}",
            result
        );
        let beef_bytes = result.unwrap();
        assert!(beef_bytes.is_some(), "BEEF should still be produced");

        // Verify the BEEF contains what it could find (at least 2 txs: child + parent)
        let beef = Beef::from_binary(&beef_bytes.unwrap()).unwrap();
        assert!(
            beef.txs.len() >= 2,
            "BEEF must contain at least the child and parent txs, got {}",
            beef.txs.len()
        );
    }

    #[tokio::test]
    async fn test_raw_tx_stored_at_create_time() {
        // Verify that after create_action + process_action, the raw_tx is stored
        // on the transactions table (not NULL).
        use crate::storage::traits::{AuthId, StorageProcessActionArgs, WalletStorageWriter};
        use sha2::{Digest, Sha256};

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
            description: "Test raw_tx storage".to_string(),
            input_beef: None,
            inputs: None,
            outputs: Some(vec![CreateActionOutput {
                locking_script: locking_script.clone(),
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

        let create_result = create_action_internal(&storage, None, user.user_id, args)
            .await
            .unwrap();

        // Build a raw tx with proper script length varint encoding.
        // Must match the number and locking scripts of outputs from create_action.
        let total_outputs = create_result.outputs.len();
        let mut raw_tx = vec![];
        raw_tx.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // version
        raw_tx.push(0x01); // 1 input (dummy coinbase)
        raw_tx.extend_from_slice(&[0u8; 32]); // txid
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // vout
        raw_tx.push(0x00); // empty unlocking script
        raw_tx.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]); // sequence
        raw_tx.push(total_outputs as u8); // output count
        for output in &create_result.outputs {
            // satoshis (8 bytes LE)
            raw_tx.extend_from_slice(&output.satoshis.to_le_bytes());
            // locking script with varint length prefix
            let script_bytes = hex::decode(&output.locking_script).unwrap_or_default();
            raw_tx.push(script_bytes.len() as u8);
            raw_tx.extend_from_slice(&script_bytes);
        }
        raw_tx.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // locktime

        // Compute txid from the raw_tx
        let hash1 = Sha256::digest(&raw_tx);
        let hash2 = Sha256::digest(hash1);
        let mut txid_bytes = hash2.to_vec();
        txid_bytes.reverse();
        let txid = hex::encode(&txid_bytes);

        // Inject input_beef so process_action doesn't reject us
        let dummy_beef = vec![0x00, 0x01, 0x00];
        sqlx::query("UPDATE transactions SET input_beef = ? WHERE reference = ?")
            .bind(&dummy_beef)
            .bind(&create_result.reference)
            .execute(storage.pool())
            .await
            .unwrap();

        let process_args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: true,
            is_delayed: false,
            reference: Some(create_result.reference.clone()),
            txid: Some(txid.clone()),
            raw_tx: Some(raw_tx.clone()),
            send_with: vec![],
        };

        let auth_id = AuthId::with_user_id("02user_identity_key", user.user_id);
        let _process_result = storage
            .process_action(&auth_id, process_args)
            .await
            .unwrap();

        // Now verify that raw_tx is stored on the transactions table
        let row = sqlx::query("SELECT raw_tx FROM transactions WHERE txid = ?")
            .bind(&txid)
            .fetch_one(storage.pool())
            .await
            .unwrap();

        let stored_raw_tx: Option<Vec<u8>> = row.get("raw_tx");
        assert!(
            stored_raw_tx.is_some(),
            "raw_tx must NOT be NULL on the transactions table after process_action"
        );
        assert_eq!(
            stored_raw_tx.unwrap(),
            raw_tx,
            "stored raw_tx must match the signed transaction"
        );
    }

    #[tokio::test]
    async fn test_build_beef_with_network_fallback() {
        // Set up mock services that return a raw tx for a missing txid.
        // Build BEEF where a parent is missing from local storage.
        // Verify the mock service was called and the BEEF is complete.
        use crate::services::mock::{MockResponse, MockWalletServices};
        use crate::services::traits::GetRawTxResult;
        use sha2::{Digest, Sha256};
        use std::sync::Arc;

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let compute_txid = |raw: &[u8]| -> String {
            let h1 = Sha256::digest(raw);
            let h2 = Sha256::digest(h1);
            let mut v = h2.to_vec();
            v.reverse();
            hex::encode(v)
        };

        // The raw tx that the mock will return (a coinbase tx)
        let missing_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap();

        // Compute the real txid from the raw bytes
        let missing_txid = compute_txid(&missing_raw_tx);

        // Configure mock services to return the raw tx
        let mock_services = MockWalletServices::builder()
            .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
                name: "mock-woc".to_string(),
                txid: missing_txid.clone(),
                raw_tx: Some(missing_raw_tx.clone()),
                error: None,
            }))
            .build();
        storage.set_services(Arc::new(mock_services));

        // Build BEEF for a tx that references the missing txid
        let extended_inputs = vec![ExtendedInput {
            vin: 0,
            txid: missing_txid.clone(),
            vout: 0,
            satoshis: 50_000_000,
            locking_script: vec![],
            unlocking_script_length: 107,
            input_description: None,
            output: None,
        }];
        let change_inputs: Vec<AllocatedChangeInput> = vec![];

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            None,
            &extended_inputs,
            &change_inputs,
            None,
            &[],
            false,
            Some(&storage),
        )
        .await
        .unwrap();

        // BEEF should be produced (network fallback fetched the tx)
        assert!(
            result.is_some(),
            "BEEF should be produced via network fallback"
        );
        let beef_bytes = result.unwrap();
        let beef = Beef::from_binary(&beef_bytes).unwrap();

        // The fetched tx should be in the BEEF
        assert!(
            beef.find_txid(&missing_txid).is_some(),
            "Network-fetched tx must be in the BEEF"
        );

        // Verify the tx was also stored locally for future use
        // Note: proven_tx_reqs is written via the conn (within the same connection),
        // and we need to use the same connection to read it back.
        let stored = sqlx::query("SELECT raw_tx FROM proven_tx_reqs WHERE txid = ?")
            .bind(&missing_txid)
            .fetch_optional(&mut *conn)
            .await
            .unwrap();
        assert!(
            stored.is_some(),
            "Network-fetched tx should be stored locally in proven_tx_reqs"
        );
    }

    // =========================================================================
    // rebuild_beef_for_broadcast Tests
    // =========================================================================

    #[tokio::test]
    async fn test_rebuild_beef_for_broadcast_with_proven_tx() {
        // Set up DB with a proven_txs entry, then call rebuild_beef_for_broadcast.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        // Use a real coinbase tx so the raw_tx parses correctly
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff\
             0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66\
             fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf23\
             42c858eeac00000000",
        )
        .unwrap();
        let txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

        // Create a valid MerklePath for this tx and serialize it
        let bump = bsv_rs::transaction::MerklePath::from_coinbase_txid(txid, 100_000);
        let merkle_path_bytes = bump.to_binary();

        seed_proven_tx(&storage, txid, &raw_tx, &merkle_path_bytes).await;

        let mut conn = storage.pool().acquire().await.unwrap();
        let beef = rebuild_beef_for_broadcast(&mut conn, &[txid.to_string()], None)
            .await
            .unwrap();

        // The tx should be in the BEEF
        assert!(
            beef.find_txid(txid).is_some(),
            "Rebuilt BEEF must contain the requested txid"
        );

        // The tx should have a bump (merkle proof)
        let beef_tx = beef.find_txid(txid).unwrap();
        assert!(
            beef_tx.bump_index().is_some(),
            "Rebuilt BEEF tx should have a merkle proof (bump_index)"
        );
    }

    #[tokio::test]
    async fn test_rebuild_beef_for_broadcast_empty_txids() {
        // Calling with empty txids should return an empty BEEF
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let beef = rebuild_beef_for_broadcast(&mut conn, &[], None)
            .await
            .unwrap();

        assert!(
            beef.txs.is_empty(),
            "Empty input txids should produce an empty BEEF"
        );
    }

    #[tokio::test]
    async fn test_rebuild_beef_for_broadcast_missing_txid() {
        // Calling with a txid not in storage should handle gracefully
        // (no crash, returns BEEF without the missing tx — network fallback
        // requires storage param which we pass as None here)
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let mut conn = storage.pool().acquire().await.unwrap();
        let result = rebuild_beef_for_broadcast(
            &mut conn,
            &["0000000000000000000000000000000000000000000000000000000000ffffff".to_string()],
            None,
        )
        .await;

        // Without network fallback (storage=None), a missing txid at depth 0
        // should error because it's a direct input
        assert!(
            result.is_err(),
            "Missing direct input txid without network fallback should error"
        );
    }

    // =========================================================================
    // UTXO selection preference tests — confirmed (completed) vs unconfirmed
    // =========================================================================

    /// Helper: insert a transaction with a given status and unique reference/txid
    /// derived from `tag`, then insert a change output with the given satoshis
    /// in the default basket. Returns (transaction_id, output_id).
    async fn seed_tagged_change_output(
        storage: &StorageSqlx,
        user_id: i64,
        satoshis: i64,
        status: &str,
        tag: &str,
    ) -> (i64, i64) {
        let now = Utc::now();
        let basket = storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();

        let txid = format!("{:0>64}", tag);
        let reference = format!("seed_ref_{}", tag);
        let raw_tx = seed_raw_tx();

        let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, ?, ?, 0, ?, 1, 0, 'Seed transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(&reference)
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let transaction_id = tx_result.last_insert_rowid();

        let locking_script =
            hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap();

        let out_result = sqlx::query(
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
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        let output_id = out_result.last_insert_rowid();
        (transaction_id, output_id)
    }

    /// Helper: create a dummy spending transaction to use as the `transaction_id`
    /// parameter for `allocate_change_input` (so `spent_by` FK is valid).
    async fn create_spending_tx(storage: &StorageSqlx, user_id: i64, tag: &str) -> i64 {
        let now = Utc::now();
        let txid = format!("{:0>64}", format!("spend_{}", tag));
        let reference = format!("spending_ref_{}", tag);
        let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at)
            VALUES (?, 'unsigned', ?, 1, 0, 1, 0, 'Spending tx', ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(&reference)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        result.last_insert_rowid()
    }

    #[tokio::test]
    async fn test_confirmed_utxo_preferred_over_unconfirmed() {
        // When both a confirmed ('completed') and unconfirmed ('unproven') output
        // of equal value exist, allocate_change_input should pick the confirmed one.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Insert confirmed (completed) output of 5000 sats
        let (_confirmed_tx_id, confirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 5000, "completed", "conf01").await;

        // Insert unconfirmed (unproven) output of 5000 sats
        let (_unconfirmed_tx_id, _unconfirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 5000, "unproven", "unpr01").await;

        // Create a dummy spending transaction for the allocation
        let spending_tx_id = create_spending_tx(&storage, user.user_id, "test1").await;

        let mut conn = storage.pool().acquire().await.unwrap();

        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            spending_tx_id,
            5000,
            true,
        )
        .await
        .unwrap();

        assert!(allocated.is_some(), "Should allocate an output");
        let allocated = allocated.unwrap();

        // The confirmed output should be selected
        assert_eq!(
            allocated.output_id, confirmed_out_id,
            "Confirmed (completed) output should be preferred over unconfirmed (unproven)"
        );
        assert_eq!(allocated.satoshis, 5000);

        // Verify the output was marked as allocated (spent_by set, spendable cleared)
        let row: (Option<i64>, i64) =
            sqlx::query_as("SELECT spent_by, spendable FROM outputs WHERE output_id = ?")
                .bind(confirmed_out_id)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(
            row.0,
            Some(spending_tx_id),
            "spent_by should be set to spending tx"
        );
        assert_eq!(row.1, 0, "spendable should be 0 (false) after allocation");
    }

    #[tokio::test]
    async fn test_unconfirmed_utxo_used_when_no_confirmed_available() {
        // When only an unconfirmed ('unproven') output exists,
        // allocate_change_input should still pick it.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Insert only an unconfirmed (unproven) output of 5000 sats
        let (_unconfirmed_tx_id, unconfirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 5000, "unproven", "unpr02").await;

        let spending_tx_id = create_spending_tx(&storage, user.user_id, "test2").await;

        let mut conn = storage.pool().acquire().await.unwrap();

        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            spending_tx_id,
            5000,
            true,
        )
        .await
        .unwrap();

        assert!(
            allocated.is_some(),
            "Should allocate an unconfirmed output when no confirmed ones exist"
        );
        let allocated = allocated.unwrap();

        assert_eq!(
            allocated.output_id, unconfirmed_out_id,
            "The unconfirmed output should be selected"
        );
        assert_eq!(allocated.satoshis, 5000);
    }

    #[tokio::test]
    async fn test_confirmed_preferred_even_if_worse_amount_fit() {
        // A confirmed output of 10000 sats should be preferred over an
        // unconfirmed output of 5000 sats even when requesting ~4500 sats
        // (the unconfirmed one is a closer fit by amount).
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Insert confirmed (completed) output of 10000 sats — farther from target
        let (_confirmed_tx_id, confirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 10_000, "completed", "conf03").await;

        // Insert unconfirmed (unproven) output of 5000 sats — closer to target
        let (_unconfirmed_tx_id, _unconfirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 5_000, "unproven", "unpr03").await;

        let spending_tx_id = create_spending_tx(&storage, user.user_id, "test3").await;

        let mut conn = storage.pool().acquire().await.unwrap();

        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            spending_tx_id,
            4500,
            true,
        )
        .await
        .unwrap();

        assert!(allocated.is_some(), "Should allocate an output");
        let allocated = allocated.unwrap();

        // The confirmed output (10000) should be selected, even though the
        // unconfirmed one (5000) is closer to the target (4500).
        // The ORDER BY sorts by status first: completed=0 < unproven=1.
        assert_eq!(
            allocated.output_id, confirmed_out_id,
            "Confirmed output should be preferred even when unconfirmed is a closer amount fit"
        );
        assert_eq!(allocated.satoshis, 10_000);
    }

    /// When proven outputs exist but are too small to cover the target,
    /// coin selection MUST fall back to unproven outputs instead of failing.
    /// This was the bug: 300 sat proven output selected over 8.9M unproven → "Insufficient funds".
    #[tokio::test]
    async fn test_fallback_to_unproven_when_proven_insufficient() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();

        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        let basket = storage
            .find_or_create_default_basket(user.user_id)
            .await
            .unwrap();

        // Insert a tiny confirmed output (300 sats) — proven but too small
        let (_confirmed_tx_id, _confirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 300, "completed", "tiny01").await;

        // Insert a large unconfirmed output (9_000_000 sats) — unproven but big enough
        let (_unconfirmed_tx_id, unconfirmed_out_id) =
            seed_tagged_change_output(&storage, user.user_id, 9_000_000, "unproven", "big01").await;

        let spending_tx_id = create_spending_tx(&storage, user.user_id, "spend1").await;
        let mut conn = storage.pool().acquire().await.unwrap();

        // Target: 270_000 sats (typical x402 upfront payment)
        let allocated = allocate_change_input(
            &mut conn,
            user.user_id,
            basket.basket_id,
            spending_tx_id,
            270_000,
            true,
        )
        .await
        .unwrap();

        assert!(
            allocated.is_some(),
            "Must allocate even when proven output is too small — should fall back to unproven"
        );
        let allocated = allocated.unwrap();
        assert_eq!(
            allocated.output_id, unconfirmed_out_id,
            "Should select the large unproven output, not the tiny proven one"
        );
        assert_eq!(allocated.satoshis, 9_000_000);
    }

    #[test]
    fn test_beef_walk_skips_stored_beef_for_proven_tx() {
        // Verify the decision logic: when an ancestor tx has a merkle proof,
        // stored input_beef should NOT be merged (BUMP terminates the chain).
        // This matches TS/Go behavior where proven txs skip stored BEEF merge.

        // Case 1: BeefTxData with merkle_path present -> has_proof = true
        // A proven tx should skip stored BEEF merge.
        let tx_data_opt: Option<BeefTxData> = Some(BeefTxData {
            raw_tx: vec![0x01, 0x00, 0x00, 0x00],
            merkle_path: Some(vec![0xDE, 0xAD]),
        });
        let has_proof = tx_data_opt
            .as_ref()
            .map(|d| d.merkle_path.is_some())
            .unwrap_or(false);
        assert!(
            has_proof,
            "Proven tx (merkle_path Some) must set has_proof = true, skipping stored BEEF merge"
        );

        // Case 2: BeefTxData with merkle_path None -> has_proof = false
        // An unproven tx should merge stored BEEF.
        let tx_data_opt: Option<BeefTxData> = Some(BeefTxData {
            raw_tx: vec![0x01, 0x00, 0x00, 0x00],
            merkle_path: None,
        });
        let has_proof = tx_data_opt
            .as_ref()
            .map(|d| d.merkle_path.is_some())
            .unwrap_or(false);
        assert!(
            !has_proof,
            "Unproven tx (merkle_path None) must set has_proof = false, allowing stored BEEF merge"
        );

        // Case 3: No tx data at all -> has_proof = false
        // Should also attempt stored BEEF merge (fallback path).
        let tx_data_opt: Option<BeefTxData> = None;
        let has_proof = tx_data_opt
            .as_ref()
            .map(|d| d.merkle_path.is_some())
            .unwrap_or(false);
        assert!(
            !has_proof,
            "Missing tx data (None) must set has_proof = false, allowing stored BEEF merge"
        );
    }

    // =========================================================================
    // Caller-complete funding rule (sighash-aware)
    // =========================================================================

    /// A structurally valid `<DER sig><sighash>` push with 32-byte r and s.
    fn fake_der_signature(sighash: u8) -> Vec<u8> {
        let mut sig = vec![0x30, 0x44, 0x02, 0x20];
        sig.extend(std::iter::repeat_n(0x11u8, 32));
        sig.extend([0x02, 0x20]);
        sig.extend(std::iter::repeat_n(0x22u8, 32));
        sig.push(sighash);
        sig
    }

    /// `<sig> <pubkey>` — a P2PKH-shaped unlocking script signed with `sighash`.
    fn p2pkh_unlock(sighash: u8) -> Vec<u8> {
        let sig = fake_der_signature(sighash);
        let mut script = vec![sig.len() as u8];
        script.extend(sig);
        script.push(33);
        script.extend(std::iter::repeat_n(0x02u8, 33));
        script
    }

    #[test]
    fn test_der_signature_sighash_reads_the_trailing_flag_byte() {
        assert_eq!(der_signature_sighash(&fake_der_signature(0x41)), Some(0x41));
        assert_eq!(der_signature_sighash(&fake_der_signature(0xc3)), Some(0xc3));
        // Minimal DER (1-byte r and s) + flags = 9 bytes.
        assert_eq!(
            der_signature_sighash(&[0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01, 0x43]),
            Some(0x43)
        );
        // Not a signature: a pubkey, a hash, empty, wrong tags, bad base type.
        let mut pubkey = vec![0x02u8];
        pubkey.extend(std::iter::repeat_n(0xabu8, 32));
        assert_eq!(der_signature_sighash(&pubkey), None);
        assert_eq!(der_signature_sighash(&[0xaa; 32]), None);
        assert_eq!(der_signature_sighash(&[]), None);
        let mut wrong_len = fake_der_signature(0x41);
        wrong_len[1] = 0x40;
        assert_eq!(der_signature_sighash(&wrong_len), None);
        assert_eq!(der_signature_sighash(&fake_der_signature(0x40)), None);
        assert_eq!(der_signature_sighash(&fake_der_signature(0x44)), None);
    }

    #[test]
    fn test_script_pushes_handles_every_push_encoding_and_stops_on_truncation() {
        // direct push, OP_PUSHDATA1, OP_PUSHDATA2, an opcode, OP_0
        let mut script = vec![0x02, 0xaa, 0xbb];
        script.extend([0x4c, 0x01, 0xcc]);
        script.extend([0x4d, 0x02, 0x00, 0xdd, 0xee]);
        script.push(0xac); // OP_CHECKSIG
        script.push(0x00); // OP_0
        let pushes = script_pushes(&script);
        assert_eq!(
            pushes,
            vec![&[0xaa, 0xbb][..], &[0xcc][..], &[0xdd, 0xee][..]]
        );

        // A push that claims more bytes than remain ends the walk.
        let truncated = vec![0x01, 0xaa, 0x05, 0xbb];
        assert_eq!(script_pushes(&truncated), vec![&[0xaa][..]]);
        assert!(script_pushes(&[]).is_empty());
    }

    #[test]
    fn test_input_commitment_by_sighash() {
        // SIGHASH_ALL|FORKID and ALL|FORKID|... commit to the whole tx.
        assert_eq!(
            input_commitment(&p2pkh_unlock(0x41)),
            InputCommitment::WholeTx
        );
        assert_eq!(
            input_commitment(&p2pkh_unlock(0x01)),
            InputCommitment::WholeTx
        );
        // ALL|ANYONECANPAY seals the output set: still not fundable.
        assert_eq!(
            input_commitment(&p2pkh_unlock(0xc1)),
            InputCommitment::WholeTx
        );
        // SINGLE|ANYONECANPAY|FORKID (the Zanaadu covenant) and NONE|ANYONECANPAY
        // commit to their own input (+ own output) only.
        assert_eq!(
            input_commitment(&p2pkh_unlock(0xc3)),
            InputCommitment::OwnOnly
        );
        assert_eq!(
            input_commitment(&p2pkh_unlock(0xc2)),
            InputCommitment::OwnOnly
        );
        assert_eq!(
            input_commitment(&p2pkh_unlock(0x83)),
            InputCommitment::OwnOnly
        );
        // A covenant unlock: preimage-like data pushes around the signature —
        // position-independent detection.
        let mut covenant = vec![0x4c, 0x60];
        covenant.extend(std::iter::repeat_n(0x01u8, 0x60));
        let sig = fake_der_signature(0xc3);
        covenant.push(sig.len() as u8);
        covenant.extend(sig);
        covenant.push(0x51); // OP_1
        assert_eq!(input_commitment(&covenant), InputCommitment::OwnOnly);
        // Two signatures, one of them SIGHASH_ALL: the whole tx is committed.
        let mut multisig = vec![0x00];
        for flags in [0xc3u8, 0x41] {
            let sig = fake_der_signature(flags);
            multisig.push(sig.len() as u8);
            multisig.extend(sig);
        }
        assert_eq!(input_commitment(&multisig), InputCommitment::WholeTx);
        // No signature at all: nothing constrains the shape.
        assert_eq!(input_commitment(&[0x51]), InputCommitment::Unconstrained);
        assert_eq!(
            input_commitment(&[0x04, 0xde, 0xad, 0xbe, 0xef]),
            InputCommitment::Unconstrained
        );
        assert_eq!(input_commitment(&[]), InputCommitment::Unconstrained);
    }

    #[test]
    fn test_caller_complete_plan_rule() {
        use CallerCompletePlan::*;
        // Self-sufficient, signatures forbid funding: verbatim whatever the fee.
        assert_eq!(caller_complete_plan(5_000, 4_000, 100, false), Verbatim);
        assert_eq!(caller_complete_plan(4_000, 4_000, 100, false), Verbatim);
        // Shortfall, signatures forbid funding: hard insufficient funds.
        assert_eq!(caller_complete_plan(1, 1_001, 100, false), Unfundable);
        // Shortfall, signatures permit funding: fund it.
        assert_eq!(caller_complete_plan(1, 1_001, 100, true), Fund);
        // Fundable and the surplus covers our fee: verbatim (surplus = caller's fee).
        assert_eq!(caller_complete_plan(5_000, 4_000, 100, true), Verbatim);
        assert_eq!(caller_complete_plan(4_100, 4_000, 100, true), Verbatim);
        // Fundable but the surplus is below our fee: fund rather than 465.
        assert_eq!(caller_complete_plan(4_099, 4_000, 100, true), Fund);
        assert_eq!(caller_complete_plan(4_000, 4_000, 100, true), Fund);
    }

    /// Seed a spendable, NON-change output (a covenant UTXO the caller will spend
    /// with its own unlocking script). Returns the parent txid (hex).
    async fn seed_custom_output(
        storage: &StorageSqlx,
        user_id: i64,
        satoshis: i64,
        locking_script: &[u8],
        tag: &str,
    ) -> String {
        let now = Utc::now();
        let txid = format!("{:0>64}", tag);
        let raw_tx = seed_raw_tx();
        let tx_result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
            VALUES (?, 'completed', ?, 1, ?, 1, 0, 'Seed covenant transaction', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(format!("seed_custom_{}", tag))
        .bind(satoshis)
        .bind(&txid)
        .bind(&raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
        let transaction_id = tx_result.last_insert_rowid();

        sqlx::query(
            r#"
            INSERT INTO outputs (
                user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                txid, type, spendable, change, provided_by, purpose, output_description,
                created_at, updated_at
            )
            VALUES (?, ?, NULL, 0, ?, ?, ?, 'custom', 1, 0, 'you', '', 'seeded covenant', ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(satoshis)
        .bind(locking_script)
        .bind(&txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        txid
    }

    fn covenant_input(txid: &str, unlocking_script: Vec<u8>) -> bsv_rs::wallet::CreateActionInput {
        bsv_rs::wallet::CreateActionInput {
            outpoint: bsv_rs::wallet::Outpoint::from_string(&format!("{}.0", txid)).unwrap(),
            input_description: "covenant input".to_string(),
            unlocking_script: Some(unlocking_script),
            unlocking_script_length: None,
            sequence_number: None,
        }
    }

    fn p2pkh_output(satoshis: u64, description: &str) -> CreateActionOutput {
        CreateActionOutput {
            locking_script: hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac")
                .unwrap(),
            satoshis,
            output_description: description.to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }
    }

    async fn caller_complete_storage() -> (StorageSqlx, i64) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test-wallet", "02test_key").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage
            .find_or_insert_user("02user_identity_key")
            .await
            .unwrap();
        (storage, user.user_id)
    }

    /// The Zanaadu V3 publish shape: ONE covenant input signed
    /// SIGHASH_SINGLE|ANYONECANPAY|FORKID (0xc3), 1 sat in, 1,001 sats out. The
    /// wallet must append a funding input and a change output, and must leave the
    /// caller's input at vin 0 and outputs at vout 0/1 untouched.
    #[tokio::test]
    async fn test_caller_complete_anyonecanpay_shortfall_gets_funding_input_and_change() {
        let (storage, user_id) = caller_complete_storage().await;
        let covenant_lock = vec![0x51u8]; // OP_TRUE stand-in for the covenant script
        let cov_txid = seed_custom_output(&storage, user_id, 1, &covenant_lock, "c0c3").await;
        seed_change_output(&storage, user_id, 100_000).await;

        let unlock = p2pkh_unlock(0xc3);
        let args = bsv_rs::wallet::CreateActionArgs {
            description: "V3 publish".to_string(),
            input_beef: None,
            inputs: Some(vec![covenant_input(&cov_txid, unlock.clone())]),
            outputs: Some(vec![
                CreateActionOutput {
                    locking_script: covenant_lock.clone(),
                    satoshis: 1,
                    output_description: "next covenant state".to_string(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                },
                p2pkh_output(1_000, "platform fee"),
            ]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user_id, args)
            .await
            .expect("an ANYONECANPAY shortfall must be funded, not rejected");

        // The caller's input stays first; the wallet's funding is appended after it.
        assert_eq!(result.inputs[0].vin, 0);
        assert_eq!(result.inputs[0].source_txid, cov_txid);
        assert_eq!(result.inputs[0].source_vout, 0);
        assert_eq!(result.inputs[0].source_satoshis, 1);
        assert_eq!(result.inputs[0].provided_by, StorageProvidedBy::You);
        assert_eq!(
            result.inputs[0].unlocking_script_length as usize,
            unlock.len()
        );
        assert!(
            result.inputs.len() >= 2,
            "a funding input must be appended, got {:?}",
            result.inputs
        );
        for (i, input) in result.inputs.iter().enumerate().skip(1) {
            assert_eq!(input.vin as usize, i);
            assert_eq!(input.provided_by, StorageProvidedBy::Storage);
        }

        // The caller's outputs keep vout 0 and 1 byte-for-byte; change is appended.
        assert_eq!(result.outputs[0].vout, 0);
        assert_eq!(result.outputs[0].satoshis, 1);
        assert_eq!(
            result.outputs[0].locking_script,
            hex::encode(&covenant_lock)
        );
        assert_eq!(result.outputs[1].vout, 1);
        assert_eq!(result.outputs[1].satoshis, 1_000);
        let change: Vec<_> = result
            .outputs
            .iter()
            .filter(|o| o.purpose.as_deref() == Some("change"))
            .collect();
        assert!(!change.is_empty(), "a change output must be appended");
        assert!(change.iter().all(|o| o.vout >= 2));

        // Funded: inputs cover outputs plus a positive miner fee.
        let in_total: u64 = result.inputs.iter().map(|i| i.source_satoshis).sum();
        let out_total: u64 = result.outputs.iter().map(|o| o.satoshis).sum();
        assert!(
            in_total > out_total,
            "in {} must exceed out {}",
            in_total,
            out_total
        );
        assert!(
            in_total - out_total < 1_000,
            "fee {} is implausibly large",
            in_total - out_total
        );
    }

    /// Same shortfall, but the caller signed SIGHASH_ALL|FORKID (0x41): the
    /// signatures commit to every input and output, so funding is impossible and
    /// the shortfall is a hard InsufficientFunds even though change is available.
    #[tokio::test]
    async fn test_caller_complete_sighash_all_shortfall_is_insufficient_funds() {
        let (storage, user_id) = caller_complete_storage().await;
        let cov_txid = seed_custom_output(&storage, user_id, 1, &[0x51], "a041").await;
        seed_change_output(&storage, user_id, 100_000).await;

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "sealed shortfall".to_string(),
            input_beef: None,
            inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0x41))]),
            outputs: Some(vec![p2pkh_output(1_000, "payment")]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let err = create_action_internal(&storage, None, user_id, args)
            .await
            .expect_err("a SIGHASH_ALL shortfall cannot be funded");
        assert!(
            matches!(
                err,
                Error::InsufficientFunds {
                    needed: 1_000,
                    available: 1
                }
            ),
            "got {:?}",
            err
        );

        // Nothing was allocated: the change UTXO is still spendable.
        let (spendable, spent_by): (i64, Option<i64>) = sqlx::query_as(
            "SELECT spendable, spent_by FROM outputs WHERE \"change\" = 1 AND satoshis = 100000",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(spendable, 1);
        assert_eq!(spent_by, None);
    }

    /// The b23a960 regression: a fully SIGHASH_ALL 2-in/3-out transaction that
    /// already covers its outputs is broadcast verbatim — no funding input and no
    /// change output are injected even though change UTXOs are available.
    #[tokio::test]
    async fn test_caller_complete_sighash_all_self_sufficient_is_verbatim() {
        let (storage, user_id) = caller_complete_storage().await;
        let in_a = seed_custom_output(&storage, user_id, 30_000, &[0x51], "a1").await;
        let in_b = seed_custom_output(&storage, user_id, 30_000, &[0x51], "b2").await;
        seed_change_output(&storage, user_id, 100_000).await;

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "2-of-2 covenant settle".to_string(),
            input_beef: None,
            inputs: Some(vec![
                covenant_input(&in_a, p2pkh_unlock(0x41)),
                covenant_input(&in_b, p2pkh_unlock(0x41)),
            ]),
            outputs: Some(vec![
                p2pkh_output(20_000, "party a"),
                p2pkh_output(20_000, "party b"),
                p2pkh_output(19_500, "party c"),
            ]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user_id, args)
            .await
            .expect("self-sufficient caller-complete tx");
        assert_eq!(result.inputs.len(), 2, "no funding input may be injected");
        assert_eq!(result.outputs.len(), 3, "no change output may be injected");
        assert!(result
            .inputs
            .iter()
            .all(|i| i.provided_by == StorageProvidedBy::You));
        assert!(result
            .outputs
            .iter()
            .all(|o| o.purpose.as_deref() != Some("change")));
        assert_eq!(result.inputs[0].source_txid, in_a);
        assert_eq!(result.inputs[1].source_txid, in_b);
    }

    /// An ANYONECANPAY|SINGLE input whose surplus already covers the wallet's fee
    /// is self-sufficient too: the surplus is the caller's chosen miner fee and
    /// the bytes go out verbatim.
    #[tokio::test]
    async fn test_caller_complete_anyonecanpay_self_sufficient_is_verbatim() {
        let (storage, user_id) = caller_complete_storage().await;
        let cov_txid = seed_custom_output(&storage, user_id, 5_000, &[0x51], "c5c3").await;
        seed_change_output(&storage, user_id, 100_000).await;

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "covenant close".to_string(),
            input_beef: None,
            inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0xc3))]),
            outputs: Some(vec![p2pkh_output(4_000, "payout")]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user_id, args)
            .await
            .unwrap();
        assert_eq!(result.inputs.len(), 1);
        assert_eq!(result.outputs.len(), 1);
    }

    /// An ANYONECANPAY|SINGLE input that covers its outputs but not the wallet's
    /// miner fee is funded instead of being sent out to a "fee too low" rejection.
    #[tokio::test]
    async fn test_caller_complete_anyonecanpay_fee_gap_is_funded() {
        let (storage, user_id) = caller_complete_storage().await;
        let cov_txid = seed_custom_output(&storage, user_id, 1_001, &[0x51], "c1c3").await;
        seed_change_output(&storage, user_id, 100_000).await;

        let args = bsv_rs::wallet::CreateActionArgs {
            description: "zero-fee covenant".to_string(),
            input_beef: None,
            inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0xc3))]),
            outputs: Some(vec![p2pkh_output(1_000, "payment")]),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = create_action_internal(&storage, None, user_id, args)
            .await
            .unwrap();
        assert!(result.inputs.len() >= 2, "the fee gap must be funded");
        assert_eq!(result.inputs[0].source_txid, cov_txid);
        assert_eq!(result.outputs[0].vout, 0);
        assert_eq!(result.outputs[0].satoshis, 1_000);
        let in_total: u64 = result.inputs.iter().map(|i| i.source_satoshis).sum();
        let out_total: u64 = result.outputs.iter().map(|o| o.satoshis).sum();
        assert!(in_total > out_total);
    }

    // =========================================================================
    // BEEF closure: a proven ancestor terminates the chain
    //
    // Mirrors the TypeScript builder in
    // packages/wallet/wallet-toolbox/src/storage/methods/getBeefForTransaction.ts:
    // a stored proven transaction contributes its raw bytes plus its BUMP and
    // returns no dependencies, an unproven one contributes its bytes and its
    // input txids, and knownTxids collapse to txid-only entries.
    // =========================================================================

    /// A minimal spend of `parent:vout`: one input, one OP_TRUE output.
    /// Returns the raw bytes and the txid.
    fn synth_tx(parent_txid: &str, vout: u32, satoshis: u64) -> (Vec<u8>, String) {
        let mut raw: Vec<u8> = Vec::new();
        raw.extend_from_slice(&1u32.to_le_bytes()); // version
        raw.push(1); // input count
        let mut prev = hex::decode(parent_txid).unwrap();
        prev.reverse();
        raw.extend_from_slice(&prev);
        raw.extend_from_slice(&vout.to_le_bytes());
        raw.push(0); // empty unlocking script
        raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // sequence
        raw.push(1); // output count
        raw.extend_from_slice(&satoshis.to_le_bytes());
        raw.push(1); // locking script length
        raw.push(0x51); // OP_TRUE
        raw.extend_from_slice(&0u32.to_le_bytes()); // locktime

        let txid = txid_of(&raw);
        (raw, txid)
    }

    fn txid_of(raw: &[u8]) -> String {
        use bsv_rs::primitives::hash::sha256d;
        let mut h = sha256d(raw);
        h.reverse();
        hex::encode(h)
    }

    /// A single-leaf merkle path: the txid is the only transaction in its block,
    /// so the computed root is the txid itself.
    fn synth_bump(height: u32, txid: &str) -> MerklePath {
        MerklePath {
            block_height: height,
            path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
                offset: 0,
                hash: Some(txid.to_string()),
                txid: true,
                duplicate: false,
            }]],
        }
    }

    /// A chain of `n` spends. `chain[0]` is the oldest. Every hop spends the
    /// previous hop's output 0, which is exactly the shape a covenant UTXO
    /// traded from wallet to wallet produces.
    fn synth_chain(n: usize) -> Vec<(Vec<u8>, String)> {
        let mut out = Vec::new();
        let mut parent = "11".repeat(32);
        for i in 0..n {
            let (raw, txid) = synth_tx(&parent, 0, 10_000 - i as u64);
            parent = txid.clone();
            out.push((raw, txid));
        }
        out
    }

    fn beef_txids(beef: &Beef) -> HashSet<String> {
        beef.txs.iter().map(|t| t.txid()).collect()
    }

    async fn seed_proven_chain_tx(storage: &StorageSqlx, txid: &str, raw_tx: &[u8], height: u32) {
        let bump = synth_bump(height, txid);
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root,
                                    merkle_path, raw_tx, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(txid)
        .bind(height as i64)
        .bind(format!("block_{}", height))
        .bind(txid)
        .bind(bump.to_binary())
        .bind(raw_tx)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    async fn seed_unproven_tx(
        storage: &StorageSqlx,
        txid: &str,
        raw_tx: &[u8],
        input_beef: Option<&[u8]>,
    ) {
        let now = Utc::now();
        let (user, _) = storage.find_or_insert_user("02abcd").await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis,
                                      version, lock_time, description, txid, raw_tx, input_beef,
                                      created_at, updated_at)
            VALUES (?, 'unproven', ?, 1, 1000, 1, 0, 'chain hop', ?, ?, ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind(format!("ref_{}", &txid[..8]))
        .bind(txid)
        .bind(raw_tx)
        .bind(input_beef)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    #[test]
    fn test_describe_invalid_beef_names_the_missing_parent_and_dumps_the_bytes() {
        let chain = synth_chain(3); // a <- b <- c
        let mut beef = Beef::new();
        // c spends b, and b is absent: the BEEF cannot be valid.
        beef.merge_raw_tx(chain[2].0.clone(), None);
        assert!(
            !beef.verify_valid(true).valid,
            "the shape under test is invalid"
        );
        let text = describe_invalid_beef(&mut beef, &[chain[2].1.clone()]);
        let b16: String = chain[1].1.chars().take(16).collect();
        let c16: String = chain[2].1.chars().take(16).collect();
        assert!(
            text.contains(&format!("missing inputs [{b16}]")),
            "names the absent parent: {text}"
        );
        assert!(
            text.contains(&format!("transactions with missing inputs [{c16}]")),
            "names the child that needs it: {text}"
        );
        assert!(
            text.contains(&format!("roots [{c16}]")),
            "names the root: {text}"
        );
        assert!(
            text.contains("dump /tmp/beef-rejected-"),
            "points at the dump: {text}"
        );
        let path = text.rsplit("dump ").next().unwrap().to_string();
        let dumped = std::fs::read_to_string(&path).expect("the dump exists");
        assert_eq!(
            dumped,
            hex::encode(beef.to_binary()),
            "the dump is the rejected bytes"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_prune_stops_at_a_proven_ancestor() {
        let chain = synth_chain(3); // a <- b <- c
        let mut beef = Beef::new();
        let bump_idx = beef.merge_bump(synth_bump(900_000, &chain[1].1));
        beef.merge_raw_tx(chain[0].0.clone(), None);
        beef.merge_raw_tx(chain[1].0.clone(), Some(bump_idx));
        beef.merge_raw_tx(chain[2].0.clone(), None);

        prune_beef_to_roots(&mut beef, &[chain[2].1.clone()]);

        let kept = beef_txids(&beef);
        assert_eq!(kept.len(), 2, "only the root and its proven parent remain");
        assert!(kept.contains(&chain[2].1), "the root is kept");
        assert!(kept.contains(&chain[1].1), "the proven parent is kept");
        assert!(
            !kept.contains(&chain[0].1),
            "the proven parent's own parent must be dropped"
        );
        assert_eq!(beef.bumps.len(), 1, "the surviving bump is kept");
        assert!(beef.verify_valid(true).valid, "the pruned BEEF is valid");
    }

    #[test]
    fn test_prune_carries_an_unproven_ancestor_whole() {
        let chain = synth_chain(3);
        let mut beef = Beef::new();
        for (raw, _) in &chain {
            beef.merge_raw_tx(raw.clone(), None);
        }

        prune_beef_to_roots(&mut beef, &[chain[2].1.clone()]);

        assert_eq!(
            beef.txs.len(),
            3,
            "with no proof anywhere the whole chain is still required"
        );
    }

    #[test]
    fn test_prune_ten_hops_with_the_middle_ones_proven() {
        let chain = synth_chain(10);
        let mut beef = Beef::new();
        for (i, (raw, txid)) in chain.iter().enumerate() {
            // Hops 3, 4 and 5 were mined; the rest are still unproven.
            if (3..=5).contains(&i) {
                let idx = beef.merge_bump(synth_bump(965_000 + i as u32, txid));
                beef.merge_raw_tx(raw.clone(), Some(idx));
            } else {
                beef.merge_raw_tx(raw.clone(), None);
            }
        }
        assert_eq!(beef.txs.len(), 10);

        prune_beef_to_roots(&mut beef, &[chain[9].1.clone()]);

        // Walking back from hop 9: 9, 8, 7, 6 are unproven and needed; hop 5 is
        // proven so it terminates the chain; hops 0..=4 are gone.
        let kept = beef_txids(&beef);
        let expected: HashSet<String> = chain[5..].iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(kept, expected, "exactly hops 5..9 survive");
        assert_eq!(beef.bumps.len(), 1, "only hop 5's bump is still referenced");
        assert!(beef.verify_valid(true).valid);
    }

    #[test]
    fn test_prune_keeps_an_unproven_root_below_a_proven_root() {
        // Both an unproven transaction and a proven descendant of it are being
        // spent. The unproven one still needs its own history, so it must
        // survive even though a proven transaction descends from it. This is
        // the case a root-blind trim gets wrong.
        let chain = synth_chain(3); // a <- b <- c
        let mut beef = Beef::new();
        beef.merge_raw_tx(chain[0].0.clone(), None);
        beef.merge_raw_tx(chain[1].0.clone(), None);
        let idx = beef.merge_bump(synth_bump(900_001, &chain[2].1));
        beef.merge_raw_tx(chain[2].0.clone(), Some(idx));

        prune_beef_to_roots(&mut beef, &[chain[1].1.clone(), chain[2].1.clone()]);

        let kept = beef_txids(&beef);
        assert!(kept.contains(&chain[2].1), "the proven root is kept");
        assert!(kept.contains(&chain[1].1), "the unproven root is kept");
        assert!(
            kept.contains(&chain[0].1),
            "the unproven root still needs its own parent"
        );
    }

    #[test]
    fn test_prune_leaves_a_beef_alone_when_no_root_is_present() {
        let chain = synth_chain(2);
        let mut beef = Beef::new();
        for (raw, _) in &chain {
            beef.merge_raw_tx(raw.clone(), None);
        }
        prune_beef_to_roots(&mut beef, &["00".repeat(32)]);
        assert_eq!(beef.txs.len(), 2, "pruning must never empty a BEEF");
    }

    #[tokio::test]
    async fn test_walk_terminates_at_a_proven_ancestor() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(3);
        seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
        seed_proven_chain_tx(&storage, &chain[1].1, &chain[1].0, 965_100).await;
        seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;

        let mut beef = Beef::new();
        let mut pending = vec![(chain[2].1.clone(), 0usize)];
        let mut processed = HashSet::new();
        let mut conn = storage.pool().acquire().await.unwrap();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            None,
            None,
        )
        .await
        .unwrap();

        let kept = beef_txids(&beef);
        assert!(kept.contains(&chain[2].1), "the subject is carried whole");
        assert!(
            kept.contains(&chain[1].1),
            "the proven parent contributes its transaction"
        );
        assert_eq!(beef.bumps.len(), 1, "and its BUMP");
        assert!(
            !kept.contains(&chain[0].1),
            "the recursion stops at the proven parent"
        );
    }

    #[tokio::test]
    async fn test_walk_carries_an_unproven_ancestor_whole() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(3);
        seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_050).await;
        seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;
        seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;

        let mut beef = Beef::new();
        let mut pending = vec![(chain[2].1.clone(), 0usize)];
        let mut processed = HashSet::new();
        let mut conn = storage.pool().acquire().await.unwrap();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            None,
            None,
        )
        .await
        .unwrap();

        let kept = beef_txids(&beef);
        assert_eq!(kept.len(), 3, "the unproven middle hop is carried whole");
        assert!(kept.contains(&chain[1].1));
        assert_eq!(beef.bumps.len(), 1, "the walk still reaches the proof");
    }

    #[tokio::test]
    async fn test_walk_honours_a_bump_already_in_the_beef() {
        // The parent has no proven_txs row of its own, but the caller's
        // inputBEEF already carries a BUMP for it. That proof is the evidence:
        // the parent's stored ancestry must not be dragged in behind it.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(3);

        // The parent's stored input_beef still holds the whole unproven history.
        let mut stored = Beef::new();
        stored.merge_raw_tx(chain[0].0.clone(), None);
        let stored_bytes = stored.to_binary();

        seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
        seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, Some(&stored_bytes)).await;

        let mut beef = Beef::new();
        beef.merge_bump(synth_bump(965_200, &chain[1].1));
        let mut pending = vec![(chain[1].1.clone(), 0usize)];
        let mut processed = HashSet::new();
        let mut conn = storage.pool().acquire().await.unwrap();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            None,
            None,
        )
        .await
        .unwrap();

        let kept = beef_txids(&beef);
        assert!(kept.contains(&chain[1].1), "the proven parent is carried");
        assert!(
            !kept.contains(&chain[0].1),
            "a BUMP in the BEEF terminates the chain, stored ancestry and all"
        );
    }

    #[tokio::test]
    async fn test_walk_links_a_root_that_a_siblings_bump_carries_unflagged() {
        // LOW seat p25 (2026-09-04): two roots mined as siblings in one block.
        // Root A's proof was merged first, and the combined BUMP carries B's
        // hash only as A's sibling (txid: false). B has stored history behind
        // it. Walking B must link it to that BUMP, not leave it unproven with
        // a parent the walk never fetched.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();
        let chain = synth_chain(2); // parent <- B
        let (raw_b, txid_b) = (chain[1].0.clone(), chain[1].1.clone());
        let txid_a = "aa".repeat(32);
        seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
        seed_unproven_tx(&storage, &txid_b, &raw_b, None).await;
        let mut beef = Beef::new();
        beef.merge_bump(MerklePath {
            block_height: 964_422,
            path: vec![vec![
                bsv_rs::transaction::MerklePathLeaf {
                    offset: 0,
                    hash: Some(txid_a.clone()),
                    txid: true,
                    duplicate: false,
                },
                bsv_rs::transaction::MerklePathLeaf {
                    offset: 1,
                    hash: Some(txid_b.clone()),
                    txid: false,
                    duplicate: false,
                },
            ]],
        });
        let mut pending = vec![(txid_b.clone(), 0usize)];
        let mut processed = HashSet::new();
        let mut conn = storage.pool().acquire().await.unwrap();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            None,
            None,
        )
        .await
        .unwrap();
        let b = beef.find_txid(&txid_b).expect("B is carried");
        assert_eq!(
            b.bump_index(),
            Some(0),
            "B is linked to the BUMP that holds it"
        );
        assert!(
            !beef_txids(&beef).contains(&chain[0].1),
            "a proven root's parent is never dragged in"
        );
        assert!(
            beef.verify_valid(true).valid,
            "the BEEF is valid: {}",
            describe_invalid_beef(&mut beef, std::slice::from_ref(&txid_b))
        );
        prune_beef_to_roots(&mut beef, std::slice::from_ref(&txid_b));
        assert!(
            beef.verify_valid(true).valid,
            "and stays valid after the trim"
        );
    }

    /// Replay a real wallet's input BEEF build. Ignored unless `BEEF_CASE_DB`
    /// points at a wallet.db copy and `BEEF_CASE_ROOTS` lists the input txids
    /// (comma separated): `BEEF_CASE_DB=... BEEF_CASE_ROOTS=a,b cargo test
    /// --lib -- --ignored replay_input_beef_case`. The p25 case of 2026-09-04
    /// lives at ~/bsv/wallet-cases/p25-0829 (five roots, two sibling proofs).
    #[tokio::test]
    #[ignore = "needs BEEF_CASE_DB and BEEF_CASE_ROOTS"]
    async fn replay_input_beef_case() {
        let db = std::env::var("BEEF_CASE_DB").expect("BEEF_CASE_DB");
        let roots: Vec<String> = std::env::var("BEEF_CASE_ROOTS")
            .expect("BEEF_CASE_ROOTS")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let storage = StorageSqlx::open(&db).await.unwrap();
        storage.make_available().await.unwrap();
        let mut beef = Beef::new();
        let mut pending: Vec<(String, usize)> = roots.iter().cloned().map(|t| (t, 0)).collect();
        let mut processed = HashSet::new();
        let mut conn = storage.pool().acquire().await.unwrap();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            None,
            None,
        )
        .await
        .unwrap();
        prune_beef_to_roots(&mut beef, &roots);
        let valid = beef.verify_valid(true).valid;
        println!(
            "replay: valid={valid} {} tx(s), {} bump(s), {} bytes",
            beef.txs.len(),
            beef.bumps.len(),
            beef.to_binary().len()
        );
        if !valid {
            panic!("{}", describe_invalid_beef(&mut beef, &roots));
        }
        for root in &roots {
            let tx = beef.find_txid(root).expect("every root is carried");
            assert!(
                tx.bump_index().is_some() || tx.raw_tx().is_some(),
                "root {root} is proven or carried whole"
            );
        }
    }

    #[tokio::test]
    async fn test_build_input_beef_trims_known_txids_to_txid_only() {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(2);
        seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_010).await;
        seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;

        let inputs = vec![ExtendedInput {
            vin: 0,
            txid: chain[1].1.clone(),
            vout: 0,
            satoshis: 1_000,
            locking_script: vec![0x51],
            unlocking_script_length: 107,
            input_description: Some("spend".to_string()),
            output: None,
        }];

        let mut conn = storage.pool().acquire().await.unwrap();
        let untrimmed = build_input_beef(&mut conn, None, &inputs, &[], None, &[], false, None)
            .await
            .unwrap()
            .expect("a BEEF is produced");

        let trimmed = build_input_beef(
            &mut conn,
            None,
            &inputs,
            &[],
            None,
            &[chain[0].1.clone()],
            false,
            None,
        )
        .await
        .unwrap()
        .expect("a BEEF is produced");

        assert!(
            trimmed.len() < untrimmed.len(),
            "a known txid drops its raw bytes: {} vs {}",
            trimmed.len(),
            untrimmed.len()
        );
        let parsed = Beef::from_binary(&trimmed).unwrap();
        let known = parsed
            .find_txid(&chain[0].1)
            .expect("the known ancestor is still referenced");
        assert!(known.is_txid_only(), "and is referenced by txid alone");
    }

    #[tokio::test]
    async fn test_build_input_beef_ten_hops_stops_at_the_proven_hop() {
        // The reported defect end to end: ten hops of one covenant UTXO, the
        // middle ones mined. The BEEF for the next spend must carry hops 5..9
        // and nothing older.
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(10);
        for (i, (raw, txid)) in chain.iter().enumerate() {
            if i == 5 {
                seed_proven_chain_tx(&storage, txid, raw, 965_200).await;
            } else {
                // Every hop stored the whole ancestry it saw at build time.
                let mut stored = Beef::new();
                for (r, _) in chain.iter().take(i) {
                    stored.merge_raw_tx(r.clone(), None);
                }
                let stored_bytes = if i == 0 {
                    None
                } else {
                    Some(stored.to_binary())
                };
                seed_unproven_tx(&storage, txid, raw, stored_bytes.as_deref()).await;
            }
        }

        let inputs = vec![ExtendedInput {
            vin: 0,
            txid: chain[9].1.clone(),
            vout: 0,
            satoshis: 1_000,
            locking_script: vec![0x51],
            unlocking_script_length: 107,
            input_description: Some("next hop".to_string()),
            output: None,
        }];

        let mut conn = storage.pool().acquire().await.unwrap();
        let bytes = build_input_beef(&mut conn, None, &inputs, &[], None, &[], false, None)
            .await
            .unwrap()
            .expect("a BEEF is produced");

        let beef = Beef::from_binary(&bytes).unwrap();
        let kept = beef_txids(&beef);
        let expected: HashSet<String> = chain[5..].iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(
            kept, expected,
            "exactly hops 5..9; the proven hop terminates the closure"
        );
    }

    /// Move a seeded transaction's record back in time so the proof lookup
    /// does not skip it as too young to be mined.
    async fn backdate_tx(storage: &StorageSqlx, txid: &str, minutes: i64) {
        let then = Utc::now() - chrono::Duration::minutes(minutes);
        sqlx::query("UPDATE transactions SET created_at = ? WHERE txid = ?")
            .bind(then)
            .bind(txid)
            .execute(storage.pool())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_build_input_beef_skips_a_proof_lookup_for_a_young_transaction() {
        // A transaction this wallet recorded seconds ago cannot be mined yet,
        // and its ancestors answer the question anyway. No network call.
        use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
        use crate::storage::traits::WalletStorageProvider;

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(2);
        seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_000).await;
        seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;

        let services = std::sync::Arc::new(
            MockWalletServices::builder()
                .get_merkle_path_response(MockResponse::Error(
                    MockErrorKind::ServiceError,
                    "must not be called".to_string(),
                ))
                .build(),
        );
        storage.set_services(services.clone());

        let inputs = vec![ExtendedInput {
            vin: 0,
            txid: chain[1].1.clone(),
            vout: 0,
            satoshis: 1_000,
            locking_script: vec![0x51],
            unlocking_script_length: 107,
            input_description: Some("spend".to_string()),
            output: None,
        }];

        let mut conn = storage.pool().acquire().await.unwrap();
        build_input_beef(
            &mut conn,
            None,
            &inputs,
            &[],
            None,
            &[],
            false,
            Some(&storage),
        )
        .await
        .unwrap()
        .expect("a BEEF is produced");

        assert_eq!(
            services.call_count("get_merkle_path"),
            0,
            "a transaction recorded seconds ago is not worth asking about"
        );
    }

    #[tokio::test]
    async fn test_build_input_beef_fetches_a_missing_proof_inside_the_open_transaction() {
        // The wallet holds the parent but no proof for it, exactly what a
        // wallet running without its monitor looks like. The builder must ask
        // services once, record the proof on the CALLER'S open transaction (a
        // second pooled connection would block on that transaction's write
        // lock), and stop the chain there.
        use crate::services::mock::{MockResponse, MockWalletServices};
        use crate::services::traits::{BlockHeader, GetMerklePathResult};
        use crate::storage::traits::WalletStorageProvider;

        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("test", "000000").await.unwrap();
        storage.make_available().await.unwrap();

        let chain = synth_chain(3);
        // Every hop unproven, and the parent's stored BEEF still holds the
        // whole history behind it.
        let mut stored = Beef::new();
        stored.merge_raw_tx(chain[0].0.clone(), None);
        let stored_bytes = stored.to_binary();
        seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
        seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, Some(&stored_bytes)).await;
        seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;
        backdate_tx(&storage, &chain[1].1, 30).await;
        // The proof LAG gate is closed on a fresh database; this test pins
        // the fetch-and-store, so the gate is open (the deferral is pinned
        // in `reorg_tests`).
        crate::storage::MonitorStorage::set_max_acceptable_proof_height(&storage, u32::MAX)
            .await
            .unwrap();

        let bump = synth_bump(965_300, &chain[1].1);
        let services = MockWalletServices::builder()
            .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
                name: Some("MockProvider".to_string()),
                merkle_path: Some(hex::encode(bump.to_binary())),
                header: Some(BlockHeader {
                    version: 1,
                    previous_hash: "00".repeat(32),
                    merkle_root: chain[1].1.clone(),
                    time: 0,
                    bits: 0,
                    nonce: 0,
                    hash: "ab".repeat(32),
                    height: 965_300,
                }),
                error: None,
                notes: vec![],
            }))
            .build();
        storage.set_services(std::sync::Arc::new(services));

        let inputs = vec![ExtendedInput {
            vin: 0,
            txid: chain[1].1.clone(),
            vout: 0,
            satoshis: 1_000,
            locking_script: vec![0x51],
            unlocking_script_length: 107,
            input_description: Some("spend".to_string()),
            output: None,
        }];

        // An open write transaction that has already written, so the database
        // write lock is held for the whole call.
        let mut tx = storage.pool().begin().await.unwrap();
        sqlx::query("UPDATE settings SET storage_name = storage_name")
            .execute(&mut *tx)
            .await
            .unwrap();

        let bytes = build_input_beef(
            &mut tx,
            None,
            &inputs,
            &[],
            None,
            &[],
            false,
            Some(&storage),
        )
        .await
        .unwrap()
        .expect("a BEEF is produced");
        tx.commit().await.unwrap();

        let beef = Beef::from_binary(&bytes).unwrap();
        let kept = beef_txids(&beef);
        assert_eq!(kept.len(), 1, "only the proven parent is needed");
        assert!(kept.contains(&chain[1].1));
        assert_eq!(beef.bumps.len(), 1, "carrying the freshly fetched BUMP");
        assert!(
            !kept.contains(&chain[0].1),
            "the parent's stored ancestry is dropped once it is proven"
        );

        let stored_proof: Option<(String,)> =
            sqlx::query_as("SELECT txid FROM proven_txs WHERE txid = ?")
                .bind(&chain[1].1)
                .fetch_optional(storage.pool())
                .await
                .unwrap();
        assert!(
            stored_proof.is_some(),
            "the proof is recorded, so the next spend pays no network cost"
        );
    }

    // =========================================================================
    // Live-store diagnostic (ignored by default)
    //
    // Runs the real BEEF assembly walk against a COPY of a wallet store and
    // reports the shape of the BEEF the wallet would submit.
    //
    //   BEEF_DIAG_DB=/path/to/copy/wallet.db \
    //   BEEF_DIAG_TXIDS=<comma separated root txids> \
    //   cargo test --lib beef_diag_live_store -- --ignored --nocapture
    // =========================================================================

    /// Report one assembled BEEF: tx count, bytes, bumps, and whether any
    /// proven ancestor dragged its own parents in.
    fn beef_diag_report(label: &str, beef: &mut Beef, roots: &[String], elapsed_ms: u128) {
        let bytes = beef.to_binary();
        let all: std::collections::HashSet<String> = beef.txs.iter().map(|t| t.txid()).collect();

        let mut with_bump = 0usize;
        let mut txid_only = 0usize;
        let mut violations: Vec<String> = Vec::new();

        // A tx carrying a BUMP is self-proving; none of its parents belong in
        // the BEEF unless some other unproven tx needs them.
        let mut needed_by_unproven: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for tx in &mut beef.txs {
            if tx.bump_index().is_some() || tx.is_txid_only() {
                continue;
            }
            if let Some(raw) = tx.raw_tx().map(|r| r.to_vec()) {
                if let Ok(ins) = parse_input_txids(&raw) {
                    for i in ins {
                        needed_by_unproven.insert(i);
                    }
                }
            }
        }

        // A root is always legitimately present: it is an input being spent.
        let root_set: std::collections::HashSet<&String> = roots.iter().collect();

        for tx in &mut beef.txs {
            if tx.is_txid_only() {
                txid_only += 1;
                continue;
            }
            if tx.bump_index().is_none() {
                continue;
            }
            with_bump += 1;
            let txid = tx.txid();
            let raw = match tx.raw_tx().map(|r| r.to_vec()) {
                Some(r) => r,
                None => continue,
            };
            if let Ok(parents) = parse_input_txids(&raw) {
                for p in parents {
                    if all.contains(&p)
                        && !needed_by_unproven.contains(&p)
                        && !root_set.contains(&p)
                    {
                        violations.push(format!(
                            "{}<-{}",
                            &txid[..8.min(txid.len())],
                            &p[..8.min(p.len())]
                        ));
                    }
                }
            }
        }

        let validity = beef.verify_valid(true);

        println!("--------------------------------------------------------------");
        println!("BEEF DIAG [{}]", label);
        println!("  roots            : {}", roots.len());
        for r in roots {
            println!("                     {}", r);
        }
        println!("  transactions     : {}", beef.txs.len());
        println!("  with BUMP        : {}", with_bump);
        println!("  txid-only        : {}", txid_only);
        println!("  bumps            : {}", beef.bumps.len());
        println!(
            "  total bytes      : {} ({} KB)",
            bytes.len(),
            bytes.len() / 1024
        );
        println!("  assembly ms      : {}", elapsed_ms);
        println!("  structurally valid: {}", validity.valid);
        println!(
            "  proven ancestors dragging their own parents : {}",
            if violations.is_empty() {
                "none".to_string()
            } else {
                violations.join(", ")
            }
        );
        println!("--------------------------------------------------------------");
    }

    #[tokio::test]
    #[ignore]
    async fn beef_diag_live_store() {
        let db = match std::env::var("BEEF_DIAG_DB") {
            Ok(v) => v,
            Err(_) => {
                println!("set BEEF_DIAG_DB to run this diagnostic");
                return;
            }
        };
        let roots: Vec<String> = std::env::var("BEEF_DIAG_TXIDS")
            .expect("BEEF_DIAG_TXIDS required")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let storage = StorageSqlx::new(&format!("sqlite:{}", db)).await.unwrap();
        let mut conn = storage.pool().acquire().await.unwrap();

        let mut beef = Beef::new();
        let mut pending: Vec<(String, usize)> = roots.iter().map(|t| (t.clone(), 0)).collect();
        let mut processed: HashSet<String> = HashSet::new();

        // With services wired the walk fetches and records missing proofs, which
        // is what lets the chain terminate. Set BEEF_DIAG_SERVICES=1 to include
        // that path (writes land on the copy only).
        let with_services = std::env::var("BEEF_DIAG_SERVICES").is_ok();
        if with_services {
            let services = crate::services::Services::new(crate::Chain::Main).unwrap();
            crate::storage::traits::WalletStorageProvider::set_services(
                &storage,
                std::sync::Arc::new(services),
            );
        }
        let storage_ref = if with_services { Some(&storage) } else { None };

        let t0 = std::time::Instant::now();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            storage_ref,
            None,
        )
        .await
        .unwrap();
        let walk_ms = t0.elapsed().as_millis();

        beef_diag_report("after walk, before prune", &mut beef, &roots, walk_ms);

        let t1 = std::time::Instant::now();
        prune_beef_to_roots(&mut beef, &roots);
        let prune_ms = t1.elapsed().as_millis();

        beef_diag_report(
            "final (walk + prune)",
            &mut beef,
            &roots,
            walk_ms + prune_ms,
        );
    }
}
