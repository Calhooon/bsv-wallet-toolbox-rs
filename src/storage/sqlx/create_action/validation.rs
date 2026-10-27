use crate::error::{Error, Result};
use crate::storage::sqlx::internalize_action::{
    validate_array_bound, MAXIMUM_SEND_WITH_TRANSACTIONS, MAXIMUM_WALLET_COLLECTION_ITEMS,
};
// =============================================================================
// Constants
// =============================================================================

/// Maximum satoshi value (total BTC supply in satoshis).
const MAX_SATOSHIS: u64 = 2_100_000_000_000_000;

/// Special satoshi value indicating "use maximum possible".
const MAX_POSSIBLE_SATOSHIS: u64 = 2_099_999_999_999_999;

/// Minimum description length.
const MIN_DESCRIPTION_LENGTH: usize = 5;

/// Maximum description length.
const MAX_DESCRIPTION_LENGTH: usize = 2000;

/// Maximum label length.
const MAX_LABEL_LENGTH: usize = 300;

/// BRC-177's module prefix (ts-stack@edf6e03 `brc177NoSendExpiry.ts:5`).
const BRC177_NO_SEND_MODULE_PREFIX: &str = "p nosend ";

/// The reference's refusal on a store without the BRC-177 capability
/// (`createNoSendExpiryAction.ts:202`); the port has none.
const BRC177_UNSUPPORTED: &str = "Active storage does not support BRC-177 noSend expiry";

// =============================================================================
// Validation
// =============================================================================

/// Validates create action arguments.
pub(super) fn validate_create_action_args(args: &bsv_rs::wallet::CreateActionArgs) -> Result<()> {
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

    // ts-stack@edf6e03 validationHelpers.ts validateCreateActionArgs order
    // (749-808): description (755), inputs (758), outputs (759, each
    // output's tags last at 639), labels (762), options (763). Each array's
    // validateArray bound (105-114) runs ahead of its items.

    // Validate inputs
    if let Some(ref inputs) = args.inputs {
        validate_array_bound(inputs.len(), "inputs", MAXIMUM_WALLET_COLLECTION_ITEMS)?;
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

    // Validate outputs
    if let Some(ref outputs) = args.outputs {
        validate_array_bound(outputs.len(), "outputs", MAXIMUM_WALLET_COLLECTION_ITEMS)?;
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
                validate_array_bound(tags.len(), "tags", MAXIMUM_WALLET_COLLECTION_ITEMS)?;
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

    // Validate labels
    if let Some(ref labels) = args.labels {
        validate_array_bound(labels.len(), "labels", MAXIMUM_WALLET_COLLECTION_ITEMS)?;
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

    // Options (763, validateCreateActionOptions at 650-674): knownTxids'
    // bound at 662, noSendChange's at 667 ahead of its duplicates, sendWith's
    // own bound at 137 (validateSendWith, called at 670).
    if let Some(ref options) = args.options {
        if let Some(ref known_txids) = options.known_txids {
            validate_array_bound(
                known_txids.len(),
                "knownTxids",
                MAXIMUM_WALLET_COLLECTION_ITEMS,
            )?;
        }
        if let Some(ref no_send_change) = options.no_send_change {
            validate_array_bound(
                no_send_change.len(),
                "noSendChange",
                MAXIMUM_WALLET_COLLECTION_ITEMS,
            )?;
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
        if let Some(ref send_with) = options.send_with {
            validate_array_bound(send_with.len(), "sendWith", MAXIMUM_SEND_WITH_TRANSACTIONS)?;
        }
    }

    // BRC-177 (0177.md:53): a wallet without module `nosend` rejects the
    // request rather than storing the label as metadata. The reference
    // routes any label starting with the prefix (`startsWith`,
    // brc177NoSendExpiry.ts:119) to createNoSendExpiryAction once the
    // arguments are valid (Wallet.ts:1155-1165), which refuses without the
    // capability. The label is matched as the SDK's validateLabel leaves it,
    // trimmed and lowercased (validationHelpers.ts:411).
    let labels = args.labels.as_deref().unwrap_or_default();
    if labels.iter().any(|label| {
        label
            .trim()
            .to_lowercase()
            .starts_with(BRC177_NO_SEND_MODULE_PREFIX)
    }) {
        return Err(Error::InvalidOperation(BRC177_UNSUPPORTED.to_string()));
    }

    Ok(())
}
#[cfg(test)]
#[path = "tests/validation_bounds.rs"]
mod bounds_tests;
#[cfg(test)]
#[path = "tests/validation_nosend_grammar.rs"]
mod nosend_grammar_tests;
#[cfg(test)]
#[path = "tests/validation_nosend.rs"]
mod nosend_tests;
#[cfg(test)]
#[path = "tests/validation.rs"]
mod tests;
