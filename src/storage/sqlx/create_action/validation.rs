use crate::error::{Error, Result};
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
#[cfg(test)]
#[path = "tests/validation_bounds.rs"]
mod bounds_tests;
#[cfg(test)]
#[path = "tests/validation.rs"]
mod tests;
