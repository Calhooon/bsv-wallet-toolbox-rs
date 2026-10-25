use super::*;

/// Default fee rate in satoshis per kilobyte.
pub(super) const DEFAULT_FEE_RATE_SAT_PER_KB: u64 = 101;

/// P2PKH locking script length (25 bytes).
pub(super) const P2PKH_LOCKING_SCRIPT_LENGTH: u32 = 25;

/// P2PKH unlocking script length (107 bytes typical).
pub(super) const P2PKH_UNLOCKING_SCRIPT_LENGTH: u32 = 107;

/// A fixed input for fee calculation.
#[derive(Debug, Clone)]
pub(super) struct FixedInput {
    pub(super) satoshis: u64,
    pub(super) unlocking_script_length: u32,
}

/// A fixed output for fee calculation.
#[derive(Debug, Clone)]
pub(super) struct FixedOutput {
    pub(super) satoshis: u64,
    pub(super) locking_script_length: u32,
}

// =============================================================================
// Size Calculation
// =============================================================================

/// Calculates transaction size given input and output script lengths.
pub(super) fn calculate_transaction_size(
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
pub(super) fn var_int_size(value: u64) -> u64 {
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

/// The miner fee the wallet would require for the transaction exactly as the
/// caller provided it (its fixed inputs and outputs only, no change).
pub(super) fn caller_fixed_fee_required(params: &GenerateChangeParams) -> u64 {
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
#[cfg(test)]
#[path = "tests/fees.rs"]
mod tests;
