use super::*;

/// Whether every caller-provided input's signatures allow the wallet to append
/// its own funding inputs and change outputs. Empty input lists are not
/// caller-complete and are never "fundable" through this path.
pub(super) fn caller_inputs_permit_funding(inputs: &[bsv_rs::wallet::CreateActionInput]) -> bool {
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
pub(super) const SIGHASH_BASE_MASK: u8 = 0x1f;
pub(super) const SIGHASH_ALL: u8 = 0x01;
pub(super) const SIGHASH_NONE: u8 = 0x02;
pub(super) const SIGHASH_SINGLE: u8 = 0x03;
pub(super) const SIGHASH_ANYONECANPAY: u8 = 0x80;

/// What the signatures in a caller-provided unlocking script commit to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InputCommitment {
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
pub(super) fn input_commitment(unlocking_script: &[u8]) -> InputCommitment {
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
pub(super) fn script_pushes(script: &[u8]) -> Vec<&[u8]> {
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
pub(super) fn der_signature_sighash(data: &[u8]) -> Option<u8> {
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
#[cfg(test)]
#[path = "tests/signing.rs"]
mod tests;
