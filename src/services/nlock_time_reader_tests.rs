//! `NLockTimeInput::from_raw_tx` reads exactly one transaction, as the
//! reference's reader does (bsv-rs 0.9.0-beta.8: #840 refuses bytes after the
//! transaction, #844 an output amount of 2^53 satoshis or more). Through
//! bsv-rs 0.9.0-beta.7 both answered `Ok`. The bytes are written by hand so
//! the same source runs at either pin.

use super::NLockTimeInput;

/// One input, one output of `satoshis` locked by `OP_TRUE`, lock time `lock`.
fn raw(satoshis: u64, lock: u32) -> Vec<u8> {
    let mut tx = hex::decode(format!(
        "0100000001{}000000000100ffffffff01",
        "11".repeat(32)
    ))
    .unwrap();
    tx.extend_from_slice(&satoshis.to_le_bytes());
    tx.extend_from_slice(&[0x01, 0x51]);
    tx.extend_from_slice(&lock.to_le_bytes());
    tx
}

#[test]
fn a_transaction_followed_by_bytes_is_refused_840() {
    assert_eq!(NLockTimeInput::from_raw_tx(&raw(1_000, 7)).unwrap().lock_time, 7);
    let mut trailing = raw(1_000, 7);
    trailing.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    let refused = NLockTimeInput::from_raw_tx(&trailing).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "Invalid argument: Failed to parse transaction bytes: transaction error: \
         Serialized transaction contains trailing data"
    );
    let hex = hex::encode(&trailing);
    assert_eq!(
        NLockTimeInput::from_hex_tx(&hex).unwrap_err().to_string(),
        refused.to_string()
    );
}

#[test]
fn an_output_of_two_to_the_53_satoshis_is_refused_844() {
    let ceiling = (1u64 << 53) - 1;
    assert_eq!(NLockTimeInput::from_raw_tx(&raw(ceiling, 9)).unwrap().lock_time, 9);
    let refused = NLockTimeInput::from_raw_tx(&raw(1u64 << 53, 9)).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "Invalid argument: Failed to parse transaction bytes: transaction error: \
         Number can only safely store up to 53 bits"
    );
}
