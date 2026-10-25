use super::*;
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
