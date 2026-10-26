//! The bananablocks response parsers: every read ladder (raw tx, merkle path,
//! status, utxo status, utxo with spender, leads) with bananablocks answering the
//! fuzzer's status and body. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs_fuzz::explore;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| explore("/bb", data));
