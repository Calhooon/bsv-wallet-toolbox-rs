//! The TSC proof intake (`tsc_json_to_bump_*`, which runs the private
//! `tsc_proof_to_merkle_path`): WhatsOnChain's and Bitails's JSON proofs
//! turned into a BUMP. The first four bytes are the block height. `None` is
//! a fine answer; a BUMP that comes out must read back.
#![no_main]

use bsv_rs::transaction::MerklePath;
use bsv_wallet_toolbox_rs::tsc_proof::{tsc_json_to_bump_binary, tsc_json_to_bump_hex};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let height = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let Ok(json) = std::str::from_utf8(&data[4..]) else {
        return;
    };
    let _ = tsc_json_to_bump_hex(json, height);
    if let Some(bump) = tsc_json_to_bump_binary(json, height) {
        let _ = MerklePath::from_binary(&bump).map(|mp| mp.compute_root(None));
    }
});
