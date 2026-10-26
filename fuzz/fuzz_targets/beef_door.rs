//! The BEEF a stranger hands the internalize and create doors: the stream
//! verifier as `refuse_invalid_beef_bytes` calls it, then the parse, the
//! broadcast check and the EF split the create door's broadcast path runs
//! on bytes the verifier let through. A refusal is a fine answer.
#![no_main]

use bsv_rs::transaction::Beef;
use bsv_wallet_toolbox_rs::storage::sqlx::refuse_invalid_beef_bytes;
use bsv_wallet_toolbox_rs::storage::validate_beef_for_broadcast;
use bsv_wallet_toolbox_rs::{beef_to_ef_batch, unproven_ancestors_in_beef};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if refuse_invalid_beef_bytes(data).is_err() {
        return;
    }
    let _ = beef_to_ef_batch(data);
    let Ok(mut beef) = Beef::from_binary(data) else {
        return;
    };
    let txids: Vec<String> = beef.txs.iter().map(|t| t.txid()).collect();
    let _ = beef.verify_valid(true);
    for txid in txids.iter().take(4) {
        let _ = validate_beef_for_broadcast(&beef, txid);
        let _ = unproven_ancestors_in_beef(data, txid);
        let _ = beef.to_binary_atomic(txid);
    }
});
