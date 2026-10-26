//! The ARC response parsers: tx status, merkle path, and the submit
//! answers of a raw post and a BEEF post, with ARC answering the fuzzer's
//! status and body. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs::Arc;
use bsv_wallet_toolbox_rs_fuzz::{answer, base, runtime, sample};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn arc() -> &'static Arc {
    static ARC: OnceLock<Arc> = OnceLock::new();
    ARC.get_or_init(|| Arc::new(format!("{}/arcx", base()), None, Some("fuzz")).expect("arc"))
}

fuzz_target!(|data: &[u8]| {
    let arc = arc();
    let (raw, txid, beef) = sample();
    answer("/arcx", data);
    runtime().block_on(async {
        let txids = [txid.clone()];
        let _ = arc.get_tx_data(txid).await;
        let _ = arc.get_merkle_path(txid).await;
        let _ = arc.post_raw_tx(raw, Some(&txids)).await;
        let _ = arc.post_beef(beef, &txids).await;
    });
});
