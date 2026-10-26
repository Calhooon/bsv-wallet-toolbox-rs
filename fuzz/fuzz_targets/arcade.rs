//! The Arcade V2 response parsers: tx status, merkle path, batch status,
//! health and the BEEF submit answer, with Arcade answering the fuzzer's
//! status and body. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs::Arcade;
use bsv_wallet_toolbox_rs_fuzz::{answer, base, runtime, sample};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn arcade() -> &'static Arcade {
    static ARCADE: OnceLock<Arcade> = OnceLock::new();
    ARCADE.get_or_init(|| Arcade::new(format!("{}/arcade", base()), None, None).expect("arcade"))
}

fuzz_target!(|data: &[u8]| {
    let arcade = arcade();
    let (_, txid, beef) = sample();
    answer("/arcade", data);
    runtime().block_on(async {
        let txids = [txid.clone()];
        let _ = arcade.get_tx_status(txid).await;
        let _ = arcade.get_merkle_path(txid).await;
        let _ = arcade.get_status_for_txids(&txids).await;
        let _ = arcade.health().await;
        let _ = arcade.post_beef(beef, &txids).await;
    });
});
