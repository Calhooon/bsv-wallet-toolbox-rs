//! The Chaintracks client's response parsers: present height, header by
//! height and by hash, chain tip, root check, and the mining tip the
//! `ChainTracker` reads, with Chaintracks answering the fuzzer's status and
//! body. No panic is the only assertion.
#![no_main]

use bsv_rs::transaction::ChainTracker;
use bsv_wallet_toolbox_rs::services::providers::ChaintracksServiceClient;
use bsv_wallet_toolbox_rs_fuzz::{answer, base, runtime};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn client() -> &'static ChaintracksServiceClient {
    static CLIENT: OnceLock<ChaintracksServiceClient> = OnceLock::new();
    CLIENT.get_or_init(|| ChaintracksServiceClient::from_url(&format!("{}/ct", base())))
}

fuzz_target!(|data: &[u8]| {
    let ct = client();
    answer("/ct", data);
    runtime().block_on(async {
        let root = "00".repeat(32);
        let _ = ct.get_present_height().await;
        let _ = ct.find_header_for_height(800_000).await;
        let _ = ct.find_header_for_block_hash(&root).await;
        let _ = ct.find_chain_tip_header().await;
        let _ = ChaintracksServiceClient::is_valid_root_for_height(ct, &root, 800_000).await;
        let _ = ChainTracker::mining_tip(ct).await;
    });
});
