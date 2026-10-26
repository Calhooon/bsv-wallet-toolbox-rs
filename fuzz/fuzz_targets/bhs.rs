//! The Block Header Service response parsers: height, header by height,
//! root check and chain tip, with BHS answering the fuzzer's status and
//! body. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs::BlockHeaderService;
use bsv_wallet_toolbox_rs_fuzz::{answer, base, runtime};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn bhs() -> &'static BlockHeaderService {
    static BHS: OnceLock<BlockHeaderService> = OnceLock::new();
    BHS.get_or_init(|| BlockHeaderService::from_url(&format!("{}/bhs", base())))
}

fuzz_target!(|data: &[u8]| {
    let bhs = bhs();
    answer("/bhs", data);
    runtime().block_on(async {
        let _ = bhs.current_height().await;
        let _ = bhs.chain_header_by_height(800_000).await;
        let _ = bhs
            .is_valid_root_for_height(&"00".repeat(32), 800_000)
            .await;
        let _ = bhs.find_chain_tip_header().await;
    });
});
