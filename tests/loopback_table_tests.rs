//! The loopback table (#683) through the library compiled as an external
//! dependency with `bench-support`, as a host's tests reach it: the
//! production ladders at their production hosts, answered from the test's
//! own table, no socket.
#![cfg(feature = "bench-support")]

use bsv_wallet_toolbox_rs::benchmark_support::{LoopbackAnswer, LoopbackTable};
use bsv_wallet_toolbox_rs::services::{Chain, Services, ServicesOptions, WalletServices};

#[tokio::test]
async fn a_host_answers_the_explorer_rungs_from_its_table() {
    let txid = "ab".repeat(32);
    let table = LoopbackTable::new();
    table.on(
        "GET",
        "api.whatsonchain.com",
        &format!("/v1/bsv/main/tx/{txid}/hex"),
        LoopbackAnswer::status(404, ""),
    );
    let services =
        table.building(|| Services::with_options(Chain::Main, ServicesOptions::mainnet()).unwrap());
    // WhatsOnChain's 404 from the table; BananaBlocks and Bitails are
    // unmatched, the socket's send fault: "could not look", never absent.
    let result = services.get_raw_tx(&txid, false).await.unwrap();
    assert!(result.raw_tx.is_none());
    assert!(result.could_not_look, "{result:?}");
    let error = result.error.unwrap_or_default();
    assert!(
        error.contains(&format!(
            "error sending request for url (https://api.bitails.io/tx/{txid}/hex)"
        )),
        "{error}"
    );
    assert_eq!(table.requests().len(), 3, "{:?}", table.requests());
}
