//! `internalizeAction` with no output to internalize is refused at both
//! doors before anything is stored or broadcast, with the reference's
//! sentence (`validateInternalizeActionArgs`, ts-stack 4d0ea63c8).
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::InternalizeActionArgs;

use common::{beef_paying, internalize_at, other_lock, stored_outputs};

const NO_OUTPUTS: &str =
    "The outputs parameter must be at least one output to internalize from the transaction";

#[tokio::test]
async fn an_empty_outputs_list_is_refused_at_both_doors() {
    for door in ["wallet", "storage"] {
        let root = PrivateKey::random();
        let (tx, txid) = beef_paying(other_lock());
        let wallet = common::wallet("internalize-no-outputs-tests", &root, &txid).await;
        let args = InternalizeActionArgs {
            tx,
            outputs: vec![],
            description: "nothing to internalize".to_string(),
            labels: None,
            seek_permission: None,
        };
        let answer = internalize_at(door, &wallet, &root, args).await;
        println!("{door}: {answer:?}");
        let expected = match door {
            "wallet" => format!("wallet error: {NO_OUTPUTS}"),
            _ => format!("Validation error: {NO_OUTPUTS}"),
        };
        assert_eq!(answer, Err(expected));
        let transactions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap();
        assert_eq!((transactions, stored_outputs(&wallet).await), (0, 0));
        assert_eq!(
            wallet.services().call_count("post_beef"),
            0,
            "nothing broadcast"
        );
    }
}
