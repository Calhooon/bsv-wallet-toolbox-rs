//! An `internalizeAction` request whose outputs list is empty is refused at
//! both doors, as the reference's validator refuses it (ts-stack@4d0ea63c8
//! sdk `validationHelpers.ts:1061-1066`): `The outputs parameter must be at
//! least one output to internalize from the transaction`, class
//! `InvalidParameter`, with nothing recorded, no request made and nothing
//! broadcast.
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/insertion.rs"]
mod insertion;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::WalletRefusalClass;

use common::{args, beef_paying, other_lock};
use insertion::*;

const EMPTY: &str = "Validation error: The outputs parameter must be at least one output to internalize from the transaction";

async fn written(wallet: &TestWallet) -> (i64, i64, i64, usize) {
    let count = |sql: &'static str| async move {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(wallet.storage().writer())
            .await
            .unwrap()
    };
    (
        count("SELECT COUNT(*) FROM transactions").await,
        count("SELECT COUNT(*) FROM proven_tx_reqs").await,
        count("SELECT COUNT(*) FROM outputs").await,
        wallet.services().call_count("post_beef"),
    )
}

#[tokio::test]
async fn an_empty_outputs_list_is_refused_at_both_doors() {
    for door in ["wallet", "storage"] {
        let (bytes, txid) = beef_paying(other_lock());
        let wallet = wallet(&PrivateKey::random(), &txid).await;
        let mut a = args(bytes, insertion(0, "tokens"));
        a.outputs.clear();
        let answer = match door {
            "wallet" => at_wallet(&wallet, a).await,
            _ => at_storage(&wallet, a).await,
        };
        assert_eq!(
            answer,
            Err((WalletRefusalClass::InvalidParameter, EMPTY.to_string())),
            "{door}"
        );
        assert_eq!(written(&wallet).await, (0, 0, 0, 0), "{door}");
    }
}
