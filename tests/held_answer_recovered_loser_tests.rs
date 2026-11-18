//! THE LOSER OF A RECOVERED ROW.
//!
//! A take recovers the sender's aborted action R; one of R's inputs was
//! taken meanwhile by another live action L of the wallet, and stays with
//! L. The recovery queues R as L's competitor (`competitorsQueued` on L's
//! request), so the competitor proof ask settles the two by a proof: when
//! R's proof meets the store's headers, L is retired as the loser
//! (`doubleSpend`).

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

#[path = "common/abort_take.rs"]
mod abort_take;

use std::sync::Arc;

use abort_take::*;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::MerklePath;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{BlockHeader, GetMerklePathResult, GetRawTxResult};
use bsv_wallet_toolbox_rs::{
    BroadcastOutcome, MonitorStorage, StorageSqlx, WalletStorageProvider, WalletStorageWriter,
};

async fn word(store: &StorageSqlx, sql: &str, id: &str) -> Option<String> {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_optional(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_recovered_row_is_queued_as_the_competitor_of_the_action_holding_its_input() {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let store = StorageSqlx::in_memory().await.unwrap();
    store
        .migrate("recovered-loser", &root.public_key().to_hex())
        .await
        .unwrap();
    store.make_available().await.unwrap();
    store.set_chain_tracker(Arc::new(Headers)).await;
    MonitorStorage::set_max_acceptable_proof_height(&store, u32::MAX)
        .await
        .unwrap();
    let f = seed(&store, &parts, Cause::AbortFree).await;
    let x = f.x.user_id.unwrap();
    abort_now(&f, Cause::AbortFree).await;

    // L: another action of X spending R's input 0, live, with its request.
    let l_txid = bsv_rs::transaction::Transaction::from_binary(&f.competitor)
        .unwrap()
        .id();
    let l = insert_tx(&store, x, &l_txid, "sending", &f.competitor, true).await;
    sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
        .bind(l)
        .bind(f.inputs[0])
        .execute(store.writer())
        .await
        .unwrap();
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
         raw_tx, created_at, updated_at) VALUES (?, 'sending', 0, '{}', 0, '{}', ?, ?, ?)",
    )
    .bind(&l_txid)
    .bind(&f.competitor)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap();

    // An instance takes R.
    store
        .update_transaction_status_after_post(
            &f.txid,
            &BroadcastOutcome::Success,
            &[common::accepted(&f.txid)],
            None,
        )
        .await
        .unwrap();
    let r = read(&f, false).await;
    let l_history = word(
        &store,
        "SELECT history FROM proven_tx_reqs WHERE txid = ?",
        &l_txid,
    )
    .await
    .unwrap();
    println!("R after its take: {r:?}\nL's request history {l_history}");
    assert_eq!(r.row.as_deref(), Some("unproven"));
    assert_eq!(r.inputs[0], (false, "other".to_string()), "L keeps it");
    assert!(
        l_history.contains("competitorsQueued") && l_history.contains(&f.txid),
        "{l_history}"
    );

    // R's proof meets our headers: the competitor proof ask retires L.
    let path = MerklePath::from_coinbase_txid(&f.txid, PROVEN_HEIGHT);
    let services = MockWalletServices::builder()
        .get_raw_tx_response(MockResponse::Success(GetRawTxResult {
            name: "mock".to_string(),
            txid: f.txid.clone(),
            raw_tx: Some(parts.raw.clone()),
            error: None,
            could_not_look: false,
        }))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("mock".to_string()),
            merkle_path: Some(hex::encode(path.to_binary().unwrap())),
            header: Some(BlockHeader {
                version: 0x2000_0000,
                previous_hash: "p".repeat(64),
                merkle_root: path.compute_root(Some(&f.txid)).unwrap(),
                time: 1_700_000_000,
                bits: 0,
                nonce: 0,
                hash: "a".repeat(64),
                height: PROVEN_HEIGHT,
            }),
            error: None,
            notes: vec![],
        }))
        .build();
    store.set_services(Arc::new(services));
    MonitorStorage::synchronize_transaction_statuses(&store)
        .await
        .unwrap();
    let l_req = word(
        &store,
        "SELECT status FROM proven_tx_reqs WHERE txid = ?",
        &l_txid,
    )
    .await;
    let l_row = word(
        &store,
        "SELECT status FROM transactions WHERE txid = ?",
        &l_txid,
    )
    .await;
    println!("L after the competitor ask: row {l_row:?} request {l_req:?}");
    assert_eq!(l_req.as_deref(), Some("doubleSpend"));
    assert_eq!(l_row.as_deref(), Some("failed"));
}
