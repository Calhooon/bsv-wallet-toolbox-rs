//! Sync rows: what a store's sync reader sends and what its sync writer takes
//! from a chunk. Each test is one rule; the rule is named in its doc line.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;
use chrono::{DateTime, Duration, Utc};

const ALICE: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SRC: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const DST: &str = "2222222222222222222222222222222222222222222222222222222222222222";

const ENTITIES: [&str; 12] = [
    "outputBasket",
    "provenTx",
    "provenTxReq",
    "txLabel",
    "outputTag",
    "transaction",
    "output",
    "txLabelMap",
    "outputTagMap",
    "certificate",
    "certificateField",
    "commission",
];

async fn store(sik: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("rows", sik).await.unwrap();
    s.make_available().await.unwrap();
    s
}

fn args_for(identity: &str) -> RequestSyncChunkArgs {
    RequestSyncChunkArgs {
        from_storage_identity_key: SRC.to_string(),
        to_storage_identity_key: DST.to_string(),
        identity_key: identity.to_string(),
        since: None,
        max_rough_size: 10_000_000,
        max_items: 1000,
        offsets: ENTITIES
            .iter()
            .map(|n| SyncOffset {
                name: n.to_string(),
                offset: 0,
            })
            .collect(),
    }
}

fn chunk_for(identity: &str) -> SyncChunk {
    SyncChunk {
        from_storage_identity_key: SRC.to_string(),
        to_storage_identity_key: DST.to_string(),
        user_identity_key: identity.to_string(),
        ..Default::default()
    }
}

async fn exec(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query(sql)
        .execute(s.writer())
        .await
        .unwrap()
        .last_insert_rowid()
}

async fn count(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query_scalar(sql).fetch_one(s.pool()).await.unwrap()
}

fn at(days: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + Duration::days(days)
}

fn basket(id: i64, name: &str) -> TableOutputBasket {
    TableOutputBasket {
        basket_id: id,
        user_id: 1,
        name: name.to_string(),
        number_of_desired_utxos: 6,
        minimum_desired_utxo_value: 1000,
        created_at: at(0),
        updated_at: at(0),
    }
}

fn transaction(id: i64, reference: &str, txid: Option<&str>) -> TableTransaction {
    TableTransaction {
        transaction_id: id,
        user_id: 1,
        txid: txid.map(str::to_string),
        status: TransactionStatus::Unproven,
        reference: reference.to_string(),
        description: "synced".to_string(),
        satoshis: 0,
        version: 1,
        lock_time: 0,
        raw_tx: None,
        input_beef: None,
        is_outgoing: false,
        proof_txid: None,
        created_at: at(0),
        updated_at: at(0),
    }
}

/// One chunk, one transaction: a chunk whose transaction row fails leaves none
/// of the rows it wrote before it.
#[tokio::test]
async fn a_failing_chunk_leaves_none_of_its_rows() {
    let s = store(DST).await;
    let (bob, _) = s.find_or_insert_user(BOB).await.unwrap();
    s.find_or_insert_user(ALICE).await.unwrap();
    exec(
        &s,
        &format!(
            "INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,description,created_at,updated_at) \
             VALUES ({},'unproven','clash',0,0,'bob''s',datetime('now'),datetime('now'))",
            bob.user_id
        ),
    )
    .await;
    let mut chunk = chunk_for(ALICE);
    chunk.output_baskets = Some(vec![basket(7, "left-behind")]);
    chunk.transactions = Some(vec![transaction(9, "clash", None)]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    assert!(r.is_err(), "the chunk fails on its transaction row: {r:?}");
    let left = count(
        &s,
        "SELECT COUNT(*) FROM output_baskets WHERE name = 'left-behind'",
    )
    .await;
    let states = count(&s, "SELECT COUNT(*) FROM sync_states").await;
    println!("WITNESS B20 rows left by the failed chunk: baskets={left} sync_states={states}");
    assert_eq!((left, states), (0, 0));
}
