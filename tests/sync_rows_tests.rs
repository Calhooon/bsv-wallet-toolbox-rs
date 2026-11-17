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

fn proof(id: i64, txid: &str) -> TableProvenTx {
    TableProvenTx {
        proven_tx_id: id,
        txid: txid.to_string(),
        height: 1,
        index: 0,
        block_hash: "e".repeat(64),
        merkle_root: "e".repeat(64),
        merkle_path: vec![0],
        raw_tx: vec![0],
        created_at: at(0),
        updated_at: at(0),
    }
}

fn request(id: i64, txid: &str, proof_id: Option<i64>) -> TableProvenTxReq {
    TableProvenTxReq {
        proven_tx_req_id: id,
        txid: txid.to_string(),
        status: ProvenTxReqStatus::Unmined,
        attempts: 0,
        history: "{}".to_string(),
        notified: false,
        notify: "{}".to_string(),
        raw_tx: Some(vec![1]),
        input_beef: None,
        proven_tx_id: proof_id,
        batch: None,
        created_at: at(0),
        updated_at: at(0),
    }
}

async fn stored_link(s: &StorageSqlx, table: &str, txid: &str) -> Option<String> {
    ::sqlx::query_scalar::<_, Option<String>>(&format!(
        "SELECT p.txid FROM {table} r LEFT JOIN proven_txs p ON p.proven_tx_id = r.proven_tx_id WHERE r.txid = ?"
    ))
    .bind(txid)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

/// A transaction's and a request's proof link travel through the chunk's
/// proof rows: a request names its proof by the source's id, a transaction by
/// the proof's txid; a link that maps to no proof row is not stored.
#[tokio::test]
async fn a_proof_link_is_mapped_through_the_chunks_proof_rows() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    // A proof row of another transaction this store already holds (local id 1).
    let other = "c".repeat(64);
    exec(&s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{other}',1,0,'{h}','{h}',X'00',X'00',datetime('now'),datetime('now'))", h = "e".repeat(64))).await;
    let mined = "d".repeat(64);
    let pending = "f".repeat(64);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![proof(7, &mined)]);
    chunk.proven_tx_reqs = Some(vec![
        request(3, &mined, Some(7)),
        request(4, &pending, Some(99)),
    ]);
    let mut t_mined = transaction(5, "r-mined", Some(&mined));
    t_mined.proof_txid = Some(mined.clone());
    let mut t_numbered = transaction(6, "r-numbered", Some(&pending));
    t_numbered.proof_txid = Some("1".to_string());
    chunk.transactions = Some(vec![t_mined, t_numbered]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    println!(
        "WITNESS B7/B10 chunk: {:?}",
        r.as_ref()
            .map(|r| (r.inserts, r.updates))
            .map_err(|e| e.to_string())
    );
    assert!(r.is_ok());
    let links = (
        stored_link(&s, "proven_tx_reqs", &mined).await,
        stored_link(&s, "proven_tx_reqs", &pending).await,
        stored_link(&s, "transactions", &mined).await,
        stored_link(&s, "transactions", &pending).await,
    );
    println!("WITNESS B7/B10 links (req mined, req unmapped, tx mined, tx numbered): {links:?}");
    assert_eq!(
        links,
        (Some(mined.clone()), None, Some(mined.clone()), None)
    );
}

/// Two rows of one chunk that claim one source id are refused, and nothing
/// of the chunk is written.
#[tokio::test]
async fn a_chunk_mapping_one_source_id_twice_is_refused() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let mut chunk = chunk_for(ALICE);
    chunk.output_baskets = Some(vec![basket(5, "one"), basket(5, "two")]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let rows = count(
        &s,
        "SELECT COUNT(*) FROM output_baskets WHERE name IN ('one','two')",
    )
    .await;
    println!(
        "WITNESS B22 chunk: {:?} rows={rows}",
        r.as_ref().map(|r| r.inserts).map_err(|e| e.to_string())
    );
    assert!(r.is_err());
    assert_eq!(rows, 0);
}

/// A transaction whose proof link is a number is not linked to the local
/// proof row that number names.
#[tokio::test]
async fn a_numbered_proof_link_names_no_local_row() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let other = "c".repeat(64);
    exec(&s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{other}',1,0,'{h}','{h}',X'00',X'00',datetime('now'),datetime('now'))", h = "e".repeat(64))).await;
    let pending = "f".repeat(64);
    let mut t = transaction(6, "r-numbered", Some(&pending));
    t.proof_txid = Some("1".to_string());
    let mut chunk = chunk_for(ALICE);
    chunk.transactions = Some(vec![t]);
    s.process_sync_chunk(args_for(ALICE), chunk).await.unwrap();
    let link = stored_link(&s, "transactions", &pending).await;
    println!("WITNESS B10 numbered link stored as: {link:?}");
    assert_eq!(link, None);
}
