//! The status review beside an `invalid` request whose txid has a `failed`
//! transaction row: one txid, two users' rows, one `failed` (its output
//! voided) and one live, a checked proof row. Such a txid is not the
//! review's proof candidate: the pass writes nothing of it (no status, link,
//! history or stamp, and no header read), so the unfail task takes the
//! request once its hour has passed and recovers the `failed` row, and the
//! proof task then completes both.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::transaction::{MerklePath, MerklePathLeaf};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    BlockHeader, GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail,
};
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
const HEIGHT: u32 = 100;

/// The txid of TX_HEX with its lock time set to `n`, its raw bytes, its
/// proof as the only transaction of a block at HEIGHT, and that root.
fn mined(n: u8) -> (String, Vec<u8>, Vec<u8>, String) {
    let mut raw = hex::decode(TX_HEX).unwrap();
    let last = raw.len() - 4;
    raw[last] = n;
    let tx = bsv_rs::transaction::Transaction::from_binary(&raw).unwrap();
    let txid = tx.id();
    let path = MerklePath::new(
        HEIGHT,
        vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
    )
    .unwrap();
    let root = path.compute_root(Some(&txid)).unwrap();
    (txid, raw, path.to_binary(), root)
}

/// One store, Alice and Bob in it, no chain tracker (a checked proof costs
/// no read).
async fn store() -> (Arc<StorageSqlx>, i64, i64) {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("one", SRC).await.unwrap();
    s.make_available().await.unwrap();
    let (alice, _) = s.find_or_insert_user(ALICE).await.unwrap();
    let (bob, _) = s.find_or_insert_user(BOB).await.unwrap();
    s.set_services(Arc::new(MockWalletServices::new()));
    s.set_max_acceptable_proof_height(HEIGHT + 5).await.unwrap();
    (s, alice.user_id, bob.user_id)
}

/// A transaction row of `user` for txid `n` with one output.
async fn tx_row(s: &StorageSqlx, user: i64, n: u8, status: &str, spendable: i64) {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(&raw);
    let now = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
    let tx = exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES ({user},'{status}','ref{user}-{n}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{now},{now})")).await;
    exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES ({user},{tx},{spendable},1,0,5000,'storage','change','P2PKH','{txid}',1,0,X'51',{now},{now})")).await;
}

/// Txid `n`: Alice's row `live` (its output counted, linked to the proof),
/// Bob's row `failed` (its output voided), a proof row this store has
/// checked, and the request `invalid`, last written two hours ago.
async fn failed_row_beside_proof(
    s: &StorageSqlx,
    alice: i64,
    bob: i64,
    n: u8,
    live: &str,
) -> String {
    let (txid, raw, path, root) = mined(n);
    tx_row(s, alice, n, live, 1).await;
    tx_row(s, bob, n, "failed", 0).await;
    let raw_hex = hex::encode(&raw);
    exec(s, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ('{txid}','invalid',0,'{{}}',0,'{{}}',X'{raw_hex}',strftime('%Y-%m-%dT%H:%M:%fZ','now','-2 hours'),strftime('%Y-%m-%dT%H:%M:%fZ','now','-2 hours'))")).await;
    ::sqlx::query("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES (?,?,0,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        .bind(&txid)
        .bind(HEIGHT as i64)
        .bind("e".repeat(64))
        .bind(&root)
        .bind(&path)
        .bind(&raw)
        .execute(s.writer())
        .await
        .unwrap();
    exec(s, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root) VALUES ('{txid}',{HEIGHT},'{root}')")).await;
    exec(
        s,
        &format!("UPDATE transactions SET proven_tx_id = (SELECT proven_tx_id FROM proven_txs WHERE txid = '{txid}') WHERE txid = '{txid}' AND user_id = {alice}"),
    )
    .await;
    txid
}

/// Each user's row of `txid` (status, spendable) and the request's status,
/// history notes and `updated_at`.
async fn rows(s: &StorageSqlx, txid: &str) -> (String, String) {
    let users: Vec<(i64, String, i64)> = ::sqlx::query_as(
        "SELECT t.user_id, t.status, o.spendable FROM transactions t JOIN outputs o ON o.transaction_id = t.transaction_id WHERE t.txid = ? ORDER BY t.user_id",
    )
    .bind(txid)
    .fetch_all(s.pool())
    .await
    .unwrap();
    let (status, history, updated): (String, String, String) = ::sqlx::query_as(
        "SELECT status, history, CAST(updated_at AS TEXT) FROM proven_tx_reqs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_one(s.pool())
    .await
    .unwrap();
    let users = users
        .iter()
        .map(|(u, st, sp)| format!("user {u} {st} spendable {sp}"))
        .collect::<Vec<_>>()
        .join(", ");
    (format!("{users} | req {status} history {history}"), updated)
}

/// Services that hold the transaction mined, its outputs unspent, and its
/// proof at HEIGHT.
fn chain_holds(txid: &str, path: &[u8], root: &str) -> MockWalletServices {
    MockWalletServices::builder()
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "MockProvider".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.to_string(),
                status: "mined".to_string(),
                depth: Some(1),
                merkle_path: None,
                block_height: None,
                block_hash: None,
            }],
        }))
        .is_utxo_response(MockResponse::Success(true))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("MockProvider".to_string()),
            merkle_path: Some(hex::encode(path)),
            header: Some(BlockHeader {
                version: 1,
                previous_hash: "00".repeat(32),
                merkle_root: root.to_string(),
                time: 1,
                bits: 1,
                nonce: 1,
                hash: "e".repeat(64),
                height: HEIGHT,
            }),
            error: None,
            notes: vec![],
        }))
        .build()
}

/// The review's passes, then the unfail task, then the proof task, for a
/// txid whose live row is `live`.
async fn a_failed_row_is_left_to_the_unfail_task(n: u8, live: &str) {
    let (s, alice, bob) = store().await;
    let txid = failed_row_beside_proof(&s, alice, bob, n, live).await;
    let (start, stamp) = rows(&s, &txid).await;
    println!("WITNESS {live} beside failed, start: {start} at {stamp}");
    for _ in 0..2 {
        MonitorStorage::review_status(&*s).await.unwrap();
    }
    let (reviewed, stamp_after) = rows(&s, &txid).await;
    println!("WITNESS {live} beside failed, after two review passes: {reviewed} at {stamp_after}");
    assert_eq!(stamp_after, stamp, "the review stamped the request");
    assert_eq!(reviewed, start);
    assert_eq!(
        reviewed,
        format!("user {alice} {live} spendable 1, user {bob} failed spendable 0 | req invalid history {{}}")
    );

    let (_, _, path, root) = mined(n);
    s.set_services(Arc::new(chain_holds(&txid, &path, &root)));
    MonitorStorage::un_fail(&*s).await.unwrap();
    let (unfailed, _) = rows(&s, &txid).await;
    println!("WITNESS {live} beside failed, the unfail task past its hour: {unfailed}");
    assert!(
        unfailed.starts_with(&format!(
            "user {alice} {live} spendable 1, user {bob} unproven spendable 1 | req unmined"
        )),
        "{unfailed}"
    );

    s.synchronize_transaction_statuses().await.unwrap();
    MonitorStorage::review_status(&*s).await.unwrap();
    let (proven, _) = rows(&s, &txid).await;
    println!("WITNESS {live} beside failed, the proof task then the review: {proven}");
    assert!(
        proven.starts_with(&format!(
            "user {alice} completed spendable 1, user {bob} completed spendable 1 | req completed"
        )),
        "{proven}"
    );
}

#[tokio::test]
async fn a_completed_row_beside_a_failed_one_is_left_to_the_unfail_task() {
    a_failed_row_is_left_to_the_unfail_task(31, "completed").await;
}

#[tokio::test]
async fn an_unproven_row_beside_a_failed_one_is_left_to_the_unfail_task() {
    a_failed_row_is_left_to_the_unfail_task(32, "unproven").await;
}
