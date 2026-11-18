//! Sync rows: a proof row of a chunk that replaces one the store holds.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::primitives::{sha256d, to_hex};
use bsv_rs::transaction::{MerklePath, MerklePathLeaf, Transaction};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::services::BlockHeader;
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

fn hash_hex(bytes: &[u8]) -> String {
    let mut h = sha256d(bytes);
    h.reverse();
    to_hex(&h)
}

/// The proof of TX_HEX as the only transaction of a block at `height`, and
/// the header of that block.
fn mined_at(height: u32) -> (TableProvenTx, BlockHeader) {
    let tx = Transaction::from_hex(TX_HEX).unwrap();
    let txid = tx.id();
    let path = MerklePath::new(
        height,
        vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
    )
    .unwrap();
    let root = path.compute_root(Some(&txid)).unwrap();
    let mut header = BlockHeader {
        version: 1,
        previous_hash: "00".repeat(32),
        merkle_root: root.clone(),
        time: 1_231_006_505,
        bits: 486_604_799,
        nonce: height,
        hash: String::new(),
        height,
    };
    header.hash = hash_hex(&header.to_binary());
    let mut p = proof(1, &txid);
    p.height = height as i64;
    p.index = 0;
    p.merkle_path = path.to_binary();
    p.raw_tx = hex::decode(TX_HEX).unwrap();
    p.block_hash = header.hash.clone();
    p.merkle_root = root;
    (p, header)
}

async fn held_store(services: MockWalletServices) -> Arc<StorageSqlx> {
    let s = store(DST).await;
    s.set_services(Arc::new(services));
    s.find_or_insert_user(ALICE).await.unwrap();
    hold(&s, &mined_at(100).0).await;
    s
}

async fn held_height(s: &StorageSqlx) -> (i64, String) {
    ::sqlx::query_as("SELECT height, block_hash FROM proven_txs")
        .fetch_one(s.pool())
        .await
        .unwrap()
}

fn verdict<T>(r: &bsv_wallet_toolbox_rs::Result<T>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(e) => e.to_string(),
    }
}

/// A proof row that replaces a held one and fails the reference's
/// validation refuses the chunk; the held proof is unchanged.
#[tokio::test]
async fn a_replacement_proof_that_fails_validation_is_refused() {
    let services = MockWalletServices::builder().roots_valid(true).build();
    let s = held_store(services).await;
    let before = held_height(&s).await;
    let (mut forged, _) = mined_at(200);
    forged.raw_tx.push(0); // the bytes no longer hash to the txid
    forged.updated_at = at(5);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![forged]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let after = held_height(&s).await;
    println!(
        "WITNESS B4 forged replacement: {}; held (height, block) {} -> {}",
        verdict(&r),
        before.0,
        after.0
    );
    assert_eq!(
        verdict(&r),
        format!("Sync error: sync chunk proof {} is refused: transaction, Merkle path, or active header could not be validated", mined_at(200).0.txid)
    );
    assert_eq!(after, before);
}

/// A replacement whose root the tracker does not hold at its height is
/// refused; one it holds, under the header of that height, replaces the
/// held proof.
#[tokio::test]
async fn a_replacement_proof_is_taken_only_under_the_tracker_and_header() {
    let (mut good, header) = mined_at(200);
    good.updated_at = at(5);

    let stale = MockWalletServices::builder().roots_valid(false).build();
    stale.set_header_for_height(header.clone());
    let s = held_store(stale).await;
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good.clone()]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let refused = (verdict(&r), held_height(&s).await.0);

    let active = MockWalletServices::builder().roots_valid(true).build();
    active.set_header_for_height(header.clone());
    let s = held_store(active).await;
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good.clone()]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let taken = (verdict(&r), held_height(&s).await);

    let no_header = MockWalletServices::builder().roots_valid(true).build();
    let s = held_store(no_header).await;
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good.clone()]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let headerless = (verdict(&r), held_height(&s).await.0);

    println!("WITNESS B4 stale root: {refused:?}; active root: {taken:?}; header not that block: {headerless:?}");
    assert_eq!(refused.1, 100);
    let txid = good.txid.clone();
    assert_eq!(
        refused.0,
        format!("Sync error: sync chunk proof {txid} is refused: Merkle root is not active at the recorded height")
    );
    assert_eq!(taken, ("ok".to_string(), (200, header.hash.clone())));
    assert_eq!(headerless.1, 100);
    assert_eq!(
        headerless.0,
        format!("Sync error: sync chunk proof {txid} is refused: block metadata does not match the active header")
    );
}

/// Every proof row's identifiers are checked: a new proof row whose txid is
/// not 32-byte hexadecimal refuses the chunk.
#[tokio::test]
async fn a_proof_row_with_a_malformed_txid_is_refused() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![proof(1, "not-hex")]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let rows = count(&s, "SELECT COUNT(*) FROM proven_txs").await;
    println!(
        "WITNESS B4 malformed txid: {}; proof rows {rows}",
        verdict(&r)
    );
    assert_eq!(
        verdict(&r),
        "Sync error: sync chunk proof not-hex is refused: txid must be 32-byte hexadecimal"
    );
    assert_eq!(rows, 0);
}

/// A store of this line holding a completed transaction linked to a proof
/// row with no block hash, its manager, and that proof's txid.
async fn wallet_with_a_proof_without_block_hash() -> (
    Arc<StorageSqlx>,
    bsv_wallet_toolbox_rs::managers::WalletStorageManager,
    String,
) {
    let act = store(SRC).await;
    let (u, _) = act.find_or_insert_user(ALICE).await.unwrap();
    let txid = seed_all(&act, u.user_id, 'a', true, true).await;
    exec(&act, "UPDATE proven_txs SET block_hash = ''").await;
    let mgr = bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    (act, mgr, txid)
}

async fn block_hash_of(s: &StorageSqlx, txid: &str) -> String {
    ::sqlx::query_scalar("SELECT block_hash FROM proven_txs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap()
}

/// A backup of a store holding a proof row with no block hash completes, and
/// the backup's transaction is linked to its row of that proof; a later
/// backup after the source's row has its block hash completes against a
/// backup store with no services, its row unchanged.
#[tokio::test]
async fn a_backup_of_a_proof_row_without_block_hash_completes() {
    let (act, mgr, txid) = wallet_with_a_proof_without_block_hash().await;
    let backup = store(DST).await;
    let backup_one = mgr.sync_to_writer(ALICE, backup.clone()).await;
    let linked = stored_link(&backup, "transactions", &txid).await;
    let after_first = block_hash_of(&backup, &txid).await;
    exec(
        &act,
        &format!(
            "UPDATE proven_txs SET block_hash = '{}', updated_at = datetime('now')",
            "e".repeat(64)
        ),
    )
    .await;
    let backup_two = mgr.sync_to_writer(ALICE, backup.clone()).await;
    let after_second = block_hash_of(&backup, &txid).await;
    println!(
        "WITNESS 0 backup with an empty block hash: {}, link {linked:?}, hash {after_first:?}; \
         after the source's is filled: {}, hash {after_second:?}",
        verdict(&backup_one),
        verdict(&backup_two)
    );
    assert_eq!(verdict(&backup_one), "ok");
    assert_eq!(linked, Some(txid.clone()));
    assert_eq!(after_first, "");
    assert_eq!(verdict(&backup_two), "ok");
    assert_eq!(after_second, "");
}

/// A backup whose own proof row has a block hash, of a source whose row has
/// none, completes and keeps the backup's block hash.
#[tokio::test]
async fn a_backup_keeps_its_block_hash_when_the_source_has_none() {
    let (_act, mgr, txid) = wallet_with_a_proof_without_block_hash().await;
    let backup = store(DST).await;
    mgr.sync_to_writer(ALICE, backup.clone()).await.unwrap();
    let filled = "9".repeat(64);
    exec(
        &backup,
        &format!("UPDATE proven_txs SET block_hash = '{filled}'"),
    )
    .await;
    let r = mgr.sync_to_writer(ALICE, backup.clone()).await;
    let kept = block_hash_of(&backup, &txid).await;
    println!(
        "WITNESS 0 source empty, backup filled: {}, hash {kept:?}",
        verdict(&r)
    );
    assert_eq!(verdict(&r), "ok");
    assert_eq!(kept, filled);
}

/// A replacement proof row with no block hash is validated as any other, and
/// the block hash stored is the active header's at its height.
#[tokio::test]
async fn a_replacement_without_block_hash_stores_the_headers_hash() {
    let (mut good, header) = mined_at(200);
    good.block_hash = String::new();
    good.updated_at = at(5);
    let active = MockWalletServices::builder().roots_valid(true).build();
    active.set_header_for_height(header.clone());
    let s = held_store(active).await;
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let held = held_height(&s).await;
    println!(
        "WITNESS 0 replacement with no block hash: {}; held {held:?}",
        verdict(&r)
    );
    assert_eq!(verdict(&r), "ok");
    assert_eq!(held, (200, header.hash.clone()));
}

/// A proof row of a txid the store did not hold when the chunk's proofs
/// were decided, which another writer stores before the chunk's write
/// transaction with other fields, refuses the chunk; that row stays. The
/// test holds the store's one writer connection until the new row's
/// validation has read its header, so the other row lands before the
/// chunk's write transaction, with no sleep.
#[tokio::test]
async fn a_proof_row_that_appears_meanwhile_refuses_the_chunk() {
    let (incoming, header) = mined_at(200);
    let services = Arc::new(MockWalletServices::builder().roots_valid(true).build());
    services.set_header_for_height(header);
    let s = store(DST).await;
    s.set_services(services.clone());
    s.find_or_insert_user(ALICE).await.unwrap();
    let txid = incoming.txid.clone();
    let mut held = s.writer().acquire().await.unwrap();
    let s2 = s.clone();
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![incoming]);
    let task = tokio::spawn(async move { s2.process_sync_chunk(args_for(ALICE), chunk).await });
    while services.call_count("get_header_for_height") == 0 {
        tokio::task::yield_now().await;
    }
    ::sqlx::query("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES (?,7,0,'','',X'00',X'00',datetime('now'),datetime('now'))")
        .bind(&txid)
        .execute(&mut *held)
        .await
        .unwrap();
    drop(held);
    let r = task.await.unwrap();
    let after = held_height(&s).await.0;
    println!(
        "WITNESS 8a proof row appearing meanwhile: {}; held height {after}",
        verdict(&r)
    );
    assert_eq!(
        verdict(&r),
        format!("Sync error: sync chunk proof {txid} is refused: a concurrent proof row appeared; retry synchronization")
    );
    assert_eq!(after, 7);
}

/// A held proof that a replacement was validated against, rewritten by
/// another writer between the validation and the chunk's write, refuses the
/// chunk; the other writer's row stays. The test holds the store's one
/// writer connection until the validation has read its header, so the
/// rewrite lands before the chunk's write transaction, with no sleep.
#[tokio::test]
async fn a_held_proof_rewritten_after_its_validation_refuses_the_chunk() {
    let (mut good, header) = mined_at(200);
    good.updated_at = at(5);
    let txid = good.txid.clone();
    let services = Arc::new(MockWalletServices::builder().roots_valid(true).build());
    services.set_header_for_height(header);
    let s = store(DST).await;
    s.set_services(services.clone());
    s.find_or_insert_user(ALICE).await.unwrap();
    hold(&s, &mined_at(100).0).await;

    let mut held = s.writer().acquire().await.unwrap();
    let s2 = s.clone();
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good]);
    let task = tokio::spawn(async move { s2.process_sync_chunk(args_for(ALICE), chunk).await });
    while services.call_count("get_header_for_height") == 0 {
        tokio::task::yield_now().await;
    }
    ::sqlx::query("UPDATE proven_txs SET height = 300 WHERE txid = ?")
        .bind(&txid)
        .execute(&mut *held)
        .await
        .unwrap();
    drop(held);
    let r = task.await.unwrap();
    let after = held_height(&s).await.0;
    println!(
        "WITNESS P3 held proof rewritten after its validation: {}; held height {after}",
        verdict(&r)
    );
    assert_eq!(
        verdict(&r),
        format!("Sync error: sync chunk proof {txid} is refused: proof changed during sync preparation; retry synchronization")
    );
    assert_eq!(after, 300);
}

/// A validated replacement is stamped the greatest of now, its chunk row's
/// stamp and the held row's stamp plus one millisecond, so a reader of this
/// store's changes since the held row's stamp finds it.
#[tokio::test]
async fn a_validated_replacement_is_stamped_after_the_held_row() {
    let (mut good, header) = mined_at(200);
    good.updated_at = at(-10);
    let services = MockWalletServices::builder().roots_valid(true).build();
    services.set_header_for_height(header);
    let s = held_store(services).await;
    let held_stamp: chrono::DateTime<chrono::Utc> =
        ::sqlx::query_scalar("SELECT updated_at FROM proven_txs")
            .fetch_one(s.pool())
            .await
            .unwrap();
    let before = chrono::Utc::now();
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![good]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let stamp: chrono::DateTime<chrono::Utc> =
        ::sqlx::query_scalar("SELECT updated_at FROM proven_txs")
            .fetch_one(s.pool())
            .await
            .unwrap();
    println!(
        "WITNESS P2 replacement stamp: {}; held {held_stamp}, chunk row {}, stored {stamp}",
        verdict(&r),
        at(-10)
    );
    assert_eq!(verdict(&r), "ok");
    assert_eq!(held_height(&s).await.0, 200);
    assert!(stamp >= before);
    assert!(stamp > held_stamp);
}
