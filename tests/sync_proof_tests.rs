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
    let (held, _) = mined_at(100);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![held]);
    s.process_sync_chunk(args_for(ALICE), chunk).await.unwrap();
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
    assert!(r.is_err());
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
    assert!(refused.0 != "ok");
    assert_eq!(taken, ("ok".to_string(), (200, header.hash.clone())));
    assert_eq!(headerless.1, 100);
    assert!(headerless.0 != "ok");
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
    assert!(r.is_err());
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
    let first = mgr.sync_to_writer(ALICE, backup.clone()).await;
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
    let second = mgr.sync_to_writer(ALICE, backup.clone()).await;
    let after_second = block_hash_of(&backup, &txid).await;
    println!(
        "WITNESS 0 backup with an empty block hash: {}, link {linked:?}, hash {after_first:?}; \
         after the source's is filled: {}, hash {after_second:?}",
        verdict(&first),
        verdict(&second)
    );
    assert_eq!(verdict(&first), "ok");
    assert_eq!(linked, Some(txid.clone()));
    assert_eq!(after_first, "");
    assert_eq!(verdict(&second), "ok");
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
