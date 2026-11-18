//! Sync rows: the proof rows of a sync whose reader is not a local store.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_rs::primitives::{sha256d, to_hex};
use bsv_rs::transaction::{MerklePath, MerklePathLeaf, Transaction};
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::services::BlockHeader;
use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::double::Double;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

fn hash_hex(bytes: &[u8]) -> String {
    let mut h = sha256d(bytes);
    h.reverse();
    to_hex(&h)
}

/// The Merkle parent of two nodes, each in display order.
fn parent(left: &str, right: &str) -> String {
    let mut bytes = hex::decode(left).unwrap();
    bytes.reverse();
    let mut r = hex::decode(right).unwrap();
    r.reverse();
    bytes.extend(r);
    hash_hex(&bytes)
}

/// `n` transactions (n a power of two) (TX_HEX with lock times 0 to n - 1) mined as the whole
/// of one block at `height`: their proof rows and the block's header.
fn block_at(height: u32, n: u8) -> (Vec<TableProvenTx>, BlockHeader) {
    let raws: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut raw = hex::decode(TX_HEX).unwrap();
            let last = raw.len() - 4;
            raw[last] = i;
            raw
        })
        .collect();
    let txids: Vec<String> = raws
        .iter()
        .map(|r| Transaction::from_binary(r).unwrap().id())
        .collect();
    let leaves = txids
        .iter()
        .enumerate()
        .map(|(i, t)| MerklePathLeaf::new_txid(i as u64, t.clone()))
        .collect();
    // Every level whole: each node's hash given, so each txid's path
    // computes the root.
    let mut levels: Vec<Vec<MerklePathLeaf>> = vec![leaves];
    let mut nodes = txids.clone();
    while nodes.len() > 2 {
        nodes = nodes
            .chunks(2)
            .map(|pair| parent(&pair[0], &pair[1]))
            .collect();
        levels.push(
            nodes
                .iter()
                .enumerate()
                .map(|(i, h)| MerklePathLeaf::new(i as u64, h.clone()))
                .collect(),
        );
    }
    let path = MerklePath::new(height, levels).unwrap();
    let root = path.compute_root(Some(&txids[0])).unwrap();
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
    let rows = txids
        .iter()
        .enumerate()
        .map(|(i, txid)| {
            let mut p = proof(i as i64 + 1, txid);
            p.height = height as i64;
            p.index = i as i64;
            p.merkle_path = path.to_binary();
            p.raw_tx = raws[i].clone();
            p.block_hash = header.hash.clone();
            p.merkle_root = root.clone();
            p
        })
        .collect();
    (rows, header)
}

/// A reader store holding alice's completed transactions, each linked to
/// its proof row of `rows`, behind a double that is local or not.
async fn reader_with(rows: &[TableProvenTx], not_local: bool) -> Arc<Double> {
    let s = store(SRC).await;
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    for (i, p) in rows.iter().enumerate() {
        let id = ::sqlx::query("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES (?,?,?,?,?,?,?,datetime('now'),datetime('now'))")
            .bind(&p.txid).bind(p.height).bind(p.index).bind(&p.block_hash).bind(&p.merkle_root).bind(&p.merkle_path).bind(&p.raw_tx)
            .execute(s.writer()).await.unwrap().last_insert_rowid();
        ::sqlx::query("INSERT INTO transactions (user_id,proven_tx_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,created_at,updated_at) VALUES (?,?,'completed',?,0,5,1,0,'paid',?,datetime('now'),datetime('now'))")
            .bind(u.user_id).bind(id).bind(format!("ref{i}")).bind(&p.txid)
            .execute(s.writer()).await.unwrap();
    }
    let mut d = Double::new((*s).clone());
    d.not_local = not_local;
    Arc::new(d)
}

/// The manager's active store, with `services`, and the manager.
async fn wallet(services: Arc<MockWalletServices>) -> (Arc<StorageSqlx>, WalletStorageManager) {
    let act = store(DST).await;
    act.set_services(services);
    act.find_or_insert_user(ALICE).await.unwrap();
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    (act, mgr)
}

async fn stored(s: &StorageSqlx) -> (i64, i64) {
    (
        count(s, "SELECT COUNT(*) FROM proven_txs").await,
        count(s, "SELECT COUNT(*) FROM transactions").await,
    )
}

/// From a reader that is not local, a new proof row that fails the
/// reference's validation refuses the chunk and nothing is written; one
/// that passes is stored. From a local reader the same failing row is
/// stored as given.
#[tokio::test]
async fn a_new_proof_row_from_a_reader_that_is_not_local_is_validated() {
    let (rows, header) = block_at(300, 1);
    let txid = rows[0].txid.clone();

    let stale = Arc::new(MockWalletServices::builder().roots_valid(false).build());
    stale.set_header_for_height(header.clone());
    let (act, mgr) = wallet(stale.clone()).await;
    let remote = mgr
        .sync_from_reader(ALICE, reader_with(&rows, true).await)
        .await;
    let remote_rows = stored(&act).await;

    let active = Arc::new(MockWalletServices::builder().roots_valid(true).build());
    active.set_header_for_height(header.clone());
    let (act, mgr) = wallet(active.clone()).await;
    let passes = mgr
        .sync_from_reader(ALICE, reader_with(&rows, true).await)
        .await;
    let passes_rows = stored(&act).await;

    let stale_local = Arc::new(MockWalletServices::builder().roots_valid(false).build());
    let (act, mgr) = wallet(stale_local.clone()).await;
    let local = mgr
        .sync_from_reader(ALICE, reader_with(&rows, false).await)
        .await;
    let local_rows = stored(&act).await;
    println!(
        "WITNESS 8 new proof row: not local, root stale: {} rows {remote_rows:?}; not local, root active: {} rows {passes_rows:?}; local, root stale: {} rows {local_rows:?} (root checks {})",
        refusal(&remote),
        refusal(&passes),
        refusal(&local),
        stale_local.root_check_count()
    );
    assert_eq!(
        refusal(&remote),
        format!("Sync error: sync chunk proof {txid} is refused: Merkle root is not active at the recorded height")
    );
    assert_eq!(remote_rows, (0, 0));
    assert_eq!(refusal(&passes), "ok");
    assert_eq!(passes_rows, (1, 1));
    assert_eq!(refusal(&local), "ok");
    assert_eq!(local_rows, (1, 1));
    assert_eq!(stale_local.root_check_count(), 0);
}

/// From a reader that is not local, the proof rows of one height cost one
/// root check and one header read for the whole sync, and a row the
/// writer already holds as the same proof costs none.
#[tokio::test]
async fn the_proof_rows_of_one_height_are_checked_once() {
    let (rows, header) = block_at(400, 4);
    let services = Arc::new(MockWalletServices::builder().roots_valid(true).build());
    services.set_header_for_height(header);
    let (act, mgr) = wallet(services.clone()).await;
    let reader = reader_with(&rows, true).await;
    let once = mgr.sync_from_reader(ALICE, reader.clone()).await;
    let after_once = (
        services.root_check_count(),
        services.call_count("get_header_for_height"),
    );
    let again = mgr.sync_from_reader(ALICE, reader).await;
    let after_again = (
        services.root_check_count(),
        services.call_count("get_header_for_height"),
    );
    let rows_stored = stored(&act).await;
    println!(
        "WITNESS 8 load, four rows at one height: {} (root checks, header reads) {after_once:?}; the same sync again: {} {after_again:?}; rows {rows_stored:?}",
        refusal(&once),
        refusal(&again)
    );
    assert_eq!(refusal(&once), "ok");
    assert_eq!(after_once, (1, 1));
    assert_eq!(refusal(&again), "ok");
    assert_eq!(after_again, (1, 1));
    assert_eq!(rows_stored, (4, 4));
}

/// A manager over a local store, passed as the reader before its own first
/// call, is a local reader: its new proof rows are stored as given, with
/// no root check, by a writer whose tracker would refute them.
#[tokio::test]
async fn a_manager_passed_as_the_reader_before_its_own_use_is_local() {
    let (rows, _) = block_at(310, 1);
    let stale = Arc::new(MockWalletServices::builder().roots_valid(false).build());
    let (act, mgr) = wallet(stale.clone()).await;
    let reader = WalletStorageManager::new(
        ALICE.to_string(),
        Some(reader_with(&rows, false).await as Arc<dyn MonitorStorage>),
        None,
    );
    let r = mgr.sync_from_reader(ALICE, Arc::new(reader)).await;
    let rows_stored = stored(&act).await;
    println!(
        "WITNESS manager as reader before its own use: {} rows {rows_stored:?} root checks {}",
        refusal(&r),
        stale.root_check_count()
    );
    assert_eq!(refusal(&r), "ok");
    assert_eq!(rows_stored, (1, 1));
    assert_eq!(stale.root_check_count(), 0);
}
