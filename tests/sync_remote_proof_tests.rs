//! Sync rows: the proof rows of a sync whose reader is not a local store.
#![cfg(feature = "sqlite")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use bsv_wallet_toolbox_rs::services::{
    FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult, GetStatusForTxidsResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult, UtxoVerdict,
    WalletServices,
};
use bsv_wallet_toolbox_rs::Result;

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

/// `n` transactions (n a power of two) (TX_HEX with lock times 0 to n - 1,
/// and the height's low byte above them) mined as the whole
/// of one block at `height`: their proof rows and the block's header.
fn block_at(height: u32, n: u8) -> (Vec<TableProvenTx>, BlockHeader) {
    let raws: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut raw = hex::decode(TX_HEX).unwrap();
            let last = raw.len() - 4;
            raw[last] = i;
            raw[last + 1] = (height & 0xff) as u8;
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
/// root check and one header call of the store's services for the whole
/// sync, and a row the writer already holds as the same proof costs none.
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
        "WITNESS 8 load, four rows at one height: {} (root checks, header calls) {after_once:?}; the same sync again: {} {after_again:?}; rows {rows_stored:?}",
        refusal(&once),
        refusal(&again)
    );
    assert_eq!(refusal(&once), "ok");
    assert_eq!(after_once, (1, 1));
    assert_eq!(refusal(&again), "ok");
    assert_eq!(after_again, (1, 1));
    assert_eq!(rows_stored, (4, 4));
}

/// A reader that answers local while it serves its first chunk and not
/// local from its second chunk on: the second chunk's new proof row, which
/// the writer's tracker refutes, refuses that chunk; the first chunk's row,
/// read while the reader answered local, is stored as given.
#[tokio::test]
async fn a_reader_that_stops_being_local_between_chunks_is_validated_from_then() {
    let (first, _) = block_at(340, 1);
    let (second, _) = block_at(341, 1);
    let txid = second[0].txid.clone();
    let stale = Arc::new(MockWalletServices::builder().roots_valid(false).build());
    let (act, mgr) = wallet(stale.clone()).await;
    let mut reader = Double::new((*store(SRC).await).clone());
    reader.inner.find_or_insert_user(ALICE).await.unwrap();
    reader.local_through = Some(1);
    let chunk = |rows: &[TableProvenTx]| {
        let mut c = chunk_for(ALICE);
        c.proven_txs = Some(rows.to_vec());
        c
    };
    *reader.script.lock().unwrap() = Some([chunk(&first), chunk(&second)].into());
    let r = mgr.sync_from_reader(ALICE, Arc::new(reader)).await;
    let proofs = count(&act, "SELECT COUNT(*) FROM proven_txs").await;
    println!(
        "WITNESS a reader local for its first chunk, not local from its second: {} proof rows {proofs} root checks {}",
        refusal(&r),
        stale.root_check_count()
    );
    assert_eq!(
        refusal(&r),
        format!("Sync error: sync chunk proof {txid} is refused: Merkle root is not active at the recorded height")
    );
    assert_eq!(proofs, 1);
    assert_eq!(stale.root_check_count(), 1);
}

/// A store under `services` that holds alice and no proof row.
async fn door(services: Arc<MockWalletServices>) -> Arc<StorageSqlx> {
    let s = store(DST).await;
    s.set_services(services);
    s.find_or_insert_user(ALICE).await.unwrap();
    s
}

/// The tracker refutes every root, or confirms it, with `header` at its height.
fn services(valid: bool, header: &BlockHeader) -> Arc<MockWalletServices> {
    let s = Arc::new(MockWalletServices::builder().roots_valid(valid).build());
    s.set_header_for_height(header.clone());
    s
}

/// A chunk handed to a store's sync door, plain or mapped, or to a
/// manager's, validates a new proof row: one that fails the reference's
/// rule refuses the chunk and nothing is written; one that passes is stored.
#[tokio::test]
async fn a_new_proof_row_at_a_stores_own_door_is_validated() {
    let (rows, header) = block_at(320, 1);
    let txid = rows[0].txid.clone();
    let text = format!(
        "Sync error: sync chunk proof {txid} is refused: Merkle root is not active at the recorded height"
    );
    let chunk = || {
        let mut c = chunk_for(ALICE);
        c.proven_txs = Some(rows.clone());
        c
    };
    let proofs =
        |s: Arc<StorageSqlx>| async move { count(&s, "SELECT COUNT(*) FROM proven_txs").await };

    let mut answers = Vec::new();
    for valid in [false, true] {
        let s = door(services(valid, &header)).await;
        let r = s.process_sync_chunk(args_for(ALICE), chunk()).await;
        answers.push(("store", valid, refusal(&r), proofs(s).await));

        let s = door(services(valid, &header)).await;
        let mut map = SyncMap::default();
        let r = s
            .process_sync_chunk_mapped(args_for(ALICE), chunk(), &mut map)
            .await;
        answers.push(("store mapped", valid, refusal(&r), proofs(s).await));

        let s = door(services(valid, &header)).await;
        let mgr = WalletStorageManager::new(
            ALICE.to_string(),
            Some(s.clone() as Arc<dyn MonitorStorage>),
            None,
        );
        let r = mgr.process_sync_chunk(args_for(ALICE), chunk()).await;
        answers.push(("manager", valid, refusal(&r), proofs(s).await));
    }
    for a in &answers {
        println!(
            "WITNESS door {} root active {}: {} proof rows {}",
            a.0, a.1, a.2, a.3
        );
    }
    for (_, valid, answer, rows) in answers {
        if valid {
            assert_eq!(answer, "ok");
            assert_eq!(rows, 1);
        } else {
            assert_eq!(answer, text);
            assert_eq!(rows, 0);
        }
    }
}

/// A manager's backup from its local active to a local backup with no
/// services takes the active's new proof rows as given.
#[tokio::test]
async fn a_local_backup_with_no_services_takes_the_wallets_own_proof_rows() {
    let (rows, _) = block_at(330, 2);
    let reader = reader_with(&rows, false).await;
    let backup = store(DST).await;
    for s in [&reader.inner, &*backup] {
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        s.update_user_active_storage(u.user_id, SRC).await.unwrap();
    }
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(reader as Arc<dyn MonitorStorage>),
        Some(vec![backup.clone() as Arc<dyn MonitorStorage>]),
    );
    let r = mgr.update_backups().await;
    let rows_stored = stored(&backup).await;
    println!(
        "WITNESS local backup, no services: {} rows {rows_stored:?}",
        refusal(&r)
    );
    assert_eq!(refusal(&r), "ok");
    assert_eq!(rows_stored, (2, 2));
}

/// From a reader that is not local, rows at two heights of which the
/// writer already holds some as the same proof: one root check and one
/// header call of the store's services per height that has a row the
/// writer does not hold, and none when the sync runs again.
#[tokio::test]
async fn the_load_counts_only_heights_with_rows_not_held() {
    let (r5, h5) = block_at(500, 4);
    let (r6, h6) = block_at(600, 4);
    let all: Vec<TableProvenTx> = r5.iter().chain(r6.iter()).cloned().collect();
    let mut answers = Vec::new();
    for held in [
        vec![&r5[0], &r6[0], &r6[1]],
        vec![&r6[0], &r6[1], &r6[2], &r6[3]],
    ] {
        let services = Arc::new(MockWalletServices::builder().roots_valid(true).build());
        services.set_header_for_height(h5.clone());
        services.set_header_for_height(h6.clone());
        let (act, mgr) = wallet(services.clone()).await;
        for p in &held {
            hold(&act, p).await;
        }
        let reader = reader_with(&all, true).await;
        let calls = || {
            (
                services.root_check_count(),
                services.call_count("get_header_for_height"),
            )
        };
        let once = mgr.sync_from_reader(ALICE, reader.clone()).await;
        let after_once = calls();
        let again = mgr.sync_from_reader(ALICE, reader).await;
        let after_again = calls();
        let proofs = count(&act, "SELECT COUNT(*) FROM proven_txs").await;
        println!(
            "WITNESS load, 4 rows at 500 and 4 at 600, {} held: {} (root checks, header calls) {after_once:?}; again: {} {after_again:?}; proof rows {proofs}",
            held.len(),
            refusal(&once),
            refusal(&again)
        );
        answers.push((
            refusal(&once),
            after_once,
            refusal(&again),
            after_again,
            proofs,
        ));
    }
    let ok = || "ok".to_string();
    assert_eq!(answers[0], (ok(), (2, 2), ok(), (2, 2), 8));
    assert_eq!(answers[1], (ok(), (1, 1), ok(), (1, 1), 8));
}

/// A tracker that confirms every root and counts the checks in flight: each
/// check yields to the scheduler before it answers, so every check started
/// together is in flight together.
#[derive(Default)]
struct Inflight {
    now: AtomicUsize,
    most: AtomicUsize,
}

#[async_trait]
impl ChainTracker for Inflight {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        self.now.fetch_sub(1, Ordering::SeqCst);
        Ok(true)
    }
    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(0)
    }
}

/// Mock services with the [`Inflight`] tracker.
struct Counting {
    mock: MockWalletServices,
    tracker: Inflight,
}

#[async_trait]
impl WalletServices for Counting {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        Ok(&self.tracker)
    }
    async fn get_height(&self) -> Result<u32> {
        self.mock.get_height().await
    }
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.mock.get_header_for_height(height).await
    }
    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        self.mock.hash_to_header(hash).await
    }
    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult> {
        self.mock.get_raw_tx(txid, use_next).await
    }
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        self.mock.get_merkle_path(txid, use_next).await
    }
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        self.mock.post_beef(beef, txids).await
    }
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        self.mock
            .get_utxo_status(output, output_format, outpoint, use_next)
            .await
    }
    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        self.mock.get_status_for_txids(txids, use_next).await
    }
    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.mock.get_bsv_exchange_rate().await
    }
    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        self.mock.get_fiat_exchange_rate(currency, base).await
    }
    fn hash_output_script(&self, script: &[u8]) -> String {
        self.mock.hash_output_script(script)
    }
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> UtxoVerdict {
        self.mock.is_utxo(txid, vout, locking_script).await
    }
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        self.mock.n_lock_time_is_final(n_lock_time).await
    }
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        self.mock.n_lock_time_is_final_for_tx(input).await
    }
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult> {
        self.mock.get_beef(txid, known_txids).await
    }
}

/// From a reader that is not local, a chunk of proof rows at twenty
/// heights has no more than eight root checks in flight at once.
#[tokio::test]
async fn no_more_than_eight_heights_are_asked_at_once() {
    let mock = MockWalletServices::builder().roots_valid(true).build();
    let mut rows = Vec::new();
    for h in 700..720 {
        let (r, header) = block_at(h, 1);
        mock.set_header_for_height(header);
        rows.extend(r);
    }
    let services = Arc::new(Counting {
        mock,
        tracker: Inflight::default(),
    });
    let act = store(DST).await;
    act.set_services(services.clone());
    act.find_or_insert_user(ALICE).await.unwrap();
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    let r = mgr
        .sync_from_reader(ALICE, reader_with(&rows, true).await)
        .await;
    let most = services.tracker.most.load(Ordering::SeqCst);
    let rows_stored = stored(&act).await;
    println!(
        "WITNESS in flight, 20 heights: {} most root checks at once {most}; rows {rows_stored:?}",
        refusal(&r)
    );
    assert_eq!(refusal(&r), "ok");
    assert_eq!(rows_stored, (20, 20));
    assert_eq!(most, 8);
}
