//! The per-user status review (`WalletStorageWriter::review_status`): a
//! transaction `unproven` or `sending` beside a request that reads
//! `completed` is completed only where the request's proof carries this
//! store's root-check record. An unchecked one is left as it is (this call
//! asks no tracker) for the monitor's status review, whose pass checks it.
#![cfg(feature = "sqlite")]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf};
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
const HEIGHT: u32 = 100;

#[derive(Default)]
struct Headers {
    roots: Mutex<HashSet<(u32, String)>>,
    reads: AtomicUsize,
}

#[async_trait]
impl ChainTracker for Headers {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .roots
            .lock()
            .unwrap()
            .contains(&(height, root.to_string())))
    }
    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(HEIGHT + 10)
    }
}

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

const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";

/// Transaction `n` of Alice's (`status`) beside a request `completed`
/// linked to its proof row, recorded as checked or not.
async fn seed(s: &StorageSqlx, n: u8, status: &str, checked: bool) -> String {
    let (txid, raw, path, root) = mined(n);
    let (raw, path) = (hex::encode(raw), hex::encode(path));
    exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'{status}','r{n}',1,-1000,1,0,'pay','{txid}',X'{raw}',{NOW},{NOW})")).await;
    let p = exec(s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{txid}',{HEIGHT},0,'{h}','{root}',X'{path}',X'{raw}',{NOW},{NOW})", h = "e".repeat(64))).await;
    exec(s, &format!("INSERT INTO proven_tx_reqs (proven_tx_id,txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ({p},'{txid}','completed',0,'{{}}',0,'{{}}',X'{raw}',{NOW},{NOW})")).await;
    if checked {
        exec(s, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{root}',{NOW})")).await;
    }
    txid
}

async fn status(s: &StorageSqlx, txid: &str) -> String {
    let (st, p): (String, Option<i64>) =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(s.pool())
            .await
            .unwrap();
    format!("{st}{}", if p.is_some() { "+link" } else { "" })
}

async fn alice_store(headers: Arc<Headers>) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("S", DST).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    s.update_user_active_storage(u.user_id, DST).await.unwrap();
    s.set_chain_tracker(headers).await;
    s
}

/// Unchecked: `unproven` and `sending` are left, no header read, through
/// the store and through the manager; the monitor's review then checks and
/// completes them. Checked: completed at once by the per-user call.
#[tokio::test]
async fn the_per_user_review_completes_only_beside_a_proof_this_store_checked() {
    let headers = Arc::new(Headers::default());
    let s = alice_store(headers.clone()).await;
    let un = seed(&s, 31, "unproven", false).await;
    let se = seed(&s, 32, "sending", false).await;
    let un_ok = seed(&s, 33, "unproven", true).await;
    let se_ok = seed(&s, 34, "sending", true).await;
    for n in [31u8, 32] {
        headers.roots.lock().unwrap().insert((HEIGHT, mined(n).3));
    }
    let auth = AuthId::with_user_id(ALICE, 1);
    let aged = chrono::Utc::now();
    let r = WalletStorageWriter::review_status(&*s, &auth, aged)
        .await
        .unwrap();
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(s.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    let mr = WalletStorageWriter::review_status(&mgr, &auth, aged)
        .await
        .unwrap();
    let after = [
        status(&s, &un).await,
        status(&s, &se).await,
        status(&s, &un_ok).await,
        status(&s, &se_ok).await,
    ];
    println!(
        "WITNESS per-user review: {after:?}, header reads {}\n  store log: {}\n  manager log: {}",
        headers.reads.load(Ordering::SeqCst),
        r.log.trim(),
        mr.log.trim()
    );
    assert_eq!(
        after,
        [
            "unproven".to_string(),
            "sending".to_string(),
            "completed+link".to_string(),
            "completed+link".to_string()
        ]
    );
    assert_eq!(headers.reads.load(Ordering::SeqCst), 0);
    assert!(r.log.contains("Left tx"), "{}", r.log);

    MonitorStorage::review_status(&*s).await.unwrap();
    let monitor = [status(&s, &un).await, status(&s, &se).await];
    println!("WITNESS the monitor's review after: {monitor:?}");
    assert_eq!(
        monitor,
        ["completed+link".to_string(), "completed+link".to_string()]
    );
}
