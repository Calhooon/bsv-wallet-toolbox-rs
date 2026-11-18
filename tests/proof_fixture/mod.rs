//! Shared fixtures of the witnesses of `completed` beside a proof this store
//! has or has not checked: a counting tracker, mined transactions, stores,
//! held rows, the reader's rows, a manager's drain and a view of a txid.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf};
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

use crate::sync_support::{count, exec, ALICE, DST, SRC};

pub const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
pub const HEIGHT: u32 = 100;

pub const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
pub const OLD: &str = "'2026-01-01T00:00:00.000Z'";
pub const LATER: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now','+1 day')";

/// A store's headers: the (height, root) pairs it carries and how many root
/// reads it was asked.
#[derive(Default)]
pub struct Headers {
    pub roots: Mutex<HashSet<(u32, String)>>,
    pub reads: AtomicUsize,
}

impl Headers {
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
    pub fn carry(&self, n: u8) {
        self.roots.lock().unwrap().insert((HEIGHT, mined(n).3));
    }
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

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Tracker {
    Confirms,
    Refutes,
    None,
}

/// Transaction `n` (TX_HEX with its lock time `n`): txid, bytes, its proof
/// as the only transaction of a block at HEIGHT, that proof's root.
pub fn mined(n: u8) -> (String, Vec<u8>, Vec<u8>, String) {
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

/// A store with Alice in it, active on W, its tracker `headers`.
pub async fn store(sik: &str, name: &str, headers: Option<Arc<Headers>>) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate(name, sik).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    s.update_user_active_storage(u.user_id, DST).await.unwrap();
    if let Some(h) = headers {
        s.set_chain_tracker(h).await;
    }
    s.set_services(Arc::new(MockWalletServices::new()));
    s
}

/// W, with the tracker `tracker` names over `headers`.
/// The proof gate stands above HEIGHT, so a proof at HEIGHT is stored.
pub async fn w_store(tracker: Tracker, headers: &Arc<Headers>) -> Arc<StorageSqlx> {
    let w = store(
        DST,
        "W",
        (tracker != Tracker::None).then(|| headers.clone()),
    )
    .await;
    w.set_max_acceptable_proof_height(HEIGHT + 5).await.unwrap();
    w
}

/// R, the reader: no tracker.
pub async fn r_store() -> Arc<StorageSqlx> {
    store(SRC, "R", None).await
}

/// A transaction row of Alice's for `n` (`status`, `reference`, stamped
/// `stamp`) with one change output of 1000; returns its id.
pub async fn held_at(s: &StorageSqlx, n: u8, status: &str, reference: &str, stamp: &str) -> i64 {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(raw);
    let t = exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'{status}','{reference}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{stamp},{stamp})")).await;
    exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES (1,{t},1,1,0,1000,'storage','change','P2PKH','{txid}',1,0,X'51',{stamp},{stamp})")).await;
    t
}

/// The proof row of `n` on `s`, stamped `stamp`; returns its id.
pub async fn proof_row(s: &StorageSqlx, n: u8, stamp: &str) -> i64 {
    let (txid, raw, path, root) = mined(n);
    exec(s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{txid}',{HEIGHT},0,'{h}','{root}',X'{p}',X'{r}',{stamp},{stamp})", h = "e".repeat(64), p = hex::encode(path), r = hex::encode(raw))).await
}

/// The proof row of `n` on `s` with this store's root-check record.
pub async fn checked_proof_row(s: &StorageSqlx, n: u8) -> i64 {
    let p = proof_row(s, n, OLD).await;
    let (txid, _, _, root) = mined(n);
    exec(s, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{root}',{NOW})")).await;
    p
}

/// A request of `n` on `s` reading `status`, linked to `link`.
pub async fn proof_request(s: &StorageSqlx, n: u8, status: &str, link: Option<i64>) {
    let (txid, raw, _, _) = mined(n);
    let link = link.map(|p| p.to_string()).unwrap_or_else(|| "NULL".into());
    exec(s, &format!("INSERT INTO proven_tx_reqs (proven_tx_id,txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ({link},'{txid}','{status}',0,'{{}}',0,'{{}}',X'{}',{NOW},{NOW})", hex::encode(raw))).await;
}

/// The reader's rows of `n`: the proof, a `completed` request linked to it
/// when `with_request`, and its own transaction row reading `status`, linked
/// when `linked`, under `reference`, stamped `stamp`.
pub async fn reader_rows(
    r: &StorageSqlx,
    n: u8,
    reference: &str,
    stamp: &str,
    status: &str,
    linked: bool,
    with_request: bool,
) {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(raw);
    let p = proof_row(r, n, stamp).await;
    if with_request {
        exec(r, &format!("INSERT INTO proven_tx_reqs (proven_tx_id,txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ({p},'{txid}','completed',0,'{{}}',0,'{{}}',X'{raw_hex}',{stamp},{stamp})")).await;
    }
    let link = if linked { p.to_string() } else { "NULL".into() };
    exec(r, &format!("INSERT INTO transactions (user_id,proven_tx_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,{link},'{status}','{reference}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{stamp},{stamp})")).await;
}

/// A manager's drain of R into W (the reader local).
pub async fn drain(w: &Arc<StorageSqlx>, r: Arc<StorageSqlx>) {
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(w.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    mgr.sync_from_reader(ALICE, r as Arc<dyn MonitorStorage>)
        .await
        .unwrap();
}

/// The status of W's row `reference` and whether it is linked.
pub async fn row(s: &StorageSqlx, reference: &str) -> String {
    let (st, p): (String, Option<i64>) =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE reference = ?")
            .bind(reference)
            .fetch_one(s.pool())
            .await
            .unwrap();
    format!("{st}:{}", if p.is_some() { "linked" } else { "-" })
}

/// What `s` holds of `n`: each transaction row as `reference:status:linked`,
/// the request, and whether the proof is recorded as checked.
pub async fn view(s: &StorageSqlx, n: u8) -> String {
    let (txid, _, _, _) = mined(n);
    let rows: Vec<(String, String, Option<i64>)> = ::sqlx::query_as(
        "SELECT reference, status, proven_tx_id FROM transactions WHERE txid = ? ORDER BY transaction_id",
    )
    .bind(&txid)
    .fetch_all(s.pool())
    .await
    .unwrap();
    let rows: Vec<String> = rows
        .into_iter()
        .map(|(r, st, p)| format!("{r}:{st}:{}", if p.is_some() { "linked" } else { "-" }))
        .collect();
    let req: Option<(String, Option<i64>, String)> =
        ::sqlx::query_as("SELECT status, proven_tx_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let req = req
        .map(|(st, p, _)| format!("{st}{}", if p.is_some() { "+link" } else { "" }))
        .unwrap_or_else(|| "none".into());
    let checked = count(
        s,
        &format!("SELECT COUNT(*) FROM proof_root_checks WHERE txid = '{txid}'"),
    )
    .await;
    format!("{} | req {req} | checked {checked}", rows.join(" "))
}

/// The request's history of `n` on `s`.
pub async fn history(s: &StorageSqlx, n: u8) -> String {
    let (txid, _, _, _) = mined(n);
    ::sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(&txid)
        .fetch_one(s.pool())
        .await
        .unwrap()
}
