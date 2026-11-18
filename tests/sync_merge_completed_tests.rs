//! The sync's merge of a transaction row this store holds, beside a proof
//! row whose root this store has not checked: the merge takes the link and
//! leaves the status this store holds; `completed` is written by the status
//! review's pass once this store's tracker confirms the root (a refutation
//! leaves the status as it was, no tracker writes nothing). A proof this
//! store had checked: the merge completes as it did.
//!
//! The road: a manager's drain from a local reader (`sync_from_reader`),
//! the reader holding proof rows, requests reading `completed` and its own
//! rows of the txids reading `completed` and linked. Three shapes: rows
//! under other references (inserted, the held rows untouched), the same
//! references with an older row (the merge of proof evidence), the same
//! references with a newer row (the merge of ordinary properties).
#![cfg(feature = "sqlite")]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf};
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
const HEIGHT: u32 = 100;

/// A store's headers: the (height, root) pairs it carries and how many
/// root reads it was asked.
#[derive(Default)]
struct Headers {
    roots: Mutex<HashSet<(u32, String)>>,
    reads: AtomicUsize,
}

impl Headers {
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
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

/// Transaction `n` (TX_HEX with its lock time `n`): txid, bytes, its proof
/// as the only transaction of a block at HEIGHT, that proof's root.
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

#[derive(Clone, Copy, PartialEq, Debug)]
enum Shape {
    /// The reader's rows under other references.
    OtherRefs,
    /// The same references, the reader's rows older (the merge of proof
    /// evidence).
    SameRefsOlder,
    /// The same references, the reader's rows newer (the merge of ordinary
    /// properties).
    SameRefsNewer,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Tracker {
    Confirms,
    Refutes,
    None,
}

async fn store(sik: &str, name: &str, headers: Option<Arc<Headers>>) -> Arc<StorageSqlx> {
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

const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
const OLD: &str = "'2026-01-01T00:00:00.000Z'";
const LATER: &str = "strftime('%Y-%m-%dT%H:%M:%fZ','now','+1 day')";

/// A transaction row of Alice's (`status`, reference `reference`, stamped
/// `stamp`) with one change output of 1000 (spendable as `spendable`).
async fn held(s: &StorageSqlx, n: u8, status: &str, reference: &str, spendable: i64) {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(raw);
    let t = exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'{status}','{reference}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{NOW},{NOW})")).await;
    exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES (1,{t},{spendable},1,0,1000,'storage','change','P2PKH','{txid}',1,0,X'51',{NOW},{NOW})")).await;
}

/// The proof row of transaction `n` on `s`, stamped `stamp`; returns its id.
async fn proof_row(s: &StorageSqlx, n: u8, stamp: &str) -> i64 {
    let (txid, raw, path, root) = mined(n);
    exec(s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{txid}',{HEIGHT},0,'{h}','{root}',X'{p}',X'{r}',{stamp},{stamp})", h = "e".repeat(64), p = hex::encode(path), r = hex::encode(raw))).await
}

/// The reader's rows of transaction `n`: the proof, a request `completed`
/// linked to it, and its own transaction row `completed` and linked under
/// `reference`, stamped `stamp`.
async fn reader_rows(r: &StorageSqlx, n: u8, reference: &str, stamp: &str) {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(raw);
    let p = proof_row(r, n, stamp).await;
    exec(r, &format!("INSERT INTO proven_tx_reqs (proven_tx_id,txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ({p},'{txid}','completed',0,'{{}}',0,'{{}}',X'{raw_hex}',{stamp},{stamp})")).await;
    exec(r, &format!("INSERT INTO transactions (user_id,proven_tx_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,{p},'completed','{reference}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{stamp},{stamp})")).await;
}

/// What `s` holds of transaction `n`: each transaction row as
/// `reference:status:linked`, the request, whether the proof is recorded
/// as checked, and the held output's spendable flag.
async fn view(s: &StorageSqlx, n: u8) -> String {
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
    let req: Option<(String, Option<i64>)> =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM proven_tx_reqs WHERE txid = ?")
            .bind(&txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let req = req
        .map(|(st, p)| format!("{st}{}", if p.is_some() { "+link" } else { "" }))
        .unwrap_or_else(|| "none".into());
    let checked = count(
        s,
        &format!("SELECT COUNT(*) FROM proof_root_checks WHERE txid = '{txid}' AND merkle_root NOT LIKE 'refuted:%'"),
    )
    .await;
    let spendable = count(
        s,
        &format!("SELECT COALESCE(SUM(spendable), 0) FROM outputs WHERE txid = '{txid}'"),
    )
    .await;
    format!(
        "{} | req {req} | checked {checked} | spendable {spendable}",
        rows.join(" ")
    )
}

/// The drain of the reader's rows of X (held `failed`) and Y (held
/// `unproven`) into W in `shape`, then W's status review under `tracker`.
/// Returns the views after the drain and after the review, and the header
/// reads of each.
async fn cell(shape: Shape, tracker: Tracker, checked_here: bool) -> [String; 4] {
    let (x, y) = (11u8, 12u8);
    let headers = Arc::new(Headers::default());
    if tracker == Tracker::Confirms {
        for n in [x, y] {
            headers.roots.lock().unwrap().insert((HEIGHT, mined(n).3));
        }
    }
    let w = store(
        DST,
        "W",
        (tracker != Tracker::None).then(|| headers.clone()),
    )
    .await;
    held(&w, x, "failed", "wx", 0).await;
    held(&w, y, "unproven", "wy", 1).await;
    if checked_here {
        for n in [x, y] {
            proof_row(&w, n, OLD).await;
            let (txid, _, _, root) = mined(n);
            exec(&w, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{root}',{NOW})")).await;
        }
    }
    let r = store(SRC, "R", None).await;
    let stamp = match shape {
        Shape::SameRefsNewer => LATER,
        _ => OLD,
    };
    for (n, own) in [(x, "wx"), (y, "wy")] {
        let reference = if shape == Shape::OtherRefs {
            own.replace('w', "r")
        } else {
            own.to_string()
        };
        reader_rows(&r, n, &reference, stamp).await;
    }
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(w.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    mgr.sync_from_reader(ALICE, r as Arc<dyn MonitorStorage>)
        .await
        .unwrap();
    let drain_reads = headers.reads();
    let after_drain = format!("X {} || Y {}", view(&w, x).await, view(&w, y).await);
    MonitorStorage::review_status(&*w).await.unwrap();
    let after_review = format!("X {} || Y {}", view(&w, x).await, view(&w, y).await);
    let review_reads = headers.reads() - drain_reads;
    println!(
        "WITNESS {shape:?} tracker {tracker:?} checked_here {checked_here}\n  after the drain ({drain_reads} header reads): {after_drain}\n  after the review ({review_reads} header reads): {after_review}"
    );
    [
        after_drain,
        drain_reads.to_string(),
        after_review,
        review_reads.to_string(),
    ]
}

/// The drain leaves W's own rows where they were (`failed`, `unproven`);
/// in the same-reference shapes each now carries the link.
fn assert_drain_leaves_status(shape: Shape, v: &[String; 4]) {
    assert_eq!(v[1], "0", "the drain read no header: {}", v[0]);
    if shape == Shape::OtherRefs {
        assert!(v[0].contains("X wx:failed:-"), "{}", v[0]);
        assert!(v[0].contains("Y wy:unproven:-"), "{}", v[0]);
    } else {
        assert!(v[0].contains("X wx:failed:linked"), "{}", v[0]);
        assert!(v[0].contains("Y wy:unproven:linked"), "{}", v[0]);
    }
    assert!(v[0].contains("checked 0"), "{}", v[0]);
}

const SHAPES: [Shape; 3] = [Shape::OtherRefs, Shape::SameRefsOlder, Shape::SameRefsNewer];

#[tokio::test]
async fn a_drain_leaves_the_held_status_and_a_confirming_tracker_completes_both() {
    for shape in SHAPES {
        let v = cell(shape, Tracker::Confirms, false).await;
        assert_drain_leaves_status(shape, &v);
        assert!(v[2].contains("wx:completed:linked"), "{}", v[2]);
        assert!(v[2].contains("wy:completed:linked"), "{}", v[2]);
        assert!(
            !v[2].contains(":failed") && !v[2].contains(":unproven"),
            "{}",
            v[2]
        );
        // The failed row completed as check 3 writes it: the status and the
        // link alone, its output as the failure left it.
        assert!(
            v[2].starts_with("X ") && v[2].contains("checked 1 | spendable 0 ||"),
            "{}",
            v[2]
        );
        assert_eq!(v[3], "2", "one header read per proof");
    }
}

#[tokio::test]
async fn a_refuting_tracker_demotes_and_brings_the_failed_row_back_unproven() {
    for shape in SHAPES {
        let v = cell(shape, Tracker::Refutes, false).await;
        assert_drain_leaves_status(shape, &v);
        // The failed row unproven beside its request asking.
        assert!(v[2].contains("wx:unproven:-"), "{}", v[2]);
        assert!(v[2].contains("req unmined"), "{}", v[2]);
        assert!(v[2].contains("wy:unproven:-"), "{}", v[2]);
        assert!(
            !v[2].contains("wx:completed") && !v[2].contains("wy:completed"),
            "{}",
            v[2]
        );
        assert!(v[2].contains("checked 0"), "{}", v[2]);
    }
}

#[tokio::test]
async fn with_no_tracker_the_review_writes_nothing() {
    for shape in SHAPES {
        let v = cell(shape, Tracker::None, false).await;
        assert_drain_leaves_status(shape, &v);
        assert_eq!(v[0], v[2], "the review wrote nothing");
    }
}

#[tokio::test]
async fn a_proof_this_store_had_checked_completes_in_the_merge_as_before() {
    for shape in [Shape::SameRefsOlder, Shape::SameRefsNewer] {
        let v = cell(shape, Tracker::Refutes, true).await;
        assert_eq!(v[1], "0", "{}", v[0]);
        assert!(v[0].contains("X wx:completed:linked"), "{}", v[0]);
        assert!(v[0].contains("Y wy:completed:linked"), "{}", v[0]);
        assert_eq!(v[3], "0", "a checked proof costs no header read");
    }
}

/// A state no task that asks the chain completes on a tracker alone: W
/// holds X `failed` beside its own request `invalid` and Y `nosend` beside
/// a `nosend` request; the drain
/// keeps both requests (the merge of a held request) and links both rows to
/// an unchecked proof. The review's pass takes them: confirmed, every row
/// and the request `completed`; refuted, the links cleared, the `failed`
/// row `unproven` with its request asking, the `nosend` row's status and
/// request left; no tracker, nothing written.
#[tokio::test]
async fn a_linked_row_beside_a_request_no_task_asks_is_a_candidate_of_the_pass() {
    let (x, y) = (21u8, 22u8);
    for tracker in [Tracker::Confirms, Tracker::Refutes, Tracker::None] {
        let headers = Arc::new(Headers::default());
        let w = store(
            DST,
            "W",
            (tracker != Tracker::None).then(|| headers.clone()),
        )
        .await;
        let r = store(SRC, "R", None).await;
        for (n, status, req) in [(x, "failed", "invalid"), (y, "nosend", "nosend")] {
            let (txid, raw, _, root) = mined(n);
            if tracker == Tracker::Confirms {
                headers.roots.lock().unwrap().insert((HEIGHT, root));
            }
            let reference = format!("w{n}");
            held(&w, n, status, &reference, 0).await;
            exec(&w, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ('{txid}','{req}',0,'{{}}',0,'{{}}',X'{}',{NOW},{NOW})", hex::encode(raw))).await;
            reader_rows(&r, n, &reference, OLD).await;
        }
        let mgr = WalletStorageManager::new(
            ALICE.to_string(),
            Some(w.clone() as Arc<dyn MonitorStorage>),
            None,
        );
        mgr.make_available().await.unwrap();
        mgr.sync_from_reader(ALICE, r as Arc<dyn MonitorStorage>)
            .await
            .unwrap();
        let drained = format!("X {} || Y {}", view(&w, x).await, view(&w, y).await);
        MonitorStorage::review_status(&*w).await.unwrap();
        let reviewed = format!("X {} || Y {}", view(&w, x).await, view(&w, y).await);
        println!("WITNESS linked {tracker:?}\n  after the drain: {drained}\n  after the review: {reviewed}");
        assert!(
            drained.contains("w21:failed:linked | req invalid |"),
            "{drained}"
        );
        assert!(
            drained.contains("w22:nosend:linked | req nosend |"),
            "{drained}"
        );
        match tracker {
            Tracker::Confirms => {
                assert!(
                    reviewed.contains("w21:completed:linked | req completed+link | checked 1"),
                    "{reviewed}"
                );
                assert!(
                    reviewed.contains("w22:completed:linked | req completed+link | checked 1"),
                    "{reviewed}"
                );
                for n in [x, y] {
                    let history: String =
                        ::sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
                            .bind(mined(n).0)
                            .fetch_one(w.pool())
                            .await
                            .unwrap();
                    println!("WITNESS linked {tracker:?} history of {n}: {history}");
                    assert!(
                        history.contains("\"what\":\"linkedProofCompletesRequest\""),
                        "{history}"
                    );
                }
            }
            Tracker::Refutes => {
                assert!(
                    reviewed.contains("w21:unproven:- | req unmined | checked 0"),
                    "{reviewed}"
                );
                assert!(
                    reviewed.contains("w22:nosend:- | req nosend | checked 0"),
                    "{reviewed}"
                );
            }
            Tracker::None => assert_eq!(drained, reviewed),
        }
    }
}
