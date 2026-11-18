//! The status review beside a request's `invalid` word: a transaction that
//! holds a proof this store has checked against its own headers is passed
//! over and its request completed; a proof row this store has not checked is
//! checked before (a root its headers do not carry demotes it, the proof asked
//! again; a tracker that does not answer moves nothing); a transaction with no
//! proof row is failed as before. Two stores under a manager, and one.
#![cfg(feature = "sqlite")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf};
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    GetMerklePathResult, GetStatusForTxidsResult, TxStatusDetail,
};
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
const HEIGHT: u32 = 100;

/// A store's own headers: the roots it carries by height, whether it
/// answers at all, and how many root reads it was asked.
#[derive(Default)]
struct Headers {
    roots: Mutex<HashMap<u32, String>>,
    silent: AtomicBool,
    /// Heights the tracker errors at every time.
    silent_at: Mutex<std::collections::HashSet<u32>>,
    reads: AtomicUsize,
    /// A write another path makes while the review waits on this answer:
    /// the store and the txid whose transaction it fails, taken once.
    fail_on_read: Mutex<Option<(Arc<StorageSqlx>, String)>>,
}

impl Headers {
    fn carrying(height: u32, root: &str) -> Arc<Self> {
        let h = Arc::new(Self::default());
        h.serve(height, root);
        h
    }
    fn serve(&self, height: u32, root: &str) {
        self.roots.lock().unwrap().insert(height, root.to_string());
    }
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
        let hook = self.fail_on_read.lock().unwrap().take();
        if let Some((s, txid)) = hook {
            exec(
                &s,
                &format!("UPDATE transactions SET status = 'failed' WHERE txid = '{txid}'"),
            )
            .await;
            exec(
                &s,
                &format!("UPDATE outputs SET spendable = 0 WHERE txid = '{txid}'"),
            )
            .await;
        }
        if self.silent.load(Ordering::SeqCst) || self.silent_at.lock().unwrap().contains(&height) {
            return Err(ChainTrackerError::NetworkError("no answer".to_string()));
        }
        Ok(self.roots.lock().unwrap().get(&height).map(String::as_str) == Some(root))
    }
    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(HEIGHT + 10)
    }
}

/// The txid of the transaction `n` (TX_HEX with its lock time set to `n`),
/// its raw bytes, its proof as the only transaction of a block at HEIGHT,
/// and that proof's root.
fn mined(n: u8) -> (String, Vec<u8>, Vec<u8>, String) {
    mined_at(n, HEIGHT)
}

/// As `mined`, the block at `height`.
fn mined_at(n: u8, height: u32) -> (String, Vec<u8>, Vec<u8>, String) {
    let mut raw = hex::decode(TX_HEX).unwrap();
    let last = raw.len() - 4;
    raw[last] = n;
    let tx = bsv_rs::transaction::Transaction::from_binary(&raw).unwrap();
    let txid = tx.id();
    let path = MerklePath::new(
        height,
        vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
    )
    .unwrap();
    let root = path.compute_root(Some(&txid)).unwrap();
    (txid, raw, path.to_binary(), root)
}

/// A store of `sik` with Alice, whose active storage is `active`.
async fn alice_store(sik: &str, name: &str, active: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate(name, sik).await.unwrap();
    s.make_available().await.unwrap();
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    s.update_user_active_storage(u.user_id, active)
        .await
        .unwrap();
    s
}

/// An outgoing transaction of Alice's (`status`) with one change output
/// (spendable as `spendable`) and its request (`req`), written by SQL.
async fn seed(s: &StorageSqlx, n: u8, status: &str, spendable: i64, req: &str) -> String {
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(&raw);
    let now = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
    let tx = exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES (1,'{status}','ref{n}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{now},{now})")).await;
    exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES (1,{tx},{spendable},1,0,5000,'storage','change','P2PKH','{txid}',1,0,X'51',{now},{now})")).await;
    exec(s, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ('{txid}','{req}',0,'{{}}',0,'{{}}',X'{raw_hex}',{now},{now})")).await;
    txid
}

/// What one store holds of `txid`: the transaction's status and proof
/// link, the proof row and whether its root is recorded as checked here,
/// the request's status and its history notes' `what`, the outputs'
/// `spendable`.
async fn rows(s: &StorageSqlx, txid: &str) -> String {
    let tx: Option<(String, Option<i64>)> =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let proof: Option<i64> =
        ::sqlx::query_scalar("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let checked = count(
        s,
        &format!("SELECT COUNT(*) FROM proof_root_checks WHERE txid = '{txid}'"),
    )
    .await;
    let req: Option<(String, Option<i64>, String)> =
        ::sqlx::query_as("SELECT status, proven_tx_id, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let spendable: Vec<i64> = ::sqlx::query_scalar(
        "SELECT o.spendable FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id WHERE t.txid = ? ORDER BY o.vout",
    )
    .bind(txid)
    .fetch_all(s.pool())
    .await
    .unwrap();
    let notes = req
        .as_ref()
        .and_then(|r| serde_json::from_str::<serde_json::Value>(&r.2).ok())
        .and_then(|h| h.get("notes").cloned())
        .and_then(|n| n.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|n| n.get("what").and_then(|w| w.as_str()).map(str::to_string))
        .collect::<Vec<_>>();
    let (tx_status, tx_link) = tx.map(|t| (t.0, t.1)).unwrap_or(("none".into(), None));
    let (req_status, req_link) = req.map(|r| (r.0, r.1)).unwrap_or(("none".into(), None));
    format!(
        "tx {tx_status} link {} | proof {} checked {checked} | req {req_status} link {} notes {notes:?} | spendable {spendable:?}",
        tx_link.is_some() && tx_link == proof,
        proof.is_some(),
        req_link.is_some() && req_link == proof,
    )
}

fn status_mined(txid: &str) -> MockResponse<GetStatusForTxidsResult> {
    MockResponse::Success(GetStatusForTxidsResult {
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
    })
}

fn no_path() -> MockResponse<GetMerklePathResult> {
    MockResponse::Success(GetMerklePathResult {
        name: Some("MockProvider".to_string()),
        merkle_path: None,
        header: None,
        error: None,
        notes: vec![],
    })
}

/// Store B wrote the request `invalid` (the unfail task's finding) and the
/// transaction `failed`; the host switched to A, where the transaction was
/// recovered (the canary) and proven (the proof funnel); A pushed to B; the
/// host switched back to B; B's review; the push back to A. Every step's
/// rows of both stores are printed. B's transaction stays `completed`, its
/// output counted, its request `completed`; nothing `failed` reaches A.
#[tokio::test]
async fn a_proof_synced_from_the_other_store_stands_against_an_invalid_request() {
    let (_, _, path, root) = mined(1);
    let b = alice_store(SRC, "B", SRC).await;
    let a = alice_store(DST, "A", SRC).await;
    let headers = Headers::carrying(HEIGHT, &root);
    b.set_chain_tracker(headers.clone()).await;
    a.set_chain_tracker(headers.clone()).await;
    let txid = seed(&b, 1, "unproven", 1, "unfail").await;
    b.set_services(Arc::new(
        MockWalletServices::builder()
            .get_merkle_path_response(no_path())
            .build(),
    ));
    a.set_services(Arc::new(
        MockWalletServices::builder()
            .get_status_for_txids_response(status_mined(&txid))
            .build(),
    ));
    let mgr = WalletStorageManager::new(
        ALICE.to_string(),
        Some(b.clone() as Arc<dyn MonitorStorage>),
        Some(vec![a.clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    let step = |what: &str, b_rows: String, a_rows: String| {
        println!("WITNESS STEP {what}\n  B: {b_rows}\n  A: {a_rows}");
    };
    step(
        "0 seeded on B",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    b.un_fail().await.unwrap();
    MonitorStorage::review_status(&*b).await.unwrap();
    step(
        "1 B: un_fail (no path) then review",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    mgr.set_active(DST).await.unwrap();
    step(
        "2 switch to A",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    // The canary's backoff: the request's stamp aged past an hour.
    exec(
        &a,
        &format!(
            "UPDATE proven_tx_reqs SET updated_at = '2026-01-01T00:00:00Z' WHERE txid = '{txid}'"
        ),
    )
    .await;
    a.un_fail().await.unwrap();
    step(
        "3a A: un_fail canary (status sources mined)",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );
    a.set_max_acceptable_proof_height(HEIGHT + 5).await.unwrap();
    let outcome = a
        .ingest_merkle_proof(&txid, &path, HEIGHT, &"e".repeat(64), Some(&root))
        .await
        .unwrap();
    step(
        &format!("3b A: proof funnel {outcome:?}"),
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    mgr.update_backups().await.unwrap();
    step(
        "4 push A to B",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    mgr.set_active(SRC).await.unwrap();
    step(
        "5 switch back to B",
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    let before = headers.reads();
    let review = MonitorStorage::review_status(&*b).await.unwrap();
    let reads = headers.reads() - before;
    step(
        &format!("6 B: review ({reads} header reads) {:?}", review.log.trim()),
        rows(&b, &txid).await,
        rows(&a, &txid).await,
    );

    mgr.update_backups().await.unwrap();
    let (b_end, a_end) = (rows(&b, &txid).await, rows(&a, &txid).await);
    step("7 push B to A", b_end.clone(), a_end.clone());

    assert!(b_end.starts_with("tx completed link true"), "{b_end}");
    assert!(b_end.contains("req completed link true"), "{b_end}");
    assert!(b_end.contains("checked 1"), "{b_end}");
    assert!(b_end.ends_with("spendable [1]"), "{b_end}");
    assert!(a_end.starts_with("tx completed"), "{a_end}");
    assert!(a_end.ends_with("spendable [1]"), "{a_end}");
}

/// One store with a tracker carrying `root` at HEIGHT and services.
async fn one_store(headers: Option<Arc<Headers>>) -> Arc<StorageSqlx> {
    let s = alice_store(SRC, "one", SRC).await;
    if let Some(h) = headers {
        s.set_chain_tracker(h).await;
    }
    s.set_services(Arc::new(MockWalletServices::new()));
    s.set_max_acceptable_proof_height(HEIGHT + 5).await.unwrap();
    s
}

/// Transaction `n` as a proof row that arrived unchecked (written by SQL,
/// as the sync writer stores a local reader's row), the transaction
/// `completed` and linked to it, its request `invalid`.
async fn synced_proof_beside_invalid(s: &StorageSqlx, n: u8) -> String {
    synced_proof_beside_invalid_at(s, n, HEIGHT).await
}

/// As `synced_proof_beside_invalid`, the proof's block at `height`.
async fn synced_proof_beside_invalid_at(s: &StorageSqlx, n: u8, height: u32) -> String {
    let (txid, raw, path, root) = mined_at(n, height);
    let txid2 = seed(s, n, "completed", 1, "invalid").await;
    assert_eq!(txid, txid2);
    ::sqlx::query("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES (?,?,0,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")
        .bind(&txid)
        .bind(height as i64)
        .bind("e".repeat(64))
        .bind(&root)
        .bind(&path)
        .bind(&raw)
        .execute(s.writer())
        .await
        .unwrap();
    exec(
        s,
        &format!("UPDATE transactions SET proven_tx_id = (SELECT proven_tx_id FROM proven_txs WHERE txid = '{txid}') WHERE txid = '{txid}'"),
    )
    .await;
    txid
}

/// One store: a transaction `completed` with the proof this store's funnel
/// checked and stored, its request then written `invalid`. The review
/// passes over it without a header read and completes the request.
#[tokio::test]
async fn a_checked_proof_beside_an_invalid_request_is_passed_over() {
    let (_, _, path, root) = mined(2);
    let headers = Headers::carrying(HEIGHT, &root);
    let s = one_store(Some(headers.clone())).await;
    let txid = seed(&s, 2, "unproven", 1, "unmined").await;
    s.ingest_merkle_proof(&txid, &path, HEIGHT, &"e".repeat(64), Some(&root))
        .await
        .unwrap();
    exec(
        &s,
        &format!("UPDATE proven_tx_reqs SET status = 'invalid' WHERE txid = '{txid}'"),
    )
    .await;
    println!(
        "WITNESS single store before review: {}",
        rows(&s, &txid).await
    );
    let before = headers.reads();
    MonitorStorage::review_status(&*s).await.unwrap();
    let after = rows(&s, &txid).await;
    println!(
        "WITNESS single store after review ({} header reads): {after}",
        headers.reads() - before
    );
    assert_eq!(headers.reads() - before, 0);
    assert!(after.starts_with("tx completed link true"), "{after}");
    assert!(after.contains("req completed link true"), "{after}");
    assert!(after.contains("invalidRequestProofStands"), "{after}");
    assert!(after.ends_with("spendable [1]"), "{after}");
}

/// A synced proof row whose root this store's headers do not carry is
/// demoted, as every unchecked proof a tracker refutes is on this line:
/// neither failed nor completed, the transaction unproven with its output
/// counted, the request back to `unmined` for the proof task to ask again.
/// Once the headers carry the block, the proof task's next pass completes it.
#[tokio::test]
async fn a_synced_proof_whose_root_does_not_check_is_asked_again() {
    let (_, _, path, root) = mined(3);
    let headers = Arc::new(Headers::default());
    let s = one_store(Some(headers.clone())).await;
    let txid = synced_proof_beside_invalid(&s, 3).await;
    MonitorStorage::review_status(&*s).await.unwrap();
    let held = rows(&s, &txid).await;
    println!("WITNESS unchecked root, headers without it: {held}");
    assert!(
        held.starts_with("tx unproven link false | proof false"),
        "{held}"
    );
    assert!(held.contains("req unmined"), "{held}");
    assert!(held.ends_with("spendable [1]"), "{held}");

    headers.serve(HEIGHT, &root);
    let services = MockWalletServices::builder()
        .get_status_for_txids_response(status_mined(&txid))
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("MockProvider".to_string()),
            merkle_path: Some(hex::encode(&path)),
            header: Some(bsv_wallet_toolbox_rs::services::BlockHeader {
                version: 1,
                previous_hash: "00".repeat(32),
                merkle_root: root.clone(),
                time: 1,
                bits: 1,
                nonce: 1,
                hash: "e".repeat(64),
                height: HEIGHT,
            }),
            error: None,
            notes: vec![],
        }))
        .build();
    s.set_services(Arc::new(services));
    let synced = s.synchronize_transaction_statuses().await.unwrap();
    println!("WITNESS the proof task's pass: {} proven", synced.len());
    MonitorStorage::review_status(&*s).await.unwrap();
    let done = rows(&s, &txid).await;
    println!("WITNESS headers serve the block, the proof task's next pass: {done}");
    assert!(done.starts_with("tx completed link true"), "{done}");
    assert!(done.contains("req completed link true"), "{done}");
    assert!(done.ends_with("spendable [1]"), "{done}");
}

/// A synced proof row and no answer (no tracker, then a tracker that does
/// not answer): neither word moves, the same candidate next pass; then the
/// headers answer and the next pass completes it.
#[tokio::test]
async fn a_synced_proof_with_no_answer_moves_neither_word() {
    let s = one_store(None).await;
    let txid = synced_proof_beside_invalid(&s, 4).await;
    let start = rows(&s, &txid).await;
    MonitorStorage::review_status(&*s).await.unwrap();
    let none = rows(&s, &txid).await;
    println!("WITNESS no tracker: {none}");
    assert_eq!(none, start);
    assert!(
        none.starts_with("tx completed link true | proof true checked 0"),
        "{none}"
    );
    assert!(none.contains("req invalid"), "{none}");

    let (_, _, _, root) = mined(4);
    let headers = Headers::carrying(HEIGHT, &root);
    headers.silent.store(true, Ordering::SeqCst);
    s.set_chain_tracker(headers.clone()).await;
    MonitorStorage::review_status(&*s).await.unwrap();
    let silent = rows(&s, &txid).await;
    println!(
        "WITNESS a tracker that does not answer ({} reads): {silent}",
        headers.reads()
    );
    assert_eq!(silent, start);
    assert_eq!(headers.reads(), 1);

    headers.silent.store(false, Ordering::SeqCst);
    MonitorStorage::review_status(&*s).await.unwrap();
    let done = rows(&s, &txid).await;
    println!("WITNESS the tracker answers: {done}");
    assert!(
        done.starts_with("tx completed link true | proof true checked 1"),
        "{done}"
    );
    assert!(done.contains("req completed link true"), "{done}");
}

/// A request `invalid` and no proof row: failed and its output voided, as
/// before.
#[tokio::test]
async fn an_invalid_request_without_a_proof_still_fails() {
    let headers = Arc::new(Headers::default());
    let s = one_store(Some(headers.clone())).await;
    let txid = seed(&s, 5, "unproven", 1, "invalid").await;
    MonitorStorage::review_status(&*s).await.unwrap();
    let after = rows(&s, &txid).await;
    println!("WITNESS no proof row: {after}");
    assert!(after.starts_with("tx failed"), "{after}");
    assert!(after.ends_with("spendable [0]"), "{after}");
    assert_eq!(headers.reads(), 0);
}

/// The load: one header read per unchecked candidate per pass, at most 100
/// per pass. A tracker that does not answer costs the same reads next pass
/// and writes nothing; once it answers, a refuted candidate leaves the set
/// (demoted) and a confirmed one leaves it (its request completed), so the
/// pass after reads nothing.
#[tokio::test]
async fn the_review_reads_one_header_per_unchecked_candidate_within_its_bound() {
    let headers = Arc::new(Headers::default());
    let s = one_store(Some(headers.clone())).await;
    let mut txids = Vec::new();
    for n in 10..111u8 {
        txids.push(synced_proof_beside_invalid(&s, n).await);
    }
    headers.silent.store(true, Ordering::SeqCst);
    let mut per_pass = Vec::new();
    for _ in 0..2 {
        let before = headers.reads();
        MonitorStorage::review_status(&*s).await.unwrap();
        per_pass.push(headers.reads() - before);
    }
    let invalid = count(
        &s,
        "SELECT COUNT(*) FROM proven_tx_reqs WHERE status = 'invalid'",
    )
    .await;
    println!("WITNESS load, 101 candidates, a tracker that does not answer: reads per pass {per_pass:?}, requests still invalid {invalid}");
    assert_eq!(per_pass, vec![100, 100]);
    assert_eq!(invalid, 101);

    // The headers answer: the last candidate's root is carried, the others not.
    headers.silent.store(false, Ordering::SeqCst);
    headers.serve(HEIGHT, &mined(110).3);
    let mut answered = Vec::new();
    for _ in 0..3 {
        let before = headers.reads();
        MonitorStorage::review_status(&*s).await.unwrap();
        answered.push(headers.reads() - before);
    }
    let failed = count(
        &s,
        "SELECT COUNT(*) FROM transactions WHERE status = 'failed'",
    )
    .await;
    let last = rows(&s, &txids[100]).await;
    println!("WITNESS load, the headers answer: reads per pass {answered:?}, failed {failed}; the carried one: {last}");
    assert_eq!(answered, vec![100, 1, 0]);
    assert_eq!(failed, 0);
    assert!(last.contains("req completed link true"), "{last}");
}

/// A transaction another path fails between the pass's read of its
/// candidates and its write (here while the tracker answers): the write
/// reads the transaction again and leaves it, so nothing of it is written
/// at this pass; its request is not completed and check 3 does not write a
/// failed transaction `completed` beside outputs left unspendable.
#[tokio::test]
async fn a_transaction_failed_between_the_read_and_the_write_is_left() {
    let (_, _, _, root) = mined(6);
    let headers = Headers::carrying(HEIGHT, &root);
    let s = one_store(Some(headers.clone())).await;
    let txid = synced_proof_beside_invalid(&s, 6).await;
    *headers.fail_on_read.lock().unwrap() = Some((s.clone(), txid.clone()));
    MonitorStorage::review_status(&*s).await.unwrap();
    let after = rows(&s, &txid).await;
    println!(
        "WITNESS failed between the read and the write ({} reads): {after}",
        headers.reads()
    );
    assert_eq!(headers.reads(), 1);
    assert!(after.starts_with("tx failed"), "{after}");
    assert!(after.contains("req invalid"), "{after}");
    assert!(!after.contains("invalidRequestProofStands"), "{after}");
    assert!(after.ends_with("spendable [0]"), "{after}");
}

/// No candidate holds a slot for good: 150 candidates the tracker errors on
/// at every read, and one behind them (a higher proof row number) whose
/// root the headers carry. A pass takes 100 after where the last pass
/// stopped, so the one behind 150 is looked at within ceil(151 / 100) = 2
/// passes and completed; no pass reads more than 100 headers, and nothing
/// of the 150 requests it leaves `invalid` is written, their `updated_at`
/// included.
#[tokio::test]
async fn a_candidate_behind_150_that_never_leave_is_reached_within_two_passes() {
    const STUCK: u32 = HEIGHT - 1;
    let (_, _, _, root) = mined(200);
    let headers = Headers::carrying(HEIGHT, &root);
    headers.silent_at.lock().unwrap().insert(STUCK);
    let s = one_store(Some(headers.clone())).await;
    for n in 10..160u8 {
        synced_proof_beside_invalid_at(&s, n, STUCK).await;
    }
    let behind = synced_proof_beside_invalid(&s, 200).await;
    let stamps = format!(
        "SELECT group_concat(CAST(updated_at AS TEXT), ',') FROM (SELECT updated_at FROM proven_tx_reqs WHERE txid != '{behind}' ORDER BY txid)"
    );
    let stamps_before: String = ::sqlx::query_scalar(&stamps)
        .fetch_one(s.pool())
        .await
        .unwrap();
    let mut per_pass = Vec::new();
    let mut reached = None;
    for pass in 1..=3 {
        let before = headers.reads();
        MonitorStorage::review_status(&*s).await.unwrap();
        per_pass.push(headers.reads() - before);
        if reached.is_none() && rows(&s, &behind).await.contains("req completed link true") {
            reached = Some(pass);
        }
    }
    let stuck = count(
        &s,
        "SELECT COUNT(*) FROM proven_tx_reqs WHERE status = 'invalid'",
    )
    .await;
    let last = rows(&s, &behind).await;
    println!("WITNESS 150 that never leave and one behind: reads per pass {per_pass:?}, completed at pass {reached:?}, still invalid {stuck}; the one behind: {last}");
    assert_eq!(reached, Some(2), "{last}");
    assert!(per_pass.iter().all(|r| *r <= 100), "{per_pass:?}");
    assert_eq!(stuck, 150);
    assert!(last.starts_with("tx completed link true"), "{last}");
    let stamps_after: String = ::sqlx::query_scalar(&stamps)
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(
        stamps_after, stamps_before,
        "a request left invalid was written"
    );
}

/// A proof row whose path does not parse: the pass writes neither word on
/// it (no tracker read, the transaction, request and outputs as they were)
/// and it does not stop the pass: a candidate behind it is completed.
#[tokio::test]
async fn a_proof_row_whose_path_does_not_parse_moves_nothing_and_stops_nothing() {
    let (_, _, _, root) = mined(8);
    let headers = Headers::carrying(HEIGHT, &root);
    let s = one_store(Some(headers.clone())).await;
    let garbled = synced_proof_beside_invalid(&s, 7).await;
    exec(
        &s,
        &format!("UPDATE proven_txs SET merkle_path = X'ff' WHERE txid = '{garbled}'"),
    )
    .await;
    let behind = synced_proof_beside_invalid(&s, 8).await;
    let start = rows(&s, &garbled).await;
    MonitorStorage::review_status(&*s).await.unwrap();
    let after = rows(&s, &garbled).await;
    let done = rows(&s, &behind).await;
    println!(
        "WITNESS a path that does not parse ({} reads): {after}; the one behind: {done}",
        headers.reads()
    );
    assert_eq!(after, start);
    assert!(after.contains("req invalid"), "{after}");
    assert_eq!(headers.reads(), 1);
    assert!(done.contains("req completed link true"), "{done}");
}
