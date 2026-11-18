//! The status review's check 3 beside a request that reads `completed` with
//! a proof link: a transaction of that txid that is not `completed` is
//! completed only where this store has checked the root of the linked proof
//! against its own headers. A proof row a sync brought in unchecked is
//! checked by the review's bounded pass before (the tracker asked with no
//! lock held, then one write that reads each row again): confirmed, the root
//! is recorded and the transaction completed in that write; refuted, the
//! proof is demoted and asked again, a `failed` row staying `failed`; with
//! no tracker or no answer nothing is written.
//!
//! The road, through the manager's real sync: Alice's store A proves her
//! payment (the proof funnel, root checked on A) and pushes to the shared
//! backup B (the request arrives `completed` with its link, the proof row
//! with no root-check record on B); then Bob's own row of the same txid
//! arrives on B from his store C, `unproven` or `failed`, after the request
//! completed.
#![cfg(feature = "sqlite")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf};
use bsv_rs::wallet::ListOutputsArgs;
use bsv_wallet_toolbox_rs::managers::WalletStorageManager;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;

mod sync_support;
use sync_support::*;

const TX_HEX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
const HEIGHT: u32 = 100;
const A_KEY: &str = SRC;
const B_KEY: &str = DST;
const C_KEY: &str = "3333333333333333333333333333333333333333333333333333333333333333";

/// A store's headers: the roots it carries by height, whether it answers,
/// how many root reads it was asked, and one write another path makes on a
/// store while the review waits on an answer.
#[derive(Default)]
struct Headers {
    roots: Mutex<HashMap<u32, String>>,
    silent: AtomicBool,
    silent_at: Mutex<std::collections::HashSet<u32>>,
    reads: AtomicUsize,
    on_read: Mutex<Option<(Arc<StorageSqlx>, String)>>,
}

impl Headers {
    fn carrying(height: u32, root: &str) -> Arc<Self> {
        let h = Arc::new(Self::default());
        h.roots.lock().unwrap().insert(height, root.to_string());
        h
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
        let hook = self.on_read.lock().unwrap().take();
        if let Some((s, sql)) = hook {
            exec(&s, &sql).await;
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

/// The txid of transaction `n` (TX_HEX with its lock time set to `n`), its
/// raw bytes, its proof as the only transaction of a block at `height`,
/// and that proof's root.
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

fn mined(n: u8) -> (String, Vec<u8>, Vec<u8>, String) {
    mined_at(n, HEIGHT)
}

/// A store `sik` holding `users` (identity, active storage), with mock
/// services and `headers` as its tracker.
async fn store_of(
    sik: &str,
    name: &str,
    users: &[(&str, &str)],
    headers: Option<Arc<Headers>>,
) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate(name, sik).await.unwrap();
    s.make_available().await.unwrap();
    for (identity, active) in users {
        let (u, _) = s.find_or_insert_user(identity).await.unwrap();
        s.update_user_active_storage(u.user_id, active)
            .await
            .unwrap();
    }
    if let Some(h) = headers {
        s.set_chain_tracker(h).await;
    }
    s.set_services(Arc::new(MockWalletServices::new()));
    s.set_max_acceptable_proof_height(HEIGHT + 5).await.unwrap();
    s
}

async fn user_id(s: &StorageSqlx, identity: &str) -> i64 {
    s.find_user(identity).await.unwrap().unwrap().user_id
}

/// A payment of `user`'s (`status`) with one change output of 5000 in the
/// default basket (spendable as `spendable`) and its request (`req`). The
/// state of a broadcast payment (`unproven` / `unmined`) or of one the
/// store failed (`failed`, its output voided, `invalid`), as the action and
/// review paths leave them; written by SQL as #861's witnesses write theirs
/// (the processAction door needs a signer this file does not carry).
async fn seed(
    s: &StorageSqlx,
    identity: &str,
    n: u8,
    status: &str,
    spendable: i64,
    req: &str,
) -> String {
    let uid = user_id(s, identity).await;
    let who = &identity[2..6];
    let (txid, raw, _, _) = mined(n);
    let raw_hex = hex::encode(&raw);
    let now = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
    let tx = exec(s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,raw_tx,created_at,updated_at) VALUES ({uid},'{status}','ref{n}{who}',1,-1000,1,0,'pay','{txid}',X'{raw_hex}',{now},{now})")).await;
    exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,basket_id,spendable,change,vout,satoshis,provided_by,purpose,type,txid,script_length,script_offset,locking_script,created_at,updated_at) VALUES ({uid},{tx},(SELECT basket_id FROM output_baskets WHERE user_id = {uid} AND name = 'default'),{spendable},1,0,5000,'storage','change','P2PKH','{txid}',1,0,X'51',{now},{now})")).await;
    let held = count(
        s,
        &format!("SELECT COUNT(*) FROM proven_tx_reqs WHERE txid = '{txid}'"),
    )
    .await;
    if held == 0 {
        exec(s, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ('{txid}','{req}',0,'{{}}',0,'{{}}',X'{raw_hex}',{now},{now})")).await;
    }
    txid
}

fn default_basket() -> ListOutputsArgs {
    ListOutputsArgs {
        basket: "default".to_string(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: Some(100),
        offset: Some(0),
        seek_permission: None,
    }
}

/// What `s` holds of `txid`: each user's transaction status (Alice's then
/// Bob's), whether the proof row is there and recorded as checked, the
/// request's status and whether its link names that row, Bob's spendable
/// outputs, and Bob's `listOutputs` count and sum.
async fn view(s: &StorageSqlx, txid: &str) -> String {
    let mut parts = Vec::new();
    for (who, identity) in [("alice", ALICE), ("bob", BOB)] {
        let st: Option<String> = ::sqlx::query_scalar(
            "SELECT t.status FROM transactions t JOIN users u ON u.user_id = t.user_id WHERE u.identity_key = ? AND t.txid = ?",
        )
        .bind(identity)
        .bind(txid)
        .fetch_optional(s.pool())
        .await
        .unwrap();
        parts.push(format!("{who} {}", st.unwrap_or_else(|| "none".into())));
    }
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
    let req: Option<(String, Option<i64>)> =
        ::sqlx::query_as("SELECT status, proven_tx_id FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(s.pool())
            .await
            .unwrap();
    let (req_status, req_link) = req.unwrap_or(("none".into(), None));
    let bob_spendable = count(
        s,
        &format!("SELECT COUNT(*) FROM outputs o JOIN users u ON u.user_id = o.user_id WHERE u.identity_key = '{BOB}' AND o.txid = '{txid}' AND o.spendable = 1"),
    )
    .await;
    let listed = match s.find_user(BOB).await.unwrap() {
        Some(u) => {
            let auth = AuthId::with_user_id(BOB, u.user_id);
            let r = s.list_outputs(&auth, default_basket()).await.unwrap();
            let sum: u64 = r.outputs.iter().map(|o| o.satoshis).sum();
            format!("{} sum {sum}", r.total_outputs)
        }
        None => "no user".into(),
    };
    format!(
        "{} | proof {} checked {checked} | req {req_status} link {} | bob spendable {bob_spendable} listOutputs {listed}",
        parts.join(" "),
        proof.is_some(),
        req_link.is_some() && req_link == proof,
    )
}

/// Every row of `s` that the review could write, with its stamps: what a
/// pass that writes nothing leaves byte for byte.
async fn snapshot(s: &StorageSqlx) -> String {
    let q = "SELECT \
      (SELECT group_concat(status || CAST(updated_at AS TEXT) || IFNULL(proven_tx_id,'-'), ',') FROM transactions) || '/' || \
      (SELECT group_concat(status || CAST(updated_at AS TEXT) || IFNULL(proven_tx_id,'-') || history, ',') FROM proven_tx_reqs) || '/' || \
      IFNULL((SELECT group_concat(proven_tx_id, ',') FROM proven_txs), '') || '/' || \
      IFNULL((SELECT group_concat(txid, ',') FROM proof_root_checks), '') || '/' || \
      (SELECT group_concat(spendable || CAST(updated_at AS TEXT), ',') FROM outputs)";
    ::sqlx::query_scalar(q).fetch_one(s.pool()).await.unwrap()
}

/// The road: A proves Alice's payment `n` and pushes to B; then Bob's row
/// of the same txid (`bob_status`, its output `bob_spendable`, his store's
/// request `bob_req`) arrives on B from C. Returns B, A and the txid; how
/// many reads of B's headers the syncs made is asserted none.
async fn bring(
    n: u8,
    bob_status: &str,
    bob_spendable: i64,
    bob_req: &str,
    b_headers: Option<Arc<Headers>>,
) -> (Arc<StorageSqlx>, Arc<StorageSqlx>, String) {
    let (_, _, path, root) = mined(n);
    let a = store_of(
        A_KEY,
        "A",
        &[(ALICE, A_KEY)],
        Some(Headers::carrying(HEIGHT, &root)),
    )
    .await;
    let b = store_of(
        B_KEY,
        "B",
        &[(ALICE, A_KEY), (BOB, C_KEY)],
        b_headers.clone(),
    )
    .await;
    let c = store_of(C_KEY, "C", &[(BOB, C_KEY)], None).await;
    let txid = seed(&a, ALICE, n, "unproven", 1, "unmined").await;
    a.ingest_merkle_proof(&txid, &path, HEIGHT, &"e".repeat(64), Some(&root))
        .await
        .unwrap();
    let alice = WalletStorageManager::new(
        ALICE.to_string(),
        Some(a.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    alice.make_available().await.unwrap();
    alice.update_backups().await.unwrap();
    seed(&c, BOB, n, bob_status, bob_spendable, bob_req).await;
    let bob = WalletStorageManager::new(
        BOB.to_string(),
        Some(c.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    bob.make_available().await.unwrap();
    bob.update_backups().await.unwrap();
    if let Some(h) = &b_headers {
        assert_eq!(h.reads(), 0, "the syncs read B's headers");
    }
    (b, a, txid)
}

/// Item 0's cells: B's review over each, its header reads, and what B then
/// holds. Printed for the run on any tree; the asserts are the rule.
async fn cell(n: u8, bob: (&str, i64, &str), confirms: bool) -> (String, String, usize) {
    let (_, _, _, root) = mined(n);
    let headers = if confirms {
        Headers::carrying(HEIGHT, &root)
    } else {
        Arc::new(Headers::default())
    };
    let (b, _a, txid) = bring(n, bob.0, bob.1, bob.2, Some(headers.clone())).await;
    let before = view(&b, &txid).await;
    MonitorStorage::review_status(&*b).await.unwrap();
    let after = view(&b, &txid).await;
    let reads = headers.reads();
    println!(
        "WITNESS CELL bob {} tracker {}: before {before}\n  after review ({reads} header reads): {after}",
        bob.0,
        if confirms { "confirms" } else { "refutes" }
    );
    (before, after, reads)
}

#[tokio::test]
async fn an_unchecked_proof_the_tracker_confirms_completes_an_unproven_row_at_that_pass() {
    let (before, after, reads) = cell(1, ("unproven", 1, "unmined"), true).await;
    assert!(before.contains("bob unproven"), "{before}");
    assert!(
        before.contains("checked 0 | req completed link true"),
        "{before}"
    );
    assert_eq!(reads, 1);
    assert!(after.contains("bob completed"), "{after}");
    assert!(
        after.contains("checked 1 | req completed link true"),
        "{after}"
    );
}

#[tokio::test]
async fn an_unchecked_proof_the_tracker_confirms_completes_a_failed_row_as_before() {
    let (_, after, reads) = cell(2, ("failed", 0, "invalid"), true).await;
    assert_eq!(reads, 1);
    assert!(after.contains("bob completed"), "{after}");
    assert!(
        after.contains("checked 1 | req completed link true"),
        "{after}"
    );
}

#[tokio::test]
async fn an_unchecked_proof_the_tracker_refutes_completes_no_unproven_row() {
    let (_, after, reads) = cell(3, ("unproven", 1, "unmined"), false).await;
    assert_eq!(reads, 1);
    assert!(after.starts_with("alice unproven bob unproven"), "{after}");
    assert!(
        after.contains("proof false checked 0 | req unmined link false"),
        "{after}"
    );
    assert!(after.contains("bob spendable 1"), "{after}");
}

#[tokio::test]
async fn an_unchecked_proof_the_tracker_refutes_leaves_a_failed_row_failed() {
    let (_, after, reads) = cell(4, ("failed", 0, "invalid"), false).await;
    assert_eq!(reads, 1);
    assert!(after.starts_with("alice unproven bob failed"), "{after}");
    assert!(
        after.contains("proof false checked 0 | req unmined link false"),
        "{after}"
    );
    assert!(after.contains("bob spendable 0"), "{after}");
}

/// No tracker on B, then a tracker that does not answer: nothing written,
/// two passes each.
#[tokio::test]
async fn with_no_tracker_or_no_answer_nothing_is_written_twice() {
    for (n, silent) in [(5u8, None), (6u8, Some(Arc::new(Headers::default())))] {
        if let Some(h) = &silent {
            h.silent.store(true, Ordering::SeqCst);
        }
        let (b, _a, txid) = bring(n, "unproven", 1, "unmined", silent.clone()).await;
        let start = snapshot(&b).await;
        for pass in 1..=2 {
            MonitorStorage::review_status(&*b).await.unwrap();
            let now = snapshot(&b).await;
            println!(
                "WITNESS no answer ({}) pass {pass}: {}",
                if silent.is_some() {
                    "silent tracker"
                } else {
                    "no tracker"
                },
                view(&b, &txid).await
            );
            assert_eq!(now, start, "pass {pass} wrote");
        }
        assert!(view(&b, &txid).await.contains("bob unproven"));
    }
}

/// The store that checked the proof (A) gets Bob's row after the request
/// completed: check 3 completes it at once with no header read.
#[tokio::test]
async fn a_checked_proof_completes_a_second_users_row_with_no_header_read() {
    let (_, _, path, root) = mined(7);
    let headers = Headers::carrying(HEIGHT, &root);
    let a = store_of(
        A_KEY,
        "A",
        &[(ALICE, A_KEY), (BOB, C_KEY)],
        Some(headers.clone()),
    )
    .await;
    let c = store_of(C_KEY, "C", &[(BOB, C_KEY)], None).await;
    let txid = seed(&a, ALICE, 7, "unproven", 1, "unmined").await;
    a.ingest_merkle_proof(&txid, &path, HEIGHT, &"e".repeat(64), Some(&root))
        .await
        .unwrap();
    seed(&c, BOB, 7, "unproven", 1, "unmined").await;
    let bob = WalletStorageManager::new(
        BOB.to_string(),
        Some(c.clone() as Arc<dyn MonitorStorage>),
        Some(vec![a.clone() as Arc<dyn MonitorStorage>]),
    );
    bob.make_available().await.unwrap();
    bob.update_backups().await.unwrap();
    let reads = headers.reads();
    let before = view(&a, &txid).await;
    MonitorStorage::review_status(&*a).await.unwrap();
    let after = view(&a, &txid).await;
    println!(
        "WITNESS checked proof, Bob's row after: before {before}\n  after ({} header reads): {after}",
        headers.reads() - reads
    );
    assert!(before.contains("bob unproven"), "{before}");
    assert_eq!(headers.reads() - reads, 0);
    assert!(after.contains("alice completed bob completed"), "{after}");
}

/// The request moves while the tracker answers (another path demotes it):
/// the confirmation writes nothing of it, no record, no completion.
#[tokio::test]
async fn a_row_that_changed_while_the_tracker_answered_is_not_written() {
    let (_, _, _, root) = mined(8);
    let headers = Headers::carrying(HEIGHT, &root);
    let (b, _a, txid) = bring(8, "unproven", 1, "unmined", Some(headers.clone())).await;
    *headers.on_read.lock().unwrap() = Some((
        b.clone(),
        format!("UPDATE proven_tx_reqs SET status = 'unmined', proven_tx_id = NULL WHERE txid = '{txid}'"),
    ));
    MonitorStorage::review_status(&*b).await.unwrap();
    let after = view(&b, &txid).await;
    println!(
        "WITNESS changed while the tracker answered ({} reads): {after}",
        headers.reads()
    );
    assert_eq!(headers.reads(), 1);
    assert!(after.contains("bob unproven"), "{after}");
    assert!(after.contains("checked 0 | req unmined"), "{after}");
}

/// One store, rows by SQL (151 three-store roads would be the same rows):
/// a proof row with no record, linked from a `completed` request, beside a
/// transaction not completed; or beside an `invalid` request.
async fn unchecked_at(s: &StorageSqlx, n: u8, height: u32, req: &str) -> String {
    let (txid, raw, path, root) = mined_at(n, height);
    seed(s, ALICE, n, "unproven", 1, req).await;
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
    let link = format!("(SELECT proven_tx_id FROM proven_txs WHERE txid = '{txid}')");
    if req == "completed" {
        exec(
            s,
            &format!("UPDATE proven_tx_reqs SET proven_tx_id = {link} WHERE txid = '{txid}'"),
        )
        .await;
    } else {
        exec(
            s,
            &format!("UPDATE transactions SET status = 'completed', proven_tx_id = {link} WHERE txid = '{txid}'"),
        )
        .await;
    }
    txid
}

/// 150 candidates of one kind whose height no tracker answers, and one of
/// the other kind behind them: the one behind is reached at pass 2, at most
/// 100 header reads a pass, whichever kind is ahead.
#[tokio::test]
async fn one_candidate_behind_150_of_the_other_kind_is_reached_within_two_passes() {
    const STUCK: u32 = HEIGHT - 1;
    for (ahead, behind_kind) in [("invalid", "completed"), ("completed", "invalid")] {
        let (_, _, _, root) = mined(200);
        let headers = Headers::carrying(HEIGHT, &root);
        headers.silent_at.lock().unwrap().insert(STUCK);
        let s = store_of(A_KEY, "one", &[(ALICE, A_KEY)], Some(headers.clone())).await;
        for n in 10..160u8 {
            unchecked_at(&s, n, STUCK, ahead).await;
        }
        let behind = unchecked_at(&s, 200, HEIGHT, behind_kind).await;
        let mut per_pass = Vec::new();
        let mut reached = None;
        for pass in 1..=3 {
            let before = headers.reads();
            MonitorStorage::review_status(&*s).await.unwrap();
            per_pass.push(headers.reads() - before);
            if reached.is_none() && view(&s, &behind).await.contains("checked 1") {
                reached = Some(pass);
            }
        }
        let last = view(&s, &behind).await;
        println!(
            "WITNESS 150 {ahead} ahead, one {behind_kind} behind: reads per pass {per_pass:?}, reached at pass {reached:?}: {last}"
        );
        assert_eq!(reached, Some(2), "{last}");
        assert!(per_pass.iter().all(|r| *r <= 100), "{per_pass:?}");
        assert!(last.starts_with("alice completed"), "{last}");
        assert!(last.contains("req completed link true"), "{last}");
    }
}
