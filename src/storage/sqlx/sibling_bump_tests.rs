//! P0-1d (bsv-stack-lean #56): a transaction the wallet holds a proof for is
//! left unlinked in the input BEEF when its stored BUMP carries its own leaf
//! as a plain hash and its sibling's as the txid leaf (two transactions mined
//! side by side in one block, the provider's path flagged for the sibling).
//!
//! The soak shape (2026-10-08): `867358fe` and its child `65e78b0a` mined at
//! 970030, offsets 160 and 161; the stored row for `65e78b0a` flags 160, not
//! 161. The walk merged it as stored and linked by explicit index, which
//! bsv-rs's `verify_valid` accepts (it checks `contains`, flagged or not), so
//! the template's BEEF left the wallet; the next `createAction` merged it as
//! `inputBEEF` with the strict linker, the link was gone, and the BEEF was
//! refused: "missing inputs [867358fe]".

use std::collections::HashSet;

use bsv_rs::transaction::{Beef, ChainTracker, MerklePath, MerklePathLeaf, MockChainTracker};
use chrono::Utc;

use super::create_action::{
    beef_bfs_walk, compact_stored_beef, describe_invalid_beef, link_txids_carried_by_bumps,
    prune_beef_to_roots,
};
use super::reorg_tests::{child_spending, seed_user, txid_of, COINBASE_TXID};
use super::StorageSqlx;
use crate::storage::traits::WalletStorageWriter;

pub(super) const HEIGHT: u32 = 970_030;

/// A tracker that knows the given `(height, root)` pairs and nothing else.
fn tracker_with(roots: &[(u32, &str)]) -> MockChainTracker {
    let mut t = MockChainTracker::new(1_000_000);
    for (h, r) in roots {
        t.add_root(*h, r.to_string());
    }
    t
}

/// The walk exactly as `build_input_beef` runs it for one change input: the
/// walk, then the trim to the roots.
async fn walk_and_prune(
    s: &StorageSqlx,
    roots: &[String],
    tracker: Option<&MockChainTracker>,
) -> Beef {
    let mut beef = Beef::new();
    walk_into(s, &mut beef, roots, tracker).await;
    prune_beef_to_roots(&mut beef, roots);
    beef
}

/// The walk from `roots` into `beef` as it stands.
async fn walk_into(
    s: &StorageSqlx,
    beef: &mut Beef,
    roots: &[String],
    tracker: Option<&MockChainTracker>,
) {
    let mut conn = s.pool().acquire().await.unwrap();
    let mut pending: Vec<(String, usize)> = roots.iter().cloned().map(|t| (t, 0)).collect();
    let mut processed = HashSet::new();
    beef_bfs_walk(
        &mut conn,
        Some(s.writer()),
        beef,
        &mut pending,
        &mut processed,
        Some(s),
        tracker.map(|t| t as &dyn ChainTracker),
    )
    .await
    .unwrap();
}

fn describe_shape(beef: &Beef) -> String {
    let mut out = String::new();
    for (i, b) in beef.bumps.iter().enumerate() {
        let leaves: Vec<String> = b.path[0]
            .iter()
            .map(|l| {
                format!(
                    "{}@{}{}",
                    l.hash.as_deref().map(|h| &h[..8]).unwrap_or("dup"),
                    l.offset,
                    if l.txid { "(txid)" } else { "" }
                )
            })
            .collect();
        out.push_str(&format!(
            "bump {i} h={} leaves [{}]; ",
            b.block_height,
            leaves.join(",")
        ));
    }
    for tx in &beef.txs {
        out.push_str(&format!(
            "tx {} bump {:?}; ",
            &tx.txid()[..8],
            tx.bump_index()
        ));
    }
    out
}

/// Two transactions mined side by side: `a` at offset 0 and its child `b`
/// at offset 1, and a template `t` spending `b`.
pub(super) struct Siblings {
    pub a: (Vec<u8>, String),
    pub b: (Vec<u8>, String),
    pub t: (Vec<u8>, String),
    pub root: String,
}

pub(super) fn siblings() -> Siblings {
    let a_raw = child_spending(COINBASE_TXID);
    let a = txid_of(&a_raw);
    let b_raw = child_spending(&a);
    let b = txid_of(&b_raw);
    let t_raw = child_spending(&b);
    let t = txid_of(&t_raw);
    let root = path_flagging(&a, &b, &a).compute_root(Some(&a)).unwrap();
    Siblings {
        a: (a_raw, a),
        b: (b_raw, b),
        t: (t_raw, t),
        root,
    }
}

/// The block's two-leaf path with only `flagged`'s leaf marked as a txid,
/// written in the provider's order (offset 1 first), as the soak row was.
pub(super) fn path_flagging(a: &str, b: &str, flagged: &str) -> MerklePath {
    let leaf = |offset: u64, hash: &str| MerklePathLeaf {
        offset,
        hash: Some(hash.to_string()),
        txid: hash == flagged,
        duplicate: false,
    };
    MerklePath {
        block_height: HEIGHT,
        path: vec![vec![leaf(1, b), leaf(0, a)]],
    }
}

/// Whether `txid`'s level-0 leaf is flagged in some bump of `beef`.
pub(super) fn leaf_flagged(beef: &Beef, txid: &str) -> bool {
    beef.bumps.iter().any(|b| {
        b.path[0]
            .iter()
            .any(|l| l.txid && l.hash.as_deref() == Some(txid))
    })
}

/// What the next `createAction` does with this BEEF as its `inputBEEF`:
/// parse the bytes and merge them (`build_input_beef`, create_action.rs).
pub(super) fn as_next_input_beef(beef: &mut Beef) -> Beef {
    let parsed = Beef::from_binary(&beef.to_binary()).unwrap();
    let mut next = Beef::new();
    next.merge_beef(&parsed);
    next
}

/// Storage holding `a` and `b` as completed with proven rows at [`HEIGHT`]:
/// `a`'s path flagged for `a`, `b`'s path flagged for `a` (the soak's row).
pub(super) async fn soak_storage(x: &Siblings) -> StorageSqlx {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test", "000000").await.unwrap();
    s.make_available().await.unwrap();
    let (user_id, _) = seed_user(&s).await;
    for (raw, txid) in [&x.a, &x.b] {
        let path = path_flagging(&x.a.1, &x.b.1, &x.a.1).to_binary();
        let now = Utc::now();
        let proven_tx_id: i64 = sqlx::query_scalar(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING proven_tx_id",
        )
        .bind(txid)
        .bind(HEIGHT as i64)
        .bind(if txid == &x.a.1 { 0i64 } else { 1i64 })
        .bind("b".repeat(64))
        .bind(&x.root)
        .bind(path)
        .bind(raw)
        .bind(now)
        .bind(now)
        .fetch_one(s.writer())
        .await
        .unwrap();
        super::reorg_tests::seed_completed_tx(&s, user_id, txid, Some(proven_tx_id), Some(raw))
            .await;
    }
    s
}

// =============================================================================
// The witness
// =============================================================================

#[tokio::test]
async fn the_walk_flags_the_leaf_of_a_stored_proof_flagged_for_its_sibling() {
    let x = siblings();
    let s = soak_storage(&x).await;
    let tracker = tracker_with(&[(HEIGHT, &x.root)]);
    let roots = vec![x.b.1.clone()];
    let mut beef = walk_and_prune(&s, &roots, Some(&tracker)).await;
    assert_eq!(beef.txs.len(), 1, "b alone: its proof stops the walk");
    let walked = describe_shape(&beef);
    let flagged = leaf_flagged(&beef, &x.b.1);
    let mut next = as_next_input_beef(&mut beef);
    assert_eq!(
        next.find_txid(&x.b.1).and_then(|t| t.bump_index()),
        Some(0),
        "the link survives the bytes and the next merge: walk [{walked}] next [{}]",
        describe_shape(&next)
    );
    assert!(
        flagged,
        "b's own leaf is flagged in the BEEF that carries it: {walked}"
    );
    assert!(
        next.verify_valid(true).valid,
        "{}",
        describe_invalid_beef(&mut next, &roots)
    );
}

#[tokio::test]
async fn compaction_flags_the_leaf_it_links() {
    let x = siblings();
    let s = soak_storage(&x).await;
    let tracker = tracker_with(&[(HEIGHT, &x.root)]);
    // A stored input BEEF from before `b` was mined: `b` rides raw.
    let mut stored = Beef::new();
    stored.merge_raw_tx(x.b.0.clone(), None);
    let mut conn = s.pool().acquire().await.unwrap();
    compact_stored_beef(
        &mut conn,
        Some(s.writer()),
        &mut stored,
        Some(&tracker as &dyn ChainTracker),
    )
    .await
    .unwrap();
    assert!(
        stored
            .find_txid(&x.b.1)
            .and_then(|t| t.bump_index())
            .is_some(),
        "compaction attaches b's stored proof"
    );
    assert!(
        leaf_flagged(&stored, &x.b.1),
        "and flags b's leaf: {}",
        describe_shape(&stored)
    );
    let next = as_next_input_beef(&mut stored);
    assert!(
        next.find_txid(&x.b.1)
            .and_then(|t| t.bump_index())
            .is_some(),
        "the link survives the next merge: {}",
        describe_shape(&next)
    );
}

#[test]
fn two_bumps_for_one_block_merge_without_losing_a_flag() {
    let x = siblings();
    for order in [[&x.a.1, &x.b.1], [&x.b.1, &x.a.1]] {
        let mut beef = Beef::new();
        for flagged in order {
            beef.merge_bump(path_flagging(&x.a.1, &x.b.1, flagged));
        }
        assert_eq!(beef.bumps.len(), 1, "one block, one bump");
        assert!(
            leaf_flagged(&beef, &x.a.1) && leaf_flagged(&beef, &x.b.1),
            "both flags kept: {}",
            describe_shape(&beef)
        );
    }
}

/// Replay the soak wallet's walk from a read-only copy of its storage.
/// Ignored unless `P0_1D_DB` names the copy. The copy is attached with
/// `immutable=1` to an in-memory storage and its three tables are read into
/// memory: nothing is written to the copy and no file is made from it.
/// `P0_1D_DB=... cargo test --lib sibling_bump -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "needs P0_1D_DB"]
async fn replay_p0_1d_soak_walk() {
    let db = std::env::var("P0_1D_DB").expect("P0_1D_DB");
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test", "000000").await.unwrap();
    s.make_available().await.unwrap();
    {
        let mut conn = s.pool().acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(&format!(
            "ATTACH DATABASE 'file:{db}?mode=ro&immutable=1' AS src"
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
        for (table, cols) in [
            ("users", "user_id, identity_key, active_storage, created_at, updated_at"),
            (
                "proven_txs",
                "proven_tx_id, txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at",
            ),
            (
                "transactions",
                "transaction_id, user_id, proven_tx_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, input_beef, raw_tx, created_at, updated_at",
            ),
            (
                "proven_tx_reqs",
                "proven_tx_req_id, proven_tx_id, status, attempts, notified, txid, batch, history, notify, raw_tx, input_beef, created_at, updated_at",
            ),
        ] {
            sqlx::query(&format!("DELETE FROM main.{table}"))
                .execute(&mut *conn)
                .await
                .unwrap();
            sqlx::query(&format!(
                "INSERT INTO main.{table} ({cols}) SELECT {cols} FROM src.{table}"
            ))
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        sqlx::query("DETACH DATABASE src")
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    let root = "65e78b0a7568e8f0aa9280c4ecec0c50b643202bd82a73adc8cd742a18f1ee95".to_string();
    let roots = vec![root.clone()];
    let tracker = tracker_with(&[(
        970_030,
        "5d10e97ae8385236a1fc5a36dbe786975cbc8112b31cf18ba84b6dadda001b8d",
    )]);
    for (name, t) in [("no tracker", None), ("tracker", Some(&tracker))] {
        let mut beef = walk_and_prune(&s, &roots, t).await;
        // The input BEEF the daemon stored for the template d19a5317.
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT input_beef FROM proven_tx_reqs WHERE txid = 'd19a5317d65cb5cc233927deca1c5b04a34f28ad146cf5cb67a53a71dcde199b'",
        )
        .fetch_one(s.pool())
        .await
        .unwrap();
        println!(
            "replay ({name}): walk {} same bytes as the daemon's stored input BEEF: {}",
            describe_shape(&beef),
            beef.to_binary() == stored
        );
        let mut next = as_next_input_beef(&mut beef);
        let linked = next.find_txid(&root).and_then(|t| t.bump_index()).is_some();
        let valid = next.verify_valid(true).valid;
        // `describe_invalid_beef` writes the bytes to /tmp; the owner's
        // wallet state is not copied, so the sorter is asked directly.
        let sr = next.sort_txs();
        println!(
            "replay ({name}): next input BEEF linked={linked} valid={valid} {} missing inputs {:?}",
            describe_shape(&next),
            sr.missing_inputs
        );
        if std::env::var("P0_1D_PRINT_TEMPLATE_BEEF").is_ok() && t.is_none() {
            // The template's BEEF as the next createAction receives it, on
            // one stdout line for the TypeScript SDK's check (piped, never
            // written to a file).
            let template: Vec<u8> = sqlx::query_scalar(
                "SELECT raw_tx FROM transactions WHERE txid = 'd19a5317d65cb5cc233927deca1c5b04a34f28ad146cf5cb67a53a71dcde199b'",
            )
            .fetch_one(s.pool())
            .await
            .unwrap();
            let mut t_beef = Beef::from_binary(&beef.to_binary()).unwrap();
            t_beef.merge_raw_tx(template, None);
            println!("P0_1D_TEMPLATE_BEEF {}", hex::encode(t_beef.to_binary()));
        }
        assert!(
            leaf_flagged(&beef, &root),
            "65e78b0a's leaf is flagged ({name})"
        );
        assert!(linked && valid, "the next merge keeps the link ({name})");
    }
}

/// The monitor's compaction (`MonitorStorage::compact_input_beefs`) rewrites
/// a completed request's stored input BEEF with the proofs it now holds, and
/// stores the result: the link it writes must survive the next merge.
#[tokio::test]
async fn the_monitors_compaction_stores_the_leaf_flagged() {
    use crate::storage::MonitorStorage;
    use std::sync::Arc;
    let x = siblings();
    let s = soak_storage(&x).await;
    super::reorg_tests::insert_req(&s, &x.t.1, &x.t.0, "completed", 0, None, Utc::now()).await;
    let mut stored = Beef::new();
    stored.merge_raw_tx(x.b.0.clone(), None);
    stored.merge_raw_tx(super::proof_root_tests::unreached_filler(), None);
    let stored_bytes = stored.to_binary();
    assert!(
        stored_bytes.len() > 1000,
        "precondition: above the threshold"
    );
    sqlx::query("UPDATE proven_tx_reqs SET input_beef = ? WHERE txid = ?")
        .bind(&stored_bytes)
        .bind(&x.t.1)
        .execute(s.writer())
        .await
        .unwrap();
    s.set_chain_tracker(Arc::new(tracker_with(&[(HEIGHT, &x.root)])))
        .await;

    assert_eq!(MonitorStorage::compact_input_beefs(&s).await.unwrap(), 1);

    let (after,): (Vec<u8>,) =
        sqlx::query_as("SELECT input_beef FROM proven_tx_reqs WHERE txid = ?")
            .bind(&x.t.1)
            .fetch_one(s.pool())
            .await
            .unwrap();
    let mut after = Beef::from_binary(&after).unwrap();
    let next = as_next_input_beef(&mut after);
    assert!(
        next.find_txid(&x.b.1)
            .and_then(|t| t.bump_index())
            .is_some(),
        "the stored link survives the next merge: {}",
        describe_shape(&next)
    );
    assert!(
        leaf_flagged(&after, &x.b.1),
        "b's leaf is flagged in storage"
    );
}

// =============================================================================
// bsv-rs #732, #765: of two BUMPs holding a txid the LAST is its proof, as
// the reference's index answers (ts-stack@edf6e03 `Beef.ts:576-585`): the
// walk's shortcut and the carried-txid linker take it too
// =============================================================================

/// A second block's path holding `b` at offset 0 beside another leaf, at
/// `HEIGHT + 1` (a reorg's second proof of one txid).
fn second_proof_of(b: &str) -> MerklePath {
    let leaf = |offset: u64, hash: String, txid: bool| MerklePathLeaf {
        offset,
        hash: Some(hash),
        txid,
        duplicate: false,
    };
    MerklePath {
        block_height: HEIGHT + 1,
        path: vec![vec![
            leaf(0, b.to_string(), false),
            leaf(1, "cd".repeat(32), false),
        ]],
    }
}

/// The walk meets `b`, which two BUMPs of the BEEF hold, and links the stored
/// transaction to the last of them. Red with the earliest (`position`), the
/// toolbox's own index before the beta.7 re-pin.
#[tokio::test]
async fn the_walk_links_a_txid_two_bumps_hold_to_the_last_732() {
    let x = siblings();
    let s = soak_storage(&x).await;
    let mut beef = Beef::new();
    beef.merge_bump(path_flagging(&x.a.1, &x.b.1, &x.a.1));
    beef.merge_bump(second_proof_of(&x.b.1));
    assert_eq!(beef.bumps.len(), 2, "two blocks, two BUMPs");
    walk_into(&s, &mut beef, std::slice::from_ref(&x.b.1), None).await;
    let linked = beef
        .find_txid(&x.b.1)
        .and_then(bsv_rs::transaction::BeefTx::bump_index);
    assert_eq!(linked, Some(1), "b's proof is the last BUMP holding it");
    assert_eq!(beef.find_bump(&x.b.1).unwrap().block_height, HEIGHT + 1);
}

/// The carried-txid linker, on an entry no merge linked (the bytes a
/// beta.6 or go-sdk writer leaves): the last BUMP holding it, its leaf
/// flagged. Red with the earliest.
#[test]
fn the_linker_takes_the_last_bump_holding_a_txid_765() {
    let x = siblings();
    let mut beef = Beef::new();
    beef.merge_bump(path_flagging(&x.a.1, &x.b.1, &x.a.1));
    beef.merge_bump(second_proof_of(&x.b.1));
    beef.merge_raw_tx(x.b.0.clone(), None);
    for tx in &mut beef.txs {
        tx.set_bump_index(None);
    }
    for bump in &mut beef.bumps {
        for leaf in &mut bump.path[0] {
            leaf.txid = leaf.hash.as_deref() == Some(x.a.1.as_str());
        }
    }
    link_txids_carried_by_bumps(&mut beef);
    assert_eq!(beef.find_txid(&x.b.1).unwrap().bump_index(), Some(1));
    let flagged = |i: usize| {
        beef.bumps[i].path[0]
            .iter()
            .any(|l| l.txid && l.hash.as_deref() == Some(x.b.1.as_str()))
    };
    assert!(
        flagged(1) && !flagged(0),
        "the last BUMP's leaf alone flagged"
    );
}
