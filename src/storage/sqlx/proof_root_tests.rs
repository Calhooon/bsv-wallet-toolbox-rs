//! The proof root is required (P0-1, bsv-stack-lean #35): no merkle proof is
//! stored as proven unless a chain tracker confirmed its root at its height,
//! the reference's `validateCanonicalMerklePathResult`
//! (ts-stack@fb1b2da packages/wallet/wallet-toolbox/src/services/getCanonicalMerklePath.ts:17-55),
//! and a stored proof that was never checked is checked once, on the next
//! read that puts it into a BEEF, and demoted when the tracker refutes it.

use std::sync::Arc;

use bsv_rs::transaction::{Beef, MockChainTracker};
use chrono::{Duration, Utc};

use super::create_action::{beef_bfs_walk, compact_stored_beef};
use super::reorg_tests::{
    child_spending, insert_proven, insert_req, open_gate, proven_row, req_state, seed_completed_tx,
    seed_user, single_leaf_bump, storage, tx_state, txid_of, COINBASE_HEX, COINBASE_TXID,
};
use super::StorageSqlx;
use crate::services::mock::{MockResponse, MockWalletServices};
use crate::services::{BlockHeader, GetMerklePathResult, WalletServices};
use crate::storage::traits::{WalletStorageProvider, WalletStorageWriter};
use crate::storage::ProofIngestOutcome;

/// A tracker that knows the given `(height, root)` pairs and nothing else.
fn tracker_with(roots: &[(u32, &str)]) -> MockChainTracker {
    let mut t = MockChainTracker::new(1000);
    for (h, r) in roots {
        t.add_root(*h, r.to_string());
    }
    t
}

/// Store `txid`'s single-leaf proof at `height` through the ingest funnel
/// with a tracker that confirms it, then drop the tracker again. The row is
/// one the wallet checked.
async fn ingest_checked(s: &StorageSqlx, txid: &str, raw: &[u8], height: u32) {
    if req_state(s, txid).await.is_none() {
        insert_req(s, txid, raw, "unmined", 0, None, Utc::now()).await;
    }
    s.set_chain_tracker(Arc::new(tracker_with(&[(height, txid)])))
        .await;
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(height, txid),
            height,
            &"c".repeat(64),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(out, ProofIngestOutcome::Ingested(_)),
        "precondition: stored through the funnel, got {out:?}"
    );
    s.clear_chain_tracker().await;
}

async fn walk(s: &StorageSqlx, root: &str, tracker: Option<&MockChainTracker>) -> Beef {
    let mut conn = s.pool().acquire().await.unwrap();
    let mut beef = Beef::new();
    let mut pending = vec![(root.to_string(), 0usize)];
    let mut processed = std::collections::HashSet::new();
    beef_bfs_walk(
        &mut conn,
        &mut beef,
        &mut pending,
        &mut processed,
        Some(s),
        tracker.map(|t| t as &dyn bsv_rs::transaction::ChainTracker),
    )
    .await
    .unwrap();
    beef
}

// =============================================================================
// Ingest: no tracker, no proof
// =============================================================================

/// The witness of the gap. A storage with no chain tracker receives a BUMP
/// for a transaction it holds: the proof is refused with a named outcome,
/// nothing is written, the request stays unmined. Before the fix the proof
/// was stored and the request completed (`[SRC] bsv-wallet-toolbox-rs@00c1634
/// src/storage/sqlx/storage_sqlx.rs:2677-2695`).
#[tokio::test]
async fn a_trackerless_storage_refuses_a_bump_and_the_row_stays_unproven() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    insert_req(
        &s,
        txid,
        &hex::decode(COINBASE_HEX).unwrap(),
        "unmined",
        0,
        None,
        Utc::now(),
    )
    .await;
    open_gate(&s, 100).await;

    // A root no chain has: any header the sender likes.
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(100, txid),
            100,
            &"f".repeat(64),
            None,
        )
        .await
        .unwrap();

    // Compared by name so this witness compiles, and fails on behavior,
    // against the crate before the fix.
    assert_eq!(
        format!("{out:?}"),
        "TrackerUnavailable",
        "a storage that cannot verify refuses"
    );
    assert!(
        proven_row(&s, txid).await.is_none(),
        "nothing is stored as proven"
    );
    let (status, attempts, linked, _) = req_state(&s, txid).await.unwrap();
    assert_eq!(status, "unmined", "the request stays unproven");
    assert_eq!(attempts, 0, "a refusal is not an attempt");
    assert!(linked.is_none());
}

/// The second door of the same gap: the BEEF walk's own proof fetch
/// (`create_action.rs` `fetch_and_store_merkle_path`) stored a provider's
/// path unchecked when no tracker was wired. Refused the same way.
#[tokio::test]
async fn the_walks_own_fetch_with_no_tracker_stores_nothing() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    insert_req(
        &s,
        txid,
        &raw,
        "unmined",
        0,
        None,
        Utc::now() - Duration::hours(1),
    )
    .await;
    let services = MockWalletServices::builder()
        .get_merkle_path_response(MockResponse::Success(GetMerklePathResult {
            name: Some("mock".to_string()),
            merkle_path: Some(hex::encode(single_leaf_bump(100, txid))),
            header: Some(BlockHeader {
                version: 0x20000000,
                previous_hash: "p".repeat(64),
                merkle_root: txid.to_string(),
                time: 1_700_000_000,
                bits: 0,
                nonce: 0,
                hash: "a".repeat(64),
                height: 100,
            }),
            error: None,
            notes: vec![],
        }))
        .build();
    s.set_services(Arc::new(services) as Arc<dyn WalletServices>);
    open_gate(&s, 100).await;

    let beef = walk(&s, txid, None).await;

    let subject = beef.find_txid(txid).expect("the subject rides in the BEEF");
    assert!(
        subject.bump_index().is_none(),
        "an unverifiable fetched proof is not attached"
    );
    assert!(
        proven_row(&s, txid).await.is_none(),
        "an unverifiable fetched proof is never stored"
    );
    assert_eq!(req_state(&s, txid).await.unwrap().0, "unmined");
}

/// With a tracker: a BUMP whose root is in the active chain is stored; one
/// whose root is not is refused and nothing is written. The reference keeps
/// a path only when `isValidRootForHeight` answers true
/// (getCanonicalMerklePath.ts:39).
#[tokio::test]
async fn with_a_tracker_an_active_root_is_stored_and_an_inactive_one_refused() {
    let s = storage().await;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let txid = COINBASE_TXID;
    insert_req(&s, txid, &raw, "unmined", 0, None, Utc::now()).await;
    open_gate(&s, 200).await;
    s.set_chain_tracker(Arc::new(tracker_with(&[(100, txid)])))
        .await;

    // Not in the active chain: the tracker knows no such root at 150.
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(150, txid),
            150,
            &"f".repeat(64),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(out, ProofIngestOutcome::InvalidMerkleRoot { .. }),
        "got {out:?}"
    );
    assert!(proven_row(&s, txid).await.is_none());
    assert_eq!(req_state(&s, txid).await.unwrap().0, "unmined");

    // In the active chain: stored and completed.
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(100, txid),
            100,
            &"a".repeat(64),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(out, ProofIngestOutcome::Ingested(_)),
        "got {out:?}"
    );
    assert_eq!(proven_row(&s, txid).await.unwrap().0, 100);
    assert_eq!(req_state(&s, txid).await.unwrap().0, "completed");
}

// =============================================================================
// Stored before the fix: checked once on read
// =============================================================================

/// A row stored as proven without a check (written before this fix, or
/// merged by sync) whose root the tracker refutes is demoted on the next
/// read that puts it into a BEEF: the transaction is unproven again, its
/// request unmined with its bytes, and the BEEF carries the raw leg.
#[tokio::test]
async fn an_unchecked_stored_proof_is_demoted_on_read_when_its_root_is_not_active() {
    let s = storage().await;
    let (user_id, _) = seed_user(&s).await;
    let parent_raw = hex::decode(COINBASE_HEX).unwrap();
    let child_raw = child_spending(COINBASE_TXID);
    let child_txid = txid_of(&child_raw);
    open_gate(&s, 1000).await;
    ingest_checked(&s, COINBASE_TXID, &parent_raw, 1).await;
    // Stored as proven at 500 with no check: the gap's own residue.
    let ptx = insert_proven(
        &s,
        &child_txid,
        500,
        &"o".repeat(64),
        &child_raw,
        &single_leaf_bump(500, &child_txid),
    )
    .await;
    seed_completed_tx(&s, user_id, &child_txid, Some(ptx), Some(&child_raw)).await;
    s.set_chain_tracker(Arc::new(tracker_with(&[(1, COINBASE_TXID)])))
        .await;

    let tracker = tracker_with(&[(1, COINBASE_TXID)]);
    let beef = walk(&s, &child_txid, Some(&tracker)).await;

    let child = beef.find_txid(&child_txid).expect("the child rides");
    assert!(
        child.bump_index().is_none(),
        "the refuted bump is not attached"
    );
    assert!(
        beef.find_txid(COINBASE_TXID)
            .and_then(|t| t.bump_index())
            .is_some(),
        "the parent's checked proof terminates the walk"
    );
    assert!(
        proven_row(&s, &child_txid).await.is_none(),
        "the never-checked refuted proof is demoted"
    );
    let (status, linked, _) = tx_state(&s, &child_txid).await;
    assert_eq!(status, "unproven");
    assert!(linked.is_none());
    let (req_status, _, _, req_raw) = req_state(&s, &child_txid).await.unwrap();
    assert_eq!(req_status, "unmined", "re-proved the normal way");
    assert_eq!(req_raw, child_raw, "the bytes survive");
}

/// The same read with no tracker anywhere cannot decide: the row is kept
/// and the bump attached as before (the recipient verifies the BEEF), and
/// it stays unchecked for the next read that has a tracker.
#[tokio::test]
async fn an_unchecked_stored_proof_is_kept_when_no_tracker_can_decide() {
    let s = storage().await;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    insert_proven(
        &s,
        COINBASE_TXID,
        500,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(500, COINBASE_TXID),
    )
    .await;

    let beef = walk(&s, COINBASE_TXID, None).await;
    assert!(beef
        .find_txid(COINBASE_TXID)
        .and_then(|t| t.bump_index())
        .is_some());
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());

    // A later read with a tracker that refutes it: now it is demoted.
    let tracker = tracker_with(&[]);
    walk(&s, COINBASE_TXID, Some(&tracker)).await;
    assert!(proven_row(&s, COINBASE_TXID).await.is_none());
}

/// An unchecked row whose root IS active is checked once and kept; after
/// that it is a checked row, and a later refutation (a reorg) is the reorg
/// and review tasks' business: the walk skips the bump and demotes nothing.
#[tokio::test]
async fn an_unchecked_active_proof_is_checked_once_then_treated_as_checked() {
    let s = storage().await;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    insert_proven(
        &s,
        COINBASE_TXID,
        500,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(500, COINBASE_TXID),
    )
    .await;

    let confirms = tracker_with(&[(500, COINBASE_TXID)]);
    let beef = walk(&s, COINBASE_TXID, Some(&confirms)).await;
    assert!(beef
        .find_txid(COINBASE_TXID)
        .and_then(|t| t.bump_index())
        .is_some());
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());

    let refutes = tracker_with(&[]);
    let beef = walk(&s, COINBASE_TXID, Some(&refutes)).await;
    assert!(
        beef.find_txid(COINBASE_TXID)
            .and_then(|t| t.bump_index())
            .is_none(),
        "a refuted bump is never attached"
    );
    assert!(
        proven_row(&s, COINBASE_TXID).await.is_some(),
        "a checked row is demoted only on positive network evidence"
    );
}

/// A row the funnel wrote with a tracker's confirmation is a checked row:
/// a later refutation on read does not demote it.
#[tokio::test]
async fn a_proof_stored_through_the_funnel_is_a_checked_row() {
    let s = storage().await;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    open_gate(&s, 100).await;
    ingest_checked(&s, COINBASE_TXID, &raw, 100).await;

    let refutes = tracker_with(&[]);
    walk(&s, COINBASE_TXID, Some(&refutes)).await;
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());
}

/// The walk's read falls back to the storage's own tracker when its caller
/// passes none (the broadcast rebuild and the BEEF-for-txids path do): an
/// unchecked refuted row is demoted there too.
#[tokio::test]
async fn the_read_check_uses_the_storages_tracker_when_the_caller_passes_none() {
    let s = storage().await;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    insert_proven(
        &s,
        COINBASE_TXID,
        500,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(500, COINBASE_TXID),
    )
    .await;
    s.set_chain_tracker(Arc::new(tracker_with(&[]))).await;

    walk(&s, COINBASE_TXID, None).await;
    assert!(proven_row(&s, COINBASE_TXID).await.is_none());
}

/// Compaction of a stored input BEEF (the walk's, the broadcast fallback's
/// and the monitor's) is a read that needs the proof: an unchecked stored
/// proof is checked there once, as in the walk. Refuted: demoted and the
/// ancestor stays a raw leg. Confirmed: attached and recorded. No tracker:
/// attached as before, left unchecked.
#[tokio::test]
async fn compaction_checks_an_unchecked_proof_once() {
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let stored_beef = || {
        let mut b = Beef::new();
        b.merge_raw_tx(raw.clone(), None);
        b
    };
    let attached = |b: &Beef| {
        b.find_txid(COINBASE_TXID)
            .and_then(|t| t.bump_index())
            .is_some()
    };
    async fn seeded(raw: &[u8]) -> StorageSqlx {
        let s = storage().await;
        insert_proven(
            &s,
            COINBASE_TXID,
            500,
            &"o".repeat(64),
            raw,
            &single_leaf_bump(500, COINBASE_TXID),
        )
        .await;
        s
    }
    async fn compact(s: &StorageSqlx, beef: &mut Beef, tracker: Option<&MockChainTracker>) {
        let mut conn = s.pool().acquire().await.unwrap();
        compact_stored_beef(
            &mut conn,
            beef,
            tracker.map(|t| t as &dyn bsv_rs::transaction::ChainTracker),
        )
        .await
        .unwrap();
    }

    // Refuted: not attached, demoted.
    let s = seeded(&raw).await;
    let mut beef = stored_beef();
    compact(&s, &mut beef, Some(&tracker_with(&[]))).await;
    assert!(
        !attached(&beef),
        "a refuted unchecked proof is not attached"
    );
    assert!(
        proven_row(&s, COINBASE_TXID).await.is_none(),
        "and is demoted"
    );

    // No tracker: attached as before, kept.
    let s = seeded(&raw).await;
    let mut beef = stored_beef();
    compact(&s, &mut beef, None).await;
    assert!(attached(&beef));
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());

    // Confirmed: attached, and from then on a checked row (a refutation
    // later does not demote it).
    let s = seeded(&raw).await;
    let mut beef = stored_beef();
    compact(&s, &mut beef, Some(&tracker_with(&[(500, COINBASE_TXID)]))).await;
    assert!(attached(&beef));
    let mut beef = stored_beef();
    compact(&s, &mut beef, Some(&tracker_with(&[]))).await;
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());
}

/// A deployed wallet opened by `make_available()` alone (the way every CLI
/// command opens one) gets migration 005 before its first proof store, and
/// every proof row it held before reads as unchecked.
#[tokio::test]
async fn migration_005_applies_on_open_of_an_existing_004_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wallet.db");
    let path = path.to_str().unwrap().to_string();
    let raw = hex::decode(COINBASE_HEX).unwrap();
    {
        let s = StorageSqlx::open(&path).await.unwrap();
        s.migrate("old-wallet", &"1".repeat(64)).await.unwrap();
        sqlx::query("DROP TABLE proof_root_checks")
            .execute(s.pool())
            .await
            .unwrap();
        // A proof the old code stored unchecked.
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, 500, 0, ?, ?, ?, ?, ?, ?)",
        )
        .bind(COINBASE_TXID)
        .bind("o".repeat(64))
        .bind(COINBASE_TXID)
        .bind(single_leaf_bump(500, COINBASE_TXID))
        .bind(&raw)
        .bind(Utc::now())
        .bind(Utc::now())
        .execute(s.pool())
        .await
        .unwrap();
        s.pool().close().await;
    }
    let s = StorageSqlx::open(&path).await.unwrap();
    s.make_available().await.unwrap();
    let table: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'proof_root_checks'",
    )
    .fetch_optional(s.pool())
    .await
    .unwrap();
    assert!(table.is_some(), "005 lands on open");

    // The old row is unchecked: the first read with a refuting tracker
    // demotes it.
    walk(&s, COINBASE_TXID, Some(&tracker_with(&[]))).await;
    assert!(proven_row(&s, COINBASE_TXID).await.is_none());

    // And the funnel writes into the new table without error.
    open_gate(&s, 600).await;
    ingest_checked(&s, COINBASE_TXID, &raw, 600).await;
    walk(&s, COINBASE_TXID, Some(&tracker_with(&[]))).await;
    assert!(
        proven_row(&s, COINBASE_TXID).await.is_some(),
        "a funnel-written row is checked"
    );
}
