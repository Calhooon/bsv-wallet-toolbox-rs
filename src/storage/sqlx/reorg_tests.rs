//! M19 R1 storage pins (2026-09-08): the proof lag gate, replace-on-differ,
//! demotion, the anchor finders, and the BEEF walk's raw-tx leg on a
//! refuted stored bump. Every test here was RED on 0.3.64 by construction:
//! the gate and the finders did not exist, `INSERT OR IGNORE` kept the
//! orphan's proof, and the walk attached a stored bump unvalidated.

use std::sync::Arc;

use bsv_rs::primitives::{sha256d, to_hex};
use bsv_rs::transaction::{Beef, MerklePath, MerklePathLeaf, MockChainTracker};
use chrono::Utc;

use super::create_action::beef_bfs_walk;
use super::storage_sqlx::same_proof_anchor;
use super::StorageSqlx;
use crate::storage::traits::*;
use crate::storage::{MonitorStorage, ProofIngestOutcome};

/// The block-1 coinbase: a real transaction (the walk parses it).
const COINBASE_HEX: &str = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000";
const COINBASE_TXID: &str = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

async fn storage() -> StorageSqlx {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test-wallet", &"0".repeat(64)).await.unwrap();
    s.make_available().await.unwrap();
    s
}

/// A single-leaf BUMP for `txid` at `height`: the root is the txid itself.
fn single_leaf_bump(height: u32, txid: &str) -> Vec<u8> {
    MerklePath {
        block_height: height,
        path: vec![vec![MerklePathLeaf {
            offset: 0,
            hash: Some(txid.to_string()),
            txid: true,
            duplicate: false,
        }]],
    }
    .to_binary()
}

fn txid_of(raw: &[u8]) -> String {
    let mut h = sha256d(raw);
    h.reverse();
    to_hex(&h)
}

/// A minimal parseable transaction spending `parent_txid`:0.
fn child_spending(parent_txid: &str) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&1u32.to_le_bytes()); // version
    raw.push(1); // vin count
    let mut prev = hex::decode(parent_txid).unwrap();
    prev.reverse();
    raw.extend_from_slice(&prev);
    raw.extend_from_slice(&0u32.to_le_bytes()); // vout
    raw.push(0); // script len
    raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // sequence
    raw.push(1); // vout count
    raw.extend_from_slice(&1000u64.to_le_bytes()); // value
    raw.push(0); // script len
    raw.extend_from_slice(&0u32.to_le_bytes()); // locktime
    raw
}

async fn insert_proven(s: &StorageSqlx, txid: &str, height: u32, hash: &str, raw: &[u8], bump: &[u8]) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
    )
    .bind(txid)
    .bind(height as i64)
    .bind(hash)
    .bind(txid) // single-leaf root == txid
    .bind(bump)
    .bind(raw)
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap();
}

/// The proof ingest needs the transaction's raw bytes somewhere in storage
/// (`proven_tx_reqs` here, as the fleet's requests hold them).
async fn insert_req(s: &StorageSqlx, txid: &str, raw: &[u8]) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) VALUES (?, 'unmined', 0, '{}', 0, '{}', ?, ?, ?)",
    )
    .bind(txid)
    .bind(raw)
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap();
}

async fn proven_row(s: &StorageSqlx, txid: &str) -> Option<(i64, String, String)> {
    sqlx::query_as("SELECT height, block_hash, merkle_root FROM proven_txs WHERE txid = ?")
        .bind(txid)
        .fetch_optional(s.pool())
        .await
        .unwrap()
}

#[test]
fn same_anchor_compares_hashes_when_known_and_roots_otherwise() {
    assert!(same_proof_anchor("aa", 10, "r1", "AA", 10, "r2"));
    assert!(!same_proof_anchor("aa", 10, "r1", "bb", 10, "r1"));
    assert!(!same_proof_anchor("aa", 10, "r1", "aa", 11, "r1"));
    // A pre-0.3.65 internalize row has no hash: the root decides.
    assert!(same_proof_anchor("", 10, "r1", "aa", 10, "R1"));
    assert!(!same_proof_anchor("", 10, "r1", "aa", 10, "r2"));
}

#[tokio::test]
async fn the_proof_lag_gate_defers_a_proof_above_the_processed_height() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(101, txid.to_string());
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
    insert_req(&s, txid, &hex::decode(COINBASE_HEX).unwrap()).await;
    s.set_max_acceptable_proof_height(100);
    assert_eq!(s.max_acceptable_proof_height(), 100);

    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(101, txid), 101, &"b".repeat(64), None)
        .await
        .unwrap();
    assert!(
        matches!(
            out,
            ProofIngestOutcome::DeferredAboveProcessedHeight { block_height: 101, processed_height: 100 }
        ),
        "expected a deferral, got {out:?}"
    );
    assert!(proven_row(&s, txid).await.is_none(), "a deferred proof is never stored");

    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(100, txid), 100, &"a".repeat(64), None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let row = proven_row(&s, txid).await.unwrap();
    assert_eq!((row.0, row.1.as_str()), (100, "a".repeat(64).as_str()));
}

#[tokio::test]
async fn a_validated_proof_for_a_different_block_replaces_the_stored_anchor() {
    // The 2026-09-07 shape: stored against the orphan at 965771, then the
    // canonical 965773 re-anchor arrives (Arcade's `reorg_reanchor`).
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1_000_000);
    tracker.add_root(965771, txid.to_string());
    tracker.add_root(965773, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
    insert_req(&s, txid, &hex::decode(COINBASE_HEX).unwrap()).await;
    let orphan = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    let canonical = "0000000000000000146bc084ec137a3c9608a07159128c66302051b6fe176e33";

    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(965771, txid), 965771, orphan, None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(965773, txid), 965773, canonical, None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let row = proven_row(&s, txid).await.unwrap();
    assert_eq!(row.0, 965773, "the re-anchor replaced the stored height");
    assert_eq!(row.1, canonical, "the re-anchor replaced the stored block hash");
    // The same anchor again is a no-op.
    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(965773, txid), 965773, canonical, None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    assert_eq!(proven_row(&s, txid).await.unwrap().0, 965773);
    // The finders see the canonical anchor and not the orphan.
    assert!(s.find_proven_txs_by_block_hash(orphan).await.unwrap().is_empty());
    let by_hash = s.find_proven_txs_by_block_hash(canonical).await.unwrap();
    assert_eq!(by_hash.len(), 1);
    assert_eq!(by_hash[0].txid, txid);
    let in_heights = s.find_proven_txs_in_heights(965770, 965773).await.unwrap();
    assert_eq!(in_heights.len(), 1);
    assert_eq!(in_heights[0].height, 965773);
    assert!(s.find_proven_txs_in_heights(0, 965772).await.unwrap().is_empty());
}

#[tokio::test]
async fn demoting_a_stale_proof_returns_the_transaction_to_unmined_and_resets_its_request() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    insert_proven(&s, txid, 965771, &"o".repeat(64), &raw, &single_leaf_bump(965771, txid)).await;
    let (proven_tx_id,): (i64,) = sqlx::query_as("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap();
    let (user, _) = s.find_or_insert_user(&"a".repeat(66)).await.unwrap();
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', 'ref-demote', 1, 1000, 'stake', ?, 1, 0, ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(txid)
    .bind(proven_tx_id)
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', 7, '{}', 0, '{}', X'00', ?, ?, ?)",
    )
    .bind(txid)
    .bind(proven_tx_id)
    .bind(now)
    .bind(now)
    .execute(s.pool())
    .await
    .unwrap();

    assert!(s.demote_stale_proof(txid).await.unwrap());
    assert!(proven_row(&s, txid).await.is_none(), "the stale proof is gone");
    let (status, linked): (String, Option<i64>) =
        sqlx::query_as("SELECT status, proven_tx_id FROM transactions WHERE txid = ?")
            .bind(txid)
            .fetch_one(s.pool())
            .await
            .unwrap();
    assert_eq!((status.as_str(), linked), ("unmined", None));
    let (rstatus, attempts, rlinked): (String, i64, Option<i64>) =
        sqlx::query_as("SELECT status, attempts, proven_tx_id FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_one(s.pool())
            .await
            .unwrap();
    assert_eq!((rstatus.as_str(), attempts, rlinked), ("unmined", 0, None));
    // A second demotion has nothing to do.
    assert!(!s.demote_stale_proof(txid).await.unwrap());
}

#[tokio::test]
async fn the_beef_walk_rides_a_refuted_stored_bump_as_a_raw_leg_and_demotes_it() {
    // Parent P: the coinbase, proven at height 1 with a root the tracker
    // accepts. Child C spends P:0 and is "proven" at height 500 with a bump
    // the tracker refutes (no root known there): the orphan class.
    let s = storage().await;
    let parent_raw = hex::decode(COINBASE_HEX).unwrap();
    let child_raw = child_spending(COINBASE_TXID);
    let child_txid = txid_of(&child_raw);
    insert_proven(&s, COINBASE_TXID, 1, &"p".repeat(64), &parent_raw, &single_leaf_bump(1, COINBASE_TXID)).await;
    insert_proven(&s, &child_txid, 500, &"o".repeat(64), &child_raw, &single_leaf_bump(500, &child_txid)).await;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(1, COINBASE_TXID.to_string());
    // No root for 500: the stored bump for C is refuted.

    let mut conn = s.pool().acquire().await.unwrap();
    let mut beef = Beef::new();
    let mut pending = vec![(child_txid.clone(), 0usize)];
    let mut processed = std::collections::HashSet::new();
    beef_bfs_walk(&mut conn, &mut beef, &mut pending, &mut processed, Some(&s), Some(&tracker))
        .await
        .unwrap();
    drop(conn);

    let child = beef.find_txid(&child_txid).expect("the child rides in the BEEF");
    assert!(child.bump_index().is_none(), "the refuted bump is not attached");
    let parent = beef.find_txid(COINBASE_TXID).expect("the parent was walked one level deeper");
    assert!(parent.bump_index().is_some(), "the parent's valid proof terminates the walk");
    assert!(proven_row(&s, &child_txid).await.is_none(), "the stale proof was demoted on the same connection");
    assert!(proven_row(&s, COINBASE_TXID).await.is_some(), "the valid proof stays");
}

#[tokio::test]
async fn a_proof_for_a_transaction_with_no_raw_bytes_is_not_reported_as_ingested() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(100, txid), 100, &"a".repeat(64), None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::InvalidProof(_)), "got {out:?}");
    assert!(proven_row(&s, txid).await.is_none());
}
