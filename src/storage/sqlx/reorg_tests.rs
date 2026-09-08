//! Reorg-resilience storage pins (0.3.66): the persisted proof LAG gate, the
//! ONE proof-store funnel (replace-on-differ, the no-raw guard, the
//! empty-hash backfill), the demotion that reverts to the pre-proof state in
//! one transaction with the coins still visible, and the BEEF walk that
//! skips a refuted stored bump without touching storage.

use std::sync::Arc;

use bsv_rs::primitives::{sha256d, to_hex};
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf, MockChainTracker,
};
use bsv_rs::wallet::{ListActionsArgs, ListOutputsArgs};
use chrono::{Duration, Utc};

use super::create_action::{allocate_change_input, beef_bfs_walk, find_or_insert_output_basket};
use super::storage_sqlx::same_proof_anchor;
use super::StorageSqlx;
use crate::services::broadcast_memory::{BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_MINED};
use crate::services::mock::{MockResponse, MockWalletServices};
use crate::services::{BlockHeader, GetMerklePathResult, WalletServices};
use crate::storage::traits::*;
use crate::storage::{MonitorStorage, ProofIngestOutcome};
use crate::AuthId;

/// The block-1 coinbase: a real transaction (the walk parses it).
const COINBASE_HEX: &str = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000";
const COINBASE_TXID: &str = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

async fn storage() -> StorageSqlx {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test-wallet", &"0".repeat(64)).await.unwrap();
    s.make_available().await.unwrap();
    s
}

async fn open_gate(s: &StorageSqlx, height: u32) {
    s.set_max_acceptable_proof_height(height).await.unwrap();
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

async fn insert_proven(
    s: &StorageSqlx,
    txid: &str,
    height: u32,
    hash: &str,
    raw: &[u8],
    bump: &[u8],
) -> i64 {
    let now = Utc::now();
    sqlx::query_scalar(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?) RETURNING proven_tx_id",
    )
    .bind(txid)
    .bind(height as i64)
    .bind(hash)
    .bind(txid) // single-leaf root == txid
    .bind(bump)
    .bind(raw)
    .bind(now)
    .bind(now)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

/// A request holding the transaction's raw bytes (the fleet's requests do).
async fn insert_req(
    s: &StorageSqlx,
    txid: &str,
    raw: &[u8],
    status: &str,
    attempts: i64,
    proven_tx_id: Option<i64>,
    created_at: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, proven_tx_id, created_at, updated_at) VALUES (?, ?, ?, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(txid)
    .bind(status)
    .bind(attempts)
    .bind(raw)
    .bind(proven_tx_id)
    .bind(created_at)
    .bind(created_at)
    .execute(s.pool())
    .await
    .unwrap();
}

async fn seed_user(s: &StorageSqlx) -> (i64, AuthId) {
    let identity_key = "a".repeat(66);
    let (user, _) = s.find_or_insert_user(&identity_key).await.unwrap();
    (
        user.user_id,
        AuthId::with_user_id(&identity_key, user.user_id),
    )
}

/// A 'completed' transaction row linked to `proven_tx_id`, with or without
/// its raw bytes (the 30-day purge clears them).
async fn seed_completed_tx(
    s: &StorageSqlx,
    user_id: i64,
    txid: &str,
    proven_tx_id: Option<i64>,
    raw: Option<&[u8]>,
) -> i64 {
    let now = Utc::now();
    sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, proven_tx_id, raw_tx, created_at, updated_at) VALUES (?, 'completed', ?, 1, 1000, 'stake', ?, 1, 0, ?, ?, ?, ?) RETURNING transaction_id",
    )
    .bind(user_id)
    .bind(format!("ref-{}", &txid[..8]))
    .bind(txid)
    .bind(proven_tx_id)
    .bind(raw)
    .bind(now)
    .bind(now)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

/// A spendable change output of `transaction_id` in `basket_id`.
async fn seed_change_output(
    s: &StorageSqlx,
    user_id: i64,
    transaction_id: i64,
    basket_id: i64,
    txid: &str,
    satoshis: i64,
) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, ?, X'76a914000000000000000000000000000000000000000088ac', ?, 'P2PKH', 1, 1, 'prefix', 'suffix', 'storage', 'change', 'seeded change', ?, ?)",
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(basket_id)
    .bind(satoshis)
    .bind(txid)
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

async fn tx_state(s: &StorageSqlx, txid: &str) -> (String, Option<i64>, Option<Vec<u8>>) {
    sqlx::query_as("SELECT status, proven_tx_id, raw_tx FROM transactions WHERE txid = ?")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap()
}

async fn req_state(s: &StorageSqlx, txid: &str) -> Option<(String, i64, Option<i64>, Vec<u8>)> {
    sqlx::query_as(
        "SELECT status, attempts, proven_tx_id, raw_tx FROM proven_tx_reqs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_optional(s.pool())
    .await
    .unwrap()
}

async fn mined_rows(s: &StorageSqlx, txid: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM broadcast_seen WHERE txid = ? AND status = 'mined'")
        .bind(txid)
        .fetch_one(s.pool())
        .await
        .unwrap()
}

fn list_outputs_args() -> ListOutputsArgs {
    ListOutputsArgs {
        basket: "default".to_string(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: Some(100),
        offset: None,
        seek_permission: None,
    }
}

fn list_actions_args() -> ListActionsArgs {
    ListActionsArgs {
        labels: vec![],
        label_query_mode: None,
        include_labels: None,
        include_inputs: None,
        include_input_source_locking_scripts: None,
        include_input_unlocking_scripts: None,
        include_outputs: None,
        include_output_locking_scripts: None,
        limit: Some(100),
        offset: None,
        seek_permission: None,
    }
}

/// A tracker in outage: every question is an error, never a verdict.
struct ErrTracker;

#[async_trait::async_trait]
impl ChainTracker for ErrTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Err(ChainTrackerError::NetworkError(
            "both chaintracks and WoC failed".to_string(),
        ))
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(1000)
    }
}

#[test]
fn same_anchor_compares_hashes_when_known_and_roots_otherwise() {
    assert!(same_proof_anchor("aa", 10, "r1", "AA", 10, "r2"));
    assert!(!same_proof_anchor("aa", 10, "r1", "bb", 10, "r1"));
    assert!(!same_proof_anchor("aa", 10, "r1", "aa", 11, "r1"));
    // A row stored without a header has no hash: the root decides.
    assert!(same_proof_anchor("", 10, "r1", "aa", 10, "R1"));
    assert!(!same_proof_anchor("", 10, "r1", "aa", 10, "r2"));
}

// =============================================================================
// The persisted proof LAG gate (F3, F13)
// =============================================================================

#[tokio::test]
async fn the_proof_lag_gate_defers_a_proof_above_the_processed_height() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(101, txid.to_string());
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
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
    assert_eq!(s.max_acceptable_proof_height().await.unwrap(), 100);

    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(101, txid),
            101,
            &"b".repeat(64),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            out,
            ProofIngestOutcome::DeferredAboveProcessedHeight {
                block_height: 101,
                processed_height: 100
            }
        ),
        "expected a deferral, got {out:?}"
    );
    assert!(
        proven_row(&s, txid).await.is_none(),
        "a deferred proof is never stored"
    );
    assert_eq!(
        req_state(&s, txid).await.unwrap().1,
        0,
        "no attempt counted"
    );

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
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let row = proven_row(&s, txid).await.unwrap();
    assert_eq!((row.0, row.1.as_str()), (100, "a".repeat(64).as_str()));
    let (status, _, linked, _) = req_state(&s, txid).await.unwrap();
    assert_eq!(status, "completed");
    assert!(linked.is_some());
}

/// A fresh database has a CLOSED gate: every proof is deferred until the
/// header task has processed a header. Nothing is stored, no attempt is
/// counted, the reference's "maxAcceptableHeight undefined".
#[tokio::test]
async fn a_closed_gate_defers_every_proof() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(1, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
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
    assert_eq!(s.max_acceptable_proof_height().await.unwrap(), 0, "closed");
    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(1, txid), 1, &"a".repeat(64), None)
        .await
        .unwrap();
    assert!(
        matches!(
            out,
            ProofIngestOutcome::DeferredAboveProcessedHeight {
                block_height: 1,
                processed_height: 0
            }
        ),
        "got {out:?}"
    );
    assert!(proven_row(&s, txid).await.is_none());
    let (status, attempts, _, _) = req_state(&s, txid).await.unwrap();
    assert_eq!((status.as_str(), attempts), ("unmined", 0));
}

// =============================================================================
// The ONE proof-store funnel (F8, F10)
// =============================================================================

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
    open_gate(&s, 1_000_000).await;
    let orphan = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    let canonical = "0000000000000000146bc084ec137a3c9608a07159128c66302051b6fe176e33";

    let out = s
        .ingest_merkle_proof(txid, &single_leaf_bump(965771, txid), 965771, orphan, None)
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(965773, txid),
            965773,
            canonical,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let row = proven_row(&s, txid).await.unwrap();
    assert_eq!(row.0, 965773, "the re-anchor replaced the stored height");
    assert_eq!(
        row.1, canonical,
        "the re-anchor replaced the stored block hash"
    );
    // The same anchor again is a no-op.
    let out = s
        .ingest_merkle_proof(
            txid,
            &single_leaf_bump(965773, txid),
            965773,
            canonical,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    assert_eq!(proven_row(&s, txid).await.unwrap().0, 965773);
    // The finders see the canonical anchor and not the orphan.
    assert!(s
        .find_proven_txs_by_block_hash(orphan)
        .await
        .unwrap()
        .is_empty());
    let by_hash = s.find_proven_txs_by_block_hash(canonical).await.unwrap();
    assert_eq!(by_hash.len(), 1);
    assert_eq!(by_hash[0].txid, txid);
    let in_heights = s.find_proven_txs_in_heights(965770, 965773).await.unwrap();
    assert_eq!(in_heights.len(), 1);
    assert_eq!(in_heights[0].height, 965773);
    assert!(s
        .find_proven_txs_in_heights(0, 965772)
        .await
        .unwrap()
        .is_empty());
}

/// F10: a stored row without a block hash (written with no header in hand)
/// is filled by the next validated proof for the same block, and the
/// storage's own backfill op fills it from the canonical header.
#[tokio::test]
async fn a_proof_with_a_known_hash_fills_an_empty_stored_hash() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
    insert_proven(&s, txid, 100, "", &raw, &single_leaf_bump(100, txid)).await;
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
    assert!(matches!(out, ProofIngestOutcome::Ingested(_)));
    let row = proven_row(&s, txid).await.unwrap();
    assert_eq!((row.0, row.1.as_str()), (100, "a".repeat(64).as_str()));
    // A known hash is never overwritten by the backfill op.
    assert!(!s
        .set_proven_tx_block_hash_if_empty(txid, &"b".repeat(64))
        .await
        .unwrap());
    // And an empty one is filled by it.
    sqlx::query("UPDATE proven_txs SET block_hash = '' WHERE txid = ?")
        .bind(txid)
        .execute(s.pool())
        .await
        .unwrap();
    assert!(s
        .set_proven_tx_block_hash_if_empty(txid, &"c".repeat(64))
        .await
        .unwrap());
    assert_eq!(proven_row(&s, txid).await.unwrap().1, "c".repeat(64));
}

#[tokio::test]
async fn a_proof_for_a_transaction_with_no_raw_bytes_is_not_reported_as_ingested() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
    open_gate(&s, 1000).await;
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
        matches!(out, ProofIngestOutcome::InvalidProof(_)),
        "got {out:?}"
    );
    assert!(proven_row(&s, txid).await.is_none());
}

// =============================================================================
// The demotion: revert to the pre-proof state (F1, F5, F11, F12)
// =============================================================================

#[tokio::test]
async fn demoting_a_stale_proof_reverts_to_the_pre_proof_state_and_keeps_the_coins_visible() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let (user_id, auth) = seed_user(&s).await;
    let proven_tx_id = insert_proven(
        &s,
        txid,
        965771,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(965771, txid),
    )
    .await;
    let transaction_id = seed_completed_tx(&s, user_id, txid, Some(proven_tx_id), Some(&raw)).await;
    insert_req(
        &s,
        txid,
        &raw,
        "completed",
        7,
        Some(proven_tx_id),
        Utc::now(),
    )
    .await;
    let basket_id = {
        let mut conn = s.pool().acquire().await.unwrap();
        find_or_insert_output_basket(&s, &mut conn, user_id, "default")
            .await
            .unwrap()
            .basket_id
    };
    seed_change_output(&s, user_id, transaction_id, basket_id, txid, 1000).await;
    s.record_broadcast_status(txid, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_MINED)
        .await
        .unwrap();
    assert_eq!(mined_rows(&s, txid).await, 1);

    assert!(s.demote_stale_proof(txid).await.unwrap());

    // The pre-proof state: the proof row is gone, the transaction is
    // unproven and unlinked with its bytes, the request is unmined with
    // attempts 0 and its bytes, the mined memory is forgotten.
    assert!(
        proven_row(&s, txid).await.is_none(),
        "the stale proof is gone"
    );
    let (status, linked, tx_raw) = tx_state(&s, txid).await;
    assert_eq!((status.as_str(), linked), ("unproven", None));
    assert_eq!(tx_raw.as_deref(), Some(raw.as_slice()));
    let (rstatus, attempts, rlinked, rraw) = req_state(&s, txid).await.unwrap();
    assert_eq!((rstatus.as_str(), attempts, rlinked), ("unmined", 0, None));
    assert_eq!(rraw, raw);
    assert_eq!(
        mined_rows(&s, txid).await,
        0,
        "the one sanctioned downgrade"
    );

    // The coins are still visible everywhere a coin is read.
    let outputs = s.list_outputs(&auth, list_outputs_args()).await.unwrap();
    assert_eq!(
        outputs.total_outputs, 1,
        "list_outputs still returns the output"
    );
    let actions = s.list_actions(&auth, list_actions_args()).await.unwrap();
    assert_eq!(actions.total_actions, 1, "list_actions still lists it");
    assert_eq!(actions.actions.len(), 1);
    // A spender in the making, the row coin selection allocates for.
    let spender_id: i64 = sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, version, lock_time, created_at, updated_at) VALUES (?, 'unsigned', 'spender', 1, 0, 'spend', 1, 0, ?, ?) RETURNING transaction_id",
    )
    .bind(user_id)
    .bind(Utc::now())
    .bind(Utc::now())
    .fetch_one(s.pool())
    .await
    .unwrap();
    {
        let mut conn = s.pool().acquire().await.unwrap();
        let picked = allocate_change_input(&mut conn, user_id, basket_id, spender_id, 500, true)
            .await
            .unwrap();
        assert!(picked.is_some(), "coin selection still finds the output");
        sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL WHERE txid = ?")
            .bind(txid)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    // A second demotion has nothing to do.
    assert!(!s.demote_stale_proof(txid).await.unwrap());

    // The negative control, the F1 defect: a status the wallet does not
    // know ('unmined' on a transaction) makes the same coins vanish. The
    // spender row goes first so the counts below are the demoted
    // transaction's alone.
    sqlx::query("DELETE FROM transactions WHERE transaction_id = ?")
        .bind(spender_id)
        .execute(s.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET status = 'unmined' WHERE txid = ?")
        .bind(txid)
        .execute(s.pool())
        .await
        .unwrap();
    let outputs = s.list_outputs(&auth, list_outputs_args()).await.unwrap();
    assert_eq!(outputs.total_outputs, 0);
    let actions = s.list_actions(&auth, list_actions_args()).await.unwrap();
    assert_eq!(actions.total_actions, 0);
    let mut conn = s.pool().acquire().await.unwrap();
    let picked = allocate_change_input(&mut conn, user_id, basket_id, spender_id, 500, true)
        .await
        .unwrap();
    assert!(picked.is_none());
}

/// F5: after the 30-day purge (`transactions.raw_tx` NULL, the completed
/// request deleted) the proof row was the last holder of the bytes; the
/// demotion re-creates the request from it and restores the transaction's
/// bytes. Nothing is lost.
#[tokio::test]
async fn demotion_recreates_the_purged_request_with_the_proof_bytes() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let (user_id, _) = seed_user(&s).await;
    let proven_tx_id = insert_proven(
        &s,
        txid,
        965771,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(965771, txid),
    )
    .await;
    seed_completed_tx(&s, user_id, txid, Some(proven_tx_id), None).await;
    assert!(
        req_state(&s, txid).await.is_none(),
        "the purge deleted the request"
    );

    assert!(s.demote_stale_proof(txid).await.unwrap());

    let (status, linked, tx_raw) = tx_state(&s, txid).await;
    assert_eq!((status.as_str(), linked), ("unproven", None));
    assert_eq!(
        tx_raw.as_deref(),
        Some(raw.as_slice()),
        "the bytes came back from the proof row"
    );
    let (rstatus, attempts, rlinked, rraw) = req_state(&s, txid)
        .await
        .expect("the request was re-created");
    assert_eq!((rstatus.as_str(), attempts, rlinked), ("unmined", 0, None));
    assert_eq!(rraw, raw, "the request holds the bytes");
    assert!(proven_row(&s, txid).await.is_none());
}

/// F12: the demotion is ONE transaction. A failure on the last step (the
/// proof row delete, forced by a trigger) leaves every earlier step
/// unapplied.
#[tokio::test]
async fn demotion_is_one_transaction() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    let (user_id, _) = seed_user(&s).await;
    let proven_tx_id = insert_proven(
        &s,
        txid,
        965771,
        &"o".repeat(64),
        &raw,
        &single_leaf_bump(965771, txid),
    )
    .await;
    seed_completed_tx(&s, user_id, txid, Some(proven_tx_id), None).await;
    insert_req(
        &s,
        txid,
        &raw,
        "completed",
        7,
        Some(proven_tx_id),
        Utc::now(),
    )
    .await;
    s.record_broadcast_status(txid, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_MINED)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER forbid_proof_delete BEFORE DELETE ON proven_txs BEGIN SELECT RAISE(ABORT, 'forced'); END",
    )
    .execute(s.pool())
    .await
    .unwrap();

    assert!(
        s.demote_stale_proof(txid).await.is_err(),
        "the forced failure surfaces"
    );

    assert!(
        proven_row(&s, txid).await.is_some(),
        "step 4 did not happen"
    );
    let (status, linked, _) = tx_state(&s, txid).await;
    assert_eq!(
        (status.as_str(), linked),
        ("completed", Some(proven_tx_id)),
        "step 2 rolled back"
    );
    let (rstatus, attempts, rlinked, _) = req_state(&s, txid).await.unwrap();
    assert_eq!(
        (rstatus.as_str(), attempts, rlinked),
        ("completed", 7, Some(proven_tx_id)),
        "step 1 rolled back"
    );
    assert_eq!(mined_rows(&s, txid).await, 1, "step 3 rolled back");

    sqlx::query("DROP TRIGGER forbid_proof_delete")
        .execute(s.pool())
        .await
        .unwrap();
    assert!(s.demote_stale_proof(txid).await.unwrap());
    assert!(proven_row(&s, txid).await.is_none());
}

// =============================================================================
// The BEEF walk never mutates storage (F2)
// =============================================================================

/// The reference's `skipInvalidProofs` shape: a stored bump the tracker
/// DEFINITELY refutes is not attached, the raw-tx leg is walked one level
/// deeper, and storage is untouched: the stale row is demoted only by the
/// reorg or review task on positive network evidence.
#[tokio::test]
async fn the_beef_walk_skips_a_refuted_stored_bump_and_demotes_nothing() {
    // Parent P: the coinbase, proven at height 1 with a root the tracker
    // accepts. Child C spends P:0 and is "proven" at height 500 with a bump
    // the tracker refutes (no root known there): the orphan class.
    let s = storage().await;
    let parent_raw = hex::decode(COINBASE_HEX).unwrap();
    let child_raw = child_spending(COINBASE_TXID);
    let child_txid = txid_of(&child_raw);
    insert_proven(
        &s,
        COINBASE_TXID,
        1,
        &"p".repeat(64),
        &parent_raw,
        &single_leaf_bump(1, COINBASE_TXID),
    )
    .await;
    insert_proven(
        &s,
        &child_txid,
        500,
        &"o".repeat(64),
        &child_raw,
        &single_leaf_bump(500, &child_txid),
    )
    .await;
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(1, COINBASE_TXID.to_string());
    // No root for 500: the stored bump for C is refuted.

    let mut conn = s.pool().acquire().await.unwrap();
    let mut beef = Beef::new();
    let mut pending = vec![(child_txid.clone(), 0usize)];
    let mut processed = std::collections::HashSet::new();
    beef_bfs_walk(
        &mut conn,
        &mut beef,
        &mut pending,
        &mut processed,
        Some(&s),
        Some(&tracker),
    )
    .await
    .unwrap();
    drop(conn);

    let child = beef
        .find_txid(&child_txid)
        .expect("the child rides in the BEEF");
    assert!(
        child.bump_index().is_none(),
        "the refuted bump is not attached"
    );
    let parent = beef
        .find_txid(COINBASE_TXID)
        .expect("the parent was walked one level deeper");
    assert!(
        parent.bump_index().is_some(),
        "the parent's valid proof terminates the walk"
    );
    assert!(
        proven_row(&s, &child_txid).await.is_some(),
        "the walk demoted nothing"
    );
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());
    let (status, _, _, _) = req_state(&s, &child_txid)
        .await
        .map(|r| (r.0, r.1, r.2, r.3))
        .unwrap_or(("none".into(), 0, None, vec![]));
    assert_eq!(status, "none", "no request was written either");
}

/// F2: a tracker OUTAGE (both header sources failing answers `Err` since
/// 0.3.66) keeps the stored bump attached and lets the final BEEF
/// verification decide, as before the reorg work; nothing is mutated.
#[tokio::test]
async fn a_tracker_outage_keeps_the_stored_bump_and_mutates_nothing() {
    let s = storage().await;
    let parent_raw = hex::decode(COINBASE_HEX).unwrap();
    let child_raw = child_spending(COINBASE_TXID);
    let child_txid = txid_of(&child_raw);
    insert_proven(
        &s,
        COINBASE_TXID,
        1,
        &"p".repeat(64),
        &parent_raw,
        &single_leaf_bump(1, COINBASE_TXID),
    )
    .await;
    insert_proven(
        &s,
        &child_txid,
        500,
        &"o".repeat(64),
        &child_raw,
        &single_leaf_bump(500, &child_txid),
    )
    .await;

    let mut conn = s.pool().acquire().await.unwrap();
    let mut beef = Beef::new();
    let mut pending = vec![(child_txid.clone(), 0usize)];
    let mut processed = std::collections::HashSet::new();
    beef_bfs_walk(
        &mut conn,
        &mut beef,
        &mut pending,
        &mut processed,
        Some(&s),
        Some(&ErrTracker),
    )
    .await
    .unwrap();
    drop(conn);

    let child = beef
        .find_txid(&child_txid)
        .expect("the child rides in the BEEF");
    assert!(
        child.bump_index().is_some(),
        "an outage is not a refutation: the bump stays"
    );
    assert!(
        beef.find_txid(COINBASE_TXID).is_none(),
        "the proven child terminates the walk"
    );
    assert!(proven_row(&s, &child_txid).await.is_some());
    assert!(proven_row(&s, COINBASE_TXID).await.is_some());
}

/// F8/F14: the walk's own proof fetch is routed through the funnel. With
/// the gate closed the fetched proof is not stored and the walk continues
/// raw; with the gate open it is stored and terminates the walk.
#[tokio::test]
async fn the_walks_own_proof_fetch_goes_through_the_funnel_and_the_gate() {
    let s = storage().await;
    let txid = COINBASE_TXID;
    let raw = hex::decode(COINBASE_HEX).unwrap();
    // The subject is held as a request without a proof, old enough for the
    // walk to ask the providers.
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
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(100, txid.to_string());
    s.set_chain_tracker(Arc::new(tracker)).await;
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
    let walk_tracker = {
        let mut t = MockChainTracker::new(1000);
        t.add_root(100, txid.to_string());
        t
    };

    // Gate closed: nothing stored, the subject rides raw.
    {
        let mut conn = s.pool().acquire().await.unwrap();
        let mut beef = Beef::new();
        let mut pending = vec![(txid.to_string(), 0usize)];
        let mut processed = std::collections::HashSet::new();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            Some(&s),
            Some(&walk_tracker),
        )
        .await
        .unwrap();
        let subject = beef.find_txid(txid).expect("the subject rides in the BEEF");
        assert!(
            subject.bump_index().is_none(),
            "a deferred proof is not attached"
        );
    }
    assert!(
        proven_row(&s, txid).await.is_none(),
        "a deferred proof is never stored"
    );
    let (status, _, _, _) = req_state(&s, txid).await.unwrap();
    assert_eq!(status, "unmined");

    // Gate open: stored through the funnel, the walk terminates on the bump.
    open_gate(&s, 100).await;
    {
        let mut conn = s.pool().acquire().await.unwrap();
        let mut beef = Beef::new();
        let mut pending = vec![(txid.to_string(), 0usize)];
        let mut processed = std::collections::HashSet::new();
        beef_bfs_walk(
            &mut conn,
            &mut beef,
            &mut pending,
            &mut processed,
            Some(&s),
            Some(&walk_tracker),
        )
        .await
        .unwrap();
        let subject = beef.find_txid(txid).expect("the subject rides in the BEEF");
        assert!(
            subject.bump_index().is_some(),
            "the stored proof is attached"
        );
    }
    let row = proven_row(&s, txid)
        .await
        .expect("stored through the funnel");
    assert_eq!((row.0, row.1.as_str()), (100, "a".repeat(64).as_str()));
    let (status, _, linked, _) = req_state(&s, txid).await.unwrap();
    assert_eq!(status, "completed");
    assert!(linked.is_some());
}
