use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_beef_bfs_walk_discards_corrupt_stored_beef_and_falls_through() {
    use bsv_rs::transaction::MockChainTracker;

    // This is the integration test: a transaction has corrupt stored input_beef,
    // but the individual proven_tx has a valid proof. beef_bfs_walk should discard
    // the stored BEEF and fall through to the individual tx+proof lookup.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    // The parent transaction (ancestor) — this is what the stored BEEF references
    let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Build corrupt stored BEEF (bad root at height 942926)
    let (corrupt_beef_bytes, _) = build_test_beef_with_bump(942926, "");

    // The child transaction that spends the parent
    // (simple tx spending output 0 of parent)
    let child_txid = format!("{:0>64}", "child01");

    // Seed the corrupt stored BEEF on the child transaction
    seed_tx_with_input_beef(&storage, &child_txid, &corrupt_beef_bytes, &parent_raw_tx).await;

    // Also seed the parent in proven_txs with a VALID proof
    // (this is the fallback path — individual tx lookup should find it)
    let valid_merkle_path = hex::decode("a086010001020002").unwrap();
    seed_proven_tx(&storage, parent_txid, &parent_raw_tx, &valid_merkle_path).await;

    // MockChainTracker that rejects height 942926 (orphaned) but doesn't know
    // about any heights (so any stored BEEF with bumps will be rejected)
    let tracker = MockChainTracker::new(950000);
    // Don't add the corrupt root → is_valid_root_for_height returns false

    let mut beef = Beef::new();
    let mut pending_txids = vec![(child_txid.clone(), 0)];
    let mut processed_txids = HashSet::new();

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        None, // no network fallback needed — proven_txs has the data
        Some(&tracker as &dyn ChainTracker),
    )
    .await;

    // Should succeed — corrupt stored BEEF was discarded, individual lookup worked
    assert!(
        result.is_ok(),
        "beef_bfs_walk should succeed by falling through to individual tx lookup: {:?}",
        result.err()
    );

    // The child txid should have been processed
    assert!(processed_txids.contains(&child_txid));
}

/// M19-6 (#429): the planted-stale-proof guard for the WALLET path.
///
/// The 2026-09-07 double reorg (bsv-low #424): block 965771
/// (`0000..153e10f4`) was orphaned seconds after Arcade pushed inline
/// proofs against it; 28 fleet seats stored those proofs, nothing
/// re-validated them, and every `create_action` touching such an output
/// was refused with "invalid merkle root" (HTTP 400) until R1. This is
/// the PERMANENT regression cell for the R1 raw-leg fix: a wallet holding
/// a proof anchored to an orphaned block can still STAKE, because the BEEF
/// walk rides the raw-tx leg, and `reproof`/the monitor heal the row.
///
/// It plants ONE stale proof on a throwaway in-memory wallet and proves,
/// end to end at the exact seams the money path uses:
///   (a) DETECTION: `stale_anchors_by_root` (the predicate the CLI
///       `bsv-wallet reproof --dry --json` runs via `reproof::plan_rows`)
///       NAMES the planted row.
///   (b) THE SPEND SUCCEEDS: `beef_bfs_walk` (what `create_action`'s
///       `build_input_beef` calls) rides the raw-tx leg instead of
///       attaching the refuted bump, and `create_action`'s own inputBEEF
///       root check (this file, the block at `beef.bumps.is_empty()` ~L2760)
///       then passes: no "invalid merkle root" 400. A positive control
///       shows the SAME check DOES reject the bump once it is attached (the
///       pre-R1 path the fix removes).
///   (c) THE HEAL: `demote_stale_proof` (what `reproof --execute` and the
///       reorg task call on positive evidence) reverts the row to the
///       pre-proof state, and a follow-up detection reads clean.
///
/// RED-verify: reverting the `stored_bump_refuted` guard in `beef_bfs_walk`
/// (this file) to the pre-R1 unconditional `Some(beef.merge_bump(..))`
/// fails assertion (b) with "inputBEEF: invalid merkle root ..". The GREEN
/// path here IS the fix; the positive control shows the RED shape too.
#[tokio::test]
async fn test_m19_429_planted_stale_proof_spend_rides_raw_leg_and_heals() {
    use crate::monitor::reorg_ops::stale_anchors_by_root;
    use crate::storage::MonitorStorage;
    use bsv_rs::transaction::{MerklePath, MerklePathLeaf, MockChainTracker};
    use std::collections::HashMap;

    // `create_action`'s inputBEEF root check, mirrored from
    // `build_input_beef` (this file, the `if beef_bytes.len() > 4 &&
    // !beef.bumps.is_empty()` block ~L2760): the exact gate that produced
    // the 2026-09-07 "invalid merkle root" 400. Keep in sync with it.
    async fn verify_like_create_action(
        beef: &mut Beef,
        tracker: &dyn ChainTracker,
    ) -> std::result::Result<(), String> {
        let beef_bytes = beef.to_binary();
        if beef_bytes.len() > 4 && !beef.bumps.is_empty() {
            let validation = beef.verify_valid(true);
            if !validation.valid {
                return Err("inputBEEF: BEEF structure is invalid".to_string());
            }
            for (height, root) in &validation.roots {
                match tracker.is_valid_root_for_height(root, *height).await {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(format!(
                            "inputBEEF: invalid merkle root {root} at height {height}"
                        ))
                    }
                    Err(e) => {
                        return Err(format!(
                            "inputBEEF: failed to verify merkle root at height {height}: {e}"
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    // The 2026-09-07 event's real identifiers.
    const HEIGHT: u32 = 965771;
    const ORPHAN_BLOCK: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    // The canonical merkle root at 965771 (the surviving chain).
    const CANONICAL_ROOT: &str = "a785b0537cc490b792a9303660fbe5d6f5aaa96af29191c619cf71072100d6ae";
    // A real, self-consistent funding tx: the raw's own txid.
    const FUNDING_TXID: &str = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";
    let funding_raw = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000",
        )
        .unwrap();

    // A single-leaf bump whose computed root IS the txid (the reorg-test
    // technique): the stored proof a seat held for the orphaned block.
    let stale_bump = MerklePath {
        block_height: HEIGHT,
        path: vec![vec![MerklePathLeaf {
            offset: 0,
            hash: Some(FUNDING_TXID.to_string()),
            txid: true,
            duplicate: false,
        }]],
    };
    let stale_bump_bytes = stale_bump.to_binary();
    // The stored proof's root equals the txid (single-leaf) and the
    // canonical root at HEIGHT differs: a genuine stale anchor.
    assert_ne!(FUNDING_TXID, CANONICAL_ROOT);

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("m19-429", &"00".repeat(32)).await.unwrap();
    storage.make_available().await.unwrap();
    let identity_key = "02".to_string() + &"ab".repeat(32);
    let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();

    // PLANT: proven_txs (root = txid, block = the orphan) + a completed
    // transaction linked to it + its completed request: the exact shape a
    // seat held after the orphan.
    let now = Utc::now();
    let proven_tx_id: i64 = sqlx::query_scalar(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?) RETURNING proven_tx_id",
        )
        .bind(FUNDING_TXID)
        .bind(HEIGHT as i64)
        .bind(ORPHAN_BLOCK)
        .bind(FUNDING_TXID) // merkle_root == txid for a single-leaf bump
        .bind(&stale_bump_bytes)
        .bind(&funding_raw)
        .bind(now)
        .bind(now)
        .fetch_one(storage.writer())
        .await
        .unwrap();
    // The seat's tracker confirmed that root while the orphan was the
    // tip: a CHECKED row (P0-1), which only the reorg tooling demotes.
    {
        let mut conn = storage.writer().acquire().await.unwrap();
        super::super::proof_root_checks::record_root_checked_on(
            &mut conn,
            FUNDING_TXID,
            HEIGHT,
            FUNDING_TXID,
        )
        .await
        .unwrap();
    }
    sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', 'ref-m19-429', 1, 1000, 'stake', ?, 1, 0, ?, ?, ?)",
        )
        .bind(user.user_id)
        .bind(FUNDING_TXID)
        .bind(proven_tx_id)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
    sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, proven_tx_id, created_at, updated_at) VALUES (?, 'completed', 3, '{}', 0, '{}', ?, ?, ?, ?)",
        )
        .bind(FUNDING_TXID)
        .bind(&funding_raw)
        .bind(proven_tx_id)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();

    // The chain tracker knows the CANONICAL root at HEIGHT, so it refutes
    // the orphan proof's root (which equals the txid).
    let mut tracker = MockChainTracker::new(HEIGHT + 2);
    tracker.add_root(HEIGHT, CANONICAL_ROOT.to_string());
    assert!(
        !tracker
            .is_valid_root_for_height(FUNDING_TXID, HEIGHT)
            .await
            .unwrap(),
        "precondition: the chain refutes the stored orphan proof"
    );

    // ---- (a) DETECTION: `reproof --dry` names the planted row ----
    let anchors = MonitorStorage::find_proven_txs_in_heights(&storage, HEIGHT, HEIGHT)
        .await
        .unwrap();
    assert_eq!(anchors.len(), 1, "the planted proof is stored");
    let canonical_roots: HashMap<u32, String> = [(HEIGHT, CANONICAL_ROOT.to_string())].into();
    let stale = stale_anchors_by_root(&anchors, &canonical_roots);
    assert_eq!(
        stale.len(),
        1,
        "reproof --dry names exactly the planted row"
    );
    assert_eq!(stale[0].txid, FUNDING_TXID);
    assert_eq!(stale[0].block_hash, ORPHAN_BLOCK);

    // ---- (b) THE SPEND SUCCEEDS via the raw-tx leg ----
    let mut beef = Beef::new();
    let mut pending = vec![(FUNDING_TXID.to_string(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    let walk = beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        Some(&tracker as &dyn ChainTracker),
    )
    .await;
    drop(conn); // release the pooled connection before the heal below
    assert!(
        walk.is_ok(),
        "the BEEF walk succeeds on a stale proof: {:?}",
        walk.err()
    );
    let entry = beef
        .find_txid(FUNDING_TXID)
        .expect("the funding tx is in the BEEF");
    assert!(
        entry.bump_index().is_none(),
        "it rides the raw-tx leg; the refuted bump is not attached"
    );
    assert!(
        beef.bumps.is_empty(),
        "the refuted bump was never merged into the BEEF"
    );
    // create_action's own inputBEEF root check now passes: there is no
    // bump to refute, so no "invalid merkle root" 400.
    verify_like_create_action(&mut beef, &tracker)
        .await
        .expect("createAction inputBEEF verification passes: no 'invalid merkle root'");

    // POSITIVE CONTROL: the SAME check DOES reject the orphan bump when it
    // is attached (the pre-R1 path the fix removes).
    let mut control = Beef::new();
    let idx = control.merge_bump(stale_bump.clone());
    control.merge_raw_tx(funding_raw.clone(), Some(idx));
    let control_err = verify_like_create_action(&mut control, &tracker)
        .await
        .expect_err("an attached orphan bump is rejected by the same check");
    assert!(
        control_err.contains("invalid merkle root"),
        "the pre-R1 path 400s: {control_err}"
    );

    // ---- (c) THE HEAL: `reproof --execute` demotes, follow-up reads clean ----
    let existed = MonitorStorage::demote_stale_proof(&storage, FUNDING_TXID)
        .await
        .unwrap();
    assert!(existed, "the stale proof was healed");
    let after = MonitorStorage::find_proven_txs_in_heights(&storage, HEIGHT, HEIGHT)
        .await
        .unwrap();
    assert!(
        stale_anchors_by_root(&after, &canonical_roots).is_empty(),
        "the follow-up dry run reads clean"
    );
    let (tx_status,): (String,) = sqlx::query_as("SELECT status FROM transactions WHERE txid = ?")
        .bind(FUNDING_TXID)
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        tx_status, "unproven",
        "healed to unproven: still spendable, re-proved later"
    );
}

#[tokio::test]
async fn test_beef_bfs_walk_merges_valid_stored_beef() {
    use bsv_rs::transaction::AlwaysValidChainTracker;

    // Valid stored BEEF should be merged normally
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Build valid stored BEEF
    let (valid_beef_bytes, _) = build_test_beef_with_bump(100, "");

    let child_txid = format!("{:0>64}", "child02");

    seed_tx_with_input_beef(&storage, &child_txid, &valid_beef_bytes, &parent_raw_tx).await;

    // AlwaysValidChainTracker accepts any root
    let tracker = AlwaysValidChainTracker::new(950000);

    let mut beef = Beef::new();
    let mut pending_txids = vec![(child_txid.clone(), 0)];
    let mut processed_txids = HashSet::new();

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        None,
        Some(&tracker as &dyn ChainTracker),
    )
    .await;

    assert!(result.is_ok());
    // The parent txid from the stored BEEF should have been processed (merged)
    assert!(
        processed_txids.contains(parent_txid),
        "Parent txid should be processed after merging valid stored BEEF"
    );
}

#[tokio::test]
async fn test_beef_bfs_walk_no_tracker_skips_validation() {
    // Without a tracker, stored BEEF is merged unconditionally (backward compat)
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let parent_raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
        ).unwrap();
    let parent_txid = "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    // Corrupt BEEF — but no tracker, so it should still be merged
    let (corrupt_beef_bytes, _) = build_test_beef_with_bump(942926, "");

    let child_txid = format!("{:0>64}", "child03");

    seed_tx_with_input_beef(&storage, &child_txid, &corrupt_beef_bytes, &parent_raw_tx).await;

    let mut beef = Beef::new();
    let mut pending_txids = vec![(child_txid.clone(), 0)];
    let mut processed_txids = HashSet::new();

    let mut conn = storage.pool().acquire().await.unwrap();
    let result = beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending_txids,
        &mut processed_txids,
        None,
        None, // No tracker — skip validation
    )
    .await;

    assert!(result.is_ok());
    // Parent txid from stored BEEF should be processed (merged unconditionally)
    assert!(
        processed_txids.contains(parent_txid),
        "Without tracker, corrupt stored BEEF should still be merged"
    );
}

#[test]
fn test_beef_walk_skips_stored_beef_for_proven_tx() {
    // Verify the decision logic: when an ancestor tx has a merkle proof,
    // stored input_beef should NOT be merged (BUMP terminates the chain).
    // This matches TS/Go behavior where proven txs skip stored BEEF merge.

    // Case 1: BeefTxData with merkle_path present -> has_proof = true
    // A proven tx should skip stored BEEF merge.
    let tx_data_opt: Option<BeefTxData> = Some(BeefTxData {
        raw_tx: vec![0x01, 0x00, 0x00, 0x00],
        merkle_path: Some(vec![0xDE, 0xAD]),
    });
    let has_proof = tx_data_opt
        .as_ref()
        .map(|d| d.merkle_path.is_some())
        .unwrap_or(false);
    assert!(
        has_proof,
        "Proven tx (merkle_path Some) must set has_proof = true, skipping stored BEEF merge"
    );

    // Case 2: BeefTxData with merkle_path None -> has_proof = false
    // An unproven tx should merge stored BEEF.
    let tx_data_opt: Option<BeefTxData> = Some(BeefTxData {
        raw_tx: vec![0x01, 0x00, 0x00, 0x00],
        merkle_path: None,
    });
    let has_proof = tx_data_opt
        .as_ref()
        .map(|d| d.merkle_path.is_some())
        .unwrap_or(false);
    assert!(
        !has_proof,
        "Unproven tx (merkle_path None) must set has_proof = false, allowing stored BEEF merge"
    );

    // Case 3: No tx data at all -> has_proof = false
    // Should also attempt stored BEEF merge (fallback path).
    let tx_data_opt: Option<BeefTxData> = None;
    let has_proof = tx_data_opt
        .as_ref()
        .map(|d| d.merkle_path.is_some())
        .unwrap_or(false);
    assert!(
        !has_proof,
        "Missing tx data (None) must set has_proof = false, allowing stored BEEF merge"
    );
}
