use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_walk_terminates_at_a_proven_ancestor() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(3);
    seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
    seed_proven_chain_tx(&storage, &chain[1].1, &chain[1].0, 965_100).await;
    seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;

    let mut beef = Beef::new();
    let mut pending = vec![(chain[2].1.clone(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();

    let kept = beef_txids(&beef);
    assert!(kept.contains(&chain[2].1), "the subject is carried whole");
    assert!(
        kept.contains(&chain[1].1),
        "the proven parent contributes its transaction"
    );
    assert_eq!(beef.bumps.len(), 1, "and its BUMP");
    assert!(
        !kept.contains(&chain[0].1),
        "the recursion stops at the proven parent"
    );
}

#[tokio::test]
async fn test_walk_carries_an_unproven_ancestor_whole() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(3);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_050).await;
    seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, None).await;
    seed_unproven_tx(&storage, &chain[2].1, &chain[2].0, None).await;

    let mut beef = Beef::new();
    let mut pending = vec![(chain[2].1.clone(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();

    let kept = beef_txids(&beef);
    assert_eq!(kept.len(), 3, "the unproven middle hop is carried whole");
    assert!(kept.contains(&chain[1].1));
    assert_eq!(beef.bumps.len(), 1, "the walk still reaches the proof");
}

/// A valid BEEF is never refused for its counts (the no-limits posture):
/// a chain of unproven ancestors of any depth is walked to its proven
/// anchor, and the BEEF the walk builds is valid to bsv-rs's in-memory
/// check and to its streaming reader. The reference stops at depth 12 and
/// throws (ts-stack@edf6e03 StorageProvider.ts:199,
/// getBeefForTransaction.ts:156-157); before this the walk skipped every
/// ancestor past 12 and the BEEF missed them.
#[tokio::test]
async fn test_walk_reaches_the_proven_anchor_at_any_depth() {
    use bsv_rs::transaction::{verify_stream_structure, MockChainTracker};

    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    const HOPS: usize = 1_000;
    const HEIGHT: u32 = 965_300;
    let chain = synth_chain(HOPS);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, HEIGHT).await;
    for (raw, txid) in &chain[1..] {
        seed_unproven_tx(&storage, txid, raw, None).await;
    }

    let subject = chain[HOPS - 1].1.clone();
    let mut beef = Beef::new();
    let mut pending = vec![(subject.clone(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();

    let kept = beef_txids(&beef);
    assert_eq!(kept.len(), HOPS, "every unproven ancestor is carried");
    assert!(kept.contains(&chain[0].1), "down to the proven anchor");
    assert_eq!(beef.bumps.len(), 1);
    // The walk holds children ahead of their parents; bsv-rs judges the
    // order held (BRC-62, since 0.9.0), so the check reads it sorted, as
    // the assembly and `to_binary` do.
    beef.sort_txs();
    assert!(
        beef.verify_valid(true).valid,
        "valid to the in-memory check"
    );

    let mut tracker = MockChainTracker::new(HEIGHT + 1);
    tracker.add_root(HEIGHT, chain[0].1.clone());
    let bytes = beef.to_binary();
    let verdict = verify_stream_structure(&bytes[..], &tracker, None).unwrap();
    assert!(verdict.is_valid(), "valid to the stream: {verdict:?}");
}

#[tokio::test]
async fn test_walk_honours_a_bump_already_in_the_beef() {
    // The parent has no proven_txs row of its own, but the caller's
    // inputBEEF already carries a BUMP for it. That proof is the evidence:
    // the parent's stored ancestry must not be dragged in behind it.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();

    let chain = synth_chain(3);

    // The parent's stored input_beef still holds the whole unproven history.
    let mut stored = Beef::new();
    stored.merge_raw_tx(chain[0].0.clone(), None);
    let stored_bytes = stored.to_binary();

    seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
    seed_unproven_tx(&storage, &chain[1].1, &chain[1].0, Some(&stored_bytes)).await;

    let mut beef = Beef::new();
    beef.merge_bump(synth_bump(965_200, &chain[1].1));
    let mut pending = vec![(chain[1].1.clone(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();

    let kept = beef_txids(&beef);
    assert!(kept.contains(&chain[1].1), "the proven parent is carried");
    assert!(
        !kept.contains(&chain[0].1),
        "a BUMP in the BEEF terminates the chain, stored ancestry and all"
    );
}

#[tokio::test]
async fn test_walk_links_a_root_that_a_siblings_bump_carries_unflagged() {
    // LOW seat p25 (2026-09-04): two roots mined as siblings in one block.
    // Root A's proof was merged first, and the combined BUMP carries B's
    // hash only as A's sibling (txid: false). B has stored history behind
    // it. Walking B must link it to that BUMP, not leave it unproven with
    // a parent the walk never fetched.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test", "000000").await.unwrap();
    storage.make_available().await.unwrap();
    let chain = synth_chain(2); // parent <- B
    let (raw_b, txid_b) = (chain[1].0.clone(), chain[1].1.clone());
    let txid_a = "aa".repeat(32);
    seed_unproven_tx(&storage, &chain[0].1, &chain[0].0, None).await;
    seed_unproven_tx(&storage, &txid_b, &raw_b, None).await;
    let mut beef = Beef::new();
    beef.merge_bump(MerklePath {
        block_height: 964_422,
        path: vec![vec![
            bsv_rs::transaction::MerklePathLeaf {
                offset: 0,
                hash: Some(txid_a.clone()),
                txid: true,
                duplicate: false,
            },
            bsv_rs::transaction::MerklePathLeaf {
                offset: 1,
                hash: Some(txid_b.clone()),
                txid: false,
                duplicate: false,
            },
        ]],
    });
    let mut pending = vec![(txid_b.clone(), 0usize)];
    let mut processed = HashSet::new();
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut pending,
        &mut processed,
        None,
        None,
    )
    .await
    .unwrap();
    let b = beef.find_txid(&txid_b).expect("B is carried");
    assert_eq!(
        b.bump_index(),
        Some(0),
        "B is linked to the BUMP that holds it"
    );
    assert!(
        !beef_txids(&beef).contains(&chain[0].1),
        "a proven root's parent is never dragged in"
    );
    assert!(
        beef.verify_valid(true).valid,
        "the BEEF is valid: {}",
        describe_invalid_beef(&mut beef, std::slice::from_ref(&txid_b))
    );
    prune_beef_to_roots(&mut beef, std::slice::from_ref(&txid_b));
    assert!(
        beef.verify_valid(true).valid,
        "and stays valid after the trim"
    );
}
