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

/// One walk from `subject` over `storage`, the read checks' writer its own;
/// `asker` is the storage a walk may ask services through, if any.
pub(super) async fn walk_from(
    storage: &StorageSqlx,
    subject: &str,
    asker: Option<&StorageSqlx>,
) -> Beef {
    let (mut beef, mut seen) = (Beef::new(), HashSet::new());
    let mut queue = vec![(subject.to_string(), 0usize)];
    let mut conn = storage.pool().acquire().await.unwrap();
    beef_bfs_walk(
        &mut conn,
        Some(storage.writer()),
        &mut beef,
        &mut queue,
        &mut seen,
        asker,
        None,
    )
    .await
    .expect("the walk completes");
    beef
}

#[tokio::test]
async fn test_walk_asks_for_at_most_its_proof_budget() {
    // One walk asks services for at most MAX_PROOF_FETCHES_PER_WALK merkle
    // paths: the budget caps the case where nothing on the chain is mined
    // yet. Ten old unproven hops above a proven root, every ask unanswered:
    // eight asks, and the walk still carries every hop.
    use crate::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
    use crate::storage::traits::WalletStorageProvider;
    use bsv_rs::transaction::MockChainTracker;

    let storage = l2_23_storage().await;

    let chain = synth_chain(11);
    seed_proven_chain_tx(&storage, &chain[0].1, &chain[0].0, 965_000).await;
    for (raw, txid) in &chain[1..] {
        seed_unproven_tx(&storage, txid, raw, None).await;
        backdate_tx(&storage, txid, 30).await;
    }

    let services = std::sync::Arc::new(
        MockWalletServices::builder()
            .get_merkle_path_response(MockResponse::Error(
                MockErrorKind::ServiceError,
                "not mined".to_string(),
            ))
            .build(),
    );
    storage.set_services(services.clone());
    let mut tracker = MockChainTracker::new(965_100);
    tracker.add_root(965_000, chain[0].1.clone());
    storage
        .set_chain_tracker(std::sync::Arc::new(tracker))
        .await;

    let beef = walk_from(&storage, &chain[10].1, Some(&storage)).await;

    assert_eq!(
        services.call_count("get_merkle_path"),
        MAX_PROOF_FETCHES_PER_WALK,
        "ten unproven hops, and the walk asks for its budget of proofs and no more"
    );
    let kept = beef_txids(&beef);
    for (_, txid) in &chain {
        assert!(
            kept.contains(txid),
            "every hop down to the proven root is carried"
        );
    }
}

#[tokio::test]
async fn test_walk_queues_every_input_of_an_unproven_transaction() {
    // An unproven transaction is carried whole and EVERY one of its inputs
    // is walked, whatever else is already queued: a parent left out leaves
    // the BEEF without the transaction an input spends.
    use super::super::test_support::synth_tx;

    let storage = l2_23_storage().await;

    let (left_raw, left) = synth_tx(&"11".repeat(32), 0, 5_000);
    let (right_raw, right) = synth_tx(&"22".repeat(32), 0, 6_000);
    seed_proven_chain_tx(&storage, &left, &left_raw, 965_000).await;
    seed_proven_chain_tx(&storage, &right, &right_raw, 965_001).await;

    // The subject spends both parents' output 0.
    let mut raw: Vec<u8> = Vec::new();
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.push(2);
    for parent in [&left, &right] {
        let mut prev = hex::decode(parent).unwrap();
        prev.reverse();
        raw.extend_from_slice(&prev);
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.push(0);
        raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
    }
    raw.push(1);
    raw.extend_from_slice(&10_000u64.to_le_bytes());
    raw.push(1);
    raw.push(0x51);
    raw.extend_from_slice(&0u32.to_le_bytes());
    let subject = txid_of(&raw);
    seed_unproven_tx(&storage, &subject, &raw, None).await;

    let beef = walk_from(&storage, &subject, None).await;

    let kept = beef_txids(&beef);
    assert!(kept.contains(&subject), "the subject is carried whole");
    assert!(kept.contains(&left), "its left parent is walked");
    assert!(
        kept.contains(&right),
        "its right parent is walked though the left one was queued ahead of it"
    );
    assert_eq!(beef.bumps.len(), 2, "each parent with its BUMP");
}
