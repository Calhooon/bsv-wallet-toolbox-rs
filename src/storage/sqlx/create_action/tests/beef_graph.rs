use super::super::*;
use super::*;

#[tokio::test]
async fn test_validate_stored_beef_valid_roots() {
    use bsv_rs::transaction::MockChainTracker;

    // Build a BEEF with a bump at height 500
    let (beef_bytes, _txid) = build_test_beef_with_bump(500, "");

    let mut beef = Beef::from_binary(&beef_bytes).unwrap();
    // Get the actual computed root so we can register it with the tracker
    let validation = beef.verify_valid(true);
    assert!(validation.valid);
    let (height, root) = validation.roots.iter().next().unwrap();

    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(*height, root.clone());

    let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
    assert!(
        result,
        "Valid BEEF with matching root should pass validation"
    );
}

#[tokio::test]
async fn test_validate_stored_beef_invalid_root_rejected() {
    use bsv_rs::transaction::MockChainTracker;

    // Build a BEEF with a bump at height 942926
    let (beef_bytes, _txid) = build_test_beef_with_bump(942926, "");

    let mut beef = Beef::from_binary(&beef_bytes).unwrap();

    // Tracker has a DIFFERENT root for height 942926 (simulates orphaned block)
    let mut tracker = MockChainTracker::new(1000);
    tracker.add_root(
        942926,
        "e6bcdfaf6cdc58d1b98dd9ccd67608b0cea09c08873b6ad4d88aa51f597fc69a".to_string(),
    );

    let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
    assert!(!result, "BEEF with invalid merkle root should be rejected");
}

#[tokio::test]
async fn test_validate_stored_beef_unknown_height_rejected() {
    use bsv_rs::transaction::MockChainTracker;

    // Build a BEEF with a bump at height 999
    let (beef_bytes, _txid) = build_test_beef_with_bump(999, "");

    let mut beef = Beef::from_binary(&beef_bytes).unwrap();

    // Tracker has NO root for height 999 → is_valid_root_for_height returns false
    let tracker = MockChainTracker::new(1000);

    let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
    assert!(!result, "BEEF with unknown height should be rejected");
}

#[tokio::test]
async fn test_validate_stored_beef_no_bumps_passes() {
    use bsv_rs::transaction::MockChainTracker;

    // Empty BEEF with no bumps — should always pass
    let mut beef = Beef::new();
    let tracker = MockChainTracker::new(1000);

    let result = validate_stored_beef(&mut beef, &tracker, "test_txid").await;
    assert!(result, "BEEF with no bumps should pass validation");
}

#[tokio::test]
async fn test_validate_stored_beef_tracker_error_rejects() {
    use bsv_rs::transaction::ChainTrackerError;

    // Build a BEEF with a bump
    let (beef_bytes, _txid) = build_test_beef_with_bump(500, "");
    let mut beef = Beef::from_binary(&beef_bytes).unwrap();

    // Tracker that always errors
    struct ErrorTracker;

    #[async_trait::async_trait]
    impl ChainTracker for ErrorTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> std::result::Result<bool, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("timeout".to_string()))
        }
        async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
            Ok(1000)
        }
    }

    let result = validate_stored_beef(&mut beef, &ErrorTracker, "test_txid").await;
    assert!(
        !result,
        "ChainTracker error should cause stored BEEF to be rejected (fail-safe)"
    );
}

#[test]
fn test_describe_invalid_beef_names_the_missing_parent_and_dumps_the_bytes() {
    let chain = synth_chain(3); // a <- b <- c
    let mut beef = Beef::new();
    // c spends b, and b is absent: the BEEF cannot be valid.
    beef.merge_raw_tx(chain[2].0.clone(), None);
    assert!(
        !beef.verify_valid(true).valid,
        "the shape under test is invalid"
    );
    let text = describe_invalid_beef(&mut beef, &[chain[2].1.clone()]);
    let b16: String = chain[1].1.chars().take(16).collect();
    let c16: String = chain[2].1.chars().take(16).collect();
    assert!(
        text.contains(&format!("missing inputs [{b16}]")),
        "names the absent parent: {text}"
    );
    assert!(
        text.contains(&format!("transactions with missing inputs [{c16}]")),
        "names the child that needs it: {text}"
    );
    assert!(
        text.contains(&format!("roots [{c16}]")),
        "names the root: {text}"
    );
    assert!(
        text.contains(&format!(
            "dump {}",
            std::env::temp_dir().join("beef-rejected-").display()
        )),
        "points at the dump: {text}"
    );
    let path = text.rsplit("dump ").next().unwrap().to_string();
    let dumped = std::fs::read_to_string(&path).expect("the dump exists");
    assert_eq!(
        dumped,
        hex::encode(beef.to_binary()),
        "the dump is the rejected bytes"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_prune_stops_at_a_proven_ancestor() {
    let chain = synth_chain(3); // a <- b <- c
    let mut beef = Beef::new();
    let bump_idx = beef.merge_bump(synth_bump(900_000, &chain[1].1));
    beef.merge_raw_tx(chain[0].0.clone(), None);
    beef.merge_raw_tx(chain[1].0.clone(), Some(bump_idx));
    beef.merge_raw_tx(chain[2].0.clone(), None);

    prune_beef_to_roots(&mut beef, &[chain[2].1.clone()]);

    let kept = beef_txids(&beef);
    assert_eq!(kept.len(), 2, "only the root and its proven parent remain");
    assert!(kept.contains(&chain[2].1), "the root is kept");
    assert!(kept.contains(&chain[1].1), "the proven parent is kept");
    assert!(
        !kept.contains(&chain[0].1),
        "the proven parent's own parent must be dropped"
    );
    assert_eq!(beef.bumps.len(), 1, "the surviving bump is kept");
    assert!(beef.verify_valid(true).valid, "the pruned BEEF is valid");
}

#[test]
fn test_prune_carries_an_unproven_ancestor_whole() {
    let chain = synth_chain(3);
    let mut beef = Beef::new();
    for (raw, _) in &chain {
        beef.merge_raw_tx(raw.clone(), None);
    }

    prune_beef_to_roots(&mut beef, &[chain[2].1.clone()]);

    assert_eq!(
        beef.txs.len(),
        3,
        "with no proof anywhere the whole chain is still required"
    );
}

#[test]
fn test_prune_ten_hops_with_the_middle_ones_proven() {
    let chain = synth_chain(10);
    let mut beef = Beef::new();
    for (i, (raw, txid)) in chain.iter().enumerate() {
        // Hops 3, 4 and 5 were mined; the rest are still unproven.
        if (3..=5).contains(&i) {
            let idx = beef.merge_bump(synth_bump(965_000 + i as u32, txid));
            beef.merge_raw_tx(raw.clone(), Some(idx));
        } else {
            beef.merge_raw_tx(raw.clone(), None);
        }
    }
    assert_eq!(beef.txs.len(), 10);

    prune_beef_to_roots(&mut beef, &[chain[9].1.clone()]);

    // Walking back from hop 9: 9, 8, 7, 6 are unproven and needed; hop 5 is
    // proven so it terminates the chain; hops 0..=4 are gone.
    let kept = beef_txids(&beef);
    let expected: HashSet<String> = chain[5..].iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(kept, expected, "exactly hops 5..9 survive");
    assert_eq!(beef.bumps.len(), 1, "only hop 5's bump is still referenced");
    assert!(beef.verify_valid(true).valid);
}

#[test]
fn test_prune_keeps_an_unproven_root_below_a_proven_root() {
    // Both an unproven transaction and a proven descendant of it are being
    // spent. The unproven one still needs its own history, so it must
    // survive even though a proven transaction descends from it. This is
    // the case a root-blind trim gets wrong.
    let chain = synth_chain(3); // a <- b <- c
    let mut beef = Beef::new();
    beef.merge_raw_tx(chain[0].0.clone(), None);
    beef.merge_raw_tx(chain[1].0.clone(), None);
    let idx = beef.merge_bump(synth_bump(900_001, &chain[2].1));
    beef.merge_raw_tx(chain[2].0.clone(), Some(idx));

    prune_beef_to_roots(&mut beef, &[chain[1].1.clone(), chain[2].1.clone()]);

    let kept = beef_txids(&beef);
    assert!(kept.contains(&chain[2].1), "the proven root is kept");
    assert!(kept.contains(&chain[1].1), "the unproven root is kept");
    assert!(
        kept.contains(&chain[0].1),
        "the unproven root still needs its own parent"
    );
}

#[test]
fn test_prune_leaves_a_beef_alone_when_no_root_is_present() {
    let chain = synth_chain(2);
    let mut beef = Beef::new();
    for (raw, _) in &chain {
        beef.merge_raw_tx(raw.clone(), None);
    }
    prune_beef_to_roots(&mut beef, &["00".repeat(32)]);
    assert_eq!(beef.txs.len(), 2, "pruning must never empty a BEEF");
}
