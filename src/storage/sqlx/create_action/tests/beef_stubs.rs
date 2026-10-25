use super::super::beef_graph::validate_stored_beef;
use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn l2_23_pin_i_internalize_refuses_stub_without_storing() {
    use bsv_rs::transaction::MockChainTracker;
    for tracker in [true, false] {
        let s = l2_23_storage().await;
        let (user, _) = s.find_or_insert_user("02abcd").await.unwrap();
        if tracker {
            s.set_chain_tracker(std::sync::Arc::new(MockChainTracker::new(101)))
                .await;
        }
        let chain = synth_chain(2);
        let mut stub = Beef::new();
        stub.merge_raw_tx(chain[1].0.clone(), None);
        let result = super::super::internalize_action::internalize_action_internal(
            &s,
            user.user_id,
            l2_23_args(stub.to_binary_atomic(&chain[1].1).unwrap()),
        )
        .await;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM transactions WHERE txid = ? AND input_beef IS NOT NULL",
        )
        .bind(&chain[1].1)
        .fetch_one(s.pool())
        .await
        .unwrap();
        assert_eq!(count, 0, "invalid input_beef must never be stored");
        assert!(
            matches!(result, Err(Error::ValidationError(ref e)) if e.contains("structure is invalid")),
            "got {result:?}"
        );
    }
}

#[tokio::test]
async fn l2_23_pin_ii_walk_recovers_the_proven_parent() {
    let (_, bytes, chain) = l2_23_assembly(true).await;
    let mut beef = Beef::from_binary(&bytes).unwrap();
    assert!(
        beef.find_txid(&chain[0].1).is_some(),
        "stored stub must not end the walk"
    );
    assert_eq!(beef.bumps.len(), 1);
    assert!(beef.verify_valid(true).valid);
}

#[tokio::test]
async fn l2_23_pin_iii_strict_round_trip_of_wallet_assembly() {
    use bsv_rs::transaction::MockChainTracker;
    let (s, bytes, chain) = l2_23_assembly(true).await;
    let (user, _) = s.find_or_insert_user("03abcd").await.unwrap();
    let mut t = MockChainTracker::new(101);
    t.add_root(100, chain[0].1.clone());
    s.set_chain_tracker(std::sync::Arc::new(t)).await;
    let mut beef = Beef::from_binary(&bytes).unwrap();
    let result = super::super::internalize_action::internalize_action_internal(
        &s,
        user.user_id,
        l2_23_args(beef.to_binary_atomic(&chain[2].1).unwrap()),
    )
    .await;
    assert!(
        result.is_ok(),
        "strict internalize must accept our assembly: {result:?}"
    );
    assert!(result.unwrap().base.accepted);
    assert_eq!(
        beef.bumps.len(),
        1,
        "unmodified assembly must carry its anchor proof"
    );
}

#[tokio::test]
async fn l2_23_stored_bumpless_structure_is_checked() {
    use bsv_rs::transaction::MockChainTracker;
    let chain = synth_chain(2);
    let mut stub = Beef::new();
    stub.merge_raw_tx(chain[1].0.clone(), None);
    assert!(!validate_stored_beef(&mut stub, &MockChainTracker::new(101), &chain[1].1).await);
}

#[tokio::test]
async fn l2_23_final_bumpless_invalid_assembly_is_an_error() {
    use bsv_rs::transaction::MockChainTracker;
    for tracker in [true, false] {
        let s = l2_23_storage().await;
        let chain = synth_chain(2);
        seed_unproven_tx(&s, &chain[1].1, &chain[1].0, None).await;
        let t = MockChainTracker::new(101);
        let mut conn = s.pool().acquire().await.unwrap();
        let result = build_input_beef(
            &mut conn,
            Some(s.writer()),
            if tracker { Some(&t) } else { None },
            &[l2_23_input(&chain[1].1)],
            &[],
            None,
            &[],
            false,
            false,
            None,
        )
        .await;
        assert!(
            matches!(result, Err(Error::ValidationError(ref e)) if e.contains("structure is invalid")),
            "invalid assembly returned: {result:?}"
        );
    }
}

#[tokio::test]
async fn l2_23_walk_without_tracker_recovers_local_parent() {
    let (_, bytes, chain) = l2_23_assembly(false).await;
    let mut beef = Beef::from_binary(&bytes).unwrap();
    assert!(beef.find_txid(&chain[0].1).is_some());
    assert!(beef.verify_valid(true).valid);
}
