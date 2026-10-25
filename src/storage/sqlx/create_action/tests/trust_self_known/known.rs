use super::*;

/// The witness: the caller's `inputBEEF` carries one BUMP-proven
/// transaction and one txid-only entry for a transaction the wallet
/// holds with its proof, both direct inputs, under a chain tracker.
/// 0.7.0 on bsv-rs 0.4.1 built it; 0.7.1 at 4cd8ee7 refused it
/// (`StubNotProven` at `verify_valid`). Storage's proof replaces the
/// entry: the BEEF built carries no txid-only entry.
#[tokio::test]
async fn a_known_proven_txid_only_input_is_resolved_from_storage() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let known = tx_spending(None, 0xcc, 5_000);
    seed_known(&storage, user_id, &known, true).await;

    let tracker = AlwaysValidChainTracker::new(100_000);
    let result = create_action_internal(
        &storage,
        Some(&tracker),
        user_id,
        args(
            vec![input(&external.id()), input(&known.id())],
            caller_beef(&external, &[known.id()]),
        ),
    )
    .await
    .expect("a txid-only entry the wallet holds is honoured under trustSelf known");

    let mut beef = Beef::from_binary(&result.input_beef.unwrap()).unwrap();
    assert!(
        beef.txs.iter().all(|t| !t.is_txid_only()),
        "no txid-only entry rides"
    );
    assert!(
        beef.find_txid(&known.id()).unwrap().bump_index().is_some(),
        "storage's proof rides"
    );
    assert!(beef.verify_valid(false).valid);
}

/// Storage holds the bytes but no proof: the bytes ride, and the walk
/// carries the transaction's proven parent from storage.
#[tokio::test]
async fn a_known_unproven_txid_only_input_rides_with_its_stored_ancestry() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let grandparent = tx_spending(None, 0xdd, 9_000);
    seed_known(&storage, user_id, &grandparent, true).await;
    let known = tx_spending(Some(&grandparent.id()), 0xcc, 5_000);
    seed_known(&storage, user_id, &known, false).await;

    let tracker = AlwaysValidChainTracker::new(100_000);
    let result = create_action_internal(
        &storage,
        Some(&tracker),
        user_id,
        args(
            vec![input(&external.id()), input(&known.id())],
            caller_beef(&external, &[known.id()]),
        ),
    )
    .await
    .expect("the wallet's own unproven transaction is resolved with its ancestry");

    let mut beef = Beef::from_binary(&result.input_beef.unwrap()).unwrap();
    assert!(beef.txs.iter().all(|t| !t.is_txid_only()));
    assert!(beef.find_txid(&known.id()).unwrap().raw_tx().is_some());
    assert!(beef
        .find_txid(&grandparent.id())
        .unwrap()
        .bump_index()
        .is_some());
    assert!(beef.verify_valid(false).valid);
}

/// Without trustSelf the same shape is refused as before: the BEEF
/// must prove every txid-only entry it carries.
#[tokio::test]
async fn without_trust_self_the_shape_is_refused() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let known = tx_spending(None, 0xcc, 5_000);
    seed_known(&storage, user_id, &known, true).await;

    let mut a = args(
        vec![input(&external.id()), input(&known.id())],
        caller_beef(&external, &[known.id()]),
    );
    a.options = None;
    let tracker = AlwaysValidChainTracker::new(100_000);
    let err = create_action_internal(&storage, Some(&tracker), user_id, a)
        .await
        .expect_err("no trustSelf, no vouching");
    assert!(
        err.to_string().contains("BEEF structure is invalid"),
        "{err}"
    );
}

/// trustSelf vouches only for what storage holds: a txid-only
/// ancestor storage does not know is refused, and the reason says so.
#[tokio::test]
async fn an_unknown_txid_only_entry_is_refused() {
    let (storage, user_id) = setup().await;
    let external = tx_spending(None, 0xaa, 5_000);
    let stranger = tx_spending(None, 0xee, 9_000);
    let unproven = tx_spending(Some(&stranger.id()), 0xbb, 5_000);

    let mut beef = Beef::from_binary(&caller_beef(&external, &[stranger.id()])).unwrap();
    beef.merge_raw_tx(unproven.to_binary(), None);

    let tracker = AlwaysValidChainTracker::new(100_000);
    let err = create_action_internal(
        &storage,
        Some(&tracker),
        user_id,
        args(
            vec![input(&external.id()), input(&unproven.id())],
            beef.to_binary(),
        ),
    )
    .await
    .expect_err("an entry storage does not know is not vouched for");
    let msg = err.to_string();
    assert!(
        msg.contains(&stranger.id()),
        "the refusal names the entry: {msg}"
    );
    assert!(
        msg.contains("not known to this wallet"),
        "the reason: {msg}"
    );
}
