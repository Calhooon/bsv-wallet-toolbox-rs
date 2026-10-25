use super::*;

/// The reviewer's race: four createActions under `tokio::join!` on a
/// wallet of eight 900,000-sat coins. At 87d3d43 two of four were
/// refused 'Insufficient funds: need 1005, have 0'; with the
/// in-flight reservation every call succeeds, each from its own
/// coin, and no reservation outlives its call.
#[tokio::test]
async fn four_concurrent_creates_on_eight_coins_all_succeed() {
    let (storage, user_id) = setup().await;
    eight_coins(&storage, user_id).await;
    let tracker = SlowTracker::new(&storage);
    let f = || {
        create_action_internal(
            &storage,
            Some(&tracker as &dyn ChainTracker),
            user_id,
            funded_args(),
        )
    };
    let (a, b, c, d) = tokio::join!(f(), f(), f(), f());
    let mut sources = Vec::new();
    for r in [a, b, c, d] {
        let r = r.expect("a funded wallet funds every call");
        sources.extend(r.inputs.into_iter().map(|i| i.source_txid));
    }
    let distinct: HashSet<&String> = sources.iter().collect();
    assert_eq!(sources.len(), 4, "{sources:?}");
    assert_eq!(distinct.len(), 4, "each call spends its own coin");
    let seen = tracker.seen_reserved.lock().unwrap().clone();
    assert!(
        seen.iter().any(|n| *n > 1),
        "the pre-checks overlapped, several coins reserved at once: {seen:?}"
    );
    assert!(
        storage.in_flight_picks().reserved().is_empty(),
        "every reservation is released when its call returns"
    );
}

/// The same race with the reservation off (the case of another
/// process): a call whose pre-check covered the action and whose
/// coin was taken under it twice is refused `StoreBusy`
/// (`MovedUnderUs`), never `InsufficientFunds`: the wallet is funded.
#[tokio::test]
async fn without_the_reservation_a_twice_lost_race_is_store_busy_never_insufficient_funds() {
    let (storage, user_id) = setup().await;
    eight_coins(&storage, user_id).await;
    storage.in_flight_picks().set_off(true);
    let tracker = SlowTracker::new(&storage);
    let f = || {
        create_action_internal(
            &storage,
            Some(&tracker as &dyn ChainTracker),
            user_id,
            funded_args(),
        )
    };
    let (a, b, c, d) = tokio::join!(f(), f(), f(), f());
    let errs: Vec<Error> = [a, b, c, d].into_iter().filter_map(|r| r.err()).collect();
    assert!(
        !errs.is_empty(),
        "with no reservation the concurrent pre-checks collide"
    );
    for err in &errs {
        assert!(
            matches!(
                err,
                Error::StoreBusy {
                    reason: StoreBusyReason::MovedUnderUs
                }
            ),
            "a funded wallet's lost race is busy: {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "Store busy: the store moved between the pre-check and the write lock twice"
        );
    }
}

/// The reservation is released on a refusal too: a call refused
/// for want of funds leaves no coin reserved, and the next call
/// picks the same coin.
#[tokio::test]
async fn a_refused_call_releases_its_reservation() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let small = tx_spending(None, 0x61, 600);
    seed_change_coin(&storage, user_id, &small, true).await;
    let tracker = SlowTracker::new(&storage);
    let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect_err("600 sats do not fund 1,000");
    assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
    assert!(storage.in_flight_picks().reserved().is_empty());

    let big = tx_spending(None, 0x62, 50_000);
    seed_change_coin(&storage, user_id, &big, true).await;
    create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect("funded now");
    assert!(storage.in_flight_picks().reserved().is_empty());
}

/// Review 3 F2 of #44: nine concurrent createActions on eight coins,
/// each needing one. Eight are done, each from its own coin; the
/// ninth's pre-check finds every coin reserved by a call in flight,
/// and planned without those reservations it is covered: it is busy
/// (`InFlight`) or done, never `Insufficient funds`.
#[tokio::test]
async fn nine_concurrent_creates_on_eight_coins_are_done_or_busy_never_insufficient_funds() {
    let (storage, user_id) = setup().await;
    eight_coins(&storage, user_id).await;
    let tracker = SlowTracker::new(&storage);
    let results = futures::future::join_all((0..9).map(|_| {
        create_action_internal(
            &storage,
            Some(&tracker as &dyn ChainTracker),
            user_id,
            funded_args(),
        )
    }))
    .await;
    let mut sources = Vec::new();
    for result in results {
        match result {
            Ok(done) => sources.extend(done.inputs.into_iter().map(|i| i.source_txid)),
            Err(err) => {
                assert!(
                    matches!(
                        err,
                        Error::StoreBusy {
                            reason: StoreBusyReason::InFlight
                        }
                    ),
                    "a funded wallet's call is busy or done: {err:?}"
                );
                assert_eq!(err.to_string(), "Store busy: coins held by calls in flight");
            }
        }
    }
    let distinct: HashSet<&String> = sources.iter().collect();
    assert_eq!(sources.len(), 8, "eight done: {sources:?}");
    assert_eq!(distinct.len(), 8, "each from its own coin");
    assert!(storage.in_flight_picks().reserved().is_empty());
}

/// The reviewer's probe (review 3 F2 of #44): the eight coins held
/// by reservations of calls in flight (held here by hand). The call
/// answers busy, `InFlight`, having written nothing; the reservations
/// released, the same call is done.
#[tokio::test]
async fn coins_held_by_calls_in_flight_are_busy_then_done_once_released() {
    let (storage, user_id) = setup().await;
    eight_coins(&storage, user_id).await;
    let coins: Vec<i64> =
        sqlx::query_scalar("SELECT output_id FROM outputs WHERE change = 1 AND spendable = 1")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    assert_eq!(coins.len(), 8);
    let mut held = PickReservation::new(storage.in_flight_picks().clone());
    for coin in &coins {
        assert!(held.try_take(*coin));
    }
    let before = the_store(&storage).await;

    let err = create_action_internal(&storage, None, user_id, funded_args())
        .await
        .expect_err("every coin is held by a call in flight");
    assert!(
        matches!(
            err,
            Error::StoreBusy {
                reason: StoreBusyReason::InFlight
            }
        ),
        "never insufficient funds on a funded wallet: {err:?}"
    );
    assert_eq!(the_store(&storage).await, before, "nothing written");
    assert_eq!(
        storage.in_flight_picks().reserved().len(),
        8,
        "the refused call holds nothing of its own"
    );

    drop(held);
    create_action_internal(&storage, None, user_id, funded_args())
        .await
        .expect("released, the wallet funds the call");
}

/// One store, two handles (a clone, as a host gives its `Wallet` and
/// its `Monitor`): the coins reserved through one handle are held
/// for the other's dry run, which answers `InFlight`, never picking
/// them; released through the first, the second's call is done.
#[tokio::test]
async fn a_reservation_through_one_handle_is_seen_by_the_others_dry_run() {
    let (storage, user_id) = setup().await;
    eight_coins(&storage, user_id).await;
    let other = storage.clone();
    let coins: Vec<i64> =
        sqlx::query_scalar("SELECT output_id FROM outputs WHERE change = 1 AND spendable = 1")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    let mut held = PickReservation::new(storage.in_flight_picks().clone());
    for coin in &coins {
        assert!(held.try_take(*coin));
    }
    assert_eq!(other.in_flight_picks().reserved().len(), 8);

    let err = create_action_internal(&other, None, user_id, funded_args())
        .await
        .expect_err("every coin is held through the first handle");
    assert!(
        matches!(
            err,
            Error::StoreBusy {
                reason: StoreBusyReason::InFlight
            }
        ),
        "{err:?}"
    );

    drop(held);
    assert!(other.in_flight_picks().reserved().is_empty());
    create_action_internal(&other, None, user_id, funded_args())
        .await
        .expect("released through the first handle, the second's call is done");
}

/// A shortfall that is the funds stays `InsufficientFunds` while a
/// call is in flight: two 300-sat coins, one held; planned without
/// the reservation the action is still uncovered, so not busy.
#[tokio::test]
async fn a_true_shortfall_with_a_call_in_flight_stays_insufficient_funds() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let small = tx_spending(None, 0x51, 300);
    let other = tx_spending(None, 0x52, 300);
    let a = seed_change_coin(&storage, user_id, &small, true).await;
    seed_change_coin(&storage, user_id, &other, true).await;
    let mut held = PickReservation::new(storage.in_flight_picks().clone());
    assert!(held.try_take(a));

    let err = create_action_internal(&storage, None, user_id, funded_args())
        .await
        .expect_err("600 sats in all do not fund 1,000");
    assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
}
