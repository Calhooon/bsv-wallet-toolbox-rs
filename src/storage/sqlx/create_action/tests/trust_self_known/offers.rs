use super::*;

/// Eight coins, an offer of four, five concurrent calls: the four
/// offered coins fund four calls, one each, and no coin outside the
/// offer is ever allocated. The fifth refuses: busy (`InFlight`)
/// when it pre-checks while the four hold the offer (review 3 F2),
/// insufficient funds when it pre-checks after they spent it.
#[tokio::test]
async fn a_coin_outside_the_offer_is_never_allocated_under_concurrency() {
    let (storage, user_id) = setup().await;
    let ids = eight_coin_ids(&storage, user_id).await;
    let offer = vec![ids[1], ids[3], ids[5], ids[7]];
    let tracker = SlowTracker::new(&storage);
    let f = || offering(&storage, &tracker, user_id, &offer);
    let (a, b, c, d, e) = tokio::join!(f(), f(), f(), f(), f());
    let mut done = 0;
    for r in [a, b, c, d, e] {
        match r {
            Ok(_) => done += 1,
            Err(e) => assert!(
                matches!(
                    e,
                    Error::InsufficientFunds { .. }
                        | Error::StoreBusy {
                            reason: StoreBusyReason::InFlight
                        }
                ),
                "{e:?}"
            ),
        }
    }
    assert_eq!(done, 4, "each offered coin funds one call");
    assert_eq!(
        allocated(&storage).await,
        offer,
        "exactly the offered coins, nothing outside"
    );
    assert!(storage.in_flight_picks().reserved().is_empty());
}

/// The pre-check's pick is intersected with the offer: one offered
/// coin among eight equal ones is the coin the action spends.
#[tokio::test]
async fn the_precheck_picks_within_the_offer() {
    let (storage, user_id) = setup().await;
    let ids = eight_coin_ids(&storage, user_id).await;
    let tracker = SlowTracker::new(&storage);
    let result = offering(&storage, &tracker, user_id, &[ids[6]])
        .await
        .expect("the offered coin covers the action");
    let txid: String = sqlx::query_scalar("SELECT txid FROM outputs WHERE output_id = ?")
        .bind(ids[6])
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let sources: Vec<&str> = result
        .inputs
        .iter()
        .map(|i| i.source_txid.as_str())
        .collect();
    assert_eq!(sources, vec![txid.as_str()]);
    assert_eq!(allocated(&storage).await, vec![ids[6]]);
}

/// An offer that does not cover the action refuses insufficient
/// funds though the store holds coins enough outside it, and the
/// action writes nothing.
#[tokio::test]
async fn an_offer_that_does_not_cover_refuses_insufficient_funds() {
    let (storage, user_id) = setup().await;
    let ids = eight_coin_ids(&storage, user_id).await;
    let small = tx_spending(None, 0x7f, 500);
    let small = seed_change_coin(&storage, user_id, &small, true).await;
    let before = the_store(&storage).await;
    let tracker = SlowTracker::new(&storage);
    let err = offering(&storage, &tracker, user_id, &[small])
        .await
        .expect_err("500 sats cannot fund 1,000");
    assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
    assert!(err.to_string().starts_with("Insufficient funds: "), "{err}");
    assert_eq!(the_store(&storage).await, before, "nothing written");
    assert!(allocated(&storage).await.is_empty(), "{ids:?} untouched");
}

/// An empty offer refuses insufficient funds, nothing written.
#[tokio::test]
async fn an_empty_offer_refuses_insufficient_funds() {
    let (storage, user_id) = setup().await;
    eight_coin_ids(&storage, user_id).await;
    let before = the_store(&storage).await;
    let tracker = SlowTracker::new(&storage);
    let err = offering(&storage, &tracker, user_id, &[])
        .await
        .expect_err("an empty offer funds nothing");
    assert!(matches!(err, Error::InsufficientFunds { .. }), "{err:?}");
    assert_eq!(the_store(&storage).await, before, "nothing written");
}
