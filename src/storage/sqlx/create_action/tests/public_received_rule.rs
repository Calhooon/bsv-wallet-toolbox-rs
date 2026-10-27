//! #304: R1 (d) of the wallet charter (section 2, the `pending, received`
//! row of section 5) holds a received coin whose parent is not `completed`
//! unless the spending action names the rule (`underReceivedRule`), off by
//! default. The public allocator (`StorageSqlx::allocate_change_input`) and
//! its count name no rule, so they hold every such coin, as the door's
//! allocator does with the rule off. The wallet's own unproven change is
//! not a received coin and stays eligible.

use super::managed_change_filters::one_coin;
use crate::storage::sqlx::StorageSqlx;

/// The public count, then the public pick of a 1000-satoshi target.
pub(super) async fn public_count_and_pick(
    storage: &StorageSqlx,
    user_id: i64,
    basket_id: i64,
    spender: i64,
    exclude_sending: bool,
) -> (i64, bool) {
    let count = storage
        .count_change_inputs(user_id, basket_id, exclude_sending)
        .await
        .unwrap();
    let picked = storage
        .allocate_change_input(user_id, basket_id, 1000, exclude_sending, spender)
        .await
        .unwrap();
    (count, picked.is_some())
}

/// The public count and pick for one managed change coin of `purpose` on a
/// parent of `status` (#295's one-coin fixture, its parent re-statused).
async fn counted_and_picked(purpose: &str, status: &str, exclude_sending: bool) -> (i64, bool) {
    let plant = format!("UPDATE outputs SET purpose = '{purpose}'");
    let (storage, user_id, basket_id, spender) = one_coin("f304", &plant).await;
    sqlx::query(
        "UPDATE transactions SET status = ? \
         WHERE transaction_id = (SELECT transaction_id FROM outputs)",
    )
    .bind(status)
    .execute(storage.writer())
    .await
    .unwrap();
    public_count_and_pick(&storage, user_id, basket_id, spender, exclude_sending).await
}

#[tokio::test]
async fn the_public_allocator_holds_an_unproven_received_coin() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("receive", "unproven", exclude_sending).await,
            (0, false),
            "received, parent unproven, exclude_sending={exclude_sending}"
        );
    }
}

#[tokio::test]
async fn the_public_allocator_holds_a_received_coin_whose_parent_is_sending() {
    assert_eq!(
        counted_and_picked("receive", "sending", false).await,
        (0, false),
        "received, parent sending"
    );
}

#[tokio::test]
async fn the_public_allocator_takes_a_received_coin_once_its_parent_is_completed() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("receive", "completed", exclude_sending).await,
            (1, true),
            "received, parent completed, exclude_sending={exclude_sending}"
        );
    }
}

#[tokio::test]
async fn the_public_allocator_still_takes_the_wallets_own_unproven_change() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("change", "unproven", exclude_sending).await,
            (1, true),
            "own change, parent unproven, exclude_sending={exclude_sending}"
        );
    }
    assert_eq!(
        counted_and_picked("change", "sending", false).await,
        (1, true),
        "own change, parent sending"
    );
}
