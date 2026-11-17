//! The received rule (the wallet charter's R1 (d)) at the storage's public
//! allocator (`StorageSqlx::allocate_change_input`) and its count
//! (`count_change_inputs`). Their signatures name no rule, so they hold a
//! received coin whose transaction is not `completed`, as the create door
//! does for an action without the label `underReceivedRule`; they take
//! change only of a transaction the door's status filter takes. The
//! wallet's own unproven change stays eligible.

use super::StorageSqlx;
use crate::storage::traits::WalletStorageWriter;
use chrono::Utc;

/// A store holding one 1000-satoshi change coin of `purpose` whose
/// transaction is `status`, and an unsigned spender: the store, the user,
/// the basket, the spender.
async fn one_coin(purpose: &str, status: &str) -> (StorageSqlx, i64, i64, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("public-received-rule", &"0".repeat(64))
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user(&"b".repeat(66)).await.unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();
    let now = Utc::now();
    let mut ids = Vec::new();
    for (status, reference) in [(status, "parent"), ("unsigned", "spender")] {
        let id = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, \
             version, lock_time, description, created_at, updated_at) \
             VALUES (?, ?, ?, 0, 0, 1, 0, 'public rule', ?, ?)",
        )
        .bind(user.user_id)
        .bind(status)
        .bind(reference)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap()
        .last_insert_rowid();
        ids.push(id);
    }
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, \
         locking_script, txid, type, spendable, change, provided_by, purpose, \
         derivation_prefix, derivation_suffix, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, 1000, X'51', ?, 'P2PKH', 1, 1, 'storage', ?, 'p', 's', \
         'public rule', ?, ?)",
    )
    .bind(user.user_id)
    .bind(ids[0])
    .bind(basket.basket_id)
    .bind("ab".repeat(32))
    .bind(purpose)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();
    (storage, user.user_id, basket.basket_id, ids[1])
}

/// The public count, then whether the public pick of a 1000-satoshi target
/// takes the coin.
async fn counted_and_picked(purpose: &str, status: &str, exclude_sending: bool) -> (i64, bool) {
    let (storage, user_id, basket_id, spender) = one_coin(purpose, status).await;
    let count = storage
        .count_change_inputs(user_id, basket_id, exclude_sending)
        .await
        .unwrap();
    let picked = storage
        .allocate_change_input(user_id, basket_id, 1000, exclude_sending, spender)
        .await
        .unwrap();
    let answer = (count, picked.is_some());
    println!("{purpose} of a {status} transaction, exclude_sending={exclude_sending}: {answer:?}");
    answer
}

#[tokio::test]
async fn the_public_allocator_holds_an_unproven_received_coin() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("receive", "unproven", exclude_sending).await,
            (0, false)
        );
    }
}

#[tokio::test]
async fn the_public_allocator_holds_a_received_coin_whose_transaction_is_sending() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("receive", "sending", exclude_sending).await,
            (0, false)
        );
    }
}

#[tokio::test]
async fn the_public_allocator_takes_a_completed_received_coin() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("receive", "completed", exclude_sending).await,
            (1, true)
        );
    }
}

#[tokio::test]
async fn the_public_allocator_still_takes_the_wallets_own_unproven_change() {
    for exclude_sending in [true, false] {
        assert_eq!(
            counted_and_picked("change", "unproven", exclude_sending).await,
            (1, true)
        );
    }
    assert_eq!(
        counted_and_picked("change", "sending", false).await,
        (1, true)
    );
    assert_eq!(
        counted_and_picked("change", "sending", true).await,
        (0, false)
    );
}

#[tokio::test]
async fn the_public_allocator_takes_no_change_of_a_transaction_the_door_does_not() {
    for status in ["failed", "unsigned", "unprocessed", "nosend", "nonfinal"] {
        for exclude_sending in [true, false] {
            assert_eq!(
                counted_and_picked("change", status, exclude_sending).await,
                (0, false),
                "own change of a {status} transaction"
            );
        }
    }
}
