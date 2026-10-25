use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn test_nosend_outputs_not_selected_for_spending() {
    // A change output whose parent tx has status 'nosend' must NOT appear
    // in UTXO selection (count_change_inputs should return 0,
    // allocate_change_input should return None).
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Seed a change output with a 'nosend' parent transaction
    seed_change_output_with_status(&storage, user.user_id, 100_000, "nosend", "aa01").await;

    let mut conn = storage.pool().acquire().await.unwrap();

    // count_change_inputs should see 0 available outputs
    let count = count_change_inputs(&mut conn, user.user_id, basket.basket_id, true)
        .await
        .unwrap();
    assert_eq!(count, 0, "nosend outputs must not be counted as available");

    // allocate_change_input should return None
    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        9999, // dummy transaction_id for allocation
        50_000,
        true,
    )
    .await
    .unwrap();
    assert!(
        allocated.is_none(),
        "nosend outputs must not be allocated for spending"
    );
}

#[tokio::test]
async fn test_completed_outputs_still_selected() {
    // Positive control: a change output whose parent tx has status 'completed'
    // MUST be selected by count_change_inputs and allocate_change_input.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Seed a change output with a 'completed' parent transaction
    seed_change_output_with_status(&storage, user.user_id, 100_000, "completed", "bb01").await;

    // Create a dummy "spending" transaction so allocate_change_input can set spent_by
    // without violating the foreign key constraint.
    let now = Utc::now();
    let spending_tx = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at)
            VALUES (?, 'unsigned', 'spending_ref', 1, 0, 1, 0, 'Spending tx', ?, ?, ?)
            "#,
        )
        .bind(user.user_id)
        .bind("cc00000000000000000000000000000000000000000000000000000000000001")
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .unwrap();
    let spending_tx_id = spending_tx.last_insert_rowid();

    let mut conn = storage.writer().acquire().await.unwrap();

    // count_change_inputs should see 1 available output
    let count = count_change_inputs(&mut conn, user.user_id, basket.basket_id, true)
        .await
        .unwrap();
    assert_eq!(count, 1, "completed outputs must be counted as available");

    // allocate_change_input should return Some
    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spending_tx_id,
        50_000,
        true,
    )
    .await
    .unwrap();
    assert!(
        allocated.is_some(),
        "completed outputs must be allocatable for spending"
    );

    let input = allocated.unwrap();
    assert_eq!(input.satoshis, 100_000);
}

#[tokio::test]
async fn test_confirmed_utxo_preferred_over_unconfirmed() {
    // When both a confirmed ('completed') and unconfirmed ('unproven') output
    // of equal value exist, allocate_change_input should pick the confirmed one.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Insert confirmed (completed) output of 5000 sats
    let (_confirmed_tx_id, confirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 5000, "completed", "conf01").await;

    // Insert unconfirmed (unproven) output of 5000 sats
    let (_unconfirmed_tx_id, _unconfirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 5000, "unproven", "unpr01").await;

    // Create a dummy spending transaction for the allocation
    let spending_tx_id = create_spending_tx(&storage, user.user_id, "test1").await;

    let mut conn = storage.writer().acquire().await.unwrap();

    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spending_tx_id,
        5000,
        true,
    )
    .await
    .unwrap();

    assert!(allocated.is_some(), "Should allocate an output");
    let allocated = allocated.unwrap();

    // The confirmed output should be selected
    assert_eq!(
        allocated.output_id, confirmed_out_id,
        "Confirmed (completed) output should be preferred over unconfirmed (unproven)"
    );
    assert_eq!(allocated.satoshis, 5000);

    // Verify the output was marked as allocated (spent_by set, spendable cleared)
    let row: (Option<i64>, i64) =
        sqlx::query_as("SELECT spent_by, spendable FROM outputs WHERE output_id = ?")
            .bind(confirmed_out_id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(
        row.0,
        Some(spending_tx_id),
        "spent_by should be set to spending tx"
    );
    assert_eq!(row.1, 0, "spendable should be 0 (false) after allocation");
}

#[tokio::test]
async fn test_unconfirmed_utxo_used_when_no_confirmed_available() {
    // When only an unconfirmed ('unproven') output exists,
    // allocate_change_input should still pick it.
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Insert only an unconfirmed (unproven) output of 5000 sats
    let (_unconfirmed_tx_id, unconfirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 5000, "unproven", "unpr02").await;

    let spending_tx_id = create_spending_tx(&storage, user.user_id, "test2").await;

    let mut conn = storage.writer().acquire().await.unwrap();

    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spending_tx_id,
        5000,
        true,
    )
    .await
    .unwrap();

    assert!(
        allocated.is_some(),
        "Should allocate an unconfirmed output when no confirmed ones exist"
    );
    let allocated = allocated.unwrap();

    assert_eq!(
        allocated.output_id, unconfirmed_out_id,
        "The unconfirmed output should be selected"
    );
    assert_eq!(allocated.satoshis, 5000);
}

#[tokio::test]
async fn test_confirmed_preferred_even_if_worse_amount_fit() {
    // A confirmed output of 10000 sats should be preferred over an
    // unconfirmed output of 5000 sats even when requesting ~4500 sats
    // (the unconfirmed one is a closer fit by amount).
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Insert confirmed (completed) output of 10000 sats — farther from target
    let (_confirmed_tx_id, confirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 10_000, "completed", "conf03").await;

    // Insert unconfirmed (unproven) output of 5000 sats — closer to target
    let (_unconfirmed_tx_id, _unconfirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 5_000, "unproven", "unpr03").await;

    let spending_tx_id = create_spending_tx(&storage, user.user_id, "test3").await;

    let mut conn = storage.writer().acquire().await.unwrap();

    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spending_tx_id,
        4500,
        true,
    )
    .await
    .unwrap();

    assert!(allocated.is_some(), "Should allocate an output");
    let allocated = allocated.unwrap();

    // The confirmed output (10000) should be selected, even though the
    // unconfirmed one (5000) is closer to the target (4500).
    // The ORDER BY sorts by status first: completed=0 < unproven=1.
    assert_eq!(
        allocated.output_id, confirmed_out_id,
        "Confirmed output should be preferred even when unconfirmed is a closer amount fit"
    );
    assert_eq!(allocated.satoshis, 10_000);
}

/// When proven outputs exist but are too small to cover the target,
/// coin selection MUST fall back to unproven outputs instead of failing.
/// This was the bug: 300 sat proven output selected over 8.9M unproven → "Insufficient funds".
#[tokio::test]
async fn test_fallback_to_unproven_when_proven_insufficient() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();

    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

    // Insert a tiny confirmed output (300 sats) — proven but too small
    let (_confirmed_tx_id, _confirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 300, "completed", "tiny01").await;

    // Insert a large unconfirmed output (9_000_000 sats) — unproven but big enough
    let (_unconfirmed_tx_id, unconfirmed_out_id) =
        seed_tagged_change_output(&storage, user.user_id, 9_000_000, "unproven", "big01").await;

    let spending_tx_id = create_spending_tx(&storage, user.user_id, "spend1").await;
    let mut conn = storage.writer().acquire().await.unwrap();

    // Target: 270_000 sats (typical x402 upfront payment)
    let allocated = allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spending_tx_id,
        270_000,
        true,
    )
    .await
    .unwrap();

    assert!(
        allocated.is_some(),
        "Must allocate even when proven output is too small — should fall back to unproven"
    );
    let allocated = allocated.unwrap();
    assert_eq!(
        allocated.output_id, unconfirmed_out_id,
        "Should select the large unproven output, not the tiny proven one"
    );
    assert_eq!(allocated.satoshis, 9_000_000);
}
