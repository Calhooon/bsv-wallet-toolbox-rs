use super::super::*;
use super::*;
use crate::storage::traits::WalletStorageWriter;

#[tokio::test]
async fn public_allocation_preserves_the_create_door_input_type() {
    for exclude_sending in [true, false] {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage.migrate("allocation-type", "fixture").await.unwrap();
        storage.make_available().await.unwrap();
        let (user, _) = storage.find_or_insert_user("fixture-user").await.unwrap();
        seed_change_output(&storage, user.user_id, 100_000).await;
        let auth = crate::storage::AuthId::with_user_id("fixture-user", user.user_id);
        let created = storage
            .create_action(&auth, super::super::seed_tests::action_args())
            .await
            .unwrap();
        assert_eq!(created.inputs.len(), 1);
        let input = &created.inputs[0];
        assert_eq!(input.input_type, "P2PKH");

        let (output_id, transaction_id, basket_id, stored_type): (i64, i64, i64, String) =
            sqlx::query_as(
                "SELECT output_id, spent_by, basket_id, type FROM outputs \
                 WHERE user_id = ? AND txid = ? AND vout = ?",
            )
            .bind(user.user_id)
            .bind(&input.source_txid)
            .bind(input.source_vout)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(stored_type, input.input_type);

        // The door has its own allocator. Release its fixture coin so the
        // public allocator can claim the same row for the same transaction.
        let mut conn = storage.writer().acquire().await.unwrap();
        release_change_input(&mut conn, output_id, &mut ChangePick::Allocate)
            .await
            .unwrap();
        drop(conn);
        let allocated = storage
            .allocate_change_input(
                user.user_id,
                basket_id,
                i64::try_from(input.source_satoshis).unwrap(),
                exclude_sending,
                transaction_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(allocated.output_id, output_id);
        assert_eq!(allocated.spent_by, Some(transaction_id));
        assert!(!allocated.spendable);
        assert_eq!(allocated.output_type, input.input_type);
        assert_eq!(
            serde_json::to_value(&allocated).unwrap()["outputType"],
            "P2PKH"
        );
    }
}

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

mod sync_received {
    use super::*;
    use crate::storage::entities::{
        TableOutput, TableOutputBasket, TableTransaction, TransactionStatus,
    };
    use crate::storage::sqlx::sync::{
        entity_names, get_sync_chunk_internal, process_sync_chunk_internal,
    };
    use crate::storage::{RequestSyncChunkArgs, SyncChunk, SyncMap, SyncOffset};

    fn sync_args() -> RequestSyncChunkArgs {
        RequestSyncChunkArgs {
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
            identity_key: "b".repeat(66),
            since: None,
            max_rough_size: 100_000,
            max_items: 1000,
            offsets: [
                entity_names::OUTPUT_BASKET,
                entity_names::TRANSACTION,
                entity_names::OUTPUT,
            ]
            .into_iter()
            .map(|name| SyncOffset {
                name: name.to_string(),
                offset: 0,
            })
            .collect(),
        }
    }

    async fn empty_storage() -> StorageSqlx {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("received-sync", &"c".repeat(66))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        storage
    }

    fn received_chunk(args: &RequestSyncChunkArgs) -> SyncChunk {
        let now = Utc::now();
        SyncChunk {
            from_storage_identity_key: args.from_storage_identity_key.clone(),
            to_storage_identity_key: args.to_storage_identity_key.clone(),
            user_identity_key: args.identity_key.clone(),
            output_baskets: Some(vec![TableOutputBasket {
                basket_id: 300,
                user_id: 1,
                name: "default".to_string(),
                number_of_desired_utxos: 10,
                minimum_desired_utxo_value: 1000,
                created_at: now,
                updated_at: now,
            }]),
            transactions: Some(vec![TableTransaction {
                transaction_id: 200,
                user_id: 1,
                txid: Some("d".repeat(64)),
                status: TransactionStatus::Unproven,
                reference: "received-sync-payment".to_string(),
                description: "Received wallet payment".to_string(),
                satoshis: 10_000,
                version: 1,
                lock_time: 0,
                raw_tx: None,
                input_beef: None,
                is_outgoing: false,
                proof_txid: None,
                created_at: now,
                updated_at: now,
            }]),
            outputs: Some(vec![TableOutput {
                output_id: 100,
                user_id: 1,
                transaction_id: 200,
                basket_id: Some(300),
                txid: "d".repeat(64),
                vout: 0,
                satoshis: 10_000,
                locking_script: Some(vec![0x51]),
                script_length: 1,
                script_offset: 0,
                output_type: "P2PKH".to_string(),
                provided_by: "storage".to_string(),
                purpose: Some("receive".to_string()),
                output_description: None,
                spent_by: None,
                sequence_number: None,
                spending_description: None,
                spendable: true,
                change: true,
                derivation_prefix: None,
                derivation_suffix: None,
                sender_identity_key: None,
                custom_instructions: None,
                created_at: now,
                updated_at: now,
            }]),
            ..SyncChunk::default()
        }
    }

    async fn stored_output(storage: &StorageSqlx) -> sqlx::sqlite::SqliteRow {
        let outputs =
            sqlx::query("SELECT output_id, user_id, basket_id, purpose, provided_by FROM outputs")
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(outputs.len(), 1);
        outputs.into_iter().next().unwrap()
    }

    async fn assert_received_rule(storage: &StorageSqlx) {
        let output = stored_output(storage).await;
        assert_eq!(output.get::<String, _>("purpose"), "receive");
        assert_eq!(output.get::<String, _>("provided_by"), "storage");
        let user_id = output.get("user_id");
        let basket_id = output.get("basket_id");
        let output_id = output.get("output_id");
        let mut conn = storage.pool().acquire().await.unwrap();
        for allow_received in [false, true] {
            let count =
                count_change_inputs_under_rule(&mut conn, user_id, basket_id, true, allow_received)
                    .await
                    .unwrap();
            let mut reserved = PickReservation::new(std::sync::Arc::default());
            let picked = allocate_change_input_by(
                &mut conn,
                user_id,
                basket_id,
                0,
                1000,
                true,
                &mut ChangePick::DryRun {
                    reserved: &mut reserved,
                    skip: &[],
                    offer: Some(&[output_id]),
                    allow_received,
                },
            )
            .await
            .unwrap();
            assert_eq!(count, usize::from(allow_received));
            assert_eq!(picked.is_some(), allow_received);
        }
    }

    #[tokio::test]
    async fn sync_received_insert_preserves_origin_and_action_rule() {
        let storage = empty_storage().await;
        let args = sync_args();
        let chunk = received_chunk(&args);
        process_sync_chunk_internal(&storage, args.clone(), chunk, &mut SyncMap::default())
            .await
            .unwrap();
        assert_received_rule(&storage).await;
    }

    #[tokio::test]
    async fn sync_received_update_preserves_origin_and_action_rule() {
        let storage = empty_storage().await;
        let args = sync_args();
        let mut older = received_chunk(&args);
        let output = &mut older.outputs.as_mut().unwrap()[0];
        output.purpose = Some("change".to_string());
        output.provided_by = "you".to_string();
        process_sync_chunk_internal(
            &storage,
            args.clone(),
            older.clone(),
            &mut SyncMap::default(),
        )
        .await
        .unwrap();

        let mut newer = older.clone();
        let output = &mut newer.outputs.as_mut().unwrap()[0];
        output.purpose = Some("receive".to_string());
        output.provided_by = "storage".to_string();
        output.updated_at += chrono::Duration::seconds(10);
        process_sync_chunk_internal(&storage, args.clone(), newer, &mut SyncMap::default())
            .await
            .unwrap();
        assert_received_rule(&storage).await;

        // An older chunk cannot remove the received mark after the update.
        process_sync_chunk_internal(&storage, args.clone(), older, &mut SyncMap::default())
            .await
            .unwrap();
        assert_received_rule(&storage).await;
    }

    #[tokio::test]
    async fn sync_received_roundtrip_preserves_origin_and_action_rule() {
        let source = empty_storage().await;
        let args = sync_args();
        process_sync_chunk_internal(
            &source,
            args.clone(),
            received_chunk(&args),
            &mut SyncMap::default(),
        )
        .await
        .unwrap();
        // Seed the stored mark directly to test chunk construction independently of ingestion.
        sqlx::query("UPDATE outputs SET purpose = 'receive', provided_by = 'storage'")
            .execute(source.writer())
            .await
            .unwrap();
        let chunk = get_sync_chunk_internal(&source, args.clone())
            .await
            .unwrap();
        let output = &chunk.outputs.as_ref().unwrap()[0];
        assert_eq!(output.purpose.as_deref(), Some("receive"));
        assert_eq!(output.provided_by, "storage");

        let destination = empty_storage().await;
        process_sync_chunk_internal(&destination, args.clone(), chunk, &mut SyncMap::default())
            .await
            .unwrap();
        assert_received_rule(&destination).await;
    }

    #[tokio::test]
    async fn sync_received_missing_purpose_keeps_legacy_default() {
        let storage = empty_storage().await;
        let args = sync_args();
        let mut chunk = received_chunk(&args);
        chunk.outputs.as_mut().unwrap()[0].purpose = None;
        process_sync_chunk_internal(&storage, args.clone(), chunk, &mut SyncMap::default())
            .await
            .unwrap();
        assert_eq!(
            stored_output(&storage).await.get::<String, _>("purpose"),
            "change"
        );
    }
}
